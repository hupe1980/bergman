//! A real Iceberg table on the local filesystem, and a committer that applies
//! commits to it.
//!
//! Compaction and manifest rewriting write Parquet, write manifests, and commit
//! snapshots that *remove* files. Unit tests can check the rules those
//! operations follow; only an actual table can check that following them
//! produces a table an engine can still read.
//!
//! No containers and no cloud credentials: a temporary directory, upstream's
//! own writers, and an in-memory catalog built on the two public entry points
//! `TableRequirement::check` and `TableUpdate::apply` — which is exactly what a
//! REST catalog does with a commit when it receives one.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bergman::commit::TableCommitter;
use bergman::error::{Error, Result};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataFile, DataFileFormat, NestedField, PrimitiveType, Schema, TableMetadata,
    TableMetadataBuilder, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{TableIdent, TableRequirement, TableUpdate};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

/// A table in a temporary directory, plus the committer that maintains it.
pub struct TestTable {
    pub dir: tempfile::TempDir,
    pub ident: TableIdent,
    pub file_io: FileIO,
    pub committer: Arc<InMemoryCommitter>,
}

impl TestTable {
    /// Create an unpartitioned two-column table with no snapshots.
    pub fn new() -> Result<Self> {
        let dir = tempfile::tempdir().expect("temp dir");
        let location = format!("file://{}", dir.path().display());

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");

        let metadata = TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            iceberg::spec::SortOrder::unsorted_order(),
            location,
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;

        let file_io = FileIO::new_with_fs();
        let ident = TableIdent::from_strs(["db", "events"]).expect("ident");

        Ok(Self {
            dir,
            ident,
            file_io,
            committer: Arc::new(InMemoryCommitter::new(metadata)),
        })
    }

    /// The table as it currently stands.
    pub fn table(&self) -> Table {
        let metadata = self.committer.metadata();
        Table::builder()
            .identifier(self.ident.clone())
            .metadata(metadata)
            .file_io(self.file_io.clone())
            // Iceberg never spawns a runtime of its own; it borrows the
            // caller's, which is the same contract Bergman's library API has.
            .runtime(iceberg::Runtime::try_current().expect("inside a tokio runtime"))
            .metadata_location(format!("{}/metadata/v1.metadata.json", self.location()))
            .build()
            .expect("table")
    }

    pub fn location(&self) -> String {
        format!("file://{}", self.dir.path().display())
    }

    /// Write one Parquet data file holding these rows.
    pub async fn write_data_file(&self, rows: &[(i32, &str)]) -> Result<DataFile> {
        let table = self.table();
        let metadata = table.metadata();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(
                    rows.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch");

        let location_generator =
            DefaultLocationGenerator::new(metadata).map_err(|e| Error::Storage(Box::new(e)))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            "test".to_string(),
            Some(uuid::Uuid::new_v4().to_string()),
            DataFileFormat::Parquet,
        );

        let rolling = RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(
                parquet::file::properties::WriterProperties::builder().build(),
                metadata.current_schema().clone(),
            ),
            // Large enough that each call produces exactly one file, so a test
            // controls how many files the table has.
            1024 * 1024 * 1024,
            self.file_io.clone(),
            location_generator,
            file_name_generator,
        );

        let mut writer = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        writer
            .write(batch)
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let mut files = writer
            .close()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        Ok(files.pop().expect("one data file"))
    }

    /// Append data files as a new snapshot.
    ///
    /// Uses Bergman's own snapshot producer, so the fixture and the code under
    /// test agree about what a commit looks like.
    pub async fn append(&self, files: Vec<DataFile>) -> Result<()> {
        use bergman::commit::{RewriteFiles, SnapshotProducer};

        let table = self.table();
        let producer = SnapshotProducer::new(&table);
        let rewrite = RewriteFiles {
            removed: Vec::new(),
            added: files,
        };

        let (requirements, updates) = producer
            .rewrite_files(&rewrite)
            .await?
            .expect("an append changes something");

        self.committer
            .commit(&self.ident, requirements, updates)
            .await
    }
}

/// A catalog that lives in a `Mutex`.
///
/// It does what a REST catalog does with a commit: check every requirement
/// against the current metadata, then apply the updates. Both entry points are
/// upstream's own (`TableRequirement::check`, `TableUpdate::apply`), so a commit
/// that this accepts is one a real catalog accepts.
#[derive(Debug)]
pub struct InMemoryCommitter {
    metadata: Mutex<TableMetadata>,
    /// Commits accepted so far, for assertions.
    pub commits: Mutex<usize>,
    /// When set, the next commit is rejected as a conflict.
    pub fail_next_as_conflict: Mutex<bool>,
}

impl InMemoryCommitter {
    pub fn new(metadata: TableMetadata) -> Self {
        Self {
            metadata: Mutex::new(metadata),
            commits: Mutex::new(0),
            fail_next_as_conflict: Mutex::new(false),
        }
    }

    pub fn metadata(&self) -> TableMetadata {
        self.metadata.lock().unwrap().clone()
    }

    pub fn commit_count(&self) -> usize {
        *self.commits.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl TableCommitter for InMemoryCommitter {
    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<()> {
        if std::mem::replace(&mut *self.fail_next_as_conflict.lock().unwrap(), false) {
            return Err(Error::CommitConflict {
                table: ident.to_string(),
                detail: "injected".into(),
            });
        }

        let mut current = self.metadata.lock().unwrap();

        // Exactly what a catalog does: every precondition, then the updates.
        for requirement in requirements {
            requirement
                .check(Some(&current))
                .map_err(|e| Error::CommitConflict {
                    table: ident.to_string(),
                    detail: e.to_string(),
                })?;
        }

        let mut builder = current.clone().into_builder(None);
        for update in updates {
            builder = update.apply(builder).map_err(Error::from)?;
        }

        *current = builder.build().map_err(Error::from)?.metadata;
        *self.commits.lock().unwrap() += 1;
        Ok(())
    }
}

/// Read every row of a table, in file order.
pub async fn read_all(table: &Table) -> Result<Vec<(i32, String)>> {
    use futures::StreamExt;

    let scan = table.scan().build()?;
    let mut stream = scan.to_arrow().await?;

    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| Error::Storage(Box::new(e)))?;
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32");
        let names = batch
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");

        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    Ok(rows)
}

/// The manifest paths the current snapshot references.
pub async fn manifest_paths(table: &Table) -> Result<Vec<String>> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(Vec::new());
    };

    let bytes = table
        .file_io()
        .new_input(snapshot.manifest_list())
        .map_err(|e| Error::Storage(Box::new(e)))?
        .read()
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;

    let list = iceberg::spec::ManifestList::parse_with_version(&bytes, metadata.format_version())
        .map_err(Error::from)?;

    let mut paths: Vec<String> = list
        .entries()
        .iter()
        .map(|m| m.manifest_path.clone())
        .collect();
    paths.sort();
    Ok(paths)
}

/// Every live data file path in the current snapshot.
pub async fn live_data_files(table: &Table) -> Result<Vec<String>> {
    use futures::StreamExt;

    let scan = table.scan().build()?;
    let mut stream = scan.plan_files().await?;

    let mut paths = Vec::new();
    while let Some(task) = stream.next().await {
        paths.push(task?.data_file_path);
    }
    paths.sort();
    Ok(paths)
}
