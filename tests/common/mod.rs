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
    /// Create an unpartitioned two-column v2 table with no snapshots.
    pub fn new() -> Result<Self> {
        Self::with_format(iceberg::spec::FormatVersion::V2)
    }

    /// The same, at a chosen format version.
    ///
    /// v3 exists here so the refusal can be tested against a real table rather
    /// than asserted about a constant: what has to hold is that Bergman *stops*,
    /// not that a function returns a string.
    pub fn with_format(format: iceberg::spec::FormatVersion) -> Result<Self> {
        Self::build(format, iceberg::spec::SortOrder::unsorted_order())
    }

    /// A table that declares a sort order of its own.
    ///
    /// The layer that stops a rewrite destroying a layout the table configured
    /// can only be checked against a table that actually declares one.
    pub fn sorted_by(fields: Vec<(i32, iceberg::spec::SortDirection)>) -> Result<Self> {
        use iceberg::spec::{NullOrder, SortField, SortOrder, Transform};

        let order = SortOrder::builder()
            .with_order_id(1)
            .with_fields(
                fields
                    .into_iter()
                    .map(|(source_id, direction)| SortField {
                        source_id,
                        transform: Transform::Identity,
                        direction,
                        null_order: match direction {
                            iceberg::spec::SortDirection::Ascending => NullOrder::First,
                            iceberg::spec::SortDirection::Descending => NullOrder::Last,
                        },
                    })
                    .collect::<Vec<_>>(),
            )
            .build(&Self::schema())
            .expect("sort order");

        Self::build(iceberg::spec::FormatVersion::V2, order)
    }

    /// A table partitioned by `id` (identity).
    ///
    /// The scan API does not report which partition spec produced a file's
    /// partition tuple, so anything that groups scanned files by partition has
    /// to get that from somewhere else. Only a partitioned table shows whether
    /// it does.
    pub fn partitioned() -> Result<Self> {
        use iceberg::spec::{Transform, UnboundPartitionSpec};

        let dir = tempfile::tempdir().expect("temp dir");
        let location = format!("file://{}", dir.path().display());

        let spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "id", Transform::Identity)
            .expect("partition field")
            .build();

        let metadata = TableMetadataBuilder::new(
            Self::schema(),
            spec,
            iceberg::spec::SortOrder::unsorted_order(),
            location,
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;

        Ok(Self {
            dir,
            ident: TableIdent::from_strs(["db", "events"]).expect("ident"),
            file_io: FileIO::new_with_fs(),
            committer: Arc::new(InMemoryCommitter::new(metadata)),
        })
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn build(
        format: iceberg::spec::FormatVersion,
        sort_order: iceberg::spec::SortOrder,
    ) -> Result<Self> {
        let dir = tempfile::tempdir().expect("temp dir");
        let location = format!("file://{}", dir.path().display());

        let metadata = TableMetadataBuilder::new(
            Self::schema(),
            UnboundPartitionSpec::builder().build(),
            sort_order,
            location,
            format,
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

    /// A catalog over this fixture's metadata.
    pub fn catalog(&self) -> Arc<dyn iceberg::Catalog> {
        Arc::new(FixtureCatalog {
            committer: Arc::clone(&self.committer),
            file_io: self.file_io.clone(),
            location: self.location(),
        })
    }

    /// A loader over this fixture's metadata.
    pub fn loader(&self) -> FixtureLoader {
        FixtureLoader {
            committer: Arc::clone(&self.committer),
            file_io: self.file_io.clone(),
            location: self.location(),
        }
    }

    /// Write one Parquet data file holding these rows.
    pub async fn write_data_file(&self, rows: &[(i32, &str)]) -> Result<DataFile> {
        self.write_data_file_into(rows, None).await
    }

    /// The same, into a named partition value.
    pub async fn write_partitioned_data_file(
        &self,
        rows: &[(i32, &str)],
        partition_id: i32,
    ) -> Result<DataFile> {
        use iceberg::spec::{Literal, PartitionKey, Struct};

        let table = self.table();
        let metadata = table.metadata();
        let key = PartitionKey::new(
            metadata.default_partition_spec().as_ref().clone(),
            metadata.current_schema().clone(),
            Struct::from_iter([Some(Literal::int(partition_id))]),
        );
        self.write_data_file_into(rows, Some(key)).await
    }

    async fn write_data_file_into(
        &self,
        rows: &[(i32, &str)],
        partition: Option<iceberg::spec::PartitionKey>,
    ) -> Result<DataFile> {
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
            .build(partition)
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
    /// Uses Bergman's own snapshot producer where it can, so the fixture and
    /// the code under test agree about what a commit looks like.
    ///
    /// A v3 table is the exception, and deliberately so: Bergman refuses to
    /// author a v3 snapshot at all (see `bergman::commit::authoring_refusal`),
    /// which is exactly what the v3 tests exist to check — so the fixture has
    /// to be able to build one the way a *writer* would, or those tests could
    /// only ever run against an empty table.
    pub async fn append(&self, files: Vec<DataFile>) -> Result<()> {
        if self.committer.metadata().format_version() == iceberg::spec::FormatVersion::V3 {
            return self.append_as_a_writer_would(files).await;
        }

        use bergman::commit::{BranchRetention, RewriteFiles, SnapshotProducer};

        let table = self.table();
        let retention = BranchRetention::load(&table).await?;
        let producer = SnapshotProducer::new(&table, retention);
        let rewrite = RewriteFiles {
            removed: Vec::new(),
            added: files,
        };

        let (requirements, updates) = producer
            .rewrite_files(&rewrite)
            .await?
            .expect("an append changes something");

        self.committer
            .commit(&self.ident, requirements, updates, fixture_ctx())
            .await
    }

    /// Append with row lineage, the way a v3-capable writer does.
    ///
    /// The snapshot declares the row-id range it consumes: `first-row-id` is
    /// the table's current `next-row-id`, and `added-rows` is what it appends.
    /// `TableMetadataBuilder::add_snapshot` rejects a v3 snapshot without it —
    /// which is one of the reasons Bergman refuses to write one rather than
    /// guessing at the numbers.
    async fn append_as_a_writer_would(&self, files: Vec<DataFile>) -> Result<()> {
        use iceberg::spec::{
            FormatVersion, ManifestFile, ManifestListWriter, ManifestWriterBuilder, Operation,
            Snapshot, SnapshotReference, SnapshotRetention, Summary,
        };

        let table = self.table();
        let metadata = table.metadata();
        let parent = metadata.current_snapshot().cloned();
        let snapshot_id = i64::from(uuid::Uuid::new_v4().as_u128() as u32)
            .abs()
            .max(1);
        let sequence_number = metadata.next_sequence_number();
        let added_rows: u64 = files.iter().map(|f| f.record_count()).sum();

        let mut manifests: Vec<ManifestFile> = match &parent {
            Some(parent) => {
                let bytes = self
                    .file_io
                    .new_input(parent.manifest_list())
                    .map_err(|e| Error::Storage(Box::new(e)))?
                    .read()
                    .await
                    .map_err(|e| Error::Storage(Box::new(e)))?;
                iceberg::spec::ManifestList::parse_with_version(&bytes, FormatVersion::V3)
                    .map_err(|e| Error::Storage(Box::new(e)))?
                    .entries()
                    .to_vec()
            }
            None => Vec::new(),
        };

        let location = format!(
            "{}/metadata/{snapshot_id}-data-{}.avro",
            self.location(),
            uuid::Uuid::new_v4()
        );
        let output = self
            .file_io
            .new_output(&location)
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v3_data();
        for file in files {
            writer
                .add_file(file, sequence_number)
                .map_err(|e| Error::Storage(Box::new(e)))?;
        }
        manifests.push(
            writer
                .write_manifest_file()
                .await
                .map_err(|e| Error::Storage(Box::new(e)))?,
        );

        let list_location = format!(
            "{}/metadata/snap-{snapshot_id}-{}.avro",
            self.location(),
            uuid::Uuid::new_v4()
        );
        let list_output = self
            .file_io
            .new_output(&list_location)
            .map_err(|e| Error::Storage(Box::new(e)))?
            .writer()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let mut list = ManifestListWriter::v3(
            list_output,
            snapshot_id,
            parent.as_ref().map(|p| p.snapshot_id()),
            sequence_number,
            Some(metadata.next_row_id()),
        );
        list.add_manifests(manifests.into_iter())
            .map_err(|e| Error::Storage(Box::new(e)))?;
        list.close()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent.as_ref().map(|p| p.snapshot_id()))
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list(list_location)
            .with_schema_id(metadata.current_schema_id())
            .with_row_range(metadata.next_row_id(), added_rows)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        self.committer
            .commit(
                &self.ident,
                vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".to_string(),
                    snapshot_id: parent.as_ref().map(|p| p.snapshot_id()),
                }],
                vec![
                    TableUpdate::AddSnapshot { snapshot },
                    TableUpdate::SetSnapshotRef {
                        ref_name: "main".to_string(),
                        reference: SnapshotReference::new(
                            snapshot_id,
                            SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        ),
                    },
                ],
                fixture_ctx(),
            )
            .await
    }
}

/// Serves the fixture's current metadata as a table.
///
/// Orphan removal reloads the table between listing and deleting, and this is
/// the whole of what it needs — one method rather than a catalog.
#[derive(Debug)]
pub struct FixtureLoader {
    committer: Arc<InMemoryCommitter>,
    file_io: FileIO,
    location: String,
}

#[async_trait::async_trait]
impl bergman::ops::TableLoader for FixtureLoader {
    async fn reload(&self, ident: &TableIdent) -> Result<Table> {
        Table::builder()
            .identifier(ident.clone())
            .metadata(self.committer.metadata())
            .file_io(self.file_io.clone())
            .runtime(iceberg::Runtime::try_current().expect("inside a tokio runtime"))
            .metadata_location(format!("{}/metadata/v1.metadata.json", self.location))
            .build()
            .map_err(Into::into)
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
    /// Commits *offered*, accepted or not.
    ///
    /// The difference between this and `commits` is wasted work: a rewrite whose
    /// compare-and-swap is rejected has already read and written its whole file
    /// group by the time it is refused. A test that only counted successes could
    /// not see a compactor doing everything twice.
    pub attempts: Mutex<usize>,
    /// When set, the next commit is rejected as a conflict.
    pub fail_next_as_conflict: Mutex<bool>,
    /// When set, every commit is rejected as a conflict.
    ///
    /// For exercising the give-up path: a table being written hard keeps
    /// winning, and the right response is to come back next cycle.
    pub always_conflict: Mutex<bool>,
}

impl InMemoryCommitter {
    pub fn new(metadata: TableMetadata) -> Self {
        Self {
            metadata: Mutex::new(metadata),
            commits: Mutex::new(0),
            attempts: Mutex::new(0),
            fail_next_as_conflict: Mutex::new(false),
            always_conflict: Mutex::new(false),
        }
    }

    pub fn metadata(&self) -> TableMetadata {
        self.metadata.lock().unwrap().clone()
    }

    pub fn commit_count(&self) -> usize {
        *self.commits.lock().unwrap()
    }

    /// How many commits were offered, accepted or not.
    pub fn attempt_count(&self) -> usize {
        *self.attempts.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl TableCommitter for InMemoryCommitter {
    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
        _ctx: bergman::obs::OperationContext<'_>,
    ) -> Result<()> {
        *self.attempts.lock().unwrap() += 1;
        if *self.always_conflict.lock().unwrap()
            || std::mem::replace(&mut *self.fail_next_as_conflict.lock().unwrap(), false)
        {
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

/// A catalog over the fixture's metadata.
///
/// Snapshot expiration goes through upstream's `Transaction`, which commits to
/// an `iceberg::Catalog` — so unlike orphan removal, it cannot be tested through
/// a narrower seam. Only two methods carry weight: `load_table` and
/// `update_table`, the latter doing exactly what a REST catalog does with a
/// commit. The rest are unreachable from any path under test, and saying so
/// loudly beats returning a plausible wrong answer.
#[derive(Debug)]
pub struct FixtureCatalog {
    committer: Arc<InMemoryCommitter>,
    file_io: FileIO,
    location: String,
}

impl FixtureCatalog {
    fn table_for(&self, ident: &TableIdent) -> Result<Table> {
        Table::builder()
            .identifier(ident.clone())
            .metadata(self.committer.metadata())
            .file_io(self.file_io.clone())
            .runtime(iceberg::Runtime::try_current().expect("inside a tokio runtime"))
            .metadata_location(format!("{}/metadata/v1.metadata.json", self.location))
            .build()
            .map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl iceberg::Catalog for FixtureCatalog {
    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        self.table_for(ident)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))
    }

    async fn update_table(&self, mut commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        let ident = commit.identifier().clone();
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        self.committer
            .commit(&ident, requirements, updates, fixture_ctx())
            .await
            .map_err(|e| {
                // Conflicts keep their identity across the boundary, or the
                // retry logic under test would see a generic failure.
                let kind = if e.is_replan() {
                    iceberg::ErrorKind::CatalogCommitConflicts
                } else {
                    iceberg::ErrorKind::Unexpected
                };
                iceberg::Error::new(kind, e.to_string())
            })?;

        self.load_table(&ident).await
    }

    async fn list_namespaces(
        &self,
        _parent: Option<&iceberg::NamespaceIdent>,
    ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
        unimplemented!("no path under test lists namespaces")
    }
    async fn create_namespace(
        &self,
        _ns: &iceberg::NamespaceIdent,
        _props: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        unimplemented!("no path under test creates namespaces")
    }
    async fn get_namespace(
        &self,
        _ns: &iceberg::NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        unimplemented!("no path under test reads namespaces")
    }
    async fn namespace_exists(&self, _ns: &iceberg::NamespaceIdent) -> iceberg::Result<bool> {
        unimplemented!("no path under test checks namespaces")
    }
    async fn update_namespace(
        &self,
        _ns: &iceberg::NamespaceIdent,
        _props: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        unimplemented!("no path under test updates namespaces")
    }
    async fn drop_namespace(&self, _ns: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
        unimplemented!("no path under test drops namespaces")
    }
    async fn list_tables(&self, _ns: &iceberg::NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        unimplemented!("no path under test lists tables")
    }
    async fn create_table(
        &self,
        _ns: &iceberg::NamespaceIdent,
        _creation: iceberg::TableCreation,
    ) -> iceberg::Result<Table> {
        unimplemented!("no path under test creates tables")
    }
    async fn drop_table(&self, _ident: &TableIdent) -> iceberg::Result<()> {
        unimplemented!("no path under test drops tables")
    }
    async fn purge_table(&self, _ident: &TableIdent) -> iceberg::Result<()> {
        unimplemented!("no path under test purges tables")
    }
    async fn table_exists(&self, _ident: &TableIdent) -> iceberg::Result<bool> {
        unimplemented!("no path under test checks tables")
    }
    async fn rename_table(&self, _src: &TableIdent, _dst: &TableIdent) -> iceberg::Result<()> {
        unimplemented!("no path under test renames tables")
    }
    async fn register_table(
        &self,
        _ident: &TableIdent,
        _location: String,
    ) -> iceberg::Result<Table> {
        unimplemented!("no path under test registers tables")
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

/// Every delete file the scan still associates with a live data file.
///
/// A delete file that survives a rewrite it was fully applied in is pure read
/// overhead — every scan opens it and it hides nothing — so tests assert on
/// this directly rather than trusting a summary line.
pub async fn live_delete_files(table: &Table) -> Result<Vec<String>> {
    use futures::StreamExt;

    let scan = table.scan().build()?;
    let mut stream = scan.plan_files().await?;

    let mut paths = Vec::new();
    while let Some(task) = stream.next().await {
        for delete in task?.deletes {
            paths.push(delete.file_path);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// The environment every operation takes, assembled from a fixture.
///
/// Operations receive one `OpEnv` rather than eight positional handles, and
/// tests should build it the same way the engine does — otherwise a change to
/// what an operation needs shows up as eight edits per test.
/// The context the *fixture's own* commits carry.
///
/// The fixture plays the foreground writer — appends, equality deletes — which
/// is not maintenance at all, so nothing here needs to be true of a real run.
/// It exists because [`bergman::commit::TableCommitter`] asks every commit to
/// say which run produced it, and a writer that could not answer would be a
/// writer Bergman's own commit path could not be tested against.
pub fn fixture_ctx() -> bergman::obs::OperationContext<'static> {
    static TABLE: std::sync::LazyLock<bergman::policy::TableRef> =
        std::sync::LazyLock::new(|| bergman::policy::TableRef::new("prod", ["db"], "events"));

    bergman::obs::OperationContext {
        run_id: "fixture",
        table: &TABLE,
        kind: bergman::plan::OperationKind::Compact,
        matched_rule: "fixture",
        reason: "the test fixture writing as a foreground writer would",
    }
}

pub fn op_env<'a>(
    table: &'a Table,
    ident: &'a TableIdent,
    loader: &'a FixtureLoader,
    committer: &'a InMemoryCommitter,
    ctx: bergman::obs::OperationContext<'a>,
) -> bergman::ops::OpEnv<'a> {
    static NOOP: bergman::obs::NoopObserver = bergman::obs::NoopObserver;
    bergman::ops::OpEnv {
        table,
        ident,
        loader,
        committer,
        observer: &NOOP,
        ctx,
        now: chrono::Utc::now(),
        max_deletes_per_run: 100_000,
    }
}

impl TestTable {
    /// Write an equality delete file naming these `id` values.
    ///
    /// This is what a streaming writer produces: a file listing the *key* of
    /// every row that should stop being visible, matched by value rather than
    /// by position. Bergman never writes one — the fixture does, because
    /// applying them is the thing under test.
    pub async fn write_equality_delete(&self, ids: &[i32]) -> Result<DataFile> {
        use iceberg::writer::base_writer::equality_delete_writer::{
            EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
        };

        let table = self.table();
        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();

        // The delete file carries only the equality columns — here `id`,
        // field 1 — and the writer projects the batch down to them.
        let config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())
            .map_err(|e| Error::Storage(Box::new(e)))?;

        // The delete file holds only the equality columns, so the Parquet
        // writer is given the *projected* schema. Handing it the table's full
        // schema makes it look for a column the projected batch does not have.
        let delete_schema = Arc::new(
            iceberg::arrow::arrow_schema_to_schema(config.projected_arrow_schema_ref())
                .map_err(|e| Error::Storage(Box::new(e)))?,
        );

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
                Arc::new(Int32Array::from(ids.to_vec())),
                // Projected away by the writer; present because the config
                // projects from the table's full schema.
                Arc::new(StringArray::from(vec![""; ids.len()])),
            ],
        )
        .expect("batch");

        let location_generator =
            DefaultLocationGenerator::new(metadata).map_err(|e| Error::Storage(Box::new(e)))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            "test-eq-delete".to_string(),
            Some(uuid::Uuid::new_v4().to_string()),
            DataFileFormat::Parquet,
        );

        let rolling = RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(
                parquet::file::properties::WriterProperties::builder().build(),
                delete_schema,
            ),
            1024 * 1024 * 1024,
            self.file_io.clone(),
            location_generator,
            file_name_generator,
        );

        let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
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

        Ok(files.pop().expect("one delete file"))
    }

    /// Commit delete files as a new snapshot.
    ///
    /// Bergman's `SnapshotProducer` puts everything it adds in a *data*
    /// manifest, because Bergman only ever adds data files. A delete file has
    /// to go in a delete manifest, so the fixture builds this commit itself
    /// with upstream's writers — which is exactly what a streaming writer does.
    pub async fn append_deletes(&self, deletes: Vec<DataFile>) -> Result<()> {
        use iceberg::spec::{
            FormatVersion, ManifestFile, ManifestListWriter, ManifestWriterBuilder, Operation,
            Snapshot, SnapshotReference, SnapshotRetention, Summary,
        };
        use iceberg::{TableRequirement, TableUpdate};

        let table = self.table();
        let metadata = table.metadata();
        let parent = metadata.current_snapshot().cloned();
        let snapshot_id = i64::from(std::process::id()) * 1_000_000
            + i64::from(uuid::Uuid::new_v4().as_u128() as u32 % 1_000_000);
        let sequence_number = metadata.next_sequence_number();

        // Everything the parent snapshot already had, carried by reference.
        let mut manifests: Vec<ManifestFile> = match &parent {
            Some(parent) => {
                let bytes = self
                    .file_io
                    .new_input(parent.manifest_list())
                    .map_err(|e| Error::Storage(Box::new(e)))?
                    .read()
                    .await
                    .map_err(|e| Error::Storage(Box::new(e)))?;
                iceberg::spec::ManifestList::parse_with_version(&bytes, FormatVersion::V2)
                    .map_err(|e| Error::Storage(Box::new(e)))?
                    .entries()
                    .to_vec()
            }
            None => Vec::new(),
        };

        // Plus one new delete manifest.
        let location = format!(
            "{}/metadata/{snapshot_id}-delete-{}.avro",
            self.location(),
            uuid::Uuid::new_v4()
        );
        let output = self
            .file_io
            .new_output(&location)
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_deletes();

        for delete in deletes {
            writer
                .add_file(delete, sequence_number)
                .map_err(|e| Error::Storage(Box::new(e)))?;
        }
        manifests.push(
            writer
                .write_manifest_file()
                .await
                .map_err(|e| Error::Storage(Box::new(e)))?,
        );

        let list_location = format!(
            "{}/metadata/snap-{snapshot_id}-{}.avro",
            self.location(),
            uuid::Uuid::new_v4()
        );
        let list_output = self
            .file_io
            .new_output(&list_location)
            .map_err(|e| Error::Storage(Box::new(e)))?
            .writer()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let mut list = ManifestListWriter::v2(
            list_output,
            snapshot_id,
            parent.as_ref().map(|p| p.snapshot_id()),
            sequence_number,
        );
        list.add_manifests(manifests.into_iter())
            .map_err(|e| Error::Storage(Box::new(e)))?;
        list.close()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent.as_ref().map(|p| p.snapshot_id()))
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list(list_location)
            .with_schema_id(metadata.current_schema_id())
            .with_summary(Summary {
                // A row-level delete, which is what an equality delete is.
                operation: Operation::Delete,
                additional_properties: HashMap::new(),
            })
            .build();

        self.committer
            .commit(
                &self.ident,
                vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".to_string(),
                    snapshot_id: parent.as_ref().map(|p| p.snapshot_id()),
                }],
                vec![
                    TableUpdate::AddSnapshot { snapshot },
                    TableUpdate::SetSnapshotRef {
                        ref_name: "main".to_string(),
                        reference: SnapshotReference::new(
                            snapshot_id,
                            SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        ),
                    },
                ],
                fixture_ctx(),
            )
            .await
    }
}

/// Every live data file with its size, for tests about which files a rewrite
/// reads.
///
/// A rewrite is eligibility-filtered by size, so a test about that rule has to
/// see sizes rather than paths — and reading them from the scan is what the
/// executor itself does.
pub async fn live_data_files_with_sizes(table: &Table) -> Result<Vec<(String, u64)>> {
    use futures::StreamExt;

    let scan = table.scan().build()?;
    let mut stream = scan.plan_files().await?;

    let mut files = Vec::new();
    while let Some(task) = stream.next().await {
        let task = task?;
        files.push((task.data_file_path, task.file_size_in_bytes));
    }
    files.sort();
    Ok(files)
}
