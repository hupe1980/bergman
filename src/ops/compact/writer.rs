//! Writing a rewritten file group back out.
//!
//! Upstream's `RollingFileWriter` does the work: it closes a Parquet file and
//! opens the next when the current one reaches the target, and it produces real
//! Iceberg `DataFile`s with the full column metrics a query planner needs. What
//! this module adds is the table's own opinion about how those files should be
//! encoded, which a rewrite must not quietly override.

use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

use crate::error::{Error, Result};

/// The writer a rewritten group is streamed into.
pub(super) type GroupWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Open a writer for one file group.
pub(super) async fn open(
    table: &Table,
    group: &[&FileScanTask],
    target: u64,
) -> Result<GroupWriter> {
    let metadata = table.metadata();
    let file_io = table.file_io().clone();

    let location_generator =
        DefaultLocationGenerator::new(metadata).map_err(|e| Error::Storage(Box::new(e)))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "bergman".to_string(),
        Some(uuid::Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        writer_properties(metadata),
        metadata.current_schema().clone(),
    );

    // The rolling writer is what makes the target file size real: it closes a
    // file and opens the next when the current one reaches the target, rather
    // than producing one enormous file per group.
    let rolling = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target as usize,
        file_io,
        location_generator,
        file_name_generator,
    );

    DataFileWriterBuilder::new(rolling)
        .build(partition_key_for(table, group)?)
        .await
        .map_err(|e| Error::Storage(Box::new(e)))
}

/// The partition value every file in the group shares.
///
/// `None` for an unpartitioned table. The group is built by partition value, so
/// they do share one — this reads it back off the first task.
fn partition_key_for(table: &Table, group: &[&FileScanTask]) -> Result<Option<PartitionKey>> {
    let metadata = table.metadata();
    let spec = metadata.default_partition_spec();

    if spec.fields().is_empty() {
        return Ok(None);
    }

    let value = group
        .first()
        .and_then(|t| t.partition.clone())
        .unwrap_or_else(Struct::empty);

    Ok(Some(PartitionKey::new(
        spec.as_ref().clone(),
        metadata.current_schema().clone(),
        value,
    )))
}

/// Parquet writer properties, honouring the table's own settings.
fn writer_properties(
    metadata: &iceberg::spec::TableMetadata,
) -> parquet::file::properties::WriterProperties {
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let properties = metadata.properties();
    let get = |key: &str| properties.get(key).map(|s| s.trim().to_ascii_lowercase());
    let level = |key: &str| {
        properties
            .get(key)
            .and_then(|s| s.trim().parse::<i32>().ok())
    };

    // Iceberg's own default is zstd. Levels are honoured where the table sets
    // one, and an out-of-range level falls back to the codec's default rather
    // than failing a rewrite over a table property.
    let compression = match get("write.parquet.compression-codec").as_deref() {
        Some("uncompressed") | Some("none") => Compression::UNCOMPRESSED,
        Some("snappy") => Compression::SNAPPY,
        Some("gzip") => Compression::GZIP(
            level("write.parquet.compression-level")
                .and_then(|l| GzipLevel::try_new(l as u32).ok())
                .unwrap_or_default(),
        ),
        Some("lz4") => Compression::LZ4_RAW,
        Some("brotli") => Compression::BROTLI(Default::default()),
        _ => Compression::ZSTD(
            level("write.parquet.compression-level")
                .and_then(|l| ZstdLevel::try_new(l).ok())
                .unwrap_or_default(),
        ),
    };

    let mut builder = WriterProperties::builder().set_compression(compression);

    // Row-group and page sizes decide how much a reader must fetch to answer a
    // predicate. A rewrite that ignored them would undo whatever the table's
    // owner tuned.
    if let Some(size) = properties
        .get("write.parquet.row-group-size-bytes")
        .and_then(|s| s.trim().parse::<usize>().ok())
    {
        // `set_max_row_group_bytes`, not `set_max_row_group_size` — the latter
        // is a *row count*, and Iceberg's property is bytes. Passing a byte
        // figure to the row-count knob would cap row groups at 128 million rows
        // for a table asking for 128 MiB ones, which is no cap at all.
        builder = builder.set_max_row_group_bytes(Some(size));
    }
    if let Some(size) = properties
        .get("write.parquet.page-size-bytes")
        .and_then(|s| s.trim().parse::<usize>().ok())
    {
        builder = builder.set_data_page_size_limit(size);
    }

    builder.build()
}
