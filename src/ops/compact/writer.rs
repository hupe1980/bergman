//! Writing a rewritten file group back out.
//!
//! Upstream's `RollingFileWriter` does the work: it closes a Parquet file and
//! opens the next when the current one reaches the target, and it produces real
//! Iceberg `DataFile`s with the full column metrics a query planner needs. What
//! this module adds is the table's own opinion about how those files should be
//! encoded, which a rewrite must not quietly override.

use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, PartitionKey};
use iceberg::table::Table;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
    ObjectStorageLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

use crate::error::{Error, Result};

/// The writer a rewritten group is streamed into.
pub(super) type GroupWriter =
    DataFileWriter<ParquetWriterBuilder, TableLocations, DefaultFileNameGenerator>;

/// Where the table says its data files go.
///
/// `write.object-storage.enabled` asks for a hashed directory tree in front of
/// each file so a hot table's objects spread across object-store prefixes; a
/// rewrite writing the plain layout instead would concentrate a compacted
/// table's objects in the one prefix the property exists to avoid. Both
/// generators are upstream's, and honour `write.data.path`.
#[derive(Clone, Debug)]
pub(super) enum TableLocations {
    Plain(DefaultLocationGenerator),
    Hashed(ObjectStorageLocationGenerator),
}

impl TableLocations {
    fn of(metadata: &iceberg::spec::TableMetadata) -> Result<Self> {
        // Java reads the property with `Boolean.parseBoolean`: `true` in any
        // case is on, and everything else — including a typo — is off.
        let hashed = metadata
            .properties()
            .get("write.object-storage.enabled")
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
        let generator = if hashed {
            ObjectStorageLocationGenerator::new(metadata).map(Self::Hashed)
        } else {
            DefaultLocationGenerator::new(metadata).map(Self::Plain)
        };
        generator.map_err(|e| Error::Storage(Box::new(e)))
    }
}

impl LocationGenerator for TableLocations {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        match self {
            Self::Plain(g) => g.generate_location(partition_key, file_name),
            Self::Hashed(g) => g.generate_location(partition_key, file_name),
        }
    }
}

/// Open a writer for one file group.
pub(super) async fn open(
    table: &Table,
    group: &[&FileScanTask],
    target: u64,
) -> Result<GroupWriter> {
    let metadata = table.metadata();
    let file_io = table.file_io().clone();

    let location_generator = TableLocations::of(metadata)?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "bergman".to_string(),
        Some(uuid::Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );

    let parquet_writer_builder =
        ParquetWriterBuilder::new(writer_properties(metadata), write_schema(metadata)?);

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

/// The reserved row-lineage field ids a rewrite of this table reads and writes.
///
/// Empty below format v3, which has no row lineage.
pub(super) fn lineage_field_ids(metadata: &iceberg::spec::TableMetadata) -> Vec<i32> {
    use iceberg::metadata_columns::{
        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
    };
    if metadata.format_version() < iceberg::spec::FormatVersion::V3 {
        return Vec::new();
    }
    vec![
        RESERVED_FIELD_ID_ROW_ID,
        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ]
}

/// The schema a rewritten file is written under: the table's current schema,
/// plus — on v3 — the two row-lineage columns as physical fields.
///
/// A rewrite moves rows to new positions in a new file, and a reader derives a
/// row's lineage from its file's position whenever the file does not carry it.
/// Written physically, every row keeps the `_row_id` and
/// `_last_updated_sequence_number` it had; a row whose value is null still
/// inherits, which is the spec's rule for rows that never had one.
fn write_schema(metadata: &iceberg::spec::TableMetadata) -> Result<iceberg::spec::SchemaRef> {
    let current = metadata.current_schema();
    if lineage_field_ids(metadata).is_empty() {
        return Ok(current.clone());
    }
    let mut fields: Vec<_> = current.as_struct().fields().to_vec();
    fields.push(iceberg::metadata_columns::row_id_field().clone());
    fields.push(iceberg::metadata_columns::last_updated_sequence_number_field().clone());
    iceberg::spec::Schema::builder()
        .with_schema_id(current.schema_id())
        .with_identifier_field_ids(current.identifier_field_ids())
        .with_fields(fields)
        .build()
        .map(std::sync::Arc::new)
        .map_err(|e| Error::Storage(Box::new(e)))
}

/// The partition value every file in the group shares.
///
/// `None` for an unpartitioned table.
///
/// This is the tuple stamped on every output file, so getting it wrong files
/// every row of the group under the wrong partition value, and nothing fails.
/// Two things are therefore checked rather than assumed, and both are refusals
/// rather than fallbacks:
///
/// - **The tasks carry a partition at all.** An absent one means the scan and
///   the manifests disagree about a file; defaulting to an empty tuple would
///   write a partitioned table's data under it.
/// - **They all carry the same one.** Equal rendered keys mean equal tuples
///   because [`crate::health::partition_path`] is injective — an argument about
///   another module, verified here because this is where it being wrong destroys
///   data.
fn partition_key_for(table: &Table, group: &[&FileScanTask]) -> Result<Option<PartitionKey>> {
    let metadata = table.metadata();
    let spec = metadata.default_partition_spec();

    if spec.fields().is_empty() {
        return Ok(None);
    }

    let refuse = |reason: String| Error::refused("compact", table.identifier().to_string(), reason);

    let mut tuples = group.iter().map(|task| task.partition());

    let Some(Some(value)) = tuples.next() else {
        return Err(refuse(
            "a file in this group carries no partition tuple, so its rows cannot be \
             written back to the partition they came from"
                .to_string(),
        ));
    };

    for other in tuples {
        if other != Some(value) {
            return Err(refuse(
                "this group's files do not all share one partition tuple; rewriting \
                 them together would file some of their rows under another partition's \
                 value"
                    .to_string(),
            ));
        }
    }

    Ok(Some(PartitionKey::new(
        spec.as_ref().clone(),
        metadata.current_schema().clone(),
        value.clone(),
    )))
}

/// Parquet writer properties, honouring the table's own settings.
fn writer_properties(
    metadata: &iceberg::spec::TableMetadata,
) -> parquet::file::properties::WriterProperties {
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::ColumnPath;

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
    if let Some(size) = properties
        .get("write.parquet.dict-size-bytes")
        .and_then(|s| s.trim().parse::<usize>().ok())
    {
        builder = builder.set_dictionary_page_size_limit(size);
    }
    if let Some(rows) = properties
        .get("write.parquet.page-row-limit")
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|rows| *rows > 0)
    {
        builder = builder.set_data_page_row_count_limit(rows);
    }

    // Bloom filters, which a rewrite must carry over for the same reason it
    // carries over the sort order: the table's owner turned them on for a
    // column they filter by, and a rewritten file without one silently makes
    // every such query read more. Iceberg spells this per column —
    // `write.parquet.bloom-filter-enabled.column.<name>` — with an optional
    // false-positive probability beside it.
    //
    // A malformed value is ignored rather than fatal, the same rule the rest of
    // this function follows: a rewrite must not fail over a table property
    // somebody typed wrong.
    for (key, value) in properties {
        if let Some(column) = key.strip_prefix(BLOOM_ENABLED_PREFIX) {
            if let Ok(on) = value.trim().parse::<bool>() {
                builder = builder
                    .set_column_bloom_filter_enabled(ColumnPath::from(column.to_string()), on);
            }
        } else if let Some(column) = key.strip_prefix(BLOOM_FPP_PREFIX)
            && let Ok(fpp) = value.trim().parse::<f64>()
            && fpp > 0.0
            && fpp < 1.0
        {
            builder =
                builder.set_column_bloom_filter_fpp(ColumnPath::from(column.to_string()), fpp);
        }
    }

    builder.build()
}

/// Iceberg's per-column bloom-filter switch.
const BLOOM_ENABLED_PREFIX: &str = "write.parquet.bloom-filter-enabled.column.";
/// Iceberg's per-column bloom-filter false-positive probability.
const BLOOM_FPP_PREFIX: &str = "write.parquet.bloom-filter-fpp.column.";

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadata,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    };
    use parquet::schema::types::ColumnPath;

    use super::*;

    fn metadata(properties: &[(&str, &str)]) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "file:///tmp/t".to_string(),
            FormatVersion::V2,
            properties
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn a_table_that_asks_for_no_bloom_filter_gets_none() {
        // The control. A rewrite must not invent filters either: they cost
        // write time and file size, and a table that did not ask for one is
        // saying it does not want to pay.
        let properties = writer_properties(&metadata(&[]));
        assert!(
            !properties
                .bloom_filter_properties(&ColumnPath::from("id"))
                .is_some()
        );
    }

    #[test]
    fn a_column_the_table_asks_to_bloom_filter_keeps_its_filter() {
        // The guarded case, and the reason it matters: the owner turned this on
        // for a column they filter by. A rewritten file without the filter is
        // one every such query reads more of, and nothing says so.
        let properties = writer_properties(&metadata(&[
            ("write.parquet.bloom-filter-enabled.column.id", "true"),
            ("write.parquet.bloom-filter-fpp.column.id", "0.02"),
        ]));

        let id = properties
            .bloom_filter_properties(&ColumnPath::from("id"))
            .expect("the filter the table asked for");
        assert!((id.fpp - 0.02).abs() < 1e-9, "fpp not carried: {}", id.fpp);

        // And only that column: a global switch where Iceberg spells a
        // per-column one would filter every column in the table.
        assert!(
            properties
                .bloom_filter_properties(&ColumnPath::from("region"))
                .is_none(),
            "a column the table said nothing about must not gain a filter"
        );
    }

    #[test]
    fn a_malformed_property_does_not_fail_the_rewrite() {
        // Same rule as the compression level above it: a rewrite must not be
        // refused over a table property somebody typed wrong. The filter is
        // simply not applied.
        let properties = writer_properties(&metadata(&[
            ("write.parquet.bloom-filter-enabled.column.id", "yes please"),
            ("write.parquet.dict-size-bytes", "lots"),
        ]));
        assert!(
            properties
                .bloom_filter_properties(&ColumnPath::from("id"))
                .is_none()
        );
    }
}
