//! The manifest walk that produces a [`TableHealth`].

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use iceberg::spec::{
    DataContentType, FormatVersion, ManifestContentType, ManifestList, ManifestStatus, Struct,
    TableMetadata,
};
use iceberg::table::Table;

use crate::error::{Error, Result};
use crate::health::{
    FileHealth, ManifestHealth, PartitionHealth, PartitionKey, SnapshotHealth, TableHealth,
};
use crate::policy::TableRef;

/// How many manifests are read concurrently.
///
/// Manifests are small Avro files fetched over the network, so this is latency-
/// bound like discovery is. The bound exists so that analyzing a table with
/// tens of thousands of manifests does not open tens of thousands of
/// connections.
const MANIFEST_CONCURRENCY: usize = 16;

/// Analyze a table's condition from its metadata.
///
/// Reads the current snapshot's manifest list and every manifest in it. No data
/// file is opened.
///
/// `manifest_target_size` decides which manifests count as undersized; it comes
/// from the resolved policy so that the same table can be judged against
/// different targets without re-reading anything.
pub async fn analyze(
    table_ref: &TableRef,
    table: &Table,
    manifest_target_size: u64,
    now: DateTime<Utc>,
) -> Result<TableHealth> {
    let metadata = table.metadata();

    let snapshots = snapshot_health(metadata, now);
    let location = metadata.location().to_string();
    let format_version = match metadata.format_version() {
        FormatVersion::V1 => 1,
        FormatVersion::V2 => 2,
        FormatVersion::V3 => 3,
    };

    // A table with no current snapshot has never been written to. Everything
    // below would read a manifest list that does not exist.
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(TableHealth {
            table: table_ref.clone(),
            format_version,
            location,
            snapshots,
            manifests: ManifestHealth::default(),
            files: FileHealth::default(),
            partitions: Vec::new(),
        });
    };

    let file_io = table.file_io();
    let manifest_list_bytes = file_io
        .new_input(snapshot.manifest_list())
        .map_err(|e| Error::Storage(Box::new(e)))?
        .read()
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;

    let manifest_list =
        ManifestList::parse_with_version(&manifest_list_bytes, metadata.format_version())
            .map_err(|e| Error::metadata(table_ref, format!("unreadable manifest list: {e}")))?;

    let mut manifests = ManifestHealth {
        count: manifest_list.entries().len(),
        ..Default::default()
    };
    for entry in manifest_list.entries() {
        manifests.bytes += entry.manifest_length.max(0) as u64;
        if (entry.manifest_length.max(0) as u64) < manifest_target_size {
            manifests.undersized_count += 1;
        }
        if entry.content == ManifestContentType::Deletes {
            manifests.delete_manifest_count += 1;
        }
    }

    // Each manifest is independent, so they are read concurrently and folded
    // afterwards. Folding in the stream would serialise on the accumulator and
    // give back most of what the concurrency bought.
    let per_manifest: Vec<ManifestTally> = stream::iter(manifest_list.entries())
        .map(|manifest_file| {
            let file_io = file_io.clone();
            async move {
                let manifest = manifest_file
                    .load_manifest(&file_io)
                    .await
                    .map_err(|e| Error::Storage(Box::new(e)))?;

                let mut tally = ManifestTally::default();
                for entry in manifest.entries() {
                    // A manifest records the history of a file, not only its
                    // present state: `Deleted` entries describe files this
                    // snapshot removed and are not live. Counting them would
                    // make every compaction look like it had achieved nothing.
                    if entry.status() == ManifestStatus::Deleted {
                        continue;
                    }
                    tally.add(entry.data_file(), manifest_file.partition_spec_id);
                }
                Ok::<_, Error>(tally)
            }
        })
        .buffer_unordered(MANIFEST_CONCURRENCY)
        .try_collect()
        .await?;

    let mut files = FileHealth::default();
    let mut partitions: HashMap<PartitionKey, PartitionHealth> = HashMap::new();
    for tally in per_manifest {
        tally.merge_into(&mut files, &mut partitions);
    }

    // Sorted once here so that every percentile downstream is a lookup rather
    // than a sort, and so that plans are stable between runs.
    files.file_sizes.sort_unstable();
    let mut partitions: Vec<PartitionHealth> = partitions.into_values().collect();
    for partition in &mut partitions {
        partition.file_sizes.sort_unstable();
    }
    partitions.sort_by(|a, b| a.key.cmp(&b.key));

    Ok(TableHealth {
        table: table_ref.clone(),
        format_version,
        location,
        snapshots,
        manifests,
        files,
        partitions,
    })
}

/// Per-manifest counts, merged after the concurrent read.
#[derive(Default)]
struct ManifestTally {
    files: FileHealth,
    partitions: HashMap<PartitionKey, PartitionHealth>,
}

impl ManifestTally {
    fn add(&mut self, data_file: &iceberg::spec::DataFile, spec_id: i32) {
        let key = PartitionKey {
            spec_id,
            value: render_partition(data_file.partition()),
        };
        let partition = self
            .partitions
            .entry(key.clone())
            .or_insert_with(|| PartitionHealth::new(key));

        let size = data_file.file_size_in_bytes();
        let records = data_file.record_count();

        match data_file.content_type() {
            DataContentType::Data => {
                self.files.data_file_count += 1;
                self.files.data_bytes += size;
                self.files.record_count += records;
                self.files.file_sizes.push(size);

                partition.data_file_count += 1;
                partition.data_bytes += size;
                partition.record_count += records;
                partition.file_sizes.push(size);
            }
            DataContentType::PositionDeletes => {
                self.files.position_delete_count += 1;
                self.files.delete_bytes += size;
                self.files.delete_record_count += records;

                partition.position_delete_count += 1;
                partition.delete_record_count += records;
            }
            DataContentType::EqualityDeletes => {
                self.files.equality_delete_count += 1;
                self.files.delete_bytes += size;
                self.files.delete_record_count += records;

                partition.equality_delete_count += 1;
                partition.delete_record_count += records;
            }
        }
    }

    fn merge_into(
        self,
        files: &mut FileHealth,
        partitions: &mut HashMap<PartitionKey, PartitionHealth>,
    ) {
        files.data_file_count += self.files.data_file_count;
        files.data_bytes += self.files.data_bytes;
        files.record_count += self.files.record_count;
        files.position_delete_count += self.files.position_delete_count;
        files.equality_delete_count += self.files.equality_delete_count;
        files.delete_bytes += self.files.delete_bytes;
        files.delete_record_count += self.files.delete_record_count;
        files.file_sizes.extend(self.files.file_sizes);

        for (key, incoming) in self.partitions {
            let target = partitions
                .entry(key.clone())
                .or_insert_with(|| PartitionHealth::new(key));
            target.data_file_count += incoming.data_file_count;
            target.data_bytes += incoming.data_bytes;
            target.record_count += incoming.record_count;
            target.position_delete_count += incoming.position_delete_count;
            target.equality_delete_count += incoming.equality_delete_count;
            target.delete_record_count += incoming.delete_record_count;
            target.file_sizes.extend(incoming.file_sizes);
        }
    }
}

/// Render a partition value to a stable string.
///
/// Bergman groups and displays partitions but never interprets them, so a
/// rendering is enough and modelling the spec's type system here would be a
/// large amount of code for no additional capability.
fn render_partition(partition: &Struct) -> String {
    let rendered: Vec<String> = partition
        .iter()
        .map(|value| match value {
            // `Literal` implements neither `Display` nor `Serialize`, so the
            // debug form is what is available. It is stable enough for grouping
            // — which is all this is load-bearing for — and readable enough for
            // a plan line. `Primitive(Int(5))` renders as `5`.
            Some(v) => render_literal(v),
            None => "null".to_string(),
        })
        .collect();

    if rendered.is_empty() {
        "unpartitioned".to_string()
    } else {
        rendered.join("/")
    }
}

/// Render one partition value.
///
/// Unwraps the `Primitive(Int(5))` debug shape to `5`, which is what an
/// operator reading a plan expects to see. Anything more structured (a nested
/// struct, a list) keeps its debug form: it still groups correctly, which is
/// the only thing this value is load-bearing for.
pub fn render_literal(literal: &iceberg::spec::Literal) -> String {
    let debug = format!("{literal:?}");
    let Some(inner) = debug
        .strip_prefix("Primitive(")
        .and_then(|s| s.strip_suffix(')'))
    else {
        return debug;
    };

    // `Int(5)` → `5`, `String("eu")` → `"eu"`.
    match inner.split_once('(') {
        Some((_, rest)) => rest.trim_end_matches(')').trim_matches('"').to_string(),
        None => inner.to_string(),
    }
}

fn snapshot_health(metadata: &TableMetadata, now: DateTime<Utc>) -> SnapshotHealth {
    let snapshots: Vec<_> = metadata.snapshots().collect();

    let oldest_age = snapshots
        .iter()
        .map(|s| s.timestamp_ms())
        .min()
        .and_then(|oldest_ms| {
            let age_ms = now.timestamp_millis().saturating_sub(oldest_ms);
            // A snapshot timestamped in the future yields no age rather than a
            // nonsensical one. Clock skew between writers is real, and a
            // negative duration would panic on construction.
            u64::try_from(age_ms).ok().map(Duration::from_millis)
        });

    SnapshotHealth {
        count: snapshots.len(),
        current_snapshot_id: metadata.current_snapshot_id(),
        oldest_age,
        has_main_branch: metadata.snapshot_for_ref("main").is_some(),
        metadata_log_count: metadata.metadata_log().len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpartitioned_renders_as_a_name_not_an_empty_string() {
        // This value ends up in plan output and audit records, where an empty
        // string would read as missing data rather than as "no partitioning".
        assert_eq!(render_partition(&Struct::empty()), "unpartitioned");
    }
}
