//! The manifest walk that produces a [`TableHealth`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use iceberg::spec::{
    DataContentType, ManifestContentType, ManifestEntry, ManifestList, ManifestStatus,
    PartitionSpec, Schema, TableMetadata,
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
    let format_version = metadata.format_version();
    let write_format = metadata
        .properties()
        .get("write.format.default")
        .map(|s| s.trim().to_ascii_lowercase());

    // A table with no current snapshot has never been written to. Everything
    // below would read a manifest list that does not exist.
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(TableHealth {
            table: table_ref.clone(),
            format_version,
            write_format,
            location,
            current_spec_id: metadata.default_partition_spec_id(),
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

    // When each snapshot happened, so a file entry can be dated by the snapshot
    // that added it. That is the only "when did this arrive" signal a manifest
    // carries, and it is what tells the planner whether a partition is still
    // being written.
    let snapshot_times: Arc<HashMap<i64, i64>> = Arc::new(
        metadata
            .snapshots()
            .map(|s| (s.snapshot_id(), s.timestamp_ms()))
            .collect(),
    );

    // Each manifest is independent, so they are read concurrently and folded
    // afterwards. Folding in the stream would serialise on the accumulator and
    // give back most of what the concurrency bought.
    let schema = metadata.current_schema().clone();
    let per_manifest: Vec<ManifestTally> = stream::iter(manifest_list.entries())
        .map(|manifest_file| {
            let file_io = file_io.clone();
            let schema = schema.clone();
            let snapshot_times = Arc::clone(&snapshot_times);
            // A manifest is written under exactly one partition spec, and a
            // table whose spec has evolved holds manifests under several. An
            // entry's partition tuple only means anything against the spec that
            // produced it, so grouping reads that spec rather than the table's
            // current one.
            let spec = metadata
                .partition_spec_by_id(manifest_file.partition_spec_id)
                .cloned();
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
                    tally.add(
                        entry,
                        manifest_file.partition_spec_id,
                        spec.as_deref(),
                        &schema,
                        &snapshot_times,
                    );
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
        write_format,
        location,
        current_spec_id: metadata.default_partition_spec_id(),
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
    fn add(
        &mut self,
        entry: &ManifestEntry,
        spec_id: i32,
        spec: Option<&PartitionSpec>,
        schema: &Schema,
        snapshot_times: &HashMap<i64, i64>,
    ) {
        let data_file = entry.data_file();
        // A spec the metadata no longer carries means the manifest names one
        // that was removed. That is malformed, but not a reason to drop the
        // file from the health report — every file under that spec still shares
        // one key, so the counts stay right and the plan says which spec.
        let key = match spec {
            Some(spec) => PartitionKey::new(spec, schema, data_file.partition()),
            None => PartitionKey::unpartitioned(spec_id),
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

                // A file arrived with the snapshot that added it, and the
                // newest arrival is what says whether the partition has
                // settled. An entry whose snapshot is no longer retained
                // carries no date, which reads as "old enough".
                if let Some(added_ms) = entry.snapshot_id().and_then(|id| snapshot_times.get(&id)) {
                    partition.newest_file_ms =
                        Some(partition.newest_file_ms.unwrap_or(i64::MIN).max(*added_ms));
                }
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
                partition.equality_delete_record_count += records;
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
            target.equality_delete_record_count += incoming.equality_delete_record_count;
            target.file_sizes.extend(incoming.file_sizes);
            target.newest_file_ms = match (target.newest_file_ms, incoming.newest_file_ms) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
        }
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
