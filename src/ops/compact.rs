//! Compaction: rewriting a partition's small files, with its delete files
//! applied.
//!
//! The commit half lives in [`crate::commit`]; this is the data half. Reading
//! is upstream's scan pipeline, which applies positional deletes
//! (`build_deletes_row_selection`) and equality deletes
//! (`build_equality_delete_predicate`) — so what arrives here is *surviving
//! rows*, and writing them back is what retires the delete files.
//!
//! # The rule that matters
//!
//! A rewrite may remove a delete file **only if every data file that delete
//! file applies to is inside the group being rewritten**. A delete file
//! shared with a file outside the group is still doing work for that file, and
//! dropping it resurrects the rows it was hiding.
//!
//! That is why planning happens over the *whole* table before anything is
//! rewritten: the question "does this delete file apply anywhere else?" cannot
//! be answered from inside the group.

use std::collections::HashSet;

use futures::StreamExt;
use iceberg::TableIdent;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};

use crate::commit::{RewriteFiles, SnapshotProducer, TableCommitter};
use crate::error::{Error, Result};
use crate::health::PartitionKey as HealthPartitionKey;
use crate::obs::MaintenanceObserver;
use crate::ops::reachability::normalize;
use crate::plan::OperationResult;
use crate::policy::{EffectiveCompaction, TableRef};
use crate::util::human_bytes;

/// What a compaction did.
#[derive(Debug, Clone, Default)]
pub struct CompactOutcome {
    /// Partitions rewritten.
    pub partitions: usize,
    /// Data files read and replaced.
    pub files_removed: usize,
    /// Delete files fully applied and retired.
    pub deletes_retired: usize,
    /// Files written.
    pub files_written: usize,
    /// Bytes read.
    pub bytes_read: u64,
    /// Bytes written.
    pub bytes_written: u64,
    /// Partitions skipped, with the reason.
    pub skipped: Vec<String>,
}

/// Compact the partitions a plan identified.
pub async fn run(
    table_ref: &TableRef,
    table: &Table,
    ident: &TableIdent,
    committer: &dyn TableCommitter,
    settings: &EffectiveCompaction,
    partitions: &[HealthPartitionKey],
    observer: &dyn MaintenanceObserver,
) -> Result<OperationResult> {
    let _ = observer;
    let metadata = table.metadata();

    if metadata.current_snapshot().is_none() {
        return Ok(OperationResult::NoOp {
            detail: "the table has never been written to".into(),
        });
    }

    // Iceberg writes Parquet, Avro and ORC. Bergman reads all three through the
    // scan pipeline but writes only Parquet, so a table configured to write
    // something else would silently change format under a rewrite — refuse
    // instead.
    let write_format = metadata
        .properties()
        .get("write.format.default")
        .map(|s| s.to_ascii_lowercase());
    if let Some(format) = write_format
        && format != "parquet"
    {
        return Err(Error::refused(
            "compact",
            table_ref,
            format!("table writes {format}; Bergman only writes Parquet"),
        ));
    }

    // Plan the whole table once. Two things need this rather than a
    // group-scoped scan: deciding which delete files are exclusive to a group,
    // and refusing partition specs Bergman cannot rewrite correctly.
    let all_tasks = plan_all(table).await?;

    let target = settings.target_file_size.value;
    let mut outcome = CompactOutcome::default();

    for partition in partitions {
        match compact_partition(
            table_ref, table, ident, committer, &all_tasks, partition, target,
        )
        .await
        {
            Ok(Some(stats)) => {
                outcome.partitions += 1;
                outcome.files_removed += stats.files_removed;
                outcome.deletes_retired += stats.deletes_retired;
                outcome.files_written += stats.files_written;
                outcome.bytes_read += stats.bytes_read;
                outcome.bytes_written += stats.bytes_written;
            }
            Ok(None) => {}
            // One partition failing does not abandon the rest. Each partition
            // commits independently, so partial progress is real progress —
            // which is what makes compacting an actively-written table
            // tractable at all.
            Err(e) if e.is_replan() => {
                outcome
                    .skipped
                    .push(format!("{partition}: {e}; will replan next cycle"));
            }
            Err(e) => {
                outcome.skipped.push(format!("{partition}: {e}"));
            }
        }
    }

    if outcome.partitions == 0 {
        let detail = if outcome.skipped.is_empty() {
            "nothing left to compact".to_string()
        } else {
            format!("no partition was rewritten: {}", outcome.skipped.join("; "))
        };
        return Ok(OperationResult::NoOp { detail });
    }

    let mut detail = format!(
        "{} partitions: {} files ({}) rewritten into {} ({})",
        outcome.partitions,
        outcome.files_removed,
        human_bytes(outcome.bytes_read),
        outcome.files_written,
        human_bytes(outcome.bytes_written),
    );
    if outcome.deletes_retired > 0 {
        detail.push_str(&format!(
            ", {} delete files retired",
            outcome.deletes_retired
        ));
    }
    if !outcome.skipped.is_empty() {
        detail.push_str(&format!(", {} partitions skipped", outcome.skipped.len()));
    }

    Ok(OperationResult::Succeeded { detail })
}

#[derive(Default)]
struct PartitionStats {
    files_removed: usize,
    deletes_retired: usize,
    files_written: usize,
    bytes_read: u64,
    bytes_written: u64,
}

/// Plan every live data file in the table.
async fn plan_all(table: &Table) -> Result<Vec<FileScanTask>> {
    let scan = table.scan().build()?;
    let mut stream = scan.plan_files().await?;

    let mut tasks = Vec::new();
    while let Some(task) = stream.next().await {
        tasks.push(task?);
    }
    Ok(tasks)
}

/// Compact one partition, committing on its own.
async fn compact_partition(
    table_ref: &TableRef,
    table: &Table,
    ident: &TableIdent,
    committer: &dyn TableCommitter,
    all_tasks: &[FileScanTask],
    partition: &HealthPartitionKey,
    target: u64,
) -> Result<Option<PartitionStats>> {
    let metadata = table.metadata();

    // Bergman renders partition values to strings for grouping and display
    // (see `crate::health::PartitionKey`), so a group is identified by matching
    // that rendering back against the planned tasks.
    let (group, rest): (Vec<&FileScanTask>, Vec<&FileScanTask>) = all_tasks
        .iter()
        .partition(|task| task_partition(task) == partition.value);

    if group.is_empty() {
        return Ok(None);
    }

    // A table whose partition spec has evolved holds files under several specs
    // at once. Rewriting them together would write output under the *current*
    // spec while claiming to replace files partitioned differently, which
    // silently mis-files rows. Refuse, and say so.
    let spec_ids: HashSet<i32> = group.iter().map(|t| partition_spec_id(t)).collect();
    if spec_ids.len() > 1 {
        return Err(Error::refused(
            "compact",
            table_ref,
            format!(
                "partition {partition} holds files under {} partition specs; \
                 rewriting across a spec change is not supported",
                spec_ids.len()
            ),
        ));
    }

    // The delete-file rule. A delete file referenced by anything outside this
    // group is still doing work there, so it stays — and because it stays, the
    // data files it applies to inside the group are still correct after the
    // rewrite.
    let deletes_elsewhere: HashSet<String> = rest
        .iter()
        .flat_map(|task| task.deletes.iter())
        .map(|d| normalize(&d.file_path))
        .collect();

    let deletes_in_group: HashSet<String> = group
        .iter()
        .flat_map(|task| task.deletes.iter())
        .map(|d| normalize(&d.file_path))
        .collect();

    let retirable: Vec<String> = deletes_in_group
        .difference(&deletes_elsewhere)
        .cloned()
        .collect();

    let bytes_read: u64 = group.iter().map(|t| t.file_size_in_bytes).sum();
    let input_paths: Vec<String> = group.iter().map(|t| t.data_file_path.clone()).collect();

    // Read with deletes applied, write at the target size.
    let written = rewrite_group(table, &group, target).await?;

    // A group whose rows were entirely deleted produces no files. That is a
    // legitimate outcome — the rewrite removes the inputs and adds nothing —
    // and it is how a partition emptied by deletes finally stops being read.
    let bytes_written: u64 = written.iter().map(|f| f.file_size_in_bytes()).sum();
    let files_written = written.len();

    let mut removed = input_paths;
    removed.extend(retirable.iter().cloned());

    let rewrite = RewriteFiles {
        removed,
        added: written,
    };

    let producer = SnapshotProducer::new(table);
    let Some((requirements, updates)) = producer.rewrite_files(&rewrite).await? else {
        return Ok(None);
    };

    committer.commit(ident, requirements, updates).await?;

    let _ = metadata;
    Ok(Some(PartitionStats {
        files_removed: group.len(),
        deletes_retired: retirable.len(),
        files_written,
        bytes_read,
        bytes_written,
    }))
}

/// Read a file group with its deletes applied and write replacements.
async fn rewrite_group(
    table: &Table,
    group: &[&FileScanTask],
    target: u64,
) -> Result<Vec<iceberg::spec::DataFile>> {
    let metadata = table.metadata();
    let file_io: FileIO = table.file_io().clone();

    // Borrows the caller's runtime rather than creating one — the library's
    // "bring your own runtime" contract reaches even here.
    let runtime = iceberg::Runtime::try_current()
        .map_err(|e| Error::config(format!("compaction needs a tokio runtime: {e}")))?;

    let tasks: Vec<Result<FileScanTask>> = group.iter().map(|t| Ok((*t).clone())).collect();
    let task_stream = futures::stream::iter(tasks.into_iter().map(|t| {
        t.map_err(|e: Error| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))
    }))
    .boxed();

    let reader = ArrowReaderBuilder::new(file_io.clone(), runtime).build();
    let mut batches = reader.read(task_stream)?.stream();

    let location_generator =
        DefaultLocationGenerator::new(metadata).map_err(|e| Error::Storage(Box::new(e)))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "bergman".to_string(),
        Some(uuid::Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );

    // Compression follows the table's own `write.parquet.compression-codec`
    // where it sets one, so a rewrite does not silently re-encode a table into
    // a different codec than every other writer uses.
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

    let partition_key = partition_key_for(table, group)?;
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(partition_key)
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;

    while let Some(batch) = batches.next().await {
        let batch = batch.map_err(|e| Error::Storage(Box::new(e)))?;
        writer
            .write(batch)
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;
    }

    writer
        .close()
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

/// Render a task's partition value the way [`crate::health`] does, so the two
/// agree on what a group is.
fn task_partition(task: &FileScanTask) -> String {
    match &task.partition {
        Some(value) => {
            let rendered: Vec<String> = value
                .iter()
                .map(|v| match v {
                    Some(literal) => crate::health::render_literal(literal),
                    None => "null".to_string(),
                })
                .collect();
            if rendered.is_empty() {
                "unpartitioned".to_string()
            } else {
                rendered.join("/")
            }
        }
        None => "unpartitioned".to_string(),
    }
}

fn partition_spec_id(task: &FileScanTask) -> i32 {
    // A task without a spec belongs to an unpartitioned table, whose only spec
    // is 0.
    task.partition_spec
        .as_ref()
        .map(|s| s.spec_id())
        .unwrap_or(0)
}

/// Parquet writer properties, honouring the table's own codec setting.
fn writer_properties(
    metadata: &iceberg::spec::TableMetadata,
) -> parquet::file::properties::WriterProperties {
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let codec = metadata
        .properties()
        .get("write.parquet.compression-codec")
        .map(|s| s.to_ascii_lowercase());

    let compression = match codec.as_deref() {
        Some("uncompressed") | Some("none") => Compression::UNCOMPRESSED,
        Some("snappy") => Compression::SNAPPY,
        Some("gzip") => Compression::GZIP(Default::default()),
        Some("lz4") => Compression::LZ4_RAW,
        // Iceberg's own default, and what an unset property means.
        _ => Compression::ZSTD(Default::default()),
    };

    WriterProperties::builder()
        .set_compression(compression)
        .build()
}

/// Which delete files a group may retire.
///
/// Split out so the rule can be tested directly: it is the one that resurrects
/// deleted rows when it is wrong, and nothing fails visibly when it is.
#[cfg_attr(not(test), allow(dead_code))]
fn retirable_deletes(in_group: &HashSet<String>, elsewhere: &HashSet<String>) -> Vec<String> {
    let mut out: Vec<String> = in_group.difference(elsewhere).cloned().collect();
    out.sort();
    out
}

/// Group data files into batches that add up to roughly the target size.
///
/// Unused by the current partition-at-a-time path, kept as the seam for
/// splitting a very large partition into several commits.
#[cfg_attr(not(test), allow(dead_code))]
fn bin_pack(sizes: &[u64], target: u64) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut current_bytes = 0u64;

    for (index, size) in sizes.iter().enumerate() {
        if !current.is_empty() && current_bytes + size > target {
            groups.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(index);
        current_bytes += size;
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|p| normalize(p)).collect()
    }

    #[test]
    fn a_delete_file_shared_with_another_group_is_not_retired() {
        // The rule that matters. This delete file is still hiding rows in a
        // file outside the group; dropping it brings them back.
        let in_group = set(&["s3://b/t/d1.parquet", "s3://b/t/shared.parquet"]);
        let elsewhere = set(&["s3://b/t/shared.parquet"]);

        assert_eq!(
            retirable_deletes(&in_group, &elsewhere),
            vec!["s3://b/t/d1.parquet".to_string()]
        );
    }

    #[test]
    fn a_delete_file_exclusive_to_the_group_is_retired() {
        // Its rows have been applied and written out, so keeping it would make
        // every future read pay for a file that hides nothing.
        let in_group = set(&["s3://b/t/d1.parquet"]);
        assert_eq!(
            retirable_deletes(&in_group, &HashSet::new()),
            vec!["s3://b/t/d1.parquet".to_string()]
        );
    }

    #[test]
    fn retirement_compares_normalized_paths() {
        // The scan reports `s3://`, the manifest may say `s3a://`. Treating
        // them as different files would retire one that is still in use.
        let in_group = set(&["s3://b/t/d1.parquet"]);
        let elsewhere: HashSet<String> = ["s3a://b/t//d1.parquet"]
            .iter()
            .map(|p| normalize(p))
            .collect();
        assert!(retirable_deletes(&in_group, &elsewhere).is_empty());
    }

    #[test]
    fn bin_packing_fills_groups_up_to_the_target() {
        let groups = bin_pack(&[400, 400, 400, 400], 1000);
        assert_eq!(groups, vec![vec![0, 1], vec![2, 3]]);
    }

    #[test]
    fn a_file_larger_than_the_target_gets_its_own_group() {
        // Never an empty group, and never one that silently drops a file.
        let groups = bin_pack(&[5000, 100], 1000);
        assert_eq!(groups, vec![vec![0], vec![1]]);
    }

    #[test]
    fn bin_packing_covers_every_file_exactly_once() {
        let sizes = [100, 200, 300, 400, 500, 600];
        let covered: Vec<usize> = bin_pack(&sizes, 700).into_iter().flatten().collect();
        assert_eq!(covered, vec![0, 1, 2, 3, 4, 5]);
    }
}
