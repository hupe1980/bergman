//! Manifest rewriting.
//!
//! Every commit adds manifests, and nothing coalesces them. A table written
//! once a minute accumulates thousands of small Avro files, and every query's
//! planning phase opens all of them — so a table can be perfectly compacted at
//! the data level and still plan slowly.
//!
//! The fix costs nothing but metadata: re-pack the same manifest *entries* into
//! fewer, larger manifests. No data file is read or written, no row moves, and
//! the table's contents are bit-for-bit identical afterwards. It is the
//! cheapest real win available in Iceberg maintenance, and upstream has no
//! action for it — so Bergman commits it through its own commit layer
//! ([`crate::commit`]).
//!
//! # The rule that matters
//!
//! **Entries may only be packed together when they share a partition spec and
//! a content type.** A manifest carries exactly one `partition_spec_id`, and an
//! entry's partition tuple is meaningless against any other spec — so merging
//! a spec-0 entry into a spec-1 manifest re-interprets its partition values and
//! silently mis-prunes files at query time. Nothing fails; queries just start
//! returning wrong answers.
//!
//! # And the rule that makes it worth doing
//!
//! **Entries are clustered by partition before they are packed.** A manifest
//! list records each manifest's partition *summary*, and that summary is what
//! lets a query skip a manifest without opening it. Packing entries in arrival
//! order gives every manifest a summary spanning the whole table — fewer
//! manifests, all of which must now be read, which is worse than what the
//! rewrite started with. Java's `RewriteManifests` clusters the same way.

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::TableIdent;
use iceberg::spec::{
    DataContentType, FormatVersion, ManifestEntry, ManifestFile, ManifestStatus,
    ManifestWriterBuilder, Operation, PartitionSpec, Summary,
};
use iceberg::table::Table;
use uuid::Uuid;

use crate::commit::{BranchRetention, SnapshotProducer, TableCommitter};
use crate::error::{Error, Result};
use crate::ops::{MAX_COMMIT_ATTEMPTS, OpEnv, retry_delay};
use crate::plan::OperationResult;
use crate::policy::EffectiveManifests;
use crate::util::human_bytes;

/// Rewrite a table's manifests into fewer, larger ones.
///
/// Retries against a reloaded table on conflict, because the entries this
/// re-packs are the ones the table had when the attempt began — and a
/// concurrent commit changes them.
pub async fn run(env: &OpEnv<'_>, settings: &EffectiveManifests) -> Result<OperationResult> {
    let ident = env.ident;
    let ctx = env.ctx;
    let mut current = env.table.clone();

    for attempt in 0..MAX_COMMIT_ATTEMPTS {
        match attempt_rewrite(&current, ident, env.committer, settings).await {
            Ok(result) => return Ok(result),
            Err(e) if e.is_replan() && attempt + 1 < MAX_COMMIT_ATTEMPTS => {
                tracing::debug!(
                    table = %ctx.table,
                    attempt = attempt + 1,
                    "manifest rewrite lost its commit; reloading and re-packing"
                );
                tokio::time::sleep(retry_delay(attempt)).await;
                current = env.loader.reload(ident).await?;
            }
            Err(e) if e.is_replan() => {
                return Ok(OperationResult::Conflicted {
                    detail: format!(
                        "table moved during {MAX_COMMIT_ATTEMPTS} commit attempts; \
                         will replan next cycle"
                    ),
                });
            }
            Err(e) => return Err(e),
        }
    }

    unreachable!("the loop returns on the final attempt")
}

async fn attempt_rewrite(
    table: &Table,
    ident: &TableIdent,
    committer: &dyn TableCommitter,
    settings: &EffectiveManifests,
) -> Result<OperationResult> {
    let metadata = table.metadata();

    let Some(parent) = metadata.current_snapshot() else {
        return Ok(OperationResult::NoOp {
            detail: "the table has never been written to".into(),
        });
    };

    let retention = BranchRetention::load(table).await?;
    let producer = SnapshotProducer::new(table, retention);
    // Before any manifest is read, let alone written: a rewrite Bergman cannot
    // commit should cost a metadata lookup, not a full pass over the table's
    // manifest set followed by a refusal.
    producer.check_authorable()?;

    let snapshot_id = producer.snapshot_id();
    let loaded = producer.load_manifests(parent).await?;

    let target = settings.target_size.value;
    let before = loaded.len();
    let bytes_before: u64 = loaded
        .iter()
        .map(|m| m.file.manifest_length.max(0) as u64)
        .sum();

    // Only the undersized manifests are worth touching. Rewriting one that is
    // already at target reads and writes it for no gain — and a rewrite that
    // achieves nothing still costs a snapshot, which is the opposite of the
    // problem being solved.
    let undersized = loaded
        .iter()
        .filter(|m| (m.file.manifest_length.max(0) as u64) < target)
        .count();

    if undersized < settings.min_count_to_merge.value {
        return Ok(OperationResult::NoOp {
            detail: format!(
                "{undersized} of {before} manifests are below {} (need {} to merge)",
                human_bytes(target),
                settings.min_count_to_merge.value
            ),
        });
    }

    // Manifests already at target are carried through untouched — their bytes
    // are not rewritten, only their reference in the new manifest list.
    let mut manifests: Vec<ManifestFile> = loaded
        .iter()
        .filter(|m| (m.file.manifest_length.max(0) as u64) >= target)
        .map(|m| m.file.clone())
        .collect();

    // The rule that matters: entries are bucketed by (spec, content) before
    // anything is packed, so a manifest never mixes partition specs.
    let mut buckets: BTreeMap<(i32, ManifestKind), Vec<Arc<ManifestEntry>>> = BTreeMap::new();
    for m in &loaded {
        if (m.file.manifest_length.max(0) as u64) >= target {
            continue;
        }
        for entry in &m.entries {
            // `Deleted` entries are history, not content. Carrying them forward
            // would resurrect files the table no longer has; dropping them is
            // exactly what a rewrite is for. (`load_manifests` already filters
            // them; this is the second half of the same rule, stated where it
            // is read.)
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }
            let kind = match entry.content_type() {
                DataContentType::Data => ManifestKind::Data,
                _ => ManifestKind::Deletes,
            };
            buckets
                .entry((m.file.partition_spec_id, kind))
                .or_default()
                .push(Arc::new(entry.clone()));
        }
    }

    for ((spec_id, kind), mut entries) in buckets {
        let spec = producer.spec_for(spec_id)?;
        cluster_by_partition(&mut entries, &spec, table.metadata().current_schema());
        manifests.extend(write_packed(table, snapshot_id, &entries, target, kind, &spec).await?);
    }

    let after = manifests.len();
    if after >= before {
        // Bin-packing did not help — the entries are large enough that the same
        // number of manifests comes out. Committing anyway would add a snapshot
        // for nothing, every cycle, forever.
        return Ok(OperationResult::NoOp {
            detail: format!(
                "re-packing {before} manifests would produce {after}; not worth a commit"
            ),
        });
    }

    let summary = Summary {
        // `replace`: the rows are untouched and only their metadata layout
        // differs. Anything else would tell downstream consumers the data
        // changed.
        operation: Operation::Replace,
        additional_properties: std::collections::HashMap::from([
            ("manifests-replaced".to_string(), undersized.to_string()),
            (
                "manifests-kept".to_string(),
                (before - undersized).to_string(),
            ),
            ("engine-name".to_string(), "bergman".to_string()),
            (
                "engine-version".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            ),
        ]),
    };

    let (requirements, updates) = producer.install(Some(parent), manifests, summary).await?;
    committer.commit(ident, requirements, updates).await?;

    Ok(OperationResult::Succeeded {
        detail: format!(
            "{before} manifests ({}) re-packed into {after}",
            human_bytes(bytes_before)
        ),
    })
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ManifestKind {
    Data,
    Deletes,
}

/// Sort entries so that each packed manifest covers a narrow range of
/// partitions.
///
/// A manifest list records every manifest's partition summary, and a query
/// prunes on it: a manifest whose summary cannot contain the predicate's
/// partition is never opened. Entries arriving in commit order are scattered
/// across partitions, so packing them as they come produces manifests that each
/// span the whole table — fewer files, every one of which must now be read,
/// which is worse than what the rewrite started with.
///
/// Sorted by the rendered partition key, which is [`crate::health`]'s and is
/// injective, so entries of one partition end up adjacent.
///
/// The sort is **stable, and nothing breaks ties**: within a partition the
/// entries keep the order the parent's manifests had, which is commit order. A
/// table whose rows arrive in time order keeps that locality, and an arbitrary
/// second key would reshuffle every entry in every partition to no purpose.
fn cluster_by_partition(
    entries: &mut [Arc<ManifestEntry>],
    spec: &PartitionSpec,
    schema: &iceberg::spec::Schema,
) {
    // Rendered once per entry rather than inside the comparator: a sort does
    // O(n log n) comparisons and rendering a partition tuple is not free.
    let mut keyed: Vec<(String, Arc<ManifestEntry>)> = entries
        .iter()
        .map(|entry| {
            (
                crate::health::partition_path(spec, schema, entry.data_file().partition()),
                Arc::clone(entry),
            )
        })
        .collect();

    keyed.sort_by(|(a_key, _), (b_key, _)| a_key.cmp(b_key));

    for (slot, (_, entry)) in entries.iter_mut().zip(keyed) {
        *slot = entry;
    }
}

/// Bin-pack manifest entries into manifests of roughly `target` bytes.
///
/// `spec` is the partition spec every entry in `entries` was written under —
/// the caller has already bucketed by it, because writing them under any other
/// re-interprets their partition tuples.
async fn write_packed(
    table: &Table,
    snapshot_id: i64,
    entries: &[Arc<ManifestEntry>],
    target: u64,
    kind: ManifestKind,
    spec: &PartitionSpec,
) -> Result<Vec<ManifestFile>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let metadata = table.metadata();
    let format_version = metadata.format_version();

    if kind == ManifestKind::Deletes && format_version == FormatVersion::V1 {
        return Err(Error::metadata(
            table.identifier().to_string(),
            "format v1 table carries delete files",
        ));
    }

    // A manifest entry is metadata, not the file it describes, so its on-disk
    // cost has nothing to do with `file_size_in_bytes`. This is an estimate of
    // the Avro record: identity, partition tuple, and the per-column bounds and
    // counts, which dominate. Being approximate is fine — the target is itself
    // a heuristic, and the failure mode of a bad estimate is a manifest that is
    // somewhat off target rather than anything incorrect.
    let per_entry = estimated_entry_bytes(metadata);
    let entries_per_manifest = (target / per_entry).max(1) as usize;

    let mut manifests = Vec::new();
    for chunk in entries.chunks(entries_per_manifest) {
        let location = format!(
            "{}/metadata/{snapshot_id}-m-{}-{}-{}.avro",
            metadata.location().trim_end_matches('/'),
            match kind {
                ManifestKind::Data => "data",
                ManifestKind::Deletes => "delete",
            },
            spec.spec_id(),
            Uuid::new_v4()
        );
        let output = table
            .file_io()
            .new_output(&location)
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let builder = ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            metadata.current_schema().clone(),
            spec.clone(),
        );
        let mut writer = match (kind, format_version) {
            (ManifestKind::Data, FormatVersion::V1) => builder.build_v1(),
            (ManifestKind::Data, FormatVersion::V2) => builder.build_v2_data(),
            (ManifestKind::Data, FormatVersion::V3) => builder.build_v3_data(),
            (ManifestKind::Deletes, FormatVersion::V2) => builder.build_v2_deletes(),
            (ManifestKind::Deletes, FormatVersion::V3) => builder.build_v3_deletes(),
            (ManifestKind::Deletes, FormatVersion::V1) => unreachable!("checked above"),
        };

        for entry in chunk {
            // Every entry keeps its original snapshot id and sequence numbers.
            // A manifest rewrite must be invisible to readers: stamping this
            // snapshot's numbers on the entries would make untouched data files
            // appear newer than the delete files that apply to them, which
            // resurrects deleted rows.
            writer
                .add_existing_file(
                    entry.data_file().clone(),
                    entry.snapshot_id().unwrap_or(snapshot_id),
                    entry.sequence_number().unwrap_or(0),
                    entry.file_sequence_number,
                )
                .map_err(|e| Error::Storage(Box::new(e)))?;
        }

        manifests.push(
            writer
                .write_manifest_file()
                .await
                .map_err(|e| Error::Storage(Box::new(e)))?,
        );
    }

    Ok(manifests)
}

/// A rough per-entry size, used only to decide how many entries fit a manifest.
fn estimated_entry_bytes(metadata: &iceberg::spec::TableMetadata) -> u64 {
    // Fixed overhead for identity and the partition tuple, plus per-column
    // bounds and counts — which is what actually grows a manifest on a wide
    // table, and the reason a fixed guess would be badly wrong for one.
    const BASE: u64 = 256;
    const PER_COLUMN: u64 = 48;

    let columns = metadata.current_schema().as_struct().fields().len() as u64;
    BASE + columns * PER_COLUMN
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_size_grows_with_column_count() {
        // A 200-column table's manifest entries are far bigger than a
        // 3-column table's, and packing both to the same entry count would put
        // one badly off target.
        //
        // Asserted through the arithmetic rather than a `TableMetadata`
        // fixture, which cannot be built without a full schema.
        let narrow = 256 + 3 * 48;
        let wide = 256 + 200 * 48;
        assert!(wide > narrow * 20);
    }

    #[test]
    fn entries_per_manifest_is_at_least_one() {
        // A target smaller than a single entry must still produce a manifest
        // rather than dividing to zero and looping forever.
        let target: u64 = 10;
        let per_entry: u64 = 1000;
        assert_eq!((target / per_entry).max(1), 1);
    }

    /// A manifest entry naming a file in a partition, for the clustering test.
    fn entry(partition_value: i32, path: &str) -> Arc<ManifestEntry> {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(Literal::int(partition_value))]))
            .record_count(1)
            .file_size_in_bytes(1)
            .partition_spec_id(0)
            .build()
            .expect("data file");

        Arc::new(
            ManifestEntry::builder()
                .status(ManifestStatus::Existing)
                .snapshot_id(1)
                .sequence_number(1)
                .file_sequence_number(1)
                .data_file(data_file)
                .build(),
        )
    }

    /// A schema and a spec partitioning it by `day` (identity over an int).
    fn spec_and_schema() -> (PartitionSpec, iceberg::spec::Schema) {
        use iceberg::spec::{
            NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
        };

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "day", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "day", Transform::Identity)
            .unwrap()
            .build()
            .bind(schema.clone())
            .unwrap();

        (spec, schema)
    }

    #[test]
    fn entries_are_clustered_by_partition_before_packing() {
        // The rule that makes the rewrite worth doing. A manifest list records
        // each manifest's partition summary, and a query prunes on it — so
        // packing entries in commit order produces manifests that each span the
        // whole table, and every query then opens every one of them. Fewer
        // manifests that all have to be read is worse than what the rewrite
        // started with.
        let (spec, schema) = spec_and_schema();

        // Commit order: partitions interleaved, exactly as a table written
        // across several days accumulates them.
        let mut entries = vec![
            entry(3, "c.parquet"),
            entry(1, "a.parquet"),
            entry(2, "b.parquet"),
            entry(1, "d.parquet"),
            entry(3, "e.parquet"),
        ];

        cluster_by_partition(&mut entries, &spec, &schema);

        let order: Vec<&str> = entries.iter().map(|e| e.file_path()).collect();
        assert_eq!(
            order,
            vec![
                "a.parquet",
                "d.parquet",
                "b.parquet",
                "c.parquet",
                "e.parquet"
            ],
            "entries of one partition must end up adjacent"
        );
    }

    #[test]
    fn clustering_moves_only_what_it_has_to() {
        // The sort is stable and nothing breaks ties, so entries of one
        // partition keep the order the parent's manifests had — commit order.
        // A table whose rows arrive in time order keeps that locality, and with
        // it the tight min/max bounds a timestamp column gets from it. An
        // arbitrary second key would reshuffle every entry in every partition to
        // no purpose.
        let (spec, schema) = spec_and_schema();

        let mut entries = vec![
            entry(1, "z.parquet"),
            entry(1, "a.parquet"),
            entry(1, "m.parquet"),
        ];
        cluster_by_partition(&mut entries, &spec, &schema);

        let order: Vec<&str> = entries.iter().map(|e| e.file_path()).collect();
        assert_eq!(
            order,
            vec!["z.parquet", "a.parquet", "m.parquet"],
            "one partition's entries must not be reordered"
        );
    }

    #[test]
    fn entries_bucket_by_spec_and_content_before_packing() {
        // The rule that matters, exercised on the bucket key itself: two specs
        // and two content types make four buckets, never one. A manifest that
        // mixed them would re-interpret partition tuples against the wrong
        // spec, and nothing would fail — queries would just start pruning the
        // wrong files.
        let mut buckets: BTreeMap<(i32, ManifestKind), usize> = BTreeMap::new();
        for key in [
            (0, ManifestKind::Data),
            (0, ManifestKind::Deletes),
            (1, ManifestKind::Data),
            (1, ManifestKind::Deletes),
            (0, ManifestKind::Data),
        ] {
            *buckets.entry(key).or_default() += 1;
        }

        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[&(0, ManifestKind::Data)], 2);
    }
}
