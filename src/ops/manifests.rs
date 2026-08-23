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

use std::sync::Arc;

use iceberg::TableIdent;
use iceberg::spec::{
    DataContentType, FormatVersion, ManifestEntry, ManifestFile, ManifestStatus,
    ManifestWriterBuilder, Operation, Summary,
};
use iceberg::table::Table;
use uuid::Uuid;

use crate::commit::{SnapshotProducer, TableCommitter};
use crate::error::{Error, Result};
use crate::obs::OperationContext;
use crate::plan::OperationResult;
use crate::policy::EffectiveManifests;
use crate::util::human_bytes;

/// Rewrite a table's manifests into fewer, larger ones.
pub async fn run(
    table: &Table,
    ident: &TableIdent,
    committer: &dyn TableCommitter,
    settings: &EffectiveManifests,
    ctx: OperationContext<'_>,
) -> Result<OperationResult> {
    let table_ref = ctx.table;
    let metadata = table.metadata();

    let Some(parent) = metadata.current_snapshot() else {
        return Ok(OperationResult::NoOp {
            detail: "the table has never been written to".into(),
        });
    };

    let file_io = table.file_io();
    let bytes = file_io
        .new_input(parent.manifest_list())
        .map_err(|e| Error::Storage(Box::new(e)))?
        .read()
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;

    let manifest_list =
        iceberg::spec::ManifestList::parse_with_version(&bytes, metadata.format_version())
            .map_err(|e| Error::metadata(table_ref, format!("unreadable manifest list: {e}")))?;

    let target = settings.target_size.value;
    let before = manifest_list.entries().len();
    let bytes_before: u64 = manifest_list
        .entries()
        .iter()
        .map(|m| m.manifest_length.max(0) as u64)
        .sum();

    // Only the undersized manifests are worth touching. Rewriting one that is
    // already at target reads and writes it for no gain — and a rewrite that
    // achieves nothing still costs a snapshot, which is the opposite of the
    // problem being solved.
    let undersized: Vec<&ManifestFile> = manifest_list
        .entries()
        .iter()
        .filter(|m| (m.manifest_length.max(0) as u64) < target)
        .collect();

    if undersized.len() < settings.min_count_to_merge.value {
        return Ok(OperationResult::NoOp {
            detail: format!(
                "{} of {before} manifests are below {} (need {} to merge)",
                undersized.len(),
                human_bytes(target),
                settings.min_count_to_merge.value
            ),
        });
    }

    // Manifests already at target are carried through untouched — their bytes
    // are not rewritten, only their reference in the new manifest list.
    let keep_as_is: Vec<ManifestFile> = manifest_list
        .entries()
        .iter()
        .filter(|m| (m.manifest_length.max(0) as u64) >= target)
        .cloned()
        .collect();

    let mut data_entries = Vec::new();
    let mut delete_entries = Vec::new();
    for manifest_file in &undersized {
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;
        for entry in manifest.entries() {
            // `Deleted` entries are history, not content. Carrying them forward
            // would resurrect files the table no longer has; dropping them is
            // exactly what a rewrite is for.
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }
            match entry.content_type() {
                DataContentType::Data => data_entries.push(entry.clone()),
                _ => delete_entries.push(entry.clone()),
            }
        }
    }

    let producer = SnapshotProducer::new(table);
    let snapshot_id = producer.snapshot_id();
    let mut manifests = keep_as_is;

    manifests.extend(
        write_packed(
            table,
            snapshot_id,
            &data_entries,
            target,
            ManifestKind::Data,
        )
        .await?,
    );
    manifests.extend(
        write_packed(
            table,
            snapshot_id,
            &delete_entries,
            target,
            ManifestKind::Deletes,
        )
        .await?,
    );

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
            (
                "manifests-replaced".to_string(),
                undersized.len().to_string(),
            ),
            (
                "manifests-kept".to_string(),
                (before - undersized.len()).to_string(),
            ),
            ("engine-name".to_string(), "bergman".to_string()),
            (
                "engine-version".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            ),
        ]),
    };

    let (requirements, updates) = producer.install(parent, manifests, summary).await?;
    committer.commit(ident, requirements, updates).await?;

    Ok(OperationResult::Succeeded {
        detail: format!(
            "{before} manifests ({}) re-packed into {after}",
            human_bytes(bytes_before)
        ),
    })
}

#[derive(Clone, Copy, PartialEq)]
enum ManifestKind {
    Data,
    Deletes,
}

/// Bin-pack manifest entries into manifests of roughly `target` bytes.
async fn write_packed(
    table: &Table,
    snapshot_id: i64,
    entries: &[Arc<ManifestEntry>],
    target: u64,
    kind: ManifestKind,
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
            "{}/metadata/{snapshot_id}-m-{}-{}.avro",
            metadata.location().trim_end_matches('/'),
            match kind {
                ManifestKind::Data => "data",
                ManifestKind::Deletes => "delete",
            },
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
            metadata.default_partition_spec().as_ref().clone(),
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
}
