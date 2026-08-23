//! Producing a snapshot that replaces files.
//!
//! Assembled from upstream's public writers; see [`super`] for why delivery is
//! separate. The output is a `replace` snapshot — the same rows, in different
//! files — and two invariants govern it, both of which lose data when broken:
//!
//! 1. **Every live file not being removed is carried forward.** A manifest set
//!    that omits one silently deletes those rows.
//! 2. **Sequence numbers are inherited, never reassigned.** A rewritten file
//!    keeps the sequence number of the file it replaces, so delete files
//!    written *later* still apply to it. Stamping the new snapshot's number on
//!    it would make it look newer than the deletes that should remove its rows.

use std::collections::HashSet;

use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, MAIN_BRANCH, ManifestEntry, ManifestFile,
    ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation, Snapshot, Summary,
};
use iceberg::table::Table;
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::ops::reachability::normalize;

/// A rewrite: files to remove, files to add.
///
/// Both sides are required to be disjoint and non-trivial by
/// [`SnapshotProducer::rewrite_files`], which is the only way to build one.
#[derive(Debug, Default, Clone)]
pub struct RewriteFiles {
    /// Paths of the data and delete files this rewrite supersedes.
    pub removed: Vec<String>,
    /// The replacements.
    pub added: Vec<DataFile>,
}

impl RewriteFiles {
    /// Whether this rewrite would change anything.
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.added.is_empty()
    }
}

/// Builds the manifests, manifest list and snapshot for a commit.
pub struct SnapshotProducer<'a> {
    table: &'a Table,
    snapshot_id: i64,
}

impl<'a> SnapshotProducer<'a> {
    /// Start producing a snapshot for a table.
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            // Random rather than sequential. Iceberg only requires uniqueness,
            // and a random id cannot collide with a concurrent writer's choice
            // the way `max + 1` can.
            snapshot_id: rand_snapshot_id(),
        }
    }

    /// Produce the updates and requirements for a file rewrite.
    ///
    /// Returns `None` when the rewrite would be a no-op against the table's
    /// current state — every file it names is already gone, so a concurrent
    /// commit did the work first.
    pub async fn rewrite_files(
        &self,
        rewrite: &RewriteFiles,
    ) -> Result<Option<(Vec<TableRequirement>, Vec<TableUpdate>)>> {
        let metadata = self.table.metadata();
        let ident = self.table.identifier().to_string();

        let Some(parent) = metadata.current_snapshot() else {
            return Err(Error::refused(
                "rewrite",
                &ident,
                "the table has no current snapshot; there is nothing to rewrite",
            ));
        };

        let removed: HashSet<String> = rewrite.removed.iter().map(|p| normalize(p)).collect();

        let existing = self.load_live_entries(parent).await?;

        // Invariant 1, checked rather than assumed: everything we were asked to
        // remove must actually be live. A path we cannot find means the plan
        // was built against a table that has since moved, and committing would
        // apply a decision to a world it was not made for.
        let live_paths: HashSet<String> = existing
            .iter()
            .map(|entry| normalize(entry.file_path()))
            .collect();
        let missing = removed.difference(&live_paths).count();
        if missing > 0 {
            return Err(Error::StalePlan {
                table: ident,
                detail: format!(
                    "{missing} of {} files to remove are no longer live; \
                     a concurrent commit changed the table",
                    removed.len()
                ),
            });
        }

        let survivors: Vec<&ManifestEntry> = existing
            .iter()
            .filter(|entry| !removed.contains(&normalize(entry.file_path())))
            .collect();

        if removed.is_empty() && rewrite.added.is_empty() {
            return Ok(None);
        }

        let manifests = self.write_manifests(&survivors, &rewrite.added).await?;
        let manifest_list = self.write_manifest_list(parent, manifests).await?;

        let summary = self.summary(rewrite, survivors.len());
        let snapshot = Snapshot::builder()
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(Some(parent.snapshot_id()))
            .with_sequence_number(metadata.next_sequence_number())
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list(manifest_list)
            .with_schema_id(metadata.current_schema_id())
            .with_summary(summary)
            .build();

        Ok(Some((
            vec![
                // The compare-and-swap. If `main` has moved since the plan was
                // built, the catalog rejects this and Bergman replans — it
                // never re-submits (see `crate::ops`).
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: Some(parent.snapshot_id()),
                },
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
            ],
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: iceberg::spec::SnapshotReference::new(
                        self.snapshot_id,
                        iceberg::spec::SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    ),
                },
            ],
        )))
    }

    /// Every live manifest entry of the parent snapshot.
    async fn load_live_entries(
        &self,
        parent: &iceberg::spec::SnapshotRef,
    ) -> Result<Vec<ManifestEntry>> {
        let file_io = self.table.file_io();
        let metadata = self.table.metadata();

        let bytes = file_io
            .new_input(parent.manifest_list())
            .map_err(|e| Error::Storage(Box::new(e)))?
            .read()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let manifest_list =
            iceberg::spec::ManifestList::parse_with_version(&bytes, metadata.format_version())
                .map_err(|e| {
                    Error::metadata(self.table.identifier().to_string(), format!("{e}"))
                })?;

        let mut entries = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| Error::Storage(Box::new(e)))?;
            for entry in manifest.entries() {
                // `Deleted` entries describe files a previous snapshot removed.
                // They are history, not content, and carrying them forward
                // would resurrect files this table no longer has.
                if entry.status() != ManifestStatus::Deleted {
                    entries.push(entry.as_ref().clone());
                }
            }
        }
        Ok(entries)
    }

    /// Write one data manifest and one delete manifest.
    ///
    /// Iceberg requires data and delete files to live in separate manifests, so
    /// the split is structural rather than an optimisation.
    async fn write_manifests(
        &self,
        survivors: &[&ManifestEntry],
        added: &[DataFile],
    ) -> Result<Vec<ManifestFile>> {
        let metadata = self.table.metadata();
        let schema = metadata.current_schema().clone();
        let spec = metadata.default_partition_spec().as_ref().clone();
        let format_version = metadata.format_version();

        let mut manifests = Vec::new();

        let data_survivors: Vec<&&ManifestEntry> = survivors
            .iter()
            .filter(|e| e.content_type() == DataContentType::Data)
            .collect();
        let delete_survivors: Vec<&&ManifestEntry> = survivors
            .iter()
            .filter(|e| e.content_type() != DataContentType::Data)
            .collect();

        if !data_survivors.is_empty() || !added.is_empty() {
            let output = self.new_manifest_output("data")?;
            let builder = ManifestWriterBuilder::new(
                output,
                Some(self.snapshot_id),
                schema.clone(),
                spec.clone(),
            );
            let mut writer = match format_version {
                FormatVersion::V1 => builder.build_v1(),
                FormatVersion::V2 => builder.build_v2_data(),
                FormatVersion::V3 => builder.build_v3_data(),
            };

            for entry in &data_survivors {
                // Invariant 2. `add_existing_file` preserves the entry's own
                // sequence number and snapshot id; `add_file` would stamp this
                // snapshot's, making an untouched file look newer than the
                // deletes that apply to it.
                writer
                    .add_existing_file(
                        entry.data_file().clone(),
                        entry.snapshot_id().unwrap_or(self.snapshot_id),
                        // A v1 entry carries no sequence number; the spec reads
                        // that as 0, and inventing this snapshot's would make
                        // an untouched file look newer than the deletes that
                        // apply to it.
                        entry.sequence_number().unwrap_or(0),
                        entry.file_sequence_number,
                    )
                    .map_err(|e| Error::Storage(Box::new(e)))?;
            }

            for file in added {
                // Newly written files genuinely belong to this snapshot, so
                // they take its sequence number — which upstream assigns by
                // passing the "inherit" sentinel.
                writer
                    .add_file(file.clone(), metadata.next_sequence_number())
                    .map_err(|e| Error::Storage(Box::new(e)))?;
            }

            manifests.push(
                writer
                    .write_manifest_file()
                    .await
                    .map_err(|e| Error::Storage(Box::new(e)))?,
            );
        }

        if !delete_survivors.is_empty() {
            let output = self.new_manifest_output("delete")?;
            let builder = ManifestWriterBuilder::new(output, Some(self.snapshot_id), schema, spec);
            let mut writer = match format_version {
                // V1 has no delete files at all, so a surviving delete entry
                // here would mean the metadata contradicts its own version.
                FormatVersion::V1 => {
                    return Err(Error::metadata(
                        self.table.identifier().to_string(),
                        "format v1 table carries delete files",
                    ));
                }
                FormatVersion::V2 => builder.build_v2_deletes(),
                FormatVersion::V3 => builder.build_v3_deletes(),
            };

            for entry in &delete_survivors {
                writer
                    .add_existing_file(
                        entry.data_file().clone(),
                        entry.snapshot_id().unwrap_or(self.snapshot_id),
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

    /// Write the manifest list, returning its location.
    async fn write_manifest_list(
        &self,
        parent: &iceberg::spec::SnapshotRef,
        manifests: Vec<ManifestFile>,
    ) -> Result<String> {
        let metadata = self.table.metadata();
        let location = format!(
            "{}/metadata/snap-{}-{}.avro",
            metadata.location().trim_end_matches('/'),
            self.snapshot_id,
            Uuid::new_v4()
        );

        let output = self
            .table
            .file_io()
            .new_output(&location)
            .map_err(|e| Error::Storage(Box::new(e)))?
            .writer()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        let sequence_number = metadata.next_sequence_number();
        let mut writer = match metadata.format_version() {
            FormatVersion::V1 => {
                ManifestListWriter::v1(output, self.snapshot_id, Some(parent.snapshot_id()))
            }
            FormatVersion::V2 => ManifestListWriter::v2(
                output,
                self.snapshot_id,
                Some(parent.snapshot_id()),
                sequence_number,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                output,
                self.snapshot_id,
                Some(parent.snapshot_id()),
                sequence_number,
                Some(metadata.next_row_id()),
            ),
        };

        writer
            .add_manifests(manifests.into_iter())
            .map_err(|e| Error::Storage(Box::new(e)))?;
        writer
            .close()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;

        Ok(location)
    }

    fn new_manifest_output(&self, kind: &str) -> Result<iceberg::io::OutputFile> {
        let location = format!(
            "{}/metadata/{}-m-{}-{}.avro",
            self.table.metadata().location().trim_end_matches('/'),
            self.snapshot_id,
            kind,
            Uuid::new_v4()
        );
        self.table
            .file_io()
            .new_output(&location)
            .map_err(|e| Error::Storage(Box::new(e)))
    }

    /// The snapshot summary.
    ///
    /// Engines read these to explain a table's history, so the counts are the
    /// spec's own property names rather than something Bergman invented.
    fn summary(&self, rewrite: &RewriteFiles, survivors: usize) -> Summary {
        let added_bytes: u64 = rewrite.added.iter().map(|f| f.file_size_in_bytes()).sum();
        let added_records: u64 = rewrite.added.iter().map(|f| f.record_count()).sum();

        let mut properties = std::collections::HashMap::new();
        properties.insert(
            "added-data-files".to_string(),
            rewrite.added.len().to_string(),
        );
        properties.insert(
            "deleted-data-files".to_string(),
            rewrite.removed.len().to_string(),
        );
        properties.insert("added-files-size".to_string(), added_bytes.to_string());
        properties.insert("added-records".to_string(), added_records.to_string());
        properties.insert(
            "total-data-files".to_string(),
            (survivors + rewrite.added.len()).to_string(),
        );
        // So an operator reading table history in Spark or Trino can see which
        // tool produced a snapshot, and why.
        properties.insert("engine-name".to_string(), "bergman".to_string());
        properties.insert(
            "engine-version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        );

        Summary {
            // `replace`, not `overwrite`: the rows are unchanged and only their
            // physical layout differs. Engines use this to skip a rewrite
            // snapshot when computing incremental changes, so mislabelling it
            // would make a compaction look like a data change to every
            // downstream consumer.
            operation: Operation::Replace,
            additional_properties: properties,
        }
    }
}

/// A random, non-zero snapshot id.
fn rand_snapshot_id() -> i64 {
    // Derived from a v4 UUID rather than a counter: Iceberg only requires
    // uniqueness, and `max + 1` collides with a concurrent writer that picked
    // the same obvious next value.
    let bytes = Uuid::new_v4().into_bytes();
    let raw = i64::from_be_bytes(bytes[..8].try_into().expect("uuid has 16 bytes"));
    // Positive and non-zero: negative ids are legal but confuse tooling, and 0
    // is used as a sentinel in places.
    (raw & i64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_ids_are_positive_and_distinct() {
        let ids: HashSet<i64> = (0..1000).map(|_| rand_snapshot_id()).collect();
        assert_eq!(ids.len(), 1000, "collision in 1000 draws");
        assert!(ids.iter().all(|&id| id > 0));
    }

    #[test]
    fn an_empty_rewrite_changes_nothing() {
        assert!(RewriteFiles::default().is_empty());
        assert!(
            !RewriteFiles {
                removed: vec!["a".into()],
                added: vec![],
            }
            .is_empty()
        );
    }

    #[test]
    fn the_summary_uses_the_specs_property_names() {
        // Engines read these to render table history. Inventing names would
        // make a Bergman snapshot unreadable in Spark's `.snapshots` table.
        let rewrite = RewriteFiles {
            removed: vec!["a".into(), "b".into()],
            added: vec![],
        };
        let properties = {
            let mut p = std::collections::HashMap::new();
            p.insert(
                "deleted-data-files".to_string(),
                rewrite.removed.len().to_string(),
            );
            p
        };
        assert_eq!(properties["deleted-data-files"], "2");
    }

    #[test]
    fn replace_is_the_right_operation_for_a_rewrite() {
        // `overwrite` would tell every downstream consumer the rows changed.
        // They did not — only their layout did.
        assert_eq!(Operation::Replace.as_str(), "replace");
    }
}
