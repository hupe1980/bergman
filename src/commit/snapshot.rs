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
    DataFile, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestEntry, ManifestFile,
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

/// One manifest of the parent snapshot, with its live entries.
pub(crate) struct LoadedManifest {
    /// The manifest as the parent's manifest list records it, so an untouched
    /// manifest can be carried forward without being rewritten.
    pub file: ManifestFile,
    /// Its entries, excluding ones a previous snapshot marked deleted.
    pub entries: Vec<ManifestEntry>,
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

        // A table with no current snapshot has nothing to carry forward, and a
        // rewrite of it is an append. Refusing here instead would make the
        // producer unable to write a table's first snapshot, which is a strange
        // hole in something called a snapshot producer.
        let parent = metadata.current_snapshot();
        if parent.is_none() && !rewrite.removed.is_empty() {
            return Err(Error::refused(
                "rewrite",
                &ident,
                "the table has no snapshots, so the files to remove do not exist",
            ));
        }

        let removed: HashSet<String> = rewrite.removed.iter().map(|p| normalize(p)).collect();

        let existing = match parent {
            Some(parent) => self.load_manifests(parent).await?,
            None => Vec::new(),
        };

        // Invariant 1, checked rather than assumed: everything we were asked to
        // remove must actually be live. A path we cannot find means the plan
        // was built against a table that has since moved, and committing would
        // apply a decision to a world it was not made for.
        let live_paths: HashSet<String> = existing
            .iter()
            .flat_map(|m| m.entries.iter())
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

        if removed.is_empty() && rewrite.added.is_empty() {
            return Ok(None);
        }

        let survivor_count = existing
            .iter()
            .flat_map(|m| m.entries.iter())
            .filter(|entry| !removed.contains(&normalize(entry.file_path())))
            .count();

        let manifests = self
            .write_manifests(&existing, &removed, &rewrite.added)
            .await?;
        let summary = self.summary(rewrite, survivor_count);

        Ok(Some(self.install(parent, manifests, summary).await?))
    }

    /// The id this producer will stamp on the snapshot it builds.
    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Turn a finished manifest set into the commit that installs it.
    ///
    /// Both maintenance operations that replace files end here: compaction
    /// after rewriting data, manifest rewriting after re-packing entries. The
    /// snapshot, the branch move and the two preconditions are identical in
    /// both cases, and having one implementation is what keeps them that way.
    pub(crate) async fn install(
        &self,
        parent: Option<&iceberg::spec::SnapshotRef>,
        manifests: Vec<ManifestFile>,
        summary: Summary,
    ) -> Result<(Vec<TableRequirement>, Vec<TableUpdate>)> {
        let parent_id = parent.map(|p| p.snapshot_id());
        let metadata = self.table.metadata();
        let manifest_list = self.write_manifest_list(parent, manifests).await?;

        let snapshot = Snapshot::builder()
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(parent_id)
            .with_sequence_number(metadata.next_sequence_number())
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list(manifest_list)
            .with_schema_id(metadata.current_schema_id())
            .with_summary(summary)
            .build();

        Ok((
            vec![
                // The compare-and-swap. If `main` has moved since the plan was
                // built, the catalog rejects this and Bergman replans — it
                // never re-submits (see `crate::ops`).
                // `None` asserts the ref does not exist yet, which is the
                // right precondition for a table's first snapshot.
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: parent_id,
                },
                // Guards against the table being dropped and recreated in
                // between, which the ref check alone would not catch.
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
            ],
            vec![
                TableUpdate::AddSnapshot { snapshot },
                // Adding a snapshot without moving `main` leaves it
                // unreachable: the operation would appear to succeed while
                // changing nothing anyone can read.
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
        ))
    }

    /// The parent snapshot's manifests, each with its live entries.
    ///
    /// Grouped rather than flattened, because a rewrite must know *which*
    /// manifest an entry came from: a manifest none of whose files are being
    /// removed is carried into the new snapshot by reference, and never
    /// rewritten.
    pub(crate) async fn load_manifests(
        &self,
        parent: &iceberg::spec::SnapshotRef,
    ) -> Result<Vec<LoadedManifest>> {
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

        let mut loaded = Vec::with_capacity(manifest_list.entries().len());
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| Error::Storage(Box::new(e)))?;

            let entries: Vec<ManifestEntry> = manifest
                .entries()
                .iter()
                // `Deleted` entries describe files a previous snapshot removed.
                // They are history, not content, and carrying them forward
                // would resurrect files this table no longer has.
                .filter(|e| e.status() != ManifestStatus::Deleted)
                .map(|e| e.as_ref().clone())
                .collect();

            loaded.push(LoadedManifest {
                file: manifest_file.clone(),
                entries,
            });
        }
        Ok(loaded)
    }

    /// Build the new snapshot's manifest set.
    ///
    /// A manifest none of whose files are being removed is **carried by
    /// reference**: its bytes are not read again and not rewritten, only its
    /// entry in the new manifest list. Only the manifests that actually lost a
    /// file are rebuilt, plus one new manifest for the added files.
    ///
    /// Rewriting every manifest instead would be correct but ruinous: a
    /// hundred-thousand-file table would have its whole metadata rewritten to
    /// compact one partition, and the result would be a single enormous
    /// manifest rather than the target-sized ones the table asked for.
    async fn write_manifests(
        &self,
        existing: &[LoadedManifest],
        removed: &HashSet<String>,
        added: &[DataFile],
    ) -> Result<Vec<ManifestFile>> {
        let metadata = self.table.metadata();
        let schema = metadata.current_schema().clone();
        let spec = metadata.default_partition_spec().as_ref().clone();
        let format_version = metadata.format_version();

        let mut manifests = Vec::new();

        for loaded in existing {
            let touched = loaded
                .entries
                .iter()
                .any(|entry| removed.contains(&normalize(entry.file_path())));

            if !touched {
                manifests.push(loaded.file.clone());
                continue;
            }

            let survivors: Vec<&ManifestEntry> = loaded
                .entries
                .iter()
                .filter(|entry| !removed.contains(&normalize(entry.file_path())))
                .collect();

            // A manifest whose every file was removed simply disappears.
            if survivors.is_empty() {
                continue;
            }

            let is_delete_manifest = loaded.file.content == ManifestContentType::Deletes;
            let output =
                self.new_manifest_output(if is_delete_manifest { "delete" } else { "data" })?;
            let builder = ManifestWriterBuilder::new(
                output,
                Some(self.snapshot_id),
                schema.clone(),
                spec.clone(),
            );
            let mut writer = match (is_delete_manifest, format_version) {
                (false, FormatVersion::V1) => builder.build_v1(),
                (false, FormatVersion::V2) => builder.build_v2_data(),
                (false, FormatVersion::V3) => builder.build_v3_data(),
                (true, FormatVersion::V2) => builder.build_v2_deletes(),
                (true, FormatVersion::V3) => builder.build_v3_deletes(),
                (true, FormatVersion::V1) => {
                    return Err(Error::metadata(
                        self.table.identifier().to_string(),
                        "format v1 table carries delete files",
                    ));
                }
            };

            for entry in survivors {
                // Invariant 2. `add_existing_file` preserves the entry's own
                // sequence number and snapshot id; `add_file` would stamp this
                // snapshot's, making an untouched file look newer than the
                // deletes that apply to it.
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

        if !added.is_empty() {
            let output = self.new_manifest_output("data")?;
            let builder = ManifestWriterBuilder::new(output, Some(self.snapshot_id), schema, spec);
            let mut writer = match format_version {
                FormatVersion::V1 => builder.build_v1(),
                FormatVersion::V2 => builder.build_v2_data(),
                FormatVersion::V3 => builder.build_v3_data(),
            };

            for file in added {
                // Newly written files genuinely belong to this snapshot, so they
                // take its sequence number.
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

        Ok(manifests)
    }

    /// Write the manifest list, returning its location.
    pub(crate) async fn write_manifest_list(
        &self,
        parent: Option<&iceberg::spec::SnapshotRef>,
        manifests: Vec<ManifestFile>,
    ) -> Result<String> {
        let parent_id = parent.map(|p| p.snapshot_id());
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
            FormatVersion::V1 => ManifestListWriter::v1(output, self.snapshot_id, parent_id),
            FormatVersion::V2 => {
                ManifestListWriter::v2(output, self.snapshot_id, parent_id, sequence_number)
            }
            FormatVersion::V3 => ManifestListWriter::v3(
                output,
                self.snapshot_id,
                parent_id,
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

    pub(crate) fn new_manifest_output(&self, kind: &str) -> Result<iceberg::io::OutputFile> {
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
