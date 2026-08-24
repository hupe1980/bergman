//! Producing a snapshot that replaces files.
//!
//! Assembled from upstream's public writers; see [`super`] for why delivery is
//! separate. The output is a `replace` snapshot — the same rows, in different
//! files — and four invariants govern it, each of which loses or corrupts data
//! when broken:
//!
//! 1. **Every live file not being removed is carried forward.** A manifest set
//!    that omits one silently deletes those rows.
//! 2. **A carried-forward file keeps its own sequence number and snapshot id.**
//!    Stamping this snapshot's numbers on an untouched file would make it look
//!    newer than the delete files that apply to it, resurrecting deleted rows.
//!    Files this snapshot genuinely *adds* are different: they take the new
//!    sequence number, which is what retires the deletes already applied to
//!    their contents.
//! 3. **A manifest is written under the partition spec it was written under.**
//!    A manifest carries exactly one `partition_spec_id`, and every entry's
//!    partition tuple is meaningless against any other spec. Rewriting an old
//!    manifest under the table's *current* spec re-interprets every tuple in
//!    it, which mis-prunes files at query time.
//! 4. **A branch's retention survives a commit that moves it.** The REST
//!    protocol's `set-snapshot-ref` replaces the whole reference, so a commit
//!    that names no retention silently erases what the table's owner
//!    configured.

use std::collections::{HashMap, HashSet};

use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
    PartitionSpec, Snapshot, SnapshotRetention, Summary,
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

/// The retention a branch carries, read from the table's own metadata file.
///
/// Invariant 4 exists because the REST protocol has no "move this ref, keep its
/// retention" update: `set-snapshot-ref` replaces the reference wholesale. A
/// commit that sends no retention therefore erases whatever
/// `ALTER TABLE … CREATE BRANCH main RETAIN …` configured — silently, and only
/// visible the next time expiration runs.
///
/// Upstream keeps `TableMetadata::refs` `pub(crate)` and does not derive
/// `Serialize`, so there is no accessor for it. The metadata file itself is a
/// public, specified JSON document, and `SnapshotReference` is a public
/// `Deserialize` type — so the retention is read from the document rather than
/// guessed at.
#[derive(Debug, Default, Clone)]
pub struct BranchRetention {
    main: Option<SnapshotRetention>,
}

impl BranchRetention {
    /// Read the `main` branch's retention from a table's metadata file.
    ///
    /// A table whose metadata location is unknown, unreadable, or malformed
    /// yields no retention, which is what a table that never configured any
    /// also yields. Failing the commit instead would make an unreadable
    /// optional field stop maintenance altogether.
    pub async fn load(table: &Table) -> Result<Self> {
        let Some(location) = table.metadata_location() else {
            return Ok(Self::default());
        };

        let Ok(input) = table.file_io().new_input(location) else {
            return Ok(Self::default());
        };
        let Ok(bytes) = input.read().await else {
            tracing::debug!(
                location,
                "metadata file could not be read; committing without branch retention"
            );
            return Ok(Self::default());
        };

        /// Just the field this needs. Deserializing the whole document would
        /// couple Bergman to every metadata field upstream adds.
        #[derive(serde::Deserialize)]
        struct Refs {
            #[serde(default)]
            refs: HashMap<String, iceberg::spec::SnapshotReference>,
        }

        match serde_json::from_slice::<Refs>(&bytes) {
            Ok(parsed) => Ok(Self {
                main: parsed
                    .refs
                    .get(MAIN_BRANCH)
                    .map(|r| r.retention.clone())
                    // Only a branch has retention worth carrying. A `main` that
                    // somehow deserialized as a tag is malformed, and moving it
                    // as a branch is the correct repair.
                    .filter(|r| matches!(r, SnapshotRetention::Branch { .. })),
            }),
            Err(e) => {
                tracing::debug!(%e, "metadata refs could not be parsed; committing without branch retention");
                Ok(Self::default())
            }
        }
    }

    /// The retention to put on `main`, defaulting to "nothing configured".
    fn main_or_default(&self) -> SnapshotRetention {
        self.main.clone().unwrap_or(SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        })
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
    retention: BranchRetention,
}

impl<'a> SnapshotProducer<'a> {
    /// Start producing a snapshot for a table.
    ///
    /// `retention` is what [`BranchRetention::load`] read for this table; see
    /// invariant 4.
    pub fn new(table: &'a Table, retention: BranchRetention) -> Self {
        Self {
            table,
            // Random rather than sequential. Iceberg only requires uniqueness,
            // and a random id cannot collide with a concurrent writer's choice
            // the way `max + 1` can.
            snapshot_id: rand_snapshot_id(),
            retention,
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
        // Checked before a single manifest is written: a refusal that arrived
        // at install time would already have left Avro files under the table's
        // metadata directory for the orphan scanner to clean up.
        self.check_authorable()?;

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

        let counts = Counts::of(&existing, &removed, &rewrite.added);
        let manifests = self
            .write_manifests(&existing, &removed, &rewrite.added)
            .await?;
        let summary = self.summary(rewrite, &counts);

        Ok(Some(self.install(parent, manifests, summary).await?))
    }

    /// Which live delete files apply to no data file any more.
    ///
    /// A delete file becomes dangling when every data file it applied to has
    /// been rewritten away — which is exactly what happens to a delete file a
    /// rewrite could not retire because it was shared, once the other files it
    /// covered are rewritten too. Nothing else ever removes them, and every scan
    /// still opens each one.
    ///
    /// Removing one that is *not* dangling resurrects every row it was hiding,
    /// so the question is answered **two independent ways and both must agree**:
    ///
    /// 1. `still_applied` — the delete files upstream's scan associated with a
    ///    live data file. Exactly the relation a reader will use, but it is one
    ///    library's index, and deleting on the strength of it alone is one bug
    ///    away from data loss.
    /// 2. The sequence-number argument, computed here from the manifests. A
    ///    delete file applies only when the numbers stand in the right order —
    ///    strictly greater for an equality delete, greater-or-equal for a
    ///    positional one — so one below every live data file's number *in its
    ///    partition* can apply to nothing. This is Java's
    ///    `RemoveDanglingDeletes` rule, reached from the other side.
    ///
    /// It costs one pass over entries already in memory, and buys that the
    /// dangerous half of this operation needs two derivations wrong at once.
    pub async fn dangling_delete_files(
        &self,
        still_applied: &HashSet<String>,
    ) -> Result<Vec<String>> {
        let Some(parent) = self.table.metadata().current_snapshot() else {
            return Ok(Vec::new());
        };

        let manifests = self.load_manifests(parent).await?;
        // The spec id comes from the manifest, not the entry: `DataFile` keeps
        // its own copy crate-private, and a manifest carries exactly one spec
        // for every entry in it, which is the authoritative answer anyway.
        let entries = || {
            manifests
                .iter()
                .flat_map(|m| m.entries.iter().map(|e| (m.file.partition_spec_id, e)))
        };

        // The lowest sequence number a live data file carries, per partition —
        // and across the whole table, for the global case below.
        let mut lowest: HashMap<(i32, iceberg::spec::Struct), i64> = HashMap::new();
        let mut lowest_anywhere: Option<i64> = None;
        for (spec_id, entry) in entries().filter(|(_, e)| e.content_type() == DataContentType::Data)
        {
            let Some(sequence) = entry.sequence_number() else {
                continue;
            };
            let key = (spec_id, entry.data_file().partition().clone());
            lowest
                .entry(key)
                .and_modify(|current| *current = (*current).min(sequence))
                .or_insert(sequence);
            lowest_anywhere = Some(lowest_anywhere.unwrap_or(sequence).min(sequence));
        }

        let mut dangling: Vec<String> = entries()
            .filter(|(_, entry)| entry.content_type() != DataContentType::Data)
            .filter(|(_, entry)| !still_applied.contains(&normalize(entry.file_path())))
            .filter(|(spec_id, entry)| {
                is_dead_by_sequence(*spec_id, entry, &lowest, lowest_anywhere)
            })
            .map(|(_, entry)| entry.file_path().to_string())
            .collect();

        dangling.sort();
        dangling.dedup();
        Ok(dangling)
    }

    /// Whether the table holds any delete manifest at all.
    ///
    /// One manifest-*list* read, and no manifest. A table with no delete
    /// manifest can have no dangling delete file, and the alternative way to
    /// learn that — planning the whole table's scan and finding no delete file
    /// associated with anything — costs a full manifest walk to reach the same
    /// answer. On the ordinary table, which has no delete files whatsoever,
    /// that walk would happen every cycle forever.
    pub async fn has_delete_manifests(&self, parent: &iceberg::spec::SnapshotRef) -> Result<bool> {
        let metadata = self.table.metadata();
        let bytes = self
            .table
            .file_io()
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

        Ok(manifest_list
            .entries()
            .iter()
            .any(|entry| entry.content == ManifestContentType::Deletes))
    }

    /// The id this producer will stamp on the snapshot it builds.
    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Refuse a table whose snapshots Bergman cannot author correctly.
    ///
    /// See [`super::authoring_refusal`] for what that means and why the answer
    /// is a refusal rather than a best effort.
    pub fn check_authorable(&self) -> Result<()> {
        match super::authoring_refusal(self.table.metadata().format_version()) {
            None => Ok(()),
            Some(reason) => Err(Error::refused(
                "rewrite",
                self.table.identifier().to_string(),
                reason,
            )),
        }
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

        // The backstop for the check `rewrite_files` and the planner also make.
        // Every snapshot Bergman authors passes through here, so an embedder or
        // a future operation that skipped both still cannot produce one for a
        // table whose row lineage it would destroy.
        self.check_authorable()?;

        // Bergman commits on `main` and asserts `main` has not moved. If the
        // table's current snapshot is not `main`'s head, that assertion is
        // about a different snapshot than the one this commit was built on —
        // and the commit would move `main` to a snapshot descending from
        // something else entirely.
        let main_head = metadata
            .snapshot_for_ref(MAIN_BRANCH)
            .map(|s| s.snapshot_id());
        if main_head != parent_id {
            return Err(Error::refused(
                "rewrite",
                self.table.identifier().to_string(),
                format!(
                    "the table's current snapshot ({parent_id:?}) is not the head of `main` \
                     ({main_head:?}); Bergman only maintains the main branch"
                ),
            ));
        }

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
                //
                // Invariant 4: the retention comes back from the table's own
                // metadata, because this update *replaces* the reference and
                // an omitted retention is an erased one.
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: iceberg::spec::SnapshotReference::new(
                        self.snapshot_id,
                        self.retention.main_or_default(),
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

            // Invariant 3. The manifest is rebuilt under the spec it was
            // written under, not the table's current one: every surviving
            // entry's partition tuple was produced by that spec and means
            // nothing against any other.
            let spec = self.spec_for(loaded.file.partition_spec_id)?;
            let is_delete_manifest = loaded.file.content == ManifestContentType::Deletes;
            let output =
                self.new_manifest_output(if is_delete_manifest { "delete" } else { "data" })?;
            let builder =
                ManifestWriterBuilder::new(output, Some(self.snapshot_id), schema.clone(), spec);
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
            // Added files are written under the table's *current* spec, which
            // is the spec they were partitioned by — the rewrite refuses any
            // group that was not (see `crate::ops::compact`).
            let spec = metadata.default_partition_spec().as_ref().clone();
            let output = self.new_manifest_output("data")?;
            let builder = ManifestWriterBuilder::new(output, Some(self.snapshot_id), schema, spec);
            let mut writer = match format_version {
                FormatVersion::V1 => builder.build_v1(),
                FormatVersion::V2 => builder.build_v2_data(),
                FormatVersion::V3 => builder.build_v3_data(),
            };

            for file in added {
                // Invariant 2, the other half. A newly written file genuinely
                // belongs to this snapshot, so it takes the new sequence
                // number — which is higher than every delete file already
                // applied to its contents, and is therefore what retires them.
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

    /// The partition spec a manifest was written under.
    pub(crate) fn spec_for(&self, spec_id: i32) -> Result<PartitionSpec> {
        self.table
            .metadata()
            .partition_spec_by_id(spec_id)
            .map(|spec| spec.as_ref().clone())
            .ok_or_else(|| {
                Error::metadata(
                    self.table.identifier().to_string(),
                    format!(
                        "a manifest names partition spec {spec_id}, which the table metadata \
                         does not carry; rewriting it would re-interpret every partition value \
                         in it"
                    ),
                )
            })
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
    /// spec's own property names rather than something Bergman invented — and
    /// the totals are the running ones the spec asks for, not just the deltas.
    /// A summary with deltas but no totals makes Spark's `.snapshots` table
    /// show blanks where every other writer shows numbers.
    fn summary(&self, rewrite: &RewriteFiles, counts: &Counts) -> Summary {
        let added_bytes: u64 = rewrite.added.iter().map(|f| f.file_size_in_bytes()).sum();
        let added_records: u64 = rewrite.added.iter().map(|f| f.record_count()).sum();

        let mut p = std::collections::HashMap::new();
        let mut set = |key: &str, value: String| {
            p.insert(key.to_string(), value);
        };

        // Deltas. Data files and delete files are counted separately: lumping
        // a retired delete file in with `deleted-data-files` reports a rewrite
        // as having dropped more data than it did.
        set("added-data-files", rewrite.added.len().to_string());
        set("deleted-data-files", counts.removed_data.to_string());
        set("added-files-size", added_bytes.to_string());
        set("removed-files-size", counts.removed_bytes.to_string());
        set("added-records", added_records.to_string());
        set("deleted-records", counts.removed_records.to_string());
        if counts.removed_position_deletes > 0 {
            set(
                "removed-position-delete-files",
                counts.removed_position_deletes.to_string(),
            );
        }
        if counts.removed_equality_deletes > 0 {
            set(
                "removed-equality-delete-files",
                counts.removed_equality_deletes.to_string(),
            );
        }
        if counts.removed_delete_files() > 0 {
            set(
                "removed-delete-files",
                counts.removed_delete_files().to_string(),
            );
        }

        // Totals, as they stand after this snapshot.
        set(
            "total-data-files",
            (counts.surviving_data + rewrite.added.len()).to_string(),
        );
        set(
            "total-delete-files",
            counts.surviving_delete_files().to_string(),
        );
        set(
            "total-position-deletes",
            counts.surviving_position_delete_records.to_string(),
        );
        set(
            "total-equality-deletes",
            counts.surviving_equality_delete_records.to_string(),
        );
        set(
            "total-records",
            (counts.surviving_records + added_records).to_string(),
        );
        set(
            "total-files-size",
            (counts.surviving_bytes + added_bytes).to_string(),
        );

        // So an operator reading table history in Spark or Trino can see which
        // tool produced a snapshot, and why.
        set("engine-name", "bergman".to_string());
        set("engine-version", env!("CARGO_PKG_VERSION").to_string());

        Summary {
            // `replace`, not `overwrite`: the rows are unchanged and only their
            // physical layout differs. Engines use this to skip a rewrite
            // snapshot when computing incremental changes, so mislabelling it
            // would make a compaction look like a data change to every
            // downstream consumer.
            operation: Operation::Replace,
            additional_properties: p,
        }
    }
}

/// Whether a delete file's own sequence number proves it applies to nothing.
///
/// The second of the two derivations [`SnapshotProducer::dangling_delete_files`]
/// requires. `lowest` is the least sequence number a live data file carries in
/// each `(spec, partition)`, and `lowest_anywhere` the least across the table.
///
/// Conservative wherever the metadata is not conclusive: a delete file with no
/// sequence number of its own is kept, because an unknown number cannot be used
/// to argue that it is low enough to be dead — the same shape as orphan
/// removal's "unknown age means young".
fn is_dead_by_sequence(
    spec_id: i32,
    entry: &ManifestEntry,
    lowest: &HashMap<(i32, iceberg::spec::Struct), i64>,
    lowest_anywhere: Option<i64>,
) -> bool {
    let Some(sequence) = entry.sequence_number() else {
        return false;
    };
    let file = entry.data_file();

    // "Equality delete files stored with an unpartitioned spec are applied as
    // global deletes" — so an empty partition tuple means the file reaches
    // every data file in the table, and the comparison has to be against the
    // whole table's lowest rather than one partition's. Comparing it against
    // one partition's would call it dead while it was still hiding rows in
    // another.
    let floor = if file.content_type() == DataContentType::EqualityDeletes
        && file.partition().fields().is_empty()
    {
        lowest_anywhere
    } else {
        lowest.get(&(spec_id, file.partition().clone())).copied()
    };

    match floor {
        // No live data file it could ever apply to.
        None => true,
        // An equality delete applies to a data file when its number is strictly
        // greater; a positional one when it is greater or equal. Dead is the
        // negation of that, asked of the lowest number any live data file
        // carries — if it cannot reach the lowest, it reaches none of them.
        Some(floor) => match file.content_type() {
            DataContentType::EqualityDeletes => sequence <= floor,
            _ => sequence < floor,
        },
    }
}

/// What a rewrite removes and what survives it, counted once.
#[derive(Debug, Default)]
struct Counts {
    removed_data: usize,
    removed_position_deletes: usize,
    removed_equality_deletes: usize,
    removed_bytes: u64,
    removed_records: u64,
    surviving_data: usize,
    surviving_position_deletes: usize,
    surviving_equality_deletes: usize,
    surviving_position_delete_records: u64,
    surviving_equality_delete_records: u64,
    surviving_bytes: u64,
    surviving_records: u64,
}

impl Counts {
    fn of(existing: &[LoadedManifest], removed: &HashSet<String>, _added: &[DataFile]) -> Self {
        let mut counts = Self::default();
        for entry in existing.iter().flat_map(|m| m.entries.iter()) {
            let file = entry.data_file();
            let gone = removed.contains(&normalize(entry.file_path()));
            match (gone, file.content_type()) {
                (true, DataContentType::Data) => {
                    counts.removed_data += 1;
                    counts.removed_bytes += file.file_size_in_bytes();
                    counts.removed_records += file.record_count();
                }
                (true, DataContentType::PositionDeletes) => {
                    counts.removed_position_deletes += 1;
                    counts.removed_bytes += file.file_size_in_bytes();
                }
                (true, DataContentType::EqualityDeletes) => {
                    counts.removed_equality_deletes += 1;
                    counts.removed_bytes += file.file_size_in_bytes();
                }
                (false, DataContentType::Data) => {
                    counts.surviving_data += 1;
                    counts.surviving_bytes += file.file_size_in_bytes();
                    counts.surviving_records += file.record_count();
                }
                (false, DataContentType::PositionDeletes) => {
                    counts.surviving_position_deletes += 1;
                    counts.surviving_position_delete_records += file.record_count();
                    counts.surviving_bytes += file.file_size_in_bytes();
                }
                (false, DataContentType::EqualityDeletes) => {
                    counts.surviving_equality_deletes += 1;
                    counts.surviving_equality_delete_records += file.record_count();
                    counts.surviving_bytes += file.file_size_in_bytes();
                }
            }
        }
        counts
    }

    fn removed_delete_files(&self) -> usize {
        self.removed_position_deletes + self.removed_equality_deletes
    }

    fn surviving_delete_files(&self) -> usize {
        self.surviving_position_deletes + self.surviving_equality_deletes
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

    /// A delete-file manifest entry in `partition`, at `sequence`.
    fn delete_entry(
        content: DataContentType,
        sequence: i64,
        partition: iceberg::spec::Struct,
    ) -> ManifestEntry {
        use iceberg::spec::{DataFileBuilder, DataFileFormat};

        let data_file = DataFileBuilder::default()
            .content(content)
            .file_path(format!("s3://b/t/deletes/{sequence}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .record_count(1)
            .file_size_in_bytes(1)
            .equality_ids(Some(vec![1]))
            .build()
            .expect("delete file");

        ManifestEntry::builder()
            .status(ManifestStatus::Existing)
            .snapshot_id(1)
            .sequence_number(sequence)
            .file_sequence_number(sequence)
            .data_file(data_file)
            .build()
    }

    fn partitioned(value: i32) -> iceberg::spec::Struct {
        iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(value))])
    }

    #[test]
    fn a_delete_below_every_live_data_file_is_dead() {
        // The second derivation, and Java's. Every live data file in this
        // partition was written at sequence 10 or later, so a delete file at 4
        // cannot apply to any of them however an index reports it.
        let lowest = HashMap::from([((0, partitioned(1)), 10i64)]);

        for content in [
            DataContentType::EqualityDeletes,
            DataContentType::PositionDeletes,
        ] {
            assert!(
                is_dead_by_sequence(
                    0,
                    &delete_entry(content, 4, partitioned(1)),
                    &lowest,
                    Some(10)
                ),
                "{content:?} at 4 cannot reach a data file at 10"
            );
            assert!(
                !is_dead_by_sequence(
                    0,
                    &delete_entry(content, 12, partitioned(1)),
                    &lowest,
                    Some(10)
                ),
                "{content:?} at 12 still applies"
            );
        }
    }

    #[test]
    fn the_boundary_differs_by_delete_kind() {
        // An equality delete applies when its number is *strictly* greater; a
        // positional one when it is greater or equal. At exactly the lowest
        // number the two answers differ, and getting it backwards either leaks
        // a dead file forever or resurrects rows.
        let lowest = HashMap::from([((0, partitioned(1)), 10i64)]);

        assert!(
            is_dead_by_sequence(
                0,
                &delete_entry(DataContentType::EqualityDeletes, 10, partitioned(1)),
                &lowest,
                Some(10)
            ),
            "an equality delete at the lowest number applies to nothing"
        );
        assert!(
            !is_dead_by_sequence(
                0,
                &delete_entry(DataContentType::PositionDeletes, 10, partitioned(1)),
                &lowest,
                Some(10)
            ),
            "a positional delete at the lowest number still applies"
        );
    }

    #[test]
    fn a_partition_with_no_live_data_file_leaves_its_deletes_dead() {
        // Everything the delete file covered has been rewritten away. This is
        // the case the whole operation exists for.
        assert!(is_dead_by_sequence(
            0,
            &delete_entry(DataContentType::EqualityDeletes, 7, partitioned(1)),
            &HashMap::new(),
            None,
        ));
    }

    #[test]
    fn a_global_equality_delete_is_measured_against_the_whole_table() {
        // "Equality delete files stored with an unpartitioned spec are applied
        // as global deletes." Measuring one against a single partition's lowest
        // sequence number would call it dead while it was still hiding rows in
        // every other partition.
        let global = delete_entry(
            DataContentType::EqualityDeletes,
            7,
            iceberg::spec::Struct::empty(),
        );
        // This partition's files are all newer than the delete...
        let lowest = HashMap::from([((0, iceberg::spec::Struct::empty()), 20i64)]);

        assert!(
            !is_dead_by_sequence(0, &global, &lowest, Some(3)),
            "...but somewhere in the table a data file at 3 is still covered"
        );
        assert!(
            is_dead_by_sequence(0, &global, &lowest, Some(9)),
            "with the whole table newer than the delete, it reaches nothing"
        );
    }

    #[test]
    fn a_delete_with_no_sequence_number_is_kept() {
        // An unknown number cannot be used to argue that a file is old enough
        // to remove — the same rule orphan removal applies to an unknown
        // modification time, for the same reason.
        let mut entry = delete_entry(DataContentType::EqualityDeletes, 1, partitioned(1));
        entry.sequence_number = None;
        assert!(!is_dead_by_sequence(0, &entry, &HashMap::new(), None));
    }

    #[test]
    fn replace_is_the_right_operation_for_a_rewrite() {
        // `overwrite` would tell every downstream consumer the rows changed.
        // They did not — only their layout did.
        assert_eq!(Operation::Replace.as_str(), "replace");
    }

    #[test]
    fn a_table_with_no_configured_retention_gets_an_empty_branch_reference() {
        // The default has to be "nothing configured" rather than anything
        // invented: a retention Bergman made up would start expiring snapshots
        // on a schedule the table's owner never asked for.
        let retention = BranchRetention::default().main_or_default();
        assert!(matches!(
            retention,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            }
        ));
    }

    #[test]
    fn a_configured_retention_survives_the_commit_that_moves_the_branch() {
        // `set-snapshot-ref` replaces the whole reference, so a commit that
        // named no retention would silently erase what
        // `ALTER TABLE … CREATE BRANCH main RETAIN …` configured — visible only
        // the next time expiration ran.
        let retention = BranchRetention {
            main: Some(SnapshotRetention::Branch {
                min_snapshots_to_keep: Some(42),
                max_snapshot_age_ms: Some(86_400_000),
                max_ref_age_ms: None,
            }),
        };

        assert!(matches!(
            retention.main_or_default(),
            SnapshotRetention::Branch {
                min_snapshots_to_keep: Some(42),
                max_snapshot_age_ms: Some(86_400_000),
                ..
            }
        ));
    }

    #[test]
    fn refs_are_read_out_of_the_metadata_document() {
        // Upstream keeps `TableMetadata::refs` crate-private and derives no
        // `Serialize`, so this parse is the only way to see the field. It reads
        // the specified JSON shape, which is stable.
        #[derive(serde::Deserialize)]
        struct Refs {
            #[serde(default)]
            refs: HashMap<String, iceberg::spec::SnapshotReference>,
        }

        let document = serde_json::json!({
            "refs": {
                "main": {
                    "snapshot-id": 7,
                    "type": "branch",
                    "min-snapshots-to-keep": 5,
                    "max-snapshot-age-ms": 3600000
                }
            }
        });

        let parsed: Refs = serde_json::from_value(document).unwrap();
        assert!(matches!(
            parsed.refs["main"].retention,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: Some(5),
                max_snapshot_age_ms: Some(3_600_000),
                ..
            }
        ));
    }

    #[test]
    fn a_metadata_document_without_refs_yields_no_retention() {
        #[derive(serde::Deserialize)]
        struct Refs {
            #[serde(default)]
            refs: HashMap<String, iceberg::spec::SnapshotReference>,
        }
        let parsed: Refs = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(parsed.refs.is_empty());
    }
}
