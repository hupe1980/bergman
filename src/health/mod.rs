//! Table health: what is actually wrong with a table, measured from metadata.
//!
//! This module reads manifests and nothing else. No data file is opened, no
//! Parquet footer is parsed — every number here comes from the manifest entries
//! Iceberg already maintains. That is what makes it cheap enough to run against
//! thousands of tables on every cycle, which in turn is what makes *triggered*
//! maintenance possible: a healthy table costs a handful of metadata reads and
//! no data I/O at all.
//!
//! The analyzer answers questions, it does not make decisions. Whether a table
//! is worth rewriting is [`crate::plan`]'s judgement, made by comparing these
//! numbers against a resolved policy.

mod analyze;
mod partition;

pub use analyze::{FileIndex, analyze, file_index};
pub use partition::{Eligible, PartitionHealth, PartitionKey, UNPARTITIONED, partition_path};

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::policy::TableRef;

/// Everything Bergman knows about one table's condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableHealth {
    /// Which table this describes.
    pub table: TableRef,
    /// The table's Iceberg format version.
    ///
    /// Serializes as the integer the spec uses (1, 2, 3) — it is upstream's own
    /// `repr(u8)` type rather than a plain number so that the planner can ask
    /// [`crate::commit::authoring_refusal`] about it directly, instead of
    /// carrying a second mapping from integer to meaning.
    pub format_version: iceberg::spec::FormatVersion,
    /// The table's base location, which bounds every file it may own.
    pub location: String,
    /// The table's `write.format.default`, lower-cased, when it sets one.
    ///
    /// Carried on the health report so the planner can say *before* a cycle that
    /// compaction will never run against this table. Bergman handles Parquet and
    /// only Parquet, in both directions — upstream's `ArrowReader` opens a
    /// Parquet stream whatever a scan task's format field says — and a rewrite
    /// must not silently change a table's format, so a table asking for anything
    /// else is refused. A refusal reported every cycle is noise where an
    /// explanation is wanted, which is why this reaches the plan as a note.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_format: Option<String>,
    /// Whether the table asks for the object-storage file layout
    /// (`write.object-storage.enabled`).
    ///
    /// Iceberg inserts a deterministic hash into a data file's path when this is
    /// on, so that a hot table's objects spread across S3 prefixes instead of
    /// concentrating in one and collecting 503s. Bergman's writer uses upstream's
    /// `DefaultLocationGenerator`, which does not — and the released crate offers
    /// no generator that does — so a rewrite quietly moves files out of the
    /// layout the table asked for.
    ///
    /// Carried here so the planner can *say so* rather than leave it silent. It
    /// is not a refusal: the table still reads correctly, every path is recorded
    /// in the metadata, and refusing to compact a table over its prefix layout
    /// would be worse than the deviation.
    #[serde(default)]
    pub object_storage_layout: bool,
    /// The partition spec new files are written under.
    ///
    /// A table whose spec has evolved holds files under several at once, and
    /// only the current one can be rewritten: output goes out under this spec,
    /// so a commit replacing files partitioned differently would mis-file them.
    #[serde(default)]
    pub current_spec_id: i32,
    /// Snapshot and history condition.
    pub snapshots: SnapshotHealth,
    /// Manifest condition.
    pub manifests: ManifestHealth,
    /// Data and delete file condition, across the whole table.
    pub files: FileHealth,
    /// The same, split by partition — the granularity compaction works at.
    pub partitions: Vec<PartitionHealth>,
    /// Why this table's manifests could not be read, if they could not.
    ///
    /// When set, every figure derived from a manifest — [`Self::files`],
    /// [`Self::manifests`] and [`Self::partitions`] — is **absent rather than
    /// zero**, and every operation that needs one is refused by name rather
    /// than left to fail on an Avro parse error every cycle forever.
    /// [`Self::snapshots`] and the metadata log stay exact, because they come
    /// from `metadata.json` rather than from a manifest.
    ///
    /// Distinguishing "unreadable" from "empty" is the whole point: a table
    /// with no files and a table Bergman cannot read produce the same zeroes,
    /// and one of them is healthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreadable: Option<String>,
}

/// Why Bergman cannot read this table's manifests, if it cannot.
///
/// Today there is one answer, and it is **encryption**. Iceberg native
/// encryption went GA in Java 1.11.0: a table declaring `encryption.key-id`
/// has its data files, its manifests *and its manifest lists* encrypted, with
/// v3 metadata carrying the wrapped keys in `encryption-keys`. Bergman opens
/// manifests with `ManifestList::parse_with_version` and
/// `ManifestFile::load_manifest`, neither of which decrypts anything.
///
/// **This is an operability refusal, not a safety one, and the difference is
/// worth stating precisely.** Every manifest walk in Bergman is all-or-nothing
/// — `reachability::compute` and the analyzer both `try_collect`, so one
/// unreadable manifest aborts the whole computation rather than returning a
/// short set. Nothing is ever deleted on the strength of a partial reading.
/// What actually happens without this check is an opaque Avro parse failure,
/// re-attempted every cycle, with `bergman plan` unable to say why. That is a
/// violation of "nothing silent" rather than a data-loss path.
///
/// Detected from metadata Bergman already holds, so it costs nothing and needs
/// no upgrade. Supporting such tables — rather than refusing them — becomes
/// possible on `iceberg` 0.11.0, which reads and writes encrypted Puffin,
/// Parquet, manifests and manifest lists.
pub fn unreadable_reason(metadata: &iceberg::spec::TableMetadata) -> Option<String> {
    let declares_key = metadata
        .properties()
        .get("encryption.key-id")
        .is_some_and(|v| !v.trim().is_empty());

    if declares_key || metadata.encryption_keys_iter().len() > 0 {
        return Some(
            "the table uses Iceberg native encryption (encryption.key-id), so its manifests \
             and manifest lists are encrypted and Bergman cannot read them; only operations \
             that need no manifest can run"
                .to_string(),
        );
    }
    None
}

/// Snapshot and history condition.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotHealth {
    /// How many snapshots the table retains.
    pub count: usize,
    /// The current snapshot, if the table has ever been written to.
    pub current_snapshot_id: Option<i64>,
    /// Age of the oldest retained snapshot.
    #[serde(with = "humantime_serde::option")]
    pub oldest_age: Option<Duration>,
    /// Whether the table carries a `main` branch reference.
    ///
    /// A *count* of branches and tags belongs here — each carries its own
    /// retention, and a snapshot reachable from any retained ref never expires
    /// however old it is, which is the usual answer to "why did nothing
    /// expire?". Upstream makes that uncountable: `TableMetadata::refs` is
    /// `pub(crate)`, the only accessor is `snapshot_for_ref(name)` which
    /// requires knowing the name, and the type derives `Deserialize` but not
    /// `Serialize`, so there is no way round it either. So this reports the one
    /// ref that can be asked for by name.
    pub has_main_branch: bool,
    /// How many entries the metadata log holds (previous `metadata.json`
    /// files), which grows on every commit and is cleaned separately.
    pub metadata_log_count: usize,
}

/// Manifest condition.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestHealth {
    /// Manifests referenced by the current snapshot.
    pub count: usize,
    /// Total size of those manifests.
    pub bytes: u64,
    /// How many are below the target size.
    ///
    /// Fragmented manifests slow down every query's planning phase, and the fix
    /// is pure metadata — which makes it the cheapest real win available.
    pub undersized_count: usize,
    /// How many carry delete-file entries rather than data-file entries.
    pub delete_manifest_count: usize,
}

/// Data and delete file condition.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileHealth {
    /// Live data files.
    pub data_file_count: usize,
    /// Their total size.
    pub data_bytes: u64,
    /// Their total row count.
    pub record_count: u64,
    /// Live positional delete files.
    pub position_delete_count: usize,
    /// Live equality delete files.
    ///
    /// Tracked separately because they cost far more to apply: a positional
    /// delete names a row by file and offset, while an equality delete must be
    /// joined against the data. A table with many of these is the case that
    /// makes Spark run out of memory.
    pub equality_delete_count: usize,
    /// Total size of all delete files.
    pub delete_bytes: u64,
    /// Rows named by delete files.
    ///
    /// An upper bound on rows actually removed, not an exact count: the same
    /// row may be named by more than one delete file, and Iceberg's metadata
    /// does not say whether it was.
    pub delete_record_count: u64,
    /// Sizes of live data files, ascending — the input to every percentile.
    pub file_sizes: Vec<u64>,
}

impl FileHealth {
    /// Fraction of live data files smaller than `threshold`.
    ///
    /// Returns 0 for an empty table: there is nothing to compact, and a ratio
    /// of `NaN` would compare false against every trigger and silently do the
    /// right thing for the wrong reason.
    pub fn small_file_ratio(&self, threshold: u64) -> f64 {
        if self.file_sizes.is_empty() {
            return 0.0;
        }
        let small = self.file_sizes.iter().filter(|&&s| s < threshold).count();
        small as f64 / self.file_sizes.len() as f64
    }

    /// How many live data files are smaller than `threshold`.
    pub fn small_file_count(&self, threshold: u64) -> usize {
        self.file_sizes.iter().filter(|&&s| s < threshold).count()
    }

    /// Rows named by delete files, as a fraction of live rows.
    pub fn delete_ratio(&self) -> f64 {
        if self.record_count == 0 {
            return 0.0;
        }
        self.delete_record_count as f64 / self.record_count as f64
    }

    /// Mean live data file size.
    pub fn average_file_size(&self) -> u64 {
        if self.data_file_count == 0 {
            return 0;
        }
        self.data_bytes / self.data_file_count as u64
    }

    /// The size at the given percentile of live data files.
    ///
    /// `file_sizes` is kept sorted by the analyzer, so this is a lookup. The
    /// percentile is nearest-rank, which needs no interpolation and therefore
    /// always returns a size some file actually has — the right property for a
    /// number an operator will compare against a target file size.
    pub fn percentile(&self, p: f64) -> u64 {
        if self.file_sizes.is_empty() {
            return 0;
        }
        let p = p.clamp(0.0, 1.0);
        let rank = (p * (self.file_sizes.len() - 1) as f64).round() as usize;
        self.file_sizes[rank]
    }
}

impl TableHealth {
    /// Whether the table has ever been written to.
    ///
    /// An empty table is not unhealthy, it is empty — and every operation
    /// should decline rather than fail on it.
    pub fn is_empty(&self) -> bool {
        self.snapshots.current_snapshot_id.is_none()
    }

    /// A one-line summary, for the table view.
    pub fn summary(&self) -> String {
        if self.is_empty() {
            return "empty".to_string();
        }
        format!(
            "{} files / {} / {} snapshots / {} manifests",
            self.files.data_file_count,
            crate::util::human_bytes(self.files.data_bytes),
            self.snapshots.count,
            self.manifests.count,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health(sizes: &[u64], records: u64, deletes: u64) -> FileHealth {
        let mut file_sizes = sizes.to_vec();
        file_sizes.sort_unstable();
        FileHealth {
            data_file_count: sizes.len(),
            data_bytes: sizes.iter().sum(),
            record_count: records,
            delete_record_count: deletes,
            file_sizes,
            ..Default::default()
        }
    }

    #[test]
    fn empty_tables_have_no_ratios_rather_than_nan() {
        // A NaN ratio compares false against every threshold, so the table
        // would be skipped for a reason nobody could see in the output.
        let empty = FileHealth::default();
        assert_eq!(empty.small_file_ratio(1000), 0.0);
        assert_eq!(empty.delete_ratio(), 0.0);
        assert_eq!(empty.average_file_size(), 0);
        assert_eq!(empty.percentile(0.5), 0);
    }

    #[test]
    fn small_file_ratio_counts_strictly_below_the_threshold() {
        let h = health(&[100, 100, 100, 1000], 0, 0);
        assert_eq!(h.small_file_ratio(1000), 0.75);
        assert_eq!(h.small_file_count(1000), 3);
        // A file exactly at target is not small; otherwise a perfectly
        // compacted table would compact itself forever.
        assert_eq!(h.small_file_ratio(100), 0.0);
    }

    #[test]
    fn delete_ratio_is_deletes_over_live_rows() {
        let h = health(&[100], 1000, 250);
        assert_eq!(h.delete_ratio(), 0.25);
    }

    #[test]
    fn percentile_returns_a_size_a_file_actually_has() {
        let h = health(&[10, 20, 30, 40, 50], 0, 0);
        assert_eq!(h.percentile(0.0), 10);
        assert_eq!(h.percentile(0.5), 30);
        assert_eq!(h.percentile(1.0), 50);
        // Out-of-range percentiles clamp rather than panic on the index.
        assert_eq!(h.percentile(1.5), 50);
        assert_eq!(h.percentile(-0.5), 10);
    }
}

#[cfg(test)]
mod readability_tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };

    use super::*;

    fn metadata(properties: HashMap<String, String>) -> iceberg::spec::TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "file:///tmp/t".to_string(),
            FormatVersion::V2,
            properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn an_ordinary_table_is_readable() {
        // The control. Without it, the test below passes just as happily
        // against a detector that refuses everything — which is the failure
        // mode a guarded-only test has, and the reason the orphan suite is
        // written in pairs.
        assert!(unreadable_reason(&metadata(HashMap::new())).is_none());

        // And a table that merely mentions encryption in an unrelated property
        // is still readable: the check must key on the spec's own property, not
        // on a substring.
        let mut properties = HashMap::new();
        properties.insert("comment".into(), "encryption.key-id is not set".into());
        assert!(unreadable_reason(&metadata(properties)).is_none());
    }

    #[test]
    fn a_table_declaring_an_encryption_key_is_refused_by_name() {
        let mut properties = HashMap::new();
        properties.insert("encryption.key-id".into(), "kms-key-1".into());

        let reason = unreadable_reason(&metadata(properties)).expect("must be refused");
        assert!(
            reason.contains("encryption.key-id"),
            "the reason must name what to look at: {reason}"
        );
    }

    #[test]
    fn an_empty_encryption_key_is_not_a_declaration() {
        // A property set to the empty string is a configuration mistake, not an
        // encrypted table. Refusing it would take a readable table out of
        // maintenance for a typo.
        let mut properties = HashMap::new();
        properties.insert("encryption.key-id".into(), "   ".into());
        assert!(unreadable_reason(&metadata(properties)).is_none());
    }
}
