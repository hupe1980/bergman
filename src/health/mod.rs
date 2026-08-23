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

pub use analyze::analyze;
pub use partition::{PartitionHealth, PartitionKey};

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::policy::TableRef;

/// Everything Bergman knows about one table's condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableHealth {
    /// Which table this describes.
    pub table: TableRef,
    /// The table's format version (1, 2 or 3).
    pub format_version: u8,
    /// The table's base location, which bounds every file it may own.
    pub location: String,
    /// Snapshot and history condition.
    pub snapshots: SnapshotHealth,
    /// Manifest condition.
    pub manifests: ManifestHealth,
    /// Data and delete file condition, across the whole table.
    pub files: FileHealth,
    /// The same, split by partition — the granularity compaction works at.
    pub partitions: Vec<PartitionHealth>,
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
