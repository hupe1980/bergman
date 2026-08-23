//! Setting types.
//!
//! Every field is an [`Option`], because absence is meaningful: it is what
//! hands the decision to the next layer down (the table's own properties, then
//! the Iceberg default). A struct of concrete values could not express "I did
//! not say", and "I did not say" is most of what a policy file contains.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::policy::MIN_ORPHAN_AGE;

/// Settings for one table, at one layer of the resolution.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSettings {
    /// Small-file and delete-file compaction.
    #[serde(default)]
    pub compaction: Option<CompactionSettings>,
    /// Snapshot expiration.
    #[serde(default)]
    pub snapshots: Option<SnapshotSettings>,
    /// Manifest rewriting.
    #[serde(default)]
    pub manifests: Option<ManifestSettings>,
    /// Orphan-file removal.
    #[serde(default)]
    pub orphans: Option<OrphanSettings>,
    /// When evaluation runs for these tables, as a cron expression.
    ///
    /// This schedules *evaluation*, not execution. Whether anything is rewritten
    /// is the health analyzer's decision.
    #[serde(default)]
    pub schedule: Option<String>,
}

impl TableSettings {
    /// Whether this layer says anything at all.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Validate the settings, naming where they came from.
    pub(crate) fn validate(&self, where_: &str) -> Result<()> {
        if let Some(orphans) = &self.orphans {
            orphans.validate(where_)?;
        }
        if let Some(compaction) = &self.compaction {
            compaction.validate(where_)?;
        }
        if let Some(snapshots) = &self.snapshots {
            snapshots.validate(where_)?;
        }
        if let Some(schedule) = &self.schedule {
            crate::policy::parse_schedule(schedule)
                .map_err(|e| Error::policy(format!("{where_}: {e}")))?;
        }
        Ok(())
    }
}

/// Compaction settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionSettings {
    /// Whether compaction may run at all.
    #[serde(default)]
    pub enabled: Option<bool>,

    /// The size output files are rolled at.
    ///
    /// Falls back to the table's `write.target-file-size-bytes`, then to the
    /// Iceberg default of 512 MiB.
    #[serde(default)]
    pub target_file_size: Option<u64>,

    /// What makes a partition worth rewriting.
    #[serde(default)]
    pub trigger: Option<CompactionTrigger>,

    /// Sort output by these columns, ascending, nulls first.
    ///
    /// A *global* sort within each file group, so output files carry tight
    /// min/max bounds and a query with a predicate on these columns can skip
    /// whole files.
    ///
    /// Deliberately just names. Direction and null placement belong to the
    /// table's own `sort-order`, which every Iceberg tool reads and which
    /// Bergman honours when a rule says nothing (see
    /// [`crate::policy::SortColumn`]) — a second place to express the same
    /// thing would be a second source of truth for the physical layout of a
    /// table, which is exactly what layering exists to avoid.
    #[serde(default)]
    pub sort: Option<Vec<String>>,

    /// How much memory one file group's rewrite may use.
    ///
    /// A real budget rather than an estimate: it becomes the executor's memory
    /// pool, and the two operators that can want more than a batch at a time —
    /// the sort, and the anti-join that applies equality deletes — **spill to
    /// disk** when they reach it rather than failing.
    #[serde(default)]
    pub max_sort_memory: Option<u64>,

    /// Most bytes one file group may read.
    ///
    /// A partition is not a unit of work. Bergman bin-packs a partition's
    /// eligible files into groups bounded by this and by
    /// [`CompactionSettings::max_input_files`], and each group commits on its
    /// own — so one conflict costs one group rather than a partition, and a
    /// partition larger than memory is still compactable.
    ///
    /// The equivalent of Spark's `max-file-group-size-bytes`.
    #[serde(default)]
    pub max_group_bytes: Option<u64>,

    /// Most files one file group may read.
    ///
    /// The other shape of the same problem: a hundred thousand one-kilobyte
    /// files fit any byte ceiling and still overwhelm a single pass.
    #[serde(default)]
    pub max_input_files: Option<usize>,
}

impl CompactionSettings {
    fn validate(&self, where_: &str) -> Result<()> {
        for (name, value) in [
            ("target_file_size", self.target_file_size),
            ("max_sort_memory", self.max_sort_memory),
            ("max_group_bytes", self.max_group_bytes),
        ] {
            if let Some(0) = value {
                return Err(Error::policy(format!(
                    "{where_}: compaction.{name} must be greater than zero"
                )));
            }
        }
        if let Some(1) = self.max_input_files {
            return Err(Error::policy(format!(
                "{where_}: compaction.max_input_files must be at least 2; \
                 a group of one file would be read and written back unchanged"
            )));
        }
        if self.sort.as_ref().is_some_and(|s| s.is_empty()) {
            return Err(Error::policy(format!(
                "{where_}: compaction.sort is an empty list; omit it to leave output unsorted"
            )));
        }
        if let Some(trigger) = &self.trigger {
            trigger.validate(where_)?;
        }
        Ok(())
    }
}

/// What makes a partition worth rewriting.
///
/// Compaction is triggered, not scheduled. A table already at target file size
/// costs one metadata read per cycle and no data I/O — which is what makes it
/// safe to evaluate thousands of tables often.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionTrigger {
    /// Rewrite when this fraction of a partition's files are below the small-file
    /// threshold (which is `min_file_size_ratio` of the target size).
    #[serde(default)]
    pub small_file_ratio: Option<f64>,

    /// Never rewrite a group of fewer than this many files.
    ///
    /// Mirrors Spark's `rewrite_data_files` default of 5, so behaviour is
    /// unsurprising to operators who already run Iceberg maintenance.
    #[serde(default)]
    pub min_input_files: Option<usize>,

    /// Rewrite when delete records applicable to a file group exceed this
    /// fraction of its rows, even if file sizes are fine.
    ///
    /// This is the trigger that matters for streaming and CDC targets, where
    /// read amplification comes from delete files rather than from small ones.
    #[serde(default)]
    pub delete_ratio: Option<f64>,

    /// What counts as "small", as a fraction of the target file size.
    #[serde(default)]
    pub min_file_size_ratio: Option<f64>,

    /// Leave a partition alone until its newest file is at least this old.
    ///
    /// The guard against fighting the writer for the hot partition. A streaming
    /// target commits to one partition continuously, and a rewrite of that
    /// partition loses its compare-and-swap to the very next micro-batch —
    /// spending a full read and write of the data to achieve nothing, over and
    /// over. Waiting for the partition to settle is strictly cheaper than
    /// competing with the writer for it.
    ///
    /// A partition whose files carry no timestamp counts as settled: failing
    /// the other way would leave such a table un-maintained forever.
    #[serde(default, with = "humantime_serde::option")]
    pub min_file_age: Option<Duration>,
}

impl CompactionTrigger {
    fn validate(&self, where_: &str) -> Result<()> {
        for (name, value) in [
            ("small_file_ratio", self.small_file_ratio),
            ("delete_ratio", self.delete_ratio),
            ("min_file_size_ratio", self.min_file_size_ratio),
        ] {
            if let Some(v) = value
                && !(0.0..=1.0).contains(&v)
            {
                return Err(Error::policy(format!(
                    "{where_}: compaction.trigger.{name} must be between 0 and 1, got {v}"
                )));
            }
        }
        Ok(())
    }
}

/// Snapshot expiration settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSettings {
    /// Whether expiration may run.
    #[serde(default)]
    pub enabled: Option<bool>,

    /// Expire snapshots older than this.
    ///
    /// Falls back to the table's `history.expire.max-snapshot-age-ms`, then to
    /// the Iceberg default of 5 days.
    #[serde(default, with = "humantime_serde::option")]
    pub max_age: Option<Duration>,

    /// Keep at least this many snapshots on every branch, regardless of age.
    #[serde(default)]
    pub min_to_keep: Option<usize>,

    /// Delete the data and metadata files that expiration orphans.
    ///
    /// Upstream's `ExpireSnapshotsAction` rewrites metadata only and documents
    /// physical cleanup as a higher-level responsibility — this is that
    /// responsibility. Off by default: with it off, expiration is a pure
    /// metadata operation and the orphan scanner reclaims the files later,
    /// which is the single-deleter design (see `crate::ops::orphans`).
    #[serde(default)]
    pub delete_files: Option<bool>,
}

impl SnapshotSettings {
    fn validate(&self, where_: &str) -> Result<()> {
        // Upstream rejects `retain_last == 0` at commit time. Catching it here
        // turns a run-time commit failure into a startup failure.
        if let Some(0) = self.min_to_keep {
            return Err(Error::policy(format!(
                "{where_}: snapshots.min_to_keep must be at least 1; \
                 expiring every snapshot would leave the table unreadable"
            )));
        }
        Ok(())
    }
}

/// Manifest rewrite settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSettings {
    /// Whether manifest rewriting may run.
    #[serde(default)]
    pub rewrite: Option<bool>,

    /// Coalesce manifests toward this size.
    ///
    /// Falls back to the table's `commit.manifest.target-size-bytes`, then to
    /// the Iceberg default of 8 MiB.
    #[serde(default)]
    pub target_size: Option<u64>,

    /// Only rewrite when at least this many manifests are below target.
    #[serde(default)]
    pub min_count_to_merge: Option<usize>,
}

/// What orphan-file removal is allowed to do.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrphanMode {
    /// Report orphans; delete nothing.
    ///
    /// The default, and deliberately so: this is the one operation that can
    /// destroy live data if the reachability set is wrong, so deleting is
    /// something an operator opts into after reading a dry run.
    #[default]
    DryRun,
    /// Delete the orphans that survive every safety check.
    Delete,
}

/// Orphan-file removal settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrphanSettings {
    /// Whether the scanner runs at all.
    #[serde(default)]
    pub enabled: Option<bool>,

    /// Report or delete.
    #[serde(default)]
    pub mode: Option<OrphanMode>,

    /// Only consider files older than this.
    ///
    /// Bounded below by [`MIN_ORPHAN_AGE`], which cannot be configured away.
    #[serde(default, with = "humantime_serde::option")]
    pub older_than: Option<Duration>,

    /// Do not scan a table more often than this.
    ///
    /// Unlike every other operation, orphan removal cannot be triggered from
    /// metadata: the only way to know whether a table has orphans is to list
    /// its whole location, and that listing *is* the expensive part. Running it
    /// on every cycle would make a five-minute cadence mean a full object-store
    /// listing of every table every five minutes — which costs real money on S3
    /// and finds nothing almost every time.
    ///
    /// So this operation is the one that is scheduled rather than triggered.
    #[serde(default, with = "humantime_serde::option")]
    pub min_interval: Option<Duration>,
}

impl OrphanSettings {
    fn validate(&self, where_: &str) -> Result<()> {
        if let Some(age) = self.older_than
            && age < MIN_ORPHAN_AGE
        {
            return Err(Error::policy(format!(
                "{where_}: orphans.older_than is {}s, below the {}s floor. \
                 Writers stage files before the commit that references them, so a young \
                 unreferenced file is more likely a live write than garbage.",
                age.as_secs(),
                MIN_ORPHAN_AGE.as_secs()
            )));
        }
        if let Some(interval) = self.min_interval
            && interval.is_zero()
        {
            return Err(Error::policy(format!(
                "{where_}: orphans.min_interval is 0, which lists the whole table location \
                 on every cycle; that listing is the entire cost of orphan removal and it \
                 finds nothing almost every time"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Config;

    #[test]
    fn orphan_age_below_the_floor_is_refused() {
        let config = Config::from_toml(
            r#"
            [[rules]]
            match = "prod.*"
            [rules.orphans]
            older_than = "1h"
            "#,
        )
        .unwrap();
        let err = crate::policy::Policy::compile(&config).unwrap_err();
        assert!(err.to_string().contains("below the"), "got: {err}");
    }

    #[test]
    fn orphan_mode_defaults_to_dry_run() {
        // The default has to be the safe one: an operator who forgets to say
        // gets a report, not a deletion.
        assert_eq!(OrphanMode::default(), OrphanMode::DryRun);
    }

    #[test]
    fn durations_parse_as_humantime() {
        let config = Config::from_toml(
            r#"
            [defaults.snapshots]
            max_age = "7d"
            "#,
        )
        .unwrap();
        assert_eq!(
            config.defaults.snapshots.unwrap().max_age,
            Some(Duration::from_secs(7 * 86400))
        );
    }

    #[test]
    fn ratios_outside_zero_to_one_are_refused() {
        let config = Config::from_toml(
            r#"
            [[rules]]
            match = "prod.*"
            [rules.compaction.trigger]
            delete_ratio = 1.5
            "#,
        )
        .unwrap();
        let err = crate::policy::Policy::compile(&config).unwrap_err();
        assert!(err.to_string().contains("between 0 and 1"), "got: {err}");
    }

    #[test]
    fn min_to_keep_zero_is_refused_at_compile_time() {
        // Upstream fails this at commit. Failing at startup instead means the
        // operator learns before a cycle runs.
        let config = Config::from_toml(
            r#"
            [defaults.snapshots]
            min_to_keep = 0
            "#,
        )
        .unwrap();
        let err = crate::policy::Policy::compile(&config).unwrap_err();
        assert!(err.to_string().contains("at least 1"), "got: {err}");
    }
}
