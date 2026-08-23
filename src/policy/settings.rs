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

    /// Sort output by these columns.
    ///
    /// A *global* sort within each partition, so output files carry tight
    /// min/max bounds and a query with a predicate on these columns can skip
    /// whole files.
    #[serde(default)]
    pub sort: Option<Vec<String>>,

    /// How much memory one partition's sort may use.
    ///
    /// Sorting needs the whole file group in hand, and Bergman does not spill
    /// to disk. A partition larger than this is refused with a named reason
    /// rather than written unsorted, because a table whose metadata says
    /// "sorted" and whose files are not is worse than one that failed loudly.
    #[serde(default)]
    pub max_sort_memory: Option<u64>,
}

impl CompactionSettings {
    fn validate(&self, where_: &str) -> Result<()> {
        if let Some(0) = self.target_file_size {
            return Err(Error::policy(format!(
                "{where_}: compaction.target_file_size must be greater than zero"
            )));
        }
        if let Some(0) = self.max_sort_memory {
            return Err(Error::policy(format!(
                "{where_}: compaction.max_sort_memory must be greater than zero; \
                 omit `sort` instead of setting a budget nothing can fit in"
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
