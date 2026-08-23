//! Plans: what maintenance would do, decided before anything is done.
//!
//! Planning is pure. It reads metadata, compares it against a resolved policy,
//! and produces a description — no file is written, no snapshot is committed,
//! nothing is deleted. `bergman plan` and `bergman run` build the identical
//! plan through this code; `run` then executes it. That is the auditability
//! contract: what you were shown is what happens.
//!
//! Every operation carries the *reason* it was planned — the measurement that
//! crossed the threshold, and the threshold it crossed. An operation an
//! operator cannot interrogate is an operation they cannot trust with their
//! data.

mod planner;
mod report;

pub use planner::plan_table;
pub use report::{OperationOutcome, OperationResult, RunReport, TableOutcome};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::health::TableHealth;
use crate::policy::{EffectivePolicy, TableRef};

/// A complete maintenance plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenancePlan {
    /// When the plan was built. Every table plan is relative to the table state
    /// at this moment, and the world moves on afterwards — which is why
    /// execution re-validates rather than trusting the plan (see
    /// [`crate::ops`]).
    pub generated_at: DateTime<Utc>,
    /// Tables with something to do.
    pub tables: Vec<TablePlan>,
    /// Tables examined and found healthy, or excluded.
    pub uneventful: Vec<Uneventful>,
    /// Tables the byte budget could not cover this cycle.
    ///
    /// Carried on the plan rather than logged and forgotten: a run that quietly
    /// maintained a subset would look exactly like one that maintained
    /// everything.
    #[serde(default)]
    pub deferred: Vec<TableRef>,
}

/// A table with nothing to do, and why.
///
/// Reported rather than dropped: "my rule does not match what I thought" and
/// "this table is fine" look identical in a plan that lists only work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Uneventful {
    /// Which table.
    pub table: TableRef,
    /// Why nothing is planned for it.
    pub reason: UneventfulReason,
}

/// Why a table has no work planned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
pub enum UneventfulReason {
    /// No rule matched it.
    Unmatched,
    /// A rule matched and excluded it.
    Skipped {
        /// The pattern that excluded it.
        pattern: String,
    },
    /// Policy matched, but no trigger fired.
    Healthy,
    /// The table has never been written to.
    Empty,
    /// The table could not be examined.
    ///
    /// One unreadable table does not stop a cycle: the rest are maintained and
    /// this is reported. A run that aborted on the first permission error would
    /// be a run that never completes in a large deployment.
    Failed {
        /// What went wrong.
        error: String,
    },
}

/// What maintenance would do to one table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePlan {
    /// Which table.
    pub table: TableRef,
    /// Its measured condition.
    pub health: TableHealth,
    /// The policy in force, with each value's provenance.
    pub policy: Box<EffectivePolicy>,
    /// The operations to perform, in execution order.
    pub operations: Vec<Operation>,
}

impl TablePlan {
    /// Operations that will actually run.
    pub fn executable(&self) -> impl Iterator<Item = &Operation> {
        self.operations.iter().filter(|op| op.is_executable())
    }

    /// Operations that were planned but cannot run in this build.
    pub fn blocked(&self) -> impl Iterator<Item = &Operation> {
        self.operations.iter().filter(|op| !op.is_executable())
    }
}

/// One maintenance operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// What kind of work this is.
    pub kind: OperationKind,
    /// Why it was planned: the measurement, and the threshold it crossed.
    pub reason: String,
    /// The partitions this operation acts on, where it is partition-grained.
    ///
    /// Carried on the plan rather than recomputed at execution time, so that
    /// `run` acts on exactly what `plan` displayed. Empty for table-wide
    /// operations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<crate::health::PartitionKey>,
    /// What it is expected to change.
    pub estimate: Estimate,
    /// Whether Bergman can execute it, and if not, why not.
    pub executability: Executability,
}

impl Operation {
    /// Whether this operation will run.
    pub fn is_executable(&self) -> bool {
        matches!(self.executability, Executability::Executable)
    }
}

/// The kinds of maintenance Bergman performs.
///
/// Order matters and is enforced per table per cycle: compacting first lets
/// expiration reclaim the superseded small files, and expiring before the
/// orphan scan shrinks the reachable set legitimately rather than leaving
/// garbage that the scanner would have to be trusted to delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationKind {
    /// Rewrite small files and apply delete files.
    Compact,
    /// Coalesce fragmented manifests.
    RewriteManifests,
    /// Remove snapshots beyond their retention.
    ExpireSnapshots,
    /// Delete files no retained snapshot references.
    RemoveOrphans,
}

impl OperationKind {
    /// The operation's name, as it appears in output and audit records.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::RewriteManifests => "rewrite-manifests",
            Self::ExpireSnapshots => "expire-snapshots",
            Self::RemoveOrphans => "remove-orphans",
        }
    }
}

impl std::fmt::Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether an operation can run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum Executability {
    /// Bergman will perform it.
    Executable,
    /// Bergman planned it but cannot perform it.
    ///
    /// This is not a failure and not a silent omission — it is the honest
    /// state of an operation whose commit path does not exist upstream yet.
    /// Reporting it keeps the plan a true description of the table's needs
    /// while being clear about what will actually happen.
    Blocked {
        /// Why it cannot run, in a sentence an operator can act on.
        reason: String,
    },
}

impl Executability {
    /// Construct a [`Executability::Blocked`].
    pub fn blocked(reason: impl Into<String>) -> Self {
        Self::Blocked {
            reason: reason.into(),
        }
    }
}

/// What an operation is expected to change.
///
/// Estimates, and named as such. Exact figures for a rewrite are not knowable
/// without reading the data, and a plan that presented a guess as a measurement
/// would be worse than one that admitted the difference.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Estimate {
    /// Files the operation reads or removes.
    pub input_files: usize,
    /// Bytes it reads or reclaims.
    pub input_bytes: u64,
    /// Files it produces.
    pub output_files: usize,
    /// Snapshots it removes.
    pub snapshots_removed: usize,
}

impl MaintenancePlan {
    /// Total operations across every table.
    pub fn operation_count(&self) -> usize {
        self.tables.iter().map(|t| t.operations.len()).sum()
    }

    /// Operations that will actually run.
    pub fn executable_count(&self) -> usize {
        self.tables.iter().map(|t| t.executable().count()).sum()
    }

    /// Bytes every planned rewrite would read.
    pub fn total_input_bytes(&self) -> u64 {
        self.tables
            .iter()
            .flat_map(|t| &t.operations)
            .map(|op| op.estimate.input_bytes)
            .sum()
    }

    /// Whether there is anything at all to do.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Apply a global byte budget, deferring what does not fit.
    ///
    /// Tables are ordered most-fragmented-first, so a budget too small for
    /// everything buys the most improvement it can. What does not fit is
    /// returned rather than dropped: a plan that silently truncated would read
    /// as "this is all there was to do".
    pub fn apply_budget(&mut self, max_bytes: u64) -> Vec<TableRef> {
        // Most-fragmented first. Small files are the metric because they are
        // what a rewrite actually fixes; a large table already at target size
        // gains nothing from being first.
        self.tables.sort_by(|a, b| {
            fragmentation_score(b)
                .partial_cmp(&fragmentation_score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut spent = 0u64;
        let mut deferred = Vec::new();
        let mut kept = Vec::with_capacity(self.tables.len());

        for table in std::mem::take(&mut self.tables) {
            let cost: u64 = table
                .operations
                .iter()
                .map(|op| op.estimate.input_bytes)
                .sum();

            // A table costing nothing (metadata-only work) always proceeds:
            // charging it against a byte budget would let a rewrite ceiling
            // block snapshot expiration, which reads nothing.
            if cost == 0 || spent.saturating_add(cost) <= max_bytes {
                spent = spent.saturating_add(cost);
                kept.push(table);
            } else {
                deferred.push(table.table.clone());
            }
        }

        self.tables = kept;
        deferred
    }
}

/// How much a table would gain from a rewrite.
///
/// Small-file ratio weighted by file count: a table with 10 000 small files
/// matters more than one with 5, and both matter more than one with none.
fn fragmentation_score(plan: &TablePlan) -> f64 {
    let threshold = plan.policy.compaction.small_file_threshold();
    let small = plan.health.files.small_file_count(threshold) as f64;
    let ratio = plan.health.files.small_file_ratio(threshold);
    small * ratio
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_order_compact_before_cleanup() {
        // The ordering is a correctness property, not a preference: compacting
        // first is what lets expiration reclaim the files compaction
        // superseded.
        let mut kinds = vec![
            OperationKind::RemoveOrphans,
            OperationKind::ExpireSnapshots,
            OperationKind::Compact,
            OperationKind::RewriteManifests,
        ];
        kinds.sort();
        assert_eq!(
            kinds,
            vec![
                OperationKind::Compact,
                OperationKind::RewriteManifests,
                OperationKind::ExpireSnapshots,
                OperationKind::RemoveOrphans,
            ]
        );
    }

    #[test]
    fn blocked_operations_are_not_executable() {
        let op = Operation {
            kind: OperationKind::Compact,
            targets: Vec::new(),
            reason: "60% small files".into(),
            estimate: Estimate::default(),
            executability: Executability::blocked("no upstream commit path"),
        };
        assert!(!op.is_executable());
    }
}
