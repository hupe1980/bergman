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

pub use planner::{PlanContext, plan_table};
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
    /// Work policy asked for that this table cannot receive, and why.
    ///
    /// An operation the policy enabled but the table's own shape forbids — a v3
    /// table's row lineage, a partition under a superseded spec — would
    /// otherwise read as "healthy". A note is not a failure: the table is fine,
    /// some of the configured maintenance simply does not apply to it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl TablePlan {
    /// Whether this entry describes work rather than only an explanation.
    pub fn has_work(&self) -> bool {
        !self.operations.is_empty()
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

    /// Bytes every planned rewrite would read.
    pub fn total_input_bytes(&self) -> u64 {
        self.tables
            .iter()
            .flat_map(|t| &t.operations)
            .map(|op| op.estimate.input_bytes)
            .sum()
    }

    /// Whether this plan would do anything.
    ///
    /// Operations, not entries. A table can appear in `tables` carrying only a
    /// note — work its policy asked for that it cannot receive — and that is an
    /// explanation rather than a plan.
    pub fn is_empty(&self) -> bool {
        self.operation_count() == 0
    }

    /// Explanations attached to tables, with no work planned for them.
    ///
    /// Separate from [`Self::is_empty`] because a cycle that will do nothing and
    /// a cycle that will do nothing *for a reason worth reading* are different
    /// answers, and only one of them wants an operator's attention.
    pub fn notes(&self) -> impl Iterator<Item = (&TableRef, &str)> {
        self.tables
            .iter()
            .flat_map(|t| t.notes.iter().map(move |note| (&t.table, note.as_str())))
    }

    /// Apply a global byte budget, deferring the rewrites that do not fit.
    ///
    /// Tables are ordered most-fragmented-first, so a budget too small for
    /// everything buys the most improvement it can. What does not fit is
    /// returned rather than dropped: a plan that silently truncated would read
    /// as "this is all there was to do".
    ///
    /// **Charged per operation, not per table.** Metadata-only work reads no
    /// data files and costs the budget nothing. Deferring a whole table because
    /// its compaction did not fit would stop that table's snapshots expiring —
    /// it would grow history without bound *because* it was too fragmented to
    /// compact, which is the opposite of what a cost control is for.
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

        for mut table in std::mem::take(&mut self.tables) {
            let before = table.operations.len();
            table.operations.retain(|op| {
                let cost = op.estimate.input_bytes;
                if cost == 0 {
                    return true;
                }
                if spent.saturating_add(cost) <= max_bytes {
                    spent = spent.saturating_add(cost);
                    return true;
                }
                false
            });

            if table.operations.len() < before {
                deferred.push(table.table.clone());
                // Never silent, even when the table stays in the plan for its
                // metadata-only half: an operator reading the run report has to
                // be able to tell "compacted" from "compaction deferred, the
                // rest ran".
                table.notes.push(format!(
                    "{} rewrite operations deferred: the cycle's \
                     limits.max_rewrite_bytes_per_run budget is spent",
                    before - table.operations.len()
                ));
            }

            // A table left with nothing to do still carries its notes, and they
            // are the reason it is worth keeping in the plan.
            if !table.operations.is_empty() || !table.notes.is_empty() {
                kept.push(table);
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
    use crate::health::{FileHealth, ManifestHealth, SnapshotHealth, TableHealth};

    fn plan_with(name: &str, ops: Vec<(OperationKind, u64)>) -> TablePlan {
        let table = TableRef::new("prod", ["db"], name);
        let policy = crate::policy::Policy::compile(
            &crate::policy::Config::from_toml("[[rules]]\nmatch = \"prod.**\"\n").unwrap(),
        )
        .unwrap();
        let crate::policy::Decision::Maintain(effective) =
            policy.decide(&table, &crate::policy::TableFacts::unknown())
        else {
            panic!("expected the table to be maintained");
        };

        TablePlan {
            table: table.clone(),
            health: TableHealth {
                table,
                format_version: iceberg::spec::FormatVersion::V2,
                write_format: None,
                location: "s3://b/wh/db/t".into(),
                current_spec_id: 0,
                snapshots: SnapshotHealth::default(),
                manifests: ManifestHealth::default(),
                files: FileHealth::default(),
                partitions: Vec::new(),
            },
            policy: effective,
            operations: ops
                .into_iter()
                .map(|(kind, input_bytes)| Operation {
                    kind,
                    reason: "test".into(),
                    targets: Vec::new(),
                    estimate: Estimate {
                        input_bytes,
                        ..Default::default()
                    },
                })
                .collect(),
            notes: Vec::new(),
        }
    }

    #[test]
    fn a_spent_budget_defers_the_rewrite_and_keeps_the_metadata_work() {
        // The failure this exists to prevent: a table too fragmented to fit the
        // budget would also stop expiring snapshots, so it would grow history
        // without bound *because* it needed compaction. A byte ceiling bounds
        // the cost of rewriting data; it is not a reason to stop reading
        // metadata.
        let mut plan = MaintenancePlan {
            generated_at: Utc::now(),
            tables: vec![plan_with(
                "t",
                vec![
                    (OperationKind::Compact, 1_000),
                    (OperationKind::ExpireSnapshots, 0),
                ],
            )],
            uneventful: Vec::new(),
            deferred: Vec::new(),
        };

        let deferred = plan.apply_budget(10);

        assert_eq!(deferred.len(), 1, "the deferral must be reported");
        let kept = &plan.tables[0];
        assert_eq!(
            kept.operations.iter().map(|op| op.kind).collect::<Vec<_>>(),
            vec![OperationKind::ExpireSnapshots],
            "metadata-only work is not charged against a rewrite budget"
        );
        assert!(
            kept.notes[0].contains("deferred"),
            "the partial deferral must not be silent: {:?}",
            kept.notes
        );
    }

    #[test]
    fn a_budget_that_covers_everything_defers_nothing() {
        let mut plan = MaintenancePlan {
            generated_at: Utc::now(),
            tables: vec![plan_with("t", vec![(OperationKind::Compact, 1_000)])],
            uneventful: Vec::new(),
            deferred: Vec::new(),
        };

        assert!(plan.apply_budget(10_000).is_empty());
        assert_eq!(plan.tables[0].operations.len(), 1);
        assert!(plan.tables[0].notes.is_empty());
    }

    #[test]
    fn the_budget_is_spent_across_tables_most_fragmented_first() {
        let mut plan = MaintenancePlan {
            generated_at: Utc::now(),
            tables: vec![
                plan_with("a", vec![(OperationKind::Compact, 100)]),
                plan_with("b", vec![(OperationKind::Compact, 100)]),
                plan_with("c", vec![(OperationKind::Compact, 100)]),
            ],
            uneventful: Vec::new(),
            deferred: Vec::new(),
        };

        // Two fit, the third does not — and the third is still in the plan,
        // carrying the note that says why nothing happened to it.
        let deferred = plan.apply_budget(250);
        assert_eq!(deferred.len(), 1);
        assert_eq!(plan.tables.iter().filter(|t| t.has_work()).count(), 2);
    }

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
}
