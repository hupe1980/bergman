//! What a run actually did.
//!
//! A report is not a log. It is the record an operator reads to answer "what
//! happened to my table", so every entry names the table, the operation, the
//! policy rule that triggered it, and the outcome — including the outcomes that
//! are not successes.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::plan::OperationKind;
use crate::policy::TableRef;

/// The result of executing a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When it finished.
    pub finished_at: DateTime<Utc>,
    /// One entry per table with work.
    pub tables: Vec<TableOutcome>,
    /// Tables deferred because the run's byte budget was exhausted.
    ///
    /// Never silently dropped: a budget that truncated invisibly would make a
    /// partial run look like a complete one.
    pub deferred: Vec<TableRef>,
}

/// What happened to one table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOutcome {
    /// Which table.
    pub table: TableRef,
    /// The rule that selected it, so an outcome can be traced to a policy line.
    pub matched_rule: String,
    /// What happened to each operation.
    pub operations: Vec<OperationOutcome>,
    /// Work policy asked for that this table cannot receive, carried through
    /// from its plan.
    ///
    /// A run report is what an operator actually reads, and a table whose
    /// compaction rule can never apply must not look identical to one that was
    /// simply healthy this cycle.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// What happened to one operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationOutcome {
    /// Which operation.
    pub kind: OperationKind,
    /// Why it was planned.
    pub reason: String,
    /// How it ended.
    pub result: OperationResult,
    /// How long it took.
    #[serde(with = "humantime_serde")]
    pub duration: std::time::Duration,
}

/// How an operation ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "kebab-case")]
pub enum OperationResult {
    /// It ran and changed the table.
    Succeeded {
        /// A one-line description of what changed.
        detail: String,
    },
    /// It ran and found nothing to do.
    ///
    /// Distinct from success: a plan built minutes ago can be overtaken by
    /// another writer, and "there was nothing left to expire" is a different
    /// fact from "eight snapshots were expired".
    NoOp {
        /// Why there was nothing to do.
        detail: String,
    },
    /// It was declined for safety.
    Refused {
        /// Why.
        reason: String,
    },
    /// The table moved underneath it and the plan was abandoned.
    ///
    /// An expected outcome, not an error: maintenance competes with foreground
    /// writers and yields to them by design.
    Conflicted {
        /// What moved.
        detail: String,
    },
    /// It failed.
    Failed {
        /// What went wrong.
        error: String,
    },
}

impl OperationResult {
    /// Whether this outcome changed the table.
    pub fn changed_anything(&self) -> bool {
        matches!(self, Self::Succeeded { .. })
    }

    /// Whether this outcome is one an operator should look at.
    pub fn needs_attention(&self) -> bool {
        matches!(self, Self::Failed { .. } | Self::Refused { .. })
    }
}

impl RunReport {
    /// How long the run took.
    pub fn duration(&self) -> chrono::Duration {
        self.finished_at - self.started_at
    }

    /// Operations that changed something.
    pub fn succeeded_count(&self) -> usize {
        self.operation_results()
            .filter(|r| r.changed_anything())
            .count()
    }

    /// Operations that failed.
    pub fn failed_count(&self) -> usize {
        self.operation_results()
            .filter(|r| matches!(r, OperationResult::Failed { .. }))
            .count()
    }

    /// Operations abandoned because the table moved.
    pub fn conflicted_count(&self) -> usize {
        self.operation_results()
            .filter(|r| matches!(r, OperationResult::Conflicted { .. }))
            .count()
    }

    /// Whether anything in this run needs an operator's attention.
    pub fn needs_attention(&self) -> bool {
        self.operation_results().any(|r| r.needs_attention())
    }

    fn operation_results(&self) -> impl Iterator<Item = &OperationResult> {
        self.tables
            .iter()
            .flat_map(|t| &t.operations)
            .map(|op| &op.result)
    }
}

impl fmt::Display for RunReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Tables that were actually acted on. An entry carrying only a note is
        // an explanation, and counting it as a maintained table would overstate
        // what the run did.
        let acted_on = self
            .tables
            .iter()
            .filter(|t| !t.operations.is_empty())
            .count();
        write!(
            f,
            "{acted_on} tables, {} operations succeeded",
            self.succeeded_count()
        )?;
        if self.conflicted_count() > 0 {
            write!(f, ", {} conflicted", self.conflicted_count())?;
        }
        if self.failed_count() > 0 {
            write!(f, ", {} failed", self.failed_count())?;
        }
        if !self.deferred.is_empty() {
            write!(f, ", {} deferred (budget)", self.deferred.len())?;
        }
        write!(f, " in {}s", self.duration().num_seconds())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(results: Vec<OperationResult>) -> RunReport {
        let now = Utc::now();
        RunReport {
            started_at: now,
            finished_at: now,
            tables: vec![TableOutcome {
                table: TableRef::new("prod", ["db"], "t"),
                matched_rule: "prod.*".into(),
                notes: Vec::new(),
                operations: results
                    .into_iter()
                    .map(|result| OperationOutcome {
                        kind: OperationKind::ExpireSnapshots,
                        reason: "test".into(),
                        result,
                        duration: std::time::Duration::from_secs(1),
                    })
                    .collect(),
            }],
            deferred: Vec::new(),
        }
    }

    #[test]
    fn a_noop_is_not_a_success() {
        // "Nothing was left to expire" and "eight snapshots expired" are
        // different facts, and a report that conflated them would overstate
        // what maintenance achieved.
        let r = report(vec![OperationResult::NoOp {
            detail: "nothing expirable".into(),
        }]);
        assert_eq!(r.succeeded_count(), 0);
        assert!(!r.needs_attention());
    }

    #[test]
    fn conflicts_are_reported_but_do_not_demand_attention() {
        // Losing to a foreground writer is the design working, not a fault.
        let r = report(vec![OperationResult::Conflicted {
            detail: "ref moved".into(),
        }]);
        assert_eq!(r.conflicted_count(), 1);
        assert_eq!(r.failed_count(), 0);
        assert!(!r.needs_attention());
    }

    #[test]
    fn failures_and_refusals_demand_attention() {
        assert!(
            report(vec![OperationResult::Failed {
                error: "boom".into()
            }])
            .needs_attention()
        );
        assert!(
            report(vec![OperationResult::Refused {
                reason: "location outside warehouse".into()
            }])
            .needs_attention()
        );
    }

    #[test]
    fn summary_mentions_conflicts_and_deferrals() {
        let mut r = report(vec![
            OperationResult::Succeeded {
                detail: "8 snapshots".into(),
            },
            OperationResult::Conflicted {
                detail: "ref moved".into(),
            },
        ]);
        r.deferred.push(TableRef::new("prod", ["db"], "other"));

        let rendered = r.to_string();
        assert!(rendered.contains("1 operations succeeded"), "{rendered}");
        assert!(rendered.contains("1 conflicted"), "{rendered}");
        assert!(rendered.contains("1 deferred (budget)"), "{rendered}");
    }
}
