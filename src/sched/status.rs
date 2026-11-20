//! What a running daemon can say about itself: what it is doing, and what needs
//! an operator. Held in memory; a restarted daemon starts empty and fills in on
//! its first cycle.
//!
//! **A table leaves the board only when a cycle that examined it had nothing to
//! say.** A run report lists only tables with work or a note, so absence means
//! "fixed" after a cycle that covered the table and "unknown" after one that did
//! not. Confusing the two would report an unexamined refusal as fixed.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Scope, Trigger};
use crate::RunReport;
use crate::plan::TableOutcome;
use crate::policy::TableRef;

/// A point-in-time copy of a daemon's status, as `/status` serves it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonStatus {
    /// When the daemon started.
    pub started_at: DateTime<Utc>,
    /// Cycles finished, successfully or not.
    pub cycles: u64,
    /// The cycle in progress, if one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<RunningCycle>,
    /// When the next cycle is due, and what will start it.
    ///
    /// An event can always start one sooner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<NextCycle>,
    /// The most recent cycle to finish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_cycle: Option<CycleRecord>,
    /// The latest outcome for every table that had work or a note, from
    /// whichever cycle last examined it, in table order.
    pub tables: Vec<TableStatus>,
}

impl DaemonStatus {
    /// Whether a cycle failed outright, or a table has a failed or refused
    /// operation. Notes do not count, as they do not fail `bergman run`.
    pub fn needs_attention(&self) -> bool {
        self.last_cycle.as_ref().is_some_and(|c| c.error.is_some())
            || self.tables.iter().any(TableStatus::needs_attention)
    }
}

/// A cycle that has started and not finished.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunningCycle {
    /// Which cycle, counting from one.
    pub number: u64,
    /// What started it.
    pub trigger: String,
    /// When it started.
    pub since: DateTime<Utc>,
}

/// When the next cycle is due.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NextCycle {
    /// The moment, already pushed past a closed maintenance window.
    pub at: DateTime<Utc>,
    /// What will start it.
    pub trigger: String,
}

/// How the last finished cycle went, as a whole.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleRecord {
    /// Which cycle, counting from one.
    pub number: u64,
    /// What started it.
    pub trigger: String,
    /// When it finished.
    pub finished_at: DateTime<Utc>,
    /// The run report's one-line summary, when the cycle produced one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Why the cycle produced no report — an unreachable catalog, say. Nothing
    /// was examined, so the tables are what earlier cycles found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Tables the cycle's byte budget pushed to a later one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deferred: Vec<TableRef>,
}

/// One table's latest outcome, and which cycle produced it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableStatus {
    /// The cycle that last examined this table and had something to say.
    pub cycle: u64,
    /// When that cycle finished.
    pub at: DateTime<Utc>,
    /// What it said.
    #[serde(flatten)]
    pub outcome: TableOutcome,
}

impl TableStatus {
    /// Whether an operation on this table failed or was refused.
    pub fn needs_attention(&self) -> bool {
        self.outcome
            .operations
            .iter()
            .any(|op| op.result.needs_attention())
    }
}

/// The live status of a daemon, shared between its loop and whoever asks.
///
/// Cheap to clone: every clone is the same board. [`Daemon::status`] hands one
/// out, and `/status` serves [`StatusBoard::snapshot`].
///
/// [`Daemon::status`]: super::Daemon::status
#[derive(Debug, Clone)]
pub struct StatusBoard(Arc<RwLock<Board>>);

#[derive(Debug)]
struct Board {
    started_at: DateTime<Utc>,
    cycles: u64,
    running: Option<RunningCycle>,
    next: Option<NextCycle>,
    last_cycle: Option<CycleRecord>,
    tables: BTreeMap<TableRef, TableStatus>,
}

impl Default for StatusBoard {
    fn default() -> Self {
        Self::new()
    }
}

impl StatusBoard {
    /// An empty board, started now.
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(Board {
            started_at: Utc::now(),
            cycles: 0,
            running: None,
            next: None,
            last_cycle: None,
            tables: BTreeMap::new(),
        })))
    }

    /// A copy of the board as it stands.
    pub fn snapshot(&self) -> DaemonStatus {
        let board = self.read();
        DaemonStatus {
            started_at: board.started_at,
            cycles: board.cycles,
            running: board.running.clone(),
            next: board.next.clone(),
            last_cycle: board.last_cycle.clone(),
            tables: board.tables.values().cloned().collect(),
        }
    }

    pub(crate) fn next_due(&self, at: DateTime<Utc>, trigger: &Trigger) {
        self.write().next = Some(NextCycle {
            at,
            trigger: trigger.to_string(),
        });
    }

    pub(crate) fn started(&self, number: u64, trigger: &Trigger) {
        let mut board = self.write();
        board.next = None;
        board.running = Some(RunningCycle {
            number,
            trigger: trigger.to_string(),
            since: Utc::now(),
        });
    }

    /// Record a finished cycle over `scope`.
    pub(super) fn finished(
        &self,
        number: u64,
        trigger: &Trigger,
        scope: &Scope,
        outcome: Result<&RunReport, String>,
    ) {
        let mut board = self.write();
        board.running = None;
        board.cycles = number;

        let report = match outcome {
            Ok(report) => report,
            Err(error) => {
                // Nothing was examined: clearing the board would make an
                // unreachable catalog look like a healthy warehouse.
                board.last_cycle = Some(CycleRecord {
                    number,
                    trigger: trigger.to_string(),
                    finished_at: Utc::now(),
                    summary: None,
                    error: Some(error),
                    deferred: Vec::new(),
                });
                return;
            }
        };

        // Forget only what this cycle examined. The budget stopped short of
        // a deferred table, so an earlier cycle's word on it stands.
        board
            .tables
            .retain(|table, _| !covers(scope, table) || report.deferred.contains(table));

        for outcome in &report.tables {
            board.tables.insert(
                outcome.table.clone(),
                TableStatus {
                    cycle: number,
                    at: report.finished_at,
                    outcome: outcome.clone(),
                },
            );
        }

        board.last_cycle = Some(CycleRecord {
            number,
            trigger: trigger.to_string(),
            finished_at: report.finished_at,
            summary: Some(report.to_string()),
            error: None,
            deferred: report.deferred.clone(),
        });
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Board> {
        // A poisoned board is at worst one cycle stale; still serve it.
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Board> {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Whether a cycle over `scope` examined `table`.
fn covers(scope: &Scope, table: &TableRef) -> bool {
    match scope {
        Scope::Everything => true,
        Scope::Matching(pattern) => {
            // The daemon planned with this same pattern, so it compiled then.
            crate::policy::TableMatcher::new(pattern).is_ok_and(|m| m.matches(table))
        }
        Scope::Tables(tables) => tables.contains(table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{OperationKind, OperationOutcome, OperationResult};

    fn t(name: &str) -> TableRef {
        TableRef::new("prod", ["db"], name)
    }

    fn refused(table: &TableRef) -> TableOutcome {
        TableOutcome {
            table: table.clone(),
            matched_rule: "prod.**".into(),
            notes: Vec::new(),
            operations: vec![OperationOutcome {
                kind: OperationKind::Compact,
                reason: "small files".into(),
                result: OperationResult::Refused {
                    reason: "cross-spec rewrite".into(),
                },
                duration: std::time::Duration::ZERO,
            }],
        }
    }

    fn report(tables: Vec<TableOutcome>) -> RunReport {
        let now = Utc::now();
        RunReport {
            started_at: now,
            finished_at: now,
            tables,
            deferred: Vec::new(),
        }
    }

    /// A board on which `prod.db.a` was refused by the first, full cycle.
    fn board_with_a_refusal() -> StatusBoard {
        let board = StatusBoard::new();
        board.finished(
            1,
            &Trigger::Interval,
            &Scope::Everything,
            Ok(&report(vec![refused(&t("a"))])),
        );
        assert!(board.snapshot().needs_attention());
        board
    }

    #[test]
    fn a_cycle_that_examined_a_table_and_found_nothing_clears_it() {
        // Control for the test below: the same empty report, from a cycle that
        // did cover the table. Without this, a board that never cleared
        // anything would pass the guarded case.
        let board = board_with_a_refusal();
        board.finished(
            2,
            &Trigger::Event { tables: 1 },
            &Scope::Tables(vec![t("a")]),
            Ok(&report(vec![])),
        );
        let status = board.snapshot();
        assert!(status.tables.is_empty(), "{status:?}");
        assert!(!status.needs_attention());
    }

    #[test]
    fn a_cycle_that_never_looked_at_a_table_does_not_clear_it() {
        // An event for `b` says nothing about `a`. Clearing `a` here would
        // report a refusal as fixed when nothing re-examined it.
        let board = board_with_a_refusal();
        board.finished(
            2,
            &Trigger::Event { tables: 1 },
            &Scope::Tables(vec![t("b")]),
            Ok(&report(vec![])),
        );
        let status = board.snapshot();
        assert_eq!(status.tables.len(), 1);
        assert_eq!(status.tables[0].cycle, 1, "the entry is the older cycle's");
        assert!(status.needs_attention());
    }

    #[test]
    fn a_schedule_clears_only_what_its_pattern_matches() {
        let board = StatusBoard::new();
        board.finished(
            1,
            &Trigger::Interval,
            &Scope::Everything,
            Ok(&report(vec![refused(&t("a")), refused(&t("b"))])),
        );
        board.finished(
            2,
            &Trigger::Schedule {
                pattern: "prod.db.a".into(),
            },
            &Scope::Matching("prod.db.a".into()),
            Ok(&report(vec![])),
        );
        let left: Vec<_> = board
            .snapshot()
            .tables
            .into_iter()
            .map(|s| s.outcome.table)
            .collect();
        assert_eq!(left, vec![t("b")]);
    }

    #[test]
    fn a_deferred_table_keeps_what_it_last_said() {
        // The budget stopped before the table, so the cycle did not examine it
        // even though its scope covered it.
        let board = board_with_a_refusal();
        let mut r = report(vec![]);
        r.deferred.push(t("a"));
        board.finished(2, &Trigger::Interval, &Scope::Everything, Ok(&r));
        let status = board.snapshot();
        assert_eq!(status.tables.len(), 1);
        assert_eq!(status.last_cycle.unwrap().deferred, vec![t("a")]);
    }

    #[test]
    fn a_failed_cycle_keeps_the_tables_and_demands_attention() {
        // An unreachable catalog examined nothing. A board that cleared itself
        // would turn an outage into a warehouse that looks healthy.
        let board = board_with_a_refusal();
        board.finished(
            2,
            &Trigger::Interval,
            &Scope::Everything,
            Err("catalog unreachable".into()),
        );
        let status = board.snapshot();
        assert_eq!(status.cycles, 2);
        assert_eq!(status.tables.len(), 1);
        let last = status.last_cycle.as_ref().unwrap();
        assert_eq!(last.error.as_deref(), Some("catalog unreachable"));
        assert!(last.summary.is_none());
    }

    #[test]
    fn a_running_cycle_is_visible_until_it_finishes() {
        let board = StatusBoard::new();
        board.next_due(Utc::now(), &Trigger::Interval);
        board.started(1, &Trigger::Interval);
        let status = board.snapshot();
        assert_eq!(status.running.as_ref().map(|r| r.number), Some(1));
        assert!(status.next.is_none(), "a started cycle is no longer next");

        board.finished(
            1,
            &Trigger::Interval,
            &Scope::Everything,
            Ok(&report(vec![])),
        );
        assert!(board.snapshot().running.is_none());
    }

    #[test]
    fn the_status_round_trips_through_json() {
        // `bergman status` reads exactly what `/status` writes.
        let status = board_with_a_refusal().snapshot();
        let json = serde_json::to_string(&status).unwrap();
        let back: DaemonStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }
}
