//! The maintenance operations themselves.
//!
//! Each operation is a free function taking an [`OpEnv`] and its own resolved
//! settings. None of them holds state between runs: a crashed run leaves only
//! files nothing references, and re-running replans from the table's current
//! snapshot. That is what "crash-only" means here, and it is why there is no
//! journal, no lock file, and nothing to repair after a `kill -9`.

/// Compaction — rewriting data files with their delete files applied.
///
/// Behind the `compaction` feature, because it is the only operation that
/// reads and writes data and therefore the only one that needs a query engine
/// (see the feature's comment in `Cargo.toml`). A build without it has no
/// compaction rather than a slower compaction: there is one executor, and the
/// feature removes the operation rather than swapping its implementation.
#[cfg(feature = "compaction")]
pub mod compact;
pub mod delete;
pub mod expire;
pub mod manifests;
pub mod orphans;
pub mod reachability;
pub mod store;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iceberg::TableIdent;
use iceberg::table::Table;

use crate::commit::TableCommitter;
use crate::obs::{MaintenanceObserver, OperationContext};

/// Everything an operation needs from the engine.
///
/// Bundled rather than passed as eight positional parameters: they are the same
/// handles for every operation, and a long positional list is where a caller
/// eventually swaps two `&dyn` arguments that happen to typecheck.
pub struct OpEnv<'a> {
    /// The table as the plan saw it.
    pub table: &'a Table,
    /// How the catalog addresses it.
    pub ident: &'a TableIdent,
    /// How to read it again — every operation that commits reloads on conflict.
    pub loader: &'a dyn TableLoader,
    /// How to deliver a commit `iceberg::Transaction` cannot express.
    pub committer: &'a dyn TableCommitter,
    /// The audit and metrics hook.
    pub observer: &'a dyn MaintenanceObserver,
    /// Which run, which table, which rule, and why.
    pub ctx: OperationContext<'a>,
    /// One clock reading for the whole operation, so two decisions inside it
    /// cannot disagree about what time it is.
    pub now: DateTime<Utc>,
    /// The blast-radius ceiling every deletion respects, from
    /// `limits.max_deletes_per_run`.
    ///
    /// On the environment rather than on each operation's settings because it
    /// is a property of *deleting*, not of orphan removal or of expiration: one
    /// deleter, one safety model (see [`delete`]).
    pub max_deletes_per_run: usize,
}

impl<'a> OpEnv<'a> {
    /// The table being maintained.
    pub fn table_ref(&self) -> &'a crate::policy::TableRef {
        self.ctx.table
    }
}

/// Loads a table's current state.
///
/// Narrower than [`iceberg::Catalog`] on purpose. Every operation that commits
/// needs exactly one thing from a catalog — the table as it is *now*, to
/// rebuild against after losing a compare-and-swap, and for the
/// re-verification orphan removal runs between listing and deleting — and
/// depending on the whole sixteen-method trait for that would make the most
/// dangerous operations in the crate the hardest ones to test.
#[async_trait]
pub trait TableLoader: Send + Sync + std::fmt::Debug {
    /// Load the table as it currently stands.
    async fn reload(&self, ident: &TableIdent) -> crate::error::Result<Table>;
}

#[async_trait]
impl TableLoader for Arc<dyn iceberg::Catalog> {
    async fn reload(&self, ident: &TableIdent) -> crate::error::Result<Table> {
        self.load_table(ident).await.map_err(Into::into)
    }
}

/// How many times a commit is retried before the operation gives up for this
/// cycle.
///
/// Small on purpose. Maintenance is a background tenant: a table being written
/// hard will keep winning, and the right response is to come back next cycle
/// rather than to spend the cycle losing repeatedly. A table that conflicts
/// every time is reported as busy, which is information — a silent retry loop
/// is not.
pub const MAX_COMMIT_ATTEMPTS: usize = 3;

/// Base delay between commit attempts.
pub const RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// The delay before attempt `attempt` (0-based): exponential, with jitter.
///
/// The jitter is not decoration. Bergman's own concurrency is bounded per
/// table, so within one process two attempts on the same table are already
/// serialised — but the deployment model is explicitly N stateless replicas
/// coordinating through catalog optimistic concurrency, and two replicas that
/// lose the same compare-and-swap at the same moment would otherwise come back
/// in lockstep, losing to each other for the whole retry budget.
///
/// Full jitter over `[base·2^n / 2, base·2^n]`: enough to break a lockstep,
/// little enough that the backoff still grows. Derived from a UUID rather than
/// a `rand` dependency, which the tree does not otherwise need.
pub fn retry_delay(attempt: usize) -> Duration {
    let ceiling = RETRY_BASE_DELAY.saturating_mul(2u32.saturating_pow(attempt as u32));
    let span = ceiling.as_millis() as u64 / 2;
    if span == 0 {
        return ceiling;
    }
    let draw = u64::from_le_bytes(
        uuid::Uuid::new_v4().into_bytes()[..8]
            .try_into()
            .expect("16"),
    );
    ceiling - Duration::from_millis(draw % span)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_grows_exponentially_within_its_jitter_band() {
        for (attempt, ceiling_ms) in [(0usize, 250u64), (1, 500), (2, 1000)] {
            for _ in 0..64 {
                let delay = retry_delay(attempt).as_millis() as u64;
                assert!(
                    delay > ceiling_ms / 2 && delay <= ceiling_ms,
                    "attempt {attempt} produced {delay}ms outside ({}, {ceiling_ms}]",
                    ceiling_ms / 2
                );
            }
        }
    }

    #[test]
    fn two_replicas_losing_together_do_not_come_back_together() {
        // The whole reason the jitter exists. Identical inputs must not produce
        // an identical delay, or N replicas that lost the same compare-and-swap
        // spend their entire retry budget losing to each other.
        let draws: std::collections::HashSet<u128> =
            (0..64).map(|_| retry_delay(2).as_millis()).collect();
        assert!(draws.len() > 1, "backoff is in lockstep across replicas");
    }
}
