//! Snapshot expiration.
//!
//! The selection rules — per-branch ancestry, per-ref retention, ref aging —
//! are upstream's `ExpireSnapshotsAction`, which follows Java's
//! `RemoveSnapshots`. Bergman does not reimplement them: that is the subtlest
//! rule in Iceberg and a second implementation would drift from the first.
//!
//! What upstream explicitly does *not* do is delete the files expiration
//! orphans — its own documentation calls physical cleanup "the responsibility
//! of a higher-level maintenance operation built on top of this action". This
//! module is that operation.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use iceberg::Catalog;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::error::{Error, Result};
use crate::obs::MaintenanceObserver;
use crate::ops::reachability::{self, ReachableSet};
use crate::ops::{MAX_COMMIT_ATTEMPTS, retry_delay};
use crate::plan::OperationResult;
use crate::policy::{EffectiveSnapshots, TableRef};

/// What expiration did.
#[derive(Debug, Clone, Default)]
pub struct ExpireOutcome {
    /// How many snapshots were removed.
    pub snapshots_removed: usize,
    /// How many files were deleted, when file cleanup was enabled.
    pub files_deleted: usize,
    /// How many files could not be deleted.
    ///
    /// A file that will not delete is not a failure of the expiration — the
    /// metadata commit already succeeded and the table is correct. It is a
    /// leak, and it is reported so it can be chased.
    pub files_failed: usize,
}

/// Expire snapshots, and optionally delete what that orphans.
pub async fn run(
    table_ref: &TableRef,
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    settings: &EffectiveSnapshots,
    observer: &dyn MaintenanceObserver,
    now: DateTime<Utc>,
) -> Result<OperationResult> {
    // The reachable set is computed *before* the commit, while the snapshots
    // still exist. Afterwards, the files they referenced are unreachable by
    // construction and there would be nothing left to diff against.
    let before = if settings.delete_files.value {
        Some(reachability::compute(table_ref, table).await?)
    } else {
        None
    };

    let snapshots_before = table.metadata().snapshots().len();

    let committed = commit_with_retry(table_ref, table, catalog, settings, now).await?;

    let Some(updated) = committed else {
        return Ok(OperationResult::Conflicted {
            detail: format!(
                "table moved during {MAX_COMMIT_ATTEMPTS} commit attempts; \
                 will replan next cycle"
            ),
        });
    };

    let snapshots_after = updated.metadata().snapshots().len();
    let removed = snapshots_before.saturating_sub(snapshots_after);

    if removed == 0 {
        // Upstream computed that nothing was expirable — a shared ancestor
        // reachable from a retained branch, or a ref keeping something alive.
        // Bergman's planner works from coarser numbers and cannot know this in
        // advance, so it is a normal outcome rather than a mistake.
        return Ok(OperationResult::NoOp {
            detail: "no snapshot was expirable under per-branch retention".into(),
        });
    }

    let mut outcome = ExpireOutcome {
        snapshots_removed: removed,
        ..Default::default()
    };

    if let Some(before) = before {
        let deleted = delete_now_unreachable(table_ref, &updated, before, observer).await?;
        outcome.files_deleted = deleted.0;
        outcome.files_failed = deleted.1;
    }

    let mut detail = format!("{removed} snapshots expired");
    if settings.delete_files.value {
        detail.push_str(&format!(", {} files deleted", outcome.files_deleted));
        if outcome.files_failed > 0 {
            detail.push_str(&format!(" ({} could not be deleted)", outcome.files_failed));
        }
    }

    Ok(OperationResult::Succeeded { detail })
}

/// Commit the expiration, retrying on conflict.
///
/// Returns `None` when every attempt lost its compare-and-swap. Each retry
/// **reloads the table**, so the next attempt is computed against the state
/// that actually exists — re-submitting the same commit would be applying a
/// decision made about a table that has since changed.
async fn commit_with_retry(
    table_ref: &TableRef,
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    settings: &EffectiveSnapshots,
    now: DateTime<Utc>,
) -> Result<Option<Table>> {
    let cutoff_ms = cutoff_millis(now, settings.max_age.value);
    let mut current = table.clone();

    for attempt in 0..MAX_COMMIT_ATTEMPTS {
        let action = Transaction::new(&current)
            .expire_snapshots()
            .expire_older_than_ms(cutoff_ms)
            .retain_last(settings.min_to_keep.value);

        let tx = action.apply(Transaction::new(&current))?;

        match tx.commit(catalog.as_ref()).await {
            Ok(updated) => return Ok(Some(updated)),
            Err(err) => {
                let mapped = Error::from(err);
                if !is_conflict(&mapped) {
                    return Err(mapped);
                }

                tracing::debug!(
                    table = %table_ref,
                    attempt = attempt + 1,
                    "expire-snapshots lost its commit; reloading and replanning"
                );

                if attempt + 1 == MAX_COMMIT_ATTEMPTS {
                    return Ok(None);
                }
                tokio::time::sleep(retry_delay(attempt)).await;

                let ident = crate::catalog::to_table_ident(table_ref)?;
                current = catalog.load_table(&ident).await?;
            }
        }
    }

    Ok(None)
}

/// The timestamp before which snapshots are expirable.
fn cutoff_millis(now: DateTime<Utc>, max_age: Duration) -> i64 {
    let age_ms = i64::try_from(max_age.as_millis()).unwrap_or(i64::MAX);
    now.timestamp_millis().saturating_sub(age_ms)
}

/// Whether a catalog error means the table moved rather than that something
/// broke.
///
/// The REST spec answers a failed requirement check with 409, and upstream
/// surfaces that as a message rather than a typed variant. Matching on text is
/// unpleasant, and it is bounded here by what it costs to be wrong: a conflict
/// mistaken for a failure is reported as a failure and retried next cycle; a
/// failure mistaken for a conflict costs at most `MAX_COMMIT_ATTEMPTS`
/// reload-and-retry rounds before being reported anyway. Neither loses data.
fn is_conflict(err: &Error) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    text.contains("conflict")
        || text.contains("409")
        || text.contains("requirement failed")
        || text.contains("commitfailed")
        || text.contains("commit failed")
}

/// Delete the files that expiration made unreachable.
///
/// The set is `reachable_before − reachable_after`. Recomputing *after* the
/// commit rather than predicting it is the point: the commit is the thing that
/// decides, and a prediction that disagreed with it would delete live files.
async fn delete_now_unreachable(
    table_ref: &TableRef,
    updated: &Table,
    before: ReachableSet,
    observer: &dyn MaintenanceObserver,
) -> Result<(usize, usize)> {
    let after = reachability::compute(table_ref, updated).await?;
    let location = updated.metadata().location();

    let mut doomed: Vec<String> = Vec::new();
    for path in before
        .data_files
        .iter()
        .chain(before.metadata_files.iter())
        .chain(before.statistics_files.iter())
    {
        if after.contains(path) {
            continue;
        }
        // Containment is re-checked per file even though these paths came out
        // of the table's own metadata. A table whose location was changed after
        // files were written can legitimately reference files outside it, and
        // deleting those would destroy data this table does not own.
        if !reachability::is_inside(location, path) {
            tracing::warn!(
                table = %table_ref,
                file = %path,
                "file is outside the table location and will not be deleted"
            );
            continue;
        }
        doomed.push(path.clone());
    }

    if doomed.is_empty() {
        return Ok((0, 0));
    }

    // Deterministic order so an audit record of a partial deletion can be read
    // against a later one.
    doomed.sort();

    // Announced before the first deletion, so a crash halfway through still
    // leaves a record of what was about to be removed.
    observer.deleting_files(table_ref, &doomed).await;

    let file_io = updated.file_io();
    let mut deleted = 0usize;
    let mut failed = 0usize;
    for path in &doomed {
        match file_io.delete(path).await {
            Ok(()) => deleted += 1,
            Err(e) => {
                // One undeletable file does not stop the rest. The metadata
                // commit has already succeeded, so the table is correct either
                // way; what remains is a leak, and leaking one file is better
                // than leaking all of them.
                tracing::warn!(table = %table_ref, file = %path, error = %e, "file could not be deleted");
                failed += 1;
            }
        }
    }

    Ok((deleted, failed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cutoff_is_now_minus_max_age() {
        let now = DateTime::from_timestamp(1_000_000, 0).unwrap();
        assert_eq!(
            cutoff_millis(now, Duration::from_secs(86400)),
            1_000_000_000 - 86_400_000
        );
    }

    #[test]
    fn an_enormous_max_age_saturates_rather_than_overflowing() {
        // `Duration::MAX` in milliseconds does not fit an i64, and a wrapped
        // cutoff would land in the future and expire everything.
        let now = Utc::now();
        let cutoff = cutoff_millis(now, Duration::MAX);
        assert!(cutoff <= now.timestamp_millis());
    }

    #[test]
    fn conflicts_are_recognised_from_the_shapes_rest_catalogs_use() {
        for text in [
            "catalog error: conflict detected",
            "catalog error: 409 Conflict",
            "catalog error: requirement failed: branch main has changed",
            "catalog error: CommitFailedException",
        ] {
            assert!(
                is_conflict(&Error::Config(text.into())),
                "not recognised: {text}"
            );
        }
    }

    #[test]
    fn ordinary_failures_are_not_conflicts() {
        // Misclassifying these would burn the retry budget reaching the same
        // conclusion.
        for text in ["connection refused", "403 Forbidden", "no such table"] {
            assert!(
                !is_conflict(&Error::Config(text.into())),
                "wrongly a conflict: {text}"
            );
        }
    }
}
