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
use crate::obs::{MaintenanceObserver, OperationContext};
use crate::ops::delete::{Deletion, announce_and_delete, withhold_beyond};
use crate::ops::reachability::{self, ReachableSet};
use crate::ops::{MAX_COMMIT_ATTEMPTS, OpEnv, retry_delay};
use crate::plan::OperationResult;
use crate::policy::{EffectiveSnapshots, TableRef};

/// Expire snapshots, and optionally delete what that orphans.
pub async fn run(
    env: &OpEnv<'_>,
    catalog: &Arc<dyn Catalog>,
    settings: &EffectiveSnapshots,
) -> Result<OperationResult> {
    let table = env.table;
    let table_ref = env.table_ref();
    let (observer, ctx, now) = (env.observer, env.ctx, env.now);

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

    // A file that will not delete is not a failure of the expiration — the
    // metadata commit already succeeded and the table is correct. It is a leak,
    // and it is counted so it can be chased.
    let deletion = match before {
        Some(before) => {
            delete_now_unreachable(
                table_ref,
                &updated,
                before,
                observer,
                ctx,
                env.max_deletes_per_run,
            )
            .await?
        }
        None => Deletion::default(),
    };

    let mut detail = format!("{removed} snapshots expired");
    if settings.delete_files.value {
        detail.push_str(&format!(", {} files deleted", deletion.deleted));
        if deletion.withheld > 0 {
            // Not a loss: what the ceiling withholds is now unreferenced by
            // every retained snapshot, which is precisely what the orphan
            // scanner reclaims.
            detail.push_str(&format!(
                " ({} left for the orphan scanner by the per-run ceiling of {})",
                deletion.withheld, env.max_deletes_per_run
            ));
        }
        if deletion.failed > 0 {
            detail.push_str(&format!(" ({} could not be deleted)", deletion.failed));
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
        let tx = Transaction::new(&current);
        let tx = tx
            .expire_snapshots()
            .expire_older_than_ms(cutoff_ms)
            .retain_last(settings.min_to_keep.value)
            .apply(tx)?;

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

/// Whether an error means the table moved rather than that something broke.
///
/// Classified by type, never by message text: the commit layer maps HTTP 409
/// and 412 to [`Error::CommitConflict`], and upstream tags its own with
/// `ErrorKind::CatalogCommitConflicts`.
///
/// A conflict misread as a failure wastes a cycle; a failure misread as a
/// conflict makes Bergman reload and retry against a world that will refuse it
/// again.
fn is_conflict(err: &Error) -> bool {
    match err {
        Error::CommitConflict { .. } | Error::StalePlan { .. } => true,
        Error::Catalog(inner) => inner.kind() == iceberg::ErrorKind::CatalogCommitConflicts,
        _ => false,
    }
}

/// Delete the files that expiration made unreachable.
///
/// The set is `reachable_before − reachable_after`. Recomputing *after* the
/// commit rather than predicting it is the point: the commit is the thing that
/// decides, and a prediction that disagreed with it would delete live files.
///
/// Deletion itself goes through [`crate::ops::delete`], the same path orphan
/// removal uses: same blast-radius ceiling, same bounded concurrency, same
/// announce-before-deleting rule.
async fn delete_now_unreachable(
    table_ref: &TableRef,
    updated: &Table,
    before: ReachableSet,
    observer: &dyn MaintenanceObserver,
    ctx: OperationContext<'_>,
    ceiling: usize,
) -> Result<Deletion> {
    let after = reachability::compute(table_ref, updated).await?;
    let location = updated.metadata().location();

    let mut doomed: Vec<String> = Vec::new();
    // `metadata_json` is deliberately absent. Expiring snapshots does not drop
    // entries from the metadata log — `write.metadata.previous-versions-max`
    // does, on the catalog's own schedule — so a `metadata.json` that leaves
    // the log leaves it *between* Bergman's before and after readings, and
    // deleting on that basis would race the catalog for a file it still owns.
    // The orphan scanner reclaims them, with its grace period.
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
        return Ok(Deletion::default());
    }

    // Deterministic order so an audit record of a partial deletion can be read
    // against a later one — and so that a ceiling truncating the list twice
    // truncates it the same way.
    doomed.sort();

    let withheld = withhold_beyond(&mut doomed, ceiling);
    if withheld > 0 {
        tracing::warn!(
            table = %table_ref,
            ceiling,
            withheld,
            "expiration orphaned more files than the per-run ceiling; the orphan \
             scanner reclaims the rest"
        );
    }

    let mut deletion = announce_and_delete(updated.file_io(), observer, ctx, &doomed).await;
    deletion.withheld = withheld;
    Ok(deletion)
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
    fn conflicts_are_recognised_by_kind_not_by_text() {
        // The commit layer classifies 409/412 itself...
        assert!(is_conflict(&Error::CommitConflict {
            table: "db.t".into(),
            detail: "ref moved".into(),
        }));
        assert!(is_conflict(&Error::StalePlan {
            table: "db.t".into(),
            detail: "inputs gone".into(),
        }));

        // ...and upstream tags its own.
        assert!(is_conflict(&Error::Catalog(Box::new(iceberg::Error::new(
            iceberg::ErrorKind::CatalogCommitConflicts,
            "one or more requirements failed",
        )))));
    }

    #[test]
    fn ordinary_failures_are_not_conflicts() {
        // A failure misread as a conflict makes Bergman reload and retry
        // against a world that will refuse it again.
        assert!(!is_conflict(&Error::Catalog(Box::new(
            iceberg::Error::new(iceberg::ErrorKind::Unexpected, "connection refused",)
        ))));
        assert!(!is_conflict(&Error::config("bad config")));
        assert!(!is_conflict(&Error::refused("compact", "db.t", "no")));

        // Text that merely *mentions* a conflict is not one.
        assert!(!is_conflict(&Error::Catalog(Box::new(
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "no table named conflict_log",
            )
        ))));
    }
}
