//! Orphan-file removal.
//!
//! This is the one operation that can destroy a healthy table, so it is the one
//! with the most machinery between a candidate and a deletion. Five independent
//! checks stand in the way, and each exists because of a way this goes wrong:
//!
//! 1. **Dry run by default.** Deleting requires an explicit `mode = "delete"`.
//! 2. **A grace period with a floor.** Writers stage files *before* the commit
//!    that references them; a young unreferenced file is more likely a live
//!    write than garbage. [`MIN_ORPHAN_AGE`] cannot be configured away, and is
//!    checked here as well as at parse time.
//! 3. **Unknown age means young.** A store that will not say how old a file is
//!    cannot be used to argue that it is old enough to delete.
//! 4. **Segment-wise containment.** Nothing outside the table's own location is
//!    ever a candidate, so `…/events` maintenance cannot reach
//!    `…/events_archive`.
//! 5. **Re-verification before deleting.** Metadata is reloaded after listing,
//!    and anything that became reachable in between is dropped from the kill
//!    list. A commit lands between a scan and a delete far more often than
//!    intuition suggests.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use iceberg::Catalog;
use iceberg::table::Table;

use crate::error::{Error, Result};
use crate::obs::{MaintenanceObserver, OperationContext};
use crate::ops::reachability::{self, ReachableSet};
use crate::ops::store::ObjectStore;
use crate::plan::OperationResult;
use crate::policy::{EffectiveOrphans, MIN_ORPHAN_AGE, OrphanMode};
use crate::util::human_bytes;

/// What the scan found and did.
#[derive(Debug, Clone, Default)]
pub struct OrphanOutcome {
    /// Objects listed under the table location.
    pub scanned: usize,
    /// Objects no retained metadata references and old enough to consider.
    pub orphans: usize,
    /// Their total size.
    pub orphan_bytes: u64,
    /// How many were deleted.
    pub deleted: usize,
    /// How many were dropped by the re-verification pass.
    ///
    /// Non-zero means a writer committed between the scan and the deletion —
    /// which is exactly what check 5 exists for, and worth reporting when it
    /// fires.
    pub reprieved: usize,
    /// How many deletions failed.
    pub failed: usize,
}

/// Scan for orphans, and delete them when policy allows.
pub async fn run(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    store: &dyn ObjectStore,
    settings: &EffectiveOrphans,
    observer: &dyn MaintenanceObserver,
    ctx: OperationContext<'_>,
    now: DateTime<Utc>,
) -> Result<OperationResult> {
    // One source of the table's identity: the context carries it.
    let table_ref = ctx.table;

    // Check 2, enforced here as well as at parse time. The library API lets an
    // embedder build settings directly, and a safety rule enforced at only one
    // of two entry points is a safety rule with a hole in it.
    let min_age = settings.older_than.value;
    if min_age < MIN_ORPHAN_AGE {
        return Err(Error::refused(
            "remove-orphans",
            table_ref,
            format!(
                "configured age {}s is below the {}s floor",
                min_age.as_secs(),
                MIN_ORPHAN_AGE.as_secs()
            ),
        ));
    }

    let location = table.metadata().location().to_string();

    let reachable = reachability::compute(table_ref, table).await?;
    // A table with metadata but no reachable files at all is not a table with
    // nothing but garbage under it — far more likely, something went wrong
    // reading it. Deleting on that basis would empty the warehouse.
    if reachable.is_empty() && table.metadata().current_snapshot().is_some() {
        return Err(Error::refused(
            "remove-orphans",
            table_ref,
            "table has a current snapshot but no reachable files; refusing to treat \
             everything under its location as garbage",
        ));
    }

    let listed = store.list(&location).await?;
    let scanned = listed.len();

    let cutoff = now - chrono::Duration::from_std(min_age).unwrap_or(chrono::Duration::MAX);

    let mut candidates = Vec::new();
    let mut orphan_bytes = 0u64;
    for object in listed {
        if reachable.contains(&object.path) {
            continue;
        }
        // Check 4. The listing is already scoped to the location, but a store
        // that returns something outside it must not be trusted to have done so
        // by accident.
        if !reachability::is_inside(&location, &object.path) {
            tracing::warn!(
                table = %table_ref,
                file = %object.path,
                "listing returned a path outside the table location; ignoring"
            );
            continue;
        }
        // Check 3.
        let Some(modified) = object.last_modified else {
            tracing::debug!(
                table = %table_ref,
                file = %object.path,
                "no modification time; treating as too young to delete"
            );
            continue;
        };
        // Check 2, per file.
        if modified > cutoff {
            continue;
        }

        orphan_bytes += object.size;
        candidates.push(object.path);
    }

    candidates.sort();

    let mut outcome = OrphanOutcome {
        scanned,
        orphans: candidates.len(),
        orphan_bytes,
        ..Default::default()
    };

    // Check 1.
    if settings.mode.value == OrphanMode::DryRun {
        return Ok(OperationResult::NoOp {
            detail: format!(
                "{} orphans totalling {} found in {scanned} objects (dry run; \
                 set mode = \"delete\" to remove them)",
                outcome.orphans,
                human_bytes(outcome.orphan_bytes),
            ),
        });
    }

    if candidates.is_empty() {
        return Ok(OperationResult::NoOp {
            detail: format!("no orphans among {scanned} objects"),
        });
    }

    // Check 5. Listing a large table takes long enough for a writer to commit,
    // and any file that commit referenced is now live.
    let fresh = catalog
        .load_table(&crate::catalog::to_table_ident(table_ref)?)
        .await?;
    let reachable_now = reachability::compute(table_ref, &fresh).await?;

    let (doomed, reprieved) = reverify(candidates, &reachable_now);
    outcome.reprieved = reprieved;

    if reprieved > 0 {
        tracing::info!(
            table = %table_ref,
            reprieved,
            "files became reachable between the scan and the deletion"
        );
    }

    if doomed.is_empty() {
        return Ok(OperationResult::NoOp {
            detail: format!("all {reprieved} candidates became reachable before deletion"),
        });
    }

    // The deletion manifest, written before the first delete. A crash halfway
    // through then leaves a record of exactly what was about to go.
    observer.deleting_files(ctx, &doomed).await;

    for path in &doomed {
        match store.delete(path).await {
            Ok(()) => outcome.deleted += 1,
            Err(e) => {
                tracing::warn!(table = %table_ref, file = %path, error = %e, "orphan could not be deleted");
                outcome.failed += 1;
            }
        }
    }

    let mut detail = format!(
        "{} orphans deleted ({}) from {scanned} objects",
        outcome.deleted,
        human_bytes(outcome.orphan_bytes)
    );
    if outcome.reprieved > 0 {
        detail.push_str(&format!(
            ", {} spared as newly reachable",
            outcome.reprieved
        ));
    }
    if outcome.failed > 0 {
        detail.push_str(&format!(", {} could not be deleted", outcome.failed));
    }

    Ok(OperationResult::Succeeded { detail })
}

/// Drop candidates that have become reachable since the scan.
///
/// Split out from [`run`] so the TOCTOU rule can be tested directly; it is the
/// check most likely to be quietly broken by a refactor, because nothing fails
/// when it is missing until a table is corrupted.
fn reverify(candidates: Vec<String>, reachable_now: &ReachableSet) -> (Vec<String>, usize) {
    let before = candidates.len();
    let doomed: Vec<String> = candidates
        .into_iter()
        .filter(|path| !reachable_now.contains(path))
        .collect();
    let reprieved = before - doomed.len();
    (doomed, reprieved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::reachability::normalize;

    fn reachable_with(paths: &[&str]) -> ReachableSet {
        let mut set = ReachableSet::default();
        for path in paths {
            set.data_files.insert(normalize(path));
        }
        set
    }

    #[test]
    fn reverification_spares_files_that_became_reachable() {
        // The scan said these were garbage; a commit landed in between and made
        // one of them live. Deleting it would corrupt the table.
        let candidates = vec![
            "s3://b/wh/t/data/a.parquet".to_string(),
            "s3://b/wh/t/data/b.parquet".to_string(),
        ];
        let now_reachable = reachable_with(&["s3://b/wh/t/data/b.parquet"]);

        let (doomed, reprieved) = reverify(candidates, &now_reachable);
        assert_eq!(doomed, vec!["s3://b/wh/t/data/a.parquet"]);
        assert_eq!(reprieved, 1);
    }

    #[test]
    fn reverification_compares_normalized_paths() {
        // The listing spells it `s3://`, the metadata `s3a://`. A comparison
        // that missed that would delete a live file.
        let candidates = vec!["s3://b/wh/t/data/a.parquet".to_string()];
        let now_reachable = reachable_with(&["s3a://b/wh/t//data/a.parquet"]);

        let (doomed, reprieved) = reverify(candidates, &now_reachable);
        assert!(doomed.is_empty());
        assert_eq!(reprieved, 1);
    }

    #[test]
    fn reverification_keeps_everything_when_nothing_became_reachable() {
        let candidates = vec!["s3://b/wh/t/data/a.parquet".to_string()];
        let (doomed, reprieved) = reverify(candidates.clone(), &ReachableSet::default());
        assert_eq!(doomed, candidates);
        assert_eq!(reprieved, 0);
    }
}
