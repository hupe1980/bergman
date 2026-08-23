//! Orphan-file removal.
//!
//! This is the one operation that can destroy a healthy table, so it is the one
//! with the most machinery between a candidate and a deletion. Seven
//! independent checks stand in the way, and each exists because of a way this
//! goes wrong:
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
//! 5. **No scanning a location another table lives inside.** A table at
//!    `…/db` whose sibling sits at `…/db/events` would see every one of that
//!    table's live files as garbage: they are unreachable from *its* metadata,
//!    because they belong to somebody else. Containment (check 4) cannot catch
//!    this — the files genuinely are inside the location. This is checked two
//!    ways, and the second is the one that matters; see [`nested_table_root`].
//! 6. **Re-verification before deleting.** Metadata is reloaded after listing,
//!    and anything that became reachable in between is dropped from the kill
//!    list. A commit lands between a scan and a delete far more often than
//!    intuition suggests.
//! 7. **A ceiling on the blast radius.** However wrong everything above turns
//!    out to be, one scan deletes at most `limits.max_deletes_per_run` files and
//!    says so — the same ceiling, through the same deleter, that expiration's
//!    cleanup uses (see [`crate::ops::delete`]).

use std::collections::BTreeSet;

use futures::StreamExt;

use crate::error::{Error, Result};
use crate::ops::OpEnv;
use crate::ops::delete::{announce_and_delete, withhold_beyond};
use crate::ops::reachability::{self, ReachableSet};
use crate::ops::store::ObjectStore;
use crate::plan::OperationResult;
use crate::policy::{EffectiveOrphans, MIN_ORPHAN_AGE, OrphanMode};
use crate::util::human_bytes;

/// The suffix every Iceberg table metadata document carries.
///
/// Finding one below the table's own `metadata/` directory is what proves
/// another table lives inside this location; see [`nested_table_root`].
const METADATA_SUFFIX: &str = ".metadata.json";

/// What the scan found and did.
#[derive(Debug, Clone, Default)]
struct OrphanOutcome {
    // Objects no retained metadata references and old enough to consider.
    orphans: usize,
    // Their total size.
    orphan_bytes: u64,
    // How many were deleted.
    deleted: usize,
    // How many were dropped by the re-verification pass.
    //
    // Non-zero means a writer committed between the scan and the deletion —
    // which is exactly what check 6 exists for, and worth reporting when it
    // fires.
    reprieved: usize,
    // How many were left for the next scan by the per-run ceiling.
    withheld: usize,
    // How many deletions failed.
    failed: usize,
}

/// Scan for orphans, and delete them when policy allows.
///
/// `siblings` are the locations of the other tables Bergman has examined, which
/// is a *best-effort* input to check 5 — see [`nested_table_root`] for the check
/// that does not depend on having examined anything.
pub async fn run(
    env: &OpEnv<'_>,
    store: &dyn ObjectStore,
    settings: &EffectiveOrphans,
    siblings: &[String],
) -> Result<OperationResult> {
    let table = env.table;
    let table_ref = env.table_ref();
    let (observer, ctx, now) = (env.observer, env.ctx, env.now);

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

    // Check 5, first half: the tables Bergman happens to have examined. Cheap,
    // and it catches the case before a single object is listed.
    if let Some(nested) = nested_sibling(&location, siblings) {
        return Err(nested_refusal(table_ref, &location, nested));
    }

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

    let cutoff = now - chrono::Duration::from_std(min_age).unwrap_or(chrono::Duration::MAX);

    // The listing is consumed as a stream and reduced to candidates as it
    // arrives. Materializing it first would hold every object in a table's
    // location in memory at once — for a large table that is millions of
    // entries whose only purpose is to be filtered away.
    let mut listing = store.list(&location).await?;

    let mut scanned = 0usize;
    let mut candidates: Vec<String> = Vec::new();
    let mut orphan_bytes = 0u64;
    let mut foreign_roots: BTreeSet<String> = BTreeSet::new();

    while let Some(object) = listing.next().await {
        let object = object?;
        scanned += 1;

        // Check 5, second half, and the one that does not depend on Bergman
        // having examined anything: another table's own metadata document,
        // found under this location.
        if let Some(root) = nested_table_root(&location, &object.path) {
            foreign_roots.insert(root);
            continue;
        }

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

    if let Some(nested) = foreign_roots.iter().next() {
        return Err(nested_refusal(table_ref, &location, nested));
    }

    candidates.sort();

    let mut outcome = OrphanOutcome {
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

    // Check 6. Listing a large table takes long enough for a writer to commit,
    // and any file that commit referenced is now live.
    let fresh = env.loader.reload(env.ident).await?;
    let reachable_now = reachability::compute(table_ref, &fresh).await?;

    let (mut doomed, reprieved) = reverify(candidates, &reachable_now);
    outcome.reprieved = reprieved;

    if reprieved > 0 {
        tracing::info!(
            table = %table_ref,
            reprieved,
            "files became reachable between the scan and the deletion"
        );
    }

    // Check 7, applied before the audit record is written so that the record
    // names exactly what will be removed.
    outcome.withheld = withhold_beyond(&mut doomed, env.max_deletes_per_run);
    if outcome.withheld > 0 {
        tracing::warn!(
            table = %table_ref,
            ceiling = env.max_deletes_per_run,
            withheld = outcome.withheld,
            "more orphans than the per-run ceiling; deleting the ceiling and \
             leaving the rest for the next scan"
        );
    }

    if doomed.is_empty() {
        return Ok(OperationResult::NoOp {
            detail: format!("all {reprieved} candidates became reachable before deletion"),
        });
    }

    let deletion = announce_and_delete(store, observer, ctx, &doomed).await;
    outcome.deleted = deletion.deleted;
    outcome.failed = deletion.failed;

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
    if outcome.withheld > 0 {
        detail.push_str(&format!(
            ", {} left for the next scan by the per-run ceiling of {}",
            outcome.withheld, env.max_deletes_per_run
        ));
    }
    if outcome.failed > 0 {
        detail.push_str(&format!(", {} could not be deleted", outcome.failed));
    }

    Ok(OperationResult::Succeeded { detail })
}

/// The refusal check 5 produces, however it was detected.
fn nested_refusal(table_ref: &crate::policy::TableRef, location: &str, nested: &str) -> Error {
    Error::refused(
        "remove-orphans",
        table_ref,
        format!(
            "table {nested:?} lives inside this table's location {location:?}; \
             its live files would look like orphans here. Move one of the two, or \
             exclude this table from orphan removal."
        ),
    )
}

/// Whether any table Bergman has examined lives inside this one's location.
///
/// The cheap half of check 5, and an incomplete one: the ledger it consults
/// only holds tables this process has already loaded, so a nested table that no
/// rule matches, that a rule skips, or that simply has not been reached yet is
/// absent from it. That is not a corner case — excluding a table from
/// maintenance is exactly the reason it would be missing — which is why
/// [`nested_table_root`] exists and why it, not this, is the check that has to
/// hold.
fn nested_sibling<'a>(location: &str, siblings: &'a [String]) -> Option<&'a String> {
    siblings
        .iter()
        .find(|sibling| reachability::is_inside(location, sibling))
}

/// The root of a foreign table, if this listed path betrays one.
///
/// An Iceberg table's identity is a `metadata/…​.metadata.json` document at its
/// root. This table's own live at `<location>/metadata/…`; a document at
/// `<location>/<anything>/metadata/…` therefore belongs to a *different* table
/// whose root is `<location>/<anything>`.
///
/// This is the half of check 5 that does not depend on Bergman having examined
/// the other table — which matters because the nested table most likely to be
/// missing from the sibling ledger is the one an operator deliberately excluded
/// from maintenance, and that exclusion must not become a licence to delete it.
/// It costs nothing: the listing is already being walked.
///
/// Deliberately not a heuristic about directory names. A table's *data* may sit
/// anywhere under its location, including in a directory called `metadata`; the
/// signal is the metadata document's own suffix, which nothing but a table root
/// produces.
pub fn nested_table_root(location: &str, path: &str) -> Option<String> {
    if !path.ends_with(METADATA_SUFFIX) {
        return None;
    }

    let location = reachability::normalize(location);
    let path = reachability::normalize(path);
    let rest = path.strip_prefix(&location)?.strip_prefix('/')?;

    // This table's own metadata is at depth zero: `metadata/x.metadata.json`
    // has nothing before the directory.
    let (root, _) = rest.split_once("/metadata/")?;
    if root.is_empty() {
        return None;
    }

    Some(format!("{location}/{root}"))
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

    #[test]
    fn a_table_nested_inside_this_one_is_detected() {
        // Check 5. Every live file of the nested table is inside this location
        // and unreachable from this table's metadata — the exact definition of
        // an orphan, and exactly wrong. Containment cannot catch it.
        let siblings = vec![
            "s3://b/wh/db/events".to_string(),
            "s3://b/wh/other/orders".to_string(),
        ];
        assert_eq!(
            nested_sibling("s3://b/wh/db", &siblings),
            Some(&"s3://b/wh/db/events".to_string())
        );
    }

    #[test]
    fn a_sibling_beside_this_table_is_not_nested() {
        let siblings = vec![
            "s3://b/wh/db/orders".to_string(),
            // The prefix trap: `events_archive` shares a string prefix with
            // `events` but is not inside it.
            "s3://b/wh/db/events_archive".to_string(),
        ];
        assert_eq!(nested_sibling("s3://b/wh/db/events", &siblings), None);
    }

    #[test]
    fn a_table_does_not_nest_inside_itself() {
        // Every table is in the sibling list, including this one, and a check
        // that said "yes" would refuse every table on earth.
        let siblings = vec!["s3://b/wh/db/events".to_string()];
        assert_eq!(nested_sibling("s3://b/wh/db/events", &siblings), None);
    }

    #[test]
    fn nesting_is_detected_across_path_spellings() {
        let siblings = vec!["s3a://b/wh/db//events".to_string()];
        assert!(nested_sibling("s3://b/wh/db", &siblings).is_some());
    }

    #[test]
    fn a_foreign_metadata_document_names_the_table_that_owns_it() {
        // The half of check 5 that does not depend on having examined the other
        // table — which is the case that matters, because the nested table most
        // likely to be missing from the ledger is one an operator deliberately
        // excluded from maintenance.
        assert_eq!(
            nested_table_root(
                "s3://b/wh/db",
                "s3://b/wh/db/events/metadata/00003-abc.metadata.json"
            ),
            Some("s3://b/wh/db/events".to_string())
        );
        // Several levels down, and the root is still the table's, not the
        // directory the document sits in.
        assert_eq!(
            nested_table_root(
                "s3://b/wh",
                "s3://b/wh/db/events/metadata/00003-abc.metadata.json"
            ),
            Some("s3://b/wh/db/events".to_string())
        );
    }

    #[test]
    fn this_tables_own_metadata_is_not_a_foreign_table() {
        // The document that proves a nested table is the same shape as the one
        // every table has. What tells them apart is depth, and getting that
        // backwards would refuse every table there is.
        assert_eq!(
            nested_table_root(
                "s3://b/wh/db/events",
                "s3://b/wh/db/events/metadata/00003-abc.metadata.json"
            ),
            None
        );
    }

    #[test]
    fn ordinary_files_are_not_mistaken_for_a_table_root() {
        // Including a data file that happens to live in a directory called
        // `metadata`, which the spec permits and which a name-based heuristic
        // would trip over.
        for path in [
            "s3://b/wh/db/events/data/00000-0-abc.parquet",
            "s3://b/wh/db/events/metadata/snap-123-1-abc.avro",
            "s3://b/wh/db/events/metadata/data/part.parquet",
            "s3://b/wh/db/events/nested/metadata/part.parquet",
        ] {
            assert_eq!(
                nested_table_root("s3://b/wh/db/events", path),
                None,
                "{path} is not a table root"
            );
        }
    }

    #[test]
    fn foreign_roots_are_detected_across_path_spellings() {
        assert_eq!(
            nested_table_root(
                "s3a://b/wh/db",
                "s3://b/wh/db//events/metadata/00003-abc.metadata.json"
            ),
            Some("s3://b/wh/db/events".to_string())
        );
    }
}
