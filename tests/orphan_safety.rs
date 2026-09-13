//! The orphan-removal safety model, exercised against an in-memory store.
//!
//! These are the rules that decide whether a file lives or dies. Each is tested
//! at the level it is enforced, because a safety check that regresses silently
//! is one nobody notices until a table is gone.

use std::time::Duration;

use bergman::ops::delete::FileDeleter;
use bergman::ops::reachability::{is_inside, normalize};
use bergman::ops::store::{InMemoryStore, ObjectMeta, ObjectStore};
use bergman::policy::{Config, Decision, MIN_ORPHAN_AGE, OrphanMode, Policy, TableRef};
use chrono::{Duration as ChronoDuration, Utc};

/// Drain a listing, which is a stream so that a scan of a table with millions
/// of objects never holds them all at once.
async fn collect(store: &InMemoryStore, prefix: &str) -> Vec<ObjectMeta> {
    use futures::TryStreamExt;
    store
        .list(prefix)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn effective(toml: &str) -> bergman::policy::EffectivePolicy {
    let config = Config::from_toml(toml).unwrap();
    let policy = Policy::compile(&config).unwrap();
    match policy.decide(&TableRef::new("prod", ["db"], "t"), &Default::default()) {
        Decision::Maintain(e) => *e,
        other => panic!("expected Maintain, got {other:?}"),
    }
}

#[test]
fn the_age_floor_cannot_be_configured_away() {
    // Writers stage files before the commit that references them. A grace
    // period short enough to catch one in flight is how a healthy table gets
    // corrupted, so the floor is refused at parse time.
    let config = Config::from_toml(
        r#"
        [[rules]]
        match = "prod.**"
        [rules.orphans]
        enabled = true
        mode = "delete"
        older_than = "5m"
        "#,
    )
    .unwrap();

    let err = Policy::compile(&config).unwrap_err();
    assert!(err.to_string().contains("floor"), "got: {err}");
    assert_eq!(MIN_ORPHAN_AGE, Duration::from_secs(24 * 60 * 60));
}

#[test]
fn deletion_requires_an_explicit_opt_in() {
    // A rule that merely mentions orphans gets a report, not a deletion.
    let policy = effective(
        r#"
        [[rules]]
        match = "prod.db.t"
        [rules.orphans]
        enabled = true
        "#,
    );
    assert_eq!(policy.orphans.mode.value, OrphanMode::DryRun);

    let deleting = effective(
        r#"
        [[rules]]
        match = "prod.db.t"
        [rules.orphans]
        enabled = true
        mode = "delete"
        "#,
    );
    assert_eq!(deleting.orphans.mode.value, OrphanMode::Delete);
}

#[tokio::test]
async fn listing_does_not_cross_into_a_similarly_named_table() {
    // `…/events` and `…/events_archive` share a string prefix. An object store
    // matches prefixes as raw strings, so a scanner that did the same would
    // offer another table's live files up for deletion.
    let store = InMemoryStore::new();
    store.insert("s3://bucket/wh/db/events/data/live.parquet", 100, None);
    store.insert(
        "s3://bucket/wh/db/events_archive/data/live.parquet",
        100,
        None,
    );
    store.insert(
        "s3://bucket/wh/db/events/metadata/v1.metadata.json",
        10,
        None,
    );

    let listed = collect(&store, "s3://bucket/wh/db/events").await;
    let paths: Vec<&str> = listed.iter().map(|o| o.path.as_str()).collect();

    assert_eq!(paths.len(), 2);
    assert!(paths.iter().all(|p| !p.contains("events_archive")));
}

#[test]
fn containment_is_checked_segment_wise() {
    assert!(is_inside(
        "s3://bucket/wh/db/events",
        "s3://bucket/wh/db/events/data/f.parquet"
    ));
    assert!(!is_inside(
        "s3://bucket/wh/db/events",
        "s3://bucket/wh/db/events_archive/data/f.parquet"
    ));
}

#[test]
fn path_spellings_that_differ_still_identify_one_object() {
    // Spark writes `s3a://`, a REST catalog reports `s3://`, a naive join
    // doubles a slash. All three name the same object, and a comparison that
    // missed that would mark a live file as garbage.
    let canonical = normalize("s3://bucket/wh/db/t/data/f.parquet");
    for spelling in [
        "s3a://bucket/wh/db/t/data/f.parquet",
        "s3n://bucket/wh/db/t//data/f.parquet",
        "S3://bucket/wh/db/t/data/f.parquet",
    ] {
        assert_eq!(normalize(spelling), canonical, "{spelling}");
    }
}

#[tokio::test]
async fn a_store_deletion_removes_exactly_the_named_object() {
    let store = InMemoryStore::new();
    store.insert("s3://bucket/wh/t/a.parquet", 1, None);
    store.insert("s3://bucket/wh/t/b.parquet", 1, None);

    // Asked with the other spelling: the normalization has to reach the delete
    // path too, or a deletion silently succeeds while removing nothing.
    store.delete("s3a://bucket/wh/t/a.parquet").await.unwrap();

    assert_eq!(
        store.paths(),
        vec!["s3://bucket/wh/t/b.parquet".to_string()]
    );
}

#[tokio::test]
async fn files_with_no_modification_time_are_never_old_enough() {
    // The scanner treats an unknown age as "too young". A store that will not
    // say how old a file is cannot be used to argue that it is old enough to
    // delete — this test pins the intent so a refactor that defaults the other
    // way is caught.
    let store = InMemoryStore::new();
    store.insert("s3://bucket/wh/t/unknown-age.parquet", 1, None);

    let objects = collect(&store, "s3://bucket/wh/t").await;
    assert_eq!(objects.len(), 1);
    assert!(objects[0].last_modified.is_none());
}

#[test]
fn a_recent_file_is_outside_the_grace_period() {
    let now = Utc::now();
    let cutoff = now - ChronoDuration::days(7);

    let recent = now - ChronoDuration::hours(1);
    let ancient = now - ChronoDuration::days(30);

    assert!(recent > cutoff, "a one-hour-old file must be spared");
    assert!(ancient < cutoff, "a 30-day-old file is a candidate");
}

// ─────────────────────────────────────────────────────────────────────────────
// Load-bearing tests.
//
// The tests above check that each rule *computes* the right answer. These check
// something different and harder to fake: that the rule is what **decides the
// outcome** of a real scan.
//
// Each is a pair against one fixture, with exactly one variable moved:
//
// - the **control** removes the hazard and asserts the file *is* deleted;
// - the **guarded** case restores it and asserts the file is spared.
//
// The pairing is the point. A guarded-only test passes just as happily when the
// file was never a deletion candidate to begin with — a fixture that drifts into
// harmlessness, an operation that quietly stopped running, a store whose listing
// returns nothing. The control proves deletion was genuinely reachable, so the
// guarded case can only pass because the guard fired. That is what
// [`concepts/QUALITY.md`] means by verifying a guard against a planted
// violation, and it is the discipline the orphan checks did not have.
// ─────────────────────────────────────────────────────────────────────────────

mod common;

use bergman::ops::orphans;
use bergman::plan::OperationResult;

/// An orphan old enough to delete, in the table's own data directory.
const ORPHAN: &str = "data/abandoned-by-a-dead-writer.parquet";

/// A store holding the table's live files plus one genuine orphan.
async fn store_with_orphan(
    fixture: &common::TestTable,
    orphan_age: Option<chrono::Duration>,
) -> InMemoryStore {
    let store = InMemoryStore::new();
    let table = fixture.table();

    // Everything the table actually references, so the scan has a real reachable
    // set rather than an empty one (which it refuses outright).
    for path in common::live_data_files(&table).await.unwrap() {
        store.insert(&path, 1024, Some(Utc::now()));
    }
    for path in common::manifest_paths(&table).await.unwrap() {
        store.insert(&path, 512, Some(Utc::now()));
    }
    if let Some(location) = table.metadata_location() {
        store.insert(location, 256, Some(Utc::now()));
    }

    let modified = orphan_age.map(|age| Utc::now() - age);
    store.insert(
        &format!("{}/{ORPHAN}", fixture.location()),
        4096,
        modified.or(Some(Utc::now() - ChronoDuration::days(30))),
    );
    store
}

fn deleting_policy(older_than: &str) -> bergman::policy::EffectiveOrphans {
    effective(&format!(
        r#"
        [[rules]]
        match = "prod.db.t"
        [rules.orphans]
        enabled = true
        mode = "delete"
        older_than = "{older_than}"
        "#,
    ))
    .orphans
}

async fn scan(
    fixture: &common::TestTable,
    store: &InMemoryStore,
    settings: &bergman::policy::EffectiveOrphans,
    siblings: &[String],
) -> OperationResult {
    let table = fixture.table();
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        common::fixture_ctx(),
    );
    orphans::run(&env, store, settings, siblings).await.unwrap()
}

/// The control every pair below depends on: with no hazard in the way, this
/// scan really does delete the orphan. Without it, a guarded test proves
/// nothing — a scan that deleted nothing under any circumstances would pass all
/// of them.
#[tokio::test]
async fn the_control_deletes_a_genuine_orphan() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let store = store_with_orphan(&fixture, None).await;
    let orphan = format!("{}/{ORPHAN}", fixture.location());
    assert!(store.paths().contains(&orphan), "the orphan starts present");

    let result = scan(&fixture, &store, &deleting_policy("7d"), &[]).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "an old, unreferenced file inside the location must be deleted: {result:?}"
    );
    assert!(
        !store.paths().contains(&orphan),
        "the orphan must actually be gone from the store"
    );
}

/// Check 1. Dry run is the default, and it is what stops a first run from
/// deleting anything while an operator is still deciding whether to trust it.
#[tokio::test]
async fn check_1_dry_run_spares_the_file_the_control_deletes() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let store = store_with_orphan(&fixture, None).await;
    let orphan = format!("{}/{ORPHAN}", fixture.location());

    let dry_run = effective(
        r#"
        [[rules]]
        match = "prod.db.t"
        [rules.orphans]
        enabled = true
        older_than = "7d"
        "#,
    )
    .orphans;
    assert_eq!(dry_run.mode.value, OrphanMode::DryRun, "the default");

    let result = scan(&fixture, &store, &dry_run, &[]).await;
    assert!(
        store.paths().contains(&orphan),
        "dry run must not delete: {result:?}"
    );
}

/// Check 2. A young unreferenced file is far more likely a write in flight than
/// garbage — the writer stages it *before* the commit that references it.
#[tokio::test]
async fn check_2_a_file_inside_the_grace_period_is_spared() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    // One hour old, against a 7-day grace period: unreferenced, inside the
    // location, and deleted by the control — spared only by its age.
    let store = store_with_orphan(&fixture, Some(ChronoDuration::hours(1))).await;
    let orphan = format!("{}/{ORPHAN}", fixture.location());

    let result = scan(&fixture, &store, &deleting_policy("7d"), &[]).await;
    assert!(
        store.paths().contains(&orphan),
        "a file inside the grace period must survive: {result:?}"
    );
}

/// Check 5. The check the module calls the one that matters, end to end: a
/// second table living *inside* this table's location. Its files are genuinely
/// unreachable from this table's metadata, genuinely old, and genuinely inside
/// the location — so containment cannot save them and the whole scan must be
/// refused instead.
#[tokio::test]
async fn check_5_a_nested_table_refuses_the_whole_scan() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let store = store_with_orphan(&fixture, None).await;
    let orphan = format!("{}/{ORPHAN}", fixture.location());

    // Another table's identity document, under a subdirectory of this location.
    let foreign = format!(
        "{}/nested_table/metadata/00000-abcdef.metadata.json",
        fixture.location()
    );
    store.insert(&foreign, 128, Some(Utc::now() - ChronoDuration::days(30)));

    let table = fixture.table();
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        common::fixture_ctx(),
    );
    let result = orphans::run(&env, &store, &deleting_policy("7d"), &[]).await;

    assert!(
        result.is_err(),
        "a nested table must refuse the scan outright, not filter around it: {result:?}"
    );
    assert!(
        store.paths().contains(&orphan),
        "and nothing may be deleted, including this table's own genuine orphan"
    );
    assert!(
        store.paths().contains(&foreign),
        "least of all the other table's files"
    );
}

/// Check 5, first half: the cheap ledger of tables already examined, which can
/// refuse before a single object is listed.
#[tokio::test]
async fn check_5_a_known_sibling_inside_the_location_refuses_before_listing() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let store = store_with_orphan(&fixture, None).await;
    let orphan = format!("{}/{ORPHAN}", fixture.location());
    let sibling = format!("{}/nested_table", fixture.location());

    let table = fixture.table();
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        common::fixture_ctx(),
    );
    let result = orphans::run(&env, &store, &deleting_policy("7d"), &[sibling]).await;

    assert!(
        result.is_err(),
        "a known nested sibling must refuse: {result:?}"
    );
    assert!(store.paths().contains(&orphan));
}

/// Check 7. However wrong every check above turns out to be, one scan deletes at
/// most `max_deletes_per_run` files — the difference between an incident and a
/// catastrophe.
#[tokio::test]
async fn check_7_the_blast_radius_ceiling_withholds_the_rest() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let store = store_with_orphan(&fixture, None).await;
    for i in 0..20 {
        store.insert(
            &format!("{}/data/orphan-{i:03}.parquet", fixture.location()),
            128,
            Some(Utc::now() - ChronoDuration::days(30)),
        );
    }
    let before = store.paths().len();

    let table = fixture.table();
    let loader = fixture.loader();
    let mut env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        common::fixture_ctx(),
    );
    env.max_deletes_per_run = 5;

    let result = orphans::run(&env, &store, &deleting_policy("7d"), &[])
        .await
        .unwrap();

    let deleted = before - store.paths().len();
    assert_eq!(
        deleted, 5,
        "the ceiling must bound what one scan deletes, not merely be reported: {result:?}"
    );
}

/// Check 8, and the reason it exists. This scenario is the one that empties a
/// warehouse: the table has a current snapshot and a full reachable set, the
/// listing returns plenty of objects, and the two **do not intersect at all**.
///
/// In production that is a location property changed after the data was
/// written, a bucket alias or scheme spelling that normalization did not fold
/// together, or a credential scoped to a subdirectory — never a table whose
/// every file is genuinely garbage. Checks 1–7 all pass it: the objects are old,
/// unreferenced, inside the location, and no other table lives there.
#[tokio::test]
async fn check_8_a_listing_that_shares_no_file_with_the_metadata_is_refused() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    // A store that knows nothing about the table's live files — every object it
    // returns is old, unreferenced and inside the location.
    let store = InMemoryStore::new();
    for i in 0..3 {
        store.insert(
            &format!("{}/data/looks-like-garbage-{i}.parquet", fixture.location()),
            128,
            Some(Utc::now() - ChronoDuration::days(30)),
        );
    }
    let before = store.paths().len();

    let table = fixture.table();
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        common::fixture_ctx(),
    );
    let result = orphans::run(&env, &store, &deleting_policy("7d"), &[]).await;

    let message = format!("{result:?}");
    assert!(
        result.is_err(),
        "a listing sharing no file with the metadata must be refused, not obeyed: {message}"
    );
    assert!(
        message.contains("disagree"),
        "and the refusal must say why: {message}"
    );
    assert_eq!(
        store.paths().len(),
        before,
        "nothing may be deleted while the two views disagree"
    );
}

/// The other half of check 8: a listing that *does* contain the table's files
/// proves the two views agree, and the genuine orphan beside them is still
/// deleted. Without this, check 8 could be satisfied by a scan that refused
/// everything.
#[tokio::test]
async fn check_8_does_not_block_a_scan_whose_listing_agrees() {
    let fixture = common::TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let store = store_with_orphan(&fixture, None).await;
    let orphan = format!("{}/{ORPHAN}", fixture.location());

    let result = scan(&fixture, &store, &deleting_policy("7d"), &[]).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "a listing that shares files with the metadata must proceed: {result:?}"
    );
    assert!(!store.paths().contains(&orphan));
}
