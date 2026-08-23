//! Orphan removal against a real table.
//!
//! This is the operation that can destroy a healthy table. Its rules are unit
//! tested; what follows checks them against a table with real files on disk —
//! that a live file survives, that a young one survives, and that the scanner
//! declines rather than guessing when something looks wrong.

mod common;

use bergman::obs::OperationContext;
use bergman::ops::orphans;
use bergman::ops::store::{InMemoryStore, ObjectStore};
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use chrono::{Duration, Utc};
use common::{TestTable, live_data_files, read_all};

fn orphan_policy(toml: &str) -> bergman::policy::EffectiveOrphans {
    let config = Config::from_toml(toml).unwrap();
    let policy = Policy::compile(&config).unwrap();
    match policy.decide(
        &TableRef::new("prod", ["db"], "events"),
        &Default::default(),
    ) {
        Decision::Maintain(e) => e.orphans.clone(),
        other => panic!("expected Maintain, got {other:?}"),
    }
}

const DELETING: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.orphans]
    enabled = true
    mode = "delete"
    older_than = "7d"
"#;

const DRY_RUN: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.orphans]
    enabled = true
    older_than = "7d"
"#;

/// A store holding the table's real files plus whatever else a test adds.
async fn store_for(fixture: &TestTable, extra: &[(&str, chrono::DateTime<Utc>)]) -> InMemoryStore {
    let store = InMemoryStore::new();
    let table = fixture.table();
    let old = Utc::now() - Duration::days(30);

    // Everything the table actually references, aged well past the grace
    // period — so only the scanner's reachability check can save them.
    for path in live_data_files(&table).await.unwrap() {
        store.insert(&path, 1024, Some(old));
    }
    if let Some(snapshot) = table.metadata().current_snapshot() {
        store.insert(snapshot.manifest_list(), 512, Some(old));
    }

    for (path, modified) in extra {
        store.insert(path, 2048, Some(*modified));
    }
    store
}

async fn scan(
    fixture: &TestTable,
    store: &dyn ObjectStore,
    settings: &bergman::policy::EffectiveOrphans,
) -> bergman::Result<OperationResult> {
    let table = fixture.table();
    let table_ref = TableRef::new("prod", ["db"], "events");

    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        OperationContext {
            run_id: "test",
            table: &table_ref,
            kind: OperationKind::RemoveOrphans,
            matched_rule: "prod.db.events",
            reason: "test",
        },
    );

    orphans::run(&env, store, settings, &[]).await
}

async fn seeded() -> TestTable {
    let fixture = TestTable::new().unwrap();
    let file = fixture
        .write_data_file(&[(1, "a"), (2, "b")])
        .await
        .unwrap();
    fixture.append(vec![file]).await.unwrap();
    fixture
}

#[tokio::test]
async fn a_live_data_file_is_never_deleted_however_old() {
    // The whole point. Every file the table references was aged 30 days in the
    // fixture, so age alone would condemn all of them — only reachability
    // saves them.
    let fixture = seeded().await;
    let store = store_for(&fixture, &[]).await;

    let before = store.paths();
    let result = scan(&fixture, &store, &orphan_policy(DELETING))
        .await
        .unwrap();

    assert!(
        matches!(result, OperationResult::NoOp { .. }),
        "nothing should be deleted, got {result:?}"
    );
    assert_eq!(store.paths(), before);
    assert_eq!(read_all(&fixture.table()).await.unwrap().len(), 2);
}

#[tokio::test]
async fn an_old_unreferenced_file_is_deleted() {
    let fixture = seeded().await;
    let orphan = format!("{}/data/left-behind.parquet", fixture.location());
    let store = store_for(&fixture, &[(&orphan, Utc::now() - Duration::days(30))]).await;

    let result = scan(&fixture, &store, &orphan_policy(DELETING))
        .await
        .unwrap();

    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );
    assert!(
        !store.paths().iter().any(|p| p.contains("left-behind")),
        "the orphan survived: {:?}",
        store.paths()
    );
    // And the table is untouched.
    assert_eq!(read_all(&fixture.table()).await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_young_unreferenced_file_survives_the_grace_period() {
    // A writer stages files before the commit that references them. This file
    // is indistinguishable from one mid-write, and deleting it would corrupt a
    // table that was doing nothing wrong.
    let fixture = seeded().await;
    let in_flight = format!("{}/data/being-written.parquet", fixture.location());
    let store = store_for(&fixture, &[(&in_flight, Utc::now() - Duration::hours(1))]).await;

    scan(&fixture, &store, &orphan_policy(DELETING))
        .await
        .unwrap();

    assert!(
        store.paths().iter().any(|p| p.contains("being-written")),
        "an hour-old file was deleted"
    );
}

#[tokio::test]
async fn a_file_with_no_modification_time_survives() {
    // A store that will not say how old a file is cannot be used to argue that
    // it is old enough to delete.
    let fixture = seeded().await;
    let store = store_for(&fixture, &[]).await;
    store.insert(
        &format!("{}/data/ageless.parquet", fixture.location()),
        1,
        None,
    );

    scan(&fixture, &store, &orphan_policy(DELETING))
        .await
        .unwrap();

    assert!(store.paths().iter().any(|p| p.contains("ageless")));
}

#[tokio::test]
async fn a_dry_run_reports_without_deleting() {
    let fixture = seeded().await;
    let orphan = format!("{}/data/left-behind.parquet", fixture.location());
    let store = store_for(&fixture, &[(&orphan, Utc::now() - Duration::days(30))]).await;

    let result = scan(&fixture, &store, &orphan_policy(DRY_RUN))
        .await
        .unwrap();

    match &result {
        OperationResult::NoOp { detail } => {
            assert!(detail.contains("dry run"), "{detail}");
            assert!(
                detail.contains('1'),
                "should have found one orphan: {detail}"
            );
        }
        other => panic!("expected a dry-run report, got {other:?}"),
    }
    assert!(store.paths().iter().any(|p| p.contains("left-behind")));
}

#[tokio::test]
async fn a_file_belonging_to_a_similarly_named_table_is_not_touched() {
    // `…/events` and `…/events_archive` share a string prefix, and an object
    // store matches prefixes as raw strings.
    let fixture = seeded().await;
    let store = store_for(&fixture, &[]).await;

    let neighbour = format!("{}_archive/data/live.parquet", fixture.location());
    store.insert(&neighbour, 4096, Some(Utc::now() - Duration::days(30)));

    scan(&fixture, &store, &orphan_policy(DELETING))
        .await
        .unwrap();

    assert!(
        store.paths().iter().any(|p| p.contains("_archive")),
        "a neighbouring table's file was deleted"
    );
}

#[tokio::test]
async fn an_age_below_the_floor_is_refused_even_from_the_library() {
    // The floor is validated when a config is parsed, but the library API is a
    // second entry point — a safety rule enforced at one of two is one with a
    // hole in it.
    let fixture = seeded().await;
    let store = store_for(&fixture, &[]).await;

    let mut settings = orphan_policy(DELETING);
    settings.older_than.value = std::time::Duration::from_secs(60);

    let err = scan(&fixture, &store, &settings).await.unwrap_err();
    assert!(err.to_string().contains("floor"), "got: {err}");
}

#[tokio::test]
async fn a_table_whose_metadata_reaches_nothing_is_refused() {
    // Far more likely than "this table is entirely garbage" is that something
    // went wrong reading it. Deleting on that basis would empty the warehouse.
    let fixture = seeded().await;

    // A store that reports the table's files but whose metadata cannot be
    // walked is simulated by pointing the scan at a table whose manifest list
    // is missing from disk.
    let table = fixture.table();
    let manifest_list = table.metadata().current_snapshot().unwrap().manifest_list();
    let path = manifest_list.trim_start_matches("file://");
    std::fs::remove_file(path).expect("remove the manifest list");

    let store = InMemoryStore::new();
    store.insert(
        &format!("{}/data/whatever.parquet", fixture.location()),
        1,
        Some(Utc::now() - Duration::days(30)),
    );

    let err = scan(&fixture, &store, &orphan_policy(DELETING))
        .await
        .unwrap_err();

    // Aborted rather than treating an unreadable reachable set as an empty one.
    assert!(
        !store.paths().is_empty(),
        "files were deleted on a failed walk"
    );
    let _ = err;
}

#[tokio::test]
async fn nothing_happens_when_the_scanner_is_disabled() {
    let settings = orphan_policy(
        r#"
        [[rules]]
        match = "prod.db.events"
        "#,
    );
    assert!(!settings.enabled.value);
}

#[tokio::test]
async fn a_table_living_inside_this_one_is_detected_from_the_listing_alone() {
    // Check 5, and the half of it that has to hold. A nested table's live files
    // are inside this location and unreachable from this table's metadata,
    // which is the definition of an orphan and exactly wrong; containment
    // cannot catch it.
    //
    // The sibling list is empty here, as it is for the table most likely to be
    // nested: one no rule matches, or one a rule skips. The scan must still
    // refuse.
    let fixture = seeded().await;
    let store = store_for(&fixture, &[]).await;

    let old = Utc::now() - Duration::days(30);
    let nested = format!("{}/sub", fixture.location());
    store.insert(
        &format!("{nested}/metadata/00000-abc.metadata.json"),
        512,
        Some(old),
    );
    store.insert(&format!("{nested}/data/live.parquet"), 4096, Some(old));

    let err = scan(&fixture, &store, &orphan_policy(DELETING))
        .await
        .expect_err("a scan that would eat a nested table must be refused");

    assert!(err.to_string().contains("/sub"), "got: {err}");
    assert!(err.to_string().contains("lives inside"), "got: {err}");

    // And nothing was deleted on the way to that conclusion.
    assert!(
        store.paths().iter().any(|p| p.contains("/sub/data/")),
        "the nested table's data was deleted"
    );
}

#[tokio::test]
async fn this_tables_own_metadata_does_not_look_like_a_nested_table() {
    // The document that proves a nested table is the same shape as the one
    // every table has; what tells them apart is depth. Getting that backwards
    // would refuse every table there is.
    let fixture = seeded().await;
    let store = store_for(&fixture, &[]).await;
    store.insert(
        &format!("{}/metadata/00000-abc.metadata.json", fixture.location()),
        512,
        Some(Utc::now() - Duration::days(30)),
    );

    let result = scan(&fixture, &store, &orphan_policy(DRY_RUN))
        .await
        .unwrap();
    assert!(
        matches!(result, OperationResult::NoOp { .. }),
        "got {result:?}"
    );
}

#[tokio::test]
async fn the_blast_radius_ceiling_leaves_the_rest_for_the_next_scan() {
    // However wrong everything upstream of this turns out to be, one scan
    // deletes at most `limits.max_deletes_per_run` — and says so. Silence here
    // would report "everything deleted" while leaving most of the garbage in
    // place, and would hide that something produced far more orphans than a
    // healthy table ever does.
    let fixture = seeded().await;
    let store = store_for(&fixture, &[]).await;

    let old = Utc::now() - Duration::days(30);
    for i in 0..10 {
        store.insert(
            &format!("{}/data/orphan-{i}.parquet", fixture.location()),
            1024,
            Some(old),
        );
    }

    let table = fixture.table();
    let table_ref = TableRef::new("prod", ["db"], "events");
    let loader = fixture.loader();
    let mut env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        OperationContext {
            run_id: "test",
            table: &table_ref,
            kind: OperationKind::RemoveOrphans,
            matched_rule: "prod.db.events",
            reason: "test",
        },
    );
    env.max_deletes_per_run = 4;

    let result = orphans::run(&env, &store, &orphan_policy(DELETING), &[])
        .await
        .unwrap();

    let detail = match &result {
        OperationResult::Succeeded { detail } => detail.clone(),
        other => panic!("expected success, got {other:?}"),
    };
    assert!(detail.contains("4 orphans deleted"), "got: {detail}");
    assert!(detail.contains("6 left for the next scan"), "got: {detail}");

    let left = store
        .paths()
        .iter()
        .filter(|p| p.contains("orphan-"))
        .count();
    assert_eq!(left, 6, "the ceiling must withhold rather than delete");
}
