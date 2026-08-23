//! Snapshot expiration against a real table.
//!
//! With `delete_files` on, this is the second operation that removes files from
//! object storage — and the only one that decides *which* by diffing
//! reachability across a commit. A diff computed against the wrong side deletes
//! files the table still reads, so what follows checks the table afterwards
//! rather than only the snapshot count.

mod common;

use std::time::Duration;

use bergman::obs::{NoopObserver, OperationContext};
use bergman::ops::expire;
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use chrono::Utc;
use common::{TestTable, live_data_files, read_all};

fn snapshot_policy(toml: &str) -> bergman::policy::EffectiveSnapshots {
    let config = Config::from_toml(toml).unwrap();
    let policy = Policy::compile(&config).unwrap();
    match policy.decide(
        &TableRef::new("prod", ["db"], "events"),
        &Default::default(),
    ) {
        Decision::Maintain(e) => e.snapshots.clone(),
        other => panic!("expected Maintain, got {other:?}"),
    }
}

/// Expire everything older than an instant, keeping one snapshot.
const AGGRESSIVE: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.snapshots]
    max_age = "1s"
    min_to_keep = 1
"#;

/// The same, and reclaim the files it orphans.
const AGGRESSIVE_DELETING: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.snapshots]
    max_age = "1s"
    min_to_keep = 1
    delete_files = true
"#;

async fn expire_now(
    fixture: &TestTable,
    settings: &bergman::policy::EffectiveSnapshots,
) -> OperationResult {
    let table = fixture.table();
    let table_ref = TableRef::new("prod", ["db"], "events");

    expire::run(
        &table,
        &fixture.catalog(),
        settings,
        &NoopObserver,
        OperationContext {
            run_id: "test",
            table: &table_ref,
            kind: OperationKind::ExpireSnapshots,
            matched_rule: "prod.db.events",
            reason: "test",
        },
        // An hour ahead, so every snapshot the fixture just wrote is older than
        // the cutoff without the test having to sleep.
        Utc::now() + chrono::Duration::hours(1),
    )
    .await
    .expect("expiration runs")
}

/// A table with `snapshots` appends, each adding one file.
async fn with_snapshots(count: usize) -> TestTable {
    let fixture = TestTable::new().unwrap();
    for i in 0..count {
        let file = fixture.write_data_file(&[(i as i32, "row")]).await.unwrap();
        fixture.append(vec![file]).await.unwrap();
    }
    fixture
}

#[tokio::test]
async fn expiring_snapshots_leaves_the_table_readable() {
    let fixture = with_snapshots(4).await;

    let rows_before = read_all(&fixture.table()).await.unwrap();
    let files_before = live_data_files(&fixture.table()).await.unwrap();
    assert_eq!(fixture.table().metadata().snapshots().len(), 4);

    let result = expire_now(&fixture, &snapshot_policy(AGGRESSIVE)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    // Expiration removes *history*, never data: every row and every live file
    // must survive.
    assert_eq!(fixture.table().metadata().snapshots().len(), 1);
    assert_eq!(read_all(&fixture.table()).await.unwrap(), rows_before);
    assert_eq!(
        live_data_files(&fixture.table()).await.unwrap(),
        files_before
    );
}

#[tokio::test]
async fn the_current_snapshot_is_never_expired() {
    // Expiring it would leave the table pointing at nothing.
    let fixture = with_snapshots(3).await;
    let current = fixture.table().metadata().current_snapshot_id();

    expire_now(&fixture, &snapshot_policy(AGGRESSIVE)).await;

    assert_eq!(fixture.table().metadata().current_snapshot_id(), current);
}

#[tokio::test]
async fn a_table_with_only_one_snapshot_is_left_alone() {
    let fixture = with_snapshots(1).await;

    let result = expire_now(&fixture, &snapshot_policy(AGGRESSIVE)).await;

    // Nothing was expirable, and that is a no-op rather than a success — a
    // report that claimed otherwise would overstate what maintenance achieved.
    assert!(
        matches!(result, OperationResult::NoOp { .. }),
        "got {result:?}"
    );
    assert_eq!(fixture.table().metadata().snapshots().len(), 1);
}

#[tokio::test]
async fn the_retention_floor_is_respected() {
    let fixture = with_snapshots(5).await;

    let settings = snapshot_policy(
        r#"
        [[rules]]
        match = "prod.db.events"
        [rules.snapshots]
        max_age = "1s"
        min_to_keep = 3
        "#,
    );
    expire_now(&fixture, &settings).await;

    // Age alone would take all but one; the floor keeps three.
    assert_eq!(fixture.table().metadata().snapshots().len(), 3);
}

#[tokio::test]
async fn file_cleanup_deletes_only_what_became_unreachable() {
    // The dangerous half. Every file the *current* snapshot reads must survive;
    // only metadata the expired snapshots alone referenced may go.
    let fixture = with_snapshots(4).await;

    let live = live_data_files(&fixture.table()).await.unwrap();
    let rows_before = read_all(&fixture.table()).await.unwrap();

    let result = expire_now(&fixture, &snapshot_policy(AGGRESSIVE_DELETING)).await;
    match &result {
        OperationResult::Succeeded { detail } => {
            assert!(detail.contains("files deleted"), "{detail}");
        }
        other => panic!("got {other:?}"),
    }

    // Every live data file is still on disk...
    for path in &live {
        let on_disk = path.trim_start_matches("file://");
        assert!(
            std::path::Path::new(on_disk).exists(),
            "{path} was deleted but the table still reads it"
        );
    }

    // ...and the table still reads correctly, which is the property that
    // matters and the one a file-existence check alone would not prove.
    assert_eq!(read_all(&fixture.table()).await.unwrap(), rows_before);
}

#[tokio::test]
async fn expiration_without_cleanup_touches_no_file() {
    // The default. Files are left for the orphan scanner, so a run that deleted
    // anything here would be ignoring the setting.
    let fixture = with_snapshots(4).await;

    let manifest_lists: Vec<String> = fixture
        .table()
        .metadata()
        .snapshots()
        .map(|s| s.manifest_list().to_string())
        .collect();

    expire_now(&fixture, &snapshot_policy(AGGRESSIVE)).await;

    for path in &manifest_lists {
        let on_disk = path.trim_start_matches("file://");
        assert!(
            std::path::Path::new(on_disk).exists(),
            "{path} was deleted with delete_files off"
        );
    }
}

#[tokio::test]
async fn a_conflicting_commit_is_reported_rather_than_retried_forever() {
    let fixture = with_snapshots(4).await;
    let rows_before = read_all(&fixture.table()).await.unwrap();

    // Every attempt loses, so the operation gives up for this cycle.
    *fixture.committer.always_conflict.lock().unwrap() = true;

    let result = expire_now(&fixture, &snapshot_policy(AGGRESSIVE)).await;

    match &result {
        OperationResult::Conflicted { detail } => {
            assert!(detail.contains("replan"), "{detail}");
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert_eq!(read_all(&fixture.table()).await.unwrap(), rows_before);
}

#[tokio::test]
async fn expiration_is_on_by_default_but_deletion_is_not() {
    let settings = snapshot_policy(
        r#"
        [[rules]]
        match = "prod.db.events"
        "#,
    );

    // Unbounded snapshot growth is the most common Iceberg health problem, and
    // metadata-only expiration writes no data files.
    assert!(settings.enabled.value);
    // Deleting them is a second, explicit decision.
    assert!(!settings.delete_files.value);
    assert_eq!(
        settings.max_age.value,
        Duration::from_secs(5 * 24 * 60 * 60)
    );
}
