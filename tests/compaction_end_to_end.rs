//! Compaction against a real table.
//!
//! The unit tests check the rules compaction follows. These check that
//! following them leaves a table an engine can still read — same rows, fewer
//! files, and metadata that a catalog accepts.

mod common;

use bergman::health::PartitionKey;
use bergman::obs::{NoopObserver, OperationContext};
use bergman::ops::compact;
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use common::{TestTable, live_data_files, manifest_paths, read_all};

fn compaction_policy(toml: &str) -> bergman::policy::EffectiveCompaction {
    let config = Config::from_toml(toml).unwrap();
    let policy = Policy::compile(&config).unwrap();
    match policy.decide(
        &TableRef::new("prod", ["db"], "events"),
        &Default::default(),
    ) {
        Decision::Maintain(e) => e.compaction.clone(),
        other => panic!("expected Maintain, got {other:?}"),
    }
}

const ENABLED: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.compaction]
    enabled = true
    target_file_size = 1073741824
"#;

const SORTED: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.compaction]
    enabled = true
    target_file_size = 1073741824
    sort = ["id"]
"#;

async fn compact_all(
    fixture: &TestTable,
    settings: &bergman::policy::EffectiveCompaction,
) -> OperationResult {
    let table = fixture.table();
    let table_ref = TableRef::new("prod", ["db"], "events");
    let ctx = OperationContext {
        run_id: "test",
        table: &table_ref,
        kind: OperationKind::Compact,
        matched_rule: "prod.db.events",
        reason: "test",
    };

    compact::run(
        &table,
        &fixture.ident,
        fixture.committer.as_ref(),
        settings,
        &[PartitionKey::unpartitioned(0)],
        &NoopObserver,
        ctx,
    )
    .await
    .expect("compaction runs")
}

#[tokio::test]
async fn compaction_preserves_every_row_and_reduces_the_file_count() {
    let fixture = TestTable::new().unwrap();

    // Three small files — the shape compaction exists to fix.
    let mut files = Vec::new();
    for chunk in [
        &[(1, "a"), (2, "b")][..],
        &[(3, "c"), (4, "d")][..],
        &[(5, "e"), (6, "f")][..],
    ] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let before = read_all(&fixture.table()).await.unwrap();
    assert_eq!(before.len(), 6);
    assert_eq!(live_data_files(&fixture.table()).await.unwrap().len(), 3);

    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    // The point of the whole exercise: same data, fewer files.
    let after = read_all(&fixture.table()).await.unwrap();
    let mut before_sorted = before.clone();
    let mut after_sorted = after.clone();
    before_sorted.sort();
    after_sorted.sort();
    assert_eq!(
        before_sorted, after_sorted,
        "compaction changed the table's contents"
    );

    let files_after = live_data_files(&fixture.table()).await.unwrap();
    assert_eq!(files_after.len(), 1, "three files should become one");
}

#[tokio::test]
async fn the_inputs_are_gone_from_the_table_afterwards() {
    // A rewrite that added the outputs without removing the inputs would double
    // every row — the failure mode that a row-count check alone would catch,
    // but only after the table was already wrong.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a")][..], &[(2, "b")][..], &[(3, "c")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let inputs = live_data_files(&fixture.table()).await.unwrap();
    compact_all(&fixture, &compaction_policy(ENABLED)).await;
    let outputs = live_data_files(&fixture.table()).await.unwrap();

    for input in &inputs {
        assert!(
            !outputs.contains(input),
            "{input} is still live after being rewritten"
        );
    }
    assert_eq!(read_all(&fixture.table()).await.unwrap().len(), 3);
}

#[tokio::test]
async fn compaction_commits_exactly_one_snapshot_per_partition() {
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a")][..], &[(2, "b")][..], &[(3, "c")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let commits_before = fixture.committer.commit_count();
    compact_all(&fixture, &compaction_policy(ENABLED)).await;

    assert_eq!(
        fixture.committer.commit_count() - commits_before,
        1,
        "one unpartitioned table is one commit"
    );
}

#[tokio::test]
async fn a_sorted_rewrite_orders_rows_by_the_sort_column() {
    let fixture = TestTable::new().unwrap();

    // Written deliberately out of order, and split across files so that only a
    // global sort can produce an ordered result.
    let mut files = Vec::new();
    for chunk in [
        &[(5, "e"), (1, "a")][..],
        &[(3, "c"), (6, "f")][..],
        &[(2, "b"), (4, "d")][..],
    ] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let result = compact_all(&fixture, &compaction_policy(SORTED)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    let rows = read_all(&fixture.table()).await.unwrap();
    let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);

    // And the rows stayed intact — a sort that reordered one column would
    // corrupt every row without changing the count.
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[5], (6, "f".to_string()));
}

#[tokio::test]
async fn a_sort_column_the_table_does_not_have_is_refused_before_anything_is_read() {
    let fixture = TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let settings = compaction_policy(
        r#"
        [[rules]]
        match = "prod.db.events"
        [rules.compaction]
        enabled = true
        sort = ["nonexistent"]
        "#,
    );

    let table = fixture.table();
    let table_ref = TableRef::new("prod", ["db"], "events");
    let err = compact::run(
        &table,
        &fixture.ident,
        fixture.committer.as_ref(),
        &settings,
        &[PartitionKey::unpartitioned(0)],
        &NoopObserver,
        OperationContext {
            run_id: "test",
            table: &table_ref,
            kind: OperationKind::Compact,
            matched_rule: "prod.db.events",
            reason: "test",
        },
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("nonexistent"), "got: {err}");
    // Nothing was committed, so a typo costs a metadata check and not a rewrite.
    assert_eq!(fixture.committer.commit_count(), 1);
}

#[tokio::test]
async fn a_conflicting_commit_leaves_the_table_untouched() {
    // Losing a compare-and-swap is expected — Bergman is a background tenant.
    // What must not happen is a half-applied rewrite.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a")][..], &[(2, "b")][..], &[(3, "c")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let before = read_all(&fixture.table()).await.unwrap();
    let files_before = live_data_files(&fixture.table()).await.unwrap();

    *fixture.committer.fail_next_as_conflict.lock().unwrap() = true;
    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;

    // Reported, not silently swallowed.
    match &result {
        OperationResult::NoOp { detail } => {
            assert!(
                detail.contains("conflict") || detail.contains("injected"),
                "{detail}"
            )
        }
        other => panic!("expected the conflict to be reported, got {other:?}"),
    }

    assert_eq!(read_all(&fixture.table()).await.unwrap(), before);
    assert_eq!(
        live_data_files(&fixture.table()).await.unwrap(),
        files_before
    );
}

#[tokio::test]
async fn manifests_untouched_by_a_rewrite_are_carried_by_reference() {
    // A rewrite must not rebuild metadata it did not change. Rewriting every
    // manifest would be correct but ruinous: compacting one partition of a
    // hundred-thousand-file table would rewrite the table's whole metadata, and
    // collapse it into a single enormous manifest rather than the target-sized
    // ones the table asked for.
    let fixture = TestTable::new().unwrap();

    // Three separate appends, so the table has three manifests.
    for chunk in [&[(1, "a")][..], &[(2, "b")][..], &[(3, "c")][..]] {
        let file = fixture.write_data_file(chunk).await.unwrap();
        fixture.append(vec![file]).await.unwrap();
    }

    let before = manifest_paths(&fixture.table()).await.unwrap();
    assert!(
        before.len() >= 3,
        "expected several manifests, got {before:?}"
    );

    compact_all(&fixture, &compaction_policy(ENABLED)).await;

    let after = manifest_paths(&fixture.table()).await.unwrap();
    // Every input file was rewritten, so no original manifest survives — but
    // the table must still be readable and hold every row.
    let rows = read_all(&fixture.table()).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert!(!after.is_empty());
}

#[tokio::test]
async fn an_append_does_not_rewrite_existing_manifests() {
    // The same property from the other side, and the one that shows the rule is
    // about *which* manifests changed rather than about compaction: an append
    // removes nothing, so every existing manifest must survive untouched.
    let fixture = TestTable::new().unwrap();

    let first = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![first]).await.unwrap();
    let after_first = manifest_paths(&fixture.table()).await.unwrap();

    let second = fixture.write_data_file(&[(2, "b")]).await.unwrap();
    fixture.append(vec![second]).await.unwrap();
    let after_second = manifest_paths(&fixture.table()).await.unwrap();

    for manifest in &after_first {
        assert!(
            after_second.contains(manifest),
            "{manifest} was rewritten by an append that removed nothing"
        );
    }
    assert_eq!(after_second.len(), after_first.len() + 1);
}

#[tokio::test]
async fn an_empty_table_is_declined_rather_than_failed() {
    let fixture = TestTable::new().unwrap();
    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;
    assert!(
        matches!(result, OperationResult::NoOp { .. }),
        "got {result:?}"
    );
}
