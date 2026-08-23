//! Manifest rewriting against a real table.
//!
//! The operation is pure metadata: the same entries, re-packed into fewer
//! manifests. That makes the property to check unusually strict — the table's
//! contents must be **bit-for-bit** what they were, and only the metadata
//! layout may differ.

mod common;

use bergman::obs::OperationContext;
use bergman::ops::manifests;
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use common::{TestTable, live_data_files, read_all};

fn manifest_policy(toml: &str) -> bergman::policy::EffectiveManifests {
    let config = Config::from_toml(toml).unwrap();
    let policy = Policy::compile(&config).unwrap();
    match policy.decide(
        &TableRef::new("prod", ["db"], "events"),
        &Default::default(),
    ) {
        Decision::Maintain(e) => e.manifests.clone(),
        other => panic!("expected Maintain, got {other:?}"),
    }
}

/// Rewriting is on, every manifest counts as undersized, and two are enough to
/// merge — so a table with several manifests is always a candidate.
const EAGER: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.manifests]
    rewrite = true
    target_size = 8388608
    min_count_to_merge = 2
"#;

async fn rewrite(
    fixture: &TestTable,
    settings: &bergman::policy::EffectiveManifests,
) -> OperationResult {
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
            kind: OperationKind::RewriteManifests,
            matched_rule: "prod.db.events",
            reason: "test",
        },
    );

    manifests::run(&env, settings)
        .await
        .expect("manifest rewrite runs")
}

/// The same, without unwrapping — for the paths that must refuse.
async fn try_rewrite(
    fixture: &TestTable,
    settings: &bergman::policy::EffectiveManifests,
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
            kind: OperationKind::RewriteManifests,
            matched_rule: "prod.db.events",
            reason: "test",
        },
    );
    manifests::run(&env, settings).await
}

/// Append each file in its own snapshot, so the table accumulates manifests.
async fn append_separately(fixture: &TestTable, chunks: &[&[(i32, &str)]]) {
    for chunk in chunks {
        let file = fixture.write_data_file(chunk).await.unwrap();
        fixture.append(vec![file]).await.unwrap();
    }
}

#[tokio::test]
async fn rewriting_manifests_changes_no_row_and_no_data_file() {
    let fixture = TestTable::new().unwrap();
    append_separately(
        &fixture,
        &[&[(1, "a")], &[(2, "b")], &[(3, "c")], &[(4, "d")]],
    )
    .await;

    let rows_before = read_all(&fixture.table()).await.unwrap();
    let files_before = live_data_files(&fixture.table()).await.unwrap();

    let result = rewrite(&fixture, &manifest_policy(EAGER)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    // The strict property: same rows, and the *same data files*. A manifest
    // rewrite that touched data would be a compaction wearing the wrong name.
    assert_eq!(read_all(&fixture.table()).await.unwrap(), rows_before);
    assert_eq!(
        live_data_files(&fixture.table()).await.unwrap(),
        files_before
    );
}

#[tokio::test]
async fn a_table_with_too_few_manifests_is_left_alone() {
    // Rewriting one manifest into one manifest costs a snapshot and achieves
    // nothing. Without this the operation would fire every cycle forever.
    let fixture = TestTable::new().unwrap();
    append_separately(&fixture, &[&[(1, "a")]]).await;

    let commits_before = fixture.committer.commit_count();
    let result = rewrite(&fixture, &manifest_policy(EAGER)).await;

    assert!(
        matches!(result, OperationResult::NoOp { .. }),
        "got {result:?}"
    );
    assert_eq!(
        fixture.committer.commit_count(),
        commits_before,
        "a no-op must not commit"
    );
}

#[tokio::test]
async fn an_empty_table_is_declined() {
    let fixture = TestTable::new().unwrap();
    let result = rewrite(&fixture, &manifest_policy(EAGER)).await;
    assert!(
        matches!(result, OperationResult::NoOp { .. }),
        "got {result:?}"
    );
}

#[tokio::test]
async fn rewriting_is_off_unless_a_rule_asks_for_it() {
    let fixture = TestTable::new().unwrap();
    append_separately(&fixture, &[&[(1, "a")], &[(2, "b")], &[(3, "c")]]).await;

    let settings = manifest_policy(
        r#"
        [[rules]]
        match = "prod.db.events"
        "#,
    );
    assert!(!settings.rewrite.value);
}

#[tokio::test]
async fn a_lost_commit_is_re_packed_against_the_table_that_now_exists() {
    // The entries this re-packs are the ones the table had when the attempt
    // began. A concurrent commit changes them, so the retry must reload and
    // re-pack rather than re-offer a manifest set built from a table that has
    // since moved.
    let fixture = TestTable::new().unwrap();
    append_separately(&fixture, &[&[(1, "a")], &[(2, "b")], &[(3, "c")]]).await;

    let rows_before = read_all(&fixture.table()).await.unwrap();
    *fixture.committer.fail_next_as_conflict.lock().unwrap() = true;

    let result = rewrite(&fixture, &manifest_policy(EAGER)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "expected the retry to succeed, got {result:?}"
    );

    // And not one row moved, which is the whole promise of a manifest rewrite.
    assert_eq!(read_all(&fixture.table()).await.unwrap(), rows_before);
}

#[tokio::test]
async fn a_table_that_conflicts_every_time_is_left_untouched() {
    let fixture = TestTable::new().unwrap();
    append_separately(&fixture, &[&[(1, "a")], &[(2, "b")], &[(3, "c")]]).await;

    let rows_before = read_all(&fixture.table()).await.unwrap();
    *fixture.committer.always_conflict.lock().unwrap() = true;

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
            kind: OperationKind::RewriteManifests,
            matched_rule: "prod.db.events",
            reason: "test",
        },
    );
    let result = manifests::run(&env, &manifest_policy(EAGER))
        .await
        .expect("a conflict is reported, not raised");

    // A conflict must be reported as one, because the operator's response to a
    // conflict is to wait for the next cycle and to anything else is not.
    match &result {
        OperationResult::Conflicted { detail } => {
            assert!(detail.contains("replan"), "{detail}")
        }
        other => panic!("expected a reported conflict, got {other:?}"),
    }
    assert_eq!(read_all(&fixture.table()).await.unwrap(), rows_before);
}

#[tokio::test]
async fn the_rewritten_table_is_still_readable_after_more_writes() {
    // A rewrite that produced subtly wrong sequence numbers would not fail at
    // commit time — it would fail later, when the next writer's data interacted
    // with it. So the table is written to again afterwards and read back.
    let fixture = TestTable::new().unwrap();
    append_separately(&fixture, &[&[(1, "a")], &[(2, "b")], &[(3, "c")]]).await;

    rewrite(&fixture, &manifest_policy(EAGER)).await;

    let file = fixture.write_data_file(&[(4, "d")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let mut rows = read_all(&fixture.table()).await.unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
            (4, "d".to_string()),
        ]
    );
}

#[tokio::test]
async fn a_v3_table_is_refused_before_a_manifest_is_written() {
    // Re-packing entries produces a snapshot like any other, so the same v3
    // limit applies (see `compaction_end_to_end`). The refusal has to arrive
    // before anything is written, or every cycle would leave a fresh set of
    // Avro files under the table's metadata directory for the orphan scanner.
    let fixture = TestTable::with_format(iceberg::spec::FormatVersion::V3).unwrap();
    for i in 0..3 {
        let file = fixture.write_data_file(&[(i, "x")]).await.unwrap();
        fixture.append(vec![file]).await.unwrap();
    }

    let before_manifests = common::manifest_paths(&fixture.table())
        .await
        .unwrap()
        .len();
    let before_commits = fixture.committer.commit_count();

    let err = try_rewrite(&fixture, &manifest_policy(EAGER))
        .await
        .expect_err("a v3 manifest rewrite must be refused");

    assert!(err.to_string().contains("format v3"), "got: {err}");
    assert_eq!(fixture.committer.commit_count(), before_commits);
    assert_eq!(
        common::manifest_paths(&fixture.table())
            .await
            .unwrap()
            .len(),
        before_manifests
    );
}
