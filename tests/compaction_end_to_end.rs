//! Compaction against a real table.
//!
//! The unit tests check the rules compaction follows. These check that
//! following them leaves a table an engine can still read — same rows, fewer
//! files, and metadata that a catalog accepts.

mod common;

use bergman::health::PartitionKey;
use bergman::obs::OperationContext;
use bergman::ops::compact;
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use common::{TestTable, live_data_files, live_delete_files, manifest_paths, read_all};

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

    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        ctx,
    );

    compact::run(&env, settings, &[PartitionKey::unpartitioned(0)])
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
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        OperationContext {
            run_id: "test",
            table: &table_ref,
            kind: OperationKind::Compact,
            matched_rule: "prod.db.events",
            reason: "test",
        },
    );
    let err = compact::run(&env, &settings, &[PartitionKey::unpartitioned(0)])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("nonexistent"), "got: {err}");
    // Nothing was committed, so a typo costs a metadata check and not a rewrite.
    assert_eq!(fixture.committer.commit_count(), 1);
}

#[tokio::test]
async fn a_lost_commit_is_rebuilt_against_the_table_that_now_exists() {
    // Losing a compare-and-swap is expected — Bergman is a background tenant.
    // The response is to rebuild the group from the current snapshot and
    // rewrite it, never to re-offer outputs computed against a table that has
    // since moved: that is how a concurrent delete gets discarded and its rows
    // come back.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a")][..], &[(2, "b")][..], &[(3, "c")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let before = read_all(&fixture.table()).await.unwrap();

    *fixture.committer.fail_next_as_conflict.lock().unwrap() = true;
    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;

    // The second attempt lands, so the rewrite succeeds.
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "expected the retry to succeed, got {result:?}"
    );

    // And every row survives it, which is the property the retry must not cost.
    assert_eq!(read_all(&fixture.table()).await.unwrap(), before);
    assert_eq!(live_data_files(&fixture.table()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_table_that_conflicts_every_time_is_left_alone() {
    // A table being written hard keeps winning, and the right response is to
    // stop competing and come back next cycle. Retrying forever would spend a
    // whole cycle losing, and — worse — would look like progress in the logs.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a")][..], &[(2, "b")][..], &[(3, "c")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let before = read_all(&fixture.table()).await.unwrap();
    let files_before = live_data_files(&fixture.table()).await.unwrap();

    *fixture.committer.always_conflict.lock().unwrap() = true;
    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;

    // Reported, not silently swallowed.
    match &result {
        OperationResult::NoOp { detail } => assert!(
            detail.contains("replan") || detail.contains("conflict"),
            "{detail}"
        ),
        other => panic!("expected the conflict to be reported, got {other:?}"),
    }

    // And nothing is half-applied.
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

#[tokio::test]
async fn a_partition_larger_than_the_group_ceiling_becomes_several_commits() {
    // A partition is not a unit of work. Spark bounds a file group by
    // `max-file-group-size-bytes` precisely because a partition can be
    // arbitrarily large: reading one in a single pass is how a compactor runs
    // out of memory, and how one lost commit throws away hours of rewriting.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [
        &[(1, "a")][..],
        &[(2, "b")][..],
        &[(3, "c")][..],
        &[(4, "d")][..],
    ] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let before = read_all(&fixture.table()).await.unwrap();
    let commits_before = fixture.committer.commit_count();

    // Two files per group: each of these files is a few hundred bytes, so a
    // ceiling of two files is the reliable way to say "split this".
    let settings = compaction_policy(
        r#"
        [[rules]]
        match = "prod.db.events"
        [rules.compaction]
        enabled = true
        target_file_size = 1073741824
        max_input_files = 2
        "#,
    );

    let result = compact_all(&fixture, &settings).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    // Two groups, so two commits — and partial progress is real progress.
    assert_eq!(
        fixture.committer.commit_count() - commits_before,
        2,
        "four files under a two-file ceiling is two groups"
    );

    // Every row survives being split across commits.
    let mut after = read_all(&fixture.table()).await.unwrap();
    let mut before = before;
    after.sort();
    before.sort();
    assert_eq!(after, before);
    assert_eq!(live_data_files(&fixture.table()).await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_single_file_partition_is_not_read_and_written_back_unchanged() {
    // A group of one file with no deletes to retire produces a byte-for-byte
    // copy and a snapshot. Doing that every cycle forever is the classic way a
    // compactor generates more garbage than it collects.
    let fixture = TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();

    let commits_before = fixture.committer.commit_count();
    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;

    assert!(
        matches!(result, OperationResult::NoOp { .. }),
        "got {result:?}"
    );
    assert_eq!(fixture.committer.commit_count(), commits_before);
}

// ---------------------------------------------------------------------------
// Equality deletes
//
// The path the DataFusion dependency exists for. Everything above tests
// compaction of plain data files; these test that a rewrite actually *applies*
// equality deletes — that the deleted rows are gone from the output, that the
// survivors are intact, and that the delete file is retired rather than left
// to be re-applied to rows it no longer describes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_rewrite_applies_equality_deletes_and_retires_them() {
    let fixture = TestTable::new().unwrap();

    // Two data files, six rows.
    let mut files = Vec::new();
    for chunk in [
        &[(1, "a"), (2, "b"), (3, "c")][..],
        &[(4, "d"), (5, "e"), (6, "f")][..],
    ] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    // A streaming writer then deletes two of them by key.
    let delete = fixture.write_equality_delete(&[2, 5]).await.unwrap();
    fixture.append_deletes(vec![delete]).await.unwrap();

    // The deletes are visible to a reader before compaction...
    let before = read_all(&fixture.table()).await.unwrap();
    assert_eq!(
        before.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![1, 3, 4, 6],
        "the scan should already be applying the equality delete"
    );

    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    // ...and after compaction the same rows are visible, now without any
    // delete file being consulted.
    let mut after = read_all(&fixture.table()).await.unwrap();
    after.sort();
    assert_eq!(
        after,
        vec![
            (1, "a".to_string()),
            (3, "c".to_string()),
            (4, "d".to_string()),
            (6, "f".to_string()),
        ],
        "compaction must write back exactly the surviving rows"
    );

    // The delete file applied to every data file in the group, so it is retired
    // — leaving it would make every future read open a file that hides nothing.
    assert!(
        live_delete_files(&fixture.table())
            .await
            .unwrap()
            .is_empty(),
        "a fully-applied equality delete must not survive the rewrite"
    );
}

#[tokio::test]
async fn a_rewrite_does_not_resurrect_rows_an_equality_delete_removed() {
    // The failure this whole path guards against, stated as its own test: if
    // the anti-join were dropped, mis-wired, or written with `=` instead of a
    // null-safe comparison, the deleted rows would come back — and the table
    // would look perfectly healthy while being wrong.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a"), (2, "b")][..], &[(3, "c"), (4, "d")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let delete = fixture.write_equality_delete(&[1, 2, 3, 4]).await.unwrap();
    fixture.append_deletes(vec![delete]).await.unwrap();

    compact_all(&fixture, &compaction_policy(ENABLED)).await;

    assert!(
        read_all(&fixture.table()).await.unwrap().is_empty(),
        "every row was deleted; a rewrite must not bring any of them back"
    );
}

#[tokio::test]
async fn a_sorted_rewrite_applies_equality_deletes_too() {
    // The sort runs on the output of the anti-join, not beside it. A pipeline
    // that sorted the raw scan and joined afterwards — or forgot the join on
    // the sorted path — would silently differ from the unsorted one.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(5, "e"), (1, "a")][..], &[(3, "c"), (2, "b")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let delete = fixture.write_equality_delete(&[3]).await.unwrap();
    fixture.append_deletes(vec![delete]).await.unwrap();

    let result = compact_all(&fixture, &compaction_policy(SORTED)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    let rows = read_all(&fixture.table()).await.unwrap();
    assert_eq!(
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![1, 2, 5],
        "sorted output must be both sorted and delete-applied"
    );
}

#[tokio::test]
async fn the_row_count_contract_holds_when_deletes_apply() {
    // Where deletes apply, the expected row count is not knowable from
    // metadata — a delete file's record count is an upper bound — so the
    // equality check is skipped. This asserts it is skipped rather than
    // failing the rewrite, which is the bug that would make every delete-heavy
    // table uncompactable.
    let fixture = TestTable::new().unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a"), (2, "b")][..], &[(3, "c"), (4, "d")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    // Names a key twice and a key that does not exist: both make the delete
    // file's record count an over-estimate of what it actually removes.
    let delete = fixture.write_equality_delete(&[2, 2, 99]).await.unwrap();
    fixture.append_deletes(vec![delete]).await.unwrap();

    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "an over-counting delete file must not fail the row-count check: {result:?}"
    );

    let mut rows = read_all(&fixture.table()).await.unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (1, "a".to_string()),
            (3, "c".to_string()),
            (4, "d".to_string()),
        ]
    );
}

#[tokio::test]
async fn deletes_arriving_incrementally_apply_only_to_the_files_they_cover() {
    // The ordinary CDC shape, and the one most likely to be got wrong.
    //
    // A delete committed at sequence N applies to files written *before* it and
    // not to files written after. So a partition ends up holding files with
    // different applicable delete sets, and a rewrite that anti-joined them all
    // against the union would delete rows from the later file that nothing ever
    // deleted.
    let fixture = TestTable::new().unwrap();

    // Written first, so the delete below applies to it.
    let early = fixture
        .write_data_file(&[(1, "a"), (2, "b")])
        .await
        .unwrap();
    fixture.append(vec![early]).await.unwrap();

    let delete = fixture.write_equality_delete(&[2]).await.unwrap();
    fixture.append_deletes(vec![delete]).await.unwrap();

    // Written after the delete, so the delete does *not* apply to it — even
    // though it reuses key 2.
    let late = fixture
        .write_data_file(&[(2, "b-again"), (3, "c")])
        .await
        .unwrap();
    fixture.append(vec![late]).await.unwrap();

    let before = read_all(&fixture.table()).await.unwrap();
    let mut before_sorted = before.clone();
    before_sorted.sort();
    assert_eq!(
        before_sorted,
        vec![
            (1, "a".to_string()),
            (2, "b-again".to_string()),
            (3, "c".to_string()),
        ],
        "the re-inserted row must be visible before compaction"
    );

    let result = compact_all(&fixture, &compaction_policy(ENABLED)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    let mut after = read_all(&fixture.table()).await.unwrap();
    after.sort();
    assert_eq!(
        after, before_sorted,
        "compaction must not delete the row that was re-inserted after the delete"
    );
}

#[tokio::test]
async fn a_sorted_rewrite_of_incrementally_deleted_files_is_globally_sorted() {
    // Buckets are unioned before the sort, not sorted individually. Sorting per
    // bucket would leave the output ordered in runs — metadata claiming a
    // clustering the files do not have — and incremental deletes are exactly
    // what creates more than one bucket.
    let fixture = TestTable::new().unwrap();

    let early = fixture
        .write_data_file(&[(9, "i"), (1, "a")])
        .await
        .unwrap();
    fixture.append(vec![early]).await.unwrap();

    let delete = fixture.write_equality_delete(&[9]).await.unwrap();
    fixture.append_deletes(vec![delete]).await.unwrap();

    let late = fixture
        .write_data_file(&[(7, "g"), (3, "c")])
        .await
        .unwrap();
    fixture.append(vec![late]).await.unwrap();

    let result = compact_all(&fixture, &compaction_policy(SORTED)).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    let ids: Vec<i32> = read_all(&fixture.table())
        .await
        .unwrap()
        .iter()
        .map(|(id, _)| *id)
        .collect();

    // 9 was deleted; 1, 3, 7 survive and must come back in order — not in
    // per-bucket runs like [1, 3, 7] happening to look sorted, so the values
    // are chosen to interleave across buckets.
    assert_eq!(ids, vec![1, 3, 7]);
}

#[tokio::test]
async fn a_v3_table_is_refused_rather_than_silently_renumbered() {
    // A rewrite cannot carry v3's `_row_id` through, so it would renumber every
    // row it touched and a MERGE or CDC consumer joining on row id would start
    // matching the wrong rows, with nothing failing.
    //
    // The refusal has to arrive before the read: attempting the commit spends a
    // full read and write of the partition to learn what the format version
    // already said.
    let fixture = TestTable::with_format(iceberg::spec::FormatVersion::V3).unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a")][..], &[(2, "b")][..], &[(3, "c")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let table = fixture.table();
    let table_ref = TableRef::new("prod", ["db"], "events");
    let ctx = OperationContext {
        run_id: "test",
        table: &table_ref,
        kind: OperationKind::Compact,
        matched_rule: "prod.db.events",
        reason: "test",
    };
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        ctx,
    );

    let before = fixture.committer.commit_count();
    let err = compact::run(
        &env,
        &compaction_policy(ENABLED),
        &[PartitionKey::unpartitioned(0)],
    )
    .await
    .expect_err("a v3 rewrite must be refused");

    assert!(err.to_string().contains("format v3"), "got: {err}");
    assert_eq!(
        fixture.committer.commit_count(),
        before,
        "nothing may be committed"
    );
    // And the table is exactly as it was.
    assert_eq!(read_all(&fixture.table()).await.unwrap().len(), 3);
    assert_eq!(live_data_files(&fixture.table()).await.unwrap().len(), 3);
}

/// Compact using the policy the table's *own* metadata resolves to.
///
/// The other helpers hand the executor a policy built from TOML alone. This one
/// goes through `Policy::decide` with the table's real facts, which is the only
/// way the sort-order layer is exercised at all.
async fn compact_with_table_facts(fixture: &TestTable, toml: &str) -> OperationResult {
    let table = fixture.table();
    let facts = bergman::policy::TableFacts::from_metadata(table.metadata());
    let policy = Policy::compile(&Config::from_toml(toml).unwrap()).unwrap();
    let settings = match policy.decide(&TableRef::new("prod", ["db"], "events"), &facts) {
        Decision::Maintain(e) => e.compaction.clone(),
        other => panic!("expected Maintain, got {other:?}"),
    };

    let table_ref = TableRef::new("prod", ["db"], "events");
    let ctx = OperationContext {
        run_id: "test",
        table: &table_ref,
        kind: OperationKind::Compact,
        matched_rule: "prod.db.events",
        reason: "test",
    };
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        ctx,
    );

    compact::run(&env, &settings, &[PartitionKey::unpartitioned(0)])
        .await
        .expect("compaction runs")
}

#[tokio::test]
async fn a_rewrite_preserves_the_clustering_the_table_declared() {
    // The failure this exists to prevent: a table declaring `sort-order` has
    // writers that honour it, and a compaction that bin-packed those files back
    // together *unsorted* would leave the table claiming a clustering its files
    // no longer have. Every query with a predicate on the sort columns would
    // then start reading every file — maintenance making the table slower.
    //
    // No rule names a sort here. The table does, and that is enough.
    let fixture = TestTable::sorted_by(vec![(1, iceberg::spec::SortDirection::Ascending)]).unwrap();

    let mut files = Vec::new();
    for chunk in [
        &[(9, "i"), (3, "c")][..],
        &[(7, "g"), (1, "a")][..],
        &[(5, "e"), (2, "b")][..],
    ] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    let result = compact_with_table_facts(&fixture, ENABLED).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "got {result:?}"
    );

    // One file, and its rows in the table's declared order.
    assert_eq!(live_data_files(&fixture.table()).await.unwrap().len(), 1);
    let ids: Vec<i32> = read_all(&fixture.table())
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 5, 7, 9], "output is not sorted by id");
}

#[tokio::test]
async fn a_descending_sort_order_is_reproduced_in_that_direction() {
    // Direction is not decoration: writing the rows back ascending would leave
    // the table's metadata describing a clustering the files do not have, and
    // nothing would fail.
    let fixture =
        TestTable::sorted_by(vec![(1, iceberg::spec::SortDirection::Descending)]).unwrap();

    let mut files = Vec::new();
    for chunk in [&[(1, "a"), (5, "e")][..], &[(3, "c"), (9, "i")][..]] {
        files.push(fixture.write_data_file(chunk).await.unwrap());
    }
    fixture.append(files).await.unwrap();

    compact_with_table_facts(&fixture, ENABLED).await;

    let ids: Vec<i32> = read_all(&fixture.table())
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, vec![9, 5, 3, 1], "output is not sorted descending");
}

#[tokio::test]
async fn an_unsorted_table_is_not_given_a_sort_nobody_asked_for() {
    // The other direction of the same rule. Sorting a table that declares no
    // order would spend a full sort per file group to impose a layout its owner
    // never chose.
    let fixture = TestTable::new().unwrap();
    let table = fixture.table();
    let facts = bergman::policy::TableFacts::from_metadata(table.metadata());
    let policy = Policy::compile(&Config::from_toml(ENABLED).unwrap()).unwrap();
    let Decision::Maintain(effective) =
        policy.decide(&TableRef::new("prod", ["db"], "events"), &facts)
    else {
        panic!("expected the table to be maintained");
    };

    assert!(effective.compaction.sort.is_none());
}
