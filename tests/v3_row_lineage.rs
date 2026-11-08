//! A format v3 table keeps every row's identity through maintenance.
//!
//! v3 gives each row a `_row_id` and a `_last_updated_sequence_number`. Readers
//! derive them from the data file's `first_row_id` plus the row's position — so
//! a rewrite, which moves rows into a new file at new positions, keeps them only
//! by writing them into the new file as **physical** columns. A rewrite that did
//! not would commit successfully, pass every row-count check, and hand every
//! row it touched a new identity: change-data readers would see the whole
//! partition deleted and re-inserted, and nothing would fail.
//!
//! So every test here reads lineage back through upstream's own scan, which is
//! the reader an engine's claim is checked by, and compares it row by row with
//! what the table said before Bergman touched it.

mod common;

use bergman::ops::{compact, manifests};
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use common::{TestTable, read_all, read_lineage};
use iceberg::spec::FormatVersion;

fn table_ref() -> TableRef {
    TableRef::new("prod", ["db"], "events")
}

fn decide(toml: &str) -> bergman::policy::EffectivePolicy {
    let policy = Policy::compile(&Config::from_toml(toml).unwrap()).unwrap();
    match policy.decide(&table_ref(), &Default::default()) {
        Decision::Maintain(e) => *e,
        other => panic!("expected Maintain, got {other:?}"),
    }
}

const COMPACT: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.compaction]
    enabled = true
"#;

const REWRITE_MANIFESTS: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.manifests]
    rewrite = true
    target_size = 8388608
    min_count_to_merge = 2
"#;

fn ctx(table: &TableRef, kind: OperationKind) -> bergman::obs::OperationContext<'_> {
    bergman::obs::OperationContext {
        run_id: "test",
        table,
        kind,
        matched_rule: "prod.db.events",
        reason: "test",
    }
}

async fn compact_all(fixture: &TestTable) -> OperationResult {
    let table = fixture.table();
    let table_ref = table_ref();
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        ctx(&table_ref, OperationKind::Compact),
    );
    compact::run(
        &env,
        &decide(COMPACT).compaction,
        &[bergman::health::PartitionKey::unpartitioned(0)],
    )
    .await
    .expect("compaction runs")
}

/// A v3 table with three small files, each appended in its own snapshot so the
/// rows carry three different `_last_updated_sequence_number`s.
async fn three_appends() -> TestTable {
    let fixture = TestTable::with_format(FormatVersion::V3).unwrap();
    for chunk in [
        &[(1, "a"), (2, "b")][..],
        &[(3, "c"), (4, "d")][..],
        &[(5, "e"), (6, "f")][..],
    ] {
        let file = fixture.write_data_file(chunk).await.unwrap();
        fixture.append(vec![file]).await.unwrap();
    }
    fixture
}

#[tokio::test]
async fn compaction_keeps_every_rows_lineage() {
    let fixture = three_appends().await;
    let before = read_lineage(&fixture.table()).await.unwrap();
    assert_eq!(before.len(), 6);
    assert!(
        before
            .iter()
            .all(|(_, id, seq)| id.is_some() && seq.is_some()),
        "the fixture assigns lineage: {before:?}"
    );
    let distinct_seqs: std::collections::HashSet<_> = before.iter().map(|r| r.2).collect();
    assert_eq!(
        distinct_seqs.len(),
        3,
        "three appends, three sequence numbers"
    );

    let result = compact_all(&fixture).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "a v3 table is compacted: {result:?}"
    );
    assert_eq!(
        common::live_data_files(&fixture.table())
            .await
            .unwrap()
            .len(),
        1,
        "three files became one"
    );

    // The whole point, row by row: same id, same last-updated sequence number.
    // A rewrite moves rows; it does not update them.
    let after = read_lineage(&fixture.table()).await.unwrap();
    assert_eq!(before, after, "compaction changed row lineage");
}

#[tokio::test]
async fn the_snapshot_advances_next_row_id_by_its_row_range() {
    // The table's `next-row-id` is the high-water mark every later writer
    // assigns from. A rewrite snapshot that claimed no range would leave it
    // where it was, and the next append would hand out ids the rewrite's new
    // manifest already covers.
    let fixture = three_appends().await;
    let before = fixture.table().metadata().next_row_id();

    compact_all(&fixture).await;

    let table = fixture.table();
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().unwrap();
    let (first, added) = snapshot
        .row_range()
        .expect("a v3 snapshot carries a row range");
    assert_eq!(
        first, before,
        "the range starts at the table's high-water mark"
    );
    assert_eq!(metadata.next_row_id(), first + added);

    // And a later append's rows do not collide with any id already in use.
    let file = fixture.write_data_file(&[(7, "g")]).await.unwrap();
    fixture.append(vec![file]).await.unwrap();
    let lineage = read_lineage(&fixture.table()).await.unwrap();
    let ids: std::collections::HashSet<_> = lineage.iter().map(|r| r.1).collect();
    assert_eq!(
        ids.len(),
        lineage.len(),
        "every row id is unique: {lineage:?}"
    );
}

#[tokio::test]
async fn manifest_rewriting_keeps_every_rows_lineage() {
    // Moving entries into a new manifest gives that manifest a new
    // `first_row_id`. Entries that inherited their own from the old manifest
    // must carry it explicitly, or every row in them is renumbered.
    let fixture = three_appends().await;
    let before = read_lineage(&fixture.table()).await.unwrap();

    let table = fixture.table();
    let table_ref = table_ref();
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        ctx(&table_ref, OperationKind::RewriteManifests),
    );
    let result = manifests::run(&env, &decide(REWRITE_MANIFESTS).manifests)
        .await
        .expect("manifest rewrite runs");
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "{result:?}"
    );
    assert_eq!(
        common::manifest_paths(&fixture.table())
            .await
            .unwrap()
            .len(),
        1
    );

    assert_eq!(before, read_lineage(&fixture.table()).await.unwrap());
}

#[tokio::test]
async fn compaction_keeps_the_rows_and_their_values() {
    // The row-count contract and the content check, on v3, so a lineage
    // failure cannot hide behind a content failure or the reverse.
    let fixture = three_appends().await;
    let mut before = read_all(&fixture.table()).await.unwrap();
    compact_all(&fixture).await;
    let mut after = read_all(&fixture.table()).await.unwrap();
    before.sort();
    after.sort();
    assert_eq!(before, after);
}

/// Offer one added file to the snapshot producer, as an embedder could.
async fn offer(fixture: &TestTable, file: iceberg::spec::DataFile) -> bergman::Result<()> {
    use bergman::commit::{BranchRetention, RewriteFiles, SnapshotProducer};
    let table = fixture.table();
    let retention = BranchRetention::load(&table).await?;
    SnapshotProducer::new(&table, retention)
        .rewrite_files(&RewriteFiles {
            removed: Vec::new(),
            added: vec![file],
        })
        .await
        .map(|_| ())
}

#[tokio::test]
async fn a_v3_rewrite_adding_a_file_without_lineage_is_refused() {
    // The trap, offered directly to the producer the way a library caller
    // could. The fixture's writer produces a plain Parquet file with no
    // `_row_id` column, so committing it into a v3 table would give its rows
    // fresh ids by inheritance — right for an append, silent renumbering for a
    // rewrite. The producer cannot tell which the caller meant, so it insists
    // on the columns that make the answer the same either way.
    let fixture = TestTable::with_format(FormatVersion::V3).unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    let err = offer(&fixture, file).await.expect_err("refused");
    assert!(
        err.to_string().contains("_row_id"),
        "the refusal names what is missing: {err}"
    );
}

#[tokio::test]
async fn the_same_file_is_accepted_by_a_v2_table() {
    // The control: v2 has no row lineage, so nothing about the file is wrong
    // there. Without this, the test above passes against a producer that
    // refuses every added file.
    let fixture = TestTable::new().unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    offer(&fixture, file).await.expect("accepted");
}

#[tokio::test]
async fn a_row_without_lineage_gets_one_on_rewrite() {
    // A table upgraded from v2 holds files with no `first_row_id`, so until its
    // first v3 commit every row reads with a null `_row_id` — there is no
    // identity to keep. (The first v3 commit by *any* writer assigns the
    // carried manifests a range; here Bergman's compaction is that commit.)
    // The spec's rule is that a null id inherits from its file, so afterwards
    // every row has an id, no two share one, and `next-row-id` is past them.
    use bergman::commit::TableCommitter;

    let fixture = TestTable::new().unwrap();
    for chunk in [&[(1, "a"), (2, "b")][..], &[(3, "c")][..]] {
        let file = fixture.write_data_file(chunk).await.unwrap();
        fixture.append(vec![file]).await.unwrap();
    }

    let parent = fixture
        .table()
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    fixture
        .committer
        .commit(
            &fixture.ident,
            vec![iceberg::TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: parent,
            }],
            vec![iceberg::TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V3,
            }],
            common::fixture_ctx(),
        )
        .await
        .unwrap();

    let before = read_lineage(&fixture.table()).await.unwrap();
    assert!(
        before.iter().all(|(_, id, _)| id.is_none()),
        "pre-v3 rows have no id until the first v3 commit: {before:?}"
    );

    let result = compact_all(&fixture).await;
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "{result:?}"
    );

    let table = fixture.table();
    let after = read_lineage(&table).await.unwrap();
    assert!(
        after.iter().all(|(_, id, _)| id.is_some()),
        "every row has an id after the rewrite: {after:?}"
    );
    let ids: std::collections::HashSet<i64> = after.iter().filter_map(|r| r.1).collect();
    assert_eq!(ids.len(), after.len(), "no two rows share one: {after:?}");
    let next = table.metadata().next_row_id() as i64;
    assert!(
        ids.iter().all(|id| *id < next),
        "next-row-id {next} is past every id"
    );
}

#[tokio::test]
async fn the_producer_refuses_an_encrypted_table_by_name() {
    // The planner and the operation refuse an encrypted table; an embedder can
    // reach the snapshot producer without either. Without its own check it
    // would fail three layers down on a manifest it cannot decrypt, with no
    // word about why. The control is the v2 test above: the same file, a plain
    // table, accepted.
    let fixture = TestTable::with_properties(&[("encryption.key-id", "kms-key-1")]).unwrap();
    let file = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    let err = offer(&fixture, file).await.expect_err("refused");
    assert!(err.to_string().contains("encryption.key-id"), "{err}");
}
