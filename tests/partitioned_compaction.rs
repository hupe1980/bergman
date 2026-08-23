//! Compaction of a *partitioned* table.
//!
//! Every other end-to-end test uses an unpartitioned fixture, which cannot see
//! the question this file exists for: how a scanned file is matched back to the
//! partition the plan named. On an unpartitioned table both sides answer
//! "unpartitioned" and agree for the wrong reason.

mod common;

use bergman::health::PartitionKey;
use bergman::obs::OperationContext;
use bergman::ops::compact;
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use common::{TestTable, live_data_files, read_all};

const ENABLED: &str = r#"
    [[rules]]
    match = "prod.db.events"
    [rules.compaction]
    enabled = true
    target_file_size = 1073741824
"#;

fn settings() -> bergman::policy::EffectiveCompaction {
    let policy = Policy::compile(&Config::from_toml(ENABLED).unwrap()).unwrap();
    match policy.decide(
        &TableRef::new("prod", ["db"], "events"),
        &bergman::policy::TableFacts::unknown(),
    ) {
        Decision::Maintain(e) => e.compaction.clone(),
        other => panic!("expected Maintain, got {other:?}"),
    }
}

/// The partitions the health analyzer reports, which is what a plan carries.
async fn planned_partitions(fixture: &TestTable) -> Vec<PartitionKey> {
    let table = fixture.table();
    let health = bergman::health::analyze(
        &TableRef::new("prod", ["db"], "events"),
        &table,
        8 * 1024 * 1024,
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    health.partitions.into_iter().map(|p| p.key).collect()
}

#[tokio::test]
async fn a_partitioned_table_is_actually_compacted() {
    // The plan names partitions the health analyzer derived from manifests,
    // where each manifest carries its own spec. The executor has to match the
    // scanned files back to those names. If the two disagree, every group is
    // empty and compaction reports "nothing left to compact" on a table that is
    // visibly fragmented -- doing nothing, and saying nothing.
    let fixture = TestTable::partitioned().unwrap();

    // Two partitions, three small files each. The partition value is the `id`
    // column, so the tuple matches the data.
    for partition in [1, 2] {
        for row in 0..3 {
            let name = format!("r{row}");
            let file = fixture
                .write_partitioned_data_file(&[(partition, name.as_str())], partition)
                .await
                .unwrap();
            fixture.append(vec![file]).await.unwrap();
        }
    }

    assert_eq!(live_data_files(&fixture.table()).await.unwrap().len(), 6);
    let before = read_all(&fixture.table()).await.unwrap();
    assert_eq!(before.len(), 6);

    let targets = planned_partitions(&fixture).await;
    assert_eq!(targets.len(), 2, "two partitions: {targets:?}");

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

    let result = compact::run(&env, &settings(), &targets).await.unwrap();
    assert!(
        matches!(result, OperationResult::Succeeded { .. }),
        "compaction did nothing on a fragmented partitioned table: {result:?}"
    );

    // Six files into two -- one per partition -- with every row intact.
    let after_files = live_data_files(&fixture.table()).await.unwrap();
    assert_eq!(after_files.len(), 2, "expected one file per partition");

    let mut after = read_all(&fixture.table()).await.unwrap();
    let mut before = before;
    after.sort();
    before.sort();
    assert_eq!(after, before);
}

#[tokio::test]
async fn each_partition_commits_on_its_own() {
    // Groups are per partition, so a partitioned table produces one commit per
    // partition rather than one for the table. That is what makes a lost
    // compare-and-swap cost one partition's work instead of all of it.
    let fixture = TestTable::partitioned().unwrap();
    for partition in [1, 2, 3] {
        for row in 0..2 {
            let name = format!("r{row}");
            let file = fixture
                .write_partitioned_data_file(&[(partition, name.as_str())], partition)
                .await
                .unwrap();
            fixture.append(vec![file]).await.unwrap();
        }
    }

    let commits_before = fixture.committer.commit_count();
    let targets = planned_partitions(&fixture).await;
    assert_eq!(targets.len(), 3);

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
    compact::run(&env, &settings(), &targets).await.unwrap();

    assert_eq!(
        fixture.committer.commit_count() - commits_before,
        3,
        "one commit per partition"
    );
    assert_eq!(live_data_files(&fixture.table()).await.unwrap().len(), 3);
}

#[tokio::test]
async fn rows_land_in_the_partition_they_came_from() {
    // The failure a wrong partition index causes is not an empty result, it is
    // rows written under someone else's partition value. Reading back through
    // the scan proves each output file carries the tuple its rows belong to.
    let fixture = TestTable::partitioned().unwrap();
    for partition in [7, 8] {
        for row in 0..3 {
            let name = format!("p{partition}r{row}");
            let file = fixture
                .write_partitioned_data_file(&[(partition, name.as_str())], partition)
                .await
                .unwrap();
            fixture.append(vec![file]).await.unwrap();
        }
    }

    let targets = planned_partitions(&fixture).await;
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
    compact::run(&env, &settings(), &targets).await.unwrap();

    // Every row's `id` must still equal the partition it is filed under.
    let after = planned_partitions(&fixture).await;
    let values: Vec<&str> = after.iter().map(|k| k.value.as_str()).collect();
    assert_eq!(values, vec!["id=7", "id=8"], "partitions were re-filed");

    for (id, name) in read_all(&fixture.table()).await.unwrap() {
        assert!(
            name.starts_with(&format!("p{id}")),
            "row {name:?} is filed under id={id}"
        );
    }
}
