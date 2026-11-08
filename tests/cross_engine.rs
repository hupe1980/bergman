//! The Bergman half of the cross-engine round trip.
//!
//! Every other suite builds its tables with `iceberg-rust`'s writers and reads
//! them back with `iceberg-rust`'s reader, which catches spec errors and not
//! disagreements between implementations. This one maintains tables another
//! engine wrote, and leaves them for that engine to read back.
//!
//! Opt-in, because it needs Python and PyIceberg: `just cross-engine` writes
//! the tables, runs these tests with `BERGMAN_CROSS_ENGINE_DIR` pointing at
//! them, and verifies the result with PyIceberg. Run alone, without the
//! variable, every test here is ignored.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use bergman::ops::{TableLoader, compact, manifests};
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::{Config, Decision, Policy, TableRef};
use common::{FixtureLoader, InMemoryCommitter};
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;

const POLICY: &str = r#"
    [[rules]]
    match = "pyiceberg.db.*"
    [rules.compaction]
    enabled = true
    [rules.manifests]
    rewrite = true
    target_size = 8388608
    min_count_to_merge = 2
"#;

fn directory() -> Option<std::path::PathBuf> {
    std::env::var_os("BERGMAN_CROSS_ENGINE_DIR").map(Into::into)
}

fn local(location: &str) -> &str {
    location.strip_prefix("file://").unwrap_or(location)
}

#[tokio::test]
#[ignore = "needs PyIceberg; run through `just cross-engine`"]
async fn bergman_maintains_every_table_pyiceberg_wrote() {
    let dir = directory().expect("BERGMAN_CROSS_ENGINE_DIR names the tables to maintain");
    let written: BTreeMap<String, serde_json::Value> =
        serde_json::from_slice(&std::fs::read(dir.join("written.json")).unwrap()).unwrap();
    let policy = Policy::compile(&Config::from_toml(POLICY).unwrap()).unwrap();

    let mut maintained = BTreeMap::new();
    for (name, record) in &written {
        let metadata_location = record["metadata"].as_str().unwrap();
        let metadata: TableMetadata =
            serde_json::from_slice(&std::fs::read(local(metadata_location)).unwrap()).unwrap();
        let location = metadata.location().to_string();

        let committer = Arc::new(InMemoryCommitter::new(metadata));
        let loader = FixtureLoader::new(Arc::clone(&committer), FileIO::new_with_fs(), location);
        let ident = TableIdent::from_strs(["db", name.as_str()]).unwrap();
        let table_ref = TableRef::new("pyiceberg", ["db"], name.as_str());

        let table = loader.reload(&ident).await.unwrap();
        let effective = match policy.decide(
            &table_ref,
            &bergman::policy::TableFacts::from_metadata(table.metadata()),
        ) {
            Decision::Maintain(e) => e,
            other => panic!("{name}: expected Maintain, got {other:?}"),
        };

        // Every partition PyIceberg wrote, as Bergman's own analyzer sees it.
        let health = bergman::health::analyze(&table_ref, &table, 8 << 20, chrono::Utc::now(), 8)
            .await
            .unwrap();
        let partitions: Vec<_> = health.partitions.iter().map(|p| p.key.clone()).collect();

        for kind in [OperationKind::Compact, OperationKind::RewriteManifests] {
            let table = loader.reload(&ident).await.unwrap();
            let env = common::op_env(
                &table,
                &ident,
                &loader,
                committer.as_ref(),
                bergman::obs::OperationContext {
                    run_id: "cross-engine",
                    table: &table_ref,
                    kind,
                    matched_rule: "pyiceberg.db.*",
                    reason: "cross-engine round trip",
                },
            );
            let result = match kind {
                OperationKind::Compact => {
                    compact::run(&env, &effective.compaction, &partitions).await
                }
                _ => manifests::run(&env, &effective.manifests).await,
            }
            .unwrap_or_else(|e| panic!("{name}: {kind:?} failed: {e}"));
            assert!(
                matches!(result, OperationResult::Succeeded { .. }),
                "{name}: {kind:?} did nothing: {result:?}"
            );
        }

        // Committed here, the way a catalog would persist it, for PyIceberg.
        let out = dir.join(format!("{name}.bergman.metadata.json"));
        std::fs::write(&out, serde_json::to_vec(&committer.metadata()).unwrap()).unwrap();
        maintained.insert(
            name.clone(),
            serde_json::json!({ "metadata": format!("file://{}", out.display()) }),
        );
        assert!(
            committer.commit_count() >= 2,
            "{name}: two operations committed"
        );
    }

    std::fs::write(
        dir.join("maintained.json"),
        serde_json::to_vec_pretty(&maintained).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
#[ignore = "needs PyIceberg; run through `just cross-engine`"]
async fn a_v3_table_bergman_compacted_is_left_for_pyiceberg() {
    // The other direction. PyIceberg 0.12 cannot write v3, so the table is
    // Bergman's fixture — two files, three rows hidden by deletion vectors in
    // one Puffin file — and what PyIceberg checks is the output: data files
    // carrying `_row_id` and `_last_updated_sequence_number` as physical
    // columns, a v3 manifest list, and a snapshot with a row range. A reader
    // that knew nothing of lineage must still see exactly the live rows.
    let dir = directory().expect("BERGMAN_CROSS_ENGINE_DIR names the output directory");
    let fixture =
        common::TestTable::with_format_in(iceberg::spec::FormatVersion::V3, &dir).unwrap();
    let first = fixture
        .write_data_file(&[(1, "a"), (2, "b"), (3, "c")])
        .await
        .unwrap();
    let second = fixture
        .write_data_file(&[(4, "d"), (5, "e")])
        .await
        .unwrap();
    fixture
        .append(vec![first.clone(), second.clone()])
        .await
        .unwrap();
    let vectors = fixture
        .write_deletion_vectors(&[(&first, &[0, 2][..]), (&second, &[1][..])])
        .await
        .unwrap();
    fixture.append_deletes(vectors).await.unwrap();

    let table = fixture.table();
    let table_ref = TableRef::new("prod", ["db"], "events");
    let loader = fixture.loader();
    let env = common::op_env(
        &table,
        &fixture.ident,
        &loader,
        fixture.committer.as_ref(),
        bergman::obs::OperationContext {
            run_id: "cross-engine",
            table: &table_ref,
            kind: OperationKind::Compact,
            matched_rule: "prod.db.events",
            reason: "cross-engine round trip",
        },
    );
    let policy = Policy::compile(
        &Config::from_toml(POLICY.replace("pyiceberg.db.*", "prod.db.events").as_str()).unwrap(),
    )
    .unwrap();
    let Decision::Maintain(effective) = policy.decide(&table_ref, &Default::default()) else {
        panic!("expected Maintain")
    };
    compact::run(
        &env,
        &effective.compaction,
        &[bergman::health::PartitionKey::unpartitioned(0)],
    )
    .await
    .expect("compacted");

    let rows = common::read_all(&fixture.table()).await.unwrap();
    let metadata = fixture.committer.metadata();
    // The fixture's directory outlives the test: PyIceberg reads it next.
    let common::TestTable { dir: kept, .. } = fixture;
    let kept = kept.keep();
    let out = kept.join("bergman-v3.metadata.json");
    std::fs::write(&out, serde_json::to_vec(&metadata).unwrap()).unwrap();
    std::fs::write(
        dir.join("v3.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "metadata": format!("file://{}", out.display()),
            "rows": rows,
        }))
        .unwrap(),
    )
    .unwrap();
}
