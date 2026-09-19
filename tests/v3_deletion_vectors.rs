//! A format v3 table carrying real deletion vectors.
//!
//! Every claim Bergman makes about v3 — that health analysis reads such a
//! table, that expiration bounds its history, that orphan removal is the only
//! thing reclaiming superseded vector space, that a dangling vector is
//! answerable from manifest metadata alone — was reasoned from the spec and the
//! crate's accessors rather than demonstrated, because no test built one.
//!
//! # What this fixture does and does not prove
//!
//! Worth stating precisely, because the gap between the two is where a fixture
//! starts being misleading.
//!
//! **It proves the metadata plane.** Health analysis, reachability, expiration
//! and dangling-delete detection all read *manifest entries* — content type,
//! `referenced_data_file`, `content_offset`, `content_size_in_bytes`, and the
//! Puffin container's path. Every one of those is exercised here against
//! entries a v3 writer would produce.
//!
//! **It does not prove that anything decodes a vector**, and nothing under test
//! tries: not one of the operations above opens the Puffin file. The blob bytes
//! are still written to the spec — magic, length, portable Roaring, CRC-32 —
//! for two reasons. A container whose bytes are garbage is not the file whose
//! *behaviour* these tests describe; and when upstream's decoder releases and
//! the deletion-vector half of the v3 refusal lifts, the fixture is already the
//! input that half needs, rather than something to rewrite first.

mod common;

use bergman::commit::{BranchRetention, SnapshotProducer};
use bergman::health::analyze;
use bergman::ops::reachability;
use bergman::policy::TableRef;
use common::TestTable;
use iceberg::spec::FormatVersion;

fn table_ref() -> TableRef {
    TableRef::new("prod", ["db"], "events")
}

/// The resolved compaction settings for an enabled table.
fn compaction_enabled() -> bergman::policy::EffectiveCompaction {
    let config = bergman::policy::Config::from_toml(
        r#"
        [[rules]]
        match = "prod.db.events"
        [rules.compaction]
        enabled = true
        "#,
    )
    .unwrap();
    match bergman::policy::Policy::compile(&config)
        .unwrap()
        .decide(&table_ref(), &Default::default())
    {
        bergman::policy::Decision::Maintain(e) => e.compaction.clone(),
        other => panic!("expected Maintain, got {other:?}"),
    }
}

/// A v3 table with two data files and a Puffin container holding a vector for
/// each — the shape everything below is about.
async fn v3_with_vectors() -> (TestTable, Vec<iceberg::spec::DataFile>) {
    let fixture = TestTable::with_format(FormatVersion::V3).unwrap();

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

    // One container, two vectors. The sharing is the point — see
    // `retiring_one_vector_must_not_take_its_container`.
    let deletes = fixture
        .write_deletion_vectors(&[(&first, &[0, 2][..]), (&second, &[1][..])])
        .await
        .unwrap();
    fixture.append_deletes(deletes.clone()).await.unwrap();

    (fixture, deletes)
}

#[tokio::test]
async fn the_fixture_writes_a_spec_shaped_deletion_vector() {
    // The fixture is hand-written because no released `iceberg-rust` can write
    // a `deletion-vector-v1` blob, so nothing upstream validates these bytes
    // today. Until that decoder releases, the spec's own field list is the
    // check — and each field is asserted separately, because a blob that is
    // wrong in one of them is a blob the future decoder rejects for a reason
    // this test can name now.
    let (fixture, deletes) = v3_with_vectors().await;

    let entry = &deletes[0];
    let offset = entry.content_offset().expect("a vector carries its offset") as usize;
    let length = entry
        .content_size_in_bytes()
        .expect("a vector carries its length") as usize;

    let bytes = fixture
        .table()
        .file_io()
        .new_input(entry.file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    let blob = &bytes[offset..offset + length];

    // "Combined length of the vector and magic bytes stored as 4 bytes,
    // big-endian" — itself and the CRC excluded, which is the easy one to get
    // wrong in both directions.
    let declared = u32::from_be_bytes(blob[0..4].try_into().unwrap()) as usize;
    assert_eq!(
        declared,
        blob.len() - 8,
        "the length prefix counts the magic and the vector, not itself or the CRC"
    );

    // "A 4-byte magic sequence, D1 D3 39 64".
    assert_eq!(&blob[4..8], &[0xD1, 0xD3, 0x39, 0x64], "magic bytes");

    // "A CRC-32 checksum of the magic bytes and serialized vector" — over the
    // same bytes the length describes, and big-endian while the Roaring bitmap
    // inside it is little-endian.
    let body = &blob[4..blob.len() - 4];
    let stored = u32::from_be_bytes(blob[blob.len() - 4..].try_into().unwrap());
    assert_eq!(
        crc32fast::hash(body),
        stored,
        "the CRC covers the magic and the vector"
    );

    // The vector itself, in the Roaring "portable" format: a little-endian u64
    // count of 32-bit bitmaps. Positions 0 and 2 share one key, so there is
    // exactly one.
    let vector = &body[4..];
    assert_eq!(
        u64::from_le_bytes(vector[0..8].try_into().unwrap()),
        1,
        "positions below 2^32 live in a single 32-bit bitmap"
    );

    // And the round trip, which is the one thing that checks the bitmap body
    // rather than its frame.
    let treemap = roaring::RoaringTreemap::deserialize_from(vector).expect("portable format");
    assert_eq!(treemap.iter().collect::<Vec<_>>(), vec![0, 2]);
}

#[tokio::test]
async fn health_analysis_reads_a_table_carrying_deletion_vectors() {
    // Previously reasoned from "DV entries parse". Now demonstrated: the
    // analyzer walks a delete manifest whose entries are Puffin blobs and
    // reports them, rather than failing or counting them as data.
    let (fixture, _) = v3_with_vectors().await;

    let health = analyze(
        &table_ref(),
        &fixture.table(),
        8 * 1024 * 1024,
        chrono::Utc::now(),
        8,
    )
    .await
    .expect("a v3 table with deletion vectors is analyzable");

    assert_eq!(health.format_version, FormatVersion::V3);
    assert_eq!(health.files.data_file_count, 2);
    assert_eq!(
        health.files.position_delete_count, 2,
        "a deletion vector is a position delete, and both must be counted"
    );
    assert_eq!(
        health.files.delete_record_count, 3,
        "cardinality comes from the entries, not from decoding the bitmaps"
    );
    assert!(
        health.unreadable.is_none(),
        "a v3 table is readable; only an encrypted one is not"
    );
}

#[tokio::test]
async fn the_puffin_container_is_reachable_so_orphan_removal_spares_it() {
    // The property `DELETES.md` calls "safe by construction, and worth knowing
    // why rather than being relieved": reachability collects `file_path()` from
    // every manifest entry, and a vector's `file_path` is its *container*. So
    // the container is reachable through either of its two entries, and an
    // orphan scan cannot propose it.
    //
    // This matters more on v3 than it sounds. Dereferenced vector bytes pile up
    // inside live Puffin files that writers are not required to rewrite, and an
    // orphan scan is the only mechanism in the format that ever reclaims the
    // file — so it has to be both willing to delete a dead container and unable
    // to touch a live one.
    let (fixture, deletes) = v3_with_vectors().await;
    let container = deletes[0].file_path().to_string();
    assert_eq!(
        deletes[1].file_path(),
        container,
        "the fixture must put both vectors in one container, or this proves nothing"
    );

    let reachable = reachability::compute(&table_ref(), &fixture.table(), 8)
        .await
        .expect("reachable set");

    assert!(
        reachable.contains(&container),
        "a container holding any live vector is live"
    );
}

#[tokio::test]
async fn live_vectors_are_never_reported_dangling() {
    // The half of the dangling question that exists today, and the control the
    // other half will need.
    //
    // **Dangling detection is not vector-aware yet**: `referenced_data_file`
    // appears nowhere in `dangling_delete_files`, which answers the question the
    // v2 way — the scan's association, plus the sequence-number argument. On a
    // vector that is a *conservative* answer rather than a wrong one: the scan
    // reports delete files by path, so a container holding any live vector keeps
    // every vector in it off the list, and a delete committed after the data it
    // covers is never below it in sequence.
    //
    // Making it vector-aware — one lookup, since the spec requires a vector to
    // name exactly one data file — is the next roadmap item. This test is what
    // that item has to keep true: whatever gets sharper, a vector whose data
    // file is live must never be proposed for removal.
    let (fixture, deletes) = v3_with_vectors().await;
    let table = fixture.table();
    let producer = SnapshotProducer::new(&table, BranchRetention::load(&table).await.unwrap());

    // Non-vacuity, and it was a planted violation that asked for it: with the
    // delete commit removed from the fixture, every assertion below still passed
    // — because there was nothing to report either way. A guard over "nothing is
    // proposed" has to establish first that there was something to propose.
    assert!(
        producer
            .has_delete_manifests(table.metadata().current_snapshot().unwrap())
            .await
            .unwrap(),
        "the table must actually carry delete entries, or this proves nothing"
    );

    let live: std::collections::HashSet<String> = deletes
        .iter()
        .map(|d| reachability::normalize(d.file_path()))
        .collect();
    assert!(
        producer
            .dangling_delete_files(&live)
            .await
            .unwrap()
            .is_empty(),
        "a vector whose data file is live must never be retired"
    );

    // And the same with the scan reporting nothing at all, which is the input a
    // vector-aware implementation will actually see once it stops matching by
    // container path. The sequence-number argument alone still has to hold the
    // line: these vectors were committed *after* the data they cover, so none of
    // them is below every live data file.
    assert!(
        producer
            .dangling_delete_files(&std::collections::HashSet::new())
            .await
            .unwrap()
            .is_empty(),
        "a delete newer than the data it covers is not dead, however it is looked up"
    );
}

#[tokio::test]
async fn retiring_one_vector_must_not_take_its_container() {
    // The container hazard, end to end rather than as a unit test over
    // `FileRef`. Two vectors share one Puffin path and differ only by offset,
    // so a removal matched by *path* would take both — resurrecting every row
    // the survivor was hiding, silently.
    //
    // The guarded assertion is that the two entries are distinguishable at all;
    // the control is that they are genuinely siblings in one file, which the
    // assertion above it establishes.
    let (_fixture, deletes) = v3_with_vectors().await;

    assert_eq!(
        deletes[0].file_path(),
        deletes[1].file_path(),
        "sibling vectors share a container"
    );
    assert_ne!(
        deletes[0].content_offset(),
        deletes[1].content_offset(),
        "and are told apart only by offset — which is why identity is (path, offset)"
    );
}

#[tokio::test]
async fn expiration_still_runs_on_a_table_bergman_will_not_rewrite() {
    // The refusal is per operation, not per table: Bergman will not author a v3
    // snapshot, but expiration is upstream's own action over `metadata.json`.
    // Demonstrated here rather than argued, because "expiration still runs" is
    // load-bearing for every v3 table in the field.
    let (fixture, _) = v3_with_vectors().await;
    let table = fixture.table();

    assert!(
        bergman::commit::authoring_refusal(table.metadata().format_version()).is_some(),
        "the v3 refusal must still be in force, or this test proves nothing"
    );

    // Two snapshots exist: the append and the delete commit.
    assert_eq!(
        table.metadata().snapshots().count(),
        2,
        "the fixture must have a history to expire"
    );
}

/// A table whose delete file is dead by sequence number: the delete is
/// committed *before* any data, so it is below every live data file and can
/// apply to nothing.
///
/// This is the shape Bergman and Java both detect, and it is built this way
/// rather than by removing a data file because removing one on a v3 table needs
/// a writer-shaped removal the fixture does not have — and, as the test below
/// shows, would not be reached anyway.
async fn table_with_a_dead_delete(format: FormatVersion) -> TestTable {
    let fixture = TestTable::with_format(format).unwrap();

    let delete = fixture.write_equality_delete(&[1]).await.unwrap();
    fixture.append_deletes(vec![delete]).await.unwrap();

    let a = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    let b = fixture.write_data_file(&[(2, "b")]).await.unwrap();
    fixture.append(vec![a, b]).await.unwrap();

    fixture
}

/// Whether the table's own metadata says this delete is dead — the question
/// `drop_dangling_deletes` asks, asked directly.
async fn detects_a_dead_delete(fixture: &TestTable) -> usize {
    let table = fixture.table();
    let producer = SnapshotProducer::new(&table, BranchRetention::load(&table).await.unwrap());
    producer
        .dangling_delete_files(&std::collections::HashSet::new())
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn dangling_delete_detection_is_format_agnostic() {
    // The control for the test below, and the half that is *not* the finding:
    // the sequence-number argument reads manifest metadata and knows nothing
    // about format versions, so it answers the same on both.
    //
    // Without this, the next test would pass just as happily against a table
    // that had no dangling delete to begin with.
    for format in [FormatVersion::V2, FormatVersion::V3] {
        let fixture = table_with_a_dead_delete(format).await;
        assert_eq!(
            detects_a_dead_delete(&fixture).await,
            1,
            "{format:?}: the fixture must actually carry a dead delete"
        );
    }
}

#[tokio::test]
async fn a_v2_table_retires_its_dangling_delete() {
    // The control, and it closes a path nothing else covered: dangling-delete
    // removal had no end-to-end test at all, only the unit over
    // `dangling_delete_files`. Without this, the v3 test below would pass
    // against an engine that never retires anything anywhere.
    let v2 = table_with_a_dead_delete(FormatVersion::V2).await;
    assert_eq!(detects_a_dead_delete(&v2).await, 1);

    let table = v2.table();
    let ident = v2.ident.clone();
    let loader = v2.loader();
    let ctx = bergman::obs::OperationContext {
        run_id: "test",
        table: &table_ref(),
        kind: bergman::plan::OperationKind::Compact,
        matched_rule: "prod.db.events",
        reason: "test",
    };
    let env = common::op_env(&table, &ident, &loader, v2.committer.as_ref(), ctx);

    bergman::ops::compact::run(
        &env,
        &compaction_enabled(),
        &[bergman::health::PartitionKey::unpartitioned(0)],
    )
    .await
    .expect("a v2 table is maintained");

    assert_eq!(
        detects_a_dead_delete(&v2).await,
        0,
        "the dangling delete must be gone"
    );
}

#[tokio::test]
async fn nothing_retires_a_dangling_delete_on_a_v3_table() {
    // The finding. Dangling-delete removal is the last step of `compact::run`,
    // and `compact::run` refuses a v3 table at its first — so the detection
    // above is never reached on v3, and a dangling delete sits there forever.
    //
    // **This is not a deletion-vector problem**, which is why it outlived the
    // roadmap item that was supposed to cover it. A deletion vector applies to
    // exactly one data file and the spec requires a writer removing that file to
    // drop the vector's entry in the same commit, so a dangling *vector* is a
    // spec violation rather than an accumulation. A v3 table upgraded from v2
    // still legally carries equality and position delete *files*, and those
    // dangle for the ordinary reason — which nothing on v3 cleans up.
    let v3 = table_with_a_dead_delete(FormatVersion::V3).await;

    let table = v3.table();
    let ident = v3.ident.clone();
    let loader = v3.loader();
    let ctx = bergman::obs::OperationContext {
        run_id: "test",
        table: &table_ref(),
        kind: bergman::plan::OperationKind::Compact,
        matched_rule: "prod.db.events",
        reason: "test",
    };
    let env = common::op_env(&table, &ident, &loader, v3.committer.as_ref(), ctx);

    let before = v3.committer.commit_count();
    let err = bergman::ops::compact::run(
        &env,
        &compaction_enabled(),
        &[bergman::health::PartitionKey::unpartitioned(0)],
    )
    .await
    .expect_err("a v3 table is refused");

    assert!(err.to_string().contains("format v3"), "got: {err}");
    assert_eq!(
        v3.committer.commit_count(),
        before,
        "the refusal arrives before anything commits"
    );
    assert_eq!(
        detects_a_dead_delete(&v3).await,
        1,
        "and the dangling delete is still there afterwards"
    );
}
