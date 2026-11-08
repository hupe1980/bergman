//! A format v3 table carrying real deletion vectors.
//!
//! The fixture writes vectors exactly as the spec defines them — Puffin blobs
//! of type `deletion-vector-v1`, magic, length, portable Roaring, CRC-32 — two
//! of them in one container, which is the shape every identity rule here is
//! about.
//!
//! Two planes are exercised against it. **The metadata plane**: health
//! analysis, reachability and dangling-delete detection read the manifest
//! entries — content type, `referenced_data_file`, `content_offset`,
//! `content_size_in_bytes` and the container's path. **The data plane**:
//! compaction decodes the vectors through upstream's reader, applies them, and
//! retires them by `(path, offset)`, which is what shows the bytes are what a
//! v3 writer produces.

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
    // The fixture is hand-written because `iceberg` 0.11.0 has no writer for a
    // `deletion-vector-v1` blob. Each field the spec defines is asserted
    // separately, so a wrong one is named here rather than surfacing as a
    // decode failure in the compaction test.
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
    // The analyzer walks a delete manifest whose entries are Puffin blobs and
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

    let reachable = reachability::compute(&fixture.table(), 8)
        .await
        .expect("reachable set");

    assert!(
        reachable.contains(&container),
        "a container holding any live vector is live"
    );
}

#[tokio::test]
async fn live_vectors_are_never_reported_dangling() {
    // Dangling detection never proposes a deletion vector at all (see
    // `a_dangling_vector_is_never_retired`), so this holds by construction now.
    // It stays as the property that must survive any change to that rule: a
    // vector whose data file is live is never proposed for removal.
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

    // And the same with the scan reporting nothing at all, so neither the scan's
    // association nor the vector exclusion is what keeps these off the list on
    // its own: they were committed *after* the data they cover, so none of them
    // is below every live data file either.
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
async fn a_v3_table_retires_its_dangling_delete_file() {
    // Dangling-delete removal is the last step of `compact::run`. While v3 was
    // refused at the first step it was never reached, and a v3 table upgraded
    // from v2 — which legally keeps its equality and position delete *files* —
    // accumulated dangling ones forever. The v2 test above is the control.
    let v3 = table_with_a_dead_delete(FormatVersion::V3).await;
    assert_eq!(detects_a_dead_delete(&v3).await, 1, "the fixture has one");

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

    bergman::ops::compact::run(
        &env,
        &compaction_enabled(),
        &[bergman::health::PartitionKey::unpartitioned(0)],
    )
    .await
    .expect("a v3 table is maintained");

    assert_eq!(
        detects_a_dead_delete(&v3).await,
        0,
        "the dangling delete must be gone"
    );
}

#[tokio::test]
async fn compaction_applies_deletion_vectors_and_retires_them() {
    // The read half of v3, end to end. Rows 1 and 3 are hidden by one vector,
    // row 5 by another in the same Puffin file. A rewrite that could not read
    // vectors would write all five rows back and resurrect three; one that
    // read them but kept the vectors would leave every scan opening a
    // container that hides nothing.
    let (fixture, _) = v3_with_vectors().await;
    let mut before = common::read_all(&fixture.table()).await.unwrap();
    before.sort();
    assert_eq!(
        before,
        vec![(2, "b".to_string()), (4, "d".to_string())],
        "the fixture's vectors hide rows 1, 3 and 5"
    );
    let lineage_before = common::read_lineage(&fixture.table()).await.unwrap();

    let table = fixture.table();
    let ident = fixture.ident.clone();
    let loader = fixture.loader();
    let ctx = bergman::obs::OperationContext {
        run_id: "test",
        table: &table_ref(),
        kind: bergman::plan::OperationKind::Compact,
        matched_rule: "prod.db.events",
        reason: "test",
    };
    let env = common::op_env(&table, &ident, &loader, fixture.committer.as_ref(), ctx);
    bergman::ops::compact::run(
        &env,
        &compaction_enabled(),
        &[bergman::health::PartitionKey::unpartitioned(0)],
    )
    .await
    .expect("a v3 table carrying deletion vectors is compacted");

    let mut after = common::read_all(&fixture.table()).await.unwrap();
    after.sort();
    assert_eq!(after, before, "deleted rows stayed deleted");
    assert_eq!(
        common::live_data_files(&fixture.table())
            .await
            .unwrap()
            .len(),
        1,
        "two files became one"
    );
    assert!(
        common::live_delete_files(&fixture.table())
            .await
            .unwrap()
            .is_empty(),
        "both vectors were applied, so both are retired"
    );
    assert_eq!(
        common::read_lineage(&fixture.table()).await.unwrap(),
        lineage_before,
        "the surviving rows kept their identity"
    );
}

#[tokio::test]
async fn a_dangling_vector_is_never_retired() {
    // A vector applies to exactly one data file, and the spec requires the
    // writer that removes that file to drop the vector in the same commit — so
    // a dangling vector is another writer's spec violation, not an
    // accumulation, and retiring one speculatively is the most dangerous
    // metadata operation there is for a state the format says cannot occur.
    // The orphan scanner reclaims its container once nothing names it.
    //
    // Built to be dead by every measure the dangling rule uses: committed
    // before any live data file, naming a file that was never in the table.
    // The control is `table_with_a_dead_delete`, whose equality delete *file*
    // in exactly this position is retired.
    let fixture = TestTable::with_format(FormatVersion::V3).unwrap();
    let never_appended = fixture.write_data_file(&[(9, "z")]).await.unwrap();
    let vectors = fixture
        .write_deletion_vectors(&[(&never_appended, &[0][..])])
        .await
        .unwrap();
    fixture.append_deletes(vectors).await.unwrap();
    let a = fixture.write_data_file(&[(1, "a")]).await.unwrap();
    fixture.append(vec![a]).await.unwrap();

    assert_eq!(detects_a_dead_delete(&fixture).await, 0);
    assert_eq!(
        detects_a_dead_delete(&table_with_a_dead_delete(FormatVersion::V3).await).await,
        1,
        "the control: a dead delete *file* in the same position is found"
    );
}
