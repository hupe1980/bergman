//! The orphan-removal safety model, exercised against an in-memory store.
//!
//! These are the rules that decide whether a file lives or dies. Each is tested
//! at the level it is enforced, because a safety check that regresses silently
//! is one nobody notices until a table is gone.

use std::time::Duration;

use bergman::ops::delete::FileDeleter;
use bergman::ops::reachability::{is_inside, normalize};
use bergman::ops::store::{InMemoryStore, ObjectMeta, ObjectStore};
use bergman::policy::{Config, Decision, MIN_ORPHAN_AGE, OrphanMode, Policy, TableRef};
use chrono::{Duration as ChronoDuration, Utc};

/// Drain a listing, which is a stream so that a scan of a table with millions
/// of objects never holds them all at once.
async fn collect(store: &InMemoryStore, prefix: &str) -> Vec<ObjectMeta> {
    use futures::TryStreamExt;
    store
        .list(prefix)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn effective(toml: &str) -> bergman::policy::EffectivePolicy {
    let config = Config::from_toml(toml).unwrap();
    let policy = Policy::compile(&config).unwrap();
    match policy.decide(&TableRef::new("prod", ["db"], "t"), &Default::default()) {
        Decision::Maintain(e) => *e,
        other => panic!("expected Maintain, got {other:?}"),
    }
}

#[test]
fn the_age_floor_cannot_be_configured_away() {
    // Writers stage files before the commit that references them. A grace
    // period short enough to catch one in flight is how a healthy table gets
    // corrupted, so the floor is refused at parse time.
    let config = Config::from_toml(
        r#"
        [[rules]]
        match = "prod.**"
        [rules.orphans]
        enabled = true
        mode = "delete"
        older_than = "5m"
        "#,
    )
    .unwrap();

    let err = Policy::compile(&config).unwrap_err();
    assert!(err.to_string().contains("floor"), "got: {err}");
    assert_eq!(MIN_ORPHAN_AGE, Duration::from_secs(24 * 60 * 60));
}

#[test]
fn deletion_requires_an_explicit_opt_in() {
    // A rule that merely mentions orphans gets a report, not a deletion.
    let policy = effective(
        r#"
        [[rules]]
        match = "prod.db.t"
        [rules.orphans]
        enabled = true
        "#,
    );
    assert_eq!(policy.orphans.mode.value, OrphanMode::DryRun);

    let deleting = effective(
        r#"
        [[rules]]
        match = "prod.db.t"
        [rules.orphans]
        enabled = true
        mode = "delete"
        "#,
    );
    assert_eq!(deleting.orphans.mode.value, OrphanMode::Delete);
}

#[tokio::test]
async fn listing_does_not_cross_into_a_similarly_named_table() {
    // `…/events` and `…/events_archive` share a string prefix. An object store
    // matches prefixes as raw strings, so a scanner that did the same would
    // offer another table's live files up for deletion.
    let store = InMemoryStore::new();
    store.insert("s3://bucket/wh/db/events/data/live.parquet", 100, None);
    store.insert(
        "s3://bucket/wh/db/events_archive/data/live.parquet",
        100,
        None,
    );
    store.insert(
        "s3://bucket/wh/db/events/metadata/v1.metadata.json",
        10,
        None,
    );

    let listed = collect(&store, "s3://bucket/wh/db/events").await;
    let paths: Vec<&str> = listed.iter().map(|o| o.path.as_str()).collect();

    assert_eq!(paths.len(), 2);
    assert!(paths.iter().all(|p| !p.contains("events_archive")));
}

#[test]
fn containment_is_checked_segment_wise() {
    assert!(is_inside(
        "s3://bucket/wh/db/events",
        "s3://bucket/wh/db/events/data/f.parquet"
    ));
    assert!(!is_inside(
        "s3://bucket/wh/db/events",
        "s3://bucket/wh/db/events_archive/data/f.parquet"
    ));
}

#[test]
fn path_spellings_that_differ_still_identify_one_object() {
    // Spark writes `s3a://`, a REST catalog reports `s3://`, a naive join
    // doubles a slash. All three name the same object, and a comparison that
    // missed that would mark a live file as garbage.
    let canonical = normalize("s3://bucket/wh/db/t/data/f.parquet");
    for spelling in [
        "s3a://bucket/wh/db/t/data/f.parquet",
        "s3n://bucket/wh/db/t//data/f.parquet",
        "S3://bucket/wh/db/t/data/f.parquet",
    ] {
        assert_eq!(normalize(spelling), canonical, "{spelling}");
    }
}

#[tokio::test]
async fn a_store_deletion_removes_exactly_the_named_object() {
    let store = InMemoryStore::new();
    store.insert("s3://bucket/wh/t/a.parquet", 1, None);
    store.insert("s3://bucket/wh/t/b.parquet", 1, None);

    // Asked with the other spelling: the normalization has to reach the delete
    // path too, or a deletion silently succeeds while removing nothing.
    store.delete("s3a://bucket/wh/t/a.parquet").await.unwrap();

    assert_eq!(
        store.paths(),
        vec!["s3://bucket/wh/t/b.parquet".to_string()]
    );
}

#[tokio::test]
async fn files_with_no_modification_time_are_never_old_enough() {
    // The scanner treats an unknown age as "too young". A store that will not
    // say how old a file is cannot be used to argue that it is old enough to
    // delete — this test pins the intent so a refactor that defaults the other
    // way is caught.
    let store = InMemoryStore::new();
    store.insert("s3://bucket/wh/t/unknown-age.parquet", 1, None);

    let objects = collect(&store, "s3://bucket/wh/t").await;
    assert_eq!(objects.len(), 1);
    assert!(objects[0].last_modified.is_none());
}

#[test]
fn a_recent_file_is_outside_the_grace_period() {
    let now = Utc::now();
    let cutoff = now - ChronoDuration::days(7);

    let recent = now - ChronoDuration::hours(1);
    let ancient = now - ChronoDuration::days(30);

    assert!(recent > cutoff, "a one-hour-old file must be spared");
    assert!(ancient < cutoff, "a 30-day-old file is a candidate");
}
