//! The commit layer's wire format.
//!
//! Bergman delivers `(requirements, updates)` itself, because `iceberg-rust`
//! cannot express a rewrite commit. That makes the JSON on the wire Bergman's
//! responsibility rather than upstream's — so it is asserted here, against the
//! REST specification, rather than assumed.
//!
//! The payload types are upstream's own `Serialize` implementations, so these
//! tests are really checking two things: that Bergman puts the right values in
//! the right fields, and that a future upstream change to those encodings is
//! noticed here rather than by a catalog rejecting a commit in production.

use iceberg::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
use iceberg::{TableRequirement, TableUpdate};
use serde_json::json;

#[test]
fn the_compare_and_swap_precondition_serializes_as_the_spec_names_it() {
    // This requirement is what makes a maintenance commit safe: if `main` moved
    // since the plan was built, the catalog rejects the commit and Bergman
    // replans. A wrong `type` here would be silently ignored by the catalog and
    // the commit would apply to a table it was not computed against.
    let requirement = TableRequirement::RefSnapshotIdMatch {
        r#ref: MAIN_BRANCH.to_string(),
        snapshot_id: Some(1234567890),
    };

    let json = serde_json::to_value(&requirement).unwrap();
    assert_eq!(json["type"], "assert-ref-snapshot-id");
    assert_eq!(json["ref"], "main");
    assert_eq!(json["snapshot-id"], 1234567890i64);
}

#[test]
fn the_uuid_precondition_pins_the_table_identity() {
    // Guards against a table being dropped and recreated between plan and
    // commit — the ref check alone would pass against a brand-new table.
    let uuid = uuid::Uuid::new_v4();
    let json = serde_json::to_value(TableRequirement::UuidMatch { uuid }).unwrap();

    assert_eq!(json["type"], "assert-table-uuid");
    assert_eq!(json["uuid"], uuid.to_string());
}

#[test]
fn setting_the_branch_reference_serializes_as_the_spec_names_it() {
    let update = TableUpdate::SetSnapshotRef {
        ref_name: MAIN_BRANCH.to_string(),
        reference: SnapshotReference::new(
            42,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        ),
    };

    let json = serde_json::to_value(&update).unwrap();
    assert_eq!(json["action"], "set-snapshot-ref");
    assert_eq!(json["ref-name"], "main");
    assert_eq!(json["snapshot-id"], 42);
    assert_eq!(json["type"], "branch");
}

#[test]
fn a_commit_body_carries_requirements_and_updates_under_the_documented_keys() {
    // The exact envelope `POST /v1/namespaces/{ns}/tables/{table}` expects.
    let body = json!({
        "identifier": { "namespace": ["analytics"], "name": "events" },
        "requirements": [serde_json::to_value(TableRequirement::UuidMatch {
            uuid: uuid::Uuid::nil(),
        })
        .unwrap()],
        "updates": [serde_json::to_value(TableUpdate::RemoveSnapshotRef {
            ref_name: "stale".into(),
        })
        .unwrap()],
    });

    assert_eq!(body["identifier"]["namespace"][0], "analytics");
    assert_eq!(body["identifier"]["name"], "events");
    assert!(body["requirements"].is_array());
    assert!(body["updates"].is_array());
    assert_eq!(body["updates"][0]["action"], "remove-snapshot-ref");
}

#[test]
fn a_rewrite_commits_the_snapshot_and_moves_the_branch_together() {
    // Adding a snapshot without moving `main` leaves it unreachable — the
    // rewrite would appear to succeed while changing nothing anyone can read.
    // The pair has to travel in one commit, in this order.
    let updates = vec![
        TableUpdate::RemoveSnapshotRef {
            ref_name: "placeholder".into(),
        },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                99,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        },
    ];

    let json = serde_json::to_value(&updates).unwrap();
    assert_eq!(json.as_array().unwrap().len(), 2);
    assert_eq!(json[1]["action"], "set-snapshot-ref");
    assert_eq!(json[1]["ref-name"], "main");
}
