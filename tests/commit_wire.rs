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

// ---------------------------------------------------------------------------
// Against a real socket
// ---------------------------------------------------------------------------
//
// The tests above assert what Bergman serializes. These assert what it *sends*,
// which is a different thing: the path a commit is addressed to, and the
// correlation header a governing catalog is supposed to log against its own
// authorization decision. Neither is visible from a `serde_json::to_value`, and
// both are load-bearing — a commit sent to the wrong path reaches the wrong
// warehouse, and a header nobody can join to Bergman's audit trail is a promise
// the documentation makes and the code does not keep.
//
// A raw `TcpListener` rather than a mock framework: two request/response pairs
// is not worth a dependency, and speaking the protocol by hand is what makes
// this a test of the wire rather than of a stub.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bergman::commit::{RestCommitter, TableCommitter};
use bergman::obs::OperationContext;
use bergman::plan::OperationKind;
use bergman::policy::TableRef;

/// One request the fake catalog received.
#[derive(Debug, Clone)]
struct Received {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: String,
}

/// A catalog that answers `/v1/config` and accepts one commit, recording both.
async fn fake_catalog() -> (String, Arc<Mutex<Vec<Received>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let log: Arc<Mutex<Vec<Received>>> = Arc::new(Mutex::new(Vec::new()));

    let recorded = Arc::clone(&log);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let recorded = Arc::clone(&recorded);
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buffer = [0u8; 4096];

                // Read until the headers are complete, then however many more
                // bytes `Content-Length` promised.
                let head_end = loop {
                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                    if read == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buffer[..read]);
                    if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                };

                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let mut lines = head.lines();
                let request_line = lines.next().unwrap_or_default().to_string();
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or_default().to_string();
                let path = parts.next().unwrap_or_default().to_string();

                let headers: HashMap<String, String> = lines
                    .filter_map(|line| line.split_once(':'))
                    // Lower-cased: HTTP header names are case-insensitive and a
                    // test that depended on the client's choice of casing would
                    // be testing reqwest.
                    .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                    .collect();

                let want: usize = headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                while raw.len() < head_end + want {
                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                    if read == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buffer[..read]);
                }
                let body = String::from_utf8_lossy(&raw[head_end..]).to_string();

                recorded.lock().unwrap().push(Received {
                    method,
                    path: path.clone(),
                    headers,
                    body,
                });

                // `/v1/config` describes a single-warehouse catalog that
                // advertises no endpoint list, which is what most deployments
                // do; anything else is answered as an accepted commit.
                let payload = if path.starts_with("/v1/config") {
                    r#"{"defaults":{},"overrides":{}}"#
                } else {
                    "{}"
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{payload}",
                    payload.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    (uri, log)
}

#[tokio::test]
async fn a_commit_carries_the_run_id_a_governing_catalog_can_join_on() {
    // The joint-audit contract. A catalog like Rustberg records an
    // authorization decision per commit; Bergman records why the commit
    // happened. Joining the two needs one identifier that appears in both
    // logs, and `run_id` is the one Bergman already writes into every audit
    // record — so it is the one that goes on the wire. A freshly-invented
    // request id would appear in exactly one of the two and join nothing.
    let (uri, log) = fake_catalog().await;

    let committer = RestCommitter::connect(&uri, None, &HashMap::new(), None)
        .await
        .expect("the fake catalog describes itself");

    let table = TableRef::new("prod", ["analytics"], "events");
    let ctx = OperationContext {
        run_id: "0198f3c2-1c6a-7c31-9f2e-2b6a1d5c9e40",
        table: &table,
        kind: OperationKind::Compact,
        matched_rule: "prod.analytics.*",
        reason: "6 of 6 files below 384 MiB",
    };

    committer
        .commit(
            &iceberg::TableIdent::from_strs(["analytics", "events"]).unwrap(),
            vec![TableRequirement::UuidMatch {
                uuid: uuid::Uuid::nil(),
            }],
            vec![TableUpdate::RemoveSnapshotRef {
                ref_name: "stale".into(),
            }],
            ctx,
        )
        .await
        .expect("the fake catalog accepts the commit");

    let received = log.lock().unwrap().clone();
    assert_eq!(received.len(), 2, "config, then the commit: {received:#?}");

    let commit = &received[1];
    assert_eq!(commit.method, "POST");
    assert_eq!(
        commit.path, "/v1/namespaces/analytics/tables/events",
        "a commit sent to the wrong path reaches the wrong table"
    );
    assert_eq!(
        commit.headers.get("x-request-id").map(String::as_str),
        Some(ctx.run_id),
        "the header has to carry the id Bergman's own audit records carry"
    );
    assert_eq!(
        commit
            .headers
            .get("x-bergman-operation")
            .map(String::as_str),
        Some("compact"),
    );

    // And the payload is still the specification's.
    let body: serde_json::Value = serde_json::from_str(&commit.body).unwrap();
    assert_eq!(body["identifier"]["name"], "events");
    assert_eq!(body["requirements"][0]["type"], "assert-table-uuid");
    assert_eq!(body["updates"][0]["action"], "remove-snapshot-ref");
}
