//! The audit trail records what actually happened.
//!
//! For a tool that deletes files this is a deliverable, not a log line. The
//! promise is that every record names the table, the operation, **the policy
//! rule that triggered it**, and why — so these tests hold the record to that,
//! field by field.

use bergman::obs::{AuditObserver, AuditRecord, JsonlSink, MaintenanceObserver, OperationContext};
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::TableRef;

fn read_records(path: &std::path::Path) -> Vec<AuditRecord> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("each line is one record"))
        .collect()
}

#[tokio::test]
async fn a_record_names_the_rule_and_the_reason_that_produced_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let observer = AuditObserver::new(JsonlSink::open(&path).unwrap());

    let table = TableRef::new("prod", ["analytics"], "events");
    let ctx = OperationContext {
        run_id: "run-7",
        table: &table,
        kind: OperationKind::ExpireSnapshots,
        matched_rule: "prod.analytics.*",
        reason: "oldest snapshot is 34d old (> 7d)",
    };

    observer
        .operation_finished(
            ctx,
            &OperationResult::Succeeded {
                detail: "58 snapshots expired".into(),
            },
            std::time::Duration::from_secs(47),
        )
        .await;

    let records = read_records(&path);
    assert_eq!(records.len(), 1);

    let record = &records[0];
    assert_eq!(record.run_id, "run-7");
    assert_eq!(record.table, "prod.analytics.events");
    assert_eq!(record.operation, "expire-snapshots");
    // The two fields the whole trail exists for. Empty strings here would make
    // an audit record unable to answer "why did this happen to my table?".
    assert_eq!(record.matched_rule, "prod.analytics.*");
    assert_eq!(record.reason, "oldest snapshot is 34d old (> 7d)");
    assert_eq!(record.took, std::time::Duration::from_secs(47));
}

#[tokio::test]
async fn a_deletion_manifest_lists_every_file_and_keeps_its_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let observer = AuditObserver::new(JsonlSink::open(&path).unwrap());

    let table = TableRef::new("prod", ["analytics"], "events");
    let doomed = vec![
        "s3://lake/wh/analytics/events/data/a.parquet".to_string(),
        "s3://lake/wh/analytics/events/data/b.parquet".to_string(),
    ];

    observer
        .deleting_files(
            OperationContext {
                run_id: "run-7",
                table: &table,
                kind: OperationKind::RemoveOrphans,
                matched_rule: "prod.analytics.*",
                reason: "2 orphans older than 7d",
            },
            &doomed,
        )
        .await;

    let records = read_records(&path);
    assert_eq!(records.len(), 1);
    // Written before the first delete, so a crash halfway through still leaves
    // a record of exactly what was about to go.
    assert_eq!(records[0].deleted_files, doomed);
    assert_eq!(records[0].matched_rule, "prod.analytics.*");
    assert_eq!(records[0].operation, "remove-orphans");
}

#[tokio::test]
async fn every_outcome_is_recorded_not_only_the_successes() {
    // A run that failed, was refused, or lost a commit is exactly what someone
    // reads the trail to find. Recording only successes would make the trail
    // useless for the question it exists to answer.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let observer = AuditObserver::new(JsonlSink::open(&path).unwrap());
    let table = TableRef::new("prod", ["db"], "t");

    let outcomes = [
        OperationResult::Succeeded {
            detail: "ok".into(),
        },
        OperationResult::NoOp {
            detail: "nothing to do".into(),
        },
        OperationResult::Refused {
            reason: "vetoed".into(),
        },
        OperationResult::Conflicted {
            detail: "ref moved".into(),
        },
        OperationResult::Failed {
            error: "boom".into(),
        },
    ];

    for outcome in &outcomes {
        observer
            .operation_finished(
                OperationContext {
                    run_id: "run-7",
                    table: &table,
                    kind: OperationKind::Compact,
                    matched_rule: "prod.**",
                    reason: "small files",
                },
                outcome,
                std::time::Duration::from_secs(1),
            )
            .await;
    }

    let records = read_records(&path);
    assert_eq!(records.len(), outcomes.len());
    assert!(records.iter().all(|r| r.matched_rule == "prod.**"));
}
