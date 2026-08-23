//! The audit trail.
//!
//! Append-only JSON Lines. Not a columnar format: an audit stream is written
//! once and read rarely, the records are small, and every log pipeline already
//! ingests JSONL.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::plan::{OperationKind, OperationResult};
use crate::policy::TableRef;

/// One audit record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    /// When it happened.
    pub at: DateTime<Utc>,
    /// The run this belongs to.
    ///
    /// Also sent as `X-Request-Id` on the catalog calls the operation makes, so
    /// one identifier joins Bergman's record of *why* to the catalog's record
    /// of *who was allowed to*.
    pub run_id: String,
    /// The table.
    pub table: String,
    /// The operation.
    ///
    /// Owned rather than `&'static str`: an audit trail exists to be read back,
    /// and a borrowed field makes the record undeserializable at any lifetime
    /// shorter than the program.
    pub operation: String,
    /// The policy rule that triggered it.
    pub matched_rule: String,
    /// Why the trigger fired.
    pub reason: String,
    /// What happened.
    pub result: OperationResult,
    /// Files deleted, when this record describes a deletion.
    ///
    /// Written *before* the deletion runs. A crash halfway through then leaves
    /// a record of exactly what was about to be removed, which is the
    /// difference between an incident with evidence and one without.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted_files: Vec<String>,
}

/// Where audit records go.
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    /// Write one record.
    ///
    /// Implementations should be durable before returning where they can be:
    /// the caller treats a successful write as permission to proceed with a
    /// deletion.
    fn write(&self, record: &AuditRecord) -> Result<()>;
}

/// Discard every record.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

impl AuditSink for NullSink {
    fn write(&self, _record: &AuditRecord) -> Result<()> {
        Ok(())
    }
}

/// Append records to a file as JSON Lines.
#[derive(Debug)]
pub struct JsonlSink {
    writer: Mutex<BufWriter<File>>,
}

impl JsonlSink {
    /// Open (or create) an audit file for appending.
    ///
    /// A file that cannot be opened is an error here rather than a silent
    /// downgrade to no audit: discovering at incident time that the trail was
    /// never being written is the worst moment to discover it.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| Error::config(format!("audit file {}: {e}", path.display())))?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }
}

impl AuditSink for JsonlSink {
    fn write(&self, record: &AuditRecord) -> Result<()> {
        let line = serde_json::to_string(record)
            .map_err(|e| Error::config(format!("audit record could not be serialized: {e}")))?;

        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        writeln!(writer, "{line}")?;
        // Flushed per record rather than per batch. An audit trail buffered in
        // a process that then dies describes a world that never existed.
        writer.flush()?;
        Ok(())
    }
}

/// An [`AuditSink`] wired up as a [`super::MaintenanceObserver`].
#[derive(Debug)]
pub struct AuditObserver<S: AuditSink> {
    sink: S,
    run_id: String,
}

impl<S: AuditSink> AuditObserver<S> {
    /// Wrap a sink for one run.
    pub fn new(sink: S, run_id: impl Into<String>) -> Self {
        Self {
            sink,
            run_id: run_id.into(),
        }
    }
}

#[async_trait::async_trait]
impl<S: AuditSink> super::MaintenanceObserver for AuditObserver<S> {
    async fn operation_finished(
        &self,
        table: &TableRef,
        kind: OperationKind,
        result: &OperationResult,
    ) {
        let record = AuditRecord {
            at: Utc::now(),
            run_id: self.run_id.clone(),
            table: table.to_string(),
            operation: kind.as_str().to_string(),
            // Filled by the engine, which knows the rule; an observer sees only
            // the operation. Kept as a field rather than dropped so the record
            // shape is the same from every producer.
            matched_rule: String::new(),
            reason: String::new(),
            result: result.clone(),
            deleted_files: Vec::new(),
        };
        if let Err(e) = self.sink.write(&record) {
            // An observer cannot fail an operation — it is a hook, not a gate.
            // Losing an audit record is still worth a loud event.
            tracing::error!(error = %e, "audit record could not be written");
        }
    }

    async fn deleting_files(&self, table: &TableRef, paths: &[String]) {
        let record = AuditRecord {
            at: Utc::now(),
            run_id: self.run_id.clone(),
            table: table.to_string(),
            operation: OperationKind::RemoveOrphans.as_str().to_string(),
            matched_rule: String::new(),
            reason: format!("{} files", paths.len()),
            result: OperationResult::Succeeded {
                detail: "deletion starting".into(),
            },
            deleted_files: paths.to_vec(),
        };
        if let Err(e) = self.sink.write(&record) {
            tracing::error!(error = %e, "deletion manifest could not be written");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_as_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlSink::open(&path).unwrap();

        sink.write(&AuditRecord {
            at: Utc::now(),
            run_id: "run-1".into(),
            table: "prod.db.t".into(),
            operation: "expire-snapshots".to_string(),
            matched_rule: "prod.*".into(),
            reason: "oldest snapshot is 30d old".into(),
            result: OperationResult::Succeeded {
                detail: "8 snapshots".into(),
            },
            deleted_files: vec![],
        })
        .unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 1);

        let parsed: AuditRecord = serde_json::from_str(contents.lines().next().unwrap()).unwrap();
        assert_eq!(parsed.table, "prod.db.t");
        assert_eq!(parsed.operation, "expire-snapshots");
    }

    #[test]
    fn appending_preserves_earlier_records() {
        // The trail is append-only; reopening must not truncate what a previous
        // run recorded.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");

        for run in ["run-1", "run-2"] {
            let sink = JsonlSink::open(&path).unwrap();
            sink.write(&AuditRecord {
                at: Utc::now(),
                run_id: run.into(),
                table: "prod.db.t".into(),
                operation: "expire-snapshots".to_string(),
                matched_rule: "prod.*".into(),
                reason: String::new(),
                result: OperationResult::NoOp {
                    detail: "nothing".into(),
                },
                deleted_files: vec![],
            })
            .unwrap();
        }

        assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 2);
    }

    #[test]
    fn an_unopenable_audit_file_is_an_error_not_a_downgrade() {
        let dir = tempfile::tempdir().unwrap();
        // A directory is not a file: opening it for append fails, and that must
        // surface rather than leaving the run silently unaudited.
        assert!(JsonlSink::open(dir.path()).is_err());
    }
}
