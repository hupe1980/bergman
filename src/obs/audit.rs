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
use crate::obs::OperationContext;
use crate::plan::OperationResult;

/// One audit record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    /// When it happened.
    pub at: DateTime<Utc>,
    /// The run this belongs to.
    ///
    /// Sent verbatim as `X-Request-Id` on every commit the run makes (see
    /// [`crate::commit::TableCommitter`]), so one identifier joins Bergman's
    /// record of *why* to a governing catalog's record of *who was allowed to*.
    /// That join is the whole reason the header exists, and it only works
    /// because the value is this one rather than something invented per
    /// request — which would appear in exactly one of the two logs.
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
    /// How long the operation took.
    #[serde(with = "humantime_serde", default)]
    pub took: std::time::Duration,
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
        // ...and *synced*, which is the guarantee actually being claimed:
        // `flush` reaches the kernel's buffer, which survives a `kill -9` but
        // not a node failure, and the deletion this record describes begins the
        // moment it returns. Records are per operation and per deletion *batch*
        // — not per file — so a cycle costs a handful of syncs.
        writer.get_ref().sync_data()?;
        Ok(())
    }
}

/// An [`AuditSink`] wired up as a [`super::MaintenanceObserver`].
#[derive(Debug)]
pub struct AuditObserver<S: AuditSink> {
    sink: S,
}

impl<S: AuditSink> AuditObserver<S> {
    /// Wrap a sink.
    pub fn new(sink: S) -> Self {
        Self { sink }
    }
}

impl AuditRecord {
    /// Build a record from an operation's context.
    fn from_context(ctx: OperationContext<'_>, result: OperationResult) -> Self {
        Self {
            at: Utc::now(),
            run_id: ctx.run_id.to_string(),
            table: ctx.table.to_string(),
            operation: ctx.kind.as_str().to_string(),
            matched_rule: ctx.matched_rule.to_string(),
            reason: ctx.reason.to_string(),
            result,
            took: std::time::Duration::ZERO,
            deleted_files: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl<S: AuditSink> super::MaintenanceObserver for AuditObserver<S> {
    async fn operation_finished(
        &self,
        ctx: OperationContext<'_>,
        result: &OperationResult,
        elapsed: std::time::Duration,
    ) {
        let mut record = AuditRecord::from_context(ctx, result.clone());
        record.took = elapsed;
        if let Err(e) = self.sink.write(&record) {
            // An observer cannot fail an operation — it is a hook, not a gate.
            // Losing an audit record is still worth a loud event.
            tracing::error!(error = %e, "audit record could not be written");
        }
    }

    async fn deleting_files(&self, ctx: OperationContext<'_>, paths: &[String]) {
        let mut record = AuditRecord::from_context(
            ctx,
            OperationResult::Succeeded {
                detail: format!("deleting {} files", paths.len()),
            },
        );
        record.deleted_files = paths.to_vec();

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
            took: std::time::Duration::from_secs(3),
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
                took: std::time::Duration::ZERO,
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
