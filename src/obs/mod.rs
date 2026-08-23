//! Observability: the hook an embedder implements, and the audit trail.
//!
//! For a tool that deletes files, the audit trail is a deliverable rather than
//! a log line. Every commit and every deletion batch produces a record naming
//! the table, the operation, the policy rule that triggered it, and what
//! changed — written *before* a deletion begins, so a crash mid-delete leaves
//! evidence of what was about to happen.
//!
//! The library installs no subscriber and opens no file on its own. It emits
//! [`tracing`] events and calls whatever [`MaintenanceObserver`] it was given;
//! the binary is what turns those into stdout, a file, or Prometheus.

mod audit;
#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "metrics")]
mod serve;

pub use audit::{AuditObserver, AuditRecord, AuditSink, JsonlSink, NullSink};
#[cfg(feature = "metrics")]
pub use metrics::{Metrics, OperationLabels};
#[cfg(feature = "metrics")]
pub use serve::{router, serve};

use std::sync::Arc;

use crate::plan::{OperationKind, OperationResult};
use crate::policy::TableRef;

/// What an observer is told about the operation it is being asked about.
///
/// A struct rather than positional arguments because every field is context an
/// audit record needs, and adding one later should not break implementors.
#[derive(Debug, Clone, Copy)]
pub struct OperationContext<'a> {
    /// Identifies this maintenance run across every record it produces.
    pub run_id: &'a str,
    /// The table being maintained.
    pub table: &'a TableRef,
    /// The operation.
    pub kind: OperationKind,
    /// The policy rule that selected this table.
    pub matched_rule: &'a str,
    /// The measurement that crossed a threshold, and the threshold it crossed.
    pub reason: &'a str,
}

/// A hook into a maintenance run.
///
/// This is the extension point the library promises instead of a plugin
/// system: an embedder wires its own metrics, approval gates, or event bus by
/// implementing this, without forking anything.
///
/// Every method has a default no-op body, so an implementation only overrides
/// what it cares about, and adding a callback later is not a breaking change
/// for existing implementors.
#[async_trait::async_trait]
pub trait MaintenanceObserver: Send + Sync + std::fmt::Debug {
    /// A table is about to be examined.
    async fn table_started(&self, _table: &TableRef) {}

    /// An operation is about to run.
    ///
    /// Returning `false` **vetoes** it, and the operation is reported as
    /// refused. This is the approval-gate hook: an embedder can require a human
    /// or a policy service to sign off on deletions without Bergman knowing
    /// anything about how that decision is made.
    async fn operation_starting(&self, _ctx: OperationContext<'_>) -> bool {
        true
    }

    /// An operation finished.
    async fn operation_finished(&self, _ctx: OperationContext<'_>, _result: &OperationResult) {}

    /// Files are about to be deleted.
    ///
    /// Called with the complete list *before* the first deletion, which is what
    /// makes the audit trail survive a crash halfway through.
    async fn deleting_files(&self, _ctx: OperationContext<'_>, _paths: &[String]) {}
}

/// An observer that does nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

#[async_trait::async_trait]
impl MaintenanceObserver for NoopObserver {}

/// Fan a run out to several observers.
///
/// Used by the binary to drive an audit sink and a metrics recorder at once,
/// and available to embedders for the same reason.
#[derive(Debug, Default)]
pub struct Observers(Vec<Arc<dyn MaintenanceObserver>>);

impl Observers {
    /// An empty set.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Add an observer.
    pub fn with(mut self, observer: Arc<dyn MaintenanceObserver>) -> Self {
        self.0.push(observer);
        self
    }

    /// Whether any observer is registered.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[async_trait::async_trait]
impl MaintenanceObserver for Observers {
    async fn table_started(&self, table: &TableRef) {
        for o in &self.0 {
            o.table_started(table).await;
        }
    }

    async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
        // Every observer is asked, and any one may veto. Short-circuiting on
        // the first `false` would mean a veto silently changes whether later
        // observers are consulted, which makes an approval gate's behaviour
        // depend on registration order.
        let mut permitted = true;
        for o in &self.0 {
            if !o.operation_starting(ctx).await {
                permitted = false;
            }
        }
        permitted
    }

    async fn operation_finished(&self, ctx: OperationContext<'_>, result: &OperationResult) {
        for o in &self.0 {
            o.operation_finished(ctx, result).await;
        }
    }

    async fn deleting_files(&self, ctx: OperationContext<'_>, paths: &[String]) {
        for o in &self.0 {
            o.deleting_files(ctx, paths).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct Vetoing(bool, Mutex<Vec<OperationKind>>);

    #[async_trait::async_trait]
    impl MaintenanceObserver for Vetoing {
        async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
            self.1.lock().unwrap().push(ctx.kind);
            self.0
        }
    }

    fn ctx<'a>(table: &'a TableRef, kind: OperationKind) -> OperationContext<'a> {
        OperationContext {
            run_id: "run-1",
            table,
            kind,
            matched_rule: "prod.**",
            reason: "test",
        }
    }

    #[tokio::test]
    async fn any_observer_may_veto_and_all_are_still_consulted() {
        // Registration order must not change who gets asked, or an approval
        // gate's behaviour would depend on where it was added.
        let permit = Arc::new(Vetoing(true, Mutex::new(Vec::new())));
        let deny = Arc::new(Vetoing(false, Mutex::new(Vec::new())));
        let after = Arc::new(Vetoing(true, Mutex::new(Vec::new())));

        let observers = Observers::new()
            .with(permit.clone())
            .with(deny.clone())
            .with(after.clone());

        let table = TableRef::new("prod", ["db"], "t");
        let allowed = observers
            .operation_starting(ctx(&table, OperationKind::RemoveOrphans))
            .await;

        assert!(!allowed);
        assert_eq!(
            after.1.lock().unwrap().len(),
            1,
            "later observer was skipped"
        );
    }

    #[tokio::test]
    async fn no_observers_permits_everything() {
        let observers = Observers::new();
        assert!(observers.is_empty());
        let table = TableRef::new("p", ["d"], "t");
        assert!(
            observers
                .operation_starting(ctx(&table, OperationKind::Compact))
                .await
        );
    }
}
