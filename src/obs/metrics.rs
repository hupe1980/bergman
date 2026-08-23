//! Prometheus metrics.
//!
//! Wired as a [`MaintenanceObserver`], because that is already the hook every
//! operation reports through — a metrics recorder is exactly the case the hook
//! exists for, and building it any other way would mean two paths to the same
//! facts.
//!
//! The labels are the ones an operator groups by when something looks wrong:
//! the table, the operation, and the outcome. Notably *not* the policy rule —
//! a rule pattern is low-cardinality today and unbounded tomorrow, and a label
//! that grows without limit takes a Prometheus server down.

use std::sync::Arc;

use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

use crate::obs::{MaintenanceObserver, OperationContext};
use crate::plan::OperationResult;

/// The labels every operation metric carries.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OperationLabels {
    /// The fully-qualified table.
    pub table: String,
    /// `compact`, `expire-snapshots`, …
    pub operation: String,
    /// `succeeded`, `no-op`, `refused`, `conflicted`, `failed`, `blocked`.
    pub outcome: String,
}

/// Metrics for a maintenance process.
#[derive(Debug)]
pub struct Metrics {
    operations: Family<OperationLabels, Counter>,
    duration: Family<OperationLabels, Histogram>,
    files_deleted: Family<OperationLabels, Counter>,
    registry: Arc<std::sync::Mutex<Registry>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Build the metric families and register them.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let operations = Family::<OperationLabels, Counter>::default();
        registry.register(
            "bergman_operations",
            "Maintenance operations by table, operation and outcome",
            operations.clone(),
        );

        let duration = Family::<OperationLabels, Histogram>::new_with_constructor(|| {
            // A rewrite runs for seconds to minutes; a metadata-only expiration
            // for milliseconds. One second to roughly two hours across twelve
            // buckets covers both without an unreadable number of series.
            Histogram::new(exponential_buckets(1.0, 2.0, 12))
        });
        registry.register(
            "bergman_operation_duration_seconds",
            "How long each maintenance operation took",
            duration.clone(),
        );

        let files_deleted = Family::<OperationLabels, Counter>::default();
        registry.register(
            "bergman_files_deleted",
            "Files deleted by maintenance",
            files_deleted.clone(),
        );

        Self {
            operations,
            duration,
            files_deleted,
            registry: Arc::new(std::sync::Mutex::new(registry)),
        }
    }

    /// Render the current values in Prometheus text format.
    pub fn encode(&self) -> String {
        let mut out = String::new();
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if encode(&mut out, &registry).is_err() {
            // Encoding writes into a `String`, so this cannot fail for I/O
            // reasons. Returning empty beats panicking in a scrape handler.
            return String::new();
        }
        out
    }

    fn labels(ctx: OperationContext<'_>, outcome: &str) -> OperationLabels {
        OperationLabels {
            table: ctx.table.to_string(),
            operation: ctx.kind.as_str().to_string(),
            outcome: outcome.to_string(),
        }
    }
}

/// The outcome name a metric is labelled with.
fn outcome_label(result: &OperationResult) -> &'static str {
    match result {
        OperationResult::Succeeded { .. } => "succeeded",
        OperationResult::NoOp { .. } => "no-op",
        OperationResult::Blocked { .. } => "blocked",
        OperationResult::Refused { .. } => "refused",
        OperationResult::Conflicted { .. } => "conflicted",
        OperationResult::Failed { .. } => "failed",
    }
}

#[async_trait::async_trait]
impl MaintenanceObserver for Metrics {
    async fn operation_finished(&self, ctx: OperationContext<'_>, result: &OperationResult) {
        self.operations
            .get_or_create(&Self::labels(ctx, outcome_label(result)))
            .inc();
    }

    async fn deleting_files(&self, ctx: OperationContext<'_>, paths: &[String]) {
        // Counted when the deletion is announced rather than after it, so a run
        // that died mid-delete still shows the attempt — which is the number
        // worth alerting on.
        self.files_deleted
            .get_or_create(&Self::labels(ctx, "attempted"))
            .inc_by(paths.len() as u64);
    }
}

impl Metrics {
    /// Record how long an operation took.
    ///
    /// Separate from the observer because a duration is the engine's to
    /// measure — the observer is told what happened, not how long it ran.
    pub fn record_duration(
        &self,
        ctx: OperationContext<'_>,
        result: &OperationResult,
        elapsed: std::time::Duration,
    ) {
        self.duration
            .get_or_create(&Self::labels(ctx, outcome_label(result)))
            .observe(elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::OperationKind;
    use crate::policy::TableRef;

    fn ctx<'a>(table: &'a TableRef) -> OperationContext<'a> {
        OperationContext {
            run_id: "run-1",
            table,
            kind: OperationKind::Compact,
            matched_rule: "prod.**",
            reason: "small files",
        }
    }

    #[tokio::test]
    async fn every_outcome_gets_its_own_series() {
        // An operator alerting on failures needs them separable from no-ops,
        // and a run that lost a commit is neither.
        let metrics = Metrics::new();
        let table = TableRef::new("prod", ["db"], "t");

        for result in [
            OperationResult::Succeeded {
                detail: "ok".into(),
            },
            OperationResult::Failed {
                error: "boom".into(),
            },
            OperationResult::Conflicted {
                detail: "moved".into(),
            },
        ] {
            metrics.operation_finished(ctx(&table), &result).await;
        }

        let encoded = metrics.encode();
        assert!(encoded.contains(r#"outcome="succeeded""#), "{encoded}");
        assert!(encoded.contains(r#"outcome="failed""#), "{encoded}");
        assert!(encoded.contains(r#"outcome="conflicted""#), "{encoded}");
    }

    #[tokio::test]
    async fn deletions_are_counted_when_announced() {
        let metrics = Metrics::new();
        let table = TableRef::new("prod", ["db"], "t");

        metrics
            .deleting_files(ctx(&table), &["a".into(), "b".into(), "c".into()])
            .await;

        let encoded = metrics.encode();
        assert!(encoded.contains("bergman_files_deleted"), "{encoded}");
        assert!(encoded.contains("3"), "{encoded}");
    }

    #[tokio::test]
    async fn the_output_is_openmetrics_text_format() {
        let metrics = Metrics::new();
        let table = TableRef::new("prod", ["db"], "t");
        metrics
            .operation_finished(
                ctx(&table),
                &OperationResult::Succeeded {
                    detail: "ok".into(),
                },
            )
            .await;

        let encoded = metrics.encode();
        assert!(encoded.contains("# HELP bergman_operations"), "{encoded}");
        assert!(encoded.contains("# TYPE bergman_operations"), "{encoded}");
        // OpenMetrics requires the terminator, and a scraper rejects an
        // exposition without it.
        assert!(encoded.trim_end().ends_with("# EOF"), "{encoded}");
    }

    #[test]
    fn an_empty_registry_is_still_a_valid_exposition() {
        // A family with no series emits no HELP or TYPE — that is Prometheus
        // behaviour, not a bug — so a scrape before the first cycle is a
        // well-formed empty document rather than an error.
        let encoded = Metrics::new().encode();
        assert_eq!(encoded.trim_end(), "# EOF");
    }

    #[test]
    fn the_policy_rule_is_not_a_label() {
        // Rule patterns are low-cardinality today and unbounded tomorrow, and a
        // label that grows without limit takes a Prometheus server down. The
        // rule belongs in the audit trail, which is where it is.
        let metrics = Metrics::new();
        assert!(!metrics.encode().contains("matched_rule"));
    }
}
