//! Prometheus metrics.
//!
//! Wired as a [`MaintenanceObserver`], because that is already the hook every
//! operation reports through — a metrics recorder is exactly the case the hook
//! exists for, and building it any other way would mean two paths to the same
//! facts.
//!
//! # Cardinality is the whole design
//!
//! A time series is created per label combination and kept forever, so every
//! label here is a bounded one: catalog, namespace, operation, outcome. A
//! deployment has tens of namespaces and four operations, so a catalog of any
//! size costs a few hundred series.
//!
//! Absent, and deliberately: the **table** and the **policy rule**. Both are
//! unbounded — fifty thousand tables would mean fifty thousand series per
//! metric, times the histogram's buckets. That is an outage of the monitoring
//! system, caused by the tool meant to be watching it.
//!
//! Per-table facts live in the audit trail, which has no cardinality budget.
//! Metrics answer "is maintenance working"; the audit trail answers "what
//! happened to this table".

use std::sync::Arc;

use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

use crate::obs::{MaintenanceObserver, OperationContext};
use crate::plan::OperationResult;

/// The labels an operation metric carries.
///
/// Bounded by construction — see the module docs for why the table name is not
/// among them.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OperationLabels {
    /// The catalog, as named in configuration.
    pub catalog: String,
    /// The namespace the table lives in, dotted.
    ///
    /// The finest grain that stays bounded: a deployment has tens of these,
    /// where it may have tens of thousands of tables.
    pub namespace: String,
    /// `compact`, `expire-snapshots`, …
    pub operation: String,
    /// `succeeded`, `no-op`, `refused`, `conflicted`, `failed`.
    pub outcome: String,
}

/// The labels the deletion counter carries.
///
/// No `outcome`: this counts files a deletion was *announced* for, which has no
/// outcome of its own. Reusing the operation label set and inventing a value
/// for it would put a series under `outcome="attempted"` alongside the real
/// outcomes, where it would silently corrupt any `sum by (outcome)` an operator
/// writes.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DeletionLabels {
    /// The catalog, as named in configuration.
    pub catalog: String,
    /// The namespace the table lives in, dotted.
    pub namespace: String,
    /// Which operation announced the deletion.
    pub operation: String,
}

/// Metrics for a maintenance process.
#[derive(Debug)]
pub struct Metrics {
    operations: Family<OperationLabels, Counter>,
    duration: Family<OperationLabels, Histogram>,
    files_deleted: Family<DeletionLabels, Counter>,
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
            "Maintenance operations by catalog, namespace, operation and outcome",
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

        let files_deleted = Family::<DeletionLabels, Counter>::default();
        registry.register(
            "bergman_files_deletion_announced",
            "Files a maintenance deletion was announced for, counted before the \
             first delete so a run that died mid-delete still shows the attempt",
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
            catalog: ctx.table.catalog.clone(),
            namespace: ctx.table.namespace.join("."),
            operation: ctx.kind.as_str().to_string(),
            outcome: outcome.to_string(),
        }
    }

    fn deletion_labels(ctx: OperationContext<'_>) -> DeletionLabels {
        DeletionLabels {
            catalog: ctx.table.catalog.clone(),
            namespace: ctx.table.namespace.join("."),
            operation: ctx.kind.as_str().to_string(),
        }
    }
}

/// The outcome name a metric is labelled with.
fn outcome_label(result: &OperationResult) -> &'static str {
    match result {
        OperationResult::Succeeded { .. } => "succeeded",
        OperationResult::NoOp { .. } => "no-op",
        OperationResult::Refused { .. } => "refused",
        OperationResult::Conflicted { .. } => "conflicted",
        OperationResult::Failed { .. } => "failed",
    }
}

#[async_trait::async_trait]
impl MaintenanceObserver for Metrics {
    async fn operation_finished(
        &self,
        ctx: OperationContext<'_>,
        result: &OperationResult,
        elapsed: std::time::Duration,
    ) {
        let labels = Self::labels(ctx, outcome_label(result));
        self.operations.get_or_create(&labels).inc();
        self.duration
            .get_or_create(&labels)
            .observe(elapsed.as_secs_f64());
    }

    async fn deleting_files(&self, ctx: OperationContext<'_>, paths: &[String]) {
        // Counted when the deletion is announced rather than after it, so a run
        // that died mid-delete still shows the attempt — which is the number
        // worth alerting on.
        self.files_deleted
            .get_or_create(&Self::deletion_labels(ctx))
            .inc_by(paths.len() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
            metrics
                .operation_finished(ctx(&table), &result, Duration::from_secs(2))
                .await;
        }

        let encoded = metrics.encode();
        assert!(encoded.contains(r#"outcome="succeeded""#), "{encoded}");
        assert!(encoded.contains(r#"outcome="failed""#), "{encoded}");
        assert!(encoded.contains(r#"outcome="conflicted""#), "{encoded}");
        // Durations land in the histogram now rather than in a method nobody
        // called.
        assert!(
            encoded.contains("bergman_operation_duration_seconds"),
            "{encoded}"
        );
    }

    #[tokio::test]
    async fn deletions_are_counted_when_announced() {
        let metrics = Metrics::new();
        let table = TableRef::new("prod", ["db"], "t");

        metrics
            .deleting_files(ctx(&table), &["a".into(), "b".into(), "c".into()])
            .await;

        let encoded = metrics.encode();
        assert!(
            encoded.contains("bergman_files_deletion_announced"),
            "{encoded}"
        );
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
                Duration::from_secs(1),
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

    #[tokio::test]
    async fn no_label_is_unbounded() {
        // The property the whole module is designed around. A time series is
        // created per label combination and kept forever, so an unbounded label
        // is an outage of the monitoring system caused by the tool that was
        // supposed to be watching. Both tempting ones — the table and the rule —
        // are absent, and their facts live in the audit trail instead.
        let metrics = Metrics::new();
        let table = TableRef::new("prod", ["analytics", "web"], "events");

        metrics
            .operation_finished(
                ctx(&table),
                &OperationResult::Succeeded {
                    detail: "ok".into(),
                },
                Duration::from_secs(1),
            )
            .await;
        metrics.deleting_files(ctx(&table), &["a".into()]).await;

        let encoded = metrics.encode();
        assert!(
            !encoded.contains(r#"table="#),
            "table is unbounded: {encoded}"
        );
        assert!(!encoded.contains("matched_rule"), "{encoded}");
        assert!(!encoded.contains("prod.analytics.web.events"), "{encoded}");

        // The bounded grain survives, and it is the one an alert groups by.
        assert!(encoded.contains(r#"catalog="prod""#), "{encoded}");
        assert!(
            encoded.contains(r#"namespace="analytics.web""#),
            "{encoded}"
        );
    }

    #[tokio::test]
    async fn the_deletion_counter_does_not_invent_an_outcome() {
        // Files announced for deletion have no outcome of their own. Putting
        // them under `outcome="attempted"` alongside the real outcomes would
        // silently corrupt any `sum by (outcome)` an operator writes.
        let metrics = Metrics::new();
        let table = TableRef::new("prod", ["db"], "t");
        metrics.deleting_files(ctx(&table), &["a".into()]).await;

        let encoded = metrics.encode();
        assert!(
            encoded.contains("bergman_files_deletion_announced"),
            "{encoded}"
        );
        assert!(!encoded.contains("attempted"), "{encoded}");
    }
}
