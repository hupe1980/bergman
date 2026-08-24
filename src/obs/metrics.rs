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
//! label here is bounded: catalog, namespace, operation, outcome.
//!
//! Absent, and deliberately: the **table** and the **policy rule**. Both are
//! unbounded — fifty thousand tables would mean fifty thousand series per
//! metric, times the histogram's buckets. That is an outage of the monitoring
//! system, caused by the tool meant to be watching it.
//!
//! Per-table facts live in the audit trail, which has no cardinality budget.
//! Metrics answer "is maintenance working"; the audit trail answers "what
//! happened to this table".
//!
//! ## The namespace label is capped, not assumed bounded
//!
//! "A warehouse has tens of namespaces" is an assumption, and it is wrong for
//! exactly the layouts that need maintenance most: one per tenant, or per day.
//! Left unbounded, `namespace` reproduces the table-label outage one level up.
//!
//! So the first [`MAX_NAMESPACE_SERIES`] namespaces a process observes keep
//! their own series and later ones fold into [`OVERFLOW_NAMESPACE`], reported
//! once. First-come is arbitrary but *stable*: a namespace does not move between
//! its own series and the shared one partway through a run, so a counter cannot
//! appear to stop.

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
    /// The finest grain that can be kept bounded — a warehouse has orders of
    /// magnitude fewer namespaces than tables — and it *is* kept bounded, by
    /// [`MAX_NAMESPACE_SERIES`] rather than by hoping. Past the cap this reads
    /// [`OVERFLOW_NAMESPACE`].
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

/// How many distinct namespaces get a series of their own.
///
/// Every namespace beyond this shares [`OVERFLOW_NAMESPACE`]. Sized for the
/// worst case rather than the typical one: the duration histogram is the
/// expensive family, and a namespace reaching every operation and outcome costs
/// `4 × 5 × 14` series on its own.
pub const MAX_NAMESPACE_SERIES: usize = 128;

/// The namespace label every namespace past the cap is folded into.
///
/// Angle brackets because no Iceberg namespace can be spelled this way, so it
/// cannot collide with a real one.
pub const OVERFLOW_NAMESPACE: &str = "<over-cardinality-cap>";

/// Metrics for a maintenance process.
#[derive(Debug)]
pub struct Metrics {
    operations: Family<OperationLabels, Counter>,
    duration: Family<OperationLabels, Histogram>,
    files_deleted: Family<DeletionLabels, Counter>,
    /// The namespaces that have a series of their own, and whether the cap has
    /// already been reported.
    ///
    /// A lock per operation, which is a handful per table per cycle against work
    /// measured in manifest reads.
    namespaces: std::sync::Mutex<(std::collections::HashSet<String>, bool)>,
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
            namespaces: std::sync::Mutex::new((std::collections::HashSet::new(), false)),
            registry: Arc::new(std::sync::Mutex::new(registry)),
        }
    }

    /// The label value a namespace is recorded under.
    ///
    /// Its own name while the cap has room, and [`OVERFLOW_NAMESPACE`] after —
    /// see the module docs for why the cap is enforced rather than assumed.
    fn namespace_label(&self, namespace: String) -> String {
        let mut guard = self
            .namespaces
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (seen, reported) = &mut *guard;

        if seen.contains(&namespace) {
            return namespace;
        }
        if seen.len() < MAX_NAMESPACE_SERIES {
            seen.insert(namespace.clone());
            return namespace;
        }
        if !*reported {
            *reported = true;
            // Once, not per operation: a warning repeated per table would be
            // the same cardinality problem moved into the log.
            tracing::warn!(
                cap = MAX_NAMESPACE_SERIES,
                first_folded = %namespace,
                "more namespaces than the metric cardinality cap; the rest share \
                 the {OVERFLOW_NAMESPACE} label. Per-namespace facts beyond the cap \
                 are in the audit trail, which has no cardinality budget"
            );
        }
        OVERFLOW_NAMESPACE.to_string()
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

    fn labels(&self, ctx: OperationContext<'_>, outcome: &str) -> OperationLabels {
        OperationLabels {
            catalog: ctx.table.catalog.clone(),
            namespace: self.namespace_label(ctx.table.namespace.join(".")),
            operation: ctx.kind.as_str().to_string(),
            outcome: outcome.to_string(),
        }
    }

    fn deletion_labels(&self, ctx: OperationContext<'_>) -> DeletionLabels {
        DeletionLabels {
            catalog: ctx.table.catalog.clone(),
            namespace: self.namespace_label(ctx.table.namespace.join(".")),
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
        let labels = self.labels(ctx, outcome_label(result));
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
            .get_or_create(&self.deletion_labels(ctx))
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
    async fn the_namespace_label_is_capped_rather_than_trusted() {
        // "A deployment has tens of namespaces" is an assumption, and it is
        // wrong for exactly the layouts that need maintenance most — a
        // namespace per tenant or per day. Unbounded, this label reproduces the
        // table-label outage one level up.
        let metrics = Metrics::new();

        for i in 0..(MAX_NAMESPACE_SERIES + 50) {
            let table = TableRef::new("prod", [format!("tenant_{i}")], "events");
            metrics
                .operation_finished(
                    ctx(&table),
                    &OperationResult::Succeeded {
                        detail: "ok".into(),
                    },
                    Duration::from_secs(1),
                )
                .await;
        }

        let encoded = metrics.encode();
        let distinct = encoded
            .lines()
            .filter(|line| line.starts_with("bergman_operations_total{"))
            .count();
        assert_eq!(
            distinct,
            MAX_NAMESPACE_SERIES + 1,
            "the cap plus one shared overflow series, got {distinct}"
        );

        // The first namespaces keep their own identity...
        assert!(encoded.contains(r#"namespace="tenant_0""#), "{encoded}");
        // ...and the ones past the cap share a label no real namespace can
        // collide with, rather than silently disappearing.
        assert!(
            encoded.contains(&format!(r#"namespace="{OVERFLOW_NAMESPACE}""#)),
            "{encoded}"
        );
    }

    #[tokio::test]
    async fn a_namespace_never_moves_between_its_series_and_the_shared_one() {
        // First-come is arbitrary, but it has to be *stable*: a namespace that
        // drifted into the overflow bucket partway through would make its own
        // counter appear to stop.
        let metrics = Metrics::new();
        let early = TableRef::new("prod", ["first"], "t");

        let record = async |table: &TableRef| {
            metrics
                .operation_finished(
                    ctx(table),
                    &OperationResult::Succeeded {
                        detail: "ok".into(),
                    },
                    Duration::from_secs(1),
                )
                .await;
        };

        record(&early).await;
        for i in 0..(MAX_NAMESPACE_SERIES + 50) {
            record(&TableRef::new("prod", [format!("later_{i}")], "t")).await;
        }
        record(&early).await;

        let encoded = metrics.encode();
        let line = encoded
            .lines()
            .find(|l| l.contains(r#"namespace="first""#))
            .unwrap_or_else(|| panic!("the early namespace lost its series:\n{encoded}"));
        assert!(
            line.trim_end().ends_with(" 2"),
            "both observations must land on the same series: {line}"
        );
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
