//! Serving metrics and health over HTTP.
//!
//! Two handlers and no router extras. A maintenance engine's HTTP surface
//! should be the smallest thing that a scrape and a liveness probe can talk to
//! — anything more is a second product hiding inside the first.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;

use crate::error::{Error, Result};
use crate::obs::Metrics;

/// Build the router.
pub fn router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/metrics", get(scrape))
        .route("/health", get(health))
        .with_state(metrics)
}

/// The same, plus an endpoint that accepts table-changed notifications.
///
/// `Lakekeeper` emits `CloudEvents` to NATS or Kafka, and Bergman carries a client
/// for neither on purpose — a maintenance engine that dragged in a message
/// broker would import the operational footprint it exists to avoid. This is
/// the seam instead: a bridge from whatever bus you already run posts here, in
/// four lines of whatever language that bridge is written in.
pub fn router_with_events(metrics: Arc<Metrics>, events: crate::sched::Events) -> Router {
    Router::new()
        .route("/metrics", get(scrape))
        .route("/health", get(health))
        .with_state(metrics)
        .merge(
            Router::new()
                .route("/events", axum::routing::post(receive_event))
                .with_state(events),
        )
}

/// The subset of a `CloudEvent` that identifies a table.
///
/// Deliberately not a `CloudEvents` parser. The envelope carries a dozen fields
/// Bergman has no use for, and validating them would make this endpoint reject
/// perfectly good notifications for reasons that do not matter to it. What it
/// needs is which table changed.
#[derive(Debug, serde::Deserialize)]
struct TableChanged {
    /// The catalog, as named in Bergman's configuration.
    catalog: String,
    /// The namespace parts, outermost first.
    namespace: Vec<String>,
    /// The table name.
    table: String,
}

async fn receive_event(
    State(events): State<crate::sched::Events>,
    body: axum::extract::Json<TableChanged>,
) -> impl IntoResponse {
    let table = crate::policy::TableRef::new(&body.catalog, body.namespace.clone(), &body.table);

    // Accepted either way. A full queue means a cycle is already pending for
    // more tables than it can be told about, and answering 503 would have a
    // bridge retry a notification that is already redundant.
    let queued = events.notify(table);
    (
        StatusCode::ACCEPTED,
        if queued { "queued" } else { "coalesced" },
    )
}

async fn scrape(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            // The version Prometheus negotiates for text exposition. Serving
            // `text/plain` alone works but makes some scrapers guess.
            HeaderValue::from_static("application/openmetrics-text; version=1.0.0; charset=utf-8"),
        )],
        metrics.encode(),
    )
}

/// Liveness, not readiness.
///
/// The process being up is all this can honestly report. Whether the *catalog*
/// is reachable is not something to answer here: a probe that failed while a
/// catalog was briefly down would have Kubernetes restart a process that is
/// working perfectly, and restarting it would not help.
async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Serve until `shutdown` resolves.
///
/// With `events`, `/events` is served too.
pub async fn serve(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    events: Option<crate::sched::Events>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::config(format!("cannot listen on {addr}: {e}")))?;

    let router = match events {
        Some(events) => {
            tracing::info!(%addr, "serving /metrics, /health and /events");
            router_with_events(metrics, events)
        }
        None => {
            tracing::info!(%addr, "serving /metrics and /health");
            router(metrics)
        }
    };

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|e| Error::config(format!("the metrics server stopped: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::{MaintenanceObserver, OperationContext};
    use crate::plan::{OperationKind, OperationResult};
    use crate::policy::TableRef;

    async fn get_body(path: &str, metrics: Arc<Metrics>) -> (StatusCode, String) {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = router(metrics)
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    #[tokio::test]
    async fn metrics_are_served_in_the_format_prometheus_expects() {
        let metrics = Arc::new(Metrics::new());
        let table = TableRef::new("prod", ["db"], "t");

        metrics
            .operation_finished(
                OperationContext {
                    run_id: "run-1",
                    table: &table,
                    kind: OperationKind::ExpireSnapshots,
                    matched_rule: "prod.**",
                    reason: "old snapshots",
                },
                &OperationResult::Succeeded {
                    detail: "8 expired".into(),
                },
                std::time::Duration::from_secs(1),
            )
            .await;

        let (status, body) = get_body("/metrics", metrics).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("bergman_operations"), "{body}");
        assert!(body.contains(r#"table="prod.db.t""#), "{body}");
        assert!(body.contains(r#"operation="expire-snapshots""#), "{body}");
    }

    #[tokio::test]
    async fn a_scrape_before_the_first_cycle_succeeds() {
        // A scraper that got a 404 or a connection error before the first cycle
        // would alert on a process that is simply idle. An empty exposition is
        // the right answer: no series yet, but a well-formed document.
        let (status, body) = get_body("/metrics", Arc::new(Metrics::new())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.trim_end(), "# EOF");
    }

    #[tokio::test]
    async fn health_reports_liveness_only() {
        let (status, body) = get_body("/health", Arc::new(Metrics::new())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn a_notification_names_the_table_that_changed() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let (events, mut stream) = crate::sched::channel(std::time::Duration::from_millis(1));
        let router = router_with_events(Arc::new(Metrics::new()), events);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/events")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"catalog":"prod","namespace":["analytics"],"table":"events"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let batch = stream.next_batch().await.expect("a batch");
        assert_eq!(batch, vec![TableRef::new("prod", ["analytics"], "events")]);
    }

    #[tokio::test]
    async fn events_are_not_served_unless_a_daemon_is_listening() {
        // `router` is the metrics-only shape; posting a notification nobody
        // would act on should 404 rather than silently succeed.
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = router(Arc::new(Metrics::new()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/events")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn nothing_else_is_served() {
        // The surface is two handlers. A maintenance engine that grew an API
        // here would be a second product hiding inside the first.
        let (status, _) = get_body("/", Arc::new(Metrics::new())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
