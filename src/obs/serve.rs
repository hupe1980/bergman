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
pub async fn serve(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::config(format!("cannot listen on {addr}: {e}")))?;

    tracing::info!(%addr, "serving /metrics and /health");

    axum::serve(listener, router(metrics))
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
    async fn nothing_else_is_served() {
        // The surface is two handlers. A maintenance engine that grew an API
        // here would be a second product hiding inside the first.
        let (status, _) = get_body("/", Arc::new(Metrics::new())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
