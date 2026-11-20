//! Serving metrics, health and status over HTTP.
//!
//! Read-only handlers and no router extras. A maintenance engine's HTTP surface
//! should be the smallest thing that a scrape, a liveness probe and an operator
//! asking "what needs me?" can talk to — anything more is a second product
//! hiding inside the first.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;

use crate::error::{Error, Result};
use crate::obs::Metrics;
use crate::sched::StatusBoard;

/// Build the router: `/metrics`, `/health` and `/status`.
///
/// `/status` names tables and carries the text of their refusals and errors —
/// the same content the daemon prints to its own output, and nothing that
/// causes work. Bind it where you would let someone read the daemon's logs.
pub fn router(metrics: Arc<Metrics>, status: StatusBoard) -> Router {
    Router::new()
        .route("/metrics", get(scrape))
        .route("/health", get(health))
        .with_state(metrics)
        .merge(
            Router::new()
                .route("/status", get(serve_status))
                .with_state(status),
        )
}

/// The token `POST /events` must present, when one is configured.
///
/// Compared in constant time. A naive `==` on a secret leaks its length and,
/// byte by byte, its contents to anyone who can measure response latency — and
/// an endpoint reachable over the network is exactly where that is measurable.
#[derive(Clone)]
pub struct EventToken(Arc<String>);

impl std::fmt::Debug for EventToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EventToken(<redacted>)")
    }
}

impl EventToken {
    /// Require this bearer token on every notification.
    pub fn new(token: impl Into<String>) -> Self {
        Self(Arc::new(token.into()))
    }

    /// Whether a presented `Authorization` header matches.
    fn accepts(&self, header: Option<&str>) -> bool {
        let Some(presented) = header.and_then(|h| h.strip_prefix("Bearer ")) else {
            return false;
        };
        constant_time_eq(presented.as_bytes(), self.0.as_bytes())
    }
}

/// Compare two byte strings without an early exit.
///
/// Length is folded into the accumulator rather than short-circuited on, so a
/// wrong-length token costs the same as a wrong-value one.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut difference = (a.len() ^ b.len()) as u8;
    // Zipping stops at the shorter input; the length difference above is what
    // makes that safe.
    for (x, y) in a.iter().zip(b.iter()) {
        difference |= x ^ y;
    }
    difference == 0
}

/// What `/events` needs to serve a request.
#[derive(Clone)]
struct EventState {
    events: crate::sched::Events,
    /// `None` leaves the endpoint open, which is correct only on a loopback or
    /// mesh-protected bind — see [`router_with_events`].
    token: Option<EventToken>,
}

/// The same, plus an endpoint that accepts table-changed notifications.
///
/// `Lakekeeper` emits `CloudEvents` to NATS or Kafka, and Bergman carries a client
/// for neither on purpose — a maintenance engine that dragged in a message
/// broker would import the operational footprint it exists to avoid. This is
/// the seam instead: a bridge from whatever bus you already run posts here, in
/// four lines of whatever language that bridge is written in.
///
/// # `token`
///
/// Unlike `/metrics` and `/health`, this endpoint *causes work*: a notification
/// makes the daemon plan and maintain a table, which lists object storage and
/// can rewrite data. An open one on a routable address lets anyone who can
/// reach the port spend the warehouse's money.
///
/// `None` leaves it open — right on a loopback bind or behind a mesh that
/// authenticates for you, wrong everywhere else. An argument rather than a
/// default, so the choice is made rather than inherited.
pub fn router_with_events(
    metrics: Arc<Metrics>,
    status: StatusBoard,
    events: crate::sched::Events,
    token: Option<EventToken>,
) -> Router {
    router(metrics, status).merge(
        Router::new()
            .route("/events", axum::routing::post(receive_event))
            .with_state(EventState { events, token }),
    )
}

/// Bergman's own notification shape: which table changed.
#[derive(Debug, serde::Deserialize)]
struct TableChanged {
    /// The catalog, as named in Bergman's configuration.
    catalog: String,
    /// The namespace parts, outermost first.
    namespace: Vec<String>,
    /// The table name.
    table: String,
}

/// What a notification asks for.
#[derive(Debug, PartialEq)]
enum Notification {
    /// This table changed; plan it.
    Table(crate::policy::TableRef),
    /// A well-formed event that changes nothing maintenance cares about — a
    /// view, a drop, a rename. Accepted, so a bridge does not retry it.
    Ignored,
}

/// The event types that mean a table's data or metadata changed.
///
/// Lakekeeper's names: a commit is `updateTable`, and a created or registered
/// table is one Bergman has never planned.
const TABLE_CHANGED: [&str; 3] = ["updateTable", "createTable", "registerTable"];

/// Read a notification in any of the three shapes `/events` accepts.
///
/// - **Bergman's own**: `{"catalog":…,"namespace":[…],"table":…}`.
/// - **A `CloudEvent` in structured mode** (`application/cloudevents+json`, or
///   any JSON body carrying `specversion`), as Lakekeeper emits them: the table
///   is in the extension attributes `name` and `namespace` (parts joined by
///   0x1F), and `tabular-type` says whether it is a table at all.
/// - **A `CloudEvent` in binary mode**: the same attributes as `ce-` headers.
///
/// A `CloudEvent` names a warehouse by Lakekeeper's id, not a catalog by
/// Bergman's name, so a bridge says which configured catalog it speaks for:
/// `/events?catalog=<name>`.
fn notification(
    query: Option<&str>,
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> std::result::Result<Notification, &'static str> {
    // Binary mode: the envelope is in the headers.
    if headers.contains_key("ce-specversion") {
        let attribute = |name: &str| {
            headers
                .get(format!("ce-{name}"))
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        return cloud_event(
            query,
            attribute("type"),
            attribute("tabular-type"),
            attribute("namespace"),
            attribute("name"),
        );
    }

    let value: serde_json::Value = serde_json::from_slice(body).map_err(|_| BAD_SHAPE)?;
    if value.get("specversion").is_some() {
        let attribute = |name: &str| value.get(name).and_then(|v| v.as_str()).map(str::to_string);
        return cloud_event(
            query,
            attribute("type"),
            attribute("tabular-type"),
            attribute("namespace"),
            attribute("name"),
        );
    }

    let body: TableChanged = serde_json::from_value(value).map_err(|_| BAD_SHAPE)?;
    Ok(Notification::Table(crate::policy::TableRef::new(
        &body.catalog,
        body.namespace,
        &body.table,
    )))
}

const BAD_SHAPE: &str = "expected {\"catalog\":…,\"namespace\":[…],\"table\":…} or a CloudEvent";

fn cloud_event(
    query: Option<&str>,
    ty: Option<String>,
    tabular_type: Option<String>,
    namespace: Option<String>,
    name: Option<String>,
) -> std::result::Result<Notification, &'static str> {
    let ty = ty.ok_or("a CloudEvent must carry a type")?;
    if !TABLE_CHANGED.contains(&ty.as_str())
        || tabular_type.as_deref().is_some_and(|t| t != "table")
    {
        return Ok(Notification::Ignored);
    }
    let catalog = query
        .into_iter()
        .flat_map(|q| q.split('&'))
        .find_map(|pair| pair.strip_prefix("catalog="))
        .filter(|c| !c.is_empty())
        .ok_or("a CloudEvent names no Bergman catalog; post it to /events?catalog=<name>")?;
    let namespace = namespace.ok_or("this CloudEvent carries no namespace extension")?;
    let name = name.ok_or("this CloudEvent carries no name extension")?;
    Ok(Notification::Table(crate::policy::TableRef::new(
        catalog,
        namespace
            .split('\u{1f}')
            .map(str::to_string)
            .collect::<Vec<_>>(),
        &name,
    )))
}

/// Handle a notification, **authenticating before the body is looked at**.
///
/// The body arrives as [`axum::body::Bytes`] and is deserialized by hand, which
/// is the whole point of the signature: axum runs every extractor before the
/// handler's first line, so a `Json<TableChanged>` argument would reject an
/// unauthenticated request before the token was checked — answering 422 for a
/// malformed body and 401 for a good one, which tells an unauthenticated caller
/// when they have found the right shape. `Bytes` has no failure mode of its
/// own.
async fn receive_event(
    State(state): State<EventState>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    if let Some(token) = &state.token {
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        if !token.accepts(presented) {
            // No detail. A 401 that explained whether the header was missing,
            // malformed or merely wrong would be an oracle for guessing it.
            return (StatusCode::UNAUTHORIZED, "unauthorized");
        }
    }

    // Safe to be specific now that the caller is authenticated.
    let table = match notification(uri.query(), &headers, &body) {
        Ok(Notification::Table(table)) => table,
        Ok(Notification::Ignored) => return (StatusCode::ACCEPTED, "ignored"),
        Err(reason) => return (StatusCode::BAD_REQUEST, reason),
    };

    // Accepted either way. A full queue means a cycle is already pending for
    // more tables than it can be told about, and answering 503 would have a
    // bridge retry a notification that is already redundant.
    let queued = state.events.notify(table);
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

/// What the daemon is doing, and what each table last said, as JSON.
async fn serve_status(State(status): State<StatusBoard>) -> impl IntoResponse {
    axum::Json(status.snapshot())
}

/// Serve until `shutdown` resolves.
///
/// With `events`, `/events` is served too.
pub async fn serve(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    status: StatusBoard,
    events: Option<crate::sched::Events>,
    token: Option<EventToken>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::config(format!("cannot listen on {addr}: {e}")))?;

    let router = match events {
        Some(events) => {
            if token.is_none() && !addr.ip().is_loopback() {
                // Loud rather than fatal: some deployments really are behind a
                // mesh that authenticates for them, and refusing to start would
                // be wrong for those. Saying nothing would be wrong for
                // everyone else.
                tracing::warn!(
                    %addr,
                    "/events is served without a token on a routable address; \
                     anyone who can reach this port can make Bergman list object \
                     storage and rewrite data"
                );
            }
            tracing::info!(%addr, authenticated = token.is_some(), "serving /metrics, /health, /status and /events");
            router_with_events(metrics, status, events, token)
        }
        None => {
            tracing::info!(%addr, "serving /metrics, /health and /status");
            router(metrics, status)
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

        let response = router(metrics, StatusBoard::new())
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
        // Bounded labels only — see `obs::metrics` for why the table name is
        // not one of them.
        assert!(body.contains(r#"catalog="prod""#), "{body}");
        assert!(body.contains(r#"namespace="db""#), "{body}");
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
    async fn status_is_the_live_board_on_both_routers() {
        // The handle is shared, not copied: what the loop records after the
        // server starts is what `/status` serves. And the router that also
        // serves `/events` must not be the one that forgot `/status`.
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let board = StatusBoard::new();
        let (sender, _stream) = crate::sched::channel(std::time::Duration::from_secs(1));
        let routers = [
            router(Arc::new(Metrics::new()), board.clone()),
            router_with_events(Arc::new(Metrics::new()), board.clone(), sender, None),
        ];
        board.started(7, &crate::sched::Trigger::Interval);

        for router in routers {
            let response = router
                .oneshot(Request::get("/status").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let status: crate::sched::DaemonStatus = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(status.running.map(|r| r.number), Some(7));
        }
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
        let router = router_with_events(Arc::new(Metrics::new()), StatusBoard::new(), events, None);

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

    /// A commit, as Lakekeeper 0.15 emits it in structured mode: the table is
    /// in extension attributes, and a multipart namespace is joined by 0x1F.
    const LAKEKEEPER_COMMIT: &str = "{\"specversion\":\"1.0\",\"id\":\"1\",\
        \"source\":\"lakekeeper\",\"type\":\"updateTable\",\"tabular-type\":\"table\",\
        \"name\":\"events\",\"namespace\":\"analytics\\u001fraw\",\"warehouse-id\":\"w\",\
        \"data\":{}}";

    #[test]
    fn a_lakekeeper_commit_names_its_table_in_the_catalog_the_bridge_names() {
        let n = notification(
            Some("catalog=lake"),
            &axum::http::HeaderMap::new(),
            LAKEKEEPER_COMMIT.as_bytes(),
        );
        assert_eq!(
            n,
            Ok(Notification::Table(TableRef::new(
                "lake",
                ["analytics", "raw"],
                "events"
            )))
        );
    }

    #[test]
    fn a_binary_mode_cloud_event_is_read_from_its_headers() {
        let mut headers = axum::http::HeaderMap::new();
        for (k, v) in [
            ("ce-specversion", "1.0"),
            ("ce-type", "updateTable"),
            ("ce-tabular-type", "table"),
            ("ce-name", "events"),
            ("ce-namespace", "analytics"),
        ] {
            headers.insert(k, v.parse().unwrap());
        }
        assert_eq!(
            notification(Some("catalog=lake"), &headers, b"{}"),
            Ok(Notification::Table(TableRef::new(
                "lake",
                ["analytics"],
                "events"
            )))
        );
    }

    #[test]
    fn events_that_change_no_table_are_accepted_and_ignored() {
        // Accepted rather than refused, so a bridge forwarding everything does
        // not retry what was never Bergman's.
        for (ty, tabular) in [
            ("dropTable", "table"),
            ("updateTable", "view"),
            ("createWarehouse", ""),
        ] {
            let body = format!(
                "{{\"specversion\":\"1.0\",\"type\":\"{ty}\",\"tabular-type\":\"{tabular}\",\
                 \"name\":\"v\",\"namespace\":\"a\"}}"
            );
            assert_eq!(
                notification(
                    Some("catalog=lake"),
                    &axum::http::HeaderMap::new(),
                    body.as_bytes()
                ),
                Ok(Notification::Ignored),
                "{ty}/{tabular}"
            );
        }
    }

    #[test]
    fn a_cloud_event_without_a_catalog_is_refused_with_the_fix() {
        let err = notification(
            None,
            &axum::http::HeaderMap::new(),
            LAKEKEEPER_COMMIT.as_bytes(),
        )
        .unwrap_err();
        assert!(err.contains("?catalog="), "{err}");
    }

    #[tokio::test]
    async fn a_cloud_event_is_authenticated_before_it_is_parsed() {
        // The same oracle the native shape is protected from: a malformed
        // CloudEvent must not earn a 400 an unauthenticated caller can read.
        let token = Some(EventToken::new("s3cret"));
        assert_eq!(
            post_body(token.clone(), None, "{\"specversion\":\"1.0\"}").await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            post_body(token, Some("Bearer s3cret"), "{\"specversion\":\"1.0\"}").await,
            StatusCode::BAD_REQUEST,
            "and once authenticated, it is told what is missing"
        );
    }

    const GOOD_EVENT: &str = r#"{"catalog":"prod","namespace":["analytics"],"table":"events"}"#;

    /// Post a notification, optionally presenting a bearer token.
    async fn post_event(token: Option<EventToken>, present: Option<&str>) -> StatusCode {
        post_body(token, present, GOOD_EVENT).await
    }

    /// The same, with control over the body.
    async fn post_body(
        token: Option<EventToken>,
        present: Option<&str>,
        body: &'static str,
    ) -> StatusCode {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let (events, _stream) = crate::sched::channel(std::time::Duration::from_millis(1));
        let router =
            router_with_events(Arc::new(Metrics::new()), StatusBoard::new(), events, token);

        let mut request = Request::builder()
            .method("POST")
            .uri("/events")
            .header("content-type", "application/json");
        if let Some(value) = present {
            request = request.header("authorization", value);
        }

        router
            .oneshot(request.body(Body::from(body)).expect("request"))
            .await
            .expect("response")
            .status()
    }

    #[tokio::test]
    async fn the_token_is_checked_before_the_body_is_looked_at() {
        // The reason `receive_event` takes `Bytes` rather than `Json`: an
        // extractor runs before the handler, so a malformed body would answer
        // 422 and a well-formed one 401, telling an unauthenticated caller when
        // they had found the right shape.
        let token = || Some(EventToken::new("s3cret"));

        for body in [GOOD_EVENT, "not json at all", "{}", ""] {
            assert_eq!(
                post_body(token(), None, body).await,
                StatusCode::UNAUTHORIZED,
                "an unauthenticated caller learned something from body {body:?}"
            );
        }

        // With a token, the body's shape is worth reporting.
        assert_eq!(
            post_body(token(), Some("Bearer s3cret"), "not json at all").await,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn a_notification_without_the_token_is_refused() {
        // Unlike `/metrics` and `/health`, this endpoint *causes work*: a
        // notification makes the daemon plan and maintain a table, which lists
        // object storage and can rewrite data. An open one on a routable
        // address lets anyone who can reach the port spend the warehouse's
        // money.
        let token = || Some(EventToken::new("s3cret"));

        assert_eq!(post_event(token(), None).await, StatusCode::UNAUTHORIZED);
        assert_eq!(
            post_event(token(), Some("Bearer wrong")).await,
            StatusCode::UNAUTHORIZED
        );
        // A token of the right length but the wrong value, which is what a
        // length-comparing check would let through.
        assert_eq!(
            post_event(token(), Some("Bearer s3cres")).await,
            StatusCode::UNAUTHORIZED
        );
        // The scheme matters: a bare token is not a bearer credential.
        assert_eq!(
            post_event(token(), Some("s3cret")).await,
            StatusCode::UNAUTHORIZED
        );

        assert_eq!(
            post_event(token(), Some("Bearer s3cret")).await,
            StatusCode::ACCEPTED
        );
    }

    #[tokio::test]
    async fn an_unauthenticated_endpoint_still_accepts_notifications() {
        // Open is the right answer on a loopback bind or behind a mesh that
        // authenticates for you. It is a deliberate choice, not a default.
        assert_eq!(post_event(None, None).await, StatusCode::ACCEPTED);
    }

    #[test]
    fn token_comparison_does_not_short_circuit() {
        // A naive `==` leaks a secret's length and, byte by byte, its contents
        // to anyone who can measure response latency — and an endpoint on the
        // network is exactly where that is measurable.
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"ab", b"abc"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn a_token_never_reaches_a_log() {
        // `EventToken` is held by the router and reachable by `Debug` from it.
        let token = EventToken::new("s3cret");
        assert!(!format!("{token:?}").contains("s3cret"));
    }

    #[tokio::test]
    async fn events_are_not_served_unless_a_daemon_is_listening() {
        // `router` is the metrics-only shape; posting a notification nobody
        // would act on should 404 rather than silently succeed.
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = router(Arc::new(Metrics::new()), StatusBoard::new())
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
