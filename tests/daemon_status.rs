//! A running daemon reports itself on `/status`, and `bergman status` reads it.
//!
//! Driven end to end through the production constructors: `Bergman::builder`
//! connects to a catalog over REST, `Daemon` runs a real cycle, and the board
//! it hands out is served by the same router the binary serves. The catalog
//! holds no namespaces, which is enough to show the loop records into the board
//! — the board's clearing rules are unit-tested beside it.
#![cfg(feature = "metrics")]

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use bergman::sched::{Daemon, DaemonConfig};
use bergman::{Bergman, Config};

/// A catalog that answers the handshake and holds nothing.
async fn empty_catalog() -> String {
    let app = Router::new()
        .route(
            "/v1/config",
            get(|| async { axum::Json(serde_json::json!({"defaults": {}, "overrides": {}})) }),
        )
        .route(
            "/v1/namespaces",
            get(|| async { axum::Json(serde_json::json!({"namespaces": []})) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    uri
}

#[tokio::test]
async fn a_finished_cycle_is_on_the_status_endpoint() {
    let uri = empty_catalog().await;
    let config = Config::from_toml(&format!(
        r#"
        [[catalogs]]
        name = "prod"
        kind = "rest"
        uri = "{uri}"

        [[rules]]
        match = "prod.**"
        "#
    ))
    .unwrap();
    let bergman = Bergman::builder(config).build().await.unwrap();

    // The interval is short so the one cycle starts at once.
    let daemon = Daemon::new(
        Arc::new(bergman),
        DaemonConfig {
            interval: Duration::from_millis(10),
            max_cycles: Some(1),
        },
    )
    .unwrap();
    let board = daemon.status();
    assert_eq!(board.snapshot().cycles, 0);

    let mut cycles = Vec::new();
    daemon
        .run(|c| cycles.push(c.outcome.is_ok()), std::future::pending())
        .await
        .unwrap();
    assert_eq!(cycles, vec![true]);

    // Served the way the binary serves it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = bergman::obs::router(Arc::new(bergman::obs::Metrics::new()), board);
    tokio::spawn(async move { axum::serve(listener, app).await });

    let status: bergman::sched::DaemonStatus = reqwest::get(format!("http://{addr}/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(status.cycles, 1);
    assert!(status.running.is_none());
    assert!(!status.needs_attention());
    let last = status.last_cycle.expect("the cycle is recorded");
    assert_eq!(last.number, 1);
    assert_eq!(last.trigger, "interval");
    assert!(last.error.is_none(), "{:?}", last.error);
    assert!(last.summary.is_some());
}
