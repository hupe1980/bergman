//! Both clients present one token, from one source, and the read path renews it.
//!
//! Bergman reads through `iceberg-catalog-rest` and commits through its own
//! client. If the two authenticated separately they would drift — reads
//! succeeding while every commit returns 401, or the reverse an hour into a
//! daemon's life when the read client's one-shot token expires. These tests
//! drive the production constructor, [`CatalogConfig::connect`], against a
//! catalog that speaks just enough of the protocol to show which token each
//! request carried.
//!
//! A raw `TcpListener` rather than a mock framework, as in `commit_wire.rs`:
//! speaking the protocol by hand is what makes this a test of the wire.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bergman::catalog::{CatalogConfig, CatalogKind};

/// What the fake catalog saw.
#[derive(Debug, Default)]
struct Seen {
    /// Token exchanges, in order.
    exchanges: usize,
    /// `(path, Authorization header)` for every request that was not an exchange.
    requests: Vec<(String, Option<String>)>,
}

/// A catalog with an `OAuth2` token endpoint, issuing `token-1`, `token-2`, …
/// each valid for `expires_in` seconds.
async fn fake_catalog(expires_in: u64) -> (String, Arc<Mutex<Seen>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let seen: Arc<Mutex<Seen>> = Arc::default();

    let state = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buffer = [0u8; 4096];
                let head_end = loop {
                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                    if read == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buffer[..read]);
                    if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let mut lines = head.lines();
                let path = lines
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_string();
                let headers: HashMap<String, String> = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                    .collect();
                let want: usize = headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                while raw.len() < head_end + want {
                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                    if read == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buffer[..read]);
                }

                let payload = if path.starts_with("/v1/oauth/tokens") {
                    let mut seen = state.lock().unwrap();
                    seen.exchanges += 1;
                    format!(
                        r#"{{"access_token":"token-{}","token_type":"bearer","expires_in":{expires_in}}}"#,
                        seen.exchanges
                    )
                } else {
                    state
                        .lock()
                        .unwrap()
                        .requests
                        .push((path.clone(), headers.get("authorization").cloned()));
                    if path.starts_with("/v1/config") {
                        r#"{"defaults":{},"overrides":{}}"#.to_string()
                    } else {
                        r#"{"namespaces":[]}"#.to_string()
                    }
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{payload}",
                    payload.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    (uri, seen)
}

fn config(uri: &str) -> CatalogConfig {
    CatalogConfig {
        name: "prod".into(),
        kind: CatalogKind::Rest,
        uri: uri.into(),
        warehouse: None,
        storage: None,
        properties: HashMap::from([("credential".into(), "svc-bergman:hunter2".into())]),
        token_env: None,
        namespaces: None,
        policy_source: None,
    }
}

#[tokio::test]
async fn both_clients_send_the_token_from_one_source() {
    let (uri, seen) = fake_catalog(3600).await;

    let connection = config(&uri).connect().await.expect("connects");
    connection
        .client
        .list_namespaces(None)
        .await
        .expect("the read client lists namespaces");

    let seen = seen.lock().unwrap();
    // One exchange, although two clients started and each made requests: the
    // commit client's `/v1/config` handshake and the read client's handshake
    // and listing all drew from the same cache.
    assert_eq!(seen.exchanges, 1, "{seen:?}");

    let config_calls = seen
        .requests
        .iter()
        .filter(|(path, _)| path.starts_with("/v1/config"))
        .count();
    assert!(
        config_calls >= 2,
        "both clients describe the catalog: {seen:?}"
    );
    for (path, auth) in &seen.requests {
        assert_eq!(
            auth.as_deref(),
            Some("Bearer token-1"),
            "{path} carried a different credential"
        );
    }
}

#[tokio::test]
async fn a_due_token_is_renewed_on_the_read_path() {
    // A token with no lifetime left is due on every use, so every read must
    // carry a freshly issued one. Upstream's own client exchanges once and
    // keeps presenting that token for ever — which is what a daemon an hour
    // into a one-hour token would be doing.
    let (uri, seen) = fake_catalog(0).await;

    let connection = config(&uri).connect().await.expect("connects");
    connection.client.list_namespaces(None).await.unwrap();
    connection.client.list_namespaces(None).await.unwrap();

    let seen = seen.lock().unwrap();
    let listings: Vec<&Option<String>> = seen
        .requests
        .iter()
        .filter(|(path, _)| path.starts_with("/v1/namespaces"))
        .map(|(_, auth)| auth)
        .collect();
    assert_eq!(listings.len(), 2, "{seen:?}");
    assert_ne!(
        listings[0], listings[1],
        "the second read presented the token the first one used: {seen:?}"
    );
    assert_eq!(
        listings[1].as_deref(),
        Some(format!("Bearer token-{}", seen.exchanges).as_str()),
        "the last read carried the token just issued"
    );
}

#[tokio::test]
async fn no_credential_sends_no_authorization_header() {
    // Some catalogs reject `Authorization: Bearer ` outright, so an
    // unauthenticated catalog must see no header at all.
    let (uri, seen) = fake_catalog(3600).await;
    let mut config = config(&uri);
    config.properties.clear();

    let connection = config.connect().await.expect("connects");
    connection.client.list_namespaces(None).await.unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen.exchanges, 0);
    assert!(
        seen.requests.iter().all(|(_, auth)| auth.is_none()),
        "{seen:?}"
    );
}
