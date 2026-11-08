//! Reading a table's maintenance policy from a Polaris catalog.
//!
//! Driven through [`CatalogConfig::connect`], the production constructor,
//! against a catalog that speaks `/v1/config` and the `applicable-policies`
//! endpoint, so what is checked is the request Bergman sends and what it does
//! with the answer — not a stub of either.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bergman::catalog::{CatalogConfig, CatalogKind, PolicySource};
use bergman::policy::TableRef;

/// `(path-and-query, authorization)` for every request.
type Seen = Arc<Mutex<Vec<(String, Option<String>)>>>;

/// A Polaris catalog whose `prefix` is `cat`. `policies` answers the policy
/// endpoint: `None` makes it fail with a 500.
async fn fake_polaris(policies: Option<Vec<&'static str>>) -> (String, Seen) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let seen: Seen = Arc::default();
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let log = Arc::clone(&log);
            let policies = policies.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                    if read == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buffer[..read]);
                    if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&raw).to_string();
                let mut lines = head.lines();
                let path = lines
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_string();
                let auth = lines
                    .filter_map(|l| l.split_once(':'))
                    .find(|(k, _)| k.trim().eq_ignore_ascii_case("authorization"))
                    .map(|(_, v)| v.trim().to_string());
                log.lock().unwrap().push((path.clone(), auth));

                let (status, payload) = if path.starts_with("/v1/config") {
                    (
                        "200 OK",
                        r#"{"defaults":{},"overrides":{"prefix":"cat"}}"#.to_string(),
                    )
                } else if path.contains("/applicable-policies") {
                    match &policies {
                        None => (
                            "500 Internal Server Error",
                            r#"{"error":"down"}"#.to_string(),
                        ),
                        Some(pages) => {
                            let page = if path.contains("pageToken=p2") { 1 } else { 0 };
                            ("200 OK", pages[page].to_string())
                        }
                    }
                } else {
                    ("404 Not Found", "{}".to_string())
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\
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
        properties: HashMap::from([("token".into(), "t0k3n".into())]),
        token_env: None,
        namespaces: None,
        policy_source: Some(PolicySource::Polaris),
    }
}

const PAGE_ONE: &str = r#"{"next-page-token":"p2","applicable-policies":[
  {"policy-type":"system.data-compaction","name":"compact","version":0,"inheritable":true,
   "inherited":true,"namespace":["finance"],
   "content":"{\"version\":\"2025-02-03\",\"enable\":true,\"config\":{\"target_file_size_bytes\":268435456}}"}]}"#;
const PAGE_TWO: &str = r#"{"applicable-policies":[
  {"policy-type":"system.snapshot-expiry","name":"expire","version":0,"inheritable":true,
   "inherited":false,"namespace":["finance","quarterly"],
   "content":"{\"enable\":true,\"config\":{\"min_snapshot_to_keep\":5}}"}]}"#;

#[tokio::test]
async fn a_tables_policies_are_read_across_pages_with_its_namespace_encoded() {
    let (uri, seen) = fake_polaris(Some(vec![PAGE_ONE, PAGE_TWO])).await;
    let connection = config(&uri).connect().await.expect("connects");
    let source = connection.policy.expect("Polaris is a policy source");

    let table = TableRef::new("prod", ["finance", "quarterly"], "transactions");
    let policy = source.applicable(&table).await.expect("reads");

    assert_eq!(policy.layers.len(), 2, "both pages were read: {policy:?}");
    assert!(policy.notes.is_empty(), "{:?}", policy.notes);

    let requests: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p.contains("applicable-policies"))
        .cloned()
        .collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].0,
        "/polaris/v1/cat/applicable-policies?namespace=finance%1Fquarterly&target-name=transactions",
        "the prefix from /v1/config, and a multipart namespace joined by 0x1F"
    );
    assert!(
        requests[1].0.ends_with("&pageToken=p2"),
        "{}",
        requests[1].0
    );
    for (_, auth) in &requests {
        assert_eq!(auth.as_deref(), Some("Bearer t0k3n"));
    }
}

#[tokio::test]
async fn an_unreadable_policy_is_an_error_not_an_empty_one() {
    // The catalog might be saying "never compact this table". Reading its
    // failure as "no policy" would let the absence of that answer override it.
    let (uri, _) = fake_polaris(None).await;
    let connection = config(&uri).connect().await.expect("connects");
    let source = connection.policy.unwrap();
    let err = source
        .applicable(&TableRef::new("prod", ["finance"], "t"))
        .await
        .expect_err("a failed read is not an empty policy");
    assert!(err.to_string().contains("500"), "{err}");
}

#[tokio::test]
async fn a_catalog_that_is_not_a_policy_source_reads_nothing() {
    let (uri, seen) = fake_polaris(Some(vec![PAGE_ONE])).await;
    let mut config = config(&uri);
    config.policy_source = None;
    let connection = config.connect().await.expect("connects");
    assert!(connection.policy.is_none());
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .all(|(p, _)| !p.contains("applicable-policies"))
    );
}
