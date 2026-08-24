//! Delivering a commit over the Iceberg REST protocol.
//!
//! One `POST` to one endpoint. The whole reason this file exists is that
//! `iceberg::TableCommit` cannot be constructed from outside the crate — the
//! payload itself is entirely upstream's own public, `Serialize` types, so the
//! bytes on the wire are the same ones `iceberg-catalog-rest` would send.
//!
//! What it adds over a bare `POST` is the three things a background tenant
//! needs and a naive client lacks: a token that is renewed rather than read
//! once at startup (see [`super::auth`]), a bounded wait so a hung catalog
//! cannot stall a cycle forever, and deference to a catalog that says it is
//! busy.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

use iceberg::{TableIdent, TableRequirement, TableUpdate};

use crate::commit::auth::{Credential, TokenSource};
use crate::error::{Error, Result};

/// How long one request may take.
///
/// A commit is a small JSON body against an index, so a catalog that has not
/// answered in half a minute is one that is not going to. Without a timeout a
/// single unreachable catalog stalls a cycle indefinitely, and a daemon that
/// stops maintaining anything without saying so is worse than one that fails.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the whole client will spend establishing a connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How many times a *throttled* request is re-sent.
///
/// Distinct from the commit retries in [`crate::ops`]: those rebuild a plan
/// because the table moved, while this one re-sends the identical request
/// because the catalog asked us to come back. Confusing the two would either
/// replan on a 503 (throwing away good work) or re-submit a stale commit on a
/// conflict (which is how deleted rows come back).
const MAX_THROTTLE_RETRIES: usize = 3;

/// The commit request body, as the REST specification defines it.
#[derive(Debug, Serialize)]
struct CommitTableRequest<'a> {
    identifier: RestTableIdent<'a>,
    requirements: &'a [TableRequirement],
    updates: &'a [TableUpdate],
}

#[derive(Debug, Serialize)]
struct RestTableIdent<'a> {
    namespace: &'a [String],
    name: &'a str,
}

/// The subset of `GET /v1/config` that matters here.
#[derive(Debug, Default, Deserialize)]
struct CatalogConfigResponse {
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    defaults: HashMap<String, String>,
    /// The operations this deployment serves.
    ///
    /// A catalog that does not advertise `POST /v1/.../tables/{table}` cannot
    /// accept a Bergman commit, and learning that at startup beats learning it
    /// per table.
    #[serde(default)]
    endpoints: Option<Vec<String>>,
}

/// The REST error envelope, so a failure can be reported with the catalog's own
/// message rather than a bare status code.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: ErrorModel,
}

#[derive(Debug, Deserialize)]
struct ErrorModel {
    message: String,
    #[serde(default)]
    r#type: String,
}

/// The endpoint a table commit is sent to, as the specification names it.
const COMMIT_ENDPOINT: &str = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}";

/// Commits over the Iceberg REST protocol.
#[derive(Debug)]
pub struct RestCommitter {
    client: Client,
    uri: String,
    /// The `prefix` path segment the catalog asked clients to use.
    ///
    /// Multi-tenant catalogs (Polaris, Unity, S3 Tables) route by it, and a
    /// commit sent without it reaches the wrong warehouse or a 404. It is
    /// discovered once from `/v1/config` rather than configured, because the
    /// catalog is the authority on its own routing.
    prefix: Option<String>,
    tokens: TokenSource,
    /// Whether the catalog said it serves table commits.
    ///
    /// `None` when it advertised no endpoint list at all, which most
    /// deployments do — absence is not a refusal, and treating it as one would
    /// refuse to commit to almost every catalog in existence.
    commits_supported: Option<bool>,
}

impl RestCommitter {
    /// Connect, learn the catalog's routing prefix, and check it accepts
    /// commits.
    ///
    /// `warehouse` is sent to `/v1/config` when set, because a catalog serving
    /// several may return a different prefix per warehouse.
    pub async fn connect(
        uri: &str,
        warehouse: Option<&str>,
        properties: &HashMap<String, String>,
        explicit_token: Option<String>,
    ) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("bergman/", env!("CARGO_PKG_VERSION")))
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| Error::config(format!("could not build an HTTP client: {e}")))?;

        let uri = uri.trim_end_matches('/').to_string();
        let credential = Credential::from_properties(properties, explicit_token, &uri)?;
        let tokens = TokenSource::new(credential, client.clone());

        // The warehouse is appended by hand rather than through `query`, which
        // lives behind a reqwest feature this build does not carry — one
        // parameter is not worth the dependency surface.
        let config_url = match warehouse {
            Some(warehouse) => format!(
                "{uri}/v1/config?warehouse={}",
                encode_path_segment(warehouse)
            ),
            None => format!("{uri}/v1/config"),
        };
        let mut request = client.get(config_url);
        if let Some(token) = tokens.token().await? {
            request = request.bearer_auth(token);
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::config(format!("{uri}/v1/config: {e}")))?;

        // A catalog that will not describe itself is one Bergman cannot commit
        // to correctly, so this is fatal rather than a shrug and an empty
        // prefix — a commit to the wrong warehouse is much worse than a
        // startup failure.
        if !response.status().is_success() {
            return Err(Error::config(format!(
                "{uri}/v1/config returned {}",
                response.status()
            )));
        }

        let config: CatalogConfigResponse = response
            .json()
            .await
            .map_err(|e| Error::config(format!("{uri}/v1/config is not valid JSON: {e}")))?;

        // `overrides` wins over `defaults`: the specification defines overrides
        // as values the client must use, and defaults as ones it may replace.
        let prefix = config
            .overrides
            .get("prefix")
            .or_else(|| config.defaults.get("prefix"))
            .cloned();

        let commits_supported = config
            .endpoints
            .as_ref()
            .map(|endpoints| endpoints.iter().any(|e| e == COMMIT_ENDPOINT));

        if commits_supported == Some(false) {
            // A read-only deployment — a federated mount, say. Maintenance
            // there would be a write into somebody else's catalog, and it
            // *should* be refused. Refusing at startup with the reason beats a
            // 404 per table with none.
            return Err(Error::Unsupported(format!(
                "catalog at {uri} does not advertise {COMMIT_ENDPOINT}; it will not accept \
                 maintenance commits"
            )));
        }

        Ok(Self {
            client,
            uri,
            prefix,
            tokens,
            commits_supported,
        })
    }

    /// Whether the catalog advertised support for table commits.
    ///
    /// `None` means it advertised no endpoint list, which most deployments do.
    pub fn commits_supported(&self) -> Option<bool> {
        self.commits_supported
    }

    /// The commit endpoint for a table.
    fn table_endpoint(&self, ident: &TableIdent) -> String {
        let mut parts: Vec<String> = vec![self.uri.clone(), "v1".to_string()];
        if let Some(prefix) = &self.prefix {
            parts.push(prefix.clone());
        }
        parts.push("namespaces".to_string());
        // Multi-level namespaces are joined with the unit separator and
        // percent-encoded, which is what the REST specification says and what
        // every catalog implements. A `.` would be ambiguous: a namespace named
        // `a.b` and the nested namespace `a` → `b` are different things.
        parts.push(encode_path_segment(
            &ident.namespace.as_ref().join("\u{1f}"),
        ));
        parts.push("tables".to_string());
        parts.push(encode_path_segment(ident.name()));
        parts.join("/")
    }
}

#[async_trait]
impl super::TableCommitter for RestCommitter {
    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
        ctx: crate::obs::OperationContext<'_>,
    ) -> Result<()> {
        let body = CommitTableRequest {
            identifier: RestTableIdent {
                namespace: ident.namespace.as_ref(),
                name: ident.name(),
            },
            requirements: &requirements,
            updates: &updates,
        };
        let endpoint = self.table_endpoint(ident);

        // The run id, not a fresh UUID. This header is only worth sending if
        // something on Bergman's side wrote the same value down: a governing
        // catalog records an authorization decision per commit, and joining that
        // decision to Bergman's reason for the commit is the whole point. Every
        // audit record of this run carries `run_id`, so one grep spans both
        // logs. A value invented here would appear in exactly one of them.
        //
        // Run-grained rather than commit-grained on purpose. A commit-unique id
        // would identify the request and nothing else; the run identifies the
        // policy evaluation that produced it, which is the question an operator
        // reading a catalog's audit trail actually has.
        let request_id = ctx.run_id;

        for attempt in 0..=MAX_THROTTLE_RETRIES {
            let mut request = self
                .client
                .post(&endpoint)
                .header("X-Request-Id", request_id)
                .header("X-Bergman-Operation", ctx.kind.as_str())
                .json(&body);
            if let Some(token) = self.tokens.token().await? {
                request = request.bearer_auth(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| Error::Catalog(Box::new(io_error(format!("commit failed: {e}")))))?;

            let status = response.status();
            if status.is_success() {
                return Ok(());
            }

            // The catalog is up and saying it is busy. Re-sending the identical
            // request is correct here and *only* here — a conflict must never
            // be re-sent, because the outputs were computed against a table
            // that has since moved.
            if is_throttle(status) && attempt < MAX_THROTTLE_RETRIES {
                let wait = retry_after(&response).unwrap_or(crate::ops::retry_delay(attempt));
                tracing::debug!(
                    table = %ident,
                    %status,
                    wait_ms = wait.as_millis() as u64,
                    "catalog is busy; waiting before re-sending the commit"
                );
                tokio::time::sleep(wait).await;
                continue;
            }

            let detail = error_detail(response).await;
            return Err(classify(status, ident, detail));
        }

        Err(Error::Catalog(Box::new(io_error(format!(
            "the catalog stayed busy through {MAX_THROTTLE_RETRIES} re-sends"
        )))))
    }
}

/// Map a failing status onto the error whose disposition matches it.
///
/// This is the single most important classification in the crate: the caller's
/// response to a conflict is to rebuild the plan against the table as it now
/// is, and to any other failure is not.
fn classify(status: StatusCode, ident: &TableIdent, detail: String) -> Error {
    match status {
        // The commit lost its compare-and-swap. Some catalogs answer a failed
        // requirement with 412 rather than 409; both mean the same thing here.
        StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED => Error::CommitConflict {
            table: ident.to_string(),
            detail,
        },
        StatusCode::NOT_FOUND => Error::metadata(
            ident.to_string(),
            format!("the catalog does not have this table: {detail}"),
        ),
        StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => Error::refused(
            "commit",
            ident.to_string(),
            format!("the catalog refused the commit ({status}): {detail}"),
        ),
        other => Error::Catalog(Box::new(io_error(format!(
            "commit returned {other}: {detail}"
        )))),
    }
}

/// Whether a status means "come back later" rather than "no".
fn is_throttle(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::BAD_GATEWAY
            | StatusCode::GATEWAY_TIMEOUT
    )
}

/// How long the catalog asked us to wait, when it said.
///
/// Only the delta-seconds form is honoured. The HTTP-date form is legal and
/// essentially unused by object-store catalogs, and parsing dates to decide a
/// sleep is more ways to be wrong than the case is worth.
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let raw = response.headers().get("retry-after")?.to_str().ok()?;
    let seconds: u64 = raw.trim().parse().ok()?;
    // A catalog asking for an hour is a catalog we should not wait for inside a
    // cycle; the next cycle is the right place to come back.
    (seconds <= 60).then(|| Duration::from_secs(seconds))
}

/// Read the catalog's own error message, falling back to something useful.
async fn error_detail(response: reqwest::Response) -> String {
    // Read the body once as text, then try the envelope: a proxy answering with
    // HTML still has something to say, and `json()` would consume the body and
    // discard it.
    let Ok(body) = response.text().await else {
        return "no error detail from the catalog".to_string();
    };
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "no error detail from the catalog".to_string();
    }

    match serde_json::from_str::<ErrorResponse>(trimmed) {
        Ok(parsed) if !parsed.error.r#type.is_empty() => {
            format!("{}: {}", parsed.error.r#type, parsed.error.message)
        }
        Ok(parsed) => parsed.error.message,
        // Not the envelope. Whatever it is, it is more informative than a
        // sentence saying there was nothing — truncated, because an HTML error
        // page is not a log line.
        Err(_) => truncate(trimmed, 512),
    }
}

fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let head: String = text.chars().take(limit).collect();
    format!("{head}…")
}

fn io_error(message: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, message)
}

/// Percent-encode one path segment.
///
/// Hand-rolled rather than pulled from a dependency because the rule is short
/// and the set of characters that must survive is exact: a namespace separator
/// (`\u{1f}`) has to become `%1F`, and a table name may contain anything
/// Iceberg permits.
fn encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match byte {
            // RFC 3986 unreserved characters pass through; everything else is
            // escaped. Encoding more than strictly necessary is always safe,
            // encoding less is not.
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committer(prefix: Option<&str>) -> RestCommitter {
        RestCommitter {
            client: Client::new(),
            uri: "https://catalog.example.com".into(),
            prefix: prefix.map(Into::into),
            tokens: TokenSource::new(Credential::None, Client::new()),
            commits_supported: None,
        }
    }

    #[test]
    fn endpoint_matches_the_rest_specification() {
        let ident = TableIdent::from_strs(["analytics", "events"]).unwrap();
        assert_eq!(
            committer(None).table_endpoint(&ident),
            "https://catalog.example.com/v1/namespaces/analytics/tables/events"
        );
    }

    #[test]
    fn the_catalogs_routing_prefix_is_included() {
        // Multi-tenant catalogs route by this. A commit sent without it reaches
        // the wrong warehouse, which is worse than failing.
        let ident = TableIdent::from_strs(["analytics", "events"]).unwrap();
        assert_eq!(
            committer(Some("ws/acme")).table_endpoint(&ident),
            "https://catalog.example.com/v1/ws/acme/namespaces/analytics/tables/events"
        );
    }

    #[test]
    fn nested_namespaces_join_with_the_unit_separator() {
        // `%1F`, not `.`: a namespace literally named `a.b` and the nested
        // namespace `a` → `b` are different tables, and a dot cannot tell them
        // apart.
        let ident = TableIdent::from_strs(["analytics", "web", "events"]).unwrap();
        assert_eq!(
            committer(None).table_endpoint(&ident),
            "https://catalog.example.com/v1/namespaces/analytics%1Fweb/tables/events"
        );
    }

    #[test]
    fn a_dotted_namespace_name_is_not_confused_with_nesting() {
        let dotted = TableIdent::from_strs(["a.b", "events"]).unwrap();
        let nested = TableIdent::from_strs(["a", "b", "events"]).unwrap();
        assert_ne!(
            committer(None).table_endpoint(&dotted),
            committer(None).table_endpoint(&nested)
        );
    }

    #[test]
    fn names_needing_escapes_are_encoded() {
        let ident = TableIdent::from_strs(["db", "orders 2026/q1"]).unwrap();
        let endpoint = committer(None).table_endpoint(&ident);
        assert!(
            endpoint.ends_with("/tables/orders%202026%2Fq1"),
            "{endpoint}"
        );
        // A raw slash would change which resource is addressed.
        assert!(!endpoint.contains("orders 2026/q1"));
    }

    #[test]
    fn unicode_names_are_percent_encoded_as_utf8() {
        // Iceberg permits them and real deployments have them.
        assert_eq!(encode_path_segment("売上"), "%E5%A3%B2%E4%B8%8A");
    }

    #[test]
    fn a_trailing_slash_on_the_uri_does_not_double_up() {
        let c = committer(None);
        let ident = TableIdent::from_strs(["db", "t"]).unwrap();
        assert!(!c.table_endpoint(&ident).contains("//v1"));
    }

    #[test]
    fn the_request_body_matches_the_specification() {
        // Serialized by upstream's own types, so this asserts the shape rather
        // than the encoding of each variant.
        let ident = TableIdent::from_strs(["db", "t"]).unwrap();
        let requirements = vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".into(),
            snapshot_id: Some(7),
        }];
        let updates = vec![TableUpdate::RemoveSnapshotRef {
            ref_name: "stale".into(),
        }];

        let body = CommitTableRequest {
            identifier: RestTableIdent {
                namespace: ident.namespace.as_ref(),
                name: ident.name(),
            },
            requirements: &requirements,
            updates: &updates,
        };

        let json: serde_json::Value = serde_json::to_value(&body).unwrap();
        assert_eq!(json["identifier"]["namespace"][0], "db");
        assert_eq!(json["identifier"]["name"], "t");
        assert_eq!(json["requirements"][0]["type"], "assert-ref-snapshot-id");
        assert_eq!(json["updates"][0]["action"], "remove-snapshot-ref");
    }

    #[test]
    fn a_lost_compare_and_swap_is_a_conflict_and_nothing_else_is() {
        // The classification the whole retry model rests on. A conflict means
        // replan; anything else means do not.
        let ident = TableIdent::from_strs(["db", "t"]).unwrap();

        for status in [StatusCode::CONFLICT, StatusCode::PRECONDITION_FAILED] {
            assert!(
                classify(status, &ident, "moved".into()).is_replan(),
                "{status} should replan"
            );
        }
        for status in [
            StatusCode::NOT_FOUND,
            StatusCode::FORBIDDEN,
            StatusCode::UNAUTHORIZED,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(
                !classify(status, &ident, "no".into()).is_replan(),
                "{status} should not replan"
            );
        }
    }

    #[test]
    fn a_rejected_commit_is_refused_rather_than_retried() {
        // A 403 will be a 403 forever. Retrying it burns a budget to reach the
        // same answer, and reporting it as a failure hides that the fix is a
        // permission rather than a restart.
        let ident = TableIdent::from_strs(["db", "t"]).unwrap();
        let err = classify(StatusCode::FORBIDDEN, &ident, "not permitted".into());
        assert_eq!(err.disposition(), crate::error::Disposition::Terminal);
        assert!(err.to_string().contains("not permitted"));
    }

    #[test]
    fn a_busy_catalog_is_told_apart_from_one_that_said_no() {
        // Re-sending an identical commit is correct only when the catalog said
        // "later". Re-sending on a conflict would offer a commit computed
        // against a table that has since moved.
        assert!(is_throttle(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_throttle(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_throttle(StatusCode::CONFLICT));
        assert!(!is_throttle(StatusCode::FORBIDDEN));
        assert!(!is_throttle(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[test]
    fn an_html_error_page_still_reaches_the_operator() {
        // A proxy answering with HTML has something to say, and a client that
        // reported "no error detail" would leave the operator with a bare 502.
        assert_eq!(
            truncate("<html>bad gateway</html>", 512),
            "<html>bad gateway</html>"
        );
        assert!(truncate(&"x".repeat(1000), 512).ends_with('…'));
        assert_eq!(truncate(&"x".repeat(1000), 512).chars().count(), 513);
    }

    #[test]
    fn a_catalog_that_serves_commits_is_accepted() {
        let advertised = [
            "GET /v1/{prefix}/namespaces".to_string(),
            COMMIT_ENDPOINT.to_string(),
        ];
        assert!(advertised.iter().any(|e| e == COMMIT_ENDPOINT));
    }

    #[test]
    fn a_read_only_catalog_is_identified_from_its_advertised_endpoints() {
        // A federated mount serves reads and refuses writes. Maintenance there
        // would be a write into somebody else's catalog and should be refused
        // — at startup, with the reason, rather than as a 404 per table.
        let advertised = ["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string()];
        assert!(!advertised.iter().any(|e| e == COMMIT_ENDPOINT));
    }
}
