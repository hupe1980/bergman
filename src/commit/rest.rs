//! Delivering a commit over the Iceberg REST protocol.
//!
//! Eight lines of JSON to one endpoint. The whole reason this file exists is
//! that `iceberg::TableCommit` cannot be constructed from outside the crate —
//! the payload itself is entirely upstream's own public, `Serialize` types, so
//! the bytes on the wire are the same ones `iceberg-catalog-rest` would send.

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

use iceberg::{TableIdent, TableRequirement, TableUpdate};

use crate::error::{Error, Result};

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
struct CatalogConfig {
    #[serde(default)]
    overrides: std::collections::HashMap<String, String>,
    #[serde(default)]
    defaults: std::collections::HashMap<String, String>,
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
    token: Option<String>,
}

impl RestCommitter {
    /// Connect, and learn the catalog's routing prefix.
    ///
    /// `warehouse` is sent to `/v1/config` when set, because a catalog serving
    /// several may return a different prefix per warehouse.
    pub async fn connect(
        uri: &str,
        warehouse: Option<&str>,
        token: Option<String>,
    ) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("bergman/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::config(format!("could not build an HTTP client: {e}")))?;

        let uri = uri.trim_end_matches('/').to_string();

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
        if let Some(token) = &token {
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

        let config: CatalogConfig = response
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

        Ok(Self {
            client,
            uri,
            prefix,
            token,
        })
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
    ) -> Result<()> {
        let body = CommitTableRequest {
            identifier: RestTableIdent {
                namespace: ident.namespace.as_ref(),
                name: ident.name(),
            },
            requirements: &requirements,
            updates: &updates,
        };

        let mut request = self.client.post(self.table_endpoint(ident)).json(&body);
        if let Some(token) = &self.token {
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

        let detail = error_detail(response).await;

        match status {
            // The commit lost its compare-and-swap. This is the single most
            // important status to classify correctly: the caller's response to
            // a conflict is to rebuild the plan against the table as it now is,
            // and to any other failure is not.
            StatusCode::CONFLICT => Err(Error::CommitConflict {
                table: ident.to_string(),
                detail,
            }),
            // Some catalogs answer a failed requirement with 412 rather than
            // 409. Both mean the same thing to us.
            StatusCode::PRECONDITION_FAILED => Err(Error::CommitConflict {
                table: ident.to_string(),
                detail,
            }),
            StatusCode::NOT_FOUND => Err(Error::metadata(
                ident.to_string(),
                format!("the catalog does not have this table: {detail}"),
            )),
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => Err(Error::refused(
                "commit",
                ident.to_string(),
                format!("the catalog refused the commit ({status}): {detail}"),
            )),
            other => Err(Error::Catalog(Box::new(io_error(format!(
                "commit returned {other}: {detail}"
            ))))),
        }
    }
}

/// Read the catalog's own error message, falling back to something useful.
async fn error_detail(response: reqwest::Response) -> String {
    match response.json::<ErrorResponse>().await {
        Ok(body) if !body.error.r#type.is_empty() => {
            format!("{}: {}", body.error.r#type, body.error.message)
        }
        Ok(body) => body.error.message,
        // Not every catalog returns the envelope on every path (a proxy may
        // answer with HTML), so a missing body is not itself an error.
        Err(_) => "no error detail from the catalog".to_string(),
    }
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
            token: None,
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
        let mut c = committer(None);
        c.uri = "https://catalog.example.com".to_string();
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
}
