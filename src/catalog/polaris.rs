//! Reading a table's maintenance policy from Apache Polaris.
//!
//! One endpoint: `GET {uri}/polaris/v1/{prefix}/applicable-policies` with the
//! table's namespace and name, which returns every policy in force for it with
//! Polaris's own inheritance already applied (a table's policy overrides its
//! namespace's, which overrides the catalog's). Paginated as the Iceberg REST
//! protocol is, with `pageToken`.
//!
//! The mapping from what comes back to Bergman's settings is
//! [`crate::policy::catalog`]'s; this module only fetches it.

use std::sync::Arc;

use serde::Deserialize;

use crate::commit::TokenSource;
use crate::error::{Error, Result};
use crate::policy::TableRef;
use crate::policy::catalog::{ApplicablePolicy, CatalogPolicy};

/// How many pages one table's policies may span before the answer is refused.
///
/// A table has at most a handful of maintenance policies; a server that keeps
/// handing out page tokens is one Bergman should stop reading rather than
/// follow forever.
const MAX_PAGES: usize = 100;

/// A Polaris catalog's maintenance policies, read per table.
#[derive(Debug)]
pub struct PolarisPolicies {
    client: reqwest::Client,
    endpoint: String,
    tokens: Arc<TokenSource>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Page {
    #[serde(default)]
    applicable_policies: Vec<ApplicablePolicy>,
    #[serde(default)]
    next_page_token: Option<String>,
}

impl PolarisPolicies {
    /// A reader for the catalog at `uri`, whose `/v1/config` named `prefix`.
    pub fn new(uri: &str, prefix: &str, tokens: Arc<TokenSource>) -> Result<Self> {
        Ok(Self {
            client: crate::commit::http_client()?,
            endpoint: format!(
                "{}/polaris/v1/{}/applicable-policies",
                uri.trim_end_matches('/'),
                crate::commit::rest::encode_path_segment(prefix)
            ),
            tokens,
        })
    }

    /// The maintenance policy in force for one table.
    ///
    /// Any failure is an error rather than an empty policy: an unread policy
    /// that said "do not compact this table" is indistinguishable from no
    /// policy at all, and treating the two alike is how a catalog's decision
    /// gets overridden by its absence.
    pub async fn applicable(&self, table: &TableRef) -> Result<CatalogPolicy> {
        // A multipart namespace travels as one parameter, its parts joined by
        // the unit separator (0x1F) — the REST protocol's own convention.
        let namespace = crate::commit::rest::encode_path_segment(&table.namespace.join("\u{1f}"));
        let name = crate::commit::rest::encode_path_segment(&table.name);

        let mut policies = Vec::new();
        let mut page_token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut url = format!("{}?namespace={namespace}&target-name={name}", self.endpoint);
            if let Some(token) = &page_token {
                url.push_str("&pageToken=");
                url.push_str(&crate::commit::rest::encode_path_segment(token));
            }
            let mut request = self.client.get(&url);
            if let Some(token) = self.tokens.token().await? {
                request = request.bearer_auth(token);
            }
            let response = request
                .send()
                .await
                .map_err(|e| Error::config(format!("{url}: {e}")))?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::config(format!(
                    "reading {table}'s catalog policy returned {status}: {}",
                    body.trim()
                )));
            }
            let page: Page = response.json().await.map_err(|e| {
                Error::config(format!("{table}'s catalog policy is not valid JSON: {e}"))
            })?;
            policies.extend(page.applicable_policies);
            match page.next_page_token.filter(|t| !t.is_empty()) {
                Some(token) => page_token = Some(token),
                None => return Ok(crate::policy::catalog::from_polaris(&policies)),
            }
        }
        Err(Error::config(format!(
            "{table}'s catalog policy spans more than {MAX_PAGES} pages; refusing to read further"
        )))
    }
}
