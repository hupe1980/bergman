//! Catalog configuration, construction, and table discovery.
//!
//! Bergman reaches every catalog the same way it reaches Rustberg: as an
//! ordinary Iceberg REST client. There is no private API and no shared types
//! with any catalog implementation — the contract is the wire, so a catalog
//! that speaks the spec works without Bergman knowing which one it is.

#[cfg(feature = "catalog-rest")]
mod auth;
mod discovery;
pub mod polaris;
pub(crate) mod read;
mod storage;

pub use discovery::{DiscoveredTable, NamespaceSource, discover};
pub use storage::{StorageScheme, resolving_factory};

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::policy::TableRef;

/// How to reach one catalog.
///
/// [`Debug`] is implemented by hand and **redacts secret property values**.
/// `properties` is deliberately an open map of Iceberg's own property names, and
/// several of those names carry secrets — `credential`, `token`,
/// `s3.secret-access-key`, `gcs.credentials-json`, `adls.sas-token`. The derived
/// impl would put every one of them into any `{:?}`, and this type is reachable
/// by `Debug` from [`crate::Config`] and [`crate::Bergman`], so one
/// `tracing::debug!(?config)` in an embedder would write the warehouse's keys to
/// its logs.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    /// The name this catalog is known by, and the first segment of every
    /// [`TableRef`] discovered from it.
    pub name: String,

    /// The catalog protocol.
    #[serde(default)]
    pub kind: CatalogKind,

    /// The catalog endpoint, e.g. `https://localhost:8181/catalog`.
    pub uri: String,

    /// The warehouse to operate in, when the catalog serves several.
    #[serde(default)]
    pub warehouse: Option<String>,

    /// Which object store holds this warehouse's data.
    ///
    /// Optional, and only a *check*: the storage backend is resolved per path
    /// at read time, so a catalog can span schemes. Declaring it makes a build
    /// missing that Cargo feature fail at startup with the feature's name,
    /// rather than on the first table with an unhelpful "unsupported scheme".
    ///
    /// Inferred from the warehouse location when absent.
    #[serde(default)]
    pub storage: Option<StorageScheme>,

    /// Properties passed to the catalog client and to storage, using Iceberg's
    /// own property names (`s3.endpoint`, `s3.region`, `gcs.project-id`, …), so
    /// they mean here what they mean in every other Iceberg tool.
    #[serde(default)]
    pub properties: HashMap<String, String>,

    /// Where the bearer token comes from.
    ///
    /// A name, not a value: a credential in a config file is a credential in
    /// version control.
    #[serde(default)]
    pub token_env: Option<String>,

    /// Restrict discovery to these namespaces rather than walking the catalog.
    ///
    /// A large catalog makes listing expensive, and most deployments maintain a
    /// known subtree.
    #[serde(default)]
    pub namespaces: Option<Vec<String>>,

    /// Read maintenance policy the catalog itself holds, as a resolution layer
    /// below `bergman.toml` (see [`crate::policy::catalog`]).
    ///
    /// Off unless named. A table whose catalog policy cannot be read is not
    /// maintained that cycle, and says why: maintaining it under the remaining
    /// layers could do exactly what the unread policy forbids.
    #[serde(default)]
    pub policy_source: Option<PolicySource>,
}

/// Where a catalog keeps maintenance policy Bergman can read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PolicySource {
    /// Apache Polaris: `GET /polaris/v1/{prefix}/applicable-policies`.
    Polaris,
}

/// The catalog protocol.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogKind {
    /// The Iceberg REST Catalog protocol.
    #[default]
    Rest,
}

/// Whether a property name carries a secret value.
///
/// Deny-list rather than allow-list is the wrong default for a security check,
/// so this is a *substring* rule over the words that mark a secret in Iceberg's
/// property vocabulary — `secret`, `key`, `token`, `credential`, `password`,
/// `sas`. It over-redacts (`s3.access-key-id` is an identifier, not a secret)
/// and that is the correct direction to be wrong in: a redacted identifier
/// costs an operator one lookup, a logged secret costs a rotation.
fn is_secret_property(key: &str) -> bool {
    const MARKERS: [&str; 6] = ["secret", "key", "token", "credential", "password", "sas"];
    let key = key.to_ascii_lowercase();
    MARKERS.iter().any(|marker| key.contains(marker))
}

impl std::fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Property *names* are shown and values are not, because "is my endpoint
        // reaching the config" is a real question and "what is my secret" is
        // never one a log should answer.
        let properties: std::collections::BTreeMap<&str, &str> = self
            .properties
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str(),
                    if is_secret_property(k) {
                        "<redacted>"
                    } else {
                        v.as_str()
                    },
                )
            })
            .collect();

        f.debug_struct("CatalogConfig")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("uri", &self.uri)
            .field("warehouse", &self.warehouse)
            .field("storage", &self.storage)
            // A variable name, never a value — that is the whole point of the
            // field — so it is safe to show and useful to see.
            .field("token_env", &self.token_env)
            .field("namespaces", &self.namespaces)
            .field("policy_source", &self.policy_source)
            .field("properties", &properties)
            .finish()
    }
}

/// A connected catalog: the client every read goes through, and Bergman's own
/// commit path for what `iceberg::Catalog` cannot express.
///
/// Built together because they must authenticate as one. Both send the bearer
/// token from a single [`crate::commit::TokenSource`] — the read client through
/// an `AuthManager` Bergman injects, the commit client directly — so there is
/// one cache, one renewal and one credential in use. Two clients that each
/// exchanged their own token would drift: reads succeeding, the plan looking
/// perfect, and every commit returning 401.
///
/// Renewal is on demand. Every request asks the source for its token, and the
/// source exchanges the credential again when the one in hand is within a
/// minute of expiry — so a daemon holding a one-hour token neither stops
/// reading at the hour nor needs a timer to prevent it.
#[derive(Debug, Clone)]
pub struct CatalogConnection {
    /// The client every read goes through.
    pub client: Arc<dyn iceberg::Catalog>,
    /// Bergman's own commit path (see [`crate::commit`]).
    pub committer: Arc<dyn crate::commit::TableCommitter>,
    /// The catalog's own maintenance policy, when it is configured as a
    /// policy source.
    pub policy: Option<Arc<polaris::PolarisPolicies>>,
}

impl CatalogConfig {
    /// Connect both clients.
    ///
    /// Credentials are read from the environment here rather than at parse
    /// time, so a configuration can be linted (`bergman policy lint`) on a
    /// machine that holds no credentials at all.
    pub async fn connect(&self) -> Result<CatalogConnection> {
        match self.kind {
            CatalogKind::Rest => self.connect_rest().await,
        }
    }

    /// The token `token_env` names, when it names one.
    #[cfg(feature = "catalog-rest")]
    fn explicit_token(&self) -> Result<Option<String>> {
        self.token_env
            .as_ref()
            .map(|var| {
                std::env::var(var).map_err(|_| {
                    Error::config(format!(
                        "catalog \"{}\": token_env names ${var}, which is not set",
                        self.name
                    ))
                })
            })
            .transpose()
    }

    #[cfg(feature = "catalog-rest")]
    async fn connect_rest(&self) -> Result<CatalogConnection> {
        use iceberg::CatalogBuilder;
        use iceberg_catalog_rest::{
            REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
        };

        let tokens = Arc::new(crate::commit::TokenSource::new(
            crate::commit::Credential::from_properties(
                &self.properties,
                self.explicit_token()?,
                &self.uri,
            )?,
            crate::commit::http_client()?,
        ));

        // The commit client first: it reads `/v1/config` and refuses a catalog
        // that does not advertise table commits, which is the better failure to
        // meet before any read is attempted.
        let committer = Arc::new(
            crate::commit::RestCommitter::connect(
                &self.uri,
                self.warehouse.as_deref(),
                Arc::clone(&tokens),
            )
            .await?,
        );

        let mut props = self.properties.clone();
        props.insert(REST_CATALOG_PROP_URI.to_string(), self.uri.clone());
        if let Some(warehouse) = &self.warehouse {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        }
        // The credential is Bergman's to present, through the injected manager.
        // Left in the properties, upstream would read it too — a second
        // exchange and a second cache, the drift this type exists to prevent.
        props.remove("token");
        props.remove("credential");

        let client = RestCatalogBuilder::default()
            .with_storage_factory(storage::resolving_factory())
            .with_auth_manager(auth::SharedTokenAuth::new(Arc::clone(&tokens)))
            .load(self.name.clone(), props)
            .await
            .map_err(|e| Error::Catalog(Box::new(e)))?;

        let policy = match self.policy_source {
            None => None,
            Some(PolicySource::Polaris) => Some(Arc::new(polaris::PolarisPolicies::new(
                &self.uri,
                committer.prefix().ok_or_else(|| {
                    Error::config(format!(
                        "catalog \"{}\" is a Polaris policy source, but its /v1/config named \
                         no prefix — Polaris always does, so this is not a Polaris catalog",
                        self.name
                    ))
                })?,
                Arc::clone(&tokens),
            )?)),
        };

        Ok(CatalogConnection {
            client: Arc::new(client),
            committer,
            policy,
        })
    }

    #[cfg(not(feature = "catalog-rest"))]
    async fn connect_rest(&self) -> Result<CatalogConnection> {
        Err(Error::Unsupported(format!(
            "catalog \"{}\" is a REST catalog, but this build has no `catalog-rest` feature",
            self.name
        )))
    }

    /// Check that this build carries the object store this catalog will need.
    ///
    /// Storage is resolved per path at read time, so this cannot be exhaustive
    /// — a table could sit anywhere. It catches the overwhelmingly common case
    /// (a warehouse on one store, in a build compiled without it) at startup,
    /// where the fix is a `--features` flag rather than a mystery on the first
    /// table.
    pub fn check_storage_available(&self) -> Result<()> {
        let scheme = self
            .storage
            .or_else(|| self.warehouse.as_deref().and_then(StorageScheme::from_uri));

        // Nothing declared and nothing inferable is not an error: the catalog
        // may vend locations Bergman has not seen yet, and refusing to start
        // over a check that is only ever advisory would be worse than the
        // problem it prevents.
        let Some(scheme) = scheme else {
            return Ok(());
        };

        if !scheme.is_available() {
            return Err(Error::Unsupported(format!(
                "catalog \"{}\" uses {scheme} storage, which this build does not carry; \
                 rebuild with --features {}",
                self.name,
                scheme.feature_name()
            )));
        }
        Ok(())
    }

    /// Validate what can be checked without contacting anything.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::config("catalog name is empty"));
        }
        // The name is the first segment of every `TableRef` this catalog
        // produces, and rule patterns are matched segment-wise against that.
        // A dot in the name would make one segment look like two and silently
        // change which patterns match.
        if self.name.contains('.') {
            return Err(Error::config(format!(
                "catalog name {:?} contains a dot, which is the segment separator in \
                 rule patterns",
                self.name
            )));
        }
        if self.uri.is_empty() {
            return Err(Error::config(format!(
                "catalog \"{}\": uri is empty",
                self.name
            )));
        }
        // Fail here rather than on the first table: a build without the right
        // storage feature cannot maintain anything, and learning that at
        // startup is much cheaper than learning it per table.
        self.check_storage_available()?;
        Ok(())
    }
}

/// Convert a [`TableRef`] into the identifier the catalog understands.
///
/// The catalog name is dropped: it is Bergman's addressing, not the catalog's.
pub fn to_table_ident(table: &TableRef) -> Result<iceberg::TableIdent> {
    let namespace = iceberg::NamespaceIdent::from_vec(table.namespace.clone())
        .map_err(|e| Error::metadata(table, format!("invalid namespace: {e}")))?;
    Ok(iceberg::TableIdent::new(namespace, table.name.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(name: &str, warehouse: Option<&str>) -> CatalogConfig {
        CatalogConfig {
            name: name.into(),
            kind: CatalogKind::Rest,
            uri: "http://localhost:8181".into(),
            warehouse: warehouse.map(Into::into),
            storage: None,
            properties: HashMap::new(),
            token_env: None,
            namespaces: None,
            policy_source: None,
        }
    }

    #[test]
    fn debug_output_redacts_secret_properties() {
        // `properties` is an open map of Iceberg property names and several of
        // them carry secrets. `CatalogConfig` is reachable by `Debug` from
        // `Config` and `Bergman`, so a derived impl would write the warehouse's
        // keys into any `tracing::debug!(?config)` an embedder happens to have.
        let mut config = config("prod", Some("s3://b/wh"));
        config.properties = HashMap::from([
            ("s3.endpoint".to_string(), "https://minio:9000".to_string()),
            ("s3.region".to_string(), "eu-central-1".to_string()),
            ("s3.secret-access-key".to_string(), "hunter2".to_string()),
            ("s3.session-token".to_string(), "ephemeral".to_string()),
            ("credential".to_string(), "id:hunter3".to_string()),
        ]);

        let rendered = format!("{config:?}");
        for secret in ["hunter2", "hunter3", "ephemeral"] {
            assert!(!rendered.contains(secret), "leaked {secret}: {rendered}");
        }
        // Non-secret settings still show, because "is my endpoint reaching the
        // config" is the question a debug line is usually being asked.
        assert!(rendered.contains("https://minio:9000"), "{rendered}");
        assert!(rendered.contains("eu-central-1"), "{rendered}");
        // The names survive redaction: knowing a secret was *supplied* is the
        // other half of debugging one that was not.
        assert!(rendered.contains("s3.secret-access-key"), "{rendered}");
    }

    #[test]
    fn every_secret_bearing_iceberg_property_is_recognised() {
        for key in [
            "credential",
            "token",
            "s3.secret-access-key",
            "s3.session-token",
            "gcs.credentials-json",
            "gcs.oauth2.token",
            "adls.auth.shared-key.account.key",
            "adls.sas-token",
        ] {
            assert!(is_secret_property(key), "{key} is a secret");
        }
        for key in ["s3.endpoint", "s3.region", "gcs.project-id"] {
            assert!(!is_secret_property(key), "{key} is not a secret");
        }
    }

    #[test]
    fn catalog_name_with_a_dot_is_refused() {
        // `prod.eu` as a catalog name would make `prod.eu.db.t` ambiguous
        // against a rule pattern, which is a silent change in what matches.
        let err = config("prod.eu", Some("file:///tmp/wh"))
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("contains a dot"), "got: {err}");
    }

    #[test]
    fn a_local_warehouse_needs_no_feature() {
        assert!(
            config("prod", Some("file:///var/lib/wh"))
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn an_unrecognisable_warehouse_does_not_block_startup() {
        // The check is advisory — storage resolves per path at read time — so
        // a scheme Bergman cannot classify must not stop the process. Refusing
        // to start over an advisory check would be worse than the problem.
        assert!(config("prod", Some("wat://bucket/wh")).validate().is_ok());
        assert!(config("prod", None).validate().is_ok());
    }

    #[test]
    #[cfg(not(feature = "storage-gcs"))]
    fn a_missing_storage_feature_is_named_at_startup() {
        // Upstream's message for this is "Unsupported storage scheme: gcs",
        // which does not tell anyone what to do about it.
        let err = config("prod", Some("gs://bucket/wh"))
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("storage-gcs"), "got: {err}");
    }

    #[test]
    fn table_ident_drops_the_catalog_segment() {
        let table = TableRef::new("prod", ["analytics", "web"], "events");
        let ident = to_table_ident(&table).unwrap();
        assert_eq!(ident.name(), "events");
        assert_eq!(ident.namespace().as_ref(), &["analytics", "web"]);
    }
}
