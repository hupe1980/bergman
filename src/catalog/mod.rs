//! Catalog configuration, construction, and table discovery.
//!
//! Bergman reaches every catalog the same way it reaches Rustberg: as an
//! ordinary Iceberg REST client. There is no private API and no shared types
//! with any catalog implementation — the contract is the wire, so a catalog
//! that speaks the spec works without Bergman knowing which one it is.

mod discovery;
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
            .field("properties", &properties)
            .finish()
    }
}

/// Renews a catalog client's own bearer token.
///
/// `iceberg-catalog-rest` exchanges an `OAuth2` credential once, at
/// construction, and never again — its source carries a `TODO: Support
/// automatic token refreshing`. Bergman's commit client renews its own (see
/// [`crate::commit::TokenSource`]); the *read* path is this one, and reads are
/// everything else: table loads, discovery, and snapshot expiration's commit. A
/// daemon holding a one-hour token keeps its cadence perfectly and stops
/// reading a single table.
///
/// Renewed before each cycle rather than in response to a 401, because
/// recognising one is not available: upstream maps it onto
/// `ErrorKind::Unexpected` with the status buried in an error context, and
/// classifying by message text is what [`crate::ops::expire`] refuses to do.
#[async_trait::async_trait]
pub trait CredentialRefresh: Send + Sync + std::fmt::Debug {
    /// Exchange the configured credential for a fresh token.
    async fn refresh(&self) -> Result<()>;
}

#[cfg(feature = "catalog-rest")]
#[async_trait::async_trait]
impl CredentialRefresh for iceberg_catalog_rest::RestCatalog {
    async fn refresh(&self) -> Result<()> {
        // `regenerate_token` fetches the new token *before* invalidating the
        // old one, so a failed exchange leaves the working token in place
        // rather than a client that can no longer authenticate at all.
        self.regenerate_token()
            .await
            .map_err(|e| Error::Catalog(Box::new(e)))
    }
}

/// A connected catalog client, and the handle that renews its credential.
///
/// One type rather than two calls, because the refresher has to be the *same*
/// client: a second one built from the same configuration would have its own
/// token cache, and renewing that would leave the client actually in use
/// holding the token that expired.
#[derive(Debug, Clone)]
pub struct CatalogConnection {
    /// The client every read goes through.
    pub client: Arc<dyn iceberg::Catalog>,
    /// How to renew its token, when it has one that expires.
    ///
    /// `None` for a static `token`, which has no exchange to repeat, and for a
    /// catalog with no credential at all.
    pub refresh: Option<Arc<dyn CredentialRefresh>>,
}

impl CatalogConfig {
    /// Build the catalog client.
    ///
    /// Credentials are read from the environment here rather than at parse
    /// time, so a configuration can be linted (`bergman policy lint`) on a
    /// machine that holds no credentials at all.
    pub async fn connect(&self) -> Result<CatalogConnection> {
        match self.kind {
            CatalogKind::Rest => self.connect_rest().await,
        }
    }

    /// Whether this catalog's credential is one that expires and can be renewed.
    ///
    /// Only the `OAuth2` client-credentials exchange is: a `token` — however it
    /// was supplied — is a value whose lifetime belongs to whatever produced
    /// it, and there is no exchange to repeat. An explicit `token_env` wins
    /// over `credential` in the client, so it counts here too.
    ///
    /// Gated with the only protocol that has an exchange to repeat, so that a
    /// build without it carries no dead code.
    #[cfg(feature = "catalog-rest")]
    fn has_renewable_credential(&self) -> bool {
        self.token_env.is_none()
            && !self.properties.contains_key("token")
            && self.properties.contains_key("credential")
    }

    #[cfg(feature = "catalog-rest")]
    async fn connect_rest(&self) -> Result<CatalogConnection> {
        use iceberg::CatalogBuilder;
        use iceberg_catalog_rest::{
            REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
        };

        let mut props = self.properties.clone();
        props.insert(REST_CATALOG_PROP_URI.to_string(), self.uri.clone());
        if let Some(warehouse) = &self.warehouse {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        }

        if let Some(var) = &self.token_env {
            let token = std::env::var(var).map_err(|_| {
                Error::config(format!(
                    "catalog \"{}\": token_env names ${var}, which is not set",
                    self.name
                ))
            })?;
            props.insert("token".to_string(), token);
        }

        let builder =
            RestCatalogBuilder::default().with_storage_factory(storage::resolving_factory());

        let catalog = Arc::new(
            builder
                .load(self.name.clone(), props)
                .await
                .map_err(|e| Error::Catalog(Box::new(e)))?,
        );

        Ok(CatalogConnection {
            client: catalog.clone(),
            refresh: self
                .has_renewable_credential()
                .then_some(catalog as Arc<dyn CredentialRefresh>),
        })
    }

    #[cfg(not(feature = "catalog-rest"))]
    async fn connect_rest(&self) -> Result<CatalogConnection> {
        Err(Error::Unsupported(format!(
            "catalog \"{}\" is a REST catalog, but this build has no `catalog-rest` feature",
            self.name
        )))
    }

    /// Build Bergman's commit path for this catalog.
    ///
    /// Separate from [`CatalogConfig::connect`] because it answers a different
    /// question: `connect` gives a client for everything `iceberg::Catalog` can
    /// already do, and this gives one for the commits it cannot express — see
    /// [`crate::commit`] for why that is a whole module rather than a method.
    pub async fn committer(&self) -> Result<Arc<dyn crate::commit::TableCommitter>> {
        match self.kind {
            CatalogKind::Rest => {
                let token = match &self.token_env {
                    Some(var) => Some(std::env::var(var).map_err(|_| {
                        Error::config(format!(
                            "catalog \"{}\": token_env names ${var}, which is not set",
                            self.name
                        ))
                    })?),
                    None => None,
                };

                // The same properties the catalog client reads, so one
                // configuration authenticates both. A commit path that
                // authenticated differently would let reads succeed and every
                // write return 401 — the tool would appear to work and quietly
                // change nothing.
                Ok(Arc::new(
                    crate::commit::RestCommitter::connect(
                        &self.uri,
                        self.warehouse.as_deref(),
                        &self.properties,
                        token,
                    )
                    .await?,
                ))
            }
        }
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
    #[cfg(feature = "catalog-rest")]
    fn only_an_exchangeable_credential_is_renewable() {
        // A token is a value whose lifetime belongs to whatever produced it —
        // there is no exchange to repeat, and asking upstream to regenerate one
        // errors rather than helping. Only the OAuth2 client-credentials flow
        // has something to renew.
        let mut plain = config("prod", Some("s3://b/wh"));
        assert!(!plain.has_renewable_credential(), "no credential at all");

        plain
            .properties
            .insert("credential".to_string(), "svc-bergman:hunter2".to_string());
        assert!(plain.has_renewable_credential());

        // A `token` wins over a `credential` in the client, so it wins here
        // too: renewing a credential whose token the client never uses would
        // be an exchange per cycle that changes nothing.
        let mut with_token = plain.clone();
        with_token
            .properties
            .insert("token".to_string(), "static".to_string());
        assert!(!with_token.has_renewable_credential());

        let mut with_token_env = plain.clone();
        with_token_env.token_env = Some("BERGMAN_CATALOG_TOKEN".to_string());
        assert!(!with_token_env.has_renewable_credential());
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
