//! Catalog configuration, construction, and table discovery.
//!
//! Bergman reaches every catalog the same way it reaches Rustberg: as an
//! ordinary Iceberg REST client. There is no private API and no shared types
//! with any catalog implementation — the contract is the wire, so a catalog
//! that speaks the spec works without Bergman knowing which one it is.

mod discovery;
mod storage;

pub use discovery::{DiscoveredTable, discover};
pub use storage::{StorageScheme, resolving_factory};

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::policy::TableRef;

/// How to reach one catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl CatalogConfig {
    /// Build the catalog client.
    ///
    /// Credentials are read from the environment here rather than at parse
    /// time, so a configuration can be linted (`bergman policy lint`) on a
    /// machine that holds no credentials at all.
    pub async fn connect(&self) -> Result<Arc<dyn iceberg::Catalog>> {
        match self.kind {
            CatalogKind::Rest => self.connect_rest().await,
        }
    }

    #[cfg(feature = "catalog-rest")]
    async fn connect_rest(&self) -> Result<Arc<dyn iceberg::Catalog>> {
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

        let catalog = builder
            .load(self.name.clone(), props)
            .await
            .map_err(|e| Error::Catalog(Box::new(e)))?;

        Ok(Arc::new(catalog))
    }

    #[cfg(not(feature = "catalog-rest"))]
    async fn connect_rest(&self) -> Result<Arc<dyn iceberg::Catalog>> {
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
                    // Some deployments put the token straight in `properties`,
                    // which `iceberg-catalog-rest` also accepts. Reading it here
                    // keeps both clients authenticating identically — one that
                    // authenticated and one that did not would fail only on the
                    // first commit, long after startup.
                    None => self.properties.get("token").cloned(),
                };

                Ok(Arc::new(
                    crate::commit::RestCommitter::connect(
                        &self.uri,
                        self.warehouse.as_deref(),
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
