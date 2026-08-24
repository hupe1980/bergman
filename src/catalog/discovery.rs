//! Walking a catalog to find the tables policy might have something to say
//! about.

use std::sync::Arc;

use futures::stream::{self, StreamExt, TryStreamExt};
use iceberg::{Catalog, NamespaceIdent};

use crate::catalog::CatalogConfig;
use crate::error::Result;
use crate::policy::TableRef;

/// How many namespaces are listed concurrently.
///
/// Discovery is latency-bound rather than CPU-bound — it is a tree walk over
/// RPCs — so the useful concurrency is much higher than the core count. It is
/// still bounded, because a catalog is a shared service and a maintenance tool
/// that stampedes it at startup is a bad tenant.
const DISCOVERY_CONCURRENCY: usize = 16;

/// A table found in a catalog.
#[derive(Debug, Clone)]
pub struct DiscoveredTable {
    /// How Bergman addresses it.
    pub table: TableRef,
    /// How the catalog addresses it.
    pub ident: iceberg::TableIdent,
}

/// The two listings a namespace walk needs.
///
/// Narrower than [`iceberg::Catalog`] for the same reason
/// [`crate::ops::TableLoader`] is: depending on the whole sixteen-method trait
/// to test a tree walk would mean building a whole catalog to prove the walk
/// terminates, and a guard nobody can test is a guard that gets removed.
#[async_trait::async_trait]
pub trait NamespaceSource: Send + Sync {
    /// The namespaces directly under `parent`, or the roots when it is `None`.
    async fn children(&self, parent: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>>;
    /// The tables directly in `namespace`.
    async fn tables(&self, namespace: &NamespaceIdent) -> Result<Vec<iceberg::TableIdent>>;
}

#[async_trait::async_trait]
impl NamespaceSource for Arc<dyn Catalog> {
    async fn children(&self, parent: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
        self.list_namespaces(parent).await.map_err(Into::into)
    }

    async fn tables(&self, namespace: &NamespaceIdent) -> Result<Vec<iceberg::TableIdent>> {
        self.list_tables(namespace).await.map_err(Into::into)
    }
}

/// List every table in a catalog, or in the configured namespaces.
///
/// Namespaces nest arbitrarily, so this is a full tree walk when no namespaces
/// are declared. Declaring them is the usual case in a large deployment, and it
/// turns an O(catalog) walk into an O(subtree) one.
pub async fn discover(
    config: &CatalogConfig,
    catalog: &(impl NamespaceSource + ?Sized),
) -> Result<Vec<DiscoveredTable>> {
    let roots: Vec<NamespaceIdent> = match &config.namespaces {
        Some(names) => names
            .iter()
            .map(|n| {
                // A configured namespace is dotted the way rule patterns are,
                // so `analytics.web` means the nested namespace, matching how
                // the same string reads everywhere else in the config.
                NamespaceIdent::from_vec(n.split('.').map(str::to_string).collect())
                    .map_err(crate::error::Error::from)
            })
            .collect::<Result<_>>()?,
        None => catalog.children(None).await?,
    };

    let mut found = Vec::new();
    let mut queue = roots;

    // A namespace is listed at most once. Nothing in the REST protocol stops a
    // catalog reporting one as its own descendant — a buggy paginator, a
    // federated mount that re-exports its root — and without this the walk
    // never terminates, so a daemon hangs in discovery and stops maintaining
    // everything. It also deduplicates a diamond, whose tables would otherwise
    // be discovered twice and have the second copy lose its compare-and-swap to
    // the first.
    let mut seen: std::collections::HashSet<NamespaceIdent> = std::collections::HashSet::new();

    while !queue.is_empty() {
        let level: Vec<NamespaceIdent> = std::mem::take(&mut queue)
            .into_iter()
            .filter(|ns| seen.insert(ns.clone()))
            .collect();
        if level.is_empty() {
            break;
        }

        // Each namespace contributes both its tables and its children. Doing
        // the two listings for one namespace in the same task keeps the
        // concurrency bound meaningful: one permit is one namespace's work.
        let results: Vec<(Vec<DiscoveredTable>, Vec<NamespaceIdent>)> = stream::iter(level)
            .map(|ns| {
                let catalog_name = config.name.as_str();
                async move {
                    let tables = catalog.tables(&ns).await?;
                    let children = catalog.children(Some(&ns)).await?;

                    let tables = tables
                        .into_iter()
                        .map(|ident| DiscoveredTable {
                            table: TableRef::new(
                                catalog_name,
                                ident.namespace().as_ref().to_vec(),
                                ident.name(),
                            ),
                            ident,
                        })
                        .collect();

                    Ok::<_, crate::error::Error>((tables, children))
                }
            })
            .buffer_unordered(DISCOVERY_CONCURRENCY)
            .try_collect()
            .await?;

        for (tables, children) in results {
            found.extend(tables);
            queue.extend(children);
        }
    }

    // Discovery order is whatever the catalog and the concurrency happened to
    // produce. Sorting makes `bergman plan` output stable between runs, which
    // is what lets an operator diff two plans and read the difference as a
    // change in the world rather than in the scheduler.
    found.sort_by(|a, b| a.table.cmp(&b.table));
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::catalog::{CatalogKind, StorageScheme};

    fn ns(parts: &[&str]) -> NamespaceIdent {
        NamespaceIdent::from_vec(parts.iter().map(|s| s.to_string()).collect()).expect("namespace")
    }

    fn config(namespaces: Option<Vec<String>>) -> CatalogConfig {
        CatalogConfig {
            name: "prod".into(),
            kind: CatalogKind::Rest,
            uri: "http://localhost:8181".into(),
            warehouse: None,
            storage: None as Option<StorageScheme>,
            properties: HashMap::new(),
            token_env: None,
            namespaces,
        }
    }

    /// A namespace tree described as a literal map, so a cyclic one is as easy
    /// to write as an honest one.
    #[derive(Default)]
    struct Tree {
        children: HashMap<Option<NamespaceIdent>, Vec<NamespaceIdent>>,
        tables: HashMap<NamespaceIdent, Vec<iceberg::TableIdent>>,
        /// How many times each namespace was listed, so a walk that revisits
        /// one is visible even when it happens to terminate.
        visits: std::sync::Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl NamespaceSource for Tree {
        async fn children(&self, parent: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
            Ok(self
                .children
                .get(&parent.cloned())
                .cloned()
                .unwrap_or_default())
        }

        async fn tables(&self, namespace: &NamespaceIdent) -> Result<Vec<iceberg::TableIdent>> {
            *self.visits.lock().expect("visits") += 1;
            Ok(self.tables.get(namespace).cloned().unwrap_or_default())
        }
    }

    #[tokio::test]
    async fn a_nested_tree_is_walked_to_its_leaves() {
        let mut tree = Tree::default();
        tree.children.insert(None, vec![ns(&["analytics"])]);
        tree.children
            .insert(Some(ns(&["analytics"])), vec![ns(&["analytics", "web"])]);
        tree.tables.insert(
            ns(&["analytics"]),
            vec![iceberg::TableIdent::from_strs(["analytics", "orders"]).unwrap()],
        );
        tree.tables.insert(
            ns(&["analytics", "web"]),
            vec![iceberg::TableIdent::from_strs(["analytics", "web", "events"]).unwrap()],
        );

        let found = discover(&config(None), &tree).await.unwrap();
        assert_eq!(
            found
                .iter()
                .map(|d| d.table.to_string())
                .collect::<Vec<_>>(),
            vec!["prod.analytics.orders", "prod.analytics.web.events"]
        );
    }

    #[tokio::test]
    async fn a_catalog_that_reports_a_namespace_as_its_own_descendant_still_terminates() {
        // Nothing in the REST protocol prevents this, and with no guard the
        // walk never ends — see `discover`.
        let mut tree = Tree::default();
        tree.children.insert(None, vec![ns(&["a"])]);
        // `a` claims itself as its own child.
        tree.children.insert(Some(ns(&["a"])), vec![ns(&["a"])]);
        tree.tables.insert(
            ns(&["a"]),
            vec![iceberg::TableIdent::from_strs(["a", "t"]).unwrap()],
        );

        let found = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            discover(&config(None), &tree),
        )
        .await
        .expect("the walk must terminate")
        .unwrap();

        // And the table appears once, not once per revisit: the same table
        // planned twice in one cycle would have the second copy lose its
        // compare-and-swap to the first.
        assert_eq!(
            found
                .iter()
                .map(|d| d.table.to_string())
                .collect::<Vec<_>>(),
            vec!["prod.a.t"]
        );
        assert_eq!(*tree.visits.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn a_diamond_lists_each_namespace_once() {
        // Two parents naming one child. Legal enough to happen, and without
        // deduplication its tables would be discovered twice.
        let mut tree = Tree::default();
        tree.children.insert(None, vec![ns(&["a"]), ns(&["b"])]);
        tree.children
            .insert(Some(ns(&["a"])), vec![ns(&["shared"])]);
        tree.children
            .insert(Some(ns(&["b"])), vec![ns(&["shared"])]);
        tree.tables.insert(
            ns(&["shared"]),
            vec![iceberg::TableIdent::from_strs(["shared", "t"]).unwrap()],
        );

        let found = discover(&config(None), &tree).await.unwrap();
        assert_eq!(
            found
                .iter()
                .map(|d| d.table.to_string())
                .collect::<Vec<_>>(),
            vec!["prod.shared.t"]
        );
    }

    #[tokio::test]
    async fn configured_namespaces_replace_the_root_listing() {
        // Declaring them turns an O(catalog) walk into an O(subtree) one, which
        // is the usual case in a large deployment.
        let mut tree = Tree::default();
        tree.children
            .insert(None, vec![ns(&["everything"]), ns(&["else"])]);
        tree.tables.insert(
            ns(&["analytics", "web"]),
            vec![iceberg::TableIdent::from_strs(["analytics", "web", "events"]).unwrap()],
        );

        let found = discover(&config(Some(vec!["analytics.web".into()])), &tree)
            .await
            .unwrap();
        assert_eq!(
            found
                .iter()
                .map(|d| d.table.to_string())
                .collect::<Vec<_>>(),
            vec!["prod.analytics.web.events"],
            "a dotted namespace means the nested one, as everywhere else in the config"
        );
    }
}
