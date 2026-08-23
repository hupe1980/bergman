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

/// List every table in a catalog, or in the configured namespaces.
///
/// Namespaces nest arbitrarily, so this is a full tree walk when no namespaces
/// are declared. Declaring them is the usual case in a large deployment, and it
/// turns an O(catalog) walk into an O(subtree) one.
pub async fn discover(
    config: &CatalogConfig,
    catalog: &Arc<dyn Catalog>,
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
        None => catalog.list_namespaces(None).await?,
    };

    let mut found = Vec::new();
    let mut queue = roots;

    while !queue.is_empty() {
        let level = std::mem::take(&mut queue);

        // Each namespace contributes both its tables and its children. Doing
        // the two listings for one namespace in the same task keeps the
        // concurrency bound meaningful: one permit is one namespace's work.
        let results: Vec<(Vec<DiscoveredTable>, Vec<NamespaceIdent>)> = stream::iter(level)
            .map(|ns| {
                let catalog = Arc::clone(catalog);
                let catalog_name = config.name.clone();
                async move {
                    let tables = catalog.list_tables(&ns).await?;
                    let children = catalog.list_namespaces(Some(&ns)).await?;

                    let tables = tables
                        .into_iter()
                        .map(|ident| DiscoveredTable {
                            table: TableRef::new(
                                &catalog_name,
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
