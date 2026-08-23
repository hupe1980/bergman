//! Object listing.
//!
//! `iceberg::io::FileIO` has read, write, delete and `delete_prefix`, but no
//! `list`. Orphan removal is *defined* by listing storage and subtracting what
//! metadata reaches, so Bergman carries this layer itself — a temporary
//! implementation while the upstream gap exists, and the natural thing to
//! contribute back.
//!
//! The trait exists so the safety logic in [`super::orphans`] can be tested
//! against an in-memory store rather than against a cloud account. Every rule
//! that decides whether a file lives or dies is exercised that way.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::error::{Error, Result};

/// One object in a store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Absolute path, with the scheme the store was addressed by.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Last modification time, when the store reports one.
    ///
    /// `None` is treated as "too young to touch" by the orphan scanner: a
    /// store that will not say how old a file is cannot be used to argue that
    /// it is old enough to delete.
    pub last_modified: Option<DateTime<Utc>>,
}

/// Listing and deletion over an object store.
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync + std::fmt::Debug {
    /// Recursively list everything under a prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// Delete one object.
    async fn delete(&self, path: &str) -> Result<()>;
}

/// An [`ObjectStore`] backed by `OpenDAL`.
#[derive(Debug)]
pub struct OpendalStore {
    operator: opendal::Operator,
    /// The `scheme://authority` the operator is rooted at, restored onto every
    /// listed path so callers see the same absolute form the table's metadata
    /// uses.
    prefix: String,
}

impl OpendalStore {
    /// Build a store for a location, using Iceberg-named properties.
    ///
    /// `properties` are the catalog's own (`s3.endpoint`, `s3.region`, …), so a
    /// deployment configures storage once and both clients read it.
    pub fn for_location(location: &str, properties: &HashMap<String, String>) -> Result<Self> {
        let (scheme, authority, _) = split_location(location)?;

        let (opendal_scheme, config) = match scheme.as_str() {
            "s3" => ("s3", s3_config(&authority, properties)),
            "gs" => ("gcs", gcs_config(&authority, properties)),
            "abfss" | "wasbs" => ("azdls", azure_config(&authority, properties)),
            "file" => ("fs", vec![("root".to_string(), "/".to_string())]),
            "memory" => ("memory", vec![]),
            other => {
                return Err(Error::Unsupported(format!(
                    "orphan scanning does not support {other}:// locations"
                )));
            }
        };

        let operator = opendal::Operator::via_iter(opendal_scheme, config).map_err(|e| {
            Error::Unsupported(format!(
                "no object-store client for {scheme}:// in this build \
                 (rebuild with the matching storage feature): {e}"
            ))
        })?;

        let prefix = if scheme == "file" {
            "file://".to_string()
        } else {
            format!("{scheme}://{authority}")
        };

        Ok(Self { operator, prefix })
    }
}

#[async_trait::async_trait]
impl ObjectStore for OpendalStore {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let (_, _, key) = split_location(prefix)?;
        // A recursive listing of a *directory*: the trailing slash is what
        // stops `…/events` from also returning `…/events_archive`, which is the
        // same prefix hazard the containment check guards.
        let key = format!("{}/", key.trim_end_matches('/'));

        let entries = self
            .operator
            .list_with(&key)
            .recursive(true)
            .await
            .map_err(|e| Error::Storage(Box::new(to_iceberg_error(e))))?;

        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let meta = entry.metadata();
            // Directory markers are not files; deleting one deletes nothing and
            // on some stores errors.
            if meta.is_dir() {
                continue;
            }
            out.push(ObjectMeta {
                path: format!("{}/{}", self.prefix, entry.path().trim_start_matches('/')),
                size: meta.content_length(),
                // OpenDAL times are `jiff`-backed; `SystemTime` is the common
                // currency between it and chrono, and both conversions are
                // infallible.
                last_modified: meta
                    .last_modified()
                    .map(|t| std::time::SystemTime::from(t).into()),
            });
        }
        Ok(out)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (_, _, key) = split_location(path)?;
        self.operator
            .delete(&key)
            .await
            .map_err(|e| Error::Storage(Box::new(to_iceberg_error(e))))
    }
}

fn to_iceberg_error(e: opendal::Error) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string())
}

/// Split `scheme://authority/key` into its parts.
fn split_location(location: &str) -> Result<(String, String, String)> {
    let (scheme, rest) = location
        .split_once("://")
        .ok_or_else(|| Error::config(format!("{location:?} has no scheme; expected scheme://…")))?;
    let scheme = match scheme.to_ascii_lowercase().as_str() {
        "s3a" | "s3n" => "s3".to_string(),
        "gcs" => "gs".to_string(),
        "abfs" => "abfss".to_string(),
        "wasb" => "wasbs".to_string(),
        other => other.to_string(),
    };

    // `file:///var/lib/wh` has an empty authority and an absolute key.
    let (authority, key) = match rest.split_once('/') {
        Some((authority, key)) => (authority.to_string(), key.to_string()),
        None => (rest.to_string(), String::new()),
    };

    Ok((scheme, authority, key))
}

/// Map Iceberg's S3 property names onto `OpenDAL`'s.
///
/// The names differ, and the mapping is the whole reason this function exists:
/// a deployment writes `s3.endpoint` because that is what every other Iceberg
/// tool reads, and it must reach the listing client too.
fn s3_config(bucket: &str, properties: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut config = vec![("bucket".to_string(), bucket.to_string())];

    for (iceberg_key, opendal_key) in [
        ("s3.endpoint", "endpoint"),
        ("s3.region", "region"),
        ("s3.access-key-id", "access_key_id"),
        ("s3.secret-access-key", "secret_access_key"),
        ("s3.session-token", "session_token"),
    ] {
        if let Some(value) = properties.get(iceberg_key) {
            config.push((opendal_key.to_string(), value.clone()));
        }
    }

    // Iceberg says "use path-style"; OpenDAL asks the inverse question. MinIO
    // deployments set this, and getting the polarity wrong makes every request
    // hit a hostname that does not resolve.
    if let Some(value) = properties.get("s3.path-style-access") {
        let path_style = value.eq_ignore_ascii_case("true");
        config.push((
            "enable_virtual_host_style".to_string(),
            (!path_style).to_string(),
        ));
    }

    config
}

fn gcs_config(bucket: &str, properties: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut config = vec![("bucket".to_string(), bucket.to_string())];
    for (iceberg_key, opendal_key) in [
        ("gcs.project-id", "project"),
        ("gcs.oauth2.token", "token"),
        ("gcs.credentials-json", "credential"),
    ] {
        if let Some(value) = properties.get(iceberg_key) {
            config.push((opendal_key.to_string(), value.clone()));
        }
    }
    config
}

fn azure_config(authority: &str, properties: &HashMap<String, String>) -> Vec<(String, String)> {
    // `abfss://filesystem@account.dfs.core.windows.net/path`
    let (filesystem, host) = authority.split_once('@').unwrap_or((authority, ""));
    let account = host.split('.').next().unwrap_or("");

    let mut config = vec![
        ("filesystem".to_string(), filesystem.to_string()),
        (
            "endpoint".to_string(),
            format!("https://{account}.dfs.core.windows.net"),
        ),
    ];
    for (iceberg_key, opendal_key) in [
        ("adls.auth.shared-key.account.name", "account_name"),
        ("adls.auth.shared-key.account.key", "account_key"),
        ("adls.sas-token", "sas_token"),
    ] {
        if let Some(value) = properties.get(iceberg_key) {
            config.push((opendal_key.to_string(), value.clone()));
        }
    }
    config
}

/// An in-memory [`ObjectStore`], for tests.
#[derive(Debug, Default)]
pub struct InMemoryStore {
    objects: std::sync::Mutex<Vec<ObjectMeta>>,
}

impl InMemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an object.
    pub fn insert(&self, path: &str, size: u64, last_modified: Option<DateTime<Utc>>) {
        self.objects.lock().unwrap().push(ObjectMeta {
            path: crate::ops::reachability::normalize(path),
            size,
            last_modified,
        });
    }

    /// Every path still present.
    pub fn paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .objects
            .lock()
            .unwrap()
            .iter()
            .map(|o| o.path.clone())
            .collect();
        paths.sort();
        paths
    }
}

#[async_trait::async_trait]
impl ObjectStore for InMemoryStore {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|o| crate::ops::reachability::is_inside(prefix, &o.path))
            .cloned()
            .collect())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let normalized = crate::ops::reachability::normalize(path);
        self.objects
            .lock()
            .unwrap()
            .retain(|o| o.path != normalized);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locations_split_into_scheme_authority_and_key() {
        assert_eq!(
            split_location("s3://bucket/wh/db/t").unwrap(),
            ("s3".into(), "bucket".into(), "wh/db/t".into())
        );
        // A `file://` URL has an empty authority and an absolute key.
        assert_eq!(
            split_location("file:///var/lib/wh").unwrap(),
            ("file".into(), "".into(), "var/lib/wh".into())
        );
    }

    #[test]
    fn split_canonicalizes_scheme_aliases() {
        assert_eq!(split_location("s3a://b/k").unwrap().0, "s3");
        assert_eq!(split_location("gcs://b/k").unwrap().0, "gs");
    }

    #[test]
    fn a_location_without_a_scheme_is_refused() {
        assert!(split_location("/var/lib/wh").is_err());
    }

    #[test]
    fn iceberg_s3_properties_map_onto_opendal_names() {
        let props = HashMap::from([
            ("s3.endpoint".to_string(), "http://minio:9000".to_string()),
            ("s3.region".to_string(), "us-east-1".to_string()),
            ("s3.access-key-id".to_string(), "key".to_string()),
        ]);
        let config = s3_config("bucket", &props);

        assert!(config.contains(&("bucket".into(), "bucket".into())));
        assert!(config.contains(&("endpoint".into(), "http://minio:9000".into())));
        assert!(config.contains(&("access_key_id".into(), "key".into())));
    }

    #[test]
    fn path_style_access_inverts_for_opendal() {
        // Iceberg asks "path style?"; OpenDAL asks "virtual host style?".
        // MinIO sets this, and the wrong polarity sends every request to a
        // hostname that does not resolve.
        let props = HashMap::from([("s3.path-style-access".to_string(), "true".to_string())]);
        let config = s3_config("bucket", &props);
        assert!(config.contains(&("enable_virtual_host_style".into(), "false".into())));
    }

    #[test]
    fn azure_authority_splits_into_filesystem_and_account() {
        let config = azure_config("fs@acct.dfs.core.windows.net", &HashMap::new());
        assert!(config.contains(&("filesystem".into(), "fs".into())));
        assert!(config.contains(&(
            "endpoint".into(),
            "https://acct.dfs.core.windows.net".into()
        )));
    }

    #[tokio::test]
    async fn in_memory_store_lists_by_containment_not_string_prefix() {
        let store = InMemoryStore::new();
        store.insert("s3://b/wh/events/data/a.parquet", 10, None);
        store.insert("s3://b/wh/events_archive/b.parquet", 10, None);

        let listed = store.list("s3://b/wh/events").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, "s3://b/wh/events/data/a.parquet");
    }
}
