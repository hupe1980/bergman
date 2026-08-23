//! Object-store wiring.
//!
//! Bergman uses upstream's *resolving* storage factory, which picks a backend
//! from each path's scheme and caches one storage per scheme. A single catalog
//! can therefore hold a warehouse on S3 and metadata on local disk, and a table
//! whose location moved between clouds still reads — none of which a
//! scheme-per-catalog factory could do.
//!
//! [`StorageScheme`] survives that simplification for one job: telling an
//! operator *which Cargo feature* a build is missing when a scheme turns out
//! not to be compiled in. Upstream's failure for that case is
//! `Unsupported storage scheme: s3`, which does not say what to do about it.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Build the storage factory every catalog uses.
pub fn resolving_factory() -> Arc<dyn iceberg::io::StorageFactory> {
    Arc::new(iceberg_storage_opendal::OpenDalResolvingStorageFactory::new())
}

/// An object-store scheme, used to report which feature a build is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageScheme {
    /// Amazon S3 and S3-compatible stores, including `MinIO`.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Data Lake Storage Gen2.
    Azure,
    /// The local filesystem.
    Fs,
    /// In-memory, for tests.
    Memory,
}

impl StorageScheme {
    /// Identify the scheme of a URI.
    pub fn from_uri(uri: &str) -> Option<Self> {
        let scheme = uri.split_once("://")?.0.to_ascii_lowercase();
        match scheme.as_str() {
            // `s3a`/`s3n` are the Hadoop-era spellings, still common in
            // warehouse configuration written years ago and copied since.
            "s3" | "s3a" | "s3n" => Some(Self::S3),
            "gs" | "gcs" => Some(Self::Gcs),
            "abfs" | "abfss" | "wasb" | "wasbs" => Some(Self::Azure),
            "file" => Some(Self::Fs),
            "memory" => Some(Self::Memory),
            _ => None,
        }
    }

    /// The Cargo feature that carries this backend.
    pub fn feature_name(&self) -> &'static str {
        match self {
            Self::S3 => "storage-s3",
            Self::Gcs => "storage-gcs",
            Self::Azure => "storage-azure",
            // Always compiled in: a build that cannot read a local warehouse
            // cannot run its own test suite.
            Self::Fs | Self::Memory => "(always available)",
        }
    }

    /// Whether this build carries the backend.
    pub fn is_available(&self) -> bool {
        match self {
            Self::S3 => cfg!(feature = "storage-s3"),
            Self::Gcs => cfg!(feature = "storage-gcs"),
            Self::Azure => cfg!(feature = "storage-azure"),
            Self::Fs | Self::Memory => true,
        }
    }
}

impl std::fmt::Display for StorageScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::S3 => "S3",
            Self::Gcs => "GCS",
            Self::Azure => "Azure",
            Self::Fs => "local filesystem",
            Self::Memory => "in-memory",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hadoop_era_s3_spellings_are_recognised() {
        // Warehouse locations written years ago say `s3a://`, and they are
        // copied forward forever. Refusing them would be pedantry.
        for uri in ["s3://b/w", "s3a://b/w", "s3n://b/w", "S3://b/w"] {
            assert_eq!(
                StorageScheme::from_uri(uri),
                Some(StorageScheme::S3),
                "{uri}"
            );
        }
    }

    #[test]
    fn azure_and_gcs_spellings() {
        assert_eq!(
            StorageScheme::from_uri("abfss://c@a.dfs.core.windows.net/w"),
            Some(StorageScheme::Azure)
        );
        assert_eq!(
            StorageScheme::from_uri("gs://bucket/w"),
            Some(StorageScheme::Gcs)
        );
    }

    #[test]
    fn a_path_without_a_scheme_is_not_guessed() {
        // A bare path could be anything. Guessing "local filesystem" would be
        // wrong exactly when a warehouse is misconfigured, which is the moment
        // a wrong guess costs the most.
        assert_eq!(StorageScheme::from_uri("/var/lib/warehouse"), None);
        assert_eq!(StorageScheme::from_uri("bucket/warehouse"), None);
    }

    #[test]
    fn local_backends_are_always_available() {
        // The test suite depends on this: no containers, no cloud credentials.
        assert!(StorageScheme::Fs.is_available());
        assert!(StorageScheme::Memory.is_available());
    }
}
