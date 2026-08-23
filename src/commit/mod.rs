//! Bergman's own commit layer.
//!
//! An Iceberg commit is `(requirements, updates)` applied atomically. The
//! `iceberg` crate cannot express one from outside: no `Transaction` action
//! removes a data file, `TransactionAction` is `pub(crate)`, and
//! [`iceberg::TableCommit`]'s builder is `pub(crate)`. Compaction and manifest
//! rewriting are therefore unreachable through that API.
//!
//! The common answer is to fork — `nimtable/iceberg-compaction` pins
//! `risingwavelabs/iceberg-rust` at a git revision — which costs a rebase
//! forever and a crate that cannot be published, since Cargo rejects git
//! dependencies on crates.io.
//!
//! Bergman owns the one blocked layer instead. Every piece of a commit is
//! already public; only the delivery is not:
//!
//! | Piece | Upstream API |
//! |---|---|
//! | Manifests | [`iceberg::spec::ManifestWriterBuilder`] |
//! | Manifest lists | [`iceberg::spec::ManifestListWriter`] |
//! | Snapshots | [`iceberg::spec::Snapshot::builder`] |
//! | Updates and preconditions | [`iceberg::TableUpdate`], [`iceberg::TableRequirement`] — public and `Serialize` |
//! | Data files | [`iceberg::writer`] |
//! | **Delivering the commit** | **— nothing** |
//!
//! So the manifests, manifest list and snapshot are written with upstream's own
//! writers, and only `(requirements, updates)` is delivered here. The bytes on
//! the wire are what `iceberg-catalog-rest` would send: the same serialized
//! types, the same endpoint.
//!
//! Operations are written against [`TableCommitter`] rather than a transport,
//! so an upstream action that can express a rewrite becomes a second
//! implementation and nothing above this module changes.

mod auth;
mod rest;
mod snapshot;

pub use auth::{Credential, TokenSource};
pub use rest::RestCommitter;
pub use snapshot::{BranchRetention, RewriteFiles, SnapshotProducer};

use async_trait::async_trait;
use iceberg::spec::FormatVersion;
use iceberg::{TableIdent, TableRequirement, TableUpdate};

use crate::error::Result;

/// Why Bergman will not author a snapshot for a table of this format version,
/// if it will not.
///
/// Only format 3 answers. V3 introduces **row lineage**: every row carries a
/// `_row_id` and a `_last_updated_sequence_number`, a snapshot must declare the
/// `first-row-id` it starts from and how many rows it added, and each manifest
/// carries the base its entries count from. Three things follow, and each on
/// its own is disqualifying:
///
/// 1. A rewrite must carry every row's existing `_row_id` through to the file
///    that replaces it. Upstream's `ArrowReader` does not project the field and
///    its `RollingFileWriter` will not accept it, so a rewrite would renumber
///    every row it touched — and a `MERGE` or a CDC consumer joining on row id
///    would then match the wrong rows, with nothing failing.
/// 2. A manifest holding *existing* files, written fresh, has no `first-row-id`
///    of its own, so `ManifestListWriter` assigns it a new range — moving files
///    that were never rewritten to row ids they never had.
/// 3. `TableMetadataBuilder::add_snapshot` rejects a v3 snapshot carrying no
///    `first-row-id` outright, so such a commit does not merely risk being
///    wrong: no spec-correct catalog applies it at all.
///
/// Operations that do not go through Bergman's snapshot producer — expiration,
/// which is upstream's own action, and orphan removal, which commits nothing —
/// stay available on a v3 table.
pub fn authoring_refusal(format_version: FormatVersion) -> Option<&'static str> {
    match format_version {
        FormatVersion::V1 | FormatVersion::V2 => None,
        FormatVersion::V3 => Some(
            "the table is Iceberg format v3, whose row lineage Bergman cannot yet preserve \
             through a rewrite; snapshot expiration and orphan removal still run",
        ),
    }
}

/// Delivers a commit to a catalog.
///
/// The abstraction exists so operations do not know *how* a commit is
/// delivered. Today there is one implementation, speaking REST. When upstream
/// grows an action that can express a rewrite, a second implementation wraps it
/// and the operations are untouched.
#[async_trait]
pub trait TableCommitter: Send + Sync + std::fmt::Debug {
    /// Apply `updates` atomically, provided every requirement still holds.
    ///
    /// Implementations must map a rejected precondition to
    /// [`crate::Error::CommitConflict`] rather than a generic failure: the
    /// caller's response to a conflict is to *replan*, and to any other error
    /// is not (see [`crate::error::Disposition`]).
    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records what it was asked to commit, so operations can be tested
    /// without a catalog.
    #[derive(Debug, Default)]
    pub struct RecordingCommitter {
        pub calls: Mutex<Vec<(Vec<TableRequirement>, Vec<TableUpdate>)>>,
    }

    #[async_trait]
    impl TableCommitter for RecordingCommitter {
        async fn commit(
            &self,
            _ident: &TableIdent,
            requirements: Vec<TableRequirement>,
            updates: Vec<TableUpdate>,
        ) -> Result<()> {
            self.calls.lock().unwrap().push((requirements, updates));
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_committer_receives_requirements_and_updates_unchanged() {
        let committer = RecordingCommitter::default();
        let ident = TableIdent::from_strs(["db", "t"]).unwrap();

        committer
            .commit(
                &ident,
                vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".into(),
                    snapshot_id: Some(42),
                }],
                vec![TableUpdate::SetLocation {
                    location: "s3://b/t".into(),
                }],
            )
            .await
            .unwrap();

        let calls = committer.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.len(), 1);
        assert_eq!(calls[0].1.len(), 1);
    }
}
