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

mod rest;
mod snapshot;

pub use rest::RestCommitter;
pub use snapshot::{RewriteFiles, SnapshotProducer};

use async_trait::async_trait;
use iceberg::{TableIdent, TableRequirement, TableUpdate};

use crate::error::Result;

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
