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
pub(crate) mod rest;
mod snapshot;

pub use auth::{Credential, TokenSource};
pub use rest::{RestCommitter, http_client};
pub use snapshot::{BranchRetention, FileRef, RewriteFiles, SnapshotProducer};

use async_trait::async_trait;
use iceberg::{TableIdent, TableRequirement, TableUpdate};

use crate::error::Result;

/// Where a table's metadata files belong.
///
/// Iceberg lets a table say, with `write.metadata.path`, that its manifests and
/// manifest lists live somewhere other than `<location>/metadata` — a separate
/// prefix with its own lifecycle rules, storage class or IAM policy is the usual
/// reason, and on some deployments it is a different bucket.
///
/// Bergman writes manifests and manifest lists itself, so it has to honour this
/// or it puts them where the table said they do not go. The released
/// `iceberg-rust` does not read the property at all (0.11.0 adds it), which is
/// why this is here rather than borrowed.
///
/// The trailing slash is trimmed so callers can join with `/` unconditionally.
pub(crate) fn metadata_dir(metadata: &iceberg::spec::TableMetadata) -> String {
    metadata
        .properties()
        .get("write.metadata.path")
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map_or_else(
            || format!("{}/metadata", metadata.location().trim_end_matches('/')),
            |p| p.trim_end_matches('/').to_string(),
        )
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
    ///
    /// `ctx` is why this commit exists — the run, the table, the operation and
    /// the rule that triggered it. It is not decoration: a governing catalog
    /// records an authorization decision per commit, and correlating that
    /// decision with Bergman's own reason for the commit needs an identifier
    /// both sides wrote down. [`RestCommitter`] sends `ctx.run_id` as
    /// `X-Request-Id`, which is the same value every audit record of the run
    /// carries (see [`crate::obs::AuditRecord`]).
    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
        ctx: crate::obs::OperationContext<'_>,
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
            _ctx: crate::obs::OperationContext<'_>,
        ) -> Result<()> {
            self.calls.lock().unwrap().push((requirements, updates));
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_committer_receives_requirements_and_updates_unchanged() {
        let committer = RecordingCommitter::default();
        let ident = TableIdent::from_strs(["db", "t"]).unwrap();
        let table = crate::policy::TableRef::new("prod", ["db"], "t");
        let ctx = crate::obs::OperationContext {
            run_id: "run-1",
            table: &table,
            kind: crate::plan::OperationKind::Compact,
            matched_rule: "prod.**",
            reason: "test",
        };

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
                ctx,
            )
            .await
            .unwrap();

        let calls = committer.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.len(), 1);
        assert_eq!(calls[0].1.len(), 1);
    }
}

#[cfg(test)]
mod metadata_dir_tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadata,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    use super::metadata_dir;

    fn metadata(properties: &[(&str, &str)]) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://bucket/wh/db/t/".to_string(),
            FormatVersion::V2,
            properties
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn a_table_that_says_nothing_gets_the_conventional_directory() {
        // The control, and it also pins the trailing-slash handling: the table
        // location above ends in one, and a doubled slash in a manifest path is
        // a path no reachability set built from the metadata would match.
        assert_eq!(metadata_dir(&metadata(&[])), "s3://bucket/wh/db/t/metadata");
    }

    #[test]
    fn a_table_that_names_a_metadata_path_gets_it() {
        // The guarded case. Writing a manifest to `<location>/metadata` when the
        // table said otherwise puts it in a prefix with different lifecycle
        // rules, a different storage class, or — where the property names
        // another bucket — no write permission at all.
        assert_eq!(
            metadata_dir(&metadata(&[(
                "write.metadata.path",
                "s3://meta-bucket/t/md/"
            )])),
            "s3://meta-bucket/t/md"
        );
    }

    #[test]
    fn an_empty_metadata_path_is_not_a_declaration() {
        assert_eq!(
            metadata_dir(&metadata(&[("write.metadata.path", "  ")])),
            "s3://bucket/wh/db/t/metadata"
        );
    }
}
