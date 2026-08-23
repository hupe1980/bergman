//! Errors.
//!
//! One error type for the whole crate. Maintenance is a background tenant, and
//! the single most important thing an error can carry is whether the caller
//! should *retry*, *replan*, or *stop* — so that distinction is a method on the
//! error rather than a comment in a handler.

use std::fmt;

use thiserror::Error;

/// The crate's result type.
pub type Result<T> = std::result::Result<T, Error>;

/// What a caller should do about an error.
///
/// This exists because the three outcomes have genuinely different handling and
/// getting them confused is how a maintenance engine corrupts a table. A
/// *conflict* means the table moved under us and our outputs describe a state
/// that no longer exists: the plan must be rebuilt from the new snapshot, never
/// re-committed as-is. A *transient* failure is the same request against a
/// working world — retrying is correct. A *terminal* failure will fail
/// identically forever, so retrying only spends money.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Retry the same operation after a backoff.
    Retry,
    /// Discard the plan and rebuild it from the current table state.
    Replan,
    /// Do not retry.
    Terminal,
}

/// A Bergman error.
#[derive(Debug, Error)]
pub enum Error {
    /// The configuration file could not be read, parsed, or validated.
    #[error("configuration error: {0}")]
    Config(String),

    /// A policy is internally inconsistent or names something that cannot exist.
    #[error("policy error: {0}")]
    Policy(String),

    /// The catalog rejected a request, or could not be reached.
    #[error("catalog error: {0}")]
    Catalog(#[source] Box<iceberg::Error>),

    /// A commit lost its compare-and-swap: the table moved between plan and
    /// commit.
    ///
    /// This is not a failure of the maintenance run. It is the expected outcome
    /// of competing with a foreground writer, and the correct response is to
    /// replan against the new snapshot.
    #[error("commit conflict on {table}: {detail}")]
    CommitConflict {
        /// The table whose commit lost.
        table: String,
        /// What moved.
        detail: String,
    },

    /// A plan was invalidated by a concurrent commit before it could be applied.
    ///
    /// Distinct from [`Error::CommitConflict`]: that one is the catalog
    /// refusing our compare-and-swap, this one is Bergman refusing to *offer*
    /// a commit it has already determined would be unsafe.
    #[error("plan for {table} is stale: {detail}")]
    StalePlan {
        /// The table whose plan is stale.
        table: String,
        /// Why the plan no longer describes the table.
        detail: String,
    },

    /// An operation was refused because performing it could not be made safe.
    ///
    /// Carries the reason so it reaches the operator rather than a log line:
    /// a skipped table with an unexplained reason is indistinguishable from a
    /// bug.
    #[error("{operation} refused on {table}: {reason}")]
    Refused {
        /// The operation that was refused.
        operation: &'static str,
        /// The table it was refused on.
        table: String,
        /// Why it was refused.
        reason: String,
    },

    /// Object storage could not be read, listed, or written.
    #[error("storage error: {0}")]
    Storage(#[source] Box<iceberg::Error>),

    /// Table metadata is malformed, or describes something Bergman cannot
    /// interpret.
    #[error("metadata error on {table}: {detail}")]
    Metadata {
        /// The table whose metadata could not be interpreted.
        table: String,
        /// What was wrong with it.
        detail: String,
    },

    /// A feature was requested that this build does not carry.
    ///
    /// Feature-gated backends fail here rather than being silently absent, so
    /// `--features` mistakes surface as a sentence naming the flag.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// An I/O failure outside object storage: reading a config file, writing an
    /// audit log.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// What the caller should do about this error.
    pub fn disposition(&self) -> Disposition {
        match self {
            // The table moved. Rebuilding the plan is the only safe response;
            // Re-committing outputs computed against a table that has moved
            // is how a concurrent delete gets discarded.
            Error::CommitConflict { .. } | Error::StalePlan { .. } => Disposition::Replan,

            // A catalog or storage failure is usually the network, and the
            // same request against a working world succeeds. Bergman does not
            // try to classify HTTP status codes into retryable and not: the
            // retry budget is small and bounded, so guessing wrong costs one
            // extra request, while guessing the *other* way costs a whole
            // maintenance cycle.
            Error::Catalog(_) | Error::Storage(_) | Error::Io(_) => Disposition::Retry,

            // These describe the request, not the world. They will fail the
            // same way forever.
            Error::Config(_)
            | Error::Policy(_)
            | Error::Refused { .. }
            | Error::Metadata { .. }
            | Error::Unsupported(_) => Disposition::Terminal,
        }
    }

    /// Whether the caller should rebuild the plan and try again.
    pub fn is_replan(&self) -> bool {
        self.disposition() == Disposition::Replan
    }

    /// Construct a [`Error::Refused`].
    pub fn refused(
        operation: &'static str,
        table: impl fmt::Display,
        reason: impl Into<String>,
    ) -> Self {
        Error::Refused {
            operation,
            table: table.to_string(),
            reason: reason.into(),
        }
    }

    /// Construct a [`Error::Metadata`].
    pub fn metadata(table: impl fmt::Display, detail: impl Into<String>) -> Self {
        Error::Metadata {
            table: table.to_string(),
            detail: detail.into(),
        }
    }

    /// Construct a [`Error::Config`].
    pub fn config(detail: impl Into<String>) -> Self {
        Error::Config(detail.into())
    }

    /// Construct a [`Error::Policy`].
    pub fn policy(detail: impl Into<String>) -> Self {
        Error::Policy(detail.into())
    }
}

/// Upstream returns one error type for both catalog RPCs and object-store I/O,
/// and the two want different dispositions in principle. In practice both are
/// [`Disposition::Retry`], so this conversion picks [`Error::Catalog`] and the
/// storage paths that care construct [`Error::Storage`] explicitly.
impl From<iceberg::Error> for Error {
    fn from(err: iceberg::Error) -> Self {
        Error::Catalog(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflicts_replan_rather_than_retry() {
        let err = Error::CommitConflict {
            table: "db.t".into(),
            detail: "ref moved".into(),
        };
        assert_eq!(err.disposition(), Disposition::Replan);
        assert!(err.is_replan());
    }

    #[test]
    fn refusals_are_terminal() {
        // A refusal is a decision, not a failure: retrying it burns a retry
        // budget to reach the same conclusion.
        let err = Error::refused("compact", "db.t", "row filter in effect");
        assert_eq!(err.disposition(), Disposition::Terminal);
        assert!(!err.is_replan());
    }

    #[test]
    fn refusal_message_names_operation_table_and_reason() {
        // The whole point of `Refused` is that the operator learns why a table
        // was skipped, so the rendering is part of the contract.
        let err = Error::refused("compact", "db.t", "row filter in effect");
        assert_eq!(
            err.to_string(),
            "compact refused on db.t: row filter in effect"
        );
    }
}
