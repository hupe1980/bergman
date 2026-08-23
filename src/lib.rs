//! # Bergman — a Rust-native maintenance engine for Apache Iceberg
//!
//! Bergman plans and executes Iceberg table maintenance — snapshot expiration,
//! manifest optimization, orphan-file cleanup, and (as upstream support lands)
//! compaction — with no Spark, no Trino, and no JVM.
//!
//! This crate is **library-first**. The `bergman` binary is a thin consumer of
//! the API below, which is the contract: anything the CLI can do, an embedder
//! can do. That matters because the Rust catalogs — Lakekeeper, Rustberg,
//! Polaris — all draw the same boundary, keeping metadata and permissions and
//! leaving data-plane rewrites to an external engine they can trigger. Bergman
//! is built to be that engine, in-process or out.
//!
//! ## Library contract
//!
//! These are guarantees, not implementation details, and the CLI is where each
//! one's opposite lives:
//!
//! - **No global state.** The library installs no logger, no tracing
//!   subscriber, no signal handler, and reads no configuration file of its own
//!   accord. It emits [`tracing`] spans and events; what listens is yours.
//! - **Bring your own runtime.** Every entry point is a plain `async fn` on the
//!   caller's runtime. Concurrency limits are parameters, never process-wide
//!   statics.
//! - **Planning is pure.** [`Bergman::plan`] performs no writes and no
//!   deletions, so dry-run is not a mode to remember — it is what happens when
//!   you stop before [`Bergman::run`].
//! - **Observation is a hook.** Implement [`obs::MaintenanceObserver`] to wire
//!   your own metrics, approval gates, or event bus without forking.
//!
//! ## Quick start
//!
//! ```no_run
//! use bergman::{Bergman, policy::Config};
//!
//! # async fn example() -> bergman::Result<()> {
//! let config = Config::from_path("bergman.toml")?;
//! let bergman = Bergman::new(config).await?;
//!
//! // Read-only: what is wrong with these tables?
//! for health in bergman.inspect().await? {
//!     println!("{}: {}", health.table, health.summary());
//! }
//!
//! // Still read-only: what would maintenance do about it?
//! let plan = bergman.plan().await?;
//! println!("{} operations across {} tables", plan.operation_count(), plan.tables.len());
//!
//! // The first call that writes anything.
//! let report = bergman.run(&plan).await?;
//! println!("{report}");
//! # Ok(())
//! # }
//! ```
//!
//! ## Status
//!
//! Snapshot expiration, expiration file cleanup, and orphan-file removal are
//! implemented. Compaction and manifest rewriting are **planned but not
//! executed**: both require committing a snapshot that *removes* files, and
//! upstream `iceberg-rust` has no such action — `TransactionAction` is
//! `pub(crate)` and `TableCommit`'s builder is `pub(crate)`, so no external
//! crate can construct one. Bergman plans and reports those operations today
//! and refuses to execute them, naming the reason, rather than pretending. See
//! `CONCEPT.md` §13 for the tracking detail.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::doc_markdown)]

pub mod catalog;
#[cfg(feature = "cli")]
pub mod cli;
pub mod error;
pub mod health;
pub mod obs;
pub mod ops;
pub mod plan;
pub mod policy;
pub mod util;

mod engine;

pub use engine::{Bergman, BergmanBuilder};
pub use error::{Disposition, Error, Result};
pub use plan::{MaintenancePlan, RunReport};
pub use policy::{Config, Policy, TableRef};
