//! # Bergman — a Rust-native maintenance engine for Apache Iceberg
//!
//! Bergman plans and executes Iceberg table maintenance — compaction, snapshot
//! expiration, manifest optimization and orphan-file cleanup — with no Spark,
//! no Trino, and no JVM.
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
//! ## Commits
//!
//! `iceberg::Transaction` has no action that removes a data file, and its
//! commit API is crate-private, so compaction and manifest rewriting cannot be
//! expressed through it. Bergman builds those commits from upstream's public
//! writers and delivers them itself — see [`commit`].
//!
//! ## Not implemented
//!
//! Z-order and other space-filling clustering — a global sort within a file
//! group works, and it spills, so a group is never too large to sort;
//! **rewriting an Iceberg format v3 table**, whose row lineage a rewrite cannot
//! preserve (expiration and orphan removal still run — see
//! [`commit::authoring_refusal`]); rewriting across a partition-spec change;
//! writing anything but Parquet; `OpenTelemetry` export (the library emits
//! [`tracing`], which an embedder's subscriber can export); and catalog
//! protocols other than REST.
//!
//! Each of these is *refused with a named reason* rather than attempted, and
//! the refusal reaches [`MaintenancePlan`] rather than only a log line.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::doc_markdown)]

pub mod catalog;
#[cfg(feature = "cli")]
pub mod cli;
pub mod commit;
pub mod error;
pub mod health;
pub mod obs;
pub mod ops;
pub mod plan;
pub mod policy;
pub mod sched;
pub mod util;

mod engine;

pub use engine::{Bergman, BergmanBuilder};
pub use error::{Disposition, Error, Result};
pub use plan::{MaintenancePlan, RunReport};
pub use policy::{Config, Policy, TableRef};
