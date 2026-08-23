//! The command-line interface.
//!
//! Everything the library deliberately does not do lives here: installing a
//! tracing subscriber, finding a config file, handling signals, and rendering
//! for a terminal. The library is driven by this module through exactly the
//! public API an embedder gets, which is how that API stays sufficient.

mod render;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};

use crate::error::{Error, Result};
use crate::obs::{AuditObserver, JsonlSink, Observers};
use crate::policy::{Config, Decision, TableRef};
use crate::{Bergman, MaintenancePlan};

/// A Rust-native maintenance engine for Apache Iceberg.
#[derive(Debug, Parser)]
#[command(name = "bergman", version, about, long_about = None)]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(
        short,
        long,
        global = true,
        env = "BERGMAN_CONFIG",
        default_value = "bergman.toml"
    )]
    config: PathBuf,

    /// Output format.
    #[arg(long, global = true, value_enum, default_value_t = Format::Text)]
    format: Format,

    /// Append an audit record for every operation to this file.
    #[arg(long, global = true, env = "BERGMAN_AUDIT_LOG")]
    audit_log: Option<PathBuf>,

    /// Log level (`error`, `warn`, `info`, `debug`, `trace`).
    #[arg(long, global = true, env = "BERGMAN_LOG", default_value = "warn")]
    log: String,

    #[command(subcommand)]
    command: Command,
}

/// How output is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// Tables and prose, for a terminal.
    Text,
    /// JSON, for a pipeline.
    Json,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Report table health. Reads only, changes nothing.
    ///
    /// The command to run first: it answers "what is wrong with my tables"
    /// before Bergman is trusted to write anything.
    Inspect {
        /// Only inspect tables matching this glob.
        #[arg(long)]
        table: Option<String>,
    },

    /// Show what maintenance would do. Reads only, changes nothing.
    Plan,

    /// Execute maintenance.
    Run {
        /// Build the plan and print it without executing.
        ///
        /// Identical to `bergman plan`, and accepted here because reaching for
        /// `--dry-run` on the command that changes things is the safer habit.
        #[arg(long)]
        dry_run: bool,
    },

    /// Policy inspection.
    #[command(subcommand)]
    Policy(PolicyCommand),
}

#[derive(Debug, Subcommand)]
enum PolicyCommand {
    /// Validate the configuration without contacting anything.
    ///
    /// Runs offline, so it works in CI on a machine holding no credentials.
    Lint,

    /// Show the effective policy for a table, and where each value came from.
    Explain {
        /// The table, as `catalog.namespace.table`.
        table: String,
    },

    /// List which tables each rule matches.
    Match,
}

/// Run the CLI.
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    let config = Config::from_path(&cli.config)?;

    // Linting must not require a catalog: it is meant for CI.
    if let Command::Policy(PolicyCommand::Lint) = cli.command {
        return lint(&config, cli.format);
    }

    let mut observers = Observers::new();
    if let Some(path) = &cli.audit_log {
        let run_id = uuid::Uuid::new_v4().to_string();
        observers = observers.with(Arc::new(AuditObserver::new(JsonlSink::open(path)?, run_id)));
    }

    let bergman = Bergman::builder(config)
        .with_observer(Arc::new(observers))
        .build()
        .await?;

    match cli.command {
        Command::Inspect { table } => {
            let mut health = bergman.inspect().await?;
            if let Some(pattern) = table {
                let matcher = crate::policy::TableMatcher::new(&pattern)
                    .map_err(|e| Error::config(format!("--table {pattern:?}: {e}")))?;
                health.retain(|h| matcher.matches(&h.table));
            }
            render::inspect(&health, cli.format)
        }

        Command::Plan => {
            let plan = bergman.plan().await?;
            render::plan(&plan, cli.format)
        }

        Command::Run { dry_run } => {
            let plan = bergman.plan().await?;
            if dry_run {
                return render::plan(&plan, cli.format);
            }
            let report = bergman.run(&plan).await?;
            render::report(&report, cli.format)?;

            // A failure inside a run is not a failure of the run — other
            // tables were maintained. But it must reach a scheduler that only
            // reads exit codes, or a broken cron job looks healthy forever.
            if report.needs_attention() {
                std::process::exit(2);
            }
            Ok(())
        }

        Command::Policy(PolicyCommand::Explain { table }) => {
            let table = parse_table_ref(&table)?;
            let decision = bergman.explain(&table).await?;
            render::explain(&table, &decision, cli.format)
        }

        Command::Policy(PolicyCommand::Match) => {
            let health = bergman.inspect().await?;
            let matches: Vec<(TableRef, Decision)> = health
                .into_iter()
                .map(|h| {
                    let decision = bergman.policy().decide(&h.table, &Default::default());
                    (h.table, decision)
                })
                .collect();
            render::matches(&matches, cli.format)
        }

        Command::Policy(PolicyCommand::Lint) => unreachable!("handled above"),
    }
}

fn lint(config: &Config, format: Format) -> Result<()> {
    for catalog in &config.catalogs {
        catalog.validate()?;
    }
    let policy = crate::policy::Policy::compile(config)?;

    let patterns: Vec<&str> = policy.patterns().collect();
    match format {
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "catalogs": config.catalogs.len(),
                "rules": patterns.len(),
            })
        ),
        Format::Text => println!(
            "ok: {}, {}",
            plural(config.catalogs.len(), "catalog"),
            plural(patterns.len(), "rule")
        ),
    }
    Ok(())
}

/// `1 catalog`, `2 catalogs`.
fn plural(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("{n} {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// Parse `catalog.namespace…​.table`.
fn parse_table_ref(input: &str) -> Result<TableRef> {
    let parts: Vec<&str> = input.split('.').collect();
    if parts.len() < 3 {
        return Err(Error::config(format!(
            "{input:?} is not a table reference; expected catalog.namespace.table"
        )));
    }
    Ok(TableRef::new(
        parts[0],
        parts[1..parts.len() - 1].to_vec(),
        parts[parts.len() - 1],
    ))
}

fn init_tracing(level: &str) {
    use tracing_subscriber::EnvFilter;

    // Logs go to stderr so that `--format json` on stdout stays machine-
    // readable however noisy the log level is.
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// A plan's exit code, for scripts.
pub fn plan_exit_code(plan: &MaintenancePlan) -> i32 {
    if plan.is_empty() { 0 } else { 1 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_references_split_into_catalog_namespace_and_name() {
        let t = parse_table_ref("prod.analytics.web.events").unwrap();
        assert_eq!(t.catalog, "prod");
        assert_eq!(t.namespace, vec!["analytics", "web"]);
        assert_eq!(t.name, "events");
    }

    #[test]
    fn a_reference_without_a_namespace_is_refused() {
        // `prod.events` is ambiguous: Bergman cannot tell a catalog-and-table
        // from a namespace-and-table, and guessing would send the request to
        // the wrong place.
        assert!(parse_table_ref("prod.events").is_err());
        assert!(parse_table_ref("events").is_err());
    }

    #[test]
    fn cli_parses_the_documented_invocations() {
        // The arguments the README promises. A parser change that broke one
        // would otherwise only be found by a user.
        assert!(Cli::try_parse_from(["bergman", "inspect"]).is_ok());
        assert!(Cli::try_parse_from(["bergman", "plan", "--format", "json"]).is_ok());
        assert!(Cli::try_parse_from(["bergman", "run", "--dry-run"]).is_ok());
        assert!(Cli::try_parse_from(["bergman", "policy", "lint"]).is_ok());
        assert!(Cli::try_parse_from(["bergman", "policy", "explain", "prod.db.t"]).is_ok());
    }

    #[test]
    fn counts_read_as_english() {
        assert_eq!(plural(1, "catalog"), "1 catalog");
        assert_eq!(plural(0, "rule"), "0 rules");
        assert_eq!(plural(3, "rule"), "3 rules");
    }

    #[test]
    fn unknown_subcommands_are_refused() {
        assert!(Cli::try_parse_from(["bergman", "compact"]).is_err());
    }
}
