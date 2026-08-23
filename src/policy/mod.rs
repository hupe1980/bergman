//! Policy: what maintenance a table should get, and where each value came from.
//!
//! A policy declares *intent*. It does not declare a schedule of rewrites — the
//! health analyzer decides whether anything actually runs (see [`crate::health`]),
//! so a healthy table costs one metadata read and no data I/O.
//!
//! # Layering
//!
//! Every setting resolves through four layers, most specific first:
//!
//! 1. the matching **rule**
//! 2. the config's **`[defaults]`**
//! 3. the **table's own Iceberg properties** (`write.target-file-size-bytes`,
//!    `history.expire.max-snapshot-age-ms`, …)
//! 4. the **Iceberg specification default**
//!
//! Layer 3 is the one that makes this more than a config file. A table already
//! carries the operator's intent in its properties, and every other Iceberg
//! tool reads them; a maintenance engine that ignored them would be a second,
//! competing source of truth. So absent an explicit policy value, the table
//! governs.
//!
//! Resolution records *which* layer answered ([`Provenance`]), which is what
//! `bergman policy explain` prints. A setting whose origin cannot be shown is a
//! setting nobody can debug.

mod matcher;
mod resolve;
mod schedule;
mod settings;
mod window;

pub use matcher::TableMatcher;
pub use resolve::{
    EffectiveCompaction, EffectiveManifests, EffectiveOrphans, EffectivePolicy, EffectiveSnapshots,
    Provenance, Resolved,
};
pub use schedule::parse as parse_schedule;
pub use settings::{
    CompactionSettings, CompactionTrigger, ManifestSettings, OrphanMode, OrphanSettings,
    SnapshotSettings, TableSettings,
};
pub use window::{MaintenanceWindow, next_open};

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub use settings::TableSettings as Settings;

use crate::error::{Error, Result};

/// The smallest age Bergman will ever consider a file orphaned.
///
/// In-flight writers stage data files *before* the commit that references them,
/// so an unreferenced young file is far more likely to be a live write than
/// garbage. Deleting one corrupts a table that was doing nothing wrong.
///
/// This floor is not configurable. It is checked when configuration is
/// validated *and* again in the scanner itself, because the library API lets an
/// embedder build [`OrphanSettings`] directly and a safety rule enforced in only
/// one of two entry points is a safety rule with a hole in it.
pub const MIN_ORPHAN_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// A complete Bergman configuration.
///
/// This is the parsed form of `bergman.toml`. It is also an ordinary Rust value
/// an embedder can construct directly — nothing here reads a file or an
/// environment variable.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The catalogs to discover tables from.
    #[serde(default)]
    pub catalogs: Vec<crate::catalog::CatalogConfig>,

    /// Settings applied to every table that does not override them.
    #[serde(default)]
    pub defaults: TableSettings,

    /// Rules, evaluated in order. The first match wins.
    #[serde(default)]
    pub rules: Vec<Rule>,

    /// Global budgets and concurrency ceilings.
    #[serde(default)]
    pub limits: Limits,
}

/// One rule: a table pattern plus the settings that apply to what it matches.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// A glob over `catalog.namespace.table`, e.g. `prod.analytics.*`.
    ///
    /// Namespaces nest, so `prod.analytics.*` matches `prod.analytics.events`
    /// but not `prod.analytics.web.events`; use `prod.analytics.**` for the
    /// whole subtree. This is the ordinary glob distinction and it is worth
    /// stating because Iceberg namespaces are dotted and the two read alike.
    #[serde(rename = "match")]
    pub pattern: String,

    /// Exclude matching tables from maintenance entirely.
    ///
    /// An explicit exclusion beats an implicit one: a table nobody wrote a rule
    /// for is reported as unmatched, while a table matched by `skip` is
    /// reported as deliberately skipped.
    #[serde(default)]
    pub skip: bool,

    /// Settings for the tables this rule matches.
    #[serde(flatten)]
    pub settings: TableSettings,
}

/// Global ceilings. These bound cost, which is the failure mode naive
/// compaction has: rewriting data you did not need to rewrite can cost more in
/// write amplification than it ever returns in scan savings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// How many tables may be maintained concurrently.
    #[serde(default = "Limits::default_max_parallel_tables")]
    pub max_parallel_tables: usize,

    /// Ceiling on bytes rewritten in one cycle, across all tables.
    ///
    /// When the budget cannot cover everything, tables are ordered
    /// most-fragmented-first and the remainder is reported as deferred — never
    /// silently dropped.
    #[serde(default)]
    pub max_rewrite_bytes_per_run: Option<u64>,

    /// Only start work inside this window, e.g. `22:00-06:00 Europe/Berlin`.
    ///
    /// The timezone is mandatory: a window in local time moves when a replica
    /// is scheduled in another region, and "not during business hours" must not
    /// move. Parsed at startup, so a malformed one is a startup failure.
    #[serde(default)]
    pub maintenance_window: Option<String>,
}

impl Limits {
    fn default_max_parallel_tables() -> usize {
        4
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_parallel_tables: Self::default_max_parallel_tables(),
            max_rewrite_bytes_per_run: None,
            maintenance_window: None,
        }
    }
}

/// A fully-qualified table name: `catalog.namespace…​.table`.
///
/// Kept as its own type rather than a `String` because it is the join key
/// between policy, health, plans and audit records, and because rendering it
/// consistently is what makes a rule pattern and an audit line comparable by
/// eye.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableRef {
    /// The catalog the table lives in, as named in configuration.
    pub catalog: String,
    /// The namespace parts, outermost first.
    pub namespace: Vec<String>,
    /// The table name.
    pub name: String,
}

impl TableRef {
    /// Build a reference from its parts.
    pub fn new(
        catalog: impl Into<String>,
        namespace: impl IntoIterator<Item = impl Into<String>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            namespace: namespace.into_iter().map(Into::into).collect(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.catalog)?;
        for part in &self.namespace {
            write!(f, ".{part}")?;
        }
        write!(f, ".{}", self.name)
    }
}

/// A compiled policy: rules with their patterns already built into matchers.
///
/// Compiling once and matching many times is the point. A rule set meets every
/// table in a catalog, so re-parsing a glob per table would make policy
/// evaluation scale with `rules × tables` in the expensive term rather than the
/// cheap one.
#[derive(Debug)]
pub struct Policy {
    defaults: TableSettings,
    rules: Vec<CompiledRule>,
    limits: Limits,
    window: Option<MaintenanceWindow>,
}

#[derive(Debug)]
struct CompiledRule {
    matcher: TableMatcher,
    pattern: String,
    skip: bool,
    settings: TableSettings,
}

/// What policy says about one table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "kebab-case")]
pub enum Decision {
    /// No rule matched. The table is not maintained.
    ///
    /// Distinct from [`Decision::Skip`] so an operator can tell "my rule does
    /// not match what I thought it did" from "I excluded this deliberately".
    Unmatched,
    /// A rule matched and excluded the table.
    Skip {
        /// The pattern that excluded it.
        pattern: String,
    },
    /// A rule matched. The table is maintained under these settings.
    Maintain(Box<EffectivePolicy>),
}

impl Policy {
    /// Compile a configuration into a policy.
    ///
    /// Validation happens here, so a bad policy is a startup failure rather
    /// than a surprise on the first table that matches it.
    pub fn compile(config: &Config) -> Result<Self> {
        config.defaults.validate("defaults")?;

        let mut rules = Vec::with_capacity(config.rules.len());
        for (idx, rule) in config.rules.iter().enumerate() {
            let where_ = format!("rules[{idx}] (match = \"{}\")", rule.pattern);
            rule.settings.validate(&where_)?;

            if rule.skip && !rule.settings.is_empty() {
                return Err(Error::policy(format!(
                    "{where_}: `skip` is set alongside settings, which cannot both apply; \
                     remove one"
                )));
            }

            let matcher = TableMatcher::new(&rule.pattern)
                .map_err(|e| Error::policy(format!("{where_}: {e}")))?;

            rules.push(CompiledRule {
                matcher,
                pattern: rule.pattern.clone(),
                skip: rule.skip,
                settings: rule.settings.clone(),
            });
        }

        let window = config
            .limits
            .maintenance_window
            .as_deref()
            .map(MaintenanceWindow::parse)
            .transpose()?;

        Ok(Self {
            defaults: config.defaults.clone(),
            rules,
            limits: config.limits.clone(),
            window,
        })
    }

    /// The maintenance window, if one is declared.
    pub fn window(&self) -> Option<&MaintenanceWindow> {
        self.window.as_ref()
    }

    /// Whether maintenance may start now.
    ///
    /// A window governs when work *begins*; a cycle already under way runs to
    /// completion. Stopping mid-rewrite at the window's edge would leave files
    /// written and uncommitted, which is worse than finishing.
    pub fn window_is_open(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.window.as_ref().is_none_or(|w| w.contains(now))
    }

    /// Decide what happens to one table.
    ///
    /// `table_properties` are the table's own Iceberg properties, which form
    /// layer 3 of the resolution. Passing them separately (rather than reading
    /// them here) keeps this function pure and testable, and lets the caller
    /// decide what a table with unreadable metadata should get.
    pub fn decide(&self, table: &TableRef, table_properties: &HashMap<String, String>) -> Decision {
        let Some(rule) = self.rules.iter().find(|r| r.matcher.matches(table)) else {
            return Decision::Unmatched;
        };

        if rule.skip {
            return Decision::Skip {
                pattern: rule.pattern.clone(),
            };
        }

        Decision::Maintain(Box::new(EffectivePolicy::resolve(
            &rule.settings,
            &rule.pattern,
            &self.defaults,
            table_properties,
        )))
    }

    /// The cron expressions rules declare, with the pattern that declared each.
    ///
    /// A rule's `schedule` governs when its tables are *evaluated*; whether
    /// anything executes is the health analyzer's decision.
    pub fn schedules(&self) -> impl Iterator<Item = (&str, &str)> {
        self.rules.iter().filter_map(|rule| {
            rule.settings
                .schedule
                .as_deref()
                .map(|schedule| (rule.pattern.as_str(), schedule))
        })
    }

    /// The patterns this policy carries, in evaluation order.
    pub fn patterns(&self) -> impl Iterator<Item = &str> {
        self.rules.iter().map(|r| r.pattern.as_str())
    }

    /// The global limits.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

impl Config {
    /// Parse a configuration from TOML.
    pub fn from_toml(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::config(e.to_string()))
    }

    /// Read and parse a configuration file.
    pub fn from_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::config(format!("{}: {e}", path.display())))?;
        Self::from_toml(&text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> TableRef {
        TableRef::new("prod", ["analytics"], name)
    }

    #[test]
    fn first_matching_rule_wins() {
        let config = Config::from_toml(
            r#"
            [[rules]]
            match = "prod.analytics.events"
            [rules.snapshots]
            max_age = "3d"

            [[rules]]
            match = "prod.analytics.*"
            [rules.snapshots]
            max_age = "30d"
            "#,
        )
        .unwrap();
        let policy = Policy::compile(&config).unwrap();

        let Decision::Maintain(eff) = policy.decide(&table("events"), &HashMap::new()) else {
            panic!("expected the table to be maintained");
        };
        // The specific rule is listed first, so it answers — the general one
        // never gets a look in. Ordering is the whole disambiguation rule.
        assert_eq!(eff.snapshots.max_age.value, Duration::from_secs(3 * 86400));
    }

    #[test]
    fn unmatched_and_skipped_are_different_answers() {
        let config = Config::from_toml(
            r#"
            [[rules]]
            match = "prod.tmp.*"
            skip = true
            "#,
        )
        .unwrap();
        let policy = Policy::compile(&config).unwrap();

        assert_eq!(
            policy.decide(&TableRef::new("prod", ["tmp"], "scratch"), &HashMap::new()),
            Decision::Skip {
                pattern: "prod.tmp.*".into()
            }
        );
        // A table nobody wrote a rule for is a different situation from one
        // deliberately excluded, and an operator debugging coverage needs to
        // tell them apart.
        assert_eq!(
            policy.decide(&table("events"), &HashMap::new()),
            Decision::Unmatched
        );
    }

    #[test]
    fn skip_alongside_settings_is_rejected() {
        // Both cannot apply. Silently preferring one would make the other a
        // line in a config file that does nothing.
        let config = Config::from_toml(
            r#"
            [[rules]]
            match = "prod.tmp.*"
            skip = true
            [rules.snapshots]
            max_age = "3d"
            "#,
        )
        .unwrap();
        let err = Policy::compile(&config).unwrap_err();
        assert!(err.to_string().contains("`skip` is set alongside settings"));
    }

    #[test]
    fn unknown_keys_are_rejected() {
        // A typo that parses is a setting that silently does nothing.
        let err = Config::from_toml(
            r#"
            [[rules]]
            match = "prod.*"
            [rules.snapshots]
            max_ago = "3d"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_ago"), "got: {err}");
    }

    #[test]
    fn table_ref_renders_nested_namespaces() {
        let t = TableRef::new("prod", ["analytics", "web"], "events");
        assert_eq!(t.to_string(), "prod.analytics.web.events");
    }
}
