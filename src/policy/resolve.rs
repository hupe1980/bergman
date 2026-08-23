//! Layered resolution, and the record of which layer answered.
//!
//! Resolution walks four layers: the matching rule, the config defaults, the
//! table's own Iceberg properties, and finally the specification default. The
//! layer that answered is kept alongside the value, because the question an
//! operator actually asks is never "what is the target file size" — it is "why
//! is it *that*".

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::policy::settings::{CompactionTrigger, OrphanMode, TableSettings};

/// Iceberg specification defaults, named so the constants are greppable
/// against the spec rather than appearing as bare numbers.
mod spec_defaults {
    use std::time::Duration;

    /// `write.target-file-size-bytes`
    pub const TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;
    /// `history.expire.max-snapshot-age-ms`
    pub const MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(5 * 24 * 60 * 60);
    /// `history.expire.min-snapshots-to-keep`
    pub const MIN_SNAPSHOTS_TO_KEEP: usize = 1;
    /// `commit.manifest.target-size-bytes`
    pub const MANIFEST_TARGET_SIZE: u64 = 8 * 1024 * 1024;
    /// `commit.manifest.min-count-to-merge`
    pub const MANIFEST_MIN_COUNT_TO_MERGE: usize = 100;
}

/// Bergman's own defaults, for knobs Iceberg does not define.
///
/// These are triggers and budgets rather than table semantics, so there is no
/// property to inherit from — but they are still stated in one place rather
/// than scattered through the planners.
mod bergman_defaults {
    use std::time::Duration;

    pub const SMALL_FILE_RATIO: f64 = 0.3;
    /// Matches Spark's `rewrite_data_files`, so operators who already run
    /// Iceberg maintenance are not surprised.
    pub const MIN_INPUT_FILES: usize = 5;
    pub const DELETE_RATIO: f64 = 0.1;
    /// A file smaller than this fraction of the target counts as small.
    pub const MIN_FILE_SIZE_RATIO: f64 = 0.75;
    /// An hour of quiet before a partition is considered settled.
    ///
    /// Long enough that a streaming writer committing every few seconds is
    /// never fought over its hot partition, short enough that a batch load
    /// landing hourly is still compacted the same day.
    pub const MIN_FILE_AGE: Duration = Duration::from_secs(3600);
    /// The executor's memory pool. A gibibyte covers the overwhelming majority
    /// of real rewrites without spilling, and the ones that exceed it spill
    /// rather than fail.
    pub const MAX_SORT_MEMORY: u64 = 1024 * 1024 * 1024;
    /// Spark's `max-file-group-size-bytes` default is 100 GB, sized for a
    /// cluster. Bergman runs in one process, so it takes the value that same
    /// documentation recommends for memory-constrained environments.
    pub const MAX_GROUP_BYTES: u64 = 8 * 1024 * 1024 * 1024;
    /// Enough files that a group is worth a commit, few enough that reading one
    /// is a bounded number of open handles.
    pub const MAX_INPUT_FILES: usize = 10_000;
    pub const ORPHAN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
    /// Listing a whole table location is the expensive part of orphan removal
    /// and it finds nothing almost every time, so daily is the cadence rather
    /// than every cycle.
    pub const ORPHAN_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
}

/// One column of a resolved sort, with the direction and null placement the
/// output is written in.
///
/// Richer than the `sort = ["a", "b"]` a rule writes, because the other source
/// of a sort is the table's own `sort-order`, which carries both — and writing
/// the rows back in the opposite direction would leave metadata claiming a
/// clustering the files do not have.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortColumn {
    /// The column name, as the table's schema spells it.
    pub name: String,
    /// Whether to sort descending.
    pub descending: bool,
    /// Whether nulls sort before non-nulls.
    pub nulls_first: bool,
}

impl SortColumn {
    /// A column sorted ascending with nulls first — what a bare name in a rule
    /// means, and Iceberg's own default for an ascending field.
    pub fn ascending(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            descending: false,
            nulls_first: true,
        }
    }
}

impl std::fmt::Display for SortColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)?;
        if self.descending {
            f.write_str(" desc")?;
        }
        if self.nulls_first != !self.descending {
            f.write_str(if self.nulls_first {
                " nulls-first"
            } else {
                " nulls-last"
            })?;
        }
        Ok(())
    }
}

/// What layered resolution reads from the table itself.
///
/// Layer 3 of the resolution is "the table's own opinion": its properties, and
/// its **sort order** — an opinion about physical layout that a rewrite can
/// destroy. Both live here rather than being read at execution time, where
/// `bergman policy explain` could not see them.
#[derive(Debug, Default, Clone)]
pub struct TableFacts {
    /// The table's Iceberg properties.
    pub properties: HashMap<String, String>,
    /// The columns the table's own `sort-order` names, in order.
    ///
    /// Empty for an unsorted table, which is the common case.
    pub sort_order: Vec<SortColumn>,
    /// Sort fields the table declares that Bergman cannot reproduce.
    ///
    /// Surfaced as a warning rather than dropped silently: a table sorted by
    /// `bucket(id, 16)` that gets rewritten sorted by nothing has lost a layout
    /// property its owner configured, and the only way anyone finds out is if
    /// somebody says so.
    pub unsupported_sort_fields: Vec<String>,
}

impl TableFacts {
    /// Nothing known about the table.
    ///
    /// The first pass of planning uses this: deciding whether *any* rule matches
    /// a table needs no metadata at all, which is what keeps a policy scoped to
    /// one namespace cheap against a catalog holding thousands of tables.
    pub fn unknown() -> Self {
        Self::default()
    }

    /// Read what resolution needs out of a table's metadata.
    ///
    /// Only sort fields Bergman can actually reproduce are kept, and only the
    /// leading run of them:
    ///
    /// - **Identity transforms only.** A field sorted by `bucket(id, 16)` orders
    ///   rows by a value that is not in the file, and sorting by `id` instead
    ///   would be a different clustering wearing the same name.
    /// - **Top-level columns only.** The executor addresses sort columns by
    ///   name, and a nested field's name is not unique in a flat namespace.
    /// - **A prefix, not a subset.** Sort order is lexicographic: dropping the
    ///   *second* of three fields and keeping the third produces an ordering
    ///   that is not the table's, whereas keeping only what precedes the first
    ///   gap produces a coarser version of exactly the table's own.
    pub fn from_metadata(metadata: &iceberg::spec::TableMetadata) -> Self {
        use iceberg::spec::{NullOrder, SortDirection, Transform};

        let schema = metadata.current_schema();
        let order = metadata.default_sort_order();

        let mut sort_order = Vec::with_capacity(order.fields.len());
        let mut unsupported = Vec::new();

        for field in &order.fields {
            // `field_by_id` finds nested fields too, so top-level membership is
            // checked separately rather than inferred from a successful lookup.
            let name = schema
                .as_struct()
                .fields()
                .iter()
                .find(|f| f.id == field.source_id)
                .map(|f| f.name.clone());

            match (name, &field.transform) {
                (Some(name), Transform::Identity) => sort_order.push(SortColumn {
                    name,
                    descending: field.direction == SortDirection::Descending,
                    nulls_first: field.null_order == NullOrder::First,
                }),
                (name, transform) => {
                    unsupported.push(format!(
                        "{}({})",
                        transform,
                        name.unwrap_or_else(|| format!("field {}", field.source_id))
                    ));
                    // Everything after a gap is dropped with it: see above.
                    break;
                }
            }
        }

        Self {
            properties: metadata.properties().clone(),
            sort_order,
            unsupported_sort_fields: unsupported,
        }
    }
}

/// Which layer supplied a value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "layer", rename_all = "kebab-case")]
pub enum Provenance {
    /// The matching rule said so.
    Rule {
        /// The rule's pattern.
        pattern: String,
    },
    /// The configuration's `[defaults]` said so.
    Defaults,
    /// The table's own Iceberg property said so.
    TableProperty {
        /// The property name, e.g. `write.target-file-size-bytes`.
        key: String,
    },
    /// The table's own `sort-order` said so.
    ///
    /// A layer of its own rather than a property, because it is not one: it is
    /// a first-class part of the table's metadata, and calling it a property in
    /// the output would send an operator looking for a key that does not exist.
    TableSortOrder,
    /// Nobody said, so the Iceberg specification default applies.
    IcebergDefault,
    /// Nobody said, and Iceberg does not define this knob, so Bergman's
    /// default applies.
    BergmanDefault,
}

impl std::fmt::Display for Provenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Provenance::Rule { pattern } => write!(f, "rule \"{pattern}\""),
            Provenance::Defaults => write!(f, "[defaults]"),
            Provenance::TableProperty { key } => write!(f, "table property {key}"),
            Provenance::TableSortOrder => f.write_str("the table's sort order"),
            Provenance::IcebergDefault => write!(f, "Iceberg default"),
            Provenance::BergmanDefault => write!(f, "Bergman default"),
        }
    }
}

/// A resolved value and where it came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resolved<T> {
    /// The value in force.
    pub value: T,
    /// Which layer supplied it.
    pub from: Provenance,
}

impl<T> Resolved<T> {
    fn new(value: T, from: Provenance) -> Self {
        Self { value, from }
    }
}

/// The context a single resolution runs in.
struct Layers<'a> {
    rule: &'a TableSettings,
    pattern: &'a str,
    defaults: &'a TableSettings,
    facts: &'a TableFacts,
}

impl Layers<'_> {
    fn rule_provenance(&self) -> Provenance {
        Provenance::Rule {
            pattern: self.pattern.to_string(),
        }
    }

    /// Resolve one setting through all four layers.
    ///
    /// `pick` selects the field from a settings layer; `property` names the
    /// Iceberg property to consult and how to parse it; `fallback` is what
    /// applies when nobody said.
    fn resolve<T>(
        &self,
        pick: impl Fn(&TableSettings) -> Option<T>,
        property: Option<PropertySource<'_, T>>,
        fallback: T,
        fallback_from: Provenance,
    ) -> Resolved<T> {
        if let Some(v) = pick(self.rule) {
            return Resolved::new(v, self.rule_provenance());
        }
        if let Some(v) = pick(self.defaults) {
            return Resolved::new(v, Provenance::Defaults);
        }
        if let Some((key, parse)) = property
            && let Some(raw) = self.facts.properties.get(key)
            && let Some(v) = parse(raw)
        {
            // A property that is present but unparseable falls through to the
            // default rather than failing the run. It is the table owner's
            // typo, it affects one table, and refusing to maintain a table
            // because of a malformed property is a worse outcome than
            // maintaining it under the documented default. The health report
            // surfaces it (see `EffectivePolicy::warnings`).
            return Resolved::new(v, Provenance::TableProperty { key: key.into() });
        }
        Resolved::new(fallback, fallback_from)
    }
}

/// How an Iceberg table property is turned into a setting's type.
///
/// Named because it appears in `Layers::resolve`'s signature, where the inline
/// form is genuinely hard to read.
type PropertyParser<'a, T> = &'a dyn Fn(&str) -> Option<T>;

/// A table property Bergman consults: its name, and how to parse it.
type PropertySource<'a, T> = (&'a str, PropertyParser<'a, T>);

fn parse_u64(s: &str) -> Option<u64> {
    s.trim().parse().ok()
}

fn parse_usize(s: &str) -> Option<usize> {
    s.trim().parse().ok()
}

fn parse_millis(s: &str) -> Option<Duration> {
    s.trim().parse::<u64>().ok().map(Duration::from_millis)
}

/// Everything policy says about one table, with each value's origin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectivePolicy {
    /// The pattern of the rule that matched.
    pub matched_rule: String,
    /// Compaction settings.
    pub compaction: EffectiveCompaction,
    /// Snapshot expiration settings.
    pub snapshots: EffectiveSnapshots,
    /// Manifest rewrite settings.
    pub manifests: EffectiveManifests,
    /// Orphan removal settings.
    pub orphans: EffectiveOrphans,
    /// The cron expression governing evaluation, if any.
    pub schedule: Option<Resolved<String>>,
    /// Table properties that were present but could not be parsed.
    ///
    /// Reported rather than fatal: the value is one table's typo, and the
    /// documented default is a better outcome than refusing to maintain it.
    pub warnings: Vec<String>,
}

/// Resolved compaction settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveCompaction {
    /// Whether compaction may run.
    pub enabled: Resolved<bool>,
    /// Output file size.
    pub target_file_size: Resolved<u64>,
    /// Fraction of small files that triggers a rewrite.
    pub small_file_ratio: Resolved<f64>,
    /// Fewest files worth rewriting.
    pub min_input_files: Resolved<usize>,
    /// Delete-record fraction that triggers a rewrite.
    pub delete_ratio: Resolved<f64>,
    /// What counts as small, as a fraction of the target.
    pub min_file_size_ratio: Resolved<f64>,
    /// How long a partition must be quiet before it is rewritten.
    pub min_file_age: Resolved<Duration>,
    /// Sort columns, if the rule, the defaults, or the table itself asks for
    /// clustering.
    pub sort: Option<Resolved<Vec<SortColumn>>>,
    /// Memory pool for one file group's rewrite. The sort and the anti-join
    /// spill when they reach it.
    pub max_sort_memory: Resolved<u64>,
    /// Byte ceiling on one file group.
    pub max_group_bytes: Resolved<u64>,
    /// File-count ceiling on one file group.
    pub max_input_files: Resolved<usize>,
}

impl EffectiveCompaction {
    /// The size below which a file counts as small.
    pub fn small_file_threshold(&self) -> u64 {
        (self.target_file_size.value as f64 * self.min_file_size_ratio.value) as u64
    }
}

/// Resolved snapshot expiration settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveSnapshots {
    /// Whether expiration may run.
    pub enabled: Resolved<bool>,
    /// Age beyond which snapshots expire.
    pub max_age: Resolved<Duration>,
    /// Snapshots kept per branch regardless of age.
    pub min_to_keep: Resolved<usize>,
    /// Whether expiration deletes the files it orphans.
    pub delete_files: Resolved<bool>,
}

/// Resolved manifest rewrite settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveManifests {
    /// Whether rewriting may run.
    pub rewrite: Resolved<bool>,
    /// Size manifests are coalesced toward.
    pub target_size: Resolved<u64>,
    /// Fewest undersized manifests worth merging.
    pub min_count_to_merge: Resolved<usize>,
}

/// Resolved orphan removal settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveOrphans {
    /// Whether the scanner runs.
    pub enabled: Resolved<bool>,
    /// Report or delete.
    pub mode: Resolved<OrphanMode>,
    /// Minimum age for a file to be considered.
    pub older_than: Resolved<Duration>,
    /// Shortest gap between two scans of one table.
    pub min_interval: Resolved<Duration>,
}

impl EffectivePolicy {
    /// Resolve a table's effective policy.
    pub(crate) fn resolve(
        rule: &TableSettings,
        pattern: &str,
        defaults: &TableSettings,
        facts: &TableFacts,
    ) -> Self {
        let l = Layers {
            rule,
            pattern,
            defaults,
            facts,
        };

        let compaction = EffectiveCompaction {
            enabled: l.resolve(
                |s| s.compaction.as_ref().and_then(|c| c.enabled),
                None,
                // Compaction defaults off. It is the one operation that rewrites
                // data, and turning it on for every table a rule happens to
                // match is not a default anybody should get by accident.
                false,
                Provenance::BergmanDefault,
            ),
            target_file_size: l.resolve(
                |s| s.compaction.as_ref().and_then(|c| c.target_file_size),
                Some(("write.target-file-size-bytes", &parse_u64)),
                spec_defaults::TARGET_FILE_SIZE,
                Provenance::IcebergDefault,
            ),
            small_file_ratio: l.resolve(
                |s| trigger(s).and_then(|t| t.small_file_ratio),
                None,
                bergman_defaults::SMALL_FILE_RATIO,
                Provenance::BergmanDefault,
            ),
            min_input_files: l.resolve(
                |s| trigger(s).and_then(|t| t.min_input_files),
                None,
                bergman_defaults::MIN_INPUT_FILES,
                Provenance::BergmanDefault,
            ),
            delete_ratio: l.resolve(
                |s| trigger(s).and_then(|t| t.delete_ratio),
                None,
                bergman_defaults::DELETE_RATIO,
                Provenance::BergmanDefault,
            ),
            min_file_size_ratio: l.resolve(
                |s| trigger(s).and_then(|t| t.min_file_size_ratio),
                None,
                bergman_defaults::MIN_FILE_SIZE_RATIO,
                Provenance::BergmanDefault,
            ),
            min_file_age: l.resolve(
                |s| trigger(s).and_then(|t| t.min_file_age),
                None,
                bergman_defaults::MIN_FILE_AGE,
                Provenance::BergmanDefault,
            ),
            // A table that declares a `sort-order` has already said how its
            // rows should be laid out, and its writers honour it. Bin-packing
            // those files back together *unsorted* would leave the table
            // claiming a clustering its files no longer have, and every query
            // with a predicate on the sort columns would start reading every
            // file. Preserving it is not an optimization; it is not breaking
            // something the table configured.
            sort: resolve_optional(&l, |s| {
                s.compaction.as_ref().and_then(|c| {
                    c.sort
                        .as_ref()
                        .map(|names| names.iter().map(SortColumn::ascending).collect())
                })
            })
            .or_else(|| {
                (!l.facts.sort_order.is_empty())
                    .then(|| Resolved::new(l.facts.sort_order.clone(), Provenance::TableSortOrder))
            }),
            max_sort_memory: l.resolve(
                |s| s.compaction.as_ref().and_then(|c| c.max_sort_memory),
                None,
                bergman_defaults::MAX_SORT_MEMORY,
                Provenance::BergmanDefault,
            ),
            max_group_bytes: l.resolve(
                |s| s.compaction.as_ref().and_then(|c| c.max_group_bytes),
                None,
                bergman_defaults::MAX_GROUP_BYTES,
                Provenance::BergmanDefault,
            ),
            max_input_files: l.resolve(
                |s| s.compaction.as_ref().and_then(|c| c.max_input_files),
                None,
                bergman_defaults::MAX_INPUT_FILES,
                Provenance::BergmanDefault,
            ),
        };

        let snapshots = EffectiveSnapshots {
            enabled: l.resolve(
                |s| s.snapshots.as_ref().and_then(|c| c.enabled),
                None,
                // Expiration defaults on: it is metadata-only unless
                // `delete_files` is also set, and unbounded snapshot growth is
                // the most common Iceberg health problem.
                true,
                Provenance::BergmanDefault,
            ),
            max_age: l.resolve(
                |s| s.snapshots.as_ref().and_then(|c| c.max_age),
                Some(("history.expire.max-snapshot-age-ms", &parse_millis)),
                spec_defaults::MAX_SNAPSHOT_AGE,
                Provenance::IcebergDefault,
            ),
            min_to_keep: l.resolve(
                |s| s.snapshots.as_ref().and_then(|c| c.min_to_keep),
                Some(("history.expire.min-snapshots-to-keep", &parse_usize)),
                spec_defaults::MIN_SNAPSHOTS_TO_KEEP,
                Provenance::IcebergDefault,
            ),
            delete_files: l.resolve(
                |s| s.snapshots.as_ref().and_then(|c| c.delete_files),
                None,
                false,
                Provenance::BergmanDefault,
            ),
        };

        let manifests = EffectiveManifests {
            rewrite: l.resolve(
                |s| s.manifests.as_ref().and_then(|c| c.rewrite),
                None,
                false,
                Provenance::BergmanDefault,
            ),
            target_size: l.resolve(
                |s| s.manifests.as_ref().and_then(|c| c.target_size),
                Some(("commit.manifest.target-size-bytes", &parse_u64)),
                spec_defaults::MANIFEST_TARGET_SIZE,
                Provenance::IcebergDefault,
            ),
            min_count_to_merge: l.resolve(
                |s| s.manifests.as_ref().and_then(|c| c.min_count_to_merge),
                Some(("commit.manifest.min-count-to-merge", &parse_usize)),
                spec_defaults::MANIFEST_MIN_COUNT_TO_MERGE,
                Provenance::IcebergDefault,
            ),
        };

        let orphans = EffectiveOrphans {
            enabled: l.resolve(
                |s| s.orphans.as_ref().and_then(|c| c.enabled),
                None,
                false,
                Provenance::BergmanDefault,
            ),
            mode: l.resolve(
                |s| s.orphans.as_ref().and_then(|c| c.mode),
                None,
                OrphanMode::DryRun,
                Provenance::BergmanDefault,
            ),
            older_than: l.resolve(
                |s| s.orphans.as_ref().and_then(|c| c.older_than),
                None,
                bergman_defaults::ORPHAN_AGE,
                Provenance::BergmanDefault,
            ),
            min_interval: l.resolve(
                |s| s.orphans.as_ref().and_then(|c| c.min_interval),
                None,
                bergman_defaults::ORPHAN_INTERVAL,
                Provenance::BergmanDefault,
            ),
        };

        let schedule = resolve_optional(&l, |s| s.schedule.clone());

        Self {
            matched_rule: pattern.to_string(),
            compaction,
            snapshots,
            manifests,
            orphans,
            schedule,
            warnings: warnings(facts),
        }
    }
}

/// Everything about this table worth telling the operator but not worth
/// failing over.
fn warnings(facts: &TableFacts) -> Vec<String> {
    let mut warnings = unparseable_properties(&facts.properties);
    if !facts.unsupported_sort_fields.is_empty() {
        warnings.push(format!(
            "the table's sort order includes {}, which Bergman cannot reproduce in a \
             rewrite; output is sorted by the fields before it, or not at all",
            facts.unsupported_sort_fields.join(", ")
        ));
    }
    warnings
}

fn trigger(s: &TableSettings) -> Option<&CompactionTrigger> {
    s.compaction.as_ref().and_then(|c| c.trigger.as_ref())
}

/// Resolve a setting that has no default: absent everywhere means absent.
fn resolve_optional<T>(
    l: &Layers<'_>,
    pick: impl Fn(&TableSettings) -> Option<T>,
) -> Option<Resolved<T>> {
    if let Some(v) = pick(l.rule) {
        return Some(Resolved::new(v, l.rule_provenance()));
    }
    pick(l.defaults).map(|v| Resolved::new(v, Provenance::Defaults))
}

/// Iceberg properties Bergman consults, and how each is parsed. Present but
/// unparseable values are reported rather than silently ignored — a table
/// property that does nothing looks exactly like one that works.
fn unparseable_properties(properties: &HashMap<String, String>) -> Vec<String> {
    /// A property name and a check for whether its value parses.
    type Check = (&'static str, fn(&str) -> bool);

    const CHECKED: &[Check] = &[
        ("write.target-file-size-bytes", |s| parse_u64(s).is_some()),
        ("history.expire.max-snapshot-age-ms", |s| {
            parse_millis(s).is_some()
        }),
        ("history.expire.min-snapshots-to-keep", |s| {
            parse_usize(s).is_some()
        }),
        ("commit.manifest.target-size-bytes", |s| {
            parse_u64(s).is_some()
        }),
        ("commit.manifest.min-count-to-merge", |s| {
            parse_usize(s).is_some()
        }),
    ];

    let mut warnings = Vec::new();
    for (key, ok) in CHECKED {
        if let Some(raw) = properties.get(*key)
            && !ok(raw)
        {
            warnings.push(format!(
                "table property {key} = {raw:?} could not be parsed; using the default instead"
            ));
        }
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{Config, Decision, Policy, TableRef};

    fn decide(toml: &str, props: &[(&str, &str)]) -> EffectivePolicy {
        let config = Config::from_toml(toml).unwrap();
        let policy = Policy::compile(&config).unwrap();
        let properties: HashMap<String, String> = props
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let facts = TableFacts {
            properties,
            ..Default::default()
        };
        match policy.decide(&TableRef::new("prod", ["db"], "t"), &facts) {
            Decision::Maintain(e) => *e,
            other => panic!("expected Maintain, got {other:?}"),
        }
    }

    const MATCH_ALL: &str = "[[rules]]\nmatch = \"prod.db.t\"\n";

    #[test]
    fn rule_beats_defaults_beats_property_beats_spec_default() {
        // The whole layering, exercised one layer at a time on the same knob.
        let spec_only = decide(MATCH_ALL, &[]);
        assert_eq!(
            spec_only.compaction.target_file_size.value,
            spec_defaults::TARGET_FILE_SIZE
        );
        assert_eq!(
            spec_only.compaction.target_file_size.from,
            Provenance::IcebergDefault
        );

        let with_property = decide(MATCH_ALL, &[("write.target-file-size-bytes", "1024")]);
        assert_eq!(with_property.compaction.target_file_size.value, 1024);
        assert_eq!(
            with_property.compaction.target_file_size.from,
            Provenance::TableProperty {
                key: "write.target-file-size-bytes".into()
            }
        );

        let with_defaults = decide(
            "[defaults.compaction]\ntarget_file_size = 2048\n\n[[rules]]\nmatch = \"prod.db.t\"\n",
            &[("write.target-file-size-bytes", "1024")],
        );
        assert_eq!(with_defaults.compaction.target_file_size.value, 2048);
        assert_eq!(
            with_defaults.compaction.target_file_size.from,
            Provenance::Defaults
        );

        let with_rule = decide(
            "[defaults.compaction]\ntarget_file_size = 2048\n\n\
             [[rules]]\nmatch = \"prod.db.t\"\n[rules.compaction]\ntarget_file_size = 4096\n",
            &[("write.target-file-size-bytes", "1024")],
        );
        assert_eq!(with_rule.compaction.target_file_size.value, 4096);
        assert_eq!(
            with_rule.compaction.target_file_size.from,
            Provenance::Rule {
                pattern: "prod.db.t".into()
            }
        );
    }

    #[test]
    fn table_property_governs_snapshot_age_when_policy_is_silent() {
        // The layer that makes Bergman a participant rather than a competing
        // source of truth: the table already carries the operator's intent.
        let eff = decide(
            MATCH_ALL,
            &[("history.expire.max-snapshot-age-ms", "86400000")],
        );
        assert_eq!(eff.snapshots.max_age.value, Duration::from_secs(86400));
        assert_eq!(
            eff.snapshots.max_age.from,
            Provenance::TableProperty {
                key: "history.expire.max-snapshot-age-ms".into()
            }
        );
    }

    #[test]
    fn unparseable_property_falls_back_and_warns() {
        // One table's typo must not stop that table being maintained — but it
        // must not be invisible either.
        let eff = decide(MATCH_ALL, &[("write.target-file-size-bytes", "big")]);
        assert_eq!(
            eff.compaction.target_file_size.value,
            spec_defaults::TARGET_FILE_SIZE
        );
        assert_eq!(
            eff.compaction.target_file_size.from,
            Provenance::IcebergDefault
        );
        assert_eq!(eff.warnings.len(), 1);
        assert!(eff.warnings[0].contains("write.target-file-size-bytes"));
    }

    #[test]
    fn destructive_operations_default_off() {
        // Compaction rewrites data; orphan removal deletes it; expiring files
        // deletes it. None may arrive by accident from a rule that merely
        // matched.
        let eff = decide(MATCH_ALL, &[]);
        assert!(!eff.compaction.enabled.value);
        assert!(!eff.orphans.enabled.value);
        assert!(!eff.snapshots.delete_files.value);
        assert_eq!(eff.orphans.mode.value, OrphanMode::DryRun);

        // Metadata-only expiration is the exception, and is on: unbounded
        // snapshot growth is the most common Iceberg health problem.
        assert!(eff.snapshots.enabled.value);
    }

    #[test]
    fn small_file_threshold_is_a_fraction_of_the_target() {
        let eff = decide(MATCH_ALL, &[("write.target-file-size-bytes", "1000")]);
        assert_eq!(eff.compaction.small_file_threshold(), 750);
    }

    /// A table sorted by the given `(field id, name, transform, direction)`
    /// fields, for the sort-order layer's tests.
    fn table_sorted_by(
        fields: Vec<(
            i32,
            &str,
            iceberg::spec::Transform,
            iceberg::spec::SortDirection,
        )>,
    ) -> iceberg::spec::TableMetadata {
        use iceberg::spec::{
            NestedField, NullOrder, PrimitiveType, Schema, SortField, SortOrder,
            TableMetadataBuilder, Type, UnboundPartitionSpec,
        };

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "event_date", Type::Primitive(PrimitiveType::Date)).into(),
                NestedField::optional(2, "customer_id", Type::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::required(3, "region", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let order = SortOrder::builder()
            .with_order_id(1)
            .with_fields(
                fields
                    .into_iter()
                    .map(|(source_id, _, transform, direction)| SortField {
                        source_id,
                        transform,
                        direction,
                        null_order: match direction {
                            iceberg::spec::SortDirection::Ascending => NullOrder::First,
                            iceberg::spec::SortDirection::Descending => NullOrder::Last,
                        },
                    })
                    .collect::<Vec<_>>(),
            )
            .build(&schema)
            .unwrap();

        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            order,
            "file:///tmp/t".to_string(),
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn a_tables_own_sort_order_becomes_the_compaction_sort() {
        // The layer that stops a rewrite from destroying something the table
        // configured. A table declaring `sort-order` has writers that honour it;
        // a compaction that bin-packed those files back together unsorted would
        // leave the table claiming a clustering its files no longer have, and
        // every query with a predicate on the sort columns would start reading
        // every file.
        use iceberg::spec::{SortDirection, Transform};

        let metadata = table_sorted_by(vec![
            (
                1,
                "event_date",
                Transform::Identity,
                SortDirection::Ascending,
            ),
            (
                2,
                "customer_id",
                Transform::Identity,
                SortDirection::Descending,
            ),
        ]);
        let facts = TableFacts::from_metadata(&metadata);

        let config = Config::from_toml(MATCH_ALL).unwrap();
        let policy = Policy::compile(&config).unwrap();
        let Decision::Maintain(eff) = policy.decide(&TableRef::new("prod", ["db"], "t"), &facts)
        else {
            panic!("expected the table to be maintained");
        };

        let sort = eff.compaction.sort.expect("the table asked to be sorted");
        assert_eq!(sort.from, Provenance::TableSortOrder);
        assert_eq!(
            sort.value,
            vec![
                SortColumn {
                    name: "event_date".into(),
                    descending: false,
                    nulls_first: true,
                },
                SortColumn {
                    name: "customer_id".into(),
                    descending: true,
                    // Direction and null placement are reproduced exactly:
                    // writing the rows back the other way would be a different
                    // clustering wearing the table's name.
                    nulls_first: false,
                },
            ]
        );
    }

    #[test]
    fn a_rule_overrides_the_tables_sort_order() {
        // Layering, unchanged: a rule is more specific than the table.
        use iceberg::spec::{SortDirection, Transform};

        let metadata = table_sorted_by(vec![(
            1,
            "event_date",
            Transform::Identity,
            SortDirection::Ascending,
        )]);
        let facts = TableFacts::from_metadata(&metadata);

        let config = Config::from_toml(
            "[[rules]]\nmatch = \"prod.db.t\"\n[rules.compaction]\nsort = [\"region\"]\n",
        )
        .unwrap();
        let policy = Policy::compile(&config).unwrap();
        let Decision::Maintain(eff) = policy.decide(&TableRef::new("prod", ["db"], "t"), &facts)
        else {
            panic!("expected the table to be maintained");
        };

        let sort = eff.compaction.sort.unwrap();
        assert_eq!(
            sort.from,
            Provenance::Rule {
                pattern: "prod.db.t".into()
            }
        );
        assert_eq!(sort.value, vec![SortColumn::ascending("region")]);
    }

    #[test]
    fn an_unsorted_table_is_not_given_a_sort() {
        // Sorting a table nobody asked to have sorted would spend a full sort
        // per file group for nothing.
        let facts = TableFacts::unknown();
        let config = Config::from_toml(MATCH_ALL).unwrap();
        let policy = Policy::compile(&config).unwrap();
        let Decision::Maintain(eff) = policy.decide(&TableRef::new("prod", ["db"], "t"), &facts)
        else {
            panic!("expected the table to be maintained");
        };
        assert!(eff.compaction.sort.is_none());
    }

    #[test]
    fn a_sort_field_bergman_cannot_reproduce_truncates_the_order_and_warns() {
        // Sort order is lexicographic, so a field Bergman cannot express does
        // not just drop out — everything after it becomes meaningless too.
        // Keeping the prefix produces a coarser version of exactly the table's
        // own ordering; keeping the suffix would produce a different one.
        use iceberg::spec::{SortDirection, Transform};

        let metadata = table_sorted_by(vec![
            (
                1,
                "event_date",
                Transform::Identity,
                SortDirection::Ascending,
            ),
            (
                2,
                "customer_id",
                Transform::Bucket(16),
                SortDirection::Ascending,
            ),
            (3, "region", Transform::Identity, SortDirection::Ascending),
        ]);
        let facts = TableFacts::from_metadata(&metadata);

        assert_eq!(facts.sort_order, vec![SortColumn::ascending("event_date")]);
        assert_eq!(facts.unsupported_sort_fields.len(), 1);
        assert!(
            facts.unsupported_sort_fields[0].contains("customer_id"),
            "{:?}",
            facts.unsupported_sort_fields
        );

        // And the operator hears about it rather than silently getting a
        // coarser layout than the table asked for.
        let config = Config::from_toml(MATCH_ALL).unwrap();
        let policy = Policy::compile(&config).unwrap();
        let Decision::Maintain(eff) = policy.decide(&TableRef::new("prod", ["db"], "t"), &facts)
        else {
            panic!("expected the table to be maintained");
        };
        assert!(
            eff.warnings.iter().any(|w| w.contains("sort order")),
            "{:?}",
            eff.warnings
        );
    }

    #[test]
    fn provenance_renders_for_humans() {
        assert_eq!(
            Provenance::Rule {
                pattern: "prod.*".into()
            }
            .to_string(),
            "rule \"prod.*\""
        );
        assert_eq!(
            Provenance::TableProperty {
                key: "write.target-file-size-bytes".into()
            }
            .to_string(),
            "table property write.target-file-size-bytes"
        );
    }
}
