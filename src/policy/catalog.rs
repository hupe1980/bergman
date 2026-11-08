//! Maintenance policy a catalog owns, read as one more resolution layer.
//!
//! Apache Polaris persists four maintenance policy types —
//! `system.data-compaction`, `system.metadata-compaction`,
//! `system.snapshot-expiry` and `system.orphan-file-removal` — at catalog,
//! namespace and table scope, and its `applicable-policies` endpoint returns
//! the ones in force for a table with inheritance already applied. This module
//! turns that answer into [`TableSettings`], the same type a `bergman.toml` rule
//! produces, so a catalog policy resolves through the same machinery and
//! `bergman policy explain` names it like any other layer.
//!
//! **The layer sits below `bergman.toml`.** A matching rule and `[defaults]`
//! win; the catalog fills only what they leave unset; the table's own
//! properties come after. And a catalog policy never makes Bergman maintain a
//! table no rule matches — it fills in a decision, it does not take one.
//!
//! Every policy type is `enable` plus an open `config` map whose schema says
//! interoperability "is not guaranteed". Bergman reads the keys Polaris's own
//! documentation uses, and **reports every other key** rather than dropping it,
//! so a policy that does nothing never looks like one that works.

use std::time::Duration;

use serde::Deserialize;

use super::resolve::Provenance;
use super::settings::{
    CompactionSettings, ManifestSettings, OrphanSettings, SnapshotSettings, TableSettings,
};

/// One policy as `GET /polaris/v1/{prefix}/applicable-policies` returns it.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ApplicablePolicy {
    /// `system.data-compaction`, …, or a custom type Bergman ignores.
    pub policy_type: String,
    /// The policy's name.
    pub name: String,
    /// Whether it was inherited from a parent rather than attached here.
    #[serde(default)]
    pub inherited: bool,
    /// Where it is attached: the namespace path, empty for the catalog.
    #[serde(default)]
    pub namespace: Vec<String>,
    /// The policy content: a JSON document, as a string.
    #[serde(default)]
    pub content: Option<String>,
}

/// One catalog policy's contribution: the settings it supplies, and how
/// `policy explain` names it.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogLayer {
    /// Only the section this policy's type governs is populated.
    pub settings: TableSettings,
    /// Which policy said so.
    pub from: Provenance,
}

/// What a catalog's applicable policies amount to for one table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CatalogPolicy {
    /// One entry per maintenance policy in force, in resolution order.
    pub layers: Vec<CatalogLayer>,
    /// What was present and not applied, with the reason — reported in the
    /// plan's warnings.
    pub notes: Vec<String>,
}

/// The four types Bergman understands.
const TYPES: [&str; 4] = [
    "system.data-compaction",
    "system.metadata-compaction",
    "system.snapshot-expiry",
    "system.orphan-file-removal",
];

/// Map a table's applicable Polaris policies into resolution layers.
pub fn from_polaris(policies: &[ApplicablePolicy]) -> CatalogPolicy {
    let mut out = CatalogPolicy::default();

    for policy_type in TYPES {
        let matching: Vec<&ApplicablePolicy> = policies
            .iter()
            .filter(|p| p.policy_type == policy_type)
            .collect();
        match matching.as_slice() {
            [] => {}
            [policy] => match layer(policy) {
                Ok((layer, mut notes)) => {
                    out.layers.push(layer);
                    out.notes.append(&mut notes);
                }
                Err(reason) => out.notes.push(format!(
                    "catalog policy \"{}\" ({policy_type}) was not applied: {reason}",
                    policy.name
                )),
            },
            // Polaris allows one inheritable policy of a type per entity and
            // resolves the hierarchy itself, so two in one answer is not a
            // shape the protocol produces. Picking one would be a guess about
            // which the catalog meant.
            many => out.notes.push(format!(
                "{} catalog policies of type {policy_type} apply ({}); none was applied, \
                 because choosing between them would be a guess",
                many.len(),
                many.iter()
                    .map(|p| format!("\"{}\"", p.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
    out
}

/// The content every maintenance type shares.
#[derive(Debug, Deserialize)]
struct Content {
    #[serde(default)]
    version: Option<String>,
    enable: bool,
    #[serde(default)]
    config: serde_json::Map<String, serde_json::Value>,
    /// Orphan removal only.
    #[serde(default)]
    max_orphan_file_age_in_days: Option<f64>,
    /// Orphan removal only.
    #[serde(default)]
    locations: Option<Vec<String>>,
}

/// The only content version the four schemas define.
const VERSION: &str = "2025-02-03";

fn layer(policy: &ApplicablePolicy) -> Result<(CatalogLayer, Vec<String>), String> {
    let raw = policy.content.as_deref().ok_or("it has no content")?;
    let content: Content =
        serde_json::from_str(raw).map_err(|e| format!("its content does not parse: {e}"))?;
    if let Some(version) = &content.version
        && version != VERSION
    {
        return Err(format!(
            "its content is schema version {version}, and Bergman reads {VERSION}"
        ));
    }

    let mut notes = Vec::new();
    let mut config = content.config.clone();
    let mut take = |key: &str| config.remove(key);
    let label = format!("catalog policy \"{}\"", policy.name);

    let mut settings = TableSettings::default();
    match policy.policy_type.as_str() {
        "system.data-compaction" => {
            let target_file_size = take("target_file_size_bytes")
                .map(|v| positive_u64(&v).ok_or(format!("target_file_size_bytes = {v}")))
                .transpose()?;
            settings.compaction = Some(CompactionSettings {
                enabled: Some(content.enable),
                target_file_size,
                ..Default::default()
            });
        }
        "system.metadata-compaction" => {
            settings.manifests = Some(ManifestSettings {
                rewrite: Some(content.enable),
                ..Default::default()
            });
        }
        "system.snapshot-expiry" => {
            let max_age = take("max_snapshot_age_days")
                .map(|v| days(&v).ok_or(format!("max_snapshot_age_days = {v}")))
                .transpose()?;
            let min_to_keep = take("min_snapshot_to_keep")
                .map(|v| {
                    positive_u64(&v)
                        .map(|n| n as usize)
                        .ok_or(format!("min_snapshot_to_keep = {v}"))
                })
                .transpose()?;
            settings.snapshots = Some(SnapshotSettings {
                enabled: Some(content.enable),
                max_age,
                min_to_keep,
                ..Default::default()
            });
        }
        "system.orphan-file-removal" => {
            let older_than = content
                .max_orphan_file_age_in_days
                .map(|d| {
                    (d.is_finite() && d >= 0.0)
                        .then(|| Duration::from_secs_f64(d * 86_400.0))
                        .ok_or(format!("max_orphan_file_age_in_days = {d}"))
                })
                .transpose()?;
            // Never honoured. A scan of a location other than the table's own
            // deletes every file in it that this table does not name —
            // including other tables' — and the schema's own description warns
            // of exactly that.
            if let Some(locations) = &content.locations
                && !locations.is_empty()
            {
                notes.push(format!(
                    "{label} names scan locations ({}); Bergman scans only the table's own \
                     location and ignores them",
                    locations.join(", ")
                ));
            }
            // `enable` turns the scan on; it never turns deletion on. Orphan
            // removal's mode stays whatever `bergman.toml` says, dry-run by
            // default, because a catalog policy is not where the decision to
            // delete files from a bucket should arrive from.
            settings.orphans = Some(OrphanSettings {
                enabled: Some(content.enable),
                older_than,
                ..Default::default()
            });
        }
        other => return Err(format!("{other} is not a maintenance policy type")),
    }

    for key in config.keys() {
        notes.push(format!(
            "{label} sets config.{key}, which Bergman does not read; it has no effect"
        ));
    }

    Ok((
        CatalogLayer {
            settings,
            from: Provenance::CatalogPolicy {
                name: policy.name.clone(),
                policy_type: policy.policy_type.clone(),
                inherited: policy.inherited,
                attached_at: policy.namespace.join("."),
            },
        },
        notes,
    ))
}

fn positive_u64(v: &serde_json::Value) -> Option<u64> {
    let n = match v {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }?;
    (n > 0).then_some(n)
}

fn days(v: &serde_json::Value) -> Option<Duration> {
    let d = match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }?;
    (d.is_finite() && d >= 0.0).then(|| Duration::from_secs_f64(d * 86_400.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(policy_type: &str, content: &str) -> ApplicablePolicy {
        ApplicablePolicy {
            policy_type: policy_type.into(),
            name: format!("{policy_type}-policy"),
            inherited: true,
            namespace: vec!["finance".into()],
            content: Some(content.into()),
        }
    }

    #[test]
    fn each_maintenance_type_supplies_its_own_section() {
        let mapped = from_polaris(&[
            policy(
                "system.data-compaction",
                r#"{"version":"2025-02-03","enable":true,"config":{"target_file_size_bytes":134217728}}"#,
            ),
            policy("system.metadata-compaction", r#"{"enable":false}"#),
            policy(
                "system.snapshot-expiry",
                r#"{"enable":true,"config":{"max_snapshot_age_days":2,"min_snapshot_to_keep":3}}"#,
            ),
            policy(
                "system.orphan-file-removal",
                r#"{"enable":true,"max_orphan_file_age_in_days":5}"#,
            ),
        ]);
        assert!(mapped.notes.is_empty(), "{:?}", mapped.notes);
        let s: Vec<&TableSettings> = mapped.layers.iter().map(|l| &l.settings).collect();

        let c = s[0].compaction.as_ref().unwrap();
        assert_eq!(
            (c.enabled, c.target_file_size),
            (Some(true), Some(134_217_728))
        );
        assert_eq!(s[1].manifests.as_ref().unwrap().rewrite, Some(false));
        let snap = s[2].snapshots.as_ref().unwrap();
        assert_eq!(snap.max_age, Some(Duration::from_secs(2 * 86_400)));
        assert_eq!(snap.min_to_keep, Some(3));
        let o = s[3].orphans.as_ref().unwrap();
        assert_eq!(o.older_than, Some(Duration::from_secs(5 * 86_400)));
        assert_eq!(
            o.mode, None,
            "a catalog policy never sets the deletion mode"
        );
        // Each layer carries only its own section, so resolution cannot read
        // one policy's silence as another's answer.
        assert!(s[0].snapshots.is_none() && s[3].compaction.is_none());
    }

    #[test]
    fn a_key_bergman_does_not_read_is_reported_not_dropped() {
        let mapped = from_polaris(&[policy(
            "system.data-compaction",
            r#"{"enable":true,"config":{"target_file_size_bytes":1,"zorder":"a,b"}}"#,
        )]);
        assert_eq!(mapped.layers.len(), 1, "the readable part still applies");
        assert!(
            mapped.notes.iter().any(|n| n.contains("config.zorder")),
            "{:?}",
            mapped.notes
        );
    }

    #[test]
    fn scan_locations_are_never_honoured() {
        let mapped = from_polaris(&[policy(
            "system.orphan-file-removal",
            r#"{"enable":true,"locations":["s3://bucket"]}"#,
        )]);
        assert!(
            mapped.notes.iter().any(|n| n.contains("s3://bucket")),
            "{:?}",
            mapped.notes
        );
    }

    #[test]
    fn unusable_content_is_reported_and_supplies_nothing() {
        for content in [
            r#"not json"#,
            r#"{"version":"2099-01-01","enable":true}"#,
            r#"{"enable":true,"config":{"target_file_size_bytes":0}}"#,
        ] {
            let mapped = from_polaris(&[policy("system.data-compaction", content)]);
            assert!(mapped.layers.is_empty(), "{content}");
            assert_eq!(mapped.notes.len(), 1, "{content}: {:?}", mapped.notes);
        }
    }

    #[test]
    fn two_policies_of_one_type_are_not_chosen_between() {
        let mapped = from_polaris(&[
            policy("system.data-compaction", r#"{"enable":true}"#),
            policy("system.data-compaction", r#"{"enable":false}"#),
        ]);
        assert!(mapped.layers.is_empty());
        assert_eq!(mapped.notes.len(), 1);
    }

    #[test]
    fn other_policy_types_are_not_maintenance_and_are_ignored() {
        let mapped = from_polaris(&[policy("custom.access", r#"{"enable":true}"#)]);
        assert_eq!(mapped, CatalogPolicy::default());
    }
}
