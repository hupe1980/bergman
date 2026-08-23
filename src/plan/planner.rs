//! Turning a policy plus a health report into a list of operations.
//!
//! Every decision here is a comparison between something measured and
//! something configured, and every operation records both. There is no
//! heuristic that cannot be printed.

use chrono::{DateTime, Utc};

use crate::health::TableHealth;
use crate::plan::{Estimate, Executability, Operation, OperationKind, TablePlan};
use crate::policy::{EffectivePolicy, OrphanMode};
use crate::util::{human_bytes, human_duration};

/// Why compaction cannot execute today.
///
/// Compaction commits a snapshot that *removes* data files and adds
/// replacements. Upstream `iceberg-rust` 0.10 has no action that removes files:
/// `Transaction` offers append, expire-snapshots and the metadata updates, and
/// both `TransactionAction` and `TableCommit`'s builder are `pub(crate)`, so no
/// external crate can supply one. Bergman plans compaction, reports it, and
/// declines to execute it rather than pretending otherwise.
const COMPACTION_BLOCKED: &str = "compaction needs a commit that removes data files; \
     iceberg-rust 0.10 has no such transaction action and its commit API is \
     crate-private (apache/iceberg-rust#2186). Planned and reported only.";

/// Why manifest rewriting cannot execute today.
const MANIFEST_REWRITE_BLOCKED: &str = "rewriting manifests needs a commit that replaces \
     the snapshot's manifest set; iceberg-rust 0.10 has no such transaction action \
     (apache/iceberg-rust#1237 was closed unmerged). Planned and reported only.";

/// Plan one table.
///
/// Returns `None` when nothing should happen — an empty table, or one already
/// healthy under its policy. The caller distinguishes the two for reporting.
pub fn plan_table(
    health: &TableHealth,
    policy: &EffectivePolicy,
    now: DateTime<Utc>,
) -> Option<TablePlan> {
    if health.is_empty() {
        return None;
    }

    let mut operations = Vec::new();

    if let Some(op) = plan_compaction(health, policy) {
        operations.push(op);
    }
    if let Some(op) = plan_manifest_rewrite(health, policy) {
        operations.push(op);
    }
    if let Some(op) = plan_expiration(health, policy, now) {
        operations.push(op);
    }
    if let Some(op) = plan_orphan_removal(health, policy) {
        operations.push(op);
    }

    if operations.is_empty() {
        return None;
    }

    // The order the kinds are declared in is the order they must run in; see
    // `OperationKind`.
    operations.sort_by_key(|op| op.kind);

    Some(TablePlan {
        table: health.table.clone(),
        health: health.clone(),
        policy: Box::new(policy.clone()),
        operations,
    })
}

fn plan_compaction(health: &TableHealth, policy: &EffectivePolicy) -> Option<Operation> {
    let settings = &policy.compaction;
    if !settings.enabled.value {
        return None;
    }

    let threshold = settings.small_file_threshold();
    let target = settings.target_file_size.value;

    // Partition-grained, because that is the granularity a rewrite commits at
    // and because a table can look healthy on average while one partition is
    // in a bad state.
    let mut triggered = Vec::new();
    for partition in &health.partitions {
        let small_files = partition.small_file_count(threshold);
        let small_ratio = partition.small_file_ratio(threshold);
        let delete_ratio = partition.delete_ratio();

        let by_small_files = small_ratio >= settings.small_file_ratio.value
            && small_files >= settings.min_input_files.value;
        let by_deletes =
            delete_ratio >= settings.delete_ratio.value && partition.delete_file_count() > 0;

        if !by_small_files && !by_deletes {
            continue;
        }

        // Rewriting N files into N files spends I/O to achieve nothing. Without
        // this check a table just below the target size would be rewritten
        // every cycle, forever.
        let output_files = partition.output_file_estimate(target);
        if output_files >= partition.data_file_count && !by_deletes {
            continue;
        }

        let reason = if by_deletes {
            format!(
                "{} of {} rows deleted ({:.0}% ≥ {:.0}%) across {} delete files",
                partition.delete_record_count,
                partition.record_count,
                delete_ratio * 100.0,
                settings.delete_ratio.value * 100.0,
                partition.delete_file_count(),
            )
        } else {
            format!(
                "{small_files} of {} files below {} ({:.0}% ≥ {:.0}%)",
                partition.data_file_count,
                human_bytes(threshold),
                small_ratio * 100.0,
                settings.small_file_ratio.value * 100.0,
            )
        };

        triggered.push((partition, output_files, reason));
    }

    if triggered.is_empty() {
        return None;
    }

    let input_files: usize = triggered.iter().map(|(p, _, _)| p.data_file_count).sum();
    let input_bytes: u64 = triggered.iter().map(|(p, _, _)| p.data_bytes).sum();
    let output_files: usize = triggered.iter().map(|(_, out, _)| *out).sum();

    let reason = if triggered.len() == 1 {
        let (partition, _, why) = &triggered[0];
        format!("partition {}: {why}", partition.key)
    } else {
        let (partition, _, why) = &triggered[0];
        format!(
            "{} partitions; e.g. {}: {why}",
            triggered.len(),
            partition.key
        )
    };

    Some(Operation {
        kind: OperationKind::Compact,
        reason,
        estimate: Estimate {
            input_files,
            input_bytes,
            output_files,
            snapshots_removed: 0,
        },
        executability: Executability::blocked(COMPACTION_BLOCKED),
    })
}

fn plan_manifest_rewrite(health: &TableHealth, policy: &EffectivePolicy) -> Option<Operation> {
    let settings = &policy.manifests;
    if !settings.rewrite.value {
        return None;
    }

    let undersized = health.manifests.undersized_count;
    if undersized < settings.min_count_to_merge.value {
        return None;
    }

    let target = settings.target_size.value;
    let output_files = health.manifests.bytes.div_ceil(target).max(1) as usize;

    Some(Operation {
        kind: OperationKind::RewriteManifests,
        reason: format!(
            "{undersized} of {} manifests below {} (≥ {} to merge)",
            health.manifests.count,
            human_bytes(target),
            settings.min_count_to_merge.value,
        ),
        estimate: Estimate {
            input_files: health.manifests.count,
            input_bytes: health.manifests.bytes,
            output_files,
            snapshots_removed: 0,
        },
        executability: Executability::blocked(MANIFEST_REWRITE_BLOCKED),
    })
}

fn plan_expiration(
    health: &TableHealth,
    policy: &EffectivePolicy,
    _now: DateTime<Utc>,
) -> Option<Operation> {
    let settings = &policy.snapshots;
    if !settings.enabled.value {
        return None;
    }

    // Bergman does not compute which snapshots expire — upstream's
    // `ExpireSnapshotsAction` does, following Java's `RemoveSnapshots`
    // including per-branch ancestry and per-ref retention. Reimplementing that
    // selection here to produce a prettier estimate would create a second
    // implementation of the subtlest rule in the system, and the two would
    // drift. So the trigger is the coarse, honest one: are there more
    // snapshots than the floor, and is the oldest older than the cutoff?
    let expirable = health
        .snapshots
        .count
        .saturating_sub(settings.min_to_keep.value);
    if expirable == 0 {
        return None;
    }

    let oldest = health.snapshots.oldest_age?;
    if oldest <= settings.max_age.value {
        return None;
    }

    Some(Operation {
        kind: OperationKind::ExpireSnapshots,
        reason: format!(
            "oldest snapshot is {} old (> {}), {} snapshots retained (keeping at least {})",
            human_duration(oldest),
            human_duration(settings.max_age.value),
            health.snapshots.count,
            settings.min_to_keep.value,
        ),
        estimate: Estimate {
            input_files: 0,
            input_bytes: 0,
            output_files: 0,
            // An upper bound: upstream decides the exact set at commit time,
            // and a shared ancestor reachable from a retained branch survives.
            snapshots_removed: expirable,
        },
        executability: Executability::Executable,
    })
}

fn plan_orphan_removal(health: &TableHealth, policy: &EffectivePolicy) -> Option<Operation> {
    let settings = &policy.orphans;
    if !settings.enabled.value {
        return None;
    }

    // The scanner has to list object storage to know whether there are any
    // orphans, and listing is the expensive part of the operation — so the plan
    // cannot estimate what it will find. It says what it will *do*, which is
    // the honest thing a plan can promise here.
    let action = match settings.mode.value {
        OrphanMode::DryRun => "report",
        OrphanMode::Delete => "delete",
    };

    Some(Operation {
        kind: OperationKind::RemoveOrphans,
        reason: format!(
            "scan {} and {action} files older than {} that no retained snapshot references",
            health.location,
            human_duration(settings.older_than.value),
        ),
        estimate: Estimate::default(),
        executability: Executability::Executable,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::health::{
        FileHealth, ManifestHealth, PartitionHealth, PartitionKey, SnapshotHealth,
    };
    use crate::policy::{Config, Decision, Policy, TableRef};

    fn effective(toml: &str) -> EffectivePolicy {
        let config = Config::from_toml(toml).unwrap();
        let policy = Policy::compile(&config).unwrap();
        match policy.decide(&TableRef::new("prod", ["db"], "t"), &HashMap::new()) {
            Decision::Maintain(e) => *e,
            other => panic!("expected Maintain, got {other:?}"),
        }
    }

    fn health_with(partitions: Vec<PartitionHealth>, snapshots: SnapshotHealth) -> TableHealth {
        let mut files = FileHealth::default();
        for p in &partitions {
            files.data_file_count += p.data_file_count;
            files.data_bytes += p.data_bytes;
            files.record_count += p.record_count;
            files.delete_record_count += p.delete_record_count;
            files.file_sizes.extend(p.file_sizes.iter().copied());
        }
        files.file_sizes.sort_unstable();

        TableHealth {
            table: TableRef::new("prod", ["db"], "t"),
            format_version: 2,
            location: "s3://bucket/wh/db/t".into(),
            snapshots,
            manifests: ManifestHealth::default(),
            files,
            partitions,
        }
    }

    fn partition(
        name: &str,
        sizes: &[u64],
        records: u64,
        deletes: u64,
        delete_files: usize,
    ) -> PartitionHealth {
        let mut p = PartitionHealth::new(PartitionKey {
            spec_id: 0,
            value: name.into(),
        });
        p.data_file_count = sizes.len();
        p.data_bytes = sizes.iter().sum();
        p.record_count = records;
        p.delete_record_count = deletes;
        p.equality_delete_count = delete_files;
        p.file_sizes = sizes.to_vec();
        p.file_sizes.sort_unstable();
        p
    }

    fn snapshots(count: usize, oldest: Duration) -> SnapshotHealth {
        SnapshotHealth {
            count,
            current_snapshot_id: Some(1),
            oldest_age: Some(oldest),
            has_main_branch: true,
            metadata_log_count: 0,
        }
    }

    const COMPACT_ON: &str = r#"
        [[rules]]
        match = "prod.db.t"
        [rules.compaction]
        enabled = true
        target_file_size = 1000
    "#;

    #[test]
    fn healthy_table_gets_no_plan() {
        // Four files already at target: nothing to do, and the whole point of
        // trigger-based maintenance is that this costs no data I/O.
        let health = health_with(
            vec![partition("d=1", &[1000, 1000, 1000, 1000], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        assert!(plan_table(&health, &effective(COMPACT_ON), Utc::now()).is_none());
    }

    #[test]
    fn small_files_trigger_compaction_with_a_legible_reason() {
        let health = health_with(
            vec![partition("d=1", &[10, 10, 10, 10, 10, 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        let plan = plan_table(&health, &effective(COMPACT_ON), Utc::now()).unwrap();
        let op = &plan.operations[0];

        assert_eq!(op.kind, OperationKind::Compact);
        assert!(
            op.reason.contains("6 of 6 files below"),
            "got: {}",
            op.reason
        );
        assert_eq!(op.estimate.input_files, 6);
        assert_eq!(op.estimate.output_files, 1);
    }

    #[test]
    fn too_few_small_files_does_not_trigger() {
        // `min_input_files` defaults to 5, matching Spark. Four tiny files are
        // not worth a commit.
        let health = health_with(
            vec![partition("d=1", &[10, 10, 10, 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        assert!(plan_table(&health, &effective(COMPACT_ON), Utc::now()).is_none());
    }

    #[test]
    fn deletes_trigger_compaction_even_when_file_sizes_are_fine() {
        // The case the whole delete-aware design exists for: a streaming target
        // whose files are the right size but whose reads are amplified by
        // delete files.
        let health = health_with(
            vec![partition("d=1", &[1000, 1000, 1000], 1000, 500, 12)],
            snapshots(1, Duration::from_secs(60)),
        );
        let plan = plan_table(&health, &effective(COMPACT_ON), Utc::now()).unwrap();
        let op = &plan.operations[0];

        assert_eq!(op.kind, OperationKind::Compact);
        assert!(op.reason.contains("rows deleted"), "got: {}", op.reason);
        assert!(op.reason.contains("12 delete files"), "got: {}", op.reason);
    }

    #[test]
    fn compaction_is_planned_but_reported_as_blocked() {
        // The honesty requirement: the plan states the table's real need and is
        // unambiguous that Bergman will not act on it yet.
        let health = health_with(
            vec![partition("d=1", &[10; 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        let plan = plan_table(&health, &effective(COMPACT_ON), Utc::now()).unwrap();

        assert_eq!(plan.executable().count(), 0);
        assert_eq!(plan.blocked().count(), 1);
        match &plan.operations[0].executability {
            Executability::Blocked { reason } => {
                assert!(reason.contains("iceberg-rust"), "got: {reason}")
            }
            other => panic!("expected Blocked, got {other:?}"),
        }
    }

    #[test]
    fn expiration_triggers_on_age_and_is_executable() {
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.snapshots]
            max_age = "7d"
            min_to_keep = 2
            "#,
        );
        let health = health_with(
            vec![partition("d=1", &[1000], 100, 0, 0)],
            snapshots(10, Duration::from_secs(30 * 86400)),
        );
        let plan = plan_table(&health, &policy, Utc::now()).unwrap();
        let op = &plan.operations[0];

        assert_eq!(op.kind, OperationKind::ExpireSnapshots);
        assert!(op.is_executable());
        assert_eq!(op.estimate.snapshots_removed, 8);
        assert!(op.reason.contains("30d old"), "got: {}", op.reason);
    }

    #[test]
    fn expiration_respects_the_retention_floor() {
        // Two snapshots and a floor of two: nothing may go, however old.
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.snapshots]
            max_age = "1s"
            min_to_keep = 2
            "#,
        );
        let health = health_with(
            vec![partition("d=1", &[1000], 100, 0, 0)],
            snapshots(2, Duration::from_secs(30 * 86400)),
        );
        assert!(plan_table(&health, &policy, Utc::now()).is_none());
    }

    #[test]
    fn operations_are_ordered_compact_then_expire_then_orphans() {
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.compaction]
            enabled = true
            target_file_size = 1000
            [rules.snapshots]
            max_age = "7d"
            min_to_keep = 1
            [rules.orphans]
            enabled = true
            "#,
        );
        let health = health_with(
            vec![partition("d=1", &[10; 10], 100, 0, 0)],
            snapshots(10, Duration::from_secs(30 * 86400)),
        );
        let plan = plan_table(&health, &policy, Utc::now()).unwrap();

        let kinds: Vec<_> = plan.operations.iter().map(|op| op.kind).collect();
        assert_eq!(
            kinds,
            vec![
                OperationKind::Compact,
                OperationKind::ExpireSnapshots,
                OperationKind::RemoveOrphans,
            ]
        );
    }
}
