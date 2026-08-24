//! Turning a policy plus a health report into a list of operations.
//!
//! Every decision here is a comparison between something measured and
//! something configured, and every operation records both. There is no
//! heuristic that cannot be printed.

use chrono::{DateTime, Utc};

use crate::health::TableHealth;
use crate::plan::{Estimate, Operation, OperationKind, TablePlan};
use crate::policy::{EffectivePolicy, OrphanMode};
use crate::util::{human_bytes, human_duration};

/// What the caller knows about a table beyond its metadata.
///
/// Only orphan removal needs anything here, and only because it is the one
/// operation whose cost cannot be judged from metadata: the only way to know
/// whether a table has orphans is to list its whole location.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlanContext {
    /// When this process last scanned the table for orphans, if it has.
    ///
    /// Deliberately per-process rather than persisted. A one-shot `bergman run`
    /// *is* the schedule — the cron entry that invoked it already decided the
    /// cadence — so it scans. A daemon on a five-minute cycle must not list
    /// every table's whole location every five minutes, and "since this process
    /// started" is exactly the right scope for that: losing it on restart costs
    /// one extra scan, which is harmless, and persisting it would mean state
    /// that has to survive a crash, which is the thing Bergman does not have.
    pub last_orphan_scan: Option<DateTime<Utc>>,
}

/// Plan one table.
///
/// Returns `None` when there is genuinely nothing to say — a table already
/// healthy under its policy, and nothing about its shape that stops the policy
/// applying. A plan with no operations but a note is still returned, because
/// "your compaction rule can never run against this table" is information an
/// operator needs and silence is not.
pub fn plan_table(
    health: &TableHealth,
    policy: &EffectivePolicy,
    context: PlanContext,
    now: DateTime<Utc>,
) -> Option<TablePlan> {
    let mut operations = Vec::new();
    let mut notes = Vec::new();

    // A table with no snapshots has no data to compact, no manifests to
    // re-pack and no snapshots to expire — but it can still hold files: a
    // first write that died between staging its data and committing leaves
    // them under the table location with nothing to reference them, and
    // nothing but the orphan scanner will ever reclaim them.
    if health.is_empty() {
        let op = plan_orphan_removal(health, policy, context, now)?;
        return Some(TablePlan {
            table: health.table.clone(),
            health: health.clone(),
            policy: Box::new(policy.clone()),
            operations: vec![op],
            notes,
        });
    }

    if let Some(op) = plan_compaction(health, policy, now, &mut notes) {
        operations.push(op);
    }
    if let Some(op) = plan_manifest_rewrite(health, policy, &mut notes) {
        operations.push(op);
    }
    if let Some(op) = plan_expiration(health, policy) {
        operations.push(op);
    }
    if let Some(op) = plan_orphan_removal(health, policy, context, now) {
        operations.push(op);
    }

    if operations.is_empty() && notes.is_empty() {
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
        notes,
    })
}

fn plan_compaction(
    health: &TableHealth,
    policy: &EffectivePolicy,
    now: DateTime<Utc>,
    notes: &mut Vec<String>,
) -> Option<Operation> {
    let settings = &policy.compaction;
    if !settings.enabled.value {
        return None;
    }

    // A table whose snapshots Bergman cannot author is one compaction can never
    // run against, however fragmented it is. Saying so once per plan is the
    // difference between an operator seeing "healthy" forever and seeing the
    // reason.
    if let Some(reason) = crate::commit::authoring_refusal(health.format_version) {
        notes.push(format!("compaction is enabled but cannot run: {reason}"));
        return None;
    }

    // Bergman reads and writes only Parquet, so a table asking for anything
    // else would have its format silently changed by a rewrite. The executor
    // refuses it too — and checks the files themselves, which this cannot see;
    // saying so here turns a refusal reported every cycle into an explanation
    // given once.
    if let Some(format) = health.write_format.as_deref()
        && format != "parquet"
    {
        notes.push(format!(
            "compaction is enabled but cannot run: the table's \
             write.format.default is {format:?}, and Bergman writes only Parquet — \
             a rewrite must not silently change a table's format"
        ));
        return None;
    }

    let small_threshold = settings.small_file_threshold();
    let large_threshold = settings.large_file_threshold();
    let target = settings.target_file_size.value;
    let now_ms = now.timestamp_millis();

    // Partition-grained, because that is the granularity a rewrite commits at
    // and because a table can look healthy on average while one partition is
    // in a bad state.
    let mut triggered = Vec::new();
    let mut superseded = 0usize;
    for partition in &health.partitions {
        // A partition still receiving writes will lose the compare-and-swap to
        // the very next micro-batch, spending a full read and write of the data
        // to achieve nothing — repeatedly. Waiting for it to settle is strictly
        // cheaper than competing with the writer for it, and this is the single
        // most common way a naive compactor burns a cycle.
        if !partition.has_settled(settings.min_file_age.value, now_ms) {
            continue;
        }

        let small_files = partition.small_file_count(small_threshold);
        let small_ratio = partition.small_file_ratio(small_threshold);
        let delete_ratio = partition.delete_ratio();

        // Three independent reasons to rewrite, and they select different sets
        // of files. Size triggers read only the files whose size is the
        // problem; the delete trigger reads the whole partition, because a
        // partition-scoped delete can apply to a file of any size and the
        // manifests do not say which.
        let size_eligible = partition.size_eligible(small_threshold, large_threshold);

        let by_small_files = small_ratio >= settings.small_file_ratio.value
            && size_eligible.count >= settings.min_input_files.value;
        // No minimum count: one file that no reader can split is already a
        // problem, and splitting it is unambiguously worth a commit.
        let by_oversized = size_eligible.oversized > 0;
        let by_deletes =
            delete_ratio >= settings.delete_ratio.value && partition.delete_file_count() > 0;

        if !by_small_files && !by_oversized && !by_deletes {
            continue;
        }

        let eligible = if by_deletes {
            partition.all_eligible()
        } else {
            size_eligible
        };

        // Rewriting N files into N files spends I/O to achieve nothing. Without
        // this check a table just below the target size would be rewritten
        // every cycle, forever.
        //
        // Not applied to the other two triggers, and for opposite reasons:
        // splitting an oversized file is *meant* to produce more files than it
        // consumed, and retiring a delete file is worth a commit however the
        // file count lands.
        let output_files = partition.output_file_estimate(eligible, target);
        if !by_deletes && !by_oversized && output_files >= eligible.count {
            continue;
        }

        // Output is written under the table's current spec, so a partition
        // whose files were written under an older one cannot be rewritten: the
        // commit would claim to replace files partitioned differently. The
        // executor refuses these too; not planning them keeps the plan honest
        // about what will happen — and counting them keeps it from being
        // silent about a partition that will never be maintained.
        //
        // Asked *after* the triggers rather than before, because the note this
        // produces is only worth an operator's attention when the partition
        // would otherwise have been rewritten. A spec-evolved table's history
        // is mostly old partitions that are perfectly healthy, and counting
        // those would put a paragraph about mis-filed rows on every plan of
        // every such table, every cycle — which is how a warning that matters
        // stops being read.
        if partition.key.spec_id != health.current_spec_id {
            superseded += 1;
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
        } else if by_small_files {
            format!(
                "{small_files} of {} files below {} ({:.0}% ≥ {:.0}%)",
                partition.data_file_count,
                human_bytes(small_threshold),
                small_ratio * 100.0,
                settings.small_file_ratio.value * 100.0,
            )
        } else {
            format!(
                "{} of {} files above {}, which no reader can split",
                size_eligible.oversized,
                partition.data_file_count,
                human_bytes(large_threshold),
            )
        };

        triggered.push((partition, eligible, output_files, reason));
    }

    if superseded > 0 {
        notes.push(format!(
            "{superseded} partitions need compacting but are under a superseded partition \
             spec (current is {}), so they are never rewritten: output goes out under the \
             current spec, and a commit claiming it replaces files partitioned differently \
             would mis-file every row in it. Migrating them to the current spec is a \
             migration tool's job",
            health.current_spec_id,
        ));
    }

    if triggered.is_empty() {
        return None;
    }

    // The *eligible* files, not the partitions' totals. This figure is what the
    // cycle's byte budget is charged, so charging a partition's untouched
    // two-thirds against it would defer real work to pay for work nobody does.
    let input_files: usize = triggered.iter().map(|(_, e, _, _)| e.count).sum();
    let input_bytes: u64 = triggered.iter().map(|(_, e, _, _)| e.bytes).sum();
    let output_files: usize = triggered.iter().map(|(_, _, out, _)| *out).sum();

    let reason = if triggered.len() == 1 {
        let (partition, _, _, why) = &triggered[0];
        format!("partition {}: {why}", partition.key)
    } else {
        let (partition, _, _, why) = &triggered[0];
        format!(
            "{} partitions; e.g. {}: {why}",
            triggered.len(),
            partition.key
        )
    };

    Some(Operation {
        kind: OperationKind::Compact,
        reason,
        // The exact partitions the triggers fired on. Carried on the operation
        // so `run` rewrites precisely what `plan` displayed, rather than
        // re-deciding against a table that has moved since.
        targets: triggered.iter().map(|(p, _, _, _)| p.key.clone()).collect(),
        estimate: Estimate {
            input_files,
            input_bytes,
            output_files,
            snapshots_removed: 0,
        },
    })
}

fn plan_manifest_rewrite(
    health: &TableHealth,
    policy: &EffectivePolicy,
    notes: &mut Vec<String>,
) -> Option<Operation> {
    let settings = &policy.manifests;
    if !settings.rewrite.value {
        return None;
    }

    // Re-packing entries produces a snapshot like any other, so the same format
    // limit applies — see `plan_compaction`.
    if let Some(reason) = crate::commit::authoring_refusal(health.format_version) {
        notes.push(format!(
            "manifest rewriting is enabled but cannot run: {reason}"
        ));
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
        targets: Vec::new(),
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
    })
}

fn plan_expiration(health: &TableHealth, policy: &EffectivePolicy) -> Option<Operation> {
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
        targets: Vec::new(),
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
    })
}

fn plan_orphan_removal(
    health: &TableHealth,
    policy: &EffectivePolicy,
    context: PlanContext,
    now: DateTime<Utc>,
) -> Option<Operation> {
    let settings = &policy.orphans;
    if !settings.enabled.value {
        return None;
    }

    // The one operation that is scheduled rather than triggered. Every other
    // operation decides from metadata Bergman has already read; this one cannot
    // know whether a table has orphans without listing its whole location, and
    // that listing *is* the cost. Running it on every cycle would make a
    // five-minute cadence mean a full object-store listing of every table every
    // five minutes — real money on S3, and it finds nothing almost every time.
    if let Some(last) = context.last_orphan_scan {
        let elapsed = now.signed_duration_since(last);
        let interval = chrono::Duration::from_std(settings.min_interval.value)
            .unwrap_or_else(|_| chrono::Duration::days(1));
        if elapsed < interval {
            return None;
        }
    }

    // Listing is the expensive part, so the plan cannot estimate what it will
    // find. It says what it will *do*, which is the honest thing a plan can
    // promise here.
    let action = match settings.mode.value {
        OrphanMode::DryRun => "report",
        OrphanMode::Delete => "delete",
    };

    Some(Operation {
        kind: OperationKind::RemoveOrphans,
        targets: Vec::new(),
        reason: format!(
            "scan {} and {action} files older than {} that no retained snapshot references",
            health.location,
            human_duration(settings.older_than.value),
        ),
        estimate: Estimate::default(),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::health::{
        FileHealth, ManifestHealth, PartitionHealth, PartitionKey, SnapshotHealth,
    };
    use crate::policy::{Config, Decision, Policy, TableFacts, TableRef};

    fn effective(toml: &str) -> EffectivePolicy {
        let config = Config::from_toml(toml).unwrap();
        let policy = Policy::compile(&config).unwrap();
        match policy.decide(&TableRef::new("prod", ["db"], "t"), &TableFacts::unknown()) {
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
            format_version: iceberg::spec::FormatVersion::V2,
            write_format: None,
            location: "s3://bucket/wh/db/t".into(),
            current_spec_id: 0,
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
        assert!(
            plan_table(
                &health,
                &effective(COMPACT_ON),
                PlanContext::default(),
                Utc::now()
            )
            .is_none()
        );
    }

    #[test]
    fn small_files_trigger_compaction_with_a_legible_reason() {
        let health = health_with(
            vec![partition("d=1", &[10, 10, 10, 10, 10, 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .unwrap();
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
        assert!(
            plan_table(
                &health,
                &effective(COMPACT_ON),
                PlanContext::default(),
                Utc::now()
            )
            .is_none()
        );
    }

    #[test]
    fn only_the_eligible_files_are_charged_to_the_plan() {
        // The estimate is what the cycle's byte budget is charged, so it has to
        // be what a rewrite reads. This partition is ten at-target files plus
        // forty tiny ones; charging the whole partition would bill the budget
        // ten thousand times the rewrite's actual cost and defer real work
        // elsewhere to pay for work nobody does.
        let mut sizes = vec![1000u64; 10];
        sizes.extend(std::iter::repeat_n(10u64, 40));

        let health = health_with(
            vec![partition("d=1", &sizes, 1000, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .unwrap();

        let op = &plan.operations[0];
        assert_eq!(op.kind, OperationKind::Compact);
        assert_eq!(
            op.estimate.input_files, 40,
            "the at-target files are not read"
        );
        assert_eq!(op.estimate.input_bytes, 400);
    }

    #[test]
    fn one_oversized_file_triggers_a_rewrite_on_its_own() {
        // The half a small-file-only compactor forgets. A file no reader can
        // split is a task that cannot be parallelised, and nothing but a
        // rewrite ever splits it — so there is no minimum count to reach and
        // the "N in, N out" guard must not veto producing more files than it
        // consumed, which is the entire point.
        let health = health_with(
            vec![partition("d=1", &[5000, 1000, 1000], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .unwrap();

        let op = &plan.operations[0];
        assert_eq!(op.kind, OperationKind::Compact);
        assert!(
            op.reason.contains("no reader can split"),
            "got: {}",
            op.reason
        );
        assert_eq!(
            op.estimate.input_files, 1,
            "only the oversized file is read"
        );
        assert_eq!(op.estimate.output_files, 5, "and it is split");
    }

    #[test]
    fn a_partition_of_healthy_files_is_left_alone_at_both_ends() {
        // Between the two thresholds is the band a rewrite cannot improve. A
        // planner that read this partition would rewrite a healthy table every
        // cycle forever.
        let health = health_with(
            vec![partition("d=1", &[750, 900, 1000, 1500, 1800], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        assert!(
            plan_table(
                &health,
                &effective(COMPACT_ON),
                PlanContext::default(),
                Utc::now()
            )
            .is_none()
        );
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
        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .unwrap();
        let op = &plan.operations[0];

        assert_eq!(op.kind, OperationKind::Compact);
        assert!(op.reason.contains("rows deleted"), "got: {}", op.reason);
        assert!(op.reason.contains("12 delete files"), "got: {}", op.reason);
    }

    #[test]
    fn compaction_is_executable_and_names_its_partitions() {
        // The plan must carry the partitions it will rewrite, so `run` acts on
        // what `plan` displayed rather than re-deciding against a table that
        // has moved since.
        let health = health_with(
            vec![partition("d=1", &[10; 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .unwrap();

        let op = &plan.operations[0];
        assert_eq!(
            op.targets
                .iter()
                .map(|k| k.value.as_str())
                .collect::<Vec<_>>(),
            vec!["d=1"]
        );
    }

    #[test]
    fn table_wide_operations_carry_no_partition_targets() {
        // Expiration and orphan removal act on the whole table, so a target
        // list would be a promise about granularity they do not have.
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.snapshots]
            max_age = "7d"
            min_to_keep = 1
            [rules.orphans]
            enabled = true
            "#,
        );
        let health = health_with(
            vec![partition("d=1", &[1000], 100, 0, 0)],
            snapshots(10, Duration::from_secs(30 * 86400)),
        );
        let plan = plan_table(&health, &policy, PlanContext::default(), Utc::now()).unwrap();
        assert!(plan.operations.iter().all(|op| op.targets.is_empty()));
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
        let plan = plan_table(&health, &policy, PlanContext::default(), Utc::now()).unwrap();
        let op = &plan.operations[0];

        assert_eq!(op.kind, OperationKind::ExpireSnapshots);
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
        assert!(plan_table(&health, &policy, PlanContext::default(), Utc::now()).is_none());
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
        let plan = plan_table(&health, &policy, PlanContext::default(), Utc::now()).unwrap();

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

    #[test]
    fn a_partition_still_being_written_is_not_compacted() {
        // The guard against fighting the streaming writer for the hot
        // partition. A rewrite of it loses its compare-and-swap to the very
        // next micro-batch, having already spent a full read and write of the
        // data — repeatedly, every cycle.
        let now = Utc::now();
        let mut hot = partition("d=1", &[10; 10], 100, 0, 0);
        hot.newest_file_ms = Some(now.timestamp_millis() - 60_000);

        let health = health_with(vec![hot], snapshots(1, Duration::from_secs(60)));
        assert!(plan_table(&health, &effective(COMPACT_ON), PlanContext::default(), now).is_none());
    }

    #[test]
    fn a_partition_that_has_settled_is_compacted() {
        // The other side of the same rule: waiting forever would be no better
        // than never waiting.
        let now = Utc::now();
        let mut cold = partition("d=1", &[10; 10], 100, 0, 0);
        cold.newest_file_ms = Some(now.timestamp_millis() - 7_200_000);

        let health = health_with(vec![cold], snapshots(1, Duration::from_secs(60)));
        let plan =
            plan_table(&health, &effective(COMPACT_ON), PlanContext::default(), now).unwrap();
        assert_eq!(plan.operations[0].kind, OperationKind::Compact);
    }

    #[test]
    fn a_partition_under_a_superseded_spec_is_not_compacted() {
        // Output is written under the table's current spec. A commit claiming
        // it replaces files partitioned by an older one mis-files every row in
        // it, so the plan must not promise the rewrite either.
        let mut old = partition("d=1", &[10; 10], 100, 0, 0);
        old.key.spec_id = 0;

        let mut health = health_with(vec![old], snapshots(1, Duration::from_secs(60)));
        health.current_spec_id = 1;

        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .expect("a partition that can never be rewritten is not silence");

        assert!(!plan.has_work(), "nothing may be planned for it");
        // ...but the operator has to learn why, or a fragmented partition reads
        // as a healthy one forever.
        assert_eq!(plan.notes.len(), 1);
        assert!(
            plan.notes[0].contains("superseded partition spec"),
            "got: {}",
            plan.notes[0]
        );
    }

    #[test]
    fn a_healthy_partition_under_an_old_spec_is_not_worth_a_note() {
        // The other half of the same rule, and the one that decides whether the
        // note above is ever read. A table whose spec evolved keeps every old
        // partition it ever had, and almost all of them are at target size and
        // want nothing. Counting those would put a paragraph about mis-filed
        // rows on every plan of every spec-evolved table, every cycle — and a
        // warning that appears when nothing is wrong is a warning nobody reads
        // when something is.
        let mut healthy = partition("d=1", &[1000, 1000, 1000], 100, 0, 0);
        healthy.key.spec_id = 0;

        let mut health = health_with(vec![healthy], snapshots(1, Duration::from_secs(60)));
        health.current_spec_id = 1;

        assert!(
            plan_table(
                &health,
                &effective(COMPACT_ON),
                PlanContext::default(),
                Utc::now()
            )
            .is_none(),
            "a healthy old partition is silence, not a warning"
        );
    }

    #[test]
    fn a_non_parquet_table_says_why_compaction_will_never_run() {
        // Bergman reads and writes only Parquet, so a rewrite of an ORC table
        // would silently change its format. The executor refuses it; planning
        // it anyway would report that refusal every cycle forever, and planning
        // nothing silently would look exactly like a healthy table.
        let mut health = health_with(
            vec![partition("d=1", &[10; 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        health.write_format = Some("orc".into());

        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .expect("a table whose policy cannot apply is not silence");

        assert!(!plan.has_work());
        assert!(plan.notes[0].contains("orc"), "got: {}", plan.notes[0]);
    }

    #[test]
    fn a_parquet_table_is_compacted_normally() {
        // The other direction: an explicit `write.format.default = "parquet"`
        // must not be mistaken for a refusal.
        let mut health = health_with(
            vec![partition("d=1", &[10; 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        health.write_format = Some("parquet".into());

        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .unwrap();
        assert_eq!(plan.operations[0].kind, OperationKind::Compact);
        assert!(plan.notes.is_empty());
    }

    #[test]
    fn a_v3_table_says_why_compaction_will_never_run() {
        // Bergman cannot author a v3 snapshot without destroying row lineage,
        // and `TableMetadataBuilder` rejects one that tries. Planning the
        // rewrite anyway would report a refusal every cycle forever; planning
        // nothing and saying nothing would look exactly like a healthy table.
        let mut health = health_with(
            vec![partition("d=1", &[10; 10], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        health.format_version = iceberg::spec::FormatVersion::V3;

        let plan = plan_table(
            &health,
            &effective(COMPACT_ON),
            PlanContext::default(),
            Utc::now(),
        )
        .expect("a table whose policy cannot apply is not silence");

        assert!(!plan.has_work());
        assert!(
            plan.notes[0].contains("format v3"),
            "got: {}",
            plan.notes[0]
        );
    }

    #[test]
    fn a_v3_table_still_gets_its_snapshots_expired() {
        // The refusal is scoped to what Bergman authors itself. Expiration is
        // upstream's own action and orphan removal commits nothing, so both
        // remain available — refusing the whole table would leave every v3
        // table's history growing without bound.
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.compaction]
            enabled = true
            [rules.snapshots]
            max_age = "7d"
            min_to_keep = 1
            "#,
        );
        let mut health = health_with(
            vec![partition("d=1", &[10; 10], 100, 0, 0)],
            snapshots(10, Duration::from_secs(30 * 86400)),
        );
        health.format_version = iceberg::spec::FormatVersion::V3;

        let plan = plan_table(&health, &policy, PlanContext::default(), Utc::now()).unwrap();
        assert_eq!(
            plan.operations.iter().map(|op| op.kind).collect::<Vec<_>>(),
            vec![OperationKind::ExpireSnapshots]
        );
        assert_eq!(plan.notes.len(), 1);
    }

    #[test]
    fn an_empty_table_is_still_scanned_for_orphans() {
        // A first write that died between staging its data and committing left
        // files under the table location that nothing references and nothing
        // else will ever reclaim. Skipping the table because it has no
        // snapshots leaks them forever.
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.orphans]
            enabled = true
            "#,
        );
        let mut health = health_with(vec![], snapshots(0, Duration::from_secs(0)));
        health.snapshots.current_snapshot_id = None;
        assert!(health.is_empty());

        let plan = plan_table(&health, &policy, PlanContext::default(), Utc::now()).unwrap();
        assert_eq!(
            plan.operations.iter().map(|op| op.kind).collect::<Vec<_>>(),
            vec![OperationKind::RemoveOrphans]
        );
    }

    #[test]
    fn an_empty_table_with_no_orphan_rule_is_left_alone() {
        let mut health = health_with(vec![], snapshots(0, Duration::from_secs(0)));
        health.snapshots.current_snapshot_id = None;
        assert!(
            plan_table(
                &health,
                &effective(COMPACT_ON),
                PlanContext::default(),
                Utc::now()
            )
            .is_none()
        );
    }

    #[test]
    fn orphan_scanning_is_not_repeated_inside_its_interval() {
        // The one operation that cannot be triggered from metadata: knowing
        // whether a table has orphans means listing its whole location, and
        // that listing is the cost. On a five-minute cadence, running it every
        // cycle would list every table every five minutes and find nothing
        // almost every time.
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.orphans]
            enabled = true
            min_interval = "24h"
            "#,
        );
        let health = health_with(
            vec![partition("d=1", &[1000], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );
        let now = Utc::now();

        let recent = PlanContext {
            last_orphan_scan: Some(now - chrono::Duration::hours(1)),
        };
        assert!(plan_table(&health, &policy, recent, now).is_none());

        let stale = PlanContext {
            last_orphan_scan: Some(now - chrono::Duration::hours(48)),
        };
        let plan = plan_table(&health, &policy, stale, now).unwrap();
        assert_eq!(plan.operations[0].kind, OperationKind::RemoveOrphans);
    }

    #[test]
    fn a_process_that_has_never_scanned_scans() {
        // A one-shot `bergman run` *is* the schedule — the cron entry that
        // invoked it already decided the cadence — so it must not skip the scan
        // for want of a memory it could never have.
        let policy = effective(
            r#"
            [[rules]]
            match = "prod.db.t"
            [rules.orphans]
            enabled = true
            "#,
        );
        let health = health_with(
            vec![partition("d=1", &[1000], 100, 0, 0)],
            snapshots(1, Duration::from_secs(60)),
        );

        let plan = plan_table(&health, &policy, PlanContext::default(), Utc::now()).unwrap();
        assert_eq!(plan.operations[0].kind, OperationKind::RemoveOrphans);
    }
}
