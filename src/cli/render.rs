//! Terminal and JSON rendering.

use comfy_table::{Cell, Table, presets};

use crate::cli::Format;
use crate::error::Result;
use crate::health::TableHealth;
use crate::plan::{MaintenancePlan, OperationResult, RunReport};
use crate::policy::{Decision, TableRef};
use crate::util::{human_bytes, human_duration};

fn table() -> Table {
    let mut t = Table::new();
    // A borderless preset: output that survives being piped into a paste, a
    // ticket, or a chat message without turning into line-noise.
    t.load_preset(presets::NOTHING);
    t
}

/// Render `bergman inspect`.
pub fn inspect(health: &[TableHealth], format: Format) -> Result<()> {
    if format == Format::Json {
        println!(
            "{}",
            serde_json::to_string_pretty(health).unwrap_or_default()
        );
        return Ok(());
    }

    if health.is_empty() {
        println!("No tables matched.");
        return Ok(());
    }

    let mut t = table();
    t.set_header(vec![
        "TABLE",
        "FILES",
        "SIZE",
        "AVG FILE",
        "DELETES",
        "SNAPSHOTS",
        "MANIFESTS",
    ]);

    for h in health {
        if h.is_empty() {
            t.add_row(vec![
                Cell::new(h.table.to_string()),
                Cell::new("—"),
                Cell::new("empty"),
                Cell::new("—"),
                Cell::new("—"),
                Cell::new(h.snapshots.count.to_string()),
                Cell::new("—"),
            ]);
            continue;
        }

        let deletes = h.files.position_delete_count + h.files.equality_delete_count;
        t.add_row(vec![
            Cell::new(h.table.to_string()),
            Cell::new(h.files.data_file_count.to_string()),
            Cell::new(human_bytes(h.files.data_bytes)),
            Cell::new(human_bytes(h.files.average_file_size())),
            // Delete files and the share of rows they name: the pair that says
            // whether a table's reads are amplified, which file counts alone do
            // not.
            Cell::new(if deletes == 0 {
                "—".to_string()
            } else {
                format!("{deletes} ({:.0}%)", h.files.delete_ratio() * 100.0)
            }),
            Cell::new(format!(
                "{}{}",
                h.snapshots.count,
                h.snapshots
                    .oldest_age
                    .map(|a| format!(" / {}", human_duration(a)))
                    .unwrap_or_default()
            )),
            Cell::new(if h.manifests.undersized_count > 0 {
                format!(
                    "{} ({} small)",
                    h.manifests.count, h.manifests.undersized_count
                )
            } else {
                h.manifests.count.to_string()
            }),
        ]);
    }

    println!("{t}");
    Ok(())
}

/// Render `bergman plan`.
pub fn plan(plan: &MaintenancePlan, format: Format) -> Result<()> {
    if format == Format::Json {
        println!("{}", serde_json::to_string_pretty(plan).unwrap_or_default());
        return Ok(());
    }

    // A plan with no operations and no notes is genuinely quiet. One with notes
    // is not: those are the tables whose configured maintenance can never run,
    // and printing "nothing to do" over them is exactly the silence the whole
    // notes mechanism exists to prevent.
    if plan.is_empty() && plan.notes().next().is_none() {
        println!("Nothing to do. {} tables examined.", plan.uneventful.len());
        return Ok(());
    }

    for table_plan in &plan.tables {
        println!("\n{}", table_plan.table);
        println!("  rule: {}", table_plan.policy.matched_rule);

        for warning in &table_plan.policy.warnings {
            println!("  warning: {warning}");
        }

        // Work the policy asked for that this table cannot receive. Printed
        // whether or not anything else is planned, because a table that appears
        // with no operations and no explanation reads as a healthy one.
        for note in &table_plan.notes {
            println!("  note: {note}");
        }

        for op in &table_plan.operations {
            println!("  -> {}", op.kind);
            println!("     why: {}", op.reason);

            if op.estimate.input_files > 0 {
                println!(
                    "     reads {} files ({}), writes ~{} files",
                    op.estimate.input_files,
                    human_bytes(op.estimate.input_bytes),
                    op.estimate.output_files
                );
            }
            if op.estimate.snapshots_removed > 0 {
                println!(
                    "     removes up to {} snapshots",
                    op.estimate.snapshots_removed
                );
            }
        }
    }

    // Tables with work, not entries in the list: a table that appears only to
    // explain why its policy cannot apply is not a table being maintained, and
    // counting it as one would overstate what the cycle will do.
    println!(
        "\n{} tables, {} operations, {} to read",
        plan.tables.iter().filter(|t| t.has_work()).count(),
        plan.operation_count(),
        human_bytes(plan.total_input_bytes()),
    );
    Ok(())
}

/// Render `bergman run`.
pub fn report(report: &RunReport, format: Format) -> Result<()> {
    if format == Format::Json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).unwrap_or_default()
        );
        return Ok(());
    }

    for table_outcome in &report.tables {
        println!("\n{}", table_outcome.table);
        for note in &table_outcome.notes {
            println!("  note: {note}");
        }
        for op in &table_outcome.operations {
            let (marker, detail) = match &op.result {
                OperationResult::Succeeded { detail } => ("ok", detail.clone()),
                OperationResult::NoOp { detail } => ("--", detail.clone()),
                OperationResult::Refused { reason } => ("XX", reason.clone()),
                OperationResult::Conflicted { detail } => ("<>", detail.clone()),
                OperationResult::Failed { error } => ("XX", error.clone()),
            };
            println!("  [{marker}] {}: {detail}", op.kind);
        }
    }

    println!("\n{report}");
    Ok(())
}

/// Render `bergman policy explain`.
pub fn explain(table: &TableRef, decision: &Decision, format: Format) -> Result<()> {
    if format == Format::Json {
        println!(
            "{}",
            serde_json::to_string_pretty(decision).unwrap_or_default()
        );
        return Ok(());
    }

    match decision {
        Decision::Unmatched => {
            println!("{table}: no rule matches. Not maintained.");
        }
        Decision::Skip { pattern } => {
            println!("{table}: excluded by rule \"{pattern}\".");
        }
        Decision::Maintain(policy) => {
            println!("{table}");
            println!("  matched rule: {}\n", policy.matched_rule);

            // Every resolved setting, not a selection. The command exists to
            // answer "why is it *that*", and a knob missing from the table is a
            // knob whose answer the operator has to go and guess.
            let mut t = table_with_provenance();
            let c = &policy.compaction;
            add(
                &mut t,
                "compaction.enabled",
                &c.enabled.value.to_string(),
                &c.enabled.from.to_string(),
            );
            add(
                &mut t,
                "compaction.target_file_size",
                &human_bytes(c.target_file_size.value),
                &c.target_file_size.from.to_string(),
            );
            match &c.sort {
                Some(sort) => add(
                    &mut t,
                    "compaction.sort",
                    &sort
                        .value
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    &sort.from.to_string(),
                ),
                // Absence is a resolved answer too, and the one an operator is
                // most likely to be surprised by.
                None => add(&mut t, "compaction.sort", "(unsorted)", "nothing asked"),
            }
            add(
                &mut t,
                "compaction.trigger.small_file_ratio",
                &format!("{:.2}", c.small_file_ratio.value),
                &c.small_file_ratio.from.to_string(),
            );
            add(
                &mut t,
                "compaction.trigger.min_file_size_ratio",
                &format!("{:.2}", c.min_file_size_ratio.value),
                &c.min_file_size_ratio.from.to_string(),
            );
            add(
                &mut t,
                "compaction.trigger.min_input_files",
                &c.min_input_files.value.to_string(),
                &c.min_input_files.from.to_string(),
            );
            add(
                &mut t,
                "compaction.trigger.delete_ratio",
                &format!("{:.2}", c.delete_ratio.value),
                &c.delete_ratio.from.to_string(),
            );
            add(
                &mut t,
                "compaction.trigger.min_file_age",
                &human_duration(c.min_file_age.value),
                &c.min_file_age.from.to_string(),
            );
            add(
                &mut t,
                "compaction.max_sort_memory",
                &human_bytes(c.max_sort_memory.value),
                &c.max_sort_memory.from.to_string(),
            );
            add(
                &mut t,
                "compaction.max_group_bytes",
                &human_bytes(c.max_group_bytes.value),
                &c.max_group_bytes.from.to_string(),
            );
            add(
                &mut t,
                "compaction.max_input_files",
                &c.max_input_files.value.to_string(),
                &c.max_input_files.from.to_string(),
            );

            let s = &policy.snapshots;
            add(
                &mut t,
                "snapshots.enabled",
                &s.enabled.value.to_string(),
                &s.enabled.from.to_string(),
            );
            add(
                &mut t,
                "snapshots.max_age",
                &human_duration(s.max_age.value),
                &s.max_age.from.to_string(),
            );
            add(
                &mut t,
                "snapshots.min_to_keep",
                &s.min_to_keep.value.to_string(),
                &s.min_to_keep.from.to_string(),
            );
            add(
                &mut t,
                "snapshots.delete_files",
                &s.delete_files.value.to_string(),
                &s.delete_files.from.to_string(),
            );

            let m = &policy.manifests;
            add(
                &mut t,
                "manifests.rewrite",
                &m.rewrite.value.to_string(),
                &m.rewrite.from.to_string(),
            );
            add(
                &mut t,
                "manifests.target_size",
                &human_bytes(m.target_size.value),
                &m.target_size.from.to_string(),
            );
            add(
                &mut t,
                "manifests.min_count_to_merge",
                &m.min_count_to_merge.value.to_string(),
                &m.min_count_to_merge.from.to_string(),
            );

            let o = &policy.orphans;
            add(
                &mut t,
                "orphans.enabled",
                &o.enabled.value.to_string(),
                &o.enabled.from.to_string(),
            );
            add(
                &mut t,
                "orphans.mode",
                match o.mode.value {
                    crate::policy::OrphanMode::DryRun => "dry-run",
                    crate::policy::OrphanMode::Delete => "delete",
                },
                &o.mode.from.to_string(),
            );
            add(
                &mut t,
                "orphans.older_than",
                &human_duration(o.older_than.value),
                &o.older_than.from.to_string(),
            );
            add(
                &mut t,
                "orphans.min_interval",
                &human_duration(o.min_interval.value),
                &o.min_interval.from.to_string(),
            );

            if let Some(schedule) = &policy.schedule {
                add(
                    &mut t,
                    "schedule",
                    &schedule.value,
                    &schedule.from.to_string(),
                );
            }

            println!("{t}");

            for warning in &policy.warnings {
                println!("\nwarning: {warning}");
            }
        }
    }
    Ok(())
}

/// Render `bergman policy match`.
pub fn matches(matches: &[(TableRef, Decision)], format: Format) -> Result<()> {
    if format == Format::Json {
        let json: Vec<_> = matches
            .iter()
            .map(|(t, d)| serde_json::json!({ "table": t.to_string(), "decision": d }))
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json).unwrap_or_default()
        );
        return Ok(());
    }

    let mut t = table();
    t.set_header(vec!["TABLE", "DECISION", "RULE"]);
    for (table_ref, decision) in matches {
        let (verdict, rule) = match decision {
            Decision::Unmatched => ("unmatched", String::new()),
            Decision::Skip { pattern } => ("skipped", pattern.clone()),
            Decision::Maintain(p) => ("maintained", p.matched_rule.clone()),
        };
        t.add_row(vec![
            Cell::new(table_ref.to_string()),
            Cell::new(verdict),
            Cell::new(rule),
        ]);
    }
    println!("{t}");
    Ok(())
}

fn table_with_provenance() -> Table {
    let mut t = table();
    t.set_header(vec!["SETTING", "VALUE", "FROM"]);
    t
}

fn add(t: &mut Table, setting: &str, value: &str, from: &str) {
    t.add_row(vec![Cell::new(setting), Cell::new(value), Cell::new(from)]);
}
