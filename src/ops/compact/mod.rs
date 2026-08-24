//! Compaction: rewriting a partition's small files, with its delete files
//! applied.
//!
//! The commit half lives in [`crate::commit`]; this is the data half. Reading
//! is upstream's scan pipeline for the data and the **positional** deletes —
//! which become a Parquet row selection, so a deleted row is never decoded —
//! and Bergman's own hash anti-join for the **equality** deletes (see the
//! `rewrite` submodule). What reaches the writer is surviving rows, and writing
//! them back at a higher sequence number is what retires the delete files.
//!
//! # The four rules that matter
//!
//! 1. **Only the files a rewrite would improve are read.** A partition earns a
//!    rewrite as a whole, but the files already at the right size gain nothing
//!    from being read and written back. Rewriting them is not incorrect, just
//!    enormously expensive.
//!
//! 2. **A rewrite may remove a delete file only if every data file that delete
//!    file applies to is inside the group being rewritten.** A delete file
//!    shared with a file outside the group is still doing work for that file,
//!    and dropping it resurrects the rows it was hiding. That is why planning
//!    happens over the *whole* table before anything is rewritten: the question
//!    "does this delete file apply anywhere else?" cannot be answered from
//!    inside the group.
//!
//! 3. **Rows in equal rows out.** Every group checks the read side — what the
//!    pipeline streamed against what the input manifests said — *and* the write
//!    side — what the output files hold against what reached the writer — and
//!    refuses to commit when either disagrees. Both are metadata, so both are
//!    free. A rewrite that silently lost rows looks exactly like one that
//!    worked, and the table it produces is wrong forever.
//!
//! 4. **A group never spans partition specs, and never rewrites files written
//!    under a spec other than the table's current one.** Output is written
//!    under the current spec; claiming it replaces files partitioned
//!    differently would mis-file every row in it.

mod rewrite;
mod writer;

use std::collections::{HashMap, HashSet};

use futures::StreamExt;
use iceberg::TableIdent;
use iceberg::scan::FileScanTask;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;

use crate::commit::{BranchRetention, RewriteFiles, SnapshotProducer, TableCommitter};
use crate::error::{Error, Result};
use crate::health::{FileIndex, PartitionKey as HealthPartitionKey};
use crate::obs::{MaintenanceObserver, OperationContext};
use crate::ops::reachability::normalize;
use crate::ops::{MAX_COMMIT_ATTEMPTS, OpEnv, TableLoader, retry_delay};
use crate::plan::OperationResult;
use crate::policy::{EffectiveCompaction, SortColumn, TableRef};
use crate::util::human_bytes;

/// How many file groups may fail in a row before compaction gives up on a table
/// for this cycle.
///
/// A failing group has already read and written its files by the time it fails,
/// so this cost is paid per group. Three in a row means the reason is the table
/// or the catalog rather than the group. Spark bounds the same thing with
/// `partial-progress.max-failed-commits`.
const MAX_CONSECUTIVE_GROUP_FAILURES: usize = 3;

/// What a compaction did.
#[derive(Debug, Clone, Default)]
struct CompactOutcome {
    // File groups rewritten.
    groups: usize,
    // Partitions touched.
    partitions: usize,
    // Data files read and replaced.
    files_removed: usize,
    // Delete files fully applied and retired.
    deletes_retired: usize,
    // Delete files removed because they applied to no live data file.
    dangling_deletes_removed: usize,
    // Files written.
    files_written: usize,
    // Bytes read.
    bytes_read: u64,
    // Bytes written.
    bytes_written: u64,
    // Groups skipped, with the reason.
    skipped: Vec<String>,
}

/// The inputs every group of one compaction run shares.
///
/// Bundled rather than threaded as a dozen parameters: they are all constant
/// for the run, and a long positional list is where a caller eventually swaps
/// two `u64`s that mean different things.
struct Job<'a> {
    ident: &'a TableIdent,
    loader: &'a dyn TableLoader,
    committer: &'a dyn TableCommitter,
    settings: &'a Settings,
    observer: &'a dyn MaintenanceObserver,
    ctx: OperationContext<'a>,
}

/// One snapshot, in the three views a rewrite needs.
///
/// They move together. Each answers a different question about the same
/// snapshot — what the table is, which files are live with which deletes, and
/// what the manifests say that the scan omits — so mixing readings from two
/// snapshots is a rewrite reasoning about a table that never existed.
///
/// Reloaded whenever the table moves, including when *we* moved it: every group
/// commits on its own, so a group offered against the plan-time parent would
/// lose its compare-and-swap to the group before it, after reading and writing
/// its whole group to find out.
struct TableState {
    table: Table,
    tasks: Vec<FileScanTask>,
    files: FileIndex,
}

impl TableState {
    /// Read all three views of the table as it currently stands.
    async fn load(
        loader: &dyn TableLoader,
        ident: &TableIdent,
        table_ref: &TableRef,
    ) -> Result<Self> {
        let table = loader.reload(ident).await?;
        Self::of(table, table_ref).await
    }

    /// The same, for a table already in hand.
    async fn of(table: Table, table_ref: &TableRef) -> Result<Self> {
        let tasks = plan_all(&table).await?;
        let files = crate::health::file_index(table_ref, &table).await?;
        Ok(Self {
            table,
            tasks,
            files,
        })
    }

    /// The partition a scanned file belongs to, as the manifests record it.
    ///
    /// The key is rendered by [`crate::health`], which is also what the planner
    /// carries, so the plan and the rewrite agree on what a partition is by
    /// construction rather than by two implementations happening to match.
    fn partition_of(&self, task: &FileScanTask) -> Option<HealthPartitionKey> {
        self.files
            .partition_of(&normalize(&task.data_file_path))
            .cloned()
    }

    /// The live tasks whose paths are `wanted`.
    ///
    /// Selected by path from *this* snapshot's tasks, never by reusing the ones
    /// a previous reading captured. A stale [`FileScanTask`] carries the delete
    /// files that applied to it then; if a commit since added an equality delete
    /// covering one of these data files, reading with the old list would not
    /// apply it — and its rows would be written straight back into the compacted
    /// output, leaving no trace.
    fn select(&self, wanted: &HashSet<String>) -> Vec<FileScanTask> {
        self.tasks
            .iter()
            .filter(|t| wanted.contains(&normalize(&t.data_file_path)))
            .cloned()
            .collect()
    }
}

/// The resolved knobs a rewrite reads, flattened out of the policy.
struct Settings {
    target: u64,
    sort: Option<Vec<SortColumn>>,
    memory_budget: u64,
    max_group_bytes: u64,
    max_input_files: usize,
    /// Below this a file counts as small; above `large` it counts as oversized.
    /// Together they decide which of a partition's files are read at all.
    small: u64,
    large: u64,
}

impl Settings {
    /// Whether one scanned file earns a place in a rewrite.
    ///
    /// The executor's version of [`crate::policy::EffectiveCompaction::is_eligible`],
    /// and exact where the planner's is not: the planner reads manifests, which
    /// do not say which data file a delete applies to, while a `FileScanTask`
    /// carries precisely the deletes that apply to it. So the planner is
    /// conservative — a delete-triggered partition offers all its files — and
    /// this narrows that to the files a rewrite actually improves.
    fn is_eligible(&self, task: &FileScanTask) -> bool {
        !task.deletes.is_empty()
            || task.file_size_in_bytes < self.small
            || task.file_size_in_bytes > self.large
    }
}

/// One unit of rewriting: the files that will become one commit.
struct FileGroup {
    partition: HealthPartitionKey,
    /// Which group this is within its partition, for legible diagnostics.
    index: usize,
    tasks: Vec<FileScanTask>,
}

impl FileGroup {
    /// What reading this group costs.
    fn bytes(&self) -> u64 {
        self.tasks.iter().map(|t| t.file_size_in_bytes).sum()
    }
}

impl std::fmt::Display for FileGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partition {}", self.partition)?;
        if self.index > 0 {
            write!(f, " (group {})", self.index + 1)?;
        }
        Ok(())
    }
}

/// Compact the partitions a plan identified.
pub async fn run(
    env: &OpEnv<'_>,
    settings: &EffectiveCompaction,
    partitions: &[HealthPartitionKey],
) -> Result<OperationResult> {
    let table = env.table;
    let table_ref = env.table_ref();
    let metadata = table.metadata();

    if metadata.current_snapshot().is_none() {
        return Ok(OperationResult::NoOp {
            detail: "the table has never been written to".into(),
        });
    }

    // Refused before the table is scanned. A v3 table's row lineage cannot
    // survive a rewrite Bergman performs (see `crate::commit::authoring_refusal`),
    // and discovering that after reading a partition would be a full data read
    // spent to reach the same answer.
    if let Some(reason) = crate::commit::authoring_refusal(metadata.format_version()) {
        return Err(Error::refused("compact", table_ref, reason));
    }

    // Iceberg's data file formats are Parquet, Avro and ORC. Bergman handles
    // Parquet and only Parquet, in both directions: upstream's `ArrowReader`
    // builds a `ParquetRecordBatchStreamBuilder` regardless of what a task's
    // `data_file_format` says, and the write side is `ParquetWriterBuilder`.
    //
    // A table declaring `write.format.default = "orc"` is refused here rather
    // than rewritten into Parquet behind its owner's back. The *files* are
    // checked separately, per group, because a table can hold files in a format
    // it no longer writes — see `rewrite_once`.
    let write_format = metadata
        .properties()
        .get("write.format.default")
        .map(|s| s.trim().to_ascii_lowercase());
    if let Some(format) = write_format
        && format != "parquet"
    {
        return Err(Error::refused(
            "compact",
            table_ref,
            format!(
                "the table's write.format.default is {format:?}; Bergman reads and writes \
                 only Parquet, and a rewrite must not silently change a table's format"
            ),
        ));
    }

    let resolved = Settings {
        target: settings.target_file_size.value,
        sort: settings.sort.as_ref().map(|r| r.value.clone()),
        memory_budget: settings.max_sort_memory.value,
        max_group_bytes: settings.max_group_bytes.value,
        max_input_files: settings.max_input_files.value,
        small: settings.small_file_threshold(),
        large: settings.large_file_threshold(),
    };

    // Checked before anything is read: a typo in a policy should cost a
    // metadata lookup, not a full partition read followed by a failure.
    if let Some(columns) = &resolved.sort {
        check_sort_columns(table_ref, metadata, columns)?;
    }

    // Plan the whole table once. Two things need this rather than a
    // group-scoped scan: deciding which delete files are exclusive to a group,
    // and finding delete files that apply to nothing at all. The manifest index
    // comes with it, because the scan omits a file's partition and a delete
    // file's format and both are needed per group.
    let mut state = TableState::of(table.clone(), table_ref).await?;

    let job = Job {
        ident: env.ident,
        loader: env.loader,
        committer: env.committer,
        settings: &resolved,
        observer: env.observer,
        ctx: env.ctx,
    };

    let groups = job.build_groups(&state, partitions)?;
    let mut outcome = CompactOutcome::default();
    let mut touched: HashSet<HealthPartitionKey> = HashSet::new();
    let mut consecutive_failures = 0usize;

    // Each group commits on its own, so one conflict costs one group's work
    // rather than the table's. Partial progress is progress — it is what makes
    // compacting an actively-written table tractable at all.
    for group in &groups {
        match job.rewrite_and_commit(&state, group).await {
            Ok(Some(stats)) => {
                outcome.groups += 1;
                touched.insert(group.partition.clone());
                outcome.files_removed += stats.files_removed;
                outcome.deletes_retired += stats.deletes_retired;
                outcome.files_written += stats.files_written;
                outcome.bytes_read += stats.bytes_read;
                outcome.bytes_written += stats.bytes_written;
                consecutive_failures = 0;

                // That commit moved `main`. The next group is offered against
                // the snapshot this one produced, not the one the plan was
                // built on — otherwise it loses its compare-and-swap to us and
                // pays a full read and write of its data to discover it. This
                // read is metadata only, and it is the same read the retry
                // would have done anyway; doing it here means doing it *before*
                // the wasted rewrite rather than after.
                match TableState::load(job.loader, job.ident, table_ref).await {
                    Ok(fresh) => state = fresh,
                    // The table cannot be re-read, so no later group can be
                    // offered honestly. Stopping keeps what has been committed
                    // and says why; carrying on would mean rewriting groups
                    // against a snapshot known to be stale.
                    Err(e) => {
                        outcome.skipped.push(format!(
                            "stopped after {} groups: the table could not be re-read ({e})",
                            outcome.groups
                        ));
                        break;
                    }
                }
            }
            Ok(None) => {}
            Err(e) if e.is_replan() => {
                consecutive_failures += 1;
                outcome
                    .skipped
                    .push(format!("{group}: {e}; will replan next cycle"));
            }
            Err(e) => {
                consecutive_failures += 1;
                outcome.skipped.push(format!("{group}: {e}"));
            }
        }

        // A group that fails has already read and written its whole file group
        // to find out. When several in a row fail, the reason is almost never
        // the group: it is the table being written hard, a credential that
        // stopped working, a catalog refusing the commit. Carrying on spends a
        // full rewrite per group to be told the same thing, and on a table with
        // a hundred groups that is the entire cycle's I/O budget burned on
        // work that lands nowhere.
        //
        // So the operation stops and says so. Nothing is lost: no group's
        // failure changed the table, and the next cycle replans from wherever
        // the table actually is. Spark bounds the same thing with
        // `partial-progress.max-failed-commits`.
        if consecutive_failures >= MAX_CONSECUTIVE_GROUP_FAILURES {
            outcome.skipped.push(format!(
                "stopped after {consecutive_failures} groups failed in a row; \
                 the rest are left for the next cycle rather than rewritten to \
                 reach the same answer"
            ));
            break;
        }
    }
    outcome.partitions = touched.len();

    // A delete file that applies to no live data file is pure read overhead:
    // every scan still opens it and it hides nothing. Retiring those is the
    // cheapest win compaction has, and it is the operation that finally clears
    // up after a rewrite that could not retire a shared delete file at the time.
    match job.drop_dangling_deletes().await {
        Ok(count) => outcome.dangling_deletes_removed = count,
        Err(e) if e.is_replan() => outcome
            .skipped
            .push(format!("dangling deletes: {e}; will replan next cycle")),
        Err(e) => outcome.skipped.push(format!("dangling deletes: {e}")),
    }

    Ok(describe(&outcome))
}

/// Turn an outcome into the line an operator reads.
fn describe(outcome: &CompactOutcome) -> OperationResult {
    if outcome.groups == 0 && outcome.dangling_deletes_removed == 0 {
        let detail = if outcome.skipped.is_empty() {
            "nothing left to compact".to_string()
        } else {
            format!("nothing was rewritten: {}", outcome.skipped.join("; "))
        };
        return OperationResult::NoOp { detail };
    }

    let mut parts = Vec::new();
    if outcome.groups > 0 {
        parts.push(format!(
            "{} files ({}) rewritten into {} ({}) across {} groups in {} partitions",
            outcome.files_removed,
            human_bytes(outcome.bytes_read),
            outcome.files_written,
            human_bytes(outcome.bytes_written),
            outcome.groups,
            outcome.partitions,
        ));
    }
    if outcome.deletes_retired > 0 {
        parts.push(format!("{} delete files retired", outcome.deletes_retired));
    }
    if outcome.dangling_deletes_removed > 0 {
        parts.push(format!(
            "{} dangling delete files removed",
            outcome.dangling_deletes_removed
        ));
    }
    if !outcome.skipped.is_empty() {
        parts.push(format!("{} groups skipped", outcome.skipped.len()));
    }

    OperationResult::Succeeded {
        detail: parts.join(", "),
    }
}

#[derive(Default)]
struct GroupStats {
    files_removed: usize,
    deletes_retired: usize,
    files_written: usize,
    bytes_read: u64,
    bytes_written: u64,
}

/// Plan every live data file in the table.
async fn plan_all(table: &Table) -> Result<Vec<FileScanTask>> {
    let scan = table.scan().build()?;
    let mut stream = scan.plan_files().await?;

    let mut tasks = Vec::new();
    while let Some(task) = stream.next().await {
        tasks.push(task?);
    }
    Ok(tasks)
}

impl Job<'_> {
    /// Split the planned partitions into the units that will each become one
    /// commit.
    ///
    /// A partition is not a unit of work. Spark bin-packs into groups bounded
    /// by `max-file-group-size-bytes` precisely because a partition can be
    /// arbitrarily large, and reading one in a single pass is how a compactor
    /// runs out of memory and how one conflict throws away hours of rewriting.
    /// Bounding by bytes *and* by file count covers both shapes of that
    /// problem: a few enormous files, and a hundred thousand tiny ones.
    fn build_groups(
        &self,
        state: &TableState,
        partitions: &[HealthPartitionKey],
    ) -> Result<Vec<FileGroup>> {
        let metadata = state.table.metadata();
        let current_spec_id = metadata.default_partition_spec_id();

        // Index the table's files by partition once, rather than re-scanning
        // the whole task list per partition. A table with ten thousand
        // partitions would otherwise cost ten thousand full passes.
        let mut by_partition: HashMap<HealthPartitionKey, Vec<&FileScanTask>> = HashMap::new();
        for task in &state.tasks {
            let Some(key) = state.partition_of(task) else {
                // The scan saw a file the manifests did not. Both read the same
                // snapshot, so this means the table moved underneath the plan;
                // rewriting a file whose partition is unknown could file its
                // rows anywhere.
                tracing::debug!(
                    table = %self.ctx.table,
                    file = %task.data_file_path,
                    "scanned file is not in the manifest partition index; skipping"
                );
                continue;
            };
            by_partition.entry(key).or_default().push(task);
        }

        let mut groups = Vec::new();

        for partition in partitions {
            // A partition whose files are all under an older spec cannot be
            // rewritten: output goes out under the *current* spec, and a commit
            // claiming it replaces files partitioned differently mis-files
            // every row. Refusing is the only correct answer, and the plan
            // reports it rather than silently skipping.
            if partition.spec_id != current_spec_id {
                tracing::info!(
                    table = %self.ctx.table,
                    partition = %partition,
                    spec_id = partition.spec_id,
                    current_spec_id,
                    "partition is under a superseded partition spec; not rewriting"
                );
                continue;
            }

            let Some(tasks) = by_partition.get(partition) else {
                continue;
            };

            // The rule that decides what compaction costs. A partition earns a
            // rewrite as a whole; only the files whose size or delete load is
            // the actual problem are read. Without this a partition that
            // triggers because a third of its files are small has the other two
            // thirds — already at target — read and written back for nothing,
            // which on a real table is the difference between rewriting
            // gigabytes and rewriting terabytes.
            let mut tasks: Vec<&FileScanTask> = tasks
                .iter()
                .copied()
                .filter(|task| self.settings.is_eligible(task))
                .collect();

            // Smallest first, so a group fills with the files that most need
            // merging and a partial run leaves the largest files alone — they
            // were closest to target anyway.
            tasks.sort_by_key(|t| t.file_size_in_bytes);

            for (index, chunk) in self
                .pack(&tasks)
                .into_iter()
                .filter(|chunk| self.worth_rewriting(chunk))
                .enumerate()
            {
                groups.push(FileGroup {
                    partition: partition.clone(),
                    index,
                    tasks: chunk.into_iter().cloned().collect(),
                });
            }
        }

        // Most consolidation first, so a cycle cut short has done its most
        // valuable work: ranked by how many files each group removes, because
        // that is what compaction is for. Ties break toward the cheaper group,
        // then to the partition key and index, so two runs of an unchanged
        // table do the same work in the same order. Spark exposes the same idea
        // as `rewrite-job-order`.
        groups.sort_by(|a, b| {
            b.tasks
                .len()
                .cmp(&a.tasks.len())
                .then_with(|| a.bytes().cmp(&b.bytes()))
                .then_with(|| a.partition.cmp(&b.partition))
                .then_with(|| a.index.cmp(&b.index))
        });

        Ok(groups)
    }

    /// Whether one packed group earns its own commit.
    ///
    /// Deliberately *not* the policy's `min_input_files`: that is a trigger, and
    /// triggers are the planner's. A threshold evaluated in two places can
    /// drift, and the half that drifted would make `bergman plan` a promise
    /// `bergman run` broke.
    ///
    /// What is left is the question only a group can answer — would rewriting it
    /// change anything? More than one file, a file above the oversized
    /// threshold, or any delete file applying. A lone at-target file with none
    /// of those would be read and written back byte for byte.
    fn worth_rewriting(&self, chunk: &[&FileScanTask]) -> bool {
        !chunk.is_empty()
            && (chunk.len() > 1
                || has_deletes(chunk)
                || chunk
                    .iter()
                    .any(|t| t.file_size_in_bytes > self.settings.large))
    }

    /// Bin-pack one partition's files into groups.
    fn pack<'t>(&self, tasks: &[&'t FileScanTask]) -> Vec<Vec<&'t FileScanTask>> {
        let mut packed: Vec<Vec<&FileScanTask>> = Vec::new();
        let mut current: Vec<&FileScanTask> = Vec::new();
        let mut bytes = 0u64;

        for task in tasks {
            let would_be = bytes.saturating_add(task.file_size_in_bytes);
            let full = !current.is_empty()
                && (would_be > self.settings.max_group_bytes
                    || current.len() >= self.settings.max_input_files);
            if full {
                packed.push(std::mem::take(&mut current));
                bytes = 0;
            }
            bytes = bytes.saturating_add(task.file_size_in_bytes);
            current.push(task);
        }

        if !current.is_empty() {
            packed.push(current);
        }
        packed
    }

    /// Rewrite one group and commit it, retrying against a reloaded table.
    ///
    /// A conflict is not retried by re-submitting: the outputs were written
    /// against a table that has since moved, so the group is rebuilt from the
    /// current snapshot and the rewrite is done again. Re-offering a commit
    /// computed against a stale table is how a concurrent delete gets
    /// discarded and its rows come back.
    async fn rewrite_and_commit(
        &self,
        state: &TableState,
        group: &FileGroup,
    ) -> Result<Option<GroupStats>> {
        // The group is named by the paths the plan chose, and its *tasks* are
        // taken from whichever snapshot is current at the time — see
        // [`TableState::select`] for why that distinction is the whole reason a
        // conflict reloads.
        let wanted: HashSet<String> = group
            .tasks
            .iter()
            .map(|t| normalize(&t.data_file_path))
            .collect();

        let mut borrowed = Some(state);
        let mut reloaded: Option<TableState> = None;

        for attempt in 0..MAX_COMMIT_ATTEMPTS {
            let current = borrowed.unwrap_or_else(|| reloaded.as_ref().expect("reloaded"));
            let tasks = current.select(&wanted);

            // Files a concurrent commit removed are simply gone, and rewriting
            // what remains is the honest continuation — until nothing worth
            // rewriting is left, which means somebody else did the work. That
            // is a success, just not ours.
            //
            // "Worth rewriting" is the same question `build_groups` asked, so
            // it is the same function: a lone oversized file is still worth
            // splitting after a conflict, and a rule spelled out twice is one
            // that can disagree with itself.
            if !self.worth_rewriting(&tasks.iter().collect::<Vec<_>>()) {
                return Ok(None);
            }

            match self.rewrite_once(current, group, &tasks).await {
                Ok(outcome) => return Ok(outcome),
                Err(e) if e.is_replan() && attempt + 1 < MAX_COMMIT_ATTEMPTS => {
                    tracing::debug!(
                        table = %self.ctx.table,
                        group = %group,
                        attempt = attempt + 1,
                        "rewrite lost its commit; reloading and rebuilding the group"
                    );
                    tokio::time::sleep(retry_delay(attempt)).await;

                    borrowed = None;
                    reloaded =
                        Some(TableState::load(self.loader, self.ident, self.ctx.table).await?);
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::CommitConflict {
            table: self.ctx.table.to_string(),
            detail: format!("{MAX_COMMIT_ATTEMPTS} attempts all lost the compare-and-swap"),
        })
    }

    /// One attempt: read the group, write replacements, commit.
    async fn rewrite_once(
        &self,
        state: &TableState,
        group: &FileGroup,
        tasks: &[FileScanTask],
    ) -> Result<Option<GroupStats>> {
        let table_ref = self.ctx.table;
        let table = &state.table;
        let all_tasks = &state.tasks;
        if tasks.is_empty() {
            return Ok(None);
        }

        let in_group: HashSet<String> =
            tasks.iter().map(|t| normalize(&t.data_file_path)).collect();

        // A group must never span partition specs: output goes out under one
        // spec, and a commit claiming it replaces files partitioned differently
        // mis-files every row in it. The spec id comes from the manifest index
        // rather than the task, which never carries one.
        let spec_ids: HashSet<i32> = tasks
            .iter()
            .filter_map(|t| state.partition_of(t).map(|k| k.spec_id))
            .collect();
        if spec_ids.len() > 1 {
            return Err(Error::refused(
                "compact",
                table_ref,
                format!(
                    "{group} holds files under {} partition specs; \
                     rewriting across a spec change is not supported",
                    spec_ids.len()
                ),
            ));
        }

        // Every file this group would read has to be one the reader can read.
        // Upstream's `ArrowReader` opens a Parquet stream whatever a task's
        // `data_file_format` says, so an Avro or ORC file reaches it as a
        // failure to find a Parquet footer — a baffling error where a named
        // refusal belongs. A table can hold such files even when it writes
        // Parquet today: `write.format.default` describes the *next* file, not
        // the ones already there.
        if let Some(other) = tasks
            .iter()
            .find(|t| t.data_file_format != DataFileFormat::Parquet)
        {
            return Err(Error::refused(
                "compact",
                table_ref,
                format!(
                    "{group} holds a {:?} data file ({}); Bergman reads only Parquet",
                    other.data_file_format, other.data_file_path
                ),
            ));
        }

        // The same rule for the delete files that apply to the group, whose
        // format `FileScanTaskDeleteFile` does not carry — so it comes from the
        // manifest index, which is exact. Reading a delete file wrongly is
        // worse than reading a data file wrongly: an equality delete that fails
        // to parse is one whose rows would not be removed.
        for delete in tasks.iter().flat_map(|t| t.deletes.iter()) {
            match state.files.delete_format_of(&normalize(&delete.file_path)) {
                Some(DataFileFormat::Parquet) => {}
                Some(other) => {
                    return Err(Error::refused(
                        "compact",
                        table_ref,
                        format!(
                            "{group} is covered by a {other:?} delete file ({}); \
                             Bergman reads only Parquet",
                            delete.file_path
                        ),
                    ));
                }
                // The scan named a delete file the manifests did not. Both read
                // the same snapshot, so the table moved underneath the plan —
                // and a rewrite that applied an unknown delete file, or failed
                // to, is not one to guess at.
                None => {
                    return Err(Error::StalePlan {
                        table: table_ref.to_string(),
                        detail: format!(
                            "delete file {} is not in the manifest index; \
                             a concurrent commit changed the table",
                            delete.file_path
                        ),
                    });
                }
            }
        }

        // The delete-file rule. A delete file referenced by anything outside
        // this group is still doing work there, so it stays — and because it
        // stays, the data files it applies to inside the group are still
        // correct after the rewrite.
        let deletes_elsewhere: HashSet<String> = all_tasks
            .iter()
            .filter(|t| !in_group.contains(&normalize(&t.data_file_path)))
            .flat_map(|t| t.deletes.iter())
            .map(|d| normalize(&d.file_path))
            .collect();

        let deletes_in_group: HashSet<String> = tasks
            .iter()
            .flat_map(|t| t.deletes.iter())
            .map(|d| normalize(&d.file_path))
            .collect();

        let retirable = retirable_deletes(&deletes_in_group, &deletes_elsewhere);

        let bytes_read: u64 = tasks.iter().map(|t| t.file_size_in_bytes).sum();

        // Rows expected out: what the manifests say went in. Metadata only, so
        // this costs nothing and still catches the failure that matters.
        let expected = expected_rows(tasks);

        let borrowed: Vec<&FileScanTask> = tasks.iter().collect();
        let rewritten = rewrite::rewrite_group(
            table,
            &borrowed,
            self.settings.target,
            self.settings.sort.as_deref(),
            self.settings.memory_budget,
        )
        .await?;
        let written = rewritten.files;

        // Rule 3. A rewrite that lost rows produces a table that is wrong
        // forever and looks fine, so the check is not optional and its failure
        // is not a warning. The outputs are abandoned; the orphan scanner
        // reclaims them after the grace period.
        //
        // Two comparisons, because a rewrite has two halves and each can lose
        // rows on its own. Both are metadata, so both are free.
        //
        // **Read side.** What the pipeline streamed against what the input
        // manifests said it would read. The count comes from the batches that
        // actually flowed, not from the output files' metadata: a writer that
        // dropped a batch would report its own output consistently, and a check
        // that trusted it would pass.
        let produced = rewritten.rows;
        if let Some(violation) = expected.and_then(|e| e.violated_by(produced)) {
            return Err(Error::refused(
                "compact",
                table_ref,
                format!("{group} {violation}"),
            ));
        }

        // **Write side.** What the output files claim to hold against what was
        // handed to the writer. This is the half the read-side check cannot
        // see: every row can reach the writer and still not reach a file, and
        // the resulting `DataFile`s are what the commit publishes — so if they
        // disagree with the stream that produced them, the commit would publish
        // a number no file backs.
        let persisted: u64 = written.iter().map(|f| f.record_count()).sum();
        if persisted != produced {
            return Err(Error::refused(
                "compact",
                table_ref,
                format!(
                    "{group}: {produced} rows were written but the output files account for \
                     {persisted}; refusing to commit files that do not hold what the rewrite read"
                ),
            ));
        }

        let bytes_written: u64 = written.iter().map(|f| f.file_size_in_bytes()).sum();
        let files_written = written.len();

        // A group whose rows were entirely deleted produces no files. That is a
        // legitimate outcome — the rewrite removes the inputs and adds nothing —
        // and it is how a partition emptied by deletes finally stops being read.
        let mut removed: Vec<String> = tasks.iter().map(|t| t.data_file_path.clone()).collect();
        removed.extend(retirable.iter().cloned());

        let rewrite = RewriteFiles {
            removed,
            added: written,
        };

        let retention = BranchRetention::load(table).await?;
        let producer = SnapshotProducer::new(table, retention);
        let Some((requirements, updates)) = producer.rewrite_files(&rewrite).await? else {
            return Ok(None);
        };

        // The superseded files are announced before the commit that drops them,
        // so an observer sees what a rewrite is about to replace — the same
        // contract the deletion manifest has.
        self.observer
            .deleting_files(self.ctx, &rewrite.removed)
            .await;

        self.committer
            .commit(self.ident, requirements, updates, self.ctx)
            .await?;

        Ok(Some(GroupStats {
            files_removed: tasks.len(),
            deletes_retired: retirable.len(),
            files_written,
            bytes_read,
            bytes_written,
        }))
    }

    /// Remove delete files that apply to no live data file.
    ///
    /// These accumulate whenever a rewrite could not retire a delete file
    /// because it was shared, and the *other* files it applied to were later
    /// rewritten too. Nothing else ever cleans them up, and every scan still
    /// opens each one. Java's `RewriteDataFiles` does the same thing under
    /// `remove-dangling-deletes`.
    async fn drop_dangling_deletes(&self) -> Result<usize> {
        // Reloaded, because the commits above moved the table and the question
        // "does this delete file apply to anything" must be asked of the table
        // as it now is.
        let table = self.loader.reload(self.ident).await?;
        let Some(parent) = table.metadata().current_snapshot().cloned() else {
            return Ok(0);
        };

        let retention = BranchRetention::load(&table).await?;
        let producer = SnapshotProducer::new(&table, retention);

        // The cheap question first, from the manifest list alone. Most tables
        // hold no delete file at all, and for those the whole operation is one
        // small read rather than a second full walk of the table's manifests on
        // every cycle.
        if !producer.has_delete_manifests(&parent).await? {
            return Ok(0);
        }

        let tasks = plan_all(&table).await?;
        let still_applied: HashSet<String> = tasks
            .iter()
            .flat_map(|t| t.deletes.iter())
            .map(|d| normalize(&d.file_path))
            .collect();

        let dangling = producer.dangling_delete_files(&still_applied).await?;
        if dangling.is_empty() {
            return Ok(0);
        }

        let rewrite = RewriteFiles {
            removed: dangling.clone(),
            added: Vec::new(),
        };
        let Some((requirements, updates)) = producer.rewrite_files(&rewrite).await? else {
            return Ok(0);
        };

        self.observer.deleting_files(self.ctx, &dangling).await;
        self.committer
            .commit(self.ident, requirements, updates, self.ctx)
            .await?;

        Ok(dangling.len())
    }
}

/// Whether any file in a chunk carries delete files.
///
/// A single file is worth rewriting on its own only when doing so retires
/// something; otherwise it would be read and written back byte-for-byte.
fn has_deletes(chunk: &[&FileScanTask]) -> bool {
    chunk.iter().any(|t| !t.deletes.is_empty())
}

/// What the manifests say a group's rewrite should produce.
///
/// Two shapes, because metadata supports two strengths of claim:
///
/// - With **no delete file** applying, the rewrite must reproduce every row it
///   read — an equality. Breaking it produces a table that is wrong forever
///   while looking fine.
/// - With deletes applying, the exact figure is unknowable: a delete file's
///   `record_count` is an upper bound (the same row may be named twice, and a
///   positional delete may name a row already gone), so subtracting it would
///   fail honest rewrites. But a rewrite can only ever remove rows, so the input
///   count is a **ceiling** — and a group that exceeded it duplicated data,
///   which is what a scan side executed twice looks like.
///
/// `None` when the scan reported no record count for some input, which happens
/// when a task covers part of a file. Checking a number that was never complete
/// would fail every rewrite of such a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedRows {
    /// Rows in equal rows out.
    Exactly(u64),
    /// Deletes apply, so some rows legitimately disappear — but none may
    /// appear.
    AtMost(u64),
}

impl ExpectedRows {
    /// Why `produced` is wrong, if it is.
    fn violated_by(self, produced: u64) -> Option<String> {
        match self {
            Self::Exactly(expected) if produced != expected => Some(format!(
                "read {expected} rows and wrote {produced}; \
                 refusing to commit a rewrite that changed the row count"
            )),
            Self::AtMost(ceiling) if produced > ceiling => Some(format!(
                "read at most {ceiling} rows and wrote {produced}; \
                 a rewrite can only remove rows, so refusing to commit one that added them"
            )),
            _ => None,
        }
    }
}

fn expected_rows(tasks: &[FileScanTask]) -> Option<ExpectedRows> {
    let total: u64 = tasks.iter().map(|t| t.record_count).sum::<Option<u64>>()?;
    if tasks.iter().any(|t| !t.deletes.is_empty()) {
        Some(ExpectedRows::AtMost(total))
    } else {
        Some(ExpectedRows::Exactly(total))
    }
}

/// Refuse sort columns the table does not have, before anything is read.
///
/// A typo in a policy should cost a metadata check, not a full partition read
/// followed by a failure.
fn check_sort_columns(
    table_ref: &TableRef,
    metadata: &iceberg::spec::TableMetadata,
    columns: &[SortColumn],
) -> Result<()> {
    let schema = metadata.current_schema();
    for column in columns {
        if schema.field_by_name(&column.name).is_none() {
            return Err(Error::refused(
                "compact",
                table_ref,
                format!(
                    "sort column {:?} is not a column of this table",
                    column.name
                ),
            ));
        }
    }
    Ok(())
}

/// Which delete files a group may retire.
///
/// Split out so the rule can be tested directly: it is the one that resurrects
/// deleted rows when it is wrong, and nothing fails visibly when it is.
fn retirable_deletes(in_group: &HashSet<String>, elsewhere: &HashSet<String>) -> Vec<String> {
    let mut out: Vec<String> = in_group.difference(elsewhere).cloned().collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|p| normalize(p)).collect()
    }

    fn job(max_group_bytes: u64, max_input_files: usize) -> Settings {
        Settings {
            target: 1024,
            sort: None,
            memory_budget: u64::MAX,
            max_group_bytes,
            max_input_files,
            small: 768,
            large: 1843,
        }
    }

    fn task(size: u64) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: size,
            start: 0,
            length: size,
            record_count: Some(10),
            data_file_path: format!("s3://b/t/data/{size}-{}.parquet", uuid::Uuid::new_v4()),
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: iceberg::spec::Schema::builder().build().unwrap().into(),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    #[test]
    fn a_file_already_at_target_is_not_read() {
        // The rule that decides what compaction costs. A partition earns a
        // rewrite as a whole; only the files whose size is the problem are
        // read. Without this, a partition that triggers because a third of its
        // files are small has the other two thirds read and written back for
        // nothing — on a real table, the difference between rewriting gigabytes
        // and rewriting terabytes.
        let settings = job(u64::MAX, usize::MAX);

        assert!(settings.is_eligible(&task(10)), "a small file is read");
        assert!(
            settings.is_eligible(&task(5000)),
            "an oversized file is read — nothing else ever splits it"
        );
        assert!(
            !settings.is_eligible(&task(1024)),
            "a file at target gains nothing from being rewritten"
        );
        assert!(
            !settings.is_eligible(&task(768)),
            "the small band is exclusive"
        );
        assert!(
            !settings.is_eligible(&task(1843)),
            "so is the oversized band"
        );
    }

    #[test]
    fn a_file_carrying_deletes_is_read_whatever_its_size() {
        // Its size is not the point: the rewrite is what applies the deletes
        // and lets the delete files be retired.
        let settings = job(u64::MAX, usize::MAX);
        assert!(settings.is_eligible(&with_a_delete(task(1024))));
    }

    #[test]
    fn a_lone_at_target_file_is_not_a_group() {
        // It would be read and written back byte for byte.
        let settings = job(u64::MAX, usize::MAX);
        let ctx_table = crate::policy::TableRef::new("p", ["d"], "t");
        let ctx = OperationContext {
            run_id: "r",
            table: &ctx_table,
            kind: crate::plan::OperationKind::Compact,
            matched_rule: "*",
            reason: "test",
        };
        let committer = NoCommit;
        let job = Job {
            ident: &TableIdent::from_strs(["d", "t"]).unwrap(),
            loader: &NoLoad,
            committer: &committer,
            settings: &settings,
            observer: &crate::obs::NoopObserver,
            ctx,
        };

        let at_target = task(1024);
        let small = task(10);
        let oversized = task(5000);
        let deleted = with_a_delete(task(1024));

        assert!(!job.worth_rewriting(&[]), "an empty group is not a commit");
        assert!(!job.worth_rewriting(&[&at_target]));
        assert!(
            job.worth_rewriting(&[&oversized]),
            "one unsplittable file is worth a commit alone"
        );
        assert!(
            job.worth_rewriting(&[&deleted]),
            "one file with deletes is worth a commit alone"
        );
        assert!(
            job.worth_rewriting(&[&small, &at_target]),
            "more than one file is worth merging"
        );
    }

    #[test]
    fn a_delete_file_shared_with_another_group_is_not_retired() {
        // The rule that matters. This delete file is still hiding rows in a
        // file outside the group; dropping it brings them back.
        let in_group = set(&["s3://b/t/d1.parquet", "s3://b/t/shared.parquet"]);
        let elsewhere = set(&["s3://b/t/shared.parquet"]);

        assert_eq!(
            retirable_deletes(&in_group, &elsewhere),
            vec!["s3://b/t/d1.parquet".to_string()]
        );
    }

    #[test]
    fn a_delete_file_exclusive_to_the_group_is_retired() {
        // Its rows have been applied and written out, so keeping it would make
        // every future read pay for a file that hides nothing.
        let in_group = set(&["s3://b/t/d1.parquet"]);
        assert_eq!(
            retirable_deletes(&in_group, &HashSet::new()),
            vec!["s3://b/t/d1.parquet".to_string()]
        );
    }

    #[test]
    fn retirement_compares_normalized_paths() {
        // The scan reports `s3://`, the manifest may say `s3a://`. Treating
        // them as different files would retire one that is still in use.
        let in_group = set(&["s3://b/t/d1.parquet"]);
        let elsewhere: HashSet<String> = ["s3a://b/t//d1.parquet"]
            .iter()
            .map(|p| normalize(p))
            .collect();
        assert!(retirable_deletes(&in_group, &elsewhere).is_empty());
    }

    #[test]
    fn a_partition_is_packed_into_groups_bounded_by_bytes() {
        // A partition is not a unit of work. Reading an arbitrarily large one
        // in a single pass is how a compactor runs out of memory, and how one
        // conflict throws away hours of rewriting.
        let settings = job(250, usize::MAX);
        let ctx_table = crate::policy::TableRef::new("p", ["d"], "t");
        let ctx = OperationContext {
            run_id: "r",
            table: &ctx_table,
            kind: crate::plan::OperationKind::Compact,
            matched_rule: "*",
            reason: "test",
        };
        let committer = NoCommit;
        let job = Job {
            ident: &TableIdent::from_strs(["d", "t"]).unwrap(),
            loader: &NoLoad,
            committer: &committer,
            settings: &settings,
            observer: &crate::obs::NoopObserver,
            ctx,
        };

        let tasks: Vec<FileScanTask> = (0..5).map(|_| task(100)).collect();
        let refs: Vec<&FileScanTask> = tasks.iter().collect();
        let packed = job.pack(&refs);

        // 100-byte files under a 250-byte ceiling: two per group, then one.
        assert_eq!(packed.len(), 3);
        assert_eq!(packed[0].len(), 2);
        assert_eq!(packed[2].len(), 1);
    }

    #[test]
    fn a_partition_is_also_packed_by_file_count() {
        // The other shape of the same problem: a hundred thousand tiny files
        // fit any byte ceiling and still overwhelm a single pass.
        let settings = job(u64::MAX, 3);
        let ctx_table = crate::policy::TableRef::new("p", ["d"], "t");
        let ctx = OperationContext {
            run_id: "r",
            table: &ctx_table,
            kind: crate::plan::OperationKind::Compact,
            matched_rule: "*",
            reason: "test",
        };
        let committer = NoCommit;
        let job = Job {
            ident: &TableIdent::from_strs(["d", "t"]).unwrap(),
            loader: &NoLoad,
            committer: &committer,
            settings: &settings,
            observer: &crate::obs::NoopObserver,
            ctx,
        };

        let tasks: Vec<FileScanTask> = (0..7).map(|_| task(1)).collect();
        let refs: Vec<&FileScanTask> = tasks.iter().collect();
        let packed = job.pack(&refs);

        assert_eq!(
            packed.iter().map(|g| g.len()).collect::<Vec<_>>(),
            vec![3, 3, 1]
        );
    }

    fn with_a_delete(mut task: FileScanTask) -> FileScanTask {
        task.deletes.push(iceberg::scan::FileScanTaskDeleteFile {
            file_path: "s3://b/t/d.parquet".into(),
            file_size_in_bytes: 1,
            file_type: iceberg::spec::DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
        });
        task
    }

    #[test]
    fn rows_in_equal_rows_out_where_no_delete_applies() {
        let tasks = vec![task(10), task(20)];
        assert_eq!(expected_rows(&tasks), Some(ExpectedRows::Exactly(20)));

        assert!(ExpectedRows::Exactly(20).violated_by(20).is_none());
        assert!(ExpectedRows::Exactly(20).violated_by(19).is_some());
        assert!(ExpectedRows::Exactly(20).violated_by(21).is_some());
    }

    #[test]
    fn where_deletes_apply_the_input_count_is_a_ceiling() {
        // A delete file's record count is an upper bound — the same row may be
        // named twice — so an exact equality would fail every rewrite of a
        // delete-heavy table for a reason that is not a fault. But a rewrite
        // can only ever *remove* rows, so producing more than went in is a
        // fault however many deletes applied.
        let tasks = vec![with_a_delete(task(10)), task(20)];
        assert_eq!(expected_rows(&tasks), Some(ExpectedRows::AtMost(20)));

        assert!(
            ExpectedRows::AtMost(20).violated_by(12).is_none(),
            "deletes legitimately remove rows"
        );
        assert!(ExpectedRows::AtMost(20).violated_by(20).is_none());
        assert!(
            ExpectedRows::AtMost(20).violated_by(21).is_some(),
            "a rewrite that added rows must be refused"
        );
    }

    #[test]
    fn expected_rows_are_unknown_when_a_task_covers_part_of_a_file() {
        let mut tasks = vec![task(10)];
        tasks[0].record_count = None;
        assert_eq!(expected_rows(&tasks), None);
    }

    /// A committer that must never be called.
    #[derive(Debug)]
    struct NoCommit;

    #[async_trait::async_trait]
    impl TableCommitter for NoCommit {
        async fn commit(
            &self,
            _: &TableIdent,
            _: Vec<iceberg::TableRequirement>,
            _: Vec<iceberg::TableUpdate>,
            _: crate::obs::OperationContext<'_>,
        ) -> Result<()> {
            panic!("packing must not commit")
        }
    }

    /// A loader that must never be called.
    #[derive(Debug)]
    struct NoLoad;

    #[async_trait::async_trait]
    impl TableLoader for NoLoad {
        async fn reload(&self, _: &TableIdent) -> Result<Table> {
            panic!("packing must not reload")
        }
    }
}
