+++
title = "Compaction"
description = "What triggers a rewrite, how file groups are bounded, how delete files are retired, and what Bergman refuses to rewrite."
weight = 8
+++
Compaction rewrites a partition's small files into larger ones, with its delete
files applied — so the rows that survive are written back plainly and the delete
files that hid the rest are retired.

Committing that means committing a snapshot which *removes* data files, and
`iceberg-rust` has no transaction action for it. Bergman
[owns that layer](@/docs/status.md#why-bergman-owns-its-commit-layer) rather
than forking, which is what the rest of the market did.

## Triggers, not schedules {#triggers}

Rewriting data you did not need to rewrite is the main failure mode of naive
compaction: write amplification can easily exceed the scan savings it buys. So
compaction is triggered by measurement, not by a clock.

A `schedule` decides when a table is **evaluated**. Whether anything runs is
decided by comparing that table against your thresholds. A table already at
target file size costs a handful of metadata reads and no data I/O at all —
which is what makes it safe to evaluate thousands of tables often.

```toml
[[rules]]
match    = "prod.analytics.**"
schedule = "0 */2 * * *"        # evaluate every two hours

[rules.compaction]
enabled          = true
target_file_size = 536870912    # or inherit write.target-file-size-bytes

[rules.compaction.trigger]
small_file_ratio    = 0.3
min_input_files     = 5
delete_ratio        = 0.1
min_file_size_ratio = 0.75
min_file_age        = "1h"
```

## The three triggers

Evaluation is **partition-grained**, because that is the granularity a rewrite
commits at — and because a table can look perfectly healthy on average while one
partition is a thousand tiny files.

### Small files

Fires when the fraction of a partition's files below the small-file threshold
(`min_file_size_ratio` × `target_file_size`) reaches `small_file_ratio`, **and**
there are at least `min_input_files` of them.

`min_input_files` defaults to 5, matching Spark's `rewrite_data_files`, so
behaviour is unsurprising to anyone already running Iceberg maintenance. Four
tiny files are not worth a commit.

```text
why: partition day=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
```

A rewrite that would produce as many files as it consumes is skipped. Without
that check, a table sitting just below target would be rewritten every cycle,
forever, achieving nothing.

### Delete files

Fires when delete records applicable to a partition exceed `delete_ratio` of its
rows — **even when file sizes are fine**.

```text
why: partition day=2026-08-20: 1204 of 5230 rows deleted (23% ≥ 10%) across 12 delete files
```

This is the trigger that matters for streaming and CDC targets. A Flink or
Kafka-Connect sink writes correctly-sized files and a stream of equality deletes;
every read then pays to apply them. File-size metrics say the table is healthy.
It is not.

Any maintenance tool without this trigger is only solving half the problem, and
the half it is not solving is the one that makes Spark run out of memory.

Applying them is an anti-join, and Bergman does it as one — see
[Where the query engine earns its place](#query-engine).

### Settle time

`min_file_age` (default one hour) is the trigger that works *backwards*: a
partition is left alone until its newest file has stopped being new.

This is the guard against fighting your own writer. A streaming target commits
to one partition continuously. A rewrite of that partition reads it, writes it,
and then loses its compare-and-swap to the micro-batch that landed in the
meantime — repeatedly, every cycle, having each time spent the full cost of the
rewrite. Waiting is strictly cheaper than competing.

```toml
[rules.compaction.trigger]
min_file_age = "2h"   # a busy streamer needs longer than the default
```

A partition whose files carry no timestamp counts as settled. Failing the other
way would leave a table Bergman cannot date un-maintained forever, which is
worse than one lost commit race.

## File groups {#file-groups}

**A partition is not a unit of work.** Its eligible files are bin-packed —
smallest first — into groups bounded by two ceilings, and each group commits on
its own:

| Setting | Default | Bounds |
|---|---|---|
| `max_group_bytes` | 8 GiB | A few enormous files |
| `max_input_files` | 10 000 | A hundred thousand tiny ones |

This is the equivalent of Spark's `max-file-group-size-bytes`, sized for one
process rather than a cluster, and it exists for the same reasons. Reading an
arbitrarily large partition in a single pass is how a compactor runs out of
memory. And without it, one lost commit throws away the whole partition's
rewriting rather than one group's.

Partial progress is real progress. It is what makes compacting an
actively-written table tractable at all.

```text
  [ok] compact: 92 files (8.11 GiB) rewritten into 17 (7.94 GiB) across 4 groups
       in 3 partitions, 12 delete files retired
```

Smallest-first packing means a group fills with the files that most need
merging, and a run cut short leaves the largest files alone — they were closest
to target anyway.

## What a plan tells you

```text
prod.streaming.events_raw
  rule: prod.streaming.events_*
  -> compact
     why: 3 partitions; e.g. day=2026-08-20: 1204 of 5230 rows deleted (23% ≥ 10%) across 12 delete files
     reads 92 files (8.11 GiB), writes ~17 files
```

The plan carries the exact partitions it will rewrite, so `run` acts on what
`plan` displayed rather than re-deciding against a table that has moved since.

The output-file estimate accounts for the delete ratio — rows removed by deletes
do not appear in the output — and is labelled an estimate, because the exact
figure is not knowable without reading the data.

Partition names are Iceberg's own: `region=eu/day=2026-08-20`, with a `day`
transform rendered as a date rather than as the integer actually stored. Values
containing `/` or `%` are percent-encoded, because this string is compared for
equality to decide which files are rewritten together — two partitions that
rendered alike would have their files merged and written out under one partition
value.

## The delete-file rule

This is the part that loses data when it is wrong, and nothing fails visibly
when it is.

A rewrite may retire a delete file **only if every data file that delete file
applies to is inside the group being rewritten**. One shared with a file outside
the group is still hiding rows there, and dropping it brings them back.

Bergman therefore plans the *whole table* before rewriting anything: the
question "does this delete file apply anywhere else?" cannot be answered from
inside the group. A delete file exclusive to the group is retired; a shared one
stays, and the data files it applies to inside the group remain correct because
it stayed.

### Dangling delete files

The rule above has a consequence: a shared delete file survives the rewrite —
and then the *other* files it covered get rewritten too, leaving it applying to
nothing at all. It is now pure read overhead. Every scan still opens it and it
hides nothing, and nothing else ever cleans it up.

So after compacting, Bergman reloads the table, asks which delete files the scan
still associates with a live data file, and commits the removal of the rest.
Java's `RewriteDataFiles` does the same thing under `remove-dangling-deletes`.
It is a metadata-only commit and it is close to free.

```text
  [ok] compact: 92 files rewritten into 17, 12 delete files retired,
       5 dangling delete files removed
```

## Where the query engine earns its place {#query-engine}

Bergman uses DataFusion only for the stages that need it:

| Stage | Who | Why |
|---|---|---|
| Scan | upstream `ArrowReader` | Already vectorized and Iceberg-correct |
| **Positional** deletes | upstream `ArrowReader` | They become a Parquet row selection, so deleted rows are never decoded — *better* than a join |
| **Equality** deletes | DataFusion hash anti-join | See below |
| Sort | DataFusion | Spills |
| Write | upstream `RollingFileWriter` | Real `DataFile`s with full column metrics |

Equality deletes are why the dependency exists. Upstream applies them by
building one predicate term *per delete row* and evaluating that tree against
every record batch: `data rows × delete rows`, a nested-loop anti-join. At a
hundred thousand delete rows a partition takes hours. A hash anti-join is
proportional to their sum, and spills — and the tables that need compaction
most are exactly the delete-heavy ones.

```toml
[rules.compaction]
max_sort_memory = 2147483648   # 2 GiB; defaults to 1 GiB
```

`max_sort_memory` is the executor's memory pool. The sort and the join's build
side spill to disk when they reach it, so no group is too large to sort.

DataFusion sits behind the default-on `compaction` feature and adds ~70 crates
to a tree that already carries Arrow, Parquet and tokio — it resolves to the
same `arrow` 58 that `iceberg` 0.10 pins, so batches pass with no conversion.
An embedder wanting only metadata maintenance carries no query engine:

```toml
bergman = { version = "0.1", default-features = false,
            features = ["catalog-rest", "storage-s3"] }
# -> expire, manifests, orphans. No DataFusion.
```

The feature gates the **operation**, never a second implementation of it: a
build without it has no compaction rather than a slower compaction.

### Two rules that are silent when broken

**Files are bucketed by which deletes apply to them.** An equality delete
applies to a data file only when its sequence number is greater, and upstream's
scan has already worked that out. So one group can hold files with different
delete sets — the ordinary CDC shape — and anti-joining a file against a delete
that does not apply to it would remove live rows.

**Nulls match.** Iceberg's equality deletes match nulls; SQL `=` does not. The
join uses `IS NOT DISTINCT FROM`, because `=` would silently keep every row
whose delete key is null.

## Rows in equal rows out

Every group compares the record count it wrote against what the manifests said
it read, and **refuses to commit when the comparison fails**. The outputs are
abandoned; the orphan scanner reclaims them after the grace period.

A rewrite that silently lost rows is indistinguishable from one that worked, and
the table it produces is wrong forever. The check costs nothing — both numbers
come from metadata — and it is the only thing standing between a reader bug and
a permanently wrong table.

Metadata supports two different strengths of claim, and the check makes both:

| Situation | The claim | Refused when |
|---|---|---|
| No delete file applies | Rows in equal rows out | The counts differ at all |
| Deletes apply | The input count is a **ceiling** | More rows came out than went in |

Where deletes apply the exact figure is not knowable from metadata: a delete
file's `record_count` is an upper bound, since the same row may be named twice
and a positional delete may name a row already gone. Subtracting it would fail
honest rewrites. But a rewrite can only ever *remove* rows, so the input count
still bounds the output — and a group that produced more than it read has
duplicated data. That is not theoretical: a plan whose scan side executed twice
looks exactly like it.

## Sequence numbers

A data file carried through a rewritten manifest untouched keeps **its own**
sequence number. Stamping the new snapshot's number on it would make it look
*newer* than the delete files that should remove its rows — which resurrects
deleted rows.

Newly written files are the opposite case: they genuinely belong to the new
snapshot and take its number, which is higher than every delete file already
applied to their contents. That is exactly what retires those deletes.

The distinction is the difference between a correct rewrite and a silent
correctness bug, and it is why the commit layer uses `add_existing_file` for
survivors and `add_file` for replacements.

## What is refused

Bergman declines rather than guessing, and every refusal names its reason:

- **A partition written under a superseded partition spec.** Output goes out
  under the table's *current* spec, so a commit claiming it replaces files
  partitioned differently mis-files every row in it. The planner does not plan
  these and the executor refuses them.
- **A group spanning two partition specs**, for the same reason.
- **A table whose `write.format.default` is not Parquet.** Bergman reads
  Parquet, Avro and ORC but writes only Parquet, and a rewrite must not
  silently change a table's format.
- **An Iceberg format v3 table.** Its row lineage cannot survive a rewrite
  Bergman performs, and a v3 snapshot Bergman could author would be rejected by
  `iceberg-rust` outright — see [below](#format-v3).
- **A rewrite whose row count is wrong**, as above.

Each is reported per group, and the rest of the table is still compacted — every
group commits on its own. The two that apply to a whole table — a v3 table, and
a non-Parquet one — appear as a **note** on the plan rather than as a per-cycle
failure, so `bergman plan` says why nothing will happen instead of reporting a
fragmented table as healthy.

## Format v3 {#format-v3}

Bergman rewrites v1 and v2 tables. A **v3** table is refused, with a reason.

v3 introduces row lineage: every row carries a `_row_id` and a
`_last_updated_sequence_number`, a snapshot must declare the `first-row-id` it
starts from and how many rows it added, and each manifest carries the base its
entries count from. Three things follow, and each on its own is disqualifying:

1. A rewrite must carry every row's existing `_row_id` through to the file
   replacing it. Upstream's reader does not project the field and its
   `RollingFileWriter` will not accept it, so a rewrite would **renumber every
   row it touched** — and a `MERGE` or a CDC consumer joining on row id would
   then match the wrong rows, with nothing failing.
2. A manifest holding *existing* files, written fresh, has no `first-row-id` of
   its own, so the manifest-list writer assigns it a new range — moving files
   that were never rewritten to row ids they never had.
3. `iceberg-rust`'s `TableMetadataBuilder::add_snapshot` rejects a v3 snapshot
   carrying no `first-row-id`. Such a commit does not merely risk being wrong:
   **against a catalog applying commits through that crate it does not apply at
   all.**

The refusal is scoped to what Bergman authors itself, so it covers compaction,
dangling-delete removal and manifest rewriting. **Snapshot expiration and orphan
removal still run** — the first is upstream's own action and the second commits
nothing — so a v3 table's history stays bounded and its storage still gets
reclaimed.

```text
prod.streaming.events
  rule: prod.streaming.*
  note: compaction is enabled but cannot run: the table is Iceberg format v3,
        whose row lineage Bergman cannot yet preserve through a rewrite;
        snapshot expiration and orphan removal still run
  -> expire-snapshots
     why: oldest snapshot is 30d old (> 7d), 41 snapshots retained (keeping at least 3)
```

## Sort-based clustering

Output is sorted when the rule asks, when `[defaults]` asks, or — and this is
the case that matters most — **when the table itself asks**:

```toml
[rules.compaction]
sort = ["event_date", "customer_id"]
```

### The table's own sort order {#the-tables-own-sort-order}

A table that declares an Iceberg `sort-order` has writers that honour it.
Bin-packing those files back together *unsorted* would leave the table claiming
a clustering its files no longer have, and every query with a predicate on the
sort columns would start reading every file — maintenance making the table
slower, silently.

So the sort resolves through the usual layers, with the table as layer 3:

| Layer | Source |
|---|---|
| rule | `[rules.compaction] sort = [...]` |
| defaults | `[defaults.compaction] sort = [...]` |
| **the table** | its `sort-order`, direction and null placement included |
| — | unsorted |

```text
 SETTING             VALUE                        FROM
 compaction.sort     event_date, customer_id desc the table's sort order
```

Direction and null placement are reproduced exactly: writing the rows back
ascending when the table asked for descending would be a different clustering
wearing the table's name.

A rule's `sort` is only column names, on purpose. Direction belongs on the
table's `sort-order`, where every other Iceberg tool reads it; a rule is the
escape hatch for a table that declares no order at all.

**Transforms Bergman cannot reproduce.** A field sorted by `bucket(id, 16)`
orders rows by a value that is not in the file. Sort order is lexicographic, so
such a field does not simply drop out — everything after it becomes meaningless
too. Bergman keeps the leading run of identity-transform fields, which is a
coarser version of exactly the table's ordering, and warns about the rest:

```text
warning: the table's sort order includes bucket[16](customer_id), which Bergman
cannot reproduce in a rewrite; output is sorted by the fields before it, or not
at all
```

### How the sort runs

Rows are sorted **globally within each file group**, so every output file ends
up with a tight min/max range for the sort columns and a query with a predicate
on them can skip whole files. Sorting each batch independently would leave every
file spanning the whole key range and buy nothing.

The sort spills to disk when it reaches `max_sort_memory`, so a large group is
sorted rather than refused. Sort columns are checked against the schema before
anything is read, so a typo costs a metadata lookup rather than a full partition
read.

Z-ordering is not implemented: it is an optimization rather than table health,
and it comes after bin-packing and sorting are proven.

> [!NOTE]
> Rewritten files do not carry a `sort_order_id`. Upstream's `DataFile` keeps
> the field crate-private with no setter, so Bergman cannot stamp it — the rows
> are clustered and their bounds are tight, but a reader has to discover that
> from the statistics rather than being told. It is the natural thing to
> contribute upstream.

## Output files

Rewritten files honour the table's own Parquet settings rather than Bergman's
opinion, so a rewrite does not silently re-encode a table differently from every
other writer:

| Property | Used for |
|---|---|
| `write.parquet.compression-codec` | zstd (Iceberg's default), snappy, gzip, lz4, brotli, uncompressed |
| `write.parquet.compression-level` | Codec level, where the codec has one |
| `write.parquet.row-group-size-bytes` | Row-group size |
| `write.parquet.page-size-bytes` | Data page size |
| `write.target-file-size-bytes` | The size output files roll at |
