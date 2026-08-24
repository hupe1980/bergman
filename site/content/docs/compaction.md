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
min_file_size_ratio = 0.75      # below this x target -> small
max_file_size_ratio = 1.8       # above this x target -> unsplittable
min_file_age        = "1h"
```

## The four triggers

Evaluation is **partition-grained**, because that is the granularity a rewrite
commits at — and because a table can look perfectly healthy on average while one
partition is a thousand tiny files.

### Small files

Fires when the fraction of a partition's files below the small-file threshold
(`min_file_size_ratio` × `target_file_size`) reaches `small_file_ratio`, **and**
at least `min_input_files` files are [eligible](#eligible).

`min_input_files` defaults to 5, matching Spark's `rewrite_data_files`, so
behaviour is unsurprising to anyone already running Iceberg maintenance. Four
tiny files are not worth a commit.

```text
why: partition day=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
```

A rewrite that would produce as many files as it consumes is skipped. Without
that check, a table sitting just below target would be rewritten every cycle,
forever, achieving nothing.

### Oversized files

Fires when any file exceeds `max_file_size_ratio` × `target_file_size` — 1.8×
by default, which is Spark's own.

This is the half a small-file-only compactor forgets, and it fails in the
opposite direction. One file too large to split is a task no reader can divide,
so a single query pays for all of it on one thread. Nothing but a rewrite ever
splits it, and a rewrite does so for free: the rolling writer rolls at the
target size however large the input was.

```text
why: partition day=2026-08-20: 1 of 40 files above 900 MiB, which no reader can split
```

One such file is enough — there is no minimum count to reach — and the "as many
files out as in" check above does not apply to it, because producing more files
than it consumed is the entire point.

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

## What a rewrite actually reads {#eligible}

A partition is what *triggers* a rewrite. It is not what the rewrite reads.

Take a partition of a hundred half-gigabyte files plus forty tiny ones: it
crosses `small_file_ratio`, but reading all hundred and forty to merge the forty
would spend fifty gigabytes of I/O to reclaim a few hundred megabytes. So a file
is **eligible** only when a rewrite would improve it:

| Eligible because | Rule |
|---|---|
| It is too small | `size < min_file_size_ratio × target` |
| It is too large to split | `size > max_file_size_ratio × target` |
| A delete file applies to it | whatever its size — the rewrite is what retires the delete |

Everything between the two thresholds is left alone. This is Spark's
`SizeBasedFileRewriter.filterFiles` rule, so a table maintained by both tools is
judged the same way.

`bergman plan` reports the eligible figures rather than the partition's totals,
because that is what the cycle's `max_rewrite_bytes_per_run` budget is charged.

## File groups {#file-groups}

**A partition is not a unit of work either.** Its eligible files are bin-packed —
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

A group earns its commit when it holds more than one file, or an oversized one,
or any delete file. `min_input_files` is deliberately not re-applied here: it is
a trigger, and triggers are the planner's. A threshold evaluated in two places is
one that can drift, and the half that drifted would make `bergman plan` a promise
`bergman run` quietly broke.

### How groups are ordered and bounded

Groups run **most-consolidation-first**: ranked by how many files each removes,
ties breaking toward the cheaper group and then the partition key, so two runs of
an unchanged table do the same work in the same order. A cycle that ends early
has then done its most valuable work first. Spark calls this
`rewrite-job-order`.

Each group commits against the snapshot the previous one produced, so no group
loses its compare-and-swap to its own predecessor and rewrites itself twice.

After **three groups fail in a row**, the table is left for the next cycle. A
failing group has already read and written its files by the time it fails, and
three in a row means the reason is the table rather than the group — a busy
writer, a credential that stopped working, a catalog refusing commits. Spark
bounds the same thing with `partial-progress.max-failed-commits`.

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
transform rendered as a date rather than as the integer actually stored.

The rendering is **injective**, and that is the point: this string is compared
for equality to decide which files are rewritten together, so two partitions that
rendered alike would have their files merged and written out under one of the two
partition values — filing every row of the other where it does not belong.
Nothing fails; queries just start missing rows. Two collisions are closed for it:

- Values containing `/` or `%` are percent-encoded, `%` first, so a value holding
  a separator cannot impersonate two fields.
- A NULL value renders as `null` — and a *string value* of `"null"` would render
  identically, so that one is escaped to `%6Eull`. No real value can reach that
  spelling, because a genuine `%` has already become `%25`.

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

So after compacting, Bergman reloads the table and removes them. Java's
`RewriteDataFiles` does the same thing under `remove-dangling-deletes`. It is a
metadata-only commit and close to free.

Removing a delete file that is *not* dangling resurrects every row it was hiding,
so the question is answered **two independent ways, and both must agree**:

| Derivation | What it says |
|---|---|
| The scan | No live data file is associated with it |
| Its sequence number | It is below every live data file's number in its partition, so it cannot apply to one |

A delete file with no sequence number is kept — an unknown number cannot argue
that a file is dead. An equality delete stored under an unpartitioned spec is a
*global* delete, so it is measured against the whole table's lowest sequence
number rather than one partition's.

None of this runs when the manifest list shows no delete manifest, which is the
ordinary table.

```text
  [ok] compact: 92 files (8.11 GiB) rewritten into 17 (7.94 GiB) across 4 groups
       in 3 partitions, 12 delete files retired, 5 dangling delete files removed
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

**The plan shape is asserted, not assumed.** `IS NOT DISTINCT FROM` only becomes
a hash join because an optimizer rule rewrites it, and that rule is deliberately
conservative — a plan that grew an ordinary `=` alongside would fall back to a
nested loop and restore the quadratic behaviour silently. The test suite builds
the physical plan and checks for `HashJoinExec` with nulls-equal on.

**Delete files may match on different columns**, which is legal and happens when
a schema change moves a sink's primary key. Anti-joins compose — a row survives
when it matches none of them — so a group carrying several key sets gets one
join per set.

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

```bash
cargo add bergman --no-default-features --features catalog-rest,storage-s3
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
join uses `IS NOT DISTINCT FROM`, because `=` would keep every row whose delete
key is null — forever, with nothing failing.

## Rows in equal rows out

Every group checks its row count on **both sides**, and refuses to commit when
either disagrees. The outputs are abandoned; the orphan scanner reclaims them
after the grace period.

| Side | Compares | Catches |
|---|---|---|
| Read | What the pipeline streamed against what the input manifests promised | A read that lost rows |
| Write | What the output files hold against what reached the writer | A write that lost rows |

Every number involved is metadata, so both checks are free. A rewrite that
silently lost rows is indistinguishable from one that worked, and the table it
produces is wrong forever.

The read side supports two strengths of claim:

| Situation | The claim | Refused when |
|---|---|---|
| No delete file applies | Rows in equal rows out | The counts differ at all |
| Deletes apply | The input count is a **ceiling** | More rows came out than went in |

Where deletes apply the exact figure is not knowable from metadata: a delete
file's `record_count` is an upper bound, since the same row may be named twice
and a positional delete may name a row already gone. But a rewrite can only ever
*remove* rows, so the input count still bounds the output — and a group that
produced more than it read has duplicated data.

## Sequence numbers

A data file carried through a rewritten manifest untouched keeps **its own**
sequence number. Stamping the new snapshot's number on it would make it look
*newer* than the delete files that should remove its rows — which resurrects
deleted rows.

Newly written files are the opposite case: they genuinely belong to the new
snapshot and take its number, which is higher than every delete file already
applied to their contents. That is exactly what retires those deletes.

This is why the commit layer uses `add_existing_file` for survivors and
`add_file` for replacements.

## What is refused

Bergman declines rather than guessing, and every refusal names its reason:

- **A partition written under a superseded partition spec.** Output goes out
  under the table's *current* spec, so a commit claiming it replaces files
  partitioned differently mis-files every row in it. The planner does not plan
  these and the executor refuses them.

  The plan carries a note when one of them would otherwise have been rewritten,
  and only then — a spec-evolved table keeps every old partition it ever had,
  and a warning that fires when nothing is wrong is one nobody reads when
  something is. Moving them to the current spec is a migration, which is
  [not maintenance](@/docs/status.md).
- **A group spanning two partition specs**, for the same reason.
- **Anything but Parquet, read or written.** Bergman handles Parquet in both
  directions and only Parquet: upstream's `ArrowReader` opens a Parquet stream
  whatever a scan task's format field says, and the writer is
  `ParquetWriterBuilder`. So a table declaring `write.format.default = "orc"` is
  refused rather than converted behind its owner's back — and every file a group
  would read is checked too, because that property describes the *next* file a
  writer produces, not the ones already there. A migrated table can hold Avro
  data files while declaring Parquet, and reading one as Parquet fails with a
  missing footer where a named refusal belongs. Delete files are checked the same
  way, from the manifests, because an equality delete that fails to parse is one
  whose rows would quietly not be removed.
- **An Iceberg format v3 table.** Its row lineage cannot survive a rewrite
  Bergman performs, and a v3 snapshot Bergman could author would be rejected by
  a field the spec requires — see [below](#format-v3).
- **A rewrite whose row count is wrong**, as above.

Each is reported per group, and the rest of the table is still compacted — every
group commits on its own. The two that apply to a whole table — a v3 table, and
a non-Parquet one — appear as a **note** on the plan rather than as a per-cycle
failure, so `bergman plan` says why nothing will happen instead of reporting a
fragmented table as healthy.

## Format v3 {#format-v3}

Bergman rewrites v1 and v2 tables. A **v3** table is refused, with a reason —
and the reason is a gap, not an impossibility.

Compaction of a v3 table is perfectly possible while spec-compliant. The spec
says how: when a row moves to a different data file, its existing `_row_id`
"must be copied into the new data file", and its `_last_updated_sequence_number`
with it if the row was not modified. Iceberg's Java implementation does this —
Spark since 1.10.0 ([#13555](https://github.com/apache/iceberg/pull/13555)),
Flink since [#14149](https://github.com/apache/iceberg/pull/14149).

What Bergman lacks is the capability, and the gap is in the *read* path.
`iceberg-rust` reserves the field ids but populates nothing: the reader resolves
a projected id against the table schema, so `_row_id` fails with "field not
found" even when the column is there, and `FileScanTask` carries neither the
file's `first_row_id` nor its data sequence number, so the inherited values
cannot be computed either.

Hand-rolling it meanwhile would mean replacing both ends — upstream's reader, to
recover `_pos` through a positional-delete row selection, and its
`ParquetWriter`, whose `DataFile` output carries column sizes, value counts and
per-column bounds Bergman would then have to compute itself. That is a fork in
all but name. The upstream fix is far smaller: two fields on `FileScanTask`, and
reader and writer support for two field ids the crate has already reserved.

It refuses, because the alternative fails silently. Three things go wrong:

1. Without the projection, output files carry no `_row_id`, so readers fall
   back to inheritance — `first_row_id + _pos` of the *new* file. Every row the
   rewrite touched is **renumbered**, and a `MERGE` or a CDC consumer joining on
   row id starts matching the wrong rows, with nothing failing.
2. A manifest holding *existing* files, written fresh, has no `first-row-id` of
   its own, so the manifest-list writer assigns it a new range — moving files
   that were never rewritten to row ids they never had.
3. The snapshot would be **invalid**. The spec lists `first-row-id` and
   `added-rows` as *required* fields of a v3 snapshot — "required even if a
   commit does not assign any ID space". `iceberg-rust` enforces it, so such a
   commit is refused outright by any catalog applying updates through it.

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
