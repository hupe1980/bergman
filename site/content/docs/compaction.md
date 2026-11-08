+++
title = "Compaction"
description = "What triggers a rewrite, how file groups are bounded, how delete files are retired, and what Bergman refuses to rewrite."
weight = 8
+++
Compaction rewrites a partition's small files into larger ones, with its delete
files applied — so the surviving rows are written back plainly and the delete
files that hid the rest are retired. The commit removes data files, which
`iceberg-rust` has no transaction action for; Bergman builds it in its
[own commit layer](@/docs/architecture.md#commit-layer).

## Triggers, not schedules {#triggers}

A `schedule` decides when a table is **evaluated**; whether anything runs is
decided by comparing the table against your thresholds. A table already at
target file size costs a few metadata reads and no data I/O.

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

Evaluation is **per partition**, the granularity a rewrite commits at.

### Small files

Fires when the fraction of a partition's files below `min_file_size_ratio` ×
`target_file_size` reaches `small_file_ratio`, **and** at least
`min_input_files` (default 5, as in Spark's `rewrite_data_files`) files are
[eligible](#eligible).

```text
why: partition day=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
```

A rewrite that would produce as many files as it consumes is skipped, so a
table just below target is not rewritten every cycle.

### Oversized files

Fires when any file exceeds `max_file_size_ratio` × `target_file_size` (1.8×,
Spark's default). A file too large to split is a task no reader can divide; the
rolling writer splits it at the target size. One such file is enough, and the
"as many files out as in" check does not apply.

```text
why: partition day=2026-08-20: 1 of 40 files above 900 MiB, which no reader can split
```

### Delete files

Fires when delete records applicable to a partition exceed `delete_ratio` of its
rows — **even when file sizes are fine**. This is the trigger for streaming and
CDC targets, which write well-sized files plus a stream of equality deletes.

```text
why: partition day=2026-08-20: 1204 of 5230 rows deleted (23% ≥ 10%) across 12 delete files
```

### Settle time

`min_file_age` (default one hour) leaves a partition alone until its newest file
is that old, so a rewrite does not lose its commit race to the next micro-batch
of a streaming writer.

```toml
[rules.compaction.trigger]
min_file_age = "2h"   # a busy streamer needs longer than the default
```

A partition whose files carry no timestamp counts as settled.

## What a rewrite reads {#eligible}

A partition *triggers* a rewrite; only its **eligible** files are read:

| Eligible because | Rule |
|---|---|
| It is too small | `size < min_file_size_ratio × target` |
| It is too large to split | `size > max_file_size_ratio × target` |
| A delete file applies to it | whatever its size — the rewrite is what retires the delete |

Files between the two thresholds are left alone — Spark's
`SizeBasedFileRewriter.filterFiles` rule. `bergman plan` reports eligible
figures, which is what the `max_rewrite_bytes_per_run` budget is charged.

## File groups {#file-groups}

Eligible files are bin-packed, smallest first, into groups, and **each group
commits on its own**:

| Setting | Default | Bounds |
|---|---|---|
| `max_group_bytes` | 8 GiB | A few enormous files |
| `max_input_files` | 10 000 | A hundred thousand tiny ones |

So a partition larger than memory is still compactable, and a lost commit costs
one group, not the partition.

```text
  [ok] compact: 92 files (8.11 GiB) rewritten into 17 (7.94 GiB) across 4 groups
       in 3 partitions, 12 delete files retired
```

A group commits when it holds more than one file, an oversized one, or any
delete file. `min_input_files` is a trigger and is evaluated only by the
planner.

### Order and failure bound

Groups run **best value per byte first**, so a cycle cut short — window closing,
byte budget exhausted, process killed — has done its most worthwhile work.

| Term | Is |
|---|---|
| **Cost** | The bytes the group reads and writes, exact from the manifests |
| **Benefit** | `\|inputs − outputs\|`, plus the delete files applying to the group |

Ties break to partition key and index, so an unchanged table is worked in the
same order every run. Each group commits against the snapshot the previous one
produced. After **three groups fail in a row** the table is left for the next
cycle.

## What a plan tells you

```text
prod.streaming.events_raw
  rule: prod.streaming.events_*
  -> compact
     why: 3 partitions; e.g. day=2026-08-20: 1204 of 5230 rows deleted (23% ≥ 10%) across 12 delete files
     reads 92 files (8.11 GiB), writes ~17 files
```

The plan carries the exact partitions it will rewrite, so `run` acts on what
`plan` showed. The output-file count is an estimate that accounts for the delete
ratio.

Partition names are Iceberg's own (`region=eu/day=2026-08-20`, a `day` transform
rendered as a date). The rendering is **injective**, because it decides which
files are rewritten together: `/` and `%` in values are percent-encoded (`%`
first), and a string value `"null"` renders as `%6Eull` so it cannot collide
with a NULL.

## The delete-file rule

A rewrite retires a delete file **only if every data file it applies to is
inside the group**. A shared one is still hiding rows elsewhere, so it stays.
This is why the whole table is planned before anything is rewritten.

### Dangling delete files

A delete file left applying to no live data file is removed after compaction, in
a metadata-only commit (Java's `remove-dangling-deletes`). Removing one that is
*not* dangling resurrects rows, so two independent derivations must agree:

| Derivation | What it says |
|---|---|
| The scan | No live data file is associated with it |
| Its sequence number | It is below every live data file's number in its partition, so it cannot apply to one |

A delete file with no sequence number is kept. An equality delete under an
unpartitioned spec is global and is measured against the whole table's lowest
sequence number. Nothing runs when the manifest list has no delete manifest.

```text
  [ok] compact: 92 files (8.11 GiB) rewritten into 17 (7.94 GiB) across 4 groups
       in 3 partitions, 12 delete files retired, 5 dangling delete files removed
```

## The query engine {#query-engine}

| Stage | Who | Why |
|---|---|---|
| Scan | upstream `ArrowReader` | Vectorized and Iceberg-correct |
| **Positional** deletes | upstream `ArrowReader` | A Parquet row selection: deleted rows are never decoded |
| **Equality** deletes | DataFusion hash anti-join | Upstream evaluates one predicate term per delete row against every batch — `data rows × delete rows`; a hash join is their sum, and spills |
| Sort | DataFusion | Spills |
| Write | upstream `RollingFileWriter` | Real `DataFile`s with full column metrics |

- **The plan shape is asserted.** The join uses `IS NOT DISTINCT FROM`, which
  becomes a hash join only through a conservative optimizer rule; the test suite
  checks the physical plan for `HashJoinExec` with nulls-equal on.
- **Nulls match**, as Iceberg's equality deletes require — SQL `=` would keep
  every row whose key is null.
- **Files are bucketed by which deletes apply to them**, taken from upstream's
  scan, so a file is never anti-joined against a delete that does not apply to it.
- **Delete files keyed on different columns** each get their own anti-join; a
  row survives when it matches none.

```toml
[rules.compaction]
max_sort_memory = 2147483648   # 2 GiB; defaults to 1 GiB
```

`max_sort_memory` is the executor's memory pool; the sort and the join's build
side spill to disk when they reach it. The plan runs at
`limits.compaction_parallelism` threads (default: the machine's available
parallelism). The pool is shared across those threads, so widening the plan
spills sooner rather than using more memory.

DataFusion is behind the default-on `compaction` feature and resolves to the
same `arrow` 58 that `iceberg` 0.11 pins. A build without it has no compaction:

```bash
cargo add bergman --no-default-features --features catalog-rest,storage-s3
# -> expire, manifests, orphans. No DataFusion.
```

## The row count must land in a known interval

Every group checks its row count on both sides and refuses to commit when
either disagrees; the outputs are abandoned and the orphan scanner reclaims them.

| Side | Compares |
|---|---|
| Read | Rows streamed against what the input manifests promised |
| Write | Rows in the output files against rows that reached the writer |

The output must land between two bounds taken from metadata:

| Bound | Is | Breached when |
|---|---|---|
| **Ceiling** | Every row read | The output duplicated data |
| **Floor** | The rows of inputs that **no delete file applies to** | Rows nothing could delete went missing |

With no delete applying, the two coincide and the check is an equality.

## Sequence numbers

A data file carried through a rewritten manifest keeps **its own** sequence
number, so later delete files still apply to it. Newly written files take the
new snapshot's number, which is what retires the deletes applied to their
contents. The commit layer uses `add_existing_file` for survivors and
`add_file` for replacements.

## What is refused

Every refusal names its reason:

- **A partition written under a superseded partition spec**, and **a group
  spanning two specs**. Output is written under the current spec, so the commit
  would mis-file every row. The plan notes such a partition only when it would
  otherwise have been rewritten. Moving data to the current spec is a migration,
  [not maintenance](@/docs/status.md).
- **Anything but Parquet**, read or written. A table whose
  `write.format.default` is not Parquet is refused, and so is any group holding
  a non-Parquet data or delete file, checked from the manifests.
- **An encrypted table** (`encryption.key-id`). Its manifests and manifest lists
  are encrypted and Bergman does not decrypt them. Snapshot expiration still
  runs, because it needs no manifest.
- **A v3 data file without its rows' lineage** — see [below](#format-v3).
- **A rewrite whose row count is wrong**, as above.

Group-level refusals leave the rest of the table compacted. The two that apply
to a whole table — encrypted and non-Parquet — appear as a **note** on the plan.

## Format v3 {#format-v3}

v1, v2 and v3 tables are maintained alike: compaction, manifest rewriting and
dangling-delete removal all run on v3.

### Row lineage

A v3 row has a `_row_id` and a `_last_updated_sequence_number`; a reader that
finds no physical value inherits them from the file's `first_row_id` and
position. Bin-packing moves rows, so a rewrite writes lineage **physically**:

- Each row's `_row_id` and `_last_updated_sequence_number` are read and written
  into the new file as columns. A rewrite is not a change.
- A row with no lineage yet (a null `_row_id`, in a table upgraded from v2) is
  written null and inherits an id from the new file's range.
- New manifest entries leave `first_row_id` null. The manifest list is seeded
  from the table's `next-row-id`, and the snapshot records the range it assigned
  (`first-row-id`, `added-rows`), so the catalog advances `next-row-id` by
  exactly that range.
- Manifest rewriting keeps every entry's `first_row_id`.
- The snapshot producer refuses, with the file named, an added v3 data file that
  lacks the two columns. Bergman's writer always writes them; the check guards
  library callers.

### Deletion vectors

Deletion vectors (`deletion-vector-v1` blobs in Puffin files) are read and
applied like position deletes, and retired under the same
[delete-file rule](#the-delete-file-rule). One Puffin file holds many vectors,
so retiring one removes a manifest entry, never a file, and files are identified
by `(path, offset)`.

A dangling deletion vector is never retired by dangling-delete removal; its
space is reclaimed by orphan removal once nothing references its Puffin file.

## Sort-based clustering

Output is sorted when the rule, `[defaults]`, or the table itself asks:

```toml
[rules.compaction]
sort = ["event_date", "customer_id"]
```

### The table's own sort order {#the-tables-own-sort-order}

A table that declares an Iceberg `sort-order` has it reproduced — direction and
null placement included — so a rewrite does not leave the table claiming a
clustering its files lack.

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

A rule's `sort` takes column names only; direction belongs on the table's
`sort-order`.

A non-identity transform such as `bucket(id, 16)` cannot be reproduced. Bergman
sorts by the leading identity fields and warns:

```text
warning: the table's sort order includes bucket[16](customer_id), which Bergman
cannot reproduce in a rewrite; output is sorted by the fields before it, or not
at all
```

### How the sort runs

Rows are sorted **globally within each file group**, so each output file has a
tight min/max range on the sort columns. The sort spills at `max_sort_memory`.
Sort columns are validated against the schema before anything is read.

Bergman does not Z-order. Rewritten files carry no `sort_order_id`: upstream's
`DataFile` keeps the field crate-private with no setter.

## Output files

Rewritten files honour the table's own settings:

| Property | Used for |
|---|---|
| `write.parquet.compression-codec` | zstd (Iceberg's default), snappy, gzip, lz4, brotli, uncompressed |
| `write.parquet.compression-level` | Codec level, where the codec has one |
| `write.parquet.row-group-size-bytes` | Row-group size |
| `write.parquet.page-size-bytes` | Data page size |
| `write.parquet.page-row-limit` | Rows per data page |
| `write.parquet.dict-size-bytes` | Dictionary page size |
| `write.parquet.bloom-filter-enabled.column.<col>` | Per-column bloom filters |
| `write.parquet.bloom-filter-fpp.column.<col>` | Their false-positive probability |
| `write.target-file-size-bytes` | The size output files roll at |
| `write.data.path` | Where data files go |
| `write.object-storage.enabled` | The hashed object-storage layout, under `write.data.path` when set — upstream's `ObjectStorageLocationGenerator` |
| `write.metadata.path` | Where manifests and manifest lists go |
| *its `sort-order`* | The clustering a rewrite reproduces — see [Sort-based clustering](#sort-based-clustering) |

A malformed value falls back to the default rather than failing the rewrite.
