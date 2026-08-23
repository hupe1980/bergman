+++
title = "Compaction"
description = "What triggers a rewrite, how delete files are retired, and what Bergman refuses to rewrite."
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
```

## The two triggers

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
why: partition d=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
```

A rewrite that would produce as many files as it consumes is skipped. Without
that check, a table sitting just below target would be rewritten every cycle,
forever, achieving nothing.

### Delete files

Fires when delete records applicable to a partition exceed `delete_ratio` of its
rows — **even when file sizes are fine**.

```text
why: partition d=2026-08-20: 1204 of 5230 rows deleted (23% ≥ 10%) across 12 delete files
```

This is the trigger that matters for streaming and CDC targets. A Flink or
Kafka-Connect sink writes correctly-sized files and a stream of equality deletes;
every read then pays to apply them. File-size metrics say the table is healthy.
It is not.

Any maintenance tool without this trigger is only solving half the problem, and
the half it is not solving is the one that makes Spark run out of memory.

## What a plan tells you

```text
prod.streaming.events_raw
  rule: prod.streaming.events_*
  -> compact
     why: 3 partitions; e.g. d=2026-08-20: 1204 of 5230 rows deleted (23% ≥ 10%) across 12 delete files
     reads 92 files (8.11 GiB), writes ~17 files
```

The plan carries the exact partitions it will rewrite, so `run` acts on what
`plan` displayed rather than re-deciding against a table that has moved since.

The output-file estimate accounts for the delete ratio — rows removed by deletes
do not appear in the output — and is labelled an estimate, because the exact
figure is not knowable without reading the data.

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

```text
  [ok] compact: 3 partitions: 92 files (8.11 GiB) rewritten into 17 (7.94 GiB),
       12 delete files retired
```

## Sequence numbers

A data file carried through a rewrite untouched keeps the sequence number of the
file it replaces. Stamping the new snapshot's number on it would make it look
*newer* than the delete files that should remove its rows — which resurrects
deleted rows.

Newly written files genuinely belong to the new snapshot and take its number.
The distinction is the difference between a correct rewrite and a silent
correctness bug, and it is why the commit layer uses `add_existing_file` for
survivors and `add_file` for replacements.

## What is refused

Bergman declines rather than guessing:

- **A partition holding files under two partition specs.** Rewriting them
  together would write output under the *current* spec while claiming to replace
  files partitioned differently, which silently mis-files rows.
- **A table whose `write.format.default` is not Parquet.** Bergman reads
  Parquet, Avro and ORC but writes only Parquet, and a rewrite must not
  silently change a table's format.
- **A sorted partition larger than the memory budget**, as above.

Both are reported with the reason, per partition, and the rest of the table is
still compacted — each partition commits on its own, so partial progress is real
progress.

## Sort-based clustering

```toml
[rules.compaction]
sort = ["event_date", "customer_id"]
```

Rows are sorted **globally within each partition**, so every output file ends up
with a tight min/max range for the sort columns and a query with a predicate on
them can skip whole files. Sorting each batch independently would leave every
file spanning the whole key range and buy nothing, so Bergman does not do that.

Global sorting means the file group has to be in memory at once. Bergman does
not spill to disk, so a partition larger than the budget is **refused with a
named reason** rather than written unsorted — a table whose metadata claims a
sort order its files do not have is worse than one that failed loudly.

```toml
[rules.compaction]
max_sort_memory = 2147483648   # 2 GiB; defaults to 1 GiB
```

```
compact refused on prod.analytics.events: partition d=2026-08-20 is 3.40 GiB and
sorting needs it in memory, over the 1.00 GiB budget; raise
compaction.max_sort_memory or drop `sort` for this rule
```

Sort columns are checked against the schema before anything is read, so a typo
costs a metadata lookup rather than a full partition read.

Z-ordering is not implemented: it is an optimization rather than table health,
and it comes after bin-packing and sorting are proven.
