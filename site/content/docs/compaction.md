+++
title = "Compaction"
description = "What triggers a rewrite, why triggers beat schedules, and why Bergman plans compaction it cannot yet execute."
weight = 8
+++
> [!WARNING]
> Compaction is **planned and reported but not executed**. Committing a rewrite
> means removing data files, and `iceberg-rust` has no transaction action that
> does. See [Status](@/docs/status.md#why-compaction-does-not-execute) for the
> detail. Everything on this page about *measurement and triggering* works
> today; only the commit is missing.

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
  !! compact
     why: 3 partitions; e.g. d=2026-08-20: 1204 of 5230 rows deleted (23% ≥ 10%) across 12 delete files
     reads 92 files (8.11 GiB), writes ~17 files
     BLOCKED: compaction needs a commit that removes data files; …
```

The output-file estimate accounts for the delete ratio — rows removed by deletes
do not appear in the output — and is labelled an estimate, because the exact
figure is not knowable without reading the data.

## Seeing the problem without fixing it

Until the commit path lands upstream, `inspect` and `plan` are still the fastest
way to find out how bad the problem is and where:

```bash
bergman inspect --format json \
  | jq -r '.[] | select(.files.data_file_count > 100)
           | "\(.table) \(.files.data_file_count) files, avg \(.files.data_bytes / .files.data_file_count | floor)"'
```

and then fix it with the engine you already have:

```sql
CALL prod.system.rewrite_data_files(table => 'analytics.events');
```

The measurement is the part Bergman does better than a query engine — it is
metadata-only, it is partition-grained, and it costs nothing to run against your
whole catalog.

## Sort-based clustering

```toml
[rules.compaction]
sort = ["event_date", "customer_id"]
```

Accepted, validated, and reported in plans. It rides on the same blocked commit
path, so it does not execute either. Z-ordering is not implemented at all: it is
an optimization rather than table health, and it comes after bin-packing and
sorting are proven.
