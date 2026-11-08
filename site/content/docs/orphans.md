+++
title = "Orphan files"
description = "The one operation that can destroy a healthy table, and the eight independent checks standing in front of it."
weight = 7
+++
An orphan is a file under a table's location that no retained metadata
references — left by failed writes, abandoned commits, or expiration that did
not reclaim its files. Removing them is the most dangerous thing Bergman does:
a reachable set missing one live file destroys a table, and object storage has
no undo. This page is the safety model.

## Enable it deliberately, twice

```toml
[[rules]]
match = "prod.analytics.**"
[rules.orphans]
enabled    = true       # the scanner runs
mode       = "delete"   # ...and is allowed to delete
older_than = "7d"
```

`enabled` alone gets you a **report**. Deleting takes a second opt-in.

## The eight checks

Every candidate passes all eight before it is deleted.

### 1. Dry run by default

`mode` defaults to `"dry-run"`:

```text
  [--] remove-orphans: 1,284 orphans totalling 4.20 GiB found in 6,102 objects
       (dry run; set mode = "delete" to remove them)
```

Read one before setting `delete`. A table with 480 live files reporting 6,000
orphans points at your configuration, not your table.

### 2. A grace period with a hard floor {#the-grace-period}

Writers stage data files **before** the commit that references them, so a young
unreferenced file is more likely a live write than garbage. `older_than`
defaults to 7 days, with a **24-hour floor that cannot be configured away**:

```toml
older_than = "1h"    # refused at startup
```

```
policy error: rules[0] (match = "prod.**"): orphans.older_than is 3600s, below
the 86400s floor. Writers stage files before the commit that references them, so
a young unreferenced file is more likely a live write than garbage.
```

The floor is checked at validation **and again in the scanner**, since the
library lets an embedder construct settings directly.

### 3. Unknown age means too young

A file whose store reports no modification time is skipped.

### 4. Segment-wise containment

`…/db/events` and `…/db/events_archive` share a string prefix. Bergman compares
whole path segments, when listing and again per file, after normalizing
spellings, so these name the same object:

- `s3://bucket/wh/t/f.parquet`
- `s3a://bucket/wh/t/f.parquet` (a Hadoop-era writer)
- `s3://bucket/wh/t//f.parquet` (a naive path join)

### 5. No scanning a location another table lives inside

If `s3://lake/wh/db/events` is a table inside `s3://lake/wh/db`, every one of
its live files is inside the outer table's location and unreachable from its
metadata — an orphan by definition, and wrong. Bergman refuses the scan:

```
remove-orphans refused on prod.db.warehouse: table "s3://lake/wh/db/events"
lives inside this table's location "s3://lake/wh/db"; its live files would look
like orphans here. Move one of the two, or exclude this table from orphan
removal.
```

It is checked two ways. The first consults the locations of tables Bergman has
examined this cycle, and can refuse before listing — but misses tables that are
unmatched, skipped, out of `--table` scope or failed to load. The second, which
has to hold, reads the listing itself: a `metadata/….metadata.json` found at
`<location>/<anything>/metadata/…` belongs to another table rooted at
`<location>/<anything>`. It keys on the metadata document, not a directory
name, so a data directory called `metadata` does not trip it.

### 6. Re-verification before deleting

After listing and before deleting, Bergman reloads the metadata, recomputes
reachability, and drops anything a writer made reachable in between:

```text
  [ok] remove-orphans: 1,284 orphans deleted (4.20 GiB) from 6,102 objects,
       3 spared as newly reachable
```

A non-zero "spared" count means tables are being written during maintenance.

### 7. A ceiling on the blast radius

One scan deletes at most `limits.max_deletes_per_run` files — 100 000 by
default — and reports what it withheld:

```text
  [ok] remove-orphans: 100000 orphans deleted (312 GiB) from 6102000 objects,
       48211 left for the next scan by the per-run ceiling of 100000
```

A healthy table does not produce that many orphans, so hitting it is itself a
signal. Deletions run with bounded concurrency (`limits.io_concurrency`). The
ceiling lives under `[limits]` because orphan removal and
[expiration's file cleanup](@/docs/snapshots.md) share one deletion path: apply
the ceiling, write the audit record, delete.

### 8. The listing and the metadata must agree that a file exists

If a listing returns objects and **not one** is a file the metadata references,
checks 1–7 would pass every one of them — yet they are almost certainly live,
and something points at the wrong place: a changed `location`, a bucket alias,
a credential scoped to a subdirectory. A healthy table's own metadata files are
under its location, so zero overlap is refused:

```text
  [refused] remove-orphans: listed 10000 objects under s3://lake/wh/db/events and
            none of the 8412 files this table references were among them; the
            listing and the table's metadata disagree about where its files are
```

The fix is always configuration.

## Two refusals

**A table with a current snapshot but no reachable files** — more likely a
metadata read gone wrong than a table that is all garbage:

```
remove-orphans refused on prod.db.t: table has a current snapshot but no
reachable files; refusing to treat everything under its location as garbage
```

**A reachable set that could not be fully computed.** Any error in the metadata
walk aborts the computation; a partial set would license deleting everything it
failed to read.

## Memory and empty tables

The listing is consumed as a **stream** and reduced to candidates as it
arrives. Orphan removal is the one operation that also runs on a table with no
snapshots, since a first write that died before committing leaves files nothing
else reclaims.

## What counts as reachable

Everything from **all retained snapshots**, not only the current one:

- data and delete files — including Puffin files holding v3 deletion vectors —
  from every manifest of every retained snapshot, **including entries marked
  `DELETED`**, which an older retained snapshot still reads
- manifests and manifest lists
- statistics and partition-statistics files
- every `metadata.json` the metadata log names, plus the current one
- `version-hint.text`, for a table migrated from a Hadoop catalog

A Puffin file stays reachable while any vector in it is referenced.

## Cadence

Orphan removal is **scheduled rather than triggered**: knowing whether a table
has orphans means listing its whole location, and that listing is the cost.

```toml
[rules.orphans]
min_interval = "24h"   # the default
```

The interval applies **within one process**: a one-shot `bergman run` always
scans, and a long-lived `bergman daemon` throttles itself. Nothing is
persisted; a restart costs one extra listing.

## The audit trail

The complete deletion list is written **before the first delete**, so a crash
mid-deletion leaves a record of what was about to go:

```bash
bergman run --audit-log /var/log/bergman.jsonl
```

```json
{"at":"2026-08-23T02:14:07Z","run_id":"6f2c…","table":"prod.analytics.events",
 "operation":"remove-orphans","matched_rule":"prod.analytics.**",
 "reason":"scan s3://lake/warehouse/analytics/events and delete files older than 7d…",
 "result":{"result":"succeeded","detail":"deleting 1284 files"},
 "deleted_files":["s3://lake/warehouse/analytics/events/data/00042-…parquet", …]}
```

## Approval gates

```rust
#[async_trait::async_trait]
impl MaintenanceObserver for RequireSignoff {
    async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
        ctx.kind != OperationKind::RemoveOrphans || self.approved(ctx.table).await
    }
}
```

Returning `false` vetoes the operation, reported as **refused**. See
[Library](@/docs/library.md#observers).

## Relationship to expiration

Upstream's `ExpireSnapshotsAction` does not delete the files it makes
unreachable. Either set `snapshots.delete_files = true` to reclaim them during
expiration, or leave it off and let this scanner reclaim them after the grace
period. Both share the same containment and normalization rules.
