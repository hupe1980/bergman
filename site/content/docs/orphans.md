+++
title = "Orphan files"
description = "The one operation that can destroy a healthy table, and the five independent checks standing in front of it."
weight = 7
+++
An orphan is a file under a table's location that no retained metadata
references. They accumulate from failed writes, abandoned commits, and
maintenance that expired snapshots without reclaiming what they held.

Removing them is the single most dangerous thing Bergman does. A reachable set
missing one live file is a reachable set that destroys a table, and object
storage has no undo. This page is the safety model.

## Enable it deliberately, twice

```toml
[[rules]]
match = "prod.analytics.**"
[rules.orphans]
enabled    = true       # the scanner runs
mode       = "delete"   # ...and is allowed to delete
older_than = "7d"
```

`enabled` alone gets you a **report**. Deleting takes a second, explicit opt-in.
That is not ceremony: the default has to be the safe one, because an operator
who forgets to say should get a list, not an incident.

## The five checks

Every candidate passes all five before it is deleted. Each exists because of a
specific way this goes wrong.

### 1. Dry run by default

`mode` defaults to `"dry-run"`, which reports what would go:

```text
  [--] remove-orphans: 1,284 orphans totalling 4.20 GiB found in 6,102 objects
       (dry run; set mode = "delete" to remove them)
```

Read one of these before you ever set `delete`. The count and the size are the
two numbers that tell you whether the reachable set looks sane — if a table with
480 live files reports 6,000 orphans, something is wrong with your configuration,
not with your table.

### 2. A grace period with a hard floor {#the-grace-period}

Writers stage data files **before** the commit that references them. Between
those two moments the file is on disk and unreferenced — indistinguishable, to a
scanner, from garbage. Deleting it corrupts a table that was doing nothing wrong.

`older_than` defaults to 7 days and is bounded below by a **24-hour floor that
cannot be configured away**:

```toml
older_than = "1h"    # refused at startup
```

```
policy error: rules[0] (match = "prod.**"): orphans.older_than is 3600s, below
the 86400s floor. Writers stage files before the commit that references them, so
a young unreferenced file is more likely a live write than garbage.
```

The floor is checked when configuration is validated **and again in the scanner
itself**, because the library API lets an embedder construct settings directly.
A safety rule enforced at one of two entry points is a safety rule with a hole in
it.

### 3. Unknown age means too young

If the object store does not report a modification time, the file is skipped. A
store that will not say how old a file is cannot be used to argue that it is old
enough to delete.

### 4. Segment-wise containment

`…/db/events` and `…/db/events_archive` share a string prefix, and object stores
match prefixes as raw strings. A containment check that missed the difference
would let one table's maintenance offer another table's live files for deletion.

Bergman compares whole path segments, both when listing and again per file. It
also normalizes spellings first, so all of these name the same object:

- `s3://bucket/wh/t/f.parquet`
- `s3a://bucket/wh/t/f.parquet` (what a Hadoop-era writer produces)
- `s3://bucket/wh/t//f.parquet` (a naive path join)

A live file spelled differently from its metadata would otherwise look exactly
like garbage.

### 5. Re-verification before deleting

Listing a large table takes long enough for a writer to commit, and any file that
commit referenced is now live. So after listing and before deleting, Bergman
reloads the table's metadata, recomputes reachability, and drops anything that
became reachable in between:

```text
  [ok] remove-orphans: 1,284 orphans deleted (4.20 GiB) from 6,102 objects,
       3 spared as newly reachable
```

A non-zero "spared" count is the check doing its job, and worth noticing — it
means your tables are being written during maintenance windows.

## Two refusals

Beyond the five checks, the scanner declines outright in two situations.

**A table with a current snapshot but no reachable files.** Far more likely than
"this table is entirely garbage" is that something went wrong reading its
metadata. Deleting on that basis would empty the warehouse, so it is refused and
reported:

```
remove-orphans refused on prod.db.t: table has a current snapshot but no
reachable files; refusing to treat everything under its location as garbage
```

**A reachable set that could not be fully computed.** An error anywhere in the
metadata walk aborts the whole computation rather than returning what it managed
to read. A partial reachable set is indistinguishable from a complete one, and
would license deleting everything it failed to read.

## What counts as reachable

Everything, from **all retained snapshots** — not only the current one. A
snapshot kept for time travel needs its data files as much as the current one
does.

- data files and delete files, from every manifest of every retained snapshot,
  **including entries marked `DELETED`** — such an entry names a file *this*
  snapshot removed, but an older retained snapshot still reads it
- manifests and manifest lists
- statistics and partition-statistics files (Puffin)
- every `metadata.json` the metadata log still names, plus the current one

## The audit trail

The complete deletion list is written **before the first delete**:

```bash
bergman run --audit-log /var/log/bergman.jsonl
```

```json
{"at":"2026-08-23T02:14:07Z","run_id":"6f2c…","table":"prod.analytics.events",
 "operation":"remove-orphans","reason":"1284 files",
 "result":{"result":"succeeded","detail":"deletion starting"},
 "deleted_files":["s3://lake/warehouse/analytics/events/data/00042-…parquet", …]}
```

Writing it first is the point. A crash halfway through a deletion then leaves a
record of exactly what was about to go — the difference between an incident with
evidence and one without.

## Approval gates

Embedders can require sign-off without Bergman knowing how that decision is made:

```rust
#[async_trait::async_trait]
impl MaintenanceObserver for RequireSignoff {
    async fn operation_starting(&self, table: &TableRef, kind: OperationKind) -> bool {
        kind != OperationKind::RemoveOrphans || self.approved(table).await
    }
}
```

Returning `false` vetoes the operation, which is then reported as **refused** —
an outcome that needs attention, not a silent skip. See
[Library](@/docs/library.md#observers).

## Relationship to expiration

Expiring snapshots makes files unreachable; something has to delete them.
Upstream's `ExpireSnapshotsAction` explicitly does not, so you have two options:

```toml
[defaults.snapshots]
delete_files = true      # reclaim immediately, during expiration
```

or leave it off and let the orphan scanner reclaim them after the grace period.
The first returns storage sooner; the second keeps a single deletion path with a
single safety model. Both use the same containment and normalization rules.
