+++
title = "Orphan files"
description = "The one operation that can destroy a healthy table, and the seven independent checks standing in front of it."
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

## The seven checks

Every candidate passes all seven before it is deleted. Each exists because of a
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

### 5. No scanning a location another table lives inside

A table at `s3://lake/wh/db` whose sibling sits at `s3://lake/wh/db/events` is a
trap that containment cannot catch. Every one of that nested table's live files
*is* inside this table's location, and *is* unreachable from this table's
metadata — because it belongs to somebody else. That is precisely the definition
of an orphan, and precisely wrong.

Bergman refuses the scan and names the nested table:

```
remove-orphans refused on prod.db.warehouse: table "s3://lake/wh/db/events"
lives inside this table's location "s3://lake/wh/db"; its live files would look
like orphans here. Move one of the two, or exclude this table from orphan
removal.
```

Refusing rather than quietly scanning around the nested table is deliberate: a
warehouse laid out this way needs fixing, and working around it would leave the
hazard in place for the next table added underneath.

**This is checked two ways, and the second is the one that has to hold.**

The first consults the locations of tables Bergman has examined. It is cheap and
it can refuse before a single object is listed — but that ledger is structurally
incomplete. A table no rule matches, one a rule `skip`s, one outside this
cycle's `--table` scope, or one that failed to load is simply absent from it.
And a deliberately-excluded table is *exactly* the one most likely to be sitting
somewhere it should not be, so relying on the ledger alone would leave the
nested table an operator was most careful about the least protected.

The second reads the listing the scan is already walking. An Iceberg table's
identity is a `metadata/….metadata.json` document at its root; this table's own
live at `<location>/metadata/…`, so one found at
`<location>/<anything>/metadata/…` belongs to a *different* table whose root is
`<location>/<anything>`. No ledger, no prior examination, no extra I/O — and it
holds for a table Bergman has never heard of.

Note that this keys on the metadata document's own suffix, not on a directory
name. A table's data may legitimately sit in a directory called `metadata`, and
a name-based heuristic would trip over it.

### 6. Re-verification before deleting

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

### 7. A ceiling on the blast radius

However wrong everything above turns out to be, one scan deletes at most
`limits.max_deletes_per_run` files — 100 000 by default — and says loudly that
it withheld the rest:

```text
  [ok] remove-orphans: 100000 orphans deleted (312 GiB) from 6102000 objects,
       48211 left for the next scan by the per-run ceiling of 100000
```

The difference between losing a thousand files and losing a million is the
difference between an incident and a catastrophe. Hitting the ceiling is also
information in its own right: a healthy table does not produce that many
orphans, and a scan that quietly deleted them all would have hidden the fact.

Deletions run with bounded concurrency. Deleting a million orphans one round
trip at a time takes hours; a burst of ten thousand concurrent deletes is how a
shared bucket starts throttling everybody.

The ceiling lives under `[limits]` rather than under `[orphans]` because it
governs **every** deletion Bergman performs. Orphan removal and
[expiration's file cleanup](@/docs/snapshots.md) decide *what* to delete by completely
different reasoning, but they share one deletion path: apply the ceiling, write
the audit record, then delete with bounded concurrency. Two implementations of
that would drift, and the half that drifted would be the half nobody was
looking at.

## Memory

The listing is consumed as a **stream** and reduced to candidates as it arrives.
A table with millions of objects is never held in memory in its entirety just to
be filtered away — and on a healthy table the answer is almost always empty.

## Empty tables are still scanned

This is the one operation that applies to a table with no snapshots, and it has
to. A first write that died between staging its data and committing left files
under the table location that nothing references and nothing else will ever
reclaim. Every other operation declines an empty table; this one does not.

## Two refusals

Beyond the seven checks, the scanner declines outright in two situations.

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
- `version-hint.text`, if the table was migrated from a Hadoop catalog — it
  matches nothing in the metadata, so a scanner without this rule would delete
  the only file that says which `metadata.json` is current

## Cadence

Orphan removal is the one operation that is **scheduled rather than
triggered**. Every other operation decides from metadata Bergman has already
read; this one cannot know whether a table has orphans without listing its whole
location, and that listing *is* the cost. Running it on every cycle would make a
five-minute cadence mean a full object-store listing of every table every five
minutes — real money on S3, and it finds nothing almost every time.

```toml
[rules.orphans]
min_interval = "24h"   # the default
```

The interval applies **within one process**. A one-shot `bergman run` always
scans, because the cron entry that invoked it already decided the cadence; a
long-lived `bergman daemon` on a short cycle throttles itself. Nothing is
persisted — losing the memory on restart costs one extra listing, and persisting
it would mean state that has to survive a crash, which is exactly what Bergman
does not have.

## The audit trail

The complete deletion list is written **before the first delete**:

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

Writing it first is the point. A crash halfway through a deletion then leaves a
record of exactly what was about to go — the difference between an incident with
evidence and one without.

## Approval gates

Embedders can require sign-off without Bergman knowing how that decision is made:

```rust
#[async_trait::async_trait]
impl MaintenanceObserver for RequireSignoff {
    async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
        ctx.kind != OperationKind::RemoveOrphans || self.approved(ctx.table).await
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
