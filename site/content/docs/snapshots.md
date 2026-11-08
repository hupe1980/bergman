+++
title = "Snapshots"
description = "Expiring snapshots, what upstream decides, and reclaiming the files expiration leaves behind."
weight = 6
+++
Every Iceberg commit adds a snapshot, and nothing removes them until something
expires them. A table written every minute gains 1,440 a day, each with a
manifest list planning may touch and each pinning data files.

## Configure it

```toml
[defaults.snapshots]
max_age      = "7d"      # -> history.expire.max-snapshot-age-ms -> 5 days
min_to_keep  = 3         # -> history.expire.min-snapshots-to-keep -> 1
delete_files = false     # reclaim storage during expiration
```

Metadata-only expiration is **on by default**; it writes no data files.
Deleting the files it orphans is opt-in.

## Bergman does not choose the snapshots

Selection is upstream's `ExpireSnapshotsAction`, which follows Java's
`RemoveSnapshots`: per-branch ancestry, per-ref retention, ref aging, and never
expiring a snapshot reachable from a retained ref. Bergman does not reimplement
it.

The plan therefore says "up to":

```text
  -> expire-snapshots
     why: oldest snapshot is 34d old (> 7d), 61 snapshots retained (keeping at least 3)
     removes up to 58 snapshots
```

If a tag pins an old snapshot or a branch shares an ancestor, fewer go:

```text
  [--] expire-snapshots: no snapshot was expirable under per-branch retention
```

That is a normal outcome. Bergman does not report which ref pins a snapshot; use
`SHOW REFS` from a query engine.

## Reclaiming the files

Upstream's action rewrites metadata only. With `delete_files = true` Bergman:

1. computes the reachable set **before** the commit
2. commits the expiration
3. computes the reachable set **again**, from the table as it now is
4. deletes the difference

Step 3 recomputes rather than predicts — the commit decides which snapshots
went.

```text
  [ok] expire-snapshots: 58 snapshots expired, 12043 files deleted
```

Every file is re-checked for containment inside the table location. A file that
will not delete is reported as a leak, not a failure — the metadata commit
already succeeded.

### One deleter, one safety model

Expiration and [orphan removal](@/docs/orphans.md) share one deletion path:

1. apply `limits.max_deletes_per_run`, the blast-radius ceiling
2. write the audit record naming exactly what will go
3. delete, with bounded concurrency

Files the ceiling withholds are left for the orphan scanner:

```text
  [ok] expire-snapshots: 58 snapshots expired, 100000 files deleted
       (12043 left for the orphan scanner by the per-run ceiling of 100000)
```

## Or leave it to the scanner

With `delete_files = false` (the default), the
[orphan scanner](@/docs/orphans.md) reclaims the files after its grace period —
the same deleter and containment rules, a few days' storage lag.

## Commits and conflicts

Expiration commits with compare-and-swap. Losing to a foreground writer is
reported as a conflict, not a failure:

```text
  [<>] expire-snapshots: table moved during 3 commit attempts; will replan next cycle
```

Each retry **reloads the table** rather than re-submitting — see
[Architecture](@/docs/architecture.md#commits).

## Metadata files

Old `metadata.json` files are governed by Iceberg's
`write.metadata.delete-after-commit.enabled`, applied at commit time. Every `metadata.json` the metadata log names is reachable.
Expiration's cleanup never deletes one, even one that leaves the log
mid-operation, since that would race the catalog; the orphan scanner reclaims
them behind its grace period.

## Format v3

Expiration runs on v3 exactly as on any other format. A Puffin file of deletion
vectors is reclaimed by [orphan removal](@/docs/orphans.md) only once nothing
references any vector in it. See [Compaction](@/docs/compaction.md#format-v3).
