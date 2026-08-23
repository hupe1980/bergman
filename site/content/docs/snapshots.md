+++
title = "Snapshots"
description = "Expiring snapshots, what upstream decides, and reclaiming the files expiration leaves behind."
weight = 6
+++
Every Iceberg commit adds a snapshot. Nothing removes them until something
expires them, so a table written every minute accumulates 1,440 snapshots a day —
each with a manifest list that planning may touch, and each pinning data files
that would otherwise be deletable.

This is the most common Iceberg health problem, and it is the one Bergman
handles end to end today.

## Configure it

```toml
[defaults.snapshots]
max_age      = "7d"      # -> history.expire.max-snapshot-age-ms -> 5 days
min_to_keep  = 3         # -> history.expire.min-snapshots-to-keep -> 1
delete_files = false     # reclaim storage during expiration
```

Metadata-only expiration is **on by default** — it is the one destructive-sounding
operation that writes no data files, and unbounded snapshot growth costs
everybody. Deleting the files it orphans is opt-in.

## Bergman does not choose the snapshots

Selection is upstream's `ExpireSnapshotsAction`, which follows Java's
`RemoveSnapshots`: per-branch ancestry, per-ref retention, ref aging, and the
rule that a snapshot reachable from *any* retained ref is never expired however
old it is.

Bergman does not reimplement that. It is the subtlest rule in Iceberg, and a
second implementation would drift from the first — quietly, and in the direction
of deleting something it should not have.

The consequence is that Bergman's *plan* uses a coarser trigger than the commit
does:

```text
  -> expire-snapshots
     why: oldest snapshot is 34d old (> 7d), 61 snapshots retained (keeping at least 3)
     removes up to 58 snapshots
```

"Up to" is doing real work there. If a tag pins an old snapshot, or a branch
shares an ancestor, fewer go — and the run reports that plainly:

```text
  [--] expire-snapshots: no snapshot was expirable under per-branch retention
```

That is a normal outcome, not a mistake. It is also the usual answer to "why did
nothing expire?".

> [!NOTE]
> Bergman cannot list a table's branches and tags to tell you *which* ref is
> pinning a snapshot. `TableMetadata::refs` is `pub(crate)` upstream, the only
> accessor requires knowing a ref's name, and the type derives `Deserialize` but
> not `Serialize`. Use `SHOW REFS` from a query engine to find the culprit.

## Reclaiming the files

Upstream's action rewrites metadata and stops there. Its own documentation says
so: *"the now-unreferenced data and metadata files are left untouched. Physical
file cleanup is the responsibility of a higher-level maintenance operation built
on top of this action."*

Bergman is that operation. With `delete_files = true`:

1. compute the reachable set **before** the commit, while the snapshots exist
2. commit the expiration
3. compute the reachable set **again**, from the table as it now is
4. delete the difference

Step 3 recomputes rather than predicts. The commit is the thing that decides
which snapshots went, and a prediction that disagreed with it would delete live
files.

```text
  [ok] expire-snapshots: 58 snapshots expired, 12043 files deleted
```

Every file is re-checked for containment inside the table location before it is
deleted, and a file that will not delete is reported as a leak rather than
failing the operation — the metadata commit has already succeeded, so the table
is correct either way.

## Or leave it to the scanner

With `delete_files = false` (the default), expiration is pure metadata and the
[orphan scanner](@/docs/orphans.md) reclaims the files later, after its grace
period. One deletion path, one safety model, at the cost of a few days' storage
lag.

Both routes share the same containment and path-normalization rules.

## Commits and conflicts

Expiration commits with compare-and-swap. Losing to a foreground writer is
expected — Bergman is a background tenant — and is reported as such:

```text
  [<>] expire-snapshots: table moved during 3 commit attempts; will replan next cycle
```

Each retry **reloads the table** rather than re-submitting. See
[Architecture](@/docs/architecture.md#commits) for why that distinction is a
correctness property and not an optimization.

## Metadata files

Old `metadata.json` files accumulate alongside snapshots. Iceberg's own
`write.metadata.delete-after-commit.enabled` handles these at write time, and
most catalogs — including [Rustberg](@/docs/rustberg.md) and Lakekeeper — enforce
it server-side. Bergman treats every `metadata.json` the metadata log still names
as reachable, so it never deletes one the table still refers to.
