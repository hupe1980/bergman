+++
title = "Architecture"
description = "How a table becomes a plan, how a plan becomes a commit, and why a conflict means replan rather than retry."
weight = 20
+++
Bergman is one crate with layered modules and a binary that composes them. There
is no separate planner service, no work queue, and no worker tier.

```text
  CLI / embedder
       │
  ┌────▼──────────────────────────────────────────────┐
  │ policy      rules · glob matching · 4-layer        │
  │             resolution with provenance             │
  ├────────────────────────────────────────────────────┤
  │ health      manifest walk, metadata only:          │
  │             file sizes · delete ratios · snapshots │
  ├────────────────────────────────────────────────────┤
  │ plan        triggers · estimates · executability   │
  ├────────────────────────────────────────────────────┤
  │ ops         expire · orphans · reachability        │
  ├────────────────────────────────────────────────────┤
  │ iceberg-rust    catalogs · FileIO                  │
  │ opendal         object listing (FileIO has none)   │
  └────────────────────────────────────────────────────┘
```

## Plan and execute are the same code

`bergman plan` and `bergman run` build the identical plan through the identical
function; `run` then executes it. There is no second code path that might
disagree, which is what makes a dry run a real preview rather than a separate
program that resembles one.

Planning is a pure function of (health, policy, clock). It performs no writes
and no deletions.

## Health is metadata only

The analyzer reads the current snapshot's manifest list and every manifest in it.
No data file is opened, no Parquet footer is parsed — every number comes from the
manifest entries Iceberg already maintains.

That is what makes it cheap enough to run against thousands of tables on every
cycle, which in turn is what makes *triggered* maintenance possible.

One subtlety: manifests record a file's history, not only its present state.
Entries marked `DELETED` describe files that snapshot removed. The health
analyzer **skips** them — counting them would make every compaction look like it
achieved nothing. The reachability walk **includes** them — an older retained
snapshot still reads those files, and deleting one would break time travel.

## Commits {#commits}

Maintenance is a background tenant. It must never win against a foreground
writer, and never corrupt one.

Commits use the catalog's compare-and-swap. When one loses, Bergman **reloads the
table and rebuilds** rather than re-submitting:

```rust
Err(err) if is_conflict(&err) => {
    current = catalog.load_table(&ident).await?;   // reload, then recompute
}
```

This is a correctness property, not an optimization. A plan is a decision made
about a specific table state. Re-submitting it against a state that has moved
applies a decision to a world it was not made for — and for a rewrite, that is
precisely how rows deleted by a concurrent commit come back to life.

Retries are few (three) and the backoff is short. A table being written hard will
keep winning, and the right response is to come back next cycle rather than to
spend the cycle losing. A conflict is reported as `conflicted`, distinct from
`failed`:

```text
  [<>] expire-snapshots: table moved during 3 commit attempts; will replan next cycle
```

That is the design working, not a fault, and it does not set the exit code.

## Crash-only

No run holds state that matters between cycles. There is no journal, no lock
file, and nothing to repair after a `kill -9`:

- files are written before the commit that references them
- a failed run leaves only uncommitted files, which the orphan scanner reclaims
  after its grace period
- re-running replans from the table's current snapshot

## Ordering

Operations run in a fixed order per table per cycle:

```text
compact → rewrite-manifests → expire-snapshots → remove-orphans
```

Compacting first lets expiration reclaim the small files compaction superseded.
Expiring before the orphan scan shrinks the reachable set *legitimately*, rather
than leaving garbage the scanner would have to be trusted to identify.

## Concurrency

Bounded everywhere, and the bounds are parameters rather than statics:

| Where | Default | Why |
|---|---|---|
| Tables in a cycle | `limits.max_parallel_tables` (4) | Configurable; the catalog is a shared service |
| Namespaces during discovery | 16 | Latency-bound tree walk, not CPU-bound |
| Manifests within a table | 16 | Same |
| Snapshots during reachability | 8 | Each fans out to 16 manifests |

Scale-out, when it comes, is stateless replicas sharding tables by UUID — no
broker, no consensus service. Optimistic catalog commits already make
double-execution safe, so sharding is a cost optimization rather than a
correctness requirement.

## What Bergman owns, and why

Two layers exist here only because upstream has no equivalent:

**Object listing.** `iceberg::io::FileIO` has read, write, delete and
`delete_prefix` — but no `list`. Orphan removal is defined by listing storage, so
Bergman carries an `ObjectStore` trait over OpenDAL, configured from the same
Iceberg-named properties the catalog uses. The trait also exists so the safety
logic can be tested against an in-memory store rather than a cloud account.

**Deletion after expiration.** Upstream's `ExpireSnapshotsAction` documents
physical cleanup as a higher-level responsibility. Bergman computes the reachable
set before and after the commit and deletes the difference.

Both are temporary implementations while the gap exists, and both are the natural
things to contribute back.

## Errors carry a disposition

The single most important thing an error can say is what to do about it, so that
is a method rather than a comment:

| Disposition | Meaning |
|---|---|
| `Replan` | The table moved. Rebuild the plan; never re-commit. |
| `Retry` | The same request against a working world would succeed. |
| `Terminal` | Will fail identically forever. Retrying only spends money. |

Refusals are `Terminal` and carry a reason, because a skipped table with an
unexplained reason is indistinguishable from a bug.
