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
  │ ops         compact · manifests · expire · orphans │
  ├────────────────────────────────────────────────────┤
  │ commit      manifests + snapshot -> REST commit    │
  │             (iceberg cannot express a rewrite)     │
  ├────────────────────────────────────────────────────┤
  │ iceberg-rust    catalogs · FileIO · scan · writers │
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

Retries are few (three) and the backoff is short, exponential, and **jittered**.
A table being written hard will keep winning, and the right response is to come
back next cycle rather than to spend the cycle losing. The jitter matters
because the scale-out model is N stateless replicas coordinating through
optimistic commits: two replicas that lose the same compare-and-swap at the same
moment would otherwise come back in lockstep and spend the whole budget losing
to each other. A conflict is reported as `conflicted`, distinct from `failed`:

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
| Tables in a cycle | `limits.max_parallel_tables` (4) | Both examined and maintained concurrently; the catalog is a shared service |
| Namespaces during discovery | 16 | Latency-bound tree walk, not CPU-bound |
| Manifests within a table | 16 | Same |
| Snapshots during reachability | 8 | Each fans out to 16 manifests |
| Deletions within an orphan scan | 32 | A million round trips in series takes hours; ten thousand at once throttles the bucket |
| Within one file group's rewrite | 1 | Parallelism is across tables and groups, which are already bounded and commit separately. Intra-group parallelism would multiply the memory budget by the core count without changing how much work a cycle does |

`compaction.max_sort_memory` is the executor's memory pool **per file group**,
and groups within a table run one at a time — so a cycle's peak is roughly
`max_parallel_tables × max_sort_memory`.

Operations *within* a table stay strictly ordered, because that ordering is a
correctness property. Two different tables share nothing but the object store,
so nothing is serialised between them.

Scale-out is stateless replicas with disjoint `[[rules]]` or disjoint catalog
`namespaces` — no broker, no consensus service. Optimistic catalog commits
already make double-execution safe, so sharding is a cost optimization rather
than a correctness requirement. Automatic shard assignment is
[not built](@/docs/status.md#limits).

## The executor {#executor}

Partition identity comes from the **manifests**, not from the scan.
`FileScanTask` has a `partition_spec` field that upstream leaves `None` in every
scan it performs, so a file's partition tuple cannot be rendered from a task.
Each manifest carries exactly one spec, which makes reading it there exact for a
spec-evolved table — and grouping on the task's missing spec instead would put
every file under `unpartitioned`, match none of the partitions the plan named,
and compact nothing while reporting success.


Compaction is the only operation that reads and writes data, and the only one
that needs a query engine. The split is deliberate, and each stage goes to
whichever component does it best:

| Stage | Who | Why |
|---|---|---|
| Scan | upstream `ArrowReader` | Already vectorized, already Iceberg-correct |
| **Positional** deletes | upstream `ArrowReader` | They become a Parquet row selection, so deleted rows are never decoded — *better* than a join |
| **Equality** deletes | DataFusion hash anti-join | Upstream builds one predicate term per delete row and evaluates that tree against every batch: `data rows × delete rows`. A hash anti-join is their sum, and spills |
| Sort | DataFusion | Spills |
| Write | upstream `RollingFileWriter` | Real `DataFile`s with full column metrics |

Two rules govern the plan, and both are silent when broken:

**Files are bucketed by which deletes apply to them.** A delete committed at
sequence N applies to files written before it and not after, and upstream's scan
has already worked that out — `FileScanTask::deletes` holds exactly the
applicable ones. So one file group can hold files with *different* delete sets,
which is the ordinary CDC shape. Anti-joining a file against a delete that does
not apply to it would remove rows nothing ever deleted.

**Buckets are unioned before the sort, not sorted individually.** Sorting per
bucket would leave the output ordered in runs rather than globally — metadata
claiming a clustering the files do not have.

`compaction` is a default-on feature; a build without it has no compaction
rather than a slower one, so there is only ever one executor to keep correct.
See [Compaction](@/docs/compaction.md#query-engine).

## The commit layer {#commit-layer}

An Iceberg commit is `(requirements, updates)` applied atomically, and the
`iceberg` crate cannot express one from outside: `TableCommit`'s builder and
`TransactionAction` are both `pub(crate)`, and no built-in action removes a data
file. Compaction and manifest rewriting are therefore unreachable through it.

Bergman owns that layer rather than forking (see
[Status](@/docs/status.md#why-bergman-owns-its-commit-layer) for why the
alternative is worse). The split is deliberate:

- **Everything a commit is made of** comes from upstream's public writers —
  `ManifestWriterBuilder`, `ManifestListWriter`, `Snapshot::builder`,
  `TableUpdate`, `TableRequirement`.
- **Only the delivery** is Bergman's: a `POST` of `{identifier, requirements,
  updates}` to the table endpoint, with the catalog's routing prefix discovered
  once from `/v1/config`. The bytes are identical to what
  `iceberg-catalog-rest` sends, because they are the same serialized types.

Operations are written against a `TableCommitter` trait rather than the
transport, so when upstream lands `OverwriteAction` a second implementation
wraps it and nothing above `src/commit` changes.

Four invariants govern a produced snapshot. Each loses or corrupts data when
broken, and none of them fails visibly:

1. **Every live file not being removed is carried forward.** A manifest set that
   omits one silently deletes those rows. Checked rather than assumed:
   everything the caller asked to remove must actually be live, or the plan is
   stale and the commit is refused before it is offered.
2. **A carried-forward file keeps its own sequence number and snapshot id**, so
   delete files written *later* still apply to it. Stamping the new snapshot's
   number on an untouched file would make it look newer than the deletes that
   should remove its rows. Files the snapshot genuinely *adds* are the opposite
   case and take the new number — which is higher than every delete already
   applied to their contents, and is therefore what retires those deletes.
3. **A manifest is rewritten under the partition spec it was written under.** A
   manifest carries exactly one `partition_spec_id`, and an entry's partition
   tuple is meaningless against any other. Rewriting an old manifest under the
   table's *current* spec re-interprets every tuple in it, which mis-prunes
   files at query time. Nothing fails; queries just start returning wrong
   answers.
4. **A branch's retention survives a commit that moves it.** The REST protocol's
   `set-snapshot-ref` replaces the whole reference, so a commit that names no
   retention silently erases what `ALTER TABLE … CREATE BRANCH main RETAIN …`
   configured — visible only the next time expiration runs. Upstream exposes no
   accessor for it, so Bergman reads the retention out of the table's own
   metadata JSON.

5. **A snapshot Bergman authors is v1 or v2.** A format v3 table's row lineage
   cannot survive a rewrite, and `TableMetadataBuilder::add_snapshot` rejects a
   v3 snapshot with no `first-row-id` outright — so such a commit does not
   merely risk being wrong, it is missing a field the spec requires. Refused with a
   reason, per operation, so expiration and orphan removal still run. See
   [Compaction](@/docs/compaction.md#format-v3).

Bergman only maintains `main`. A table whose current snapshot is not `main`'s
head is refused with that reason, rather than having `main` moved to a snapshot
descending from something else.

### Re-packing manifests clusters by partition

Manifest rewriting has a second rule beside invariant 3, and it is the one that
makes the operation worth doing at all.

A manifest list records each manifest's partition *summary* — the range of
values its entries cover — and that summary is what lets a query skip a manifest
without opening it. Re-packing entries in whatever order they happened to arrive
produces manifests whose summaries each span the whole table, so every query
opens every one of them: fewer manifests, all of which must now be read, which
is worse than what the rewrite started with. One sort of metadata already in
memory prevents it.

The sort is **stable and breaks no ties**, so within one partition entries keep
the order the parent's manifests had — commit order. A table whose rows arrive
in time order keeps that locality, and with it the tight min/max bounds a
timestamp column gets from it; an arbitrary second key would reshuffle every
entry in every partition to no purpose. Java's `RewriteManifests` clusters the
same way.

## What Bergman owns, and why

Three layers exist here only because upstream has no equivalent:

**Object listing.** `iceberg::io::FileIO` has read, write, delete and
`delete_prefix` — but no `list`. Orphan removal is defined by listing storage, so
Bergman carries an `ObjectStore` trait over OpenDAL, configured from the same
Iceberg-named properties the catalog uses. The trait also exists so the safety
logic can be tested against an in-memory store rather than a cloud account.

**Deletion after expiration.** Upstream's `ExpireSnapshotsAction` documents
physical cleanup as a higher-level responsibility. Bergman computes the reachable
set before and after the commit and deletes the difference.

**Commit delivery**, as above — including the OAuth2 client-credentials
exchange, with refresh. `iceberg-catalog-rest` carries a `TODO: Support
automatic token refreshing`, and the commit path reads the same auth properties
the read path does. Two clients that authenticated differently would let reads
succeed and every write return 401 — the tool would appear to work and quietly
change nothing.

All four are temporary implementations while the gap exists, and the first two
are the natural things to contribute back.

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
