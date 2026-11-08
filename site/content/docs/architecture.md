+++
title = "Architecture"
description = "How a table becomes a plan, how a plan becomes a commit, and why a conflict means replan rather than retry."
weight = 20
+++
Bergman is one crate with layered modules and a binary that composes them — no
planner service, work queue or worker tier.

```text
  CLI / embedder
       │
  ┌────▼──────────────────────────────────────────────┐
  │ policy      rules · glob matching · 5-layer        │
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

`bergman plan` and `bergman run` build the plan through the same function; `run`
then executes it, so a dry run is a real preview. Planning is a pure function of
(health, policy, clock) and writes nothing.

## Health is metadata only

The analyzer reads the current snapshot's manifest list and manifests; no data
file or Parquet footer is opened. Entries marked `DELETED` are **skipped** by
health analysis and **included** by the reachability walk, because a retained
older snapshot still reads those files.

## Commits {#commits}

Commits use the catalog's compare-and-swap. When one loses, Bergman **reloads the
table and rebuilds** rather than re-submitting:

```rust
Err(err) if is_conflict(&err) => {
    current = catalog.load_table(&ident).await?;   // reload, then recompute
}
```

Re-submitting a plan against a table that has moved is how rows deleted by a
concurrent commit come back. Attempts are capped at three, with short, jittered
exponential backoff so replicas losing the same race do not retry in lockstep. A
lost table is reported as `conflicted`, not `failed`, and does not set the exit
code:

```text
  [<>] expire-snapshots: table moved during 3 commit attempts; will replan next cycle
```

## Crash-only

No state matters between cycles — no journal, no lock file, nothing to repair
after a `kill -9`:

- files are written before the commit that references them
- a failed run leaves only uncommitted files, which the orphan scanner reclaims
  after its grace period
- re-running replans from the table's current snapshot

## Ordering

Operations run in a fixed order per table per cycle:

```text
compact → rewrite-manifests → expire-snapshots → remove-orphans
```

Compacting first lets expiration reclaim the files compaction superseded;
expiring before the orphan scan shrinks the reachable set legitimately.

## Concurrency

Bounded everywhere, by parameters rather than statics:

| Where | Bound | Default |
|---|---|---|
| Tables in a cycle, examined and maintained | `limits.max_parallel_tables` | 4 |
| Namespaces during discovery | fixed | 16 |
| Object-store requests per operation — manifest reads, reachability, deletions | `limits.io_concurrency` | 32 |
| Threads in one file group's rewrite plan | `limits.compaction_parallelism` | the machine's available parallelism |

File groups within a table run one at a time, each with a
`compaction.max_sort_memory` pool shared across its threads, so a cycle's peak
is roughly `max_parallel_tables × max_sort_memory`. Operations within a table are
strictly ordered; different tables share nothing but the object store.

Scale-out is stateless replicas with disjoint `[[rules]]` or disjoint catalog
`namespaces`. Optimistic commits make double execution safe, so sharding saves
cost rather than ensuring correctness. There is no automatic shard assignment
and no coordination between replicas.

## The executor {#executor}

Partition identity comes from the **manifests**, not the scan: upstream leaves
`FileScanTask::partition_spec` as `None`, and each manifest carries exactly one
spec.

Compaction is the only operation that reads and writes data:

| Stage | Who | Why |
|---|---|---|
| Scan | upstream `ArrowReader` | Vectorized and Iceberg-correct |
| **Positional** deletes | upstream `ArrowReader` | A Parquet row selection: deleted rows are never decoded |
| **Equality** deletes | DataFusion hash anti-join | Upstream's path is `data rows × delete rows`; a hash join is their sum, and spills |
| Sort | DataFusion | Spills |
| Write | upstream `RollingFileWriter` | Real `DataFile`s with full column metrics |

Files are bucketed by which deletes apply to them (`FileScanTask::deletes`), and
buckets are unioned before the sort, so output is ordered globally rather than in
runs. A build without the `compaction` feature has no compaction. See
[Compaction](@/docs/compaction.md#query-engine).

## The commit layer {#commit-layer}

An Iceberg commit is `(requirements, updates)` applied atomically. The `iceberg`
crate cannot express a rewrite from outside: `TableCommit`'s builder and
`TransactionAction` are `pub(crate)`, and no built-in action removes a data
file. Bergman builds the commit itself instead of forking upstream:

- **Everything a commit is made of** comes from upstream's public writers —
  `ManifestWriterBuilder`, `ManifestListWriter`, `Snapshot::builder`,
  `TableUpdate`, `TableRequirement`.
- **Only delivery** is Bergman's: a `POST` of `{identifier, requirements,
  updates}` to the table endpoint, with the routing prefix discovered from
  `/v1/config`. The bytes are those `iceberg-catalog-rest` would send.

Operations commit through a `TableCommitter` trait, not the transport.

Five invariants govern a produced snapshot; each corrupts data silently when
broken:

1. **Every live file not being removed is carried forward.** Everything the
   caller asks to remove must be live, or the plan is stale and the commit is
   refused before it is offered.
2. **A carried-forward file keeps its own sequence number and snapshot id**, so
   delete files written later still apply to it. Files the snapshot *adds* take
   the new number, which retires the deletes already applied to their contents.
3. **A manifest is rewritten under the partition spec it was written under.**
   An entry's partition tuple is meaningless against any other spec.
4. **A branch's retention survives a commit that moves it.** REST's
   `set-snapshot-ref` replaces the whole reference; upstream exposes no accessor
   for retention, so Bergman reads it from the table's metadata JSON.
5. **A v3 rewrite keeps every row's lineage.** `_row_id` and
   `_last_updated_sequence_number` are written as physical columns, and the
   snapshot claims exactly the row range its manifest list assigned from the
   table's `next-row-id`. An added v3 data file without the lineage columns is
   refused by name. See [Compaction](@/docs/compaction.md#format-v3).

The snapshot producer also refuses encrypted tables. Bergman maintains only
`main`: a table whose current snapshot is not `main`'s head is refused.

### Re-packing manifests clusters by partition

Manifest rewriting sorts entries by partition so each new manifest's partition
summary is narrow and queries can skip it. The sort is **stable and breaks no
ties**, so within a partition entries keep commit order — as Java's
`RewriteManifests` does.

## What Bergman owns

Layers that exist because upstream has no equivalent:

- **Object listing.** `iceberg::io::FileIO` has no `list`, so Bergman carries an
  `ObjectStore` trait over OpenDAL, configured from the catalog's Iceberg-named
  properties and testable against an in-memory store.
- **Deletion after expiration.** Upstream's `ExpireSnapshotsAction` leaves
  physical cleanup to the caller; Bergman deletes the difference between the
  reachable sets before and after the commit.
- **Commit delivery**, as above, including the OAuth2 client-credentials
  exchange, from the same auth properties the read path uses.
- **Token renewal for the read path.** `iceberg-catalog-rest` 0.11.0 exchanges
  its OAuth2 credential once (read 2026-11-08: `TODO: Support automatic token
  refreshing`). Bergman injects an `AuthManager` so the catalog client — table
  loads, discovery, snapshot expiration's commit — takes every request's token
  from the commit client's token source, which renews on demand a minute before
  expiry.

`RestCatalogBuilder` has no setter for extra headers, so requests made through
upstream's catalog client do not carry Bergman's run id (see
[Rustberg](@/docs/rustberg.md#joined-audit-trails)).

## Errors carry a disposition

| Disposition | Meaning |
|---|---|
| `Replan` | The table moved. Rebuild the plan; never re-commit. |
| `Retry` | The same request against a working world would succeed. |
| `Terminal` | Will fail identically forever. |

Refusals are `Terminal` and carry a reason.
