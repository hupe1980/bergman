+++
title = "Status"
description = "What Bergman executes, what it will not do, and why it owns its commit layer."
weight = 2
+++
Bergman is pre-release. All four maintenance operations execute.

| Operation | Built on |
|---|---|
| Table health analysis | Manifest metadata only; no data file is opened |
| Snapshot expiration | Upstream `ExpireSnapshotsAction` |
| Expiration file cleanup | Bergman — upstream leaves physical cleanup to a higher layer |
| Orphan-file removal | Bergman's object-store layer — `FileIO` has no `list` |
| Manifest rewrite | Bergman's commit layer |
| Compaction | Upstream's scan and writers, `DataFusion`, Bergman's commit layer |

## What it will not do {#limits}

Every one of these appears in `bergman plan` and in the run report with its
reason — a table whose configured maintenance can never apply must not read as a
healthy one.

**Refused, with the reason named per table and per operation:**

| | |
|---|---|
| **Rewriting a format v3 table** | Row lineage cannot be preserved through a rewrite, and deletion vectors cannot be read. Expiration and orphan removal still run — see [Compaction](@/docs/compaction.md#format-v3) |
| **Rewriting or scanning an encrypted table** | `encryption.key-id` encrypts the manifests and manifest lists Bergman reads. Snapshot expiration still runs; it needs no manifest |
| **Anything but Parquet** | Refused per table *and* per file, since a table can hold files in a format it no longer writes |
| **Rewriting across a partition-spec change** | Output goes out under the current spec; claiming it replaces files partitioned differently would mis-file every row |
| **Any branch but `main`** | A commit moving `main` to a snapshot descending from something else is not maintenance |

**Reported, not refused:** a table setting `write.object-storage.enabled` gets
rewritten files under the plain data path. The table stays correct — every path
is in its metadata — but repeated compaction concentrates its objects in one
prefix, so the plan says so once per table.

**Out of scope by design:**

| | |
|---|---|
| **Catalogs other than REST** | Each protocol is a second commit path to keep correct |
| **Z-order and Hilbert clustering** | Global sort within a file group works; space-filling curves are an optimization rather than table health |
| **`OpenTelemetry` export** | The library emits `tracing`; a subscriber in your process exports it |
| **A NATS or Kafka client** | Notifications arrive over HTTP and the bridge is yours — see [Operating](@/docs/operating.md#reacting-to-commits) |
| **Rewriting into a different partition spec** | Java exposes it as `outputSpecId`. It changes what a table *is*, so a maintenance run must never do it by accident |
| **SQL, BI or any query API** | Bergman writes tables; it does not answer questions about their contents |

A refusal is never a partial result. Every manifest walk is all-or-nothing — one
unreadable manifest aborts the computation rather than returning a short set — so
nothing is deleted on the strength of a table only partly read.

Scale out by running replicas with disjoint `[[rules]]` or disjoint catalog
`namespaces`; optimistic commits already make overlap safe.

## Why Bergman owns its commit layer

An Iceberg commit is `(requirements, updates)` applied atomically, and the
`iceberg` crate cannot express one from outside: no `Transaction` action removes
a data file, and both `TransactionAction` and `TableCommit`'s builder are
`pub(crate)`. Compaction and manifest rewriting are unreachable through it.

The common answer is to fork upstream, which costs a rebase forever and a crate
that cannot be published, since Cargo rejects git dependencies on crates.io.

Bergman owns the one blocked layer instead, because the blockage is narrow:
every piece of a commit is public and only the delivery is not.

| Piece | Upstream API |
|---|---|
| Manifests | `ManifestWriterBuilder` |
| Manifest lists | `ManifestListWriter` |
| Snapshots | `Snapshot::builder` |
| Updates and preconditions | `TableUpdate`, `TableRequirement` — public and `Serialize` |
| Reading with deletes applied | `TableScan` / `ArrowReader` |
| Writing data files | `ParquetWriterBuilder` → `RollingFileWriter` → `DataFileWriter` |
| **Delivering the commit** | **— nothing** |

So Bergman writes the manifests, manifest list and snapshot with upstream's own
writers, then `POST`s `(requirements, updates)` to the table endpoint. The bytes
are identical to what `iceberg-catalog-rest` sends. Operations commit through a
`TableCommitter` trait rather than a transport, so an upstream action that can
express a rewrite becomes a second implementation and nothing above
`src/commit` changes. See [Architecture](@/docs/architecture.md#commit-layer).

Three smaller gaps are filled the same way: expiration's file cleanup (upstream
documents it as a higher layer's job), object listing (`Storage` has no `list`,
without which orphan removal cannot exist), and reading a branch's retention
(`TableMetadata::refs` is `pub(crate)`, so a commit that moves `main` would
silently erase it — Bergman reads the field from the metadata JSON).

### Where the seam falls

"Bergman's commit layer" means the commits Bergman *authors*: compaction,
dangling-delete removal, manifest rewriting. **Snapshot expiration is upstream's
own action**, and upstream's actions commit through `iceberg::Catalog` —
`TransactionAction::commit` is `pub(crate)`, so its `(requirements, updates)`
cannot be intercepted and re-delivered. Two consequences:

- Expiration's commits carry no `X-Request-Id`, so they sit outside the audit
  join described in [Rustberg](@/docs/rustberg.md#joined-audit-trails). They are
  still authorized and audited by the catalog under the same principal; only the
  correlation by run id is missing.
- The catalog client's token is renewed from outside, because it renews none of
  its own. See [Credentials](@/docs/operating.md#credentials).
