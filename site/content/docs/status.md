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

## Limits

What Bergman declines to do, and says so rather than guessing. Every one of
these appears in `bergman plan` and in the run report with its reason, not only
in a log line — a table whose configured maintenance can never apply must not
read as a healthy one:

| | |
|---|---|
| **Rewriting a format v3 table** | Preserving row lineage needs `_row_id` projected out of the scan and written back, which upstream does not offer; `iceberg-rust` rejects a v3 snapshot with no `first-row-id` outright. Expiration and orphan removal still run on it — see [Compaction](@/docs/compaction.md#format-v3) |
| **Non-Parquet writes** | It reads Parquet, Avro and ORC but writes only Parquet, and refuses a table whose `write.format.default` is something else |
| **Rewriting across a partition-spec change** | Output goes out under the current spec; a commit claiming it replaces files partitioned differently would mis-file every row |
| **Catalogs other than REST** | Each additional protocol is a second commit path to keep correct |
| **Z-order clustering** | Global sort within a file group works; space-filling curves are an optimization rather than table health |
| **`OpenTelemetry` export** | The library emits `tracing`; a subscriber in your process exports it today |
| **A NATS or Kafka client** | Notifications arrive over HTTP and the bridge is yours — see [Operating](@/docs/operating.md#reacting-to-commits) |
| **Rewriting position delete files** | Java has an action of its own for coalescing small live position deletes. Bergman retires them the expensive way, as a side effect of compaction, and removes the ones that apply to nothing. The middle case needs a reader and writer for a file type Bergman otherwise never writes |
| **Stamping `sort_order_id` on rewritten files** | Output is clustered and its bounds are tight, but the file does not advertise *which* order it satisfies. Upstream keeps the field crate-private with no setter |

Scale out by running replicas with disjoint `[[rules]]` or disjoint catalog
`namespaces`; optimistic commits already make overlap safe.

## Why Bergman owns its commit layer

An Iceberg commit is `(requirements, updates)` applied atomically, and the
`iceberg` crate cannot express one from outside: no `Transaction` action removes
a data file, and both `TransactionAction` and `TableCommit`'s builder are
`pub(crate)`. Compaction and manifest rewriting are unreachable through it.

The common answer is to fork —
[`nimtable/iceberg-compaction`](https://github.com/nimtable/iceberg-compaction)
pins `risingwavelabs/iceberg-rust` at a git revision — which costs a rebase
forever and a crate that cannot be published, since Cargo rejects git
dependencies on crates.io.

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
