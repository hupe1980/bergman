+++
title = "Status"
description = "What Bergman executes, what it refuses, and why it owns its commit layer."
weight = 2
+++
Bergman is pre-1.0, built on `iceberg-rust` 0.11.0, MSRV Rust 1.95. All four
maintenance operations execute.

| Operation | Built on |
|---|---|
| Table health analysis | Manifest metadata only; no data file is opened |
| Snapshot expiration | Upstream `ExpireSnapshotsAction` |
| Expiration file cleanup | Bergman — upstream leaves physical cleanup to a higher layer |
| Orphan-file removal | Bergman's object-store layer — `FileIO` has no `list` |
| Manifest rewrite | Bergman's commit layer |
| Compaction | Upstream's scan and writers, `DataFusion`, Bergman's commit layer |

Format v3 is maintained like any other format: compaction, manifest rewriting
and dangling-delete removal all run, row lineage is written physically, and
deletion vectors are applied — see [Compaction](@/docs/compaction.md#format-v3).

## What it refuses {#limits}

Each refusal is named per table and per operation in `bergman plan` and in the
run report, so a table whose maintenance can never apply does not read as
healthy.

| Refused | Why |
|---|---|
| **Encrypted tables** (`encryption.key-id`) | Manifests and manifest lists are encrypted, and Bergman holds no key-management client. Snapshot expiration still runs; it needs no manifest |
| **Anything but Parquet** | Checked per table *and* per file, since a table can hold files in a format other than the one it writes |
| **Rewriting across a partition-spec change** | Output is written under the current spec; replacing files partitioned differently would mis-file every row |
| **Any branch but `main`** | A commit moving `main` to a snapshot descending from something else is not maintenance |
| **A v3 data file without lineage columns** | The snapshot producer refuses an added v3 file lacking `_row_id` and `_last_updated_sequence_number`, naming the file |

A refusal is never a partial result. Every manifest walk is all-or-nothing, so
nothing is deleted on the strength of a table only partly read.

Bergman does not support catalogs other than REST, Z-order or Hilbert
clustering, rewriting into a different partition spec, `OpenTelemetry` export
(the library emits `tracing`), a NATS or Kafka client (notifications arrive over
HTTP — see [Operating](@/docs/operating.md#reacting-to-commits)), or any query
API.

Compaction throughput is not measured.

Scale out by running replicas with disjoint `[[rules]]` or disjoint catalog
`namespaces`; optimistic commits make overlap safe.

## Checked against another engine {#cross-engine}

`just cross-engine` is the check whose reader shares no code with Bergman's
writer. It needs `uv` and is opt-in, outside `just ci`:

- PyIceberg 0.12.0 writes v2 tables — unpartitioned, identity- and
  bucket-partitioned, each with a copy-on-write delete. Bergman compacts them
  and rewrites their manifests; PyIceberg reads back exactly the rows it wrote.
- PyIceberg reads a v3 table Bergman compacted, with deletion vectors applied
  and lineage written physically.

PyIceberg 0.12.0 cannot write v3 or read `_row_id`, so row lineage is verified
with `iceberg-rust`'s reader in the regular suite.

## Why Bergman owns its commit layer

An Iceberg commit is `(requirements, updates)` applied atomically. The `iceberg`
crate cannot express one from outside: no `Transaction` action removes a data
file, and both `TransactionAction` and `TableCommit`'s builder are `pub(crate)`.
Every piece of a commit is public; only the delivery is not.

| Piece | Upstream API |
|---|---|
| Manifests | `ManifestWriterBuilder` |
| Manifest lists | `ManifestListWriter` |
| Snapshots | `Snapshot::builder` |
| Updates and preconditions | `TableUpdate`, `TableRequirement` — public and `Serialize` |
| Reading with deletes applied | `TableScan` / `ArrowReader` |
| Writing data files | `ParquetWriterBuilder` → `RollingFileWriter` → `DataFileWriter` |
| **Delivering the commit** | **— nothing** |

Bergman writes the manifests, manifest list and snapshot with upstream's own
writers and `POST`s `(requirements, updates)` to the table endpoint — the bytes
`iceberg-catalog-rest` would send. Operations commit through a `TableCommitter`
trait, so an upstream action that can express a rewrite becomes a second
implementation. A fork was not an option: Cargo rejects git dependencies on
crates.io. See [Architecture](@/docs/architecture.md#commit-layer).

Three smaller gaps are filled the same way: expiration's file cleanup, object
listing (`Storage` has no `list`), and a branch's retention
(`TableMetadata::refs` is `pub(crate)`, so Bergman reads it from the metadata
JSON rather than erase it on a commit that moves `main`).

### Where the seam falls

Bergman's commit layer carries the commits Bergman *authors*: compaction,
dangling-delete removal, manifest rewriting. **Snapshot expiration is upstream's
action** and commits through `iceberg::Catalog`, so:

- Expiration's commits carry no `X-Request-Id` and sit outside the
  [audit join](@/docs/rustberg.md#joined-audit-trails). The catalog still
  authorizes and audits them under the same principal.
- The catalog client authenticates through Bergman's token source — the same
  one the commit client uses. See [Credentials](@/docs/operating.md#credentials).
