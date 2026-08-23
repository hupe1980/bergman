+++
title = "Status"
description = "What Bergman executes, why it owns its commit layer, and what is not built."
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
| Compaction | Upstream's scan and writers, Bergman's commit layer |

## Why Bergman owns its commit layer

An Iceberg commit is `(requirements, updates)` applied atomically. The `iceberg`
crate cannot express one from outside:

- **No action removes a data file.** `Transaction` offers `fast_append`,
  `expire_snapshots`, `update_schema`, `update_properties`, `update_location`,
  `update_statistics`, `replace_sort_order` and `upgrade_table_version`. That is
  the complete list.
- **`TransactionAction` is `pub(crate)`**, so no external crate can add one.
- **`TableCommit`'s builder is `pub(crate)`**, so the transaction API cannot be
  bypassed either.

Compaction and manifest rewriting are therefore unreachable through it. The
common answer is to fork — [`nimtable/iceberg-compaction`](https://github.com/nimtable/iceberg-compaction)
pins `risingwavelabs/iceberg-rust` at a git revision, and RisingWave vendored
the logic into its engine. A fork costs a rebase forever and a crate that
cannot be published, since Cargo rejects git dependencies on crates.io.

Bergman owns the one blocked layer instead, and keeps upstream for everything
else — the same call [Rustberg](@/docs/rustberg.md) made about
`iceberg::Catalog`. It works because the blockage is narrow: every piece of a
commit is public, and only the delivery is not.

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
are identical to what `iceberg-catalog-rest` sends — the same serialized types,
the same endpoint. See [Architecture](@/docs/architecture.md#commit-layer).

Operations commit through a `TableCommitter` trait rather than a transport, so
when [#2185](https://github.com/apache/iceberg-rust/pull/2185) or
[#2752](https://github.com/apache/iceberg-rust/pull/2752) lands, a second
implementation wraps it and nothing above `src/commit` changes.

## Owed upstream

Two pieces are natural contributions, and Bergman carries them until they land
there:

- **File cleanup after expiration.** `ExpireSnapshotsAction` documents physical
  cleanup as *"the responsibility of a higher-level maintenance operation built
  on top of this action."*
- **Object listing.** The `Storage` trait has `read`, `write`, `delete`,
  `delete_prefix` and `delete_stream`, but no `list` — without which orphan
  removal cannot exist.

## What is not built

- **Z-order clustering.** Sorting works; z-order does not. It is an
  optimization rather than table health.
- **Spilling to disk.** A sorted partition must fit `max_sort_memory`, and one
  that does not is refused rather than written unsorted.
- **Rewriting across a partition-spec change.** A partition holding files under
  two specs is refused with a named reason rather than rewritten under the
  current spec, which would mis-file rows.
- **Non-Parquet writes.** Bergman reads Parquet, Avro and ORC but writes only
  Parquet, and refuses a table whose `write.format.default` is something else.
- **Event triggers.** The daemon follows the schedules rules declare; it does
  not yet react to catalog commit events.
