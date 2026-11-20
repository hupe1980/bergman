# Changelog

Notable changes to Bergman. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Bergman is pre-1.0: **breaking changes land in minor versions**, each with a migration note.

## [0.5.0] — 2026-11-20

**Breaking:** `obs::router`, `obs::router_with_events` and `obs::serve` take a `sched::StatusBoard`
— pass `daemon.status()`. `metrics` is a default feature; a library that wants neither `axum` nor
`prometheus-client` sets `default-features = false`.

### Added

- **`bergman status`** asks a running daemon what it is doing and what needs attention, from the
  new **`/status`** endpoint. No config or credentials needed; failures and refusals print first;
  exits `2` when something needs attention. `Daemon::status()` exposes the same board to embedders.

### Changed

- `cargo install bergman` includes the daemon's HTTP surface and `bergman status`, like every
  published binary.

### Security

- `chrono`, `chrono-tz` and `humantime` (RUSTSEC-2026-0543, -0555, -0542) are flagged unmaintained,
  with no fix and no vulnerability. They reach Bergman through arrow, datafusion, iceberg and
  object_store, and are ignored in `deny.toml` with that reason.

## [0.4.0] — 2026-11-08

**Breaking:** MSRV **1.95**. `Bergman::refresh_credentials()` is removed — tokens renew on their
own; delete the call. `CatalogConfig::connect()` returns both clients and `committer()` is gone.
`RestCommitter::connect` takes an `Arc<TokenSource>`; `reachability::compute` drops its `TableRef`;
`health::file_index`, `FileIndex` and `commit::authoring_refusal` are removed;
`TableHealth::object_storage_layout` is gone and `TableFacts` gains `catalog`. A 0.3.0
`bergman.toml` parses unchanged.

### Added

- **Format v3 tables are maintained** — compaction, manifest rewriting and dangling-delete removal.
  Every row keeps its `_row_id` and `_last_updated_sequence_number`; deletion vectors are applied
  and retired per vector.
- **Polaris policy as a resolution layer** (`policy_source = "polaris"`), below `bergman.toml`.
- **Lakekeeper CloudEvents** on `/events?catalog=<name>`, structured and binary mode.
- **Homebrew**: `brew install hupe1980/tap/bergman` (macOS on Apple Silicon, Linux). Releases include
  a macOS arm64 binary.
- **`just cross-engine`**: PyIceberg 0.12.0 writes tables, Bergman maintains them, PyIceberg reads
  them back.

### Changed

- **`iceberg` 0.11.0** and `opendal` 0.58.
- **One token source for both catalog clients**, renewed on demand.
- **`write.object-storage.enabled` is honoured.**

### Fixed

- **A dangling deletion vector could have been retired.** Vectors are now excluded explicitly.
- **The snapshot producer did not refuse encrypted tables by name** when driven directly.

### Security

- rustls 0.23.46, rustls-webpki 0.103.16, webpki-root-certs 1.0.11 (RUSTSEC-2026-0346, -0347,
  -0348, -0349, -0382, -0383); tokio 1.54.1 (RUSTSEC-2026-0386); zstd 0.13.4, zstd-safe 7.4.0,
  zstd-sys 2.1.2 (RUSTSEC-2026-0391, -0398, -0403, -0410); ctutils 0.4.8 (RUSTSEC-2026-0385);
  parquet 58.4.1 with thrift 0.23.0 (RUSTSEC-2026-0435).

## [0.3.0] — 2026-09-18

**Breaking:** `[limits]` gained two optional keys, `TableHealth` gained a field, and
`health::analyze`, `health::file_index`, `reachability::compute` and `ops::delete` take a
concurrency argument. A `bergman.toml` written for 0.2.0 parses unchanged.

### Added

- **Encrypted tables are refused by name, per operation.** Iceberg native encryption
  (`encryption.key-id`) encrypts a table's manifests and manifest lists, which Bergman cannot
  read, so compaction, manifest rewriting and orphan removal are refused with that reason in
  `bergman plan`. Snapshot expiration still runs — it needs no manifest — with its file-cleanup
  half skipped and reported.
- **`limits.compaction_parallelism`** — CPU threads for one file group's rewrite. Absent means
  the machine's available parallelism.
- **`limits.io_concurrency`** (default 32) — object-store requests in flight per operation,
  covering manifest reads and deletions.

### Changed

- **Compaction uses every core.** The executor ran on a single DataFusion partition, so one
  table's rewrite used one core whatever the host had. Memory is unaffected: the spill pool is
  per session, not per partition.
- **The Parquet reader is tuned for object storage** — a 64 KiB footer size hint removes a round
  trip per file, and data-file concurrency follows `io_concurrency` rather than the core count.
- **Manifests shared between snapshots are read once.** The reachability walk re-read the whole
  history once per snapshot.
- **The v3 refusal names both of its reasons**, row lineage and deletion vectors, rather than
  only the first.

### Fixed

- **`write.metadata.path` was ignored**, so manifests and manifest lists were written to
  `<location>/metadata` whatever the table said — and failed with no useful error where that
  prefix is not writable.
- **Bloom filters and dictionary settings were dropped on rewrite.**
  `write.parquet.bloom-filter-enabled.column.*`, `-fpp.column.*`, `dict-size-bytes` and
  `page-row-limit` are honoured now, as the other `write.parquet.*` settings already were.
- **`write.object-storage.enabled` is reported rather than silently ignored.** Bergman cannot
  produce the hashed layout, so repeated compaction concentrates a table's objects in one
  prefix; `bergman plan` now says so once per table.
- **The format v3 writer paths returned invalid snapshots if reached.** They are explicit errors
  now, so lifting the v3 refusal cannot accidentally enable a writer that renumbers rows.
- **Two panics became errors:** a poisoned ledger mutex in the daemon, and a format v1 table
  carrying delete files.

### Security

- **rustls 0.23.43 → 0.23.45** ([RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285)):
  TLS 1.3 handshake messages accepted across encryption-level boundaries. On the path of every
  catalog commit and object-store request.

## [0.2.0] — 2026-09-13

A correctness release. Four defects were found by auditing the implementation
against its own design documents, and a fifth by writing the tests that prove the
orphan checks are load-bearing. Three of the five were silent.

### Fixed

- **Delete files were identified by path, which is unsafe for Iceberg v3.** A
  deletion vector is a blob at an offset inside a Puffin file, and one Puffin file
  may hold the vectors of many data files. Retiring one dangling vector would have
  dropped every live entry sharing its container, resurrecting the rows those
  vectors were hiding. Identity is now `FileRef { path, offset }`. Unreachable in
  0.1.0 because v3 rewrites are refused, but reachable by any v3 work built on it.
- **The row-count contract could not detect row loss when any delete file
  applied.** It compared the output against a ceiling only, so a rewrite that
  dropped every row passed — putting the weakest check on the delete-heavy tables
  compaction matters most for. The contract is now a closed interval with a floor:
  the rows of inputs that no delete file applies to may not disappear.
- **File groups were ranked by file count, so cost never entered the decision.**
  Ties on count are rare, so a group merging a hundred 100 MB files outranked one
  merging ninety-nine 1 KB files that bought nearly the same reduction for a
  hundred-thousandth of the I/O. Under a byte budget or a maintenance window that
  is most of a cycle spent on the wrong work. Ranking is now benefit per byte.
- **`plan` under-promised what `run` would do.** The planner gated "offer every
  file in this partition" on the delete-ratio *trigger* where it needed "this
  partition holds any delete file". Partitions with a few deletes below the ratio
  had at-target delete-carrying files rewritten by the executor and counted by
  neither the plan nor the byte budget.

### Added

- **Orphan-removal check 8: the listing and the table's metadata must share at
  least one file.** A full reachable set and a full listing that do not intersect
  means one of the two is describing a different world — a `location` changed
  after the data was written, a bucket alias or URI scheme normalization did not
  fold together, a credential scoped to a subdirectory. Every live file then looks
  exactly like garbage and checks 1–7 all pass it. The scan now refuses and names
  both counts.
- **Orphan safety is tested as load-bearing.** Each check is pinned by a pair
  against one fixture: a control that removes the hazard and asserts the file *is*
  deleted, and a guarded case that restores it and asserts the file survives. The
  control is what stops a guarded test passing vacuously. Writing these pairs is
  how check 8 was found.

### Changed

- **The release pipeline is ordered by increasing irreversibility** and runs
  strictly serially: verify → binaries → image → crates.io → GitHub release. A
  crates.io version is permanent even after a yank, while a GHCR tag and a
  GitHub release can be deleted, so the step that cannot be undone runs after
  everything that can still fail has not. Previously the GitHub release ran
  beside the image and the publish, so it could announce a version whose image
  push had failed and whose crate never published.
- **The release workflow now verifies the tagged commit.** `ci.yml` runs on
  branches and pull requests, not tags, so nothing checked what a tag actually
  pointed at before publishing it. Releases now run the suite, the lints and
  `cargo package` first.
- **`workflow_dispatch` is a true rehearsal.** It skipped the container push but
  still ran `cargo publish` when dispatched against a tag — skipping the
  reversible artefact and performing the irreversible one. It now publishes
  nothing.
- **The container `latest` tag works.** It was gated on `is_default_branch`,
  which is never true in a tag-triggered workflow, so `latest` was never
  applied. It now follows the tag, and is withheld from pre-releases.
- **Breaking:** `RewriteFiles::removed` is `Vec<FileRef>` rather than
  `Vec<String>`, and `SnapshotProducer::dangling_delete_files` returns
  `Vec<FileRef>`. *Migration:* wrap paths with `FileRef::path(p)`. A `FileRef`
  built from a path does not match a deletion-vector entry, which carries an
  offset — so such a removal fails the commit's liveness check loudly rather than
  matching by path and taking live siblings with it.
- The `MaintenanceObserver::deleting_files` hook is announced whole files only.
  A blob reference names a vector inside a container other vectors may share, and
  the hook's contract is "these paths are about to stop existing".

## [0.1.0] — 2026-08-24

Initial release. Table health analysis, snapshot expiration with file cleanup,
orphan-file removal, manifest rewriting, and compaction with delete-file
application — as one crate, one binary, and a library, on `iceberg-rust`.

[0.5.0]: https://github.com/hupe1980/bergman/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/hupe1980/bergman/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/hupe1980/bergman/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/hupe1980/bergman/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/hupe1980/bergman/releases/tag/v0.1.0
