# Changelog

Notable changes to Bergman. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Bergman is pre-1.0: **breaking changes land in minor versions**, each with a migration note.

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

[0.2.0]: https://github.com/hupe1980/bergman/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/hupe1980/bergman/releases/tag/v0.1.0
