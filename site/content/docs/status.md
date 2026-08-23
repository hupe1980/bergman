+++
title = "Status"
description = "Which maintenance operations Bergman executes today, which are planned but blocked upstream, and exactly what is blocking them."
weight = 2
+++
Bergman is pre-release. This page is the honest state rather than the intended
one, because the gap between the two is large enough that discovering it by
surprise would be worse than reading about it here.

## What executes

| Operation | Planned | Executed |
|---|:--:|:--:|
| Table health analysis | ✅ | ✅ |
| Snapshot expiration | ✅ | ✅ |
| Expiration file cleanup | ✅ | ✅ |
| Orphan-file removal | ✅ | ✅ |
| Compaction | ✅ | ❌ |
| Manifest rewrite | ✅ | ❌ |

## Why compaction does not execute

Committing a compaction means committing a snapshot that **removes** data files
and adds replacements. As of `iceberg-rust` 0.10.1, and on `main` at the time of
writing, there is no way for an external crate to do that:

- **No action removes files.** `Transaction` offers `fast_append`,
  `expire_snapshots`, `update_schema`, `update_properties`, `update_location`,
  `update_statistics`, `replace_sort_order` and `upgrade_table_version`. None
  removes a data file.
- **`RewriteFilesAction` was never merged.**
  [#1606](https://github.com/apache/iceberg-rust/pull/1606) was closed unmerged.
- **The replacements are still open.**
  [`OverwriteAction` (#2185)](https://github.com/apache/iceberg-rust/pull/2185)
  and the [core CoW rewrite primitive (#2752)](https://github.com/apache/iceberg-rust/pull/2752)
  are open pull requests. The tracking epic,
  [#2186](https://github.com/apache/iceberg-rust/issues/2186), lists
  "compaction via rewrite" unchecked.
- **There is no way round it.** `TransactionAction` is `pub(crate)`, so Bergman
  cannot supply its own action. `TableCommit`'s builder is
  `#[builder(build_method(vis = "pub(crate)"))]`, carrying the comment *"the
  builder is marked as private since it's dangerous and error-prone to construct
  `TableCommit` directly"* — so Bergman cannot bypass the transaction API either.

Manifest rewriting is blocked the same way; its own attempt,
[#1237](https://github.com/apache/iceberg-rust/pull/1237), was also closed
unmerged.

## What Bergman does about it

It plans them anyway, and says so.

The analysis is real: the health analyzer measures the partition, the trigger
fires against your thresholds, and the plan states the table's actual need with
an estimate. The operation is then marked `BLOCKED` with the reason and the
upstream issue number, and `run` reports it as blocked rather than attempting it.

```text
  !! compact
     why: partition d=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
     reads 480 files (2.14 GiB), writes ~5 files
     BLOCKED: compaction needs a commit that removes data files; …
```

The alternative — omitting the operation from the plan — would make an unhealthy
table look healthy. A tool that silently did nothing about a need it had just
measured would be worse than one that never looked.

## Where upstream is asking for help

Two of the operations Bergman *does* execute exist because upstream deliberately
does not provide them:

**Snapshot expiration deletes no files.** `ExpireSnapshotsAction`'s own
documentation says so: *"This only rewrites metadata; the now-unreferenced data
and metadata files are left untouched. Physical file cleanup is the
responsibility of a higher-level maintenance operation built on top of this
action."* Bergman is that operation.

**`FileIO` cannot list.** The `Storage` trait has `read`, `write`, `delete`,
`delete_prefix` and `delete_stream` — but no `list`. Orphan removal is *defined*
by listing storage and subtracting what metadata reaches, so Bergman carries its
own object-store layer, built on OpenDAL and configured from the same
Iceberg-named properties (`s3.endpoint`, `s3.region`, …) the catalog uses.

Both are temporary implementations while the upstream gap exists, and both are
the natural things to contribute back.

## What this means for adopting it

Bergman is useful today if your problem is **metadata growth** — snapshot sprawl,
leaked files, storage that never comes back. That is a large share of real
Iceberg pain and it is fully handled.

If your problem is **small files or delete-file amplification**, Bergman will
measure it precisely and tell you exactly how bad it is, but you still need Spark
or Trino to fix it — until #2185 lands, at which point the execution half slots
in behind planning that already works.
