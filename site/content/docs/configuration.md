+++
title = "Configuration"
description = "Every setting in bergman.toml: catalogs, defaults, rules and limits."
weight = 4
+++
One TOML file. Every setting is optional: what it leaves unset falls through to
the [catalog's policy](#catalog-policy) when one is read, then the table's
Iceberg properties, then the specification default — see
[Policies](@/docs/policies.md#layering). Validate it offline:

```bash
bergman policy lint
```

## `[[catalogs]]`

```toml
[[catalogs]]
name       = "prod"
kind       = "rest"
uri        = "http://localhost:8181/catalog"
warehouse  = "s3://lake/warehouse"
token_env  = "BERGMAN_CATALOG_TOKEN"
namespaces = ["analytics", "streaming.raw"]

[catalogs.properties]
"s3.region"            = "eu-central-1"
"s3.endpoint"          = "http://minio:9000"
"s3.path-style-access" = "true"
```

| Key | Required | Notes |
|---|:--:|---|
| `name` | ✅ | First segment of every table reference, and what rule patterns match against. **May not contain a dot** |
| `kind` | | `rest`, the only protocol — it reaches Rustberg, Polaris, Lakekeeper, Nessie and Unity |
| `uri` | ✅ | Catalog endpoint |
| `warehouse` | | Required when the catalog serves several; also checks the right storage feature is compiled in |
| `storage` | | `s3`/`gcs`/`azure`/`fs`/`memory`. A startup check only — storage is resolved per path |
| `token_env` | | The **name** of the environment variable holding the bearer token, never the token |
| `namespaces` | | Restrict discovery to these subtrees. Dotted, as in rule patterns |
| `properties` | | Iceberg property names (`s3.endpoint` means what it means in Spark, Trino and PyIceberg) |
| `policy_source` | | `"polaris"` reads each table's Apache Polaris maintenance policy as a layer below this file. Off when absent — see [below](#catalog-policy) |

Properties reach both the catalog's file access and the object-store client
orphan scanning uses. Prefer the provider chain or catalog-vended credentials
over keys in `properties` — see [Operating](@/docs/operating.md#credentials).

### Catalog policy {#catalog-policy}

```toml
[[catalogs]]
name          = "prod"
uri           = "https://polaris.example.com/api/catalog"
warehouse     = "prod"
policy_source = "polaris"
```

The catalog policy fills only what a matching rule and `[defaults]` leave unset.
It never recruits a table no rule matches (use a catch-all rule to let Polaris
drive everything) and never sets orphan `mode`. A table whose policy cannot be
read is skipped that cycle, with the reason in the plan. Fields and mapping:
[Policies](@/docs/policies.md#catalog-policy).

## `[defaults]` and `[[rules]]`

Identical setting blocks. `[defaults]` applies to every matched table; a rule's
own values override it.

### `snapshots`

```toml
[defaults.snapshots]
enabled      = true      # metadata-only expiration; on by default
max_age      = "7d"      # -> history.expire.max-snapshot-age-ms -> 5 days
min_to_keep  = 3         # -> history.expire.min-snapshots-to-keep -> 1
delete_files = false     # reclaim orphaned files during expiration
```

`min_to_keep = 0` is refused at startup. See [Snapshots](@/docs/snapshots.md).

### `orphans`

```toml
[defaults.orphans]
enabled      = false      # the scanner runs at all
mode         = "dry-run"  # or "delete"
older_than   = "7d"       # hard floor of 24h
min_interval = "24h"      # shortest gap between two scans of one table
```

`older_than` below 24 hours is refused at startup and again in the scanner.
`min_interval` applies within one process; a one-shot `bergman run` always
scans — see [Cadence](@/docs/orphans.md#cadence). The blast-radius ceiling is
[`limits.max_deletes_per_run`](#limits), shared by every deleter. Read
[Orphan files](@/docs/orphans.md) before setting `mode = "delete"`.

### `compaction`

```toml
[defaults.compaction]
enabled          = false
target_file_size = 536870912    # -> write.target-file-size-bytes -> 512 MiB
sort             = ["event_date", "customer_id"]
max_sort_memory  = 1073741824   # 1 GiB executor memory pool
max_group_bytes  = 8589934592   # 8 GiB per file group
max_input_files  = 10000        # files per file group

[defaults.compaction.trigger]
small_file_ratio    = 0.3     # fraction of small files that triggers a rewrite
min_input_files     = 5       # matches Spark's rewrite_data_files
delete_ratio        = 0.1     # delete records as a fraction of rows
min_file_size_ratio = 0.75    # what counts as "small", and worth reading
max_file_size_ratio = 1.8     # what counts as "oversized", and worth splitting
min_file_age        = "1h"    # leave a partition alone until it settles
```

Ratios outside 0–1 are refused at startup, as is an empty `sort` list and a
`max_input_files` below 2.

`max_group_bytes` and `max_input_files` bound a **file group**, the unit a
rewrite commits at; a partition is bin-packed into as many groups as it needs
([File groups](@/docs/compaction.md#file-groups)). `min_file_age` leaves a
partition alone until its newest file has settled. `sort` orders rows within
each file group. `max_sort_memory` is the executor's memory pool; the sort and
the equality-delete anti-join **spill to disk** at it
([Compaction](@/docs/compaction.md#query-engine)).

### `manifests`

```toml
[defaults.manifests]
rewrite            = false
target_size        = 8388608   # -> commit.manifest.target-size-bytes -> 8 MiB
min_count_to_merge = 100       # -> commit.manifest.min-count-to-merge
```

### `schedule`

```toml
[[rules]]
match    = "prod.streaming.**"
schedule = "0 */2 * * *"
```

When a table is evaluated, not whether anything runs — measurements decide
that. Five-field crontab (at second zero) or six-field with seconds.
[`bergman daemon`](@/docs/operating.md#as-a-daemon) wakes at the earliest due
schedule and evaluates **only the tables that rule matches**. `bergman run`
ignores `schedule`; scope it with `--table`.

### `skip`

```toml
[[rules]]
match = "prod.tmp.**"
skip  = true
```

Cannot be combined with settings; doing so is a configuration error.

## `[limits]`

```toml
[limits]
max_parallel_tables       = 4
max_rewrite_bytes_per_run = 536870912000   # 500 GiB
max_deletes_per_run       = 100000         # blast-radius ceiling, every deleter
io_concurrency            = 32             # object-store requests in flight
# compaction_parallelism  = 8              # CPU threads per group; omit for auto
maintenance_window        = "22:00-06:00 Europe/Berlin"
operation_timeout         = "1h"           # optional; bounds unbounded waiting
```

- `io_concurrency` bounds **latency-bound** work — manifest reads and deletions —
  as object-store requests in flight. Lower it when Bergman shares a service's
  HTTP connection pool.
- `compaction_parallelism` bounds **CPU-bound** work: the DataFusion
  `target_partitions` of a rewrite. Absent means available parallelism. It does
  not raise memory use; `compaction.max_sort_memory` is the ceiling, shared
  across threads.
- `max_deletes_per_run` caps the files any one operation deletes —
  [orphan removal](@/docs/orphans.md) and
  [expiration's file cleanup](@/docs/snapshots.md) alike. What is withheld is
  reported and reclaimed by the next pass. `0` is refused at startup.
- `max_rewrite_bytes_per_run` is charged per operation, and only to
  [compaction](@/docs/compaction.md). When it cannot cover everything, tables
  are ordered most-fragmented-first and the rest is reported as **deferred**.
  Expiration, manifest rewriting and orphan removal never consume it, so a table
  too fragmented to compact still keeps its history and manifests in check.
- `maintenance_window` governs when work *begins*; a cycle under way finishes.
  Outside it, `run` reports every planned table as deferred and
  [`daemon`](@/docs/operating.md#as-a-daemon) sleeps to the window's edge. **The
  timezone is mandatory** — `"22:00-06:00"` alone is a startup error.
- `operation_timeout` bounds *waiting* — an object store that accepts a
  connection and never answers. Cancelling is safe: an interrupted rewrite
  leaves only unreferenced files, and a commit is one atomic request. Absent by
  default; meant for the daemon.

## Global flags

| Flag | Env | Default |
|---|---|---|
| `--config` | `BERGMAN_CONFIG` | `bergman.toml` |
| `--format` | | `text` (or `json`) |
| `--audit-log` | `BERGMAN_AUDIT_LOG` | none |
| `--log` | `BERGMAN_LOG` | `warn` |

Logs go to stderr, so `--format json` on stdout stays machine-readable.

## A complete example

The repository ships [`bergman.example.toml`](https://github.com/hupe1980/bergman/blob/main/bergman.example.toml),
annotated and checked against the parser by the test suite.
