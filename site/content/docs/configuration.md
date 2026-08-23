+++
title = "Configuration"
description = "Every setting in bergman.toml: catalogs, defaults, rules and limits."
weight = 4
+++
One TOML file. Every setting is optional — what a policy does not state falls
through to the table's own Iceberg properties and then to the specification
default, so this file says only what differs from what your tables already
declare. See [Policies](@/docs/policies.md) for how that resolution works.

Validate it offline, contacting nothing:

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
| `kind` | | `rest` — the only protocol implemented, and the one that reaches Rustberg, Polaris, Lakekeeper, Nessie and Unity |
| `uri` | ✅ | Catalog endpoint |
| `warehouse` | | Required when the catalog serves several. Also used to check the right storage feature is compiled in |
| `storage` | | `s3`/`gcs`/`azure`/`fs`/`memory`. Only a startup check — storage is resolved per path at read time, so one catalog may span schemes |
| `token_env` | | The **name** of the environment variable holding the bearer token. Never the token: a credential in a config file is a credential in version control |
| `namespaces` | | Restrict discovery instead of walking the whole catalog. Dotted, as in rule patterns. Turns an O(catalog) tree walk into an O(subtree) one |
| `properties` | | Iceberg's own property names, so `s3.endpoint` means here what it means in Spark, Trino and PyIceberg |

Properties reach both clients: the catalog's file access and the object-store
client orphan scanning needs.

Prefer the provider chain (instance role, workload identity) or catalog-vended
credentials over putting keys in `properties`. See [Operating](@/docs/operating.md#credentials).

## `[defaults]` and `[[rules]]`

The two carry identical setting blocks. `[defaults]` applies to every table a
rule matches; a rule's own values override it.

### `snapshots`

```toml
[defaults.snapshots]
enabled      = true      # metadata-only expiration; on by default
max_age      = "7d"      # -> history.expire.max-snapshot-age-ms -> 5 days
min_to_keep  = 3         # -> history.expire.min-snapshots-to-keep -> 1
delete_files = false     # reclaim orphaned files during expiration
```

`min_to_keep = 0` is refused at startup — expiring every snapshot would leave the
table unreadable, and upstream would reject the commit anyway. Failing early
means learning before a cycle runs.

See [Snapshots](@/docs/snapshots.md).

### `orphans`

```toml
[defaults.orphans]
enabled    = false       # the scanner runs at all
mode       = "dry-run"   # or "delete"
older_than = "7d"        # hard floor of 24h
```

`older_than` below 24 hours is refused at startup and again in the scanner. Read
[Orphan files](@/docs/orphans.md) before setting `mode = "delete"`.

### `compaction`

```toml
[defaults.compaction]
enabled                    = false
target_file_size           = 536870912   # -> write.target-file-size-bytes -> 512 MiB
sort                       = ["event_date", "customer_id"]
max_sort_memory            = 1073741824   # 1 GiB

[defaults.compaction.trigger]
small_file_ratio    = 0.3     # fraction of small files that triggers a rewrite
min_input_files     = 5       # matches Spark's rewrite_data_files
delete_ratio        = 0.1     # delete records as a fraction of rows
min_file_size_ratio = 0.75    # what counts as "small"
```

Ratios outside 0–1 are refused at startup, as is an empty `sort` list.

`sort` orders rows globally within each partition, which needs the file group in
memory. `max_sort_memory` bounds that; a larger partition is refused rather than
written unsorted. See [Compaction](@/docs/compaction.md#sort-based-clustering).

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

Governs when evaluation runs, not whether anything executes — the health
analyzer decides that. Both the five-field crontab form everyone writes and the
six-field form with seconds are accepted; a five-field expression means "at
second zero".

Read by [`bergman daemon`](@/docs/operating.md#as-a-daemon), which wakes at the
earliest schedule any rule declares. Under `bergman run` the whole cycle is
driven by whatever scheduler invoked it, so `schedule` is inert.

### `skip`

```toml
[[rules]]
match = "prod.tmp.**"
skip  = true
```

Cannot be combined with settings — both cannot apply, and silently preferring one
would make the other a line that does nothing.

## `[limits]`

```toml
[limits]
max_parallel_tables       = 4
max_rewrite_bytes_per_run = 536870912000   # 500 GiB
maintenance_window        = "22:00-06:00 Europe/Berlin"
```

When the byte budget cannot cover everything, tables are ordered
most-fragmented-first and the remainder is reported as **deferred**, never
silently dropped — the run report names them. Metadata-only work is not charged
against it, so a rewrite ceiling cannot block snapshot expiration.

`maintenance_window` governs when work *begins*. A cycle already under way runs
to completion: stopping mid-rewrite at the window's edge would leave files
written and uncommitted, which is worse than finishing. Outside it, `run` does
nothing and reports every planned table as deferred, and
[`daemon`](@/docs/operating.md#as-a-daemon) sleeps to the edge rather than waking
every interval to find it shut.

**The timezone is mandatory.** A window in local time moves when a replica is
scheduled in another region, and "not during business hours" must not move. It
is parsed at startup, so a malformed one is a startup failure:

```
policy error: maintenance_window "22:00-06:00" has no timezone; write it as
"22:00-06:00 Europe/Berlin". A window without one moves when a replica is
scheduled in another region.
```

## Global flags

| Flag | Env | Default |
|---|---|---|
| `--config` | `BERGMAN_CONFIG` | `bergman.toml` |
| `--format` | | `text` (or `json`) |
| `--audit-log` | `BERGMAN_AUDIT_LOG` | none |
| `--log` | `BERGMAN_LOG` | `warn` |

Logs go to stderr, so `--format json` on stdout stays machine-readable at any
log level.

## A complete example

The repository ships [`bergman.example.toml`](https://github.com/hupe1980/bergman/blob/main/bergman.example.toml),
which is annotated throughout and checked against the real parser by the test
suite — so it cannot drift from the schema.
