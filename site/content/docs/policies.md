+++
title = "Policies"
description = "How a rule matches a table, how five layers resolve a setting, and how to find out why a value is what it is."
weight = 3
+++
A policy declares *intent*; whether anything runs is decided by measuring the
table ([Compaction](@/docs/compaction.md#triggers)).

## Rules

Rules are evaluated in order, and the **first match wins**.

```toml
[[rules]]
match = "prod.analytics.events"     # specific rules first
[rules.snapshots]
max_age = "3d"

[[rules]]
match = "prod.analytics.*"          # general rules after
[rules.snapshots]
max_age = "30d"
```

`events` gets 3 days. There is no specificity scoring. A catch-all written
**before** a specific rule makes it dead, and Bergman refuses to start:

```toml
[[rules]]
match = "prod.**"

[[rules]]
match = "prod.tmp.**"     # unreachable — `prod.**` already matched
skip  = true
```

```text
policy error: rules[1] (match = "prod.tmp.**"): this rule can never match,
because rule "prod.**" is listed before it and already matches everything it
does. Rules are evaluated in order and the first match wins, so put the
specific rule first
```

The check is sound, not complete: it catches a repeated pattern and a literal
prefix followed by `**`. `bergman policy match` shows what a rule set actually
selects.

### Patterns

Globs over `catalog.namespace….table`, with `.` as the separator. `*` stops at a
namespace boundary and `**` crosses it:

| Pattern | `prod.analytics.events` | `prod.analytics.web.events` |
|---|:--:|:--:|
| `prod.analytics.*` | ✅ | ❌ |
| `prod.analytics.**` | ✅ | ✅ |
| `prod.**` | ✅ | ✅ |
| `prod.streaming.events_*` | ❌ | ❌ |

> [!NOTE]
> A nested namespace reads exactly like a table name. If a rule matches less
> than expected, you probably want `**`.

A dot inside a name is one segment; write `\.` to match it literally:

| Pattern | Matches |
|---|---|
| `prod.analytics.a.b` | table `b` in namespace `analytics.a` |
| `prod.analytics.a\.b` | table `a.b` in namespace `analytics` |

`/` and `%` in a name are percent-encoded per segment before matching, so the
table `b/c` in namespace `a` and the table `c` in namespace `a.b` are never
confused. Write the name as it is — `prod.a.b/c`.

### Skipping

```toml
[[rules]]
match = "prod.tmp.**"
skip  = true
```

A skipped table is reported as **skipped**; a table no rule matches as
**unmatched**. `skip` alongside settings is a configuration error.

## Layering

Every setting resolves through five layers, most specific first:

1. the matching **rule**
2. the config's **`[defaults]`**
3. the **catalog's policy**, when the catalog is configured as a
   [policy source](#catalog-policy)
4. the **table's own metadata** — its Iceberg properties, and its `sort-order`
5. the **Iceberg specification default**, or Bergman's own for knobs Iceberg
   does not define

Layer 4 means a table's own configuration is honoured. Properties consulted:

| Setting | Table property | Spec default |
|---|---|---|
| `compaction.target_file_size` | `write.target-file-size-bytes` | 512 MiB |
| `snapshots.max_age` | `history.expire.max-snapshot-age-ms` | 5 days |
| `snapshots.min_to_keep` | `history.expire.min-snapshots-to-keep` | 1 |
| `manifests.target_size` | `commit.manifest.target-size-bytes` | 8 MiB |
| `manifests.min_count_to_merge` | `commit.manifest.min-count-to-merge` | 100 |

And one thing that is not a property:

| Setting | Table metadata | Default |
|---|---|---|
| `compaction.sort` | the table's `sort-order` | unsorted |

A rewrite keeps the table's declared clustering — see
[Compaction](@/docs/compaction.md#the-tables-own-sort-order).

A property that is present but unusable falls through to the default and is
reported as a warning. **Zero is unusable** for every size and count (a zero
target file size would make every file oversized). An age keeps its zero:
`history.expire.max-snapshot-age-ms = 0` means no snapshot is too young, and
`min_to_keep` still bounds what goes.

## Catalog policy {#catalog-policy}

Apache Polaris stores maintenance policies on the catalog, a namespace or a
table, inherited down the tree. With `policy_source = "polaris"` Bergman reads
them as layer 3:

```toml
[[catalogs]]
name = "prod"
uri = "https://polaris.example.com/api/catalog"
warehouse = "prod"
policy_source = "polaris"
```

Per planned table Bergman calls
`GET {uri}/polaris/v1/{prefix}/applicable-policies?namespace=…&target-name=…`
(prefix from `/v1/config`, every page followed); Polaris applies inheritance.

**It fills only what `bergman.toml` leaves unset.** A matching rule and
`[defaults]` win, and it never recruits a table no rule matches. To let Polaris
drive every table, add a catch-all rule with no settings:

```toml
[[rules]]
match = "prod.**"
```

What is read, and where it lands:

| Policy type | Field | Setting |
|---|---|---|
| `system.data-compaction` | `enable` | `compaction.enabled` |
| | `config.target_file_size_bytes` | `compaction.target_file_size` |
| `system.metadata-compaction` | `enable` | `manifests.rewrite` |
| `system.snapshot-expiry` | `enable` | `snapshots.enabled` |
| | `config.max_snapshot_age_days` | `snapshots.max_age` |
| | `config.min_snapshot_to_keep` | `snapshots.min_to_keep` |
| `system.orphan-file-removal` | `enable` | `orphans.enabled` |
| | `max_orphan_file_age_in_days` | `orphans.older_than` |

Only content of schema version `2025-02-03` is read. Everything else is
**reported as a plan warning**, not silently dropped:

- an orphan-file-removal policy's `locations` — Bergman scans only the table's
  own location;
- any other `config` key;
- **two policies of one type** on one table — neither is applied;
- content that does not parse, or names another schema version.

A catalog policy **never sets orphan `mode`**: enabling orphan removal through
Polaris enables the scan, which stays `dry-run` unless `bergman.toml` says
otherwise.

**A policy that cannot be read skips that table, not the run**, with the reason
in the plan.

`bergman policy explain` names the policy, its type, and where it was inherited
from:

```text
 compaction.target_file_size   256 MiB   catalog policy "compaction-policy" (system.data-compaction, inherited from finance)
```

## Provenance


```bash
bergman policy explain prod.analytics.events
```

```text
prod.analytics.events
  matched rule: prod.analytics.*

 SETTING                                 VALUE                    FROM
 compaction.enabled                      true                     rule "prod.analytics.*"
 compaction.target_file_size             128 MiB                  table property write.target-file-size-bytes
 compaction.sort                         event_date, region desc  the table's sort order
 compaction.trigger.small_file_ratio     0.30                     Bergman default
 compaction.trigger.min_file_size_ratio  0.75                     Bergman default
 compaction.trigger.max_file_size_ratio  1.80                     Bergman default
 compaction.trigger.min_input_files      5                        Bergman default
 compaction.trigger.delete_ratio         0.10                     Bergman default
 compaction.trigger.min_file_age         1h                       Bergman default
 compaction.max_sort_memory              1.00 GiB                 Bergman default
 compaction.max_group_bytes              8.00 GiB                 Bergman default
 compaction.max_input_files              10000                    Bergman default
 snapshots.enabled                       true                     Bergman default
 snapshots.max_age                       7d                       [defaults]
 snapshots.min_to_keep                   3                        [defaults]
 snapshots.delete_files                  false                    Bergman default
 manifests.rewrite                       false                    Bergman default
 manifests.target_size                   8.00 MiB                 Iceberg default
 manifests.min_count_to_merge            100                      Iceberg default
 orphans.enabled                         false                    Bergman default
 orphans.mode                            dry-run                  Bergman default
 orphans.older_than                      7d                       Bergman default
 orphans.min_interval                    24h                      Bergman default
```

Every resolved setting is listed, each with the layer that answered.

## Which tables a policy covers

```bash
bergman policy match
```

```text
 TABLE                        DECISION     RULE
 prod.analytics.events        maintained   prod.analytics.*
 prod.analytics.web.events    unmatched
 prod.tmp.scratch             skipped      prod.tmp.**
```

`unmatched` on a table you expected to cover is usually `*` where `**` was
meant.

## Defaults

Bergman's own defaults, for knobs Iceberg does not define:

| Setting | Default | Note |
|---|---|---|
| `compaction.trigger.small_file_ratio` | 0.3 | |
| `compaction.trigger.min_input_files` | 5 | Matches Spark's `rewrite_data_files` |
| `compaction.trigger.delete_ratio` | 0.1 | |
| `compaction.trigger.min_file_size_ratio` | 0.75 | What counts as "small", and therefore worth reading |
| `compaction.trigger.max_file_size_ratio` | 1.8 | What counts as "oversized" — a multiple of the target, not a fraction |
| `compaction.trigger.min_file_age` | 1 hour | How long a partition must be quiet before it is rewritten |
| `compaction.max_group_bytes` | 8 GiB | Bytes per [file group](@/docs/compaction.md#file-groups) |
| `compaction.max_input_files` | 10 000 | Files per file group |
| `compaction.max_sort_memory` | 1 GiB | Executor memory pool; the sort and the anti-join spill when they reach it |
| `compaction.sort` | the table's own `sort-order` | Output clustering — see [Compaction](@/docs/compaction.md#the-tables-own-sort-order) |
| `orphans.older_than` | 7 days | Floor of 24h, [not configurable](@/docs/orphans.md#the-grace-period) |
| `orphans.min_interval` | 24 hours | Shortest gap between two [scans](@/docs/orphans.md#cadence) of one table |
| `limits.max_deletes_per_run` | 100 000 | Ceiling on one operation's blast radius — every deleter, not only the scanner |

Everything that rewrites or deletes data defaults **off**. Metadata-only
snapshot expiration is the single exception.

## Validation

Unknown keys are errors. `bergman policy lint` runs offline, so it belongs in
CI:

```yaml
- run: bergman policy lint --config bergman.toml
```

Schedules accept five-field crontab (`0 */2 * * *`) and six-field with seconds.
