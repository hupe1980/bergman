+++
title = "Policies"
description = "How a rule matches a table, how four layers resolve a setting, and how to find out why a value is what it is."
weight = 3
+++
A policy declares *intent*. It does not declare a schedule of rewrites — whether
anything actually runs is decided by measuring the table (see
[Compaction](@/docs/compaction.md#triggers)).

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

`events` gets 3 days. Ordering is the whole disambiguation rule — there is no
specificity scoring, because a scoring function is a thing you have to simulate
in your head to predict.

### Patterns

Globs over `catalog.namespace….table`, with `.` as the separator. `*` stops at a
namespace boundary and `**` crosses it — the same distinction `/` has in a
filesystem glob:

| Pattern | `prod.analytics.events` | `prod.analytics.web.events` |
|---|:--:|:--:|
| `prod.analytics.*` | ✅ | ❌ |
| `prod.analytics.**` | ✅ | ✅ |
| `prod.**` | ✅ | ✅ |
| `prod.streaming.events_*` | ❌ | ❌ |

> [!NOTE]
> This is the rule most likely to surprise, because Iceberg namespaces are
> dotted and a nested namespace reads exactly like a table name. If a rule
> matches less than you expected, you probably want `**`.

A table name may itself contain a dot. Bergman treats it as one segment, so
`prod.analytics.*` matches the table named `a.b`, while `prod.analytics.a.b`
matches the table `b` in namespace `a` — and not the other way round.

### Skipping

```toml
[[rules]]
match = "prod.tmp.**"
skip  = true
```

A skipped table is reported as **skipped**; a table no rule matches is reported
as **unmatched**. The distinction matters: one means "I excluded this", the other
means "my pattern is not doing what I thought".

`skip` alongside settings is a configuration error rather than a silent
preference for one over the other.

## Layering

Every setting resolves through four layers, most specific first:

1. the matching **rule**
2. the config's **`[defaults]`**
3. the **table's own Iceberg properties**
4. the **Iceberg specification default**

Layer 3 is what makes this more than a config file. A table already carries its
owner's intent in its properties, and every other Iceberg tool reads them; a
maintenance engine that ignored them would be a second, competing source of
truth.

Properties consulted:

| Setting | Table property | Spec default |
|---|---|---|
| `compaction.target_file_size` | `write.target-file-size-bytes` | 512 MiB |
| `snapshots.max_age` | `history.expire.max-snapshot-age-ms` | 5 days |
| `snapshots.min_to_keep` | `history.expire.min-snapshots-to-keep` | 1 |
| `manifests.target_size` | `commit.manifest.target-size-bytes` | 8 MiB |
| `manifests.min_count_to_merge` | `commit.manifest.min-count-to-merge` | 100 |

A property that is present but unparseable falls through to the default rather
than failing the run — it is one table's typo, and refusing to maintain a table
over it would be a worse outcome. It is reported as a warning rather than
ignored, because a property that silently does nothing looks exactly like one
that works.

## Provenance

The question is never "what is the target file size". It is "why is it *that*".

```bash
bergman policy explain prod.analytics.events
```

```text
prod.analytics.events
  matched rule: prod.analytics.*

 SETTING                                VALUE      FROM
 compaction.enabled                     false      Bergman default
 compaction.target_file_size            128 MiB    table property write.target-file-size-bytes
 compaction.trigger.small_file_ratio    0.30       Bergman default
 compaction.trigger.min_input_files     5          Bergman default
 compaction.trigger.delete_ratio        0.10       Bergman default
 snapshots.enabled                      true       Bergman default
 snapshots.max_age                      7d         [defaults]
 snapshots.min_to_keep                  3          [defaults]
 snapshots.delete_files                 false      Bergman default
 manifests.rewrite                      false      Bergman default
 orphans.enabled                        false      Bergman default
 orphans.mode                           DryRun     Bergman default
 orphans.older_than                     7d         Bergman default
```

Every row names the layer that answered. A setting whose origin cannot be shown
is a setting nobody can debug.

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

`unmatched` on a table you expected to cover is the `*` versus `**` distinction
above, nine times out of ten.

## Defaults

Bergman's own defaults, for knobs Iceberg does not define:

| Setting | Default | Note |
|---|---|---|
| `compaction.trigger.small_file_ratio` | 0.3 | |
| `compaction.trigger.min_input_files` | 5 | Matches Spark's `rewrite_data_files` |
| `compaction.trigger.delete_ratio` | 0.1 | |
| `compaction.trigger.min_file_size_ratio` | 0.75 | What counts as "small" |
| `compaction.max_concurrent_file_groups` | 4 | |
| `orphans.older_than` | 7 days | Floor of 24h, [not configurable](@/docs/orphans.md#the-grace-period) |

Everything that rewrites or deletes data defaults **off**. Metadata-only
snapshot expiration is the single exception.

## Validation

Unknown keys are errors, not ignored — a typo that parses is a setting that
silently does nothing. `bergman policy lint` runs offline, contacting no catalog,
so it belongs in CI:

```yaml
- run: bergman policy lint --config bergman.toml
```

Schedules accept the five-field crontab form everyone writes (`0 */2 * * *`) as
well as the six-field form with seconds.
