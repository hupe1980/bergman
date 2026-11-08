+++
title = "Operating"
description = "Running Bergman as a cron job, wiring credentials, reading the audit trail, and what the exit codes mean."
weight = 10
+++
`bergman run` performs one cycle and exits, so it fits a scheduler you already
run.

## As a cron job

```bash
bergman run \
  --config /etc/bergman/bergman.toml \
  --audit-log /var/log/bergman.jsonl
```

### Kubernetes

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: bergman
spec:
  schedule: "0 * * * *"
  concurrencyPolicy: Forbid      # a cycle that overruns must not overlap itself
  jobTemplate:
    spec:
      backoffLimit: 0            # a failed cycle is retried by the next schedule,
      template:                  # not immediately — the table is probably busy
        spec:
          restartPolicy: Never
          serviceAccountName: bergman
          containers:
            - name: bergman
              image: ghcr.io/hupe1980/bergman:latest
              args: ["run", "--config", "/etc/bergman/bergman.toml"]
              env:
                - name: BERGMAN_CATALOG_TOKEN
                  valueFrom:
                    secretKeyRef: { name: bergman-catalog, key: token }
                - name: BERGMAN_LOG
                  value: info
              volumeMounts:
                - name: config
                  mountPath: /etc/bergman
              resources:
                requests: { cpu: 500m, memory: 512Mi }
                # See "Sizing" below: compaction's memory pool is
                # max_parallel_tables x max_sort_memory, 4 GiB by default.
                limits:   { memory: 6Gi }
          volumes:
            - name: config
              configMap: { name: bergman-config }
```

Overlapping cycles are safe — commits are compare-and-swap — but waste work, hence
`concurrencyPolicy: Forbid`.

## As a daemon

The daemon is a long-lived process that follows the schedules your rules
declare:

```bash
bergman daemon --interval 1h
```

It sleeps until the earliest trigger fires. What wakes it decides what the cycle
covers:

| Woken by | Evaluates |
|---|---|
| `--interval` | Every table the catalogs hold |
| A rule's `schedule` | Only the tables that rule's pattern matches |
| An event | Only the tables it was told about |

```toml
[[rules]]
match    = "prod.streaming.**"
schedule = "*/15 * * * *"      # wakes every 15 minutes, for these tables only

[[rules]]
match    = "prod.archive.**"
schedule = "0 3 * * *"         # nightly, for these
```

```yaml
# As a Deployment rather than a CronJob, when you want the metrics endpoint.
containers:
  - name: bergman
    args: ["daemon", "--interval", "1h", "--metrics-addr", "0.0.0.0:9090"]
    ports:
      - { name: metrics, containerPort: 9090 }
    livenessProbe:
      httpGet: { path: /health, port: metrics }
```

### Reacting to commits

The daemon can be told which table changed:

```bash
bergman daemon --listen 0.0.0.0:9090 --events --debounce 30s
```

```bash
curl -XPOST localhost:9090/events -H 'content-type: application/json' \
  -d '{"catalog":"prod","namespace":["analytics"],"table":"events"}'
```

An event cycle plans only the tables it names. Notifications within the debounce
window collapse into one cycle. Timers still fire: `--interval` is an absolute
deadline that only an interval cycle advances, so no stream of events can
postpone the periodic sweep.

Bergman carries no NATS or Kafka client; bridge from the bus you run, forwarding
what it receives.

#### Lakekeeper's CloudEvents {#cloudevents}

`/events` also accepts Lakekeeper's CloudEvents, forwarded unchanged:

- **Structured**: a JSON body carrying `specversion`, sent as
  `application/cloudevents+json`.
- **Binary**: the attributes as `ce-*` headers.

The table comes from the extension attributes `name` and `namespace` (a
multi-part namespace joined by the 0x1F unit separator), and `tabular-type` must
be `table`. `updateTable`, `createTable` and `registerTable` queue the table;
any other type, and any view event, is answered `202 ignored`.

A CloudEvent names Lakekeeper's warehouse, not a Bergman catalog, so the bridge
names the catalog in the query string:

```bash
curl -XPOST 'localhost:9090/events?catalog=prod' \
  -H 'content-type: application/cloudevents+json' -d @event.json
```

Without `?catalog=`, a CloudEvent is answered `400`. Other envelope fields are
ignored. The [token](#events-token), when configured, is checked before the body
is read.

#### Authenticate it {#events-token}

Unlike `/metrics` and `/health`, `/events` **causes work** — listing object
storage, rewriting data.

```bash
BERGMAN_EVENTS_TOKEN=$(openssl rand -hex 32) \
  bergman daemon --listen 0.0.0.0:9090 --events \
                 --events-token-env BERGMAN_EVENTS_TOKEN
```

```bash
curl -XPOST localhost:9090/events \
  -H "authorization: Bearer $BERGMAN_EVENTS_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"catalog":"prod","namespace":["analytics"],"table":"events"}'
```

The flag names a variable, never a value, and the token is compared in constant
time. An unauthorized request gets a bare `401` before the body is examined.
The daemon warns when `/events` is served without a token on an address that is
neither loopback nor behind an authenticating mesh.

### Windows

With a `maintenance_window` set, the daemon sleeps to the window's edge.
`SIGTERM` and Ctrl-C stop it after the cycle in hand; a cycle killed outright
leaves only unreferenced files, which the orphan scanner reclaims. A failed cycle
is logged with its trigger and does not stop the daemon.

> [!NOTE]
> Prefer `run` under a scheduler you already operate: a `CronJob` gives you
> retries, history, alerting and resource limits the daemon does not.

## Sizing

Metadata-only maintenance — expire, manifests, orphans — holds only manifests.

Compaction needs headroom. `compaction.max_sort_memory` (1 GiB) is the memory
pool **per file group**, groups within a table run one at a time, and
`limits.max_parallel_tables` (4) tables run at once:

```text
max_parallel_tables x max_sort_memory   +   headroom
        4           x      1 GiB        +   ~1 GiB    =  ~5 GiB
```

Below the pool size the sort and anti-join spill to disk rather than fail, so
give the container writable scratch space if you tighten it.

`limits.compaction_parallelism` does not enter that budget — the pool is shared
across a group's threads. On a CPU-limited container, pin it to the quota:

```text
# 2 CPUs requested in Kubernetes
[limits]
compaction_parallelism = 2
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Everything that ran, ran. Includes no-ops and conflicts |
| `1` | Bergman could not start: bad config, unreachable catalog |
| `2` | The run completed, but an operation **failed or was refused** |

Conflicts do not set the exit code: losing a commit to a foreground writer is
the design working.

## Credentials {#credentials}

In order of preference:

**Catalog-vended.** If your catalog vends storage credentials — Rustberg,
Polaris, Lakekeeper, S3 Tables — Bergman holds no storage secret; it receives
short-lived, prefix-scoped credentials per table.

**Provider chain.** Instance roles, IRSA, workload identity.

**OAuth2 client credentials.** `credential = "client-id:secret"` in
`[catalogs.properties]`, optionally with `oauth2-server-uri` and `scope` — the
properties every Iceberg client reads. The catalog read client and Bergman's
commit client present the token of **one** token source, which renews on demand
within a minute of expiry; concurrent requests wait for one renewal. There is no
timer — see [Long-lived processes](@/docs/library.md#long-lived).

**Environment.** `token_env` names the variable holding a static bearer token.
Bergman does not renew a token it was handed.

**Last resort.** Keys in `[catalogs.properties]`. The file is then a secret.

### Secrets never reach a log

`Credential`, the cached bearer token and `CatalogConfig` redact in `Debug`, so
`tracing::debug!(?bergman)` in an embedding service logs no secret. Property
*names* survive, and non-secret settings such as `s3.endpoint` and `s3.region`
are shown in full:

```text
CatalogConfig { name: "prod", uri: "https://polaris.example.com/api/catalog",
  token_env: Some("BERGMAN_CATALOG_TOKEN"),
  properties: {"s3.endpoint": "https://minio:9000", "s3.region": "eu-central-1",
               "s3.secret-access-key": "<redacted>"} }
```

A property is redacted when its name contains `secret`, `key`, `token`,
`credential`, `password` or `sas`.

### Least privilege

Bergman needs, on the warehouse prefix:

| Action | Needed for |
|---|---|
| `s3:ListBucket` | Orphan scanning — the listing *is* the operation |
| `s3:GetObject` | Reading metadata, and reading data files during compaction |
| `s3:PutObject` | Writing rewritten data files, manifests and manifest lists |
| `s3:DeleteObject` | **Only if** you enable orphan deletion or `snapshots.delete_files` |

`inspect` and `plan` alone need only the first two.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::lake",
      "Condition": { "StringLike": { "s3:prefix": ["warehouse/*"] } }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::lake/warehouse/*"
    }
  ]
}
```

Grant `DeleteObject` only after reading a dry run and enabling deletion
deliberately.

## The audit trail

```bash
bergman run --audit-log /var/log/bergman.jsonl
```

Append-only JSON Lines, flushed per record:

```json
{"at":"2026-08-23T02:14:07Z","run_id":"6f2c…","table":"prod.analytics.events",
 "operation":"expire-snapshots","matched_rule":"prod.analytics.*",
 "reason":"oldest snapshot is 34d old (> 7d), 61 snapshots retained",
 "result":{"result":"succeeded","detail":"58 snapshots expired, 12043 files deleted"}}
```

Every record names the rule that matched and the measurement that fired.
Deletion manifests carry the complete file list and are written **before** the
first delete.

```bash
# What was deleted last night?
jq -r 'select(.deleted_files) | .deleted_files[]' /var/log/bergman.jsonl

# What needs attention?
jq -c 'select(.result.result == "failed" or .result.result == "refused")' /var/log/bergman.jsonl

# How often are we losing to writers?
jq -r 'select(.result.result == "conflicted") | .table' /var/log/bergman.jsonl | sort | uniq -c
```

A table that conflicts every cycle is written harder than your window allows —
move the window or narrow the rule.

## Rolling it out

1. **`inspect`** for a week. `--table <glob>` scopes the examination itself, so
   narrowing to one namespace reads only that namespace's metadata.
2. **`plan`** with the policy you intend, and read the reasons and the **notes**:
   a `note:` line is work your policy asked for that the table cannot receive —
   an [encrypted table](@/docs/compaction.md#what-is-refused), a partition under
   a superseded spec.
3. **`run`** with metadata-only expiration (the default). Nothing is deleted.
4. **Orphans in `dry-run`.** Read the counts. A table with 480 live files
   reporting 6,000 orphans has a configuration problem.
5. **`mode = "delete"`**, on one namespace, with `--audit-log`, and read the
   manifest afterwards.

Do not skip step 4.

## Logs

The library emits `tracing` spans and installs no subscriber; the binary sets one
up from `--log`/`BERGMAN_LOG`, writing to stderr so `--format json` on stdout
stays machine-readable.

```bash
BERGMAN_LOG=info bergman run
BERGMAN_LOG=bergman::ops=debug bergman run   # per-module filtering
```

## Metrics

Built with the `metrics` feature and served by the daemon:

```bash
bergman daemon --metrics-addr 0.0.0.0:9090
```

| Metric | Labels | What it is |
|---|---|---|
| `bergman_operations` | catalog, namespace, operation, outcome | Operations, counted by how they ended |
| `bergman_operation_duration_seconds` | catalog, namespace, operation, outcome | How long each took |
| `bergman_files_deletion_announced` | catalog, namespace, operation | Files a deletion was announced for, counted *before* the first delete |

`outcome` is `succeeded`, `no-op`, `refused`, `conflicted` or `failed`. Only
`failed` and `refused` need attention; `conflicted` is maintenance yielding to a
writer.

```promql
# Something needs looking at.
sum by (namespace, operation) (rate(bergman_operations{outcome=~"failed|refused"}[1h])) > 0

# Losing to writers repeatedly — the window is probably wrong.
sum by (namespace) (rate(bergman_operations{outcome="conflicted"}[6h])) > 0.5
```

> [!IMPORTANT]
> **There is no `table` label**, and no rule label: a series per table would
> overwhelm Prometheus on a large warehouse. Group by `namespace`. Namespaces are
> capped too — the first 128 a process sees keep their own series, the rest share
> `namespace="<over-cardinality-cap>"`, reported once in the log. Per-table facts
> are in the audit trail:
>
> ```bash
> jq -r 'select(.result.result == "failed") | "\(.table)\t\(.result.error)"' \
>   /var/log/bergman.jsonl
> ```

Metrics are recorded whether or not an endpoint serves them, so adding
`--metrics-addr` later gives history from that moment.

`/health` reports **liveness, not readiness**: it does not check the catalog, so
a brief catalog outage does not get a working process restarted.

For anything else, implement a [`MaintenanceObserver`](@/docs/library.md#observers).
