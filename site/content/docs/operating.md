+++
title = "Operating"
description = "Running Bergman as a cron job, wiring credentials, reading the audit trail, and what the exit codes mean."
weight = 10
+++
Bergman runs one cycle and exits. That makes it a natural fit for a scheduler you
already have rather than a daemon you have to babysit.

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

`concurrencyPolicy: Forbid` matters. Two overlapping cycles are safe — commits
are compare-and-swap and the loser replans — but they waste work competing with
each other.

## As a daemon

One cycle and exit is the right shape for most deployments. The daemon is for a
long-lived process that follows the schedules your rules declare:

```bash
bergman daemon --interval 1h
```

It sleeps until the **earliest** trigger fires — the interval, or the soonest
rule `schedule`. Waking more often than the busiest rule asks for would be
waste; waking less often would silently stretch that rule's cadence.

What wakes it decides what the cycle covers:

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

Scoping a schedule to its own rule is what keeps one aggressive cadence from
setting the pace for the whole catalog: without it, `*/15` above would mean the
archive tables were evaluated every fifteen minutes too.

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

A cron cadence is a guess: too slow and a streaming table stays fragmented for an
hour, too fast and a quiet catalog is rescanned for nothing. The daemon can be
told what changed instead:

```bash
bergman daemon --listen 0.0.0.0:9090 --events --debounce 30s
```

```bash
curl -XPOST localhost:9090/events -H 'content-type: application/json' \
  -d '{"catalog":"prod","namespace":["analytics"],"table":"events"}'
```

An event-driven cycle plans **only the tables it was told about**, so reacting to
one commit does not rescan a catalog of thousands. Notifications within the
debounce window collapse into one cycle — a writer committing every two seconds
produces one cycle, not thirty.

Timers still fire. Events are an addition to the cadence, not a replacement: a
notification can always be lost, and a table that *stops* being written still
needs its snapshots expired.

That holds however busy the event stream gets: `--interval` is an absolute
deadline that only an interval cycle advances, so a constant stream of
notifications cannot push the periodic sweep out of the way.

> [!NOTE]
> Bergman carries no NATS or Kafka client on purpose. Lakekeeper emits
> `CloudEvents` to a broker, and dragging one into a maintenance engine would
> import exactly the operational footprint the project exists to avoid — and
> would put it in the default build, which almost nobody would use. Bridge from
> whatever bus you already run; the endpoint needs four lines of anything.

The endpoint accepts anything carrying a catalog, namespace and table, and
ignores the rest of a `CloudEvents` envelope — validating fields Bergman has no
use for would reject perfectly good notifications for reasons that do not matter
to it.

#### Authenticate it {#events-token}

Unlike `/metrics` and `/health`, this endpoint **causes work**: a notification
makes the daemon plan and maintain a table, which lists object storage and can
rewrite data. An open one on a routable address lets anyone who can reach the
port spend the warehouse's money.

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

The token is a variable *name*, never a value, and it is compared in constant
time — a naive `==` leaks a secret's length and, byte by byte, its contents to
anyone who can measure response latency, and an endpoint on the network is
exactly where that is measurable.

An unauthorized request gets a bare `401`, and nothing about it is examined
first — not even whether the body parses. Answering `400` for a malformed body
and `401` for a good one would tell an unauthenticated caller when they had
found the right shape. Once authenticated, a bad body *is* reported.

Leaving it open is right on a loopback bind, or behind a service mesh that
authenticates for you. It is the wrong answer everywhere else, so the daemon
warns when `/events` is served without a token on an address that is neither.

### Windows

With a `maintenance_window` set, the daemon sleeps to the window's edge rather
than waking every interval to find it shut — a daemon that logged "outside the
window" sixty times a night is a daemon whose logs nobody reads.

`SIGTERM` and Ctrl-C stop it after the cycle in hand. A cycle killed outright is
safe too — it leaves only files nothing references, which the orphan scanner
reclaims — but finishing is tidier, and it is what a container runtime's grace
period is for.

A failed cycle does not stop the daemon: the catalog may simply have been
unreachable, and the next cycle is the retry. Failures are logged with the
trigger that fired.

> [!NOTE]
> Prefer `run` under a scheduler you already operate. A `CronJob` gives you
> retries, history, alerting and resource limits that the daemon does not
> reimplement.

## Sizing

Metadata-only maintenance — expire, manifests, orphans — needs very little:
a few hundred MiB covers a large catalog, because nothing but manifests is ever
held.

Compaction is what needs headroom. `compaction.max_sort_memory` (1 GiB by
default) is the executor's memory pool **per file group**, groups within a table
run one at a time, and `limits.max_parallel_tables` (4) tables run at once — so
budget roughly:

```text
max_parallel_tables x max_sort_memory   +   headroom
        4           x      1 GiB        +   ~1 GiB    =  ~5 GiB
```

Both knobs are worth lowering on a small node. Going below the pool size does
not fail — the sort and the anti-join spill to disk — it just gets slower, so
give the container writable scratch space if you tighten it.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Everything that ran, ran. Includes no-ops and conflicts |
| `1` | Bergman could not start: bad config, unreachable catalog |
| `2` | The run completed, but an operation **failed or was refused** |

Code `2` is the important one. A failure inside a run is not a failure *of* the
run — other tables were maintained — but it has to reach a scheduler that only
reads exit codes, or a broken job looks healthy forever.

Conflicts do **not** set the exit code. Losing a commit to a foreground writer is
the design working.

## Credentials {#credentials}

In order of preference:

**Catalog-vended.** If your catalog vends storage credentials — Rustberg,
Polaris, Lakekeeper, S3 Tables — Bergman holds no storage secret at all. It
authenticates to the catalog and receives short-lived, prefix-scoped credentials
per table.

**Provider chain.** Instance roles, IRSA, workload identity. Nothing in the
config file, nothing in the environment.

**OAuth2 client credentials.** Set `credential = "client-id:secret"` in
`[catalogs.properties]`, optionally with `oauth2-server-uri` and `scope` — the
same properties every Iceberg client reads, so one configuration authenticates
both Bergman's read path and its commit path.

Tokens are renewed on both paths, differently, because they are different
clients. Bergman's commit client refreshes on a margin before expiry; the
catalog client is upstream's and refreshes nothing, so `bergman daemon` renews it
before each cycle. Without that, a daemon holding a one-hour token keeps its
cadence perfectly and stops reading a single table. Embedders driving their own
loop call `refresh_credentials()` — see
[Long-lived processes](@/docs/library.md#long-lived).

**Environment.** `token_env` names the variable holding a static bearer token;
the value never appears in `bergman.toml`. Whatever produced that token owns its
lifetime — Bergman does not renew a token it was handed.

**Last resort.** Keys in `[catalogs.properties]`. Works, but the file is now a
secret.

### Secrets never reach a log

`Credential`, the cached bearer token and `CatalogConfig` implement `Debug` by
hand and **redact**. That is not belt-and-braces: every one of those types is
reachable by `Debug` from `Bergman` itself, so a derived impl would mean a
single `tracing::debug!(?bergman)` in an embedding service wrote the client
secret and the warehouse's storage keys into that service's logs.

Property *names* survive redaction — knowing a secret was supplied is the other
half of debugging one that was not — and non-secret settings such as
`s3.endpoint` and `s3.region` are shown in full, because "is my endpoint
reaching the config" is usually the question being asked:

```text
CatalogConfig { name: "prod", uri: "https://polaris.example.com/api/catalog",
  token_env: Some("BERGMAN_CATALOG_TOKEN"),
  properties: {"s3.endpoint": "https://minio:9000", "s3.region": "eu-central-1",
               "s3.secret-access-key": "<redacted>"} }
```

The redaction rule is a substring match over the words that mark a secret in
Iceberg's property vocabulary — `secret`, `key`, `token`, `credential`,
`password`, `sas` — so it over-redacts rather than under-redacts. A redacted
identifier costs you one lookup; a logged secret costs you a rotation.

### Least privilege

Bergman needs, on the warehouse prefix:

| Action | Needed for |
|---|---|
| `s3:ListBucket` | Orphan scanning — the listing *is* the operation |
| `s3:GetObject` | Reading metadata, and reading data files during compaction |
| `s3:PutObject` | Writing rewritten data files, manifests and manifest lists |
| `s3:DeleteObject` | **Only if** you enable orphan deletion or `snapshots.delete_files` |

A read-only deployment — `inspect` and `plan`, nothing else — needs only the
first two.

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

Grant `DeleteObject` only when you have read a dry run and enabled deletion
deliberately. "Give it `s3:*`" is how orphan removal becomes an incident.

## The audit trail

```bash
bergman run --audit-log /var/log/bergman.jsonl
```

Append-only JSON Lines, flushed per record — an audit trail buffered in a process
that then dies describes a world that never existed.

```json
{"at":"2026-08-23T02:14:07Z","run_id":"6f2c…","table":"prod.analytics.events",
 "operation":"expire-snapshots","matched_rule":"prod.analytics.*",
 "reason":"oldest snapshot is 34d old (> 7d), 61 snapshots retained",
 "result":{"result":"succeeded","detail":"58 snapshots expired, 12043 files deleted"}}
```

Every record names the rule that triggered it and the measurement that fired,
so a line in the trail answers "why did this happen to my table?" on its own.

Deletion manifests carry the complete file list and are written **before** the
first delete, so a crash halfway through still leaves evidence.

Useful queries:

```bash
# What was deleted last night?
jq -r 'select(.deleted_files) | .deleted_files[]' /var/log/bergman.jsonl

# What needs attention?
jq -c 'select(.result.result == "failed" or .result.result == "refused")' /var/log/bergman.jsonl

# How often are we losing to writers?
jq -r 'select(.result.result == "conflicted") | .table' /var/log/bergman.jsonl | sort | uniq -c
```

That last one is worth watching. A table that conflicts every cycle is being
written harder than your maintenance window allows for — move the window or
narrow the rule.

## Rolling it out

1. **`inspect`** for a week. No writes, no risk, and you learn what your tables
   actually look like. `--table <glob>` scopes the *examination*, not just the
   output, so narrowing to one namespace costs one namespace's metadata reads.
2. **`plan`** with the policy you intend. Read the reasons; adjust thresholds
   until the plans are ones you would sign off. Read the **notes** too: a
   `note:` line is work your policy asked for that the table cannot receive —
   a [format v3 table](@/docs/compaction.md#format-v3), a partition under a
   superseded spec — and without it that table would read as simply healthy.
3. **`run`** with metadata-only expiration (the default). Nothing is deleted.
4. **Orphans in `dry-run`.** Read the counts. A table with 480 live files
   reporting 6,000 orphans means something is wrong with the configuration, not
   with the table.
5. **`mode = "delete"`**, on one namespace, with `--audit-log`, and read the
   manifest afterwards.

Step 4 is the one people skip. Do not skip it.

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

`outcome` is `succeeded`, `no-op`, `refused`, `conflicted` or
`failed`. The distinctions matter for alerting: a `no-op` is a healthy table, a
`conflicted` is maintenance yielding to a writer as designed, and only `failed`
and `refused` need anyone's attention.

```promql
# Something needs looking at.
sum by (namespace, operation) (rate(bergman_operations{outcome=~"failed|refused"}[1h])) > 0

# Losing to writers repeatedly — the window is probably wrong.
sum by (namespace) (rate(bergman_operations{outcome="conflicted"}[6h])) > 0.5
```

Group by `namespace`: there is no `table` label.

> [!IMPORTANT]
> **The table name is deliberately not a label**, and neither is the policy
> rule. A time series is created per label combination and kept forever, so a
> warehouse with fifty thousand tables would produce fifty thousand series per
> metric — multiplied, for the histogram, by its bucket count. That is not a
> slow dashboard; it is an outage of the monitoring system, caused by the tool
> that was supposed to be watching it.
>
> The namespace is the finest grain that stays bounded, and it is enough to tell
> you where to look. It is kept bounded by a cap rather than by assuming a
> warehouse has few namespaces — one per tenant or per day is common. The first
> 128 a process sees keep their own series and the rest share
> `namespace="<over-cardinality-cap>"`, reported once in the log.
>
> The per-table facts are in the audit trail, which is append-only and has no
> cardinality budget:
>
> ```bash
> jq -r 'select(.result.result == "failed") | "\(.table)\t\(.result.error)"' \
>   /var/log/bergman.jsonl
> ```
>
> Metrics answer "is maintenance working". The audit trail answers "what
> happened to this table". Asking either to do the other's job makes both
> worse.

Metrics are recorded whether or not an endpoint serves them, so adding
`--metrics-addr` later gives you history from that moment rather than from the
next restart.

`/health` reports **liveness, not readiness** — that the process is up. Whether
the catalog is reachable is not answered there: a probe failing during a brief
catalog outage would have Kubernetes restart a process that is working perfectly,
and restarting would not help.

For anything else, implement a [`MaintenanceObserver`](@/docs/library.md#observers).
