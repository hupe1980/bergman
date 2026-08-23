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
                requests: { cpu: 200m, memory: 128Mi }
                limits:   { memory: 512Mi }
          volumes:
            - name: config
              configMap: { name: bergman-config }
```

`concurrencyPolicy: Forbid` matters. Two overlapping cycles are safe — commits
are compare-and-swap and the loser replans — but they waste work competing with
each other.

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

**Environment.** `token_env` names the variable holding the catalog token; the
value never appears in `bergman.toml`.

**Last resort.** Keys in `[catalogs.properties]`. Works, but the file is now a
secret.

### Least privilege

Bergman needs `s3:ListBucket` and `s3:GetObject` on the warehouse prefix, plus
`s3:DeleteObject` **only if** you enable orphan deletion or
`snapshots.delete_files`:

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
      "Action": ["s3:GetObject", "s3:DeleteObject"],
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
   actually look like.
2. **`plan`** with the policy you intend. Read the reasons; adjust thresholds
   until the plans are ones you would sign off.
3. **`run`** with metadata-only expiration (the default). Nothing is deleted.
4. **Orphans in `dry-run`.** Read the counts. A table with 480 live files
   reporting 6,000 orphans means something is wrong with the configuration, not
   with the table.
5. **`mode = "delete"`**, on one namespace, with `--audit-log`, and read the
   manifest afterwards.

Step 4 is the one people skip. Do not skip it.

## Observability

The library emits `tracing` spans and installs no subscriber. The binary sets one
up from `--log`/`BERGMAN_LOG`, writing to stderr.

```bash
BERGMAN_LOG=info bergman run
BERGMAN_LOG=bergman::ops=debug bergman run   # per-module filtering
```

For metrics today, parse `--format json` output or implement a
[`MaintenanceObserver`](@/docs/library.md#observers). A Prometheus endpoint
arrives with daemon mode.
