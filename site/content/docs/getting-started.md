+++
title = "Getting started"
description = "Install Bergman, point it at a catalog, and read what is wrong with your tables — before it writes anything."
weight = 1
+++
The first two commands read only.

## Install

```bash
brew install hupe1980/tap/bergman   # macOS (Apple Silicon) and Linux
cargo install bergman               # anywhere Rust builds
```

Or as a container — distroless, non-root, statically linked, no shell:

```bash
docker run --rm \
  -v ./bergman.toml:/etc/bergman/bergman.toml:ro \
  ghcr.io/hupe1980/bergman:latest inspect
```

Or a binary from the [releases page](https://github.com/hupe1980/bergman/releases):
static musl Linux for `x86_64` and `aarch64`, and macOS on Apple Silicon, each with
a `.sha256` beside it.

Or from source — Rust 1.95 or later:

```bash
git clone https://github.com/hupe1980/bergman && cd bergman
cargo build --release
```

## Point it at a catalog

The smallest useful `bergman.toml`:

```toml
[[catalogs]]
name      = "prod"
uri       = "http://localhost:8181/catalog"
warehouse = "s3://lake/warehouse"

[catalogs.properties]
"s3.region" = "eu-central-1"

[[rules]]
match = "prod.**"
```

Check it offline — no catalog, no credentials, so it runs in CI:

```bash
bergman policy lint
```

```
ok: 1 catalog, 1 rule
```

## Look before you write

```bash
bergman inspect
```

```
 TABLE                        FILES   SIZE       FILE SIZE p50/p95    DELETES     SNAPSHOTS   MANIFESTS
 prod.analytics.events        480     2.14 GiB   3.10 MiB / 9.80 MiB  —           61 / 34d    48 (41 small)
 prod.streaming.events_raw    92      8.11 GiB   88.4 MiB / 96.1 MiB  1204 (23%)  8 / 2h      6
 prod.finance.ledger          12      6.02 GiB   402 MiB / 2.90 GiB   —           4 / 6d      2
```

Every number comes from manifest metadata; no data file is opened.

- **`p50` far below target** — small files. Half of `events`' files are under
  3.10 MiB against the 512 MiB default target.
- **`p95` far above target** — oversized files a mean would hide. `ledger`'s
  largest are 2.90 GiB, so one query task does all their work.
- **`DELETES` with a percentage** — read amplification: 23% of `events_raw`'s
  rows are named by delete files, and every query pays to apply them.
- **`SNAPSHOTS` with an age** — metadata growth: 61 manifest lists reaching back
  34 days.

## Ask what maintenance would do

```bash
bergman plan
```

```text
prod.analytics.events
  rule: prod.**
  -> compact
     why: partition d=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
     reads 412 files (1.83 GiB), writes ~5 files
  -> expire-snapshots
     why: oldest snapshot is 34d old (> 7d), 61 snapshots retained (keeping at least 3)
     removes up to 58 snapshots

1 tables, 2 operations, 1.83 GiB to read
```

Each operation names the measurement that triggered it and the threshold it
crossed; [`policy explain`](@/docs/policies.md#provenance) shows where each
threshold came from. Only the files a rewrite would
[improve](@/docs/compaction.md#eligible) are read — 412, not 480.

## Run it

```bash
bergman run --audit-log /var/log/bergman.jsonl
```

```text
prod.analytics.events
  [ok] compact: 412 files (1.83 GiB) rewritten into 5 (1.79 GiB) across 1 groups
       in 1 partitions
  [ok] expire-snapshots: 58 snapshots expired

1 tables, 2 operations succeeded in 47s
```

`run` builds the plan through the same code as `plan`, then executes it. Both
take `--table <glob>` to scope which tables are examined and `--only` to scope
which operations run:

```bash
# Reclaim storage tonight; rewrite nothing.
bergman run --only remove-orphans,expire-snapshots
```

Any subset is safe: per-table order is preserved.

> [!NOTE]
> `bergman run` exits `2` when any operation failed or was refused.

## What is on by default

| Operation | Default | Opt in |
|---|---|---|
| Snapshot expiration (metadata) | **on** | — writes no data files |
| Expiration file cleanup | off | `snapshots.delete_files` |
| Orphan removal | off | `orphans.enabled`, then `mode = "delete"` |
| Compaction | off | `compaction.enabled` |
| Manifest rewrite | off | `manifests.rewrite` |

## Next

- [Policies](@/docs/policies.md) — how a setting is resolved, and why
- [Orphan files](@/docs/orphans.md) — read before enabling deletion
- [Status](@/docs/status.md) — what executes, and what is refused
