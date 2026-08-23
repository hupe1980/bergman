+++
title = "Getting started"
description = "Install Bergman, point it at a catalog, and read what is wrong with your tables — before it writes anything."
weight = 1
+++
The first two commands read only. That is deliberate: a maintenance tool earns
write access by first showing you it understands your tables.

## Install

```bash
cargo install bergman
```

Or as a container — distroless, non-root, statically linked, no shell:

```bash
docker run --rm \
  -v ./bergman.toml:/etc/bergman/bergman.toml:ro \
  ghcr.io/hupe1980/bergman:latest inspect
```

Or from source — Rust 1.94 or later:

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

Check it without contacting anything — this works in CI on a machine holding no
credentials:

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
 TABLE                        FILES   SIZE       AVG FILE   DELETES     SNAPSHOTS   MANIFESTS
 prod.analytics.events        480     2.14 GiB   4.56 MiB   —           61 / 34d    48 (41 small)
 prod.streaming.events_raw    92      8.11 GiB   90.2 MiB   1204 (23%)  8 / 2h      6
 prod.finance.ledger          12      6.02 GiB   513 MiB    —           4 / 6d      2
```

Every number here comes from manifest metadata Iceberg already maintains — no
data file is opened. That is what makes this cheap enough to run against
thousands of tables.

Three things to read in that output:

- **`AVG FILE` far below your target** is the small-file problem. `events` is
  averaging 4.56 MiB against a 512 MiB default target.
- **`DELETES` with a percentage** is read amplification. 23% of `events_raw`'s
  rows are named by delete files, and every query pays to apply them — even
  though its file sizes are fine.
- **`SNAPSHOTS` with an age** is metadata growth. 61 snapshots reaching back 34
  days is 61 manifest lists every planning phase may touch.

## Ask what maintenance would do

```bash
bergman plan
```

```text
prod.analytics.events
  rule: prod.**
  -> compact
     why: partition d=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
     reads 480 files (2.14 GiB), writes ~5 files
  -> expire-snapshots
     why: oldest snapshot is 34d old (> 7d), 61 snapshots retained (keeping at least 3)
     removes up to 58 snapshots

1 tables, 2 operations (2 will run, 0 blocked), 2.14 GiB to read
```

Every operation carries the measurement that triggered it and the threshold it
crossed. If a plan surprises you, [`policy explain`](@/docs/policies.md#provenance)
shows where each threshold came from.

## Run it

```bash
bergman run --audit-log /var/log/bergman.jsonl
```

```text
prod.analytics.events
  [ok] compact: 1 partitions: 480 files (2.14 GiB) rewritten into 5 (2.09 GiB)
  [ok] expire-snapshots: 58 snapshots expired

1 tables, 2 operations succeeded in 47s
```

`run` builds the plan through exactly the same code `plan` does, then executes
it. What you were shown is what happens.

> [!NOTE]
> `bergman run` exits `2` when any operation failed or was refused, so a broken
> cron job does not look healthy to a scheduler that only reads exit codes.

## What is on by default

Almost nothing destructive:

| Operation | Default | Why |
|---|---|---|
| Snapshot expiration (metadata) | **on** | Unbounded snapshot growth is the most common Iceberg health problem, and this writes no data files |
| Expiration file cleanup | off | Deletes files; opt in with `snapshots.delete_files` |
| Orphan removal | off | Deletes files; opt in with `orphans.enabled`, and again with `mode = "delete"` |
| Compaction | off | Rewrites data; never arrives from a rule that merely matched |
| Manifest rewrite | off | Same |

## Next

- [Policies](@/docs/policies.md) — how a setting is resolved, and how to find out why
- [Orphan files](@/docs/orphans.md) — read this before enabling deletion
- [Status](@/docs/status.md) — what executes, and what is not built
