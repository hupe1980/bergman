# Bergman

**A Rust-native maintenance engine for Apache Iceberg. One crate. One binary. No JVM.**

Bergman keeps Iceberg tables healthy — expiring snapshots, reclaiming the files
that leaves behind, and removing orphans — driven by declarative policies and
built on [`iceberg-rust`](https://github.com/apache/iceberg-rust). No Spark, no
Trino, no cluster.

It is **library-first**: the `bergman` binary is a thin consumer of the same
public API an embedder gets, so a catalog, a streaming writer, or a lakehouse
control plane can hold the whole engine in-process.

```bash
bergman inspect        # what is wrong with my tables?   (reads only)
bergman plan           # what would maintenance do?      (reads only)
bergman run            # do it
```

- **Concept and design:** [CONCEPT.md](CONCEPT.md)
- **Sister project:** [Rustberg](https://github.com/hupe1980/rustberg), an
  authenticated, policy-controlled Iceberg REST catalog. The two are designed
  to pair (§10 of the concept) and each works fully without the other.

---

## Status

Bergman is pre-release, and the table below is the honest state rather than the
intended one. The reason for the gaps is upstream and specific: **no
`iceberg-rust` API removes a data file**, and the commit API is crate-private
(`TransactionAction` and `TableCommit`'s builder are both `pub(crate)`), so no
external crate can construct such a commit.

| Operation | Planned | Executed | Why |
|---|:--:|:--:|---|
| Table health analysis | ✅ | ✅ | Metadata-only; no data file is opened |
| Snapshot expiration | ✅ | ✅ | Upstream `ExpireSnapshotsAction` |
| Expiration file cleanup | ✅ | ✅ | Upstream delegates physical cleanup upward — this is that |
| Orphan-file removal | ✅ | ✅ | On Bergman's own object-store layer; `FileIO` has no `list` |
| Compaction | ✅ | ❌ | Blocked: [apache/iceberg-rust#2185](https://github.com/apache/iceberg-rust/pull/2185), [#2752](https://github.com/apache/iceberg-rust/pull/2752) |
| Manifest rewrite | ✅ | ❌ | Blocked: no upstream action ([#1237](https://github.com/apache/iceberg-rust/pull/1237) closed unmerged) |

Blocked operations are still **planned and reported**. `bergman plan` states
the table's real need and marks the operation `BLOCKED` with the reason:

```
prod.analytics.events
  rule: prod.analytics.*
  !! compact
     why: partition d=2026-08-20: 412 of 480 files below 384 MiB (86% ≥ 30%)
     reads 480 files (2.14 GiB), writes ~5 files
     BLOCKED: compaction needs a commit that removes data files; iceberg-rust
     0.10 has no such transaction action and its commit API is crate-private
     (apache/iceberg-rust#2186). Planned and reported only.
  -> expire-snapshots
     why: oldest snapshot is 34d old (> 7d), 61 snapshots retained (keeping at least 3)
     removes up to 58 snapshots
```

A tool that silently did nothing about a need it had just reported would be
worse than one that never looked.

---

## Install

```bash
cargo install bergman
```

Or build from source (requires Rust 1.94+):

```bash
git clone https://github.com/hupe1980/bergman && cd bergman
cargo build --release
```

---

## Configure

`bergman.toml`. Every setting is optional — see [Layering](#layering).

```toml
[[catalogs]]
name      = "prod"
uri       = "http://localhost:8181/catalog"
warehouse = "s3://lake/warehouse"
token_env = "BERGMAN_CATALOG_TOKEN"   # a name, never a value

[catalogs.properties]                  # Iceberg's own property names
"s3.region"   = "eu-central-1"
"s3.endpoint" = "https://s3.eu-central-1.amazonaws.com"

[defaults.snapshots]
max_age     = "7d"
min_to_keep = 3

[[rules]]
match = "prod.analytics.*"

[[rules]]
match = "prod.tmp.*"
skip  = true
```

Validate it offline — no catalog, no credentials, suitable for CI:

```bash
bergman policy lint
```

### Layering

Every setting resolves through four layers, most specific first:

1. the matching **rule**
2. the config's **`[defaults]`**
3. the **table's own Iceberg properties** (`write.target-file-size-bytes`,
   `history.expire.max-snapshot-age-ms`, …)
4. the **Iceberg specification default**

Layer 3 is what makes Bergman a participant rather than a competing source of
truth: a table already carries its owner's intent in its properties, and every
other Iceberg tool reads them.

`bergman policy explain` shows the result *and the origin of every value*,
because the question is never "what is the target file size" — it is "why is it
*that*":

```
$ bergman policy explain prod.analytics.events

prod.analytics.events
  matched rule: prod.analytics.*

 SETTING                        VALUE      FROM
 compaction.target_file_size    128 MiB    table property write.target-file-size-bytes
 snapshots.max_age              7d         [defaults]
 snapshots.min_to_keep          3          [defaults]
 snapshots.delete_files         false      Bergman default
 orphans.mode                   DryRun     Bergman default
 orphans.older_than             7d         Bergman default
```

### Rule patterns

Globs over `catalog.namespace…​.table`, with `.` as the separator. `*` stops at
a namespace boundary and `**` crosses it — the same distinction `/` has in a
filesystem glob:

| Pattern | Matches `prod.analytics.events` | Matches `prod.analytics.web.events` |
|---|:--:|:--:|
| `prod.analytics.*` | ✅ | ❌ |
| `prod.analytics.**` | ✅ | ✅ |
| `prod.**` | ✅ | ✅ |

Rules are evaluated in order; the first match wins. A table no rule matches is
reported as `unmatched` — distinct from one a rule deliberately `skip`s, so you
can tell "my pattern is wrong" from "I excluded this".

---

## Safety

Maintenance is a background tenant: it must never win against a foreground
writer, and never corrupt one.

**Nothing destructive is on by default.** Compaction, orphan deletion, and
expiration's file cleanup all default off. Metadata-only snapshot expiration is
the single exception, because unbounded snapshot growth is the most common
Iceberg health problem.

**Orphan removal has five independent checks**, because it is the one operation
that can destroy a healthy table:

1. **Dry run by default** — deleting needs an explicit `mode = "delete"`.
2. **A grace period with a hard 24-hour floor.** Writers stage files *before*
   the commit that references them, so a young unreferenced file is more likely
   a live write than garbage. The floor is refused at parse time *and* rechecked
   in the scanner, because the library API is a second entry point.
3. **Unknown age means young.** A store that will not say how old a file is
   cannot be used to argue it is old enough to delete.
4. **Segment-wise containment.** `…/events` maintenance can never reach
   `…/events_archive`, which a raw string-prefix check would allow.
5. **Re-verification before deleting.** Metadata is reloaded after listing, and
   anything that became reachable in between is spared.

Path comparison is normalized throughout (`s3://` vs `s3a://`, doubled slashes,
trailing slashes), because a live file spelled differently from its metadata
would otherwise look exactly like garbage.

**Commits are compare-and-swap with replan-on-conflict.** Losing to a foreground
writer is reported as `conflicted`, not failed — it is the design working. Each
retry *reloads the table*, because re-submitting a commit computed against a
table that has since moved is how deleted rows come back.

**Every operation is auditable.** `--audit-log` appends a JSON Lines record per
operation, and a deletion manifest is written *before* the first delete — so a
crash halfway through still leaves evidence of what was about to go.

---

## Embedding

Bergman is a library first. The CLI drives exactly this API.

```rust
use bergman::{Bergman, policy::Config};

let config = Config::from_path("bergman.toml")?;
let bergman = Bergman::new(config).await?;

// Read-only.
let plan = bergman.plan().await?;
println!("{} operations", plan.operation_count());

// The first call that writes anything.
let report = bergman.run(&plan).await?;
```

```toml
[dependencies]
bergman = { version = "0.1", default-features = false, features = ["catalog-rest", "storage-s3"] }
```

The contract:

- **No global state.** No logger, no tracing subscriber, no signal handler, no
  config file read on its own initiative. The library emits `tracing` events;
  what listens is yours.
- **Bring your own runtime.** Plain `async fn` on the caller's runtime.
  Concurrency limits are parameters, never process-wide statics.
- **Planning is pure.** `plan()` writes nothing and deletes nothing, so dry-run
  is not a mode to remember — it is what happens when you stop before `run()`.
- **Observation is a hook.** Implement `MaintenanceObserver` for metrics, an
  event bus, or an approval gate — `operation_starting` returning `false` vetoes
  the operation, which is then reported as refused.

```rust
#[async_trait::async_trait]
impl MaintenanceObserver for RequireSignoff {
    async fn operation_starting(&self, table: &TableRef, kind: OperationKind) -> bool {
        kind != OperationKind::RemoveOrphans || self.approved(table).await
    }
}
```

---

## Commands

| Command | Writes? | What it does |
|---|:--:|---|
| `bergman inspect` | no | Table health: file counts, sizes, delete ratios, snapshots, manifests |
| `bergman plan` | no | The operations maintenance would perform, and why |
| `bergman run` | **yes** | Executes the plan |
| `bergman run --dry-run` | no | Identical to `plan` |
| `bergman policy lint` | no | Validates config offline — for CI |
| `bergman policy explain <table>` | no | Effective policy with per-value provenance |
| `bergman policy match` | no | Which tables each rule matches |

Global flags: `--config`, `--format text|json`, `--audit-log <path>`, `--log <level>`.

`bergman run` exits `2` when any operation failed or was refused, so a broken
cron job does not look healthy.

---

## Features

| Feature | Default | Carries |
|---|:--:|---|
| `cli` | ✅ | The binary, terminal rendering, logging setup |
| `catalog-rest` | ✅ | Iceberg REST catalog client |
| `storage-s3` / `-gcs` / `-azure` | ✅ via `storage-all` | Cloud object stores |

Local filesystem and in-memory storage are always available, which is what keeps
the test suite free of containers. Embedders take `default-features = false`.

---

## Development

```bash
just test          # the full suite
just lint          # fmt + clippy -D warnings
just check-all     # every feature alone, plus all and none
```

Every optional feature is built **alone** in CI, not only under `--all-features`
— a feature that compiles in that combination can be broken by itself, because
another crate's feature unification was switching on what it needed.

## License

Apache-2.0 OR MIT.
