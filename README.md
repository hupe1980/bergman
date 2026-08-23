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

- **Documentation:** <https://hupe1980.github.io/bergman/>
- **Sister project:** [Rustberg](https://github.com/hupe1980/rustberg), an
  authenticated, policy-controlled Iceberg REST catalog. The two are designed
  to pair (§10 of the concept) and each works fully without the other.

---

## Status

Bergman is pre-release. All four maintenance operations execute.

| Operation | Executes | Built on |
|---|:--:|---|
| Table health analysis | ✅ | Manifest metadata only; no data file is opened |
| Snapshot expiration | ✅ | Upstream `ExpireSnapshotsAction` |
| Expiration file cleanup | ✅ | Bergman — upstream documents this as a higher-level responsibility |
| Orphan-file removal | ✅ | Bergman's object-store layer — `FileIO` has no `list` |
| Manifest rewrite | ✅ | Bergman's commit layer — no upstream action exists |
| Compaction | ✅ | Upstream's scan + writers, Bergman's commit layer |

### Why bergman owns its commit layer

`iceberg::Transaction` has no action that removes a data file, and both
`TransactionAction` and `TableCommit`'s builder are `pub(crate)` — so
compaction and manifest rewriting cannot be expressed through it at all.

The common answer is to fork: [`nimtable/iceberg-compaction`](https://github.com/nimtable/iceberg-compaction)
pins `risingwavelabs/iceberg-rust` at a git revision. That costs a rebase
forever and a crate that cannot be published, since Cargo rejects git
dependencies on crates.io.

Bergman owns the one blocked layer instead. Every piece of a commit is already
public — `ManifestWriterBuilder`, `ManifestListWriter`, `Snapshot::builder`,
`TableUpdate`/`TableRequirement` — so bergman builds the commit with upstream's
own writers and `POST`s it to the table endpoint itself. The bytes are the same
ones `iceberg-catalog-rest` sends.

Operations commit through a `TableCommitter` trait, not a transport, so when
[#2185](https://github.com/apache/iceberg-rust/pull/2185) lands a second
implementation wraps it and nothing above `src/commit` changes.
[More →](https://hupe1980.github.io/bergman/docs/status/)

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

**A rewrite never drops a delete file another file still needs.** Compaction
retires a delete file only when *every* data file it applies to is inside the
group being rewritten. One shared with a file outside the group is still hiding
rows there, and dropping it would bring them back — so the whole table is
planned before anything is rewritten, because that question cannot be answered
from inside the group.

**Rewritten files inherit their sequence numbers.** A data file carried through
a rewrite keeps the sequence number of the file it replaces, so delete files
written *later* still apply to it. Stamping the new snapshot's number on it
would make it look newer than the deletes that should remove its rows.

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
    async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
        ctx.kind != OperationKind::RemoveOrphans || self.approved(ctx.table).await
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
| `bergman daemon` | **yes** | Runs cycles on the schedules rules declare |
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
