# 🏔️ Bergman

**A Rust-native maintenance engine for Apache Iceberg. One crate. One binary. No JVM.**

Bergman keeps Iceberg tables healthy: it compacts small files, applies and
retires delete files, expires snapshots, re-packs manifests and removes orphans.
Declarative policies drive it, and it is built on
[`iceberg-rust`](https://github.com/apache/iceberg-rust) 0.11.0 — no Spark, no
Trino, no cluster. It is **library-first**: the `bergman` binary is a thin
consumer of the same public API an embedder gets.

```bash
bergman inspect   # 🔍 what is wrong with my tables?     (reads only)
bergman plan      # 🧭 what would maintenance do, and why? (reads only)
bergman run       # 🛠️ do it
bergman status    # 📡 what is the daemon doing, and what needs me?
```

📚 **Docs:** <https://hupe1980.github.io/bergman/> ·
🤝 **Sister project:** [Rustberg](https://github.com/hupe1980/rustberg), an
authenticated, policy-controlled Iceberg REST catalog. Each works fully without
the other.

---

## 📦 Install

```bash
brew install hupe1980/tap/bergman            # macOS (Apple Silicon) and Linux
cargo install bergman                        # anywhere Rust 1.95+ builds
```

🐳 As a container — distroless, non-root, statically linked:

```bash
docker run --rm \
  -v ./bergman.toml:/etc/bergman/bergman.toml:ro \
  ghcr.io/hupe1980/bergman:latest plan
```

Or grab a binary from the [releases page](https://github.com/hupe1980/bergman/releases):
static musl Linux for `x86_64` and `aarch64`, and macOS on Apple Silicon, each
with a `.sha256` beside it.

---

## ⚡ Quickstart

`bergman.toml` — every setting is optional:

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

```bash
bergman policy lint                              # ✅ validate offline, for CI
bergman policy explain prod.analytics.events     # 🧾 each setting and where it came from
bergman plan                                     # 🧭 read-only, always safe
```

Settings resolve, most specific first, through the matching **rule**,
**`[defaults]`**, the **catalog's own policy** (`policy_source = "polaris"`),
the **table's properties and `sort-order`**, and the **Iceberg default** — and
`policy explain` shows which layer every value came from.
[Configuration →](https://hupe1980.github.io/bergman/docs/configuration/) ·
[Policies →](https://hupe1980.github.io/bergman/docs/policies/)

---

## 🧹 What it does

| | Operation | Built on |
|:--:|---|---|
| 🔍 | Table health analysis | Manifest metadata only; no data file is opened |
| 🗜️ | Compaction | Upstream's scan and writers, Bergman's commit layer |
| 🧽 | Dangling delete-file removal | Bergman's commit layer |
| 📚 | Manifest rewrite | Bergman's commit layer |
| ⏳ | Snapshot expiration | Upstream `ExpireSnapshotsAction` |
| 🗑️ | Expiration file cleanup | Bergman — upstream leaves it to the caller |
| 👻 | Orphan-file removal | Bergman's object-store layer — `FileIO` has no `list` |

🎯 **Compaction is triggered, not scheduled** — by small files, oversized files,
delete ratio, and a settle time that leaves a partition being written alone. It
honours the table's `sort-order` and `write.*` properties, and applies equality
deletes with a DataFusion hash anti-join.
[Compaction →](https://hupe1980.github.io/bergman/docs/compaction/)

🆕 **Format v3** tables are maintained in full: a rewrite writes each row's
`_row_id` and `_last_updated_sequence_number` physically and the snapshot claims
its row range; deletion vectors are applied and retired by `(path, offset)`.

🚫 **Refused by name, never guessed at** — in `bergman plan`, the run report and
`bergman status`: encrypted tables (snapshot expiration still runs), anything but
Parquet, cross-spec rewrites, and any branch but `main`.
[Status →](https://hupe1980.github.io/bergman/docs/status/)

🧱 Bergman owns its commit layer because `iceberg::Transaction` cannot express a
rewrite: it builds the commit from upstream's public writers and `POST`s it to
the table endpoint, behind a `TableCommitter` trait.
[Architecture →](https://hupe1980.github.io/bergman/docs/architecture/)

---

## ⌨️ Commands

| Command | Writes? | What it does |
|---|:--:|---|
| `bergman inspect` | — | Table health: file counts, sizes, delete ratios, snapshots, manifests |
| `bergman plan` | — | The operations maintenance would perform, and why — or why not |
| `bergman run` | ✍️ | Executes the plan |
| `bergman run --dry-run` | — | Identical to `plan` |
| `… --table <glob>` | | Scopes `inspect`, `plan` and `run`; an excluded table is never read |
| `… --only <op>[,<op>]` | | Scopes `plan` and `run` to `compact`, `rewrite-manifests`, `expire-snapshots`, `remove-orphans` |
| `bergman daemon` | ✍️ | Runs cycles on schedules, or when told a table changed (`POST /events`, including Lakekeeper CloudEvents) |
| `bergman status` | — | Asks a running daemon what it is doing; what needs attention comes first |
| `bergman policy lint` | — | Validates config offline |
| `bergman policy explain <table>` | — | Effective policy with per-value provenance |
| `bergman policy match` | — | Which tables each rule matches |

Global flags: `--config`, `--format text|json`, `--audit-log <path>`, `--log <level>`.
`bergman run` and `bergman status` exit `2` when an operation failed or was
refused, so a scheduler or a check that reads only exit codes still notices.

📡 A daemon started with `--listen` serves `/metrics`, `/health`, `/status` and
(with `--events`) `/events`. `bergman status --daemon http://host:9090` reads
`/status` with no config file and no credentials — point it at a pod from your
laptop. [Operating →](https://hupe1980.github.io/bergman/docs/operating/)

---

## 🛡️ Safety

- 🔒 **Nothing destructive is on by default.** Compaction, orphan deletion and
  expiration's file cleanup default off; metadata-only snapshot expiration is on.
- 📖 **A table Bergman cannot fully read is refused**, and every manifest walk is
  all-or-nothing, so nothing is deleted on a partial reading.
- 👻 **Orphan removal has eight independent checks**, including a 24-hour grace
  floor, segment-wise containment, re-verification before deleting, a deletion
  ceiling, and a refusal when the listing and the metadata share no file.
  [Orphans →](https://hupe1980.github.io/bergman/docs/orphans/)
- 🎯 **One deleter**: orphan removal and expiration cleanup share one blast-radius
  ceiling, audit record and bounded deletion path.
- 🧩 **A rewrite never drops a delete file another file still needs**, and every
  file group's row count must land between a floor and a ceiling taken from
  metadata.
- 🔢 **Carried-forward files keep their sequence numbers; manifests keep their
  partition spec; a moved branch keeps its retention.**
- ⚖️ **Commits are compare-and-swap; a lost one is rebuilt, never re-offered.** A
  429/503 is retried; a 409/412 replans.
- 🧾 **Every operation is auditable**: `--audit-log` writes JSON Lines, and a
  deletion manifest is written before the first delete.
- 🤫 **Secrets never reach a log**: credentials and catalog config redact in
  `Debug`. `POST /events` can require a bearer token (`--events-token-env`).

---

## 🦀 Library

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

```bash
cargo add bergman --no-default-features --features catalog-rest,storage-s3
```

No global state, no background threads, the caller's async runtime, and
concurrency limits as parameters. Both catalog clients share one token source,
renewed on demand. `MaintenanceObserver` hooks metrics, events or an approval
gate — `operation_starting` returning `false` vetoes an operation:

```rust
#[async_trait::async_trait]
impl MaintenanceObserver for RequireSignoff {
    async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
        ctx.kind != OperationKind::RemoveOrphans || self.approved(ctx.table).await
    }
}
```

Embedding the daemon? `Daemon::status()` hands you the same live board
`/status` serves.

| Feature | Default | Carries |
|---|:--:|---|
| `cli` | ✅ | The binary, terminal rendering, logging setup |
| `catalog-rest` | ✅ | Iceberg REST catalog client |
| `compaction` | ✅ | Data-file rewriting, and the DataFusion executor it needs |
| `storage-s3` / `-gcs` / `-azure` | ✅ via `storage-all` | Cloud object stores |
| `metrics` | ✅ | Prometheus metrics, and the daemon's HTTP surface: `/metrics`, `/health`, `/status`, `/events`; `bergman status` |

[Library →](https://hupe1980.github.io/bergman/docs/library/)

---

## 🧪 Development

```bash
just test          # the full suite
just lint          # fmt + clippy -D warnings
just check-all     # every feature alone, plus all and none
just site          # the documentation site's internal links
just ci            # lint, check-all, test, docs, site
just deny          # advisories, licences, bans, sources — run separately
just msrv          # build on the MSRV — run separately
just cross-engine  # opt-in, needs uv: PyIceberg 0.12.0 writes, Bergman maintains, PyIceberg reads
```

The suite needs no containers or cloud credentials: it builds real tables on the
local filesystem and commits to an in-memory catalog.

## 📝 Changelog

[CHANGELOG.md](CHANGELOG.md). Bergman is pre-1.0: breaking changes land in minor
versions, with migration notes.

## ⚖️ License

Apache-2.0 OR MIT.
