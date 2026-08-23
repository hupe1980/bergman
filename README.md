# Bergman

**A Rust-native maintenance engine for Apache Iceberg. One crate. One binary. No JVM.**

Bergman keeps Iceberg tables healthy — compacting small files, applying and
retiring delete files, expiring snapshots, re-packing manifests and removing
orphans — driven by declarative policies and built on
[`iceberg-rust`](https://github.com/apache/iceberg-rust). No Spark, no Trino,
no cluster.

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
  to pair, and each works fully without the other.

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
| Dangling delete-file removal | ✅ | Bergman's commit layer — nothing else ever cleans these up |

Format **v1 and v2** tables are rewritten. A **v3** table's data-plane
operations are refused with a named reason: preserving row lineage through a
rewrite needs `_row_id` projected out of the scan and written back, which
upstream's reader and writer do not yet offer — and `iceberg-rust` rejects a v3
snapshot carrying no `first-row-id` outright, so such a commit does not apply at
all. Snapshot expiration and orphan removal still run, so a v3 table's history
stays bounded and its storage still gets reclaimed. The refusal appears in
`bergman plan`, not only in a log line.

### Why bergman owns its commit layer

`iceberg::Transaction` has no action that removes a data file, and both
`TransactionAction` and `TableCommit`'s builder are `pub(crate)` — so
compaction and manifest rewriting cannot be expressed through it at all. The
common answer is to fork; [`nimtable/iceberg-compaction`](https://github.com/nimtable/iceberg-compaction)
pins `risingwavelabs/iceberg-rust` at a git revision, which costs a rebase
forever and a crate that cannot be published.

Bergman owns just that layer instead. Every piece of a commit is already public,
so it builds one with upstream's own writers and `POST`s it to the table
endpoint. Operations commit through a `TableCommitter` trait rather than a
transport, so an upstream action that can express a rewrite becomes a second
implementation and nothing above `src/commit` changes.
[Detail →](https://hupe1980.github.io/bergman/docs/status/)

---

## Install

```bash
cargo install bergman
```

Or as a container — distroless, non-root, statically linked:

```bash
docker run --rm \
  -v ./bergman.toml:/etc/bergman/bergman.toml:ro \
  ghcr.io/hupe1980/bergman:latest plan
```

Or from source (requires Rust 1.94+):

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
3. the **table's own metadata** — its Iceberg properties
   (`write.target-file-size-bytes`, `history.expire.max-snapshot-age-ms`, …)
   *and its `sort-order`*
4. the **Iceberg specification default**

Layer 3 is what makes Bergman a participant rather than a competing source of
truth: a table already carries its owner's intent, and every other Iceberg tool
reads it.

`bergman policy explain` shows every resolved setting *and the layer that
answered*, because the question is never "what is the target file size" — it is
"why is it *that*":

```
$ bergman policy explain prod.analytics.events

prod.analytics.events
  matched rule: prod.analytics.*

 SETTING                       VALUE                        FROM
 compaction.target_file_size   128 MiB                      table property write.target-file-size-bytes
 compaction.sort               event_date, customer_id desc the table's sort order
 snapshots.max_age             7d                           [defaults]
 snapshots.min_to_keep         3                            [defaults]
 snapshots.delete_files        false                        Bergman default
 orphans.mode                  dry-run                      Bergman default
 …
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

## Compaction

Compaction is *triggered*, not scheduled. A table already at target file size
costs one metadata read per cycle and no data I/O.

| Trigger | Default | What it catches |
|---|---|---|
| `small_file_ratio` | 30% of files below 75% of target, and ≥ 5 of them | The classic small-file problem |
| `delete_ratio` | 10% of rows named by delete files | Streaming and CDC targets, where read amplification comes from deletes rather than file sizes |
| `min_file_age` | partition quiet for 1h | Its *inverse* — a partition still being written is left alone, rather than losing a commit race to the next micro-batch |

```toml
[[rules]]
match = "prod.streaming.events_*"

[rules.compaction]
enabled = true
sort    = ["event_date", "customer_id"]   # optional — see below

[rules.compaction.trigger]
delete_ratio = 0.05    # aggressive: this is a CDC target
min_file_age = "2h"    # but leave the partition it is writing alone
```

**A table's own sort order is honoured without being asked.** A table that
declares `sort-order` has writers that respect it, so a rewrite that bin-packed
those files back together *unsorted* would leave the table claiming a clustering
its files no longer have — and every query with a predicate on the sort columns
would start reading every file. Direction and null placement are reproduced
exactly. A rule's `sort` overrides it; where neither says anything, output is
unsorted.

**A partition is not a unit of work.** Its eligible files are bin-packed into
groups bounded by `max_group_bytes` (8 GiB) and `max_input_files` (10 000), and
each group commits on its own — so one lost commit costs one group, and a
partition larger than memory is still compactable.

Each group is read with its delete files applied, optionally sorted, and written
back at the target size honouring the table's own `write.parquet.*` settings. A
delete file is retired only when every data file it applies to is inside the
group; ones left applying to nothing are removed separately, since nothing else
ever cleans them up.

Equality deletes go through a DataFusion hash anti-join, which is why the
default build carries a query engine — upstream applies them as a nested-loop
join over `data rows × delete rows`. It sits behind the default-on `compaction`
feature, so an embedder wanting only metadata maintenance takes
`default-features = false` and carries none of it.
[Detail →](https://hupe1980.github.io/bergman/docs/compaction/)

---

## Safety

Maintenance is a background tenant: it must never win against a foreground
writer, and never corrupt one.

**Nothing destructive is on by default.** Compaction, orphan deletion, and
expiration's file cleanup all default off. Metadata-only snapshot expiration is
the single exception, because unbounded snapshot growth is the most common
Iceberg health problem.

**Orphan removal has seven independent checks**, because it is the one
operation that can destroy a healthy table: dry run by default; a grace period
with a hard 24-hour floor that cannot be configured away; unknown file age
counts as too young; segment-wise containment, so `…/events` can never reach
`…/events_archive`; a refusal to scan a location another table lives inside —
detected from the listing itself, so a table deliberately excluded from
maintenance is still protected; re-verification against freshly-loaded metadata
before deleting; and a ceiling on how many files one operation may remove.
[Detail →](https://hupe1980.github.io/bergman/docs/orphans/)

**One deleter, one safety model.** Orphan removal and expiration's file cleanup
decide *what* to delete by different reasoning, then share one deletion path:
blast-radius ceiling, audit record, bounded-concurrency deletion. What the
ceiling withholds is reported and reclaimed by the next pass.

Path comparison is normalized throughout (`s3://` vs `s3a://`, doubled slashes,
trailing slashes), because a live file spelled differently from its metadata
would otherwise look exactly like garbage.

**A rewrite never drops a delete file another file still needs.** Compaction
retires a delete file only when *every* data file it applies to is inside the
group being rewritten. One shared with a file outside the group is still hiding
rows there, and dropping it would bring them back — so the whole table is
planned before anything is rewritten, because that question cannot be answered
from inside the group.

**Rows in equal rows out — or, where deletes apply, no more than went in.**
Every file group compares what it wrote against what the manifests said it read,
and refuses to commit when the comparison fails. With no delete file applying,
the counts must be equal; where deletes apply the exact figure is unknowable
from metadata, so the input count stands as a ceiling and a rewrite that
*added* rows is refused. The outputs are abandoned and the orphan scanner
reclaims them. A rewrite that silently lost rows is indistinguishable from one
that worked, and the table it produces is wrong forever.

**A carried-forward file keeps its sequence number.** A data file carried
through a rewritten manifest keeps its own number, so delete files written
*later* still apply to it. Stamping the new snapshot's number on it would make
it look newer than the deletes that should remove its rows. Files the rewrite
genuinely *adds* are the opposite case and take the new number — which is what
retires the deletes already applied to their contents.

**Manifests are never re-interpreted under the wrong partition spec.** A
manifest carries exactly one `partition_spec_id`, and an entry's partition
tuple means nothing against any other. Bergman rewrites each manifest under the
spec it was written under, and refuses to compact a partition written under a
spec other than the table's current one — mixing them mis-prunes files at query
time, and nothing fails while it does.

**Commits are compare-and-swap, and a lost one is rebuilt, never re-offered.**
On conflict Bergman reloads the table, re-plans the group against the files that
are still live, and rewrites. Re-submitting outputs computed against a table
that has since moved is how a concurrent delete gets discarded and its rows
come back. After three attempts it reports `conflicted` and comes back next
cycle — a table being written hard keeps winning, and that is the design
working.

**A busy catalog and a moved table are told apart.** A 429 or 503 means "come
back later" and the identical request is re-sent; a 409 or 412 means the table
moved and the plan is rebuilt. Confusing the two would either throw away good
work or re-submit a stale commit.

**Nothing is silent.** A truncated plan, a withheld deletion, a partition under
a superseded spec, an operation the policy enabled that this table's format
cannot receive — each appears in `bergman plan` and in the run report with its
reason, because otherwise those tables read as healthy.

**Every operation is auditable.** `--audit-log` appends a JSON Lines record per
operation, and a deletion manifest is written *before* the first delete — so a
crash halfway through still leaves evidence of what was about to go.

**Secrets never reach a log.** `Credential`, the cached bearer token and
`CatalogConfig` redact in `Debug` — all three are reachable from `Bergman`
itself, so a derived impl would put the client secret and the warehouse's
storage keys into any `tracing::debug!(?bergman)` an embedder writes.

**`POST /events` can require a token.** Unlike `/metrics` and `/health` it
*causes work* — listing object storage, rewriting data. `--events-token-env`
names the variable holding a bearer token, compared in constant time; the daemon
warns when the endpoint is open on a routable address.

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
| `… --table <glob>` | — | Scopes `inspect`, `plan` and `run` — the *examination*, not just the output: an excluded table is never read |
| `bergman daemon` | **yes** | Runs cycles on schedules, or when told a table changed |
| `bergman daemon --events --events-token-env VAR` | **yes** | …and requires a bearer token on `POST /events` |
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
| `compaction` | ✅ | Data-file rewriting, and the DataFusion executor it needs |
| `storage-s3` / `-gcs` / `-azure` | ✅ via `storage-all` | Cloud object stores |
| `metrics` | — | Prometheus metrics, and the daemon's HTTP surface: `/metrics`, `/health`, `/events` |

Metric labels are `catalog`, `namespace`, `operation` and `outcome` — bounded by
construction. The table name is deliberately not one: a series is created per
label combination and kept forever, so a large warehouse would take the
Prometheus server down. Per-table facts live in the audit trail, which has no
cardinality budget.

Local filesystem and in-memory storage are always available, which is what keeps
the test suite free of containers. Embedders take `default-features = false`.

---

## Development

```bash
just test          # the full suite
just lint          # fmt + clippy -D warnings
just check-all     # every feature alone, plus all and none
just deny          # advisories, licences, bans, sources
just site          # the documentation site's internal links
just ci            # everything above, in the order CI runs it
```

Every optional feature is built **alone** in CI, not only under `--all-features`
— a feature that compiles in that combination can be broken by itself, because
another crate's feature unification was switching on what it needed.

## License

Apache-2.0 OR MIT.
