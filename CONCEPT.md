# Bergman — Concept

**A Rust-native maintenance engine for Apache Iceberg. One crate. One binary. No JVM.**

Bergman plans *and* executes Iceberg table maintenance — compaction, snapshot
expiration, manifest optimization, orphan-file removal — as a single static
binary built on `iceberg-rust`, Apache Arrow, DataFusion, and Parquet. It is
policy-driven like a control plane and self-sufficient like an engine: no
Spark, no Trino, no external execution cluster.

- **Status:** Concept / pre-implementation
- **License:** Apache-2.0 OR MIT
- **Repository:** `hupe1980/bergman`
- **Crate:** `bergman` — **library-first**, with the `bergman` CLI binary as a
  thin, feature-gated consumer of the same public API (§4.1)

---

## 1. Positioning: the gap in the landscape

Iceberg maintenance today splits into two camps, and neither is complete:

| Project | Control plane (policies, scheduling) | Execution | Runtime |
|---|---|---|---|
| [Floe](https://github.com/nssalian/floe) | ✅ Declarative policies, cron, multi-catalog | ❌ Delegates to Spark (via Livy) / Trino | Kotlin/JVM + external clusters |
| [nimtable/iceberg-compaction](https://github.com/nimtable/iceberg-compaction) | ❌ Library only, no policies/scheduling | ✅ DataFusion-based compaction | Rust, embedded |
| RisingWave embedded compactor | ❌ Tied to RisingWave's own ingestion | ✅ Rust + DataFusion (5.5× faster than Spark, ~35% of RAM in their benchmarks) | Rust, proprietary integration |
| [Lakekeeper](https://docs.lakekeeper.io/docs/latest/table-maintenance/) | ✅ Catalog-side: expire snapshots, orphan removal, metadata cleanup — event-driven, post-commit scheduling | ❌ **No compaction** — emits CloudEvents so an *external* engine can compact | Rust catalog |
| [Rustberg](https://github.com/hupe1980/rustberg) *(sister project)* | ✅ Governance: Cedar authorization, credential vending/remote signing, audit trail | ❌ Deliberately not — its design names maintenance as the external system's job (§10) | Rust catalog |
| Spark procedures / Flink TableMaintenance | ❌ Imperative, per-job | ✅ Battle-tested, full-featured | JVM cluster |
| Managed services (Tabular/Databricks, AWS S3 Tables, LakeOps, IOMETE, …) | ✅ | ✅ | Vendor lock-in |

**Floe validated the control-plane idea** (policy-driven, glob matching,
multi-catalog, record outcomes) but punts execution to the very JVM
infrastructure that most small/mid-size lakehouse teams want to avoid.
**nimtable/iceberg-compaction and RisingWave validated the execution idea**
(Rust + DataFusion compacts faster than Spark with a fraction of the memory,
and survives delete-heavy workloads that OOM Spark) but ship no policies, no
scheduler, no lifecycle operations beyond compaction.

The catalog rows deserve emphasis: the Rust catalogs — Lakekeeper, Rustberg,
and Polaris in its maintenance discussion (#538) — independently reached the
same boundary: **the catalog manages metadata and enforces permissions;
data-plane rewrites belong to an external engine** the catalog can trigger
and govern. That external engine is exactly the seat Bergman takes — and it
is why Bergman must be embeddable, not just runnable (§4.1), and why it is
designed to be *driven through* a governing catalog rather than around one
(§10).

**Bergman is the missing intersection**: Floe's operating model with
nimtable's execution model, in one auditable binary you can run as a cron
job, a sidecar, or a long-lived daemon — and one library any Rust service
can embed — for the 95% of tables that do not need a distributed shuffle to
stay healthy.

### What upstream provides, and what it does not

Verified against `iceberg-rust` 0.10.1 and `main`:

| Capability | Upstream state | Bergman |
|---|---|---|
| Snapshot expiration (metadata) | ✅ `Transaction::expire_snapshots()` — per-branch ancestry, per-ref retention, ref aging, at Java `RemoveSnapshots` parity | **Implemented**, delegating selection to upstream |
| Expiration *file cleanup* | ❌ Explicitly out of scope — its docs call physical cleanup "the responsibility of a higher-level maintenance operation built on top of this action" | **Implemented** — Bergman is that operation |
| Orphan-file removal | ❌ No API, and no way to build one: `FileIO`/`Storage` have read, write, delete, `delete_prefix` — **no `list`** | **Implemented**, on Bergman's own object-store layer |
| Table health analysis | ✅ Manifests readable via `ManifestList::parse_with_version` + `ManifestFile::load_manifest` | **Implemented** |
| Compaction (rewrite data files) | ⚠️ **No action removes data files** — `RewriteFilesAction` (#1606) closed unmerged, `OverwriteAction` (#2185) and the CoW primitive (#2752) open. But the *pieces* are public: `ManifestWriterBuilder`, `ManifestListWriter`, `Snapshot::builder`, `TableUpdate`/`TableRequirement` (both `Serialize`), `ArrowReader` (applies positional **and** equality deletes), `ParquetWriterBuilder` → `RollingFileWriter` → `DataFileWriter` | **Implemented**, on Bergman's own commit layer |
| Manifest rewrite | ⚠️ No action; #1237 closed unmerged. Same public pieces | **Implemented**, same way |
| Delivering a commit | ❌ `TableCommit`'s builder and `TransactionAction` are both `pub(crate)` | **Bergman owns this** (§4.2) |
| Custom commits as a workaround | ❌ Closed off: `TransactionAction` is `pub(crate)`, and `TableCommit`'s builder is `#[builder(build_method(vis = "pub(crate)"))]` with the comment *"dangerous and error-prone to construct directly"* | — |

Two consequences shape everything below.

**First, the blocker is narrower than it looks, and Bergman routes around it
rather than waiting.** An external crate cannot *deliver* an Iceberg commit —
but it can build every byte of one, because the manifest writers, the snapshot
builder, and `TableUpdate`/`TableRequirement` are all public and serializable.
So Bergman owns the commit layer (§4.2) and delivers `(requirements, updates)`
over the REST protocol itself. The bytes on the wire are identical to what
`iceberg-catalog-rest` sends, because they are the same serialized types going
to the same endpoint.

This is what the rest of the market did too, only worse: `nimtable/iceberg-
compaction` pins a **fork** (`risingwavelabs/iceberg-rust` at a git rev), and
RisingWave vendored the logic into its engine. A fork costs a rebase forever
*and* a crate that cannot be published, since Cargo rejects git dependencies on
crates.io — fatal for a library-first project. Owning one small layer is the
cheaper trade, and it is the same call Rustberg made when `iceberg::Catalog`
proved structurally unusable for a catalog server.

**Second, the metadata plane is not just feasible, it is where upstream is
explicitly asking for a partner.** `ExpireSnapshotsAction`'s own
documentation delegates file cleanup upward; `FileIO` has no `list` at all.
Those two gaps are precisely snapshot expiration's cleanup half and orphan
removal — both implemented here, both with the safety model of §5.4, and both
the natural things to contribute upstream.

This also settles open question §14.1: expiration cannot defer file deletion
to the orphan scanner *as a matter of upstream policy* — upstream deletes
nothing — so Bergman implements deletion once, in one place, and expiration
either uses it (`delete_files = true`) or leaves the files for the scanner.
One deleter, one safety model, as intended.

---

## 2. Critique of the Floe-RS RFC (what we deliberately do differently)

The circulating "Floe-RS" RFC points in the right direction but hand-waves
every hard problem. Bergman's concept exists to close those gaps:

1. **No delete-file story.** The RFC never mentions positional deletes,
   equality deletes, or v3 deletion vectors. This is disqualifying: the
   tables that need maintenance *most* are streaming targets (Flink,
   Kafka Connect, RisingWave, CDC pipelines) whose write pattern is
   micro-batches *plus equality deletes*. Compaction that cannot apply and
   retire delete files is a toy. Bergman treats **delete-file compaction as a
   first-class operation**, not a footnote.
2. **"Retry on conflict" is not a commit model.** Blindly re-committing a
   rewrite after a concurrent row-level delete silently **resurrects deleted
   rows**. A correct engine must re-validate on every retry: no new delete
   files applicable to the rewritten data files, no removed source files,
   sequence-number correctness. Bergman specifies this contract (§6).
3. **Orphan-file removal is missing entirely** — and it is the single most
   dangerous maintenance operation (one bad reachability set deletes live
   data; Iceberg's own community has multiple postmortems). Bergman includes
   it with a safety model (§5.4), because expire-snapshots without orphan
   cleanup leaks storage forever.
4. **Cargo-cult distribution.** "v2: NATS/Redis/Kafka work queues" imports
   the operational footprint the project claims to eliminate. Maintenance
   parallelism is naturally *table- and partition-grained*; a work-stealing
   scheduler inside one process, and N stateless replicas coordinating
   through catalog optimistic concurrency, covers the realistic scale (§8).
   No message broker in any phase.
5. **No cost model.** Rewriting data you don't need to rewrite is the #1
   failure mode of naive compaction (write amplification can exceed the read
   savings). Bergman plans *triggered* maintenance: operations run when
   table-health metrics cross thresholds, not on every tick (§5.1, §7).
6. **"Web dashboard" before correctness.** Bergman's v1 UI is a CLI with
   `plan`/`--dry-run` output and Prometheus metrics. A dashboard is a
   later luxury, not Phase 2.

---

## 3. Design principles

1. **Do no harm.** Every operation has a dry-run mode; destructive actions
   (orphan deletion) default to dry-run and require explicit opt-in plus a
   grace period. Every commit is validated, every deletion double-checked
   against a freshly-loaded reachability set.
2. **Policies declare intent, tables keep authority.** Bergman policies
   layer *on top of* Iceberg table properties
   (`write.target-file-size-bytes`, `history.expire.*`,
   `commit.retry.num-retries`). Absent an explicit policy value, the table
   property governs; absent both, Iceberg's documented defaults apply. No
   second source of truth.
3. **Library-first, single crate.** The public library API is the product;
   the CLI is its first customer and proof that the API is sufficient
   (anything the CLI can do, an embedder can do). Catalogs, object stores,
   and the CLI itself sit behind Cargo features so `default-features =
   false` yields a lean, embeddable core.
4. **Plan/execute separation.** Planning is pure and cheap (metadata-only);
   execution is effectful. `bergman plan` shows exactly what `bergman run`
   would do — same code path, execution stubbed. This is the auditability
   contract.
5. **Crash-only software.** Any run can be killed at any point without
   corrupting a table: files are written before the commit that references
   them; a failed run leaves only uncommitted files that the orphan scanner
   (or the writer's own cleanup) later removes. No local state that matters.
6. **Observability is a feature, not an add-on.** Every commit emits a
   structured audit record (what ran, why the policy triggered it, files
   before/after, bytes, duration, snapshot IDs). Prometheus + OTel traces
   from day one.
7. **Upstream-first.** Metadata logic that belongs in `iceberg-rust`
   (reachability computation, manifest rewrite actions) gets contributed
   upstream; Bergman carries a temporary implementation only while the
   upstream gap exists, clearly marked.

---

## 4. Architecture

```text
                        bergman (single crate, single binary)
┌──────────────────────────────────────────────────────────────────────┐
│  CLI / daemon (clap)                                                 │
│   ├─ bergman plan|run|daemon|inspect|policy                          │
├──────────────────────────────────────────────────────────────────────┤
│  Scheduler (tokio)                                                   │
│   cron · health-metric triggers · event triggers (catalog events)    │
│   per-table locks · concurrency limits · retry with jittered backoff │
│   maintenance windows                                                │
├──────────────────────────────────────────────────────────────────────┤
│  Policy engine                                                       │
│   YAML policies · glob table matching · layered resolution           │
│   (policy → table properties → Iceberg defaults) · validation        │
├──────────────────────────────────────────────────────────────────────┤
│  Table-health analyzer (metadata only, no data I/O)                  │
│   file-size histograms · delete-file ratios · snapshot counts        │
│   manifest fragmentation · partition skew  → triggers & priorities   │
├──────────────────────────────────────────────────────────────────────┤
│  Planner                                                             │
│   op planners: compact · expire · rewrite-manifests · orphans        │
│   partition-grained task DAGs · cost estimates · dry-run rendering   │
├──────────────────────────────────────────────────────────────────────┤
│  Executor                                                            │
│   DataFusion streaming exec (memory pool, spill-to-disk)             │
│   Parquet read → apply deletes → repartition/sort → Parquet write    │
│   task-level parallelism · progress · cancellation                   │
├──────────────────────────────────────────────────────────────────────┤
│  Commit layer                                                        │
│   iceberg-rust Transaction API · RewriteFiles validation             │
│   snapshot-isolation re-validation on retry · commit audit log       │
├──────────────────────────────────────────────────────────────────────┤
│  iceberg-rust: catalogs (REST · Glue · HMS · SQL · S3Tables)         │
│  FileIO / OpenDAL: S3 · GCS · ADLS · MinIO · local fs                │
└──────────────────────────────────────────────────────────────────────┘
```

There is deliberately **no separate "maintenance planner service", queue, or
worker tier**. The layers are library modules; the binary composes them.

### 4.1 Consumption modes: library first, binary second

Bergman ships as one crate with three ways in — all driving the same
planner/executor/commit code, so behavior and audit records are identical:

1. **Embedded library** (`bergman`, `default-features = false`): call the
   planner and executor from your own service. Target embedders, each backed
   by an existing precedent:
   - **Rust catalogs** — Lakekeeper deliberately does not compact and emits
     CloudEvents to trigger external compaction; Rustberg draws the same
     boundary (§10); a catalog (or its operator) can embed Bergman as the
     compactor both leave room for.
   - **Streaming writers / CDC sinks** — RisingWave proved the
     embedded-compactor pattern by building one into their engine; anyone
     writing Iceberg from Rust should not have to.
   - **Lakehouse platforms & control planes** — nimtable's observability
     platform embeds `iceberg-compaction` the same way; Bergman offers the
     full operation set (expire, manifests, orphans), not just compaction.
2. **CLI, one-shot** (`bergman plan|run`, `cli` feature, on by default for
   the binary): cron / Kubernetes CronJob / CI.
3. **Daemon** (`bergman daemon`): long-lived scheduler + metrics endpoint.

Library contract (the rules that make embedding actually work, learned from
DataFusion/tantivy-style lib-first projects):

- **No global state, no surprises:** no logger/tracing-subscriber
  initialization, no signal handlers, no ambient config file reads in the
  library — those live in `cli/`. Instrumentation via `tracing` spans only.
- **Bring-your-own runtime:** plain `async fn` on the caller's tokio
  runtime; concurrency limits are parameters, not process-wide statics.
- **Sans-CLI types:** `Policy`, `TableHealth`, `MaintenancePlan`,
  `RunReport` are plain serializable types; `plan()` is side-effect-free so
  embedders get dry-run for free.
- **Hooks:** a `MaintenanceObserver` trait (task started/finished, commit
  attempted/conflicted, files deleted) so embedders wire their own metrics,
  approval gates, or event buses without forking.
- **Semver honesty:** pre-1.0, breaking changes only in minor versions with
  a changelog migration note; the CLI surface stabilizes before the library
  API does, and docs say so.

Deliberately still a single crate: no `bergman-core`/`bergman-cli` split
until a real embedder's dependency tree forces it — feature gates solve
today's problem without a workspace's release overhead. Python bindings
(pyo3) are attractive later — PyIceberg has expire-snapshots but compaction
is still only "planned" — but bindings are packaging, kept out of the core
crate's scope (§14).

### Module map (single crate)

```text
bergman
├── src/
│   ├── policy/        # schema, parsing, validation, layered resolution
│   ├── catalog/       # discovery, table matching, catalog config
│   ├── health/        # metadata analysis, trigger evaluation
│   ├── plan/          # op planners, task DAGs, cost estimation
│   ├── exec/          # DataFusion pipelines, parquet io, spill config
│   ├── commit/        # transaction wrappers, validation, retry
│   ├── ops/           # compact / expire / manifests / orphans / sort
│   ├── sched/         # cron, triggers, per-table locking, budgets
│   ├── obs/           # metrics, tracing, audit log
│   └── cli/           # clap commands, human + json output (feature "cli")
└── Cargo features: cli (default) · rest (default) · glue · hms ·
                    sql-catalog · s3 (default) · gcs · azure · otel
    (embedders: default-features = false + the backends they need)
```

---

## 5. Maintenance operations (with correctness contracts)

Ordering matters and Bergman enforces it per table per cycle:
**compact → rewrite manifests → expire snapshots → remove orphans.**
Compacting first makes expiration reclaim the superseded small files;
expiring before orphan-scanning shrinks the reachability set legitimately.

### 5.1 Small-file & delete-file compaction (bin-pack)

*Trigger, not schedule.* A partition is eligible when policy thresholds are
crossed, e.g.:

- `small_file_ratio`: fraction of files under `min-file-size` (default 75% of
  target) exceeds threshold, **and** at least `min-input-files` (default 5)
  are eligible — mirrors Spark's `rewrite_data_files` defaults so behavior is
  unsurprising to Iceberg operators;
- `delete_ratio`: positional/equality delete records applicable to a file
  group exceed a threshold (default 10%), triggering merge-on-read cleanup
  even when file sizes are fine;
- `age`: files older than a floor, so hot partitions still being written
  aren't churned (avoid compacting the partition the streamer is appending
  to — this is the top source of pointless commit conflicts).

*Execution.* Per file group (partition-grained): DataFusion scan of the
input data files **with applicable deletes applied** (positional, equality,
and v3 deletion vectors as `iceberg-rust` support matures) → optional sort →
Parquet writes rolled at target size with the table's configured compression
and the writer producing full column metrics. Memory-bounded via DataFusion's
memory pool with spill-to-disk; file groups are sized so a single group never
requires more than the configured memory budget.

*Commit.* `RewriteFilesAction`: removes input data files, removes the delete
files fully applied during the rewrite, adds outputs — one atomic snapshot
(`replace` operation). Sequence-number handling per spec so later deletes
still apply to the new files.

*Contract:* row-count in == row-count out minus rows removed by applied
deletes (verified per task, not assumed); never drop a delete file that also
applies to files *outside* the rewritten group.

### 5.2 Sort-based clustering & z-order (later phase)

Same pipeline with a sort stage; range-partitioned by sort key so output
files carry tight min/max bounds. Z-order arrives after bin-pack and sort
are proven — it is an optimization, not table health. Hilbert curves are
explicitly out of scope until someone demonstrates need.

### 5.3 Snapshot expiration & metadata cleanup

Built on `iceberg-rust`'s `ExpireSnapshotsAction`, with Bergman enforcing
the full Java-parity contract before enabling deletion of data:

- Respect `max_snapshot_age`, `min_snapshots_to_keep`, **and per-ref
  retention** — branches and tags have their own retention; a snapshot
  reachable from any retained ref is never expired.
- File deletion only for files unreachable from *all* retained snapshots:
  data files, delete files, manifests, manifest lists, statistics files
  (Puffin), and partition-statistics files.
- Old `metadata.json` cleanup honoring
  `write.metadata.delete-after-commit.enabled` semantics.

*Contract:* expiration deletes metadata *and* newly-unreachable files, or —
if the upstream action's file-cleanup is not yet trustworthy — deletes
metadata only and defers file removal to the orphan scanner. This choice is
explicit in config, never implicit.

### 5.4 Orphan-file removal (the dangerous one)

The reachable set is computed from **all** metadata: every retained
`metadata.json`, every reachable snapshot's manifest list, all manifests,
all data/delete/statistics files. The candidate set is an object-store
listing under the table location. Orphans = candidates − reachable.

Safety model, non-negotiable:

- **Dry-run by default.** `mode: delete` must be set explicitly per policy.
- **Grace period** (`older_than`, default 7 days, hard floor of 24h that
  cannot be configured away): in-flight writers stage files before
  committing; deleting young unreferenced files is how you corrupt a table.
- **Re-verify before delete:** metadata is re-loaded after listing; any file
  that became reachable between scan and delete is dropped from the kill
  list (TOCTOU guard).
- Location-prefix sanity checks (never operate outside the table location;
  refuse tables with overlapping locations), path-normalization rules for
  scheme/authority mismatches, deletion rate-limits, and a full audit
  manifest of every deleted object written *before* deletion begins.

### 5.5 Manifest rewrite

Coalesce fragmented manifests toward `commit.manifest.target-size-bytes`,
cluster manifest entries by partition for planning locality. Pure
metadata operation (Avro read/write + snapshot with `replace` semantics) —
cheap, low-risk, high leverage on query planning latency. If upstream lacks
a `RewriteManifests` action at implementation time, this is the first thing
Bergman implements directly against the spec and offers upstream.

---

## 6. Commit & conflict model

Maintenance is a **background tenant**: it must never win against a
foreground writer and never corrupt one.

- **Optimistic concurrency** via the catalog's atomic swap; retries with
  jittered exponential backoff honoring the table's `commit.retry.*`
  properties.
- **Re-validation on every retry** (this is the part naive designs get
  wrong). For a rewrite commit against a moved table head:
  - abort-and-replan if any input file was removed by a concurrent commit
    (it was compacted/deleted elsewhere — our outputs are stale);
  - abort-and-replan if new delete files apply to any input file
    (re-committing would resurrect deleted rows);
  - proceed only when the concurrent commits are disjoint (appends to other
    partitions, other file groups).
  `iceberg-rust`'s `RewriteFilesAction` validation covers the core checks;
  Bergman treats validation failure as *replan*, never *force*.
- **Partition-grained commits.** Each file-group task commits independently
  (configurable batching), so one conflict costs one group's work, not the
  table's. Partial progress is progress — this is what makes compaction of
  actively-written tables tractable.
- **Idempotent tasks.** Task identity = (table UUID, starting snapshot ID,
  file-group content hash). A crashed run leaves only unreferenced output
  files; re-running replans from the current snapshot. Uncommitted outputs
  from dead runs are reclaimed by the orphan scanner after the grace period.
- **Backpressure / politeness:** configurable max commit rate per table and
  a `busy-table` heuristic (if N consecutive replans, back off for the
  cycle and record why).

---

## 7. Policy engine

Configuration is **TOML**, matching the sister project Rustberg (§10.4) — one
config language across the family, and the maintained parser both trees
already carry. An earlier draft used YAML; the only YAML-specific feature it
used was an anchor (`<<: *defaults`) to share settings, and layered resolution
makes that redundant: `[defaults]` is a real layer in the resolver, not a
templating trick.

```toml
# bergman.toml

[[catalogs]]
name      = "prod"
kind      = "rest"
uri       = "https://polaris.example.com/api/catalog"
warehouse = "s3://lake/warehouse"
token_env = "BERGMAN_CATALOG_TOKEN"   # a name, never a value

[catalogs.properties]                  # Iceberg's own property names
"s3.region"   = "eu-central-1"
"s3.endpoint" = "https://s3.eu-central-1.amazonaws.com"

# Layered under every rule that does not override it.
[defaults.compaction]
# target_file_size omitted -> table property -> Iceberg default

[defaults.compaction.trigger]
small_file_ratio = 0.3
min_input_files  = 5
delete_ratio     = 0.1

[defaults.snapshots]
max_age     = "7d"        # omitted -> history.expire.max-snapshot-age-ms
min_to_keep = 3

[defaults.orphans]
mode       = "dry-run"    # deleting must be opted into explicitly
older_than = "7d"

# Rules are evaluated in order; the first match wins.
[[rules]]
match = "prod.analytics.*"          # `*` stops at a namespace boundary

# CDC targets: aggressive delete-file cleanup.
[[rules]]
match    = "prod.streaming.events_*"
schedule = "0 */2 * * *"
[rules.compaction]
enabled = true
sort    = ["event_date", "customer_id"]
[rules.compaction.trigger]
delete_ratio = 0.05

[[rules]]
match = "prod.tmp.*"
skip  = true                        # explicit exclusion beats implicit

[limits]
max_parallel_tables       = 4
max_rewrite_bytes_per_run = 536_870_912_000   # cost ceiling per cycle
maintenance_window        = "22:00-06:00 Europe/Berlin"
```

Key semantics:

- **Layered resolution** (§3.2): rule → `defaults` → table properties →
  Iceberg defaults. `bergman policy explain <table>` prints the fully
  resolved effective policy with the provenance of every value.
- **Triggers over schedules.** `schedule` defines when *evaluation* runs;
  the health analyzer decides whether anything *executes*. A table that's
  already healthy costs one metadata read, zero data I/O.
- **Event triggers beat polling.** Lakekeeper set the state of the art here:
  it schedules maintenance *after commits*, adaptively, instead of cron
  scans. Bergman's daemon accepts commit events as an evaluation trigger —
  catalog CloudEvents (Lakekeeper) via webhook, and embedders push events
  directly through the library API — with cron as the fallback for catalogs
  that emit nothing. Debounced per table so a busy streamer doesn't trigger
  evaluation on every micro-batch.
- **Budgets** make cost explicit: byte ceilings per cycle, maintenance
  windows, table-priority ordering when the budget doesn't cover everything
  (most-fragmented-first by default).
- **Validation is strict:** unknown keys are errors, `bergman policy lint`
  runs in CI, matched-table preview via `bergman policy match`.

Deliberately absent: a policy plugin system (a Rust library API is the
extension point; embedding a scripting language into a correctness-critical
deleter is complexity without a constituency), and per-policy credentials in
YAML (credentials come from env/IAM only).

---

## 8. Deployment & scalability model

**v1 — one process, honest parallelism.** Tokio + DataFusion parallelize
across tables (bounded), file groups within a table, and cores within a file
group. A single node with 8–16 cores and a few GB of RAM covers thousands
of tables when maintenance is trigger-based — the RisingWave/nimtable data
points (Spark-class throughput at ~1/3 the memory, no cluster) are the
existence proof.

Modes: `bergman run` (one cycle, exits — perfect for cron/Kubernetes
CronJob/ECS scheduled task) and `bergman daemon` (long-lived, internal cron,
`/metrics`, health endpoints).

**v2 — stateless replicas, catalog-mediated coordination.** Scale-out =
run N replicas with deterministic table-shard assignment (rendezvous hashing
over the table UUID set, config-declared replica index) — no broker, no
consensus service, no shared database. Optimistic catalog commits already
make double-execution safe (one replica's commit conflicts and replans), so
sharding is a cost optimization, not a correctness requirement. A message
queue enters the design only if petabyte-scale single-table rewrites with
cross-node shuffle become a real target — which is a non-goal (§11).

---

## 9. Observability & security

**Observability.** Prometheus metrics (tables evaluated/triggered, file
groups rewritten, bytes/files before→after, deletes retired, snapshots
expired, orphans found/deleted, commit conflicts, replans, task duration
histograms); OTel traces per operation (plan → exec → commit spans); a
structured **audit log** (JSONL, optionally written to object storage) with
one record per commit and per deletion batch: policy rule that triggered it,
inputs, outputs, snapshot IDs, wall time, outcome. `bergman inspect <table>`
renders table health (file-size histogram, delete ratio, snapshot/manifest
counts) without changing anything — useful on day zero, before anyone
trusts Bergman to write.

**Security.** Credentials via standard provider chains (AWS/GCP/Azure SDK
semantics through OpenDAL) and env vars only; catalog-vended credentials
(REST credential vending — Rustberg, Polaris, S3 Tables, Lakekeeper)
preferred where available, in which case Bergman holds no storage secret at
all (§10.2); OAuth2/SigV4 for catalogs; TLS verification on by default with
no global insecure switch (per-catalog opt-out for lab MinIO only). Bergman
needs delete permission on the warehouse prefix — the docs ship
least-privilege IAM policy templates, because "give it `s3:*`" is how orphan
deletion becomes an incident.

---

## 10. Companion catalog: Rustberg

[Rustberg](https://github.com/hupe1980/rustberg) is the sister project: an
authenticated, Cedar-policy-controlled Iceberg REST endpoint, shipped — like
Bergman — as one crate, library-first, with a feature-gated CLI binary.
Rustberg's design explicitly excludes maintenance ("compaction is data
rewriting… a different availability shape: minutes-to-hours of held work
against a server whose whole story is microsecond decisions") and describes
what a maintenance system driving it gets: compare-and-swap commits, an
authorization decision per operation, and an audit record naming the
principal. Bergman is that maintenance system. The pairing is a deliberate
division of labor, not a bundle: each works fully without the other.

### 10.1 The contract is the wire

Bergman reaches Rustberg the way it reaches every catalog — as an Iceberg
REST client via `iceberg-catalog-rest`. No private API, no shared types, no
version coupling beyond the REST spec both already implement. Consequences:

- **Commits compose.** Bergman's rewrite/expire commits arrive as ordinary
  `(requirements, updates)` and land on Rustberg's compare-and-swap
  registries — the same optimistic-concurrency model §6 already assumes, so
  a conflicting foreground write costs Bergman a replan, never Rustberg a
  lost update.
- **Capabilities are negotiated, not assumed.** Rustberg advertises the
  honest intersection through `/v1/config`; Bergman feature-detects from
  `endpoints` and skips (with a named reason) what a deployment doesn't
  offer — e.g. read-only federated mounts, where maintenance would be a
  write into somebody else's catalog and *should* be refused.
- **Snapshot expiration is catalog-mediated by design.** `expireSnapshots`
  arrives as ordinary `TableUpdate`s, which Rustberg serves natively — so
  Phase 1 works against Rustberg unchanged.

### 10.2 Governed maintenance (the joint story neither has alone)

Run against Rustberg, Bergman becomes *governed* maintenance:

- **A named principal.** Bergman authenticates as its own identity (e.g.
  `svc-bergman` via OIDC or API key); Cedar policies grant it `Read`/`Update`
  exactly on the subtrees its policies cover. Every maintenance commit is
  authorized per-operation and lands in Rustberg's audit trail attributed to
  the principal — the answer to "what changed my table?" that Spark-job
  maintenance never gives.
- **Joined audit trails.** Bergman sends `X-Request-Id` on every catalog
  call using the same id it writes to its own audit log (kept within
  Rustberg's accepted token alphabet and 128-char bound), so one grep joins
  Bergman's "why" (policy rule, trigger, plan) to Rustberg's "who was
  allowed to" (principal, decision, matched policies).
- **No ambient storage credential.** With `vended-credentials`, Bergman
  holds no storage keys at all: Rustberg vends short-lived, table-prefix-
  scoped credentials (STS / GCS token exchange / Azure SAS), write-scoped
  exactly when Bergman may `Update`. This honors Rustberg's stated
  precondition that it be the only path to its backends — maintenance
  included. Remote signing also works but pays a round trip per object;
  for bulk Parquet rewriting, vending is the right delegation form.
- **Restricted tables fail safe, by construction.** Compaction must read
  *every* row — rewriting a table through a row filter or column mask would
  silently destroy data. Rustberg refuses to vend credentials or sign for a
  caller whose matching permits carry a restriction, so a mis-scoped
  Bergman principal cannot half-read a table into a rewrite: the credential
  is refused, Bergman skips the table and reports the named reason. The
  deployment rule is therefore simple and checkable: the maintenance
  principal's permits carry no `@row_filter`/`@column_mask`, or the table
  is skipped.
- **Orphan removal stays inside the fence.** Bergman's location-containment
  rules (§5.4) and Rustberg's segment-wise containment enforce the same
  boundary from both sides; a vended credential scoped to the table prefix
  makes listing and deleting outside it not just checked but *impossible*.
  One open detail is pinned in §14: vended write credentials must include
  delete permission on the prefix for orphan deletion to run credential-less.

### 10.3 Development leverage

`rustberg --dev --insecure-http` is a real REST catalog in one command with
~45 ms cold start. Bergman's integration tests use it as the default catalog
fixture — full-fidelity REST semantics (CAS conflicts, capability
negotiation, auth) at unit-test speed, no containers — alongside the
cross-engine round-trip gate (§12), which still runs against
Spark-written tables. Symmetrically, Bergman joins Rustberg's client
conformance matrix next to PyIceberg, DuckDB, and Trino: each project is the
other's CI client.

### 10.4 Shared engineering bar

The two crates are a family and hold the same line, so a contributor moving
between them relearns nothing:

- One crate, `[lib]` + `[[bin]]` with `required-features = ["cli"]`;
  features are for optional dependencies, separate crates are for separate
  release cadence — and there is one.
- Every optional dependency behind a `dep:`-prefixed feature, so the
  feature surface is exactly the declared list; CI builds the matrix
  including **each feature alone**, not only `--all-features`.
- Edition 2024; MSRV declared as the floor the dependency tree actually
  imposes (`iceberg` 0.10.x ⇒ 1.94 today) and checked against that real
  toolchain.
- `#![forbid(unsafe_code)]`; `cargo deny` over advisories, licenses, bans,
  sources; static musl release builds; Linux/macOS/Windows in CI.
- Storage configuration speaks Iceberg property names (`s3.endpoint`,
  `gcs.project-id`, …) through `FileIO`/OpenDAL — the same vocabulary in
  both config files, and the same one every other Iceberg tool uses.
- Benchmarks live in the library (`bergman bench`), measured on every PR,
  gated on order-of-magnitude regressions with deliberately loose ceilings
  — a gate that flakes gets disabled and then protects nothing.

---

## 11. Non-goals

- SQL serving, BI, or any query API — Bergman writes tables, it doesn't
  answer questions about their contents.
- Distributed shuffle / Ballista. Partition-grained parallelism covers the
  target workloads; tables needing cross-node shuffle for a single sort
  should use Spark — and that's fine.
- Hive-table or Delta/Hudi support. Iceberg only.
- Being a catalog, or replacing catalog-side policy stores (Polaris table
  policies): Bergman should eventually *read* catalog-defined maintenance
  policies where the REST spec exposes them, not compete with them.
- CDC/ingestion. Maintenance only.

---

## 12. Roadmap

**Phase 0 — trust before writes. ✅ Implemented.** `bergman inspect` +
`bergman plan`: catalog discovery, the metadata-only health analyzer, the
policy engine with layered resolution and `policy explain` provenance,
planners for all four operations, `policy lint` (offline, for CI). Read-only,
immediately useful, and it validated the `iceberg-rust` integration depth
risk-free — which is how §1's corrections were found.

**Phase 1 — metadata operations. ✅ Implemented, minus manifest rewrite.**
Snapshot expiration via upstream's action, plus the file cleanup upstream
delegates; orphan-file removal with the full §5.4 safety model, on Bergman's
own object-store layer (upstream `FileIO` cannot list); JSONL audit trail;
the `MaintenanceObserver` hook. REST catalog + S3/GCS/Azure/local fs. The
library API ships here and the CLI is written against it, so embeddability is
enforced by construction rather than retrofitted. **Manifest rewrite is
implemented too**, on Bergman's own commit layer (§4.2).

**Phase 2 — compaction. ✅ Implemented.** Bin-pack with positional- and
equality-delete application (upstream's scan applies both), partition-grained
commits with pre-commit re-validation, and the delete-file retirement rule:
a delete file is retired only when every data file it applies to is inside the
group. Not gated on #2185 after all — §1 explains why. Still absent: the sort
stage (output is bin-packed, `sort` is accepted and reported but not applied),
z-order, memory budget and spill.

**Phase 2b — daemon.** `bergman daemon`: cron and event triggers, Prometheus
`/metrics`, maintenance windows. Deliberately after the operations rather
than alongside them: a scheduler for work that does not yet run is a
scheduler nobody can test.

**Phase 3 — hardening.** Chaos tests (kill -9 mid-run, concurrent-writer
fuzzing against Spark and Flink writers); cross-engine round-trip gate;
Glue/HMS catalogs.

**Phase 4 — scale & intelligence.** Stateless replica sharding; z-order;
event-trigger integrations (Lakekeeper CloudEvents webhook); cost-based
prioritization ("which table's compaction buys the most scan improvement
per byte written"); policy recommendations from observed access patterns;
deletion-vector (v3) native handling as upstream lands it; optional
`bergman-py` bindings if Phase 2 proves out (§14.4).

Every phase ships with correctness tests against reference tables written by
Spark and read back by Spark/Trino after Bergman maintenance — **cross-engine
round-trip is the release gate**, not unit tests alone.

---

## 13. Risks — stated honestly

1. **`iceberg-rust` maturity is the load-bearing wall — and it is currently
   load-bearing against us.** As §1 documents, no upstream API removes a data
   file, and the commit API is crate-private, so the entire data plane is
   blocked on #2185/#2752 landing. This is the project's single largest risk
   and it is not hypothetical: it is the present state.
   Mitigation, in force today: Phase 0/1 depend only on what exists and are
   implemented; compaction and manifest rewriting are planned, reported, and
   marked `Blocked` with the issue number rather than silently omitted, so the
   product is honest at every stage; and the value that sits *above* the
   commit — policy layering, triggers, health analysis, the safety model,
   audit — is complete and independent of when upstream lands. If #2185 stalls
   indefinitely, the fallback is to contribute it, which is a better use of
   effort than routing around it.
2. **Equality-delete application at scale is genuinely hard** (the 20GB
   eq-delete benchmark that OOMs Spark). Mitigation: file-group sizing
   against a memory budget, spill, and refusing (with a clear diagnostic)
   groups that exceed it — degraded loudly beats OOM silently.
3. **Orphan deletion carries data-loss risk by nature.** Mitigation is the
   entire §5.4 safety model plus shipping it *last*, after the audit and
   testing muscle exists.
4. **Overlap with nimtable/iceberg-compaction.** It is Apache-2.0 and
   good; depending on it is tempting. Decision: Bergman owns its executor
   (the crate's scope is compaction-only, its API is young, and the
   executor is where Bergman's memory/commit contracts live), while
   treating it and RisingWave's published results as the benchmark bar:
   **Bergman's compaction must be within 10% of iceberg-compaction's
   throughput on the same hardware, or we adopt it as a dependency and
   contribute instead.** Revisit at Phase 2 start.
5. **Catalogs are already absorbing metadata maintenance.** Not
   hypothetical: Lakekeeper ships expire-snapshots, orphan removal, and
   metadata cleanup built-in with event-driven scheduling; S3 Tables
   auto-compacts; Polaris defines policy entities. Consequences Bergman
   accepts openly: (a) the durable moat is the **data plane** — compaction,
   delete-file retirement, clustering — which Lakekeeper explicitly leaves
   to external engines and signals via CloudEvents; (b) metadata ops
   (Phase 1) are table stakes for the *other* catalogs (REST/Glue/HMS
   fleets, Polaris, Nessie) and for teams wanting one auditable tool across
   heterogeneous catalogs; (c) the policy layer defers to catalog-owned
   policies rather than fighting them. Bergman's posture toward Lakekeeper
   is complement, not competitor: be the compactor its events trigger.
6. **Upstream may absorb compaction itself.** `iceberg-rust` has an open
   epic for Rust-based compaction (#624). If a maintained `RewriteDataFiles`
   equivalent lands upstream, Bergman swaps its executor internals for it —
   the concept's durable value (policies, triggers, safety model, audit,
   orphan reachability, operations product) sits *above* that layer by
   design. Same posture as with nimtable: adopt and contribute rather than
   duplicate, once something upstream is demonstrably as good.

---

## 14. Open questions

1. ~~Should Phase 1 snapshot expiration delete data files itself or defer all
   file deletion to the orphan scanner?~~ **Settled** (§1): upstream deletes
   nothing, so Bergman owns deletion regardless. It is implemented once, and
   expiration opts in per policy (`snapshots.delete_files`) or leaves the
   files for the scanner. One deleter, one safety model.
2. Policy layering vs. Polaris/REST catalog policy entities — read them as
   another layer below local rules once the spec stabilizes?
3. Minimum viable v3 (deletion vectors, row lineage) support level for
   Phase 2, given upstream flux?
4. Python bindings (pyo3): PyIceberg has expire-snapshots but no native
   compaction — a `bergman`-backed compactor for Python would fill a real
   gap. Separate `bergman-py` packaging crate post-Phase 2, or leave it to
   downstream?
5. Event-trigger transport: is a webhook receiver for Lakekeeper
   CloudEvents enough for v1 daemon mode, or is a pull-based fallback
   (metadata-log diffing) needed for catalogs without events? Rustberg
   currently emits no commit events — worth proposing there (a webhook is
   cheap and fits its audit pipeline) rather than polling around it.
6. Orphan deletion under vended credentials: does the vended write
   credential (STS inline policy / SAS permissions) include delete on the
   table prefix everywhere? If not, orphan removal needs either a direct
   credential (weakening §10.2's no-ambient-secret story for that one op)
   or a catalog-side delete API.
7. Single-process pairing: an adapter implementing `iceberg::Catalog` over
   Rustberg's embedded `Session` would let one Rust binary host catalog and
   maintenance together with no HTTP hop. Feasible (the trait's
   implementor-side API is open); worth building only when a real embedder
   asks.

---

## 15. Name

Bergman: *berg* as in iceberg; Bergman as in the director — long takes,
minimal cast, no unnecessary spectacle. Maintenance should be the same.
