+++
title = "Library"
description = "Embedding Bergman: the API contract, the observer hook, and what the library deliberately refuses to do."
weight = 9
+++
Bergman is a library first. The `bergman` binary is a thin consumer of the same
public API you get, which is how that API stays sufficient — anything the CLI can
do, an embedder can do.

```toml
[dependencies]
bergman = { version = "0.1", default-features = false, features = ["catalog-rest", "storage-s3"] }
```

`default-features = false` drops the CLI, the terminal renderer, the logging
setup and the cloud backends you are not using.

## The shape

```rust
use bergman::{Bergman, policy::Config};

let config = Config::from_path("bergman.toml")?;
let bergman = Bergman::new(config).await?;

// Reads only.
let health = bergman.inspect().await?;
let plan   = bergman.plan().await?;

// The first call that writes anything.
let report = bergman.run(&plan).await?;
```

`Config` is an ordinary Rust value. Nothing forces you to have a file at all:

```rust
use bergman::policy::{Config, Rule, TableSettings, SnapshotSettings};

let config = Config {
    catalogs: vec![catalog],
    rules: vec![Rule {
        pattern: "prod.**".into(),
        settings: TableSettings {
            snapshots: Some(SnapshotSettings {
                max_age: Some(Duration::from_secs(7 * 86400)),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    }],
    ..Default::default()
};
```

## The contract

These are guarantees, not implementation details. The CLI is where each one's
opposite lives.

**No global state.** The library installs no logger, no tracing subscriber and no
signal handler, and reads no configuration file on its own initiative. It emits
[`tracing`](https://docs.rs/tracing) spans and events; what listens is yours.

**Bring your own runtime.** Every entry point is a plain `async fn` on the
caller's runtime. Concurrency limits are parameters (`limits.max_parallel_tables`),
never process-wide statics.

**Planning is pure.** `plan()` performs no writes and no deletions, so dry-run is
not a mode to remember — it is what happens when you stop before `run()`. The
same `MaintenancePlan` that `plan()` returns is what `run()` executes.

**Types are plain and serializable.** `TableHealth`, `MaintenancePlan`,
`EffectivePolicy` and `RunReport` all implement `Serialize`, so an embedder can
store a plan, diff two of them, or ship one over a wire without reaching into
Bergman's internals.

**Semver honesty.** Pre-1.0, breaking changes land in minor versions with a
migration note in the changelog. The CLI surface stabilizes before the library
API does, and this sentence exists so nobody has to guess.

## Observers {#observers}

The extension point, in place of a plugin system. Every method has a default
no-op body, so you override only what you care about and a new callback is not a
breaking change.

```rust
use bergman::obs::MaintenanceObserver;
use bergman::plan::{OperationKind, OperationResult};
use bergman::policy::TableRef;

#[derive(Debug)]
struct Metrics(prometheus_client::registry::Registry);

#[async_trait::async_trait]
impl MaintenanceObserver for Metrics {
    async fn operation_finished(
        &self,
        table: &TableRef,
        kind: OperationKind,
        result: &OperationResult,
    ) {
        self.record(table, kind, result);
    }
}
```

```rust
let bergman = Bergman::builder(config)
    .with_observer(Arc::new(observers))
    .build()
    .await?;
```

### Approval gates

`operation_starting` returning `false` **vetoes** the operation, which is then
reported as `Refused` — an outcome that needs attention, not a silent skip:

```rust
async fn operation_starting(&self, table: &TableRef, kind: OperationKind) -> bool {
    kind != OperationKind::RemoveOrphans || self.approved(table).await
}
```

This is how you require a human, a ticket, or a policy service to sign off on
deletions without Bergman knowing anything about how that decision is made.

### Deletion manifests

`deleting_files` is called with the **complete list before the first deletion**,
which is what makes an audit trail survive a crash halfway through. Use it to
mirror deletions into your own system of record.

### Combining observers

```rust
use bergman::obs::Observers;

let observers = Observers::new()
    .with(Arc::new(AuditObserver::new(JsonlSink::open("audit.jsonl")?, run_id)))
    .with(Arc::new(metrics))
    .with(Arc::new(approval_gate));
```

Every observer is consulted on `operation_starting` and any one may veto —
deliberately without short-circuiting, so an approval gate's behaviour does not
depend on the order it was registered in.

## Who this is for

Three shapes, each with a precedent:

**Rust catalogs.** [Lakekeeper](https://docs.lakekeeper.io/docs/latest/table-maintenance/)
ships expire-snapshots and orphan removal but deliberately does not compact,
emitting CloudEvents so an external engine can. [Rustberg](@/docs/rustberg.md)
draws the same boundary. Bergman is designed to be the engine both leave room
for.

**Streaming writers and CDC sinks.** RisingWave proved the embedded-compactor
pattern by building one into their engine. Anyone writing Iceberg from Rust
should not have to.

**Lakehouse platforms and control planes.** nimtable embeds
`iceberg-compaction` the same way; Bergman offers the full operation set —
expiration, cleanup, orphans — rather than compaction alone.

## Errors

One error type, and the most useful thing on it is what to *do*:

```rust
match bergman.run(&plan).await {
    Err(e) if e.is_replan() => { /* the table moved; rebuild the plan */ }
    Err(e) => match e.disposition() {
        Disposition::Retry    => { /* transient; back off */ }
        Disposition::Terminal => { /* will fail the same way forever */ }
        Disposition::Replan   => unreachable!(),
    },
    Ok(report) => { /* per-operation outcomes inside */ }
}
```

A conflict is `Replan`, never `Retry`. Re-submitting a commit computed against a
table that has since moved is how deleted rows come back — see
[Architecture](@/docs/architecture.md#commits).
