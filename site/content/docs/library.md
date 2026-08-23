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
setup, the cloud backends you are not using — and the query engine.

## What each feature costs you

| Feature | Default | Carries |
|---|:--:|---|
| `cli` | ✅ | The binary, terminal rendering, logging setup, signal handling |
| `catalog-rest` | ✅ | The Iceberg REST catalog client |
| `compaction` | ✅ | Data-file rewriting — **and DataFusion**, which applies equality deletes as a hash anti-join |
| `storage-s3` / `-gcs` / `-azure` | ✅ via `storage-all` | Cloud object stores |
| `metrics` | — | Prometheus metrics and the `/metrics` endpoint |

`compaction` is the one worth a decision. It is the only operation that reads
and writes *data*, and therefore the only one that needs a query engine — see
[Compaction](@/docs/compaction.md#query-engine) for why equality deletes make
that unavoidable. It adds ~70 crates and a noticeable amount of compile time.

If you are embedding Bergman for **metadata maintenance** — the Lakekeeper
shape: expire snapshots, re-pack manifests, remove orphans — leave it off and
carry no query engine at all:

```toml
bergman = { version = "0.1", default-features = false, features = ["catalog-rest", "storage-s3"] }
# -> expire, manifests, orphans. No DataFusion.
```

The feature gates the **operation**, never a second implementation of it. A
build without it has no compaction rather than a slower compaction, so there is
only ever one executor to keep correct. Planning stays feature-independent —
`plan()` still reports that a table needs compacting, and `run()` reports the
operation as `refused` naming the feature to rebuild with, rather than silently
omitting it.

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

Every one of these has a scoped sibling — `inspect_matching(pattern)`,
`plan_matching(pattern)`, `plan_tables(&[table])` — and they scope the
*examination*, not the output. A table the pattern excludes is never read, which
is what makes "what is wrong with this one namespace" cost a namespace's
metadata rather than the warehouse's.

A plan may carry **notes**: work the policy asked for that a table cannot
receive, with the reason — a [format v3 table](@/docs/compaction.md#format-v3),
a non-Parquet one, a partition under a superseded spec. Surface them, or those
tables read as healthy:

```rust
for (table, note) in plan.notes() {
    tracing::warn!(%table, %note);
}
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
use bergman::obs::{MaintenanceObserver, OperationContext};
use bergman::plan::OperationResult;

#[derive(Debug)]
struct Metrics(prometheus_client::registry::Registry);

#[async_trait::async_trait]
impl MaintenanceObserver for Metrics {
    async fn operation_finished(&self, ctx: OperationContext<'_>, result: &OperationResult) {
        self.record(ctx.table, ctx.kind, result);
    }
}
```

`OperationContext` carries the run id, the table, the operation, **the policy
rule that matched**, and the reason the trigger fired — everything an audit
record or a metric label needs.

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
async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
    ctx.kind != OperationKind::RemoveOrphans || self.approved(ctx.table).await
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

## Running on a schedule

`Daemon` wraps the same engine and calls back after each cycle. It has no
opinion about output — returning the report rather than logging it is what keeps
that true.

```rust
use bergman::sched::{Daemon, DaemonConfig};

let daemon = Daemon::new(Arc::new(bergman), DaemonConfig {
    interval: Duration::from_secs(3600),
    max_cycles: None,
})?;

daemon
    .run(
        |cycle| tracing::info!(n = cycle.number, trigger = %cycle.trigger, "cycle done"),
        shutdown_signal(),
    )
    .await?;
```

`shutdown` is any future; the loop checks it before each sleep and prefers it
over a ready timer, so a stop is noticed promptly rather than after several
cycles.

### Reacting to changes

Bergman owns the trigger and the debounce; the transport stays yours. Whatever
already receives events in your deployment — a NATS subscriber, a Kafka
consumer, a webhook — calls `notify`:

```rust
use bergman::sched::channel;

let (events, stream) = channel(Duration::from_secs(30));

tokio::spawn(async move {
    while let Some(msg) = subscription.next().await {
        events.notify(table_from(msg));   // never blocks, never fails
    }
});

daemon.run_with_events(on_cycle, Some(stream), shutdown_signal()).await?;
```

`notify` returns whether the notification was queued. A full queue drops rather
than blocking: a maintenance engine must never be able to stall the thing
notifying it, and the *n*-th notification for one table says nothing the first
did not.

`Bergman::plan_tables` is the same thing without a daemon — plan a named subset
and run it.

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
