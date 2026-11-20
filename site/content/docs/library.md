+++
title = "Library"
description = "Embedding Bergman: the API contract, the observer hook, and what the library deliberately refuses to do."
weight = 9
+++
The `bergman` binary is a thin consumer of the same public API — anything the
CLI can do, an embedder can do.

```bash
cargo add bergman --no-default-features --features catalog-rest,storage-s3
```

## What each feature costs you

| Feature | Default | Carries |
|---|:--:|---|
| `cli` | ✅ | The binary, terminal rendering, logging setup, signal handling |
| `catalog-rest` | ✅ | The Iceberg REST catalog client |
| `compaction` | ✅ | Data-file rewriting — **and DataFusion**, which applies equality deletes as a hash anti-join |
| `storage-s3` / `-gcs` / `-azure` | ✅ via `storage-all` | Cloud object stores |
| `metrics` | ✅ | Prometheus metrics and the daemon's HTTP surface: `/metrics`, `/health`, `/status`, `/events` |

`compaction` is the only operation that reads and writes *data*, and the only
one needing a query engine ([why](@/docs/compaction.md#query-engine)). For
metadata maintenance only — expire, manifests, orphans — leave it off, as in the
`cargo add` above, and carry no DataFusion. Planning is feature-independent: a
build without it still plans compaction, and `run()` reports the operation as
`refused`, naming the feature to rebuild with.

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

Scoped siblings — `inspect_matching(pattern)`, `plan_matching(pattern)`,
`plan_tables(&[table])` — scope the *examination*: an excluded table is never
read. To scope *which operations* run, filter the plan with
`MaintenancePlan::retain_operations` (what `bergman run --only` calls):

```rust
let mut plan = bergman.plan().await?;
plan.retain_operations(&[OperationKind::RemoveOrphans]);
let report = bergman.run(&plan).await?;
```

Any subset is safe; per-table order is preserved.

A plan may carry **notes**: work the policy asked for that a table cannot
receive, with the reason — an [encrypted table](@/docs/compaction.md#what-is-refused),
a non-Parquet one, a partition under a superseded spec. Surface them:

```rust
for (table, note) in plan.notes() {
    tracing::warn!(%table, %note);
}
```

`Config` is an ordinary Rust value; no file is needed:

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

**No global state.** No logger, tracing subscriber or signal handler is
installed, and no configuration file is read unasked. The library emits
[`tracing`](https://docs.rs/tracing) spans and events.

**Bring your own runtime.** Every entry point is a plain `async fn`. Every
concurrency limit is a parameter: `limits.max_parallel_tables` (tables in
flight), `limits.io_concurrency` (object-store requests per operation) and
`limits.compaction_parallelism` (CPU width of a rewrite).

**No background threads**, including for credentials — a token is renewed by
the request that finds it due ([Long-lived processes](#long-lived)).

**Planning is pure.** `plan()` writes and deletes nothing; dry-run is stopping
before `run()`, which executes exactly the plan it is given.

**Types are serializable.** `TableHealth`, `MaintenancePlan`, `EffectivePolicy`
and `RunReport` implement `Serialize`.

**Semver.** Pre-1.0, breaking changes land in minor versions with a migration
note in the changelog.

## Observers {#observers}

The extension point. Every method has a default no-op body; override only what
you need.

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

`OperationContext` carries the run id, the table, the operation, the matched
rule, and the trigger's reason.

```rust
let bergman = Bergman::builder(config)
    .with_observer(Arc::new(observers))
    .build()
    .await?;
```

### Approval gates

`operation_starting` returning `false` **vetoes** the operation, reported as
`Refused`:

```rust
async fn operation_starting(&self, ctx: OperationContext<'_>) -> bool {
    ctx.kind != OperationKind::RemoveOrphans || self.approved(ctx.table).await
}
```

### Deletion manifests

`deleting_files` receives the **complete list before the first deletion**, so a
record survives a crash mid-deletion.

### Combining observers

```rust
use bergman::obs::Observers;

let observers = Observers::new()
    .with(Arc::new(AuditObserver::new(JsonlSink::open("audit.jsonl")?, run_id)))
    .with(Arc::new(metrics))
    .with(Arc::new(approval_gate));
```

Every observer is consulted on `operation_starting`, without short-circuiting,
and any one may veto.

## Running on a schedule

`Daemon` wraps the same engine and calls back with each cycle's report.

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

`shutdown` is any future; it is checked before each sleep and wins over a ready
timer.

### Reacting to changes

Bergman owns the trigger and the debounce; the transport is yours. Whatever
receives events — a NATS subscriber, a Kafka consumer, a webhook — calls
`notify`:

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

`notify` returns whether the notification was queued; a full queue drops rather
than blocks. Without a daemon, `Bergman::plan_tables` plans a named subset.

### Long-lived processes {#long-lived}

A process that outlives its catalog token needs nothing extra. Each catalog has
one token source shared by both clients — the read client
(`iceberg-catalog-rest`, through its `AuthManager` hook) and Bergman's commit
client. It renews **on demand**: the first request finding the token within a
minute of expiry exchanges the credential again, and concurrent requests wait
for that one exchange.

```rust
loop {
    // No credential step: a due token is renewed by whichever request needs it.
    let plan = bergman.plan().await?;
    let report = bergman.run(&plan).await?;
    // ...
}
```

A failed renewal fails the request that needed it, carrying the token
endpoint's answer; the next request retries. A static `token_env` token is
presented as given.

`CatalogConfig::connect()` returns both clients together as
`CatalogConnection { client, committer, policy }`.

## Errors

One error type, which says what to *do*:

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

A conflict is `Replan`, never `Retry`: re-submitting a commit computed against a
moved table can resurrect deleted rows — see
[Architecture](@/docs/architecture.md#commits).
