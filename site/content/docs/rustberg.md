+++
title = "With Rustberg"
description = "Bergman and Rustberg: a maintenance engine and a governing catalog, designed to pair and fully usable apart."
weight = 21
+++
[Rustberg](https://github.com/hupe1980/rustberg) is the sister project: an
authenticated, Cedar-policy-controlled Iceberg REST catalog. Like Bergman it is
one crate, library-first, with a feature-gated CLI binary.

The pairing is a deliberate division of labour, not a bundle. Each works fully
without the other.

## Why the split

Rustberg's own design excludes maintenance, and says why: *"compaction is data
rewriting… a different availability shape: minutes-to-hours of held work against
a server whose whole story is microsecond decisions."* It then describes what a
maintenance system driving it gets — compare-and-swap commits, an authorization
decision per operation, and an audit record naming the principal.

Bergman is that maintenance system.

This is not an idiosyncratic split. Lakekeeper reached the same boundary
independently, shipping expire-snapshots and orphan removal while emitting
CloudEvents so an *external* engine can compact. Apache Polaris took the question
up in its own issue #538 and concluded the same: the catalog manages metadata and
enforces permissions; the data plane belongs elsewhere.

## The contract is the wire

Bergman reaches Rustberg the way it reaches every catalog — as an ordinary
Iceberg REST client. No private API, no shared types, no version coupling beyond
the REST specification both already implement.

```toml
[[catalogs]]
name      = "prod"
kind      = "rest"
uri       = "https://rustberg.internal/catalog"
token_env = "BERGMAN_CATALOG_TOKEN"
```

That is the whole integration.

## What the pairing adds

### A named principal

Bergman authenticates as its own identity. Cedar policies grant it `Read` and
`Update` on exactly the subtrees its own rules cover:

```cedar
permit(
  principal == Rustberg::User::"svc-bergman",
  action    in [Rustberg::Action::"Read", Rustberg::Action::"Update"],
  resource  in Rustberg::Namespace::"acme\u{1F}analytics"
);
```

Every maintenance commit is then authorized per operation and lands in Rustberg's
audit trail attributed to that principal — the answer to "what changed my table?"
that Spark-job maintenance never gives.

### No ambient storage credential

With credential vending, Bergman holds no storage keys at all: Rustberg vends
short-lived, table-prefix-scoped credentials, write-scoped exactly when Bergman
is permitted to `Update`. That honours Rustberg's stated precondition — that it
be the only path to its backends — for maintenance as well as for queries.

### Restricted tables fail safe by construction

This is the sharpest part of the pairing.

Compaction must read **every row**. Rewriting a table through a row filter would
silently destroy the rows the filter hides. Rustberg refuses to vend credentials
or sign requests for a caller whose matching permits carry a `@row_filter` or
`@column_mask` — so a mis-scoped Bergman principal *cannot* half-read a table
into a rewrite. The credential is refused, Bergman skips the table, and the
reason is reported.

The deployment rule follows: the maintenance principal's permits carry no row
filter or column mask, or the table is skipped. Nothing enforces this by
convention; the credential simply does not exist.

### Joined audit trails

Bergman writes its own JSONL trail recording *why* — the policy rule, the
trigger, the measurement. Rustberg records *who was allowed to* — the principal,
the decision, the matched Cedar policies. One `X-Request-Id` joins them, so a
single grep answers a question neither log answers alone.

## For development

`rustberg --dev --insecure-http` brings up a real REST catalog in one command,
with a ~45 ms cold start. It is Bergman's default integration-test fixture:
full-fidelity REST semantics — compare-and-swap conflicts, capability
negotiation, authentication — at unit-test speed and with no containers.

The relationship runs both ways: Bergman joins Rustberg's client conformance
matrix alongside PyIceberg, DuckDB and Trino. Each project is the other's CI
client.

## A shared engineering bar

The two crates hold the same line, so a contributor moving between them relearns
nothing:

- one crate, `[lib]` + `[[bin]]` with `required-features = ["cli"]`
- every optional dependency behind a `dep:`-prefixed feature, and CI builds
  **each feature alone** rather than only `--all-features`
- edition 2024, with the MSRV declared as the floor the dependency tree actually
  imposes and checked against that real toolchain
- `#![forbid(unsafe_code)]`, `cargo deny`, static musl release builds
- storage configured with Iceberg property names (`s3.endpoint`,
  `gcs.project-id`) — the same vocabulary in both config files, and the same one
  every other Iceberg tool uses

## Licensing

Rustberg is Apache-2.0. Bergman is Apache-2.0 OR MIT. No conflict in either
direction.
