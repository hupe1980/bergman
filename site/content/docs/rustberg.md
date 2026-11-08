+++
title = "With Rustberg"
description = "Bergman and Rustberg: a maintenance engine and a governing catalog, designed to pair and fully usable apart."
weight = 21
+++
[Rustberg](https://github.com/hupe1980/rustberg) is the sister project: an
authenticated, Cedar-policy-controlled Iceberg REST catalog. Rustberg leaves
maintenance to an external engine; Bergman is that engine. Each works fully
without the other.

## The contract is the wire

Bergman reaches Rustberg as an ordinary Iceberg REST client — no private API, no
shared types:

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

Bergman authenticates as its own identity, granted `Read` and `Update` on the
subtrees its rules cover:

```cedar
permit(
  principal == Rustberg::User::"svc-bergman",
  action    in [Rustberg::Action::"Read", Rustberg::Action::"Update"],
  resource  in Rustberg::Namespace::"acme\u{1F}analytics"
);
```

Every maintenance commit is authorized per operation and lands in Rustberg's
audit trail under that principal.

### No ambient storage credential

With credential vending, Bergman holds no storage keys: Rustberg vends
short-lived, table-prefix-scoped credentials, write-scoped when Bergman may
`Update`.

### Restricted tables fail safe

Compaction must read **every row**; rewriting through a row filter would destroy
the rows it hides. Rustberg refuses to vend credentials or sign requests for a
caller whose permits carry a `@row_filter` or `@column_mask`, so a mis-scoped
principal cannot half-read a table into a rewrite; the refusal is reported. Give the maintenance principal no row filter or
column mask.

### Joined audit trails

Bergman's JSONL trail records *why* — rule, trigger, measurement. Rustberg's
records *who was allowed to* — principal, decision, matched Cedar policies.
Bergman sends its **run id** as `X-Request-Id` on every commit it authors, the
same id its own audit records carry, and `X-Bergman-Operation` names the
operation. One grep joins the two logs.

**Snapshot expiration sits outside the join**: it commits through upstream's
action and carries no header ([Status](@/docs/status.md#where-the-seam-falls)).
Those commits are still authorized and audited under the same principal.

## Licensing

Rustberg is Apache-2.0. Bergman is Apache-2.0 OR MIT.
