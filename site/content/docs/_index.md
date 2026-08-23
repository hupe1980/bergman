+++
title = "Documentation"
description = "How Bergman measures table health, resolves policy, plans maintenance, and decides — seven checks deep — whether a file may be deleted."
sort_by = "weight"
template = "docs-section.html"
page_template = "docs-page.html"
+++

Bergman keeps Apache Iceberg tables healthy without a JVM: one crate, one
binary, and a library any Rust service can embed.

If you have five minutes, start with [Getting started](@/docs/getting-started.md)
— the first command reads your catalog and changes nothing. If you are
evaluating whether Bergman fits, read [Status](@/docs/status.md) first: it
states plainly what executes today, what upstream owes, and what is not built,
and why.

Before you let it delete anything, read [Orphan files](@/docs/orphans.md). That
page is the safety model, and it is the one page worth reading twice.
