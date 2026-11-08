+++
title = "Documentation"
description = "How Bergman measures table health, resolves policy, plans maintenance, and decides — eight checks deep — whether a file may be deleted."
sort_by = "weight"
template = "docs-section.html"
page_template = "docs-page.html"
+++

Bergman keeps Apache Iceberg tables healthy without a JVM: one crate, one
binary, and a library any Rust service can embed.

Start with [Getting started](@/docs/getting-started.md) — the first command
reads your catalog and changes nothing. To judge fit, read
[Status](@/docs/status.md): what executes and what is refused. Before enabling
deletion, read [Orphan files](@/docs/orphans.md), the safety model.
