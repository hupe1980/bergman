# AGENTS.md — bergman

A Rust-native maintenance engine for Apache Iceberg: compaction, snapshot
expiration, manifest rewriting, orphan-file removal. **One crate, one binary, no
JVM.** Library-first — the CLI is a thin consumer of the same public API.

`README.md` is the product description and `site/content/docs/**` the operator
guides; neither is restated here. This file is the standing facts and traps an
agent needs before touching code.

## Ground rules

- **Breaking changes are fine.** Pre-1.0 and unreleased: hard cuts, no backward
  compatibility. Breaking changes land in **minor** versions with a migration
  note in `CHANGELOG.md`.
- **Some working directories are gitignored**, so a session that edited only
  those leaves `git status` clean. Never conclude from a clean tree that nothing
  happened, and say so explicitly in a handover — the git history will not.
- **The published tree is self-contained.** Nothing in it — code, doc comments,
  `README.md`, `site/`, or this file — may point at a path or an identifier that
  only resolves in an ignored directory. On docs.rs that renders as a reference
  the reader cannot open. Say what you mean, or say nothing.
- **Docs and comments state current truth only.** No changelogs, no "used to", no
  pointers at open work. `CHANGELOG.md` is the history.
- **Every number is asserted by a test and names it.** A number no test pins has
  already rotted. If you cannot name the test, write "not measured" and mean it.
  Compaction throughput against the competing Rust engines is the live example:
  there is a stated bar and no measurement, so nothing in the tree claims a
  number — an absent claim is the honest form, not an omission to be filled in
  with an estimate.
- **Every claim about somebody else's software carries the day it was read**, and
  a pull request's state is `merged`, not `closed`. Reading `closed` as merged
  has produced several confidently wrong claims here about what `iceberg-rust`
  does and does not have. Verify from the API or the vendored source, which is on
  disk under `~/.cargo/registry/src/*/iceberg-<version>/`.
- **A claim that something is absent is a claim about a command's output.** Put
  the command in the document. `grep` is case-sensitive and an absence is the one
  result worth confirming twice, because a false positive is invisible.

## Build and test

`just ci` is the gate: `lint check-all test docs site`.

```bash
just test         # cargo test --all-features
just lint         # fmt --check + clippy --all-features --all-targets -D warnings
just check-all    # no-default-features, all-features, and every feature alone
just ci           # the gate
```

**`just ci` is not CI.** The workflow runs eight jobs and three are not in the
recipe:

| job | recipe | in `just ci`? |
|---|---|:--:|
| `msrv` | `just msrv` | ❌ |
| `deny` | `just deny` | ❌ |
| `portability` | — | ❌ |

Run `just msrv` and `just deny` by hand whenever the lock file moves. `deny`
checks advisories on the clock rather than on a push, so it turns red on a tree
nobody touched — that is how the rustls TLS 1.3 advisory was found.

`just cross-engine` is in neither: it needs `uv` and fetches PyIceberg 0.12.0,
which writes v2 tables, lets Bergman compact them and rewrite their manifests,
and reads them back — plus a v3 table Bergman compacted. It is the only check
whose reader shares no code with Bergman's writer, so run it whenever the
writer, the snapshot producer or the `iceberg` version moves. PyIceberg 0.12.0
cannot read `_row_id`; lineage is checked with `iceberg-rust`'s reader in the
regular suite.

Read the gate's verdict from a log, never from a pipe:

```bash
just ci > /tmp/ci.log 2>&1; echo "EXIT:$?" >> /tmp/ci.log
```

Piping through `tail` or `grep` masks the exit code. And run `cargo fmt` after
any scripted edit — a `sed` or heredoc patch reliably leaves formatting `lint`
rejects, and the failure reads as a real one.

**No containers, no cloud credentials**, and a full run in seconds. The suite
builds real Iceberg tables on the local filesystem and commits to an in-memory
catalog built on `TableRequirement::check` and `TableUpdate::apply` — which is
what a REST catalog does with a commit when it receives one. Keep it that way.

## What breaks data, and does not fail visibly

These are the invariants. Each one, when broken, produces a table that is wrong
forever and looks fine.

- **A carried-forward file keeps its own sequence number and snapshot id.**
  Stamping the new snapshot's numbers on an untouched file makes it look newer
  than the delete files that apply to it, which resurrects deleted rows. Files a
  snapshot genuinely *adds* are the opposite case and take the new number — that
  is what retires the deletes already applied to their contents.
- **A manifest is rewritten under the partition spec it was written under.** A
  manifest carries exactly one `partition_spec_id` and an entry's partition tuple
  is meaningless against any other. Re-interpreting one mis-prunes files at query
  time and nothing fails while it does.
- **A branch's retention survives a commit that moves it.** REST's
  `set-snapshot-ref` replaces the whole reference, so a commit naming no
  retention erases what the table's owner configured — visible only the next time
  expiration runs.
- **A rewrite never drops a delete file another file still needs.** A delete file
  is retired only when *every* data file it applies to is inside the group being
  rewritten, which is why the whole table is planned before anything is written.
- **A file's identity is `(path, offset)`, not a path.** Correct for everything
  that owns its file; data loss for a v3 deletion vector, which is a blob at an
  offset inside a Puffin container that may hold many others. Removing one dead
  vector by path takes its live siblings.
- **A v3 rewrite writes row lineage physically.** A moved row keeps its
  `_row_id` and `_last_updated_sequence_number` as columns in the new file, and
  the new entries leave `first_row_id` null. Left to inheritance, the new file's
  positions would renumber every row it holds, and a `MERGE` or CDC consumer
  joining on row id would match the wrong rows. The snapshot claims exactly the
  row range its manifest list assigned, seeded from the table's `next-row-id` —
  claim more or less and the catalog's `next-row-id` hands an id out twice or
  skips a range.
- **Nothing deletes a file because a delete it contains became dead.** Files are
  deleted for exactly one reason — nothing in the reachable set names them — and
  that reason is computed in one place.
- **A reachable set is all-or-nothing.** Any error anywhere aborts the whole
  computation rather than returning a partial set, because a partial set is
  indistinguishable from a complete one and would license deleting everything it
  failed to read. Do not "improve" this into something that skips a bad manifest.
- **A rewrite inherits every opinion the table has about how its files are
  written** — compression, row-group and page sizing, dictionary size, bloom
  filters, data and metadata paths, and the sort order. That list is longer than
  it looks — Iceberg defines dozens of `write.*` properties — and the checklist
  when adding a writer knob is Iceberg's own `TableProperties`, read against
  `src/ops/compact/writer.rs` and the commit layer.

## Refusals are a feature

Bergman refuses rather than guessing, and each refusal names its reason in
`bergman plan` as well as in the run report — a table whose configured
maintenance can never apply must not read as a healthy one.

Currently refused: **encrypted tables** (manifests and manifest lists are
encrypted and unreadable — though snapshot expiration still runs, because it
needs no manifest; decrypting needs a key-management client, and choosing one is
a key-custody decision that has not been made), **anything but Parquet**,
**cross-spec rewrites**, and **any branch but `main`**. Format v3 is maintained,
and its one refusal is precise rather than blanket: the snapshot producer
refuses, by name, an added data file in a v3 rewrite that lacks the `_row_id`
and `_last_updated_sequence_number` columns, so a library caller cannot commit a
file that would renumber rows.

**A refusal discharges a question as fully as an implementation.** When adding
one, put it in all the places the existing ones are: the planner, the operation,
and the snapshot producer — so an embedder calling an operation directly gets the
same named error rather than a parse failure from three layers down.

## Testing bar

**A check that fails if the change is reverted.** Not that it works, not that a
test passes.

- **A guard that prevents something is tested as a pair against one fixture**: a
  control that removes the hazard and asserts the action happens, and a guarded
  case that restores it and asserts it does not. A guarded-only test passes just
  as happily when the action was never reachable — writing these pairs for orphan
  removal is what found the missing eighth check.
- **The fixture must be the production constructor.** The physical-plan assertion
  that keeps the equality-delete anti-join a *hash* join once built a default
  `SessionContext`, which shares neither the memory pool nor the
  `target_partitions` of the session this crate builds.
- **For a defect, write the failing proof first**, run it, and read the output
  before touching the fix. A defect you cannot reproduce is a hypothesis.

## Layout

```text
src/policy/   schema, validation, layered resolution with provenance
src/catalog/  config, connection, discovery, storage wiring
src/health/   metadata-only analysis, partition identity
src/plan/     op planners, estimates, budget, run report
src/commit/   snapshot production, REST delivery, OAuth2  ← Bergman's own
src/ops/      compact / expire / manifests / orphans
              delete.rs       the one deleter both destructive ops share
              reachability.rs what a table still needs — safety-critical
              store.rs        object listing, which FileIO does not offer
src/sched/    daemon, cron triggers, event debounce
src/obs/      observer trait, audit log, metrics
src/cli/      clap commands, human + JSON output
```

Two boundaries are load-bearing rather than tidy. **`ops::delete` is the one
deleter** — expiration and orphan removal decide *what* to delete by different
reasoning, then share one blast-radius ceiling, one audit record and one bounded
deletion path. **`commit` sits behind a `TableCommitter` trait**, so when
upstream grows an action that can express a rewrite it becomes a second
implementation and nothing above `src/commit` changes.

## Why Bergman owns its commit layer

An Iceberg commit is `(requirements, updates)` applied atomically, and the
`iceberg` crate cannot express one from outside: no `Transaction` action removes
a data file, and both `TransactionAction` and `TableCommit`'s builder are
`pub(crate)`. Every *piece* is public and serializable, so Bergman builds the
commit with upstream's own writers and delivers it over REST itself — the bytes
on the wire are what `iceberg-catalog-rest` would send.

The alternative the rest of the field took is a fork, which costs a rebase
forever *and* a crate that cannot be published, since Cargo rejects git
dependencies on crates.io. Fatal for a library-first project.

**Do not fork upstream, and do not vendor it.** If something is missing, the
options are: route around it with public pieces, refuse with a named reason, or
contribute it.
