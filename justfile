# Bergman — a Rust-native maintenance engine for Apache Iceberg
# https://github.com/hupe1980/bergman
#
# Run `just --list` to see all available recipes.

default:
    @just --list

# ============================================================================
# Development
# ============================================================================

# Build in debug mode
build:
    cargo build --all-features

# Build the release binary
build-release:
    cargo build --release --all-features

# Run the CLI
run *ARGS:
    cargo run --all-features -- {{ARGS}}

# Inspect the tables in the example configuration
inspect:
    cargo run --all-features -- --config bergman.example.toml inspect

clean:
    cargo clean

# ============================================================================
# Testing
# ============================================================================

test:
    cargo test --all-features

test-verbose:
    cargo test --all-features -- --nocapture

test-unit:
    cargo test --lib --all-features

test-integration:
    cargo test --test '*' --all-features

test-doc:
    cargo test --doc --all-features

# Coverage report (requires cargo-llvm-cov)
coverage:
    cargo llvm-cov --all-features --html

# ============================================================================
# Quality gates — what CI runs
# ============================================================================

# Everything CI checks, in the order CI checks it
ci: lint check-all test docs site

lint:
    cargo fmt --all -- --check
    cargo clippy --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all

# Build every feature combination that matters.
#
# The per-feature loop is the one that earns its runtime: a feature that
# compiles under `--all-features` can be broken *alone*, because some other
# crate's feature was switching on the thing it needed. That failure only
# appears in exactly this build.
#
# Warnings are errors here, not only under `--all-features`: `just lint` runs
# clippy against the full build, so a warning that exists only in a narrower one
# — code left dead when a feature is off, most often — is invisible everywhere
# else.
check-all:
    RUSTFLAGS="-D warnings" cargo check --no-default-features
    RUSTFLAGS="-D warnings" cargo check --all-features
    @for feature in cli catalog-rest compaction storage-s3 storage-gcs storage-azure metrics; do \
        echo "--- checking feature: $feature (alone) ---"; \
        RUSTFLAGS="-D warnings" cargo check --no-default-features --features "$feature" || exit 1; \
    done

# Documentation must build without warnings: a broken intra-doc link is a
# promise the docs make and cannot keep.
docs:
    RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps

docs-open:
    cargo doc --all-features --no-deps --open

# The documentation site's internal links (requires zola).
#
# External links are skipped: a third-party site being down is not a reason to
# fail a local check.
site:
    zola --root site check --skip-external-links

# Advisories, licences, bans, sources (requires cargo-deny).
#
# Every ignored advisory in `deny.toml` carries a written reason — an ignore
# with no argument is a suppressed alarm.
deny:
    cargo deny check

# The cross-engine round trip (requires uv): PyIceberg writes tables, Bergman
# compacts and rewrites their manifests, PyIceberg reads them back.
#
# Every other suite writes with `iceberg-rust` and reads with `iceberg-rust`,
# which catches spec errors but not disagreements between implementations —
# so this is the one check whose reader shares no code with Bergman's writer.
# Opt-in and outside `ci`: it needs Python and fetches PyIceberg, whose
# version is pinned here so a run is reproducible.
cross-engine:
    #!/usr/bin/env bash
    set -euo pipefail
    dir=$(mktemp -d)
    trap 'rm -rf "$dir"' EXIT
    py() { uv run --isolated --no-project --python 3.12 \
        --with 'pyiceberg[sql-sqlite,pyarrow,pyiceberg-core]==0.12.0' \
        python -I tests/cross_engine/pyiceberg_roundtrip.py "$@"; }
    py write "$dir"
    BERGMAN_CROSS_ENGINE_DIR="$dir" cargo test --all-features --test cross_engine -- --ignored
    py verify "$dir"

# Verify the declared MSRV against a real toolchain rather than trusting the
# number in Cargo.toml.
msrv:
    @msrv=$(grep '^rust-version' Cargo.toml | cut -d'"' -f2); \
    echo "checking MSRV $msrv"; \
    rustup toolchain install "$msrv" --profile minimal 2>/dev/null || true; \
    cargo "+$msrv" check --all-features

# Build the container image, the way the release does: a static musl binary
# first, then a distroless image around it.
#
# The binary is compiled inside a musl container rather than on this machine, so
# the recipe works from any dev machine — macOS included, where there is no musl
# toolchain — and produces the same artifact CI does.
image tag="bergman:dev":
    mkdir -p dist
    docker run --rm -v "$PWD":/src:ro -v "$PWD/dist":/out -w /src \
        -e CARGO_TARGET_DIR=/tmp/target rust:1.95-alpine sh -c '\
            apk add --no-cache musl-dev >/dev/null && \
            cargo build --release --locked --all-features && \
            cp /tmp/target/release/bergman \
               /out/bergman-$(uname -m | sed "s/x86_64/amd64/;s/aarch64/arm64/")'
    docker build -t {{tag}} .

# ============================================================================
# Release
# ============================================================================

# Everything that must pass before publishing
pre-release: ci deny msrv
    cargo publish --dry-run --all-features

audit:
    cargo audit
