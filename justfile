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

# Verify the declared MSRV against a real toolchain rather than trusting the
# number in Cargo.toml.
msrv:
    @msrv=$(grep '^rust-version' Cargo.toml | cut -d'"' -f2); \
    echo "checking MSRV $msrv"; \
    rustup toolchain install "$msrv" --profile minimal 2>/dev/null || true; \
    cargo "+$msrv" check --all-features

# Build the container image. Distroless and statically linked, so it needs no
# base-image patching and runs anywhere.
image tag="bergman:dev":
    docker build -t {{tag}} .

# ============================================================================
# Release
# ============================================================================

# Everything that must pass before publishing
pre-release: ci deny msrv
    cargo publish --dry-run --all-features

audit:
    cargo audit
