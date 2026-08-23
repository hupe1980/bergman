# Bergman — a Rust-native maintenance engine for Apache Iceberg.
#
# The result is a statically linked musl binary on a distroless base: no shell,
# no package manager, no libc to keep patched. A maintenance engine holds
# catalog credentials and delete permission on a warehouse, so the smallest
# reachable surface is worth the slightly longer build.

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
FROM rust:1.94-alpine AS build

# `musl-dev` for the C runtime the linker needs; `pkgconfig` and `openssl-dev`
# are deliberately absent — the tree is rustls-only, and `cargo deny` fails the
# build if an OpenSSL dependency ever appears.
RUN apk add --no-cache musl-dev

WORKDIR /build

# Dependencies first, in their own layer. Their sources change far less often
# than ours, so a code-only edit reuses this and rebuilds in seconds.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
COPY README.md ./

# Default features plus `metrics`, which the daemon's `/metrics`, `/health` and
# `/events` endpoints need. Spelled as an addition rather than as a full feature
# list, so it cannot drift from whatever `default` becomes.
#
# `touch` because cargo decides staleness by mtime, and the files copied above
# can be older than the stub build that just ran.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked --features metrics \
    && strip target/release/bergman

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /build/target/release/bergman /usr/local/bin/bergman

# Non-root by default. Bergman needs no filesystem of its own — configuration
# is mounted, and everything else lives in object storage.
USER nonroot:nonroot

# Where a mounted ConfigMap is expected.
ENV BERGMAN_CONFIG=/etc/bergman/bergman.toml

# Metrics and the events endpoint, when the daemon is asked to serve them.
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/bergman"]

# One cycle and exit: the shape a CronJob wants, and the one most deployments
# should use. Override with `["daemon", "--listen", "0.0.0.0:9090"]`.
CMD ["run"]

LABEL org.opencontainers.image.title="bergman" \
      org.opencontainers.image.description="A Rust-native maintenance engine for Apache Iceberg" \
      org.opencontainers.image.source="https://github.com/hupe1980/bergman" \
      org.opencontainers.image.licenses="Apache-2.0 OR MIT"
