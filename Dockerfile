# Bergman's container image: a static musl binary on a distroless base. No
# shell, no package manager, no libc to keep patched. A maintenance engine holds
# catalog credentials and delete permission on a warehouse, so the smallest
# reachable surface is worth having.
#
# The binary is built outside this file rather than in a build stage of it, and
# that is the point: the release workflow has already compiled exactly these
# binaries for the tarballs it publishes, so the image ships *those* — the same
# bytes an operator can verify against the published `.sha256`, not a second
# compile that happens to agree. It also means nothing here executes, so buildx
# produces both architectures with no emulation.
#
# `just image` builds the binary first and then this. See the justfile.

FROM gcr.io/distroless/static-debian12:nonroot

# `amd64` or `arm64`, set by buildx for each platform it is asked for. Nothing
# in this file executes, so producing both needs no emulation at all.
ARG TARGETARCH
COPY dist/bergman-${TARGETARCH} /usr/local/bin/bergman

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
