# Build context is the evm-amm-search repository. The sidecar compiles against
# the released crates.io packages and does not require sibling checkouts.
FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder

WORKDIR /build

# GNU ld was the dominant silent phase in release builds of the full protocol
# feature set. LLD keeps peak memory and link latency bounded on small builders.
RUN apt-get update \
    && apt-get install -y --no-install-recommends lld \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"
ARG CARGO_BUILD_JOBS=4
ARG VCS_REF=unknown
ENV EVM_AMM_ROUTE_SOURCE_REVISION="$VCS_REF"

COPY sidecar/Cargo.lock ./Cargo.lock
COPY sidecar/Cargo.toml ./Cargo.toml
COPY sidecar/benches ./benches
COPY sidecar/src ./src

RUN --mount=type=cache,id=amm-route-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=amm-route-release-target,target=/build/target,sharing=locked \
    cargo build \
    --locked \
    --release \
    --jobs "$CARGO_BUILD_JOBS" \
    --bin evm-amm-route-sidecar \
    && cp /build/target/release/evm-amm-route-sidecar /tmp/evm-amm-route-sidecar

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 router
COPY --from=builder /tmp/evm-amm-route-sidecar /usr/local/bin/evm-amm-route-sidecar

ARG VCS_REF=unknown
ARG IMAGE_VERSION=0.1.0-beta.1
ARG BUILD_CREATED=unknown
ARG SOURCE_DATE_EPOCH=0
LABEL org.opencontainers.image.title="evm-amm-route-sidecar" \
      org.opencontainers.image.description="Self-hosted AMM quote and execution routing sidecar" \
      org.opencontainers.image.version="$IMAGE_VERSION" \
      org.opencontainers.image.created="$BUILD_CREATED" \
      org.opencontainers.image.revision="$VCS_REF" \
      org.opencontainers.image.source="https://github.com/KaiCode2/evm-amm-search" \
      org.opencontainers.image.documentation="https://github.com/KaiCode2/evm-amm-search/tree/main/sidecar" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0" \
      io.evm-amm-search.source-date-epoch="$SOURCE_DATE_EPOCH"

USER router
EXPOSE 8080
ENV AMM_ROUTE_CONFIG=/config/config.toml
ENV RUST_LOG=evm_amm_route_sidecar=info,tower_http=info

HEALTHCHECK --interval=15s --timeout=3s --start-period=180s --retries=4 \
    CMD curl --fail --silent http://127.0.0.1:8080/readyz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/evm-amm-route-sidecar"]
