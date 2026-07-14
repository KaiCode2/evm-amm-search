# Build context is the evm-amm-search repository. The sidecar compiles against
# the released crates.io packages and does not require sibling checkouts.
FROM rust:1.88-bookworm AS builder

WORKDIR /build

COPY sidecar/Cargo.lock ./Cargo.lock
COPY sidecar/Cargo.toml ./Cargo.toml
COPY sidecar/src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build \
    --locked \
    --release \
    && cp /build/target/release/evm-amm-route-sidecar /tmp/evm-amm-route-sidecar

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 router
COPY --from=builder /tmp/evm-amm-route-sidecar /usr/local/bin/evm-amm-route-sidecar

USER router
EXPOSE 8080
ENV AMM_ROUTE_CONFIG=/config/config.toml
ENV RUST_LOG=evm_amm_route_sidecar=info,tower_http=info

HEALTHCHECK --interval=15s --timeout=3s --start-period=180s --retries=4 \
    CMD curl --fail --silent http://127.0.0.1:8080/readyz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/evm-amm-route-sidecar"]
