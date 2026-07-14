# Releasing `evm-amm-search`

Release candidates are published in dependency order:

1. `alloy-transport-balancer 0.2.0`;
2. `evm-fork-cache 0.3.0`;
3. `evm-amm-state 0.2.0`;
4. `evm-amm-search 0.1.1`.

Local development keeps versioned path dependencies. Cargo removes the paths
from the packaged manifest and resolves the stated crates.io versions.

## Required gates

```bash
cargo fmt --all -- --check
cargo fmt --manifest-path sidecar/Cargo.toml --all -- --check
cargo test --all-features
cargo test --locked --manifest-path sidecar/Cargo.toml
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --locked --manifest-path sidecar/Cargo.toml --all-targets -- -D warnings
cargo check --all-targets --no-default-features
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps
RUSTDOCFLAGS='-D warnings' cargo doc --locked --manifest-path sidecar/Cargo.toml --no-deps
cargo +1.88 check --all-targets --all-features
cargo +1.88 check --locked --manifest-path sidecar/Cargo.toml
cargo audit --ignore RUSTSEC-2025-0055
cargo bench --bench graph_lifecycle
cargo bench --all-features --bench live_search_runtime
cargo publish --dry-run
```

Refresh the sidecar lockfile with its declared MSRV as the resolver ceiling:

```bash
CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback \
  cargo +1.88 update --manifest-path sidecar/Cargo.toml
```

Build the sidecar from the `evm-amm-search` repository root and run its
artifact-level smoke gate:

```bash
docker build -f sidecar/Dockerfile \
  -t evm-amm-route-sidecar:release-candidate .
sidecar/scripts/smoke-container.sh \
  evm-amm-route-sidecar:release-candidate

ETHEREUM_WS_URL=wss://your-private-mainnet-node \
  sidecar/scripts/live-smoke-container.sh \
  evm-amm-route-sidecar:release-candidate
```

Run these commands from the `evm-amm-search` repository root. The image is
built solely from this checkout and the exact crates.io releases recorded in
`sidecar/Cargo.lock`; sibling crate checkouts are not part of its build context.

The sidecar remains `publish = false` and is excluded from the normalized
`evm-amm-search` crate. It ships with repository tags/container artifacts, not
inside the crates.io tarball. Describe it as experimental until load, provider
disconnect, and reorg gates are automated.

`RUSTSEC-2025-0055` is narrowly ignored because `ark-relations` records
`tracing-subscriber 0.2.25` as an optional lockfile dependency while it remains
absent from `cargo tree --target all --all-features`. Remove the exception if
that version enters the active graph.

The core-crate CI jobs reproduce the test/clippy/docs/MSRV matrix with sibling
state and cache checkouts for local path development. The sidecar container job
deliberately uses only this repository and released crates.io dependencies. The
package surface intentionally includes
the examples, tests, benchmark sources, sample configuration, and Solidity
sources/runtime artifact: they are the executable documentation and fixtures
for the public search crate, and no endpoint or local `.env` file is included.

The provider-backed examples share the versioned Alloy/reqwest provider helper
under `examples/support`. Release validation must compile the examples from the
normalized `.crate` archive so a dependency that Cargo omits during packaging
cannot silently break the published executable documentation.

Run the provider-backed examples and TUI benchmark after loading a private RPC
environment. Never print or commit endpoint values. Record the block, pool and
token counts, cache mode, sample count, median, p95, maximum, failures, and
retries in the benchmark document.

Run `../evm-amm-state/scripts/stage10-live-gates.sh` for the fail-closed combined
state/search gate. The final 2026-07-12 fresh-cache capture settled the complete
bounded startup set of `77` pools, published runtime handles at `12.276s`, and
streamed the first USDC → WETH route at `12.280s`, with zero transport, RPC, or
runtime-work failures. Direct focus quotes succeeded across Pancake V3 (`4/0`),
Sushi V3 (`4/0`), Uniswap V3 (`4/0`), and V2 (`1/0`). This is a stable-baseline
full-startup-idle measurement; do not compare it directly with the retired
post-ready basket benchmark.

`cargo publish` is never part of an automated local validation pass. Tag and
publish only after the dependency versions above are visible on crates.io and
the packaged-source build plus downstream compatibility checks pass.
