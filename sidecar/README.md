# AMM route sidecar

`evm-amm-route-sidecar` is a deployment package around `evm-amm-search`; it is
not part of the core library API. One process owns one chain, maintains a live
AMM graph from the canonical websocket, and exposes bounded REST quote and token
preparation operations.

The default HTTP boundary is quote-only. An additional, disabled-by-default
`POST /v1/executable-quote` endpoint can translate a route and its exact graph
snapshot into calldata for the experimental executor, simulate that transaction
at the canonically verified quoted block hash, estimate gas, and produce any
prerequisite ERC-20 approval transaction. The service never signs or submits
transactions, does not split
orders, and does not rank routes using gas or gas-token prices.

Status: **public beta; deliberately not production-grade**. The sidecar has
live mainnet smoke coverage and automated HTTP/config/container checks. Its
fail-closed and websocket recovery
paths have exact-image 5-second, 40-second, five-minute, endpoint-failover,
state-provider-failure, provider-restart, DNS-failure, and shallow-reorg
evidence. It has not completed a multi-hour soak, packet-blackhole transport,
deep-reorg, or venue-wide executable-route matrix. Treat it as a self-hosted
showcase for controlled deployments, not as a public transaction router.

See [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) for the implemented
boundary, hard release blockers, known limitations, and deployment checklist.
See [QUOTER_HARDENING.md](QUOTER_HARDENING.md) for the measured concurrency,
fail-closed quote contract, websocket-recovery design, and ranked robustness
backlog.
The focused [graph-index benchmark](GRAPH_INDEX_BENCHMARKS.md) records the
removed per-request reconstruction cost and cached lookup/update scaling.

Run the offline graph-metadata benchmark from the repository root:

```bash
cargo bench --manifest-path sidecar/Cargo.toml --bench graph_index -- --noplot
```

## Run locally

```bash
export ETHEREUM_WS_URL=wss://your-ethereum-node
export AMM_ROUTE_ADMIN_TOKEN="$(openssl rand -hex 32)"
cargo run --manifest-path sidecar/Cargo.toml -- \
  --config sidecar/examples/ethereum-mainnet.toml
```

Validate a profile without connecting to a node:

```bash
ETHEREUM_WS_URL=wss://example.invalid \
  AMM_ROUTE_ADMIN_TOKEN=configuration-check-only \
  cargo run --manifest-path sidecar/Cargo.toml -- \
  --config sidecar/examples/ethereum-mainnet.toml --check-config
```

## Docker

The sidecar consumes the released crates.io packages, so its image builds from
the `evm-amm-search` repository alone. The release wrapper applies the bounded
build settings and writes machine-readable image evidence:

```bash
sidecar/scripts/build-release-image.sh \
  evm-amm-route-sidecar:local \
  sidecar-release-evidence.json

docker run --rm -p 127.0.0.1:8080:8080 \
  --read-only \
  -e ETHEREUM_WS_URL \
  -e AMM_ROUTE_ADMIN_TOKEN \
  -v "$PWD/sidecar/examples/ethereum-mainnet.toml:/config/config.toml:ro" \
  evm-amm-route-sidecar:local
```

Run those commands from the `evm-amm-search` repository root. `docker compose
-f sidecar/compose.yaml up` is also available after both environment variables
are set. Compose binds the service to host loopback, drops Linux capabilities,
enables `no-new-privileges`, and uses a read-only root filesystem.

For a published beta, pin either its immutable version tag or, preferably, its
manifest digest and use the pull-only Compose profile:

```bash
export AMM_ROUTE_IMAGE=ghcr.io/kaicode2/evm-amm-route-sidecar:0.1.0-beta.1
export ETHEREUM_WS_URL=wss://your-ethereum-node
export AMM_ROUTE_ADMIN_TOKEN="$(openssl rand -hex 32)"
docker compose -f sidecar/compose.release.yaml pull
docker compose -f sidecar/compose.release.yaml up -d
```

`evm-amm-route-sidecar --version` reports the package version. `/v1/status`
also exposes that version and the source revision embedded by the release
builder, allowing operators to match a running process to an image digest and
repository commit.

## Quote semantics and limitations

`POST /v1/quote` returns indicative exact-input routes computed against a
specific observed chain snapshot. It does not construct or submit a swap
transaction and does not guarantee execution or output.

- `amount_out` is the simulated gross token output. It is not a
  slippage-protected `minimum_amount_out`. Callers must choose an appropriate
  slippage tolerance and enforce the resulting minimum output in the transaction
  or settlement contract.
- Routes are ranked by gross output only. Gas costs, gas-token prices, L1/L2
  data fees, approvals, wrapping costs, and transaction tips are not included
  in ranking. The executable endpoint's gas estimate is disclosure, not an
  input to route selection.
- A returned route is the best observed route among the candidates evaluated.
  It is not guaranteed to be the best price available across all AMMs,
  aggregators, protocols, or possible split routes.
- `fast` and `balanced` searches are heuristic. `exhaustive` only covers the
  configured graph, protocols, connector tokens, hop limit, and candidate
  budget.
- Quotes can become stale immediately because of new blocks, competing
  transactions, reorgs, liquidity changes, or MEV. Callers should reject stale
  source blocks and re-quote immediately before execution.
- The service does not classify token transfer restrictions, rebasing behavior,
  recipient constraints, or whether a token is safe to trade. The executor
  rejects funding or delivery balance deltas that differ from the encoded
  amounts, so fee-on-transfer behavior fails closed rather than consuming
  pre-existing router balances or bypassing the final minimum.
- Discovery readiness means the configured discovery job completed. It does not
  mean every relevant pool or venue has been found.
- AMM fees and price impact are reflected only insofar as they are modeled by
  the configured pool adapter. Downstream transfer taxes and execution-specific
  costs may not be represented.

## Experimental executable quotes

When explicitly enabled, `POST /v1/executable-quote` accepts the normal quote
fields plus sender, recipient, minimum-output policy, deadline, and input
authorization. It builds from the ordered route candidates and registry in one
immutable search snapshot, then simulates the finished transaction and gas
estimate against that exact block hash. A response is returned only after the
simulation succeeds, its decoded output exactly equals the selected route
quote, and that output satisfies the final minimum. A mismatch returns
`simulation_quote_mismatch` instead of executable calldata.

The builder encodes Uniswap V2, Uniswap V3, PancakeSwap V3, Slipstream,
Solidly V2, Balancer V2, and the supported Curve variants directly from graph
metadata. Opaque custom adapters are not executable: the permissionless router
does not expose an arbitrary external-call opcode.

Input funding can use a pre-existing router allowance, an atomic ERC-2612
permit, Permit2 SignatureTransfer, or native currency wrapping. A Permit2 swap
still reports the prerequisite token-to-Permit2 allowance. Native input is
accepted only when the route starts with the configured WETH address. Allowance
and Permit2 requests check the sender's prerequisite on-chain allowance at the
source block before swap simulation. If it is insufficient, the endpoint returns
`428 Precondition Required` with an exact-amount approval transaction and gas
estimate; submit it, then request a fresh route and simulation.

`min_amount_out` and `slippage_bps` are mutually exclusive. If both are omitted,
a validated second-best executor-allowed route for the same input and token pair
supplies the final-output floor; a result with only one eligible route therefore
requires an explicit minimum or slippage tolerance. Routes containing protocol
families outside `executor.allowed_protocols` are skipped. Protection applies
only to the final output. Intermediate legs remain unprotected in this
experimental contract.

Every successful response carries the canonically rechecked block hash and
timestamp, graph revision, selected gross-price rank,
transaction target/value/calldata/deadline, final minimum, simulated output,
transaction gas estimate, and current allowance details when applicable.
Deadline TTLs are based on the quoted block timestamp rather than the container
clock, and freshness is measured against the canonical head timestamp. Stale
snapshots fail closed. Re-simulate and re-quote immediately before signing; the
endpoint cannot reserve liquidity or protect against later state changes.

## API

Quote exact input. Search controls are stable service policies and are clamped
by the TOML server limits rather than exposing the crate's internal config:

```bash
curl -sS http://127.0.0.1:8080/v1/quote \
  -H 'content-type: application/json' \
  -d '{
    "token_in":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "token_out":"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "amount_in":"1000000",
    "options":{
      "quality":"balanced",
      "max_hops":3,
      "top_k":3,
      "timeout_ms":5000,
      "discovery":"if_missing"
    }
  }'
```

Request a slippage-protected executable allowance swap after enabling and
deploying the experimental executor:

```bash
curl -sS http://127.0.0.1:8080/v1/executable-quote \
  -H 'content-type: application/json' \
  -d '{
    "token_in":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "token_out":"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "amount_in":"1000000",
    "sender":"0x1111111111111111111111111111111111111111",
    "recipient":"0x2222222222222222222222222222222222222222",
    "slippage_bps":50,
    "deadline_secs":60,
    "authorization":{"type":"allowance"},
    "options":{"quality":"balanced","top_k":3,"discovery":"off"}
  }'
```

Authorization objects are `{"type":"allowance"}`, `{"type":"native"}`,
`{"type":"erc2612","v":27,"r":"0x...","s":"0x..."}`, or
`{"type":"permit2","nonce":"...","deadline":"...","signature":"0x..."}`.
ERC-2612 requests must also set the top-level absolute `deadline` used in the
signed permit. Other modes may set either an absolute `deadline` or a relative
`deadline_secs`, but not both. A Permit2 signature deadline must be at least as
late as the swap deadline.
Executable quotes reject implicit `discovery:"refresh"`; prewarm or explicitly
refresh coverage first, then request against the settled graph.

Prepare an unconfigured token before its quote amount or counterpart is known:

```bash
curl -sS -X PUT \
  http://127.0.0.1:8080/v1/tokens/0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32/prewarm \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $AMM_ROUTE_ADMIN_TOKEN" \
  -d '{"mode":"ensure"}'

curl -sS \
  http://127.0.0.1:8080/v1/tokens/0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32
```

Prewarm is idempotent and supports optional `connectors`, `protocols`, a
`refresh` mode, and `wait: true`. If `server.admin_bearer_token` is configured,
prewarm and quote-time refresh require `Authorization: Bearer ...`. Without a
token these cost-incurring operations are intentionally available for private
network deployments and a startup warning is emitted.

Other endpoints:

- `GET /livez`: process liveness
- `GET /readyz`: live runtime readiness using the same trust/freshness gate as quotes
- `GET /v1/status`: graph, work queue, canonical recovery, profile, and token coverage status
- `POST /v1/executable-quote`: opt-in simulated executor transaction
- `GET /v1/tokens/{address}`: one token's current coverage

## Configuration

Profiles can extend `ethereum-mainnet` or start from `none`. Arrays of tokens,
factories, and manual pools merge by on-chain identity; `replace_tokens`,
`replace_factories`, and `replace_pools` replace the inherited lists. `${NAME}`
placeholders are expanded only inside parsed TOML string values, so an
environment value cannot inject arbitrary TOML structure.

The sidecar distinguishes three layers:

1. The profile's desired tokens, factories, and pools.
2. The coverage ledger exposed by token/status endpoints.
3. The live graph containing only hydrated, searchable pools.

Configured tokens are discovered against configured connector tokens at
startup. Quote requests use the warm graph directly unless their discovery
policy explicitly requests missing coverage or refresh.

`storage.persist_cache` must remain `false` in this release. Startup always
rebuilds a verified graph from the canonical chain; configuration validation
fails instead of pretending persistence is active.

`rpc.canonical_ws` is the primary canonical feed and
`rpc.canonical_ws_fallbacks` is an ordered recovery list. Brief stream failures
are retried and hash-pinned log windows are backfilled in place. A terminal
subscriber failure, untrusted runtime, or lack of canonical progress beyond
`canonical_max_stale_secs` invalidates the complete routing generation. The
sidecar then recreates the provider and builds a fresh verified generation,
rotating endpoints and retrying indefinitely with capped exponential backoff
and jitter. Liveness remains healthy during this process; readiness and both
quote endpoints return 503 until the replacement is coherent.

The transport, terminated-stream retry/backfill, freshness watchdog, rebuild
timeout, and supervisor backoff settings are all under `[rpc]`; the production
defaults are shown in `examples/ethereum-mainnet.toml`. Omitting
`canonical_stream_reconnect_max_attempts` retries terminated streams forever.
The resolved Alloy transport sends its fixed keepalive ping every ten seconds;
the independent freshness watchdog is the fail-closed protection for a silent
or half-open connection.

Executable quotes are disabled unless `[executor].enabled = true`. Enabling
requires the deployed router, WETH, Permit2, and expected runtime bytecode hash;
startup fetches code at the canonical baseline and fails closed if the hash does
not match. Deadline, slippage, simulation-concurrency, and simulation-timeout
limits are server policy:

```toml
[executor]
enabled = true
router = "0x..."
weth = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
permit2 = "0x000000000022D473030F116dDEE9F6B43aC78BA3"
expected_runtime_code_hash = "0x..."
allowed_protocols = ["uniswap_v2", "uniswap_v3", "pancake_v3", "curve"]
default_deadline_secs = 120
max_deadline_secs = 600
max_snapshot_age_secs = 60
max_slippage_bps = 500
max_in_flight_simulations = 8
simulation_timeout_ms = 3000
```

The built-in registry serves Uniswap V2, Uniswap V3, PancakeSwap V3,
Slipstream, Solidly V2, Balancer V2, and Curve. Factory discovery covers the
configured V2/V3 families plus Slipstream and Aerodrome/Velodrome V2. Balancer
V2 and Curve remain manual-pool profiles. Slipstream factory discovery is
discovery-only in the current upstream adapter; executable quoting requires a
manual pool with explicit fee, tick spacing, token pair, and a compatible
fee-parameter quote target.

The default executor allowlist contains the protocol families covered by the
full generated-route gate: Uniswap V2, Uniswap V3, PancakeSwap V3, and Curve.
Balancer V2 passes the direct executor contract fork but is not enabled by
default: its current upstream cold-start finalizer declares whole-account pool
state that the prepared-state runtime correctly refuses to certify. Slipstream,
Solidly V2, and Balancer V2 encoding remain available but must be enabled only
after the operator closes and re-runs the applicable full-service fork gate.

## Deploy and verify the executor

Deploy through the included CREATE2 factory script. Use a dedicated deployer
and supply secrets through the process environment; do not put a private key in
TOML or Compose:

```bash
export DEPLOYER_PRIVATE_KEY=...
export WETH_ADDRESS=0x...
export PERMIT2_ADDRESS=0x...
export EXECUTOR_SALT=0x...

forge script script/DeployExperimentalExecutor.s.sol \
  --rpc-url "$EXECUTOR_RPC_URL" \
  --broadcast
```

`EXECUTOR_FACTORY` may point at an existing factory. After deployment, generate
the exact sidecar configuration and independently verify an expected runtime
hash when one is available:

```bash
EXECUTOR_RPC_URL="$EXECUTOR_RPC_URL" \
EXECUTOR_ROUTER=0x... \
EXPECTED_RUNTIME_CODE_HASH=0x... \
sidecar/scripts/executor-preflight.sh
```

Startup repeats the code-hash check at the canonical baseline and refuses to
serve executable quotes on any mismatch.

## Security and operations

- The service terminates plain HTTP. Put it behind a trusted reverse proxy or
  service mesh for TLS, client authentication, and public rate limiting.
- The bearer token protects prewarm and explicit refresh operations; it is not
  general quote authentication. Keep the service on a private network unless a
  gateway authenticates all endpoints.
- One process owns one chain. Run separate containers for separate chains.
- `/livez` stays available while canonical recovery is retrying. `/readyz` is
  available only after graph bootstrap and while the active generation is
  trusted and fresh. The image healthcheck allows a three-minute startup period
  for slower RPC providers.
- `/v1/status` exposes `routing_generation`, canonical connection state and age,
  active endpoint index/count, subscriber state, and reconnect attempts. It
  never exposes configured endpoint URLs.
- A quote that spans a recovery boundary is rejected with
  `routing_generation_changed`; stale, rebuilding, and untrusted requests use
  the stable 503 codes `canonical_stale`, `canonical_reconnecting`, and
  `runtime_untrusted`.

## Release validation

The sidecar is included in the workspace test, lint, formatting, docs, and MSRV
checks. To reproduce the container-specific smoke after building an image:

```bash
sidecar/scripts/smoke-container.sh evm-amm-route-sidecar:local
```

The smoke verifies the non-root UID, readiness healthcheck, CLI contract, and
configuration loading inside a read-only container without connecting to RPC.

The deterministic release gate starts a synthetic local chain, installs an
exact Uniswap V2 runtime fixture, routes canonical and state traffic through a
digest-pinned fault proxy, and exercises the exact image:

```bash
QUOTER_SIDECAR_IMAGE=evm-amm-route-sidecar:local \
QUOTER_EVIDENCE_JSON=quoter-release-gate.json \
sidecar/scripts/quoter-hardening-anvil.sh
```

Pull requests run a bounded version of this gate. The scheduled/manual nightly
workflow uses a five-minute outage, ten reconnect cycles, a provider process
restart, and a shallow reorg. Both retain image identity, structured results,
logs, SBOM, and vulnerability-scan artifacts.

Live provider validation remains a manual release gate:

```bash
export ETHEREUM_WS_URL=wss://your-ethereum-node
sidecar/scripts/live-smoke-container.sh evm-amm-route-sidecar:local
```

That gate boots the exact image with the hardened runtime settings, waits for
readiness, requires a non-empty graph, and requires a balanced USDC to WETH
route. Release notes should additionally record chain, block, graph size,
startup time, token coverage, and fast/balanced quote results.

The executor has a separate opt-in archive-RPC smoke gate pinned to Ethereum
block `21,000,000`:

```bash
export ETHEREUM_RPC_URL=https://your-ethereum-archive-node
sidecar/scripts/executor-fork-smoke.sh
```

It executes seven real scenarios: Uniswap V2 allowance, Uniswap V3 native input,
PancakeSwap V3 native input, Balancer V2 native input, mixed Uniswap V2 to
Curve, ERC-2612, and Permit2. The suite checks output delivery, approval cleanup,
signature/nonce consumption, and the router's no-residual-funds invariant.
The RPC-free pull-request suite explicitly excludes this contract. The
scheduled/manual `Executor pinned-fork reliability gate` requires the archive
RPC secret, fails if any case is skipped, and retains the complete output.

The full service-to-chain gate forks pinned Ethereum block `21,000,000` into
Anvil, installs the executor without changing that canonical block hash, and
runs four sidecar-generated scenarios: Uniswap V2, Uniswap V3, PancakeSwap V3,
and mixed Uniswap V2 to Curve:

```bash
export ETHEREUM_RPC_URL=https://your-ethereum-node
sidecar/scripts/executor-e2e-anvil.sh
```

For every scenario it requires the expected hop family, exact route-quote to
executor-simulation parity, a successful mined receipt, exact simulated-output
to recipient-balance parity, and final-minimum satisfaction. The chain is
restored between scenarios so every result uses identical pinned liquidity.
Set `EXECUTOR_E2E_EVIDENCE_JSON=path.json` to retain the block hash, route,
output, gas estimate, and transaction hash for each case. The scheduled/manual
archive-fork workflow runs both executor suites at the pinned default and
uploads their evidence. `EXECUTOR_E2E_FORK_BLOCK=latest` is available for an
additional non-reproducible live-head check when a provider lacks archive
state; it is not a substitute for the pinned release gate.

`EXECUTOR_E2E_ONLY=balancer_v2` retains the known-gap characterization: it must
remain outside release evidence until it passes. The direct Solidity fork case
continues to prove that the Balancer executor opcode itself works; the remaining
failure is in sidecar state hydration, before route generation.
