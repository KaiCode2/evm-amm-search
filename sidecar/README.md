# AMM route sidecar

`evm-amm-route-sidecar` is a deployment package around `evm-amm-search`; it is
not part of the core library API. One process owns one chain, maintains a live
AMM graph from the canonical websocket, and exposes bounded REST quote and token
preparation operations.

The initial boundary is intentionally quote-only: it does not build execution
calldata, submit transactions, split orders, or rank routes using gas/USD data.

Status: **experimental**. The sidecar has live mainnet smoke coverage and
automated HTTP/config/container checks, but has not yet completed sustained load,
reorg, or provider-failure testing. Treat it as a self-hosted quote service for
controlled deployments, not as a public transaction router.

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
the `evm-amm-search` repository alone:

```bash
docker build \
  -f sidecar/Dockerfile \
  -t evm-amm-route-sidecar:local \
  .

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

## Quote semantics and limitations

This service returns indicative exact-input route quotes computed against a
specific observed chain snapshot. It does not construct or submit a swap
transaction and does not guarantee execution or output.

- `amount_out` is the simulated gross token output. It is not a
  slippage-protected `minimum_amount_out`. Callers must choose an appropriate
  slippage tolerance and enforce the resulting minimum output in the transaction
  or settlement contract.
- Routes are ranked by gross output only. Gas costs, gas-token prices, L1/L2
  data fees, approvals, wrapping costs, and transaction tips are not included.
- A returned route is the best observed route among the candidates evaluated.
  It is not guaranteed to be the best price available across all AMMs,
  aggregators, protocols, or possible split routes.
- `fast` and `balanced` searches are heuristic. `exhaustive` only covers the
  configured graph, protocols, connector tokens, hop limit, and candidate
  budget.
- Quotes can become stale immediately because of new blocks, competing
  transactions, reorgs, liquidity changes, or MEV. Callers should reject stale
  source blocks and re-quote immediately before execution.
- The service does not validate balances, allowances, token transfer
  restrictions, fee-on-transfer behavior, rebasing behavior, recipient
  constraints, or whether a token is safe to trade.
- Discovery readiness means the configured discovery job completed. It does not
  mean every relevant pool or venue has been found.
- AMM fees and price impact are reflected only insofar as they are modeled by
  the configured pool adapter. Downstream transfer taxes and execution-specific
  costs may not be represented.

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
- `GET /readyz`: live runtime readiness
- `GET /v1/status`: graph, work queue, profile, and token coverage status
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

## Security and operations

- The service terminates plain HTTP. Put it behind a trusted reverse proxy or
  service mesh for TLS, client authentication, and public rate limiting.
- The bearer token protects prewarm and explicit refresh operations; it is not
  general quote authentication. Keep the service on a private network unless a
  gateway authenticates all endpoints.
- One process owns one chain. Run separate containers for separate chains.
- `/readyz` is available after graph bootstrap. The image healthcheck allows a
  three-minute startup period for slower RPC providers.

## Release validation

The sidecar is included in the workspace test, lint, formatting, docs, and MSRV
checks. To reproduce the container-specific gate after building an image:

```bash
sidecar/scripts/smoke-container.sh evm-amm-route-sidecar:local
```

The smoke verifies the non-root UID, readiness healthcheck, CLI contract, and
configuration loading inside a read-only container without connecting to RPC.
Live provider validation remains a manual release gate:

```bash
export ETHEREUM_WS_URL=wss://your-ethereum-node
sidecar/scripts/live-smoke-container.sh evm-amm-route-sidecar:local
```

That gate boots the exact image with the hardened runtime settings, waits for
readiness, requires a non-empty graph, and requires a balanced USDC to WETH
route. Release notes should additionally record chain, block, graph size,
startup time, token coverage, and fast/balanced quote results.
