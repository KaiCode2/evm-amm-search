# Changelog

All notable changes to this project are documented in this file. The format is
based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Added

- An experimental, separately packaged HTTP routing sidecar with TOML profiles,
  warm canonical state, bounded fast/balanced/exhaustive quotes, and token
  prewarming/discovery endpoints.
- HTTP contract tests, fail-fast configuration bounds, and a non-root/read-only
  Docker smoke gate for the sidecar deployment artifact.
- An unaudited experimental exact-input executor contract covering all currently
  routable protocol families, ERC-2612 and Permit2 authorization, native-input
  wrapping, final-output minimums, per-hop full-consumption checks, and reusable
  CREATE2 deployment.
- A sidecar-local quote-to-executor encoder covering every executor protocol
  tag, allowance/permit/native transaction envelopes, approval disclosure,
  second-best-route minimum policy, explicit rejection of opaque custom
  adapters, and pinned Ethereum fork smoke scaffolding.
- A disabled-by-default executable-quote HTTP endpoint with final-output
  slippage policy, snapshot-bound calldata, exact-block simulation, transaction
  and approval gas estimates, deployment bytecode verification, and structured
  failure responses.
- Sidecar adapter and profile coverage for manual Slipstream, Solidly V2, and
  Balancer V2 pools plus Slipstream and Solidly factory discovery.
- Canonical-block timestamp deadlines, stale-snapshot rejection, explicit
  ERC-2612 deadlines, executor protocol allowlists, and a structured
  approval-required response with ready-to-submit approval calldata.
- Exact router funding and recipient-delivery balance invariants, including
  regression tests that fail closed for fee-on-transfer inputs and outputs.
- Deterministic executor deployment/preflight tooling, Solidity CI, and pinned
  real-fork execution coverage for Uniswap V2/V3, PancakeSwap V3, Balancer V2,
  Curve, ERC-2612, and Permit2.
- A full daemon-to-chain Anvil gate that turns a live mainnet-fork route into
  calldata, requires the route quote and exact-block executor simulation to
  agree, submits it, and verifies a successful receipt plus exact recipient
  output parity across four single- and mixed-protocol scenarios.
- A dedicated scheduled/manual archive-fork workflow that fails on missing RPC
  configuration or skipped fork cases and retains contract and service-to-chain
  execution evidence separately from the RPC-free pull-request suite.
- Quoter concurrency characterization tests plus a reproducible Anvil and
  Toxiproxy hardening harness covering parallel live quotes, fail-fast overload,
  short websocket recovery, and terminal-outage stale-state detection.
- Fail-closed readiness and quote generation fencing, configurable canonical
  freshness and stream recovery, ordered websocket failover, and a sidecar
  supervisor that replaces terminal routing generations from a fresh verified
  baseline while retrying indefinitely.
- A deterministic digest-pinned sidecar image build plus exact-image pull-request
  and nightly recovery gates covering parallel load, stale-state rejection,
  endpoint and DNS failover, independent state-provider failure, missed-block
  catch-up, provider restart, shallow reorg, bounded memory growth, SBOM
  generation, and vulnerability scanning.
- A dated production-readiness record and ranked quoter hardening backlog with
  explicit release acceptance criteria.
- A commit-maintained sidecar graph index with constant-time request admission,
  contiguous runtime-delta updates, lag/version-gap recovery, version-fenced
  token coverage, and reproducible 128/1,024/8,192-pool benchmarks.
- Release identity through `--version`, `/v1/status`, deterministic OCI labels,
  a pull-only Compose profile, and a private vulnerability-reporting policy.

### Changed

- Executable quotes now fail closed when exact-block executor simulation output
  differs from the selected route quote, even when both exceed the caller's
  minimum output.
- Manual V3 profiles now require and apply an explicit positive tick spacing,
  manual Balancer profiles require their verified Vault read set, and profiles
  with no factories no longer install unusable discovery watchers.
- Balancer V2 is removed from the default executor allowlist until its
  whole-account prepared-state ownership gap is fixed upstream and the full
  generated-route gate passes; hand-encoded contract-fork coverage remains.
- The crates.io package explicitly excludes the sidecar, executor deployment
  package, workflows, and local evidence while retaining the library examples
  and their required demo contracts.
- The sidecar enters its independent `0.1.0-beta.1` version line and pins the
  exact released `evm-amm-search` dependency recorded by its lockfile.

## [0.1.1] - 2026-07-21

### Changed

- Heuristic searches now cache token degree and connector-liquidity scores for
  the duration of one request, avoiding repeated centrality work without
  retaining stale state across searches.
- `AmmGraph` now maintains a synchronized outgoing-edge view used by route
  expansion, reachability checks, upper-bound pruning, exhaustive enumeration,
  and incremental replacement probes.
- Search internals and their memory/performance tradeoffs are documented in a
  dedicated performance model.

### Performance

- The synthetic warm scheduler probe improved from an initial `19.78ms`
  average to `4.63ms` in the final parity-preserving implementation, a `76.6%`
  reduction (`4.27x` faster). The final adjacency-cache round independently
  reduced its fresh baseline from `5.87ms` to `4.63ms` (`21.1%`).

## [0.1.0] - 2026-07-14

### Added

- Incremental `AmmGraph` mutation, graph versions, liquidity-sidecar updates,
  and equivalence coverage against full reconstruction.
- Snapshot-bound `LiveAmmGraph`, O(1) `LiveSearchView` construction, lazy exact
  quote provenance, and versioned route/gas results.
- `LiveRouteRuntime` with recoverable subscriptions, typed pipeline events,
  bounded persistent workers, in-place request replacement, cancellation, and
  complete stale-result fences.
- Progressive and dynamic providerless examples proving independently usable
  pools and add/remove routing without runtime or adapter reconstruction.
- A responsive live route TUI composed from the AMM runtime, background
  cold-start worker, canonical subscriber, factory watchers, incremental graph,
  and live route subscriptions.
- Offline graph and production live-search runtime benchmarks.
- Deterministic cancellation/replacement storm coverage and public scheduler
  benchmarks for commit-to-route publication at 1, 8, and 32 subscriptions.
- Crash-consistent TUI warm generations that atomically pair persistent EVM
  state, registration metadata, and a hash-certified canonical checkpoint.

### Changed

- Heuristic routing now uses liquidity-aware ordering, conservative pruning,
  streamed first-result/finality policies, and reusable quote-prefix caching.
- Live route request edits reuse one subscription and coalesce to the newest
  request epoch.
- Live search views retain immutable registry/revision snapshots instead of
  scanning every pool, exact reachability backtracks in place rather than
  cloning branch sets, and workers discard already-cancelled queued jobs before
  constructing a searcher.
- Streaming exhaustive enumeration now enforces the remaining candidate budget
  while generating paths and cooperatively polls live cancellation between
  queue expansions and edges. Dense-graph cancellation no longer blocks route
  runtime shutdown behind complete-universe materialization.
- Provider-backed examples now share a gzip-enabled Alloy/reqwest HTTP helper
  composed entirely from registry dependencies, so every shipped example also
  compiles from Cargo's normalized published archive.
- Persistent TUI shutdown now seals immutable generation files before replacing
  a synced manifest; interrupted or incomplete generations cannot displace the
  last committed warm checkpoint, and canonically rejected state is discarded.
- Initial cold-start admission now expands to fit a restored registration batch,
  preventing legacy archives larger than the normal 256-job floor from failing
  migration with queue backpressure.
- The headless live-start benchmark now uses the interactive TUI's heuristic,
  connector, hop, and liquidity-pruning search configuration, so its first-route
  milestone measures the product path rather than default library search.
- Live route failures now retain the first per-path quote diagnostic when every
  candidate fails, instead of collapsing the error to a candidate count.
- Progressive TUI bootstrap now warms the shared Uniswap/Sushi Router02,
  Uniswap-compatible QuoterV2, and PancakeSwap QuoterV2 accounts before the
  cache becomes an immutable actor snapshot, so first routes remain fully
  offline on cold starts and legacy-generation migration. Verified warm resumes
  reuse those generation-owned accounts without repeating code/proof RPCs.
- Updated the TUI to Ratatui 0.30 with its layout cache disabled, removing the
  affected `lru` release while preserving the existing rendering and input
  behavior under the unified Crossterm 0.29 backend.
- Progressive bootstrap now publishes runtime and route handles after the
  configured startup token pair produces an offline immutable-snapshot quote
  instead of waiting for all background discovery and cold-start work to become
  idle. Canonical subscriber attachment follows that focused stable-baseline
  milestone, avoiding block churn while the first useful pools hydrate and
  rejecting topology-only paths that cannot execute. Separate fail-closed
  bootstrap and first-route timeouts prevent the release benchmark from hanging
  outside its measured window.
- Full-basket factory discovery now starts only after runtime handles are
  published. Subscriber attachment sees the small focused pool set instead of
  racing unrelated commits through repeated WebSocket interest revisions.
- Warm resume now limits exact canonical catch-up to 256 blocks by default,
  configurable with `AMM_ROUTE_TUI_MAX_WARM_CATCHUP_BLOCKS`. Older checkpoints
  rebuild at the verified latest block with registration/read-set hints instead
  of blocking startup on an unbounded block-by-block replay.
- The headless release benchmark now treats route-local misses on a partially
  warmed graph as recoverable and waits for later topology commits to make the
  requested pair routable; runtime-wide failures remain immediately terminal.
- Headless failure accounting now closes before intentional subscriber and
  worker teardown, so cancellation fallout is not reported as live runtime
  failure.
- Shutdown now publishes a warm generation only after discovery, cold-start,
  and pending state work are quiescent. An early quit preserves the previous
  complete manifest instead of replacing it with a partial registration set.
- Built-in Curve 3pool, FRAX/USDC, and tricryptoUSDC-ng registrations now ship
  with provider-verified `get_dy` read sets, selecting the supported one-round
  background verifier instead of failing the worker-only discover phase. Their
  exact-hash account/code proofs are prewarmed so immutable route snapshots can
  execute those pools without lazy RPC faults.
- Startup discovery and restored registration hints now share one deterministic
  128-pool default budget, with configured and focus-pair pools prioritized.
  Subscriber attachment waits for that bounded set to become fully idle on the
  original baseline, preventing block churn from starving slower V3 hydration.
- Interactive search now terminates at the heuristic boundary by default and
  retains up to 16 top quotes for multi-venue display; exhaustive continuation
  remains opt-in with `AMM_ROUTE_TUI_EXHAUSTIVE_SEARCH=1`.

### Fixed

- Snapshot-only V3 quotes now use simulation-scoped ERC-20 transfer overrides,
  preserving real pool/quoter math without requiring arbitrary token balance
  mappings or mutating the canonical snapshot.
- Pancake V3 cold start now verifies its real pool runtime and hydrates its
  two-word `slot0`, shifted fee-growth/protocol-fee slots, and observation ring;
  the missing `unlocked` word previously caused every offline quote to revert
  with `LOK`.
- Benchmark failure observers now survive receiver lag and report skipped-event
  counts instead of silently returning a false zero-failure summary.

[Unreleased]: https://github.com/KaiCode2/evm-amm-search/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/KaiCode2/evm-amm-search/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/KaiCode2/evm-amm-search/releases/tag/v0.1.0
