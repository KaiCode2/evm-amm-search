# Production readiness

This document separates the route sidecar's implemented behavior from the
remaining release gates. The executor is a production candidate for controlled
self-hosted deployments; it is not yet an audited public transaction router.

The quoter's concurrency and websocket-failure baseline, including the ranked
next-work list, is recorded in [QUOTER_HARDENING.md](QUOTER_HARDENING.md).

## Implemented

- One process owns one chain, builds a verified AMM cache, discovers and
  hydrates configured liquidity, follows canonical WebSocket updates, and
  serves bounded quote APIs.
- Quote responses identify the exact chain id, block number, block hash, state
  version, and graph revision used by the search.
- Executable quotes encode the supported route directly from graph metadata,
  recheck that the source block is still canonical and fresh, enforce an
  operator protocol allowlist, and simulate the final transaction at the exact
  source block hash. They fail closed unless the selected route quote and the
  decoded executor result are exactly equal and satisfy the final minimum.
- The executor supports existing allowance, ERC-2612, Permit2
  SignatureTransfer, and native-WETH funding. Missing prerequisite allowance
  returns a structured exact-amount approval transaction instead of an
  unusable swap response.
- Deadline TTLs derive from chain time. Signed ERC-2612 requests require an
  explicit absolute deadline, and Permit2 signatures cannot expire before the
  swap. Snapshot freshness is measured against the canonical head timestamp,
  not the container clock.
- The Solidity router validates route continuity, final minimum output, exact
  input funding, exact recipient delivery, complete hop consumption, restored
  router balances, callback provenance, and a reentrancy lock.
- Opaque custom adapters are rejected; the permissionless router has no
  arbitrary external-call opcode.
- Startup pins the configured executor runtime bytecode hash. Deployment and
  preflight helpers report the address, immutable dependencies, and exact hash
  operators must place in the sidecar profile.
- The container runs as a non-root user with a read-only root filesystem,
  dropped capabilities, `no-new-privileges`, health checks, bounded request
  size/concurrency, and authenticated cost-incurring refresh operations.
- Quote admission and status endpoints read token membership and graph counts
  from a commit-maintained index. Normal requests do not reconstruct or scan
  the live graph; observer lag or a version gap rebuilds the index off the
  request path before prepared-token coverage is published.
- Release builds use digest-pinned base images, bounded parallel compilation,
  LLD, a minimal Docker context, and machine-readable artifact evidence. Pull
  requests and nightly runs generate an SBOM and scan the exact built image.

## Current validation

The worktree release gate covers:

- Rust unit, HTTP integration, formatting, clippy, and documentation checks.
- Solidity formatting, size limits, lint, and local unit tests.
- A pinned Ethereum archive-fork matrix for Uniswap V2, Uniswap V3,
  PancakeSwap V3, Balancer V2, Curve, ERC-2612, and Permit2.
- A pinned Anvil mainnet-fork flow that starts the real daemon and generates
  executable calldata for Uniswap V2, Uniswap V3, PancakeSwap V3, and mixed
  Uniswap V2 to Curve. Every scenario requires a successful receipt
  and exact quote-to-simulation-to-recipient-output parity.
- A scheduled/manual archive-fork workflow that requires RPC configuration,
  rejects skipped tests, and uploads contract and full-daemon evidence. The
  normal pull-request contract suite is explicitly RPC-free instead of silently
  reporting skipped fork cases as a green test run.

The 2026-07-21 executor reliability run established:

- The pinned block `21,000,000` Solidity matrix passed all seven real-protocol
  and authorization cases with `7 passed; 0 failed; 0 skipped`.
- The full daemon matrix passed at live Ethereum block `25,583,844` for every
  default-enabled family: Uniswap V2, Uniswap V3, PancakeSwap V3, and mixed
  Uniswap V2 to Curve. All four receipts succeeded, and each mined recipient
  delta exactly equaled both the route quote and exact-block executor
  simulation. The machine-readable record is
  [sidecar-executor-e2e-evidence.json](../sidecar-executor-e2e-evidence.json).
- The public endpoint available during this run allowed pinned contract calls
  but rejected the historical account-state reads Anvil needs to build the
  pinned full-daemon fork. The pinned daemon matrix is therefore automated but
  still needs its first run with the configured archive-RPC CI secret; the
  live-head run is strong execution evidence, not a substitute for that gate.
- The Balancer V2 generated-route characterization failed before search with
  `prepared AMM pool ... requires proof for 1 whole-account dependencies`.
  Its direct executor fork case passes, isolating the blocker to upstream
  sidecar hydration. Balancer is consequently absent from the default executor
  allowlist.
- The pull-request gate builds and smokes the exact image, runs the local-chain
  load/recovery contract, generates an SBOM, blocks fixed critical image
  vulnerabilities, and retains its evidence. The nightly gate adds a
  five-minute outage, ten reconnect cycles, provider restart, shallow reorg,
  and fixed high-severity vulnerability policy.

The initial 2026-07-21 quoter hardening run established:

- 40 Rust unit tests and 22 HTTP tests pass. The HTTP suite includes 64-way
  parallel quote correctness and fail-fast overload behavior at an exact
  configured capacity.
- The live daemon completed 128 mainnet-fork WETH-to-USDC quotes at concurrency
  32 without an error.
- A fault-injected five-second canonical websocket outage recovered and caught
  up, while a forty-second outage became terminal: readiness failed after 26
  seconds and did not recover after the connection returned.
- In that terminal `untrusted` state the direct indicative quote endpoint still
  returned HTTP 200 from a stale graph. This was the acceptance failure used to
  drive the hardening work below.

The follow-up delivery closes that failure:

- Readiness and both quote endpoints now share a trust/freshness policy and
  generation fence. Stale, reconnecting, and untrusted state returns a stable
  503 and no route.
- A sidecar-owned supervisor invalidates terminal generations, recreates the
  provider/runtime, rotates ordered websocket endpoints, and retries
  indefinitely while liveness remains healthy.
- 43 unit tests and 26 HTTP tests pass, including stale/untrusted and in-flight
  recovery cases. Three graph-index integration tests cover exact counters,
  topology reconciliation, and real runtime add/remove deltas. Strict
  all-target linting also passes.
- Live fault injection passed 128 concurrent quotes, a 40-second dual-endpoint
  outage, fresh-generation failover to endpoint index 1, a five-second
  four-block catch-up, and a healthy post-recovery quote. A separate five-minute
  dual-endpoint outage also remained fail-closed and recovered on the fallback.
- The digest-pinned 43.0 MB image then passed the complete deterministic matrix:
  128 quotes at concurrency 32, a five-minute dual-endpoint outage, DNS
  fallback rotation, independent state-provider failure, ten missed-block
  catch-ups, a provider process restart, and a depth-three reorg. It remained
  fail-closed, advanced routing generation 1 to 3 and then 5, finished healthy,
  and grew by only 794,624 bytes across the recovery sequence.
- Two consecutive warm builds produced the identical image digest
  `sha256:39c4ec8d22c07756a37f92f83ae54f94c483788c217cecf86f08f2155216db94`.
  Its local SBOM contained 153 packages and the exact-image scan found no fixed
  high or critical vulnerabilities.
- The release-mode graph benchmark reduced two-token admission metadata from an
  estimated 233.5 ms of reconstruction/walking to 81.95 ns at 8,192 pools on
  the measured host. A real one-pool graph commit took 1.02 us, and 32 reader
  threads sustained about 1.72 million complete metadata admissions per second.
  These are isolated metadata measurements, not end-to-end route throughput;
  see [GRAPH_INDEX_BENCHMARKS.md](GRAPH_INDEX_BENCHMARKS.md).
- The post-change arm64 release image
  `sha256:383ca8a91a410929ccee1ec6b59302491e85d94ba590345ac1c91bfebd4c9fa8`
  is 43,040,078 bytes and runs as user `router`. That exact image passed 128
  local-chain quotes at concurrency 32 (p50 20.076 ms, p95 136.559 ms, p99
  177.952 ms), failed closed with HTTP 503 during a 40-second dual-endpoint
  outage, recovered from generation 1 to 3 on fallback endpoint 1, caught up
  four blocks, and finished healthy with a successful quote.

## Blocking release gates

Do not describe this as generally production-ready until all of these gates are
closed:

1. Obtain an independent security review of the executor, including every
   enabled protocol family, approval mode, callback path, and malicious-token
   assumption. Treat this as a hard blocker for public value-bearing traffic.
2. Add pinned fork cases for each target-chain Slipstream or Solidly deployment
   before adding those families to `executor.allowed_protocols`. Their encoders
   are unit-tested, but they are intentionally absent from the default
   allowlist.
3. Fix the upstream Balancer V2 prepared-state ownership model so the pool's
   exact runtime and storage dependencies can be certified without claiming an
   unverifiable whole account, then make the sidecar-generated Balancer case
   pass before restoring it to the default allowlist. The direct contract-fork
   execution case already passes; route hydration is the remaining blocker.
4. Extend the delivered CI/nightly load and recovery gates with a multi-hour
   soak, request flood, packet-blackhole/half-open transport, delayed or
   malformed provider responses, and a reorg beyond retained lineage against
   the intended deployment topology. Set explicit SLOs for readiness, quote
   latency, simulation failures, graph lag, and provider errors.
5. Export production metrics and alerts. Structured logs and status endpoints
   are present, but an operator still needs dashboards and paging for graph
   freshness, work queues, canonical subscription health, and executable-quote
   rejection rates.
6. Deploy and verify the audited bytecode on every target chain, pin the
   runtime hash in configuration, run preflight, and execute a low-value canary
   for every enabled authorization and protocol family.
7. Define the caller's signing and submission policy. The sidecar deliberately
   does not hold keys, send transactions, set fee/tip policy, choose public
   versus private submission, or protect against post-simulation MEV.

## Known product limitations

- Search ranks gross output. It does not optimize net output after gas, L1 data
  fees, approvals, tips, or gas-token conversion.
- Routes are single-path; there is no split routing or order decomposition.
- Intermediate hops have no per-hop minimum. The executor protects the final
  recipient amount and reverts the entire transaction on failure.
- Cache persistence is disabled, so cold-start time and provider capacity are
  part of every restart's availability budget.
- Discovery and search cover only configured tokens, connectors, protocols,
  factories, manual pools, hop limits, and candidate budgets.
- Token allowlisting and risk policy remain operator responsibilities. Exact
  balance-delta checks reject fee-on-transfer input/output behavior but cannot
  make arbitrary tokens trustworthy.

## Deployment checklist

- Pin image digest, configuration, chain id, executor address, runtime code
  hash, WETH, Permit2, tokens, factories, pools, and protocol allowlist.
- Use independent canonical WebSocket and load-balanced proof-capable state
  endpoints with sufficient historical access and documented rate limits.
- Keep the service private or authenticate it at the ingress; bind refresh and
  prewarm operations to a strong bearer token.
- Run configuration validation, executor preflight, Rust/Solidity tests, the
  target-chain fork matrix, the full daemon-to-chain E2E, and the container
  smoke test from the exact release revision.
- Start with value and rate limits, monitor every rejection class, and retain a
  fast rollback path to the previous image and executor allowlist.
