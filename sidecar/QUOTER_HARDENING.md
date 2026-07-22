# Quoter hardening assessment

Assessment date: 2026-07-21.

This document records the current quote service boundary, the evidence gathered
from concurrency and websocket-failure testing, and the order in which the
remaining robustness work should be tackled. The executor security gates in
`PRODUCTION_READINESS.md` remain separate; this assessment is about keeping the
quoter correct and available under load and provider failure.

## Delivered hardening state

- Axum accepts requests on a multi-threaded Tokio runtime. Quote admission is
  bounded by `server.max_in_flight_quotes` (64 by default) and fails fast with
  HTTP 429 and the stable `quote_capacity` code when all permits are occupied.
- Each accepted request creates an independent, ephemeral route subscription.
  CPU search work runs on the route runtime's persistent worker threads. The
  worker count and maximum subscriptions are configurable; the internal job,
  actor-command, and result queue capacities currently use library defaults.
- Discovery and executor simulation have separate semaphores, so they cannot
  consume quote permits without also remaining inside their own configured
  limits. Request size, hop count, candidate count, result count, and search
  duration are bounded by service policy.
- Canonical updates use a primary websocket plus an ordered fallback list.
  Transport retry and terminated-stream backfill policies are configurable;
  terminated streams retry indefinitely by default.
- A sidecar-owned supervisor tracks the last observed canonical point, runtime
  trust, and subscriber state. A terminal, untrusted, or stale generation is
  removed from traffic before its provider/runtime is shut down. The supervisor
  then rotates endpoints and retries fresh verified generation builds
  indefinitely with capped exponential backoff and jitter.
- `/readyz`, indicative quotes, and executable quotes share one trust/freshness
  gate. They return stable 503 errors while stale, untrusted, reconnecting, or
  shutting down. Requests record the routing generation before work and are
  rejected if recovery crosses the request in flight.
- `/v1/status` exposes the generation, connection state, endpoint index/count,
  canonical observation age, subscriber state, retry count, and last sanitized
  recovery error. Endpoint URLs are never returned.

## Evidence before hardening

The normal sidecar suite passed 40 unit tests and 22 HTTP tests. Two of the HTTP
tests are now explicit concurrency characterization gates:

- 64 concurrent synthetic quotes on four workers all completed successfully and
  preserved their request-specific input/output values. The local sample took
  60 ms end to end.
- With four quote permits, a simultaneous 20-request synthetic burst admitted
  exactly four and immediately rejected 16 with HTTP 429 `quote_capacity`.

The full daemon was then run against a mainnet-forked Anvil node, with its
canonical websocket routed through Toxiproxy:

- 128 WETH-to-USDC quotes at concurrency 32 all succeeded. Observed request
  latency was p50 48 ms, p95 133 ms, and p99 175 ms.
- A five-second websocket outage recovered automatically. After the proxy was
  restored, the AMM runtime caught up through four missed/new blocks and quotes
  remained usable.
- `/readyz` continued to return HTTP 200 during that five-second blind window;
  the service exposes no reconnecting or canonical-lag state.
- A forty-second websocket outage caused readiness to fail after 26 seconds.
  Restoring the proxy did not recover the runtime during the following
  15-second observation window. It remained `untrusted` and three blocks behind.
- Despite that terminal untrusted state, `POST /v1/quote` still returned HTTP
  200 using the stale graph. This is the most important correctness failure
  found by the assessment.

The latency numbers are characterization data from a heavily contended local
workstation and a two-pool graph. They prove concurrent request correctness but
are not production capacity claims or SLOs. The reusable harness is
`scripts/quoter-hardening-anvil.sh`; its topology is defined in
`examples/anvil-quoter-hardening.toml`.

## Hardening delivery evidence

The completed implementation passed 43 unit tests and 26 HTTP tests. The new
HTTP gates cover stale indicative quotes, untrusted executable quotes, readiness
agreement, and an in-flight generation change. Formatting, `git diff --check`,
shell syntax, and strict all-target Clippy also pass.

The full daemon was re-run against the fault-injected mainnet fork:

- 128 WETH-to-USDC requests at concurrency 32 all returned correct quotes. The
  final workstation sample observed p50 135 ms, p95 503 ms, and p99 622 ms;
  these remain contended local characterization data, not an SLO.
- Both canonical proxies were disabled for 40 seconds. Readiness closed after
  four seconds and direct quotes returned HTTP 503
  `canonical_reconnecting`; no stale route was emitted.
- Only the fallback proxy was restored. The process stayed alive, built a fresh
  verified runtime, advanced from generation 1 to generation 3, selected
  endpoint index 1, and reopened quote traffic without a restart.
- A following five-second outage on that recovered feed kept readiness HTTP 200
  inside the configured grace window, then caught up four locally mined blocks.
  Runtime health finished `healthy` and the post-recovery quote returned 200.
- A separate five-minute dual-endpoint outage remained fail-closed and also
  recovered generation 3 on endpoint index 1. Its initially combined short-leg
  check waited beyond the 15-second test grace and triggered a correct rebuild
  against locally mined hashes that the remote state provider cannot serve.
  The harness timing was corrected, and the complete 40-second mixed sequence
  above then passed. The five-minute outage and recovery boundary itself passed.

The first attempted live run exceeded the harness startup window while the
debug binary was still linking; the daemon had not started. Subsequent runs use
a prebuilt binary so build time is excluded from recovery measurements.

The exact hardened image is now a validated release artifact:

- The Rust and Debian base images and the Toxiproxy fault image are pinned by
  digest. The Docker context contains only the sidecar manifest, lockfile, and
  sources. Release compilation uses four bounded jobs, LLD, stable BuildKit
  cache identifiers, and stripped symbols.
- The first cold release compile completed in 30 minutes 32 seconds on the
  heavily contended workstation instead of disappearing into an unbounded GNU
  linker phase. Two following builds completed in nine and two seconds and
  produced the identical image digest
  `sha256:39c4ec8d22c07756a37f92f83ae54f94c483788c217cecf86f08f2155216db94`.
- The 43.0 MB `linux/arm64` image runs as the non-root `router` user and passed
  the read-only configuration and healthcheck smoke gate.
- The exact image completed 128 local-chain quotes at concurrency 32 (p50
  19.620 ms, p95 117.169 ms, p99 123.952 ms), a five-minute dual-endpoint
  outage, ten missed-block recovery cycles, independent state-provider
  failure, DNS endpoint rotation, an Anvil process restart, and a depth-three
  reorg. Readiness closed after 14 seconds, stale quotes returned HTTP 503, the
  fallback rebuild advanced generation 1 to 3, the process restart advanced it
  to 5, and the final runtime was healthy at the replacement block hash.
- Container memory grew by 794,624 bytes across that complete recovery matrix,
  below the 128 MiB gate. The post-recovery quote returned HTTP 200.
- The generated image SBOM contained 153 packages. A local scan found no fixed
  high- or critical-severity vulnerability. Pull-request and nightly workflows
  retain the image identity, structured recovery evidence, logs, SBOM, and
  vulnerability report as downloadable artifacts.

## Ranked hardening backlog

### 1. Fail closed on untrusted or stale routing state — safety contract delivered

This is the first change. A quoter that returns a plausible HTTP 200 from a
known-stale graph is worse than one that is unavailable.

Implementation boundary:

- Gate both quote endpoints on runtime trust and a configurable maximum
  canonical-head age. Return a stable 503 error that distinguishes
  `canonical_reconnecting`, `canonical_stale`, and `runtime_untrusted`.
- Track last canonical block number, hash, timestamp, and local observation
  time without rebuilding the graph. Include subscriber-driver state and lag in
  `/v1/status`.
- Make `/readyz` use the same state policy. A short configurable grace period is
  acceptable, but readiness and quote admission must agree once it expires.

Acceptance criteria:

- During an outage beyond the configured grace period, readiness and both quote
  endpoints return 503; no stale route is emitted.
- Recovery cannot reopen quote admission until a canonically coherent catch-up
  has completed.

Remaining observability follow-up: status now carries the last block number and
hash, local canonical age, subscriber state, and recovery state, but not the
source block timestamp or a separately polled provider-head lag. Local age is
the enforced fail-closed signal; timestamp and provider-lag telemetry belong in
item 6.

### 2. Add a sidecar-owned websocket recovery supervisor — core delivered

Dependency-level reconnect handles brief faults, but the forty-second test
proved that the service cannot recover after its retry budget is exhausted.

Implementation boundary:

- Expose keepalive, retry, exponential-backoff, jitter, maximum-delay, and
  outage-grace settings in the sidecar profile. Production defaults should
  retry indefinitely while the service remains not-ready.
- Recreate the websocket provider and subscriber after terminal transport or
  subscriber-driver failure. Support an ordered list of canonical endpoints,
  not a single URL.
- Reconcile from the last trusted canonical point, including missed headers and
  logs, and rebuild from a fresh verified baseline when the retained lineage or
  backfill window is insufficient.
- Keep liveness healthy while retrying so the container is not restarted in a
  tight loop; readiness remains false until reconciliation succeeds.

Acceptance criteria:

- The daemon recovers without process restart after 5-second, 40-second, and
  5-minute outages, provider restarts, DNS failures, and endpoint failover.
- Every recovery proves block/hash continuity and catches up before serving.

Remaining transport follow-up: the implemented Alloy release has a fixed
ten-second transport keepalive rather than a configurable one; the independent
freshness watchdog supplies the fail-closed bound. DNS failure, process restart,
state-provider failure, fallback rotation, five-minute outage, and shallow
reorg recovery are now exercised together by the exact-image gate. Explicit
packet-blackhole and delayed-frame toxics remain in item 3.

### 3. Promote chaos, reorg, and stale-state tests into release gates — core delivered

The deterministic local-chain harness now runs the exact release image through
a digest-pinned fault proxy. Pull requests run parallel load, fail-closed
outage, independent state-provider failure, DNS rotation, endpoint failover,
missed-block catch-up, and memory checks. A scheduled and manually dispatchable
nightly job extends the same artifact to a five-minute outage, ten reconnect
cycles, a provider-process restart, and a shallow reorg. Both jobs publish
machine-readable evidence and full failure logs; the nightly also blocks on
fixed high or critical image vulnerabilities.

Implementation boundary:

- Run the daemon or release image through a pinned fault proxy in CI/nightly
  jobs and assert the fail-closed and recovery contracts above.
- Cover websocket close frames, silent half-open connections, delayed traffic,
  malformed/provider errors, short and extended outages, endpoint rotation,
  shallow reorgs, reorgs beyond retained lineage, and state-provider failure.
- Assert source block number/hash monotonicity or explicit reorg transitions;
  never assert availability alone.

Acceptance criteria:

- Tests fail if readiness, quote admission, runtime trust, or catch-up diverge.
- A soak run repeatedly disconnects and reconnects without subscription leaks,
  memory growth, missed canonical updates, or duplicate state application.

Remaining matrix follow-up: add a true packet blackhole/half-open toxic,
delayed and malformed provider responses, a reorg beyond retained lineage, and
a longer multi-hour soak. The current memory assertion measures bounded growth
over ten cycles; it is not yet a leak proof over production timescales.

### 4. Remove per-request graph reconstruction before raising concurrency — core delivered

The request path now uses a sidecar-owned graph index for token membership,
token pool counts, and global graph counts. The index is built once from the
same coherent snapshot as the route runtime, advances from contiguous
`AmmCommitApplied` graph deltas, and publishes a version fence used by token
preparation. Duplicate deltas are ignored. A version gap or lagged route-event
observer triggers one recovery rebuild from the latest coherent AMM snapshot
off the request path.

The removed implementation reconstructed a `LiveAmmGraph` and walked every
edge for each endpoint token. The replacement takes a read lock and performs
hash-map lookups; single-pool graph deltas update only affected pool/token
entries rather than cloning the full index.

The 2026-07-21 release-mode microbenchmark on an Apple M1 Pro used 512 tokens
and 128, 1,024, and 8,192 ready pools. Median estimates were:

| Pools | Removed rebuild + walk, one token | New admission, both tokens | Single-pool commit |
| ---: | ---: | ---: | ---: |
| 128 | 862.86 us | 73.61 ns | 496.05 ns |
| 1,024 | 11.60 ms | 78.39 ns | 819.52 ns |
| 8,192 | 116.76 ms | 81.95 ns | 1.02 us |

At 8,192 pools, the old two-token quote admission would spend about 233.5 ms
on graph metadata before search; the indexed path remained about 82 ns in this
microbenchmark. On the same graph, 1, 8, and 32 concurrent reader threads
completed approximately 11.76 million, 2.30 million, and 1.72 million complete
two-token admissions per second. These figures isolate graph metadata and lock
contention; they are not HTTP or route-search throughput claims. Methodology,
confidence intervals, and the reproducible command are in
[GRAPH_INDEX_BENCHMARKS.md](GRAPH_INDEX_BENCHMARKS.md).

The exact post-change container also passed the deterministic 128-request,
concurrency-32 route gate at p50/p95/p99 20.076/136.559/177.952 ms, followed by
fail-closed 40-second endpoint loss, fallback recovery, four-block catch-up,
and a healthy quote. This supplies an artifact regression result at one
concurrency level; the realistic large-graph matrix below remains open.

Implementation boundary:

- Serve membership and graph counts from the immutable live route view or from
  commit-maintained counters/indexes with O(1) lookups.
- Profile CPU, allocation, queue wait, and search time independently before
  choosing worker counts. Then benchmark realistic graph sizes, route mixes,
  qualities, hop limits, and block-update rates.
- Consider snapshot-keyed request coalescing or a very short-lived result cache
  for identical requests only after correctness and invalidation are proven.

Acceptance criteria:

- Publish p50/p95/p99 latency and throughput at 1, 8, 32, 64, and overload
  concurrency for the intended CPU/memory limit and realistic graph.
- Block ingestion remains within its lag SLO during quote saturation.

Remaining performance follow-up: run the complete HTTP and route-search matrix
against the intended production graph and container CPU/memory limits. The
metadata bottleneck is removed, but end-to-end latency percentiles and block
ingestion lag still require deployment-specific SLOs and measurements.

### 5. Make admission, deadlines, and fairness end to end

The client quote timeout starts after a route subscription has been accepted.
Waiting to send into a saturated actor command channel is outside that timeout,
and every request consumes one equal quote permit even when exhaustive search
is much more expensive than fast search.

Implementation boundary:

- Apply one server deadline from request admission through discovery,
  subscription, search, optional simulation, and response construction.
- Bound or time out actor admission, return `Retry-After` on capacity errors,
  and expose queue wait separately from search time.
- Add weighted or separate capacity for fast, balanced, exhaustive, discovery,
  and executable requests. Add per-caller rate limits at the service or ingress
  so one tenant cannot occupy every global permit.
- Verify cancellation promptly releases subscriptions and worker jobs after
  client disconnects and deadlines.

Acceptance criteria:

- Overload has bounded memory and latency, no request exceeds the advertised
  server deadline, and cheap traffic retains its SLO under expensive traffic.

### 6. Export production metrics and actionable health

Structured logs and status JSON are not enough to operate the quoter under
load or during provider incidents.

Required signals include request totals/latency/rejections by endpoint and
result class; active permits; command/job/result queue depth; worker occupancy;
search candidates; canonical block age and lag; websocket connection state,
reconnect attempts and outage duration; catch-up/reorg counts; provider request
latency/errors; graph revision; and runtime trust transitions.

Acceptance criteria:

- Dashboards and alerts can distinguish overload, slow search, stale canonical
  state, provider failure, graph failure, and executor simulation failure.
- Cardinality is bounded and request identifiers remain logs/traces rather than
  metric labels.

### 7. Prove state-provider failover and isolate RPC workloads

HTTP state reads already support weighted endpoints, but failover, rate-limit,
bad-data, batching, and recovery behavior have not been exercised as a service.
Executable canonical checks and simulations currently share the canonical
websocket provider, which can make transaction-building traffic compete with
the feed that keeps quotes fresh.

Acceptance criteria:

- Hydration and refresh continue across individual HTTP endpoint failures and
  fail closed on inconsistent state.
- Canonical subscription traffic, bulk state reads, and execution simulation
  have independent capacity, timeout, and failover policies.

### 8. Close the container and rollout availability gaps

- Set explicit CPU/memory limits, graceful-drain and hard-stop deadlines,
  connection limits, log rotation, restart backoff, and disruption budgets.
- Base and fault images are digest-pinned, and the exact built image now has an
  SBOM and blocking vulnerability scan. Add signed build provenance and pin the
  deployed image digest in the target environment.
- Reduce verified cold-start time or add safe cache/checkpoint restoration so a
  restart does not require a full provider-heavy rebuild before readiness.
- Prove multiple replicas can roll without synchronized cold starts exhausting
  providers and without exposing divergent/stale quotes.

## Recommended next implementation slice

The fail-closed supervisor, deterministic exact-image release gate, and
request-path graph index are ready. Move to item 5: enforce one end-to-end
deadline across admission, discovery, search, simulation, and response
construction, with fair capacity for cheap and expensive work. Implement item
6 alongside it so the resulting load tests have explicit latency, queue, graph
lag, and freshness SLOs rather than pass/fail availability alone.
