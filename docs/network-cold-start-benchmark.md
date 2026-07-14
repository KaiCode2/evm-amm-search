# Network cold-start benchmark

Measured on 2026-07-12 against Ethereum mainnet blocks `25517240..25517604`.
This document records both the investigation baseline and the final
implementation benchmark.

## What is measured

The headless TUI benchmark runs the same progressive bootstrap as the
interactive application. Its transport observer sits:

- after `BatchingTransport` and before `LoadBalancedTransport` for state HTTP,
  so one observation is one physical HTTP JSON-RPC packet;
- around the canonical WebSocket transport, so subscription setup and
  reconciliation calls are visible too.

It records phase, method, logical calls, physical packets, request/response JSON
size, in-flight concurrency, latency, and exact duplicates keyed by
`method + params`. JSON sizes are uncompressed serialized sizes; gzip wire bytes
are not measured. Endpoint credentials and full RPC URLs are never printed.

The provider ceiling example uses the same state-override storage extractor as
`evm-fork-cache`, forces one `eth_call`, and runs each configured provider in
isolation so load-balancer failover cannot hide a rejection.

## Reproduction

Cold progressive bootstrap with the production worker concurrency (`8`):

```sh
AMM_ROUTE_TUI_BENCH=1 \
AMM_ROUTE_TUI_NETWORK_PROFILE=1 \
AMM_ROUTE_TUI_PERSIST_CACHE=0 \
AMM_ROUTE_TUI_PRICES=0 \
AMM_ROUTE_TUI_GAS_ESTIMATES=0 \
AMM_ROUTE_TUI_BENCH_BOOTSTRAP_TIMEOUT_SECS=120 \
AMM_ROUTE_TUI_BENCH_ROUTE_TIMEOUT_SECS=1 \
cargo run --bin amm-route-tui
```

Explicit concurrency override:

```sh
AMM_ROUTE_TUI_BENCH=1 \
AMM_ROUTE_TUI_BENCH_COLD_START_CONCURRENCY=32 \
AMM_ROUTE_TUI_PERSIST_CACHE=0 \
cargo run --bin amm-route-tui
```

Provider payload sweep:

```sh
AMM_NETWORK_BENCH_SIZES=10000,25000,35000,40000 \
cargo run --example rpc_payload_ceiling
```

Use `AMM_NETWORK_BENCH_PROVIDER_INDEX` to isolate one configured endpoint.

## Baseline cold-start call graph

The representative balanced, in-memory cold run warmed `252` pools (`3`
additional candidates failed) and reached live runtime handles in `45.36s`:

| Phase | Wall time | Share of readiness |
|---|---:|---:|
| Connect, canonical baseline, cache construction | `3.99s` | `8.8%` |
| Initial graph warmup | `13.56s` | `29.9%` |
| Canonical subscriber attach | `27.81s` | `61.3%` |
| Total to runtime handles | `45.36s` | `100%` |

The transport profile contained `762` logical calls in `759` physical packets:

| Method | Calls | Request JSON | Response JSON | Max response | Interpretation |
|---|---:|---:|---:|---:|---|
| `eth_subscribe` | `255` | `78.9 KiB` | `25.8 KiB` | `104 B` | One sequential subscription per pool/filter plus the block stream |
| `eth_call` | `253` | `221.4 KiB` | `5.63 MiB` | `701.8 KiB` | Pool discovery/cold-start extraction work |
| `eth_getProof` | `248` | `52.3 KiB` | `1.89 MiB` | `8.1 KiB` | Account/root verification for prepared state |
| `eth_getBlockByNumber` | `4` | `341 B` | `~116 KiB` | `~29 KiB` | Canonical baseline, cache pin, and subscriber certification |
| `eth_chainId` | `2` | `94 B` | `78 B` | `39 B` | State and canonical provider metadata |

Three exact duplicates existed, all in chain/block metadata. There were no
exact duplicate `eth_call` or `eth_getProof` requests.

### Batching effectiveness

The batch-size histogram was `756` packets of one call and `3` packets of two
calls. Transparent batching therefore saved only three physical requests
(`0.4%`) at the production worker concurrency. Even at concurrency `32`, only
ten two-call packets formed. The cold-start worker emits mostly staggered,
per-pool request sequences, so a 1ms accumulator rarely sees simultaneous work.

### Response-volume concentration

State HTTP returned `7.55 MiB`; `eth_call` accounted for `5.63 MiB` (`~75%`)
and `eth_getProof` for `1.89 MiB` (`~25%`). The WebSocket attach phase moved
only about `164 KiB` of JSON in both directions, yet cost `27.5s`. That phase is
latency/sequencing bound, not bandwidth bound.

The subscriber currently issues `255` `eth_subscribe` calls sequentially. This
explains almost the entire attach delay: each small request pays another
WebSocket request/response round trip.

## Provider and concurrency comparisons

Single-trial, in-memory cold runs with the same `252` ready pools:

| State transport | Worker concurrency | Graph warmup | Runtime handles | Transport errors |
|---|---:|---:|---:|---:|
| QuickNode only | `8` | `17.14s` | `48.49s` | `0` |
| Alchemy only | `8` | `13.69s` | `42.51s` | `0` |
| Balanced QuickNode + Alchemy | `8` | `13.56s` | `45.36s` | `0` |
| Balanced QuickNode + Alchemy | `16` | `8.55s` | `39.90s` | `0` |
| Balanced QuickNode + Alchemy | `32` | `7.34s` | `38.64s` | `0` |
| Balanced QuickNode + Alchemy | `64` | `7.34s` | `44.14s`* | `0` |

`*` The concurrency-64 run had a `6.53s` cache-header outlier before warmup;
warmup itself did not improve over concurrency 32.

Equal-weight balancing did not beat the faster endpoint at concurrency 8. The
material gain came from increasing independent pool work: concurrency 32 cut
graph warmup by `46%` and handle readiness by `15%`. Concurrency 64 provided no
additional warmup gain and increased tail latency.

## Persistent-cache comparison

The existing TUI cache was `61 MiB` (`30.5 MiB` state, `21.4 MiB`
registrations, `12.1 MiB` bytecode). A persistent-cache run still issued
`253 eth_call`, `248 eth_getProof`, and `255 eth_subscribe` calls. It warmed the
graph in `12.96s` and reached handles in `45.22s`, effectively unchanged from
the in-memory cold baseline. It did, however, produce a provisional route
immediately after handle readiness; the in-memory runs did not do so within the
short post-readiness observation window.

The persisted cache therefore improves post-bootstrap usability but does not
currently eliminate pinned-block revalidation or subscription setup.

## Bulk storage payload ceilings

The limit is serialized request-body size, not a universal slot count. Each
additional slot adds 64 JSON bytes in this extractor envelope.

| Provider | Largest accepted | Request JSON | First rejected | Request JSON | Failure |
|---|---:|---:|---:|---:|---|
| Alchemy | `39,058` slots | `2,499,951 B` | `39,059` slots | `2,500,015 B` | HTTP `413` |
| QuickNode | `81,916` slots | `5,242,863 B` | `81,917` slots | `5,242,927 B` | HTTP `413` |

The boundaries align exactly with provider body caps of `2,500,000 B` and
`5 MiB`, respectively. They are envelope-sensitive: another method, extra
override data, or a larger JSON field changes the maximum slot count.

At the candidate `25,000`-slot size, the exact request and response JSON were
`1,600,239 B` and `1,600,038 B`. Both providers accepted it; the observed
single-call latencies were `1.45s` on Alchemy and `1.96s` on QuickNode. This is
`64%` of Alchemy's request cap and `31%` of QuickNode's.

The current TUI cold-start never exercised this limit: its largest real request
was only `8.8 KiB`. Raising the 10k-slot extractor default would therefore not
change this measured TUI startup, though it can help other workloads that
actually produce multi-thousand-slot chunks.

## Evidence-ranked conclusions

1. The largest present cold-start cost is sequential WebSocket subscription
   setup (`255` round trips, `~27.5s`).
2. HTTP pool loading is parallelism-bound. Worker concurrency `32` was the best
   measured point; `64` showed no further warmup gain.
3. Transparent JSON-RPC batching is correctly installed but is almost idle for
   this workload because requests do not arrive together.
4. There are no meaningful duplicate pool reads to delete. Metadata duplication
   is tiny compared with pool loading and subscription setup.
5. Equal-weight provider balancing is not automatically faster when endpoint
   latency differs; the slower endpoint can dilute the faster one.
6. A `25k` bulk limit is supported by both configured providers with useful
   safety margin, but byte caps—not provider names or a global slot count—are
   the durable constraint.
7. Warm persistence currently improves first-route availability, not network
   cold-start cost.

These findings formed the baseline for the implementation below.

## Implemented networking changes

- Compatible owner log filters now fan into provider-side address/topic
  supersets while exact owner routing remains local. Address arrays split at a
  configurable ceiling (default `1,024`).
- The TUI uses adaptive cold-start concurrency: `16` for one state endpoint and
  `32` for two or more, with TOML and environment overrides.
- Cold-start, discovery, and repair workers now share the configured storage
  batch/fetch strategy instead of constructing hard-coded defaults.
- TUI bootstrap prewarms its three shared Router02/QuoterV2 accounts before
  publishing actor snapshots, keeping first-route simulation offline even when
  migrating from an uncertified legacy cache.
- The TUI bulk extractor defaults to `25,000` slots with a `2.4 MB` byte guard.
  Both limits are user-configurable; byte planning clamps slot planning.
- State endpoints have configurable weights, maximum request bytes, and
  maximum in-flight requests. Oversized packets skip ineligible endpoints and
  HTTP `413` participates in failover. Known Alchemy and QuickNode hosts use
  measured safe profiles; unknown providers use conservative limits.
- Persistent shutdown writes the actor-owned cache, registration archive, and
  canonical block-hash checkpoint into a private generation, syncs and seals
  it, then atomically replaces a synced manifest last. Restart trusts only the
  manifest-selected complete generation, verifies its checkpoint, restores
  ready pools at the certified block, subscribes first, replays every
  intervening canonical block, waits for repair work to settle, and only then
  exposes route handles. Interrupted generations leave the prior commit intact;
  invalid checkpoints discard persisted EVM state and fall back to cold start.

The generation layout, exact commit order, legacy migration behavior, and crash
failure matrix are in
[`warm-checkpoint-consistency.md`](warm-checkpoint-consistency.md).

All production settings are exposed in `[network]` in
`.amm-route-tui.toml.sample`; existing RPC environment variables remain
supported and take precedence.

## Final cold benchmark

Two independent in-memory runs used the new production defaults and warmed the
same `252` pools (`3` failed candidates):

| Metric | Baseline | Final trial A | Final trial B | Improvement |
|---|---:|---:|---:|---:|
| Graph warmup | `13.56s` | `6.26s` | `5.49s` | `54–60%` faster |
| Subscriber attach | `27.81s` | `0.66s` | `0.69s` | `97.5–97.6%` faster |
| Runtime handles | `45.36s` | `9.10s` | `8.20s` | `79.9–81.9%` faster |
| `eth_subscribe` | `255` | `2` | `2` | `99.2%` fewer |
| Physical packets | `759` | `496` | `498` | `34.4–34.7%` fewer |
| Logical calls | `762` | `511` | `511` | `32.9%` fewer |
| Max in flight | `8` | `32` | `32` | configured optimum |
| Transport / RPC errors | `0 / 0` | `0 / 0` | `0 / 0` | unchanged clean |

The average handle time was `8.65s`, an `80.9%` reduction from the measured
baseline. Subscriber fan-in accounts for the largest gain; higher bounded HTTP
parallelism accounts for the graph-warmup reduction. Response volume remains
about `7.6 MiB`, as expected: these changes remove latency and redundant
provider subscriptions, not the authoritative pool state reads.

The real cold path still produced a maximum request of only `11.9 KiB`, so the
new `25k`/byte-aware bulk policy is exercised by large tick/balance workloads,
not this particular 252-pool startup basket.

## Final verified-warm benchmark

The final persistent run restored `279` ready pools at canonical block
`25517597`, subscribed, replayed blocks `25517598..25517604`, admitted one pool
created during that window, and exposed `280` ready pools:

| Metric | Previous persistent behavior | Verified warm resume |
|---|---:|---:|
| Runtime handles | `45.22s` | `5.86s` |
| First usable route | after handles | `5.86s` |
| Physical / logical RPC calls | `~759 / 762` | `33 / 33` |
| `eth_subscribe` | `255` | `3` |
| State response volume | `~7.55 MiB` | `493.7 KiB` total response JSON |
| Pending/loading/queued at handoff | not eliminated | `0 / 0 / 0` |
| Transport / RPC / post-ready failures | `0 / 0 / 0` | `0 / 0 / 0` |

That is an `87.1%` reduction in warm handle time. Warm latency now scales with
the number of canonical blocks missed since the checkpoint: this trial replayed
seven blocks and issued `14 eth_getLogs` calls plus six parent-header fetches.
The runtime does not trust stale state merely because it was persisted; the
hash check, subscribe-first overlap, ordered block replay, exact local routing,
and repair-settle fence are all required before readiness.

## Crash-consistent generation verification

After replacing independent cache/archive/checkpoint writes with the manifest
generation store, the first migration run deliberately ignored uncertified
legacy EVM state, cold-started `289` pools, and committed the first generation.
It also exposed and fixed two cold-fallback gaps: restored batches can exceed
the worker's 256-job floor, and progressive per-pool hydration must explicitly
prewarm the shared quote entrypoints that `cold_start_many` normally warms at
batch scope.

Two immediate verified-warm samples from the committed generation each replayed
one block and measured:

| Metric | Crash-consistent warm samples |
|---|---:|
| Runtime handles | `3.656–4.223s` |
| First usable provisional route | `3.700–4.266s` |
| Ready pools | `293` |
| Physical / logical RPC calls | `10 / 10` |
| `eth_subscribe` | `2` |
| Transport / RPC / runtime-work failures | `0 / 0 / 0` |

The final warm path issues no `eth_getCode`, `eth_getProof`, or `eth_call`:
hash-pinned Router02/QuoterV2 accounts fetched during cold/migration bootstrap
are part of the committed generation. Reusing them makes the first search
snapshot offline-quoteable without network revalidation or old uncertified
cache residue. Both runs produced a USDC → WETH route through SushiSwap V2.

A preceding catch-up-heavy trial replayed 28 blocks, admitted one newly created
pool, and reached handles/first route in `36.326s` / `36.369s` with no errors.
This confirms the earlier scaling caveat: when a checkpoint is far behind,
ordered canonical replay—not generation copying or manifest commit—dominates
warm latency.

## Release-candidate focused cold start

The release-candidate TUI separates the configured focus pair from background
basket discovery and defines readiness as a successful quote against the
immutable graph snapshot. A fresh-cache Ethereum mainnet run at block
`25519162` measured:

| Milestone | Elapsed |
|---|---:|
| First successful offline focus quote | `3.264s` |
| Runtime handles published | `3.953s` |
| First provisional USDC → WETH route | `8.587s` |

At the first route the graph contained `16` ready pools at state version `18`.
The run used `65` physical packets for `74` logical calls, transferred
`61.7 KiB` of request JSON and `2.51 MiB` of response JSON, and reached `29`
requests in flight. It recorded zero transport errors, RPC errors, or runtime
work failures before shutdown. The method totals were `22 eth_getBlockByNumber`
calls, `19 eth_call` calls, `15 eth_getProof` calls, `11 eth_subscribe` calls,
and `6 eth_getCode` calls.

This result measures first-route latency rather than completion of the entire
startup basket, so it is not directly interchangeable with the earlier
252-pool full-warmup table. It verifies the intended user-facing critical path:
the requested market becomes quoteable and searchable while unrelated pool
discovery continues in the background. Shutdown detected that background work
was still active and correctly declined to publish an incomplete checkpoint
generation, preserving the prior manifest.

## Final stable-baseline TUI regression benchmark

The post-ready basket design above was retired after interactive testing showed
that subscriber attachment advanced the canonical baseline while slower V3
pool hydration was still in flight. V2 jobs commonly completed inside one block;
V3 jobs became stale and retried, producing an apparent V2-only TUI. Exhaustive
search also restarted on every block before reaching terminal finality.

The corrected startup admits one bounded set before subscriber attachment and
waits for discovery, hydration, and pending state updates to become idle. The
default `AMM_ROUTE_TUI_MAX_POOLS=128` applies to restored hints as well as fresh
discovery; a fresh-cache run admitted 77 viable pools. Interactive search now
finishes at its heuristic boundary unless explicitly configured otherwise.

Final Ethereum mainnet capture at block `25519820`:

| Metric | Result |
|---|---:|
| Runtime handles | `12.276s` |
| First provisional route | `12.280s` |
| Ready / failed pools | `77 / 0` |
| Protocols | Curve 3, Pancake V3 21, Sushi V3 6, Uniswap V3 25, V2 22 |
| Physical / logical calls | `209 / 237` |
| Request / response JSON | `511.8 KiB / 4.67 MiB` |
| Max requests in flight | `32` |
| Transport / RPC / runtime-work failures | `0 / 0 / 0` |
| Direct USDC to WETH quotes | Pancake V3 `4/0`, Sushi V3 `4/0`, Uniswap V3 `4/0`, V2 `1/0` |

Pancake parity required two snapshot dependencies that canonical Uniswap/Sushi
do not: exact-proof installation of the real fork pool runtime, and the second
word of Pancake's wider `slot0` struct (which carries `unlocked`). Snapshot V3
execution also substitutes a call-scoped success-only ERC-20 runtime for the
output transfer immediately preceding QuoterV2's intentional quote-data revert.
The override is restored after each call and never changes pool math, ticks, or
the shared immutable snapshot.
