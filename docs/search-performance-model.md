# Search Performance Model

This crate is optimized around a warmed-cache routing loop:

1. `evm-amm-state` discovers, cold-starts, and keeps AMM state fresh.
2. `AmmGraph` turns ready pools into a directed token graph.
3. `AmmSearcher` schedules candidate paths, simulates swaps against the warmed
   cache or overlay snapshots, and streams improving quotes.

Most production latency comes from swap simulation count and cache readiness.
The search layer still has meaningful CPU work, especially when graph fan-out is
large, so the current implementation keeps the graph scheduler allocation-light
and cache-friendly.

## Graph Storage

`AmmGraph` owns the canonical graph as:

```text
StableDiGraph<Address, EdgeData>
```

Token addresses are nodes. Each directed edge stores the `PoolKey` that can
execute that token-in to token-out swap. Multi-token pools add all directed
token pairs, and parallel pools remain separate edges.

The graph also maintains index maps:

| Index | Purpose |
| --- | --- |
| `node_map` | Token address to stable graph node. |
| `edge_map` | Pool key to all directed graph edges for that pool. |
| `pool_tokens` | Pool key to token set, used by mutation and liquidity tracking. |
| `pair_pools` | Directed token pair to parallel pool set. |
| `outgoing_edges` | Cached hot-loop view of outgoing edges by source node. |

The canonical `StableDiGraph` remains the source of truth for public graph
identity. The cached outgoing-edge view exists only to remove repeated
petgraph/node lookups from route expansion.

## Cached Outgoing Edges

Search hot loops repeatedly need the same fields:

```text
source node -> target node
token_in -> token_out
pool key
```

Without the cache, every expansion walks petgraph edges, reads the target node
weight, reads the current node weight, and clones the pool key from `EdgeData`.
The cached view stores exactly the fields needed by the scheduler:

```text
CachedOutgoingEdge {
    target,
    token_in,
    token_out,
    pool,
}
```

The cache is updated whenever pool edges are added or removed. Pool removal only
cleans the source-token buckets that the removed pool used, rather than scanning
every token bucket. Compacting removal also removes the orphan node's outgoing
bucket.

One subtle point: the cached view mirrors petgraph's outgoing-edge iteration
order. Some heuristic tests intentionally depend on deterministic parallel-edge
ordering before prefix dominance runs. Preserving iteration order keeps the
optimization behavior-preserving.

## Request-Local Scheduler Cache

Heuristic search computes token centrality several times while building the
fast lane and frontier ordering. A single request now keeps:

| Cache | Scope | Why it is safe |
| --- | --- | --- |
| token degree by `NodeIndex` | one heuristic request | Graph topology cannot change during one search call. |
| connector score by token | one heuristic request | Liquidity freshness is evaluated from the attached index at request time. |

These caches are not stored on `AmmSearcher`. They are created for one
heuristic run and discarded when the request finishes, so live graph updates or
liquidity refreshes cannot inherit stale scheduler scores.

## Quote Cache And Simulation Reuse

Search also shares a quote cache across heuristic, streaming, incremental, and
parallel phases. The quote cache is keyed by hop/path inputs and prevents
re-running the same deterministic warmed-cache simulation during one search
session.

This is separate from the graph scheduler caches:

| Cache | Saves |
| --- | --- |
| outgoing-edge cache | graph traversal and token/pool lookup CPU |
| request scheduler cache | repeated connector/degree scoring CPU |
| quote cache | repeated swap simulations |

## Parallel Search

Parallel route search splits materialized candidate paths across workers. Each
worker runs against an isolated `EvmOverlay` over the same warmed `EvmCache`
snapshot, while sharing the search quote cache. This lets failed or successful
per-hop quotes be reused without mutating the parent cache from worker threads.

Streaming search starts with heuristic-first scheduling, emits best updates as
soon as routes are quoted, and can continue into a deduplicated exhaustive
remainder. Exhaustive mode remains exact inside the configured hop and
candidate bounds.

## Benchmark Snapshot

A synthetic scheduler-only probe was used to measure the graph/search CPU
impact of the latest internal caches. The probe used:

```text
pools=320
tokens=32
iterations=400 warm heuristic searches
max_hops=3
liquidity index attached
mock adapter returning deterministic quotes
debug-profile tests with debug info disabled
```

Results on the local development machine:

| Version | Best | Average |
| --- | ---: | ---: |
| Before cached outgoing-edge view | `4.35ms` | `5.87ms` |
| After cached outgoing-edge view | `4.27ms` | `4.63ms` |

The best case was effectively flat, while average scheduler time improved by
about `21%`. This probe intentionally excludes RPC, cold start, real EVM
execution, and provider variance. Treat it as a low-level scheduler regression
check, not as an end-to-end routing benchmark.

The end-to-end route benchmarks in the README remain the better source for user
latency because they include realistic graph construction, liquidity refresh,
streaming, exhaustive remainder, and live AMM quote behavior.

## Memory Tradeoff

The outgoing-edge cache stores one additional record per directed graph edge.
For two-token pools, that means two cached records per pool. For multi-token
pools, it scales with `n * (n - 1)`, matching the directed graph edge count.

The memory increase is persistent but bounded by graph size. The scheduler
caches are request-local and bounded by the number of touched tokens/connectors
in one heuristic run.

## Safety Boundaries

- The cached outgoing-edge view must be updated on every graph topology change.
- Graph rebuilds naturally rebuild the cache from scratch.
- Pool removal leaves token nodes in place for non-compacting removal, but
  removes every cached edge for the pool.
- Compacting removal also drops orphan token buckets.
- Request-local scheduler caches must not outlive one search request.
- Liquidity-derived scores remain fail-open: stale or unknown liquidity may
  affect ordering, but conservative pruning is disabled when required freshness
  is missing.

## Useful Checks

Run these after changing graph mutation or search scheduling code:

```text
cargo fmt --check
CARGO_PROFILE_DEV_DEBUG=0 cargo check --no-default-features --all-targets
CARGO_PROFILE_TEST_DEBUG=0 cargo test --test search --no-default-features
cargo bench --bench graph_lifecycle
```

If a scheduler microbenchmark is added temporarily, keep it ignored and remove
it before committing unless it is promoted into the committed benchmark suite.
