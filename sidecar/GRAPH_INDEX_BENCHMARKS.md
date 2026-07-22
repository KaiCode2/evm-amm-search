# Graph-index benchmark

This benchmark records the request-path cost removed from the routing sidecar.
It is fully offline and uses the same released `evm-amm-search` dependency and
`LiveAmmGraph::from_snapshot` path as the deployable container.

## Workloads

Each fixture contains 512 tokens and 128, 1,024, or 8,192 ready Uniswap V2
pools. The benchmark measures four distinct operations:

1. `legacy_reconstruct_per_request`: the removed behavior for one token —
   reconstruct `LiveAmmGraph` from a coherent AMM snapshot, walk every edge,
   deduplicate pools, and check membership.
2. `cached_quote_admission`: the replacement behavior for both quote endpoint
   tokens — acquire the production-style read lock and perform indexed
   membership lookups.
3. `single_pool_commit`: acquire the write lock and apply a real contiguous
   one-pool `GraphDelta`. Fixture cloning and destruction are outside the timed
   section.
4. `cached_parallel_admission`: repeatedly perform both endpoint lookups from
   1, 8, or 32 reader threads against the 8,192-pool index.

Run it from the repository root:

```bash
cargo bench --manifest-path sidecar/Cargo.toml --bench graph_index -- --noplot
```

## 2026-07-21 result

Environment: Apple M1 Pro, 16 GiB RAM, arm64 macOS 25.5.0, release profile,
`rustc 1.96.0-nightly (900485642 2026-04-08)`. Criterion reports a 95%
confidence interval; the middle value below is its estimate.

| Workload | Size | 95% confidence interval | Estimate |
| --- | ---: | ---: | ---: |
| Legacy rebuild + walk | 128 pools | 846.79–876.29 us | 862.86 us |
| Legacy rebuild + walk | 1,024 pools | 10.821–12.874 ms | 11.597 ms |
| Legacy rebuild + walk | 8,192 pools | 111.84–124.19 ms | 116.76 ms |
| Cached two-token admission | 128 pools | 72.988–74.297 ns | 73.608 ns |
| Cached two-token admission | 1,024 pools | 75.218–82.004 ns | 78.392 ns |
| Cached two-token admission | 8,192 pools | 76.449–91.293 ns | 81.954 ns |
| Single-pool commit | 128 pools | 473.27–520.28 ns | 496.05 ns |
| Single-pool commit | 1,024 pools | 780.84–870.32 ns | 819.52 ns |
| Single-pool commit | 8,192 pools | 996.26 ns–1.0394 us | 1.0184 us |

Parallel admission on the 8,192-pool fixture:

| Reader threads | Complete two-token admissions per second | Timed batch |
| ---: | ---: | ---: |
| 1 | 11.76 million | 348.19 us for 4,096 admissions |
| 8 | 2.30 million | 14.218 ms for 32,768 admissions |
| 32 | 1.72 million | 76.284 ms for 131,072 admissions |

The higher-thread results include standard-library `RwLock` contention and
thread scheduling. Even the 32-reader result leaves graph admission far below
route-search cost, but it also shows that the shared lock is not infinitely
scalable. If profiling later identifies it as meaningful, an immutable
atomically swapped index can remove reader contention without changing the
public API.

## Interpretation limits

This is a focused metadata benchmark, not an end-to-end service capacity
claim. It does not include JSON, HTTP, discovery, route enumeration, AMM
simulation, executor simulation, provider traffic, or container resource
limits. Production sizing still requires p50/p95/p99 HTTP benchmarks with the
intended graph, route mix, quality/hop policies, canonical block rate, and CPU
and memory limits while separately measuring graph-update lag.

As a regression gate, the exact 43.0 MB arm64 release image built after this
change completed 128 deterministic one-pool HTTP quotes at concurrency 32 with
p50/p95/p99 of 20.076/136.559/177.952 ms. It then failed closed during a
40-second dual-endpoint outage, recovered on endpoint 1 with a fresh routing
generation, caught up four missed blocks, and returned a healthy quote. This
validates the integrated container path but is not a realistic large-graph
route-search capacity result.
