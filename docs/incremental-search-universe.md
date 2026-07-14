# Incremental search universe

Stage 7 replaces registry-wide graph reconstruction on AMM lifecycle changes
with a versioned, commit-driven search universe.

## Runtime contract

`LiveAmmGraph` starts from one immutable `AmmStateSnapshot` and consumes the
reliable `AmmStateCommit` stream. It owns the token graph, a liquidity-target
sidecar, the active pool-generation/revision index, and the last applied AMM
state point/version.

- Snapshots and commits carry a process-unique `AmmRuntimeId`; a numerically
  contiguous commit from another runtime is rejected before mutation.
- Commits must be contiguous and their snapshot/change-set point and version
  must agree. Rejected commits leave every owned structure unchanged.
- A topology or active-generation change advances `GraphVersion` exactly once,
  regardless of how many pools changed in the commit.
- A quote-state-only revision advances AMM state but leaves `GraphVersion`
  stable.
- A lagged consumer may reconcile directly from a newer snapshot. Equal or
  older snapshots are rejected.
- `GraphDelta` reports the before/after indexed pool identities, topology
  impact, graph versions, source AMM state, and liquidity-target mutations.

`GraphVersion` contains a process-unique graph lineage plus a monotonic revision.
Independently rebuilt graphs therefore cannot compare equal merely because they
have the same node/edge counts.

## Bounded mutation

`AmmGraph::apply_pool` and `remove_pool_compacting` update only one pool's
directed edges, token nodes, pool-token metadata, and token-pair membership.
The pair membership index identifies the neighboring pools whose liquidity
targets can change when a pair crosses the one-pool/two-pool parallelism
boundary. `PoolLiquidityIndex::reconcile_pools` then rebuilds only those target
records, preserves fresh balances when token/source identity is unchanged, and
reuses removed target slots under churn.

Known liquidity slots can be refreshed through
`LiveAmmGraph::refresh_liquidity_from_snapshot`; it accepts only the exact
runtime/version/point currently represented by the graph and reads the immutable
cache snapshot without provider calls. Arbitrary cache/provider hydration is not
exposed on the live owner.

The full-build scanners remain for initial construction and explicit recovery;
normal add/remove commits do not rebuild the graph or liquidity index.

## Quote provenance

`AmmSearcher::from_snapshot(snapshot, &live_graph)` verifies that the graph and
snapshot share a runtime lineage, AMM state version, and point, creates live quote-cache context from
the snapshot, and constructs every simulation overlay from the snapshot's own
immutable `EvmSnapshot`. The cache argument retained by existing search method
signatures is not used as live quote state. Each hop key is scoped by:

- `PoolInstanceId` (logical key plus generation),
- `PoolStateRevision`,
- complete `AmmStatePoint` (chain, block number/hash, transaction position),
- token direction, amount, and simulation configuration.

Missing live pool state is an error and never falls back to a static cache key.
Route sessions compare state points before accepting an empty affected-pool set,
and require a full recompute when the snapshot point or graph version changes.
Their targeted pool-to-key index evicts obsolete same-point revisions without a
whole-cache scan, including cached failures.

## Verification

The offline suite covers idempotent adds, token rewiring, compact removals,
mixed model sequences compared semantically with full rebuilds, one-to-two and
two-to-one parallel liquidity transitions, fresh-balance preservation, target
slot reuse, state-only commits, skipped-commit atomicity, snapshot recovery,
stale recovery rejection, and quote-key isolation across revision, generation,
and state-point changes.

Run the lifecycle benchmark with:

```text
cargo bench --bench graph_lifecycle --features uniswap-v2
```

The benchmark retains `add_one_via_full_rebuild` as the Stage 0 baseline and
adds `add_one_incremental` plus compact incremental removal. Results are recorded
for 16, 64, and 320 existing two-token pools; the Stage 7 gate requires the
incremental add median to remain below 10% of the corresponding full rebuild.

Quick Criterion run on 2026-07-11 (`arm64` macOS 26.5.1, Rust
1.96.0-nightly, 200 ms warm-up, 500 ms measurement, 10 samples):

| Existing pools | Full-rebuild add median | Incremental add median | Ratio |
| ---: | ---: | ---: | ---: |
| 16 | 39.113 us | 0.534 us | 1.37% |
| 64 | 178.25 us | 0.790 us | 0.44% |
| 320 | 1,165.2 us | 0.456 us | 0.04% |

All measured sizes clear the `<10%` gate. These are offline microbenchmark
results, not paid-RPC end-to-end latency; the latter remains a Stage 10 release
gate.
