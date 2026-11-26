# Static Reachability Pruning in Heuristic Search

Heuristic search checks static graph reachability in `group_heuristic_hops`
before quoting an intermediate hop or admitting it into the frontier. Quoting a
pool executes adapter/EVM simulation and consumes limited beam capacity, whereas
checking topology reachability in memory is pure graph traversal. Pruning
dead-end branches before simulation avoids wasted quotes and preserves beam
slots for viable prefixes.

## Previous Bottleneck

Previously, `has_static_route_to_target` ran an unmemoized recursive DFS
(`has_static_route_to_target_dfs`) for every outgoing edge in
`group_heuristic_hops`:

1. Every candidate edge cloned `visited_tokens` and `used_pools` before calling
   the reachability check.
2. Parallel pools between the same token pair were explored separately at every
   recursion level.
3. Because a node could be reached via a longer dead-end branch before a shorter
   valid branch, the DFS had to backtrack and unmark nodes on return, yielding
   exponential `O(E^(max_hops - 1))` worst-case time whenever `target` was
   unreachable (or only reachable outside the remaining hop budget).

At `max_hops = 3` this already showed cubic scaling in parallel pool count; at
`max_hops = 6..8` on dense token graphs with parallel pools it caused multi-second
to multi-minute CPU stalls.

## Two-Stage Reachability Design

Static reachability is now split into a request-local reverse distance table and
a bounded, deduplicated forward BFS:

### 1. Request-Local Reverse BFS (`min_hops_to_target`)

On the first reachability check of a heuristic search request,
`AmmSearcher::min_hops_to_target` runs a single reverse BFS starting from
`target` over incoming edges (`Direction::Incoming`), stopping at depth
`max_hops - 1`:

- Only tokens allowed by `connector_allowed_with_tokens` are traversed.
- Parallel pools to the same predecessor node are deduplicated automatically by
  `distances.contains_key(&prev_node)`.
- Complexity is `O(V + E)` once per request, and the resulting map is stored on
  `HeuristicRunCache::min_hops_to_target`.

When `group_heuristic_hops` evaluates an intermediate edge to `next_node` with
`remaining_after_hop` hops left, it first performs an `O(1)` lookup without
cloning `visited_tokens` or `used_pools`:

```text
min_hops_to_target.get(&next_node).is_none_or(|&d| d > remaining_after_hop)
```

Because restricting edges by `visited_tokens` and `used_pools` can only increase
(never decrease) the shortest-path length to `target`, any node failing this
check is guaranteed unreachable. Dead-end subgraphs and disconnected components
are rejected in `O(1)` with zero allocations.

### 2. Deduplicated Forward BFS (Exact Constraint Check)

If `min_hops_to_target[next_node] <= remaining_after_hop`, the static graph has
a short enough path, but already-visited tokens (`visited_tokens`,
`extra_visited_token`) or already-used pools (`used_pools`, `extra_used_pool`)
might block it (for example, a multi-token Curve pool or a 2-hop cycle
`source == target` with only one pool).

In that case, `has_static_route_to_target` runs a token-deduplicated forward BFS
starting from `next_node`:

- Skips any edge whose pool is in `used_pools` or equals `extra_used_pool`.
- Skips any edge whose destination token is in `visited_tokens` or equals
  `extra_visited_token`, unless `allow_final_source_revisit && source == target && next_node == source`.
- Immediately returns `true` when an edge closes `target`.
- Prunes intermediate nodes with `min_hops_to_target[v] > remaining_after`
  before enqueueing them.
- Marks each intermediate `NodeIndex` in `seen` at most once, collapsing
  parallel pools and guaranteeing `O(V + E)` worst-case work per call.

### Exactness Properties

- **Zero false negatives from reverse BFS:** `min_hops_to_target[u]` is an exact
  lower bound on the hop distance from `u` to `target` in the connector-filtered
  graph.
- **No backtracking required:** BFS visits every intermediate token at its
  minimum hop depth first, so a longer dead-end branch can never starve a
  shorter valid branch through a shared node (covered by
  `heuristic_reachability_backtracks_from_a_dead_end_before_a_shared_branch`).
- **Multi-token pools:** `AmmGraph` indexes every pool as a complete directed
  clique over its tokens, so a shortest path never needs to traverse the same
  unused multi-token pool twice (`X -P-> Y -P-> Z` is strictly dominated by
  `X -P-> Z`).
- **Behavior-preserving ranking:** Outgoing-edge iteration order, connector
  scoring, and route selection are unchanged; route quality is identical.

## Compatibility With Existing Design Docs

This design aligns with the invariants in
[search-performance-model.md](./search-performance-model.md) and
[live-route-runtime.md](./live-route-runtime.md):

- **Request-local lifetime:** `min_hops_to_target` lives on `HeuristicRunCache`,
  which is created for one heuristic search call and dropped when the call
  returns. Nothing is cached on `AmmSearcher`, so live graph mutations cannot
  observe stale distances.
- **Immutable snapshot safety:** Reverse BFS reads only the immutable `AmmGraph`
  reference held by `AmmSearcher` for the duration of the request.
- **Deterministic edge ordering:** `group_heuristic_hops` iterates
  `AmmGraph::outgoing_edges` in the exact same order as before.

## Benchmarks And Verification

### Criterion Benchmark (`search_hot_path/dead_end_reachability`)

Measured with `cargo bench --bench live_search_runtime -- search_hot_path`
(32-token ring, `max_hops = 3`, unreachable target 16 hops away):

| Pool count | Before (DFS) | After (Reverse BFS + Forward BFS) | Change | Speedup |
| --- | ---: | ---: | ---: | ---: |
| `16` | `461.89 ns` | `311.21 ns` | `-32.5%` | `1.48x` |
| `64` | `2.5649 µs` | `601.61 ns` | `-76.6%` | `4.26x` |
| `320` | `142.58 µs` | `1.4031 µs` | `-99.02%` | `101.6x` |

### High-Hop Reproduction Test

`heuristic_dead_end_reachability_does_not_blow_up_at_high_hop_depth` in
`tests/search.rs` builds a 7-token clique with 3 parallel pools per pair (63
pools) plus a disconnected target at `max_hops = 6`:

- Before: `1.236 s` (fails the `200 ms` bound).
- After: `< 1 ms` (the full 64-test `tests/search.rs` suite finishes in `0.03 s`).

Exact constraint handling for multi-token `used_pools` and 2-hop cycles
(`allow_final_source_revisit`) is verified by
`heuristic_reachability_respects_used_pools_and_source_cycle` in
`tests/search.rs`.
