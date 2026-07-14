# Live consumer migration

Stage 9 moves interactive consumers from manually rebuilding a registry graph
and synchronously quoting after every event to the state and search actors built
in Stages 4–8.

## Composition

The live ownership order is:

1. create `AmmRuntime` from a full verified header and matching `EvmCache`;
2. attach the bounded cold-start worker;
3. attach the canonical Alloy subscriber;
4. install generation-owned factory watchers;
5. start one `LiveRouteRuntime` over the reliable AMM commit stream;
6. create logical route subscriptions immediately;
7. queue restored/manual/focused registrations and background discovery.

The route runtime may start with an empty graph. Subscribing returns immediately;
the initial state can report no path, and every independently published pool
commit refreshes the graph and recomputes the same logical route.

## Authoritative UI state

Each `RouteSubscriptionSnapshot` carries the exact `Arc<LiveSearchView>` used by
its state. Rendering therefore uses one coherent registry, graph, liquidity
index, cache snapshot, state point, and graph version without rebuilding or
racing `latest_snapshot()`.

Request changes use `LiveRouteSubscription::replace` when the consumer owns the
subscription directly. Replacement increments the subscription epoch, keeps the
previous accepted quote only as historical context, cancels old work, and
rejects late results. Consumers that create replacement tasks must add their own
request-generation fence in addition to cancelling the previous token.

The TUI keeps one subscription driver for the selected pair. A latest-value
request channel coalesces rapid amount/policy edits into in-place `replace()`
calls, while a separate UI generation rejects any already-queued display event.
It does not allocate one actor subscription per keystroke.

The recoverable `watch` state drives UI correctness. Broadcast events are useful
for stream text and hooks, but lag is expected and never used as authoritative
state.

## Responsive startup and discovery

The TUI starts its terminal shell and input receiver before provider connection,
cache loading, discovery, or hydration. Bootstrap runs as a local background task
because `EvmCache` is intentionally thread-local. Typed progress events update
the shell, and quitting triggers structured shutdown of any partially created
route runtime, subscriber, cold-start worker, and AMM runtime.

Once the actor handles are ready, runtime status/snapshot/subscriber watches
replace manual progress counters and log ingestion. Dynamic token requests queue
generation-owned factory discovery; results flow through cold start, canonical
publication, incremental graph mutation, and subscribed-route recomputation
without registry replacement or UI-thread search work.

## Examples

- `cargo run --example progressive_live_routes --no-default-features --features live-runtime,uniswap-v2`
- `cargo run --example dynamic_live_routes --no-default-features --features live-runtime,uniswap-v2`

The first demonstrates pool-by-pool route availability. The second demonstrates
dynamic addition, an improved connector route, exact-generation removal, and
fallback routing while preserving runtime identity and adapter instances.
