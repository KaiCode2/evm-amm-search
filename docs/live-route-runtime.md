# Live route runtime

Stage 8 turns the incremental graph/search primitives into a search-owned live
actor. It consumes exactly one reliable `AmmChangeSubscription` and multiplexes
logical route subscriptions without putting quote latency or UI backpressure on
the canonical AMM cache writer.

## Public surfaces

`LiveRouteRuntime::spawn` acquires the AMM runtime's critical change stream,
builds the baseline `LiveAmmGraph`, captures one immutable `LiveSearchView`, and
starts the coordinator plus a bounded persistent worker pool.

Each `LiveRouteSubscription` provides:

- `latest()` / `changed()` over a recoverable `watch` publication;
- the exact immutable `LiveSearchView` represented by every publication;
- typed `LiveRouteRuntimeEvent` observations with explicit `Lagged` errors;
- an immediate cloneable `RouteCancellationToken`;
- in-place `replace()` for request changes without changing subscription identity;
- acknowledged `cancel()`; dropping the subscription also removes it.

Every authoritative snapshot also exposes the current request `epoch()`, so a
consumer recovering from watch state can distinguish replacements even when two
requests share the same AMM source.

The watch state is authoritative. Observer events are diagnostics and pipeline
hooks; missing them cannot corrupt graph or route state. Cancellation-token use
also enqueues actor removal, so it cannot strand subscription capacity. A
terminal graph/state application failure is retained as
`RouteSubscriptionState::RuntimeFailed` even if its lossy observer event is
missed.

## Causal pipeline

```mermaid
flowchart LR
    A["Reliable AmmStateCommit"] --> B["LiveAmmGraph.apply_commit"]
    B --> C["AmmCommitApplied"]
    C --> D["RouteInvalidated"]
    D --> E["SearchScheduled"]
    E --> F["Persistent snapshot worker"]
    F --> G["Fenced SearchEvent"]
    G --> H["RoutePublished watch state"]
```

Only the coordinator assigns public event sequence numbers. For a commit, it
applies the graph before invalidating routes, and invalidates before admitting
replacement searches. Worker completion order across subscriptions is naturally
nondeterministic, but every event retains its complete job fence.

## Immutable worker view

One `Arc<LiveSearchView>` is created per coherent AMM publication and shared by
every job for that source. It contains:

- the immutable `AmmStateSnapshot` and cache snapshot;
- an `Arc<AmmGraph>`;
- an `Arc<PoolLiquidityIndex>`;
- the once-built pool-generation/revision quote scope.

`AmmSearcher::stream_routes_snapshot` therefore needs no mutable cache borrow.
The quote scope's all-pools map is held behind `Arc`, so constructing a searcher
or per-job quote cache does not clone every tracked pool.
The hot state-only graph path reuses the exact graph and liquidity `Arc`s and
updates only changed pool revision records.

## Freshness fence

Every worker event/result carries `RouteJobStamp`:

- `AmmRuntimeId`;
- `AmmStateVersion`;
- complete `AmmStatePoint`;
- `GraphVersion`;
- `RouteSubscriptionId` and subscription epoch;
- `RouteSearchJobId`.

The coordinator compares the full stamp before publishing progressive results
or installing a final watch state. Results from a previous block, reorg point,
same-point graph generation, cancelled subscription, or superseded job are
discarded. A late completion produces `StaleResultRejected`; it never becomes
authoritative.

Replacing a request increments the subscription epoch, cancels the old attempt,
retains the last accepted quote only as historical UI context, and coalesces to
one job for the newest request. A late result from the previous request cannot
cross the epoch fence even when both requests use the same AMM state point.

## Bounded concurrency and coalescing

Route subscriptions are the persistent worker unit. Stage 8 deliberately forces
the existing per-search parallel configuration to one worker inside each job,
preventing an outer-worker by inner-worker thread explosion. The configured
long-lived pool is the single concurrency budget.

Worker admission is bounded and non-blocking for the coordinator. Each
subscription has at most one queued/in-flight attempt. If commits arrive while
that attempt is busy, its cancellation token is set and the desired source is
replaced in place. Once the old worker returns, exactly one search for the newest
source is admitted.

Dirty subscriptions are held in a deduplicated pending queue. Progressive worker
events do not allocate or scan the complete subscription map, keeping reliable
commit intake responsive as subscription and event counts grow. Commit intake
normally has priority; after a bounded commit burst, control and worker channels
receive a fairness turn. If a worker message wins that turn while a commit is
ready, the coordinator applies one ready commit before inspecting the message,
so the advanced fence still rejects the old result.

Worker progress/results use a bounded internal channel. Public observer delivery
uses `broadcast`; slow observers report lag while `watch` retains the newest
authoritative route state.

## Cancellation and failure isolation

Cancellation is cooperative: it is checked at search event boundaries and
before public worker delivery. An adapter simulation already executing cannot be
preempted safely, so its eventual output is rejected by the actor fence.
Calling the public token schedules subscription removal; `cancel()` waits for
the actor acknowledgement and `Cancelled` watch state.

Each worker job is wrapped in `catch_unwind`. A panicking adapter produces a
typed `RouteSearchFailure` and the persistent worker continues serving later
subscriptions. Runtime shutdown cancels subscriptions, closes result delivery,
then joins workers off the async executor. It does not stop the upstream AMM
runtime. Dropping all external handles also terminates the actor and releases
the one critical AMM change stream so a replacement route runtime can start.
Failure to create an operating-system worker is returned as typed
`LiveRouteRuntimeError::WorkerSpawn`; already-created workers are stopped and
joined before startup returns the error.

## Verified behavior

The offline integration suite proves:

- baseline subscription to snapshot-bound quote;
- commit/graph/invalidation/scheduling event order;
- current-point recomputation after state-only commits;
- commit draining while every worker is busy;
- stale result rejection across state points and same-point graph versions;
- newest-only coalescing across multiple commits;
- newest-only coalescing across rapid request and search-policy replacements;
- last-published-source attribution when an invalidated job never published;
- exact-view gas simulation rejection for cross-snapshot quotes;
- external and drop-triggered cancellation;
- hard global worker-concurrency bounds;
- worker-panic isolation and continued service;
- explicit observer lag with recoverable latest watch state;
- observer closure despite retained sender handles;
- automatic critical-stream release after every external handle is dropped.

Cross-block reuse remains conservative: a newer complete `AmmStatePoint`
recomputes subscribed routes. Adapter-declared block-context independence is a
future optimization and requires equivalence proof before relaxing this fence.

## Known scale limits

Configured candidate caps are enforced during exhaustive path generation, and
live cancellation is polled between traversal states and edges. An explicitly
uncapped exhaustive request still asks for the complete universe by definition.
Active adapter simulation is cooperative only after the adapter returns, and
AMM commits currently invalidate every subscription rather than using a
pool-to-subscription dependency index; both are bounded by stale-result fences
but remain scale fast-follows.

The global route observer sequence uses saturating arithmetic at the practically
unreachable `u64::MAX` boundary. Normal allocation paths already return
`SequenceExhausted`, but terminal observer publication needs one checked
allocator that reserves a final failure/close sequence before saturation can be
made fully uniform. This is a documented P3 hardening follow-up; wrapping or
silently duplicating ordinary sequence values is not permitted.
