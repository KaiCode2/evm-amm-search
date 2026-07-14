//! Production-path offline benchmarks for immutable live search views and commits.
//!
//! Unlike `graph_lifecycle`, these workloads construct coherent `AmmRuntime`
//! snapshots and exercise `LiveSearchView::new` plus `LiveAmmGraph::apply_commit`.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_consensus::Header as ConsensusHeader;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::Header as RpcHeader;
use alloy_transport::mock::Asserter;
use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use evm_amm_search::{
    AmmGraph, AmmSearcher, FastLaneConfig, GraphBuildOptions, HeuristicSearchConfig, LiveAmmGraph,
    LiveRouteRuntime, LiveRouteRuntimeConfig, LiveRouteRuntimeHandle, LiveRouteSubscription,
    LiveSearchView, RouteRequest, RouteSubscriptionSpec, RouteSubscriptionState, SearchConfig,
    SearchMode, StreamingSearchConfig,
};
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmAdapter, AmmCanonicalBatch, AmmPreparedPoolState, AmmRuntime,
    AmmRuntimeBaseline, AmmRuntimeConfig, AmmRuntimeHandle, AmmStateCommit, AmmStateSnapshot,
    AmmStateVersion, CacheError, CallOutcome, PoolKey, PoolRegistration, PoolStateDependencies,
    PoolStatus, ProtocolId, ProtocolMetadata, SimConfig, SimError, SlotChange, StateDiff,
    StateUpdate, StateView, SwapQuote, UniswapV2Metadata,
};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::reactive::ReactiveInputBatch;

const POOL_COUNTS: [usize; 3] = [16, 64, 320];

struct BenchAdapter;

impl AmmAdapter for BenchAdapter {
    fn protocol(&self) -> ProtocolId {
        ProtocolId::UniswapV2
    }

    fn state_dependencies(&self, _pool: &PoolRegistration) -> PoolStateDependencies {
        PoolStateDependencies::default()
    }

    fn simulate_swap(
        &self,
        _pool: &PoolRegistration,
        _cache: &mut dyn AdapterCache,
        _token_in: Address,
        _token_out: Address,
        amount_in: U256,
        _config: &SimConfig,
    ) -> Result<SwapQuote, SimError> {
        Ok(SwapQuote::new(amount_in))
    }
}

struct Fixture {
    live: LiveAmmGraph,
    snapshot: Arc<AmmStateSnapshot>,
    next_commit: Arc<AmmStateCommit>,
}

struct SearchFixture {
    registry: AdapterRegistry,
    graph: AmmGraph,
    request: RouteRequest,
}

struct SchedulerScenario {
    runtime: AmmRuntimeHandle,
    routes: LiveRouteRuntimeHandle,
    subscriptions: Vec<LiveRouteSubscription>,
    last_header: RpcHeader,
    next_block: u64,
    token_in: Address,
    token_out: Address,
}

struct NoopCache;

impl StateView for NoopCache {
    fn storage(&self, _address: Address, _slot: U256) -> Option<U256> {
        None
    }
}

impl AdapterCache for NoopCache {
    fn cached_storage(&self, _address: Address, _slot: U256) -> Option<U256> {
        None
    }

    fn apply_updates(&mut self, _updates: &[StateUpdate]) -> StateDiff {
        StateDiff::default()
    }

    fn verify_slots(&mut self, _slots: &[(Address, U256)]) -> Result<Vec<SlotChange>, CacheError> {
        Ok(Vec::new())
    }

    fn purge_storage(&mut self, _address: Address) -> StateDiff {
        StateDiff::default()
    }

    fn purge_slots(&mut self, _address: Address, _slots: &[U256]) -> StateDiff {
        StateDiff::default()
    }

    fn read_storage_slot(&mut self, _address: Address, _slot: U256) -> Result<U256, CacheError> {
        Ok(U256::ZERO)
    }

    fn call_raw(
        &mut self,
        _from: Address,
        _to: Address,
        _calldata: Bytes,
        _commit: bool,
    ) -> Result<CallOutcome, CacheError> {
        Ok(CallOutcome::Halt {
            reason: "noop benchmark cache".to_owned(),
        })
    }
}

fn address(value: u64) -> Address {
    let mut bytes = [0_u8; 20];
    bytes[12..].copy_from_slice(&value.to_be_bytes());
    Address::from(bytes)
}

fn pool(index: usize) -> PoolRegistration {
    PoolRegistration::new(PoolKey::UniswapV2(address(index as u64 + 1)))
        .with_metadata(ProtocolMetadata::UniswapV2(
            UniswapV2Metadata::default()
                .with_token0(address(10_000 + (index % 32) as u64))
                .with_token1(address(10_000 + ((index + 1) % 32) as u64))
                .with_fee_bps(30),
        ))
        .with_status(PoolStatus::Ready)
}

fn header(number: u64) -> RpcHeader {
    chained_header(number, B256::repeat_byte(0x49))
}

fn chained_header(number: u64, parent_hash: B256) -> RpcHeader {
    RpcHeader::new(ConsensusHeader {
        parent_hash,
        number,
        timestamp: 1_700_000_000 + number,
        base_fee_per_gas: Some(100 + number),
        beneficiary: Address::repeat_byte(0xcb),
        gas_limit: 30_000_000,
        mix_hash: B256::repeat_byte(0xab),
        ..ConsensusHeader::default()
    })
}

fn fixture(pool_count: usize) -> Fixture {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark Tokio runtime");
    tokio::task::LocalSet::new().block_on(&runtime, async move {
        let baseline_header = header(500);
        let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
        let mut cache = EvmCache::new(Arc::new(provider)).await;
        cache
            .advance_block(&baseline_header)
            .expect("benchmark header is coherent");

        let mut registry = AdapterRegistry::new();
        registry
            .register_adapter(Arc::new(BenchAdapter))
            .expect("unique benchmark adapter");
        let runtime = AmmRuntime::spawn(
            cache,
            registry,
            AmmRuntimeBaseline::from_verified_header(1, baseline_header)
                .expect("verified benchmark baseline"),
            AmmRuntimeConfig::default(),
        )
        .expect("spawn benchmark AMM runtime");
        let mut changes = runtime
            .subscribe_changes()
            .await
            .expect("subscribe to benchmark commits");
        runtime
            .install_prepared_pools(
                (0..pool_count).map(pool).collect(),
                changes.snapshot().point(),
            )
            .await
            .expect("install benchmark pools");
        let installed = changes.next_commit().await.expect("installed-pools commit");
        let live = LiveAmmGraph::from_snapshot(installed.snapshot(), GraphBuildOptions::default())
            .expect("construct benchmark live graph");
        let snapshot = Arc::clone(installed.snapshot());

        runtime
            .commit_prepared_pool(
                AmmPreparedPoolState::new(pool(pool_count), snapshot.point(), [])
                    .expect("prepare one additional pool"),
            )
            .await
            .expect("commit one additional pool");
        let next_commit = changes
            .next_commit()
            .await
            .expect("incremental benchmark commit");
        runtime
            .shutdown()
            .await
            .expect("shutdown benchmark runtime");

        Fixture {
            live,
            snapshot,
            next_commit,
        }
    })
}

fn search_fixture(pool_count: usize) -> SearchFixture {
    let mut registry = AdapterRegistry::new();
    registry
        .register_adapter(Arc::new(BenchAdapter))
        .expect("unique search benchmark adapter");
    for registration in (0..pool_count).map(pool) {
        registry
            .register_pool(registration)
            .expect("unique search benchmark pool");
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let connectors = (0..32).map(|index| address(10_000 + index));
    let request = RouteRequest::new(address(10_000), address(10_016), U256::from(100_u64))
        .with_config(
            SearchConfig::default()
                .with_hops(1, 3)
                .with_connector_tokens(connectors)
                .with_mode(SearchMode::Heuristic(
                    HeuristicSearchConfig::default()
                        .with_beam_width(None)
                        .with_prefix_dominance(false)
                        .with_fast_lane(FastLaneConfig::disabled())
                        .with_finalist_simulation(false, 1),
                )),
        );
    SearchFixture {
        registry,
        graph,
        request,
    }
}

impl SchedulerScenario {
    async fn spawn(fanout: usize) -> Self {
        let baseline = header(1_000);
        let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
        let mut cache = EvmCache::new(Arc::new(provider)).await;
        cache
            .advance_block(&baseline)
            .expect("scheduler benchmark baseline");
        let token_in = address(20_000);
        let token_out = address(20_001);
        let mut registry = AdapterRegistry::new();
        registry
            .register_adapter(Arc::new(BenchAdapter))
            .expect("unique scheduler adapter");
        registry
            .register_pool(
                PoolRegistration::new(PoolKey::UniswapV2(address(20_002)))
                    .with_metadata(ProtocolMetadata::UniswapV2(
                        UniswapV2Metadata::default()
                            .with_token0(token_in)
                            .with_token1(token_out)
                            .with_fee_bps(30),
                    ))
                    .with_status(PoolStatus::Ready),
            )
            .expect("scheduler benchmark pool");
        let runtime = AmmRuntime::spawn(
            cache,
            registry,
            AmmRuntimeBaseline::from_verified_header(1, baseline.clone())
                .expect("scheduler benchmark baseline is verified"),
            AmmRuntimeConfig::default(),
        )
        .expect("spawn scheduler AMM runtime");
        let routes = LiveRouteRuntime::spawn(
            &runtime,
            GraphBuildOptions::default(),
            LiveRouteRuntimeConfig::default()
                .with_worker_threads(4)
                .with_job_queue_capacity(fanout.max(1))
                .with_max_subscriptions(fanout.max(1)),
        )
        .await
        .expect("spawn scheduler route runtime");
        let mut subscriptions = Vec::with_capacity(fanout);
        for index in 0..fanout {
            subscriptions.push(
                routes
                    .subscribe(RouteSubscriptionSpec::new(
                        RouteRequest::new(token_in, token_out, U256::from(100 + index as u64)),
                        StreamingSearchConfig::default().heuristic_only(),
                    ))
                    .await
                    .expect("scheduler benchmark subscription"),
            );
        }
        let initial_version = runtime.latest_snapshot().version();
        for subscription in &mut subscriptions {
            wait_for_ready_version(subscription, initial_version).await;
        }
        Self {
            runtime,
            routes,
            subscriptions,
            last_header: baseline,
            next_block: 1_001,
            token_in,
            token_out,
        }
    }

    async fn measure_commit_fanout(&mut self, iterations: u64) -> Duration {
        let started = Instant::now();
        for _ in 0..iterations {
            let next = chained_header(self.next_block, self.last_header.hash);
            self.runtime
                .ingest_batch(
                    AmmCanonicalBatch::from_verified_block(
                        1,
                        next.clone(),
                        self.runtime.interest_revision(),
                        ReactiveInputBatch::new(Vec::new()),
                    )
                    .expect("scheduler benchmark canonical batch"),
                )
                .await
                .expect("apply scheduler benchmark commit");
            let expected = self.runtime.latest_snapshot().version();
            for subscription in &mut self.subscriptions {
                wait_for_ready_version(subscription, expected).await;
            }
            self.last_header = next;
            self.next_block += 1;
        }
        started.elapsed()
    }

    async fn measure_replacement_storm(&mut self, iterations: u64) -> Duration {
        let started = Instant::now();
        for iteration in 0..iterations {
            for replacement in 0..8_u64 {
                for (index, subscription) in self.subscriptions.iter().enumerate() {
                    subscription
                        .replace(RouteSubscriptionSpec::new(
                            RouteRequest::new(
                                self.token_in,
                                self.token_out,
                                U256::from(
                                    1_000_000
                                        + iteration * 10_000
                                        + replacement * 100
                                        + index as u64,
                                ),
                            ),
                            StreamingSearchConfig::default().heuristic_only(),
                        ))
                        .await
                        .expect("replace benchmark route request");
                }
            }
            for subscription in &mut self.subscriptions {
                let expected_epoch = subscription.latest().epoch();
                wait_for_ready_epoch(subscription, expected_epoch).await;
            }
        }
        started.elapsed()
    }

    async fn shutdown(self) {
        for subscription in &self.subscriptions {
            subscription
                .cancel()
                .await
                .expect("cancel scheduler benchmark subscription");
        }
        self.routes
            .shutdown()
            .await
            .expect("shutdown scheduler route runtime");
        self.runtime
            .shutdown()
            .await
            .expect("shutdown scheduler AMM runtime");
    }
}

async fn wait_for_ready_version(
    subscription: &mut LiveRouteSubscription,
    expected: AmmStateVersion,
) {
    loop {
        let snapshot = subscription.latest();
        if matches!(
            snapshot.state(),
            RouteSubscriptionState::Ready { source, .. } if source.state_version() == expected
        ) {
            return;
        }
        subscription
            .changed()
            .await
            .expect("scheduler benchmark route state");
    }
}

async fn wait_for_ready_epoch(subscription: &mut LiveRouteSubscription, expected: u64) {
    loop {
        let snapshot = subscription.latest();
        if snapshot.epoch() == expected
            && matches!(snapshot.state(), RouteSubscriptionState::Ready { .. })
        {
            return;
        }
        subscription
            .changed()
            .await
            .expect("scheduler benchmark replacement state");
    }
}

fn live_search_runtime(c: &mut Criterion) {
    let fixtures = POOL_COUNTS
        .into_iter()
        .map(|count| (count, fixture(count)))
        .collect::<Vec<_>>();

    let mut views = c.benchmark_group("live_search_runtime/view_creation");
    for (count, fixture) in &fixtures {
        views.bench_with_input(BenchmarkId::from_parameter(count), fixture, |b, fixture| {
            b.iter(|| {
                LiveSearchView::new(
                    black_box(Arc::clone(&fixture.snapshot)),
                    black_box(&fixture.live),
                )
                .expect("coherent live search view")
            });
        });
    }
    views.finish();

    let mut commits = c.benchmark_group("live_search_runtime/topology_commit");
    for (count, fixture) in &fixtures {
        commits.bench_with_input(BenchmarkId::from_parameter(count), fixture, |b, fixture| {
            b.iter_batched(
                || fixture.live.clone(),
                |mut live| {
                    let delta = live
                        .apply_commit(black_box(&fixture.next_commit))
                        .expect("contiguous topology commit");
                    (live, delta)
                },
                BatchSize::SmallInput,
            );
        });
    }
    commits.finish();

    let search_fixtures = POOL_COUNTS
        .into_iter()
        .map(|count| (count, search_fixture(count)))
        .collect::<Vec<_>>();
    let mut reachability = c.benchmark_group("search_hot_path/dead_end_reachability");
    for (count, fixture) in &search_fixtures {
        reachability.bench_with_input(BenchmarkId::from_parameter(count), fixture, |b, fixture| {
            let searcher = AmmSearcher::new(&fixture.registry, &fixture.graph);
            let mut cache = NoopCache;
            b.iter(|| searcher.find_routes(black_box(&fixture.request), &mut cache));
        });
    }
    reachability.finish();

    const FANOUTS: [usize; 3] = [1, 8, 32];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("scheduler benchmark Tokio runtime");
    let local = tokio::task::LocalSet::new();

    let mut commit_fanout = c.benchmark_group("live_route_scheduler/commit_to_all_ready");
    for fanout in FANOUTS {
        let mut scenario = local.block_on(&runtime, SchedulerScenario::spawn(fanout));
        commit_fanout.bench_function(BenchmarkId::from_parameter(fanout), |b| {
            b.iter_custom(|iterations| {
                local.block_on(&runtime, scenario.measure_commit_fanout(iterations))
            });
        });
        local.block_on(&runtime, scenario.shutdown());
    }
    commit_fanout.finish();

    let mut replacements = c.benchmark_group("live_route_scheduler/eight_replacements_to_ready");
    for fanout in FANOUTS {
        let mut scenario = local.block_on(&runtime, SchedulerScenario::spawn(fanout));
        replacements.bench_function(BenchmarkId::from_parameter(fanout), |b| {
            b.iter_custom(|iterations| {
                local.block_on(&runtime, scenario.measure_replacement_storm(iterations))
            });
        });
        local.block_on(&runtime, scenario.shutdown());
    }
    replacements.finish();
}

criterion_group!(benches, live_search_runtime);
criterion_main!(benches);
