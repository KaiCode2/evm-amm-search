//! Request-path graph metadata benchmark.
//!
//! Compares the removed behavior (reconstruct a searchable live graph and walk
//! its edges for each token membership check) with the commit-maintained index
//! now used by the HTTP service.

use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::Duration,
};

use alloy_consensus::Header as ConsensusHeader;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::Header as RpcHeader;
use alloy_transport::mock::Asserter;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use evm_amm_route_sidecar::graph_index::GraphIndex;
use evm_amm_search::{GraphBuildOptions, GraphDelta, LiveAmmGraph};
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmAdapter, AmmRuntime, AmmRuntimeBaseline, AmmRuntimeConfig,
    AmmRuntimeHandle, AmmStateSnapshot, PoolKey, PoolRegistration, PoolStateDependencies,
    PoolStatus, ProtocolId, ProtocolMetadata, SimConfig, SimError, SwapQuote, UniswapV2Metadata,
};
use evm_fork_cache::cache::EvmCache;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};

const POOL_COUNTS: [usize; 3] = [128, 1_024, 8_192];
const TOKEN_COUNT: usize = 512;

struct TestAdapter;

impl AmmAdapter for TestAdapter {
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
    _runtime: AmmRuntimeHandle,
    snapshot: Arc<AmmStateSnapshot>,
    index: Arc<RwLock<GraphIndex>>,
    single_pool_delta: GraphDelta,
    token_in: Address,
    token_out: Address,
}

fn address(value: u64) -> Address {
    let mut bytes = [0_u8; 20];
    bytes[12..].copy_from_slice(&value.to_be_bytes());
    Address::from(bytes)
}

fn pool(index: usize) -> PoolRegistration {
    let token0_index = index % TOKEN_COUNT;
    let mut token1_index = (index.wrapping_mul(17) + 1) % TOKEN_COUNT;
    if token1_index == token0_index {
        token1_index = (token1_index + 1) % TOKEN_COUNT;
    }
    PoolRegistration::new(PoolKey::UniswapV2(address(1_000_000 + index as u64)))
        .with_metadata(ProtocolMetadata::UniswapV2(
            UniswapV2Metadata::default()
                .with_token0(address(10_000 + token0_index as u64))
                .with_token1(address(10_000 + token1_index as u64))
                .with_fee_bps(30),
        ))
        .with_status(PoolStatus::Ready)
}

fn header() -> RpcHeader {
    RpcHeader::new(ConsensusHeader {
        number: 500,
        parent_hash: B256::repeat_byte(0x49),
        timestamp: 1_700_000_500,
        base_fee_per_gas: Some(100),
        gas_limit: 30_000_000,
        ..ConsensusHeader::default()
    })
}

async fn fixture(pool_count: usize) -> Fixture {
    let baseline = header();
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = EvmCache::new(Arc::new(provider)).await;
    cache.advance_block(&baseline).expect("benchmark block");
    let mut registry = AdapterRegistry::new();
    registry
        .register_adapter(Arc::new(TestAdapter))
        .expect("unique benchmark adapter");
    for index in 0..pool_count {
        registry
            .register_pool(pool(index))
            .expect("unique benchmark pool");
    }
    let runtime = AmmRuntime::spawn(
        cache,
        registry,
        AmmRuntimeBaseline::from_verified_header(1, baseline).expect("verified baseline"),
        AmmRuntimeConfig::default(),
    )
    .expect("benchmark runtime");
    let mut changes = runtime
        .subscribe_changes()
        .await
        .expect("benchmark changes");
    let snapshot = Arc::clone(changes.snapshot());
    let mut live = LiveAmmGraph::from_snapshot(&snapshot, GraphBuildOptions::default())
        .expect("benchmark live graph");
    let index = GraphIndex::from_graph(live.graph(), snapshot.version());
    runtime
        .install_prepared_pools(vec![pool(pool_count)], snapshot.point())
        .await
        .expect("install benchmark topology update");
    let commit = changes
        .next_commit()
        .await
        .expect("benchmark topology commit");
    let single_pool_delta = live.apply_commit(&commit).expect("benchmark graph delta");
    Fixture {
        _runtime: runtime,
        snapshot,
        index: Arc::new(RwLock::new(index)),
        single_pool_delta,
        token_in: address(10_000),
        token_out: address(10_001),
    }
}

fn legacy_request_stats(
    snapshot: &AmmStateSnapshot,
    token: Address,
) -> (usize, usize, usize, bool, usize) {
    let live = LiveAmmGraph::from_snapshot(snapshot, GraphBuildOptions::default())
        .expect("reconstruct live graph");
    let graph = live.graph();
    let mut all_pools = HashSet::new();
    let mut token_pools = HashSet::new();
    for edge in graph.graph().edge_references() {
        all_pools.insert(edge.weight().pool.clone());
        if graph.node_token(edge.source()) == Some(token)
            || graph.node_token(edge.target()) == Some(token)
        {
            token_pools.insert(edge.weight().pool.clone());
        }
    }
    (
        graph.node_count(),
        graph.edge_count(),
        all_pools.len(),
        graph.node_index(&token).is_some(),
        token_pools.len(),
    )
}

fn cached_token_present(fixture: &Fixture, token: Address) -> bool {
    fixture
        .index
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .stats(Some(token))
        .token_present()
}

fn graph_index(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark Tokio runtime");
    let fixtures = POOL_COUNTS
        .into_iter()
        .map(|count| {
            let fixture = runtime.block_on(tokio::task::LocalSet::new().run_until(fixture(count)));
            (count, fixture)
        })
        .collect::<Vec<_>>();

    let mut legacy = c.benchmark_group("quoter_graph_metadata/legacy_reconstruct_per_request");
    legacy.warm_up_time(Duration::from_secs(1));
    legacy.measurement_time(Duration::from_secs(3));
    legacy.sample_size(20);
    for (pool_count, fixture) in &fixtures {
        legacy.throughput(Throughput::Elements(*pool_count as u64));
        legacy.bench_with_input(
            BenchmarkId::from_parameter(pool_count),
            fixture,
            |b, fixture| {
                b.iter(|| legacy_request_stats(black_box(&fixture.snapshot), fixture.token_in));
            },
        );
    }
    legacy.finish();

    let mut cached = c.benchmark_group("quoter_graph_metadata/cached_quote_admission");
    cached.warm_up_time(Duration::from_secs(1));
    cached.measurement_time(Duration::from_secs(3));
    for (pool_count, fixture) in &fixtures {
        cached.throughput(Throughput::Elements(2));
        cached.bench_with_input(
            BenchmarkId::from_parameter(pool_count),
            fixture,
            |b, fixture| {
                b.iter(|| {
                    black_box(cached_token_present(fixture, fixture.token_in));
                    black_box(cached_token_present(fixture, fixture.token_out));
                });
            },
        );
    }
    cached.finish();

    let mut updates = c.benchmark_group("quoter_graph_metadata/single_pool_commit");
    updates.warm_up_time(Duration::from_secs(1));
    updates.measurement_time(Duration::from_secs(3));
    for (pool_count, fixture) in &fixtures {
        updates.throughput(Throughput::Elements(1));
        updates.bench_with_input(
            BenchmarkId::from_parameter(pool_count),
            fixture,
            |b, fixture| {
                let base = fixture
                    .index
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                b.iter_batched_ref(
                    || RwLock::new(base.clone()),
                    |index| {
                        black_box(
                            index
                                .write()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .apply_delta(&fixture.single_pool_delta)
                                .expect("contiguous benchmark delta"),
                        );
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    updates.finish();

    let (_, largest) = fixtures.last().expect("at least one fixture");
    let mut parallel = c.benchmark_group("quoter_graph_metadata/cached_parallel_admission");
    parallel.warm_up_time(Duration::from_secs(1));
    parallel.measurement_time(Duration::from_secs(3));
    parallel.sample_size(20);
    for workers in [1_usize, 8, 32] {
        const LOOKUPS_PER_WORKER: usize = 4_096;
        parallel.throughput(Throughput::Elements(
            (workers * LOOKUPS_PER_WORKER * 2) as u64,
        ));
        parallel.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.iter(|| {
                    std::thread::scope(|scope| {
                        for _ in 0..workers {
                            scope.spawn(|| {
                                for _ in 0..LOOKUPS_PER_WORKER {
                                    black_box(cached_token_present(largest, largest.token_in));
                                    black_box(cached_token_present(largest, largest.token_out));
                                }
                            });
                        }
                    });
                });
            },
        );
    }
    parallel.finish();
}

criterion_group!(benches, graph_index);
criterion_main!(benches);
