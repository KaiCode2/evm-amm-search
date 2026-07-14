#![cfg(feature = "live-runtime")]

use std::{
    collections::HashMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use alloy_consensus::Header as ConsensusHeader;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::Header as RpcHeader;
use alloy_transport::mock::Asserter;
use anyhow::Result;
use evm_amm_search::{
    DemoRouterConfig, FastLaneConfig, GraphBuildOptions, HeuristicSearchConfig,
    LiveRouteObserverError, LiveRouteRuntime, LiveRouteRuntimeConfig, LiveRouteRuntimeError,
    LiveRouteRuntimeEventKind, LiveRouteRuntimeHandle, RouteInvalidationReason, RouteRequest,
    RouteSearchEvent, RouteSearchPhase, RouteSubscriptionSpec, RouteSubscriptionState,
    SearchConfig, SearchMode, StreamingSearchConfig, simulate_versioned_route_gas,
    simulate_versioned_route_gas_with_balance_mappings,
};
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmAdapter, AmmCanonicalBatch, AmmPreparedPoolState, AmmRuntime,
    AmmRuntimeBaseline, AmmRuntimeConfig, AmmRuntimeHandle, AmmStateVersion, PoolKey,
    PoolRegistration, PoolStateDependencies, PoolStatus, ProtocolId, ProtocolMetadata, SimConfig,
    SimError, SwapQuote, UniswapV2Metadata,
};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::reactive::ReactiveInputBatch;

struct RouteAdapter;

impl AmmAdapter for RouteAdapter {
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
        Ok(SwapQuote::new(amount_in * U256::from(2_u64)))
    }
}

#[derive(Default)]
struct RouteBlocker {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl RouteBlocker {
    async fn wait_until_entered(&self) {
        loop {
            if self.state.lock().expect("blocker poisoned").0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    fn block(&self) {
        let mut state = self.state.lock().expect("blocker poisoned");
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).expect("blocker poisoned");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("blocker poisoned");
        state.1 = true;
        self.changed.notify_all();
    }
}

struct BlockingRouteAdapter {
    blocker: Arc<RouteBlocker>,
}

struct ConcurrencyRouteAdapter {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    released: Arc<AtomicBool>,
}

struct PanicOnceRouteAdapter {
    calls: Arc<AtomicUsize>,
}

struct CoalescingRouteAdapter {
    blocker: Arc<RouteBlocker>,
    calls: Arc<AtomicUsize>,
}

struct BlockAfterFirstRouteAdapter {
    blocker: Arc<RouteBlocker>,
    calls: AtomicUsize,
}

impl AmmAdapter for BlockAfterFirstRouteAdapter {
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
        if self.calls.fetch_add(1, Ordering::AcqRel) > 0 {
            self.blocker.block();
        }
        Ok(SwapQuote::new(amount_in * U256::from(2_u64)))
    }
}

impl AmmAdapter for CoalescingRouteAdapter {
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
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.blocker.block();
        Ok(SwapQuote::new(amount_in * U256::from(2_u64)))
    }
}

impl AmmAdapter for PanicOnceRouteAdapter {
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
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            panic!("intentional route worker test panic");
        }
        Ok(SwapQuote::new(amount_in * U256::from(2_u64)))
    }
}

impl AmmAdapter for ConcurrencyRouteAdapter {
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
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.maximum.fetch_max(active, Ordering::AcqRel);
        while !self.released.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        self.active.fetch_sub(1, Ordering::AcqRel);
        Ok(SwapQuote::new(amount_in * U256::from(2_u64)))
    }
}

impl AmmAdapter for BlockingRouteAdapter {
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
        self.blocker.block();
        Ok(SwapQuote::new(amount_in * U256::from(2_u64)))
    }
}

fn header(number: u64, parent_hash: B256) -> RpcHeader {
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

async fn setup_cache(header: &RpcHeader) -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = EvmCache::new(Arc::new(provider)).await;
    cache.advance_block(header).expect("valid block context");
    cache
}

fn ready_pool(pool: Address, token0: Address, token1: Address) -> PoolRegistration {
    PoolRegistration::new(PoolKey::UniswapV2(pool))
        .with_metadata(ProtocolMetadata::UniswapV2(
            UniswapV2Metadata::default()
                .with_token0(token0)
                .with_token1(token1)
                .with_fee_bps(30),
        ))
        .with_status(PoolStatus::Ready)
}

fn indexed_address(value: u64) -> Address {
    let mut bytes = [0_u8; 20];
    bytes[12..].copy_from_slice(&value.to_be_bytes());
    Address::from(bytes)
}

async fn spawn_system(
    block: u64,
) -> Result<(
    AmmRuntimeHandle,
    LiveRouteRuntimeHandle,
    RpcHeader,
    Address,
    Address,
)> {
    spawn_system_with_adapter(block, Arc::new(RouteAdapter)).await
}

async fn spawn_system_with_adapter(
    block: u64,
    adapter: Arc<dyn AmmAdapter>,
) -> Result<(
    AmmRuntimeHandle,
    LiveRouteRuntimeHandle,
    RpcHeader,
    Address,
    Address,
)> {
    spawn_system_with_adapter_config(
        block,
        adapter,
        LiveRouteRuntimeConfig::default().with_worker_threads(1),
    )
    .await
}

async fn spawn_system_with_adapter_config(
    block: u64,
    adapter: Arc<dyn AmmAdapter>,
    route_config: LiveRouteRuntimeConfig,
) -> Result<(
    AmmRuntimeHandle,
    LiveRouteRuntimeHandle,
    RpcHeader,
    Address,
    Address,
)> {
    let baseline = header(block, B256::repeat_byte(0x69));
    let cache = setup_cache(&baseline).await;
    let token_in = Address::repeat_byte(0x01);
    let token_out = Address::repeat_byte(0x02);
    let mut registry = AdapterRegistry::new();
    registry.register_adapter(adapter)?;
    registry.register_pool(ready_pool(Address::repeat_byte(0x70), token_in, token_out))?;
    let runtime = AmmRuntime::spawn(
        cache,
        registry,
        AmmRuntimeBaseline::from_verified_header(1, baseline.clone())?,
        AmmRuntimeConfig::default(),
    )?;
    let routes =
        LiveRouteRuntime::spawn(&runtime, GraphBuildOptions::default(), route_config).await?;
    Ok((runtime, routes, baseline, token_in, token_out))
}

#[tokio::test(flavor = "multi_thread")]
async fn route_subscription_publishes_an_initial_snapshot_bound_quote() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (runtime, routes, _, token_in, token_out) = spawn_system(700).await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;

            let ready = loop {
                let snapshot = subscription.changed().await?;
                if matches!(snapshot.state(), RouteSubscriptionState::Ready { .. }) {
                    break snapshot;
                }
            };
            let RouteSubscriptionState::Ready { best, source, .. } = ready.state() else {
                unreachable!();
            };
            assert_eq!(
                best.as_ref().expect("route quote").quote().amount_out,
                U256::from(20_u64)
            );
            assert_eq!(source.runtime_id(), runtime.latest_snapshot().runtime_id());
            assert_eq!(source.state_version(), runtime.latest_snapshot().version());
            assert_eq!(source.point(), runtime.latest_snapshot().point());
            assert_eq!(ready.view().snapshot().runtime_id(), source.runtime_id());
            assert_eq!(ready.view().snapshot().version(), source.state_version());
            assert_eq!(ready.view().snapshot().point(), source.point());
            assert_eq!(ready.view().graph().version(), source.graph_version());

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn route_runtime_started_empty_publishes_after_first_pool_arrives() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let baseline = header(701, B256::repeat_byte(0x6a));
            let cache = setup_cache(&baseline).await;
            let token_in = Address::repeat_byte(0x01);
            let token_out = Address::repeat_byte(0x02);
            let mut registry = AdapterRegistry::new();
            registry.register_adapter(Arc::new(RouteAdapter))?;
            let runtime = AmmRuntime::spawn(
                cache,
                registry,
                AmmRuntimeBaseline::from_verified_header(1, baseline)?,
                AmmRuntimeConfig::default(),
            )?;
            let routes = LiveRouteRuntime::spawn(
                &runtime,
                GraphBuildOptions::default(),
                LiveRouteRuntimeConfig::default().with_worker_threads(1),
            )
            .await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;

            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    let snapshot = subscription.changed().await?;
                    if matches!(snapshot.state(), RouteSubscriptionState::Failed { .. }) {
                        return Ok::<_, anyhow::Error>(());
                    }
                }
            })
            .await??;

            runtime
                .install_prepared_pools(
                    vec![ready_pool(Address::repeat_byte(0x71), token_in, token_out)],
                    runtime.latest_snapshot().point(),
                )
                .await?;

            let ready = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    let snapshot = subscription.changed().await?;
                    if matches!(
                        snapshot.state(),
                        RouteSubscriptionState::Ready { best: Some(_), .. }
                    ) {
                        return Ok::<_, anyhow::Error>(snapshot);
                    }
                }
            })
            .await??;
            let RouteSubscriptionState::Ready { best, .. } = ready.state() else {
                unreachable!();
            };
            assert_eq!(
                best.as_ref().expect("route quote").quote().amount_out,
                U256::from(20_u64)
            );

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn amm_commit_is_applied_before_route_invalidation_and_recompute() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (runtime, routes, baseline, token_in, token_out) = spawn_system(710).await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            let initial = loop {
                let snapshot = subscription.changed().await?;
                if matches!(snapshot.state(), RouteSubscriptionState::Ready { .. }) {
                    break snapshot;
                }
            };
            let RouteSubscriptionState::Ready {
                source: initial_source,
                ..
            } = initial.state()
            else {
                unreachable!();
            };

            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    header(711, baseline.hash),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;

            let mut applied_sequence = None;
            let mut invalidated_sequence = None;
            let scheduled_sequence = loop {
                let event = subscription.next_event().await?;
                match event.kind() {
                    LiveRouteRuntimeEventKind::AmmCommitApplied { .. } => {
                        applied_sequence = Some(event.sequence());
                    }
                    LiveRouteRuntimeEventKind::RouteInvalidated { .. }
                        if applied_sequence.is_some() =>
                    {
                        assert_ne!(
                            subscription.latest().state().source(),
                            Some(*initial_source),
                            "invalidation observers must never see the old authoritative source",
                        );
                        invalidated_sequence = Some(event.sequence());
                    }
                    LiveRouteRuntimeEventKind::SearchScheduled { .. }
                        if invalidated_sequence.is_some() =>
                    {
                        break event.sequence();
                    }
                    _ => {}
                }
            };
            let applied_sequence = applied_sequence.expect("commit event");
            let invalidated_sequence = invalidated_sequence.expect("invalidation event");
            assert!(applied_sequence < invalidated_sequence);
            assert!(invalidated_sequence < scheduled_sequence);

            let current = loop {
                let snapshot = subscription.changed().await?;
                if let RouteSubscriptionState::Ready { source, .. } = snapshot.state()
                    && source.state_version() > initial_source.state_version()
                {
                    break snapshot;
                }
            };
            let RouteSubscriptionState::Ready { source, best, .. } = current.state() else {
                unreachable!();
            };
            assert_eq!(source.graph_version(), initial_source.graph_version());
            assert_eq!(
                best.as_ref().expect("recomputed route").quote().amount_out,
                U256::from(20_u64)
            );

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn external_cancellation_prevents_a_late_inflight_route_publication() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let blocker = Arc::new(RouteBlocker::default());
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter(
                720,
                Arc::new(BlockingRouteAdapter {
                    blocker: Arc::clone(&blocker),
                }),
            )
            .await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            blocker.wait_until_entered().await;

            subscription.cancellation_token().cancel();
            loop {
                let snapshot = subscription.changed().await?;
                if matches!(snapshot.state(), RouteSubscriptionState::Cancelled) {
                    break;
                }
            }
            assert!(matches!(
                subscription.latest().state(),
                RouteSubscriptionState::Cancelled
            ));
            blocker.release();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(matches!(
                subscription.latest().state(),
                RouteSubscriptionState::Cancelled
            ));

            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_interrupts_dense_exhaustive_enumeration_before_shutdown() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            const TOKEN_COUNT: usize = 34;
            let baseline = header(720, B256::repeat_byte(0x68));
            let cache = setup_cache(&baseline).await;
            let tokens = (0..TOKEN_COUNT)
                .map(|index| indexed_address(40_000 + index as u64))
                .collect::<Vec<_>>();
            let mut registry = AdapterRegistry::new();
            registry.register_adapter(Arc::new(RouteAdapter))?;
            let mut pool_index = 0_u64;
            for left in 0..TOKEN_COUNT {
                for right in (left + 1)..TOKEN_COUNT {
                    registry.register_pool(ready_pool(
                        indexed_address(50_000 + pool_index),
                        tokens[left],
                        tokens[right],
                    ))?;
                    pool_index += 1;
                }
            }
            let runtime = AmmRuntime::spawn(
                cache,
                registry,
                AmmRuntimeBaseline::from_verified_header(1, baseline)?,
                AmmRuntimeConfig::default(),
            )?;
            let routes = LiveRouteRuntime::spawn(
                &runtime,
                GraphBuildOptions::default(),
                LiveRouteRuntimeConfig::default()
                    .with_worker_threads(1)
                    .with_job_queue_capacity(1),
            )
            .await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(tokens[0], tokens[TOKEN_COUNT - 1], U256::from(10_u64))
                        .with_config(
                            SearchConfig::default()
                                .with_hops(1, 5)
                                .with_connector_tokens(tokens.iter().copied())
                                .with_mode(SearchMode::Heuristic(
                                    HeuristicSearchConfig::default()
                                        .with_beam_width(Some(1))
                                        .with_fast_lane(FastLaneConfig::enabled())
                                        .with_finalist_simulation(false, 1),
                                )),
                        ),
                    StreamingSearchConfig::default(),
                ))
                .await?;

            loop {
                let event = subscription.next_event().await?;
                if matches!(
                    event.kind(),
                    LiveRouteRuntimeEventKind::SearchEvent(RouteSearchEvent::PhaseCompleted {
                        phase: RouteSearchPhase::Heuristic,
                        ..
                    })
                ) {
                    break;
                }
            }

            subscription.cancel().await?;
            tokio::time::timeout(std::time::Duration::from_secs(1), routes.shutdown())
                .await
                .expect("dense exhaustive enumeration must observe cancellation")?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn replacing_a_route_request_rejects_the_old_inflight_result() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let blocker = Arc::new(RouteBlocker::default());
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter(
                723,
                Arc::new(BlockingRouteAdapter {
                    blocker: Arc::clone(&blocker),
                }),
            )
            .await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            let id = subscription.id();
            blocker.wait_until_entered().await;

            subscription
                .replace(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(11_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            subscription
                .replace(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(12_u64)),
                    StreamingSearchConfig::default().heuristic_only(),
                ))
                .await?;
            assert_eq!(subscription.id(), id);
            assert_eq!(subscription.latest().epoch(), 2);
            blocker.release();

            let ready = loop {
                let snapshot = subscription.changed().await?;
                if let RouteSubscriptionState::Ready { best: Some(_), .. } = snapshot.state() {
                    break snapshot;
                }
            };
            assert_eq!(ready.epoch(), 2);
            let RouteSubscriptionState::Ready {
                best: Some(best),
                source,
                ..
            } = ready.state()
            else {
                unreachable!();
            };
            assert_eq!(best.quote().amount_in, U256::from(12_u64));
            assert_eq!(best.quote().amount_out, U256::from(24_u64));
            assert_eq!(best.source(), *source);
            assert_eq!(ready.view().snapshot().version(), source.state_version());
            assert_eq!(ready.view().graph().version(), source.graph_version());

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_replacement_storm_coalesces_each_subscription_to_its_latest_epoch() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            const FANOUT: usize = 16;
            const REPLACEMENTS: u64 = 8;

            let blocker = Arc::new(RouteBlocker::default());
            let calls = Arc::new(AtomicUsize::new(0));
            let route_config = LiveRouteRuntimeConfig::default()
                .with_worker_threads(1)
                .with_job_queue_capacity(FANOUT + 1)
                .with_max_subscriptions(FANOUT);
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter_config(
                730,
                Arc::new(CoalescingRouteAdapter {
                    blocker: Arc::clone(&blocker),
                    calls: Arc::clone(&calls),
                }),
                route_config,
            )
            .await?;

            let mut subscriptions = Vec::with_capacity(FANOUT);
            for index in 0..FANOUT {
                subscriptions.push(
                    routes
                        .subscribe(RouteSubscriptionSpec::new(
                            RouteRequest::new(
                                token_in,
                                token_out,
                                U256::from(100_u64 + index as u64),
                            ),
                            StreamingSearchConfig::default(),
                        ))
                        .await?,
                );
            }
            blocker.wait_until_entered().await;

            for epoch in 1..=REPLACEMENTS {
                for (index, subscription) in subscriptions.iter().enumerate() {
                    subscription
                        .replace(RouteSubscriptionSpec::new(
                            RouteRequest::new(
                                token_in,
                                token_out,
                                U256::from(epoch * 1_000 + index as u64),
                            ),
                            StreamingSearchConfig::default(),
                        ))
                        .await?;
                }
            }
            assert!(subscriptions
                .iter()
                .all(|subscription| subscription.latest().epoch() == REPLACEMENTS));
            blocker.release();

            for (index, subscription) in subscriptions.iter_mut().enumerate() {
                let ready = loop {
                    let snapshot = subscription.latest();
                    if snapshot.epoch() == REPLACEMENTS
                        && matches!(
                            snapshot.state(),
                            RouteSubscriptionState::Ready { best: Some(_), .. }
                        )
                    {
                        break snapshot;
                    }
                    subscription.changed().await?;
                };
                let RouteSubscriptionState::Ready {
                    best: Some(best), ..
                } = ready.state()
                else {
                    unreachable!();
                };
                let expected = U256::from(REPLACEMENTS * 1_000 + index as u64);
                assert_eq!(best.quote().amount_in, expected);
                assert_eq!(best.quote().amount_out, expected * U256::from(2_u64));
            }
            assert_eq!(
                calls.load(Ordering::Acquire),
                FANOUT + 1,
                "only the blocked initial simulation and one latest simulation per subscription execute",
            );

            for subscription in &subscriptions {
                subscription.cancel().await?;
            }
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn request_invalidation_attributes_the_last_published_source() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let blocker = Arc::new(RouteBlocker::default());
            let adapter = Arc::new(BlockAfterFirstRouteAdapter {
                blocker: Arc::clone(&blocker),
                calls: AtomicUsize::new(0),
            });
            let (runtime, routes, baseline, token_in, token_out) =
                spawn_system_with_adapter(724, adapter).await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            let initial_source = loop {
                let snapshot = subscription.changed().await?;
                if let RouteSubscriptionState::Ready { source, .. } = snapshot.state() {
                    break *source;
                }
            };

            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    header(725, baseline.hash),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;
            blocker.wait_until_entered().await;
            subscription
                .replace(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(11_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;

            loop {
                let event = subscription.next_event().await?;
                if let LiveRouteRuntimeEventKind::RouteInvalidated { previous, reason } =
                    event.kind()
                    && *reason == RouteInvalidationReason::RequestChanged
                {
                    assert_eq!(*previous, initial_source);
                    break;
                }
            }
            blocker.release();

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn versioned_gas_simulation_rejects_a_quote_from_another_view() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (runtime, routes, baseline, token_in, token_out) = spawn_system(726).await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            let old_quote = loop {
                let snapshot = subscription.changed().await?;
                if let RouteSubscriptionState::Ready {
                    best: Some(best), ..
                } = snapshot.state()
                {
                    break Arc::clone(best);
                }
            };

            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    header(727, baseline.hash),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;
            let new_view = loop {
                let snapshot = subscription.changed().await?;
                if snapshot.view().snapshot().version() > old_quote.source().state_version() {
                    break Arc::clone(snapshot.view());
                }
            };

            let error = simulate_versioned_route_gas(
                &new_view,
                &old_quote,
                DemoRouterConfig::default(),
                None,
            )
            .expect_err("cross-view quote must be rejected before EVM execution");
            assert!(
                error
                    .to_string()
                    .contains("does not belong to the supplied live search view")
            );
            let error = simulate_versioned_route_gas_with_balance_mappings(
                &new_view,
                &old_quote,
                DemoRouterConfig::default(),
                None,
                &HashMap::new(),
            )
            .expect_err("mapping-aware simulation must enforce the same view fence");
            assert!(
                error
                    .to_string()
                    .contains("does not belong to the supplied live search view")
            );

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_closes_observers_even_while_external_handles_remain() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (runtime, routes, _, _, _) = spawn_system(725).await?;
            let retained_handle = routes.clone();
            let mut observer = routes.subscribe_events();

            routes.shutdown().await?;
            assert!(matches!(
                tokio::time::timeout(std::time::Duration::from_secs(1), observer.next_event())
                    .await
                    .expect("observer close must not hang"),
                Err(LiveRouteObserverError::Closed)
            ));

            drop(retained_handle);
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_all_route_handles_releases_the_critical_amm_subscription() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (runtime, routes, _, _, _) = spawn_system(726).await?;
            drop(routes);

            let replacement = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    match LiveRouteRuntime::spawn(
                        &runtime,
                        GraphBuildOptions::default(),
                        LiveRouteRuntimeConfig::default().with_worker_threads(1),
                    )
                    .await
                    {
                        Ok(replacement) => break replacement,
                        Err(LiveRouteRuntimeError::AmmSubscription(_)) => {
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("unexpected replacement error: {error}"),
                    }
                }
            })
            .await
            .expect("dropped handles must release the critical subscription");

            replacement.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn newer_commit_is_drained_while_busy_and_rejects_the_old_worker_result() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let blocker = Arc::new(RouteBlocker::default());
            let (runtime, routes, baseline, token_in, token_out) = spawn_system_with_adapter(
                730,
                Arc::new(BlockingRouteAdapter {
                    blocker: Arc::clone(&blocker),
                }),
            )
            .await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            blocker.wait_until_entered().await;
            let old_source = match subscription.latest().state() {
                RouteSubscriptionState::Searching { stamp, .. } => stamp.source(),
                state => panic!("expected blocked search, got {state:?}"),
            };

            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    header(731, baseline.hash),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;
            loop {
                let event = subscription.next_event().await?;
                if matches!(
                    event.kind(),
                    LiveRouteRuntimeEventKind::AmmCommitApplied { .. }
                ) {
                    break;
                }
            }
            assert!(matches!(
                subscription.latest().state(),
                RouteSubscriptionState::Pending { source, .. }
                    if source.state_version() > old_source.state_version()
            ));

            blocker.release();
            let mut stale_rejected = false;
            while !stale_rejected {
                let event = subscription.next_event().await?;
                stale_rejected = matches!(
                    event.kind(),
                    LiveRouteRuntimeEventKind::StaleResultRejected { produced, current }
                        if *produced == old_source && current.state_version() > old_source.state_version()
                );
            }
            let ready = loop {
                let snapshot = subscription.changed().await?;
                if let RouteSubscriptionState::Ready { source, .. } = snapshot.state()
                    && source.state_version() > old_source.state_version()
                {
                    break snapshot;
                }
            };
            assert!(matches!(
                ready.state(),
                RouteSubscriptionState::Ready { best: Some(_), .. }
            ));

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn reusable_worker_pool_enforces_one_global_concurrency_budget() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let active = Arc::new(AtomicUsize::new(0));
            let maximum = Arc::new(AtomicUsize::new(0));
            let released = Arc::new(AtomicBool::new(false));
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter_config(
                740,
                Arc::new(ConcurrencyRouteAdapter {
                    active: Arc::clone(&active),
                    maximum: Arc::clone(&maximum),
                    released: Arc::clone(&released),
                }),
                LiveRouteRuntimeConfig::default()
                    .with_worker_threads(2)
                    .with_job_queue_capacity(2),
            )
            .await?;
            let mut subscriptions = Vec::new();
            for amount in 1..=4_u64 {
                subscriptions.push(
                    routes
                        .subscribe(RouteSubscriptionSpec::new(
                            RouteRequest::new(token_in, token_out, U256::from(amount)),
                            StreamingSearchConfig::default(),
                        ))
                        .await?,
                );
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while maximum.load(Ordering::Acquire) < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await?;
            assert_eq!(maximum.load(Ordering::Acquire), 2);
            released.store(true, Ordering::Release);

            for subscription in &mut subscriptions {
                loop {
                    let snapshot = subscription.changed().await?;
                    if matches!(snapshot.state(), RouteSubscriptionState::Ready { .. }) {
                        break;
                    }
                }
            }
            assert_eq!(maximum.load(Ordering::Acquire), 2);
            for subscription in subscriptions {
                subscription.cancel().await?;
            }
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_panic_is_isolated_and_the_reusable_worker_keeps_serving() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter(
                750,
                Arc::new(PanicOnceRouteAdapter {
                    calls: Arc::clone(&calls),
                }),
            )
            .await?;
            let mut failed = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            loop {
                let snapshot = failed.changed().await?;
                if let RouteSubscriptionState::Failed { failure, .. } = snapshot.state() {
                    assert!(failure.worker_panicked());
                    break;
                }
            }

            let mut succeeding = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(11_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            loop {
                let snapshot = succeeding.changed().await?;
                if let RouteSubscriptionState::Ready { best, .. } = snapshot.state() {
                    assert_eq!(
                        best.as_ref().expect("worker recovered").quote().amount_out,
                        U256::from(22_u64)
                    );
                    break;
                }
            }

            failed.cancel().await?;
            succeeding.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn observer_lag_is_explicit_while_latest_route_state_remains_recoverable() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter_config(
                760,
                Arc::new(RouteAdapter),
                LiveRouteRuntimeConfig::default()
                    .with_worker_threads(1)
                    .with_event_capacity(2),
            )
            .await?;
            let mut observer = routes.subscribe_events();
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            loop {
                let snapshot = subscription.changed().await?;
                if matches!(snapshot.state(), RouteSubscriptionState::Ready { .. }) {
                    break;
                }
            }
            assert!(matches!(
                observer.next_event().await,
                Err(LiveRouteObserverError::Lagged(_))
            ));
            assert!(matches!(
                subscription.latest().state(),
                RouteSubscriptionState::Ready { best: Some(_), .. }
            ));

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn commits_coalesce_to_one_newest_replacement_while_a_worker_is_busy() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let blocker = Arc::new(RouteBlocker::default());
            let calls = Arc::new(AtomicUsize::new(0));
            let (runtime, routes, baseline, token_in, token_out) = spawn_system_with_adapter(
                770,
                Arc::new(CoalescingRouteAdapter {
                    blocker: Arc::clone(&blocker),
                    calls: Arc::clone(&calls),
                }),
            )
            .await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            blocker.wait_until_entered().await;
            let initial_version = runtime.latest_snapshot().version();
            let first = header(771, baseline.hash);
            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    first.clone(),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;
            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    header(772, first.hash),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;

            let mut applied = 0;
            while applied < 2 {
                let event = subscription.next_event().await?;
                if matches!(
                    event.kind(),
                    LiveRouteRuntimeEventKind::AmmCommitApplied { .. }
                ) {
                    applied += 1;
                }
            }
            blocker.release();
            let latest_version = AmmStateVersion::new(initial_version.get() + 2);
            loop {
                let snapshot = subscription.changed().await?;
                if let RouteSubscriptionState::Ready { source, .. } = snapshot.state()
                    && source.state_version() == latest_version
                {
                    break;
                }
            }
            assert_eq!(calls.load(Ordering::Acquire), 2);

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_storm_fanout_converges_every_subscription_on_the_latest_source() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            const FANOUT: usize = 16;
            let route_config = LiveRouteRuntimeConfig::default()
                .with_worker_threads(2)
                .with_job_queue_capacity(4)
                .with_max_subscriptions(FANOUT);
            let (runtime, routes, baseline, token_in, token_out) =
                spawn_system_with_adapter_config(775, Arc::new(RouteAdapter), route_config).await?;
            let mut subscriptions = Vec::with_capacity(FANOUT);
            for index in 0..FANOUT {
                let mut subscription = routes
                    .subscribe(RouteSubscriptionSpec::new(
                        RouteRequest::new(token_in, token_out, U256::from(100_u64 + index as u64)),
                        StreamingSearchConfig::default(),
                    ))
                    .await?;
                loop {
                    let snapshot = subscription.latest();
                    if matches!(snapshot.state(), RouteSubscriptionState::Ready { .. }) {
                        break;
                    }
                    subscription.changed().await?;
                }
                subscriptions.push(subscription);
            }

            let first = header(776, baseline.hash);
            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    first.clone(),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;
            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    header(777, first.hash),
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;
            let expected = runtime.latest_snapshot().version();

            for subscription in &mut subscriptions {
                loop {
                    let snapshot = subscription.latest();
                    if matches!(
                        snapshot.state(),
                        RouteSubscriptionState::Ready { source, best: Some(_), .. }
                            if source.state_version() == expected
                    ) {
                        break;
                    }
                    subscription.changed().await?;
                }
            }

            for subscription in &subscriptions {
                subscription.cancel().await?;
            }
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn same_point_graph_change_rejects_work_from_the_previous_graph_version() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let blocker = Arc::new(RouteBlocker::default());
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter(
                780,
                Arc::new(BlockingRouteAdapter {
                    blocker: Arc::clone(&blocker),
                }),
            )
            .await?;
            let mut subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            blocker.wait_until_entered().await;
            let old_source = subscription
                .latest()
                .state()
                .source()
                .expect("blocked search source");
            runtime
                .commit_prepared_pool(AmmPreparedPoolState::new(
                    ready_pool(Address::repeat_byte(0x71), token_in, token_out),
                    runtime.latest_snapshot().point(),
                    [],
                )?)
                .await?;
            loop {
                let event = subscription.next_event().await?;
                if matches!(
                    event.kind(),
                    LiveRouteRuntimeEventKind::AmmCommitApplied { .. }
                ) {
                    break;
                }
            }
            let new_source = subscription
                .latest()
                .state()
                .source()
                .expect("new graph source");
            assert_eq!(new_source.point(), old_source.point());
            assert_ne!(new_source.graph_version(), old_source.graph_version());

            blocker.release();
            loop {
                let snapshot = subscription.changed().await?;
                if matches!(
                    snapshot.state(),
                    RouteSubscriptionState::Ready { source, .. } if *source == new_source
                ) {
                    break;
                }
            }

            subscription.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_subscription_cancels_it_and_releases_capacity() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (runtime, routes, _, token_in, token_out) = spawn_system_with_adapter_config(
                790,
                Arc::new(RouteAdapter),
                LiveRouteRuntimeConfig::default()
                    .with_worker_threads(1)
                    .with_max_subscriptions(1),
            )
            .await?;
            let subscription = routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, U256::from(10_u64)),
                    StreamingSearchConfig::default(),
                ))
                .await?;
            assert!(matches!(
                routes
                    .subscribe(RouteSubscriptionSpec::new(
                        RouteRequest::new(token_in, token_out, U256::from(11_u64)),
                        StreamingSearchConfig::default(),
                    ))
                    .await,
                Err(LiveRouteRuntimeError::SubscriptionCapacity)
            ));
            drop(subscription);

            let replacement = loop {
                match routes
                    .subscribe(RouteSubscriptionSpec::new(
                        RouteRequest::new(token_in, token_out, U256::from(12_u64)),
                        StreamingSearchConfig::default(),
                    ))
                    .await
                {
                    Ok(subscription) => break subscription,
                    Err(LiveRouteRuntimeError::SubscriptionCapacity) => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            replacement.cancel().await?;
            routes.shutdown().await?;
            runtime.shutdown().await?;
            Ok(())
        })
        .await
}
