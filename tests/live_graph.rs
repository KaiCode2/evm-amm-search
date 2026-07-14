#![cfg(feature = "live-runtime")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
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
    AmmSearcher, GraphBuildOptions, GraphTopologyImpact, IncrementalRouteUpdateStatus,
    LiveAmmGraph, LiveGraphError, LiveSearchView, RecomputeReason, RouteRequest, SearchControl,
    StreamingSearchConfig,
};
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmAdapter, AmmCanonicalBatch, AmmEvictionPolicy,
    AmmPreparedPoolState, AmmRuntime, AmmRuntimeBaseline, AmmRuntimeConfig, PoolKey,
    PoolRegistration, PoolStateDependencies, PoolStatus, ProtocolId, ProtocolMetadata, SimConfig,
    SimError, StateUpdate, SwapQuote, UniswapV2Metadata,
};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::reactive::ReactiveInputBatch;

struct GraphTestAdapter {
    quotes: Arc<AtomicUsize>,
}

impl AmmAdapter for GraphTestAdapter {
    fn protocol(&self) -> ProtocolId {
        ProtocolId::UniswapV2
    }

    fn state_dependencies(&self, _pool: &PoolRegistration) -> PoolStateDependencies {
        PoolStateDependencies::default()
    }

    fn simulate_swap(
        &self,
        _pool: &PoolRegistration,
        cache: &mut dyn AdapterCache,
        _token_in: Address,
        _token_out: Address,
        amount_in: U256,
        _config: &SimConfig,
    ) -> Result<SwapQuote, SimError> {
        self.quotes.fetch_add(1, Ordering::Relaxed);
        let multiplier = cache
            .cached_storage(Address::repeat_byte(0xee), U256::ZERO)
            .unwrap_or(U256::ONE);
        Ok(SwapQuote::new(amount_in * multiplier))
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
    cache
        .advance_block(header)
        .expect("test header provides block context");
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

#[tokio::test(flavor = "multi_thread")]
async fn live_graph_applies_contiguous_commits_without_rebuilding_state_only_changes() -> Result<()>
{
    tokio::task::LocalSet::new()
        .run_until(async {
            let baseline_header = header(500, B256::repeat_byte(0x49));
            let mut cache = setup_cache(&baseline_header).await;
            AdapterCache::apply_updates(
                &mut cache,
                &[
                    StateUpdate::slot(
                        Address::repeat_byte(0xee),
                        U256::ZERO,
                        U256::from(2_u64),
                    ),
                    StateUpdate::slot(
                        Address::repeat_byte(0x02),
                        U256::from(7_u64),
                        U256::from(777_u64),
                    ),
                ],
            );
            let mut registry = AdapterRegistry::new();
            let quotes = Arc::new(AtomicUsize::new(0));
            registry.register_adapter(Arc::new(GraphTestAdapter {
                quotes: Arc::clone(&quotes),
            }))?;
            let runtime = AmmRuntime::spawn(
                cache,
                registry,
                AmmRuntimeBaseline::from_verified_header(1, baseline_header.clone())?,
                AmmRuntimeConfig::default(),
            )?;
            let mut subscription = runtime.subscribe_changes().await?;
            let mut live =
                LiveAmmGraph::from_snapshot(subscription.snapshot(), GraphBuildOptions::default())?;
            let initial_graph_version = live.version();
            let foreign_cache = setup_cache(&baseline_header).await;
            let mut foreign_registry = AdapterRegistry::new();
            foreign_registry.register_adapter(Arc::new(GraphTestAdapter {
                quotes: Arc::clone(&quotes),
            }))?;
            let foreign_runtime = AmmRuntime::spawn(
                foreign_cache,
                foreign_registry,
                AmmRuntimeBaseline::from_verified_header(1, baseline_header.clone())?,
                AmmRuntimeConfig::default(),
            )?;
            let mut foreign_subscription = foreign_runtime.subscribe_changes().await?;
            assert!(matches!(
                AmmSearcher::from_snapshot(foreign_subscription.snapshot(), &live),
                Err(evm_amm_search::SearchError::SnapshotGraphMismatch)
            ));
            let foreign_pool = ready_pool(
                Address::repeat_byte(0x50),
                Address::repeat_byte(0x01),
                Address::repeat_byte(0x02),
            );
            foreign_runtime
                .commit_prepared_pool(AmmPreparedPoolState::new(
                    foreign_pool,
                    foreign_subscription.snapshot().point(),
                    [],
                )?)
                .await?;
            let foreign_commit = foreign_subscription
                .next_commit()
                .await
                .expect("foreign runtime addition");
            assert!(matches!(
                live.apply_commit(&foreign_commit),
                Err(LiveGraphError::RuntimeMismatch { .. })
            ));
            assert_eq!(live.version(), initial_graph_version);
            assert_eq!(live.state_version(), subscription.snapshot().version());
            foreign_runtime.shutdown().await?;

            let pool = ready_pool(
                Address::repeat_byte(0x51),
                Address::repeat_byte(0x01),
                Address::repeat_byte(0x02),
            );

            runtime
                .commit_prepared_pool(AmmPreparedPoolState::new(
                    pool.clone(),
                    subscription.snapshot().point(),
                    [],
                )?)
                .await?;
            let added = subscription.next_commit().await.expect("addition commit");
            let added_delta = live.apply_commit(&added)?;
            assert_eq!(added_delta.impact(), GraphTopologyImpact::Localized);
            assert_eq!(live.version().lineage(), initial_graph_version.lineage());
            assert_eq!(live.version().revision(), 1);
            assert_eq!(live.graph().edge_count(), 2);
            let mut lagged = live.clone();
            let mut recovering = live.clone();
            let mut quote_cache = setup_cache(&baseline_header).await;
            AdapterCache::apply_updates(&mut quote_cache, &[StateUpdate::slot(
                Address::repeat_byte(0xee),
                U256::ZERO,
                U256::from(9_u64),
            )]);
            let searcher = AmmSearcher::from_snapshot(added.snapshot(), &live)?;
            let request = RouteRequest::new(
                Address::repeat_byte(0x01),
                Address::repeat_byte(0x02),
                U256::from(10_u64),
            );
            let snapshot_quote = searcher.find_best_route_snapshot(&request)?;
            assert_eq!(snapshot_quote.amount_out, U256::from(20_u64));
            let snapshot_routes = searcher.find_routes_snapshot(&request)?;
            assert_eq!(snapshot_routes.len(), 1);
            assert_eq!(snapshot_routes[0].amount_out, U256::from(20_u64));
            let added_view = LiveSearchView::new(Arc::clone(added.snapshot()), &live)?;
            let mut session = searcher.start_route_session(
                &request,
                &mut quote_cache,
                StreamingSearchConfig::default(),
                |_| SearchControl::Continue,
            )?;
            assert_eq!(quotes.load(Ordering::Relaxed), 3);
            assert_eq!(
                session.best().expect("snapshot-backed quote").amount_out,
                U256::from(20_u64),
                "live search must ignore an arbitrary caller cache and quote the immutable snapshot",
            );
            let explicit = searcher.quote_path_snapshot(
                &session.best().expect("snapshot-backed route").path,
                U256::from(10_u64),
                &SimConfig::default(),
            )?;
            assert_eq!(explicit.amount_out, U256::from(20_u64));
            assert_eq!(quotes.load(Ordering::Relaxed), 4);

            let graph_before_state = live.graph_snapshot();
            let liquidity_before_state = live.liquidity_snapshot();
            let next_header = header(501, baseline_header.hash);
            runtime
                .ingest_batch(AmmCanonicalBatch::from_verified_block(
                    1,
                    next_header,
                    runtime.interest_revision(),
                    ReactiveInputBatch::new(Vec::new()),
                )?)
                .await?;
            let state_only = subscription.next_commit().await.expect("state-only commit");
            assert!(matches!(
                AmmSearcher::from_snapshot(state_only.snapshot(), &recovering),
                Err(evm_amm_search::SearchError::SnapshotGraphMismatch)
            ));
            let state_delta = live.apply_commit(&state_only)?;
            assert_eq!(state_delta.impact(), GraphTopologyImpact::Unchanged);
            assert_eq!(
                state_delta.from_graph_version(),
                state_delta.to_graph_version()
            );
            assert_eq!(live.version().revision(), 1);
            assert!(Arc::ptr_eq(&graph_before_state, &live.graph_snapshot()));
            assert!(Arc::ptr_eq(
                &liquidity_before_state,
                &live.liquidity_snapshot()
            ));
            assert!(matches!(
                LiveSearchView::new(Arc::clone(added.snapshot()), &live),
                Err(evm_amm_search::SearchError::SnapshotGraphMismatch)
            ));
            let state_view = LiveSearchView::new(Arc::clone(state_only.snapshot()), &live)?;
            let old_view_quote = added_view.searcher().quote_path(
                &session.best().expect("snapshot-backed route").path,
                U256::from(10_u64),
                &mut quote_cache,
                &SimConfig::default(),
            )?;
            let current_view_quote = state_view.searcher().quote_path(
                &session.best().expect("snapshot-backed route").path,
                U256::from(10_u64),
                &mut quote_cache,
                &SimConfig::default(),
            )?;
            assert_eq!(old_view_quote, current_view_quote);
            assert_eq!(added_view.snapshot().version(), added.snapshot().version());
            assert_eq!(
                state_view.snapshot().version(),
                state_only.snapshot().version()
            );
            let state_searcher = AmmSearcher::from_snapshot(state_only.snapshot(), &live)?;
            let stopped_refresh = session.refresh_affected(
                &state_searcher,
                &mut quote_cache,
                Default::default(),
                |_| SearchControl::Stop,
            );
            assert_eq!(
                stopped_refresh.status,
                IncrementalRouteUpdateStatus::RecomputeRequired
            );
            assert!(matches!(
                stopped_refresh.recompute_reason,
                Some(RecomputeReason::StatePointChanged { .. })
            ));
            let refresh = session.refresh_affected(
                &state_searcher,
                &mut quote_cache,
                Default::default(),
                |_| SearchControl::Continue,
            );
            assert_eq!(
                refresh.status,
                IncrementalRouteUpdateStatus::RecomputeRequired
            );
            assert!(matches!(
                refresh.recompute_reason,
                Some(RecomputeReason::StatePointChanged { .. })
            ));
            assert_eq!(quotes.load(Ordering::Relaxed), 6);
            let static_searcher = AmmSearcher::new(
                state_only.snapshot().registry().registry(),
                live.graph(),
            );
            let scope_refresh = session.refresh_affected(
                &static_searcher,
                &mut quote_cache,
                Default::default(),
                |_| SearchControl::Continue,
            );
            assert_eq!(
                scope_refresh.recompute_reason,
                Some(RecomputeReason::StateScopeChanged)
            );

            let second_pool = ready_pool(
                Address::repeat_byte(0x52),
                Address::repeat_byte(0x01),
                Address::repeat_byte(0x02),
            );
            runtime
                .commit_prepared_pool(AmmPreparedPoolState::new(
                    second_pool.clone(),
                    state_only.snapshot().point(),
                    [],
                )?)
                .await?;
            let second_added = subscription.next_commit().await.expect("parallel addition");

            let lagged_version = lagged.version();
            let lagged_state_version = lagged.state_version();
            assert!(matches!(
                lagged.apply_commit(&second_added),
                Err(LiveGraphError::NonContiguousStateVersion { .. })
            ));
            assert_eq!(lagged.version(), lagged_version);
            assert_eq!(lagged.state_version(), lagged_state_version);
            assert_eq!(lagged.graph().edge_count(), 2);
            lagged.apply_commit(&state_only)?;
            lagged.apply_commit(&second_added)?;
            assert_eq!(lagged.graph().edge_count(), 4);

            let recovery_delta = recovering.reconcile_snapshot(second_added.snapshot())?;
            assert_eq!(
                recovery_delta.impact(),
                GraphTopologyImpact::FullReconciliation
            );
            assert_eq!(recovering.version().revision(), 2);
            assert_eq!(recovering.graph().edge_count(), 4);
            assert!(matches!(
                recovering.reconcile_snapshot(added.snapshot()),
                Err(LiveGraphError::StaleSnapshot { .. })
            ));

            let second_delta = live.apply_commit(&second_added)?;
            assert_eq!(second_delta.impact(), GraphTopologyImpact::Localized);
            assert_eq!(live.version().revision(), 2);
            assert_eq!(live.liquidity().len(), 4);
            assert_eq!(
                live.set_erc20_liquidity_slot(
                    Address::repeat_byte(0x02),
                    Address::repeat_byte(0x52),
                    U256::from(7_u64),
                ),
                1
            );
            let liquidity_refresh =
                live.refresh_liquidity_from_snapshot(second_added.snapshot())?;
            assert!(liquidity_refresh.refreshed_balances >= 1);
            assert_eq!(
                live.liquidity()
                    .fresh_balance(&second_pool.key, Address::repeat_byte(0x02)),
                Some(U256::from(777_u64))
            );
            assert!(matches!(
                live.refresh_liquidity_from_snapshot(added.snapshot()),
                Err(LiveGraphError::SnapshotStateMismatch { .. })
            ));
            live.mark_liquidity_stale(&second_pool.key, Address::repeat_byte(0x02))?;
            assert_eq!(
                live.liquidity()
                    .fresh_balance(&second_pool.key, Address::repeat_byte(0x02)),
                None
            );

            let instance = second_added
                .snapshot()
                .registry()
                .pool_instance(&pool.key)
                .expect("active pool generation")
                .clone();
            runtime
                .remove_pool(instance, AmmEvictionPolicy::Retain)
                .await?;
            let removed = subscription.next_commit().await.expect("removal commit");
            let removed_delta = live.apply_commit(&removed)?;
            assert_eq!(removed_delta.impact(), GraphTopologyImpact::Localized);
            assert_eq!(removed_delta.liquidity().removed_targets, 4);
            assert_eq!(live.version().revision(), 3);
            assert_eq!(live.liquidity().len(), 0);
            assert_eq!(live.graph().node_count(), 2);
            assert_eq!(live.graph().edge_count(), 2);

            let second_instance = removed
                .snapshot()
                .registry()
                .pool_instance(&second_pool.key)
                .expect("second pool generation")
                .clone();
            runtime
                .remove_pool(second_instance, AmmEvictionPolicy::Retain)
                .await?;
            let second_removed = subscription
                .next_commit()
                .await
                .expect("second removal commit");
            live.apply_commit(&second_removed)?;
            assert_eq!(live.version().revision(), 4);
            assert_eq!(live.graph().node_count(), 0);
            assert_eq!(live.graph().edge_count(), 0);

            runtime.shutdown().await?;
            Ok(())
        })
        .await
}
