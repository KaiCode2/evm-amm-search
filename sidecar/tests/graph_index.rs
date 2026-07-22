use std::sync::Arc;

use alloy_consensus::Header as ConsensusHeader;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::Header as RpcHeader;
use alloy_transport::mock::Asserter;
use evm_amm_route_sidecar::graph_index::{GraphIndex, GraphIndexError};
use evm_amm_search::{AmmGraph, GraphBuildOptions, GraphPoolMutation, LiveAmmGraph};
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmAdapter, AmmEvictionPolicy, AmmRuntime, AmmRuntimeBaseline,
    AmmRuntimeConfig, AmmStateVersion, BalancerV2Metadata, PoolKey, PoolRegistration,
    PoolStateDependencies, PoolStatus, ProtocolId, ProtocolMetadata, SimConfig, SimError,
    SwapQuote, UniswapV2Metadata,
};
use evm_fork_cache::cache::EvmCache;

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

fn address(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn ready_v2(pool: u8, token0: Address, token1: Address) -> PoolRegistration {
    PoolRegistration::new(PoolKey::UniswapV2(address(pool)))
        .with_metadata(ProtocolMetadata::UniswapV2(
            UniswapV2Metadata::default()
                .with_token0(token0)
                .with_token1(token1)
                .with_fee_bps(30),
        ))
        .with_status(PoolStatus::Ready)
}

fn header(number: u64) -> RpcHeader {
    RpcHeader::new(ConsensusHeader {
        number,
        parent_hash: B256::repeat_byte(0x49),
        timestamp: 1_700_000_000 + number,
        base_fee_per_gas: Some(100 + number),
        gas_limit: 30_000_000,
        ..ConsensusHeader::default()
    })
}

#[test]
fn graph_index_reports_membership_and_pool_counts_for_the_searchable_graph() {
    let token_a = address(0x01);
    let token_b = address(0x02);
    let token_c = address(0x03);
    let token_d = address(0x04);
    let mut registry = AdapterRegistry::new();
    registry
        .register_pool(ready_v2(0x10, token_a, token_b))
        .unwrap();
    registry
        .register_pool(ready_v2(0x11, token_a, token_b))
        .unwrap();
    registry
        .register_pool(
            PoolRegistration::new(PoolKey::BalancerV2(B256::repeat_byte(0x12)))
                .with_metadata(ProtocolMetadata::BalancerV2(
                    BalancerV2Metadata::default().with_tokens([token_b, token_c, token_d]),
                ))
                .with_status(PoolStatus::Ready),
        )
        .unwrap();

    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let index = GraphIndex::from_graph(&graph, AmmStateVersion::new(7));

    assert_eq!(index.state_version(), AmmStateVersion::new(7));
    assert_eq!(index.stats(None).tokens(), 4);
    assert_eq!(index.stats(None).edges(), 10);
    assert_eq!(index.stats(None).pools(), 3);
    assert!(index.stats(Some(token_a)).token_present());
    assert_eq!(index.stats(Some(token_a)).token_pools(), 2);
    assert_eq!(index.stats(Some(token_b)).token_pools(), 3);
    assert!(!index.stats(Some(address(0xff))).token_present());
    assert_eq!(index.stats(Some(address(0xff))).token_pools(), 0);
}

#[test]
fn graph_index_reconciles_added_and_removed_topology() {
    let token_a = address(0x01);
    let token_b = address(0x02);
    let token_c = address(0x03);
    let first = ready_v2(0x10, token_a, token_b);
    let second = ready_v2(0x11, token_b, token_c);
    let second_key = second.key.clone();
    let mut registry = AdapterRegistry::new();
    registry.register_pool(first).unwrap();
    let mut graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let mut index = GraphIndex::from_graph(&graph, AmmStateVersion::new(1));

    assert_eq!(
        graph.apply_pool(&second, GraphBuildOptions::default()),
        GraphPoolMutation::Added { directed_edges: 2 }
    );
    index.reconcile_graph(&graph, AmmStateVersion::new(2));
    assert_eq!(index.stats(None).pools(), 2);
    assert_eq!(index.stats(None).tokens(), 3);
    assert_eq!(index.stats(Some(token_b)).token_pools(), 2);

    assert_eq!(
        graph.remove_pool_compacting(&second_key),
        GraphPoolMutation::Removed { directed_edges: 2 }
    );
    index.reconcile_graph(&graph, AmmStateVersion::new(3));
    assert_eq!(index.stats(None).pools(), 1);
    assert_eq!(index.stats(None).tokens(), 2);
    assert!(!index.stats(Some(token_c)).token_present());
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_index_applies_contiguous_runtime_deltas() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let baseline = header(500);
            let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
            let mut cache = EvmCache::new(Arc::new(provider)).await;
            cache.advance_block(&baseline).unwrap();
            let mut registry = AdapterRegistry::new();
            registry.register_adapter(Arc::new(TestAdapter)).unwrap();
            let runtime = AmmRuntime::spawn(
                cache,
                registry,
                AmmRuntimeBaseline::from_verified_header(1, baseline).unwrap(),
                AmmRuntimeConfig::default(),
            )
            .unwrap();
            let mut changes = runtime.subscribe_changes().await.unwrap();
            let mut live =
                LiveAmmGraph::from_snapshot(changes.snapshot(), GraphBuildOptions::default())
                    .unwrap();
            let mut index = GraphIndex::from_graph(live.graph(), changes.snapshot().version());
            let token_a = address(0x01);
            let token_b = address(0x02);
            let token_c = address(0x03);
            // Preserve a deliberately non-canonical metadata order: graph-index
            // reconciliation must compare token sets, not adapter ordering.
            let first = ready_v2(0x10, token_b, token_a);
            let second = ready_v2(0x11, token_b, token_c);

            runtime
                .install_prepared_pools(vec![first.clone(), second], changes.snapshot().point())
                .await
                .unwrap();
            let added = changes.next_commit().await.unwrap();
            let added_delta = live.apply_commit(&added).unwrap();
            let mut skipped_commit_index = index.clone();
            index.apply_delta(&added_delta).unwrap();
            assert_eq!(index.state_version(), added.snapshot().version());
            assert_eq!(index.stats(None).pools(), 2);
            assert_eq!(index.stats(Some(token_b)).token_pools(), 2);

            let first_instance = added
                .snapshot()
                .registry()
                .pool_instance(&first.key)
                .unwrap()
                .clone();
            runtime
                .remove_pool(first_instance, AmmEvictionPolicy::Retain)
                .await
                .unwrap();
            let removed = changes.next_commit().await.unwrap();
            let removed_delta = live.apply_commit(&removed).unwrap();
            assert!(matches!(
                skipped_commit_index.apply_delta(&removed_delta),
                Err(GraphIndexError::NonContiguous { .. })
            ));
            index.apply_delta(&removed_delta).unwrap();
            assert_eq!(index.state_version(), removed.snapshot().version());
            assert_eq!(index.stats(None).pools(), 1);
            assert!(!index.stats(Some(token_a)).token_present());
            assert!(!index.apply_delta(&removed_delta).unwrap());

            runtime.shutdown().await.unwrap();
        })
        .await;
}
