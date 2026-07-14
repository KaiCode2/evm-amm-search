use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, Bytes, Log, U256, keccak256};
use evm_amm_search::{
    AdaptiveEdgeShortlistConfig, AffectedPools, AmmGraph, AmmSearcher, BalanceState, CycleRequest,
    FastLaneConfig, GraphBuildOptions, GraphPoolMutation, HeuristicSearchConfig, Hop,
    IncrementalRouteUpdateStatus, LiquidityIndexScope, LiquidityPruningConfig,
    ParallelSearchConfig, PoolLiquidityIndex, RecomputeReason, RoutePath, RouteRequest,
    RouteSearchEvent, RouteSearchPhase, RouteUpdateEvent, SearchConfig, SearchControl, SearchError,
    SearchFinality, SearchMode, SkippedPoolReason, StreamingSearchConfig, StreamingThresholdPolicy,
};
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmAdapter, CacheError, CallOutcome, CurveMetadata,
    CurveVariant, EventSource, PoolKey, PoolRegistration, PoolStatus, ProtocolId, ProtocolMetadata,
    SimConfig, SimError, SlotChange, StateDiff, StateUpdate, StateView, SwapQuote,
    UniswapV2Metadata, V3Metadata,
};
use evm_amm_state::adapters::{BalancerTokenBalance, BalancerV2Metadata, SolidlyV2Metadata};
use evm_fork_cache::cache::EvmCache;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use alloy_provider::{RootProvider, network::AnyNetwork};
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn ready_v2(pool: Address, token0: Address, token1: Address) -> PoolRegistration {
    PoolRegistration::new(PoolKey::UniswapV2(pool))
        .with_metadata(ProtocolMetadata::UniswapV2(
            UniswapV2Metadata::default()
                .with_token0(token0)
                .with_token1(token1)
                .with_fee_bps(30),
        ))
        .with_status(PoolStatus::Ready)
}

fn ready_v3(pool: Address, token0: Address, token1: Address, fee: u32) -> PoolRegistration {
    PoolRegistration::new(PoolKey::UniswapV3(pool))
        .with_metadata(ProtocolMetadata::UniswapV3(
            V3Metadata::default()
                .with_token0(token0)
                .with_token1(token1)
                .with_fee(fee),
        ))
        .with_status(PoolStatus::Ready)
}

fn register(registry: &mut AdapterRegistry, pool: PoolRegistration) {
    registry.register_pool(pool).expect("pool registers");
}

#[test]
fn heuristic_default_is_balanced_and_latency_first_is_opt_in() {
    let balanced = HeuristicSearchConfig::default();
    assert_eq!(balanced, HeuristicSearchConfig::balanced());
    assert!(balanced.target_first);
    assert!(balanced.prefix_dominance);
    assert!(balanced.fast_lane.enabled);
    assert!(!balanced.edge_shortlist.enabled);
    assert!(balanced.edge_shortlist.protocol_ordering);
    assert!(balanced.upper_bound_pruning.enabled);
    assert!(balanced.upper_bound_pruning.balance_cap_pruning);

    let latency_first = HeuristicSearchConfig::latency_first();
    assert!(latency_first.edge_shortlist.enabled);
    assert!(latency_first.edge_shortlist.refine_parallel_edges);
    assert_eq!(latency_first.edge_shortlist.initial_edges_per_pair, 1);
    assert_eq!(latency_first.edge_shortlist.refinement_edges_per_pair, 3);
}

#[test]
fn graph_indexes_supported_ready_metadata_and_skips_the_rest() {
    let mut registry = AdapterRegistry::new();

    register(&mut registry, ready_v2(addr(0x10), addr(0x01), addr(0x02)));
    register(
        &mut registry,
        PoolRegistration::new(PoolKey::UniswapV3(addr(0x11)))
            .with_metadata(ProtocolMetadata::UniswapV3(
                V3Metadata::default()
                    .with_token0(addr(0x03))
                    .with_token1(addr(0x04))
                    .with_fee(500),
            ))
            .with_status(PoolStatus::Ready),
    );
    register(
        &mut registry,
        PoolRegistration::new(PoolKey::SolidlyV2(addr(0x12)))
            .with_metadata(ProtocolMetadata::SolidlyV2(
                SolidlyV2Metadata::default()
                    .with_token0(addr(0x05))
                    .with_token1(addr(0x06)),
            ))
            .with_status(PoolStatus::Ready),
    );
    register(
        &mut registry,
        PoolRegistration::new(PoolKey::BalancerV2(alloy_primitives::B256::repeat_byte(
            0x13,
        )))
        .with_metadata(ProtocolMetadata::BalancerV2(
            BalancerV2Metadata::default().with_tokens([addr(0x07), addr(0x08), addr(0x09)]),
        ))
        .with_status(PoolStatus::Ready),
    );
    register(
        &mut registry,
        PoolRegistration::new(PoolKey::Curve(addr(0x14)))
            .with_metadata(ProtocolMetadata::Curve(
                CurveMetadata::default()
                    .with_coins([addr(0x0a), addr(0x0b), addr(0x0c)])
                    .with_variant(CurveVariant::CryptoSwap),
            ))
            .with_status(PoolStatus::Ready),
    );

    register(
        &mut registry,
        ready_v2(addr(0x15), addr(0x20), addr(0x21)).with_status(PoolStatus::Pending),
    );
    register(
        &mut registry,
        PoolRegistration::new(PoolKey::UniswapV2(addr(0x16)))
            .with_metadata(ProtocolMetadata::UniswapV2(
                UniswapV2Metadata::default().with_token0(addr(0x22)),
            ))
            .with_status(PoolStatus::Ready),
    );
    register(
        &mut registry,
        PoolRegistration::new(PoolKey::Custom(
            evm_amm_state::adapters::CustomPoolKey::Address {
                protocol: "test",
                address: addr(0x17),
            },
        ))
        .with_status(PoolStatus::Ready),
    );

    let report = AmmGraph::from_registry(&registry, GraphBuildOptions::default());

    assert_eq!(report.indexed_pools.len(), 5);
    assert_eq!(report.graph.edge_count(), 18);
    assert_eq!(report.graph.node_count(), 12);
    assert_eq!(report.skipped_pools.len(), 3);
    assert!(
        report
            .skipped_pools
            .iter()
            .any(|skip| skip.reason == SkippedPoolReason::Status(PoolStatus::Pending))
    );
    assert!(
        report
            .skipped_pools
            .iter()
            .any(|skip| matches!(skip.reason, SkippedPoolReason::MissingMetadata(_)))
    );
    assert!(
        report
            .skipped_pools
            .iter()
            .any(|skip| skip.reason == SkippedPoolReason::UnsupportedMetadata)
    );
}

#[test]
fn graph_can_include_degraded_and_remove_pool_edges() {
    let mut registry = AdapterRegistry::new();
    let pool = PoolKey::UniswapV2(addr(0x10));
    register(
        &mut registry,
        ready_v2(addr(0x10), addr(0x01), addr(0x02)).with_status(PoolStatus::Degraded),
    );

    let mut graph = AmmGraph::from_registry(&registry, GraphBuildOptions::include_degraded()).graph;
    assert_eq!(graph.edge_count(), 2);
    assert_eq!(graph.edges_for_pool(&pool).len(), 2);

    assert_eq!(graph.remove_pool(&pool), 2);
    assert_eq!(graph.edge_count(), 0);
    assert_eq!(
        graph.node_count(),
        2,
        "token nodes are intentionally retained"
    );
    registry.unregister_pool(&pool);
    let before_compaction = graph.version();
    graph.rebuild_from_registry(&registry, GraphBuildOptions::include_degraded());
    assert_eq!(graph.node_count(), 0);
    assert_eq!(graph.version().lineage(), before_compaction.lineage());
    assert_eq!(graph.version().revision(), before_compaction.revision() + 1);
}

#[test]
fn graph_preserves_parallel_pool_edges() {
    let mut registry = AdapterRegistry::new();
    register(&mut registry, ready_v2(addr(0x10), addr(0x01), addr(0x02)));
    register(&mut registry, ready_v2(addr(0x11), addr(0x01), addr(0x02)));

    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;

    assert_eq!(graph.node_count(), 2);
    assert_eq!(graph.edge_count(), 4);
    assert_eq!(
        graph.edges_for_pool(&PoolKey::UniswapV2(addr(0x10))).len(),
        2
    );
    assert_eq!(
        graph.edges_for_pool(&PoolKey::UniswapV2(addr(0x11))).len(),
        2
    );
}

#[test]
fn liquidity_targets_reconcile_incrementally_across_parallelism_transitions() {
    let token_a = addr(0x01);
    let token_b = addr(0x02);
    let first = ready_v2(addr(0x10), token_a, token_b);
    let second = ready_v2(addr(0x11), token_a, token_b);
    let mut registry = AdapterRegistry::new();
    register(&mut registry, first.clone());
    let mut graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, initial) = PoolLiquidityIndex::from_registry(&registry, &graph);
    assert_eq!(initial.tracked_balances, 0);

    register(&mut registry, second.clone());
    graph.apply_pool(&second, GraphBuildOptions::default());
    let added = liquidity.reconcile_pools(
        &registry,
        &graph,
        LiquidityIndexScope::ParallelEdgeOutputs,
        [first.key.clone(), second.key.clone()],
    );
    assert_eq!(added.added_targets, 4);
    assert_eq!(liquidity.len(), 4);
    assert_eq!(
        liquidity.set_erc20_balance_slot(token_b, addr(0x10), U256::from(7_u64)),
        1
    );
    liquidity
        .set_balance(&first.key, token_b, U256::from(777_u64))
        .expect("tracked output balance");

    let unchanged = liquidity.reconcile_pools(
        &registry,
        &graph,
        LiquidityIndexScope::ParallelEdgeOutputs,
        [first.key.clone()],
    );
    assert_eq!(unchanged.preserved_fresh_targets, 1);
    assert_eq!(
        liquidity.fresh_balance(&first.key, token_b),
        Some(U256::from(777_u64))
    );

    registry.unregister_pool(&second.key);
    graph.remove_pool_compacting(&second.key);
    let removed = liquidity.reconcile_pools(
        &registry,
        &graph,
        LiquidityIndexScope::ParallelEdgeOutputs,
        [first.key.clone(), second.key.clone()],
    );
    assert_eq!(removed.removed_targets, 4);
    assert!(liquidity.is_empty());

    register(&mut registry, second.clone());
    graph.apply_pool(&second, GraphBuildOptions::default());
    let readded = liquidity.reconcile_pools(
        &registry,
        &graph,
        LiquidityIndexScope::ParallelEdgeOutputs,
        [first.key.clone(), second.key.clone()],
    );
    assert_eq!(readded.added_targets, 4);
    assert_eq!(liquidity.len(), 4);
}

#[test]
fn incremental_graph_mutations_advance_versions_only_when_topology_changes() {
    let token_a = addr(0x01);
    let token_b = addr(0x02);
    let token_c = addr(0x03);
    let first = ready_v2(addr(0x10), token_a, token_b);
    let second = ready_v2(addr(0x11), token_b, token_c);
    let mut registry = AdapterRegistry::new();
    register(&mut registry, first.clone());

    let mut graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let initial = graph.version();
    assert_eq!(initial.revision(), 0);

    register(&mut registry, second.clone());
    assert!(matches!(
        graph.apply_pool(&second, GraphBuildOptions::default()),
        GraphPoolMutation::Added { directed_edges: 2 }
    ));
    assert_eq!(graph.version().lineage(), initial.lineage());
    assert_eq!(graph.version().revision(), 1);
    assert_graph_equivalent_to_full_build(&graph, &registry);

    assert_eq!(
        graph.apply_pool(&second, GraphBuildOptions::default()),
        GraphPoolMutation::Unchanged
    );
    assert_eq!(graph.version().lineage(), initial.lineage());
    assert_eq!(graph.version().revision(), 1);

    let second_rewired = ready_v2(addr(0x11), token_a, token_c);
    registry.unregister_pool(&second.key);
    register(&mut registry, second_rewired.clone());
    assert!(matches!(
        graph.apply_pool(&second_rewired, GraphBuildOptions::default()),
        GraphPoolMutation::Updated {
            removed_edges: 2,
            added_edges: 2
        }
    ));
    assert_eq!(graph.version().lineage(), initial.lineage());
    assert_eq!(graph.version().revision(), 2);
    assert_graph_equivalent_to_full_build(&graph, &registry);

    registry.unregister_pool(&first.key);
    assert!(matches!(
        graph.remove_pool_compacting(&first.key),
        GraphPoolMutation::Removed { directed_edges: 2 }
    ));
    assert_eq!(graph.version().lineage(), initial.lineage());
    assert_eq!(graph.version().revision(), 3);
    assert_graph_equivalent_to_full_build(&graph, &registry);
}

#[test]
fn incremental_graph_matches_full_rebuild_across_mixed_mutation_sequence() {
    let mut registry = AdapterRegistry::new();
    let mut graph = AmmGraph::new();

    for step in 0..192_usize {
        let pool_address = addr(0x40 + ((step * 17) % 32) as u8);
        let key = PoolKey::UniswapV2(pool_address);
        match step % 6 {
            0..=2 => {
                let token0 = addr(1 + ((step * 7) % 24) as u8);
                let token1 = addr(1 + (((step * 7) + 5) % 24) as u8);
                let registration = ready_v2(pool_address, token0, token1);
                registry.unregister_pool(&key);
                register(&mut registry, registration.clone());
                graph.apply_pool(&registration, GraphBuildOptions::default());
            }
            3 => {
                if let Some(registration) = registry.pool(&key).cloned() {
                    assert_eq!(
                        graph.apply_pool(&registration, GraphBuildOptions::default()),
                        GraphPoolMutation::Unchanged
                    );
                }
            }
            4 => {
                let pending =
                    ready_v2(pool_address, addr(0x21), addr(0x22)).with_status(PoolStatus::Pending);
                registry.unregister_pool(&key);
                register(&mut registry, pending.clone());
                graph.apply_pool(&pending, GraphBuildOptions::default());
            }
            5 => {
                registry.unregister_pool(&key);
                graph.remove_pool_compacting(&key);
            }
            _ => unreachable!(),
        }
        assert_graph_equivalent_to_full_build(&graph, &registry);
    }
}

fn assert_graph_equivalent_to_full_build(graph: &AmmGraph, registry: &AdapterRegistry) {
    let rebuilt = AmmGraph::from_registry(registry, GraphBuildOptions::default()).graph;
    let semantic_edges = |graph: &AmmGraph| {
        let mut edges = graph
            .graph()
            .edge_references()
            .map(|edge| {
                (
                    graph.node_token(edge.source()).expect("live source"),
                    graph.node_token(edge.target()).expect("live target"),
                    edge.weight().pool.clone(),
                )
            })
            .collect::<Vec<_>>();
        edges.sort_unstable();
        edges
    };
    let mut tokens = graph.graph().node_weights().copied().collect::<Vec<_>>();
    let mut rebuilt_tokens = rebuilt.graph().node_weights().copied().collect::<Vec<_>>();
    tokens.sort_unstable();
    rebuilt_tokens.sort_unstable();
    assert_eq!(tokens, rebuilt_tokens);
    assert_eq!(semantic_edges(graph), semantic_edges(&rebuilt));
}

#[test]
fn route_search_quotes_and_sorts_direct_and_multihop_routes() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ac.clone(), a, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(&RouteRequest::new(a, c, U256::from(100_u64)), &mut cache)
        .expect("routes quote");

    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].amount_out, U256::from(400_u64));
    assert_eq!(
        routes[0].path.hops,
        vec![Hop::new(p_ab, a, b), Hop::new(p_bc, b, c)]
    );
    assert_eq!(routes[1].amount_out, U256::from(300_u64));
    assert_eq!(routes[1].path.hops, vec![Hop::new(p_ac, a, c)]);
}

#[test]
fn route_search_treats_parallel_edges_as_distinct_candidates() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p1 = PoolKey::UniswapV2(addr(0x10));
    let p2 = PoolKey::UniswapV2(addr(0x11));

    let mut registry =
        registry_with_mock_adapter([rate(p1.clone(), a, b, 2, 1), rate(p2.clone(), a, b, 3, 1)]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(&RouteRequest::new(a, b, U256::from(100_u64)), &mut cache)
        .expect("routes quote");

    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].amount_out, U256::from(300_u64));
    assert_eq!(routes[1].amount_out, U256::from(200_u64));
}

#[test]
fn heuristic_search_keeps_best_parallel_edge_for_same_token_step() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab_weak = PoolKey::UniswapV2(addr(0x10));
    let p_ab_strong = PoolKey::UniswapV2(addr(0x11));
    let p_bc = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab_weak.clone(), a, b, 3, 2),
        rate(p_ab_strong.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    register(&mut registry, ready_v2(addr(0x12), b, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(
            &RouteRequest::new(a, c, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(2, 2)
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_beam_width(None)
                            .with_edge_shortlist(AdaptiveEdgeShortlistConfig::disabled())
                            .with_finalist_simulation(true, 4),
                    )),
            ),
            &mut cache,
        )
        .expect("heuristic route quotes");

    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].amount_out, U256::from(400_u64));
    assert_eq!(
        routes[0].path.hops,
        vec![
            Hop::new(p_ab_strong.clone(), a, b),
            Hop::new(p_bc.clone(), b, c)
        ]
    );

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ab_weak, a, b, U256::from(100_u64))).copied(),
        Some(1),
        "dominance still quotes the weak parallel edge once"
    );
    assert_eq!(
        counts
            .get(&(p_ab_strong, a, b, U256::from(100_u64)))
            .copied(),
        Some(1)
    );
    assert_eq!(
        counts
            .get(&(p_bc.clone(), b, c, U256::from(200_u64)))
            .copied(),
        Some(1)
    );
    assert_eq!(counts.get(&(p_bc, b, c, U256::from(150_u64))), None);
}

#[tokio::test]
async fn protocol_ranking_prefers_v3_over_v2_in_same_liquidity_bucket() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_v2 = PoolKey::UniswapV2(addr(0x10));
    let p_v3 = PoolKey::UniswapV3(addr(0x11));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_v2.clone(), a, b, 1, 1),
        rate(p_v3.clone(), a, b, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v3(addr(0x11), a, b, 500));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    for pool in [&p_v2, &p_v3] {
        liquidity
            .set_balance(pool, a, U256::from(1_000_u64))
            .unwrap();
        liquidity
            .set_balance(pool, b, U256::from(1_000_u64))
            .unwrap();
    }

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = setup_mock_cache().await;
    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .heuristic()
                    .with_liquidity_pruning(LiquidityPruningConfig::enabled()),
            ),
            &mut cache,
            StreamingSearchConfig::default().fast_lane_only(),
            |_| SearchControl::Continue,
        )
        .expect("fast lane search completes");

    assert_eq!(
        report.best.map(|quote| quote.path.hops[0].pool.clone()),
        Some(p_v3.clone())
    );
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_v3, a, b, U256::from(100_u64))).copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_v2, a, b, U256::from(100_u64))).copied(),
        None
    );
    assert!(report.liquidity_pruning.protocol_ranked_edges >= 2);
}

#[tokio::test]
async fn protocol_ranking_keeps_deeper_v2_ahead_of_thinner_v3() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_v2 = PoolKey::UniswapV2(addr(0x10));
    let p_v3 = PoolKey::UniswapV3(addr(0x11));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_v2.clone(), a, b, 1, 1),
        rate(p_v3.clone(), a, b, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v3(addr(0x11), a, b, 500));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    liquidity
        .set_balance(&p_v2, a, U256::from(1_024_u64))
        .unwrap();
    liquidity
        .set_balance(&p_v2, b, U256::from(1_024_u64))
        .unwrap();
    liquidity.set_balance(&p_v3, a, U256::from(16_u64)).unwrap();
    liquidity.set_balance(&p_v3, b, U256::from(16_u64)).unwrap();

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = setup_mock_cache().await;
    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .heuristic()
                    .with_liquidity_pruning(LiquidityPruningConfig::enabled()),
            ),
            &mut cache,
            StreamingSearchConfig::default().fast_lane_only(),
            |_| SearchControl::Continue,
        )
        .expect("fast lane search completes");

    assert_eq!(
        report.best.map(|quote| quote.path.hops[0].pool.clone()),
        Some(p_v2.clone())
    );
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_v2, a, b, U256::from(100_u64))).copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_v3, a, b, U256::from(100_u64))).copied(),
        None
    );
}

#[test]
fn liquidity_index_derives_targets_and_balancer_cash_slots() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let d = addr(0x04);
    let v2_pool = PoolKey::UniswapV2(addr(0x10));
    let v2_pool_2 = PoolKey::UniswapV2(addr(0x11));
    let solo_pool = PoolKey::UniswapV2(addr(0x12));
    let balancer_pool = PoolKey::BalancerV2(alloy_primitives::B256::repeat_byte(0x20));
    let balancer_pool_2 = PoolKey::BalancerV2(alloy_primitives::B256::repeat_byte(0x21));
    let vault = addr(0x30);
    let cash_slot = U256::from(9_u64);
    let cash_slot_2 = U256::from(10_u64);

    let mut registry = AdapterRegistry::new();
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    register(&mut registry, ready_v2(addr(0x12), addr(0x05), addr(0x06)));
    register(
        &mut registry,
        PoolRegistration::new(balancer_pool.clone())
            .with_metadata(ProtocolMetadata::BalancerV2(
                BalancerV2Metadata::default()
                    .with_vault(vault)
                    .with_pool_address(addr(0x31))
                    .with_tokens([c, d])
                    .with_token_cash([
                        BalancerTokenBalance::new(c, cash_slot, false),
                        BalancerTokenBalance::new(d, cash_slot, true),
                    ]),
            ))
            .with_status(PoolStatus::Ready),
    );
    register(
        &mut registry,
        PoolRegistration::new(balancer_pool_2.clone())
            .with_metadata(ProtocolMetadata::BalancerV2(
                BalancerV2Metadata::default()
                    .with_vault(vault)
                    .with_pool_address(addr(0x32))
                    .with_tokens([c, d])
                    .with_token_cash([
                        BalancerTokenBalance::new(c, cash_slot_2, false),
                        BalancerTokenBalance::new(d, cash_slot_2, true),
                    ]),
            ))
            .with_status(PoolStatus::Ready),
    );
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;

    let (mut index, report) = PoolLiquidityIndex::from_registry(&registry, &graph);
    assert_eq!(report.tracked_balances, 8);
    assert_eq!(report.unknown_balances, 0);
    assert_eq!(index.transfer_event_sources().len(), 2);
    assert!(
        index
            .set_balance(&solo_pool, addr(0x05), U256::ONE)
            .is_err(),
        "default scope should not track non-parallel pools"
    );
    assert_eq!(
        index.balance_state(&solo_pool, addr(0x05)),
        BalanceState::Unknown
    );

    let (mut broad_index, broad_report) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    assert_eq!(broad_report.tracked_balances, 10);
    broad_index
        .set_balance(&solo_pool, addr(0x05), U256::ONE)
        .expect("broad scope tracks all directed edge tokens");

    let low = U256::from(123_u64);
    let high = U256::from(456_u64);
    let packed = low | (high << 112);
    assert_eq!(
        index.apply_storage_updates(&[StateUpdate::slot(vault, cash_slot, packed)]),
        2
    );
    assert_eq!(index.fresh_balance(&balancer_pool, c), Some(low));
    assert_eq!(index.fresh_balance(&balancer_pool, d), Some(high));
    assert!(index.is_two_token_pool(&v2_pool));
    assert!(index.is_two_token_pool(&v2_pool_2));
    assert!(index.is_two_token_pool(&balancer_pool));
}

#[test]
fn liquidity_pruning_orders_parallel_edges_and_skips_balance_dominated_pools() {
    let a = addr(0x01);
    let b = addr(0x02);
    let pools = [
        PoolKey::UniswapV2(addr(0x10)),
        PoolKey::UniswapV2(addr(0x11)),
        PoolKey::UniswapV2(addr(0x12)),
        PoolKey::UniswapV2(addr(0x13)),
    ];

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(pools[0].clone(), a, b, 2, 1),
        rate(pools[1].clone(), a, b, 100, 1),
        rate(pools[2].clone(), a, b, 100, 1),
        rate(pools[3].clone(), a, b, 100, 1),
    ]);
    for pool in &pools {
        if let PoolKey::UniswapV2(address) = pool {
            register(&mut registry, ready_v2(*address, a, b));
        } else {
            panic!("test pool must be v2");
        }
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry(&registry, &graph);
    liquidity
        .set_balance(&pools[0], b, U256::from(1_000_u64))
        .unwrap();
    for pool in pools.iter().skip(1) {
        liquidity.set_balance(pool, b, U256::from(50_u64)).unwrap();
    }

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = NoopCache;
    let request = RouteRequest::new(a, b, U256::from(100_u64)).with_config(
        SearchConfig::default()
            .with_mode(SearchMode::Heuristic(
                HeuristicSearchConfig::default()
                    .with_beam_width(None)
                    .with_finalist_simulation(true, 4),
            ))
            .with_liquidity_pruning(LiquidityPruningConfig::enabled()),
    );

    let routes = searcher
        .find_routes(&request, &mut cache)
        .expect("route quotes");
    assert_eq!(routes[0].amount_out, U256::from(200_u64));
    assert_eq!(routes[0].path.hops, vec![Hop::new(pools[0].clone(), a, b)]);

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts
            .get(&(pools[0].clone(), a, b, U256::from(100_u64)))
            .copied(),
        Some(1)
    );
    for pool in pools.iter().skip(1) {
        assert_eq!(counts.get(&(pool.clone(), a, b, U256::from(100_u64))), None);
    }
}

#[test]
fn liquidity_pruning_is_disabled_by_default_and_runs_for_any_parallel_edges() {
    let a = addr(0x01);
    let b = addr(0x02);
    let pools = [
        PoolKey::UniswapV2(addr(0x10)),
        PoolKey::UniswapV2(addr(0x11)),
        PoolKey::UniswapV2(addr(0x12)),
    ];

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(pools[0].clone(), a, b, 2, 1),
        rate(pools[1].clone(), a, b, 1, 3),
        rate(pools[2].clone(), a, b, 1, 4),
    ]);
    for pool in &pools {
        let PoolKey::UniswapV2(address) = pool else {
            panic!("test pool must be v2");
        };
        register(&mut registry, ready_v2(*address, a, b));
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry(&registry, &graph);
    liquidity
        .set_balance(&pools[0], b, U256::from(1_000_u64))
        .unwrap();
    liquidity.set_balance(&pools[1], b, U256::from(50)).unwrap();
    liquidity.set_balance(&pools[2], b, U256::from(50)).unwrap();

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = NoopCache;
    let config = SearchConfig::default().with_mode(SearchMode::Heuristic(
        HeuristicSearchConfig::default()
            .with_beam_width(None)
            .with_finalist_simulation(true, 4),
    ));
    let request = RouteRequest::new(a, b, U256::from(100_u64)).with_config(config.clone());

    let routes = searcher
        .find_routes(&request, &mut cache)
        .expect("routes quote");
    assert_eq!(routes[0].amount_out, U256::from(200_u64));

    let counts = counts.lock().expect("counts lock");
    for pool in &pools {
        assert_eq!(
            counts
                .get(&(pool.clone(), a, b, U256::from(100_u64)))
                .copied(),
            Some(1),
            "default-disabled liquidity pruning should not change quote count"
        );
    }
    drop(counts);

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(pools[0].clone(), a, b, 2, 1),
        rate(pools[1].clone(), a, b, 1, 3),
        rate(pools[2].clone(), a, b, 1, 4),
    ]);
    for pool in &pools {
        let PoolKey::UniswapV2(address) = pool else {
            panic!("test pool must be v2");
        };
        register(&mut registry, ready_v2(*address, a, b));
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry(&registry, &graph);
    liquidity
        .set_balance(&pools[0], b, U256::from(1_000_u64))
        .unwrap();
    liquidity.set_balance(&pools[1], b, U256::from(50)).unwrap();
    liquidity.set_balance(&pools[2], b, U256::from(50)).unwrap();

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = NoopCache;
    let request = RouteRequest::new(a, b, U256::from(100_u64))
        .with_config(config.with_liquidity_pruning(LiquidityPruningConfig::enabled()));

    let routes = searcher
        .find_routes(&request, &mut cache)
        .expect("routes quote");
    assert_eq!(routes[0].amount_out, U256::from(200_u64));

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts
            .get(&(pools[0].clone(), a, b, U256::from(100_u64)))
            .copied(),
        Some(1)
    );
    for pool in pools.iter().skip(1) {
        assert_eq!(
            counts.get(&(pool.clone(), a, b, U256::from(100_u64))),
            None,
            "liquidity pruning should run for any parallel group, including three edges"
        );
    }
}

#[test]
fn liquidity_pruning_keeps_stale_balances_fail_open() {
    let a = addr(0x01);
    let b = addr(0x02);
    let pools = [
        PoolKey::UniswapV2(addr(0x10)),
        PoolKey::UniswapV2(addr(0x11)),
        PoolKey::UniswapV2(addr(0x12)),
        PoolKey::UniswapV2(addr(0x13)),
    ];

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(pools[0].clone(), a, b, 2, 1),
        rate(pools[1].clone(), a, b, 3, 1),
        rate(pools[2].clone(), a, b, 100, 1),
        rate(pools[3].clone(), a, b, 100, 1),
    ]);
    for pool in &pools {
        let PoolKey::UniswapV2(address) = pool else {
            panic!("test pool must be v2");
        };
        register(&mut registry, ready_v2(*address, a, b));
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry(&registry, &graph);
    liquidity
        .set_balance(&pools[0], b, U256::from(1_000_u64))
        .unwrap();
    liquidity
        .set_balance(&pools[1], b, U256::from(1_000_u64))
        .unwrap();
    liquidity.mark_stale(&pools[1], b).unwrap();
    liquidity
        .set_balance(&pools[2], b, U256::from(50_u64))
        .unwrap();
    liquidity
        .set_balance(&pools[3], b, U256::from(50_u64))
        .unwrap();

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = NoopCache;
    let request = RouteRequest::new(a, b, U256::from(100_u64)).with_config(
        SearchConfig::default()
            .with_mode(SearchMode::Heuristic(
                HeuristicSearchConfig::default()
                    .with_beam_width(None)
                    .with_finalist_simulation(true, 4),
            ))
            .with_liquidity_pruning(LiquidityPruningConfig::enabled()),
    );

    let routes = searcher
        .find_routes(&request, &mut cache)
        .expect("routes quote");
    assert_eq!(routes[0].path.hops, vec![Hop::new(pools[1].clone(), a, b)]);
    assert_eq!(routes[0].amount_out, U256::from(300_u64));

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts
            .get(&(pools[0].clone(), a, b, U256::from(100_u64)))
            .copied(),
        Some(1)
    );
    assert_eq!(
        counts
            .get(&(pools[1].clone(), a, b, U256::from(100_u64)))
            .copied(),
        Some(1),
        "stale balances must stay fail-open and still be simulated"
    );
    for pool in pools.iter().skip(2) {
        assert_eq!(counts.get(&(pool.clone(), a, b, U256::from(100_u64))), None);
    }
}

#[test]
fn liquidity_transfer_hooks_update_or_stale_tracked_balances() {
    let a = addr(0x01);
    let b = addr(0x02);
    let pool_address = addr(0x10);
    let pool = PoolKey::UniswapV2(pool_address);
    let mut registry = AdapterRegistry::new();
    register(&mut registry, ready_v2(pool_address, a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry(&registry, &graph);
    liquidity.set_erc20_balance_slot(b, pool_address, U256::from(7_u64));
    liquidity
        .set_balance(&pool, b, U256::from(100_u64))
        .expect("tracked balance");

    let mut cache = NoopCache;
    let report = liquidity.apply_transfer_log(
        &mut cache,
        &transfer_log(b, pool_address, addr(0x40), U256::from(20_u64)),
    );
    assert!(report.matched);
    assert_eq!(report.updated_balances, 1);
    assert_eq!(liquidity.fresh_balance(&pool, b), Some(U256::from(80_u64)));

    let removed_report =
        liquidity.mark_transfer_log_stale(&transfer_log(b, addr(0x40), pool_address, U256::ONE));
    assert!(removed_report.matched);
    assert_eq!(removed_report.stale_balances, 1);
    assert_eq!(liquidity.balance_state(&pool, b), BalanceState::Stale);
}

#[test]
fn heuristic_search_auto_connectors_prune_low_degree_intermediates() {
    let a = addr(0x01);
    let c = addr(0x03);
    let niche = addr(0x04);
    let hub = addr(0x05);
    let extra = addr(0x06);
    let p_a_niche = PoolKey::UniswapV2(addr(0x10));
    let p_niche_c = PoolKey::UniswapV2(addr(0x11));
    let p_a_hub = PoolKey::UniswapV2(addr(0x12));
    let p_hub_c = PoolKey::UniswapV2(addr(0x13));

    let mut registry = registry_with_mock_adapter([
        rate(p_a_niche.clone(), a, niche, 10, 1),
        rate(p_niche_c.clone(), niche, c, 10, 1),
        rate(p_a_hub.clone(), a, hub, 2, 1),
        rate(p_hub_c.clone(), hub, c, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, niche));
    register(&mut registry, ready_v2(addr(0x11), niche, c));
    register(&mut registry, ready_v2(addr(0x12), a, hub));
    register(&mut registry, ready_v2(addr(0x13), hub, c));
    register(&mut registry, ready_v2(addr(0x14), hub, extra));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut exhaustive_cache = NoopCache;
    let exhaustive = searcher
        .find_routes(
            &RouteRequest::new(a, c, U256::from(100_u64))
                .with_config(SearchConfig::default().with_hops(2, 2)),
            &mut exhaustive_cache,
        )
        .expect("exhaustive route quotes");
    assert_eq!(exhaustive[0].path.hops[0].pool, p_a_niche);

    let mut heuristic_cache = NoopCache;
    let heuristic = searcher
        .find_routes(
            &RouteRequest::new(a, c, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(2, 2)
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_auto_connectors(1, 6)
                            .with_beam_width(None)
                            .with_finalist_simulation(true, 4),
                    )),
            ),
            &mut heuristic_cache,
        )
        .expect("heuristic route quotes through the hub");

    assert_eq!(heuristic.len(), 1);
    assert_eq!(
        heuristic[0].path.hops,
        vec![Hop::new(p_a_hub, a, hub), Hop::new(p_hub_c, hub, c)]
    );
}

#[test]
fn heuristic_search_prunes_static_dead_end_prefixes_before_simulation() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_ac = PoolKey::UniswapV2(addr(0x11));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 10, 1),
        rate(p_ac.clone(), a, c, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(
            &RouteRequest::new(a, c, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(1, 2)
                    .with_connector_tokens([b])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default().with_finalist_simulation(true, 4),
                    )),
            ),
            &mut cache,
        )
        .expect("heuristic direct route quotes");

    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].path.hops, vec![Hop::new(p_ac.clone(), a, c)]);

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ac, a, c, U256::from(100_u64))).copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_ab, a, b, U256::from(100_u64))),
        None,
        "dead-end connector prefix should not be simulated"
    );
}

#[test]
fn heuristic_reachability_backtracks_from_a_dead_end_before_a_shared_branch() {
    let a = addr(0x01);
    let b = addr(0x02);
    let d = addr(0x03);
    let e = addr(0x04);
    let target = addr(0x05);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_be = PoolKey::UniswapV2(addr(0x11));
    let p_et = PoolKey::UniswapV2(addr(0x12));
    let p_de = PoolKey::UniswapV2(addr(0x13));
    // This pool is registered last, so its B -> D edge is visited before the
    // B -> E sibling by StableGraph's adjacency iterator. The D -> E branch
    // consumes the remaining hop budget without reaching `target`; exact
    // reachability must then undo E before trying the shared B -> E branch.
    let p_bd = PoolKey::UniswapV2(addr(0x14));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 1, 1),
        rate(p_be.clone(), b, e, 1, 1),
        rate(p_et.clone(), e, target, 1, 1),
        rate(p_de.clone(), d, e, 1, 1),
        rate(p_bd.clone(), b, d, 1, 1),
    ]);
    for (pool, token0, token1) in [
        (addr(0x10), a, b),
        (addr(0x11), b, e),
        (addr(0x12), e, target),
        (addr(0x13), d, e),
        (addr(0x14), b, d),
    ] {
        register(&mut registry, ready_v2(pool, token0, token1));
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let base_config = SearchConfig::default()
        .with_hops(3, 3)
        .with_connector_tokens([b, d, e]);

    let mut exhaustive_cache = NoopCache;
    let exhaustive = searcher
        .find_routes(
            &RouteRequest::new(a, target, U256::from(100_u64))
                .with_config(base_config.clone().exhaustive()),
            &mut exhaustive_cache,
        )
        .expect("exhaustive shared-branch route");

    let mut heuristic_cache = NoopCache;
    let heuristic = searcher
        .find_routes(
            &RouteRequest::new(a, target, U256::from(100_u64)).with_config(
                base_config.with_mode(SearchMode::Heuristic(
                    HeuristicSearchConfig::default()
                        .with_beam_width(None)
                        .with_finalist_simulation(true, 8),
                )),
            ),
            &mut heuristic_cache,
        )
        .expect("heuristic search backtracks into the shared branch");

    assert_eq!(heuristic, exhaustive);
    assert_eq!(
        heuristic[0].path.hops,
        vec![
            Hop::new(p_ab, a, b),
            Hop::new(p_be, b, e),
            Hop::new(p_et, e, target),
        ]
    );
}

#[test]
fn heuristic_prefix_dominance_prunes_worse_same_token_prefix() {
    let a = addr(0x01);
    let b = addr(0x02);
    let d = addr(0x03);
    let e = addr(0x04);
    let p_ae = PoolKey::UniswapV2(addr(0x10));
    let p_eb = PoolKey::UniswapV2(addr(0x11));
    let p_ad = PoolKey::UniswapV2(addr(0x12));
    let p_de = PoolKey::UniswapV2(addr(0x13));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ae.clone(), a, e, 2, 1),
        rate(p_eb.clone(), e, b, 1, 1),
        rate(p_ad.clone(), a, d, 3, 2),
        rate(p_de.clone(), d, e, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, e));
    register(&mut registry, ready_v2(addr(0x11), e, b));
    register(&mut registry, ready_v2(addr(0x12), a, d));
    register(&mut registry, ready_v2(addr(0x13), d, e));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(2, 3)
                    .with_connector_tokens([d, e])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_beam_width(None)
                            .with_finalist_simulation(true, 4),
                    )),
            ),
            &mut cache,
        )
        .expect("heuristic routes quote");

    assert_eq!(
        routes[0].path.hops,
        vec![Hop::new(p_ae, a, e), Hop::new(p_eb.clone(), e, b)]
    );
    assert_eq!(routes[0].amount_out, U256::from(200_u64));

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts
            .get(&(p_eb.clone(), e, b, U256::from(200_u64)))
            .copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_de, d, e, U256::from(150_u64))).copied(),
        Some(1),
        "the candidate prefix must be quoted before it can be dominated"
    );
    assert_eq!(
        counts.get(&(p_eb, e, b, U256::from(150_u64))),
        None,
        "downstream hop from dominated E prefix should not be simulated"
    );
}

#[test]
fn heuristic_prefix_dominance_retains_better_same_token_prefix() {
    let a = addr(0x01);
    let b = addr(0x02);
    let d = addr(0x03);
    let e = addr(0x04);
    let p_ae = PoolKey::UniswapV2(addr(0x10));
    let p_eb = PoolKey::UniswapV2(addr(0x11));
    let p_ad = PoolKey::UniswapV2(addr(0x12));
    let p_de = PoolKey::UniswapV2(addr(0x13));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ae.clone(), a, e, 1, 1),
        rate(p_eb.clone(), e, b, 1, 1),
        rate(p_ad.clone(), a, d, 2, 1),
        rate(p_de.clone(), d, e, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, e));
    register(&mut registry, ready_v2(addr(0x11), e, b));
    register(&mut registry, ready_v2(addr(0x12), a, d));
    register(&mut registry, ready_v2(addr(0x13), d, e));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(2, 3)
                    .with_connector_tokens([d, e])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_beam_width(None)
                            .with_finalist_simulation(true, 4),
                    )),
            ),
            &mut cache,
        )
        .expect("heuristic routes quote");

    assert_eq!(
        routes[0].path.hops,
        vec![
            Hop::new(p_ad, a, d),
            Hop::new(p_de, d, e),
            Hop::new(p_eb.clone(), e, b)
        ]
    );
    assert_eq!(routes[0].amount_out, U256::from(200_u64));

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts
            .get(&(p_eb.clone(), e, b, U256::from(200_u64)))
            .copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_eb, e, b, U256::from(100_u64))).copied(),
        Some(1),
        "higher-output via-D prefix is retained, but it cannot dominate the looser direct E prefix"
    );
}

#[test]
fn heuristic_prefix_dominance_keeps_incomparable_constraints() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let d = addr(0x04);
    let e = addr(0x05);
    let p_ac = PoolKey::UniswapV2(addr(0x10));
    let p_ce = PoolKey::UniswapV2(addr(0x11));
    let p_ad = PoolKey::UniswapV2(addr(0x12));
    let p_de = PoolKey::UniswapV2(addr(0x13));
    let p_eb = PoolKey::UniswapV2(addr(0x14));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ac.clone(), a, c, 3, 1),
        rate(p_ce.clone(), c, e, 1, 1),
        rate(p_ad.clone(), a, d, 2, 1),
        rate(p_de.clone(), d, e, 1, 1),
        rate(p_eb.clone(), e, b, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, c));
    register(&mut registry, ready_v2(addr(0x11), c, e));
    register(&mut registry, ready_v2(addr(0x12), a, d));
    register(&mut registry, ready_v2(addr(0x13), d, e));
    register(&mut registry, ready_v2(addr(0x14), e, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(3, 3)
                    .with_connector_tokens([c, d, e])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_beam_width(None)
                            .with_finalist_simulation(true, 4),
                    )),
            ),
            &mut cache,
        )
        .expect("heuristic routes quote");

    assert_eq!(routes.len(), 2);
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts
            .get(&(p_eb.clone(), e, b, U256::from(300_u64)))
            .copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_eb, e, b, U256::from(200_u64))).copied(),
        Some(1),
        "different visited-token sets are not dominance subsets"
    );
}

#[test]
fn route_search_rejects_token_revisits() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_ba = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 1, 1),
        rate(p_ba.clone(), b, a, 1, 1),
        rate(p_ac, a, c, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let err = searcher
        .find_routes(
            &RouteRequest::new(a, c, U256::from(100_u64))
                .with_config(SearchConfig::default().with_hops(3, 3)),
            &mut cache,
        )
        .expect_err("route cannot revisit the source token as an intermediate");

    assert!(matches!(err, SearchError::NoPath { .. }));
}

#[test]
fn connector_allowlist_filters_intermediate_tokens_only() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let d = addr(0x04);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ad = PoolKey::UniswapV2(addr(0x12));
    let p_dc = PoolKey::UniswapV2(addr(0x13));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 1, 1),
        rate(p_bc.clone(), b, c, 1, 1),
        rate(p_ad.clone(), a, d, 10, 1),
        rate(p_dc.clone(), d, c, 10, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), a, d));
    register(&mut registry, ready_v2(addr(0x13), d, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let request = RouteRequest::new(a, c, U256::from(100_u64)).with_config(
        SearchConfig::default()
            .with_hops(2, 2)
            .with_connector_tokens([b]),
    );
    let routes = searcher
        .find_routes(&request, &mut cache)
        .expect("allowed connector route quotes");

    assert_eq!(routes.len(), 1);
    assert_eq!(
        routes[0].path.hops,
        vec![Hop::new(p_ab, a, b), Hop::new(p_bc, b, c)]
    );
}

#[test]
fn cycle_search_rejects_reusing_the_same_pool() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_ab = PoolKey::UniswapV2(addr(0x10));

    let mut registry =
        registry_with_mock_adapter([rate(p_ab.clone(), a, b, 2, 1), rate(p_ab, b, a, 2, 1)]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let err = searcher
        .find_cycles(
            &CycleRequest::new(a, U256::from(10_u64))
                .with_config(SearchConfig::default().with_hops(2, 2)),
            &mut cache,
        )
        .expect_err("cycle cannot reuse the same pool for the closing hop");

    assert!(matches!(err, SearchError::NoPath { .. }));
}

#[test]
fn cycle_search_allows_base_only_as_final_token_and_no_pool_reuse() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ca = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ca.clone(), c, a, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), c, a));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let cycles = searcher
        .find_cycles(
            &CycleRequest::new(a, U256::from(10_u64))
                .with_config(SearchConfig::default().with_hops(2, 3)),
            &mut cache,
        )
        .expect("cycle quotes");

    let expected_cycle = RoutePath::from_hops(vec![
        Hop::new(p_ab, a, b),
        Hop::new(p_bc, b, c),
        Hop::new(p_ca, c, a),
    ]);
    assert!(
        cycles
            .iter()
            .any(|cycle| cycle.route.path == expected_cycle)
    );
    assert!(cycles.iter().all(|cycle| {
        let mut pools = std::collections::HashSet::new();
        cycle
            .route
            .path
            .hops
            .iter()
            .all(|hop| pools.insert(hop.pool.clone()))
    }));
    assert!(cycles[0].is_profitable());
}

#[test]
fn cycle_search_rejects_base_token_as_intermediate() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab_0 = PoolKey::UniswapV2(addr(0x10));
    let p_ab_1 = PoolKey::UniswapV2(addr(0x11));
    let p_ac_0 = PoolKey::UniswapV2(addr(0x12));
    let p_ac_1 = PoolKey::UniswapV2(addr(0x13));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab_0.clone(), a, b, 2, 1),
        rate(p_ab_1.clone(), b, a, 2, 1),
        rate(p_ac_0.clone(), a, c, 2, 1),
        rate(p_ac_1.clone(), c, a, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    register(&mut registry, ready_v2(addr(0x13), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let err = searcher
        .find_cycles(
            &CycleRequest::new(a, U256::from(10_u64))
                .with_config(SearchConfig::default().with_hops(4, 4)),
            &mut cache,
        )
        .expect_err("base-token revisit cannot be used as an intermediate hop");

    assert!(matches!(err, SearchError::NoPath { .. }));
}

#[test]
fn missing_quotes_return_no_viable_route_with_failures() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_ab = PoolKey::UniswapV2(addr(0x10));

    let mut registry = registry_with_mock_adapter([]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let err = searcher
        .find_routes(&RouteRequest::new(a, b, U256::from(100_u64)), &mut cache)
        .expect_err("candidate exists but quote fails");

    match err {
        SearchError::NoViableRoute {
            candidates,
            failures,
        } => {
            assert_eq!(candidates, 1);
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].path.hops, vec![Hop::new(p_ab, a, b)]);
        }
        other => panic!("expected NoViableRoute, got {other:?}"),
    }
}

#[test]
fn quote_path_dispatches_each_hop_in_order() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 5, 1),
        rate(p_bc.clone(), b, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let quote = searcher
        .quote_path(
            &RoutePath::from_hops(vec![Hop::new(p_ab, a, b), Hop::new(p_bc, b, c)]),
            U256::from(7_u64),
            &mut cache,
            &SimConfig::default(),
        )
        .expect("path quotes");

    assert_eq!(quote.hops[0].amount_in, U256::from(7_u64));
    assert_eq!(quote.hops[0].amount_out, U256::from(35_u64));
    assert_eq!(quote.hops[1].amount_in, U256::from(35_u64));
    assert_eq!(quote.amount_out, U256::from(105_u64));
}

#[tokio::test]
async fn parallel_route_search_matches_serial_results() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ac.clone(), a, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let request = RouteRequest::new(a, c, U256::from(100_u64));

    let mut serial_cache = NoopCache;
    let serial = searcher
        .find_routes(&request, &mut serial_cache)
        .expect("serial routes quote");
    let mut overlay_cache = setup_mock_cache().await;
    let parallel = searcher
        .find_routes_parallel(
            &request,
            &mut overlay_cache,
            ParallelSearchConfig::default().with_workers(2),
        )
        .expect("parallel routes quote");

    assert_eq!(parallel, serial);
}

#[tokio::test]
async fn parallel_cycle_search_matches_serial_results() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ca = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ca.clone(), c, a, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), c, a));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let request = CycleRequest::new(a, U256::from(10_u64))
        .with_config(SearchConfig::default().with_hops(2, 3));

    let mut serial_cache = NoopCache;
    let serial = searcher
        .find_cycles(&request, &mut serial_cache)
        .expect("serial cycles quote");
    let mut overlay_cache = setup_mock_cache().await;
    let parallel = searcher
        .find_cycles_parallel(
            &request,
            &mut overlay_cache,
            ParallelSearchConfig::default().with_workers(3),
        )
        .expect("parallel cycles quote");

    assert_eq!(parallel, serial);
}

#[tokio::test]
async fn parallel_route_batch_matches_serial_results_in_request_order() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ac.clone(), a, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let requests = vec![
        RouteRequest::new(a, c, U256::from(100_u64)),
        RouteRequest::new(a, b, U256::from(100_u64)),
    ];

    let mut serial_cache = NoopCache;
    let serial = requests
        .iter()
        .map(|request| searcher.find_routes(request, &mut serial_cache))
        .collect::<Result<Vec<_>, _>>()
        .expect("serial routes quote");
    let mut overlay_cache = setup_mock_cache().await;
    let parallel = searcher
        .find_routes_batch_parallel(
            &requests,
            &mut overlay_cache,
            ParallelSearchConfig::default().with_workers(2),
        )
        .expect("batch workers complete")
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parallel routes quote");

    assert_eq!(parallel, serial);
}

#[tokio::test]
async fn parallel_cycle_batch_matches_serial_results_in_request_order() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ca = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ca.clone(), c, a, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), c, a));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let requests = vec![
        CycleRequest::new(a, U256::from(10_u64))
            .with_config(SearchConfig::default().with_hops(2, 3)),
        CycleRequest::new(a, U256::from(20_u64))
            .with_config(SearchConfig::default().with_hops(2, 3)),
    ];

    let mut serial_cache = NoopCache;
    let serial = requests
        .iter()
        .map(|request| searcher.find_cycles(request, &mut serial_cache))
        .collect::<Result<Vec<_>, _>>()
        .expect("serial cycles quote");
    let mut overlay_cache = setup_mock_cache().await;
    let parallel = searcher
        .find_cycles_batch_parallel(
            &requests,
            &mut overlay_cache,
            ParallelSearchConfig::default().with_workers(2),
        )
        .expect("batch workers complete")
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parallel cycles quote");

    assert_eq!(parallel, serial);
}

#[tokio::test]
async fn parallel_search_rejects_zero_workers() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_ab = PoolKey::UniswapV2(addr(0x10));

    let mut registry = registry_with_mock_adapter([rate(p_ab, a, b, 2, 1)]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;

    let err = searcher
        .find_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)),
            &mut cache,
            ParallelSearchConfig::default().with_workers(0),
        )
        .expect_err("zero workers are invalid");

    assert!(matches!(err, SearchError::InvalidConfig { .. }));
}

#[tokio::test]
async fn streaming_search_emits_heuristic_best_then_exhaustive_improvement() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);

    let registry = streaming_improvement_registry(a, b, c);
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut best_updates = Vec::new();

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default()
                .with_top_k(3)
                .with_parallel(ParallelSearchConfig::default().with_workers(2)),
            |event| {
                if let RouteSearchEvent::BestUpdated { phase, quote, .. } = event {
                    best_updates.push((phase, quote.amount_out));
                }
                SearchControl::Continue
            },
        )
        .expect("streaming search completes");

    assert_eq!(
        best_updates,
        vec![
            (RouteSearchPhase::Heuristic, U256::from(300_u64)),
            (RouteSearchPhase::Exhaustive, U256::from(400_u64)),
        ]
    );
    assert_eq!(report.finality, SearchFinality::Exhaustive);
    assert_eq!(
        report.heuristic_best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(300_u64))
    );
    assert_eq!(
        report.best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(400_u64))
    );
    assert_eq!(report.heuristic_was_final_best, Some(false));
    assert_eq!(report.improvements_after_heuristic, 1);
    assert_eq!(report.top_routes[0].amount_out, U256::from(400_u64));
    assert!(
        report
            .top_routes
            .iter()
            .any(|quote| quote.amount_out == U256::from(300_u64))
    );
    assert_eq!(report.duplicate_paths_skipped, 1);
    assert!(report.initial_results_released);
}

#[tokio::test]
async fn streaming_progress_reports_heuristic_and_exhaustive_completion() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);

    let registry = streaming_improvement_registry(a, b, c);
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut progress_events = Vec::new();

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default()
                .with_parallel(ParallelSearchConfig::default().with_workers(1)),
            |event| {
                if let RouteSearchEvent::Progress { progress } = event {
                    progress_events.push(progress);
                }
                SearchControl::Continue
            },
        )
        .expect("streaming search completes");

    assert!(progress_events.iter().any(|progress| {
        progress.phase == Some(RouteSearchPhase::Heuristic)
            && progress.total_candidates.is_none()
            && progress.confidence_bps == 7_000
            && progress.best_amount_out == Some(U256::from(300_u64))
    }));
    assert!(progress_events.iter().any(|progress| {
        progress.phase == Some(RouteSearchPhase::Heuristic)
            && progress.total_candidates.is_none()
            && progress.confidence_bps == 9_000
    }));
    assert!(progress_events.iter().any(|progress| {
        progress.phase == Some(RouteSearchPhase::Exhaustive)
            && progress.total_candidates == Some(2)
            && progress.exhaustive_fraction_bps == Some(5_000)
    }));
    assert_eq!(report.finality, SearchFinality::Exhaustive);
    assert_eq!(report.progress.total_candidates, Some(2));
    assert_eq!(report.progress.exhaustive_fraction_bps, Some(10_000));
    assert_eq!(report.progress.confidence_bps, 10_000);
    assert_eq!(report.progress.candidates_evaluated, 2);
    assert_eq!(report.progress.viable_routes_observed, 2);
}

#[tokio::test]
async fn streaming_stop_policy_stops_once_confidence_threshold_is_reached() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);

    let registry = streaming_improvement_registry(a, b, c);
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut completed_finality = None;

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default().stop_at_confidence_bps(9_000),
            |event| {
                if let RouteSearchEvent::Completed { report } = event {
                    completed_finality = Some(report.finality);
                }
                SearchControl::Continue
            },
        )
        .expect("streaming search stops by policy");

    assert_eq!(report.finality, SearchFinality::StopPolicySatisfied);
    assert_eq!(
        completed_finality,
        Some(SearchFinality::StopPolicySatisfied)
    );
    assert_eq!(
        report.best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(300_u64))
    );
    assert_eq!(report.progress.confidence_bps, 9_000);
    assert_eq!(report.progress.total_candidates, None);
    assert_eq!(report.heuristic_was_final_best, None);
}

#[tokio::test]
async fn streaming_initial_result_gate_releases_once_then_continues_search() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);

    let registry = streaming_improvement_registry(a, b, c);
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut quote_events = Vec::new();

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default().emit_initial_results_at_confidence_bps(9_000),
            |event| {
                match event {
                    RouteSearchEvent::InitialResultsReady { progress, best, .. } => quote_events
                        .push(format!(
                            "initial:{}:{}",
                            best.amount_out, progress.confidence_bps
                        )),
                    RouteSearchEvent::BestUpdated { quote, .. } => {
                        quote_events.push(format!("best:{}", quote.amount_out));
                    }
                    RouteSearchEvent::RouteFound { quote, .. } => {
                        quote_events.push(format!("found:{}", quote.amount_out));
                    }
                    _ => {}
                }
                SearchControl::Continue
            },
        )
        .expect("streaming search completes");

    assert_eq!(
        quote_events.first().map(String::as_str),
        Some("initial:300:9000")
    );
    assert!(
        quote_events
            .iter()
            .any(|event| event == "best:400" || event == "found:400")
    );
    assert_eq!(report.finality, SearchFinality::Exhaustive);
    assert!(report.initial_results_released);
    assert_eq!(
        report.best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(400_u64))
    );
}

#[tokio::test]
async fn streaming_combined_threshold_helpers_use_all_semantics() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);

    let registry = streaming_improvement_registry(a, b, c);
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default()
                .stop_at_confidence_and_exhaustive_fraction_bps(9_000, 6_000),
            |_| SearchControl::Continue,
        )
        .expect("streaming search stops by combined policy");

    assert_eq!(report.finality, SearchFinality::StopPolicySatisfied);
    assert_eq!(
        report.best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(400_u64))
    );
    assert_eq!(report.progress.exhaustive_fraction_bps, Some(10_000));
    assert!(report.progress.confidence_bps >= 9_000);
}

#[tokio::test]
async fn streaming_threshold_any_mode_stops_when_either_threshold_is_met() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);

    let registry = streaming_improvement_registry(a, b, c);
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default().with_stop_policy(
                StreamingThresholdPolicy::any()
                    .with_min_confidence_bps(9_000)
                    .with_min_exhaustive_fraction_bps(10_000),
            ),
            |_| SearchControl::Continue,
        )
        .expect("streaming search stops by any-threshold policy");

    assert_eq!(report.finality, SearchFinality::StopPolicySatisfied);
    assert_eq!(
        report.best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(300_u64))
    );
    assert_eq!(report.progress.exhaustive_fraction_bps, None);
}

#[tokio::test]
async fn heuristic_target_first_streams_direct_route_before_multihop() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ac = PoolKey::UniswapV2(addr(0x10));
    let p_cb = PoolKey::UniswapV2(addr(0x11));
    let p_ab = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ac.clone(), a, c, 2, 1),
        rate(p_cb.clone(), c, b, 2, 1),
        rate(p_ab.clone(), a, b, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, c));
    register(&mut registry, ready_v2(addr(0x11), c, b));
    register(&mut registry, ready_v2(addr(0x12), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut best_updates = Vec::new();

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(1, 2)
                    .with_connector_tokens([c])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_beam_width(None)
                            .with_finalist_simulation(true, 4),
                    )),
            ),
            &mut cache,
            StreamingSearchConfig::default().heuristic_only(),
            |event| {
                if let RouteSearchEvent::BestUpdated { quote, .. } = event {
                    best_updates.push(quote.path.clone());
                }
                SearchControl::Continue
            },
        )
        .expect("streaming search completes");

    assert_eq!(report.finality, SearchFinality::HeuristicOnly);
    assert_eq!(
        best_updates,
        vec![
            RoutePath::from_hops(vec![Hop::new(p_ab, a, b)]),
            RoutePath::from_hops(vec![Hop::new(p_ac, a, c), Hop::new(p_cb, c, b)]),
        ]
    );
    assert_eq!(
        report.liquidity_pruning.target_first_groups, 2,
        "diagnostics should count each target-closing group ordered first"
    );
}

#[tokio::test]
async fn liquidity_branch_ranking_prioritizes_deeper_current_token_liquidity() {
    let a = addr(0x01);
    let b = addr(0x02);
    let deep = addr(0x03);
    let thin = addr(0x04);
    let unknown = addr(0x05);
    let p_a_deep = PoolKey::UniswapV2(addr(0x10));
    let p_deep_b = PoolKey::UniswapV2(addr(0x11));
    let p_a_thin = PoolKey::UniswapV2(addr(0x12));
    let p_thin_b = PoolKey::UniswapV2(addr(0x13));
    let p_a_unknown = PoolKey::UniswapV2(addr(0x14));
    let p_unknown_b = PoolKey::UniswapV2(addr(0x15));

    let mut registry = registry_with_mock_adapter([
        rate(p_a_deep.clone(), a, deep, 1, 1),
        rate(p_deep_b.clone(), deep, b, 1, 1),
        rate(p_a_thin.clone(), a, thin, 3, 1),
        rate(p_thin_b.clone(), thin, b, 1, 1),
        rate(p_a_unknown.clone(), a, unknown, 4, 1),
        rate(p_unknown_b.clone(), unknown, b, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, deep));
    register(&mut registry, ready_v2(addr(0x11), deep, b));
    register(&mut registry, ready_v2(addr(0x12), a, thin));
    register(&mut registry, ready_v2(addr(0x13), thin, b));
    register(&mut registry, ready_v2(addr(0x14), a, unknown));
    register(&mut registry, ready_v2(addr(0x15), unknown, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    liquidity
        .set_balance(&p_a_deep, a, U256::from(1_000_u64))
        .unwrap();
    liquidity
        .set_balance(&p_a_thin, a, U256::from(10_u64))
        .unwrap();

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = setup_mock_cache().await;
    let mut found_outputs = Vec::new();

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(2, 2)
                    .with_connector_tokens([deep, thin, unknown])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_beam_width(None)
                            .with_finalist_simulation(false, 8),
                    ))
                    .with_liquidity_pruning(LiquidityPruningConfig::enabled()),
            ),
            &mut cache,
            StreamingSearchConfig::default()
                .heuristic_only()
                .with_emit_all_viable(true),
            |event| {
                if let RouteSearchEvent::RouteFound { quote, .. } = event {
                    found_outputs.push(quote.amount_out);
                }
                SearchControl::Continue
            },
        )
        .expect("streaming search completes");

    assert_eq!(
        found_outputs,
        vec![
            U256::from(100_u64),
            U256::from(300_u64),
            U256::from(400_u64)
        ],
        "known deep liquidity should be evaluated first and unknown liquidity remains fail-open"
    );
    assert_eq!(report.liquidity_pruning.liquidity_ranked_branch_groups, 2);
    assert_eq!(report.liquidity_pruning.liquidity_unknown_branch_groups, 1);
}

#[tokio::test]
async fn fast_lane_prioritizes_central_connector_routes() {
    let a = addr(0x01);
    let b = addr(0x02);
    let hub = addr(0x03);
    let thin = addr(0x04);
    let extra = addr(0x05);
    let p_a_hub = PoolKey::UniswapV2(addr(0x10));
    let p_hub_b = PoolKey::UniswapV2(addr(0x11));
    let p_a_thin = PoolKey::UniswapV2(addr(0x12));
    let p_thin_b = PoolKey::UniswapV2(addr(0x13));
    let p_hub_extra = PoolKey::UniswapV2(addr(0x14));

    let mut registry = registry_with_mock_adapter([
        rate(p_a_hub.clone(), a, hub, 1, 1),
        rate(p_hub_b.clone(), hub, b, 1, 1),
        rate(p_a_thin.clone(), a, thin, 2, 1),
        rate(p_thin_b.clone(), thin, b, 2, 1),
        rate(p_hub_extra.clone(), hub, extra, 1, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, hub));
    register(&mut registry, ready_v2(addr(0x11), hub, b));
    register(&mut registry, ready_v2(addr(0x12), a, thin));
    register(&mut registry, ready_v2(addr(0x13), thin, b));
    register(&mut registry, ready_v2(addr(0x14), hub, extra));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut paths = Vec::new();

    searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(2, 2)
                    .with_connector_tokens([hub, thin])
                    .heuristic(),
            ),
            &mut cache,
            StreamingSearchConfig::default()
                .fast_lane_only()
                .with_emit_all_viable(true),
            |event| {
                if let RouteSearchEvent::RouteFound { quote, .. } = event {
                    paths.push(quote.path.clone());
                }
                SearchControl::Continue
            },
        )
        .expect("fast lane search completes");

    assert_eq!(
        paths.first(),
        Some(&RoutePath::from_hops(vec![
            Hop::new(p_a_hub, a, hub),
            Hop::new(p_hub_b, hub, b),
        ]))
    );
    assert_eq!(
        paths.get(1),
        Some(&RoutePath::from_hops(vec![
            Hop::new(p_a_thin, a, thin),
            Hop::new(p_thin_b, thin, b),
        ])),
        "lower-degree connector is still evaluated after the central connector"
    );
}

#[tokio::test]
async fn adaptive_shortlist_initial_pass_quotes_one_parallel_edge() {
    let a = addr(0x01);
    let b = addr(0x02);
    let pools = [
        PoolKey::UniswapV2(addr(0x10)),
        PoolKey::UniswapV2(addr(0x11)),
        PoolKey::UniswapV2(addr(0x12)),
    ];

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(pools[0].clone(), a, b, 1, 1),
        rate(pools[1].clone(), a, b, 2, 1),
        rate(pools[2].clone(), a, b, 3, 1),
    ]);
    for pool in &pools {
        let PoolKey::UniswapV2(address) = pool else {
            panic!("test pool must be v2");
        };
        register(&mut registry, ready_v2(*address, a, b));
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default().with_mode(SearchMode::Heuristic(
                    HeuristicSearchConfig::default()
                        .with_fast_lane(FastLaneConfig::disabled())
                        .with_edge_shortlist(AdaptiveEdgeShortlistConfig {
                            refine_parallel_edges: false,
                            ..AdaptiveEdgeShortlistConfig::enabled()
                        })
                        .with_finalist_simulation(false, 8),
                )),
            ),
            &mut cache,
            StreamingSearchConfig::default().heuristic_only(),
            |_| SearchControl::Continue,
        )
        .expect("heuristic search completes");

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts
            .get(&(pools[0].clone(), a, b, U256::from(100_u64)))
            .copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(pools[1].clone(), a, b, U256::from(100_u64))),
        None
    );
    assert_eq!(
        counts.get(&(pools[2].clone(), a, b, U256::from(100_u64))),
        None
    );
    assert_eq!(report.liquidity_pruning.shortlist_initial_edges, 1);
    assert_eq!(report.liquidity_pruning.shortlist_deferred_edges, 2);
}

#[tokio::test]
async fn adaptive_shortlist_refinement_quotes_deferred_edges() {
    let a = addr(0x01);
    let b = addr(0x02);
    let pools = [
        PoolKey::UniswapV2(addr(0x10)),
        PoolKey::UniswapV2(addr(0x11)),
        PoolKey::UniswapV2(addr(0x12)),
    ];

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(pools[0].clone(), a, b, 1, 1),
        rate(pools[1].clone(), a, b, 2, 1),
        rate(pools[2].clone(), a, b, 3, 1),
    ]);
    for pool in &pools {
        let PoolKey::UniswapV2(address) = pool else {
            panic!("test pool must be v2");
        };
        register(&mut registry, ready_v2(*address, a, b));
    }
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default().with_mode(SearchMode::Heuristic(
                    HeuristicSearchConfig::default()
                        .with_fast_lane(FastLaneConfig::disabled())
                        .with_edge_shortlist(AdaptiveEdgeShortlistConfig::enabled())
                        .with_finalist_simulation(false, 8),
                )),
            ),
            &mut cache,
            StreamingSearchConfig::default()
                .heuristic_only()
                .with_emit_all_viable(true),
            |_| SearchControl::Continue,
        )
        .expect("heuristic search completes");

    let counts = counts.lock().expect("counts lock");
    for pool in &pools {
        assert_eq!(
            counts
                .get(&(pool.clone(), a, b, U256::from(100_u64)))
                .copied(),
            Some(1)
        );
    }
    assert_eq!(report.liquidity_pruning.shortlist_initial_edges, 1);
    assert_eq!(report.liquidity_pruning.shortlist_refinement_edges, 3);
    assert_eq!(report.liquidity_pruning.shortlist_deferred_edges, 2);
}

#[tokio::test]
async fn upper_bound_pruning_skips_prefix_when_target_balance_cap_cannot_beat_incumbent() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_direct = PoolKey::UniswapV2(addr(0x10));
    let p_ac = PoolKey::UniswapV2(addr(0x11));
    let p_cb = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_direct.clone(), a, b, 5, 1),
        rate(p_ac.clone(), a, c, 1, 1),
        rate(p_cb.clone(), c, b, 10, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, c));
    register(&mut registry, ready_v2(addr(0x12), c, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    liquidity
        .set_balance(&p_cb, b, U256::from(400_u64))
        .unwrap();
    assert_eq!(
        liquidity.balance_state(&p_cb, b),
        BalanceState::Fresh(U256::from(400_u64))
    );

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = setup_mock_cache().await;
    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(1, 2)
                    .with_connector_tokens([c])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default().with_fast_lane(FastLaneConfig {
                            max_connectors: 0,
                            ..FastLaneConfig::enabled()
                        }),
                    )),
            ),
            &mut cache,
            StreamingSearchConfig::default().heuristic_only(),
            |_| SearchControl::Continue,
        )
        .expect("heuristic search completes");

    assert_eq!(
        report.best.map(|quote| quote.amount_out),
        Some(U256::from(500_u64))
    );
    assert_eq!(report.liquidity_pruning.upper_bound_pruned_prefixes, 1);
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ac, a, c, U256::from(100_u64))).copied(),
        Some(1)
    );
    assert_eq!(counts.get(&(p_cb, c, b, U256::from(100_u64))), None);
}

#[tokio::test]
async fn upper_bound_pruning_fails_open_when_target_balance_is_unknown() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_direct = PoolKey::UniswapV2(addr(0x10));
    let p_ac = PoolKey::UniswapV2(addr(0x11));
    let p_cb = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_direct.clone(), a, b, 5, 1),
        rate(p_ac.clone(), a, c, 1, 1),
        rate(p_cb.clone(), c, b, 10, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, c));
    register(&mut registry, ready_v2(addr(0x12), c, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (liquidity, _) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );

    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = setup_mock_cache().await;
    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, b, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(1, 2)
                    .with_connector_tokens([c])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default().with_fast_lane(FastLaneConfig {
                            max_connectors: 0,
                            ..FastLaneConfig::enabled()
                        }),
                    )),
            ),
            &mut cache,
            StreamingSearchConfig::default().heuristic_only(),
            |_| SearchControl::Continue,
        )
        .expect("heuristic search completes");

    assert_eq!(
        report.best.map(|quote| quote.amount_out),
        Some(U256::from(1_000_u64))
    );
    assert!(report.liquidity_pruning.upper_bound_unknown_prefixes >= 1);
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_cb, c, b, U256::from(100_u64))).copied(),
        Some(1)
    );
}

#[test]
fn upper_bound_pruning_is_disabled_for_cycles_by_default() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_ba = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));
    let p_ca = PoolKey::UniswapV2(addr(0x13));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 5, 1),
        rate(p_ba.clone(), b, a, 1, 1),
        rate(p_ac.clone(), a, c, 1, 1),
        rate(p_ca.clone(), c, a, 10, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, a));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    register(&mut registry, ready_v2(addr(0x13), c, a));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let (mut liquidity, _) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    liquidity.set_balance(&p_ca, a, U256::from(1_u64)).unwrap();
    let searcher = AmmSearcher::new(&registry, &graph).with_liquidity_index(&liquidity);
    let mut cache = NoopCache;

    let cycles = searcher
        .find_cycles(
            &CycleRequest::new(a, U256::from(100_u64)).with_config(
                SearchConfig::default()
                    .with_hops(2, 2)
                    .with_connector_tokens([b, c])
                    .with_mode(SearchMode::Heuristic(
                        HeuristicSearchConfig::default()
                            .with_fast_lane(FastLaneConfig::disabled())
                            .with_finalist_simulation(false, 8),
                    )),
            ),
            &mut cache,
        )
        .expect("cycle search completes");

    assert!(!cycles.is_empty());
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ca, c, a, U256::from(100_u64))).copied(),
        Some(1),
        "cycle search must not use route upper-bound pruning by default"
    );
}

#[tokio::test]
async fn streaming_search_can_stop_after_heuristic_phase() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ac.clone(), a, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default().heuristic_only(),
            |_| SearchControl::Continue,
        )
        .expect("heuristic-only stream completes");

    assert_eq!(report.finality, SearchFinality::HeuristicOnly);
    assert_eq!(
        report.best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(300_u64))
    );
    assert_eq!(report.heuristic_was_final_best, None);

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ac, a, c, U256::from(100_u64))).copied(),
        Some(1)
    );
    assert_eq!(counts.get(&(p_ab, a, b, U256::from(100_u64))), None);
    assert_eq!(counts.get(&(p_bc, b, c, U256::from(200_u64))), None);
}

#[tokio::test]
async fn streaming_search_stops_when_callback_requests_stop() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
        rate(p_ac.clone(), a, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;

    let report = searcher
        .stream_routes_parallel(
            &RouteRequest::new(a, c, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default().stop_at_confidence_bps(9_000),
            |event| match event {
                RouteSearchEvent::BestUpdated { .. } => SearchControl::Stop,
                _ => SearchControl::Continue,
            },
        )
        .expect("streaming search stops cleanly");

    assert_eq!(report.finality, SearchFinality::Stopped);
    assert_eq!(
        report.best.as_ref().map(|quote| quote.amount_out),
        Some(U256::from(300_u64))
    );

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ac, a, c, U256::from(100_u64))).copied(),
        Some(1)
    );
    assert_eq!(counts.get(&(p_ab, a, b, U256::from(100_u64))), None);
    assert_eq!(counts.get(&(p_bc, b, c, U256::from(200_u64))), None);
}

#[tokio::test]
async fn streaming_exhaustive_remainder_skips_heuristic_duplicate_paths() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;

    let request = RouteRequest::new(a, c, U256::from(100_u64)).with_config(
        SearchConfig::default()
            .with_hops(2, 2)
            .with_connector_tokens([b]),
    );
    let report = searcher
        .stream_routes_parallel(
            &request,
            &mut cache,
            StreamingSearchConfig::default()
                .with_parallel(ParallelSearchConfig::default().with_workers(2)),
            |_| SearchControl::Continue,
        )
        .expect("streaming search completes");

    assert_eq!(report.finality, SearchFinality::Exhaustive);
    assert_eq!(report.heuristic_was_final_best, Some(true));
    assert_eq!(report.duplicate_paths_skipped, 1);
    assert_eq!(report.quote_cache.executed, 2);

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ab, a, b, U256::from(100_u64))).copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_bc, b, c, U256::from(200_u64))).copied(),
        Some(1)
    );
}

#[test]
fn affected_pools_extracts_explicit_keys_and_routed_logs() {
    let a = addr(0x01);
    let b = addr(0x02);
    let pool = PoolKey::UniswapV2(addr(0x10));

    let mut registry = registry_with_mock_adapter([rate(pool.clone(), a, b, 2, 1)]);
    register(
        &mut registry,
        ready_v2(addr(0x10), a, b).with_event_source(EventSource::direct(
            addr(0x10),
            vec![keccak256(b"Sync(uint112,uint112)")],
        )),
    );

    let explicit = AffectedPools::from_pool_keys([pool.clone()]);
    assert!(explicit.pools().contains(&pool));
    assert_eq!(explicit.unknown_logs(), 0);

    let log = sync_log(addr(0x10));
    let routed = AffectedPools::from_logs(&registry, [&log]);
    assert_eq!(routed.pools().len(), 1);
    assert!(routed.pools().contains(&pool));

    let unknown = Log::new(
        addr(0xff),
        vec![keccak256(b"Sync(uint112,uint112)")],
        Bytes::new(),
    )
    .expect("valid log");
    let routed = AffectedPools::from_logs(&registry, [&unknown]);
    assert_eq!(routed.pools().len(), 0);
    assert_eq!(routed.unknown_logs(), 1);
}

#[tokio::test]
async fn incremental_refresh_unrelated_pool_runs_zero_simulations() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let d = addr(0x04);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_cd = PoolKey::UniswapV2(addr(0x11));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_cd.clone(), c, d, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), c, d));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut session = searcher
        .start_route_session(
            &RouteRequest::new(a, b, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default(),
            |_| SearchControl::Continue,
        )
        .expect("session starts");

    let before = counts.lock().expect("counts lock").clone();
    let report = session.refresh_affected(
        &searcher,
        &mut cache,
        AffectedPools::from_pool_keys([p_cd]),
        |_| SearchControl::Continue,
    );

    assert_eq!(report.status, IncrementalRouteUpdateStatus::Unchanged);
    assert_eq!(report.routes_requoted, 0);
    assert_eq!(report.probe_routes_quoted, 0);
    assert_eq!(*counts.lock().expect("counts lock"), before);
}

#[tokio::test]
async fn incremental_refresh_falls_back_when_current_best_worsens() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_best = PoolKey::UniswapV2(addr(0x10));
    let p_fallback = PoolKey::UniswapV2(addr(0x11));

    let (mut registry, rates, _) = registry_with_mutable_counting_mock_adapter([
        rate(p_best.clone(), a, b, 3, 1),
        rate(p_fallback.clone(), a, b, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut session = searcher
        .start_route_session(
            &RouteRequest::new(a, b, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default(),
            |_| SearchControl::Continue,
        )
        .expect("session starts");
    assert_eq!(
        session.best().map(|quote| quote.path.hops[0].pool.clone()),
        Some(p_best.clone())
    );

    set_rate(&rates, p_best.clone(), a, b, 1, 1);
    let mut best_changed = false;
    let report = session.refresh_affected(
        &searcher,
        &mut cache,
        AffectedPools::from_pool_keys([p_best]),
        |event| {
            if matches!(event, RouteUpdateEvent::BestChanged { .. }) {
                best_changed = true;
            }
            SearchControl::Continue
        },
    );

    assert_eq!(report.status, IncrementalRouteUpdateStatus::Updated);
    assert!(best_changed);
    assert_eq!(
        report.best.map(|quote| quote.path.hops[0].pool.clone()),
        Some(p_fallback)
    );
}

#[tokio::test]
async fn incremental_refresh_promotes_improved_non_best_route() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_best = PoolKey::UniswapV2(addr(0x10));
    let p_other = PoolKey::UniswapV2(addr(0x11));

    let (mut registry, rates, _) = registry_with_mutable_counting_mock_adapter([
        rate(p_best.clone(), a, b, 3, 1),
        rate(p_other.clone(), a, b, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut session = searcher
        .start_route_session(
            &RouteRequest::new(a, b, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default(),
            |_| SearchControl::Continue,
        )
        .expect("session starts");

    set_rate(&rates, p_other.clone(), a, b, 4, 1);
    let report = session.refresh_affected(
        &searcher,
        &mut cache,
        AffectedPools::from_pool_keys([p_other.clone()]),
        |_| SearchControl::Continue,
    );

    assert_eq!(report.status, IncrementalRouteUpdateStatus::Updated);
    assert_eq!(
        report.best.map(|quote| quote.path.hops[0].pool.clone()),
        Some(p_other)
    );
}

#[tokio::test]
async fn incremental_refresh_probes_heuristic_parallel_replacement() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_materialized = PoolKey::UniswapV2(addr(0x10));
    let p_probe = PoolKey::UniswapV2(addr(0x11));

    let (mut registry, rates, _) = registry_with_mutable_counting_mock_adapter([
        rate(p_materialized.clone(), a, b, 3, 1),
        rate(p_probe.clone(), a, b, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let request = RouteRequest::new(a, b, U256::from(100_u64)).with_config(
        SearchConfig::default().with_mode(SearchMode::Heuristic(
            HeuristicSearchConfig::default()
                .with_beam_width(None)
                .with_parallel_edge_limit(1)
                .with_edge_shortlist(AdaptiveEdgeShortlistConfig {
                    refine_parallel_edges: false,
                    ..AdaptiveEdgeShortlistConfig::enabled()
                })
                .with_finalist_simulation(false, 4),
        )),
    );
    let mut session = searcher
        .start_route_session(
            &request,
            &mut cache,
            StreamingSearchConfig::default().heuristic_only(),
            |_| SearchControl::Continue,
        )
        .expect("session starts");
    assert_eq!(
        session.best().map(|quote| quote.path.hops[0].pool.clone()),
        Some(p_materialized)
    );

    set_rate(&rates, p_probe.clone(), a, b, 5, 1);
    let mut probe_seen = false;
    let report = session.refresh_affected(
        &searcher,
        &mut cache,
        AffectedPools::from_pool_keys([p_probe.clone()]),
        |event| {
            if let RouteUpdateEvent::ProbeRouteFound { quote } = event {
                probe_seen = true;
                assert_eq!(quote.path.hops[0].pool, p_probe);
            }
            SearchControl::Continue
        },
    );

    assert!(probe_seen);
    assert_eq!(report.probe_routes_quoted, 1);
    assert_eq!(
        report.best.map(|quote| quote.path.hops[0].pool.clone()),
        Some(p_probe)
    );
}

#[tokio::test]
async fn incremental_refresh_invalidates_only_affected_pool_quotes() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));

    let (mut registry, rates, counts) = registry_with_mutable_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc.clone(), b, c, 2, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let request = RouteRequest::new(a, c, U256::from(100_u64))
        .with_config(SearchConfig::default().with_hops(2, 2));
    let mut session = searcher
        .start_route_session(
            &request,
            &mut cache,
            StreamingSearchConfig::default(),
            |_| SearchControl::Continue,
        )
        .expect("session starts");

    set_rate(&rates, p_bc.clone(), b, c, 3, 1);
    let report = session.refresh_affected(
        &searcher,
        &mut cache,
        AffectedPools::from_pool_keys([p_bc.clone()]),
        |_| SearchControl::Continue,
    );

    assert_eq!(report.routes_requoted, 1);
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ab, a, b, U256::from(100_u64))).copied(),
        Some(1),
        "unaffected first hop should be reused from the session quote cache"
    );
    assert_eq!(
        counts.get(&(p_bc, b, c, U256::from(200_u64))).copied(),
        Some(2),
        "affected second hop should be re-executed"
    );
}

#[tokio::test]
async fn incremental_refresh_reports_conservative_fallbacks() {
    let a = addr(0x01);
    let b = addr(0x02);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_extra = PoolKey::UniswapV2(addr(0x11));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_extra.clone(), a, b, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = setup_mock_cache().await;
    let mut session = searcher
        .start_route_session(
            &RouteRequest::new(a, b, U256::from(100_u64)),
            &mut cache,
            StreamingSearchConfig::default(),
            |_| SearchControl::Continue,
        )
        .expect("session starts");

    let unknown = session.refresh_affected(
        &searcher,
        &mut cache,
        AffectedPools::from_pool_keys([p_extra.clone()]),
        |_| SearchControl::Continue,
    );
    assert_eq!(
        unknown.status,
        IncrementalRouteUpdateStatus::RecomputeRequired
    );
    assert_eq!(
        unknown.recompute_reason,
        Some(RecomputeReason::UnknownAffectedPool(p_extra.clone()))
    );

    register(&mut registry, ready_v2(addr(0x11), a, b));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher_with_topology_change = AmmSearcher::new(&registry, &graph);
    let topology = session.refresh_affected(
        &searcher_with_topology_change,
        &mut cache,
        AffectedPools::default(),
        |_| SearchControl::Continue,
    );
    assert_eq!(
        topology.recompute_reason,
        Some(RecomputeReason::TopologyChanged)
    );
}

#[test]
fn route_quote_memo_reuses_shared_prefix_hop() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc_1 = PoolKey::UniswapV2(addr(0x11));
    let p_bc_2 = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc_1.clone(), b, c, 2, 1),
        rate(p_bc_2.clone(), b, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), b, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let routes = searcher
        .find_routes(
            &RouteRequest::new(a, c, U256::from(100_u64))
                .with_config(SearchConfig::default().with_hops(2, 2)),
            &mut cache,
        )
        .expect("routes quote");

    assert_eq!(routes.len(), 2);
    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ab, a, b, U256::from(100_u64))).copied(),
        Some(1),
        "shared first hop should be simulated once"
    );
    assert_eq!(
        counts.get(&(p_bc_1, b, c, U256::from(200_u64))).copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_bc_2, b, c, U256::from(200_u64))).copied(),
        Some(1)
    );
}

#[test]
fn route_quote_dag_reuses_shared_failed_prefix_hop() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc_1 = PoolKey::UniswapV2(addr(0x11));
    let p_bc_2 = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_bc_1.clone(), b, c, 2, 1),
        rate(p_bc_2.clone(), b, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), b, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let mut cache = NoopCache;

    let err = searcher
        .find_routes(
            &RouteRequest::new(a, c, U256::from(100_u64))
                .with_config(SearchConfig::default().with_hops(2, 2)),
            &mut cache,
        )
        .expect_err("shared first hop fails for both candidates");

    match err {
        SearchError::NoViableRoute {
            candidates,
            failures,
        } => {
            assert_eq!(candidates, 2);
            assert_eq!(failures.len(), 2);
        }
        other => panic!("expected NoViableRoute, got {other:?}"),
    }

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ab, a, b, U256::from(100_u64))).copied(),
        Some(1),
        "shared failing first hop should be simulated once"
    );
    assert_eq!(counts.get(&(p_bc_1, b, c, U256::from(100_u64))), None);
    assert_eq!(counts.get(&(p_bc_2, b, c, U256::from(100_u64))), None);
}

#[tokio::test]
async fn parallel_route_batch_reuses_quote_cache_across_workers() {
    let a = addr(0x01);
    let b = addr(0x02);
    let c = addr(0x03);
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc_1 = PoolKey::UniswapV2(addr(0x11));
    let p_bc_2 = PoolKey::UniswapV2(addr(0x12));

    let (mut registry, counts) = registry_with_counting_mock_adapter([
        rate(p_ab.clone(), a, b, 2, 1),
        rate(p_bc_1.clone(), b, c, 2, 1),
        rate(p_bc_2.clone(), b, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), b, c));
    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let request = RouteRequest::new(a, c, U256::from(100_u64))
        .with_config(SearchConfig::default().with_hops(2, 2));
    let requests = vec![request.clone(), request];
    let mut overlay_cache = setup_mock_cache().await;

    let report = searcher
        .find_routes_batch_parallel_with_stats(
            &requests,
            &mut overlay_cache,
            ParallelSearchConfig::default().with_workers(2),
        )
        .expect("batch workers complete");
    let routes = report
        .results
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parallel routes quote");

    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0], routes[1]);
    assert_eq!(report.quote_cache.executed, 3);
    assert_eq!(report.quote_cache.misses, 3);
    assert!(report.quote_cache.hits >= 3);

    let counts = counts.lock().expect("counts lock");
    assert_eq!(
        counts.get(&(p_ab, a, b, U256::from(100_u64))).copied(),
        Some(1),
        "shared first hop should be simulated once across workers"
    );
    assert_eq!(
        counts.get(&(p_bc_1, b, c, U256::from(200_u64))).copied(),
        Some(1)
    );
    assert_eq!(
        counts.get(&(p_bc_2, b, c, U256::from(200_u64))).copied(),
        Some(1)
    );
}

fn registry_with_mock_adapter(rates: impl IntoIterator<Item = Rate>) -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry
        .register_adapter(Arc::new(MockAdapter {
            rates: rates
                .into_iter()
                .map(|rate| {
                    (
                        (rate.pool, rate.token_in, rate.token_out),
                        (rate.num, rate.den),
                    )
                })
                .collect(),
            counts: None,
        }))
        .expect("adapter registers");
    registry
}

fn streaming_improvement_registry(a: Address, b: Address, c: Address) -> AdapterRegistry {
    let p_ab = PoolKey::UniswapV2(addr(0x10));
    let p_bc = PoolKey::UniswapV2(addr(0x11));
    let p_ac = PoolKey::UniswapV2(addr(0x12));

    let mut registry = registry_with_mock_adapter([
        rate(p_ab, a, b, 2, 1),
        rate(p_bc, b, c, 2, 1),
        rate(p_ac, a, c, 3, 1),
    ]);
    register(&mut registry, ready_v2(addr(0x10), a, b));
    register(&mut registry, ready_v2(addr(0x11), b, c));
    register(&mut registry, ready_v2(addr(0x12), a, c));
    registry
}

fn registry_with_counting_mock_adapter(
    rates: impl IntoIterator<Item = Rate>,
) -> (AdapterRegistry, QuoteCounts) {
    let counts = Arc::new(Mutex::new(HashMap::new()));
    let mut registry = AdapterRegistry::new();
    registry
        .register_adapter(Arc::new(MockAdapter {
            rates: rates
                .into_iter()
                .map(|rate| {
                    (
                        (rate.pool, rate.token_in, rate.token_out),
                        (rate.num, rate.den),
                    )
                })
                .collect(),
            counts: Some(Arc::clone(&counts)),
        }))
        .expect("adapter registers");
    (registry, counts)
}

fn registry_with_mutable_counting_mock_adapter(
    rates: impl IntoIterator<Item = Rate>,
) -> (AdapterRegistry, MutableRates, QuoteCounts) {
    let counts = Arc::new(Mutex::new(HashMap::new()));
    let rates = Arc::new(Mutex::new(
        rates
            .into_iter()
            .map(|rate| {
                (
                    (rate.pool, rate.token_in, rate.token_out),
                    (rate.num, rate.den),
                )
            })
            .collect(),
    ));
    let mut registry = AdapterRegistry::new();
    registry
        .register_adapter(Arc::new(MutableMockAdapter {
            rates: Arc::clone(&rates),
            counts: Some(Arc::clone(&counts)),
        }))
        .expect("adapter registers");
    (registry, rates, counts)
}

fn set_rate(
    rates: &MutableRates,
    pool: PoolKey,
    token_in: Address,
    token_out: Address,
    num: u64,
    den: u64,
) {
    rates
        .lock()
        .expect("rates lock")
        .insert((pool, token_in, token_out), (num, den));
}

fn rate(pool: PoolKey, token_in: Address, token_out: Address, num: u64, den: u64) -> Rate {
    Rate {
        pool,
        token_in,
        token_out,
        num,
        den,
    }
}

fn sync_log(pool: Address) -> Log {
    Log::new(
        pool,
        vec![keccak256(b"Sync(uint112,uint112)")],
        Bytes::new(),
    )
    .expect("valid sync log")
}

fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
    Log::new(
        token,
        vec![
            keccak256(b"Transfer(address,address,uint256)"),
            from.into_word(),
            to.into_word(),
        ],
        value.to_be_bytes_vec().into(),
    )
    .expect("valid transfer log")
}

async fn setup_mock_cache() -> EvmCache {
    let client = RpcClient::mocked(Asserter::new());
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

struct Rate {
    pool: PoolKey,
    token_in: Address,
    token_out: Address,
    num: u64,
    den: u64,
}

type QuoteCounts = Arc<Mutex<HashMap<(PoolKey, Address, Address, U256), usize>>>;
type MutableRates = Arc<Mutex<HashMap<(PoolKey, Address, Address), (u64, u64)>>>;

struct MockAdapter {
    rates: HashMap<(PoolKey, Address, Address), (u64, u64)>,
    counts: Option<QuoteCounts>,
}

impl AmmAdapter for MockAdapter {
    fn protocol(&self) -> ProtocolId {
        ProtocolId::UniswapV2
    }

    fn protocols(&self) -> Vec<ProtocolId> {
        vec![
            ProtocolId::UniswapV2,
            ProtocolId::UniswapV3,
            ProtocolId::PancakeV3,
            ProtocolId::Slipstream,
            ProtocolId::SolidlyV2,
            ProtocolId::BalancerV2,
            ProtocolId::Curve,
        ]
    }

    fn simulate_swap(
        &self,
        pool: &PoolRegistration,
        _cache: &mut dyn AdapterCache,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        _config: &SimConfig,
    ) -> Result<SwapQuote, SimError> {
        if let Some(counts) = &self.counts {
            *counts
                .lock()
                .expect("counts lock")
                .entry((pool.key.clone(), token_in, token_out, amount_in))
                .or_default() += 1;
        }

        let (num, den) = self
            .rates
            .get(&(pool.key.clone(), token_in, token_out))
            .copied()
            .ok_or_else(|| SimError::Custom("missing mock rate".to_string()))?;
        Ok(SwapQuote::new(
            amount_in * U256::from(num) / U256::from(den),
        ))
    }
}

struct MutableMockAdapter {
    rates: MutableRates,
    counts: Option<QuoteCounts>,
}

impl AmmAdapter for MutableMockAdapter {
    fn protocol(&self) -> ProtocolId {
        ProtocolId::UniswapV2
    }

    fn protocols(&self) -> Vec<ProtocolId> {
        vec![
            ProtocolId::UniswapV2,
            ProtocolId::UniswapV3,
            ProtocolId::PancakeV3,
            ProtocolId::Slipstream,
            ProtocolId::SolidlyV2,
            ProtocolId::BalancerV2,
            ProtocolId::Curve,
        ]
    }

    fn simulate_swap(
        &self,
        pool: &PoolRegistration,
        _cache: &mut dyn AdapterCache,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        _config: &SimConfig,
    ) -> Result<SwapQuote, SimError> {
        if let Some(counts) = &self.counts {
            *counts
                .lock()
                .expect("counts lock")
                .entry((pool.key.clone(), token_in, token_out, amount_in))
                .or_default() += 1;
        }

        let (num, den) = self
            .rates
            .lock()
            .expect("rates lock")
            .get(&(pool.key.clone(), token_in, token_out))
            .copied()
            .ok_or_else(|| SimError::Custom("missing mock rate".to_string()))?;
        Ok(SwapQuote::new(
            amount_in * U256::from(num) / U256::from(den),
        ))
    }
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
            reason: "noop cache".to_string(),
        })
    }
}
