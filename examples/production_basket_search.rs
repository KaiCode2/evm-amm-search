//! Larger live basket benchmark for discovery, cold-start, graph build, and
//! route search.
//!
//! The token basket below is embedded from `@uniswap/default-token-list@22.4.0`
//! mainnet entries so this example has no npm runtime dependency. It uses
//! `evm-amm-state` factory discovery for canonical Ethereum Uniswap V2 + V3,
//! warms the discovered pools, builds an `AmmGraph`, then runs many bounded
//! route searches through live simulations at one pinned block.
//!
//! ```text
//! E2E_RPC_URL=<mainnet-url> cargo run --release --example production_basket_search
//! ```
//!
//! Useful knobs:
//!
//! ```text
//! PROD_BASKET_TOKEN_LIMIT=60
//! PROD_BASKET_MAX_POOLS=0          # 0 means no cap
//! PROD_BASKET_SEARCH_LIMIT=600
//! PROD_BASKET_SEARCH_WORKERS=0    # 0 means available parallelism
//! PROD_BASKET_MAX_HOPS=2
//! PROD_BASKET_MAX_CANDIDATES=64
//! PROD_BASKET_AMOUNT_UNITS=1
//! PROD_BASKET_BLOCK_LAG=8
//! PROD_BASKET_PRIME_CACHE=0       # 1 runs a direct-cache priming search before measured passes
//! PROD_BASKET_WARM_SEARCH_RUNS=1
//! PROD_BASKET_SEARCH_MODE=exhaustive # exhaustive or heuristic
//! PROD_BASKET_HEURISTIC_PRESET=balanced # balanced or latency_first
//! PROD_BASKET_HEURISTIC_BEAM_WIDTH=64 # 0 disables beam truncation
//! PROD_BASKET_HEURISTIC_PARALLEL_EDGE_LIMIT=1
//! PROD_BASKET_HEURISTIC_TARGET_FIRST=1
//! PROD_BASKET_HEURISTIC_PREFIX_DOMINANCE=1
//! PROD_BASKET_HEURISTIC_FAST_LANE=1
//! PROD_BASKET_FAST_LANE_CONNECTORS=8
//! PROD_BASKET_FAST_LANE_DIRECT_EDGES=1
//! PROD_BASKET_FAST_LANE_CONNECTOR_EDGES=1
//! PROD_BASKET_EDGE_SHORTLIST=1
//! PROD_BASKET_EDGE_SHORTLIST_INITIAL=1
//! PROD_BASKET_EDGE_SHORTLIST_REFINEMENT=3
//! PROD_BASKET_EDGE_SHORTLIST_REFINE=1
//! PROD_BASKET_PROTOCOL_ORDERING=1
//! PROD_BASKET_UPPER_BOUND_PRUNING=1
//! PROD_BASKET_LIQUIDITY_PRIMING=0
//! PROD_BASKET_LIQUIDITY_COMPARE=0
//! PROD_BASKET_LIQUIDITY_COMPARE_AFTER=0
//! PROD_BASKET_INCREMENTAL_COMPARE=0
//! PROD_BASKET_ORDERING_COMPARE=0
//! ```

use std::collections::{HashSet, VecDeque};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_eips::BlockId;
use alloy_primitives::{Address, U256, address};
use alloy_provider::Provider;
use anyhow::{Context, Result, ensure};
use evm_amm_search::{
    AdaptiveEdgeShortlistConfig, AffectedPools, AmmGraph, AmmSearcher, FastLaneConfig,
    GraphBuildOptions, HeuristicSearchConfig, IncrementalRouteUpdateStatus, LiquidityIndexScope,
    LiquidityPruneStats, LiquidityPruningConfig, ParallelSearchConfig, PoolLiquidityIndex,
    QuoteCacheStats, RouteRequest, RouteSearchEvent, SearchConfig, SearchControl, SearchError,
    SearchMode, StreamingSearchConfig, UpperBoundPruningConfig,
};
use evm_amm_state::adapters::{
    AdapterRegistry, ColdStartOutcome, ColdStartPolicy, ConcentratedLiquidityAdapter,
    FactoryConfig, PoolDiscovery, PoolKey, PoolQuery, PoolRegistration, PoolStatus,
    ProtocolMetadata, SimConfig, UniswapV2Adapter, UniswapV2FactoryConfig, UniswapV3FactoryConfig,
};
use evm_fork_cache::cache::EvmCache;

mod support;

const UNISWAP_DEFAULT_TOKEN_LIST_VERSION: &str = "22.4.0";
const DEFAULT_TOKEN_LIMIT: usize = 48;
const DEFAULT_MAX_POOLS: usize = 160;
const DEFAULT_SEARCH_LIMIT: usize = 120;
const DEFAULT_SEARCH_WORKERS: usize = 0;
const DEFAULT_MAX_HOPS: usize = 2;
const DEFAULT_MAX_CANDIDATES: usize = 16;
const DEFAULT_AMOUNT_UNITS: u64 = 1;
const DEFAULT_BLOCK_LAG: u64 = 8;
const DEFAULT_PRIME_CACHE: bool = false;
const DEFAULT_WARM_SEARCH_RUNS: usize = 1;
const DEFAULT_SEARCH_MODE: BenchSearchMode = BenchSearchMode::Exhaustive;
const DEFAULT_LIQUIDITY_PRIMING: bool = false;
const DEFAULT_LIQUIDITY_COMPARE: bool = false;
const DEFAULT_INCREMENTAL_COMPARE: bool = false;
const DEFAULT_ORDERING_COMPARE: bool = false;

const UNISWAP_V2_FACTORY: Address = address!("5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
const UNISWAP_V3_FACTORY: Address = address!("1F98431c8aD98523631AE4a59f267346ea31F984");
const V2_ROUTER_02: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const V3_QUOTER_V2: Address = address!("61fFE014bA17989E743c5F6cB21bF9697530B21e");

const CONNECTOR_SYMBOLS: &[&str] = &["WETH", "USDC", "USDT", "DAI", "WBTC"];

#[derive(Clone, Copy, Debug)]
struct TokenInfo {
    symbol: &'static str,
    address: Address,
    decimals: u8,
}

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    token_limit: usize,
    max_pools: usize,
    search_limit: usize,
    search_workers: usize,
    max_hops: usize,
    max_candidates: usize,
    amount_units: u64,
    block_lag: u64,
    prime_cache: bool,
    warm_search_runs: usize,
    search_mode: BenchSearchMode,
    heuristic: HeuristicSearchConfig,
    liquidity_priming: bool,
    liquidity_compare: bool,
    liquidity_compare_after: bool,
    incremental_compare: bool,
    ordering_compare: bool,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            token_limit: env_usize("PROD_BASKET_TOKEN_LIMIT", DEFAULT_TOKEN_LIMIT),
            max_pools: env_usize("PROD_BASKET_MAX_POOLS", DEFAULT_MAX_POOLS),
            search_limit: env_usize("PROD_BASKET_SEARCH_LIMIT", DEFAULT_SEARCH_LIMIT),
            search_workers: env_usize("PROD_BASKET_SEARCH_WORKERS", DEFAULT_SEARCH_WORKERS),
            max_hops: env_usize("PROD_BASKET_MAX_HOPS", DEFAULT_MAX_HOPS),
            max_candidates: env_usize("PROD_BASKET_MAX_CANDIDATES", DEFAULT_MAX_CANDIDATES),
            amount_units: env_u64("PROD_BASKET_AMOUNT_UNITS", DEFAULT_AMOUNT_UNITS),
            block_lag: env_u64("PROD_BASKET_BLOCK_LAG", DEFAULT_BLOCK_LAG),
            prime_cache: env_bool("PROD_BASKET_PRIME_CACHE", DEFAULT_PRIME_CACHE),
            warm_search_runs: env_usize("PROD_BASKET_WARM_SEARCH_RUNS", DEFAULT_WARM_SEARCH_RUNS),
            search_mode: env_search_mode("PROD_BASKET_SEARCH_MODE", DEFAULT_SEARCH_MODE),
            heuristic: heuristic_config_from_env(),
            liquidity_priming: env_bool("PROD_BASKET_LIQUIDITY_PRIMING", DEFAULT_LIQUIDITY_PRIMING),
            liquidity_compare: env_bool("PROD_BASKET_LIQUIDITY_COMPARE", DEFAULT_LIQUIDITY_COMPARE),
            liquidity_compare_after: env_bool("PROD_BASKET_LIQUIDITY_COMPARE_AFTER", false),
            incremental_compare: env_bool(
                "PROD_BASKET_INCREMENTAL_COMPARE",
                DEFAULT_INCREMENTAL_COMPARE,
            ),
            ordering_compare: env_bool("PROD_BASKET_ORDERING_COMPARE", DEFAULT_ORDERING_COMPARE),
        }
    }

    fn parallel_search_config(&self) -> ParallelSearchConfig {
        let workers = if self.search_workers == 0 {
            std::thread::available_parallelism().map_or(1, usize::from)
        } else {
            self.search_workers
        };
        ParallelSearchConfig::default().with_workers(workers)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchSearchMode {
    Exhaustive,
    Heuristic,
}

impl BenchSearchMode {
    fn label(self) -> &'static str {
        match self {
            Self::Exhaustive => "exhaustive",
            Self::Heuristic => "heuristic",
        }
    }
}

#[derive(Default)]
struct SearchStats {
    ok: usize,
    no_path: usize,
    no_viable: usize,
    errors: usize,
    viable_routes: usize,
    failed_candidates: usize,
    quote_cache: QuoteCacheStats,
    liquidity_pruning: LiquidityPruneStats,
    latencies: Vec<Duration>,
    examples: Vec<String>,
    failure_examples: Vec<String>,
}

#[derive(Default)]
struct IncrementalCompareStats {
    sessions: usize,
    session_errors: usize,
    refreshes: usize,
    recompute_required: usize,
    routes_requoted: usize,
    probe_routes_quoted: usize,
    quote_executions: usize,
    full_recompute_ok: usize,
    divergences: usize,
    session_elapsed: Duration,
    incremental_elapsed: Duration,
    full_recompute_elapsed: Duration,
    examples: Vec<String>,
    failure_examples: Vec<String>,
}

#[derive(Default)]
struct OrderingCompareStats {
    ok: usize,
    errors: usize,
    final_best_found: usize,
    heuristic_divergences: usize,
    routes_observed: usize,
    route_index_at_best: Vec<usize>,
    time_to_first_quote: Vec<Duration>,
    time_to_best: Vec<Duration>,
    elapsed: Duration,
    quote_cache: QuoteCacheStats,
    liquidity_pruning: LiquidityPruneStats,
    failure_examples: Vec<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Some(rpc_url) = rpc_url_from_env() else {
        println!(
            "production_basket_search: set E2E_RPC_URL, ETHEREUM_RPC_URL, MAINNET_RPC_URL, ETH_RPC_URL, or RPC_URL; skipping."
        );
        return Ok(());
    };

    let config = BenchConfig::from_env();
    let basket = token_basket(config.token_limit);
    ensure!(
        basket.len() >= 2,
        "token basket must contain at least two tokens"
    );

    let setup_start = Instant::now();
    let provider = support::http_provider(&rpc_url)?;

    let latest = provider.get_block_number().await.context("latest block")?;
    let pinned =
        env_u64_opt("E2E_BLOCK").unwrap_or_else(|| latest.saturating_sub(config.block_lag));
    let mut cache = EvmCache::builder(provider.clone())
        .block(BlockId::number(pinned))
        .build()
        .await;
    let setup_elapsed = setup_start.elapsed();

    println!(
        "production_basket_search: @uniswap/default-token-list@{}, tokens={}, pinned_block={}, transport=balanced+batched+gzip",
        UNISWAP_DEFAULT_TOKEN_LIST_VERSION,
        basket.len(),
        pinned
    );
    println!(
        "limits: max_pools={}, searches={}, search_workers={}, max_hops={}, max_candidates={}, amount_units={}, prime_cache={}, warm_search_runs={}, search_mode={}, liquidity_priming={}, liquidity_compare={}, liquidity_compare_after={}, incremental_compare={}, ordering_compare={}",
        cap_label(config.max_pools),
        config.search_limit,
        config.parallel_search_config().workers,
        config.max_hops,
        config.max_candidates,
        config.amount_units,
        config.prime_cache,
        config.warm_search_runs.max(1),
        config.search_mode.label(),
        config.liquidity_priming,
        config.liquidity_compare,
        config.liquidity_compare_after,
        config.incremental_compare,
        config.ordering_compare
    );
    if config.search_mode == BenchSearchMode::Heuristic {
        println!(
            "heuristic: max_auto_connectors={}, min_auto_connector_degree={}, beam_width={}, parallel_edge_limit={}, simulate_finalists={}, max_finalists={}, target_first={}, prefix_dominance={}, fast_lane={}, fast_lane_connectors={}, fast_lane_direct_edges={}, fast_lane_connector_edges={}, edge_shortlist={}, shortlist_initial={}, shortlist_refinement={}, shortlist_refine={}, protocol_ordering={}, upper_bound_pruning={}, balance_cap_pruning={}, estimated_rate_pruning={}",
            config.heuristic.max_auto_connectors,
            config.heuristic.min_auto_connector_degree,
            optional_usize_label(config.heuristic.beam_width),
            config.heuristic.parallel_edge_limit,
            config.heuristic.simulate_finalists,
            config.heuristic.max_finalists,
            config.heuristic.target_first,
            config.heuristic.prefix_dominance,
            config.heuristic.fast_lane.enabled,
            config.heuristic.fast_lane.max_connectors,
            config.heuristic.fast_lane.direct_edges_per_pair,
            config.heuristic.fast_lane.connector_edges_per_pair,
            config.heuristic.edge_shortlist.enabled,
            config.heuristic.edge_shortlist.initial_edges_per_pair,
            config.heuristic.edge_shortlist.refinement_edges_per_pair,
            config.heuristic.edge_shortlist.refine_parallel_edges,
            config.heuristic.edge_shortlist.protocol_ordering,
            config.heuristic.upper_bound_pruning.enabled,
            config.heuristic.upper_bound_pruning.balance_cap_pruning,
            config.heuristic.upper_bound_pruning.estimated_rate_pruning
        );
    }
    println!("basket: {}", basket_symbols(&basket));

    let mut registry = registry_with_uniswap_adapters()?;
    let discovery = PoolDiscovery::for_registry(&registry, factory_config());

    let discovery_start = Instant::now();
    let discovered = discovery
        .find(
            &mut cache,
            PoolQuery::basket(basket.iter().map(|token| token.address)),
        )
        .context("factory basket discovery")?;
    let discovery_elapsed = discovery_start.elapsed();

    let connectors = connector_tokens(&basket);
    let mut pools: Vec<PoolRegistration> = discovered
        .into_iter()
        .map(|pool| pool.registration)
        .collect();
    sort_pools_for_benchmark(&mut pools, &connectors);
    let discovered_pool_count = pools.len();
    pools = cap_pools_for_benchmark(pools, config.max_pools);

    println!(
        "discovery: pools_found={}, pools_selected={}, elapsed={:?}",
        discovered_pool_count,
        pools.len(),
        discovery_elapsed
    );
    print_protocol_counts("selected", &pools);

    let cold_start_start = Instant::now();
    let outcomes = registry
        .cold_start_many(
            &mut pools,
            &mut cache,
            provider.as_ref(),
            ColdStartPolicy::Eager,
        )
        .await
        .context("cold-start discovered pools")?;
    let cold_start_elapsed = cold_start_start.elapsed();

    let ready_outcomes = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                ColdStartOutcome::Ready(_) | ColdStartOutcome::ReadyWithDeferred(_, _)
            )
        })
        .count();
    let repair_outcomes = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ColdStartOutcome::NeedsRepair(_, _)))
        .count();

    for pool in pools {
        registry.register_pool(pool)?;
    }

    let (ready, degraded, other) = registry_status_counts(&registry);
    println!(
        "cold_start: ready_outcomes={}, repair_outcomes={}, registry_ready={}, registry_degraded={}, registry_other={}, elapsed={:?}",
        ready_outcomes, repair_outcomes, ready, degraded, other, cold_start_elapsed
    );
    ensure!(
        ready > 0,
        "no pools reached Ready; cannot build a useful search graph"
    );

    let graph_start = Instant::now();
    let report = AmmGraph::from_registry(&registry, GraphBuildOptions::default());
    let graph_elapsed = graph_start.elapsed();
    println!(
        "graph: indexed_pools={}, skipped_pools={}, nodes={}, edges={}, elapsed={:?}",
        report.indexed_pools.len(),
        report.skipped_pools.len(),
        report.graph.node_count(),
        report.graph.edge_count(),
        graph_elapsed
    );

    let mut liquidity_index = None;
    if config.liquidity_priming {
        let liquidity_start = Instant::now();
        let liquidity_scope = if config.search_mode == BenchSearchMode::Heuristic {
            LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs
        } else {
            LiquidityIndexScope::ParallelEdgeOutputs
        };
        let (mut index, build_report) =
            PoolLiquidityIndex::from_registry_with_scope(&registry, &report.graph, liquidity_scope);
        let refresh_report = index.refresh_all(&mut cache, provider.as_ref()).await;
        println!(
            "liquidity: scope={:?}, tracked_balances={}, unknown_on_build={}, refreshed_balances={}, unknown_after_refresh={}, stale_after_refresh={}, storage_reads={}, failures={}, elapsed={:?}",
            liquidity_scope,
            build_report.tracked_balances,
            build_report.unknown_balances,
            refresh_report.refreshed_balances,
            refresh_report.unknown_balances,
            refresh_report.stale_balances,
            refresh_report.storage_reads,
            refresh_report.failures.len(),
            liquidity_start.elapsed()
        );
        liquidity_index = Some(index);
    }

    let queries = build_queries(&basket, config.search_limit);
    let mut search_config = SearchConfig::default()
        .with_hops(1, config.max_hops)
        .with_max_candidates(config.max_candidates)
        .with_connector_tokens(connectors.iter().copied());
    if config.search_mode == BenchSearchMode::Heuristic {
        search_config = search_config.with_mode(SearchMode::Heuristic(config.heuristic));
    }
    if config.liquidity_priming {
        search_config = search_config.with_liquidity_pruning(LiquidityPruningConfig::enabled());
    }
    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);
    let parallel_config = config.parallel_search_config();
    let mut searcher = AmmSearcher::new(&registry, &report.graph);
    if let Some(index) = liquidity_index.as_ref() {
        searcher = searcher.with_liquidity_index(index);
    }

    if config.prime_cache {
        let prime_start = Instant::now();
        let prime_stats = prime_search_cache(
            &searcher,
            &mut cache,
            &queries,
            &search_config,
            &sim_config,
            config.amount_units,
        );
        print_search_report(
            "prime_search",
            &queries,
            &prime_stats,
            prime_start.elapsed(),
        );
    }

    if config.liquidity_compare
        && config.liquidity_priming
        && config.search_mode == BenchSearchMode::Heuristic
    {
        let baseline_searcher = AmmSearcher::new(&registry, &report.graph);
        let baseline_config = search_config
            .clone()
            .with_liquidity_pruning(LiquidityPruningConfig::disabled());
        let baseline_start = Instant::now();
        let baseline_stats = run_searches(
            &baseline_searcher,
            &mut cache,
            &queries,
            &baseline_config,
            &sim_config,
            parallel_config,
            config.amount_units,
        );
        print_search_report(
            "liquidity_baseline_off",
            &queries,
            &baseline_stats,
            baseline_start.elapsed(),
        );
    }

    if config.ordering_compare && config.search_mode == BenchSearchMode::Heuristic {
        run_ordering_compare(
            &registry,
            &report.graph,
            liquidity_index.as_ref(),
            &mut cache,
            &queries,
            &search_config,
            &sim_config,
            parallel_config,
            config.amount_units,
        );
    }

    let mut search_elapsed = Duration::default();
    let mut last_stats = SearchStats::default();
    for pass in 0..config.warm_search_runs.max(1) {
        let search_start = Instant::now();
        let stats = run_searches(
            &searcher,
            &mut cache,
            &queries,
            &search_config,
            &sim_config,
            parallel_config,
            config.amount_units,
        );
        let elapsed = search_start.elapsed();
        print_search_report(&format!("search_pass[{pass}]"), &queries, &stats, elapsed);
        search_elapsed += elapsed;
        last_stats = stats;
    }

    if config.liquidity_compare_after
        && config.liquidity_priming
        && config.search_mode == BenchSearchMode::Heuristic
    {
        let baseline_searcher = AmmSearcher::new(&registry, &report.graph);
        let baseline_config = search_config
            .clone()
            .with_liquidity_pruning(LiquidityPruningConfig::disabled());
        let baseline_start = Instant::now();
        let baseline_stats = run_searches(
            &baseline_searcher,
            &mut cache,
            &queries,
            &baseline_config,
            &sim_config,
            parallel_config,
            config.amount_units,
        );
        print_search_report(
            "liquidity_baseline_off_after",
            &queries,
            &baseline_stats,
            baseline_start.elapsed(),
        );
    }

    if config.incremental_compare {
        let stats = run_incremental_compare(
            &searcher,
            &mut cache,
            &queries,
            &search_config,
            &sim_config,
            parallel_config,
            config.amount_units,
        );
        print_incremental_compare_report("incremental_compare", &queries, &stats);
    }

    println!(
        "last_search_summary: ok={}, no_path={}, no_viable={}, errors={}, viable_routes={}, failed_candidates={}",
        last_stats.ok,
        last_stats.no_path,
        last_stats.no_viable,
        last_stats.errors,
        last_stats.viable_routes,
        last_stats.failed_candidates
    );
    println!(
        "timing: setup={:?}, discovery={:?}, cold_start={:?}, graph_build={:?}, measured_search_total={:?}, total={:?}",
        setup_elapsed,
        discovery_elapsed,
        cold_start_elapsed,
        graph_elapsed,
        search_elapsed,
        setup_start.elapsed()
    );

    Ok(())
}

fn registry_with_uniswap_adapters() -> Result<AdapterRegistry> {
    let mut registry = AdapterRegistry::new();
    registry.register_adapter(Arc::new(UniswapV2Adapter::default()))?;
    registry.register_adapter(Arc::new(ConcentratedLiquidityAdapter::default()))?;
    Ok(registry)
}

fn factory_config() -> FactoryConfig {
    FactoryConfig::default()
        .with_uniswap_v2(UniswapV2FactoryConfig::uniswap_v2(UNISWAP_V2_FACTORY).with_fee_bps(30))
        .with_uniswap_v3(UniswapV3FactoryConfig::uniswap_v3(UNISWAP_V3_FACTORY))
}

fn prime_search_cache(
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    queries: &[(TokenInfo, TokenInfo)],
    search_config: &SearchConfig,
    sim_config: &SimConfig,
    amount_units: u64,
) -> SearchStats {
    let mut stats = SearchStats {
        ..SearchStats::default()
    };

    for (token_in, token_out) in queries {
        let request = RouteRequest::new(
            token_in.address,
            token_out.address,
            amount_for(token_in.decimals, amount_units),
        )
        .with_config(search_config.clone())
        .with_sim_config(*sim_config);

        match searcher.find_routes(&request, cache) {
            Ok(routes) => {
                stats.ok += 1;
                stats.viable_routes += routes.len();
            }
            Err(SearchError::NoPath { .. }) | Err(SearchError::TokenNotFound(_)) => {
                stats.no_path += 1;
            }
            Err(SearchError::NoViableRoute {
                candidates,
                failures,
            }) => {
                stats.no_viable += 1;
                stats.failed_candidates += candidates;
                if stats.failure_examples.len() < 5 {
                    let reason = failures
                        .first()
                        .map(|failure| failure.reason.as_str())
                        .unwrap_or("no failure detail");
                    stats.failure_examples.push(format!(
                        "{} -> {}: candidates={}, first_failure={}",
                        token_in.symbol, token_out.symbol, candidates, reason
                    ));
                }
            }
            Err(error) => {
                stats.errors += 1;
                if stats.failure_examples.len() < 5 {
                    stats.failure_examples.push(format!(
                        "{} -> {}: {}",
                        token_in.symbol, token_out.symbol, error
                    ));
                }
            }
        }
    }

    stats
}

fn run_searches(
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    queries: &[(TokenInfo, TokenInfo)],
    search_config: &SearchConfig,
    sim_config: &SimConfig,
    parallel_config: ParallelSearchConfig,
    amount_units: u64,
) -> SearchStats {
    let mut stats = SearchStats {
        ..SearchStats::default()
    };

    let requests = queries
        .iter()
        .map(|(token_in, token_out)| {
            RouteRequest::new(
                token_in.address,
                token_out.address,
                amount_for(token_in.decimals, amount_units),
            )
            .with_config(search_config.clone())
            .with_sim_config(*sim_config)
        })
        .collect::<Vec<_>>();

    let report =
        match searcher.find_routes_batch_parallel_with_stats(&requests, cache, parallel_config) {
            Ok(report) => report,
            Err(error) => {
                stats.errors = queries.len();
                stats
                    .failure_examples
                    .push(format!("batch search: {error}"));
                return stats;
            }
        };
    stats.quote_cache = report.quote_cache;
    stats.liquidity_pruning = report.liquidity_pruning;

    for ((token_in, token_out), result) in queries.iter().copied().zip(report.results) {
        match result {
            Ok(routes) => {
                stats.ok += 1;
                stats.viable_routes += routes.len();
                if stats.examples.len() < 5
                    && let Some(best) = routes.first()
                {
                    stats.examples.push(format!(
                        "{} -> {}: {} hop(s), {} routes, best_raw_out={}",
                        token_in.symbol,
                        token_out.symbol,
                        best.path.len(),
                        routes.len(),
                        best.amount_out
                    ));
                }
            }
            Err(SearchError::NoPath { .. }) | Err(SearchError::TokenNotFound(_)) => {
                stats.no_path += 1;
            }
            Err(SearchError::NoViableRoute {
                candidates,
                failures,
            }) => {
                stats.no_viable += 1;
                stats.failed_candidates += candidates;
                if stats.failure_examples.len() < 5 {
                    let reason = failures
                        .first()
                        .map(|failure| failure.reason.as_str())
                        .unwrap_or("no failure detail");
                    stats.failure_examples.push(format!(
                        "{} -> {}: candidates={}, first_failure={}",
                        token_in.symbol, token_out.symbol, candidates, reason
                    ));
                }
            }
            Err(error) => {
                stats.errors += 1;
                if stats.failure_examples.len() < 5 {
                    stats.failure_examples.push(format!(
                        "{} -> {}: {}",
                        token_in.symbol, token_out.symbol, error
                    ));
                }
            }
        }
    }

    stats
}

#[allow(clippy::too_many_arguments)]
fn run_ordering_compare(
    registry: &AdapterRegistry,
    graph: &AmmGraph,
    liquidity_index: Option<&PoolLiquidityIndex>,
    cache: &mut EvmCache,
    queries: &[(TokenInfo, TokenInfo)],
    search_config: &SearchConfig,
    sim_config: &SimConfig,
    parallel_config: ParallelSearchConfig,
    amount_units: u64,
) {
    let SearchMode::Heuristic(base_heuristic) = search_config.mode else {
        return;
    };

    let legacy_heuristic = base_heuristic
        .with_fast_lane(FastLaneConfig::disabled())
        .with_edge_shortlist(AdaptiveEdgeShortlistConfig::disabled())
        .with_upper_bound_pruning(UpperBoundPruningConfig::disabled());
    let target_prefix = legacy_heuristic
        .with_target_first(true)
        .with_prefix_dominance(true);
    let fast_lane = target_prefix.with_fast_lane(base_heuristic.fast_lane);
    let shortlist = fast_lane.with_edge_shortlist(base_heuristic.edge_shortlist);
    let upper_bound = shortlist.with_upper_bound_pruning(base_heuristic.upper_bound_pruning);
    let variants = [
        (
            "ordering_baseline",
            legacy_heuristic
                .with_target_first(false)
                .with_prefix_dominance(false),
            LiquidityPruningConfig::disabled(),
            None,
        ),
        (
            "ordering_target_first",
            legacy_heuristic
                .with_target_first(true)
                .with_prefix_dominance(false),
            LiquidityPruningConfig::disabled(),
            None,
        ),
        (
            "ordering_target_prefix",
            target_prefix,
            LiquidityPruningConfig::disabled(),
            None,
        ),
        (
            "ordering_fast_lane",
            fast_lane,
            LiquidityPruningConfig::disabled(),
            None,
        ),
        (
            "ordering_shortlist_protocol",
            shortlist,
            LiquidityPruningConfig::disabled(),
            None,
        ),
        (
            "ordering_upper_bound",
            upper_bound,
            LiquidityPruningConfig::disabled(),
            None,
        ),
        (
            "ordering_full_liquidity",
            base_heuristic,
            LiquidityPruningConfig::enabled(),
            liquidity_index,
        ),
    ];

    for (label, heuristic, liquidity_pruning, variant_liquidity) in variants {
        if label.ends_with("liquidity") && variant_liquidity.is_none() {
            println!("{label}: skipped; set PROD_BASKET_LIQUIDITY_PRIMING=1 for branch ranking");
            continue;
        }

        let mut variant_config = search_config
            .clone()
            .with_mode(SearchMode::Heuristic(heuristic))
            .with_liquidity_pruning(liquidity_pruning);
        if label != "ordering_full_liquidity" {
            variant_config =
                variant_config.with_liquidity_pruning(LiquidityPruningConfig::disabled());
        }

        let mut searcher = AmmSearcher::new(registry, graph);
        if let Some(index) = variant_liquidity {
            searcher = searcher.with_liquidity_index(index);
        }
        let stats = run_streaming_time_to_best(
            &searcher,
            cache,
            queries,
            &variant_config,
            sim_config,
            parallel_config,
            amount_units,
        );
        print_ordering_compare_report(label, queries, &stats);
    }
}

fn run_streaming_time_to_best(
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    queries: &[(TokenInfo, TokenInfo)],
    search_config: &SearchConfig,
    sim_config: &SimConfig,
    parallel_config: ParallelSearchConfig,
    amount_units: u64,
) -> OrderingCompareStats {
    let mut stats = OrderingCompareStats::default();
    let run_start = Instant::now();
    let streaming_config = StreamingSearchConfig::default()
        .with_parallel(parallel_config)
        .with_emit_all_viable(true);

    for (token_in, token_out) in queries {
        let request = RouteRequest::new(
            token_in.address,
            token_out.address,
            amount_for(token_in.decimals, amount_units),
        )
        .with_config(search_config.clone())
        .with_sim_config(*sim_config);

        let query_start = Instant::now();
        let mut observed_routes = 0_usize;
        let mut first_quote_elapsed = None;
        let mut best_events = Vec::<(usize, Duration, evm_amm_search::RoutePath, U256)>::new();
        match searcher.stream_routes_parallel(&request, cache, streaming_config, |event| {
            match event {
                RouteSearchEvent::BestUpdated { quote, .. } => {
                    first_quote_elapsed.get_or_insert_with(|| query_start.elapsed());
                    best_events.push((
                        observed_routes + 1,
                        query_start.elapsed(),
                        quote.path,
                        quote.amount_out,
                    ));
                }
                RouteSearchEvent::RouteFound { .. } => {
                    observed_routes += 1;
                    first_quote_elapsed.get_or_insert_with(|| query_start.elapsed());
                }
                _ => {}
            }
            SearchControl::Continue
        }) {
            Ok(report) => {
                stats.ok += 1;
                stats.routes_observed += report.routes_observed;
                add_quote_cache_stats(&mut stats.quote_cache, report.quote_cache);
                add_liquidity_prune_stats(&mut stats.liquidity_pruning, report.liquidity_pruning);
                if report.heuristic_was_final_best == Some(false) {
                    stats.heuristic_divergences += 1;
                }
                if let Some(elapsed) = first_quote_elapsed {
                    stats.time_to_first_quote.push(elapsed);
                }
                if let Some(best) = report.best
                    && let Some((index, elapsed, _, _)) =
                        best_events.into_iter().find(|(_, _, path, amount)| {
                            *path == best.path && *amount == best.amount_out
                        })
                {
                    stats.final_best_found += 1;
                    stats.route_index_at_best.push(index);
                    stats.time_to_best.push(elapsed);
                }
            }
            Err(error) => {
                stats.errors += 1;
                if stats.failure_examples.len() < 5 {
                    stats.failure_examples.push(format!(
                        "{} -> {}: {error}",
                        token_in.symbol, token_out.symbol
                    ));
                }
            }
        }
    }

    stats.elapsed = run_start.elapsed();
    stats
}

fn run_incremental_compare(
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    queries: &[(TokenInfo, TokenInfo)],
    search_config: &SearchConfig,
    sim_config: &SimConfig,
    parallel_config: ParallelSearchConfig,
    amount_units: u64,
) -> IncrementalCompareStats {
    let mut stats = IncrementalCompareStats::default();
    let streaming_config = StreamingSearchConfig::default()
        .with_top_k(1)
        .with_parallel(parallel_config);

    for (token_in, token_out) in queries {
        let request = RouteRequest::new(
            token_in.address,
            token_out.address,
            amount_for(token_in.decimals, amount_units),
        )
        .with_config(search_config.clone())
        .with_sim_config(*sim_config);

        let session_start = Instant::now();
        let mut session =
            match searcher.start_route_session(&request, cache, streaming_config, |_| {
                SearchControl::Continue
            }) {
                Ok(session) => session,
                Err(error) => {
                    stats.session_errors += 1;
                    if stats.failure_examples.len() < 5 {
                        stats.failure_examples.push(format!(
                            "{} -> {} session: {error}",
                            token_in.symbol, token_out.symbol
                        ));
                    }
                    continue;
                }
            };
        stats.session_elapsed += session_start.elapsed();
        stats.sessions += 1;

        let Some(best) = session.best().cloned() else {
            continue;
        };
        let Some(affected_pool) = best.path.hops.first().map(|hop| hop.pool.clone()) else {
            continue;
        };

        let before_cache = session.quote_cache_stats();
        let incremental_start = Instant::now();
        let report = session.refresh_affected(
            searcher,
            cache,
            AffectedPools::from_pool_keys([affected_pool.clone()]),
            |_| SearchControl::Continue,
        );
        stats.incremental_elapsed += incremental_start.elapsed();
        stats.refreshes += 1;
        stats.routes_requoted += report.routes_requoted;
        stats.probe_routes_quoted += report.probe_routes_quoted;
        stats.quote_executions += report
            .quote_cache
            .executed
            .saturating_sub(before_cache.executed);
        if report.status == IncrementalRouteUpdateStatus::RecomputeRequired {
            stats.recompute_required += 1;
        }

        let full_start = Instant::now();
        match searcher.find_routes_parallel(&request, cache, parallel_config) {
            Ok(routes) => {
                stats.full_recompute_elapsed += full_start.elapsed();
                stats.full_recompute_ok += 1;
                let full_best = routes.first();
                if full_best != report.best.as_ref() {
                    stats.divergences += 1;
                    if stats.failure_examples.len() < 5 {
                        stats.failure_examples.push(format!(
                            "{} -> {} divergence after {:?}: incremental_best={}, full_best={}",
                            token_in.symbol,
                            token_out.symbol,
                            affected_pool,
                            report
                                .best
                                .as_ref()
                                .map(|quote| quote.amount_out.to_string())
                                .unwrap_or_else(|| "none".to_owned()),
                            full_best
                                .map(|quote| quote.amount_out.to_string())
                                .unwrap_or_else(|| "none".to_owned())
                        ));
                    }
                }
                if stats.examples.len() < 5 {
                    stats.examples.push(format!(
                        "{} -> {} affected={:?}, requoted={}, probes={}, quote_exec_delta={}",
                        token_in.symbol,
                        token_out.symbol,
                        affected_pool,
                        report.routes_requoted,
                        report.probe_routes_quoted,
                        report
                            .quote_cache
                            .executed
                            .saturating_sub(before_cache.executed)
                    ));
                }
            }
            Err(error) => {
                stats.full_recompute_elapsed += full_start.elapsed();
                if stats.failure_examples.len() < 5 {
                    stats.failure_examples.push(format!(
                        "{} -> {} full recompute audit: {error}",
                        token_in.symbol, token_out.symbol
                    ));
                }
            }
        }
    }

    stats
}

fn print_search_report(
    label: &str,
    queries: &[(TokenInfo, TokenInfo)],
    stats: &SearchStats,
    search_elapsed: Duration,
) {
    println!(
        "{label}: queries={}, ok={}, no_path={}, no_viable={}, errors={}, viable_routes={}, failed_candidates={}, elapsed={:?}",
        queries.len(),
        stats.ok,
        stats.no_path,
        stats.no_viable,
        stats.errors,
        stats.viable_routes,
        stats.failed_candidates,
        search_elapsed
    );
    if stats.latencies.is_empty() {
        println!("{label} latency: per-query latency not measured in batch mode");
    } else {
        println!(
            "{label} latency: median={:?}, p95={:?}, max={:?}",
            percentile(&stats.latencies, 50),
            percentile(&stats.latencies, 95),
            stats.latencies.iter().max().copied().unwrap_or_default()
        );
    }
    println!(
        "{label} quote cache: executed={}, failed={}, hits={}, misses={}, waits={}",
        stats.quote_cache.executed,
        stats.quote_cache.failed,
        stats.quote_cache.hits,
        stats.quote_cache.misses,
        stats.quote_cache.waits
    );
    println!(
        "{label} liquidity pruning: ordered_groups={}, pruned_edges={}, stale_or_unknown_fail_open={}, balance_reads={}, refresh_failures={}, target_first_groups={}, prefix_dominated_states={}, liquidity_ranked_branch_groups={}, liquidity_unknown_branch_groups={}, fast_lane_routes={}, fast_lane_quotes={}, shortlist_initial_edges={}, shortlist_refinement_edges={}, shortlist_deferred_edges={}, protocol_ranked_edges={}, upper_bound_pruned_prefixes={}, upper_bound_unknown_prefixes={}",
        stats.liquidity_pruning.ordered_groups,
        stats.liquidity_pruning.pruned_edges,
        stats.liquidity_pruning.stale_or_unknown_skipped_for_pruning,
        stats.liquidity_pruning.balance_reads,
        stats.liquidity_pruning.refresh_failures,
        stats.liquidity_pruning.target_first_groups,
        stats.liquidity_pruning.prefix_dominated_states,
        stats.liquidity_pruning.liquidity_ranked_branch_groups,
        stats.liquidity_pruning.liquidity_unknown_branch_groups,
        stats.liquidity_pruning.fast_lane_routes,
        stats.liquidity_pruning.fast_lane_quotes,
        stats.liquidity_pruning.shortlist_initial_edges,
        stats.liquidity_pruning.shortlist_refinement_edges,
        stats.liquidity_pruning.shortlist_deferred_edges,
        stats.liquidity_pruning.protocol_ranked_edges,
        stats.liquidity_pruning.upper_bound_pruned_prefixes,
        stats.liquidity_pruning.upper_bound_unknown_prefixes
    );

    if !stats.examples.is_empty() {
        println!("{label} sample successful routes:");
        for example in &stats.examples {
            println!("  {example}");
        }
    }
    if !stats.failure_examples.is_empty() {
        println!("{label} sample failures:");
        for example in &stats.failure_examples {
            println!("  {example}");
        }
    }
}

fn print_incremental_compare_report(
    label: &str,
    queries: &[(TokenInfo, TokenInfo)],
    stats: &IncrementalCompareStats,
) {
    println!(
        "{label}: queries={}, sessions={}, session_errors={}, refreshes={}, recompute_required={}, routes_requoted={}, probe_routes_quoted={}, quote_exec_delta={}, full_recompute_ok={}, divergences={}",
        queries.len(),
        stats.sessions,
        stats.session_errors,
        stats.refreshes,
        stats.recompute_required,
        stats.routes_requoted,
        stats.probe_routes_quoted,
        stats.quote_executions,
        stats.full_recompute_ok,
        stats.divergences
    );
    println!(
        "{label} timing: session_start_total={:?}, incremental_total={:?}, full_recompute_total={:?}",
        stats.session_elapsed, stats.incremental_elapsed, stats.full_recompute_elapsed
    );
    if stats.refreshes > 0 {
        println!(
            "{label} avg: incremental={:?}, full_recompute={:?}",
            stats.incremental_elapsed / stats.refreshes as u32,
            stats.full_recompute_elapsed / stats.refreshes as u32
        );
    }
    if !stats.examples.is_empty() {
        println!("{label} sample refreshes:");
        for example in &stats.examples {
            println!("  {example}");
        }
    }
    if !stats.failure_examples.is_empty() {
        println!("{label} sample failures:");
        for example in &stats.failure_examples {
            println!("  {example}");
        }
    }
}

fn print_ordering_compare_report(
    label: &str,
    queries: &[(TokenInfo, TokenInfo)],
    stats: &OrderingCompareStats,
) {
    println!(
        "{label}: queries={}, ok={}, errors={}, final_best_found={}, heuristic_divergences={}, routes_observed={}, elapsed={:?}",
        queries.len(),
        stats.ok,
        stats.errors,
        stats.final_best_found,
        stats.heuristic_divergences,
        stats.routes_observed,
        stats.elapsed
    );
    if !stats.time_to_best.is_empty() {
        if !stats.time_to_first_quote.is_empty() {
            println!(
                "{label} time_to_first_quote: median={:?}, p95={:?}, max={:?}",
                percentile(&stats.time_to_first_quote, 50),
                percentile(&stats.time_to_first_quote, 95),
                stats
                    .time_to_first_quote
                    .iter()
                    .max()
                    .copied()
                    .unwrap_or_default()
            );
        }
        println!(
            "{label} time_to_eventual_best: median={:?}, p95={:?}, max={:?}, median_route_index={}, max_route_index={}",
            percentile(&stats.time_to_best, 50),
            percentile(&stats.time_to_best, 95),
            stats.time_to_best.iter().max().copied().unwrap_or_default(),
            percentile_usize(&stats.route_index_at_best, 50),
            stats
                .route_index_at_best
                .iter()
                .max()
                .copied()
                .unwrap_or_default()
        );
    }
    println!(
        "{label} quote cache: executed={}, failed={}, hits={}, misses={}, waits={}",
        stats.quote_cache.executed,
        stats.quote_cache.failed,
        stats.quote_cache.hits,
        stats.quote_cache.misses,
        stats.quote_cache.waits
    );
    println!(
        "{label} ordering stats: target_first_groups={}, prefix_dominated_states={}, liquidity_ranked_branch_groups={}, liquidity_unknown_branch_groups={}, parallel_ordered_groups={}, pruned_edges={}, balance_reads={}, fast_lane_routes={}, fast_lane_quotes={}, shortlist_initial_edges={}, shortlist_refinement_edges={}, shortlist_deferred_edges={}, protocol_ranked_edges={}, upper_bound_pruned_prefixes={}, upper_bound_unknown_prefixes={}",
        stats.liquidity_pruning.target_first_groups,
        stats.liquidity_pruning.prefix_dominated_states,
        stats.liquidity_pruning.liquidity_ranked_branch_groups,
        stats.liquidity_pruning.liquidity_unknown_branch_groups,
        stats.liquidity_pruning.ordered_groups,
        stats.liquidity_pruning.pruned_edges,
        stats.liquidity_pruning.balance_reads,
        stats.liquidity_pruning.fast_lane_routes,
        stats.liquidity_pruning.fast_lane_quotes,
        stats.liquidity_pruning.shortlist_initial_edges,
        stats.liquidity_pruning.shortlist_refinement_edges,
        stats.liquidity_pruning.shortlist_deferred_edges,
        stats.liquidity_pruning.protocol_ranked_edges,
        stats.liquidity_pruning.upper_bound_pruned_prefixes,
        stats.liquidity_pruning.upper_bound_unknown_prefixes
    );
    if !stats.failure_examples.is_empty() {
        println!("{label} sample failures:");
        for example in &stats.failure_examples {
            println!("  {example}");
        }
    }
}

fn add_quote_cache_stats(total: &mut QuoteCacheStats, next: QuoteCacheStats) {
    total.hits += next.hits;
    total.misses += next.misses;
    total.waits += next.waits;
    total.executed += next.executed;
    total.failed += next.failed;
}

fn add_liquidity_prune_stats(total: &mut LiquidityPruneStats, next: LiquidityPruneStats) {
    total.ordered_groups += next.ordered_groups;
    total.pruned_edges += next.pruned_edges;
    total.stale_or_unknown_skipped_for_pruning += next.stale_or_unknown_skipped_for_pruning;
    total.balance_reads += next.balance_reads;
    total.refresh_failures += next.refresh_failures;
    total.target_first_groups += next.target_first_groups;
    total.prefix_dominated_states += next.prefix_dominated_states;
    total.liquidity_ranked_branch_groups += next.liquidity_ranked_branch_groups;
    total.liquidity_unknown_branch_groups += next.liquidity_unknown_branch_groups;
    total.fast_lane_routes += next.fast_lane_routes;
    total.fast_lane_quotes += next.fast_lane_quotes;
    total.shortlist_initial_edges += next.shortlist_initial_edges;
    total.shortlist_refinement_edges += next.shortlist_refinement_edges;
    total.shortlist_deferred_edges += next.shortlist_deferred_edges;
    total.protocol_ranked_edges += next.protocol_ranked_edges;
    total.upper_bound_pruned_prefixes += next.upper_bound_pruned_prefixes;
    total.upper_bound_unknown_prefixes += next.upper_bound_unknown_prefixes;
}

fn build_queries(tokens: &[TokenInfo], limit: usize) -> Vec<(TokenInfo, TokenInfo)> {
    let mut queries = Vec::new();
    let mut seen = HashSet::new();
    let connector_symbols: HashSet<&str> = CONNECTOR_SYMBOLS.iter().copied().collect();
    let connectors: Vec<TokenInfo> = tokens
        .iter()
        .copied()
        .filter(|token| connector_symbols.contains(token.symbol))
        .collect();

    for connector in &connectors {
        for token in tokens {
            push_query(&mut queries, &mut seen, *connector, *token, limit);
            push_query(&mut queries, &mut seen, *token, *connector, limit);
            if queries.len() >= limit {
                return queries;
            }
        }
    }

    for token_in in tokens {
        for token_out in tokens {
            push_query(&mut queries, &mut seen, *token_in, *token_out, limit);
            if queries.len() >= limit {
                return queries;
            }
        }
    }

    queries
}

fn push_query(
    queries: &mut Vec<(TokenInfo, TokenInfo)>,
    seen: &mut HashSet<(Address, Address)>,
    token_in: TokenInfo,
    token_out: TokenInfo,
    limit: usize,
) {
    if token_in.address == token_out.address || queries.len() >= limit {
        return;
    }
    if seen.insert((token_in.address, token_out.address)) {
        queries.push((token_in, token_out));
    }
}

fn token_basket(limit: usize) -> Vec<TokenInfo> {
    UNISWAP_DEFAULT_TOKENS
        .iter()
        .copied()
        .take(limit.min(UNISWAP_DEFAULT_TOKENS.len()))
        .collect()
}

fn connector_tokens(tokens: &[TokenInfo]) -> Vec<Address> {
    tokens
        .iter()
        .filter(|token| CONNECTOR_SYMBOLS.contains(&token.symbol))
        .map(|token| token.address)
        .collect()
}

fn sort_pools_for_benchmark(pools: &mut [PoolRegistration], connectors: &[Address]) {
    pools.sort_by_key(|pool| {
        (
            !pool_touches_any(pool, connectors),
            pool_protocol_rank(&pool.key),
            format!("{:?}", pool.key),
        )
    });
}

fn cap_pools_for_benchmark(
    pools: Vec<PoolRegistration>,
    max_pools: usize,
) -> Vec<PoolRegistration> {
    if max_pools == 0 || pools.len() <= max_pools {
        return pools;
    }

    let mut v3 = VecDeque::new();
    let mut v2 = VecDeque::new();
    let mut other = VecDeque::new();
    for pool in pools {
        match pool.key {
            PoolKey::UniswapV3(_) => v3.push_back(pool),
            PoolKey::UniswapV2(_) => v2.push_back(pool),
            _ => other.push_back(pool),
        }
    }

    let mut selected = Vec::with_capacity(max_pools);
    while selected.len() < max_pools && (!v3.is_empty() || !v2.is_empty() || !other.is_empty()) {
        push_next_pool(&mut selected, &mut v3, max_pools);
        push_next_pool(&mut selected, &mut v2, max_pools);
        push_next_pool(&mut selected, &mut other, max_pools);
    }
    selected
}

fn push_next_pool(
    selected: &mut Vec<PoolRegistration>,
    queue: &mut VecDeque<PoolRegistration>,
    max_pools: usize,
) {
    if selected.len() < max_pools
        && let Some(pool) = queue.pop_front()
    {
        selected.push(pool);
    }
}

fn pool_touches_any(pool: &PoolRegistration, connectors: &[Address]) -> bool {
    pool_tokens(pool)
        .iter()
        .any(|token| connectors.iter().any(|connector| connector == token))
}

fn pool_tokens(pool: &PoolRegistration) -> Vec<Address> {
    match &pool.metadata {
        ProtocolMetadata::UniswapV2(metadata) => {
            metadata.token0.into_iter().chain(metadata.token1).collect()
        }
        ProtocolMetadata::UniswapV3(metadata) => {
            metadata.token0.into_iter().chain(metadata.token1).collect()
        }
        _ => Vec::new(),
    }
}

fn pool_protocol_rank(pool: &PoolKey) -> u8 {
    match pool {
        PoolKey::UniswapV3(_) => 0,
        PoolKey::UniswapV2(_) => 1,
        _ => 2,
    }
}

fn print_protocol_counts(label: &str, pools: &[PoolRegistration]) {
    let v2 = pools
        .iter()
        .filter(|pool| matches!(pool.key, PoolKey::UniswapV2(_)))
        .count();
    let v3 = pools
        .iter()
        .filter(|pool| matches!(pool.key, PoolKey::UniswapV3(_)))
        .count();
    println!("{label} pools by protocol: uniswap_v2={v2}, uniswap_v3={v3}");
}

fn registry_status_counts(registry: &AdapterRegistry) -> (usize, usize, usize) {
    let mut ready = 0;
    let mut degraded = 0;
    let mut other = 0;
    for pool in registry.pools() {
        match pool.status {
            PoolStatus::Ready => ready += 1,
            PoolStatus::Degraded => degraded += 1,
            _ => other += 1,
        }
    }
    (ready, degraded, other)
}

fn amount_for(decimals: u8, whole_units: u64) -> U256 {
    U256::from(whole_units) * decimal_multiplier(decimals)
}

fn decimal_multiplier(decimals: u8) -> U256 {
    let mut multiplier = U256::from(1);
    for _ in 0..decimals {
        multiplier *= U256::from(10);
    }
    multiplier
}

fn percentile(latencies: &[Duration], percentile: usize) -> Duration {
    if latencies.is_empty() {
        return Duration::default();
    }

    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile) / 100;
    sorted[index]
}

fn percentile_usize(values: &[usize], percentile: usize) -> usize {
    if values.is_empty() {
        return 0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile) / 100;
    sorted[index]
}

fn basket_symbols(tokens: &[TokenInfo]) -> String {
    tokens
        .iter()
        .map(|token| token.symbol)
        .collect::<Vec<_>>()
        .join(", ")
}

fn cap_label(cap: usize) -> String {
    if cap == 0 {
        "none".to_string()
    } else {
        cap.to_string()
    }
}

fn optional_usize_label(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn rpc_url_from_env() -> Option<String> {
    [
        "E2E_RPC_URL",
        "ETHEREUM_RPC_URL",
        "MAINNET_RPC_URL",
        "ETH_RPC_URL",
        "RPC_URL",
    ]
    .into_iter()
    .find_map(|name| env::var(name).ok().filter(|value| !value.is_empty()))
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize_opt(name: &str) -> Option<usize> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_u64_opt(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn env_search_mode(name: &str, default: BenchSearchMode) -> BenchSearchMode {
    env::var(name)
        .ok()
        .and_then(|value| match value.as_str() {
            "exhaustive" | "EXHAUSTIVE" => Some(BenchSearchMode::Exhaustive),
            "heuristic" | "HEURISTIC" => Some(BenchSearchMode::Heuristic),
            _ => None,
        })
        .unwrap_or(default)
}

fn heuristic_config_from_env() -> HeuristicSearchConfig {
    let default = env_heuristic_preset("PROD_BASKET_HEURISTIC_PRESET");
    let max_auto_connectors = env_usize(
        "PROD_BASKET_HEURISTIC_MAX_AUTO_CONNECTORS",
        default.max_auto_connectors,
    );
    let min_auto_connector_degree = env_usize(
        "PROD_BASKET_HEURISTIC_MIN_AUTO_CONNECTOR_DEGREE",
        default.min_auto_connector_degree,
    );
    let beam_width = env_usize_opt("PROD_BASKET_HEURISTIC_BEAM_WIDTH")
        .map(|value| (value != 0).then_some(value))
        .unwrap_or(default.beam_width);
    let parallel_edge_limit = env_usize(
        "PROD_BASKET_HEURISTIC_PARALLEL_EDGE_LIMIT",
        default.parallel_edge_limit,
    );
    let simulate_finalists = env_bool(
        "PROD_BASKET_HEURISTIC_SIMULATE_FINALISTS",
        default.simulate_finalists,
    );
    let max_finalists = env_usize("PROD_BASKET_HEURISTIC_MAX_FINALISTS", default.max_finalists);
    let target_first = env_bool("PROD_BASKET_HEURISTIC_TARGET_FIRST", default.target_first);
    let prefix_dominance = env_bool(
        "PROD_BASKET_HEURISTIC_PREFIX_DOMINANCE",
        default.prefix_dominance,
    );
    let fast_lane_enabled = env_bool("PROD_BASKET_HEURISTIC_FAST_LANE", default.fast_lane.enabled);
    let fast_lane = FastLaneConfig {
        enabled: fast_lane_enabled,
        max_connectors: env_usize(
            "PROD_BASKET_FAST_LANE_CONNECTORS",
            default.fast_lane.max_connectors,
        ),
        direct_edges_per_pair: env_usize(
            "PROD_BASKET_FAST_LANE_DIRECT_EDGES",
            default.fast_lane.direct_edges_per_pair,
        ),
        connector_edges_per_pair: env_usize(
            "PROD_BASKET_FAST_LANE_CONNECTOR_EDGES",
            default.fast_lane.connector_edges_per_pair,
        ),
        evaluate_direct_best_first: env_bool(
            "PROD_BASKET_FAST_LANE_DIRECT_FIRST",
            default.fast_lane.evaluate_direct_best_first,
        ),
    };
    let edge_shortlist_enabled =
        env_bool("PROD_BASKET_EDGE_SHORTLIST", default.edge_shortlist.enabled);
    let edge_shortlist = AdaptiveEdgeShortlistConfig {
        enabled: edge_shortlist_enabled,
        initial_edges_per_pair: env_usize(
            "PROD_BASKET_EDGE_SHORTLIST_INITIAL",
            default.edge_shortlist.initial_edges_per_pair,
        ),
        refinement_edges_per_pair: env_usize(
            "PROD_BASKET_EDGE_SHORTLIST_REFINEMENT",
            default.edge_shortlist.refinement_edges_per_pair,
        ),
        refine_parallel_edges: env_bool(
            "PROD_BASKET_EDGE_SHORTLIST_REFINE",
            default.edge_shortlist.refine_parallel_edges,
        ),
        protocol_ordering: env_bool(
            "PROD_BASKET_PROTOCOL_ORDERING",
            default.edge_shortlist.protocol_ordering,
        ),
    };
    let upper_bound_pruning = UpperBoundPruningConfig {
        enabled: env_bool(
            "PROD_BASKET_UPPER_BOUND_PRUNING",
            default.upper_bound_pruning.enabled,
        ),
        balance_cap_pruning: env_bool(
            "PROD_BASKET_BALANCE_CAP_PRUNING",
            default.upper_bound_pruning.balance_cap_pruning,
        ),
        estimated_rate_pruning: env_bool(
            "PROD_BASKET_ESTIMATED_RATE_PRUNING",
            default.upper_bound_pruning.estimated_rate_pruning,
        ),
        fail_open_on_unknown: env_bool(
            "PROD_BASKET_UPPER_BOUND_FAIL_OPEN",
            default.upper_bound_pruning.fail_open_on_unknown,
        ),
    };

    default
        .with_auto_connectors(max_auto_connectors, min_auto_connector_degree)
        .with_beam_width(beam_width)
        .with_parallel_edge_limit(parallel_edge_limit)
        .with_finalist_simulation(simulate_finalists, max_finalists)
        .with_target_first(target_first)
        .with_prefix_dominance(prefix_dominance)
        .with_fast_lane(fast_lane)
        .with_edge_shortlist(edge_shortlist)
        .with_upper_bound_pruning(upper_bound_pruning)
}

fn env_heuristic_preset(name: &str) -> HeuristicSearchConfig {
    env::var(name)
        .ok()
        .and_then(|value| match value.as_str() {
            "balanced" | "BALANCED" | "default" | "DEFAULT" => {
                Some(HeuristicSearchConfig::balanced())
            }
            "latency_first" | "LATENCY_FIRST" | "latency-first" | "LATENCY-FIRST"
            | "aggressive" | "AGGRESSIVE" => Some(HeuristicSearchConfig::latency_first()),
            _ => None,
        })
        .unwrap_or_else(HeuristicSearchConfig::balanced)
}

const UNISWAP_DEFAULT_TOKENS: &[TokenInfo] = &[
    TokenInfo {
        symbol: "WETH",
        address: address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "USDC",
        address: address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
        decimals: 6,
    },
    TokenInfo {
        symbol: "USDT",
        address: address!("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
        decimals: 6,
    },
    TokenInfo {
        symbol: "DAI",
        address: address!("0x6B175474E89094C44Da98b954EedeAC495271d0F"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "WBTC",
        address: address!("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"),
        decimals: 8,
    },
    TokenInfo {
        symbol: "LINK",
        address: address!("0x514910771AF9Ca656af840dff83E8264EcF986CA"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "UNI",
        address: address!("0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "AAVE",
        address: address!("0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "MKR",
        address: address!("0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "CRV",
        address: address!("0xD533a949740bb3306d119CC777fa900bA034cd52"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "LDO",
        address: address!("0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "cbETH",
        address: address!("0xBe9895146f7AF43049ca1c1AE358B0541Ea49704"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "COMP",
        address: address!("0xc00e94Cb662C3520282E6f5717214004A7f26888"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "SNX",
        address: address!("0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "YFI",
        address: address!("0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "1INCH",
        address: address!("0x111111111117dC0aa78b770fA6A738034120C302"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "BAL",
        address: address!("0xba100000625a3754423978a60c9317c58a424e3D"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "ENS",
        address: address!("0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "FRAX",
        address: address!("0x853d955aCEf822Db058eb8505911ED77F175b99e"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "FXS",
        address: address!("0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "GNO",
        address: address!("0x6810e776880C02933D47DB1b9fc05908e5386b96"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "RPL",
        address: address!("0xD33526068D116cE69F19A9ee46F0bd304F21A51f"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "SUSHI",
        address: address!("0x6B3595068778DD592e39A122f4f5a5cF09C90fE2"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "SHIB",
        address: address!("0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "PEPE",
        address: address!("0x6982508145454Ce325dDbE47a25d4ec3d2311933"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "GRT",
        address: address!("0xc944E90C64B2c07662A292be6244BDf05Cda44a7"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "MATIC",
        address: address!("0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "POL",
        address: address!("0x455e53CBB86018Ac2B8092FdCd39d8444aFFC3F6"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "ARB",
        address: address!("0xB50721BCf8d664c30412Cfbc6cf7a15145234ad1"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "MNT",
        address: address!("0x3c3a81e81dc49A522A592e7622A7E711c06bf354"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "PYUSD",
        address: address!("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
        decimals: 6,
    },
    TokenInfo {
        symbol: "APE",
        address: address!("0x4d224452801ACEd8B2F0aebE155379bb5D594381"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "PENDLE",
        address: address!("0x808507121B80c02388fAd14726482e061B8da827"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "ENA",
        address: address!("0x57e114B691Db790C35207b2e685D4A43181e6061"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "EIGEN",
        address: address!("0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "MORPHO",
        address: address!("0x58D97B57BB95320F9a05dC918Aef65434969c2B2"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "SKY",
        address: address!("0x56072C95FAA701256059aa122697B133aDEd9279"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "USDS",
        address: address!("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "SPX",
        address: address!("0xE0f63A424a4439cBE457D80E4f4b51aD25b2c56C"),
        decimals: 8,
    },
    TokenInfo {
        symbol: "FET",
        address: address!("0xaea46A60368A7bD060eec7DF8CBa43b7EF41Ad85"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "RNDR",
        address: address!("0x6De037ef9aD2725EB40118Bb1702EBb27e4Aeb24"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "IMX",
        address: address!("0xF57e7e7C23978C3cAEC3C3548E3D615c346e79fF"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "LRC",
        address: address!("0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "ZRX",
        address: address!("0xE41d2489571d322189246DaFA5ebDe1F4699F498"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "BAT",
        address: address!("0x0D8775F648430679A709E98d2b0Cb6250d2887EF"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "AMP",
        address: address!("0xfF20817765cB7f73d4bde2e66e067E58D11095C2"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "ANKR",
        address: address!("0x8290333ceF9e6D528dD5618Fb97a76f268f3EDD4"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "API3",
        address: address!("0x0b38210ea11411557c13457D4dA7dC6ea731B88a"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "AXL",
        address: address!("0x467719aD09025FcC6cF6F8311755809d45a5E5f3"),
        decimals: 6,
    },
    TokenInfo {
        symbol: "BLUR",
        address: address!("0x5283D291DBCF85356A21bA090E6db59121208b44"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "BNT",
        address: address!("0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "CAKE",
        address: address!("0x152649eA73beAb28c5b49B26eb48f7EAD6d4c898"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "CVX",
        address: address!("0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "COW",
        address: address!("0xDEf1CA1fb7FBcDC777520aa7f396b4E015F497aB"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "DYDX",
        address: address!("0x92D6C1e31e14520e676a687F0a93788B716BEff5"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "ETHFI",
        address: address!("0xFe0c30065B384F05761f15d0CC899D4F9F9Cc0eB"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "LQTY",
        address: address!("0x6DEA81C8171D0bA574754EF6F8b412F2Ed88c54D"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "MOG",
        address: address!("0xaaeE1A9723aaDB7afA2810263653A34bA2C21C7a"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "ONDO",
        address: address!("0xfAbA6f8e4a5E8Ab82F62fe7C39859FA577269BE3"),
        decimals: 18,
    },
    TokenInfo {
        symbol: "SAFE",
        address: address!("0x5aFE3855358E112B5647B952709E6165e1c1eEEe"),
        decimals: 18,
    },
];
