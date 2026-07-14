//! Focused live benchmark for README numbers.
//!
//! The workload is intentionally small and repeatable:
//!
//! - 10 WETH -> USDC
//! - 100 LINK -> AAVE
//! - 1000 DAI -> UNI
//!
//! ```text
//! E2E_RPC_URL=<mainnet-url> cargo run --release --example docs_route_benchmark
//! ```

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_eips::BlockId;
use alloy_primitives::{Address, U256, address};
use alloy_provider::Provider;
use anyhow::{Context, Result, ensure};
use evm_amm_search::{
    AffectedPools, AmmGraph, AmmSearcher, GraphBuildOptions, HeuristicSearchConfig,
    LiquidityIndexScope, LiquidityPruneStats, LiquidityPruningConfig, ParallelSearchConfig,
    PoolLiquidityIndex, QuoteCacheStats, RoutePath, RouteQuote, RouteRequest, RouteSearchEvent,
    RouteSearchPhase, SearchConfig, SearchControl, SearchMode, StreamingSearchConfig,
};
use evm_amm_state::adapters::{
    AdapterRegistry, ColdStartOutcome, ColdStartPolicy, ConcentratedLiquidityAdapter,
    FactoryConfig, PoolDiscovery, PoolKey, PoolQuery, PoolRegistration, PoolStatus, SimConfig,
    UniswapV2Adapter, UniswapV2FactoryConfig, UniswapV3FactoryConfig,
};
use evm_fork_cache::cache::{CacheConfig, CacheSpeedMode, EvmCache, SharedMemoryCapacity};

mod support;

const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const USDT: Address = address!("dAC17F958D2ee523a2206206994597C13D831ec7");
const DAI: Address = address!("6B175474E89094C44Da98b954EedeAC495271d0F");
const WBTC: Address = address!("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599");
const LINK: Address = address!("514910771AF9Ca656af840dff83E8264EcF986CA");
const UNI: Address = address!("1f9840a85d5aF5bf1D1762F925BDADdC4201F984");
const AAVE: Address = address!("7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9");

const UNISWAP_V2_FACTORY: Address = address!("5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
const UNISWAP_V3_FACTORY: Address = address!("1F98431c8aD98523631AE4a59f267346ea31F984");
const V2_ROUTER_02: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const V3_QUOTER_V2: Address = address!("61fFE014bA17989E743c5F6cB21bF9697530B21e");

const DEFAULT_RUNS: usize = 30;
const DEFAULT_BLOCK_LAG: u64 = 8;
const DEFAULT_MAX_HOPS: usize = 3;
const DEFAULT_WORKERS: usize = 0;
const DEFAULT_PERSIST_CACHE: bool = true;

#[derive(Clone, Copy, Debug)]
struct TokenInfo {
    symbol: &'static str,
    address: Address,
    decimals: u8,
}

#[derive(Clone, Copy, Debug)]
struct BenchRoute {
    label: &'static str,
    token_in: TokenInfo,
    token_out: TokenInfo,
    amount_units: u64,
}

#[derive(Clone, Debug)]
struct BenchConfig {
    runs: usize,
    block_lag: u64,
    max_hops: usize,
    workers: usize,
    persist_cache: bool,
    cache_dir: PathBuf,
}

#[derive(Default)]
struct RouteTimingStats {
    ok: usize,
    errors: usize,
    first_quote: Vec<Duration>,
    heuristic_done: Vec<Duration>,
    best_found: Vec<Duration>,
    exhaustive_done: Vec<Duration>,
    first_output: Vec<U256>,
    best_output: Vec<U256>,
    first_to_best_gap_bps: Vec<f64>,
    routes_observed: usize,
    improvements_after_heuristic: usize,
    heuristic_divergences: usize,
    quote_cache: QuoteCacheStats,
    liquidity_pruning: LiquidityPruneStats,
    sample_best: Option<RouteQuote>,
    sample_error: Option<String>,
}

#[derive(Default)]
struct VariantStats {
    routes: Vec<(&'static str, RouteTimingStats)>,
}

#[derive(Default)]
struct IncrementalStats {
    ok: usize,
    session_errors: usize,
    refreshes: usize,
    divergences: usize,
    routes_requoted: usize,
    probes_quoted: usize,
    quote_exec_delta: usize,
    session_start: Vec<Duration>,
    refresh: Vec<Duration>,
    full_recompute: Vec<Duration>,
    sample: Option<String>,
    sample_error: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Some(rpc_url) = rpc_url_from_env() else {
        println!(
            "docs_route_benchmark: set E2E_RPC_URL, ETHEREUM_RPC_URL, MAINNET_RPC_URL, ETH_RPC_URL, or RPC_URL; skipping."
        );
        return Ok(());
    };

    let config = BenchConfig::from_env();
    let provider = support::http_provider(&rpc_url)?;

    let latest = provider.get_block_number().await.context("latest block")?;
    let pinned = env_u64_opt("DOCS_BENCH_BLOCK")
        .or_else(|| env_u64_opt("E2E_BLOCK"))
        .unwrap_or_else(|| latest.saturating_sub(config.block_lag));

    let cache_build_start = Instant::now();
    let mut builder = EvmCache::builder(provider.clone())
        .block(BlockId::number(pinned))
        .chain_id(1)
        .speed_mode(CacheSpeedMode::Fast);
    if config.persist_cache {
        builder = builder
            .cache_config(CacheConfig::new(
                &config.cache_dir,
                1,
                Default::default(),
                Default::default(),
            ))
            .shared_memory_capacity(SharedMemoryCapacity::Auto);
    }
    let mut cache = builder.build().await;
    let cache_build_elapsed = cache_build_start.elapsed();

    println!(
        "docs_route_benchmark: pinned_block={}, runs={}, max_hops={}, workers={}, persist_cache={}, cache_dir={}, transport=balanced+batched+gzip",
        pinned,
        config.runs,
        config.max_hops,
        parallel_config(&config).workers,
        config.persist_cache,
        config.cache_dir.display()
    );
    println!("cache_build: elapsed={cache_build_elapsed:?}");

    let mut registry = registry_with_uniswap_adapters()?;
    let discovery = PoolDiscovery::for_registry(&registry, factory_config());
    let basket = token_universe();

    let discovery_start = Instant::now();
    let discovered = discovery
        .find(
            &mut cache,
            PoolQuery::basket(basket.iter().map(|token| token.address)),
        )
        .context("factory basket discovery")?;
    let discovery_elapsed = discovery_start.elapsed();

    let mut pools: Vec<PoolRegistration> = discovered
        .into_iter()
        .map(|pool| pool.registration)
        .collect();
    pools.sort_by_key(|pool| (pool_protocol_rank(&pool.key), format!("{:?}", pool.key)));
    let discovered_pools = pools.len();
    ensure!(discovered_pools > 0, "no pools discovered");
    println!("discovery: pools={discovered_pools}, elapsed={discovery_elapsed:?}");
    print_protocol_counts("discovered", &pools);

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
        "cold_start_without_liquidity: ready_outcomes={}, repair_outcomes={}, registry_ready={}, registry_degraded={}, registry_other={}, elapsed={:?}",
        ready_outcomes, repair_outcomes, ready, degraded, other, cold_start_elapsed
    );
    ensure!(ready > 0, "no pools reached Ready");

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

    let liquidity_start = Instant::now();
    let (mut liquidity, liquidity_build) = PoolLiquidityIndex::from_registry_with_scope(
        &registry,
        &report.graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    let liquidity_refresh = liquidity.refresh_all(&mut cache, provider.as_ref()).await;
    let liquidity_elapsed = liquidity_start.elapsed();
    println!(
        "liquidity_refresh: scope={:?}, tracked_balances={}, unknown_on_build={}, refreshed_balances={}, unknown_after_refresh={}, stale_after_refresh={}, storage_reads={}, failures={}, elapsed={:?}",
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
        liquidity_build.tracked_balances,
        liquidity_build.unknown_balances,
        liquidity_refresh.refreshed_balances,
        liquidity_refresh.unknown_balances,
        liquidity_refresh.stale_balances,
        liquidity_refresh.storage_reads,
        liquidity_refresh.failures.len(),
        liquidity_elapsed
    );
    println!(
        "cold_start_with_liquidity: elapsed={:?}",
        cold_start_elapsed + liquidity_elapsed
    );

    let routes = benchmark_routes();
    let connectors = connector_tokens();
    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);

    let base_searcher = AmmSearcher::new(&registry, &report.graph);
    let liquidity_searcher =
        AmmSearcher::new(&registry, &report.graph).with_liquidity_index(&liquidity);
    let variants = [
        ("balanced_no_liquidity_sim_winner_on", false, true),
        ("balanced_no_liquidity_sim_winner_off", false, false),
        ("balanced_liquidity_sim_winner_on", true, true),
        ("balanced_liquidity_sim_winner_off", true, false),
    ];

    for (label, use_liquidity, simulate_finalists) in variants {
        let searcher = if use_liquidity {
            &liquidity_searcher
        } else {
            &base_searcher
        };
        let search_config = search_config(
            config.max_hops,
            connectors.iter().copied(),
            use_liquidity,
            simulate_finalists,
        );
        let stats = run_streaming_variant(
            searcher,
            &mut cache,
            &routes,
            &search_config,
            &sim_config,
            &config,
        );
        print_variant_report(label, &routes, &stats);
    }

    let incremental_config = search_config(config.max_hops, connectors.iter().copied(), true, true);
    let incremental = run_incremental_benchmark(
        &liquidity_searcher,
        &mut cache,
        &routes,
        &incremental_config,
        &sim_config,
        &config,
    );
    print_incremental_report("incremental_route_refresh", &routes, &incremental);

    if config.persist_cache {
        let flush_start = Instant::now();
        cache.flush().context("flush warmed cache")?;
        println!("cache_flush: elapsed={:?}", flush_start.elapsed());
    }

    Ok(())
}

impl BenchConfig {
    fn from_env() -> Self {
        let cache_dir = env::var("DOCS_BENCH_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".cache/docs-route-benchmark"));
        Self {
            runs: env_usize("DOCS_BENCH_RUNS", DEFAULT_RUNS),
            block_lag: env_u64("DOCS_BENCH_BLOCK_LAG", DEFAULT_BLOCK_LAG),
            max_hops: env_usize("DOCS_BENCH_MAX_HOPS", DEFAULT_MAX_HOPS),
            workers: env_usize("DOCS_BENCH_WORKERS", DEFAULT_WORKERS),
            persist_cache: env_bool("DOCS_BENCH_PERSIST_CACHE", DEFAULT_PERSIST_CACHE),
            cache_dir,
        }
    }
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

fn parallel_config(config: &BenchConfig) -> ParallelSearchConfig {
    let workers = if config.workers == 0 {
        std::thread::available_parallelism().map_or(1, usize::from)
    } else {
        config.workers
    };
    ParallelSearchConfig::default().with_workers(workers)
}

fn search_config(
    max_hops: usize,
    connectors: impl IntoIterator<Item = Address>,
    use_liquidity: bool,
    simulate_finalists: bool,
) -> SearchConfig {
    let heuristic = HeuristicSearchConfig::balanced()
        .with_finalist_simulation(simulate_finalists, 16)
        .with_auto_connectors(8, 4);
    let mut config = SearchConfig::default()
        .with_hops(1, max_hops)
        .with_connector_tokens(connectors)
        .with_mode(SearchMode::Heuristic(heuristic));
    if use_liquidity {
        config = config.with_liquidity_pruning(LiquidityPruningConfig::enabled());
    }
    config
}

fn run_streaming_variant(
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    routes: &[BenchRoute],
    search_config: &SearchConfig,
    sim_config: &SimConfig,
    config: &BenchConfig,
) -> VariantStats {
    let mut variant = VariantStats::default();
    for route in routes {
        let mut stats = RouteTimingStats::default();
        for _ in 0..config.runs {
            let request = route_request(route, search_config.clone(), *sim_config);
            let started = Instant::now();
            let mut first_elapsed = None;
            let mut first_output = None;
            let mut heuristic_done = None;
            let mut best_events = Vec::<(Duration, RoutePath, U256)>::new();
            let result = searcher.stream_routes_parallel(
                &request,
                cache,
                StreamingSearchConfig::default()
                    .with_parallel(parallel_config(config))
                    .with_top_k(1),
                |event| {
                    match event {
                        RouteSearchEvent::BestUpdated { quote, .. } => {
                            first_elapsed.get_or_insert_with(|| started.elapsed());
                            first_output.get_or_insert(quote.amount_out);
                            best_events.push((started.elapsed(), quote.path, quote.amount_out));
                        }
                        RouteSearchEvent::RouteFound { quote, .. } => {
                            first_elapsed.get_or_insert_with(|| started.elapsed());
                            first_output.get_or_insert(quote.amount_out);
                        }
                        RouteSearchEvent::PhaseCompleted {
                            phase: RouteSearchPhase::Heuristic,
                            ..
                        } => {
                            heuristic_done.get_or_insert_with(|| started.elapsed());
                        }
                        _ => {}
                    }
                    SearchControl::Continue
                },
            );
            let total = started.elapsed();
            match result {
                Ok(report) => {
                    stats.ok += 1;
                    stats.routes_observed += report.routes_observed;
                    stats.improvements_after_heuristic += report.improvements_after_heuristic;
                    if report.heuristic_was_final_best == Some(false) {
                        stats.heuristic_divergences += 1;
                    }
                    add_quote_cache_stats(&mut stats.quote_cache, report.quote_cache);
                    add_liquidity_stats(&mut stats.liquidity_pruning, report.liquidity_pruning);
                    if let Some(elapsed) = first_elapsed {
                        stats.first_quote.push(elapsed);
                    }
                    if let Some(elapsed) = heuristic_done {
                        stats.heuristic_done.push(elapsed);
                    }
                    stats.exhaustive_done.push(total);
                    if let Some(best) = report.best {
                        let first = first_output.unwrap_or(best.amount_out);
                        stats.first_output.push(first);
                        stats.best_output.push(best.amount_out);
                        stats.first_to_best_gap_bps.push(gap_bps(
                            first,
                            best.amount_out,
                            route.token_out.decimals,
                        ));
                        if let Some((elapsed, _, _)) =
                            best_events.iter().find(|(_, path, amount)| {
                                *path == best.path && *amount == best.amount_out
                            })
                        {
                            stats.best_found.push(*elapsed);
                        }
                        stats.sample_best.get_or_insert(best);
                    }
                }
                Err(error) => {
                    stats.errors += 1;
                    stats.sample_error.get_or_insert_with(|| error.to_string());
                }
            }
        }
        variant.routes.push((route.label, stats));
    }
    variant
}

fn run_incremental_benchmark(
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    routes: &[BenchRoute],
    search_config: &SearchConfig,
    sim_config: &SimConfig,
    config: &BenchConfig,
) -> IncrementalStats {
    let mut stats = IncrementalStats::default();
    for route in routes {
        let request = route_request(route, search_config.clone(), *sim_config);
        for _ in 0..config.runs {
            let session_start = Instant::now();
            let mut session = match searcher.start_route_session(
                &request,
                cache,
                StreamingSearchConfig::default()
                    .with_parallel(parallel_config(config))
                    .with_top_k(1),
                |_| SearchControl::Continue,
            ) {
                Ok(session) => session,
                Err(error) => {
                    stats.session_errors += 1;
                    stats.sample_error.get_or_insert_with(|| {
                        format!("{} session start failed: {error}", route.label)
                    });
                    continue;
                }
            };
            stats.session_start.push(session_start.elapsed());

            let Some(best) = session.best().cloned() else {
                continue;
            };
            let Some(affected_pool) = best.path.hops.first().map(|hop| hop.pool.clone()) else {
                continue;
            };
            let before_cache = session.quote_cache_stats();
            let refresh_start = Instant::now();
            let report = session.refresh_affected(
                searcher,
                cache,
                AffectedPools::from_pool_keys([affected_pool.clone()]),
                |_| SearchControl::Continue,
            );
            stats.refresh.push(refresh_start.elapsed());
            stats.refreshes += 1;
            stats.routes_requoted += report.routes_requoted;
            stats.probes_quoted += report.probe_routes_quoted;
            stats.quote_exec_delta += report
                .quote_cache
                .executed
                .saturating_sub(before_cache.executed);

            let full_start = Instant::now();
            match searcher.find_routes_parallel(&request, cache, parallel_config(config)) {
                Ok(routes) => {
                    stats.full_recompute.push(full_start.elapsed());
                    stats.ok += 1;
                    if routes.first() != report.best.as_ref() {
                        stats.divergences += 1;
                    }
                    stats.sample.get_or_insert_with(|| {
                        format!(
                            "{} affected={:?}, requoted={}, probes={}, exec_delta={}",
                            route.label,
                            affected_pool,
                            report.routes_requoted,
                            report.probe_routes_quoted,
                            report
                                .quote_cache
                                .executed
                                .saturating_sub(before_cache.executed)
                        )
                    });
                }
                Err(error) => {
                    stats.sample_error.get_or_insert_with(|| {
                        format!("{} full recompute failed: {error}", route.label)
                    });
                }
            }
        }
    }
    stats
}

fn route_request(
    route: &BenchRoute,
    search_config: SearchConfig,
    sim_config: SimConfig,
) -> RouteRequest {
    RouteRequest::new(
        route.token_in.address,
        route.token_out.address,
        amount_for(route.token_in.decimals, route.amount_units),
    )
    .with_config(search_config)
    .with_sim_config(sim_config)
}

fn print_variant_report(label: &str, routes: &[BenchRoute], stats: &VariantStats) {
    println!("variant[{label}]:");
    for (route_label, route_stats) in &stats.routes {
        let route = routes
            .iter()
            .find(|candidate| candidate.label == *route_label)
            .expect("route stats label must match route");
        println!(
            "  route[{route_label}]: runs={}, ok={}, errors={}, routes_observed={}, improvements_after_heuristic={}, heuristic_divergences={}",
            route_stats.ok + route_stats.errors,
            route_stats.ok,
            route_stats.errors,
            route_stats.routes_observed,
            route_stats.improvements_after_heuristic,
            route_stats.heuristic_divergences
        );
        println!(
            "  route[{route_label}] time_to_first_quote: p50={:?}, p95={:?}, worst={:?}",
            percentile_duration(&route_stats.first_quote, 50),
            percentile_duration(&route_stats.first_quote, 95),
            max_duration(&route_stats.first_quote)
        );
        println!(
            "  route[{route_label}] time_to_best: p50={:?}, p95={:?}, worst={:?}",
            percentile_duration(&route_stats.best_found, 50),
            percentile_duration(&route_stats.best_found, 95),
            max_duration(&route_stats.best_found)
        );
        println!(
            "  route[{route_label}] heuristic_done: p50={:?}, p95={:?}, worst={:?}",
            percentile_duration(&route_stats.heuristic_done, 50),
            percentile_duration(&route_stats.heuristic_done, 95),
            max_duration(&route_stats.heuristic_done)
        );
        println!(
            "  route[{route_label}] exhaustive_done: p50={:?}, p95={:?}, worst={:?}",
            percentile_duration(&route_stats.exhaustive_done, 50),
            percentile_duration(&route_stats.exhaustive_done, 95),
            max_duration(&route_stats.exhaustive_done)
        );
        println!(
            "  route[{route_label}] first_to_best_gap_bps: p50={:.4}, p95={:.4}, worst={:.4}",
            percentile_f64(&route_stats.first_to_best_gap_bps, 50),
            percentile_f64(&route_stats.first_to_best_gap_bps, 95),
            max_f64(&route_stats.first_to_best_gap_bps)
        );
        if let Some(best) = &route_stats.sample_best {
            let first = route_stats
                .first_output
                .first()
                .copied()
                .unwrap_or(best.amount_out);
            println!(
                "  route[{route_label}] sample_output: first={} {}, best={} {}, best_hops={}",
                format_units(first, route.token_out.decimals),
                route.token_out.symbol,
                format_units(best.amount_out, route.token_out.decimals),
                route.token_out.symbol,
                best.path.len()
            );
        }
        println!(
            "  route[{route_label}] quote_cache: executed={}, failed={}, hits={}, misses={}, waits={}",
            route_stats.quote_cache.executed,
            route_stats.quote_cache.failed,
            route_stats.quote_cache.hits,
            route_stats.quote_cache.misses,
            route_stats.quote_cache.waits
        );
        println!(
            "  route[{route_label}] heuristic_stats: fast_lane_routes={}, fast_lane_quotes={}, target_first_groups={}, prefix_dominated_states={}, ordered_groups={}, pruned_edges={}, upper_bound_pruned_prefixes={}, protocol_ranked_edges={}, liquidity_ranked_branch_groups={}",
            route_stats.liquidity_pruning.fast_lane_routes,
            route_stats.liquidity_pruning.fast_lane_quotes,
            route_stats.liquidity_pruning.target_first_groups,
            route_stats.liquidity_pruning.prefix_dominated_states,
            route_stats.liquidity_pruning.ordered_groups,
            route_stats.liquidity_pruning.pruned_edges,
            route_stats.liquidity_pruning.upper_bound_pruned_prefixes,
            route_stats.liquidity_pruning.protocol_ranked_edges,
            route_stats.liquidity_pruning.liquidity_ranked_branch_groups
        );
        if let Some(error) = &route_stats.sample_error {
            println!("  route[{route_label}] sample_error: {error}");
        }
    }
}

fn print_incremental_report(label: &str, routes: &[BenchRoute], stats: &IncrementalStats) {
    println!(
        "{label}: route_cases={}, runs_per_route={}, ok={}, session_errors={}, refreshes={}, divergences={}, routes_requoted={}, probes_quoted={}, quote_exec_delta={}",
        routes.len(),
        stats
            .refreshes
            .checked_div(routes.len())
            .unwrap_or_default(),
        stats.ok,
        stats.session_errors,
        stats.refreshes,
        stats.divergences,
        stats.routes_requoted,
        stats.probes_quoted,
        stats.quote_exec_delta
    );
    println!(
        "{label} session_start: p50={:?}, p95={:?}, worst={:?}",
        percentile_duration(&stats.session_start, 50),
        percentile_duration(&stats.session_start, 95),
        max_duration(&stats.session_start)
    );
    println!(
        "{label} refresh_affected: p50={:?}, p95={:?}, worst={:?}",
        percentile_duration(&stats.refresh, 50),
        percentile_duration(&stats.refresh, 95),
        max_duration(&stats.refresh)
    );
    println!(
        "{label} full_recompute: p50={:?}, p95={:?}, worst={:?}",
        percentile_duration(&stats.full_recompute, 50),
        percentile_duration(&stats.full_recompute, 95),
        max_duration(&stats.full_recompute)
    );
    if let Some(sample) = &stats.sample {
        println!("{label} sample: {sample}");
    }
    if let Some(error) = &stats.sample_error {
        println!("{label} sample_error: {error}");
    }
}

fn token_universe() -> Vec<TokenInfo> {
    vec![weth(), usdc(), usdt(), dai(), wbtc(), link(), uni(), aave()]
}

fn connector_tokens() -> Vec<Address> {
    vec![WETH, USDC, USDT, DAI, WBTC]
}

fn benchmark_routes() -> Vec<BenchRoute> {
    vec![
        BenchRoute {
            label: "10 WETH -> USDC",
            token_in: weth(),
            token_out: usdc(),
            amount_units: 10,
        },
        BenchRoute {
            label: "100 LINK -> AAVE",
            token_in: link(),
            token_out: aave(),
            amount_units: 100,
        },
        BenchRoute {
            label: "1000 DAI -> UNI",
            token_in: dai(),
            token_out: uni(),
            amount_units: 1_000,
        },
    ]
}

fn weth() -> TokenInfo {
    TokenInfo {
        symbol: "WETH",
        address: WETH,
        decimals: 18,
    }
}

fn usdc() -> TokenInfo {
    TokenInfo {
        symbol: "USDC",
        address: USDC,
        decimals: 6,
    }
}

fn usdt() -> TokenInfo {
    TokenInfo {
        symbol: "USDT",
        address: USDT,
        decimals: 6,
    }
}

fn dai() -> TokenInfo {
    TokenInfo {
        symbol: "DAI",
        address: DAI,
        decimals: 18,
    }
}

fn wbtc() -> TokenInfo {
    TokenInfo {
        symbol: "WBTC",
        address: WBTC,
        decimals: 8,
    }
}

fn link() -> TokenInfo {
    TokenInfo {
        symbol: "LINK",
        address: LINK,
        decimals: 18,
    }
}

fn uni() -> TokenInfo {
    TokenInfo {
        symbol: "UNI",
        address: UNI,
        decimals: 18,
    }
}

fn aave() -> TokenInfo {
    TokenInfo {
        symbol: "AAVE",
        address: AAVE,
        decimals: 18,
    }
}

fn amount_for(decimals: u8, whole_units: u64) -> U256 {
    U256::from(whole_units) * U256::from(10_u64).pow(U256::from(decimals))
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

fn format_units(amount: U256, decimals: u8) -> String {
    let raw = amount.to_string();
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return raw;
    }
    let formatted = if raw.len() <= decimals {
        let zeros = "0".repeat(decimals - raw.len());
        format!("0.{zeros}{raw}")
    } else {
        let split = raw.len() - decimals;
        format!("{}.{}", &raw[..split], &raw[split..])
    };
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn amount_to_f64(amount: U256, decimals: u8) -> f64 {
    format_units(amount, decimals).parse::<f64>().unwrap_or(0.0)
}

fn gap_bps(first: U256, best: U256, decimals: u8) -> f64 {
    let first = amount_to_f64(first, decimals);
    let best = amount_to_f64(best, decimals);
    if best <= 0.0 || first >= best {
        return 0.0;
    }
    ((best - first) / best) * 10_000.0
}

fn percentile_duration(values: &[Duration], pct: usize) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[percentile_index(sorted.len(), pct)]
}

fn max_duration(values: &[Duration]) -> Duration {
    values.iter().max().copied().unwrap_or_default()
}

fn percentile_f64(values: &[f64], pct: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[percentile_index(sorted.len(), pct)]
}

fn max_f64(values: &[f64]) -> f64 {
    values.iter().copied().reduce(f64::max).unwrap_or(0.0)
}

fn percentile_index(len: usize, pct: usize) -> usize {
    if len <= 1 {
        return 0;
    }
    ((len - 1) * pct.min(100)) / 100
}

fn add_quote_cache_stats(total: &mut QuoteCacheStats, next: QuoteCacheStats) {
    total.hits += next.hits;
    total.misses += next.misses;
    total.waits += next.waits;
    total.executed += next.executed;
    total.failed += next.failed;
}

fn add_liquidity_stats(total: &mut LiquidityPruneStats, next: LiquidityPruneStats) {
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

fn rpc_url_from_env() -> Option<String> {
    [
        "E2E_RPC_URL",
        "ETHEREUM_RPC_URL",
        "MAINNET_RPC_URL",
        "ETH_RPC_URL",
        "RPC_URL",
    ]
    .into_iter()
    .find_map(|key| env::var(key).ok().filter(|value| !value.trim().is_empty()))
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64_opt(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|value| value.parse().ok())
}
