//! Live quote-quality benchmark against external aggregators.
//!
//! The example cold-starts and warms a focused local search universe: Uniswap V2
//! and Uniswap V3 by default. It then starts LI.FI and 1inch quote requests
//! in parallel with the local streaming search. External APIs quote current
//! chain state and do not expose a block pin, so the local cache is pinned to
//! the latest block observed before setup unless `AGG_BENCH_BLOCK` or
//! `E2E_BLOCK` is set.
//!
//! ```text
//! set -a; source .env; set +a
//! cargo run --release --example aggregator_quote_comparison
//! ```
//!
//! Useful knobs:
//!
//! ```text
//! AGG_BENCH_RUNS=3
//! AGG_BENCH_BLOCK_LAG=0
//! AGG_BENCH_MAX_HOPS=3
//! AGG_BENCH_WORKERS=0
//! AGG_BENCH_COMPLETION=heuristic    # fast_lane, heuristic, exhaustive
//! AGG_BENCH_STAGGER_QUOTES_BY_BLOCK=1
//! AGG_BENCH_SIMULATE_LOCAL_SWAP_GAS=1
//! AGG_BENCH_PRIME_SEARCHES=1
//! AGG_BENCH_PERSIST_CACHE=1
//! AGG_BENCH_CACHE_DIR=.cache/aggregator-quote-comparison
//! AGG_BENCH_TAKER=0x000000000000000000000000000000000000dEaD
//! ```

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_eips::BlockId;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, U256, address};
use alloy_provider::Provider;
use anyhow::{Context, Result, anyhow, ensure};
use evm_amm_search::{
    AmmGraph, AmmSearcher, DEMO_ROUTER, DemoRouterConfig, GraphBuildOptions, HeuristicSearchConfig,
    LiquidityIndexScope, LiquidityPruningConfig, ParallelSearchConfig, PoolLiquidityIndex,
    RouteQuote, RouteRequest, RouteSearchEvent, SearchConfig, SearchControl, SearchMode,
    StreamingSearchConfig, install_demo_router, simulate_route_gas,
};
use evm_amm_state::adapters::{
    AdapterRegistry, ColdStartOutcome, ColdStartPolicy, ConcentratedLiquidityAdapter,
    FactoryConfig, PoolDiscovery, PoolKey, PoolQuery, PoolRegistration, PoolStatus, SimConfig,
    UniswapV2Adapter, UniswapV2FactoryConfig, UniswapV3FactoryConfig,
};
use evm_fork_cache::cache::{CacheConfig, CacheSpeedMode, EvmCache, SharedMemoryCapacity};
use reqwest::{Client, Url};
use serde_json::Value;
use tokio::{task::JoinHandle, time::sleep};

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
const DEFAULT_RUNS: usize = 3;
const DEFAULT_BLOCK_LAG: u64 = 0;
const DEFAULT_MAX_HOPS: usize = 3;
const DEFAULT_WORKERS: usize = 0;
const DEFAULT_COMPLETION: BenchCompletion = BenchCompletion::Heuristic;
const DEFAULT_STAGGER_QUOTES_BY_BLOCK: bool = true;
const DEFAULT_SIMULATE_LOCAL_SWAP_GAS: bool = true;
const DEFAULT_PRIME_SEARCHES: bool = true;
const DEFAULT_PERSIST_CACHE: bool = true;
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 20;
const DEFAULT_TAKER: Address = address!("000000000000000000000000000000000000dEaD");

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
    completion: BenchCompletion,
    stagger_quotes_by_block: bool,
    simulate_local_swap_gas: bool,
    prime_searches: bool,
    persist_cache: bool,
    cache_dir: PathBuf,
    http_timeout: Duration,
    taker: Address,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchCompletion {
    FastLane,
    Heuristic,
    Exhaustive,
}

impl BenchCompletion {
    const fn label(self) -> &'static str {
        match self {
            Self::FastLane => "fast_lane",
            Self::Heuristic => "heuristic",
            Self::Exhaustive => "exhaustive",
        }
    }
}

#[derive(Clone, Debug)]
struct AggregatorKeys {
    lifi: Option<String>,
    oneinch: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProviderName {
    Lifi,
    OneInch,
}

impl ProviderName {
    const fn label(self) -> &'static str {
        match self {
            Self::Lifi => "LI.FI",
            Self::OneInch => "1inch",
        }
    }
}

#[derive(Clone, Debug)]
struct AggregatorQuote {
    provider: ProviderName,
    amount_out: U256,
    latency: Duration,
    gas: Option<u64>,
}

#[derive(Clone, Debug)]
struct AggregatorFailure {
    provider: ProviderName,
    latency: Duration,
    error: String,
}

#[derive(Clone, Debug)]
enum AggregatorResult {
    Quote(AggregatorQuote),
    Failure(AggregatorFailure),
}

#[derive(Clone, Debug)]
struct LocalQuote {
    first_amount_out: Option<U256>,
    first_latency: Option<Duration>,
    best: RouteQuote,
    best_latency: Duration,
    total_latency: Duration,
    routes_observed: usize,
    quote_executions: usize,
    quote_failures: usize,
    swap_gas: Option<LocalSwapGas>,
}

#[derive(Clone, Debug)]
struct LocalSwapGas {
    gross_amount_out: U256,
    gas_used: u64,
    gas_cost_out: Option<U256>,
    net_amount_out: Option<U256>,
    latency: Duration,
}

#[derive(Default)]
struct ProviderStats {
    quotes: usize,
    failures: usize,
    latencies: Vec<Duration>,
    diff_vs_local_bps: Vec<f64>,
    net_diff_vs_local_bps: Vec<f64>,
}

#[derive(Default)]
struct RouteStats {
    runs: usize,
    local_first_latency: Vec<Duration>,
    local_best_latency: Vec<Duration>,
    local_total_latency: Vec<Duration>,
    first_gap_bps: Vec<f64>,
    sample_best: Option<RouteQuote>,
    providers: Vec<(ProviderName, ProviderStats)>,
}

struct ComparisonContext<'a, 'search, P>
where
    P: Provider<AnyNetwork> + Sync,
{
    provider: &'a P,
    registry: &'a AdapterRegistry,
    searcher: &'a AmmSearcher<'search>,
    client: &'a Client,
    keys: &'a AggregatorKeys,
    search_config: SearchConfig,
    sim_config: SimConfig,
    gas_price_wei: Option<u128>,
    config: &'a BenchConfig,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Some(rpc_url) = rpc_url_from_env() else {
        println!(
            "aggregator_quote_comparison: set E2E_RPC_URL, ETHEREUM_RPC_URL, MAINNET_RPC_URL, ETH_RPC_URL, or RPC_URL; skipping."
        );
        return Ok(());
    };

    let config = BenchConfig::from_env()?;
    let keys = AggregatorKeys::from_env();
    let enabled_providers = keys.enabled_providers();
    if enabled_providers.is_empty() {
        println!(
            "aggregator_quote_comparison: set at least one of LIFI_API_KEY or ONEINCH_API_KEY; skipping external comparisons."
        );
    }

    let client = Client::builder()
        .user_agent("evm-amm-search-aggregator-quote-comparison/0.1")
        .timeout(config.http_timeout)
        .build()
        .context("build reqwest client")?;

    let provider = support::http_provider(&rpc_url)?;

    let latest = provider.get_block_number().await.context("latest block")?;
    let pinned = env_u64_opt("AGG_BENCH_BLOCK")
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
        "aggregator_quote_comparison: latest_block={}, pinned_block={}, runs={}, max_hops={}, workers={}, completion={}, stagger_quotes_by_block={}, simulate_local_swap_gas={}, prime_searches={}, persist_cache={}, cache_dir={}, taker={}, transport=balanced+batched+gzip",
        latest,
        pinned,
        config.runs,
        config.max_hops,
        parallel_config(&config).workers,
        config.completion.label(),
        config.stagger_quotes_by_block,
        config.simulate_local_swap_gas,
        config.prime_searches,
        config.persist_cache,
        config.cache_dir.display(),
        format_address(config.taker)
    );
    println!(
        "providers: lifi={}, 1inch={}",
        keys.lifi.is_some(),
        keys.oneinch.is_some()
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
    let discovered_pools = pools.len();
    let manual_pool_count = 0;
    pools.sort_by_key(|pool| (pool_protocol_rank(&pool.key), format!("{:?}", pool.key)));
    ensure!(
        !pools.is_empty(),
        "no pools discovered or manually registered"
    );
    println!(
        "discovery: factory_pools={}, manual_pools={}, total_pools={}, elapsed={:?}",
        discovered_pools,
        manual_pool_count,
        pools.len(),
        discovery_elapsed
    );
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
        "cold_start: ready_outcomes={}, repair_outcomes={}, registry_ready={}, registry_degraded={}, registry_other={}, elapsed={:?}",
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

    let searcher = AmmSearcher::new(&registry, &report.graph).with_liquidity_index(&liquidity);
    let routes = benchmark_routes();
    let connectors = connector_tokens();
    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);
    let search_config = search_config(config.max_hops, connectors.iter().copied());
    let gas_price_wei = if config.simulate_local_swap_gas {
        let code_hash = install_demo_router(&mut cache)?;
        let gas_price = provider
            .get_gas_price()
            .await
            .context("fetch gas price for gas-adjusted benchmark")?;
        println!(
            "demo_router: address={}, code_hash={code_hash:?}, gas_price_wei={gas_price}",
            format_address(DEMO_ROUTER)
        );
        Some(gas_price)
    } else {
        None
    };

    if config.prime_searches {
        let prime_start = Instant::now();
        let mut ok = 0;
        let mut errors = 0;
        for route in &routes {
            let request = route_request(route, search_config.clone(), sim_config);
            match searcher.stream_routes_parallel(
                &request,
                &mut cache,
                streaming_config(&config),
                |_| SearchControl::Continue,
            ) {
                Ok(report) if report.best.is_some() => ok += 1,
                Ok(_) => errors += 1,
                Err(error) => {
                    errors += 1;
                    println!("prime_search[{}] error: {error}", route.label);
                }
            }
        }
        println!(
            "prime_searches: ok={}, errors={}, elapsed={:?}",
            ok,
            errors,
            prime_start.elapsed()
        );
    }

    let mut all_stats = Vec::new();
    let comparison = ComparisonContext {
        provider: provider.as_ref(),
        registry: &registry,
        searcher: &searcher,
        client: &client,
        keys: &keys,
        search_config: search_config.clone(),
        sim_config,
        gas_price_wei,
        config: &config,
    };
    let mut quote_block_cursor = None;
    for route in &routes {
        let stats =
            run_route_comparison(&comparison, &mut cache, route, &mut quote_block_cursor).await;
        print_route_report(route, &stats);
        all_stats.push((route.label, stats));
    }

    print_summary(&routes, &all_stats);

    if config.persist_cache {
        let flush_start = Instant::now();
        cache.flush().context("flush warmed cache")?;
        println!("cache_flush: elapsed={:?}", flush_start.elapsed());
    }

    Ok(())
}

async fn run_route_comparison<P>(
    comparison: &ComparisonContext<'_, '_, P>,
    cache: &mut EvmCache,
    route: &BenchRoute,
    quote_block_cursor: &mut Option<u64>,
) -> RouteStats
where
    P: Provider<AnyNetwork> + Sync,
{
    let mut stats = RouteStats {
        providers: comparison
            .keys
            .enabled_providers()
            .into_iter()
            .map(|provider| (provider, ProviderStats::default()))
            .collect(),
        ..Default::default()
    };

    for run in 0..comparison.config.runs {
        stats.runs += 1;
        let quote_block = if comparison.keys.enabled_providers().is_empty() {
            None
        } else {
            match wait_for_quote_block(
                comparison.provider,
                quote_block_cursor,
                comparison.config.stagger_quotes_by_block,
            )
            .await
            {
                Ok(block) => block,
                Err(error) => {
                    println!(
                        "run[{run}] {} external quote block wait failed; continuing unpaced: {error}",
                        route.label
                    );
                    None
                }
            }
        };
        let mut tasks =
            spawn_aggregator_quotes(comparison.client, comparison.keys, route, comparison.config);
        let local = run_local_search(
            comparison.searcher,
            comparison.registry,
            cache,
            route,
            comparison.search_config.clone(),
            comparison.sim_config,
            comparison.config,
            comparison.gas_price_wei,
        );

        let aggregator_results = collect_aggregator_results(&mut tasks).await;
        match local {
            Ok(local) => {
                if let Some(first_latency) = local.first_latency {
                    stats.local_first_latency.push(first_latency);
                }
                stats.local_best_latency.push(local.best_latency);
                stats.local_total_latency.push(local.total_latency);
                let first_amount = local.first_amount_out.unwrap_or(local.best.amount_out);
                stats.first_gap_bps.push(gap_bps(
                    first_amount,
                    local.best.amount_out,
                    route.token_out.decimals,
                ));
                stats.sample_best.get_or_insert_with(|| local.best.clone());

                println!(
                    "run[{run}] {} local: quote_block={}, first={} {} in {:?}, best={} {} in {:?}, total={:?}, routes_observed={}, quote_exec={}, quote_failures={}, best_hops={}",
                    route.label,
                    quote_block
                        .map(|block| block.to_string())
                        .unwrap_or_else(|| "n/a".to_owned()),
                    format_units(first_amount, route.token_out.decimals),
                    route.token_out.symbol,
                    local.first_latency.unwrap_or_default(),
                    format_units(local.best.amount_out, route.token_out.decimals),
                    route.token_out.symbol,
                    local.best_latency,
                    local.total_latency,
                    local.routes_observed,
                    local.quote_executions,
                    local.quote_failures,
                    local.best.path.len()
                );
                if let Some(gas) = &local.swap_gas {
                    println!(
                        "run[{run}] {} local_gas: router_out={} {}, quote_gap={:+.4} bps, gas={}, gas_cost={} {}, net={} {}, latency={:?}",
                        route.label,
                        format_units(gas.gross_amount_out, route.token_out.decimals),
                        route.token_out.symbol,
                        signed_diff_bps(
                            gas.gross_amount_out,
                            local.best.amount_out,
                            route.token_out.decimals,
                        ),
                        gas.gas_used,
                        gas.gas_cost_out
                            .map(|amount| format_units(amount, route.token_out.decimals))
                            .unwrap_or_else(|| "n/a".to_owned()),
                        route.token_out.symbol,
                        gas.net_amount_out
                            .map(|amount| format_units(amount, route.token_out.decimals))
                            .unwrap_or_else(|| "n/a".to_owned()),
                        route.token_out.symbol,
                        gas.latency
                    );
                }

                for result in aggregator_results {
                    match result {
                        AggregatorResult::Quote(quote) => {
                            let diff = signed_diff_bps(
                                quote.amount_out,
                                local.best.amount_out,
                                route.token_out.decimals,
                            );
                            let net_diff = provider_net_diff_bps(&quote, &local, route);
                            update_provider_stats(&mut stats, quote.provider, |provider_stats| {
                                provider_stats.quotes += 1;
                                provider_stats.latencies.push(quote.latency);
                                provider_stats.diff_vs_local_bps.push(diff);
                                if let Some(net_diff) = net_diff {
                                    provider_stats.net_diff_vs_local_bps.push(net_diff);
                                }
                            });
                            println!(
                                "run[{run}] {} {}: out={} {}, diff_vs_local={:+.4} bps, net_diff_vs_local={}, latency={:?}, gas={}",
                                route.label,
                                quote.provider.label(),
                                format_units(quote.amount_out, route.token_out.decimals),
                                route.token_out.symbol,
                                diff,
                                net_diff
                                    .map(|bps| format!("{bps:+.4} bps"))
                                    .unwrap_or_else(|| "n/a".to_owned()),
                                quote.latency,
                                quote
                                    .gas
                                    .map(|gas| gas.to_string())
                                    .unwrap_or_else(|| "n/a".to_owned())
                            );
                        }
                        AggregatorResult::Failure(failure) => {
                            update_provider_stats(&mut stats, failure.provider, |provider_stats| {
                                provider_stats.failures += 1;
                                provider_stats.latencies.push(failure.latency);
                            });
                            println!(
                                "run[{run}] {} {}: error after {:?}: {}",
                                route.label,
                                failure.provider.label(),
                                failure.latency,
                                failure.error
                            );
                        }
                    }
                }
            }
            Err(error) => {
                println!("run[{run}] {} local error: {error}", route.label);
                for result in aggregator_results {
                    if let AggregatorResult::Failure(failure) = result {
                        update_provider_stats(&mut stats, failure.provider, |provider_stats| {
                            provider_stats.failures += 1;
                            provider_stats.latencies.push(failure.latency);
                        });
                    }
                }
            }
        }
    }

    stats
}

async fn wait_for_quote_block<P>(
    provider: &P,
    quote_block_cursor: &mut Option<u64>,
    enabled: bool,
) -> Result<Option<u64>>
where
    P: Provider<AnyNetwork> + Sync,
{
    if !enabled {
        return Ok(None);
    }

    let mut current = provider
        .get_block_number()
        .await
        .context("get external quote pacing block")?;
    while quote_block_cursor.is_some_and(|last| current <= last) {
        sleep(Duration::from_secs(2)).await;
        current = provider
            .get_block_number()
            .await
            .context("get external quote pacing block")?;
    }
    *quote_block_cursor = Some(current);
    Ok(Some(current))
}

#[allow(clippy::too_many_arguments)]
fn run_local_search(
    searcher: &AmmSearcher<'_>,
    registry: &AdapterRegistry,
    cache: &mut EvmCache,
    route: &BenchRoute,
    search_config: SearchConfig,
    sim_config: SimConfig,
    config: &BenchConfig,
    gas_price_wei: Option<u128>,
) -> Result<LocalQuote> {
    let request = route_request(route, search_config.clone(), sim_config);
    let started = Instant::now();
    let mut first_latency = None;
    let mut first_amount_out = None;
    let mut best_events = Vec::<(Duration, U256)>::new();

    let report = searcher
        .stream_routes_parallel(&request, cache, streaming_config(config), |event| {
            match event {
                RouteSearchEvent::BestUpdated { quote, .. } => {
                    first_latency.get_or_insert_with(|| started.elapsed());
                    first_amount_out.get_or_insert(quote.amount_out);
                    best_events.push((started.elapsed(), quote.amount_out));
                }
                RouteSearchEvent::RouteFound { quote, .. } => {
                    first_latency.get_or_insert_with(|| started.elapsed());
                    first_amount_out.get_or_insert(quote.amount_out);
                }
                _ => {}
            }
            SearchControl::Continue
        })
        .map_err(|error| anyhow!(error))?;
    let total_latency = started.elapsed();
    let best = report
        .best
        .ok_or_else(|| anyhow!("local search produced no best route"))?;
    let best_latency = best_events
        .iter()
        .find_map(|(elapsed, amount)| (*amount == best.amount_out).then_some(*elapsed))
        .unwrap_or(total_latency);
    let swap_gas = if config.simulate_local_swap_gas {
        match simulate_local_swap_gas(
            searcher,
            registry,
            cache,
            route,
            &best,
            search_config,
            sim_config,
            config,
            gas_price_wei,
        ) {
            Ok(gas) => Some(gas),
            Err(error) => {
                println!(
                    "run_local_search[{}] local gas simulation skipped: {error}",
                    route.label
                );
                None
            }
        }
    } else {
        None
    };

    Ok(LocalQuote {
        first_amount_out,
        first_latency,
        best,
        best_latency,
        total_latency,
        routes_observed: report.routes_observed,
        quote_executions: report.quote_cache.executed,
        quote_failures: report.quote_cache.failed,
        swap_gas,
    })
}

#[allow(clippy::too_many_arguments)]
fn simulate_local_swap_gas(
    searcher: &AmmSearcher<'_>,
    registry: &AdapterRegistry,
    cache: &mut EvmCache,
    route: &BenchRoute,
    quote: &RouteQuote,
    search_config: SearchConfig,
    sim_config: SimConfig,
    config: &BenchConfig,
    gas_price_wei: Option<u128>,
) -> Result<LocalSwapGas> {
    let started = Instant::now();
    let estimate = simulate_route_gas(
        registry,
        cache,
        quote,
        DemoRouterConfig {
            caller: config.taker,
            ..DemoRouterConfig::default()
        },
        gas_price_wei,
    )?;

    let gas_cost_out = estimate
        .gas_cost_native
        .map(|gas_cost_wei| {
            quote_weth_cost_in_output_token(
                searcher,
                cache,
                route.token_out,
                gas_cost_wei,
                search_config,
                sim_config,
            )
        })
        .transpose()?;
    let net_amount_out =
        gas_cost_out.map(|gas_cost| sub_floor(estimate.gross_amount_out, gas_cost));

    Ok(LocalSwapGas {
        gross_amount_out: estimate.gross_amount_out,
        gas_used: estimate.gas_used,
        gas_cost_out,
        net_amount_out,
        latency: started.elapsed(),
    })
}

fn spawn_aggregator_quotes(
    client: &Client,
    keys: &AggregatorKeys,
    route: &BenchRoute,
    config: &BenchConfig,
) -> Vec<(ProviderName, JoinHandle<AggregatorResult>)> {
    let mut tasks = Vec::new();
    let sell_amount = amount_for(route.token_in.decimals, route.amount_units);

    if let Some(key) = &keys.lifi {
        let client = client.clone();
        let key = key.clone();
        let route = *route;
        let taker = config.taker;
        tasks.push((
            ProviderName::Lifi,
            tokio::spawn(async move { quote_lifi(client, key, route, sell_amount, taker).await }),
        ));
    }
    if let Some(key) = &keys.oneinch {
        let client = client.clone();
        let key = key.clone();
        let route = *route;
        tasks.push((
            ProviderName::OneInch,
            tokio::spawn(async move { quote_1inch(client, key, route, sell_amount).await }),
        ));
    }

    tasks
}

async fn collect_aggregator_results(
    tasks: &mut Vec<(ProviderName, JoinHandle<AggregatorResult>)>,
) -> Vec<AggregatorResult> {
    let mut results = Vec::with_capacity(tasks.len());
    for (provider, task) in tasks.drain(..) {
        match task.await {
            Ok(result) => results.push(result),
            Err(error) => results.push(AggregatorResult::Failure(AggregatorFailure {
                provider,
                latency: Duration::ZERO,
                error: format!("join failed: {error}"),
            })),
        }
    }
    results
}

async fn quote_lifi(
    client: Client,
    api_key: String,
    route: BenchRoute,
    sell_amount: U256,
    from_address: Address,
) -> AggregatorResult {
    let provider = ProviderName::Lifi;
    let started = Instant::now();
    let base_url =
        env::var("LIFI_QUOTE_URL").unwrap_or_else(|_| "https://li.quest/v1/quote".to_owned());
    let response = async {
        let url = Url::parse_with_params(
            &base_url,
            [
                ("fromChain", "1".to_owned()),
                ("toChain", "1".to_owned()),
                ("fromToken", format_address(route.token_in.address)),
                ("toToken", format_address(route.token_out.address)),
                ("fromAmount", sell_amount.to_string()),
                ("fromAddress", format_address(from_address)),
                ("slippage", "0.005".to_owned()),
                ("integrator", "evm-amm-search-bench".to_owned()),
            ],
        )
        .context("build LI.FI quote URL")?;
        let response = client
            .get(url)
            .header("x-lifi-api-key", api_key)
            .header("accept", "application/json")
            .send()
            .await
            .context("send LI.FI quote request")?;
        decode_json_response(response).await
    }
    .await;

    match response.and_then(|json| {
        parse_amount_field(&json, &["estimate", "toAmount"]).map(|amount| (json, amount))
    }) {
        Ok((json, amount_out)) => AggregatorResult::Quote(AggregatorQuote {
            provider,
            amount_out,
            latency: started.elapsed(),
            gas: parse_lifi_gas(&json),
        }),
        Err(error) => AggregatorResult::Failure(AggregatorFailure {
            provider,
            latency: started.elapsed(),
            error: error.to_string(),
        }),
    }
}

async fn quote_1inch(
    client: Client,
    api_key: String,
    route: BenchRoute,
    sell_amount: U256,
) -> AggregatorResult {
    let provider = ProviderName::OneInch;
    let started = Instant::now();
    let base_url = env::var("ONEINCH_QUOTE_URL")
        .unwrap_or_else(|_| "https://api.1inch.dev/swap/v6.1/1/quote".to_owned());
    let response = async {
        let url = Url::parse_with_params(
            &base_url,
            [
                ("src", format_address(route.token_in.address)),
                ("dst", format_address(route.token_out.address)),
                ("amount", sell_amount.to_string()),
                ("includeTokensInfo", "false".to_owned()),
                ("includeProtocols", "false".to_owned()),
                ("includeGas", "true".to_owned()),
            ],
        )
        .context("build 1inch quote URL")?;
        let response = client
            .get(url)
            .bearer_auth(api_key)
            .header("accept", "application/json")
            .send()
            .await
            .context("send 1inch quote request")?;
        decode_json_response(response).await
    }
    .await;

    match response
        .and_then(|json| parse_amount_field(&json, &["dstAmount"]).map(|amount| (json, amount)))
    {
        Ok((json, amount_out)) => AggregatorResult::Quote(AggregatorQuote {
            provider,
            amount_out,
            latency: started.elapsed(),
            gas: parse_u64_field(&json, &["gas"]),
        }),
        Err(error) => AggregatorResult::Failure(AggregatorFailure {
            provider,
            latency: started.elapsed(),
            error: error.to_string(),
        }),
    }
}

async fn decode_json_response(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let text = response.text().await.context("read quote response")?;
    let json: Value = serde_json::from_str(&text).with_context(|| {
        let snippet = text.chars().take(400).collect::<String>();
        format!("decode JSON response, status={status}, body={snippet}")
    })?;
    if !status.is_success() {
        return Err(anyhow!(
            "HTTP {status}: {}",
            compact_json(&json).chars().take(500).collect::<String>()
        ));
    }
    Ok(json)
}

fn parse_amount_field(json: &Value, path: &[&str]) -> Result<U256> {
    let value =
        json_path(json, path).ok_or_else(|| anyhow!("missing amount field {}", path.join(".")))?;
    let text = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|value| value.to_string()))
        .ok_or_else(|| anyhow!("amount field {} was not a string/u64", path.join(".")))?;
    U256::from_str_radix(&text, 10)
        .with_context(|| format!("parse amount field {}", path.join(".")))
}

fn parse_u64_field(json: &Value, path: &[&str]) -> Option<u64> {
    let value = json_path(json, path)?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn parse_lifi_gas(json: &Value) -> Option<u64> {
    json_path(json, &["estimate", "gasCosts"])?
        .as_array()?
        .iter()
        .filter_map(|entry| parse_u64_field(entry, &["estimate"]))
        .sum::<u64>()
        .checked_add(0)
}

fn json_path<'a>(json: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = json;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn compact_json(json: &Value) -> String {
    serde_json::to_string(json).unwrap_or_else(|_| "<unprintable json>".to_owned())
}

impl BenchConfig {
    fn from_env() -> Result<Self> {
        let cache_dir = env::var("AGG_BENCH_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".cache/aggregator-quote-comparison"));
        Ok(Self {
            runs: env_usize("AGG_BENCH_RUNS", DEFAULT_RUNS),
            block_lag: env_u64("AGG_BENCH_BLOCK_LAG", DEFAULT_BLOCK_LAG),
            max_hops: env_usize("AGG_BENCH_MAX_HOPS", DEFAULT_MAX_HOPS),
            workers: env_usize("AGG_BENCH_WORKERS", DEFAULT_WORKERS),
            completion: env_completion("AGG_BENCH_COMPLETION", DEFAULT_COMPLETION),
            stagger_quotes_by_block: env_bool(
                "AGG_BENCH_STAGGER_QUOTES_BY_BLOCK",
                DEFAULT_STAGGER_QUOTES_BY_BLOCK,
            ),
            simulate_local_swap_gas: env_bool(
                "AGG_BENCH_SIMULATE_LOCAL_SWAP_GAS",
                DEFAULT_SIMULATE_LOCAL_SWAP_GAS,
            ),
            prime_searches: env_bool("AGG_BENCH_PRIME_SEARCHES", DEFAULT_PRIME_SEARCHES),
            persist_cache: env_bool("AGG_BENCH_PERSIST_CACHE", DEFAULT_PERSIST_CACHE),
            cache_dir,
            http_timeout: Duration::from_secs(env_u64(
                "AGG_BENCH_HTTP_TIMEOUT_SECS",
                DEFAULT_HTTP_TIMEOUT_SECS,
            )),
            taker: env_address("AGG_BENCH_TAKER")?.unwrap_or(DEFAULT_TAKER),
        })
    }
}

impl AggregatorKeys {
    fn from_env() -> Self {
        Self {
            lifi: env_nonempty("LIFI_API_KEY"),
            oneinch: env_nonempty("ONEINCH_API_KEY"),
        }
    }

    fn enabled_providers(&self) -> Vec<ProviderName> {
        let mut providers = Vec::new();
        if self.lifi.is_some() {
            providers.push(ProviderName::Lifi);
        }
        if self.oneinch.is_some() {
            providers.push(ProviderName::OneInch);
        }
        providers
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

fn search_config(max_hops: usize, connectors: impl IntoIterator<Item = Address>) -> SearchConfig {
    let heuristic = HeuristicSearchConfig::balanced()
        .with_finalist_simulation(true, 16)
        .with_auto_connectors(8, 4);
    SearchConfig::default()
        .with_hops(1, max_hops)
        .with_connector_tokens(connectors)
        .with_mode(SearchMode::Heuristic(heuristic))
        .with_liquidity_pruning(LiquidityPruningConfig::enabled())
}

fn parallel_config(config: &BenchConfig) -> ParallelSearchConfig {
    let workers = if config.workers == 0 {
        std::thread::available_parallelism().map_or(1, usize::from)
    } else {
        config.workers
    };
    ParallelSearchConfig::default().with_workers(workers)
}

fn streaming_config(config: &BenchConfig) -> StreamingSearchConfig {
    let streaming = StreamingSearchConfig::default()
        .with_parallel(parallel_config(config))
        .with_top_k(1);
    match config.completion {
        BenchCompletion::FastLane => streaming.fast_lane_only(),
        BenchCompletion::Heuristic => streaming.heuristic_only(),
        BenchCompletion::Exhaustive => streaming.exhaustive(),
    }
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

fn quote_weth_cost_in_output_token(
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    token_out: TokenInfo,
    gas_cost_wei: U256,
    search_config: SearchConfig,
    sim_config: SimConfig,
) -> Result<U256> {
    if gas_cost_wei.is_zero() {
        return Ok(U256::ZERO);
    }
    if token_out.address == WETH {
        return Ok(gas_cost_wei);
    }

    let request = RouteRequest::new(WETH, token_out.address, gas_cost_wei)
        .with_config(search_config)
        .with_sim_config(sim_config);
    let quote = searcher
        .find_best_route(&request, cache)
        .map_err(|error| anyhow!(error))?;
    Ok(quote.amount_out)
}

fn provider_net_diff_bps(
    quote: &AggregatorQuote,
    local: &LocalQuote,
    route: &BenchRoute,
) -> Option<f64> {
    let local_gas = local.swap_gas.as_ref()?;
    let local_net = local_gas.net_amount_out?;
    let local_gas_cost_out = local_gas.gas_cost_out?;
    let provider_gas = quote.gas?;
    let provider_gas_cost_out = scale_amount(local_gas_cost_out, provider_gas, local_gas.gas_used)?;
    let provider_net = sub_floor(quote.amount_out, provider_gas_cost_out);
    Some(signed_diff_bps(
        provider_net,
        local_net,
        route.token_out.decimals,
    ))
}

fn scale_amount(amount: U256, numerator: u64, denominator: u64) -> Option<U256> {
    (denominator != 0).then(|| amount * U256::from(numerator) / U256::from(denominator))
}

fn sub_floor(lhs: U256, rhs: U256) -> U256 {
    if lhs > rhs { lhs - rhs } else { U256::ZERO }
}

fn update_provider_stats(
    stats: &mut RouteStats,
    provider: ProviderName,
    update: impl FnOnce(&mut ProviderStats),
) {
    if let Some((_, provider_stats)) = stats
        .providers
        .iter_mut()
        .find(|(candidate, _)| *candidate == provider)
    {
        update(provider_stats);
    }
}

fn print_route_report(route: &BenchRoute, stats: &RouteStats) {
    println!("route_summary[{}]: runs={}", route.label, stats.runs);
    println!(
        "route_summary[{}] local_first_latency: p50={:?}, p95={:?}, worst={:?}",
        route.label,
        percentile_duration(&stats.local_first_latency, 50),
        percentile_duration(&stats.local_first_latency, 95),
        max_duration(&stats.local_first_latency)
    );
    println!(
        "route_summary[{}] local_best_latency: p50={:?}, p95={:?}, worst={:?}",
        route.label,
        percentile_duration(&stats.local_best_latency, 50),
        percentile_duration(&stats.local_best_latency, 95),
        max_duration(&stats.local_best_latency)
    );
    println!(
        "route_summary[{}] local_total_latency: p50={:?}, p95={:?}, worst={:?}",
        route.label,
        percentile_duration(&stats.local_total_latency, 50),
        percentile_duration(&stats.local_total_latency, 95),
        max_duration(&stats.local_total_latency)
    );
    println!(
        "route_summary[{}] first_gap_bps: p50={:.4}, p95={:.4}, worst={:.4}",
        route.label,
        percentile_f64(&stats.first_gap_bps, 50),
        percentile_f64(&stats.first_gap_bps, 95),
        max_f64(&stats.first_gap_bps)
    );
    if let Some(best) = &stats.sample_best {
        println!(
            "route_summary[{}] sample_best: out={} {}, hops={}",
            route.label,
            format_units(best.amount_out, route.token_out.decimals),
            route.token_out.symbol,
            best.path.len()
        );
    }
    for (provider, provider_stats) in &stats.providers {
        println!(
            "route_summary[{}] {}: quotes={}, failures={}, latency_p50={:?}, latency_p95={:?}, diff_vs_local_bps_p50={}, diff_vs_local_bps_worst_abs={}, net_diff_vs_local_bps_p50={}",
            route.label,
            provider.label(),
            provider_stats.quotes,
            provider_stats.failures,
            percentile_duration(&provider_stats.latencies, 50),
            percentile_duration(&provider_stats.latencies, 95),
            format_optional_signed_bps(&provider_stats.diff_vs_local_bps, 50),
            format_optional_abs_bps(&provider_stats.diff_vs_local_bps),
            format_optional_signed_bps(&provider_stats.net_diff_vs_local_bps, 50)
        );
    }
}

fn print_summary(routes: &[BenchRoute], stats: &[(&'static str, RouteStats)]) {
    println!("comparison_summary:");
    for route in routes {
        let Some((_, route_stats)) = stats.iter().find(|(label, _)| *label == route.label) else {
            continue;
        };
        let local = route_stats
            .sample_best
            .as_ref()
            .map(|best| format_units(best.amount_out, route.token_out.decimals))
            .unwrap_or_else(|| "n/a".to_owned());
        println!(
            "  {}: local_best={} {}, local_best_p50={:?}, total_p50={:?}, first_gap_p50={:.4} bps",
            route.label,
            local,
            route.token_out.symbol,
            percentile_duration(&route_stats.local_best_latency, 50),
            percentile_duration(&route_stats.local_total_latency, 50),
            percentile_f64(&route_stats.first_gap_bps, 50)
        );
        for (provider, provider_stats) in &route_stats.providers {
            println!(
                "  {} vs {}: quotes={}, failures={}, latency_p50={:?}, diff_vs_local_bps_p50={}, net_diff_vs_local_bps_p50={}",
                route.label,
                provider.label(),
                provider_stats.quotes,
                provider_stats.failures,
                percentile_duration(&provider_stats.latencies, 50),
                format_optional_signed_bps(&provider_stats.diff_vs_local_bps, 50),
                format_optional_signed_bps(&provider_stats.net_diff_vs_local_bps, 50)
            );
        }
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

fn signed_diff_bps(candidate: U256, baseline: U256, decimals: u8) -> f64 {
    let candidate = amount_to_f64(candidate, decimals);
    let baseline = amount_to_f64(baseline, decimals);
    if baseline <= 0.0 {
        return 0.0;
    }
    ((candidate - baseline) / baseline) * 10_000.0
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

fn max_abs_f64(values: &[f64]) -> f64 {
    values
        .iter()
        .copied()
        .map(f64::abs)
        .reduce(f64::max)
        .unwrap_or(0.0)
}

fn format_optional_signed_bps(values: &[f64], pct: usize) -> String {
    if values.is_empty() {
        "n/a".to_owned()
    } else {
        format!("{:+.4}", percentile_f64(values, pct))
    }
}

fn format_optional_abs_bps(values: &[f64]) -> String {
    if values.is_empty() {
        "n/a".to_owned()
    } else {
        format!("{:.4}", max_abs_f64(values))
    }
}

fn percentile_index(len: usize, pct: usize) -> usize {
    if len <= 1 {
        return 0;
    }
    ((len - 1) * pct.min(100)) / 100
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

fn env_nonempty(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
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

fn env_completion(key: &str, default: BenchCompletion) -> BenchCompletion {
    env::var(key)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "fast" | "fast_lane" | "fast-lane" | "fastlane" => Some(BenchCompletion::FastLane),
            "heuristic" | "heuristic_only" | "heuristic-only" => Some(BenchCompletion::Heuristic),
            "exhaustive" | "full" => Some(BenchCompletion::Exhaustive),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_address(key: &str) -> Result<Option<Address>> {
    let Some(value) = env_nonempty(key) else {
        return Ok(None);
    };
    value
        .parse::<Address>()
        .map(Some)
        .with_context(|| format!("parse {key} as address"))
}

fn format_address(address: Address) -> String {
    format!("{address:#x}")
}
