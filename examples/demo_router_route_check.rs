//! Live DemoRouter execution check for generated routes.
//!
//! This example warms a small, deterministic mainnet pool universe and then
//! proves that the DemoRouter can execute generated routes, including the
//! Curve 3pool -> tricryptoUSDC-ng DAI -> WBTC shape used by the TUI.
//!
//! ```text
//! set -a; source .env; set +a
//! cargo run --release --example demo_router_route_check
//! ```

use std::{env, path::PathBuf, sync::Arc};

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, Bytes, U256, address, hex};
use alloy_provider::Provider;
use anyhow::{Context, Result, anyhow, ensure};
use evm_amm_search::{
    AmmGraph, AmmSearcher, DEMO_ROUTER, DemoRouterConfig, GraphBuildOptions, RouteQuote,
    RouteRequest, SearchConfig, demo_router_hops_for_quote, encode_demo_router_execute_calldata,
    install_demo_router, simulate_route_gas, simulate_route_prefix_gas,
};
use evm_amm_state::adapters::storage::V3StorageLayout;
use evm_amm_state::adapters::{
    AdapterRegistry, ColdStartOutcome, ColdStartPolicy, ConcentratedLiquidityAdapter, CurveAdapter,
    CurveMetadata, CurveVariant, PoolKey, PoolRegistration, ProtocolMetadata, SimConfig,
    UniswapV2Adapter, UniswapV2Metadata, V3Metadata,
};
use evm_fork_cache::cache::{
    CacheConfig, CacheSpeedMode, EvmCache, SharedMemoryCapacity, TxConfig,
};
use evm_fork_cache::{CallTrace, CallTracer};
use revm::context::result::ExecutionResult;

mod support;

const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const USDT: Address = address!("dAC17F958D2ee523a2206206994597C13D831ec7");
const DAI: Address = address!("6B175474E89094C44Da98b954EedeAC495271d0F");
const WBTC: Address = address!("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599");

const V2_ROUTER_02: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const V3_QUOTER_V2: Address = address!("61fFE014bA17989E743c5F6cB21bF9697530B21e");
const V2_USDC_WETH_PAIR: Address = address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
const V3_USDC_WETH_005: Address = address!("88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640");
const CURVE_3POOL: Address = address!("bEbc44782C7dB0a1A60Cb6Fe97d0B483032FF1C7");
const TRICRYPTO_USDC_NG: Address = address!("7F86Bf177Dd4F3494b841a37e810A34dD56c829B");

#[derive(Clone, Copy)]
struct TokenInfo {
    symbol: &'static str,
    address: Address,
    decimals: u8,
}

#[derive(Clone, Copy)]
struct CheckRoute {
    label: &'static str,
    token_in: TokenInfo,
    token_out: TokenInfo,
    amount_units: u64,
    max_hops: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let Some(rpc_url) = rpc_url_from_env() else {
        println!(
            "demo_router_route_check: set ETH_WS_URL, WS_RPC_URL, E2E_RPC_URL, ETHEREUM_RPC_URL, MAINNET_RPC_URL, ETH_RPC_URL, or RPC_URL; skipping."
        );
        return Ok(());
    };

    let provider = provider_from_rpc_url(rpc_url)?;
    let latest = provider.get_block_number().await.context("latest block")?;
    let pinned = env_u64_opt("DEMO_ROUTER_CHECK_BLOCK")
        .or_else(|| env_u64_opt("E2E_BLOCK"))
        .unwrap_or(latest);

    let mut cache = build_cache(provider.clone(), pinned).await;
    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);
    let mut registry = build_registry(sim_config)?;
    let mut pools = known_pools();

    println!(
        "demo_router_route_check: latest_block={latest}, pinned_block={pinned}, pools={}, router={DEMO_ROUTER:#x}",
        pools.len()
    );

    let outcomes = registry
        .cold_start_many(
            &mut pools,
            &mut cache,
            provider.as_ref(),
            ColdStartPolicy::Eager,
        )
        .await
        .context("cold-start demo-router pools")?;
    let ready = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                ColdStartOutcome::Ready(_) | ColdStartOutcome::ReadyWithDeferred(_, _)
            )
        })
        .count();
    ensure!(
        ready == pools.len(),
        "only {ready}/{} pools reached Ready: {outcomes:?}",
        pools.len()
    );
    for pool in pools {
        registry.register_pool(pool)?;
    }

    let graph_report = AmmGraph::from_registry(&registry, GraphBuildOptions::default());
    println!(
        "graph: indexed_pools={}, skipped_pools={}, nodes={}, edges={}",
        graph_report.indexed_pools.len(),
        graph_report.skipped_pools.len(),
        graph_report.graph.node_count(),
        graph_report.graph.edge_count()
    );

    let code_hash = install_demo_router(&mut cache).context("install DemoRouter")?;
    println!("demo_router: code_hash={code_hash:?}");

    let gas_price_wei = provider.get_gas_price().await.ok();
    let searcher = AmmSearcher::new(&registry, &graph_report.graph);
    let mut failures = 0usize;
    for route in check_routes() {
        if let Err(error) = check_route(
            &registry,
            &searcher,
            &mut cache,
            route,
            sim_config,
            gas_price_wei,
        ) {
            failures += 1;
            println!("route_check[{}] FAILED: {error:#}", route.label);
        }
    }

    ensure!(failures == 0, "{failures} route check(s) failed");
    println!("demo_router_route_check: all routes executed successfully");
    Ok(())
}

fn build_registry(sim_config: SimConfig) -> Result<AdapterRegistry> {
    let mut registry = AdapterRegistry::new().with_sim_config(sim_config);
    registry.register_adapter(Arc::new(UniswapV2Adapter::default()))?;
    registry.register_adapter(Arc::new(ConcentratedLiquidityAdapter::default()))?;
    registry.register_adapter(Arc::new(CurveAdapter::default()))?;
    Ok(registry)
}

async fn build_cache<P>(provider: Arc<P>, block: u64) -> EvmCache
where
    P: Provider<AnyNetwork> + Clone + 'static,
{
    let mut builder = EvmCache::builder(provider)
        .block(BlockId::Number(BlockNumberOrTag::Number(block)))
        .chain_id(1)
        .speed_mode(CacheSpeedMode::Fast);
    if env_bool("DEMO_ROUTER_CHECK_PERSIST_CACHE", true) {
        let cache_dir = env::var("DEMO_ROUTER_CHECK_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".cache/demo-router-route-check"));
        builder = builder
            .cache_config(CacheConfig::new(
                &cache_dir,
                1,
                Default::default(),
                Default::default(),
            ))
            .shared_memory_capacity(SharedMemoryCapacity::Auto);
    }
    builder.build().await
}

fn provider_from_rpc_url(
    rpc_url: String,
) -> Result<Arc<impl Provider<AnyNetwork> + Clone + 'static>> {
    support::http_provider(&rpc_url)
}

fn known_pools() -> Vec<PoolRegistration> {
    vec![
        PoolRegistration::new(PoolKey::UniswapV2(V2_USDC_WETH_PAIR))
            .with_state_address(V2_USDC_WETH_PAIR)
            .with_metadata(ProtocolMetadata::UniswapV2(
                UniswapV2Metadata::default()
                    .with_token0(USDC)
                    .with_token1(WETH)
                    .with_fee_bps(30),
            )),
        PoolRegistration::new(PoolKey::UniswapV3(V3_USDC_WETH_005))
            .with_state_address(V3_USDC_WETH_005)
            .with_metadata(ProtocolMetadata::UniswapV3(
                V3Metadata::default()
                    .with_token0(USDC)
                    .with_token1(WETH)
                    .with_fee(500)
                    .with_tick_spacing(10)
                    .with_storage_layout(V3StorageLayout::uniswap(10)),
            )),
        curve_pool(CURVE_3POOL, [DAI, USDC, USDT], CurveVariant::StableSwap),
        curve_pool(
            TRICRYPTO_USDC_NG,
            [USDC, WBTC, WETH],
            CurveVariant::CryptoSwapNG,
        ),
    ]
}

fn curve_pool(
    pool: Address,
    coins: impl IntoIterator<Item = Address>,
    variant: CurveVariant,
) -> PoolRegistration {
    PoolRegistration::new(PoolKey::Curve(pool))
        .with_state_address(pool)
        .with_metadata(ProtocolMetadata::Curve(
            CurveMetadata::default()
                .with_coins(coins)
                .with_variant(variant),
        ))
}

fn check_routes() -> Vec<CheckRoute> {
    vec![
        CheckRoute {
            label: "1000 DAI -> USDC",
            token_in: dai(),
            token_out: usdc(),
            amount_units: 1_000,
            max_hops: 1,
        },
        CheckRoute {
            label: "1000 USDC -> WBTC",
            token_in: usdc(),
            token_out: wbtc(),
            amount_units: 1_000,
            max_hops: 1,
        },
        CheckRoute {
            label: "1000 DAI -> WBTC",
            token_in: dai(),
            token_out: wbtc(),
            amount_units: 1_000,
            max_hops: 2,
        },
        CheckRoute {
            label: "10 WETH -> USDC",
            token_in: weth(),
            token_out: usdc(),
            amount_units: 10,
            max_hops: 1,
        },
    ]
}

fn check_route(
    registry: &AdapterRegistry,
    searcher: &AmmSearcher<'_>,
    cache: &mut EvmCache,
    route: CheckRoute,
    sim_config: SimConfig,
    gas_price_wei: Option<u128>,
) -> Result<()> {
    let request = RouteRequest::new(
        route.token_in.address,
        route.token_out.address,
        amount_for(route.token_in.decimals, route.amount_units),
    )
    .with_config(SearchConfig::default().with_hops(1, route.max_hops))
    .with_sim_config(sim_config);
    let quote = searcher
        .find_best_route(&request, cache)
        .map_err(|error| anyhow!(error))?;
    print_quote(route, &quote)?;

    let demo_hops = demo_router_hops_for_quote(registry, &quote)?;
    println!(
        "route_check[{}]: demo_router_hops={demo_hops:?}",
        route.label
    );

    for prefix_len in 1..=quote.hops.len() {
        let prefix = match simulate_route_prefix_gas(
            registry,
            cache,
            &quote,
            prefix_len,
            DemoRouterConfig::default(),
            gas_price_wei,
        ) {
            Ok(prefix) => prefix,
            Err(error) => {
                maybe_debug_prefix(registry, cache, &quote, prefix_len)?;
                return Err(error)
                    .with_context(|| format!("prefix {prefix_len}/{} failed", quote.hops.len()));
            }
        };
        if prefix.gross_amount_out.is_zero() {
            maybe_debug_prefix(registry, cache, &quote, prefix_len)?;
            return Err(anyhow!(
                "prefix {prefix_len}/{} returned zero output",
                quote.hops.len()
            ));
        }
        println!(
            "route_check[{}]: prefix {}/{} ok out={} gas={}",
            route.label,
            prefix_len,
            quote.hops.len(),
            prefix.gross_amount_out,
            prefix.gas_used
        );
    }

    let estimate = simulate_route_gas(
        registry,
        cache,
        &quote,
        DemoRouterConfig::default(),
        gas_price_wei,
    )?;
    ensure!(
        estimate.gross_amount_out > U256::ZERO,
        "full route returned zero output"
    );
    println!(
        "route_check[{}]: full ok quoted_out={} actual_out={} gas={} latency={:?}",
        route.label,
        quote.amount_out,
        estimate.gross_amount_out,
        estimate.gas_used,
        estimate.latency
    );
    Ok(())
}

fn maybe_debug_prefix(
    registry: &AdapterRegistry,
    cache: &mut EvmCache,
    quote: &RouteQuote,
    prefix_len: usize,
) -> Result<()> {
    if !env_bool("DEMO_ROUTER_CHECK_DEBUG", false) {
        return Ok(());
    }
    let hop = &quote.hops[prefix_len - 1];
    println!(
        "debug_prefix[{prefix_len}]: pool={:?} {} -> {} amount_in={}",
        hop.hop.pool,
        symbol_for(hop.hop.token_in),
        symbol_for(hop.hop.token_out),
        hop.amount_in
    );
    let Some(pool) = hop.hop.pool.address() else {
        return Ok(());
    };
    debug_router_trace(registry, cache, quote)?;
    debug_direct_curve_call(
        cache,
        pool,
        hop.hop.token_in,
        hop.hop.token_out,
        hop.amount_in,
    )
}

fn debug_router_trace(
    registry: &AdapterRegistry,
    cache: &mut EvmCache,
    quote: &RouteQuote,
) -> Result<()> {
    let token_in = quote
        .hops
        .first()
        .map(|hop| hop.hop.token_in)
        .context("empty route")?;
    let calldata = encode_demo_router_execute_calldata(registry, quote)?;
    let mut overlay = cache.mock_overlay();
    ensure!(
        overlay.mock_balance(token_in, DEMO_ROUTER, quote.amount_in)?,
        "debug trace could not mock router input balance"
    );
    let (result, tracer) = overlay.call_raw_with_inspector(
        Address::ZERO,
        DEMO_ROUTER,
        calldata,
        &TxConfig::default(),
        CallTracer::new(),
        false,
    )?;
    print_result("debug_router: result", result);
    if let Some(trace) = tracer.into_trace() {
        print_trace(&trace, 0);
    }
    Ok(())
}

fn print_trace(trace: &CallTrace, indent: usize) {
    let pad = "  ".repeat(indent);
    let selector = if trace.input.len() >= 4 {
        format!("0x{}", hex::encode(&trace.input[..4]))
    } else {
        "-".to_owned()
    };
    println!(
        "{pad}trace depth={} {:?} {} -> {} status={:?} gas={} selector={} output=0x{}",
        trace.depth,
        trace.kind,
        trace.from,
        trace.to,
        trace.status,
        trace.gas_used,
        selector,
        hex::encode(&trace.output)
    );
    for subcall in &trace.subcalls {
        print_trace(subcall, indent + 1);
    }
}

fn debug_direct_curve_call(
    cache: &mut EvmCache,
    pool: Address,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<()> {
    let mut overlay = cache.mock_overlay();
    println!(
        "debug_direct: mock_balance={}",
        overlay.mock_balance(token_in, DEMO_ROUTER, amount_in)?
    );
    println!(
        "debug_direct: mock_allowance={}",
        overlay.mock_allowance(token_in, DEMO_ROUTER, pool, amount_in)?
    );
    print_raw_u256(
        "debug_direct: token_in balance",
        overlay.call_raw(Address::ZERO, token_in, balance_of_calldata(DEMO_ROUTER))?,
    );
    print_raw_u256(
        "debug_direct: allowance",
        overlay.call_raw(
            Address::ZERO,
            token_in,
            allowance_calldata(DEMO_ROUTER, pool),
        )?,
    );
    for (label, calldata) in [
        (
            "exchange(int128,int128,uint256,uint256)",
            curve_exchange_4_calldata([0x3d, 0xf0, 0x21, 0x24], 0, 1, amount_in),
        ),
        (
            "exchange(uint256,uint256,uint256,uint256)",
            curve_exchange_4_calldata([0x5b, 0x41, 0xb9, 0x08], 0, 1, amount_in),
        ),
        (
            "exchange(uint256,uint256,uint256,uint256,bool)",
            curve_exchange_5_bool_calldata(0, 1, amount_in, false),
        ),
        (
            "exchange(uint256,uint256,uint256,uint256,address)",
            curve_exchange_5_address_calldata(0, 1, amount_in, DEMO_ROUTER),
        ),
    ] {
        print_result(
            &format!("debug_direct: {label}"),
            overlay.call_raw(DEMO_ROUTER, pool, calldata)?,
        );
        print_raw_u256(
            "debug_direct: token_out balance",
            overlay.call_raw(Address::ZERO, token_out, balance_of_calldata(DEMO_ROUTER))?,
        );
    }
    Ok(())
}

fn print_result(label: &str, result: ExecutionResult) {
    match result {
        ExecutionResult::Success {
            gas_used, output, ..
        } => println!(
            "{label}: success gas={gas_used} output=0x{}",
            hex::encode(output.into_data())
        ),
        ExecutionResult::Revert { gas_used, output } => {
            println!(
                "{label}: revert gas={gas_used} output=0x{}",
                hex::encode(output)
            )
        }
        ExecutionResult::Halt { reason, gas_used } => {
            println!("{label}: halt gas={gas_used} reason={reason:?}")
        }
    }
}

fn print_raw_u256(label: &str, result: ExecutionResult) {
    match result {
        ExecutionResult::Success { output, .. } => {
            let data = output.into_data();
            if data.len() >= 32 {
                println!("{label}: {}", U256::from_be_slice(&data[..32]));
            } else {
                println!("{label}: short output 0x{}", hex::encode(data));
            }
        }
        other => print_result(label, other),
    }
}

fn balance_of_calldata(owner: Address) -> Bytes {
    let mut out = Vec::with_capacity(36);
    out.extend_from_slice(&[0x70, 0xa0, 0x82, 0x31]);
    push_address_word(&mut out, owner);
    Bytes::from(out)
}

fn allowance_calldata(owner: Address, spender: Address) -> Bytes {
    let mut out = Vec::with_capacity(68);
    out.extend_from_slice(&[0xdd, 0x62, 0xed, 0x3e]);
    push_address_word(&mut out, owner);
    push_address_word(&mut out, spender);
    Bytes::from(out)
}

fn curve_exchange_4_calldata(selector: [u8; 4], i: u8, j: u8, amount: U256) -> Bytes {
    let mut out = Vec::with_capacity(132);
    out.extend_from_slice(&selector);
    push_u256_word(&mut out, U256::from(i));
    push_u256_word(&mut out, U256::from(j));
    push_u256_word(&mut out, amount);
    push_u256_word(&mut out, U256::ZERO);
    Bytes::from(out)
}

fn curve_exchange_5_bool_calldata(i: u8, j: u8, amount: U256, use_eth: bool) -> Bytes {
    let mut out = Vec::with_capacity(164);
    out.extend_from_slice(&[0x39, 0x47, 0x47, 0xc5]);
    push_u256_word(&mut out, U256::from(i));
    push_u256_word(&mut out, U256::from(j));
    push_u256_word(&mut out, amount);
    push_u256_word(&mut out, U256::ZERO);
    push_u256_word(&mut out, if use_eth { U256::from(1) } else { U256::ZERO });
    Bytes::from(out)
}

fn curve_exchange_5_address_calldata(i: u8, j: u8, amount: U256, recipient: Address) -> Bytes {
    let mut out = Vec::with_capacity(164);
    out.extend_from_slice(&[0xa6, 0x48, 0x33, 0xa0]);
    push_u256_word(&mut out, U256::from(i));
    push_u256_word(&mut out, U256::from(j));
    push_u256_word(&mut out, amount);
    push_u256_word(&mut out, U256::ZERO);
    push_address_word(&mut out, recipient);
    Bytes::from(out)
}

fn push_address_word(out: &mut Vec<u8>, address: Address) {
    out.extend_from_slice(&[0; 12]);
    out.extend_from_slice(address.as_slice());
}

fn push_u256_word(out: &mut Vec<u8>, value: U256) {
    let mut word = [0u8; 32];
    let bytes = value.to_be_bytes_vec();
    word[32 - bytes.len()..].copy_from_slice(&bytes);
    out.extend_from_slice(&word);
}

fn print_quote(route: CheckRoute, quote: &RouteQuote) -> Result<()> {
    println!(
        "route_check[{}]: best {} {} -> {} {} in {} hop(s)",
        route.label,
        route.amount_units,
        route.token_in.symbol,
        format_units(quote.amount_out, route.token_out.decimals),
        route.token_out.symbol,
        quote.hops.len()
    );
    for (index, hop) in quote.hops.iter().enumerate() {
        println!(
            "  hop {}: {:?} {} -> {} amount_in={} amount_out={}",
            index + 1,
            hop.hop.pool,
            symbol_for(hop.hop.token_in),
            symbol_for(hop.hop.token_out),
            hop.amount_in,
            hop.amount_out
        );
    }
    Ok(())
}

fn rpc_url_from_env() -> Option<String> {
    [
        "E2E_RPC_URL",
        "ETHEREUM_RPC_URL",
        "MAINNET_RPC_URL",
        "ETH_RPC_URL",
        "RPC_URL",
        "ETH_WS_URL",
        "WS_RPC_URL",
    ]
    .into_iter()
    .find_map(|key| env::var(key).ok().filter(|value| !value.trim().is_empty()))
    .map(|url| {
        url.replacen("wss://", "https://", 1)
            .replacen("ws://", "http://", 1)
    })
}

fn env_u64_opt(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|value| value.parse().ok())
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

fn amount_for(decimals: u8, whole_units: u64) -> U256 {
    U256::from(whole_units) * U256::from(10u128.pow(u32::from(decimals)))
}

fn format_units(value: U256, decimals: u8) -> String {
    let scale = U256::from(10u128.pow(u32::from(decimals)));
    let whole = value / scale;
    let fractional = value % scale;
    if fractional.is_zero() {
        return whole.to_string();
    }
    let mut frac = fractional.to_string();
    let width = usize::from(decimals);
    if frac.len() < width {
        frac = format!("{}{}", "0".repeat(width - frac.len()), frac);
    }
    while frac.ends_with('0') {
        frac.pop();
    }
    format!("{whole}.{frac}")
}

fn symbol_for(token: Address) -> &'static str {
    match token {
        WETH => "WETH",
        USDC => "USDC",
        USDT => "USDT",
        DAI => "DAI",
        WBTC => "WBTC",
        _ => "UNKNOWN",
    }
}

const fn weth() -> TokenInfo {
    TokenInfo {
        symbol: "WETH",
        address: WETH,
        decimals: 18,
    }
}

const fn usdc() -> TokenInfo {
    TokenInfo {
        symbol: "USDC",
        address: USDC,
        decimals: 6,
    }
}

const fn dai() -> TokenInfo {
    TokenInfo {
        symbol: "DAI",
        address: DAI,
        decimals: 18,
    }
}

const fn wbtc() -> TokenInfo {
    TokenInfo {
        symbol: "WBTC",
        address: WBTC,
        decimals: 8,
    }
}
