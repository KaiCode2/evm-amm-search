//! Env-gated cross-DEX route search demo.
//!
//! This mirrors the manual `evm-amm-state` arbitrage examples, but lets
//! `evm-amm-search` build the token graph and enumerate candidate routes.
//!
//! ```text
//! E2E_RPC_URL=<mainnet-url> [E2E_BLOCK=<block>] cargo run --example arbitrage_search
//! ```

use std::sync::Arc;

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{Address, U256, address};
use anyhow::{Context, Result, ensure};
use evm_amm_search::{AmmGraph, AmmSearcher, GraphBuildOptions, RouteRequest, SearchConfig};
use evm_amm_state::adapters::storage::V3StorageLayout;
use evm_amm_state::adapters::{
    AdapterRegistry, ColdStartOutcome, ColdStartPolicy, ConcentratedLiquidityAdapter, CurveAdapter,
    CurveMetadata, CurveVariant, PoolKey, PoolRegistration, ProtocolMetadata, SimConfig,
    UniswapV2Adapter, UniswapV2Metadata, V3Metadata,
};
use evm_fork_cache::cache::EvmCache;

mod support;

const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const WBTC: Address = address!("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599");

const V3_QUOTER_V2: Address = address!("61fFE014bA17989E743c5F6cB21bF9697530B21e");
const V3_USDC_WETH_005: Address = address!("88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640");
const V2_ROUTER_02: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const V2_USDC_WETH_PAIR: Address = address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
const TRICRYPTO_USDC_NG: Address = address!("7F86Bf177Dd4F3494b841a37e810A34dD56c829B");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Ok(url) = std::env::var("E2E_RPC_URL") else {
        eprintln!("E2E_RPC_URL unset; this example needs a mainnet endpoint to warm real pools.");
        return Ok(());
    };

    let mut registry = AdapterRegistry::new();
    registry.register_adapter(Arc::new(UniswapV2Adapter::default()))?;
    registry.register_adapter(Arc::new(ConcentratedLiquidityAdapter::default()))?;
    registry.register_adapter(Arc::new(CurveAdapter::default()))?;

    let provider = support::http_provider(&url)?;
    println!("using Alloy's gzip-enabled registry HTTP transport");
    let mut cache = if let Ok(block) = std::env::var("E2E_BLOCK") {
        let block = block.parse::<u64>().context("parse E2E_BLOCK")?;
        println!("using pinned block {block}");
        EvmCache::at_block(
            provider.clone(),
            BlockId::Number(BlockNumberOrTag::Number(block)),
        )
        .await
    } else {
        println!("using latest block tag");
        EvmCache::new(provider.clone()).await
    };

    let mut pools = vec![
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
        PoolRegistration::new(PoolKey::Curve(TRICRYPTO_USDC_NG))
            .with_state_address(TRICRYPTO_USDC_NG)
            .with_metadata(ProtocolMetadata::Curve(
                CurveMetadata::default()
                    .with_coins([USDC, WBTC, WETH])
                    .with_variant(CurveVariant::CryptoSwapNG),
            )),
    ];

    let outcomes = registry
        .cold_start_many(
            &mut pools,
            &mut cache,
            provider.as_ref(),
            ColdStartPolicy::Eager,
        )
        .await
        .context("bootstrap pools")?;
    let ready_count = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ColdStartOutcome::Ready(_)))
        .count();
    ensure!(
        ready_count == pools.len(),
        "only {ready_count}/{} pools reached Ready",
        pools.len()
    );
    println!("bootstrapped {ready_count}/{} pools to Ready", pools.len());
    for pool in pools {
        registry.register_pool(pool)?;
    }

    let report = AmmGraph::from_registry(&registry, GraphBuildOptions::default());
    println!(
        "indexed {} pools, skipped {} pools, graph edges={}",
        report.indexed_pools.len(),
        report.skipped_pools.len(),
        report.graph.edge_count()
    );

    let searcher = AmmSearcher::new(&registry, &report.graph);
    let cfg = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);
    let request = RouteRequest::new(USDC, WETH, U256::from(100_000_000_000_u64))
        .with_config(SearchConfig::default().with_hops(1, 2))
        .with_sim_config(cfg);

    let routes = searcher.find_routes(&request, &mut cache)?;
    println!("found {} viable route(s)", routes.len());

    let best = routes.first().context("at least one route")?;
    println!(
        "best route: {} hops, {} raw WETH out",
        best.path.len(),
        best.amount_out
    );
    for (index, route) in routes.iter().enumerate() {
        println!(
            "route #{}: {} hops, {} raw WETH out",
            index + 1,
            route.path.len(),
            route.amount_out
        );
        for hop in &route.hops {
            println!(
                "  {:?}: {:?} -> {:?}, {} -> {}",
                hop.hop.pool, hop.hop.token_in, hop.hop.token_out, hop.amount_in, hop.amount_out
            );
        }
    }

    Ok(())
}
