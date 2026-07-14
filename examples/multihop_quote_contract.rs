//! Experiment: etch a generic multi-hop quote contract into `EvmCache` and
//! compare one top-level EVM call against the current per-hop adapter quote path.
//!
//! ```text
//! E2E_RPC_URL=<mainnet-url> [E2E_BLOCK=<block>] cargo run --release --example multihop_quote_contract
//! ```

use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{Address, Bytes, U256, address, aliases::U24, hex};
use alloy_sol_types::SolCall;
use anyhow::{Context, Result, bail, ensure};
use evm_amm_search::{
    AmmGraph, AmmSearcher, GraphBuildOptions, RoutePath, RouteRequest, SearchConfig,
};
use evm_amm_state::adapters::storage::V3StorageLayout;
use evm_amm_state::adapters::{
    AdapterRegistry, ColdStartOutcome, ColdStartPolicy, ConcentratedLiquidityAdapter, PoolKey,
    PoolRegistration, ProtocolMetadata, SimConfig, UniswapV2Adapter, UniswapV2Metadata, V3Metadata,
};
use evm_fork_cache::cache::EvmCache;

mod support;

mod abi {
    alloy_sol_types::sol! {
        struct QuoteExactInputSingleParams {
            address tokenIn;
            address tokenOut;
            uint256 amountIn;
            uint24 fee;
            uint160 sqrtPriceLimitX96;
        }

        function quoteExactInputSingle(QuoteExactInputSingleParams params)
            returns (
                uint256 amountOut,
                uint160 sqrtPriceX96After,
                uint32 initializedTicksCrossed,
                uint256 gasEstimate
            );

        function getAmountsOut(uint256 amountIn, address[] path)
            returns (uint256[] amounts);

        struct Hop {
            address target;
            bytes data;
            uint256 amountOffset;
            uint8 decodeMode;
        }

        function quote(uint256 amountIn, Hop[] hops) returns (uint256 amountOut);
    }
}

const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const WBTC: Address = address!("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599");

const V2_ROUTER_02: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const V3_QUOTER_V2: Address = address!("61fFE014bA17989E743c5F6cB21bF9697530B21e");
const V2_USDC_WETH_PAIR: Address = address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
const V3_WBTC_WETH_030: Address = address!("cbcdf9626bc03e24f779434178a73a0b4bad62ed");
const MULTIHOP_QUOTER: Address = address!("00000000000000000000000000000000000A11CE");

const DECODE_UINT256_ARRAY_LAST: u8 = 1;
const DECODE_FIRST_WORD: u8 = 2;
const V2_AMOUNT_OFFSET: u64 = 4;
const V3_AMOUNT_OFFSET: u64 = 68;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Ok(url) = std::env::var("E2E_RPC_URL") else {
        eprintln!("E2E_RPC_URL unset; this example needs a mainnet endpoint.");
        return Ok(());
    };

    let iters = env_usize("MULTIHOP_QUOTE_ITERS", 25);
    let amount_in = U256::from(env_u64("MULTIHOP_QUOTE_AMOUNT_RAW", 100_000_000_000));

    let provider = support::http_provider(&url)?;

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

    let mut registry = registry_with_adapters()?;
    let mut pools = vec![
        PoolRegistration::new(PoolKey::UniswapV2(V2_USDC_WETH_PAIR))
            .with_state_address(V2_USDC_WETH_PAIR)
            .with_metadata(ProtocolMetadata::UniswapV2(
                UniswapV2Metadata::default()
                    .with_token0(USDC)
                    .with_token1(WETH)
                    .with_fee_bps(30),
            )),
        PoolRegistration::new(PoolKey::UniswapV3(V3_WBTC_WETH_030))
            .with_state_address(V3_WBTC_WETH_030)
            .with_metadata(ProtocolMetadata::UniswapV3(
                V3Metadata::default()
                    .with_token0(WBTC)
                    .with_token1(WETH)
                    .with_fee(3000)
                    .with_tick_spacing(60)
                    .with_storage_layout(V3StorageLayout::uniswap(60)),
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
        .context("cold start pools")?;
    let ready_count = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ColdStartOutcome::Ready(_)))
        .count();
    ensure!(
        ready_count == pools.len(),
        "only {ready_count}/{} pools reached Ready",
        pools.len()
    );
    for pool in pools {
        registry.register_pool(pool)?;
    }

    let graph = AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
    let searcher = AmmSearcher::new(&registry, &graph);
    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);
    let request = RouteRequest::new(USDC, WBTC, amount_in)
        .with_config(SearchConfig::default().with_hops(2, 2))
        .with_sim_config(sim_config);

    let routes = searcher.find_routes(&request, &mut cache)?;
    let route = routes
        .iter()
        .find(|route| route.path.len() == 2)
        .context("expected a 2-hop USDC -> WETH -> WBTC route")?;
    print_route("selected route", &route.path);

    let runtime = compile_multihop_quoter_runtime()?;
    let code_hash = cache.etch_account_code(MULTIHOP_QUOTER, runtime)?;
    println!("etched MultihopQuoter at {MULTIHOP_QUOTER:?}, code_hash={code_hash:?}");

    let contract_hops = contract_hops(&registry, &route.path, &sim_config)?;

    let adapter_warm = searcher.quote_path(&route.path, amount_in, &mut cache, &sim_config)?;
    let contract_warm = quote_contract(&mut cache, &sim_config, amount_in, contract_hops.clone())?;
    ensure!(
        adapter_warm.amount_out == contract_warm,
        "adapter output {} != contract output {}",
        adapter_warm.amount_out,
        contract_warm
    );
    println!("warmup output={contract_warm}");

    let (adapter_elapsed, adapter_out) = bench_adapter(
        &searcher,
        &route.path,
        &mut cache,
        &sim_config,
        amount_in,
        iters,
    )?;
    let (contract_elapsed, contract_out) =
        bench_contract(&mut cache, &sim_config, amount_in, &contract_hops, iters)?;
    ensure!(
        adapter_out == contract_out,
        "adapter bench output {adapter_out} != contract bench output {contract_out}"
    );

    println!(
        "adapter per-hop: iters={}, elapsed={:?}, avg={:?}",
        iters,
        adapter_elapsed,
        adapter_elapsed / iters as u32
    );
    println!(
        "contract one-shot: iters={}, elapsed={:?}, avg={:?}",
        iters,
        contract_elapsed,
        contract_elapsed / iters as u32
    );

    Ok(())
}

fn registry_with_adapters() -> Result<AdapterRegistry> {
    let mut registry = AdapterRegistry::new();
    registry.register_adapter(Arc::new(UniswapV2Adapter::default()))?;
    registry.register_adapter(Arc::new(ConcentratedLiquidityAdapter::default()))?;
    Ok(registry)
}

fn contract_hops(
    registry: &AdapterRegistry,
    path: &RoutePath,
    config: &SimConfig,
) -> Result<Vec<abi::Hop>> {
    path.hops
        .iter()
        .map(|hop| {
            let pool = registry
                .pool(&hop.pool)
                .with_context(|| format!("missing pool {:?}", hop.pool))?;

            match &hop.pool {
                PoolKey::UniswapV2(_) => Ok(abi::Hop {
                    target: config.v2_router,
                    data: Bytes::from(
                        abi::getAmountsOutCall {
                            amountIn: U256::ZERO,
                            path: vec![hop.token_in, hop.token_out],
                        }
                        .abi_encode(),
                    ),
                    amountOffset: U256::from(V2_AMOUNT_OFFSET),
                    decodeMode: DECODE_UINT256_ARRAY_LAST,
                }),
                PoolKey::UniswapV3(_) | PoolKey::PancakeV3(_) | PoolKey::Slipstream(_) => {
                    let metadata = v3_metadata(&pool.metadata)
                        .context("V3-family pool missing V3 metadata")?;
                    let fee = metadata.fee.context("V3-family pool missing fee")?;
                    let quoter = metadata.quoter.unwrap_or(config.v3_quoter);
                    let params = abi::QuoteExactInputSingleParams {
                        tokenIn: hop.token_in,
                        tokenOut: hop.token_out,
                        amountIn: U256::ZERO,
                        fee: U24::from(fee),
                        sqrtPriceLimitX96: U256::ZERO.to(),
                    };

                    Ok(abi::Hop {
                        target: quoter,
                        data: Bytes::from(abi::quoteExactInputSingleCall { params }.abi_encode()),
                        amountOffset: U256::from(V3_AMOUNT_OFFSET),
                        decodeMode: DECODE_FIRST_WORD,
                    })
                }
                _ => bail!("unsupported protocol for contract quote: {:?}", hop.pool),
            }
        })
        .collect()
}

fn v3_metadata(metadata: &ProtocolMetadata) -> Option<&V3Metadata> {
    match metadata {
        ProtocolMetadata::UniswapV3(metadata)
        | ProtocolMetadata::PancakeV3(metadata)
        | ProtocolMetadata::Slipstream(metadata) => Some(metadata),
        _ => None,
    }
}

fn quote_contract(
    cache: &mut EvmCache,
    config: &SimConfig,
    amount_in: U256,
    hops: Vec<abi::Hop>,
) -> Result<U256> {
    cache
        .call_sol_from(
            config.from,
            MULTIHOP_QUOTER,
            abi::quoteCall {
                amountIn: amount_in,
                hops,
            },
        )
        .context("contract quote")
}

fn bench_adapter(
    searcher: &AmmSearcher<'_>,
    path: &RoutePath,
    cache: &mut EvmCache,
    config: &SimConfig,
    amount_in: U256,
    iters: usize,
) -> Result<(Duration, U256)> {
    let mut last = U256::ZERO;
    let started = Instant::now();
    for _ in 0..iters {
        last = searcher
            .quote_path(path, amount_in, cache, config)?
            .amount_out;
    }
    Ok((started.elapsed(), last))
}

fn bench_contract(
    cache: &mut EvmCache,
    config: &SimConfig,
    amount_in: U256,
    hops: &[abi::Hop],
    iters: usize,
) -> Result<(Duration, U256)> {
    let mut last = U256::ZERO;
    let started = Instant::now();
    for _ in 0..iters {
        last = quote_contract(cache, config, amount_in, hops.to_vec())?;
    }
    Ok((started.elapsed(), last))
}

fn compile_multihop_quoter_runtime() -> Result<Bytes> {
    let source = format!(
        "{}/contracts/MultihopQuoter.sol",
        env!("CARGO_MANIFEST_DIR")
    );
    let output = Command::new("solc")
        .args(["--optimize", "--bin-runtime", &source])
        .output()
        .context("run solc --optimize --bin-runtime")?;

    if !output.status.success() {
        bail!(
            "solc failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let stdout = String::from_utf8(output.stdout).context("solc stdout utf8")?;
    let marker = "Binary of the runtime part:";
    let runtime_hex = stdout
        .lines()
        .skip_while(|line| !line.contains(marker))
        .skip(1)
        .find_map(|line| {
            let line = line.trim();
            (!line.is_empty()).then_some(line)
        })
        .context("parse solc runtime bytecode")?;
    Ok(Bytes::from(hex::decode(runtime_hex)?))
}

fn print_route(label: &str, path: &RoutePath) {
    println!("{label}: {} hop(s)", path.len());
    for hop in &path.hops {
        println!(
            "  {:?}: {:?} -> {:?}",
            hop.pool, hop.token_in, hop.token_out
        );
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
