use std::time::{Duration, Instant};

#[cfg(feature = "live-runtime")]
use std::collections::HashMap;

use alloy_primitives::{Address, B256, Bytes, U256, address, hex};
use alloy_sol_types::SolCall;
use anyhow::{Context, Result, anyhow, bail, ensure};
use evm_amm_state::adapters::{AdapterRegistry, CurveVariant, PoolKey, ProtocolMetadata};
use evm_fork_cache::cache::{EvmCache, EvmOverlay};
#[cfg(feature = "live-runtime")]
use evm_fork_cache::mapping_probe::TrackedMapping;
use revm::context::result::ExecutionResult;

use crate::RouteQuote;
#[cfg(feature = "live-runtime")]
use crate::{LiveSearchView, VersionedRouteQuote};

pub const DEMO_ROUTER: Address = address!("00000000000000000000000000000000000A11CF");
const PACKED_UNISWAP_V2: u8 = 0;
const PACKED_UNISWAP_V3: u8 = 1;
const PACKED_PANCAKE_V3: u8 = 2;
const PACKED_SLIPSTREAM: u8 = 3;
const PACKED_SOLIDLY_V2: u8 = 4;
const PACKED_BALANCER_V2: u8 = 5;
const PACKED_CURVE_STABLE: u8 = 6;
const PACKED_CURVE_CRYPTO: u8 = 7;
const PACKED_CURVE_CRYPTO_NG: u8 = 8;
const PACKED_GENERIC_CALL: u8 = 255;
const U32_NONE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
pub struct DemoRouterConfig {
    pub router: Address,
    pub caller: Address,
}

impl Default for DemoRouterConfig {
    fn default() -> Self {
        Self {
            router: DEMO_ROUTER,
            caller: Address::ZERO,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SwapGasEstimate {
    pub gross_amount_out: U256,
    pub gas_used: u64,
    pub gas_price_wei: Option<u128>,
    pub gas_cost_native: Option<U256>,
    pub latency: Duration,
}

#[derive(Clone, Debug)]
pub struct GenericExecution {
    pub target: Address,
    pub spender: Option<Address>,
    pub calldata: Bytes,
    pub amount_in_offset: Option<usize>,
}

impl GenericExecution {
    pub fn new(target: Address, calldata: Bytes) -> Self {
        Self {
            target,
            spender: None,
            calldata,
            amount_in_offset: None,
        }
    }

    pub fn with_spender(mut self, spender: Address) -> Self {
        self.spender = Some(spender);
        self
    }

    pub fn with_amount_in_offset(mut self, offset: usize) -> Self {
        self.amount_in_offset = Some(offset);
        self
    }
}

#[derive(Clone, Debug)]
pub enum DemoRouterHop {
    UniswapV2 {
        pool: Address,
        token_in: Address,
        token_out: Address,
    },
    UniswapV3 {
        pool: Address,
        token_in: Address,
        token_out: Address,
    },
    PancakeV3 {
        pool: Address,
        token_in: Address,
        token_out: Address,
    },
    Slipstream {
        pool: Address,
        token_in: Address,
        token_out: Address,
    },
    SolidlyV2 {
        pool: Address,
        token_in: Address,
        token_out: Address,
    },
    BalancerV2 {
        vault: Address,
        pool_id: B256,
        token_in: Address,
        token_out: Address,
    },
    Curve {
        pool: Address,
        token_in: Address,
        token_out: Address,
        variant: CurveVariant,
        i: u8,
        j: u8,
    },
    Generic {
        token_in: Address,
        token_out: Address,
        execution: GenericExecution,
    },
}

impl DemoRouterHop {
    pub fn uniswap_v2(pool: Address, token_in: Address, token_out: Address) -> Self {
        Self::UniswapV2 {
            pool,
            token_in,
            token_out,
        }
    }

    pub fn uniswap_v3(pool: Address, token_in: Address, token_out: Address) -> Self {
        Self::UniswapV3 {
            pool,
            token_in,
            token_out,
        }
    }

    pub fn generic(token_in: Address, token_out: Address, execution: GenericExecution) -> Self {
        Self::Generic {
            token_in,
            token_out,
            execution,
        }
    }
}

pub fn install_demo_router(cache: &mut EvmCache) -> Result<B256> {
    let runtime = demo_router_runtime()?;
    cache
        .etch_account_code(DEMO_ROUTER, runtime)
        .context("etch demo router")
}

/// Warm the storage/code path an ERC-20 transfer from the demo router needs.
///
/// Live route simulation runs against immutable snapshots without provider
/// fallback. Balance-slot discovery alone is insufficient for proxy tokens or
/// tokens whose transfer path reads pause/blacklist/configuration state. This
/// non-committing call loads those dependencies into the cache, then restores
/// the router's canonical balance before the snapshot is published.
#[cfg(feature = "live-runtime")]
pub fn prewarm_demo_router_token_transfer(
    cache: &mut EvmCache,
    token: Address,
    balance_mapping: &TrackedMapping,
) -> Result<()> {
    const RECEIVER: Address = address!("00000000000000000000000000000000000A11D0");
    let balance_slot = balance_mapping
        .slot_for(DEMO_ROUTER.into_word())
        .with_context(|| format!("unsupported balance layout for token {token}"))?;
    let balance_slot = U256::from_be_slice(balance_slot.as_slice());
    let original_balance = cache
        .read_storage_slot(token, balance_slot)
        .with_context(|| format!("read canonical demo-router balance for token {token}"))?;
    cache
        .insert_storage_slot(token, balance_slot, U256::from(1))
        .with_context(|| format!("seed demo-router transfer balance for token {token}"))?;

    let calldata = Bytes::from(
        token_abi::transferCall {
            to: RECEIVER,
            amount: U256::from(1),
        }
        .abi_encode(),
    );
    let transfer = cache.call_raw(DEMO_ROUTER, token, calldata, false);
    cache
        .insert_storage_slot(token, balance_slot, original_balance)
        .with_context(|| format!("restore canonical demo-router balance for token {token}"))?;

    match transfer.with_context(|| format!("prewarm transfer path for token {token}"))? {
        ExecutionResult::Success { .. } => Ok(()),
        ExecutionResult::Revert { output, .. } => bail!(
            "token {token} transfer prewarm reverted: 0x{}",
            hex::encode(output)
        ),
        ExecutionResult::Halt { reason, .. } => {
            bail!("token {token} transfer prewarm halted: {reason:?}")
        }
    }
}

pub fn simulate_route_gas(
    registry: &AdapterRegistry,
    cache: &mut EvmCache,
    quote: &RouteQuote,
    config: DemoRouterConfig,
    gas_price_wei: Option<u128>,
) -> Result<SwapGasEstimate> {
    let hops = demo_router_hops_for_quote(registry, quote)?;
    let token_in = quote
        .hops
        .first()
        .map(|hop| hop.hop.token_in)
        .context("cannot simulate gas for an empty route")?;
    let token_out = quote
        .hops
        .last()
        .map(|hop| hop.hop.token_out)
        .context("cannot simulate gas for an empty route")?;
    simulate_gas_hops(
        cache,
        token_in,
        quote.amount_in,
        token_out,
        hops,
        config,
        gas_price_wei,
    )
}

/// Simulate a versioned route against the exact immutable view that produced it.
#[cfg(feature = "live-runtime")]
pub fn simulate_versioned_route_gas(
    view: &LiveSearchView,
    quote: &VersionedRouteQuote,
    config: DemoRouterConfig,
    gas_price_wei: Option<u128>,
) -> Result<SwapGasEstimate> {
    let source = quote.source();
    ensure!(
        source.runtime_id() == view.snapshot().runtime_id()
            && source.state_version() == view.snapshot().version()
            && source.point() == view.snapshot().point()
            && source.graph_version() == view.graph().version(),
        "versioned route quote does not belong to the supplied live search view"
    );
    let quote = quote.quote();
    let hops = demo_router_hops_for_quote(view.snapshot().registry().registry(), quote)?;
    let token_in = quote
        .hops
        .first()
        .map(|hop| hop.hop.token_in)
        .context("cannot simulate gas for an empty route")?;
    let token_out = quote
        .hops
        .last()
        .map(|hop| hop.hop.token_out)
        .context("cannot simulate gas for an empty route")?;
    let mut overlay = EvmOverlay::new(view.snapshot().cache_snapshot(), None);
    simulate_gas_hops_overlay(
        &mut overlay,
        token_in,
        quote.amount_in,
        token_out,
        hops,
        config,
        gas_price_wei,
    )
}

/// Simulate a versioned route with pre-discovered ERC-20 balance layouts.
///
/// Immutable live snapshots deliberately have no provider fallback. The
/// mappings let the simulator seed router and venue balances inside its local
/// overlay so token transfers execute without reading storage that was not
/// quote-relevant to the AMM snapshot.
#[cfg(feature = "live-runtime")]
pub fn simulate_versioned_route_gas_with_balance_mappings(
    view: &LiveSearchView,
    quote: &VersionedRouteQuote,
    config: DemoRouterConfig,
    gas_price_wei: Option<u128>,
    balance_mappings: &HashMap<Address, TrackedMapping>,
) -> Result<SwapGasEstimate> {
    let source = quote.source();
    ensure!(
        source.runtime_id() == view.snapshot().runtime_id()
            && source.state_version() == view.snapshot().version()
            && source.point() == view.snapshot().point()
            && source.graph_version() == view.graph().version(),
        "versioned route quote does not belong to the supplied live search view"
    );
    let quote = quote.quote();
    let hops = demo_router_hops_for_quote(view.snapshot().registry().registry(), quote)?;
    let token_in = quote
        .hops
        .first()
        .map(|hop| hop.hop.token_in)
        .context("cannot simulate gas for an empty route")?;
    let token_out = quote
        .hops
        .last()
        .map(|hop| hop.hop.token_out)
        .context("cannot simulate gas for an empty route")?;
    let mut overlay = EvmOverlay::new(view.snapshot().cache_snapshot(), None);
    simulate_gas_hops_overlay_with_balance_mappings(
        &mut overlay,
        token_in,
        quote.amount_in,
        token_out,
        hops,
        config,
        gas_price_wei,
        balance_mappings,
    )
}

pub fn encode_demo_router_execute_calldata(
    registry: &AdapterRegistry,
    quote: &RouteQuote,
) -> Result<Bytes> {
    let hops = demo_router_hops_for_quote(registry, quote)?;
    let token_out = quote
        .hops
        .last()
        .map(|hop| hop.hop.token_out)
        .context("cannot encode demo router calldata for an empty route")?;
    execute_calldata(quote.amount_in, token_out, hops)
}

pub fn encode_demo_router_execute_from_calldata(
    registry: &AdapterRegistry,
    quote: &RouteQuote,
    sender: Address,
) -> Result<Bytes> {
    let hops = demo_router_hops_for_quote(registry, quote)?;
    let token_in = quote
        .hops
        .first()
        .map(|hop| hop.hop.token_in)
        .context("cannot encode demo router calldata for an empty route")?;
    let token_out = quote
        .hops
        .last()
        .map(|hop| hop.hop.token_out)
        .context("cannot encode demo router calldata for an empty route")?;
    let route = encode_packed_route(token_out, hops)?;
    let calldata = router_abi::executeFromCall {
        sender,
        tokenIn: token_in,
        amountIn: quote.amount_in,
        route,
    }
    .abi_encode();
    Ok(Bytes::from(calldata))
}

pub fn demo_router_hops_for_quote(
    registry: &AdapterRegistry,
    quote: &RouteQuote,
) -> Result<Vec<DemoRouterHop>> {
    route_hops(registry, quote)
}

pub fn simulate_route_prefix_gas(
    registry: &AdapterRegistry,
    cache: &mut EvmCache,
    quote: &RouteQuote,
    prefix_len: usize,
    config: DemoRouterConfig,
    gas_price_wei: Option<u128>,
) -> Result<SwapGasEstimate> {
    ensure!(prefix_len > 0, "prefix length must be non-zero");
    ensure!(
        prefix_len <= quote.hops.len(),
        "prefix length {prefix_len} exceeds route hop count {}",
        quote.hops.len()
    );
    let token_in = quote
        .hops
        .first()
        .map(|hop| hop.hop.token_in)
        .context("cannot simulate gas for an empty route")?;
    let prefix_out = quote.hops[prefix_len - 1].hop.token_out;
    let hops = route_hops(registry, quote)?
        .into_iter()
        .take(prefix_len)
        .collect();
    simulate_gas_hops(
        cache,
        token_in,
        quote.amount_in,
        prefix_out,
        hops,
        config,
        gas_price_wei,
    )
}

pub fn simulate_gas_hops(
    cache: &mut EvmCache,
    token_in: Address,
    amount_in: U256,
    token_out: Address,
    hops: Vec<DemoRouterHop>,
    config: DemoRouterConfig,
    gas_price_wei: Option<u128>,
) -> Result<SwapGasEstimate> {
    let mut overlay = cache.mock_overlay();
    simulate_gas_hops_overlay(
        &mut overlay,
        token_in,
        amount_in,
        token_out,
        hops,
        config,
        gas_price_wei,
    )
}

fn simulate_gas_hops_overlay(
    overlay: &mut EvmOverlay,
    token_in: Address,
    amount_in: U256,
    token_out: Address,
    hops: Vec<DemoRouterHop>,
    config: DemoRouterConfig,
    gas_price_wei: Option<u128>,
) -> Result<SwapGasEstimate> {
    let started = Instant::now();
    let calldata = execute_calldata(amount_in, token_out, hops)?;
    ensure!(
        overlay.mock_balance(token_in, config.router, amount_in)?,
        "could not mock router balance for input token"
    );
    let result = overlay
        .call_raw(config.caller, config.router, calldata)
        .context("simulate demo router route")?;
    let (gross_amount_out, gas_used) = decode_execute_result(result)?;
    Ok(SwapGasEstimate {
        gross_amount_out,
        gas_used,
        gas_price_wei,
        gas_cost_native: gas_price_wei.map(|price| U256::from(price) * U256::from(gas_used)),
        latency: started.elapsed(),
    })
}

#[cfg(feature = "live-runtime")]
#[allow(clippy::too_many_arguments)]
fn simulate_gas_hops_overlay_with_balance_mappings(
    overlay: &mut EvmOverlay,
    token_in: Address,
    amount_in: U256,
    token_out: Address,
    hops: Vec<DemoRouterHop>,
    config: DemoRouterConfig,
    gas_price_wei: Option<u128>,
    balance_mappings: &HashMap<Address, TrackedMapping>,
) -> Result<SwapGasEstimate> {
    let started = Instant::now();
    let input_mapping = balance_mappings
        .get(&token_in)
        .with_context(|| format!("missing balance mapping for input token {token_in}"))?;
    override_tracked_balance(overlay, token_in, config.router, amount_in, input_mapping)?;
    let venue_balance = benchmark_venue_balance();
    for (hop_index, hop) in hops.iter().enumerate() {
        let (venue, hop_token_in, hop_token_out) = hop_balance_context(hop);
        let output_mapping = balance_mappings
            .get(&hop_token_out)
            .with_context(|| format!("missing balance mapping for token {hop_token_out}"))?;
        override_tracked_balance(overlay, hop_token_out, venue, venue_balance, output_mapping)?;
        if should_preflight_router_input(hop_index) {
            verify_mocked_transfer(overlay, config.router, hop_token_in, venue)
                .context("verify initial router-to-venue input transfer")?;
        }
        verify_mocked_transfer(overlay, venue, hop_token_out, config.router)
            .context("verify venue-to-router output transfer")?;
    }
    let calldata = execute_calldata(amount_in, token_out, hops)?;
    let result = overlay
        .call_raw(config.caller, config.router, calldata)
        .context("simulate demo router route")?;
    let (gross_amount_out, gas_used) = decode_execute_result(result)?;
    Ok(SwapGasEstimate {
        gross_amount_out,
        gas_used,
        gas_price_wei,
        gas_cost_native: gas_price_wei.map(|price| U256::from(price) * U256::from(gas_used)),
        latency: started.elapsed(),
    })
}

#[cfg(feature = "live-runtime")]
fn should_preflight_router_input(hop_index: usize) -> bool {
    hop_index == 0
}

#[cfg(feature = "live-runtime")]
fn benchmark_venue_balance() -> U256 {
    // UNI and some other ERC-20s store balances as uint96. Keep every seeded
    // bit inside that packed field while retaining ample benchmark headroom.
    (U256::from(1) << 95) - U256::from(1)
}

#[cfg(feature = "live-runtime")]
fn tracked_balance_slot(token: Address, holder: Address, mapping: &TrackedMapping) -> Result<U256> {
    ensure!(
        mapping.contract == token,
        "balance mapping contract {} does not match token {token}",
        mapping.contract
    );
    let slot = mapping
        .slot_for(holder.into_word())
        .with_context(|| format!("unsupported balance layout for token {token}"))?;
    Ok(U256::from_be_slice(slot.as_slice()))
}

#[cfg(feature = "live-runtime")]
fn override_tracked_balance(
    overlay: &mut EvmOverlay,
    token: Address,
    holder: Address,
    amount: U256,
    mapping: &TrackedMapping,
) -> Result<()> {
    let slot = tracked_balance_slot(token, holder, mapping)?;
    overlay.override_slot(token, slot, amount);
    Ok(())
}

#[cfg(feature = "live-runtime")]
fn verify_mocked_transfer(
    overlay: &mut EvmOverlay,
    sender: Address,
    token: Address,
    receiver: Address,
) -> Result<()> {
    let calldata = Bytes::from(
        token_abi::transferCall {
            to: receiver,
            amount: U256::from(1),
        }
        .abi_encode(),
    );
    match overlay
        .call_raw(sender, token, calldata)
        .with_context(|| format!("call token {token} from {sender} to {receiver}"))?
    {
        ExecutionResult::Success { .. } => Ok(()),
        ExecutionResult::Revert { output, .. } => bail!(
            "token {token} transfer from {sender} to {receiver} reverted: 0x{}",
            hex::encode(output)
        ),
        ExecutionResult::Halt { reason, .. } => {
            bail!("token {token} transfer from {sender} to {receiver} halted: {reason:?}")
        }
    }
}

#[cfg(feature = "live-runtime")]
fn hop_balance_context(hop: &DemoRouterHop) -> (Address, Address, Address) {
    match hop {
        DemoRouterHop::UniswapV2 {
            pool,
            token_in,
            token_out,
        }
        | DemoRouterHop::UniswapV3 {
            pool,
            token_in,
            token_out,
        }
        | DemoRouterHop::PancakeV3 {
            pool,
            token_in,
            token_out,
        }
        | DemoRouterHop::Slipstream {
            pool,
            token_in,
            token_out,
        }
        | DemoRouterHop::SolidlyV2 {
            pool,
            token_in,
            token_out,
        }
        | DemoRouterHop::Curve {
            pool,
            token_in,
            token_out,
            ..
        } => (*pool, *token_in, *token_out),
        DemoRouterHop::BalancerV2 {
            vault,
            token_in,
            token_out,
            ..
        } => (*vault, *token_in, *token_out),
        DemoRouterHop::Generic {
            token_in,
            token_out,
            execution,
        } => (execution.target, *token_in, *token_out),
    }
}

#[cfg(all(test, feature = "live-runtime"))]
mod tests {
    use super::*;
    use evm_fork_cache::mapping_probe::SlotLayout;

    #[test]
    fn tracked_balance_slot_uses_the_supplied_mapping_descriptor() {
        let token = Address::repeat_byte(0x51);
        let holder = Address::repeat_byte(0x52);
        let mapping = TrackedMapping::new(token, U256::from(7), SlotLayout::VyperMapping);

        let slot = tracked_balance_slot(token, holder, &mapping).expect("tracked balance slot");

        assert_eq!(
            slot,
            U256::from_be_slice(mapping.slot_for(holder.into_word()).unwrap().as_slice())
        );
    }

    #[test]
    fn only_the_first_hop_preflights_router_input_balance() {
        assert!(should_preflight_router_input(0));
        assert!(!should_preflight_router_input(1));
        assert!(!should_preflight_router_input(2));
    }

    #[test]
    fn venue_balance_seed_survives_uint96_packed_balances() {
        let balance = benchmark_venue_balance();

        assert!(balance < (U256::from(1) << 96));
        assert!(balance > U256::from(1_000_000_000_000_000_000_000_000_u128));
    }
}

fn route_hops(registry: &AdapterRegistry, quote: &RouteQuote) -> Result<Vec<DemoRouterHop>> {
    quote
        .path
        .hops
        .iter()
        .map(|hop| {
            let pool = registry
                .pool(&hop.pool)
                .with_context(|| format!("missing pool {:?}", hop.pool))?;
            match &hop.pool {
                PoolKey::UniswapV2(_) => {
                    let pool_address = pool_address(pool, &hop.pool, "V2 pool")?;
                    Ok(DemoRouterHop::uniswap_v2(
                        pool_address,
                        hop.token_in,
                        hop.token_out,
                    ))
                }
                PoolKey::UniswapV3(_) => {
                    let pool_address = pool_address(pool, &hop.pool, "V3 pool")?;
                    Ok(DemoRouterHop::uniswap_v3(
                        pool_address,
                        hop.token_in,
                        hop.token_out,
                    ))
                }
                PoolKey::PancakeV3(_) => {
                    let pool_address = pool_address(pool, &hop.pool, "Pancake V3 pool")?;
                    Ok(DemoRouterHop::PancakeV3 {
                        pool: pool_address,
                        token_in: hop.token_in,
                        token_out: hop.token_out,
                    })
                }
                PoolKey::Slipstream(_) => {
                    let pool_address = pool_address(pool, &hop.pool, "Slipstream pool")?;
                    Ok(DemoRouterHop::Slipstream {
                        pool: pool_address,
                        token_in: hop.token_in,
                        token_out: hop.token_out,
                    })
                }
                PoolKey::SolidlyV2(_) => {
                    let pool_address = pool_address(pool, &hop.pool, "Solidly V2 pool")?;
                    Ok(DemoRouterHop::SolidlyV2 {
                        pool: pool_address,
                        token_in: hop.token_in,
                        token_out: hop.token_out,
                    })
                }
                PoolKey::Curve(pool_address) => {
                    let ProtocolMetadata::Curve(metadata) = &pool.metadata else {
                        bail!("missing Curve metadata for {:?}", hop.pool);
                    };
                    curve_hop(*pool_address, metadata, hop.token_in, hop.token_out)
                }
                PoolKey::BalancerV2(pool_id) => {
                    let ProtocolMetadata::BalancerV2(metadata) = &pool.metadata else {
                        bail!("missing Balancer V2 metadata for {:?}", hop.pool);
                    };
                    let vault = metadata
                        .vault
                        .or_else(|| pool.state_addresses.first().copied())
                        .with_context(|| format!("missing Balancer V2 vault for {:?}", hop.pool))?;
                    Ok(DemoRouterHop::BalancerV2 {
                        vault,
                        pool_id: *pool_id,
                        token_in: hop.token_in,
                        token_out: hop.token_out,
                    })
                }
                _ => Err(anyhow!("demo router does not support {:?}", hop.pool)),
            }
        })
        .collect()
}

fn pool_address(
    pool: &evm_amm_state::adapters::PoolRegistration,
    key: &PoolKey,
    label: &'static str,
) -> Result<Address> {
    key.address()
        .or_else(|| pool.state_addresses.first().copied())
        .with_context(|| format!("missing {label} address for {key:?}"))
}

fn curve_hop(
    pool: Address,
    metadata: &evm_amm_state::adapters::CurveMetadata,
    token_in: Address,
    token_out: Address,
) -> Result<DemoRouterHop> {
    let i = metadata
        .coins
        .iter()
        .position(|coin| *coin == token_in)
        .context("Curve token_in not in pool coins")?;
    let j = metadata
        .coins
        .iter()
        .position(|coin| *coin == token_out)
        .context("Curve token_out not in pool coins")?;
    ensure!(i != j, "Curve token_in == token_out");
    ensure!(i <= u8::MAX as usize, "Curve token_in index overflows u8");
    ensure!(j <= u8::MAX as usize, "Curve token_out index overflows u8");

    Ok(DemoRouterHop::Curve {
        pool,
        token_in,
        token_out,
        variant: metadata.variant,
        i: i as u8,
        j: j as u8,
    })
}

fn execute_calldata(
    amount_in: U256,
    token_out: Address,
    hops: Vec<DemoRouterHop>,
) -> Result<Bytes> {
    let route = encode_packed_route(token_out, hops)?;
    let calldata = router_abi::execute_p43bff2e1Call {
        amountIn: amount_in,
        route,
    }
    .abi_encode();
    Ok(Bytes::from(calldata))
}

fn encode_packed_route(token_out: Address, hops: Vec<DemoRouterHop>) -> Result<Bytes> {
    let mut out = Vec::with_capacity(20 + hops.len() * 64);
    out.extend_from_slice(token_out.as_slice());
    for hop in hops {
        match hop {
            DemoRouterHop::UniswapV2 {
                pool,
                token_in,
                token_out,
            } => push_common(&mut out, PACKED_UNISWAP_V2, pool, token_in, token_out),
            DemoRouterHop::UniswapV3 {
                pool,
                token_in,
                token_out,
            } => push_common(&mut out, PACKED_UNISWAP_V3, pool, token_in, token_out),
            DemoRouterHop::PancakeV3 {
                pool,
                token_in,
                token_out,
            } => push_common(&mut out, PACKED_PANCAKE_V3, pool, token_in, token_out),
            DemoRouterHop::Slipstream {
                pool,
                token_in,
                token_out,
            } => push_common(&mut out, PACKED_SLIPSTREAM, pool, token_in, token_out),
            DemoRouterHop::SolidlyV2 {
                pool,
                token_in,
                token_out,
            } => push_common(&mut out, PACKED_SOLIDLY_V2, pool, token_in, token_out),
            DemoRouterHop::BalancerV2 {
                vault,
                pool_id,
                token_in,
                token_out,
            } => {
                push_common(&mut out, PACKED_BALANCER_V2, vault, token_in, token_out);
                out.extend_from_slice(pool_id.as_slice());
            }
            DemoRouterHop::Curve {
                pool,
                token_in,
                token_out,
                variant,
                i,
                j,
            } => {
                let protocol = match variant {
                    CurveVariant::StableSwap => PACKED_CURVE_STABLE,
                    CurveVariant::CryptoSwap => PACKED_CURVE_CRYPTO,
                    CurveVariant::CryptoSwapNG => PACKED_CURVE_CRYPTO_NG,
                    _ => bail!("unsupported Curve variant for demo router"),
                };
                push_common(&mut out, protocol, pool, token_in, token_out);
                out.push(i);
                out.push(j);
            }
            DemoRouterHop::Generic {
                token_in,
                token_out,
                execution,
            } => {
                push_common(
                    &mut out,
                    PACKED_GENERIC_CALL,
                    execution.target,
                    token_in,
                    token_out,
                );
                out.extend_from_slice(execution.spender.unwrap_or(Address::ZERO).as_slice());
                let amount_in_offset = execution
                    .amount_in_offset
                    .map(|offset| {
                        u32::try_from(offset).context("generic amount offset overflows u32")
                    })
                    .transpose()?
                    .unwrap_or(U32_NONE);
                let data_len = u32::try_from(execution.calldata.len())
                    .context("generic calldata length overflows u32")?;
                out.extend_from_slice(&amount_in_offset.to_be_bytes());
                out.extend_from_slice(&data_len.to_be_bytes());
                out.extend_from_slice(&execution.calldata);
            }
        }
    }
    Ok(Bytes::from(out))
}

fn push_common(
    out: &mut Vec<u8>,
    protocol: u8,
    endpoint: Address,
    token_in: Address,
    token_out: Address,
) {
    out.push(protocol);
    out.extend_from_slice(endpoint.as_slice());
    out.extend_from_slice(token_in.as_slice());
    out.extend_from_slice(token_out.as_slice());
}

fn decode_execute_result(result: ExecutionResult) -> Result<(U256, u64)> {
    match result {
        ExecutionResult::Success {
            gas_used, output, ..
        } => {
            let decoded =
                router_abi::execute_p43bff2e1Call::abi_decode_returns(&output.into_data())
                    .context("decode gas router return")?;
            Ok((decoded, gas_used))
        }
        ExecutionResult::Revert { gas_used, output } => Err(anyhow!(
            "demo router reverted after {gas_used} gas: 0x{}",
            hex::encode(output)
        )),
        ExecutionResult::Halt { reason, gas_used } => Err(anyhow!(
            "demo router halted after {gas_used} gas: {reason:?}"
        )),
    }
}

pub fn demo_router_runtime() -> Result<Bytes> {
    let runtime_hex = include_str!("../contracts/DemoRouter.runtime.hex").trim();
    Ok(Bytes::from(hex::decode(runtime_hex)?))
}

mod router_abi {
    #![allow(non_snake_case)]

    use alloy_sol_types::sol;

    sol! {
        function execute_p43bff2e1(uint256 amountIn, bytes route) returns (uint256 amountOut);
        function executeFrom(address sender, address tokenIn, uint256 amountIn, bytes route) returns (uint256 amountOut);
    }
}

#[cfg(feature = "live-runtime")]
mod token_abi {
    #![allow(non_snake_case)]

    use alloy_sol_types::sol;

    sol! {
        function transfer(address to, uint256 amount) returns (bool);
    }
}
