//! Sidecar-only construction of executable exact-input swap transactions.
//!
//! This module deliberately does not belong to `evm-amm-search`'s public API.
//! It translates an in-process sidecar quote into the unstable experimental
//! executor contract format.

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::{Context, Result, bail, ensure};
use evm_amm_search::RouteQuote;
use evm_amm_state::adapters::{AdapterRegistry, CurveVariant, PoolKey, ProtocolMetadata};

/// Deployed contracts required to build an executable transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutorDeployment {
    pub router: Address,
    pub weth: Address,
    pub permit2: Address,
}

/// Input-funding mechanism selected by the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputAuthorization {
    Allowance,
    Native,
    Erc2612 {
        v: u8,
        r: B256,
        s: B256,
    },
    Permit2 {
        nonce: U256,
        deadline: U256,
        signature: Bytes,
    },
}

/// Inputs that are not derivable from a graph quote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRequest {
    pub deployment: ExecutorDeployment,
    pub recipient: Address,
    pub deadline: U256,
    pub min_amount_out: Option<U256>,
    pub authorization: InputAuthorization,
}

/// ERC-20 approval that must exist before submitting the swap transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApprovalRequirement {
    pub token: Address,
    pub spender: Address,
    pub minimum_amount: U256,
}

/// Executor call payload. The caller still supplies chain, sender, nonce, gas,
/// and fee fields before signing a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutableSwap {
    pub to: Address,
    pub value: U256,
    pub data: Bytes,
    pub approval: Option<ApprovalRequirement>,
    pub min_amount_out: U256,
    pub deadline: U256,
}

/// User-specific call to simulate at the exact block that produced a quote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionSimulationRequest {
    pub block_hash: B256,
    pub sender: Address,
    pub swap: ExecutableSwap,
}

/// Allowance lookup performed before simulating a transaction that uses an
/// existing router or Permit2 approval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionApprovalCheckRequest {
    pub block_hash: B256,
    pub sender: Address,
    pub approval: ApprovalRequirement,
}

/// Current allowance and the estimated cost of setting the exact requirement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionApprovalState {
    pub current_allowance: U256,
    pub gas_estimate: Option<u64>,
}

/// Successful execution simulation and gas estimate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionSimulation {
    pub amount_out: U256,
    pub gas_estimate: u64,
}

/// Resolve a basis-point tolerance against the best route's gross output.
pub fn min_amount_out_from_slippage(amount_out: U256, slippage_bps: u16) -> Result<U256> {
    ensure!(slippage_bps < 10_000, "slippage_bps must be below 10000");
    let scale = U256::from(10_000);
    let retained = U256::from(10_000_u16 - slippage_bps);
    let minimum = (amount_out / scale) * retained + ((amount_out % scale) * retained) / scale;
    ensure!(
        minimum != U256::ZERO,
        "slippage produces a zero min_amount_out"
    );
    Ok(minimum)
}

/// Build a transaction for the best route. When `min_amount_out` is omitted,
/// the second route's output becomes the final-output floor.
pub fn build_executable_swap(
    registry: &AdapterRegistry,
    routes: &[RouteQuote],
    request: ExecutionRequest,
) -> Result<ExecutableSwap> {
    validate_request(&request)?;
    let quote = routes
        .first()
        .context("cannot execute an empty route set")?;
    validate_quote(quote)?;
    let first_hop = quote
        .path
        .hops
        .first()
        .context("cannot execute an empty route")?;
    let final_hop = quote
        .path
        .hops
        .last()
        .context("cannot execute an empty route")?;
    let min_amount_out = match request.min_amount_out {
        Some(minimum) => minimum,
        None => {
            let fallback = routes
                .get(1)
                .context("min_amount_out is required when no second route is available")?;
            validate_quote(fallback).context("invalid second route used for min_amount_out")?;
            ensure!(
                fallback.amount_in == quote.amount_in,
                "second route amount_in does not match the best route"
            );
            ensure!(
                fallback.path.hops.first().map(|hop| hop.token_in) == Some(first_hop.token_in)
                    && fallback.path.hops.last().map(|hop| hop.token_out)
                        == Some(final_hop.token_out),
                "second route token pair does not match the best route"
            );
            ensure!(
                quote.amount_out >= fallback.amount_out,
                "routes are not ordered best-first"
            );
            fallback.amount_out
        }
    };
    ensure!(
        min_amount_out != U256::ZERO,
        "min_amount_out must be non-zero"
    );

    let mut packed = Vec::with_capacity(83);
    packed.extend_from_slice(final_hop.token_out.as_slice());
    for hop in &quote.path.hops {
        let registration = registry
            .pool(&hop.pool)
            .with_context(|| format!("missing pool registration for {:?}", hop.pool))?;
        let (protocol, endpoint, extra) = match (&hop.pool, &registration.metadata) {
            (PoolKey::UniswapV2(pool), ProtocolMetadata::UniswapV2(metadata)) => {
                let fee_bps = metadata
                    .fee_bps
                    .context("Uniswap V2 execution requires configured fee_bps")?;
                ensure!(fee_bps < 10_000, "Uniswap V2 fee_bps must be below 10000");
                let fee_bps =
                    u16::try_from(fee_bps).context("Uniswap V2 fee_bps overflows uint16")?;
                (0, *pool, fee_bps.to_be_bytes().to_vec())
            }
            (PoolKey::UniswapV3(pool), ProtocolMetadata::UniswapV3(_)) => (1, *pool, Vec::new()),
            (PoolKey::PancakeV3(pool), ProtocolMetadata::PancakeV3(_)) => (2, *pool, Vec::new()),
            (PoolKey::Slipstream(pool), ProtocolMetadata::Slipstream(_)) => (3, *pool, Vec::new()),
            (PoolKey::SolidlyV2(pool), ProtocolMetadata::SolidlyV2(_)) => (4, *pool, Vec::new()),
            (PoolKey::BalancerV2(pool_id), ProtocolMetadata::BalancerV2(metadata)) => {
                let vault = metadata
                    .vault
                    .or_else(|| registration.state_addresses.first().copied())
                    .context("Balancer V2 execution requires a vault address")?;
                (5, vault, pool_id.as_slice().to_vec())
            }
            (PoolKey::Curve(pool), ProtocolMetadata::Curve(metadata)) => {
                let input_index = metadata
                    .coins
                    .iter()
                    .position(|coin| *coin == hop.token_in)
                    .context("Curve token_in is absent from pool metadata")?;
                let output_index = metadata
                    .coins
                    .iter()
                    .position(|coin| *coin == hop.token_out)
                    .context("Curve token_out is absent from pool metadata")?;
                ensure!(
                    input_index != output_index,
                    "Curve hop uses the same coin twice"
                );
                let input_index =
                    u8::try_from(input_index).context("Curve input index overflows uint8")?;
                let output_index =
                    u8::try_from(output_index).context("Curve output index overflows uint8")?;
                let protocol = match metadata.variant {
                    CurveVariant::StableSwap => 6,
                    CurveVariant::CryptoSwap => 7,
                    CurveVariant::CryptoSwapNG => 8,
                    _ => bail!("unsupported Curve variant for executor router"),
                };
                (protocol, *pool, vec![input_index, output_index])
            }
            (PoolKey::Custom(_), ProtocolMetadata::Custom(_)) => {
                bail!("custom graph adapters are not supported by the executor router")
            }
            _ => bail!(
                "unsupported or mismatched execution metadata for {:?}",
                hop.pool
            ),
        };
        packed.push(protocol);
        packed.extend_from_slice(endpoint.as_slice());
        packed.extend_from_slice(hop.token_in.as_slice());
        packed.extend_from_slice(hop.token_out.as_slice());
        packed.extend_from_slice(&extra);
    }

    let params = router_abi::ExactInputParams {
        tokenIn: first_hop.token_in,
        tokenOut: final_hop.token_out,
        amountIn: quote.amount_in,
        minAmountOut: min_amount_out,
        recipient: request.recipient,
        deadline: request.deadline,
        route: Bytes::from(packed),
    };
    let (data, approval, value) = match request.authorization {
        InputAuthorization::Allowance => (
            Bytes::from(router_abi::executeExactInputCall { params }.abi_encode()),
            Some(ApprovalRequirement {
                token: first_hop.token_in,
                spender: request.deployment.router,
                minimum_amount: quote.amount_in,
            }),
            U256::ZERO,
        ),
        InputAuthorization::Erc2612 { v, r, s } => (
            Bytes::from(
                router_abi::executeExactInputWithPermitCall {
                    params,
                    permit: router_abi::ERC2612Permit { v, r, s },
                }
                .abi_encode(),
            ),
            None,
            U256::ZERO,
        ),
        InputAuthorization::Permit2 {
            nonce,
            deadline,
            signature,
        } => (
            Bytes::from(
                router_abi::executeExactInputWithPermit2Call {
                    params,
                    permit: router_abi::Permit2SignatureTransfer {
                        nonce,
                        deadline,
                        signature,
                    },
                }
                .abi_encode(),
            ),
            Some(ApprovalRequirement {
                token: first_hop.token_in,
                spender: request.deployment.permit2,
                minimum_amount: quote.amount_in,
            }),
            U256::ZERO,
        ),
        InputAuthorization::Native => {
            ensure!(
                first_hop.token_in == request.deployment.weth,
                "native input requires the route token_in to equal configured WETH"
            );
            (
                Bytes::from(router_abi::executeExactInputNativeCall { params }.abi_encode()),
                None,
                quote.amount_in,
            )
        }
    };
    Ok(ExecutableSwap {
        to: request.deployment.router,
        value,
        data,
        approval,
        min_amount_out,
        deadline: request.deadline,
    })
}

fn validate_request(request: &ExecutionRequest) -> Result<()> {
    ensure!(
        request.deployment.router != Address::ZERO,
        "router must be non-zero"
    );
    ensure!(
        request.deployment.weth != Address::ZERO,
        "WETH must be non-zero"
    );
    ensure!(
        request.deployment.permit2 != Address::ZERO,
        "Permit2 must be non-zero"
    );
    ensure!(
        request.recipient != Address::ZERO,
        "recipient must be non-zero"
    );
    ensure!(request.deadline != U256::ZERO, "deadline must be non-zero");
    if let InputAuthorization::Permit2 { deadline, .. } = &request.authorization {
        ensure!(*deadline != U256::ZERO, "Permit2 deadline must be non-zero");
    }
    Ok(())
}

fn validate_quote(quote: &RouteQuote) -> Result<()> {
    ensure!(
        quote.amount_in != U256::ZERO,
        "route amount_in must be non-zero"
    );
    ensure!(
        quote.path.hops.len() == quote.hops.len(),
        "route path and quote trace lengths differ"
    );
    let mut expected_token = None;
    let mut expected_amount = quote.amount_in;
    for (path_hop, quoted_hop) in quote.path.hops.iter().zip(&quote.hops) {
        ensure!(
            path_hop == &quoted_hop.hop,
            "route path and quote trace hops differ"
        );
        if let Some(expected_token) = expected_token {
            ensure!(
                path_hop.token_in == expected_token,
                "route token continuity is broken"
            );
        }
        ensure!(
            quoted_hop.amount_in == expected_amount,
            "route amount continuity is broken"
        );
        expected_token = Some(path_hop.token_out);
        expected_amount = quoted_hop.amount_out;
    }
    ensure!(
        expected_amount == quote.amount_out,
        "route final amount does not match its quote trace"
    );
    Ok(())
}

mod router_abi {
    #![allow(non_snake_case)]

    use alloy_sol_types::sol;

    sol! {
        struct ExactInputParams {
            address tokenIn;
            address tokenOut;
            uint256 amountIn;
            uint256 minAmountOut;
            address recipient;
            uint256 deadline;
            bytes route;
        }

        function executeExactInput(ExactInputParams params) returns (uint256 amountOut);
        function executeExactInputNative(ExactInputParams params) payable returns (uint256 amountOut);

        struct ERC2612Permit {
            uint8 v;
            bytes32 r;
            bytes32 s;
        }

        function executeExactInputWithPermit(ExactInputParams params, ERC2612Permit permit)
            returns (uint256 amountOut);

        struct Permit2SignatureTransfer {
            uint256 nonce;
            uint256 deadline;
            bytes signature;
        }

        function executeExactInputWithPermit2(
            ExactInputParams params,
            Permit2SignatureTransfer permit
        ) returns (uint256 amountOut);
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::address;
    use alloy_sol_types::SolCall;
    use evm_amm_search::{Hop, HopQuote, RoutePath};
    use evm_amm_state::adapters::{
        BalancerV2Metadata, CurveMetadata, CurveVariant, CustomPoolKey, PoolKey, PoolRegistration,
        ProtocolMetadata, SolidlyV2Metadata, UniswapV2Metadata, V3Metadata,
    };
    use std::sync::Arc;

    use super::*;

    #[test]
    fn allowance_v2_quote_becomes_executor_calldata() {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        let pool = address!("0000000000000000000000000000000000000033");
        let router = address!("0000000000000000000000000000000000000044");
        let recipient = address!("0000000000000000000000000000000000000055");
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(PoolKey::UniswapV2(pool)).with_metadata(
                    ProtocolMetadata::UniswapV2(
                        UniswapV2Metadata::default()
                            .with_token0(token_in)
                            .with_token1(token_out)
                            .with_fee_bps(100),
                    ),
                ),
            )
            .unwrap();
        let best = quote(
            PoolKey::UniswapV2(pool),
            token_in,
            token_out,
            U256::from(1_000),
            U256::from(900),
        );
        let request = ExecutionRequest {
            deployment: ExecutorDeployment {
                router,
                weth: address!("0000000000000000000000000000000000000066"),
                permit2: address!("0000000000000000000000000000000000000077"),
            },
            recipient,
            deadline: U256::from(123_456),
            min_amount_out: Some(U256::from(850)),
            authorization: InputAuthorization::Allowance,
        };

        let swap = build_executable_swap(&registry, &[best], request).unwrap();
        let call = router_abi::executeExactInputCall::abi_decode(&swap.data).unwrap();

        assert_eq!(swap.to, router);
        assert_eq!(swap.value, U256::ZERO);
        assert_eq!(call.params.tokenIn, token_in);
        assert_eq!(call.params.tokenOut, token_out);
        assert_eq!(call.params.amountIn, U256::from(1_000));
        assert_eq!(call.params.minAmountOut, U256::from(850));
        assert_eq!(call.params.recipient, recipient);
        assert_eq!(call.params.deadline, U256::from(123_456));
        assert_eq!(call.params.route.len(), 83);
        assert_eq!(&call.params.route[..20], token_out.as_slice());
        assert_eq!(call.params.route[20], 0);
        assert_eq!(&call.params.route[21..41], pool.as_slice());
        assert_eq!(&call.params.route[41..61], token_in.as_slice());
        assert_eq!(&call.params.route[61..81], token_out.as_slice());
        assert_eq!(&call.params.route[81..83], &100_u16.to_be_bytes());
        assert_eq!(
            swap.approval,
            Some(ApprovalRequirement {
                token: token_in,
                spender: router,
                minimum_amount: U256::from(1_000),
            })
        );
    }

    #[test]
    fn omitted_min_out_uses_the_second_best_route() {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        let pool = address!("0000000000000000000000000000000000000033");
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(PoolKey::UniswapV2(pool)).with_metadata(
                    ProtocolMetadata::UniswapV2(
                        UniswapV2Metadata::default()
                            .with_token0(token_in)
                            .with_token1(token_out)
                            .with_fee_bps(30),
                    ),
                ),
            )
            .unwrap();
        let best = quote(
            PoolKey::UniswapV2(pool),
            token_in,
            token_out,
            U256::from(1_000),
            U256::from(900),
        );
        let second = quote(
            PoolKey::UniswapV2(pool),
            token_in,
            token_out,
            U256::from(1_000),
            U256::from(825),
        );
        let request = ExecutionRequest {
            deployment: deployment(),
            recipient: address!("0000000000000000000000000000000000000055"),
            deadline: U256::from(123_456),
            min_amount_out: None,
            authorization: InputAuthorization::Allowance,
        };

        let swap = build_executable_swap(&registry, &[best, second], request).unwrap();
        let call = router_abi::executeExactInputCall::abi_decode(&swap.data).unwrap();

        assert_eq!(swap.min_amount_out, U256::from(825));
        assert_eq!(call.params.minAmountOut, U256::from(825));
    }

    #[test]
    fn slippage_basis_points_resolve_to_a_final_output_floor() {
        let minimum = min_amount_out_from_slippage(U256::from(1_000), 100).unwrap();

        assert_eq!(minimum, U256::from(990));
    }

    #[test]
    fn malformed_second_route_cannot_supply_the_default_minimum() {
        let (registry, best) = v2_fixture();
        let mut second = best.clone();
        second.amount_out = U256::from(825);
        second.hops[0].amount_in = U256::from(999);
        let mut execution_request = request();
        execution_request.min_amount_out = None;

        let error =
            build_executable_swap(&registry, &[best, second], execution_request).unwrap_err();

        assert!(format!("{error:#}").contains("route amount continuity"));
    }

    #[test]
    fn v3_family_quotes_use_their_executor_protocol_tags() {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        for (protocol, pool_key, metadata) in [
            (
                1_u8,
                PoolKey::UniswapV3(address!("0000000000000000000000000000000000000031")),
                ProtocolMetadata::UniswapV3(
                    V3Metadata::default()
                        .with_token0(token_in)
                        .with_token1(token_out)
                        .with_fee(500),
                ),
            ),
            (
                2_u8,
                PoolKey::PancakeV3(address!("0000000000000000000000000000000000000032")),
                ProtocolMetadata::PancakeV3(
                    V3Metadata::default()
                        .with_token0(token_in)
                        .with_token1(token_out)
                        .with_fee(500),
                ),
            ),
            (
                3_u8,
                PoolKey::Slipstream(address!("0000000000000000000000000000000000000033")),
                ProtocolMetadata::Slipstream(
                    V3Metadata::default()
                        .with_token0(token_in)
                        .with_token1(token_out)
                        .with_fee(500),
                ),
            ),
        ] {
            let pool = pool_key.address().unwrap();
            let mut registry = AdapterRegistry::new();
            registry
                .register_pool(PoolRegistration::new(pool_key.clone()).with_metadata(metadata))
                .unwrap();
            let best = quote(
                pool_key,
                token_in,
                token_out,
                U256::from(1_000),
                U256::from(900),
            );

            let swap = build_executable_swap(&registry, &[best], request()).unwrap();
            let call = router_abi::executeExactInputCall::abi_decode(&swap.data).unwrap();

            assert_eq!(call.params.route.len(), 81);
            assert_eq!(call.params.route[20], protocol);
            assert_eq!(&call.params.route[21..41], pool.as_slice());
        }
    }

    #[test]
    fn solidly_v2_quote_uses_pool_as_endpoint() {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        let pool = address!("0000000000000000000000000000000000000033");
        let key = PoolKey::SolidlyV2(pool);
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(key.clone()).with_metadata(ProtocolMetadata::SolidlyV2(
                    SolidlyV2Metadata::default()
                        .with_token0(token_in)
                        .with_token1(token_out)
                        .with_stable(false),
                )),
            )
            .unwrap();
        let best = quote(key, token_in, token_out, U256::from(1_000), U256::from(900));

        let swap = build_executable_swap(&registry, &[best], request()).unwrap();
        let call = router_abi::executeExactInputCall::abi_decode(&swap.data).unwrap();

        assert_eq!(call.params.route.len(), 81);
        assert_eq!(call.params.route[20], 4);
        assert_eq!(&call.params.route[21..41], pool.as_slice());
    }

    #[test]
    fn balancer_v2_quote_encodes_vault_and_pool_id() {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        let vault = address!("0000000000000000000000000000000000000033");
        let pool_id = B256::repeat_byte(0x44);
        let key = PoolKey::BalancerV2(pool_id);
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(key.clone()).with_metadata(ProtocolMetadata::BalancerV2(
                    BalancerV2Metadata::default()
                        .with_vault(vault)
                        .with_tokens([token_in, token_out]),
                )),
            )
            .unwrap();
        let best = quote(key, token_in, token_out, U256::from(1_000), U256::from(900));

        let swap = build_executable_swap(&registry, &[best], request()).unwrap();
        let call = router_abi::executeExactInputCall::abi_decode(&swap.data).unwrap();

        assert_eq!(call.params.route.len(), 113);
        assert_eq!(call.params.route[20], 5);
        assert_eq!(&call.params.route[21..41], vault.as_slice());
        assert_eq!(&call.params.route[81..113], pool_id.as_slice());
    }

    #[test]
    fn curve_family_quotes_encode_variant_and_coin_indices() {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        for (protocol, variant, pool) in [
            (
                6_u8,
                CurveVariant::StableSwap,
                address!("0000000000000000000000000000000000000031"),
            ),
            (
                7_u8,
                CurveVariant::CryptoSwap,
                address!("0000000000000000000000000000000000000032"),
            ),
            (
                8_u8,
                CurveVariant::CryptoSwapNG,
                address!("0000000000000000000000000000000000000033"),
            ),
        ] {
            let key = PoolKey::Curve(pool);
            let mut registry = AdapterRegistry::new();
            registry
                .register_pool(
                    PoolRegistration::new(key.clone()).with_metadata(ProtocolMetadata::Curve(
                        CurveMetadata::default()
                            .with_coins([token_out, token_in])
                            .with_variant(variant),
                    )),
                )
                .unwrap();
            let best = quote(key, token_in, token_out, U256::from(1_000), U256::from(900));

            let swap = build_executable_swap(&registry, &[best], request()).unwrap();
            let call = router_abi::executeExactInputCall::abi_decode(&swap.data).unwrap();

            assert_eq!(call.params.route.len(), 83);
            assert_eq!(call.params.route[20], protocol);
            assert_eq!(&call.params.route[21..41], pool.as_slice());
            assert_eq!(&call.params.route[81..83], &[1, 0]);
        }
    }

    #[test]
    fn custom_graph_adapters_are_not_executable() {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        let pool = address!("0000000000000000000000000000000000000033");
        let key = PoolKey::Custom(CustomPoolKey::Address {
            protocol: "demo-custom",
            address: pool,
        });
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(key.clone())
                    .with_metadata(ProtocolMetadata::Custom(Arc::new(()))),
            )
            .unwrap();
        let best = quote(
            key.clone(),
            token_in,
            token_out,
            U256::from(1_000),
            U256::from(900),
        );
        let error = build_executable_swap(&registry, &[best], request()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("custom graph adapters are not supported")
        );
    }

    #[test]
    fn erc2612_authorization_is_embedded_without_an_approval_requirement() {
        let (registry, best) = v2_fixture();
        let r = B256::repeat_byte(0x11);
        let s = B256::repeat_byte(0x22);
        let mut execution_request = request();
        execution_request.authorization = InputAuthorization::Erc2612 { v: 27, r, s };

        let swap = build_executable_swap(&registry, &[best], execution_request).unwrap();
        let call = router_abi::executeExactInputWithPermitCall::abi_decode(&swap.data).unwrap();

        assert_eq!(call.permit.v, 27);
        assert_eq!(call.permit.r, r);
        assert_eq!(call.permit.s, s);
        assert_eq!(call.params.amountIn, U256::from(1_000));
        assert_eq!(swap.approval, None);
        assert_eq!(swap.value, U256::ZERO);
    }

    #[test]
    fn permit2_signature_is_embedded_and_reports_the_permit2_approval() {
        let (registry, best) = v2_fixture();
        let signature = Bytes::from(vec![0xabu8; 65]);
        let mut execution_request = request();
        execution_request.authorization = InputAuthorization::Permit2 {
            nonce: U256::from(7),
            deadline: U256::from(222_222),
            signature: signature.clone(),
        };

        let swap = build_executable_swap(&registry, &[best], execution_request).unwrap();
        let call = router_abi::executeExactInputWithPermit2Call::abi_decode(&swap.data).unwrap();

        assert_eq!(call.permit.nonce, U256::from(7));
        assert_eq!(call.permit.deadline, U256::from(222_222));
        assert_eq!(call.permit.signature, signature);
        assert_eq!(
            swap.approval,
            Some(ApprovalRequirement {
                token: address!("0000000000000000000000000000000000000011"),
                spender: deployment().permit2,
                minimum_amount: U256::from(1_000),
            })
        );
        assert_eq!(swap.value, U256::ZERO);
    }

    #[test]
    fn native_authorization_wraps_the_exact_input_value() {
        let token_in = deployment().weth;
        let token_out = address!("0000000000000000000000000000000000000022");
        let pool = address!("0000000000000000000000000000000000000033");
        let key = PoolKey::UniswapV2(pool);
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(key.clone()).with_metadata(ProtocolMetadata::UniswapV2(
                    UniswapV2Metadata::default()
                        .with_token0(token_in)
                        .with_token1(token_out)
                        .with_fee_bps(30),
                )),
            )
            .unwrap();
        let best = quote(key, token_in, token_out, U256::from(1_000), U256::from(900));
        let mut execution_request = request();
        execution_request.authorization = InputAuthorization::Native;

        let swap = build_executable_swap(&registry, &[best], execution_request).unwrap();
        let call = router_abi::executeExactInputNativeCall::abi_decode(&swap.data).unwrap();

        assert_eq!(call.params.tokenIn, token_in);
        assert_eq!(call.params.amountIn, U256::from(1_000));
        assert_eq!(swap.value, U256::from(1_000));
        assert_eq!(swap.approval, None);
    }

    #[test]
    fn discontinuous_multihop_quote_is_rejected_before_calldata_is_built() {
        let token_a = address!("0000000000000000000000000000000000000011");
        let token_b = address!("0000000000000000000000000000000000000022");
        let token_c = address!("0000000000000000000000000000000000000033");
        let token_d = address!("0000000000000000000000000000000000000044");
        let pool_ab = address!("0000000000000000000000000000000000000051");
        let pool_cd = address!("0000000000000000000000000000000000000052");
        let first = Hop::new(PoolKey::UniswapV2(pool_ab), token_a, token_b);
        let second = Hop::new(PoolKey::UniswapV3(pool_cd), token_c, token_d);
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(first.pool.clone()).with_metadata(
                    ProtocolMetadata::UniswapV2(
                        UniswapV2Metadata::default()
                            .with_token0(token_a)
                            .with_token1(token_b)
                            .with_fee_bps(30),
                    ),
                ),
            )
            .unwrap();
        registry
            .register_pool(
                PoolRegistration::new(second.pool.clone()).with_metadata(
                    ProtocolMetadata::UniswapV3(
                        V3Metadata::default()
                            .with_token0(token_c)
                            .with_token1(token_d)
                            .with_fee(500),
                    ),
                ),
            )
            .unwrap();
        let malformed = RouteQuote {
            path: RoutePath::from_hops(vec![first.clone(), second.clone()]),
            amount_in: U256::from(1_000),
            amount_out: U256::from(800),
            hops: vec![
                HopQuote {
                    hop: first,
                    amount_in: U256::from(1_000),
                    amount_out: U256::from(900),
                },
                HopQuote {
                    hop: second,
                    amount_in: U256::from(900),
                    amount_out: U256::from(800),
                },
            ],
        };

        let error = build_executable_swap(&registry, &[malformed], request()).unwrap_err();

        assert!(error.to_string().contains("route token continuity"));
    }

    #[test]
    fn mixed_multihop_quote_preserves_protocol_boundaries() {
        let token_a = address!("0000000000000000000000000000000000000011");
        let token_b = address!("0000000000000000000000000000000000000022");
        let token_c = address!("0000000000000000000000000000000000000033");
        let pool_ab = address!("0000000000000000000000000000000000000051");
        let pool_bc = address!("0000000000000000000000000000000000000052");
        let first = Hop::new(PoolKey::UniswapV2(pool_ab), token_a, token_b);
        let second = Hop::new(PoolKey::UniswapV3(pool_bc), token_b, token_c);
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(first.pool.clone()).with_metadata(
                    ProtocolMetadata::UniswapV2(
                        UniswapV2Metadata::default()
                            .with_token0(token_a)
                            .with_token1(token_b)
                            .with_fee_bps(30),
                    ),
                ),
            )
            .unwrap();
        registry
            .register_pool(
                PoolRegistration::new(second.pool.clone()).with_metadata(
                    ProtocolMetadata::UniswapV3(
                        V3Metadata::default()
                            .with_token0(token_b)
                            .with_token1(token_c)
                            .with_fee(500),
                    ),
                ),
            )
            .unwrap();
        let quote = RouteQuote {
            path: RoutePath::from_hops(vec![first.clone(), second.clone()]),
            amount_in: U256::from(1_000),
            amount_out: U256::from(800),
            hops: vec![
                HopQuote {
                    hop: first,
                    amount_in: U256::from(1_000),
                    amount_out: U256::from(900),
                },
                HopQuote {
                    hop: second,
                    amount_in: U256::from(900),
                    amount_out: U256::from(800),
                },
            ],
        };

        let swap = build_executable_swap(&registry, &[quote], request()).unwrap();
        let call = router_abi::executeExactInputCall::abi_decode(&swap.data).unwrap();

        assert_eq!(call.params.route.len(), 144);
        assert_eq!(&call.params.route[..20], token_c.as_slice());
        assert_eq!(call.params.route[20], 0);
        assert_eq!(&call.params.route[81..83], &30_u16.to_be_bytes());
        assert_eq!(call.params.route[83], 1);
        assert_eq!(&call.params.route[84..104], pool_bc.as_slice());
        assert_eq!(&call.params.route[104..124], token_b.as_slice());
        assert_eq!(&call.params.route[124..144], token_c.as_slice());
    }

    #[test]
    fn zero_recipient_is_rejected_before_calldata_is_built() {
        let (registry, best) = v2_fixture();
        let mut execution_request = request();
        execution_request.recipient = Address::ZERO;

        let error = build_executable_swap(&registry, &[best], execution_request).unwrap_err();

        assert!(error.to_string().contains("recipient must be non-zero"));
    }

    fn quote(
        pool: PoolKey,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        amount_out: U256,
    ) -> RouteQuote {
        let hop = Hop::new(pool, token_in, token_out);
        RouteQuote {
            path: RoutePath::from_hops(vec![hop.clone()]),
            amount_in,
            amount_out,
            hops: vec![HopQuote {
                hop,
                amount_in,
                amount_out,
            }],
        }
    }

    fn deployment() -> ExecutorDeployment {
        ExecutorDeployment {
            router: address!("0000000000000000000000000000000000000044"),
            weth: address!("0000000000000000000000000000000000000066"),
            permit2: address!("0000000000000000000000000000000000000077"),
        }
    }

    fn request() -> ExecutionRequest {
        ExecutionRequest {
            deployment: deployment(),
            recipient: address!("0000000000000000000000000000000000000055"),
            deadline: U256::from(123_456),
            min_amount_out: Some(U256::from(850)),
            authorization: InputAuthorization::Allowance,
        }
    }

    fn v2_fixture() -> (AdapterRegistry, RouteQuote) {
        let token_in = address!("0000000000000000000000000000000000000011");
        let token_out = address!("0000000000000000000000000000000000000022");
        let pool = address!("0000000000000000000000000000000000000033");
        let key = PoolKey::UniswapV2(pool);
        let mut registry = AdapterRegistry::new();
        registry
            .register_pool(
                PoolRegistration::new(key.clone()).with_metadata(ProtocolMetadata::UniswapV2(
                    UniswapV2Metadata::default()
                        .with_token0(token_in)
                        .with_token1(token_out)
                        .with_fee_bps(30),
                )),
            )
            .unwrap();
        let route = quote(key, token_in, token_out, U256::from(1_000), U256::from(900));
        (registry, route)
    }
}
