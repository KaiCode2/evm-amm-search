use std::sync::Arc;

use alloy_network::AnyNetwork;
use alloy_provider::{Provider, ProviderBuilder};
use anyhow::{Context, Result};
use reqwest::Url;

/// Build the registry-backed HTTP provider shared by the networked examples.
#[allow(dead_code)]
pub fn http_provider(
    rpc_url: &str,
) -> Result<Arc<impl Provider<AnyNetwork> + Clone + 'static + use<>>> {
    let endpoint = Url::parse(rpc_url).context("parse RPC URL")?;
    let client = reqwest::Client::builder()
        .gzip(true)
        .build()
        .context("build gzip-enabled RPC client")?;
    Ok(Arc::new(
        ProviderBuilder::new_with_network::<AnyNetwork>().connect_reqwest(client, endpoint),
    ))
}

#[cfg(feature = "live-runtime")]
#[allow(dead_code)]
mod live {
    use std::sync::Arc;

    use alloy_consensus::Header as ConsensusHeader;
    use alloy_network::AnyNetwork;
    use alloy_primitives::{Address, B256, U256};
    use alloy_provider::RootProvider;
    use alloy_rpc_client::RpcClient;
    use alloy_rpc_types_eth::Header as RpcHeader;
    use alloy_transport::mock::Asserter;
    use anyhow::Result;
    use evm_amm_search::{
        GraphBuildOptions, LiveRouteRuntime, LiveRouteRuntimeConfig, LiveRouteRuntimeHandle,
        LiveRouteSubscription, RouteSubscriptionState, VersionedRouteQuote,
    };
    use evm_amm_state::adapters::{
        AdapterCache, AdapterRegistry, AmmAdapter, AmmPreparedPoolState, AmmPreparedStorage,
        AmmRuntime, AmmRuntimeBaseline, AmmRuntimeConfig, AmmRuntimeHandle, AmmStateVersion,
        PoolKey, PoolRegistration, PoolStateDependencies, PoolStatus, ProtocolId, ProtocolMetadata,
        SimConfig, SimError, SwapQuote, UniswapV2Metadata,
    };
    use evm_fork_cache::cache::EvmCache;

    /// Deterministic providerless example adapter: every hop doubles its input.
    struct ExampleRouteAdapter;

    impl AmmAdapter for ExampleRouteAdapter {
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
            Ok(SwapQuote::new(amount_in * U256::from(2_u64)))
        }
    }

    fn header(number: u64) -> RpcHeader {
        RpcHeader::new(ConsensusHeader {
            parent_hash: B256::repeat_byte(0x69),
            number,
            timestamp: 1_700_000_000 + number,
            base_fee_per_gas: Some(100 + number),
            beneficiary: Address::repeat_byte(0xcb),
            gas_limit: 30_000_000,
            mix_hash: B256::repeat_byte(0xab),
            ..ConsensusHeader::default()
        })
    }

    /// Start empty AMM/search actors over an in-memory mock transport.
    pub async fn spawn_empty_system(
        block: u64,
    ) -> Result<(AmmRuntimeHandle, LiveRouteRuntimeHandle)> {
        let baseline = header(block);
        let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
        let mut cache = EvmCache::new(Arc::new(provider)).await;
        cache.advance_block(&baseline)?;

        let mut registry = AdapterRegistry::new();
        registry.register_adapter(Arc::new(ExampleRouteAdapter))?;
        let runtime = AmmRuntime::spawn(
            cache,
            registry,
            AmmRuntimeBaseline::from_verified_header(1, baseline)?,
            AmmRuntimeConfig::default(),
        )?;
        let routes = LiveRouteRuntime::spawn(
            &runtime,
            GraphBuildOptions::default(),
            LiveRouteRuntimeConfig::default().with_worker_threads(1),
        )
        .await?;
        Ok((runtime, routes))
    }

    pub fn ready_pool(pool: Address, token0: Address, token1: Address) -> PoolRegistration {
        PoolRegistration::new(PoolKey::UniswapV2(pool))
            .with_metadata(ProtocolMetadata::UniswapV2(
                UniswapV2Metadata::default()
                    .with_token0(token0)
                    .with_token1(token1)
                    .with_fee_bps(30),
            ))
            .with_status(PoolStatus::Ready)
    }

    /// Commit one independently prepared pool at the actor's current exact point.
    pub async fn commit_pool(
        runtime: &AmmRuntimeHandle,
        pool: PoolRegistration,
    ) -> Result<AmmStateVersion> {
        let prepared = AmmPreparedPoolState::new(
            pool,
            runtime.latest_snapshot().point(),
            std::iter::empty::<AmmPreparedStorage>(),
        )?;
        Ok(runtime.commit_prepared_pool(prepared).await?.version())
    }

    /// Wait until a subscription has completed against at least `version`.
    pub async fn wait_ready_at(
        subscription: &mut LiveRouteSubscription,
        version: AmmStateVersion,
    ) -> Result<Option<Arc<VersionedRouteQuote>>> {
        loop {
            let current = subscription.latest();
            if let RouteSubscriptionState::Ready { source, best, .. } = current.state()
                && source.state_version().get() >= version.get()
            {
                return Ok(best.clone());
            }
            subscription.changed().await?;
        }
    }
}

#[cfg(feature = "live-runtime")]
#[allow(unused_imports)]
pub use live::*;
