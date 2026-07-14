use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_network::{AnyNetwork, Ethereum, primitives::BlockResponse as _};
use alloy_primitives::{Address, B256, keccak256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_client::{ClientBuilder, RpcClient, WsConnect};
use alloy_transport_balancer::{
    BatchingConfig, BatchingTransport, EndpointConfig, HttpClientConfig, LoadBalancedTransport,
    Weight,
};
use anyhow::{Context, Result, anyhow, bail};
use evm_amm_search::{
    GraphBuildOptions, LiveAmmGraph, LiveRouteRuntime, LiveRouteRuntimeConfig,
    LiveRouteRuntimeHandle,
};
use evm_amm_state::adapters::{
    AdapterRegistry, AmmColdStartOptions, AmmColdStartWorkerConfig, AmmColdStartWorkerHandle,
    AmmDiscoveryOptions, AmmFactoryWatcherRegistration, AmmRuntime, AmmRuntimeBaseline,
    AmmRuntimeConfig, AmmRuntimeHandle, AmmSubscriberDriverConfig, AmmSubscriberDriverHandle,
    AmmWorkClass, ClFactorySpec, ConcentratedLiquidityAdapter, CurveAdapter, CurveMetadata,
    CurveVariant, DiscoveryOwnerId, DiscoveryOwnerKey, FactoryConfig as StateFactoryConfig,
    PoolDiscovery, PoolKey, PoolRegistration, ProtocolId, ProtocolMetadata, SimConfig,
    TokenEdgeDiscoveryRequest, UniswapV2Adapter, UniswapV2FactoryConfig, UniswapV2Metadata,
    V3Metadata,
};
use evm_fork_cache::{
    PreparedAccountPatch, PreparedAccountValue,
    bulk_storage::BulkCallConfig,
    cache::{AccountProof, CacheSpeedMode, EvmCache, StorageBatchConfig, StorageFetchStrategy},
    reactive::{AlloySubscriber, SubscriberConfig, SubscriberMode},
};
use futures::StreamExt;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{info, warn};
use url::Url;

use crate::{
    config::{FactoryConfig, PoolConfig, SidecarConfig, parse_address, parse_u256},
    coverage::{CoverageLedger, CoverageState, TokenCoverage},
};

#[derive(Clone, Debug, Default)]
pub struct PrepareTokenOptions {
    pub connectors: Vec<Address>,
    pub protocols: Vec<ProtocolId>,
    pub refresh: bool,
    pub wait: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeStatus {
    pub ready: bool,
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub state_version: u64,
    pub runtime_health: String,
    pub graph_tokens: usize,
    pub graph_edges: usize,
    pub graph_pools: usize,
    pub active_work: usize,
    pub queued_work: usize,
    pub profile_fingerprint: String,
}

/// Running state/search composition owned by the deployment sidecar.
pub struct RoutingNode {
    pub config: Arc<SidecarConfig>,
    pub amm: AmmRuntimeHandle,
    pub routes: LiveRouteRuntimeHandle,
    pub coverage: CoverageLedger,
    pub sim_config: SimConfig,
    discovery: Vec<(ProtocolId, DiscoveryOwnerId)>,
    subscriber: AmmSubscriberDriverHandle,
    cold_start: AmmColdStartWorkerHandle,
    prepare_locks: Mutex<std::collections::HashMap<Address, Arc<Mutex<()>>>>,
}

impl RoutingNode {
    /// Bootstrap a coherent AMM runtime at the verified latest block, hydrate the
    /// configured universe, then attach canonical websocket updates.
    pub async fn bootstrap(config: Arc<SidecarConfig>) -> Result<Arc<Self>> {
        info!("connecting canonical and state providers");
        let subscriber_provider = async {
            let client = ClientBuilder::default()
                .ws(WsConnect::new(config.rpc.canonical_ws.clone()))
                .await
                .context("connect canonical websocket")?;
            Ok::<_, anyhow::Error>(RootProvider::<Ethereum>::new(client))
        };
        let (state_provider, subscriber_provider) =
            tokio::try_join!(connect_state_provider(&config), subscriber_provider,)?;
        let subscriber_provider = Arc::new(subscriber_provider);

        let chain_id = subscriber_provider.get_chain_id().await?;
        if chain_id != config.chain.expected_chain_id {
            bail!(
                "RPC chain id {chain_id} does not match configured chain id {}",
                config.chain.expected_chain_id
            );
        }
        let latest_block = subscriber_provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await?
            .context("latest canonical block unavailable")?;
        let latest_header = latest_block.header().clone();
        let block_number = latest_header.inner.number;
        info!(block_number, block_hash = %latest_header.hash, "building coherent cache baseline");

        let mut cache = EvmCache::builder(Arc::clone(&state_provider))
            .block(BlockId::Number(BlockNumberOrTag::Number(block_number)))
            .chain_id(chain_id)
            .speed_mode(CacheSpeedMode::Fast)
            .build()
            .await;

        let factory_config = build_factory_config(&config.factories)?;
        let sim_config = SimConfig::default()
            .with_v2_router(config.chain.v2_router)
            .with_v3_quoter(config.chain.v3_quoter);
        let registry = build_registry(sim_config)?;
        let discovery = Arc::new(PoolDiscovery::for_registry(&registry, factory_config));

        let quote_targets = quote_targets(&config)?;
        let patch = prepare_quote_targets(
            state_provider.as_ref(),
            latest_header.hash,
            block_number,
            quote_targets,
        )
        .await?;
        cache.set_block(BlockId::from((latest_header.hash, Some(true))));
        cache.set_block_context(Some(block_number), latest_header.inner.base_fee_per_gas);
        cache
            .apply_prepared_account_patch(&patch)
            .context("install verified quote entrypoints")?;

        let baseline = AmmRuntimeBaseline::from_verified_header(chain_id, latest_header.clone())?;
        let amm = AmmRuntime::spawn(cache, registry, baseline, AmmRuntimeConfig::default())?;
        let cold_start_config = AmmColdStartWorkerConfig::default()
            .with_queue_capacity(config.discovery.max_startup_pools.max(256))
            .with_max_concurrency(config.rpc.cold_start_concurrency)
            .with_storage_batch_config(StorageBatchConfig::new(
                config.rpc.point_read_slots_per_batch,
                config.rpc.point_read_concurrency,
            ))
            .with_storage_fetch_strategy(StorageFetchStrategy::BulkCall(BulkCallConfig {
                max_slots_per_call: config.rpc.bulk_max_slots_per_call,
                max_slots_per_request: config.rpc.bulk_max_slots_per_request,
                max_request_bytes: config.rpc.bulk_max_request_bytes,
                max_concurrent_calls: config.rpc.bulk_max_concurrent_calls,
                ..BulkCallConfig::default()
            }));
        let cold_start = amm
            .attach_cold_start_worker(state_provider.as_ref().clone(), cold_start_config)
            .await?;

        let discovery_owners = install_factory_watchers(&amm, discovery).await?;
        let coverage = CoverageLedger::default();
        for token in &config.tokens {
            coverage.mark_configured(token.parsed_address()?).await;
        }

        let manual = manual_pools(&config)?;
        if !manual.is_empty() {
            amm.queue_cold_start(
                manual,
                AmmColdStartOptions::default().with_class(AmmWorkClass::Bootstrap),
            )
            .await
            .context("queue configured pools")?;
        }
        queue_configured_universe(&config, &amm, &discovery_owners, &coverage).await?;
        wait_for_runtime_idle(&amm, Duration::from_secs(180)).await?;
        refresh_configured_coverage(&config, &amm, &coverage).await?;

        let subscriber = AlloySubscriber::new(
            subscriber_provider.as_ref().clone(),
            SubscriberMode::Auto,
            SubscriberConfig {
                max_log_addresses_per_subscription: config.rpc.max_log_addresses_per_subscription,
                ..SubscriberConfig::default()
            },
        );
        let subscriber = amm
            .attach_alloy_subscriber(subscriber, AmmSubscriberDriverConfig::default())
            .await?;
        let routes = LiveRouteRuntime::spawn(
            &amm,
            GraphBuildOptions::default(),
            LiveRouteRuntimeConfig::default()
                .with_worker_threads(config.routing.route_worker_threads)
                .with_max_subscriptions(config.routing.max_subscriptions),
        )
        .await?;

        let node = Arc::new(Self {
            config,
            amm,
            routes,
            coverage,
            sim_config,
            discovery: discovery_owners,
            subscriber,
            cold_start,
            prepare_locks: Mutex::new(std::collections::HashMap::new()),
        });
        info!(?chain_id, block_number, "routing sidecar ready");
        Ok(node)
    }

    pub fn graph_contains(&self, token: Address) -> bool {
        graph_stats(&self.amm, Some(token))
            .map(|stats| stats.token_present)
            .unwrap_or(false)
    }

    pub async fn status(&self) -> Result<NodeStatus> {
        let snapshot = self.amm.latest_snapshot();
        let status = self.amm.latest_status();
        let graph = graph_stats(&self.amm, None)?;
        Ok(NodeStatus {
            ready: true,
            chain_id: snapshot.point().chain_id(),
            block_number: snapshot.point().block_number(),
            block_hash: format!("{:#x}", snapshot.point().block_hash()),
            state_version: snapshot.version().get(),
            runtime_health: format!("{:?}", status.health()).to_ascii_lowercase(),
            graph_tokens: graph.tokens,
            graph_edges: graph.edges,
            graph_pools: graph.pools,
            active_work: status.active_work_items().count(),
            queued_work: status.queue_depths().iter().map(|(_, depth)| depth).sum(),
            profile_fingerprint: format!("{:#x}", self.config.profile_fingerprint),
        })
    }

    /// Queue connector-focused discovery. Repeated ensure requests coalesce at
    /// the service ledger and runtime scheduler; refresh explicitly requeues.
    pub async fn prepare_token(
        self: &Arc<Self>,
        token: Address,
        options: PrepareTokenOptions,
    ) -> Result<TokenCoverage> {
        let token_lock = {
            let mut locks = self.prepare_locks.lock().await;
            Arc::clone(
                locks
                    .entry(token)
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _guard = token_lock.lock().await;

        let scoped_request = !options.connectors.is_empty() || !options.protocols.is_empty();
        let current = self.coverage.get(token).await;
        if !options.refresh
            && !scoped_request
            && matches!(
                current.state,
                CoverageState::Queued | CoverageState::Discovering | CoverageState::Ready
            )
        {
            return Ok(current);
        }
        if !options.refresh
            && !scoped_request
            && self
                .coverage
                .negative_is_fresh(token, self.config.discovery.negative_ttl)
                .await
        {
            return Ok(current);
        }

        let connectors = if options.connectors.is_empty() {
            self.config.connector_addresses()?
        } else {
            options.connectors
        };
        let connectors = connectors
            .into_iter()
            .filter(|connector| *connector != token)
            .collect::<Vec<_>>();
        if connectors.is_empty() {
            bail!("token preparation requires at least one connector");
        }
        let requested_protocols = if options.protocols.is_empty() {
            self.discovery
                .iter()
                .map(|(protocol, _)| *protocol)
                .collect::<HashSet<_>>()
        } else {
            options.protocols.into_iter().collect()
        };

        let mut accepted = 0usize;
        let mut protocols = Vec::new();
        let mut errors = Vec::new();
        for (protocol, owner) in &self.discovery {
            if !requested_protocols.contains(protocol) {
                continue;
            }
            match self
                .amm
                .queue_token_discovery(
                    owner.clone(),
                    TokenEdgeDiscoveryRequest::new(token, connectors.iter().copied())
                        .with_protocol(*protocol),
                    AmmDiscoveryOptions::default().with_class(AmmWorkClass::Focused),
                )
                .await
            {
                Ok(_) => {
                    accepted += 1;
                    protocols.push(protocol_name(*protocol).to_owned());
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        if accepted == 0 {
            let message = if errors.is_empty() {
                "none of the requested protocols are enabled".to_owned()
            } else {
                errors.join("; ")
            };
            self.coverage.mark_failed(token, &message).await;
            bail!(message);
        }
        self.coverage
            .mark_queued(token, protocols, connectors, accepted)
            .await;
        self.coverage.mark_discovering(token).await;

        if options.wait {
            wait_for_runtime_idle(
                &self.amm,
                self.config.routing.max_timeout + Duration::from_secs(30),
            )
            .await?;
            let stats = graph_stats(&self.amm, Some(token))?;
            self.coverage
                .mark_settled(token, stats.token_pools, stats.token_present)
                .await;
        } else {
            let node = Arc::clone(self);
            tokio::spawn(async move {
                let result = wait_for_runtime_idle(&node.amm, Duration::from_secs(180)).await;
                match result.and_then(|_| graph_stats(&node.amm, Some(token))) {
                    Ok(stats) => {
                        node.coverage
                            .mark_settled(token, stats.token_pools, stats.token_present)
                            .await;
                    }
                    Err(error) => node.coverage.mark_failed(token, error.to_string()).await,
                }
            });
        }
        Ok(self.coverage.get(token).await)
    }

    pub async fn token_coverage(&self, token: Address) -> Result<TokenCoverage> {
        let stats = graph_stats(&self.amm, Some(token))?;
        self.coverage
            .refresh_graph_state(token, stats.token_pools, stats.token_present)
            .await;
        Ok(self.coverage.get(token).await)
    }

    pub async fn shutdown(&self) {
        if let Err(error) = self.routes.shutdown().await {
            warn!(%error, "route runtime shutdown failed");
        }
        if let Err(error) = self.subscriber.shutdown().await {
            warn!(%error, "subscriber shutdown failed");
        }
        self.cold_start.shutdown();
        if let Err(error) = self.amm.shutdown().await {
            warn!(%error, "AMM runtime shutdown failed");
        }
    }
}

#[derive(Default)]
struct GraphStats {
    tokens: usize,
    edges: usize,
    pools: usize,
    token_present: bool,
    token_pools: usize,
}

fn graph_stats(amm: &AmmRuntimeHandle, token: Option<Address>) -> Result<GraphStats> {
    let snapshot = amm.latest_snapshot();
    let live = LiveAmmGraph::from_snapshot(&snapshot, GraphBuildOptions::default())?;
    let graph = live.graph();
    let mut all_pools = HashSet::new();
    let mut token_pools = HashSet::new();
    for edge in graph.graph().edge_references() {
        all_pools.insert(edge.weight().pool.clone());
        if token.is_some_and(|token| {
            graph.node_token(edge.source()) == Some(token)
                || graph.node_token(edge.target()) == Some(token)
        }) {
            token_pools.insert(edge.weight().pool.clone());
        }
    }
    Ok(GraphStats {
        tokens: graph.node_count(),
        edges: graph.edge_count(),
        pools: all_pools.len(),
        token_present: token.is_some_and(|token| graph.node_index(&token).is_some()),
        token_pools: token_pools.len(),
    })
}

async fn connect_state_provider(config: &SidecarConfig) -> Result<Arc<RootProvider<AnyNetwork>>> {
    if config.rpc.state.is_empty() {
        return Ok(Arc::new(
            RootProvider::<AnyNetwork>::connect(&config.rpc.canonical_ws)
                .await
                .context("connect state websocket")?,
        ));
    }
    let endpoints = config
        .rpc
        .state
        .iter()
        .map(|endpoint| {
            let mut configured =
                EndpointConfig::new(Url::parse(&endpoint.url)?, Weight(endpoint.weight));
            configured.max_request_bytes = endpoint.max_request_bytes;
            configured.max_in_flight = endpoint.max_in_flight;
            Ok(configured)
        })
        .collect::<Result<Vec<_>>>()?;
    let transport = LoadBalancedTransport::builder_with_endpoints(endpoints)
        .http_client_config(HttpClientConfig {
            gzip: true,
            ..Default::default()
        })
        .build();
    let transport = BatchingTransport::new(
        transport,
        BatchingConfig {
            max_batch_size: config.rpc.batch_size,
            ..Default::default()
        },
    );
    Ok(Arc::new(RootProvider::<AnyNetwork>::new(RpcClient::new(
        transport, false,
    ))))
}

fn build_registry(sim_config: SimConfig) -> Result<AdapterRegistry> {
    let mut registry = AdapterRegistry::new().with_sim_config(sim_config);
    registry.register_adapter(Arc::new(UniswapV2Adapter::default()))?;
    registry.register_adapter(Arc::new(ConcentratedLiquidityAdapter::default()))?;
    registry.register_adapter(Arc::new(CurveAdapter::default()))?;
    Ok(registry)
}

fn build_factory_config(factories: &[FactoryConfig]) -> Result<StateFactoryConfig> {
    let mut output = StateFactoryConfig::default().with_verify_derivations(false);
    for factory in factories {
        let address = factory.parsed_address()?;
        match factory.normalized_protocol().as_str() {
            "uniswap-v2" => {
                let mut spec = UniswapV2FactoryConfig::uniswap_v2(address);
                if let Some(fee) = factory.fee_bps {
                    spec = spec.with_fee_bps(fee);
                }
                if let Some(slot) = &factory.get_pair_base_slot {
                    spec = spec.with_get_pair_base_slot(parse_u256(slot)?);
                }
                if let Some(hash) = &factory.init_code_hash {
                    spec = spec.with_init_code_hash(B256::from_str(hash)?);
                }
                output = output.with_uniswap_v2(spec);
            }
            "uniswap-v3" | "sushi-v3" | "pancake-v3" => {
                let mut spec = match factory.normalized_protocol().as_str() {
                    "uniswap-v3" => ClFactorySpec::uniswap_v3(address),
                    "sushi-v3" => ClFactorySpec::sushi_v3(address),
                    "pancake-v3" => ClFactorySpec::pancake_v3(address),
                    _ => unreachable!(),
                };
                if let Some(quoter) = &factory.quoter {
                    spec = spec.with_quoter(parse_address(quoter)?);
                }
                output = output.with_concentrated_liquidity(spec);
            }
            protocol => bail!("unsupported factory protocol {protocol}"),
        }
    }
    Ok(output)
}

async fn install_factory_watchers(
    amm: &AmmRuntimeHandle,
    discovery: Arc<PoolDiscovery>,
) -> Result<Vec<(ProtocolId, DiscoveryOwnerId)>> {
    let adapters = amm
        .latest_snapshot()
        .registry()
        .adapters()
        .map(|(key, instance)| (key.clone(), instance.clone()))
        .collect::<Vec<_>>();
    let mut owners = Vec::new();
    for (index, (key, adapter)) in adapters.into_iter().enumerate() {
        let supported = key
            .protocols()
            .iter()
            .copied()
            .filter(|protocol| {
                matches!(
                    protocol,
                    ProtocolId::UniswapV2 | ProtocolId::UniswapV3 | ProtocolId::PancakeV3
                )
            })
            .collect::<Vec<_>>();
        if supported.is_empty() {
            continue;
        }
        let owner = amm
            .add_factory_watcher(AmmFactoryWatcherRegistration::new(
                DiscoveryOwnerKey::new(format!("amm-route-sidecar-factories-{index}")),
                adapter,
                Arc::clone(&discovery),
            ))
            .await?;
        owners.extend(
            supported
                .into_iter()
                .map(|protocol| (protocol, owner.clone())),
        );
    }
    Ok(owners)
}

async fn queue_configured_universe(
    config: &SidecarConfig,
    amm: &AmmRuntimeHandle,
    owners: &[(ProtocolId, DiscoveryOwnerId)],
    coverage: &CoverageLedger,
) -> Result<()> {
    let connectors = config.connector_addresses()?;
    let jobs = config.tokens.len().saturating_mul(owners.len()).max(1);
    let quota = (config.discovery.max_startup_pools / jobs).max(1);
    for token in &config.tokens {
        let token = token.parsed_address()?;
        let token_connectors = connectors
            .iter()
            .copied()
            .filter(|connector| *connector != token)
            .collect::<Vec<_>>();
        let mut accepted = 0;
        let mut protocols = Vec::new();
        for (protocol, owner) in owners {
            if token_connectors.is_empty() {
                continue;
            }
            if amm
                .queue_token_discovery(
                    owner.clone(),
                    TokenEdgeDiscoveryRequest::new(token, token_connectors.iter().copied())
                        .with_protocol(*protocol),
                    AmmDiscoveryOptions::default()
                        .with_class(AmmWorkClass::Bootstrap)
                        .with_max_candidates(quota),
                )
                .await
                .is_ok()
            {
                accepted += 1;
                protocols.push(protocol_name(*protocol).to_owned());
            }
        }
        coverage
            .mark_queued(token, protocols, token_connectors, accepted)
            .await;
        coverage.mark_discovering(token).await;
    }
    Ok(())
}

async fn refresh_configured_coverage(
    config: &SidecarConfig,
    amm: &AmmRuntimeHandle,
    coverage: &CoverageLedger,
) -> Result<()> {
    for token in &config.tokens {
        let token = token.parsed_address()?;
        let stats = graph_stats(amm, Some(token))?;
        coverage
            .mark_settled(token, stats.token_pools, stats.token_present)
            .await;
    }
    Ok(())
}

async fn wait_for_runtime_idle(amm: &AmmRuntimeHandle, timeout: Duration) -> Result<()> {
    let mut status = amm.subscribe_status();
    tokio::time::timeout(timeout, async {
        loop {
            let current = status.borrow_and_update().clone();
            let active = current.active_work_items().next().is_some();
            let queued = current.queue_depths().iter().any(|(_, depth)| depth > 0);
            if !active && !queued {
                break;
            }
            status
                .changed()
                .await
                .map_err(|_| anyhow!("AMM runtime closed while waiting for discovery"))?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for AMM discovery")??;
    Ok(())
}

fn manual_pools(config: &SidecarConfig) -> Result<Vec<PoolRegistration>> {
    config
        .pools
        .iter()
        .map(|pool| manual_pool(pool, config))
        .collect()
}

fn manual_pool(pool: &PoolConfig, config: &SidecarConfig) -> Result<PoolRegistration> {
    let address = parse_address(&pool.address)?;
    let tokens = pool
        .tokens
        .iter()
        .map(|token| parse_address(token))
        .collect::<Result<Vec<_>>>()?;
    let protocol = pool.normalized_protocol();
    match protocol.as_str() {
        "uniswap_v2" | "sushiswap_v2" | "v2" => {
            let mut metadata =
                UniswapV2Metadata::default().with_fee_bps(pool.fee_bps.unwrap_or(30));
            if tokens.len() == 2 {
                metadata = metadata.with_token0(tokens[0]).with_token1(tokens[1]);
            } else if !tokens.is_empty() {
                bail!(
                    "manual V2 pool {} must list exactly two tokens",
                    pool.address
                );
            }
            Ok(PoolRegistration::new(PoolKey::UniswapV2(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::UniswapV2(metadata)))
        }
        "uniswap_v3" | "sushi_v3" | "v3" | "pancake_v3" | "pancakeswap_v3" => {
            let fee = pool
                .fee
                .or_else(|| pool.fee_bps.map(|fee| fee.saturating_mul(100)))
                .ok_or_else(|| anyhow!("manual V3 pool {} must set fee", pool.address))?;
            let mut metadata = V3Metadata::default().with_fee(fee);
            if tokens.len() == 2 {
                metadata = metadata.with_token0(tokens[0]).with_token1(tokens[1]);
            } else if !tokens.is_empty() {
                bail!(
                    "manual V3 pool {} must list exactly two tokens",
                    pool.address
                );
            }
            let factory_config = config.factories.iter().find(|factory| {
                let factory_protocol = factory.normalized_protocol().replace('-', "_");
                factory_protocol == protocol
                    || (protocol == "v3" && factory_protocol == "uniswap_v3")
                    || (protocol == "pancakeswap_v3" && factory_protocol == "pancake_v3")
            });
            let factory = pool
                .factory
                .as_deref()
                .map(parse_address)
                .transpose()?
                .or_else(|| factory_config.and_then(|factory| factory.parsed_address().ok()));
            let quoter = pool
                .quoter
                .as_deref()
                .map(parse_address)
                .transpose()?
                .or_else(|| {
                    factory_config
                        .and_then(|factory| factory.quoter.as_deref())
                        .and_then(|quoter| parse_address(quoter).ok())
                })
                .or((!protocol.starts_with("pancake") && protocol != "sushi_v3")
                    .then_some(config.chain.v3_quoter));
            if let Some(factory) = factory {
                metadata = metadata.with_factory(factory);
            }
            if let Some(quoter) = quoter {
                metadata = metadata.with_quoter(quoter);
            }
            if protocol.starts_with("pancake") {
                Ok(PoolRegistration::new(PoolKey::PancakeV3(address))
                    .with_state_address(address)
                    .with_metadata(ProtocolMetadata::PancakeV3(metadata)))
            } else {
                Ok(PoolRegistration::new(PoolKey::UniswapV3(address))
                    .with_state_address(address)
                    .with_metadata(ProtocolMetadata::UniswapV3(metadata)))
            }
        }
        "curve" | "curve_stable" | "curve_crypto" | "curve_crypto_ng" => {
            if tokens.len() < 2 {
                bail!(
                    "manual Curve pool {} must list at least two tokens",
                    pool.address
                );
            }
            let variant = match pool.variant.as_deref().unwrap_or("stable") {
                "stable" | "stableswap" => CurveVariant::StableSwap,
                "crypto" | "cryptoswap" => CurveVariant::CryptoSwap,
                "crypto_ng" | "cryptoswap_ng" | "ng" => CurveVariant::CryptoSwapNG,
                variant => bail!("unsupported Curve variant {variant}"),
            };
            let slots = pool
                .discovered_slots
                .iter()
                .map(|slot| parse_u256(slot))
                .collect::<Result<Vec<_>>>()?;
            Ok(PoolRegistration::new(PoolKey::Curve(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::Curve(
                    CurveMetadata::default()
                        .with_coins(tokens)
                        .with_variant(variant)
                        .with_discovered_slots(slots),
                )))
        }
        protocol => bail!("unsupported manual pool protocol {protocol}"),
    }
}

fn quote_targets(config: &SidecarConfig) -> Result<Vec<Address>> {
    let mut targets = vec![config.chain.v2_router, config.chain.v3_quoter];
    for factory in &config.factories {
        if let Some(quoter) = &factory.quoter {
            targets.push(parse_address(quoter)?);
        }
    }
    targets.sort_unstable();
    targets.dedup();
    Ok(targets)
}

async fn prepare_quote_targets(
    provider: &RootProvider<AnyNetwork>,
    block_hash: B256,
    block_number: u64,
    targets: impl IntoIterator<Item = Address>,
) -> Result<PreparedAccountPatch> {
    let block = BlockId::from((block_hash, Some(true)));
    let values = futures::stream::iter(targets)
        .map(|address| async move {
            let (code, proof) = tokio::try_join!(
                provider.get_code_at(address).block_id(block),
                provider.get_proof(address, Vec::new()).block_id(block),
            )
            .with_context(|| format!("fetch quote entrypoint {address}"))?;
            if code.is_empty() {
                bail!("quote entrypoint {address} has no runtime code");
            }
            let actual = keccak256(&code);
            if actual != proof.code_hash {
                bail!(
                    "quote entrypoint {address} code hash mismatch: code={actual}, proof={}",
                    proof.code_hash
                );
            }
            Ok(PreparedAccountValue::new(
                address,
                AccountProof {
                    storage_hash: proof.storage_hash,
                    balance: proof.balance,
                    nonce: proof.nonce,
                    code_hash: proof.code_hash,
                    slots: Vec::new(),
                },
                code,
            ))
        })
        .buffer_unordered(4)
        .collect::<Vec<Result<PreparedAccountValue>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedAccountPatch::new(block_hash, block_number, values))
}

pub fn parse_protocol(value: &str) -> Result<ProtocolId> {
    match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "uniswap_v2" | "v2" => Ok(ProtocolId::UniswapV2),
        "uniswap_v3" | "sushi_v3" | "v3" => Ok(ProtocolId::UniswapV3),
        "pancake_v3" | "pancakeswap_v3" => Ok(ProtocolId::PancakeV3),
        value => bail!("unsupported discovery protocol {value}"),
    }
}

pub fn protocol_name(protocol: ProtocolId) -> &'static str {
    match protocol {
        ProtocolId::UniswapV2 => "uniswap_v2",
        ProtocolId::UniswapV3 => "uniswap_v3",
        ProtocolId::PancakeV3 => "pancake_v3",
        ProtocolId::Slipstream => "slipstream",
        ProtocolId::SolidlyV2 => "solidly_v2",
        ProtocolId::BalancerV2 => "balancer_v2",
        ProtocolId::Curve => "curve",
        ProtocolId::Custom(_) => "custom",
        _ => "unknown",
    }
}
