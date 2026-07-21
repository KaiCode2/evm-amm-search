use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_network::{AnyNetwork, Ethereum, Network, primitives::BlockResponse as _};
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_client::{ClientBuilder, RpcClient, WsConnect};
use alloy_rpc_types_eth::{TransactionInput, TransactionRequest};
use alloy_transport_balancer::{
    BatchingConfig, BatchingTransport, EndpointConfig, HttpClientConfig, LoadBalancedTransport,
    Weight,
};
use anyhow::{Context, Result, anyhow, bail};
use evm_amm_search::{
    GraphBuildOptions, LiveAmmGraph, LiveRouteObserver, LiveRouteObserverError, LiveRouteRuntime,
    LiveRouteRuntimeConfig, LiveRouteRuntimeEventKind, LiveRouteRuntimeHandle,
};
use evm_amm_state::adapters::{
    AdapterRegistry, AmmColdStartOptions, AmmColdStartWorkerConfig, AmmColdStartWorkerHandle,
    AmmDiscoveryOptions, AmmFactoryWatcherRegistration, AmmRuntime, AmmRuntimeBaseline,
    AmmRuntimeConfig, AmmRuntimeEventKind, AmmRuntimeHandle, AmmRuntimeHealth, AmmStateVersion,
    AmmSubscriberDriverConfig, AmmSubscriberDriverHandle, AmmSubscriberDriverState, AmmWorkClass,
    BalancerV2Adapter, BalancerV2Metadata, ClFactorySpec, ConcentratedLiquidityAdapter,
    CurveAdapter, CurveMetadata, CurveVariant, DiscoveryOwnerId, DiscoveryOwnerKey,
    FactoryConfig as StateFactoryConfig, PoolDiscovery, PoolKey, PoolRegistration, ProtocolId,
    ProtocolMetadata, SimConfig, SolidlyFactoryConfig, SolidlyStorageLayout, SolidlyV2Adapter,
    SolidlyV2Metadata, TokenEdgeDiscoveryRequest, UniswapV2Adapter, UniswapV2FactoryConfig,
    UniswapV2Metadata, V3Metadata, V3StorageLayout,
};
use evm_fork_cache::{
    PreparedAccountPatch, PreparedAccountValue,
    bulk_storage::BulkCallConfig,
    cache::{AccountProof, CacheSpeedMode, EvmCache, StorageBatchConfig, StorageFetchStrategy},
    reactive::{AlloySubscriber, SubscriberConfig, SubscriberMode, SubscriberReconnectConfig},
};
use futures::StreamExt;
use serde::Serialize;
use tokio::{
    sync::{Mutex, Notify, watch},
    task::JoinHandle,
};
use tracing::{error, info, warn};
use url::Url;

use crate::{
    SERVICE_VERSION, SOURCE_REVISION,
    config::{FactoryConfig, PoolConfig, SidecarConfig, parse_address, parse_u256},
    coverage::{CoverageLedger, CoverageState, TokenCoverage},
    execution::{
        ApprovalRequirement, ExecutionApprovalCheckRequest, ExecutionApprovalState,
        ExecutionSimulation, ExecutionSimulationRequest,
    },
    graph_index::{GraphIndex, GraphStats},
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
    pub service_version: &'static str,
    pub source_revision: &'static str,
    pub ready: bool,
    pub routing_generation: u64,
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
    pub canonical_connection_state: String,
    pub canonical_endpoint_index: usize,
    pub canonical_endpoint_count: usize,
    pub canonical_age_ms: u64,
    pub canonical_max_stale_ms: u64,
    pub reconnect_attempts: u64,
    pub subscriber_state: String,
    pub last_recovery_error: Option<String>,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum QuoteReadinessError {
    #[error("canonical websocket recovery is in progress")]
    Reconnecting,
    #[error("canonical state is stale ({age_ms}ms old; maximum {max_age_ms}ms)")]
    Stale { age_ms: u64, max_age_ms: u64 },
    #[error("routing runtime is untrusted: {0}")]
    Untrusted(String),
    #[error("routing service is shutting down")]
    ShuttingDown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SupervisorPhase {
    Ready,
    Reconnecting,
    Untrusted,
    ShuttingDown,
}

impl SupervisorPhase {
    const fn name(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Reconnecting => "reconnecting",
            Self::Untrusted => "untrusted",
            Self::ShuttingDown => "shutting_down",
        }
    }
}

struct SupervisorState {
    phase: SupervisorPhase,
    generation: u64,
    endpoint_index: usize,
    reconnect_attempts: u64,
    last_progress: Instant,
    last_point: (u64, B256),
    last_status: NodeStatus,
    last_error: Option<String>,
}

/// Stable HTTP-facing owner for replaceable routing generations.
///
/// The dependency subscriber handles short disconnects and exact backfill. If
/// it becomes terminally failed, untrusted, or stale, this supervisor removes
/// that generation from quote traffic and builds a fresh verified runtime.
pub struct RoutingSupervisor {
    pub config: Arc<SidecarConfig>,
    active: RwLock<Option<Arc<RoutingNode>>>,
    state: RwLock<SupervisorState>,
    shutting_down: AtomicBool,
    shutdown_notify: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalBlockContext {
    pub source_timestamp: u64,
    pub head_timestamp: u64,
}

/// Running state/search composition owned by the deployment sidecar.
pub struct RoutingNode {
    pub config: Arc<SidecarConfig>,
    pub amm: AmmRuntimeHandle,
    pub routes: LiveRouteRuntimeHandle,
    graph_index: Arc<GraphIndexCache>,
    pub coverage: CoverageLedger,
    pub sim_config: SimConfig,
    execution_provider: Arc<RootProvider<Ethereum>>,
    discovery: Vec<(ProtocolId, DiscoveryOwnerId)>,
    subscriber: AmmSubscriberDriverHandle,
    cold_start: AmmColdStartWorkerHandle,
    prepare_locks: Mutex<std::collections::HashMap<Address, Arc<Mutex<()>>>>,
}

impl RoutingNode {
    /// Bootstrap a coherent AMM runtime at the verified latest block, hydrate the
    /// configured universe, then attach canonical websocket updates.
    pub async fn bootstrap(config: Arc<SidecarConfig>) -> Result<Arc<Self>> {
        let canonical_ws = config.rpc.canonical_ws.clone();
        Self::bootstrap_with_canonical(config, canonical_ws).await
    }

    async fn bootstrap_with_canonical(
        config: Arc<SidecarConfig>,
        canonical_ws: String,
    ) -> Result<Arc<Self>> {
        info!("connecting canonical and state providers");
        let subscriber_provider = async {
            let connection = WsConnect::new(canonical_ws.clone())
                .with_max_retries(config.rpc.canonical_transport_max_retries)
                .with_retry_interval(config.rpc.canonical_transport_retry_interval);
            let client = ClientBuilder::default()
                .ws(connection)
                .await
                .context("connect canonical websocket")?;
            Ok::<_, anyhow::Error>(RootProvider::<Ethereum>::new(client))
        };
        let (state_provider, subscriber_provider) = tokio::try_join!(
            connect_state_provider(&config, &canonical_ws),
            subscriber_provider,
        )?;
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
        verify_executor_deployment(&config, subscriber_provider.as_ref(), latest_header.hash)
            .await?;

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
        spawn_runtime_event_logger(&amm);
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

        let discovery_owners = if config.factories.is_empty() {
            Vec::new()
        } else {
            install_factory_watchers(&amm, discovery).await?
        };
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
        let graph_index = Arc::new(GraphIndexCache::from_amm(&amm)?);
        refresh_configured_coverage(&config, &graph_index, &coverage).await?;

        let subscriber = AlloySubscriber::new(
            subscriber_provider.as_ref().clone(),
            SubscriberMode::Auto,
            SubscriberConfig {
                max_log_addresses_per_subscription: config.rpc.max_log_addresses_per_subscription,
                reconnect: SubscriberReconnectConfig {
                    enabled: true,
                    initial_delay: config.rpc.canonical_stream_reconnect_initial_delay,
                    retry_delay: config.rpc.canonical_stream_reconnect_retry_delay,
                    max_delay: config.rpc.canonical_stream_reconnect_max_delay,
                    max_attempts: config.rpc.canonical_stream_reconnect_max_attempts,
                    dedupe_window: config.rpc.canonical_stream_dedupe_window,
                },
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
        let graph_events = routes.subscribe_events();
        graph_index.rebuild_from_amm(&amm)?;
        spawn_graph_index_updater(&amm, graph_events, Arc::clone(&graph_index));

        let node = Arc::new(Self {
            config,
            amm,
            routes,
            graph_index,
            coverage,
            sim_config,
            execution_provider: subscriber_provider,
            discovery: discovery_owners,
            subscriber,
            cold_start,
            prepare_locks: Mutex::new(std::collections::HashMap::new()),
        });
        info!(?chain_id, block_number, "routing sidecar ready");
        Ok(node)
    }

    pub async fn simulate_executor(
        &self,
        request: ExecutionSimulationRequest,
    ) -> Result<ExecutionSimulation> {
        let block = BlockId::from((request.block_hash, Some(true)));
        let min_amount_out = request.swap.min_amount_out;
        let transaction = TransactionRequest::default()
            .from(request.sender)
            .to(request.swap.to)
            .value(request.swap.value)
            .input(TransactionInput::from(request.swap.data));
        let call = self
            .execution_provider
            .call(transaction.clone())
            .block(block);
        let estimate = self
            .execution_provider
            .estimate_gas(transaction)
            .block(block);
        let (output, gas_estimate) = tokio::try_join!(call, estimate)
            .context("simulate and estimate executor transaction")?;
        let amount_out = decode_executor_output(&output)?;
        if amount_out < min_amount_out {
            bail!("simulated executor output {amount_out} is below minimum {min_amount_out}");
        }
        Ok(ExecutionSimulation {
            amount_out,
            gas_estimate,
        })
    }

    pub async fn canonical_block_context(
        &self,
        block_number: u64,
        block_hash: B256,
    ) -> Result<CanonicalBlockContext> {
        let source = self
            .execution_provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number));
        let head = self
            .execution_provider
            .get_block_by_number(BlockNumberOrTag::Latest);
        let (source, head) =
            tokio::try_join!(source, head).context("load canonical block context")?;
        let source =
            source.with_context(|| format!("canonical block {block_number} is unavailable"))?;
        let head = head.context("canonical head is unavailable")?;
        let source_header = source.header();
        if source_header.hash != block_hash {
            bail!(
                "quote block was reorganized: expected {block_hash:#x}, canonical block {block_number} is {:#x}",
                source_header.hash
            );
        }
        Ok(CanonicalBlockContext {
            source_timestamp: source_header.inner.timestamp,
            head_timestamp: head.header().inner.timestamp,
        })
    }

    pub async fn check_executor_approval(
        &self,
        request: ExecutionApprovalCheckRequest,
    ) -> Result<ExecutionApprovalState> {
        let block = BlockId::from((request.block_hash, Some(true)));
        let output = self
            .execution_provider
            .call(allowance_transaction(
                request.sender,
                request.approval.token,
                request.approval.spender,
            ))
            .block(block)
            .await
            .context("read prerequisite token allowance")?;
        let current_allowance = decode_abi_u256(&output, "token allowance")?;
        let gas_estimate = if current_allowance < request.approval.minimum_amount {
            Some(
                self.execution_provider
                    .estimate_gas(approval_transaction(request.sender, request.approval))
                    .block(block)
                    .await
                    .context("simulate prerequisite exact token approval")?,
            )
        } else {
            None
        };
        Ok(ExecutionApprovalState {
            current_allowance,
            gas_estimate,
        })
    }

    pub fn graph_contains(&self, token: Address) -> bool {
        self.graph_index.stats(Some(token)).token_present()
    }

    pub async fn status(&self) -> Result<NodeStatus> {
        let snapshot = self.amm.latest_snapshot();
        let status = self.amm.latest_status();
        let graph = self.graph_index.stats(None);
        let subscriber_state = self.subscriber.latest_state();
        let ready = !matches!(
            status.health(),
            AmmRuntimeHealth::Untrusted | AmmRuntimeHealth::ShuttingDown
        ) && matches!(subscriber_state, AmmSubscriberDriverState::Running { .. });
        Ok(NodeStatus {
            service_version: SERVICE_VERSION,
            source_revision: SOURCE_REVISION,
            ready,
            routing_generation: 0,
            chain_id: snapshot.point().chain_id(),
            block_number: snapshot.point().block_number(),
            block_hash: format!("{:#x}", snapshot.point().block_hash()),
            state_version: snapshot.version().get(),
            runtime_health: format!("{:?}", status.health()).to_ascii_lowercase(),
            graph_tokens: graph.tokens(),
            graph_edges: graph.edges(),
            graph_pools: graph.pools(),
            active_work: status.active_work_items().count(),
            queued_work: status.queue_depths().iter().map(|(_, depth)| depth).sum(),
            profile_fingerprint: format!("{:#x}", self.config.profile_fingerprint),
            canonical_connection_state: if ready { "ready" } else { "untrusted" }.to_owned(),
            canonical_endpoint_index: 0,
            canonical_endpoint_count: self.config.canonical_ws_endpoints().len(),
            canonical_age_ms: 0,
            canonical_max_stale_ms: duration_ms(self.config.rpc.canonical_max_stale),
            reconnect_attempts: 0,
            subscriber_state: subscriber_state_name(&subscriber_state),
            last_recovery_error: None,
        })
    }

    pub fn quote_readiness(&self) -> Result<u64, QuoteReadinessError> {
        match self.amm.latest_status().health() {
            AmmRuntimeHealth::Untrusted => {
                return Err(QuoteReadinessError::Untrusted(
                    "AMM runtime lost canonical trust".to_owned(),
                ));
            }
            AmmRuntimeHealth::ShuttingDown => return Err(QuoteReadinessError::ShuttingDown),
            AmmRuntimeHealth::Healthy | AmmRuntimeHealth::Degraded => {}
            _ => {
                return Err(QuoteReadinessError::Untrusted(
                    "AMM runtime reported an unsupported health state".to_owned(),
                ));
            }
        }
        match self.subscriber.latest_state() {
            AmmSubscriberDriverState::Running { .. } => Ok(0),
            AmmSubscriberDriverState::Failed(_) => Err(QuoteReadinessError::Untrusted(
                "canonical subscriber failed".to_owned(),
            )),
            AmmSubscriberDriverState::Paused => Err(QuoteReadinessError::Reconnecting),
            AmmSubscriberDriverState::Stopped => Err(QuoteReadinessError::Untrusted(
                "canonical subscriber stopped".to_owned(),
            )),
            _ => Err(QuoteReadinessError::Untrusted(
                "canonical subscriber reported an unsupported state".to_owned(),
            )),
        }
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
            self.graph_index
                .wait_for_version(
                    self.amm.latest_snapshot().version(),
                    self.config.routing.max_timeout + Duration::from_secs(30),
                )
                .await?;
            let stats = self.graph_index.stats(Some(token));
            self.coverage
                .mark_settled(token, stats.token_pools(), stats.token_present())
                .await;
        } else {
            let node = Arc::clone(self);
            tokio::spawn(async move {
                let result = wait_for_runtime_idle(&node.amm, Duration::from_secs(180)).await;
                let result = match result {
                    Ok(()) => {
                        let target = node.amm.latest_snapshot().version();
                        node.graph_index
                            .wait_for_version(target, Duration::from_secs(30))
                            .await
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(()) => {
                        let stats = node.graph_index.stats(Some(token));
                        node.coverage
                            .mark_settled(token, stats.token_pools(), stats.token_present())
                            .await;
                    }
                    Err(error) => node.coverage.mark_failed(token, error.to_string()).await,
                }
            });
        }
        Ok(self.coverage.get(token).await)
    }

    pub async fn token_coverage(&self, token: Address) -> Result<TokenCoverage> {
        let stats = self.graph_index.stats(Some(token));
        self.coverage
            .refresh_graph_state(token, stats.token_pools(), stats.token_present())
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

fn spawn_runtime_event_logger(amm: &AmmRuntimeHandle) {
    let mut events = amm.subscribe_events();
    tokio::spawn(async move {
        loop {
            match events.next_event().await {
                Ok(event) => match event.kind() {
                    AmmRuntimeEventKind::WorkFailed { work, message } => {
                        warn!(
                            sequence = event.sequence(),
                            ?work,
                            error = %message,
                            "AMM runtime work failed"
                        );
                    }
                    AmmRuntimeEventKind::Reorg { dropped } => {
                        info!(
                            sequence = event.sequence(),
                            dropped_blocks = dropped.len(),
                            "AMM runtime applied a canonical reorg"
                        );
                    }
                    _ => {}
                },
                Err(error) => {
                    if error.to_string().contains("closed") {
                        break;
                    }
                    warn!(error = %error, "AMM runtime event observer lagged");
                }
            }
        }
    });
}

impl RoutingSupervisor {
    pub async fn bootstrap(config: Arc<SidecarConfig>) -> Result<Arc<Self>> {
        let endpoints = config
            .canonical_ws_endpoints()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        let mut selected = None;
        for (index, endpoint) in endpoints.iter().enumerate() {
            info!(
                endpoint_index = index,
                endpoint_count = endpoints.len(),
                "bootstrapping canonical routing generation"
            );
            match tokio::time::timeout(
                config.rpc.canonical_rebuild_timeout,
                RoutingNode::bootstrap_with_canonical(Arc::clone(&config), endpoint.clone()),
            )
            .await
            {
                Ok(Ok(node)) => {
                    selected = Some((index, node));
                    break;
                }
                Ok(Err(error)) => {
                    warn!(endpoint_index = index, %error, "canonical endpoint bootstrap failed");
                    failures.push(format!("endpoint {index}: {error:#}"));
                }
                Err(_) => {
                    warn!(
                        endpoint_index = index,
                        "canonical endpoint bootstrap timed out"
                    );
                    failures.push(format!(
                        "endpoint {index}: bootstrap exceeded {}ms",
                        config.rpc.canonical_rebuild_timeout.as_millis()
                    ));
                }
            }
        }
        let (endpoint_index, node) = selected.ok_or_else(|| {
            anyhow!(
                "all canonical websocket endpoints failed initial bootstrap: {}",
                failures.join("; ")
            )
        })?;
        let mut status = node.status().await?;
        let snapshot = node.amm.latest_snapshot();
        let point = (
            snapshot.point().block_number(),
            snapshot.point().block_hash(),
        );
        overlay_supervisor_status(
            &config,
            &mut status,
            SupervisorPhase::Ready,
            1,
            endpoint_index,
            0,
            Duration::ZERO,
            None,
        );
        let supervisor = Arc::new(Self {
            config,
            active: RwLock::new(Some(node)),
            state: RwLock::new(SupervisorState {
                phase: SupervisorPhase::Ready,
                generation: 1,
                endpoint_index,
                reconnect_attempts: 0,
                last_progress: Instant::now(),
                last_point: point,
                last_status: status,
                last_error: None,
            }),
            shutting_down: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            task: Mutex::new(None),
        });
        let task = tokio::task::spawn_local(Self::supervise(Arc::clone(&supervisor)));
        *supervisor.task.lock().await = Some(task);
        Ok(supervisor)
    }

    pub(crate) fn active_node(&self) -> Option<Arc<RoutingNode>> {
        read_lock(&self.active).clone()
    }

    pub(crate) fn require_node(&self) -> Result<Arc<RoutingNode>> {
        self.active_node()
            .context("canonical routing generation is rebuilding")
    }

    pub fn quote_readiness(&self) -> Result<u64, QuoteReadinessError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(QuoteReadinessError::ShuttingDown);
        }
        let node = self
            .active_node()
            .ok_or(QuoteReadinessError::Reconnecting)?;
        node.quote_readiness()?;
        let state = read_lock(&self.state);
        match state.phase {
            SupervisorPhase::Ready => {
                let age = state.last_progress.elapsed();
                if age > self.config.rpc.canonical_max_stale {
                    Err(QuoteReadinessError::Stale {
                        age_ms: duration_ms(age),
                        max_age_ms: duration_ms(self.config.rpc.canonical_max_stale),
                    })
                } else {
                    Ok(state.generation)
                }
            }
            SupervisorPhase::Reconnecting => Err(QuoteReadinessError::Reconnecting),
            SupervisorPhase::Untrusted => Err(QuoteReadinessError::Untrusted(
                state
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "canonical trust was lost".to_owned()),
            )),
            SupervisorPhase::ShuttingDown => Err(QuoteReadinessError::ShuttingDown),
        }
    }

    pub async fn status(&self) -> Result<NodeStatus> {
        let state = read_lock(&self.state);
        let mut status = state.last_status.clone();
        let age = state.last_progress.elapsed();
        overlay_supervisor_status(
            &self.config,
            &mut status,
            state.phase,
            state.generation,
            state.endpoint_index,
            state.reconnect_attempts,
            age,
            state.last_error.clone(),
        );
        Ok(status)
    }

    async fn supervise(supervisor: Arc<Self>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(supervisor.config.rpc.canonical_health_check_interval) => {}
                _ = supervisor.shutdown_notify.notified() => {}
            }
            if supervisor.shutting_down.load(Ordering::Acquire) {
                return;
            }
            let Some(node) = supervisor.active_node() else {
                continue;
            };
            let snapshot = node.amm.latest_snapshot();
            let point = (
                snapshot.point().block_number(),
                snapshot.point().block_hash(),
            );
            let runtime_health = node.amm.latest_status().health();
            let subscriber_state = node.subscriber.latest_state();
            let point_changed = {
                let state = read_lock(&supervisor.state);
                point != state.last_point
            };
            let status = if point_changed {
                match node.status().await {
                    Ok(status) => Some(status),
                    Err(error) => {
                        warn!(%error, "could not refresh routing supervisor status");
                        None
                    }
                }
            } else {
                None
            };
            let trigger = {
                let mut state = write_lock(&supervisor.state);
                if point != state.last_point {
                    state.last_point = point;
                    state.last_progress = Instant::now();
                }
                if let Some(status) = status {
                    state.last_status = status;
                }
                state.last_status.runtime_health =
                    format!("{runtime_health:?}").to_ascii_lowercase();
                state.last_status.subscriber_state = subscriber_state_name(&subscriber_state);
                match (&subscriber_state, runtime_health) {
                    (AmmSubscriberDriverState::Failed(_), _) => {
                        Some("canonical subscriber failed".to_owned())
                    }
                    (AmmSubscriberDriverState::Stopped, _) => {
                        Some("canonical subscriber stopped unexpectedly".to_owned())
                    }
                    (_, AmmRuntimeHealth::Untrusted) => {
                        Some("AMM runtime lost canonical trust".to_owned())
                    }
                    (_, AmmRuntimeHealth::ShuttingDown) => {
                        Some("AMM runtime began shutting down unexpectedly".to_owned())
                    }
                    _ if state.last_progress.elapsed()
                        > supervisor.config.rpc.canonical_max_stale =>
                    {
                        Some(format!(
                            "canonical head made no progress for {}ms",
                            state.last_progress.elapsed().as_millis()
                        ))
                    }
                    _ => None,
                }
            };
            if let Some(reason) = trigger {
                supervisor.rebuild(reason).await;
            }
        }
    }

    async fn rebuild(&self, reason: String) {
        let old = write_lock(&self.active).take();
        {
            let mut state = write_lock(&self.state);
            state.phase = SupervisorPhase::Untrusted;
            state.generation = state.generation.saturating_add(1);
            state.last_error = Some(reason.clone());
        }
        warn!(%reason, "invalidated routing generation; starting canonical rebuild");
        if let Some(old) = old
            && tokio::time::timeout(Duration::from_secs(15), old.shutdown())
                .await
                .is_err()
        {
            warn!("timed out shutting down invalidated routing generation");
        }
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        {
            let mut state = write_lock(&self.state);
            state.phase = SupervisorPhase::Reconnecting;
        }

        let endpoints = self
            .config
            .canonical_ws_endpoints()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let initial_index = {
            let state = read_lock(&self.state);
            (state.endpoint_index + 1) % endpoints.len()
        };
        let mut attempt = 0_u64;
        loop {
            if self.shutting_down.load(Ordering::Acquire) {
                return;
            }
            let endpoint_index = (initial_index + attempt as usize) % endpoints.len();
            {
                let mut state = write_lock(&self.state);
                state.endpoint_index = endpoint_index;
                state.reconnect_attempts = state.reconnect_attempts.saturating_add(1);
                state.phase = SupervisorPhase::Reconnecting;
            }
            info!(
                endpoint_index,
                attempt = attempt + 1,
                "rebuilding canonical routing generation"
            );
            let bootstrap = tokio::time::timeout(
                self.config.rpc.canonical_rebuild_timeout,
                RoutingNode::bootstrap_with_canonical(
                    Arc::clone(&self.config),
                    endpoints[endpoint_index].clone(),
                ),
            );
            let result = tokio::select! {
                result = bootstrap => Some(result),
                _ = self.shutdown_notify.notified() => None,
            };
            let Some(result) = result else {
                return;
            };
            match result {
                Ok(Ok(node)) => {
                    let snapshot = node.amm.latest_snapshot();
                    let point = (
                        snapshot.point().block_number(),
                        snapshot.point().block_hash(),
                    );
                    match node.status().await {
                        Ok(status) => {
                            *write_lock(&self.active) = Some(node);
                            let mut state = write_lock(&self.state);
                            state.phase = SupervisorPhase::Ready;
                            state.generation = state.generation.saturating_add(1);
                            state.endpoint_index = endpoint_index;
                            state.last_progress = Instant::now();
                            state.last_point = point;
                            state.last_status = status;
                            state.last_error = None;
                            info!(
                                generation = state.generation,
                                endpoint_index, "fresh canonical routing generation is ready"
                            );
                            return;
                        }
                        Err(error) => {
                            error!(%error, "fresh routing generation could not produce status");
                            node.shutdown().await;
                            let mut state = write_lock(&self.state);
                            state.last_error = Some(error.to_string());
                        }
                    }
                }
                Ok(Err(error)) => {
                    warn!(endpoint_index, %error, "canonical rebuild attempt failed");
                    let mut state = write_lock(&self.state);
                    state.last_error = Some(format!(
                        "canonical endpoint {endpoint_index} failed to build a verified generation"
                    ));
                }
                Err(_) => {
                    warn!(endpoint_index, "canonical rebuild attempt timed out");
                    let mut state = write_lock(&self.state);
                    state.last_error = Some(format!(
                        "bootstrap exceeded {}ms",
                        self.config.rpc.canonical_rebuild_timeout.as_millis()
                    ));
                }
            }
            let delay = reconnect_delay(&self.config, attempt);
            attempt = attempt.saturating_add(1);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.shutdown_notify.notified() => return,
            }
        }
    }

    pub async fn shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        {
            let mut state = write_lock(&self.state);
            state.phase = SupervisorPhase::ShuttingDown;
            state.generation = state.generation.saturating_add(1);
        }
        self.shutdown_notify.notify_waiters();
        let task = self.task.lock().await.take();
        if let Some(task) = task
            && tokio::time::timeout(Duration::from_secs(20), task)
                .await
                .is_err()
        {
            warn!("timed out stopping routing supervisor");
        }
        let node = write_lock(&self.active).take();
        if let Some(node) = node {
            node.shutdown().await;
        }
    }
}

fn subscriber_state_name(state: &AmmSubscriberDriverState) -> String {
    match state {
        AmmSubscriberDriverState::Paused => "paused",
        AmmSubscriberDriverState::Running { .. } => "running",
        AmmSubscriberDriverState::Failed(_) => "failed",
        AmmSubscriberDriverState::Stopped => "stopped",
        _ => "unknown",
    }
    .to_owned()
}

#[allow(clippy::too_many_arguments)]
fn overlay_supervisor_status(
    config: &SidecarConfig,
    status: &mut NodeStatus,
    phase: SupervisorPhase,
    generation: u64,
    endpoint_index: usize,
    reconnect_attempts: u64,
    age: Duration,
    last_error: Option<String>,
) {
    status.ready = phase == SupervisorPhase::Ready
        && age <= config.rpc.canonical_max_stale
        && status.runtime_health != "untrusted"
        && status.subscriber_state == "running";
    status.routing_generation = generation;
    status.canonical_connection_state =
        if phase == SupervisorPhase::Ready && age > config.rpc.canonical_max_stale {
            "stale".to_owned()
        } else {
            phase.name().to_owned()
        };
    status.canonical_endpoint_index = endpoint_index;
    status.canonical_endpoint_count = config.canonical_ws_endpoints().len();
    status.canonical_age_ms = duration_ms(age);
    status.canonical_max_stale_ms = duration_ms(config.rpc.canonical_max_stale);
    status.reconnect_attempts = reconnect_attempts;
    status.last_recovery_error = last_error;
}

fn reconnect_delay(config: &SidecarConfig, attempt: u64) -> Duration {
    let multiplier = 1_u32 << attempt.min(16);
    let base = config
        .rpc
        .canonical_reconnect_initial_delay
        .saturating_mul(multiplier)
        .min(config.rpc.canonical_reconnect_max_delay);
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        ^ attempt.wrapping_mul(0x9e37_79b9);
    let jitter_percent = 80 + entropy % 41;
    base.mul_f64(jitter_percent as f64 / 100.0)
        .min(config.rpc.canonical_reconnect_max_delay)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct GraphIndexCache {
    index: RwLock<GraphIndex>,
    versions: watch::Sender<AmmStateVersion>,
}

impl GraphIndexCache {
    fn from_amm(amm: &AmmRuntimeHandle) -> Result<Self> {
        let index = graph_index_from_amm(amm)?;
        let (versions, _) = watch::channel(index.state_version());
        Ok(Self {
            index: RwLock::new(index),
            versions,
        })
    }

    fn stats(&self, token: Option<Address>) -> GraphStats {
        read_lock(&self.index).stats(token)
    }

    fn rebuild_from_amm(&self, amm: &AmmRuntimeHandle) -> Result<()> {
        let next = graph_index_from_amm(amm)?;
        let version = next.state_version();
        *write_lock(&self.index) = next;
        self.versions.send_replace(version);
        Ok(())
    }

    async fn wait_for_version(&self, expected: AmmStateVersion, timeout: Duration) -> Result<()> {
        let mut versions = self.versions.subscribe();
        tokio::time::timeout(timeout, async {
            loop {
                let current = *versions.borrow_and_update();
                if current >= expected {
                    return Ok(());
                }
                versions
                    .changed()
                    .await
                    .map_err(|_| anyhow!("graph index updater stopped at {current:?}"))?;
            }
        })
        .await
        .with_context(|| format!("graph index did not reach {expected:?} within {timeout:?}"))?
    }
}

fn graph_index_from_amm(amm: &AmmRuntimeHandle) -> Result<GraphIndex> {
    let snapshot = amm.latest_snapshot();
    let live = LiveAmmGraph::from_snapshot(&snapshot, GraphBuildOptions::default())?;
    Ok(GraphIndex::from_graph(live.graph(), snapshot.version()))
}

fn spawn_graph_index_updater(
    amm: &AmmRuntimeHandle,
    mut events: LiveRouteObserver,
    graph_index: Arc<GraphIndexCache>,
) {
    let amm = amm.clone();
    tokio::spawn(async move {
        loop {
            match events.next_event().await {
                Ok(event) => {
                    if let LiveRouteRuntimeEventKind::AmmCommitApplied { graph_delta, .. } =
                        event.kind()
                    {
                        let result = write_lock(&graph_index.index).apply_delta(graph_delta);
                        match result {
                            Ok(true) => {
                                graph_index
                                    .versions
                                    .send_replace(graph_delta.source_state_version());
                            }
                            Ok(false) => {}
                            Err(error) => {
                                warn!(%error, "graph index delta rejected; rebuilding from current state");
                                if let Err(error) = graph_index.rebuild_from_amm(&amm) {
                                    error!(%error, "graph index recovery rebuild failed");
                                }
                            }
                        }
                    }
                }
                Err(LiveRouteObserverError::Lagged(count)) => {
                    warn!(
                        count,
                        "graph index observer lagged; rebuilding from current state"
                    );
                    if let Err(error) = graph_index.rebuild_from_amm(&amm) {
                        error!(%error, "graph index lag recovery rebuild failed");
                    }
                }
                Err(LiveRouteObserverError::Closed) => break,
            }
        }
    });
}

async fn verify_executor_deployment<N: Network>(
    config: &SidecarConfig,
    provider: &impl Provider<N>,
    block_hash: B256,
) -> Result<()> {
    if !config.executor.enabled {
        return Ok(());
    }
    let code = provider
        .get_code_at(config.executor.router)
        .block_id(BlockId::from((block_hash, Some(true))))
        .await
        .context("load configured executor runtime code")?;
    if code.is_empty() {
        bail!(
            "configured executor router {} has no code",
            config.executor.router
        );
    }
    let actual = keccak256(&code);
    let expected = config
        .executor
        .expected_runtime_code_hash
        .context("enabled executor is missing its expected runtime code hash")?;
    if actual != expected {
        bail!(
            "configured executor runtime code hash mismatch: expected {expected:#x}, received {actual:#x}"
        );
    }
    Ok(())
}

fn decode_executor_output(output: &[u8]) -> Result<U256> {
    decode_abi_u256(output, "executor simulation")
}

fn decode_abi_u256(output: &[u8], label: &str) -> Result<U256> {
    if output.len() != 32 {
        bail!(
            "{label} returned {} bytes instead of one ABI word",
            output.len()
        );
    }
    Ok(U256::from_be_slice(output))
}

fn approval_transaction(sender: Address, approval: ApprovalRequirement) -> TransactionRequest {
    let mut calldata = [0_u8; 68];
    calldata[..4].copy_from_slice(&[0x09, 0x5e, 0xa7, 0xb3]);
    calldata[16..36].copy_from_slice(approval.spender.as_slice());
    calldata[36..68].copy_from_slice(&approval.minimum_amount.to_be_bytes::<32>());
    TransactionRequest::default()
        .from(sender)
        .to(approval.token)
        .input(TransactionInput::from(Bytes::copy_from_slice(&calldata)))
}

fn allowance_transaction(sender: Address, token: Address, spender: Address) -> TransactionRequest {
    let mut calldata = [0_u8; 68];
    calldata[..4].copy_from_slice(&[0xdd, 0x62, 0xed, 0x3e]);
    calldata[16..36].copy_from_slice(sender.as_slice());
    calldata[48..68].copy_from_slice(spender.as_slice());
    TransactionRequest::default()
        .from(sender)
        .to(token)
        .input(TransactionInput::from(Bytes::copy_from_slice(&calldata)))
}

async fn connect_state_provider(
    config: &SidecarConfig,
    canonical_ws: &str,
) -> Result<Arc<RootProvider<AnyNetwork>>> {
    if config.rpc.state.is_empty() {
        return Ok(Arc::new(
            RootProvider::<AnyNetwork>::connect(canonical_ws)
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
    registry.register_adapter(Arc::new(SolidlyV2Adapter::default()))?;
    registry.register_adapter(Arc::new(BalancerV2Adapter::default()))?;
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
            "uniswap-v3" | "sushi-v3" | "pancake-v3" | "slipstream" => {
                let mut spec = match factory.normalized_protocol().as_str() {
                    "uniswap-v3" => ClFactorySpec::uniswap_v3(address),
                    "sushi-v3" => ClFactorySpec::sushi_v3(address),
                    "pancake-v3" => ClFactorySpec::pancake_v3(address),
                    "slipstream" => ClFactorySpec::slipstream(address),
                    _ => unreachable!(),
                };
                if factory.normalized_protocol() != "slipstream"
                    && let Some(quoter) = &factory.quoter
                {
                    spec = spec.with_quoter(parse_address(quoter)?);
                }
                output = output.with_concentrated_liquidity(spec);
            }
            "aerodrome-v2" => {
                output = output.with_solidly(SolidlyFactoryConfig::aerodrome(address));
            }
            "velodrome-v2" => {
                output = output.with_solidly(SolidlyFactoryConfig::velodrome(address));
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
                    ProtocolId::UniswapV2
                        | ProtocolId::UniswapV3
                        | ProtocolId::PancakeV3
                        | ProtocolId::Slipstream
                        | ProtocolId::SolidlyV2
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
    graph_index: &GraphIndexCache,
    coverage: &CoverageLedger,
) -> Result<()> {
    for token in &config.tokens {
        let token = token.parsed_address()?;
        let stats = graph_index.stats(Some(token));
        coverage
            .mark_settled(token, stats.token_pools(), stats.token_present())
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
            let tick_spacing = pool
                .tick_spacing
                .ok_or_else(|| anyhow!("manual V3 pool {} must set tick_spacing", pool.address))?;
            let storage_layout = if protocol.starts_with("pancake") {
                V3StorageLayout::pancake(tick_spacing)
            } else {
                V3StorageLayout::uniswap(tick_spacing)
            };
            let mut metadata = V3Metadata::default()
                .with_fee(fee)
                .with_tick_spacing(tick_spacing)
                .with_storage_layout(storage_layout);
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
        "slipstream" | "aerodrome_cl" => {
            let fee = pool
                .fee
                .or_else(|| pool.fee_bps.map(|fee| fee.saturating_mul(100)))
                .ok_or_else(|| anyhow!("manual Slipstream pool {} must set fee", pool.address))?;
            let tick_spacing = pool.tick_spacing.ok_or_else(|| {
                anyhow!(
                    "manual Slipstream pool {} must set tick_spacing",
                    pool.address
                )
            })?;
            let quoter = pool
                .quoter
                .as_deref()
                .map(parse_address)
                .transpose()?
                .ok_or_else(|| {
                    anyhow!(
                        "manual Slipstream pool {} must set a compatible quoter",
                        pool.address
                    )
                })?;
            let mut metadata = V3Metadata::default()
                .with_token0(tokens[0])
                .with_token1(tokens[1])
                .with_fee(fee)
                .with_tick_spacing(tick_spacing)
                .with_quoter(quoter);
            if let Some(factory) = pool.factory.as_deref().map(parse_address).transpose()? {
                metadata = metadata.with_factory(factory);
            }
            Ok(PoolRegistration::new(PoolKey::Slipstream(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::Slipstream(metadata)))
        }
        "solidly_v2" | "aerodrome_v2" | "velodrome_v2" => {
            let stable = pool.stable.ok_or_else(|| {
                anyhow!("manual Solidly V2 pool {} must set stable", pool.address)
            })?;
            let layout = SolidlyStorageLayout::new(
                parse_u256(pool.reserve0_slot.as_deref().ok_or_else(|| {
                    anyhow!(
                        "manual Solidly V2 pool {} missing reserve0_slot",
                        pool.address
                    )
                })?)?,
                parse_u256(pool.reserve1_slot.as_deref().ok_or_else(|| {
                    anyhow!(
                        "manual Solidly V2 pool {} missing reserve1_slot",
                        pool.address
                    )
                })?)?,
                parse_u256(pool.token0_slot.as_deref().ok_or_else(|| {
                    anyhow!(
                        "manual Solidly V2 pool {} missing token0_slot",
                        pool.address
                    )
                })?)?,
                parse_u256(pool.token1_slot.as_deref().ok_or_else(|| {
                    anyhow!(
                        "manual Solidly V2 pool {} missing token1_slot",
                        pool.address
                    )
                })?)?,
            );
            Ok(PoolRegistration::new(PoolKey::SolidlyV2(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::SolidlyV2(
                    SolidlyV2Metadata::default()
                        .with_token0(tokens[0])
                        .with_token1(tokens[1])
                        .with_stable(stable)
                        .with_storage_layout(layout),
                )))
        }
        "balancer_v2" => {
            let pool_id = B256::from_str(pool.pool_id.as_deref().ok_or_else(|| {
                anyhow!("manual Balancer V2 pool {} must set pool_id", pool.address)
            })?)?;
            let vault = parse_address(pool.vault.as_deref().ok_or_else(|| {
                anyhow!("manual Balancer V2 pool {} must set vault", pool.address)
            })?)?;
            Ok(PoolRegistration::new(PoolKey::BalancerV2(pool_id))
                .with_state_address(vault)
                .with_metadata(ProtocolMetadata::BalancerV2(
                    BalancerV2Metadata::default()
                        .with_vault(vault)
                        .with_tokens(tokens)
                        .with_balance_slots(
                            pool.discovered_slots
                                .iter()
                                .map(|slot| parse_u256(slot))
                                .collect::<Result<Vec<_>>>()?,
                        ),
                )))
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
    let sim_config = SimConfig::default()
        .with_v2_router(config.chain.v2_router)
        .with_v3_quoter(config.chain.v3_quoter);
    for pool in manual_pools(config)? {
        targets.extend(pool.quote_code_targets(&sim_config));
    }
    for pool in &config.pools {
        if matches!(pool.normalized_protocol().as_str(), "balancer_v2" | "curve") {
            targets.push(parse_address(&pool.address)?);
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
                    code_hash: actual,
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
        "slipstream" | "aerodrome_cl" => Ok(ProtocolId::Slipstream),
        "solidly_v2" | "aerodrome_v2" | "velodrome_v2" => Ok(ProtocolId::SolidlyV2),
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rpc_client::RpcClient;
    use alloy_transport::mock::Asserter;

    #[test]
    fn registry_serves_every_executable_protocol_family() {
        let registry = build_registry(SimConfig::default()).unwrap();

        for protocol in [
            ProtocolId::UniswapV2,
            ProtocolId::UniswapV3,
            ProtocolId::PancakeV3,
            ProtocolId::Slipstream,
            ProtocolId::SolidlyV2,
            ProtocolId::BalancerV2,
            ProtocolId::Curve,
        ] {
            assert!(
                registry.adapter(protocol).is_some(),
                "missing adapter for {protocol:?}"
            );
        }
    }

    #[test]
    fn manual_executable_families_build_complete_registrations() {
        let config = SidecarConfig::parse(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"

                [[pools]]
                protocol = "uniswap_v3"
                address = "0x0000000000000000000000000000000000000001"
                tokens = [
                  "0x0000000000000000000000000000000000000011",
                  "0x0000000000000000000000000000000000000022"
                ]
                fee = 500
                tick_spacing = 10

                [[pools]]
                protocol = "slipstream"
                address = "0x0000000000000000000000000000000000000101"
                tokens = [
                  "0x0000000000000000000000000000000000000011",
                  "0x0000000000000000000000000000000000000022"
                ]
                fee = 500
                tick_spacing = 10
                quoter = "0x0000000000000000000000000000000000000102"

                [[pools]]
                protocol = "solidly_v2"
                address = "0x0000000000000000000000000000000000000201"
                tokens = [
                  "0x0000000000000000000000000000000000000011",
                  "0x0000000000000000000000000000000000000022"
                ]
                stable = false
                reserve0_slot = "20"
                reserve1_slot = "21"
                token0_slot = "13"
                token1_slot = "14"

                [[pools]]
                protocol = "balancer_v2"
                address = "0x0000000000000000000000000000000000000301"
                pool_id = "0x3333333333333333333333333333333333333333333333333333333333333333"
                vault = "0x0000000000000000000000000000000000000302"
                tokens = [
                  "0x0000000000000000000000000000000000000011",
                  "0x0000000000000000000000000000000000000022"
                ]
                discovered_slots = ["3", "4"]
            "#,
        )
        .unwrap();

        let pools = manual_pools(&config).unwrap();
        let ProtocolMetadata::UniswapV3(v3) = &pools[0].metadata else {
            panic!("expected Uniswap V3 metadata");
        };
        assert_eq!(v3.tick_spacing, Some(10));
        assert_eq!(v3.storage_layout, Some(V3StorageLayout::uniswap(10)));
        assert!(matches!(pools[1].key, PoolKey::Slipstream(_)));
        assert!(matches!(pools[1].metadata, ProtocolMetadata::Slipstream(_)));
        assert!(matches!(pools[2].key, PoolKey::SolidlyV2(_)));
        assert!(matches!(pools[2].metadata, ProtocolMetadata::SolidlyV2(_)));
        assert!(matches!(pools[3].key, PoolKey::BalancerV2(_)));
        let ProtocolMetadata::BalancerV2(balancer) = &pools[3].metadata else {
            panic!("expected Balancer V2 metadata");
        };
        assert_eq!(balancer.balance_slots, vec![U256::from(3), U256::from(4)]);
    }

    #[test]
    fn manual_pool_quote_targets_are_prepared_at_startup() {
        let slipstream_quoter =
            Address::from_str("0x0000000000000000000000000000000000000102").unwrap();
        let balancer_vault =
            Address::from_str("0x0000000000000000000000000000000000000302").unwrap();
        let balancer_pool =
            Address::from_str("0x0000000000000000000000000000000000000301").unwrap();
        let config = SidecarConfig::parse(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"

                [[pools]]
                protocol = "slipstream"
                address = "0x0000000000000000000000000000000000000101"
                tokens = [
                  "0x0000000000000000000000000000000000000011",
                  "0x0000000000000000000000000000000000000022"
                ]
                fee = 500
                tick_spacing = 10
                quoter = "0x0000000000000000000000000000000000000102"

                [[pools]]
                protocol = "balancer_v2"
                address = "0x0000000000000000000000000000000000000301"
                pool_id = "0x3333333333333333333333333333333333333333333333333333333333333333"
                vault = "0x0000000000000000000000000000000000000302"
                tokens = [
                  "0x0000000000000000000000000000000000000011",
                  "0x0000000000000000000000000000000000000022"
                ]
                discovered_slots = ["3", "4"]
            "#,
        )
        .unwrap();

        let targets = quote_targets(&config).unwrap();
        assert!(targets.contains(&slipstream_quoter));
        assert!(targets.contains(&balancer_vault));
        assert!(targets.contains(&balancer_pool));
    }

    #[test]
    fn factory_config_builds_slipstream_and_solidly_discovery_drivers() {
        let config = SidecarConfig::parse(
            r#"
                extends = "ethereum-mainnet"
                replace_factories = true
                [rpc]
                canonical_ws = "wss://rpc.example"

                [[factories]]
                name = "aerodrome-cl"
                protocol = "slipstream"
                address = "0x0000000000000000000000000000000000000101"

                [[factories]]
                name = "aerodrome-v2"
                protocol = "aerodrome-v2"
                address = "0x0000000000000000000000000000000000000201"
            "#,
        )
        .unwrap();

        let factories = build_factory_config(&config.factories).unwrap();
        assert_eq!(factories.concentrated_liquidity.len(), 1);
        assert_eq!(
            factories.concentrated_liquidity[0].protocol,
            ProtocolId::Slipstream
        );
        assert_eq!(factories.solidly.len(), 1);
    }

    #[tokio::test]
    async fn executor_startup_accepts_only_the_configured_runtime_hash() {
        let runtime = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]);
        let asserter = Asserter::new();
        asserter.push_success(&runtime);
        let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(asserter));
        let mut config = SidecarConfig::parse(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
            "#,
        )
        .unwrap();
        config.executor.enabled = true;
        config.executor.router = Address::repeat_byte(0x44);
        config.executor.expected_runtime_code_hash = Some(keccak256(&runtime));

        verify_executor_deployment(&config, &provider, B256::repeat_byte(0x70))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn executor_startup_rejects_a_runtime_hash_mismatch() {
        let runtime = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]);
        let asserter = Asserter::new();
        asserter.push_success(&runtime);
        let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(asserter));
        let mut config = SidecarConfig::parse(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
            "#,
        )
        .unwrap();
        config.executor.enabled = true;
        config.executor.router = Address::repeat_byte(0x44);
        config.executor.expected_runtime_code_hash = Some(B256::repeat_byte(0xff));

        let error = verify_executor_deployment(&config, &provider, B256::repeat_byte(0x70))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("runtime code hash mismatch"));
    }
}
