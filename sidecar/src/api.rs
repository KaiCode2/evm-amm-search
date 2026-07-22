use std::{future::Future, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256, Bytes, U256};
use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use evm_amm_search::{
    HeuristicSearchConfig, LiveRouteRuntimeError, LiveRouteSubscription, RouteProvenance,
    RouteQuote, RouteRequest, RouteSubscriptionSnapshot, RouteSubscriptionSpec,
    RouteSubscriptionState, SearchConfig, SearchMode, StreamingSearchConfig,
};
use evm_amm_state::adapters::SimConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, TryAcquireError};
use tower_http::{
    catch_panic::CatchPanicLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

use crate::{
    config::{DiscoveryMode, SearchQuality, SidecarConfig, parse_address, parse_u256},
    coverage::TokenCoverage,
    execution::{
        ApprovalRequirement, ExecutableSwap, ExecutionApprovalCheckRequest, ExecutionApprovalState,
        ExecutionRequest, ExecutionSimulation, ExecutionSimulationRequest, ExecutorDeployment,
        InputAuthorization, build_executable_swap, min_amount_out_from_slippage,
    },
    node::{
        CanonicalBlockContext, NodeStatus, PrepareTokenOptions, QuoteReadinessError, RoutingNode,
        RoutingSupervisor, parse_protocol, protocol_name,
    },
};

/// Narrow contract between the HTTP boundary and a routing implementation.
///
/// Keeping this interface smaller than [`RoutingNode`] lets the HTTP contract
/// be tested without a live RPC connection while production still delegates to
/// the real graph and route runtimes.
pub trait RoutingBackend: Send + Sync + 'static {
    fn config(&self) -> &SidecarConfig;
    fn quote_readiness(&self) -> Result<u64, QuoteReadinessError>;
    fn status(&self) -> impl Future<Output = anyhow::Result<NodeStatus>> + Send;
    fn token_coverages(&self) -> impl Future<Output = Vec<TokenCoverage>> + Send;
    fn token_coverage(
        &self,
        token: Address,
    ) -> impl Future<Output = anyhow::Result<TokenCoverage>> + Send;
    fn graph_contains(&self, token: Address) -> bool;
    fn prepare_token(
        self: Arc<Self>,
        token: Address,
        options: PrepareTokenOptions,
    ) -> impl Future<Output = anyhow::Result<TokenCoverage>> + Send;
    fn sim_config(&self) -> SimConfig;
    fn subscribe(
        &self,
        spec: RouteSubscriptionSpec,
    ) -> impl Future<Output = Result<LiveRouteSubscription, LiveRouteRuntimeError>> + Send;
    fn simulate_executor(
        &self,
        request: ExecutionSimulationRequest,
    ) -> impl Future<Output = anyhow::Result<ExecutionSimulation>> + Send;
    fn check_executor_approval(
        &self,
        request: ExecutionApprovalCheckRequest,
    ) -> impl Future<Output = anyhow::Result<ExecutionApprovalState>> + Send;
    fn canonical_block_context(
        &self,
        block_number: u64,
        block_hash: B256,
    ) -> impl Future<Output = anyhow::Result<CanonicalBlockContext>> + Send;
}

impl RoutingBackend for RoutingNode {
    fn config(&self) -> &SidecarConfig {
        &self.config
    }

    fn quote_readiness(&self) -> Result<u64, QuoteReadinessError> {
        RoutingNode::quote_readiness(self)
    }

    fn status(&self) -> impl Future<Output = anyhow::Result<NodeStatus>> + Send {
        RoutingNode::status(self)
    }

    fn token_coverages(&self) -> impl Future<Output = Vec<TokenCoverage>> + Send {
        self.coverage.all()
    }

    fn token_coverage(
        &self,
        token: Address,
    ) -> impl Future<Output = anyhow::Result<TokenCoverage>> + Send {
        RoutingNode::token_coverage(self, token)
    }

    fn graph_contains(&self, token: Address) -> bool {
        RoutingNode::graph_contains(self, token)
    }

    async fn prepare_token(
        self: Arc<Self>,
        token: Address,
        options: PrepareTokenOptions,
    ) -> anyhow::Result<TokenCoverage> {
        RoutingNode::prepare_token(&self, token, options).await
    }

    fn sim_config(&self) -> SimConfig {
        self.sim_config
    }

    fn subscribe(
        &self,
        spec: RouteSubscriptionSpec,
    ) -> impl Future<Output = Result<LiveRouteSubscription, LiveRouteRuntimeError>> + Send {
        self.routes.subscribe(spec)
    }

    fn simulate_executor(
        &self,
        request: ExecutionSimulationRequest,
    ) -> impl Future<Output = anyhow::Result<ExecutionSimulation>> + Send {
        RoutingNode::simulate_executor(self, request)
    }

    fn check_executor_approval(
        &self,
        request: ExecutionApprovalCheckRequest,
    ) -> impl Future<Output = anyhow::Result<ExecutionApprovalState>> + Send {
        RoutingNode::check_executor_approval(self, request)
    }

    fn canonical_block_context(
        &self,
        block_number: u64,
        block_hash: B256,
    ) -> impl Future<Output = anyhow::Result<CanonicalBlockContext>> + Send {
        RoutingNode::canonical_block_context(self, block_number, block_hash)
    }
}

impl RoutingBackend for RoutingSupervisor {
    fn config(&self) -> &SidecarConfig {
        &self.config
    }

    fn quote_readiness(&self) -> Result<u64, QuoteReadinessError> {
        RoutingSupervisor::quote_readiness(self)
    }

    fn status(&self) -> impl Future<Output = anyhow::Result<NodeStatus>> + Send {
        RoutingSupervisor::status(self)
    }

    async fn token_coverages(&self) -> Vec<TokenCoverage> {
        match self.active_node() {
            Some(node) => node.coverage.all().await,
            None => Vec::new(),
        }
    }

    async fn token_coverage(&self, token: Address) -> anyhow::Result<TokenCoverage> {
        let node = self.require_node()?;
        node.token_coverage(token).await
    }

    fn graph_contains(&self, token: Address) -> bool {
        self.active_node()
            .is_some_and(|node| node.graph_contains(token))
    }

    async fn prepare_token(
        self: Arc<Self>,
        token: Address,
        options: PrepareTokenOptions,
    ) -> anyhow::Result<TokenCoverage> {
        let node = self.require_node()?;
        node.prepare_token(token, options).await
    }

    fn sim_config(&self) -> SimConfig {
        self.active_node().map_or_else(
            || {
                SimConfig::default()
                    .with_v2_router(self.config.chain.v2_router)
                    .with_v3_quoter(self.config.chain.v3_quoter)
            },
            |node| node.sim_config,
        )
    }

    async fn subscribe(
        &self,
        spec: RouteSubscriptionSpec,
    ) -> Result<LiveRouteSubscription, LiveRouteRuntimeError> {
        let Some(node) = self.active_node() else {
            return Err(LiveRouteRuntimeError::Closed);
        };
        node.routes.subscribe(spec).await
    }

    async fn simulate_executor(
        &self,
        request: ExecutionSimulationRequest,
    ) -> anyhow::Result<ExecutionSimulation> {
        self.require_node()?.simulate_executor(request).await
    }

    async fn check_executor_approval(
        &self,
        request: ExecutionApprovalCheckRequest,
    ) -> anyhow::Result<ExecutionApprovalState> {
        self.require_node()?.check_executor_approval(request).await
    }

    async fn canonical_block_context(
        &self,
        block_number: u64,
        block_hash: B256,
    ) -> anyhow::Result<CanonicalBlockContext> {
        self.require_node()?
            .canonical_block_context(block_number, block_hash)
            .await
    }
}

pub struct AppState<B: RoutingBackend = RoutingNode> {
    backend: Arc<B>,
    quote_slots: Arc<Semaphore>,
    discovery_slots: Arc<Semaphore>,
    simulation_slots: Arc<Semaphore>,
}

impl<B: RoutingBackend> Clone for AppState<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            quote_slots: Arc::clone(&self.quote_slots),
            discovery_slots: Arc::clone(&self.discovery_slots),
            simulation_slots: Arc::clone(&self.simulation_slots),
        }
    }
}

impl<B: RoutingBackend> AppState<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            quote_slots: Arc::new(Semaphore::new(backend.config().server.max_in_flight_quotes)),
            discovery_slots: Arc::new(Semaphore::new(
                backend.config().discovery.max_concurrent_requests,
            )),
            simulation_slots: Arc::new(Semaphore::new(
                backend.config().executor.max_in_flight_simulations,
            )),
            backend,
        }
    }
}

pub fn router<B: RoutingBackend>(state: AppState<B>) -> Router {
    let max_request_bytes = state.backend.config().server.max_request_bytes;
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz::<B>))
        .route("/v1/status", get(status::<B>))
        .route("/v1/quote", post(quote::<B>))
        .route("/v1/executable-quote", post(executable_quote::<B>))
        .route("/v1/tokens/{address}", get(token_status::<B>))
        .route("/v1/tokens/{address}/prewarm", put(prewarm_token::<B>))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::new(
            axum::http::HeaderName::from_static("x-request-id"),
            MakeRequestUuid,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(CatchPanicLayer::new())
        .with_state(state)
}

async fn livez() -> impl IntoResponse {
    (StatusCode::OK, Json(HealthResponse { ok: true }))
}

async fn executable_quote<B: RoutingBackend>(
    State(state): State<AppState<B>>,
    Json(request): Json<ExecutableQuoteRequest>,
) -> Result<Json<ExecutableQuoteResponse>, ApiError> {
    if !state.backend.config().executor.enabled {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "executor_disabled",
            "experimental executable quotes are disabled by configuration",
        ));
    }
    if request
        .slippage_bps
        .is_some_and(|bps| bps > state.backend.config().executor.max_slippage_bps)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "slippage_out_of_range",
            format!(
                "slippage_bps must be within 0..={}",
                state.backend.config().executor.max_slippage_bps
            ),
        ));
    }
    if request.min_amount_out.is_some() && request.slippage_bps.is_some() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "conflicting_minimum_output",
            "min_amount_out and slippage_bps are mutually exclusive",
        ));
    }
    if request.deadline.is_some() && request.deadline_secs.is_some() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "conflicting_deadline",
            "deadline and deadline_secs are mutually exclusive",
        ));
    }
    if matches!(&request.authorization, AuthorizationRequest::Erc2612 { .. })
        && request.deadline.is_none()
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "erc2612_deadline_required",
            "ERC-2612 authorization requires the absolute deadline that was signed",
        ));
    }
    let _permit = state
        .quote_slots
        .try_acquire()
        .map_err(quote_capacity_error)?;
    let routing_generation = quote_generation(&state)?;
    let token_in = parse_address(&request.token_in).map_err(ApiError::bad_request)?;
    let token_out = parse_address(&request.token_out).map_err(ApiError::bad_request)?;
    if token_in == token_out {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "same_token",
            "token_in and token_out must differ",
        ));
    }
    let amount_in = parse_u256(&request.amount_in).map_err(ApiError::bad_request)?;
    if amount_in == U256::ZERO {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "zero_amount",
            "amount_in must be non-zero",
        ));
    }
    let sender = parse_address(&request.sender).map_err(ApiError::bad_request)?;
    let recipient = parse_address(&request.recipient).map_err(ApiError::bad_request)?;
    if sender == Address::ZERO || recipient == Address::ZERO {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "zero_execution_address",
            "sender and recipient must be non-zero",
        ));
    }
    let deadline_ttl = request
        .deadline_secs
        .map(Duration::from_secs)
        .unwrap_or(state.backend.config().executor.default_deadline);
    if request.deadline.is_none()
        && (deadline_ttl.is_zero() || deadline_ttl > state.backend.config().executor.max_deadline)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "deadline_out_of_range",
            format!(
                "deadline_secs must be within 1..={}",
                state.backend.config().executor.max_deadline.as_secs()
            ),
        ));
    }
    let policy = QuotePolicy::resolve(state.backend.config(), request.options)?;
    let missing = [token_in, token_out]
        .into_iter()
        .filter(|token| !state.backend.graph_contains(*token))
        .collect::<Vec<_>>();
    match policy.discovery {
        DiscoveryMode::Off if !missing.is_empty() => {
            return Err(ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "coverage_missing",
                format!("tokens are absent from the graph: {}", addresses(&missing)),
            ));
        }
        DiscoveryMode::IfMissing => prepare_for_quote(&state, missing, false).await?,
        DiscoveryMode::Refresh => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "executable_refresh_unsupported",
                "executable quotes do not perform an implicit graph refresh",
            ));
        }
        DiscoveryMode::Off => {}
    }
    let snapshot = search_routes(&state, token_in, token_out, amount_in, &policy).await?;
    ensure_quote_generation(&state, routing_generation)?;
    let RouteSubscriptionState::Ready { source, report, .. } = snapshot.state() else {
        return Err(route_terminal_error(snapshot.state()));
    };
    let block_context = tokio::time::timeout(
        state.backend.config().executor.simulation_timeout,
        state
            .backend
            .canonical_block_context(source.point().block_number(), source.point().block_hash()),
    )
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "canonical_block_timeout",
            "canonical quote-block verification exceeded its configured timeout",
        )
    })?
    .map_err(|error| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "quote_block_unavailable",
            format!("could not verify canonical quote block: {error}"),
        )
    })?;
    let snapshot_age = block_context
        .head_timestamp
        .saturating_sub(block_context.source_timestamp);
    if snapshot_age > state.backend.config().executor.max_snapshot_age.as_secs() {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "stale_snapshot",
            format!(
                "quoted block is {snapshot_age}s old; executor policy allows at most {}s",
                state.backend.config().executor.max_snapshot_age.as_secs()
            ),
        ));
    }
    let deadline = resolve_deadline(
        request.deadline.as_deref(),
        deadline_ttl,
        block_context.source_timestamp,
        state.backend.config().executor.max_deadline,
    )?;
    let executable_routes = report
        .top_routes
        .iter()
        .enumerate()
        .filter(|(_, route)| {
            route.path.hops.iter().all(|hop| {
                state
                    .backend
                    .config()
                    .executor_allows_protocol(protocol_name(hop.pool.protocol()))
            })
        })
        .map(|(index, route)| (index + 1, route.clone()))
        .collect::<Vec<_>>();
    let (route_rank, best) = executable_routes.first().ok_or_else(|| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "no_executable_route",
            "no returned route uses only executor-allowed protocol families",
        )
    })?;
    let min_amount_out = match (&request.min_amount_out, request.slippage_bps) {
        (Some(minimum), None) => Some(parse_u256(minimum).map_err(ApiError::bad_request)?),
        (None, Some(slippage_bps)) => Some(
            min_amount_out_from_slippage(best.amount_out, slippage_bps)
                .map_err(ApiError::bad_request)?,
        ),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("conflicting minimum policy rejected above"),
    };
    let authorization = match request.authorization {
        AuthorizationRequest::Allowance => InputAuthorization::Allowance,
        AuthorizationRequest::Native => InputAuthorization::Native,
        AuthorizationRequest::Erc2612 { v, r, s } => InputAuthorization::Erc2612 {
            v,
            r: B256::from_str(&r).map_err(ApiError::bad_request)?,
            s: B256::from_str(&s).map_err(ApiError::bad_request)?,
        },
        AuthorizationRequest::Permit2 {
            nonce,
            deadline: permit_deadline,
            signature,
        } => {
            let permit_deadline = parse_u256(&permit_deadline).map_err(ApiError::bad_request)?;
            if permit_deadline < U256::from(deadline) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "permit2_deadline_too_soon",
                    "Permit2 deadline must not precede the swap deadline",
                ));
            }
            InputAuthorization::Permit2 {
                nonce: parse_u256(&nonce).map_err(ApiError::bad_request)?,
                deadline: permit_deadline,
                signature: Bytes::from_str(&signature).map_err(ApiError::bad_request)?,
            }
        }
    };
    let execution_request = ExecutionRequest {
        deployment: ExecutorDeployment {
            router: state.backend.config().executor.router,
            weth: state.backend.config().executor.weth,
            permit2: state.backend.config().executor.permit2,
        },
        recipient,
        deadline: U256::from(deadline),
        min_amount_out,
        authorization,
    };
    let executable_route_quotes = executable_routes
        .iter()
        .map(|(_, route)| route.clone())
        .collect::<Vec<_>>();
    let swap = build_executable_swap(
        snapshot.view().snapshot().registry().registry(),
        &executable_route_quotes,
        execution_request,
    )
    .map_err(ApiError::unprocessable)?;
    let _simulation_permit = state
        .simulation_slots
        .try_acquire()
        .map_err(simulation_capacity_error)?;
    let approval_state = if let Some(approval) = swap.approval {
        let approval_state = tokio::time::timeout(
            state.backend.config().executor.simulation_timeout,
            state
                .backend
                .check_executor_approval(ExecutionApprovalCheckRequest {
                    block_hash: source.point().block_hash(),
                    sender,
                    approval,
                }),
        )
        .await
        .map_err(|_| {
            ApiError::new(
                StatusCode::GATEWAY_TIMEOUT,
                "approval_check_timeout",
                "token allowance check exceeded its configured timeout",
            )
        })?
        .map_err(|error| {
            ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "approval_check_failed",
                format!("token allowance check failed: {error}"),
            )
        })?;
        if approval_state.current_allowance < approval.minimum_amount {
            return Err(ApiError::new(
                StatusCode::PRECONDITION_REQUIRED,
                "approval_required",
                "the sender must submit the prerequisite token approval and request a fresh executable quote",
            )
            .with_approval(approval_response(
                state.backend.config().chain.expected_chain_id,
                sender,
                approval,
                approval_state,
            )));
        }
        Some(approval_state)
    } else {
        None
    };
    let simulation = tokio::time::timeout(
        state.backend.config().executor.simulation_timeout,
        state.backend.simulate_executor(ExecutionSimulationRequest {
            block_hash: source.point().block_hash(),
            sender,
            swap: swap.clone(),
        }),
    )
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "simulation_timeout",
            "executor simulation exceeded its configured timeout",
        )
    })?
    .map_err(|error| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "simulation_failed",
            format!("executor simulation failed: {error}"),
        )
    })?;
    if simulation.amount_out < swap.min_amount_out {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "simulation_below_minimum",
            format!(
                "simulated output {} is below minimum {}",
                simulation.amount_out, swap.min_amount_out
            ),
        ));
    }
    if simulation.amount_out != best.amount_out {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "simulation_quote_mismatch",
            format!(
                "exact-block executor simulation output {} differs from selected route output {}",
                simulation.amount_out, best.amount_out
            ),
        ));
    }

    ensure_quote_generation(&state, routing_generation)?;

    Ok(Json(executable_quote_response(
        ExecutableResponseContext {
            chain_id: state.backend.config().chain.expected_chain_id,
            sender,
            source,
            block_timestamp: block_context.source_timestamp,
            route_rank: *route_rank,
            route: best,
        },
        swap,
        approval_state,
        simulation,
    )))
}

async fn readyz<B: RoutingBackend>(
    State(state): State<AppState<B>>,
) -> Result<impl IntoResponse, ApiError> {
    let status = state.backend.status().await.map_err(ApiError::internal)?;
    if status.ready && state.backend.quote_readiness().is_ok() {
        Ok((StatusCode::OK, Json(HealthResponse { ok: true })))
    } else {
        Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            "routing runtime is not ready",
        ))
    }
}

async fn status<B: RoutingBackend>(
    State(state): State<AppState<B>>,
) -> Result<impl IntoResponse, ApiError> {
    let node = state.backend.status().await.map_err(ApiError::internal)?;
    let tokens = state.backend.token_coverages().await;
    Ok(Json(StatusResponse { node, tokens }))
}

async fn token_status<B: RoutingBackend>(
    State(state): State<AppState<B>>,
    Path(address): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let token = parse_address(&address).map_err(ApiError::bad_request)?;
    let coverage = state
        .backend
        .token_coverage(token)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(coverage))
}

async fn prewarm_token<B: RoutingBackend>(
    State(state): State<AppState<B>>,
    Path(address): Path<String>,
    headers: HeaderMap,
    body: Option<Json<PrewarmRequest>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_admin(state.backend.config(), &headers)?;
    let _permit = state
        .discovery_slots
        .try_acquire()
        .map_err(discovery_capacity_error)?;
    let token = parse_address(&address).map_err(ApiError::bad_request)?;
    let request = body.map(|Json(body)| body).unwrap_or_default();
    let connectors = request
        .connectors
        .iter()
        .map(|connector| parse_address(connector).map_err(ApiError::bad_request))
        .collect::<Result<Vec<_>, _>>()?;
    let protocols = request
        .protocols
        .iter()
        .map(|protocol| parse_protocol(protocol).map_err(ApiError::bad_request))
        .collect::<Result<Vec<_>, _>>()?;
    let coverage = Arc::clone(&state.backend)
        .prepare_token(
            token,
            PrepareTokenOptions {
                connectors,
                protocols,
                refresh: request.mode == PrewarmMode::Refresh,
                wait: request.wait,
            },
        )
        .await
        .map_err(ApiError::unprocessable)?;
    let status = match coverage.state {
        crate::coverage::CoverageState::Queued | crate::coverage::CoverageState::Discovering => {
            StatusCode::ACCEPTED
        }
        _ => StatusCode::OK,
    };
    Ok((status, Json(coverage)))
}

async fn quote<B: RoutingBackend>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(request): Json<QuoteRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let _permit = state
        .quote_slots
        .try_acquire()
        .map_err(quote_capacity_error)?;
    let routing_generation = quote_generation(&state)?;
    let token_in = parse_address(&request.token_in).map_err(ApiError::bad_request)?;
    let token_out = parse_address(&request.token_out).map_err(ApiError::bad_request)?;
    if token_in == token_out {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "same_token",
            "token_in and token_out must differ",
        ));
    }
    let amount_in = parse_u256(&request.amount_in).map_err(ApiError::bad_request)?;
    if amount_in == U256::ZERO {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "zero_amount",
            "amount_in must be non-zero",
        ));
    }
    let policy = QuotePolicy::resolve(state.backend.config(), request.options)?;
    if policy.discovery == DiscoveryMode::Refresh {
        authorize_admin(state.backend.config(), &headers)?;
    }

    let missing = [token_in, token_out]
        .into_iter()
        .filter(|token| !state.backend.graph_contains(*token))
        .collect::<Vec<_>>();
    match policy.discovery {
        DiscoveryMode::Off if !missing.is_empty() => {
            return Err(ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "coverage_missing",
                format!("tokens are absent from the graph: {}", addresses(&missing)),
            ));
        }
        DiscoveryMode::IfMissing => {
            prepare_for_quote(&state, missing, false).await?;
        }
        DiscoveryMode::Refresh => {
            prepare_for_quote(&state, vec![token_in, token_out], true).await?;
        }
        DiscoveryMode::Off => {}
    }

    let snapshot = search_routes(&state, token_in, token_out, amount_in, &policy).await?;

    let response = match snapshot.state() {
        RouteSubscriptionState::Ready { source, report, .. } => QuoteResponse {
            token_in: format!("{token_in:#x}"),
            token_out: format!("{token_out:#x}"),
            amount_in: amount_in.to_string(),
            routes: report.top_routes.iter().map(route_response).collect(),
            finality: finality_name(report.finality).to_owned(),
            candidates_evaluated: report.progress.candidates_evaluated,
            viable_routes_observed: report.routes_observed,
            source: SourceResponse {
                chain_id: source.point().chain_id(),
                block_number: source.point().block_number(),
                block_hash: format!("{:#x}", source.point().block_hash()),
                block_timestamp: None,
                state_version: source.state_version().get(),
                graph_revision: source.graph_version().revision(),
            },
        },
        RouteSubscriptionState::Failed { failure, .. } => {
            return Err(ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "search_failed",
                failure.message(),
            ));
        }
        RouteSubscriptionState::RuntimeFailed { failure } => {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime_failed",
                failure.message(),
            ));
        }
        RouteSubscriptionState::Cancelled | RouteSubscriptionState::Closed => {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime_closed",
                "route runtime closed",
            ));
        }
        RouteSubscriptionState::Pending { .. } | RouteSubscriptionState::Searching { .. } => {
            unreachable!("terminal state loop returned a non-terminal state")
        }
        _ => {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "unknown_route_state",
                "route runtime returned an unsupported state",
            ));
        }
    };
    ensure_quote_generation(&state, routing_generation)?;
    Ok(Json(response))
}

fn quote_generation<B: RoutingBackend>(state: &AppState<B>) -> Result<u64, ApiError> {
    state
        .backend
        .quote_readiness()
        .map_err(quote_readiness_error)
}

fn ensure_quote_generation<B: RoutingBackend>(
    state: &AppState<B>,
    expected: u64,
) -> Result<(), ApiError> {
    let actual = quote_generation(state)?;
    if actual != expected {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "routing_generation_changed",
            "routing state changed while the quote was being generated; retry the request",
        ));
    }
    Ok(())
}

fn quote_readiness_error(error: QuoteReadinessError) -> ApiError {
    let code = match error {
        QuoteReadinessError::Reconnecting => "canonical_reconnecting",
        QuoteReadinessError::Stale { .. } => "canonical_stale",
        QuoteReadinessError::Untrusted(_) => "runtime_untrusted",
        QuoteReadinessError::ShuttingDown => "service_shutting_down",
    };
    ApiError::new(StatusCode::SERVICE_UNAVAILABLE, code, error.to_string())
}

async fn prepare_for_quote<B: RoutingBackend>(
    state: &AppState<B>,
    tokens: Vec<Address>,
    refresh: bool,
) -> Result<(), ApiError> {
    if tokens.is_empty() {
        return Ok(());
    }
    let _permit = state
        .discovery_slots
        .try_acquire()
        .map_err(discovery_capacity_error)?;
    for token in tokens {
        Arc::clone(&state.backend)
            .prepare_token(
                token,
                PrepareTokenOptions {
                    refresh,
                    wait: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::unprocessable)?;
    }
    Ok(())
}

async fn search_routes<B: RoutingBackend>(
    state: &AppState<B>,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    policy: &QuotePolicy,
) -> Result<Arc<RouteSubscriptionSnapshot>, ApiError> {
    let connectors = state
        .backend
        .config()
        .connector_addresses()
        .map_err(ApiError::internal)?;
    let search = SearchConfig::default()
        .with_hops(1, policy.max_hops)
        .with_max_candidates(state.backend.config().routing.max_candidates)
        .with_connector_tokens(connectors)
        .with_mode(policy.search_mode());
    let route_request = RouteRequest::new(token_in, token_out, amount_in)
        .with_config(search)
        .with_sim_config(state.backend.sim_config());
    let spec = RouteSubscriptionSpec::new(route_request, policy.streaming());
    let mut subscription = state
        .backend
        .subscribe(spec)
        .await
        .map_err(ApiError::internal)?;
    tokio::time::timeout(policy.timeout, async {
        loop {
            let snapshot = subscription.latest();
            match snapshot.state() {
                RouteSubscriptionState::Ready { .. }
                | RouteSubscriptionState::Failed { .. }
                | RouteSubscriptionState::Cancelled
                | RouteSubscriptionState::Closed
                | RouteSubscriptionState::RuntimeFailed { .. } => return Ok::<_, String>(snapshot),
                _ => subscription
                    .changed()
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())?,
            }
        }
    })
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "quote_timeout",
            format!("route search exceeded {}ms", policy.timeout.as_millis()),
        )
    })?
    .map_err(ApiError::internal)
}

fn route_response(route: &RouteQuote) -> RouteResponse {
    RouteResponse {
        amount_in: route.amount_in.to_string(),
        amount_out: route.amount_out.to_string(),
        hops: route
            .hops
            .iter()
            .map(|hop| HopResponse {
                protocol: protocol_name(hop.hop.pool.protocol()).to_owned(),
                pool: hop
                    .hop
                    .pool
                    .address()
                    .map(|address| format!("{address:#x}"))
                    .or_else(|| hop.hop.pool.bytes32().map(|id| format!("{id:#x}")))
                    .unwrap_or_else(|| format!("{:?}", hop.hop.pool)),
                token_in: format!("{:#x}", hop.hop.token_in),
                token_out: format!("{:#x}", hop.hop.token_out),
                amount_in: hop.amount_in.to_string(),
                amount_out: hop.amount_out.to_string(),
            })
            .collect(),
    }
}

struct ExecutableResponseContext<'a> {
    chain_id: u64,
    sender: Address,
    source: &'a RouteProvenance,
    block_timestamp: u64,
    route_rank: usize,
    route: &'a RouteQuote,
}

fn executable_quote_response(
    context: ExecutableResponseContext<'_>,
    swap: ExecutableSwap,
    approval_state: Option<ExecutionApprovalState>,
    simulation: ExecutionSimulation,
) -> ExecutableQuoteResponse {
    ExecutableQuoteResponse {
        warning: "EXPERIMENTAL: UNAUDITED DEMONSTRATION ROUTER; NOT INTENDED FOR PUBLIC OR PRODUCTION USE",
        source: SourceResponse {
            chain_id: context.source.point().chain_id(),
            block_number: context.source.point().block_number(),
            block_hash: format!("{:#x}", context.source.point().block_hash()),
            block_timestamp: Some(context.block_timestamp),
            state_version: context.source.state_version().get(),
            graph_revision: context.source.graph_version().revision(),
        },
        route_rank: context.route_rank,
        route: route_response(context.route),
        min_amount_out: swap.min_amount_out.to_string(),
        transaction: TransactionResponse {
            chain_id: context.chain_id,
            from: format!("{:#x}", context.sender),
            to: format!("{:#x}", swap.to),
            value: swap.value.to_string(),
            data: format!("{:#x}", swap.data),
            deadline: swap.deadline.to_string(),
        },
        approval: swap
            .approval
            .zip(approval_state)
            .map(|(approval, approval_state)| {
                approval_response(context.chain_id, context.sender, approval, approval_state)
            }),
        simulation: SimulationResponse {
            status: "succeeded",
            amount_out: simulation.amount_out.to_string(),
            gas_estimate: simulation.gas_estimate,
        },
    }
}

fn approval_response(
    chain_id: u64,
    sender: Address,
    approval: ApprovalRequirement,
    state: ExecutionApprovalState,
) -> ApprovalResponse {
    ApprovalResponse {
        required: state.current_allowance < approval.minimum_amount,
        token: format!("{:#x}", approval.token),
        spender: format!("{:#x}", approval.spender),
        minimum_amount: approval.minimum_amount.to_string(),
        current_allowance: state.current_allowance.to_string(),
        gas_estimate: state.gas_estimate,
        transaction: ApprovalTransactionResponse {
            chain_id,
            from: format!("{sender:#x}"),
            to: format!("{:#x}", approval.token),
            value: "0".to_owned(),
            data: format!("{:#x}", approval_calldata(approval)),
        },
    }
}

fn approval_calldata(approval: ApprovalRequirement) -> Bytes {
    let mut calldata = [0_u8; 68];
    calldata[..4].copy_from_slice(&[0x09, 0x5e, 0xa7, 0xb3]);
    calldata[16..36].copy_from_slice(approval.spender.as_slice());
    calldata[36..68].copy_from_slice(&approval.minimum_amount.to_be_bytes::<32>());
    Bytes::copy_from_slice(&calldata)
}

fn resolve_deadline(
    absolute: Option<&str>,
    ttl: Duration,
    block_timestamp: u64,
    max_deadline: Duration,
) -> Result<u64, ApiError> {
    let deadline = match absolute {
        Some(value) => {
            let parsed = parse_u256(value).map_err(ApiError::bad_request)?;
            u64::try_from(parsed).map_err(|_| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "deadline_out_of_range",
                    "deadline must fit in an unsigned 64-bit Unix timestamp",
                )
            })?
        }
        None => block_timestamp
            .checked_add(ttl.as_secs())
            .context("executor deadline overflow")
            .map_err(ApiError::internal)?,
    };
    let horizon = deadline.checked_sub(block_timestamp).ok_or_else(|| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "deadline_expired",
            "deadline must be later than the quoted block timestamp",
        )
    })?;
    if horizon == 0 || horizon > max_deadline.as_secs() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "deadline_out_of_range",
            format!(
                "deadline must be within 1..={} seconds after the quoted block",
                max_deadline.as_secs()
            ),
        ));
    }
    Ok(deadline)
}

fn route_terminal_error(state: &RouteSubscriptionState) -> ApiError {
    match state {
        RouteSubscriptionState::Failed { failure, .. } => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "search_failed",
            failure.message(),
        ),
        RouteSubscriptionState::RuntimeFailed { failure } => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime_failed",
            failure.message(),
        ),
        RouteSubscriptionState::Cancelled | RouteSubscriptionState::Closed => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime_closed",
            "route runtime closed",
        ),
        _ => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unknown_route_state",
            "route runtime returned an unsupported state",
        ),
    }
}

fn finality_name(finality: evm_amm_search::SearchFinality) -> &'static str {
    match finality {
        evm_amm_search::SearchFinality::FastLaneOnly => "fast_lane_only",
        evm_amm_search::SearchFinality::HeuristicOnly => "heuristic_only",
        evm_amm_search::SearchFinality::Exhaustive => "exhaustive",
        evm_amm_search::SearchFinality::StopPolicySatisfied => "stop_policy_satisfied",
        evm_amm_search::SearchFinality::Stopped => "stopped",
    }
}

#[derive(Debug, Deserialize)]
pub struct QuoteRequest {
    pub token_in: String,
    pub token_out: String,
    pub amount_in: String,
    #[serde(default)]
    pub options: QuoteOptions,
}

#[derive(Debug, Deserialize)]
struct ExecutableQuoteRequest {
    token_in: String,
    token_out: String,
    amount_in: String,
    sender: String,
    recipient: String,
    min_amount_out: Option<String>,
    slippage_bps: Option<u16>,
    deadline: Option<String>,
    deadline_secs: Option<u64>,
    authorization: AuthorizationRequest,
    #[serde(default)]
    options: QuoteOptions,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AuthorizationRequest {
    Allowance,
    Native,
    Erc2612 {
        v: u8,
        r: String,
        s: String,
    },
    Permit2 {
        nonce: String,
        deadline: String,
        signature: String,
    },
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct QuoteOptions {
    pub quality: Option<SearchQuality>,
    pub max_hops: Option<usize>,
    pub top_k: Option<usize>,
    pub timeout_ms: Option<u64>,
    pub discovery: Option<DiscoveryMode>,
}

#[derive(Clone, Debug)]
struct QuotePolicy {
    quality: SearchQuality,
    max_hops: usize,
    top_k: usize,
    timeout: Duration,
    discovery: DiscoveryMode,
}

impl QuotePolicy {
    fn resolve(config: &SidecarConfig, requested: QuoteOptions) -> Result<Self, ApiError> {
        let quality = requested.quality.unwrap_or(config.routing.default_quality);
        if quality == SearchQuality::Exhaustive && !config.routing.allow_exhaustive {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "exhaustive_disabled",
                "exhaustive search is disabled by server policy",
            ));
        }
        let max_hops = requested
            .max_hops
            .unwrap_or(config.routing.default_max_hops);
        let top_k = requested.top_k.unwrap_or(config.routing.default_top_k);
        let timeout = Duration::from_millis(
            requested
                .timeout_ms
                .unwrap_or(config.routing.default_timeout.as_millis() as u64),
        );
        if max_hops == 0 || max_hops > config.routing.max_hops {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "max_hops_out_of_range",
                format!("max_hops must be within 1..={}", config.routing.max_hops),
            ));
        }
        if top_k == 0 || top_k > config.routing.max_top_k {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "top_k_out_of_range",
                format!("top_k must be within 1..={}", config.routing.max_top_k),
            ));
        }
        if timeout.is_zero() || timeout > config.routing.max_timeout {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "timeout_out_of_range",
                format!(
                    "timeout_ms must be within 1..={}",
                    config.routing.max_timeout.as_millis()
                ),
            ));
        }
        Ok(Self {
            quality,
            max_hops,
            top_k,
            timeout,
            discovery: requested
                .discovery
                .unwrap_or(config.discovery.quote_default),
        })
    }

    fn search_mode(&self) -> SearchMode {
        match self.quality {
            SearchQuality::Fast => SearchMode::Heuristic(HeuristicSearchConfig::latency_first()),
            SearchQuality::Balanced | SearchQuality::Exhaustive => {
                SearchMode::Heuristic(HeuristicSearchConfig::balanced())
            }
        }
    }

    fn streaming(&self) -> StreamingSearchConfig {
        let config = StreamingSearchConfig::default().with_top_k(self.top_k);
        match self.quality {
            SearchQuality::Fast => config.fast_lane_only(),
            SearchQuality::Balanced => config.heuristic_only(),
            SearchQuality::Exhaustive => config.exhaustive(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrewarmMode {
    #[default]
    Ensure,
    Refresh,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct PrewarmRequest {
    #[serde(default)]
    pub connectors: Vec<String>,
    #[serde(default)]
    pub protocols: Vec<String>,
    #[serde(default)]
    pub mode: PrewarmMode,
    #[serde(default)]
    pub wait: bool,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Serialize)]
struct StatusResponse {
    node: crate::node::NodeStatus,
    tokens: Vec<TokenCoverage>,
}

#[derive(Serialize)]
struct QuoteResponse {
    token_in: String,
    token_out: String,
    amount_in: String,
    routes: Vec<RouteResponse>,
    finality: String,
    candidates_evaluated: usize,
    viable_routes_observed: usize,
    source: SourceResponse,
}

#[derive(Serialize)]
struct ExecutableQuoteResponse {
    warning: &'static str,
    source: SourceResponse,
    route_rank: usize,
    route: RouteResponse,
    min_amount_out: String,
    transaction: TransactionResponse,
    approval: Option<ApprovalResponse>,
    simulation: SimulationResponse,
}

#[derive(Serialize)]
struct TransactionResponse {
    chain_id: u64,
    from: String,
    to: String,
    value: String,
    data: String,
    deadline: String,
}

#[derive(Debug, Serialize)]
struct ApprovalResponse {
    required: bool,
    token: String,
    spender: String,
    minimum_amount: String,
    current_allowance: String,
    gas_estimate: Option<u64>,
    transaction: ApprovalTransactionResponse,
}

#[derive(Debug, Serialize)]
struct ApprovalTransactionResponse {
    chain_id: u64,
    from: String,
    to: String,
    value: String,
    data: String,
}

#[derive(Serialize)]
struct SimulationResponse {
    status: &'static str,
    amount_out: String,
    gas_estimate: u64,
}

#[derive(Serialize)]
struct SourceResponse {
    chain_id: u64,
    block_number: u64,
    block_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_timestamp: Option<u64>,
    state_version: u64,
    graph_revision: u64,
}

#[derive(Serialize)]
struct RouteResponse {
    amount_in: String,
    amount_out: String,
    hops: Vec<HopResponse>,
}

#[derive(Serialize)]
struct HopResponse {
    protocol: String,
    pool: String,
    token_in: String,
    token_out: String,
    amount_in: String,
    amount_out: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    approval: Option<Box<ApprovalResponse>>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            approval: None,
        }
    }

    fn with_approval(mut self, approval: ApprovalResponse) -> Self {
        self.approval = Some(Box::new(approval));
        self
    }

    fn bad_request(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            error.to_string(),
        )
    }

    fn unprocessable(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "request_unprocessable",
            error.to_string(),
        )
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error = %error, "request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "internal routing error",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                    approval: self.approval,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<Box<ApprovalResponse>>,
}

fn authorize_admin(config: &SidecarConfig, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = config.server.admin_bearer_token.as_deref() else {
        return Ok(());
    };
    let authorized = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value.as_bytes() == expected.as_bytes());
    if authorized {
        Ok(())
    } else {
        Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "admin_auth_required",
            "a valid admin bearer token is required",
        ))
    }
}

fn discovery_capacity_error(error: TryAcquireError) -> ApiError {
    ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        "discovery_capacity",
        format!("discovery request capacity reached: {error}"),
    )
}

fn quote_capacity_error(error: TryAcquireError) -> ApiError {
    ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        "quote_capacity",
        format!("quote request capacity reached: {error}"),
    )
}

fn simulation_capacity_error(error: TryAcquireError) -> ApiError {
    ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        "simulation_capacity",
        format!("executor simulation capacity reached: {error}"),
    )
}

fn addresses(values: &[Address]) -> String {
    values
        .iter()
        .map(|value| format!("{value:#x}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SidecarConfig {
        SidecarConfig::parse(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example.invalid"
            "#,
        )
        .unwrap()
    }

    #[test]
    fn policy_rejects_client_bounds_above_server_caps() {
        let mut config = test_config();
        config.routing.max_hops = 3;
        let error = QuotePolicy::resolve(
            &config,
            QuoteOptions {
                max_hops: Some(4),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.code, "max_hops_out_of_range");
    }

    #[test]
    fn omitted_quality_uses_balanced_heuristic_completion() {
        let config = test_config();
        let policy = QuotePolicy::resolve(&config, QuoteOptions::default()).unwrap();

        assert_eq!(policy.quality, SearchQuality::Balanced);
        assert_eq!(
            policy.search_mode(),
            SearchMode::Heuristic(HeuristicSearchConfig::balanced())
        );
        assert_eq!(
            policy.streaming().completion,
            evm_amm_search::StreamingCompletion::HeuristicExhausted
        );
    }
}
