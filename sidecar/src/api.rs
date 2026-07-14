use std::{future::Future, sync::Arc, time::Duration};

use alloy_primitives::{Address, U256};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use evm_amm_search::{
    HeuristicSearchConfig, LiveRouteRuntimeError, LiveRouteSubscription, RouteQuote, RouteRequest,
    RouteSubscriptionSpec, RouteSubscriptionState, SearchConfig, SearchMode, StreamingSearchConfig,
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
    node::{NodeStatus, PrepareTokenOptions, RoutingNode, parse_protocol, protocol_name},
};

/// Narrow contract between the HTTP boundary and a routing implementation.
///
/// Keeping this interface smaller than [`RoutingNode`] lets the HTTP contract
/// be tested without a live RPC connection while production still delegates to
/// the real graph and route runtimes.
pub trait RoutingBackend: Send + Sync + 'static {
    fn config(&self) -> &SidecarConfig;
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
}

impl RoutingBackend for RoutingNode {
    fn config(&self) -> &SidecarConfig {
        &self.config
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
}

pub struct AppState<B: RoutingBackend = RoutingNode> {
    backend: Arc<B>,
    quote_slots: Arc<Semaphore>,
    discovery_slots: Arc<Semaphore>,
}

impl<B: RoutingBackend> Clone for AppState<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            quote_slots: Arc::clone(&self.quote_slots),
            discovery_slots: Arc::clone(&self.discovery_slots),
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

async fn readyz<B: RoutingBackend>(
    State(state): State<AppState<B>>,
) -> Result<impl IntoResponse, ApiError> {
    let status = state.backend.status().await.map_err(ApiError::internal)?;
    if status.ready && status.runtime_health != "untrusted" {
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

    let snapshot = tokio::time::timeout(policy.timeout, async {
        loop {
            let snapshot = subscription.latest();
            match snapshot.state() {
                RouteSubscriptionState::Ready { .. }
                | RouteSubscriptionState::Failed { .. }
                | RouteSubscriptionState::Cancelled
                | RouteSubscriptionState::Closed
                | RouteSubscriptionState::RuntimeFailed { .. } => {
                    return Ok::<_, String>(snapshot);
                }
                RouteSubscriptionState::Pending { .. }
                | RouteSubscriptionState::Searching { .. } => {
                    subscription
                        .changed()
                        .await
                        .map_err(|error| error.to_string())?;
                }
                _ => {
                    subscription
                        .changed()
                        .await
                        .map_err(|error| error.to_string())?;
                }
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
    .map_err(ApiError::internal)?;

    match snapshot.state() {
        RouteSubscriptionState::Ready { source, report, .. } => Ok(Json(QuoteResponse {
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
                state_version: source.state_version().get(),
                graph_revision: source.graph_version().revision(),
            },
        })),
        RouteSubscriptionState::Failed { failure, .. } => Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "search_failed",
            failure.message(),
        )),
        RouteSubscriptionState::RuntimeFailed { failure } => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime_failed",
            failure.message(),
        )),
        RouteSubscriptionState::Cancelled | RouteSubscriptionState::Closed => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime_closed",
            "route runtime closed",
        )),
        RouteSubscriptionState::Pending { .. } | RouteSubscriptionState::Searching { .. } => {
            unreachable!("terminal state loop returned a non-terminal state")
        }
        _ => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unknown_route_state",
            "route runtime returned an unsupported state",
        )),
    }
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
struct SourceResponse {
    chain_id: u64,
    block_number: u64,
    block_hash: String,
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
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
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
