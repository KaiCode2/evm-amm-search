use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use alloy_consensus::Header as ConsensusHeader;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::Header as RpcHeader;
use alloy_transport::mock::Asserter;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use evm_amm_route_sidecar::{
    SERVICE_VERSION, SOURCE_REVISION,
    api::{AppState, RoutingBackend, router},
    config::SidecarConfig,
    coverage::{CoverageState, TokenCoverage},
    execution::{
        ExecutionApprovalCheckRequest, ExecutionApprovalState, ExecutionSimulation,
        ExecutionSimulationRequest,
    },
    node::{CanonicalBlockContext, NodeStatus, PrepareTokenOptions, QuoteReadinessError},
};
use evm_amm_search::{
    GraphBuildOptions, LiveRouteRuntime, LiveRouteRuntimeConfig, LiveRouteRuntimeError,
    LiveRouteRuntimeHandle, LiveRouteSubscription, RouteSubscriptionSpec,
};
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmAdapter, AmmRuntime, AmmRuntimeBaseline, AmmRuntimeConfig,
    AmmRuntimeHandle, PoolKey, PoolRegistration, PoolStateDependencies, PoolStatus, ProtocolId,
    ProtocolMetadata, SimConfig, SimError, SwapQuote, UniswapV2Metadata,
};
use evm_fork_cache::cache::EvmCache;
use tower::ServiceExt;

struct TestBackend {
    config: SidecarConfig,
    status: NodeStatus,
    prepared: Mutex<Vec<(Address, PrepareTokenOptions)>>,
    _amm: Option<AmmRuntimeHandle>,
    routes: Option<LiveRouteRuntimeHandle>,
    graph_tokens: Vec<Address>,
    simulation_error: bool,
    simulation_amount_out: U256,
    approval_current_allowance: U256,
    block_timestamp: u64,
    head_timestamp: u64,
    quote_error: Option<QuoteReadinessError>,
    quote_generation: AtomicU64,
    advance_generation_on_subscribe: bool,
}

impl TestBackend {
    fn new() -> Self {
        Self {
            config: SidecarConfig::parse(
                r#"
                    extends = "ethereum-mainnet"
                    [rpc]
                    canonical_ws = "wss://rpc.example.invalid"
                "#,
            )
            .unwrap(),
            status: NodeStatus {
                service_version: SERVICE_VERSION,
                source_revision: SOURCE_REVISION,
                ready: false,
                routing_generation: 0,
                chain_id: 1,
                block_number: 0,
                block_hash: format!("{:#x}", alloy_primitives::B256::ZERO),
                state_version: 0,
                runtime_health: "untrusted".to_owned(),
                graph_tokens: 0,
                graph_edges: 0,
                graph_pools: 0,
                active_work: 0,
                queued_work: 0,
                profile_fingerprint: format!("{:#x}", alloy_primitives::B256::ZERO),
                canonical_connection_state: "untrusted".to_owned(),
                canonical_endpoint_index: 0,
                canonical_endpoint_count: 1,
                canonical_age_ms: 0,
                canonical_max_stale_ms: 45_000,
                reconnect_attempts: 0,
                subscriber_state: "failed".to_owned(),
                last_recovery_error: None,
            },
            prepared: Mutex::new(Vec::new()),
            _amm: None,
            routes: None,
            graph_tokens: Vec::new(),
            simulation_error: false,
            simulation_amount_out: U256::from(2_000),
            approval_current_allowance: U256::MAX,
            block_timestamp: 1_700_000_700,
            head_timestamp: 1_700_000_700,
            quote_error: None,
            quote_generation: AtomicU64::new(1),
            advance_generation_on_subscribe: false,
        }
    }
}

impl RoutingBackend for TestBackend {
    fn config(&self) -> &SidecarConfig {
        &self.config
    }

    fn quote_readiness(&self) -> Result<u64, QuoteReadinessError> {
        match &self.quote_error {
            Some(error) => Err(error.clone()),
            None => Ok(self.quote_generation.load(Ordering::Acquire)),
        }
    }

    async fn status(&self) -> anyhow::Result<NodeStatus> {
        Ok(self.status.clone())
    }

    async fn token_coverages(&self) -> Vec<TokenCoverage> {
        Vec::new()
    }

    async fn token_coverage(&self, _token: Address) -> anyhow::Result<TokenCoverage> {
        unreachable!("liveness does not inspect token coverage")
    }

    fn graph_contains(&self, token: Address) -> bool {
        self.graph_tokens.contains(&token)
    }

    async fn prepare_token(
        self: Arc<Self>,
        token: Address,
        options: PrepareTokenOptions,
    ) -> anyhow::Result<TokenCoverage> {
        self.prepared.lock().unwrap().push((token, options));
        Ok(TokenCoverage {
            token: format!("{token:#x}"),
            state: CoverageState::Queued,
            configured: false,
            graph_present: false,
            protocols: vec!["uniswap_v3".to_owned()],
            connectors: Vec::new(),
            pools: 0,
            jobs: 1,
            updated_at_unix_ms: 0,
            error: None,
        })
    }

    fn sim_config(&self) -> SimConfig {
        SimConfig::default()
    }

    async fn subscribe(
        &self,
        spec: RouteSubscriptionSpec,
    ) -> Result<LiveRouteSubscription, LiveRouteRuntimeError> {
        let subscription = self
            .routes
            .as_ref()
            .expect("test route runtime is configured")
            .subscribe(spec)
            .await?;
        if self.advance_generation_on_subscribe {
            self.quote_generation.fetch_add(1, Ordering::AcqRel);
        }
        Ok(subscription)
    }

    async fn simulate_executor(
        &self,
        _request: ExecutionSimulationRequest,
    ) -> anyhow::Result<ExecutionSimulation> {
        if self.simulation_error {
            anyhow::bail!("mock execution reverted");
        }
        Ok(ExecutionSimulation {
            amount_out: self.simulation_amount_out,
            gas_estimate: 123_456,
        })
    }

    async fn check_executor_approval(
        &self,
        _request: ExecutionApprovalCheckRequest,
    ) -> anyhow::Result<ExecutionApprovalState> {
        Ok(ExecutionApprovalState {
            current_allowance: self.approval_current_allowance,
            gas_estimate: (self.approval_current_allowance < U256::from(1_000)).then_some(45_000),
        })
    }

    async fn canonical_block_context(
        &self,
        _block_number: u64,
        _block_hash: B256,
    ) -> anyhow::Result<CanonicalBlockContext> {
        Ok(CanonicalBlockContext {
            source_timestamp: self.block_timestamp,
            head_timestamp: self.head_timestamp,
        })
    }
}

#[tokio::test]
async fn status_reports_the_release_identity() {
    let app = router(AppState::new(Arc::new(TestBackend::new())));

    let response = app
        .oneshot(Request::get("/v1/status").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["node"]["service_version"], env!("CARGO_PKG_VERSION"));
    assert!(
        body["node"]["source_revision"]
            .as_str()
            .is_some_and(|revision| !revision.is_empty())
    );
}

struct RouteAdapter {
    delay: std::time::Duration,
}

impl AmmAdapter for RouteAdapter {
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
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
        Ok(SwapQuote::new(amount_in * U256::from(2)))
    }
}

async fn route_backend(worker_threads: usize, delay: std::time::Duration) -> TestBackend {
    let token_in = Address::repeat_byte(0x01);
    let token_out = Address::repeat_byte(0x02);
    let header = RpcHeader::new(ConsensusHeader {
        parent_hash: B256::repeat_byte(0x69),
        number: 700,
        timestamp: 1_700_000_700,
        base_fee_per_gas: Some(800),
        beneficiary: Address::repeat_byte(0xcb),
        gas_limit: 30_000_000,
        mix_hash: B256::repeat_byte(0xab),
        ..ConsensusHeader::default()
    });
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = EvmCache::new(Arc::new(provider)).await;
    cache.advance_block(&header).unwrap();
    let mut registry = AdapterRegistry::new();
    registry
        .register_adapter(Arc::new(RouteAdapter { delay }))
        .unwrap();
    registry
        .register_pool(
            PoolRegistration::new(PoolKey::UniswapV2(Address::repeat_byte(0x70)))
                .with_metadata(ProtocolMetadata::UniswapV2(
                    UniswapV2Metadata::default()
                        .with_token0(token_in)
                        .with_token1(token_out)
                        .with_fee_bps(30),
                ))
                .with_status(PoolStatus::Ready),
        )
        .unwrap();
    let amm = AmmRuntime::spawn(
        cache,
        registry,
        AmmRuntimeBaseline::from_verified_header(1, header).unwrap(),
        AmmRuntimeConfig::default(),
    )
    .unwrap();
    let routes = LiveRouteRuntime::spawn(
        &amm,
        GraphBuildOptions::default(),
        LiveRouteRuntimeConfig::default().with_worker_threads(worker_threads),
    )
    .await
    .unwrap();
    let mut backend = TestBackend::new();
    backend.graph_tokens = vec![token_in, token_out];
    backend._amm = Some(amm);
    backend.routes = Some(routes);
    backend
}

async fn executable_backend() -> TestBackend {
    let mut backend = route_backend(1, std::time::Duration::ZERO).await;
    backend.config.executor.enabled = true;
    backend.config.executor.router = Address::repeat_byte(0x44);
    backend.config.executor.weth = Address::repeat_byte(0x66);
    backend.config.executor.permit2 = Address::repeat_byte(0x77);
    backend.config.executor.max_snapshot_age = std::time::Duration::from_secs(u64::MAX);
    backend
}

fn quote_request(amount_in: u64) -> Request<Body> {
    Request::post("/v1/quote")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{
                "token_in":"0x0101010101010101010101010101010101010101",
                "token_out":"0x0202020202020202020202020202020202020202",
                "amount_in":"{amount_in}",
                "options":{{"quality":"balanced","top_k":1,"timeout_ms":2000,"discovery":"off"}}
            }}"#
        )))
        .unwrap()
}

#[tokio::test]
async fn liveness_is_available_without_touching_the_routing_backend() {
    let app = router(AppState::new(Arc::new(TestBackend::new())));

    let response = app
        .oneshot(Request::get("/livez").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn readiness_fails_closed_with_a_stable_error_body() {
    let app = router(AppState::new(Arc::new(TestBackend::new())));

    let response = app
        .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "not_ready");
}

#[tokio::test]
async fn readiness_succeeds_only_for_a_trusted_ready_runtime() {
    let mut backend = TestBackend::new();
    backend.status.ready = true;
    backend.status.runtime_health = "healthy".to_owned();
    let app = router(AppState::new(Arc::new(backend)));

    let response = app
        .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn readiness_uses_the_same_stale_state_gate_as_quotes() {
    let mut backend = TestBackend::new();
    backend.status.ready = true;
    backend.status.runtime_health = "healthy".to_owned();
    backend.quote_error = Some(QuoteReadinessError::Stale {
        age_ms: 46_000,
        max_age_ms: 45_000,
    });
    let app = router(AppState::new(Arc::new(backend)));

    let response = app
        .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn indicative_quotes_fail_closed_when_canonical_state_is_stale() {
    let mut backend = TestBackend::new();
    backend.quote_error = Some(QuoteReadinessError::Stale {
        age_ms: 46_000,
        max_age_ms: 45_000,
    });
    let app = router(AppState::new(Arc::new(backend)));

    let response = app.oneshot(quote_request(1_000)).await.unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "canonical_stale");
}

#[tokio::test]
async fn executable_quotes_fail_closed_when_runtime_is_untrusted() {
    let mut backend = TestBackend::new();
    backend.config.executor.enabled = true;
    backend.quote_error = Some(QuoteReadinessError::Untrusted(
        "canonical subscriber failed".to_owned(),
    ));
    let app = router(AppState::new(Arc::new(backend)));

    let response = app
        .oneshot(
            Request::post("/v1/executable-quote")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "token_in":"0x0101010101010101010101010101010101010101",
                        "token_out":"0x0202020202020202020202020202020202020202",
                        "amount_in":"1000",
                        "sender":"0x1111111111111111111111111111111111111111",
                        "recipient":"0x2222222222222222222222222222222222222222",
                        "authorization":{"type":"allowance"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "runtime_untrusted");
}

#[tokio::test]
async fn prewarm_requires_the_configured_admin_bearer_token() {
    let mut backend = TestBackend::new();
    backend.config.server.admin_bearer_token = Some("release-secret".to_owned());
    let app = router(AppState::new(Arc::new(backend)));

    let response = app
        .oneshot(
            Request::put("/v1/tokens/0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32/prewarm")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"mode":"ensure"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "admin_auth_required");
}

#[tokio::test]
async fn authorized_prewarm_accepts_refresh_options_and_returns_queued_state() {
    let mut backend = TestBackend::new();
    backend.config.server.admin_bearer_token = Some("release-secret".to_owned());
    let backend = Arc::new(backend);
    let app = router(AppState::new(Arc::clone(&backend)));

    let response = app
        .oneshot(
            Request::put("/v1/tokens/0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32/prewarm")
                .header("authorization", "Bearer release-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"mode":"refresh","wait":true,"protocols":["uniswap_v3"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let prepared = backend.prepared.lock().unwrap();
    assert_eq!(prepared.len(), 1);
    assert!(prepared[0].1.refresh);
    assert!(prepared[0].1.wait);
}

#[tokio::test]
async fn quote_with_discovery_disabled_fails_when_graph_coverage_is_missing() {
    let app = router(AppState::new(Arc::new(TestBackend::new())));

    let response = app
        .oneshot(
            Request::post("/v1/quote")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "token_in":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                        "token_out":"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                        "amount_in":"1000000",
                        "options":{"discovery":"off"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "coverage_missing");
}

#[tokio::test]
async fn every_response_carries_a_request_id() {
    let app = router(AppState::new(Arc::new(TestBackend::new())));

    let response = app
        .oneshot(Request::get("/livez").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-request-id"));
}

#[tokio::test]
async fn quote_request_bodies_are_limited_by_server_policy() {
    let mut backend = TestBackend::new();
    backend.config.server.max_request_bytes = 64;
    let app = router(AppState::new(Arc::new(backend)));

    let response = app
        .oneshot(
            Request::post("/v1/quote")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"padding":"{}"}}"#,
                    "x".repeat(128)
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn quote_options_cannot_exceed_server_search_limits() {
    let app = router(AppState::new(Arc::new(TestBackend::new())));

    let response = app
        .oneshot(
            Request::post("/v1/quote")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "token_in":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                        "token_out":"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                        "amount_in":"1000000",
                        "options":{"max_hops":99}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "max_hops_out_of_range");
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_quotes_preserve_independent_results() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = route_backend(4, std::time::Duration::from_millis(2)).await;
            backend.config.server.max_in_flight_quotes = 64;
            let app = router(AppState::new(Arc::new(backend)));
            let started = std::time::Instant::now();
            let responses = futures::future::join_all((0..64_u64).map(|index| {
                let app = app.clone();
                async move {
                    let amount_in = 1_000 + index;
                    let response = app.oneshot(quote_request(amount_in)).await.unwrap();
                    (amount_in, response)
                }
            }))
            .await;

            for (amount_in, response) in responses {
                assert_eq!(response.status(), StatusCode::OK);
                let body: serde_json::Value =
                    serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                        .unwrap();
                assert_eq!(body["amount_in"], amount_in.to_string());
                assert_eq!(body["routes"][0]["amount_out"], (amount_in * 2).to_string());
            }
            eprintln!(
                "parallel quote characterization: requests=64 workers=4 elapsed_ms={}",
                started.elapsed().as_millis()
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn quote_overload_fails_fast_at_the_configured_capacity() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = route_backend(1, std::time::Duration::from_millis(50)).await;
            backend.config.server.max_in_flight_quotes = 4;
            let app = router(AppState::new(Arc::new(backend)));
            let responses = futures::future::join_all((0..20_u64).map(|index| {
                let app = app.clone();
                async move { app.oneshot(quote_request(1_000 + index)).await.unwrap() }
            }))
            .await;

            let succeeded = responses
                .iter()
                .filter(|response| response.status() == StatusCode::OK)
                .count();
            let rejected = responses
                .iter()
                .filter(|response| response.status() == StatusCode::TOO_MANY_REQUESTS)
                .count();
            assert_eq!(succeeded, 4);
            assert_eq!(rejected, 16);

            for response in responses
                .into_iter()
                .filter(|response| response.status() == StatusCode::TOO_MANY_REQUESTS)
            {
                let body: serde_json::Value =
                    serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                        .unwrap();
                assert_eq!(body["error"]["code"], "quote_capacity");
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn quote_is_rejected_if_the_routing_generation_changes_in_flight() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = route_backend(1, std::time::Duration::ZERO).await;
            backend.advance_generation_on_subscribe = true;
            let app = router(AppState::new(Arc::new(backend)));

            let response = app.oneshot(quote_request(1_000)).await.unwrap();

            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["error"]["code"], "routing_generation_changed");
        })
        .await;
}

#[tokio::test]
async fn executable_quote_endpoint_fails_closed_when_disabled() {
    let app = router(AppState::new(Arc::new(TestBackend::new())));

    let response = app
        .oneshot(
            Request::post("/v1/executable-quote")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "token_in":"0x0101010101010101010101010101010101010101",
                        "token_out":"0x0202020202020202020202020202020202020202",
                        "amount_in":"1000",
                        "sender":"0x1111111111111111111111111111111111111111",
                        "recipient":"0x2222222222222222222222222222222222222222",
                        "authorization":{"type":"allowance"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "executor_disabled");
}

#[tokio::test]
async fn executable_quote_rejects_slippage_above_server_policy() {
    let mut backend = TestBackend::new();
    backend.config.executor.enabled = true;
    backend.config.executor.max_slippage_bps = 100;
    let app = router(AppState::new(Arc::new(backend)));

    let response = app
        .oneshot(
            Request::post("/v1/executable-quote")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "token_in":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                        "token_out":"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                        "amount_in":"1000000",
                        "sender":"0x0000000000000000000000000000000000000011",
                        "recipient":"0x0000000000000000000000000000000000000022",
                        "slippage_bps":101,
                        "authorization":{"type":"allowance"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "slippage_out_of_range");
}

#[tokio::test]
async fn executable_quote_rejects_two_minimum_output_policies() {
    let mut backend = TestBackend::new();
    backend.config.executor.enabled = true;
    let app = router(AppState::new(Arc::new(backend)));

    let response = app
        .oneshot(
            Request::post("/v1/executable-quote")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "token_in":"0x0101010101010101010101010101010101010101",
                        "token_out":"0x0202020202020202020202020202020202020202",
                        "amount_in":"1000",
                        "sender":"0x1111111111111111111111111111111111111111",
                        "recipient":"0x2222222222222222222222222222222222222222",
                        "min_amount_out":"900000",
                        "slippage_bps":50,
                        "authorization":{"type":"allowance"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "conflicting_minimum_output");
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_returns_snapshot_bound_calldata_and_approval() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let app = router(AppState::new(Arc::new(executable_backend().await)));

            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                        "token_in":"0x0101010101010101010101010101010101010101",
                        "token_out":"0x0202020202020202020202020202020202020202",
                        "amount_in":"1000",
                        "sender":"0x1111111111111111111111111111111111111111",
                        "recipient":"0x2222222222222222222222222222222222222222",
                        "slippage_bps":100,
                        "deadline_secs":60,
                        "authorization":{"type":"allowance"},
                        "options":{"top_k":1,"discovery":"off"}
                    }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["source"]["block_number"], 700);
            assert_eq!(body["source"]["block_timestamp"], 1_700_000_700_u64);
            assert_eq!(body["route"]["amount_out"], "2000");
            assert_eq!(
                body["transaction"]["from"],
                "0x1111111111111111111111111111111111111111"
            );
            assert_eq!(
                body["transaction"]["to"],
                "0x4444444444444444444444444444444444444444"
            );
            assert_eq!(body["transaction"]["value"], "0");
            assert_eq!(body["transaction"]["deadline"], "1700000760");
            assert!(
                body["transaction"]["data"]
                    .as_str()
                    .unwrap()
                    .starts_with("0x")
            );
            assert_eq!(body["min_amount_out"], "1980");
            assert_eq!(
                body["approval"]["spender"],
                "0x4444444444444444444444444444444444444444"
            );
            assert_eq!(body["approval"]["minimum_amount"], "1000");
            assert_eq!(body["approval"]["required"], false);
            assert_eq!(body["approval"]["current_allowance"], U256::MAX.to_string());
            assert_eq!(
                body["approval"]["transaction"]["to"],
                "0x0101010101010101010101010101010101010101"
            );
            assert!(
                body["approval"]["transaction"]["data"]
                    .as_str()
                    .unwrap()
                    .starts_with("0x095ea7b3")
            );
            assert_eq!(body["simulation"]["status"], "succeeded");
            assert_eq!(body["simulation"]["amount_out"], "2000");
            assert_eq!(body["simulation"]["gas_estimate"], 123456);
            assert!(body["warning"].as_str().unwrap().contains("EXPERIMENTAL"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_returns_a_structured_approval_precondition() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = executable_backend().await;
            backend.approval_current_allowance = U256::ZERO;
            let app = router(AppState::new(Arc::new(backend)));

            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                                "token_in":"0x0101010101010101010101010101010101010101",
                                "token_out":"0x0202020202020202020202020202020202020202",
                                "amount_in":"1000",
                                "sender":"0x1111111111111111111111111111111111111111",
                                "recipient":"0x2222222222222222222222222222222222222222",
                                "min_amount_out":"1900",
                                "authorization":{"type":"allowance"},
                                "options":{"top_k":1,"discovery":"off"}
                            }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["error"]["code"], "approval_required");
            assert_eq!(body["error"]["approval"]["required"], true);
            assert_eq!(body["error"]["approval"]["current_allowance"], "0");
            assert_eq!(body["error"]["approval"]["gas_estimate"], 45000);
            assert_eq!(
                body["error"]["approval"]["transaction"]["to"],
                "0x0101010101010101010101010101010101010101"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_uses_the_signed_absolute_erc2612_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let app = router(AppState::new(Arc::new(executable_backend().await)));
            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                                "token_in":"0x0101010101010101010101010101010101010101",
                                "token_out":"0x0202020202020202020202020202020202020202",
                                "amount_in":"1000",
                                "sender":"0x1111111111111111111111111111111111111111",
                                "recipient":"0x2222222222222222222222222222222222222222",
                                "min_amount_out":"1900",
                                "deadline":"1700000760",
                                "authorization":{
                                    "type":"erc2612",
                                    "v":27,
                                    "r":"0x1111111111111111111111111111111111111111111111111111111111111111",
                                    "s":"0x2222222222222222222222222222222222222222222222222222222222222222"
                                },
                                "options":{"top_k":1,"discovery":"off"}
                            }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["transaction"]["deadline"], "1700000760");
            assert!(body["approval"].is_null());
        })
        .await;
}

#[tokio::test]
async fn executable_quote_requires_an_absolute_erc2612_deadline() {
    let mut backend = TestBackend::new();
    backend.config.executor.enabled = true;
    let app = router(AppState::new(Arc::new(backend)));
    let response = app
        .oneshot(
            Request::post("/v1/executable-quote")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "token_in":"0x0101010101010101010101010101010101010101",
                        "token_out":"0x0202020202020202020202020202020202020202",
                        "amount_in":"1000",
                        "sender":"0x1111111111111111111111111111111111111111",
                        "recipient":"0x2222222222222222222222222222222222222222",
                        "min_amount_out":"1900",
                        "authorization":{
                            "type":"erc2612",
                            "v":27,
                            "r":"0x1111111111111111111111111111111111111111111111111111111111111111",
                            "s":"0x2222222222222222222222222222222222222222222222222222222222222222"
                        }
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "erc2612_deadline_required");
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_rejects_a_stale_canonical_snapshot() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = executable_backend().await;
            backend.config.executor.max_snapshot_age = std::time::Duration::from_secs(1);
            backend.head_timestamp = backend.block_timestamp + 2;
            let app = router(AppState::new(Arc::new(backend)));
            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                                "token_in":"0x0101010101010101010101010101010101010101",
                                "token_out":"0x0202020202020202020202020202020202020202",
                                "amount_in":"1000",
                                "sender":"0x1111111111111111111111111111111111111111",
                                "recipient":"0x2222222222222222222222222222222222222222",
                                "min_amount_out":"1900",
                                "authorization":{"type":"allowance"},
                                "options":{"top_k":1,"discovery":"off"}
                            }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["error"]["code"], "stale_snapshot");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_can_wrap_native_input_without_an_approval() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = executable_backend().await;
            backend.config.executor.weth = Address::repeat_byte(0x01);
            let app = router(AppState::new(Arc::new(backend)));
            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                                "token_in":"0x0101010101010101010101010101010101010101",
                                "token_out":"0x0202020202020202020202020202020202020202",
                                "amount_in":"1000",
                                "sender":"0x1111111111111111111111111111111111111111",
                                "recipient":"0x2222222222222222222222222222222222222222",
                                "min_amount_out":"1900",
                                "authorization":{"type":"native"},
                                "options":{"top_k":1,"discovery":"off"}
                            }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["transaction"]["value"], "1000");
            assert!(body["approval"].is_null());
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_embeds_permit2_and_discloses_its_token_approval() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let app = router(AppState::new(Arc::new(executable_backend().await)));
            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                                "token_in":"0x0101010101010101010101010101010101010101",
                                "token_out":"0x0202020202020202020202020202020202020202",
                                "amount_in":"1000",
                                "sender":"0x1111111111111111111111111111111111111111",
                                "recipient":"0x2222222222222222222222222222222222222222",
                                "min_amount_out":"1900",
                                "authorization":{
                                    "type":"permit2",
                                    "nonce":"7",
                                    "deadline":"2000000000",
                                    "signature":"0x1234"
                                },
                                "options":{"top_k":1,"discovery":"off"}
                            }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["transaction"]["value"], "0");
            assert_eq!(
                body["approval"]["spender"],
                "0x7777777777777777777777777777777777777777"
            );
            assert_eq!(body["approval"]["minimum_amount"], "1000");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_fails_closed_when_the_transaction_reverts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = executable_backend().await;
            backend.simulation_error = true;
            let app = router(AppState::new(Arc::new(backend)));
            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                                "token_in":"0x0101010101010101010101010101010101010101",
                                "token_out":"0x0202020202020202020202020202020202020202",
                                "amount_in":"1000",
                                "sender":"0x1111111111111111111111111111111111111111",
                                "recipient":"0x2222222222222222222222222222222222222222",
                                "min_amount_out":"1900",
                                "authorization":{"type":"allowance"},
                                "options":{"top_k":1,"discovery":"off"}
                            }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["error"]["code"], "simulation_failed");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn executable_quote_rejects_output_that_differs_from_the_selected_route() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut backend = executable_backend().await;
            backend.simulation_amount_out = U256::from(1_999);
            let app = router(AppState::new(Arc::new(backend)));
            let response = app
                .oneshot(
                    Request::post("/v1/executable-quote")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{
                                "token_in":"0x0101010101010101010101010101010101010101",
                                "token_out":"0x0202020202020202020202020202020202020202",
                                "amount_in":"1000",
                                "sender":"0x1111111111111111111111111111111111111111",
                                "recipient":"0x2222222222222222222222222222222222222222",
                                "min_amount_out":"1900",
                                "authorization":{"type":"allowance"},
                                "options":{"top_k":1,"discovery":"off"}
                            }"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap())
                    .unwrap();
            assert_eq!(body["error"]["code"], "simulation_quote_mismatch");
        })
        .await;
}
