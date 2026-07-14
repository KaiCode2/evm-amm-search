use std::sync::{Arc, Mutex};

use alloy_primitives::Address;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use evm_amm_route_sidecar::{
    api::{AppState, RoutingBackend, router},
    config::SidecarConfig,
    coverage::{CoverageState, TokenCoverage},
    node::{NodeStatus, PrepareTokenOptions},
};
use evm_amm_search::{LiveRouteRuntimeError, LiveRouteSubscription, RouteSubscriptionSpec};
use evm_amm_state::adapters::SimConfig;
use tower::ServiceExt;

struct TestBackend {
    config: SidecarConfig,
    status: NodeStatus,
    prepared: Mutex<Vec<(Address, PrepareTokenOptions)>>,
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
                ready: false,
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
            },
            prepared: Mutex::new(Vec::new()),
        }
    }
}

impl RoutingBackend for TestBackend {
    fn config(&self) -> &SidecarConfig {
        &self.config
    }

    async fn status(&self) -> anyhow::Result<NodeStatus> {
        Ok(self.status.clone())
    }

    async fn token_coverages(&self) -> Vec<TokenCoverage> {
        unreachable!("liveness does not inspect token coverage")
    }

    async fn token_coverage(&self, _token: Address) -> anyhow::Result<TokenCoverage> {
        unreachable!("liveness does not inspect token coverage")
    }

    fn graph_contains(&self, _token: Address) -> bool {
        false
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
        unreachable!("liveness does not simulate routes")
    }

    async fn subscribe(
        &self,
        _spec: RouteSubscriptionSpec,
    ) -> Result<LiveRouteSubscription, LiveRouteRuntimeError> {
        unreachable!("liveness does not subscribe to routes")
    }
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
