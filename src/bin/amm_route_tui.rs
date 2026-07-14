#![allow(dead_code)] // The headless bootstrap profiler retains legacy diagnostic helpers.

#[path = "amm_route_tui/rpc_profile.rs"]
mod rpc_profile;
#[path = "amm_route_tui/warm_store.rs"]
mod warm_store;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs, io,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use alloy_consensus::BlockHeader as _;
use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_network::{AnyNetwork, Ethereum, Network, primitives::BlockResponse as _};
use alloy_primitives::{Address, B256, Bytes, U256, address, hex, keccak256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_client::{ClientBuilder, RpcClient, WsConnect};
use alloy_rpc_types_eth::{Filter, Log as RpcLog, TransactionInput, TransactionRequest};
use alloy_transport_balancer::{
    BatchingConfig, BatchingTransport, EndpointConfig, HttpClientConfig, LoadBalancedTransport,
    Weight,
};
use anyhow::{Context, Result, bail};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use evm_amm_search::{
    AffectedPools, AmmGraph, AmmSearcher, DEMO_ROUTER, DemoRouterConfig, FastLaneConfig,
    GraphBuildOptions, HeuristicSearchConfig, Hop, IncrementalRouteUpdateStatus,
    LiquidityIndexScope, LiquidityPruningConfig, LiveAmmGraph, LiveRouteRuntime,
    LiveRouteRuntimeConfig, LiveRouteRuntimeHandle, LiveSearchView, ParallelSearchConfig,
    PoolLiquidityIndex, RouteCancellationToken, RoutePath, RouteQuote, RouteRequest,
    RouteSearchEvent, RouteSearchPhase, RouteSearchSession, RouteSubscriptionSnapshot,
    RouteSubscriptionSpec, RouteSubscriptionState, RouteUpdateEvent, SearchConfig, SearchControl,
    SearchError, SearchFinality, SearchMode, StreamingSearchConfig, SwapGasEstimate,
    VersionedRouteQuote, demo_router_runtime, encode_demo_router_execute_calldata,
    install_demo_router, prewarm_demo_router_token_transfer, simulate_route_gas,
    simulate_versioned_route_gas_with_balance_mappings,
};
use evm_amm_state::adapters::{
    AdapterRegistry, AmmColdStartOptions, AmmColdStartWorkerConfig, AmmColdStartWorkerHandle,
    AmmColdStartWorkerState, AmmDiscoveryOptions, AmmFactoryWatcherRegistration, AmmObserverError,
    AmmRegistrationArchive, AmmRuntime, AmmRuntimeBaseline, AmmRuntimeConfig, AmmRuntimeEventKind,
    AmmRuntimeHandle, AmmRuntimeStatusSnapshot, AmmSubscriberDriverConfig,
    AmmSubscriberDriverHandle, AmmSubscriberDriverState, AmmSyncEngine, AmmWorkClass, AmmWorkKind,
    ColdStartOutcome, ColdStartPolicy, ConcentratedLiquidityAdapter, CurveAdapter, CurveMetadata,
    CurveVariant, DiscoveryOwnerId, DiscoveryOwnerKey, FactoryConfig, PoolDiscovery, PoolKey,
    PoolQuery, PoolRegistration, PoolRuntimeState, PoolStatus, ProtocolId, ProtocolMetadata,
    RuntimeOwnerId, RuntimeWorkId, SimConfig, TokenEdgeDiscoveryRequest, UniswapV2Adapter,
    UniswapV2FactoryConfig, UniswapV2Metadata, UniswapV3FactoryConfig, V3Metadata,
};
use evm_fork_cache::bulk_storage::BulkCallConfig;
use evm_fork_cache::cache::{
    AccountProof, CacheConfig, CacheSpeedMode, EvmCache, SharedMemoryCapacity, StorageBatchConfig,
    StorageFetchStrategy,
};
use evm_fork_cache::mapping_probe::TrackedMapping;
use evm_fork_cache::reactive::{
    AlloySubscriber, BlockRef, ChainStatus, InputSource, ReactiveContext, ReactiveInput,
    ReactiveInputBatch, ReactiveInputRecord, SubscriberConfig, SubscriberMode,
};
use evm_fork_cache::{PreparedAccountPatch, PreparedAccountValue};
use futures::StreamExt;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use ratatui::{
    Frame, Terminal,
    backend::{Backend, CrosstermBackend, TestBackend},
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};
use reqwest::Url;
use rpc_profile::{RpcProfileLayer, RpcProfilePhase, RpcProfileTransport};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, watch};

type Header = <AnyNetwork as Network>::HeaderResponse;

const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const DAI: Address = address!("6B175474E89094C44Da98b954EedeAC495271d0F");
const WBTC: Address = address!("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599");
const USDT: Address = address!("dAC17F958D2ee523a2206206994597C13D831ec7");
const USDE: Address = address!("4c9EDD5852cd905f086C759E8383e09bff1E68B3");
const SUSDE: Address = address!("9d39a5de30e57443bff2a8307a4256c8797a3497");
const LINK: Address = address!("514910771AF9Ca656af840dff83E8264EcF986CA");
const UNI: Address = address!("1f9840a85d5aF5bf1D1762F925BDADdC4201F984");
const AAVE: Address = address!("7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9");
const MKR: Address = address!("9f8F72aA9304c8B593d555F12eF6589cC3A579A2");
const CRV: Address = address!("D533a949740bb3306d119CC777fa900bA034cd52");
const LDO: Address = address!("5A98FcBEA516Cf06857215779Fd812CA3beF1B32");
const FRAX: Address = address!("853d955aCEf822Db058eb8505911ED77F175b99e");
const SUSHI: Address = address!("6B3595068778DD592e39A122f4f5a5cF09C90fE2");
const PEPE: Address = address!("6982508145454Ce325dDbE47a25d4ec3d2311933");

const V2_ROUTER_02: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const V3_QUOTER_V2: Address = address!("61fFE014bA17989E743c5F6cB21bF9697530B21e");
// Sushi's canonical Ethereum deployment:
// https://github.com/sushiswap/v3-periphery/blob/master/deployments/ethereum/QuoterV2.json
const SUSHISWAP_V3_QUOTER_V2: Address = address!("64e8802FE490fa7cc61d3463958199161Bb608A7");
const PANCAKE_V3_QUOTER_V2: Address = address!("B048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997");

const UNISWAP_V2_FACTORY: Address = address!("5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
const SUSHISWAP_V2_FACTORY: Address = address!("C0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac");
const UNISWAP_V3_FACTORY: Address = address!("1F98431c8aD98523631AE4a59f267346ea31F984");
const SUSHISWAP_V3_FACTORY: Address = address!("bACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F");
const PANCAKESWAP_V3_FACTORY: Address = address!("0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865");

const CURVE_3POOL: Address = address!("bEbc44782C7dB0a1A60Cb6Fe97d0B483032FF1C7");
const CURVE_FRAX_USDC: Address = address!("DcEF968d416a41Cdac0ED8702fAC8128A64241A2");
const TRICRYPTO_USDC_NG: Address = address!("7F86Bf177Dd4F3494b841a37e810A34dD56c829B");

const DEFAULT_MAX_STARTUP_POOLS: usize = 128;
const DEFAULT_MAX_DYNAMIC_POOLS: usize = 64;
const DEFAULT_DYNAMIC_CONNECTORS: usize = 12;
const MAX_ALTERNATIVE_PATHS: usize = 6;
const QUOTE_REFRESH_DEBOUNCE: Duration = Duration::from_millis(350);
const DEFAULT_TUI_CACHE_DIR: &str = ".cache/amm-route-tui";
const DEFAULT_PRICE_SOURCE: &str = "coingecko";
const DEFAULT_TOKEN_REGISTRY_URL: &str = "https://tokens.uniswap.org";
const DEFAULT_KEYLESS_PRICE_REFRESH_SECS: usize = 60;
const DEFAULT_KEYED_PRICE_REFRESH_SECS: usize = 60;
const DEFAULT_KEYLESS_COINGECKO_REQUESTS_PER_REFRESH: usize = 10;
const DEFAULT_MAX_VALUE_LOSS_BPS: u16 = 1_000;
const DEFAULT_TENDERLY_OUTPUT_TOLERANCE_BPS: u16 = 1;
const DEFAULT_KEYED_COINGECKO_REQUESTS_PER_REFRESH: usize = 60;
const DEFAULT_KEYLESS_COINGECKO_REQUEST_DELAY_MS: usize = 2_100;
const DEFAULT_KEYED_COINGECKO_REQUEST_DELAY_MS: usize = 500;
const DEFAULT_SIMULATE_FROM: Address = address!("000000000000000000000000000000000000dEaD");
const STARTUP_STEPS: usize = 8;
const ERC20_SYMBOL_SELECTOR: [u8; 4] = [0x95, 0xd8, 0x9b, 0x41];
const ERC20_DECIMALS_SELECTOR: [u8; 4] = [0x31, 0x3c, 0xe5, 0x67];

#[derive(Clone)]
struct TokenInfo {
    symbol: String,
    address: Address,
    decimals: u8,
}

impl TokenInfo {
    fn new(symbol: &str, address: Address, decimals: u8) -> Self {
        Self {
            symbol: symbol.to_owned(),
            address,
            decimals,
        }
    }
}

#[derive(Clone)]
struct PoolInfo {
    label: String,
    registration: PoolRegistration,
}

#[derive(Clone, Default)]
struct PriceBook {
    usd_by_token: HashMap<Address, f64>,
    source: String,
    last_updated: Option<Instant>,
    last_error: Option<String>,
}

impl PriceBook {
    fn disabled() -> Self {
        Self {
            usd_by_token: HashMap::new(),
            source: "prices disabled".to_owned(),
            last_updated: None,
            last_error: None,
        }
    }

    fn coverage_label(&self, token_count: usize) -> String {
        if self.source == "prices disabled" || self.source.starts_with("prices unavailable") {
            return self.source.clone();
        }
        let age = self
            .last_updated
            .map(|updated| format!(" age={}s", updated.elapsed().as_secs()))
            .unwrap_or_default();
        let error = self
            .last_error
            .as_deref()
            .map(|error| format!(" warn={}", fit_to_width(error, 24)))
            .unwrap_or_default();
        format!(
            "{} prices {}/{}{}{}",
            self.source,
            self.usd_by_token.len(),
            token_count,
            age,
            error
        )
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct TuiUserConfig {
    replace_default_tokens: bool,
    tokens: Vec<UserTokenConfig>,
    pools: Vec<UserPoolConfig>,
    curve_pools: Vec<UserCurvePoolConfig>,
    network: TuiNetworkConfig,
}

#[derive(Deserialize)]
#[serde(default)]
struct TuiNetworkConfig {
    rpc_endpoints: Vec<TuiRpcEndpointConfig>,
    cold_start_concurrency: Option<usize>,
    rpc_batch_size: usize,
    point_read_slots_per_batch: usize,
    point_read_concurrency: usize,
    bulk_max_slots_per_call: usize,
    bulk_max_slots_per_request: usize,
    bulk_max_request_bytes: usize,
    bulk_max_concurrent_calls: usize,
    max_log_addresses_per_subscription: usize,
}

impl Default for TuiNetworkConfig {
    fn default() -> Self {
        Self {
            rpc_endpoints: Vec::new(),
            cold_start_concurrency: None,
            rpc_batch_size: 150,
            point_read_slots_per_batch: 150,
            point_read_concurrency: 8,
            bulk_max_slots_per_call: 25_000,
            bulk_max_slots_per_request: 25_000,
            bulk_max_request_bytes: 2_400_000,
            bulk_max_concurrent_calls: 4,
            max_log_addresses_per_subscription: 1_024,
        }
    }
}

#[derive(Deserialize)]
struct TuiRpcEndpointConfig {
    url: String,
    weight: Option<u32>,
    max_request_bytes: Option<usize>,
    max_in_flight: Option<usize>,
}

#[derive(Deserialize)]
struct UserTokenConfig {
    symbol: String,
    address: String,
    decimals: u8,
}

#[derive(Deserialize)]
struct UserCurvePoolConfig {
    label: String,
    address: String,
    coins: Vec<String>,
    variant: Option<String>,
    #[serde(default)]
    discovered_slots: Vec<String>,
}

#[derive(Deserialize)]
struct UserPoolConfig {
    protocol: String,
    address: String,
    label: Option<String>,
    tokens: Option<Vec<String>>,
    fee_bps: Option<u32>,
    fee: Option<u32>,
    variant: Option<String>,
}

#[derive(Deserialize)]
struct CoingeckoTokenPrice {
    usd: Option<f64>,
}

#[derive(Deserialize)]
struct TokenRegistryList {
    tokens: Vec<TokenRegistryEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenRegistryEntry {
    chain_id: u64,
    address: String,
    symbol: String,
    decimals: u8,
}

struct CoingeckoPriceSettings {
    base_url: String,
    timeout: Duration,
    request_delay: Duration,
    requests_per_refresh: usize,
    api_key: Option<CoingeckoApiKey>,
}

struct CoingeckoApiKey {
    value: String,
    header_name: &'static str,
    tier: &'static str,
}

#[derive(Clone)]
struct TenderlyConfig {
    api_key: String,
    account_slug: String,
    project_slug: String,
    from: Address,
}

#[derive(Clone)]
struct TenderlyUiState {
    config: Option<TenderlyConfig>,
    status: String,
    in_flight: bool,
    ok: Option<bool>,
}

struct TenderlySimulationRequest {
    api_key: String,
    endpoint: String,
    dashboard_url: String,
    payload: Value,
    block_number: u64,
    block_hash: B256,
    expected_amount_out: U256,
    output_symbol: String,
    output_decimals: u8,
    output_tolerance_bps: u16,
}

struct TenderlySimulationOutcome {
    url: String,
    gas_used: Option<u64>,
    block_number: u64,
    transaction_index: u64,
    amount_out: U256,
    output_delta: U256,
    output_symbol: String,
    output_decimals: u8,
}

#[derive(Debug)]
struct TenderlyValidation {
    gas_used: Option<u64>,
    amount_out: U256,
    output_delta: U256,
}

#[derive(Clone)]
struct LiveTenderlyQuote {
    view: Arc<LiveSearchView>,
    quote: Arc<VersionedRouteQuote>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActiveField {
    Input,
    Output,
    Amount,
    TokenSearch,
    TokenAddress,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Input,
    Output,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Side::Input => "input",
            Side::Output => "output",
        }
    }

    fn active_field(self) -> ActiveField {
        match self {
            Side::Input => ActiveField::Input,
            Side::Output => ActiveField::Output,
        }
    }
}

enum UiEvent {
    Key(KeyEvent),
}

enum UiAction {
    Continue,
    RequestQuote,
    SelectToken { side: Side },
    SimulateSwap,
    DiscoverToken { side: Side, query: String },
    Quit,
}

enum ChainEvent {
    Log(RpcLog),
    Block(Box<Header>),
    Error(String),
}

enum PriceEvent {
    Completed { result: Result<PriceBook, String> },
}

enum GasPriceEvent {
    Completed { result: Result<u128, String> },
}

enum SimulationEvent {
    Completed {
        result: Result<TenderlySimulationOutcome, String>,
    },
}

#[derive(Clone)]
struct GasEstimateView {
    summary: String,
    detail: String,
    gas_used: Option<u64>,
    ok: bool,
    estimate: Option<SwapGasEstimate>,
}

enum QuoteRefresh {
    Full,
    Incremental(AffectedPools),
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChainSyncPhase {
    Synced,
    Applying,
    Degraded,
}

struct ChainSyncView {
    phase: ChainSyncPhase,
    detail: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GraphSyncPhase {
    Synced,
    Warming,
    Error,
}

struct GraphSyncView {
    phase: GraphSyncPhase,
    routing_pools: usize,
    discovered_pools: usize,
    loading_pools: usize,
    queued_loads: usize,
    pending_state_updates: usize,
    degraded_pools: usize,
    failed_pools: usize,
    detail: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct GraphSyncCounts {
    routing_pools: usize,
    discovered_pools: usize,
    loading_pools: usize,
    queued_loads: usize,
    pending_state_updates: usize,
    degraded_pools: usize,
    failed_pools: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RouteWorkPhase {
    Ready,
    Discovering,
    Quoting,
    Error,
}

struct RouteWorkView {
    phase: RouteWorkPhase,
    detail: String,
    progress: Option<(usize, usize)>,
    started_at: Option<Instant>,
}

struct ChainProcessResult {
    refresh: QuoteRefresh,
    phase: ChainSyncPhase,
    detail: String,
}

struct BootstrappedTui {
    app: AppState,
    sync: AmmSyncEngine,
    cache: EvmCache,
    provider: Arc<RootProvider<AnyNetwork>>,
    sim_config: SimConfig,
    ui_rx: mpsc::Receiver<UiEvent>,
    chain_rx: mpsc::Receiver<ChainEvent>,
    timings: BootstrapTimings,
}

enum LiveBootstrapEvent {
    Progress(String),
    Ready(Box<LiveTuiRuntime>),
    Failed(String),
}

enum LiveBootstrapOutcome {
    Quit,
    Ready(Box<LiveTuiRuntime>),
}

struct LiveTuiRuntime {
    provider: Arc<RootProvider<AnyNetwork>>,
    amm: AmmRuntimeHandle,
    routes: LiveRouteRuntimeHandle,
    subscriber: AmmSubscriberDriverHandle,
    cold_start: AmmColdStartWorkerHandle,
    discovery: Vec<(ProtocolId, DiscoveryOwnerId)>,
    sim_config: SimConfig,
    gas_router_ready: bool,
    gas_router_status: String,
    gas_balance_mappings: HashMap<Address, TrackedMapping>,
    warm_session: warm_store::WarmSession,
}

#[derive(Default)]
struct LiveBootstrapResources {
    amm: Option<AmmRuntimeHandle>,
    routes: Option<LiveRouteRuntimeHandle>,
    subscriber: Option<AmmSubscriberDriverHandle>,
    cold_start: Option<AmmColdStartWorkerHandle>,
}

impl LiveBootstrapResources {
    async fn shutdown(&mut self) {
        if let Some(routes) = self.routes.take() {
            let _ = routes.shutdown().await;
        }
        if let Some(subscriber) = self.subscriber.take() {
            let _ = subscriber.shutdown().await;
        }
        if let Some(cold_start) = self.cold_start.take() {
            cold_start.shutdown();
        }
        if let Some(amm) = self.amm.take() {
            let _ = amm.shutdown().await;
        }
    }
}

enum LiveRouteUiEvent {
    Attached {
        cancellation: RouteCancellationToken,
    },
    Snapshot {
        generation: u64,
        snapshot: Arc<RouteSubscriptionSnapshot>,
    },
    Failed {
        generation: u64,
        message: String,
    },
}

#[derive(Clone)]
struct LiveRouteDriverRequest {
    generation: u64,
    spec: RouteSubscriptionSpec,
}

struct DynamicDiscoveryEvent {
    generation: u64,
    side: Side,
    query: String,
    outcome: Result<DynamicDiscoveryOutcome, String>,
}

struct DynamicDiscoveryOutcome {
    token: TokenInfo,
    discovery: Result<usize, String>,
}

struct SelectedTokenDiscoveryPlan {
    token: TokenInfo,
    connectors: Vec<Address>,
}

#[derive(Default)]
struct LiveRouteUiController {
    generation: u64,
    cancellation: Option<RouteCancellationToken>,
    requests: Option<watch::Sender<LiveRouteDriverRequest>>,
}

impl LiveRouteUiController {
    fn begin_replacement(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.generation
    }

    fn accepts(&self, generation: u64) -> bool {
        self.generation == generation
    }
}

async fn wait_for_live_bootstrap<B: Backend>(
    terminal: &mut Terminal<B>,
    app: &mut AppState,
    ui_rx: &mut mpsc::Receiver<UiEvent>,
    bootstrap_rx: &mut mpsc::Receiver<LiveBootstrapEvent>,
) -> Result<LiveBootstrapOutcome>
where
    B::Error: Send + Sync + 'static,
{
    loop {
        terminal.draw(|frame| draw(frame, app))?;
        tokio::select! {
            Some(event) = ui_rx.recv() => {
                match handle_ui_event(app, event) {
                    UiAction::Quit => return Ok(LiveBootstrapOutcome::Quit),
                    UiAction::Continue
                    | UiAction::RequestQuote
                    | UiAction::SelectToken { .. } => {
                        app.status = "runtime bootstrap continues in the background".to_owned();
                    }
                    UiAction::SimulateSwap | UiAction::DiscoverToken { .. } => {
                        app.status = "this action becomes available when the live runtime is ready".to_owned();
                    }
                }
            }
            event = bootstrap_rx.recv() => match event {
                Some(LiveBootstrapEvent::Progress(detail)) => {
                    app.status = detail.clone();
                    set_route_work(app, RouteWorkPhase::Discovering, detail, None);
                }
                Some(LiveBootstrapEvent::Ready(runtime)) => {
                    return Ok(LiveBootstrapOutcome::Ready(runtime));
                }
                Some(LiveBootstrapEvent::Failed(error)) => {
                    set_route_work(app, RouteWorkPhase::Error, fit_to_width(&error, 48), None);
                    bail!("live runtime bootstrap failed: {error}");
                }
                None => bail!("live runtime bootstrap task stopped before readiness"),
            }
        }
    }
}

/// Per-phase wall-clock breakdown of [`bootstrap_tui`], captured so the headless
/// `AMM_ROUTE_TUI_BENCH=1` mode can report exactly where cold-start and warm-start
/// load time goes. Every field is the elapsed time of one bootstrap phase, in the
/// order the phases run.
#[derive(Default)]
struct BootstrapTimings {
    persist_cache: bool,
    warm_cache: bool,
    ws_connect: Duration,
    chain_meta: Duration,
    cache_build: Duration,
    discovery: Duration,
    cold_start: Duration,
    register_ready: Duration,
    liquidity_index: Duration,
    prime: Duration,
    subscribe: Duration,
    first_search: Duration,
    gas_sim: Duration,
    flush: Duration,
    discovered_pools: usize,
    ready_pools: usize,
    fast_pools: usize,
    fallback_pools: usize,
    token_nodes: usize,
    directed_edges: usize,
}

/// Classify cold-start outcomes into (fast-bundled, sequential-fallback) counts.
/// The bundled fast path injects state straight into the cache and leaves the
/// report's `applied` `StateDiff` empty; the per-pool fallback path finalizes
/// through a planner that materializes a non-empty `applied` diff. Unsupported
/// pools carry no report and count as neither.
fn classify_cold_start_outcomes(outcomes: &[ColdStartOutcome]) -> (usize, usize) {
    let mut fast = 0;
    let mut fallback = 0;
    for outcome in outcomes {
        let report = match outcome {
            ColdStartOutcome::Ready(report)
            | ColdStartOutcome::ReadyWithDeferred(report, _)
            | ColdStartOutcome::NeedsRepair(report, _) => report,
            ColdStartOutcome::Unsupported(_) => continue,
            _ => continue,
        };
        if report.applied.is_empty() {
            fast += 1;
        } else {
            fallback += 1;
        }
    }
    (fast, fallback)
}

impl BootstrapTimings {
    /// Sum of every timed phase — the wall-clock a user waits before the TUI is
    /// interactive (minus terminal draw overhead, which is negligible headless).
    fn total(&self) -> Duration {
        self.ws_connect
            + self.chain_meta
            + self.cache_build
            + self.discovery
            + self.cold_start
            + self.register_ready
            + self.liquidity_index
            + self.prime
            + self.subscribe
            + self.first_search
            + self.gas_sim
            + self.flush
    }

    fn print_report(&self) {
        let total = self.total();
        let total_ms = total.as_secs_f64() * 1e3;
        let pct = |d: Duration| {
            if total_ms > 0.0 {
                (d.as_secs_f64() * 1e3) / total_ms * 100.0
            } else {
                0.0
            }
        };
        let mode = if !self.persist_cache {
            "in-memory (no disk cache)"
        } else if self.warm_cache {
            "warm (disk cache present)"
        } else {
            "cold (fresh disk cache)"
        };
        println!("\n=== amm-route-tui bootstrap profile ===");
        println!("mode:            {mode}");
        println!(
            "pools:           {} discovered, {} ready ({} fast-bundled, {} sequential-fallback)",
            self.discovered_pools, self.ready_pools, self.fast_pools, self.fallback_pools
        );
        println!(
            "graph:           {} token nodes, {} directed edges",
            self.token_nodes, self.directed_edges
        );
        println!("{:-<52}", "");
        println!("{:<22}{:>14}{:>10}", "phase", "time", "share");
        println!("{:-<52}", "");
        let row = |label: &str, d: Duration| {
            println!("{label:<22}{:>14}{:>9.1}%", format!("{d:?}"), pct(d));
        };
        row("ws connect", self.ws_connect);
        row("chain meta (3 rpc)", self.chain_meta);
        row("cache build", self.cache_build);
        row("discovery", self.discovery);
        row("cold_start_many", self.cold_start);
        row("register ready", self.register_ready);
        row("liquidity index", self.liquidity_index);
        row("prime cache", self.prime);
        row("subscribe", self.subscribe);
        row("first search", self.first_search);
        row("gas simulation", self.gas_sim);
        row("cache flush", self.flush);
        println!("{:-<52}", "");
        println!("{:<22}{:>14}{:>9.1}%", "total", format!("{total:?}"), 100.0);
    }
}

#[derive(Clone, Copy)]
enum GraphStyle {
    Normal,
    Highlight,
    Warning,
    Secondary,
    Dim,
}

struct GraphLine {
    text: String,
    style: GraphStyle,
}

#[derive(Clone)]
struct RouteTokenView {
    symbol: String,
    address: Address,
    edge_count: usize,
}

#[derive(Clone)]
struct RouteLegView {
    token_in: String,
    token_out: String,
    token_in_address: Address,
    token_out_address: Address,
    amount_in: String,
    amount_out: String,
    pool: String,
    pool_address: Option<Address>,
}

#[derive(Clone)]
struct AlternativePathView {
    label: String,
    replaced_leg: usize,
    legs: Vec<RouteLegView>,
}

#[derive(Clone)]
struct RouteViz {
    tokens: Vec<RouteTokenView>,
    selected_legs: Vec<RouteLegView>,
    alternatives: Vec<AlternativePathView>,
}

struct QuoteView {
    best: RouteQuote,
    block_number: u64,
    live_tenderly: Option<LiveTenderlyQuote>,
    output: String,
    gas: Option<GasEstimateView>,
    route: RouteViz,
    quoted_routes: Vec<String>,
    quoted_venues: String,
    warnings: Vec<String>,
    coverage: String,
    stream_lines: Vec<String>,
    routes: usize,
    graph_pools: usize,
    elapsed: Duration,
}

struct QuoteViewStats {
    routes: usize,
    graph_pools: usize,
    elapsed: Duration,
    block_number: u64,
}

struct TokenSearchState {
    side: Side,
    query: String,
    selected: usize,
}

struct AppState {
    tokens: Vec<TokenInfo>,
    pools: Vec<PoolInfo>,
    route_session: Option<ActiveRouteSession>,
    liquidity_index: Option<PoolLiquidityIndex>,
    input_index: usize,
    output_index: usize,
    amount: String,
    amount_editing: bool,
    active: ActiveField,
    token_search: Option<TokenSearchState>,
    custom_address: String,
    custom_side: Side,
    quote: Option<QuoteView>,
    quote_error: Option<String>,
    quote_loading: bool,
    chain_sync: ChainSyncView,
    graph_sync: GraphSyncView,
    route_work: RouteWorkView,
    last_block: u64,
    applied_logs: u64,
    routed_logs: u64,
    ignored_logs: u64,
    resync_updates: u64,
    resync_failures: u64,
    degraded_pools: u64,
    recovered_pools: u64,
    ready_pools: usize,
    skipped_pools: usize,
    quote_updates: u64,
    topology_updates: u64,
    prices: PriceBook,
    gas_price_wei: Option<u128>,
    gas_router_ready: bool,
    gas_router_status: String,
    gas_balance_mappings: HashMap<Address, TrackedMapping>,
    tenderly: TenderlyUiState,
    status: String,
}

struct ActiveRouteSession {
    session: RouteSearchSession,
    topology_updates: u64,
}

impl AppState {
    fn new(
        tokens: Vec<TokenInfo>,
        pools: Vec<PoolInfo>,
        last_block: u64,
        ready_pools: usize,
        skipped_pools: usize,
    ) -> Self {
        let input_index = env_token_index(&tokens, "AMM_ROUTE_TUI_INPUT")
            .or_else(|| tokens.iter().position(|token| token.symbol == "USDC"))
            .unwrap_or(0);
        let output_index = env_token_index(&tokens, "AMM_ROUTE_TUI_OUTPUT")
            .or_else(|| tokens.iter().position(|token| token.symbol == "WETH"))
            .unwrap_or_else(|| usize::from(input_index == 0 && tokens.len() > 1));
        let amount = std::env::var("AMM_ROUTE_TUI_AMOUNT").unwrap_or_else(|_| "1000".to_owned());
        Self {
            tokens,
            pools,
            route_session: None,
            liquidity_index: None,
            input_index,
            output_index,
            amount,
            amount_editing: false,
            active: ActiveField::Input,
            token_search: None,
            custom_address: String::new(),
            custom_side: Side::Input,
            quote: None,
            quote_error: None,
            quote_loading: false,
            chain_sync: ChainSyncView {
                phase: ChainSyncPhase::Synced,
                detail: format!("synced block {last_block}"),
            },
            graph_sync: GraphSyncView {
                phase: GraphSyncPhase::Synced,
                routing_pools: ready_pools,
                discovered_pools: 0,
                loading_pools: 0,
                queued_loads: 0,
                pending_state_updates: 0,
                degraded_pools: 0,
                failed_pools: 0,
                detail: format!("routing {ready_pools}, loading 0, queued 0, updates 0"),
            },
            route_work: RouteWorkView {
                phase: RouteWorkPhase::Ready,
                detail: "waiting for first quote".to_owned(),
                progress: None,
                started_at: None,
            },
            last_block,
            applied_logs: 0,
            routed_logs: 0,
            ignored_logs: 0,
            resync_updates: 0,
            resync_failures: 0,
            degraded_pools: 0,
            recovered_pools: 0,
            ready_pools,
            skipped_pools,
            quote_updates: 0,
            topology_updates: 0,
            prices: PriceBook::disabled(),
            gas_price_wei: None,
            gas_router_ready: false,
            gas_router_status: "demo router not installed".to_owned(),
            gas_balance_mappings: HashMap::new(),
            tenderly: tenderly_ui_state_from_env(),
            status: "starting".to_owned(),
        }
    }

    fn token_in(&self) -> TokenInfo {
        self.tokens[self.input_index].clone()
    }

    fn token_out(&self) -> TokenInfo {
        self.tokens[self.output_index].clone()
    }

    fn select_next_field(&mut self) {
        self.token_search = None;
        self.amount_editing = false;
        self.active = match self.active {
            ActiveField::Input => ActiveField::Output,
            ActiveField::Output => ActiveField::Amount,
            ActiveField::Amount | ActiveField::TokenSearch | ActiveField::TokenAddress => {
                ActiveField::Input
            }
        };
    }

    fn select_prev_field(&mut self) {
        self.token_search = None;
        self.amount_editing = false;
        self.active = match self.active {
            ActiveField::Input | ActiveField::TokenSearch | ActiveField::TokenAddress => {
                ActiveField::Amount
            }
            ActiveField::Output => ActiveField::Input,
            ActiveField::Amount => ActiveField::Output,
        };
    }

    fn begin_amount_edit(&mut self) {
        self.token_search = None;
        self.amount_editing = true;
        self.active = ActiveField::Amount;
        self.status = "editing amount".to_owned();
    }

    fn finish_amount_edit(&mut self) {
        self.amount_editing = false;
        self.status = "amount edit complete".to_owned();
    }

    fn begin_custom_token(&mut self) {
        let search_side = self.token_search.as_ref().map(|search| search.side);
        self.custom_side = match self.active {
            ActiveField::Output => Side::Output,
            ActiveField::TokenSearch => search_side.unwrap_or(Side::Input),
            _ => Side::Input,
        };
        self.token_search = None;
        self.amount_editing = false;
        self.custom_address.clear();
        self.active = ActiveField::TokenAddress;
        self.status = format!(
            "enter token address or symbol for {} token",
            self.custom_side.label()
        );
    }

    fn cancel_custom_token(&mut self) {
        self.custom_address.clear();
        self.amount_editing = false;
        self.active = self.custom_side.active_field();
        self.status = "token lookup cancelled".to_owned();
    }

    fn begin_token_search(&mut self, side: Side, initial: Option<char>) {
        let mut query = String::new();
        if let Some(c) = initial {
            query.push(c);
        }
        self.active = ActiveField::TokenSearch;
        self.amount_editing = false;
        self.token_search = Some(TokenSearchState {
            side,
            query,
            selected: 0,
        });
        self.status = format!("searching {} token list", side.label());
    }

    fn cancel_token_search(&mut self) {
        if let Some(search) = self.token_search.take() {
            self.active = search.side.active_field();
            self.status = "token search cancelled".to_owned();
        }
    }

    fn move_token_search_selection(&mut self, delta: isize) {
        let Some(search) = &self.token_search else {
            return;
        };
        let matches = token_search_matches(&self.tokens, &search.query);
        if matches.is_empty() {
            return;
        }
        let selected = cycle_index(search.selected.min(matches.len() - 1), matches.len(), delta);
        if let Some(search) = &mut self.token_search {
            search.selected = selected;
        }
    }

    fn append_token_search_char(&mut self, c: char) {
        if let Some(search) = &mut self.token_search {
            search.query.push(c);
            search.selected = 0;
        }
    }

    fn pop_token_search_char(&mut self) {
        if let Some(search) = &mut self.token_search {
            search.query.pop();
            search.selected = 0;
        }
    }

    fn upsert_token(&mut self, token: TokenInfo) -> usize {
        if let Some(index) = self
            .tokens
            .iter()
            .position(|known| known.address == token.address)
        {
            return index;
        }
        self.tokens.push(token);
        self.tokens.len() - 1
    }

    fn select_token_index(&mut self, index: usize, side: Side) {
        self.token_search = None;
        self.active = side.active_field();
        match side {
            Side::Input => {
                self.input_index = index;
                if self.input_index == self.output_index && self.tokens.len() > 1 {
                    self.output_index = cycle_index(self.output_index, self.tokens.len(), 1);
                }
            }
            Side::Output => {
                self.output_index = index;
                if self.output_index == self.input_index && self.tokens.len() > 1 {
                    self.input_index = cycle_index(self.input_index, self.tokens.len(), 1);
                }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    let Some(ws_url) = endpoint_from_env() else {
        println!(
            "amm-route-tui: set ETH_WS_URL=wss://... or E2E_RPC_URL=https://... to run the live TUI"
        );
        return Ok(());
    };

    // Headless bootstrap profiler: run the exact TUI load path against a
    // throwaway in-memory backend, print the per-phase timing breakdown, and
    // exit before the interactive loop. Used to benchmark cold/warm start.
    if env_bool("AMM_ROUTE_TUI_BENCH", false) {
        return run_bench(ws_url).await;
    }

    run_tui(ws_url).await
}

/// Drive the same progressive bootstrap used by the interactive TUI and report
/// when each phase becomes visible. Set `AMM_ROUTE_TUI_LEGACY_BENCH=1` only
/// when comparing against the retired synchronous bootstrap path.
async fn run_bench(ws_url: String) -> Result<()> {
    if !env_bool("AMM_ROUTE_TUI_LEGACY_BENCH", false) {
        return run_live_bootstrap_bench(ws_url).await;
    }

    run_legacy_bench(ws_url).await
}

async fn run_live_bootstrap_bench(ws_url: String) -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let user_config = load_tui_user_config().context("load TUI config")?;
            let tokens = token_list(&user_config).context("load token list")?;
            let benchmark_tokens = tokens.clone();
            let (tx, mut rx) = mpsc::channel(32);
            let (_cancel, cancelled) = watch::channel(false);
            let started = Instant::now();
            let route_timeout = Duration::from_secs(
                env_usize("AMM_ROUTE_TUI_BENCH_ROUTE_TIMEOUT_SECS", 30) as u64,
            );
            let bootstrap_timeout = Duration::from_secs(
                env_usize(
                    "AMM_ROUTE_TUI_BENCH_BOOTSTRAP_TIMEOUT_SECS",
                    route_timeout.as_secs() as usize,
                ) as u64,
            );
            let bootstrap = tokio::task::spawn_local(bootstrap_live_tui(
                ws_url,
                user_config,
                tokens,
                tx,
                cancelled,
            ));

            let runtime = tokio::time::timeout(bootstrap_timeout, async {
                loop {
                    match rx.recv().await {
                        Some(LiveBootstrapEvent::Progress(detail)) => {
                            eprintln!("[live-bootstrap +{:?}] {detail}", started.elapsed());
                        }
                        Some(LiveBootstrapEvent::Ready(runtime)) => break Ok(*runtime),
                        Some(LiveBootstrapEvent::Failed(error)) => bail!(error),
                        None => bail!("live bootstrap ended before publishing runtime handles"),
                    }
                }
            })
            .await
            .with_context(|| {
                format!("live runtime handles were not ready within {bootstrap_timeout:?}")
            })??;
            println!(
                "live runtime handles ready after {:?}; initial graph usable and subscriber attached",
                started.elapsed()
            );
            bootstrap.await.context("join live bootstrap task")?;

            let failure_observer = start_bench_failure_observer(&runtime.amm);

            let (token_in, token_out) = startup_focus_pair(&benchmark_tokens)
                .context("benchmark token list has no distinct startup pair")?;
            let decimals = benchmark_tokens
                .iter()
                .find(|token| token.address == token_in)
                .map_or(18, |token| token.decimals);
            rpc_profile::set_phase(RpcProfilePhase::FirstRoute);
            let mut route = runtime
                .routes
                .subscribe(RouteSubscriptionSpec::new(
                    RouteRequest::new(token_in, token_out, ten_pow(decimals))
                        .with_config(tui_search_config_for_tokens(&benchmark_tokens))
                        .with_sim_config(runtime.sim_config),
                    tui_streaming_config(),
                ))
                .await?;
            let mut status = runtime.amm.subscribe_status();
            let measurement: Result<()> = async {
                let (ready, milestone) = tokio::time::timeout(route_timeout, async {
                    loop {
                        tokio::select! {
                            changed = route.changed() => {
                                let snapshot = changed.context("live route benchmark subscription closed")?;
                                let milestone = match snapshot.state() {
                                    RouteSubscriptionState::Searching { provisional: Some(_), .. } => Some("provisional"),
                                    RouteSubscriptionState::Ready { best: Some(_), .. } => Some("ready"),
                                    // A route-local miss is recoverable during progressive
                                    // bootstrap: every later topology commit invalidates this
                                    // snapshot and searches the expanded graph again.
                                    RouteSubscriptionState::Ready { best: None, .. }
                                    | RouteSubscriptionState::Failed { .. } => None,
                                    RouteSubscriptionState::RuntimeFailed { failure } => {
                                        bail!("live route runtime failed: {failure:?}")
                                    }
                                    RouteSubscriptionState::Cancelled
                                    | RouteSubscriptionState::Closed => {
                                        bail!("startup route subscription closed before a result")
                                    }
                                    _ => None,
                                };
                                if let Some(milestone) = milestone {
                                    return Ok::<_, anyhow::Error>((snapshot, milestone));
                                }
                            }
                            changed = status.changed() => {
                                changed.context("AMM status stream closed during live benchmark")?;
                                print_live_benchmark_progress(started, &status.borrow_and_update());
                            }
                        }
                    }
                })
                .await
                .with_context(|| format!("no usable startup route within {route_timeout:?}"))??;
                println!(
                    "first usable streamed route ({milestone}) after {:?} at state v{} with {} ready pool(s)",
                    started.elapsed(),
                    ready.view().snapshot().version().get(),
                    ready.view().snapshot().registry().pool_count(),
                );

                let idle_timeout =
                    env_usize("AMM_ROUTE_TUI_BENCH_IDLE_TIMEOUT_SECS", 0) as u64;
                if idle_timeout > 0 {
                    let timeout = Duration::from_secs(idle_timeout);
                    tokio::time::timeout(timeout, async {
                        loop {
                            let current = status.borrow_and_update().clone();
                            let counts = graph_sync_counts(&current);
                            if current.active_work_items().next().is_none()
                                && counts.loading_pools == 0
                                && counts.queued_loads == 0
                                && counts.pending_state_updates == 0
                            {
                                return Ok::<_, anyhow::Error>(());
                            }
                            status.changed().await.context("AMM status stream closed before idle")?;
                            print_live_benchmark_progress(started, &status.borrow_and_update());
                        }
                    })
                    .await
                    .with_context(|| format!("graph did not finish warming within {timeout:?}"))??;
                    println!("graph synced after {:?}", started.elapsed());
                }
                Ok(())
            }
            .await;

            let final_snapshot = measurement
                .is_ok()
                .then(|| (route.latest(), status.borrow_and_update().clone()));
            if let Some((route_snapshot, status_snapshot)) = final_snapshot {
                print_live_benchmark_final(
                    started,
                    &route_snapshot,
                    &status_snapshot,
                    &benchmark_tokens,
                    &runtime.gas_balance_mappings,
                );
            }

            rpc_profile::print_report();

            finish_bench_failure_observer(failure_observer, "pre-shutdown runtime work failures")
                .await;
            let cancel = route.cancel().await;
            let shutdown = shutdown_live_tui_runtime_with_archive(runtime).await;
            measurement?;
            cancel?;
            shutdown
        })
        .await
}

#[derive(Default)]
struct BenchFailureSummary {
    total: usize,
    skipped_events: u64,
    by_owner: BTreeMap<String, usize>,
    by_protocol: BTreeMap<&'static str, usize>,
    by_message: BTreeMap<String, usize>,
    samples: Vec<String>,
}

struct BenchFailureObserver {
    failures: Arc<Mutex<BenchFailureSummary>>,
    handle: tokio::task::JoinHandle<()>,
}

fn start_bench_failure_observer(amm: &AmmRuntimeHandle) -> Option<BenchFailureObserver> {
    if !env_bool("AMM_ROUTE_TUI_BENCH", false) {
        return None;
    }
    let failures = Arc::new(Mutex::new(BenchFailureSummary::default()));
    let mut observer = amm.subscribe_events();
    let observed = Arc::clone(&failures);
    let handle = tokio::task::spawn_local(async move {
        loop {
            let event = match observer.next_event().await {
                Ok(event) => event,
                Err(AmmObserverError::Lagged { skipped }) => {
                    observed.lock().await.record_observer_lag(skipped);
                    continue;
                }
                Err(AmmObserverError::Closed) => break,
                Err(_) => break,
            };
            if let AmmRuntimeEventKind::WorkFailed { work, message } = event.kind() {
                observed.lock().await.record(work, message);
            }
        }
    });
    Some(BenchFailureObserver { failures, handle })
}

async fn finish_bench_failure_observer(observer: Option<BenchFailureObserver>, label: &str) {
    let Some(observer) = observer else {
        return;
    };
    observer.handle.abort();
    let _ = observer.handle.await;
    println!("{label}:");
    observer.failures.lock().await.print_report();
}

impl BenchFailureSummary {
    fn record_observer_lag(&mut self, skipped: u64) {
        self.skipped_events = self.skipped_events.saturating_add(skipped);
    }

    fn record(&mut self, work: &RuntimeWorkId, message: &str) {
        self.total += 1;
        *self
            .by_owner
            .entry(work_owner_label(work).to_owned())
            .or_default() += 1;
        *self
            .by_protocol
            .entry(work_protocol_label(work).unwrap_or("unknown"))
            .or_default() += 1;
        *self
            .by_message
            .entry(fit_to_width(message, 180))
            .or_default() += 1;
        if self.samples.len() < 12 {
            self.samples.push(format!(
                "{} {}: {}",
                work_owner_detail(work),
                work.work().get(),
                fit_to_width(message, 180)
            ));
        }
    }

    fn print_report(&self) {
        println!("runtime work failures: {}", self.total);
        if self.skipped_events > 0 {
            println!(
                "observer skipped {} event(s); recorded failure totals are a lower bound",
                self.skipped_events
            );
        }
        if self.total == 0 {
            return;
        }
        println!(
            "failure owners: {}",
            format_count_map(
                self.by_owner
                    .iter()
                    .map(|(label, count)| (label.as_str(), *count))
            )
        );
        println!(
            "failure protocols: {}",
            format_count_map(
                self.by_protocol
                    .iter()
                    .map(|(label, count)| (*label, *count))
            )
        );
        println!("top failure messages:");
        for (message, count) in top_counts(&self.by_message, 8) {
            println!("  {count}x {message}");
        }
        println!("failure samples:");
        for sample in &self.samples {
            println!("  {sample}");
        }
    }
}

fn work_owner_label(work: &RuntimeWorkId) -> &'static str {
    match work.owner() {
        RuntimeOwnerId::Pool(_) => "pool",
        RuntimeOwnerId::Adapter(_) => "adapter",
        RuntimeOwnerId::Discovery(_) => "discovery",
        _ => "other",
    }
}

fn work_protocol_label(work: &RuntimeWorkId) -> Option<&'static str> {
    match work.owner() {
        RuntimeOwnerId::Pool(pool) => Some(protocol_id_label(pool.key().protocol())),
        RuntimeOwnerId::Adapter(adapter) => adapter
            .key()
            .protocols()
            .first()
            .copied()
            .map(protocol_id_label),
        RuntimeOwnerId::Discovery(_) => None,
        _ => None,
    }
}

fn work_owner_detail(work: &RuntimeWorkId) -> String {
    match work.owner() {
        RuntimeOwnerId::Pool(pool) => {
            let protocol = protocol_id_label(pool.key().protocol());
            let address = pool
                .key()
                .address()
                .map(short_address)
                .unwrap_or_else(|| format!("{:?}", pool.key()));
            format!("{protocol} pool {address} work")
        }
        RuntimeOwnerId::Adapter(adapter) => {
            let protocols = adapter
                .key()
                .protocols()
                .iter()
                .copied()
                .map(protocol_id_label)
                .collect::<Vec<_>>()
                .join("/");
            format!("adapter {protocols} work")
        }
        RuntimeOwnerId::Discovery(owner) => {
            format!("discovery {} work", owner.key().as_str())
        }
        _ => "unknown-owner work".to_owned(),
    }
}

fn protocol_id_label(protocol: ProtocolId) -> &'static str {
    match protocol {
        ProtocolId::UniswapV2 => "V2",
        ProtocolId::UniswapV3 => "V3",
        ProtocolId::PancakeV3 => "Pancake V3",
        ProtocolId::Slipstream => "Slipstream",
        ProtocolId::SolidlyV2 => "SolidlyV2",
        ProtocolId::BalancerV2 => "BalancerV2",
        ProtocolId::Curve => "Curve",
        #[cfg(feature = "experimental-protocols")]
        ProtocolId::BalancerV3 => "BalancerV3",
        #[cfg(feature = "experimental-protocols")]
        ProtocolId::Erc4626 => "ERC4626",
        #[cfg(feature = "experimental-protocols")]
        ProtocolId::UniswapV4 => "UniswapV4",
        ProtocolId::Custom(_) => "Custom",
        _ => "Protocol",
    }
}

fn format_count_map<'a>(items: impl Iterator<Item = (&'a str, usize)>) -> String {
    items
        .map(|(label, count)| format!("{label}={count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn top_counts<K: Ord + ToString>(
    counts: &BTreeMap<K, usize>,
    limit: usize,
) -> Vec<(String, usize)> {
    let mut pairs = counts
        .iter()
        .map(|(key, count)| (key.to_string(), *count))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    pairs.truncate(limit);
    pairs
}

fn print_live_benchmark_final(
    started: Instant,
    route: &RouteSubscriptionSnapshot,
    status: &AmmRuntimeStatusSnapshot,
    tokens: &[TokenInfo],
    gas_balance_mappings: &HashMap<Address, TrackedMapping>,
) {
    let counts = graph_sync_counts(status);
    println!(
        "final live graph after {:?}: routing={} discovered={} loading={} queued={} updates={} degraded={} failed={}",
        started.elapsed(),
        counts.routing_pools,
        counts.discovered_pools,
        counts.loading_pools,
        counts.queued_loads,
        counts.pending_state_updates,
        counts.degraded_pools,
        counts.failed_pools,
    );

    let mut protocols = BTreeMap::<&'static str, usize>::new();
    let registry = route.view().snapshot().registry();
    for (_, instance) in registry.pools() {
        if let Some(registration) = registry.pool(instance) {
            *protocols.entry(protocol_label(registration)).or_default() += 1;
        }
    }
    let protocol_summary = protocols
        .into_iter()
        .map(|(protocol, count)| format!("{protocol}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    println!("final live graph protocols: {protocol_summary}");

    let best = match route.state() {
        RouteSubscriptionState::Searching {
            provisional: Some(best),
            ..
        }
        | RouteSubscriptionState::Ready {
            best: Some(best), ..
        } => Some(best.as_ref()),
        _ => None,
    };
    if let Some(best) = best {
        println!(
            "final benchmark route: {}",
            live_benchmark_route_summary(route, best.quote(), tokens)
        );
        match simulate_versioned_route_gas_with_balance_mappings(
            route.view(),
            best,
            DemoRouterConfig::default(),
            None,
            gas_balance_mappings,
        ) {
            Ok(estimate) => println!(
                "final benchmark route gas: {} gas in {:?}",
                estimate.gas_used, estimate.latency
            ),
            Err(error) => println!("final benchmark route gas unavailable: {error:#}"),
        }
    } else {
        println!("final benchmark route: no viable route in latest snapshot");
    }
    print_direct_quote_diagnostics(route, tokens);
}

fn print_direct_quote_diagnostics(route: &RouteSubscriptionSnapshot, tokens: &[TokenInfo]) {
    let Some((token_in, token_out)) = startup_focus_pair(tokens) else {
        return;
    };
    let decimals = tokens
        .iter()
        .find(|token| token.address == token_in)
        .map_or(18, |token| token.decimals);
    let amount_in = ten_pow(decimals);
    let view = route.view();
    let searcher = view.searcher();
    let registry = view.snapshot().registry();
    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);
    let mut by_protocol = BTreeMap::<&'static str, (usize, usize)>::new();
    let mut failures = BTreeMap::<String, usize>::new();
    let mut seen = HashSet::new();

    for edge in view.graph().graph().edge_references() {
        if !seen.insert(edge.weight().pool.clone())
            || view.graph().node_token(edge.source()) != Some(token_in)
            || view.graph().node_token(edge.target()) != Some(token_out)
        {
            continue;
        }
        let protocol = registry
            .pool_instance(&edge.weight().pool)
            .and_then(|instance| registry.pool(instance))
            .map(protocol_label)
            .unwrap_or("unknown");
        let path = RoutePath::from_hops(vec![Hop::new(
            edge.weight().pool.clone(),
            token_in,
            token_out,
        )]);
        match searcher.quote_path_snapshot(&path, amount_in, &sim_config) {
            Ok(_) => by_protocol.entry(protocol).or_default().0 += 1,
            Err(error) => {
                by_protocol.entry(protocol).or_default().1 += 1;
                let message = match error {
                    SearchError::QuoteFailed { source, .. } => source.to_string(),
                    other => other.to_string(),
                };
                *failures.entry(fit_to_width(&message, 180)).or_default() += 1;
            }
        }
    }

    println!(
        "direct focus quotes (success/failure): {}",
        by_protocol
            .iter()
            .map(|(protocol, (success, failure))| format!("{protocol}={success}/{failure}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    for (message, count) in top_counts(&failures, 4) {
        println!("  direct quote failure {count}x {message}");
    }
}

fn live_benchmark_route_summary(
    route: &RouteSubscriptionSnapshot,
    quote: &RouteQuote,
    tokens: &[TokenInfo],
) -> String {
    let registry = route.view().snapshot().registry();
    quote
        .hops
        .iter()
        .map(|hop| {
            let protocol = registry
                .pool_instance(&hop.hop.pool)
                .and_then(|instance| registry.pool(instance))
                .map(protocol_label)
                .unwrap_or("unknown");
            format!(
                "{} -> {} via {} {}",
                token_symbol(tokens, hop.hop.token_in),
                token_symbol(tokens, hop.hop.token_out),
                protocol,
                hop.hop
                    .pool
                    .address()
                    .map(short_address)
                    .unwrap_or_else(|| format!("{:?}", hop.hop.pool))
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn print_live_benchmark_progress(started: Instant, status: &AmmRuntimeStatusSnapshot) {
    let counts = graph_sync_counts(status);
    let completed = status
        .active_work_items()
        .map(|(_, progress)| progress.completed())
        .sum::<u64>();
    let total = status
        .active_work_items()
        .filter_map(|(_, progress)| progress.total())
        .sum::<u64>();
    eprintln!(
        "[live-progress +{:?}] routing_pools={} discovered_pools={} loading_pools={} queued_loads={} pending_state_updates={} degraded_pools={} failed_pools={} completed={completed} total={total}",
        started.elapsed(),
        counts.routing_pools,
        counts.discovered_pools,
        counts.loading_pools,
        counts.queued_loads,
        counts.pending_state_updates,
        counts.degraded_pools,
        counts.failed_pools,
    );
}

async fn run_legacy_bench(ws_url: String) -> Result<()> {
    let mut terminal = Terminal::new(TestBackend::new(200, 60))?;
    let BootstrappedTui {
        mut app,
        sync,
        mut cache,
        sim_config,
        timings,
        ..
    } = bootstrap_tui(&mut terminal, ws_url).await?;
    timings.print_report();

    if env_bool("AMM_ROUTE_TUI_BENCH_TRACE", false) {
        run_prewarm_trace(&app, &sync, &mut cache, &sim_config);
    }

    let search_runs = env_usize("AMM_ROUTE_TUI_BENCH_SEARCHES", 0);
    if search_runs > 0 {
        run_warm_search_bench(&mut app, &sync, &mut cache, &sim_config, search_runs);
    }
    Ok(())
}

/// Diagnose the first-search lazy-fetch storm: run ONE injecting `find_routes`
/// (the single-threaded path that fetches AND writes into the base cache) for the
/// startup pair, and diff the base cache's per-address storage-slot counts before
/// and after. The delta is exactly the state a search reads that cold-start +
/// liquidity refresh did not pre-warm — i.e. the batched-prewarm target. Slots
/// are classified as pool / token / quoter / router / other(proxy impl) so we can
/// see whether the miss set scales per-pool or per-token.
fn run_prewarm_trace(
    app: &AppState,
    sync: &AmmSyncEngine,
    cache: &mut EvmCache,
    sim_config: &SimConfig,
) {
    let pool_addrs: HashSet<Address> = sync
        .registry()
        .pools()
        .filter_map(|pool| pool.key.address())
        .collect();
    let token_addrs: HashSet<Address> = app.tokens.iter().map(|token| token.address).collect();

    let snapshot = |cache: &EvmCache| -> HashMap<Address, usize> {
        cache
            .all_cached_contract_addresses()
            .into_iter()
            .map(|addr| {
                (
                    addr,
                    cache.contract_storage_slot_count(addr)
                        + cache.cache_db_storage_slot_count(addr),
                )
            })
            .collect()
    };
    let classify = |addr: &Address| -> &'static str {
        if token_addrs.contains(addr) {
            "token"
        } else if *addr == V3_QUOTER_V2 {
            "quoter"
        } else if *addr == V2_ROUTER_02 {
            "router"
        } else if pool_addrs.contains(addr) {
            "pool"
        } else {
            "other(impl?)"
        }
    };

    let before = snapshot(cache);
    let addrs_before = before.len();

    let token_in = app.token_in();
    let token_out = app.token_out();
    let amount_in = match parse_units(&app.amount, token_in.decimals) {
        Ok(amount) => amount,
        Err(error) => {
            println!("prewarm trace: bad amount: {error}");
            return;
        }
    };
    let graph_report = AmmGraph::from_registry(sync.registry(), GraphBuildOptions::default());
    let mut searcher = AmmSearcher::new(sync.registry(), &graph_report.graph);
    if let Some(index) = app.liquidity_index.as_ref() {
        searcher = searcher.with_liquidity_index(index);
    }
    let request = RouteRequest::new(token_in.address, token_out.address, amount_in)
        .with_config(tui_search_config(app))
        .with_sim_config(*sim_config);

    let started = Instant::now();
    let routes = searcher.find_routes(&request, cache);
    let elapsed = started.elapsed();

    let after = snapshot(cache);
    let mut deltas: Vec<(Address, usize)> = Vec::new();
    let mut new_slots_total = 0usize;
    let mut by_class: HashMap<&str, (usize, usize)> = HashMap::new();
    for (addr, after_count) in &after {
        let before_count = before.get(addr).copied().unwrap_or(0);
        if *after_count > before_count {
            let delta = after_count - before_count;
            new_slots_total += delta;
            deltas.push((*addr, delta));
            let entry = by_class.entry(classify(addr)).or_default();
            entry.0 += 1;
            entry.1 += delta;
        }
    }
    deltas.sort_by_key(|entry| std::cmp::Reverse(entry.1));

    println!(
        "\n=== prewarm trace: injecting find_routes {} -> {} ===",
        token_in.symbol, token_out.symbol
    );
    println!(
        "search elapsed (injecting): {elapsed:?}; routes: {}",
        routes
            .map(|r| r.len().to_string())
            .unwrap_or_else(|e| format!("err: {e}"))
    );
    println!(
        "addresses with cached storage: {addrs_before} -> {}",
        after.len()
    );
    println!(
        "new slots injected by the search: {new_slots_total} across {} addresses",
        deltas.len()
    );
    println!("by class (addresses / new slots):");
    let mut classes: Vec<_> = by_class.into_iter().collect();
    classes.sort_by_key(|entry| std::cmp::Reverse(entry.1.1));
    for (class, (addrs, slots)) in classes {
        println!("  {class:<14} {addrs:>4} addrs  {slots:>6} slots");
    }
    println!("top addresses by new slots:");
    for (addr, delta) in deltas.iter().take(15) {
        println!("  {addr:?} {:<14} +{delta}", classify(addr));
    }
}

/// Warm-search micro-benchmark: re-run the exact TUI quote path
/// ([`refresh_quote`]) over a spread of directed token pairs, forcing a fresh
/// full search each time (no session reuse), and report per-pair latency
/// percentiles. Pure in-memory once the cache is warm; the first sample per pair
/// may still absorb a lazy code fetch, which is why we report min alongside p50.
fn run_warm_search_bench(
    app: &mut AppState,
    sync: &AmmSyncEngine,
    cache: &mut EvmCache,
    sim_config: &SimConfig,
    runs: usize,
) {
    let token_count = app.tokens.len();
    if token_count < 2 {
        println!("\nwarm-search bench skipped: fewer than two connected tokens");
        return;
    }
    // A spread of directed pairs: consecutive hops plus a few longer reaches, so
    // the sample mixes deep-liquidity majors with thinner cross-pairs.
    let mut pairs: Vec<(usize, usize)> = Vec::new();
    for step in [1usize, 2, 3] {
        for i in 0..token_count {
            let o = (i + step) % token_count;
            if i != o {
                pairs.push((i, o));
            }
        }
    }
    pairs.truncate(24);

    println!("\n=== warm search micro-benchmark ({runs} runs/pair) ===");
    println!(
        "{:<16}{:>12}{:>12}{:>12}{:>12}",
        "pair", "min", "p50", "p95", "max"
    );
    println!("{:-<64}", "");
    let mut all_p50 = Vec::new();
    for (i, o) in pairs {
        app.input_index = i;
        app.output_index = o;
        let mut samples: Vec<Duration> = Vec::with_capacity(runs);
        let mut failed = false;
        for _ in 0..runs {
            app.route_session = None;
            app.topology_updates += 1;
            let start = Instant::now();
            let ok = refresh_quote(app, sync, cache, sim_config, QuoteRefresh::Full);
            let dt = start.elapsed();
            if ok {
                samples.push(dt);
            } else {
                failed = true;
            }
        }
        let label = format!("{}->{}", app.tokens[i].symbol, app.tokens[o].symbol);
        if samples.is_empty() {
            println!("{label:<16}{:>12}", "no route");
            continue;
        }
        samples.sort_unstable();
        let pick = |q: f64| samples[((samples.len() as f64 * q) as usize).min(samples.len() - 1)];
        let p50 = pick(0.50);
        all_p50.push(p50);
        let note = if failed { " (some failed)" } else { "" };
        println!(
            "{label:<16}{:>12}{:>12}{:>12}{:>12}{note}",
            format!("{:?}", samples[0]),
            format!("{p50:?}"),
            format!("{:?}", pick(0.95)),
            format!("{:?}", samples[samples.len() - 1]),
        );
    }
    if !all_p50.is_empty() {
        all_p50.sort_unstable();
        let median_of_p50 = all_p50[all_p50.len() / 2];
        println!("{:-<64}", "");
        println!("median warm-search p50 across pairs: {median_of_p50:?}");
    }
}

async fn run_tui(ws_url: String) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let local = tokio::task::LocalSet::new();
    let loop_result = local
        .run_until(async {
            let user_config = load_tui_user_config().context("load TUI config")?;
            let tokens = token_list(&user_config).context("load token list")?;
            let mut app = AppState::new(tokens.clone(), Vec::new(), 0, 0, 0);
            app.quote_loading = true;
            app.status = "starting live runtime in the background".to_owned();
            app.chain_sync.phase = ChainSyncPhase::Applying;
            app.chain_sync.detail = "connecting canonical subscriber".to_owned();
            set_route_work(
                &mut app,
                RouteWorkPhase::Discovering,
                "starting live runtime",
                None,
            );

            let (ui_tx, mut ui_rx) = mpsc::channel(128);
            spawn_input_thread(ui_tx);
            let (bootstrap_tx, mut bootstrap_rx) = mpsc::channel(32);
            let (bootstrap_cancel, bootstrap_cancel_rx) = watch::channel(false);
            let bootstrap_task = tokio::task::spawn_local(bootstrap_live_tui(
                ws_url,
                user_config,
                tokens,
                bootstrap_tx,
                bootstrap_cancel_rx,
            ));

            let outcome =
                wait_for_live_bootstrap(&mut terminal, &mut app, &mut ui_rx, &mut bootstrap_rx)
                    .await?;
            let LiveBootstrapOutcome::Ready(runtime) = outcome else {
                bootstrap_cancel.send_replace(true);
                let _ = bootstrap_task.await;
                while let Ok(event) = bootstrap_rx.try_recv() {
                    if let LiveBootstrapEvent::Ready(runtime) = event {
                        shutdown_live_tui_runtime(*runtime).await;
                    }
                }
                return Ok(());
            };
            let _ = bootstrap_task.await;
            run_live_tui_loop(&mut terminal, &mut app, *runtime, &mut ui_rx).await
        })
        .await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    loop_result
}

async fn bootstrap_live_tui(
    ws_url: String,
    user_config: TuiUserConfig,
    tokens: Vec<TokenInfo>,
    tx: mpsc::Sender<LiveBootstrapEvent>,
    cancelled: watch::Receiver<bool>,
) {
    let mut resources = LiveBootstrapResources::default();
    let result = tokio::select! {
        biased;
        () = wait_for_bootstrap_cancellation(cancelled.clone()) => {
            Err(anyhow::anyhow!("live runtime bootstrap cancelled"))
        }
        result = bootstrap_live_tui_inner(
            ws_url,
            user_config,
            tokens,
            &tx,
            &cancelled,
            &mut resources,
        ) => result,
    };
    if let Err(error) = result {
        resources.shutdown().await;
        if !*cancelled.borrow() {
            let _ = tx
                .send(LiveBootstrapEvent::Failed(format!("{error:#}")))
                .await;
        }
    }
}

async fn wait_for_bootstrap_cancellation(mut cancelled: watch::Receiver<bool>) {
    while !*cancelled.borrow() {
        if cancelled.changed().await.is_err() {
            break;
        }
    }
}

async fn bootstrap_live_tui_inner(
    ws_url: String,
    user_config: TuiUserConfig,
    tokens: Vec<TokenInfo>,
    tx: &mpsc::Sender<LiveBootstrapEvent>,
    cancelled: &watch::Receiver<bool>,
    resources: &mut LiveBootstrapResources,
) -> Result<()> {
    rpc_profile::set_phase(RpcProfilePhase::Connect);
    ensure_bootstrap_active(cancelled)?;
    send_bootstrap_progress(tx, "connecting state and canonical providers").await?;
    let subscriber_provider = async {
        let client = ClientBuilder::default()
            .layer(RpcProfileLayer::websocket())
            .ws(WsConnect::new(ws_url.clone()))
            .await
            .context("connect canonical subscriber websocket endpoint")?;
        Ok::<_, anyhow::Error>(RootProvider::<Ethereum>::new(client))
    };
    let (state_connection, subscriber_provider) = tokio::try_join!(
        connect_state_provider(&ws_url, &user_config.network),
        subscriber_provider,
    )?;
    let state_provider = state_connection.provider;
    let subscriber_provider = Arc::new(subscriber_provider);

    ensure_bootstrap_active(cancelled)?;
    send_bootstrap_progress(tx, "fetching verified canonical baseline").await?;
    let chain_id = subscriber_provider.get_chain_id().await?;
    let latest_block = subscriber_provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await?
        .context("latest canonical block unavailable")?;
    let latest_header = latest_block.header().clone();
    let latest = latest_header.inner.number;
    let cache_dir = tui_cache_dir();
    let persist_cache = env_bool("AMM_ROUTE_TUI_PERSIST_CACHE", true);
    let warm_store = warm_store::WarmStore::new(&cache_dir, chain_id);
    let mut warm_session = warm_store
        .begin(persist_cache)
        .context("begin crash-consistent AMM warm generation")?;
    let resume_candidate = warm_session.resume_checkpoint();
    let max_warm_catchup_blocks = env_usize("AMM_ROUTE_TUI_MAX_WARM_CATCHUP_BLOCKS", 256) as u64;
    let persisted_checkpoint = resume_candidate.filter(|checkpoint| {
        warm_checkpoint_is_resumable(checkpoint.block_number, latest, max_warm_catchup_blocks)
    });
    if let Some(checkpoint) = resume_candidate
        && checkpoint.block_number <= latest
        && persisted_checkpoint.is_none()
    {
        send_bootstrap_progress(
            tx,
            format!(
                "warm checkpoint is {} blocks behind head; rebuilding at latest and retaining registration hints",
                latest.saturating_sub(checkpoint.block_number)
            ),
        )
        .await?;
    }
    let resumed_header = if let Some(checkpoint) = persisted_checkpoint {
        subscriber_provider
            .get_block_by_number(BlockNumberOrTag::Number(checkpoint.block_number))
            .await?
            .map(|block| block.header().clone())
            .filter(|header| header.hash == checkpoint.block_hash)
    } else {
        None
    };
    if resume_candidate.is_some() && resumed_header.is_none() {
        warm_session
            .discard_unverified_cache()
            .context("discard AMM state from an unverified warm checkpoint")?;
    }
    let registration_archive = warm_session.registration_archive().to_path_buf();
    let baseline_header = resumed_header
        .clone()
        .unwrap_or_else(|| latest_header.clone());
    let baseline_number = baseline_header.inner.number;

    ensure_bootstrap_active(cancelled)?;
    send_bootstrap_progress(tx, format!("loading fork cache at block {baseline_number}")).await?;
    rpc_profile::set_phase(RpcProfilePhase::CacheBuild);
    let mut cache = build_tui_cache(
        Arc::clone(&state_provider),
        baseline_number,
        chain_id,
        persist_cache,
        warm_session.cache_base_dir(),
    )
    .await;
    send_bootstrap_progress(tx, "prewarming shared AMM quote entrypoints").await?;
    let quote_targets = prepare_tui_quote_targets(
        state_provider.as_ref(),
        baseline_header.hash,
        baseline_header.inner.number,
        [
            V2_ROUTER_02,
            V3_QUOTER_V2,
            SUSHISWAP_V3_QUOTER_V2,
            PANCAKE_V3_QUOTER_V2,
            CURVE_3POOL,
            CURVE_FRAX_USDC,
            TRICRYPTO_USDC_NG,
        ],
    )
    .await?;
    cache.set_block(BlockId::from((baseline_header.hash, Some(true))));
    cache.set_block_context(
        Some(baseline_header.inner.number),
        baseline_header.inner.base_fee_per_gas,
    );
    cache
        .apply_prepared_account_patch(&quote_targets)
        .context("install verified shared AMM quote entrypoints")?;
    let gas_enabled = gas_estimates_enabled();
    let execution_validation_enabled = gas_enabled || tenderly_credentials_configured();
    if execution_validation_enabled {
        send_bootstrap_progress(
            tx,
            format!(
                "preparing route execution validation for {} tokens",
                tokens.len()
            ),
        )
        .await?;
    }
    let (mut gas_router_ready, mut gas_router_status) =
        prepare_live_gas_router(&mut cache, execution_validation_enabled);
    let gas_balance_mappings = if gas_router_ready {
        cache.set_blocking_provider_reads(true);
        let (ready, detail) = prepare_live_gas_tokens(&mut cache, &tokens);
        cache.set_blocking_provider_reads(false);
        gas_router_ready = !ready.is_empty();
        gas_router_status = detail.clone();
        send_bootstrap_progress(tx, detail).await?;
        ready
    } else {
        HashMap::new()
    };
    ensure_bootstrap_active(cancelled)?;

    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);
    let registry = build_registry(sim_config)?;
    let discovery = Arc::new(PoolDiscovery::for_registry(&registry, factory_config()));
    let mut manual = manual_config_pools(&user_config, &tokens)?
        .into_iter()
        .map(|pool| pool.registration)
        .collect::<Vec<_>>();
    let configured_registration_keys = manual
        .iter()
        .map(|registration| registration.key.clone())
        .collect::<HashSet<_>>();
    let mut restored_registration_keys = HashSet::new();
    if registration_archive.exists() {
        let archive_path = registration_archive.clone();
        let loaded = tokio::task::spawn_blocking(move || {
            AmmRegistrationArchive::load(archive_path, chain_id)
        })
        .await
        .context("join registration archive reader")?;
        ensure_bootstrap_active(cancelled)?;
        match loaded {
            Ok(archive) => {
                for mut restored in archive.into_registrations() {
                    normalize_tui_registration(&mut restored);
                    restored_registration_keys.insert(restored.key.clone());
                    if let Some(configured) = manual
                        .iter_mut()
                        .find(|configured| configured.key == restored.key)
                    {
                        merge_registration_hints(configured, restored);
                    } else {
                        manual.push(restored);
                    }
                }
                manual.sort_by(|left, right| left.key.cmp(&right.key));
                manual.dedup_by(|left, right| left.key == right.key);
            }
            Err(error) => {
                send_bootstrap_progress(tx, format!("registration warm resume ignored: {error}"))
                    .await?;
            }
        }
    }
    retain_queueable_registrations(&mut manual);
    cap_startup_registrations(
        &mut manual,
        env_usize("AMM_ROUTE_TUI_MAX_POOLS", DEFAULT_MAX_STARTUP_POOLS),
        &configured_registration_keys,
        startup_focus_pair(&tokens),
    );
    let baseline = AmmRuntimeBaseline::from_verified_header(chain_id, baseline_header)?;
    let checkpoint_matches_baseline = resumed_header.is_some();
    let baseline_point = baseline.point();

    ensure_bootstrap_active(cancelled)?;
    send_bootstrap_progress(tx, "starting canonical AMM cache owner").await?;
    rpc_profile::set_phase(RpcProfilePhase::InitialWarmup);
    let amm = AmmRuntime::spawn(cache, registry, baseline, AmmRuntimeConfig::default())?;
    resources.amm = Some(amm.clone());
    ensure_bootstrap_active(cancelled)?;
    if checkpoint_matches_baseline && !restored_registration_keys.is_empty() {
        let mut prepared = manual
            .iter()
            .filter(|registration| restored_registration_keys.contains(&registration.key))
            .cloned()
            .collect::<Vec<_>>();
        for registration in &mut prepared {
            registration.status = PoolStatus::Ready;
        }
        if !prepared.is_empty() {
            match amm.install_prepared_pools(prepared, baseline_point).await {
                Ok(_) => {
                    manual.retain(|registration| {
                        !restored_registration_keys.contains(&registration.key)
                    });
                    send_bootstrap_progress(
                        tx,
                        format!(
                            "verified warm resume restored {} pools at canonical block {}",
                            restored_registration_keys.len(),
                            baseline_point.block_number()
                        ),
                    )
                    .await?;
                }
                Err(error) => {
                    send_bootstrap_progress(
                        tx,
                        format!("verified warm resume fell back to cold start: {error}"),
                    )
                    .await?;
                }
            }
        }
    }
    let adaptive_concurrency = if state_connection.endpoint_count >= 2 {
        32
    } else {
        16
    };
    let max_concurrency = user_config
        .network
        .cold_start_concurrency
        .unwrap_or(adaptive_concurrency);
    let max_request_bytes = user_config
        .network
        .bulk_max_request_bytes
        .min(state_connection.max_request_bytes);
    let storage_strategy = StorageFetchStrategy::BulkCall(BulkCallConfig {
        max_slots_per_call: user_config.network.bulk_max_slots_per_call,
        max_slots_per_request: user_config.network.bulk_max_slots_per_request,
        max_request_bytes,
        max_concurrent_calls: user_config.network.bulk_max_concurrent_calls,
        ..BulkCallConfig::default()
    });
    let mut cold_start_config = AmmColdStartWorkerConfig::default()
        .with_queue_capacity(manual.len().max(256))
        .with_max_concurrency(env_usize(
            "AMM_ROUTE_TUI_COLD_START_CONCURRENCY",
            max_concurrency,
        ))
        .with_storage_batch_config(StorageBatchConfig::new(
            user_config.network.point_read_slots_per_batch,
            user_config.network.point_read_concurrency,
        ))
        .with_storage_fetch_strategy(storage_strategy);
    if env_bool("AMM_ROUTE_TUI_BENCH", false) {
        cold_start_config = cold_start_config.with_max_concurrency(env_usize(
            "AMM_ROUTE_TUI_BENCH_COLD_START_CONCURRENCY",
            cold_start_config.max_concurrency(),
        ));
    }
    let cold_start = amm
        .attach_cold_start_worker(state_provider.as_ref().clone(), cold_start_config)
        .await?;
    resources.cold_start = Some(cold_start.clone());

    ensure_bootstrap_active(cancelled)?;
    let adapter_families = amm
        .latest_snapshot()
        .registry()
        .adapters()
        .map(|(key, instance)| (key.clone(), instance.clone()))
        .collect::<Vec<_>>();
    let mut discovery_owners = Vec::new();
    for (index, (key, adapter)) in adapter_families.into_iter().enumerate() {
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
                DiscoveryOwnerKey::new(format!("amm-route-tui-factories-{index}")),
                adapter,
                Arc::clone(&discovery),
            ))
            .await?;
        discovery_owners.extend(
            supported
                .into_iter()
                .map(|protocol| (protocol, owner.clone())),
        );
        ensure_bootstrap_active(cancelled)?;
    }

    ensure_bootstrap_active(cancelled)?;
    send_bootstrap_progress(tx, "warming initial graph at a stable block").await?;
    let startup_request = startup_focus_pair(&tokens).map(|(token_in, token_out)| {
        let decimals = tokens
            .iter()
            .find(|token| token.address == token_in)
            .map_or(18, |token| token.decimals);
        RouteRequest::new(token_in, token_out, ten_pow(decimals))
            .with_config(tui_search_config_for_tokens(&tokens))
            .with_sim_config(sim_config)
    });
    let warmup_failures = start_bench_failure_observer(&amm);
    if !manual.is_empty() {
        amm.queue_cold_start(
            manual,
            AmmColdStartOptions::default().with_class(AmmWorkClass::Focused),
        )
        .await
        .context("queue manual pools for initial graph warmup")?;
    }
    if !checkpoint_matches_baseline {
        queue_startup_discovery(&amm, &discovery_owners, &tokens).await;
    }
    wait_for_initial_graph_warmup(&amm, startup_request.as_ref(), cancelled, tx).await?;

    ensure_bootstrap_active(cancelled)?;
    send_bootstrap_progress(tx, "attaching canonical block and log subscriber").await?;
    rpc_profile::set_phase(RpcProfilePhase::SubscriberAttach);
    let subscriber = AlloySubscriber::new(
        subscriber_provider.as_ref().clone(),
        SubscriberMode::Auto,
        SubscriberConfig {
            max_log_addresses_per_subscription: user_config
                .network
                .max_log_addresses_per_subscription,
            ..SubscriberConfig::default()
        },
    );
    let subscriber = amm
        .attach_alloy_subscriber(subscriber, AmmSubscriberDriverConfig::default())
        .await?;
    resources.subscriber = Some(subscriber.clone());
    if baseline_point.block_number() < latest_header.inner.number {
        send_bootstrap_progress(
            tx,
            format!(
                "replaying canonical blocks {}..={} from verified warm checkpoint",
                baseline_point.block_number().saturating_add(1),
                latest_header.inner.number
            ),
        )
        .await?;
        subscriber
            .catch_up_to(latest_header)
            .await
            .context("catch up verified warm checkpoint through latest canonical block")?;
        wait_for_initial_graph_warmup(&amm, startup_request.as_ref(), cancelled, tx)
            .await
            .context("settle warm-resume repair work after canonical catch-up")?;
    }
    finish_bench_failure_observer(warmup_failures, "bootstrap runtime work failures").await;

    send_bootstrap_progress(tx, "starting incremental live route runtime").await?;
    let routes = LiveRouteRuntime::spawn(
        &amm,
        GraphBuildOptions::default(),
        LiveRouteRuntimeConfig::default(),
    )
    .await?;
    resources.routes = Some(routes.clone());
    ensure_bootstrap_active(cancelled)?;

    let permit = tx
        .reserve()
        .await
        .map_err(|_| anyhow::anyhow!("TUI closed before live runtime became ready"))?;
    ensure_bootstrap_active(cancelled)?;
    let ready = LiveTuiRuntime {
        provider: state_provider,
        amm: resources.amm.take().expect("AMM runtime recorded"),
        routes: resources.routes.take().expect("route runtime recorded"),
        subscriber: resources
            .subscriber
            .take()
            .expect("subscriber driver recorded"),
        cold_start: resources
            .cold_start
            .take()
            .expect("cold-start worker recorded"),
        discovery: discovery_owners,
        sim_config,
        gas_router_ready,
        gas_router_status,
        gas_balance_mappings,
        warm_session,
    };
    permit.send(LiveBootstrapEvent::Ready(Box::new(ready)));
    Ok(())
}

fn merge_registration_hints(configured: &mut PoolRegistration, restored: PoolRegistration) {
    match (&mut configured.metadata, restored.metadata) {
        (ProtocolMetadata::Curve(configured), ProtocolMetadata::Curve(restored))
            if configured.coins == restored.coins && configured.variant == restored.variant =>
        {
            configured.discovered_slots = restored.discovered_slots;
            if configured.code_seed.is_none() {
                configured.code_seed = restored.code_seed;
            }
        }
        (ProtocolMetadata::BalancerV2(configured), ProtocolMetadata::BalancerV2(restored))
            if configured.vault == restored.vault
                && configured.pool_address == restored.pool_address
                && configured.tokens == restored.tokens =>
        {
            configured.balance_slots = restored.balance_slots;
            configured.token_cash = restored.token_cash;
        }
        _ => {}
    }
}

fn normalize_tui_registration(registration: &mut PoolRegistration) {
    let ProtocolMetadata::UniswapV3(metadata) = &mut registration.metadata else {
        return;
    };
    if metadata.factory == Some(SUSHISWAP_V3_FACTORY) {
        metadata.quoter = Some(SUSHISWAP_V3_QUOTER_V2);
    }
}

fn retain_queueable_registrations(registrations: &mut Vec<PoolRegistration>) {
    registrations.retain(|registration| {
        matches!(
            registration.status,
            evm_amm_state::adapters::PoolStatus::Pending
        )
    });
}

fn cap_startup_registrations(
    registrations: &mut Vec<PoolRegistration>,
    max_pools: usize,
    configured: &HashSet<PoolKey>,
    focus: Option<(Address, Address)>,
) {
    if max_pools == 0 || registrations.len() <= max_pools {
        return;
    }
    registrations.sort_by(|left, right| {
        let left_configured = configured.contains(&left.key);
        let right_configured = configured.contains(&right.key);
        let left_focus = focus.is_some_and(|pair| registration_serves_pair(left, pair));
        let right_focus = focus.is_some_and(|pair| registration_serves_pair(right, pair));
        right_configured
            .cmp(&left_configured)
            .then_with(|| right_focus.cmp(&left_focus))
            .then_with(|| left.key.cmp(&right.key))
    });
    registrations.truncate(max_pools);
}

fn registration_serves_pair(
    registration: &PoolRegistration,
    (token_a, token_b): (Address, Address),
) -> bool {
    registration
        .tokens()
        .is_some_and(|tokens| tokens.contains(&token_a) && tokens.contains(&token_b))
}

async fn queue_startup_discovery(
    amm: &AmmRuntimeHandle,
    owners: &[(ProtocolId, DiscoveryOwnerId)],
    tokens: &[TokenInfo],
) {
    let focus = startup_focus_pair(tokens);
    let mut jobs = Vec::new();
    if let Some((token, connector)) = focus {
        for (protocol, owner) in owners {
            jobs.push((
                *protocol,
                owner.clone(),
                TokenEdgeDiscoveryRequest::new(token, [connector]).with_protocol(*protocol),
            ));
        }
    }
    let focus_jobs = jobs.len();

    for token in tokens {
        let mut connectors = connector_addresses(tokens, token.address);
        if let Some((token_in, token_out)) = focus {
            connectors.retain(|connector| {
                !((token.address == token_in && *connector == token_out)
                    || (token.address == token_out && *connector == token_in))
            });
        }
        if connectors.is_empty() {
            continue;
        }
        for (protocol, owner) in owners {
            jobs.push((
                *protocol,
                owner.clone(),
                TokenEdgeDiscoveryRequest::new(token.address, connectors.iter().copied())
                    .with_protocol(*protocol),
            ));
        }
    }

    let max_pools = env_usize("AMM_ROUTE_TUI_MAX_POOLS", DEFAULT_MAX_STARTUP_POOLS);
    let quotas = if max_pools == 0 {
        vec![None; jobs.len()]
    } else {
        let status = amm.latest_status();
        let existing = latest_pool_lifecycle_states(&status).len();
        let budget = max_pools.saturating_sub(existing);
        let focus_budget = budget.min(focus_jobs.saturating_mul(8));
        discovery_candidate_quotas(focus_budget, focus_jobs)
            .into_iter()
            .chain(discovery_candidate_quotas(
                budget.saturating_sub(focus_budget),
                jobs.len().saturating_sub(focus_jobs),
            ))
            .map(Some)
            .collect()
    };

    for ((_, owner, request), quota) in jobs.into_iter().zip(quotas) {
        if quota == Some(0) {
            continue;
        }
        let options = quota.map_or_else(
            || AmmDiscoveryOptions::default().with_class(AmmWorkClass::Bootstrap),
            |max_candidates| {
                AmmDiscoveryOptions::default()
                    .with_class(AmmWorkClass::Bootstrap)
                    .with_max_candidates(max_candidates)
            },
        );
        let _ = amm.queue_token_discovery(owner, request, options).await;
    }
}

fn discovery_candidate_quotas(total: usize, jobs: usize) -> Vec<usize> {
    if jobs == 0 {
        return Vec::new();
    }
    let base = total / jobs;
    let remainder = total % jobs;
    (0..jobs)
        .map(|index| base + usize::from(index < remainder))
        .collect()
}

fn ensure_bootstrap_active(cancelled: &watch::Receiver<bool>) -> Result<()> {
    if *cancelled.borrow() {
        bail!("live runtime bootstrap cancelled");
    }
    Ok(())
}

async fn send_bootstrap_progress(
    tx: &mpsc::Sender<LiveBootstrapEvent>,
    detail: impl Into<String>,
) -> Result<()> {
    tx.send(LiveBootstrapEvent::Progress(detail.into()))
        .await
        .map_err(|_| anyhow::anyhow!("TUI closed during live runtime bootstrap"))
}

async fn wait_for_initial_graph_warmup(
    amm: &AmmRuntimeHandle,
    startup_request: Option<&RouteRequest>,
    cancelled: &watch::Receiver<bool>,
    tx: &mpsc::Sender<LiveBootstrapEvent>,
) -> Result<()> {
    let mut status = amm.subscribe_status();
    let mut last_progress = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap_or_else(Instant::now);
    let mut last_quote_version = None;
    let mut focus_ready = false;

    loop {
        ensure_bootstrap_active(cancelled)?;
        let snapshot = status.borrow_and_update().clone();
        let counts = graph_sync_counts(&snapshot);
        let active_jobs = snapshot.active_work_items().count();
        let focus_ready = startup_request.map(|request| {
            let state = amm.latest_snapshot();
            if last_quote_version != Some(state.version()) {
                focus_ready = (|| {
                    let graph =
                        LiveAmmGraph::from_snapshot(&state, GraphBuildOptions::default()).ok()?;
                    let searcher = AmmSearcher::from_snapshot(&state, &graph).ok()?;
                    searcher.find_best_route_snapshot(request).ok()
                })()
                .is_some();
                last_quote_version = Some(state.version());
            }
            focus_ready
        });
        if initial_graph_ready_for_routes(counts, active_jobs, focus_ready) {
            let idle = active_jobs == 0
                && counts.loading_pools == 0
                && counts.queued_loads == 0
                && counts.pending_state_updates == 0;
            let state = if idle {
                "idle"
            } else {
                "warming in background"
            };
            send_bootstrap_progress(
                tx,
                format!(
                    "initial graph usable: {} pools routing, {} failed to load, {state}",
                    counts.routing_pools, counts.failed_pools
                ),
            )
            .await?;
            return Ok(());
        }

        if last_progress.elapsed() >= Duration::from_millis(750) {
            send_bootstrap_progress(tx, graph_sync_detail(&snapshot, counts)).await?;
            last_progress = Instant::now();
        }

        tokio::select! {
            changed = status.changed() => {
                changed.context("AMM status stream closed during initial graph warmup")?;
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

fn initial_graph_ready_for_routes(
    counts: GraphSyncCounts,
    active_jobs: usize,
    _focus_ready: Option<bool>,
) -> bool {
    active_jobs == 0
        && counts.loading_pools == 0
        && counts.queued_loads == 0
        && counts.pending_state_updates == 0
}

fn warm_generation_ready_to_commit(counts: GraphSyncCounts, active_jobs: usize) -> bool {
    active_jobs == 0
        && counts.loading_pools == 0
        && counts.queued_loads == 0
        && counts.pending_state_updates == 0
}

fn warm_checkpoint_is_resumable(
    checkpoint_block: u64,
    latest_block: u64,
    max_catchup_blocks: u64,
) -> bool {
    latest_block
        .checked_sub(checkpoint_block)
        .is_some_and(|distance| distance <= max_catchup_blocks)
}

async fn run_live_tui_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut AppState,
    runtime: LiveTuiRuntime,
    ui_rx: &mut mpsc::Receiver<UiEvent>,
) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut snapshots = runtime.amm.subscribe_snapshots();
    let mut status = runtime.amm.subscribe_status();
    let mut subscriber_state = runtime.subscriber.subscribe_state();
    let (route_tx, mut route_rx) = mpsc::channel(128);
    let (discovery_tx, mut discovery_rx) = mpsc::channel(16);
    let (price_tx, mut price_rx) = mpsc::channel(8);
    let (gas_price_tx, mut gas_price_rx) = mpsc::channel(4);
    let (simulation_tx, mut simulation_rx) = mpsc::channel(4);
    let mut price_refresh_in_flight = false;
    let mut next_price_refresh = price_refresh_enabled().then_some(Instant::now());
    let mut gas_price_refresh_in_flight = gas_estimates_enabled()
        && start_gas_price_refresh(Arc::clone(&runtime.provider), gas_price_tx.clone());
    let mut routes = LiveRouteUiController::default();
    let mut discovery_generations = [0_u64; 2];

    app.gas_router_ready = runtime.gas_router_ready;
    app.gas_router_status = runtime.gas_router_status.clone();
    app.gas_balance_mappings = runtime.gas_balance_mappings.clone();
    app.chain_sync.phase = ChainSyncPhase::Synced;
    app.chain_sync.detail = format!(
        "canonical runtime at block {}",
        runtime.amm.latest_snapshot().point().block_number()
    );
    replace_live_route(
        app,
        &runtime.routes,
        runtime.sim_config,
        &route_tx,
        &mut routes,
    );
    for side in [Side::Input, Side::Output] {
        start_selected_token_discovery(
            app,
            &runtime,
            side,
            discovery_tx.clone(),
            &mut discovery_generations,
        );
    }
    let background_discovery = env_bool("AMM_ROUTE_TUI_BACKGROUND_DISCOVERY", true).then(|| {
        tokio::spawn(run_idle_background_discovery(
            runtime.amm.clone(),
            runtime.discovery.clone(),
            app.tokens.clone(),
        ))
    });

    loop {
        terminal.draw(|frame| draw(frame, app))?;
        tokio::select! {
            _ = tick.tick() => {
                if should_start_price_refresh(next_price_refresh, price_refresh_in_flight) {
                    price_refresh_in_flight = start_price_refresh(app, &price_tx);
                    next_price_refresh = price_refresh_enabled()
                        .then_some(Instant::now() + price_refresh_interval());
                }
            }
            Some(event) = ui_rx.recv() => match handle_ui_event(app, event) {
                UiAction::Quit => break,
                UiAction::Continue => {}
                UiAction::RequestQuote => {
                    replace_live_route(
                        app,
                        &runtime.routes,
                        runtime.sim_config,
                        &route_tx,
                        &mut routes,
                    );
                }
                UiAction::SelectToken { side } => {
                    start_selected_token_discovery(
                        app,
                        &runtime,
                        side,
                        discovery_tx.clone(),
                        &mut discovery_generations,
                    );
                    replace_live_route(
                        app,
                        &runtime.routes,
                        runtime.sim_config,
                        &route_tx,
                        &mut routes,
                    );
                }
                UiAction::SimulateSwap => {
                    start_tenderly_simulation(
                        app,
                        Arc::clone(&runtime.provider),
                        simulation_tx.clone(),
                    );
                }
                UiAction::DiscoverToken { side, query } => {
                    start_live_dynamic_discovery(
                        app,
                        &runtime,
                        side,
                        query,
                        discovery_tx.clone(),
                        &mut discovery_generations,
                    );
                }
            },
            Some(event) = simulation_rx.recv() => {
                apply_simulation_event(app, event);
            }
            Some(event) = route_rx.recv() => {
                match event {
                    LiveRouteUiEvent::Attached { cancellation } => {
                        routes.cancellation = Some(cancellation);
                    }
                    LiveRouteUiEvent::Snapshot { generation, snapshot } => {
                        if routes.accepts(generation) {
                            apply_live_route_snapshot(app, &snapshot);
                        }
                    }
                    LiveRouteUiEvent::Failed { generation, message } => {
                        if routes.accepts(generation) {
                            app.quote_loading = false;
                            app.quote = None;
                            app.quote_error = Some(message.clone());
                            set_route_work(app, RouteWorkPhase::Error, fit_to_width(&message, 48), None);
                            app.status = message;
                        }
                    }
                }
            }
            Some(event) = discovery_rx.recv() => {
                if apply_live_dynamic_discovery(app, event, &discovery_generations) {
                    replace_live_route(
                        app,
                        &runtime.routes,
                        runtime.sim_config,
                        &route_tx,
                        &mut routes,
                    );
                    next_price_refresh = price_refresh_enabled().then_some(Instant::now());
                }
            }
            Some(event) = price_rx.recv() => {
                price_refresh_in_flight = false;
                apply_live_price_event(app, event);
            }
            Some(event) = gas_price_rx.recv() => {
                gas_price_refresh_in_flight = false;
                apply_gas_price_event(app, event);
            }
            changed = snapshots.changed() => {
                if changed.is_err() {
                    app.chain_sync.phase = ChainSyncPhase::Degraded;
                    app.chain_sync.detail = "canonical snapshot stream closed".to_owned();
                } else {
                    let snapshot = snapshots.borrow_and_update().clone();
                    app.last_block = snapshot.point().block_number();
                    app.chain_sync.phase = ChainSyncPhase::Synced;
                    app.chain_sync.detail = format!(
                        "canonical block {} state v{}",
                        snapshot.point().block_number(),
                        snapshot.version().get(),
                    );
                    if !gas_price_refresh_in_flight {
                        gas_price_refresh_in_flight = start_gas_price_refresh(
                            Arc::clone(&runtime.provider),
                            gas_price_tx.clone(),
                        );
                    }
                }
            }
            changed = status.changed() => {
                if changed.is_ok() {
                    apply_live_runtime_status(app, &status.borrow_and_update());
                }
            }
            changed = subscriber_state.changed() => {
                if changed.is_ok() {
                    apply_live_subscriber_state(app, &subscriber_state.borrow_and_update());
                }
            }
        }
    }

    if let Some(cancellation) = routes.cancellation.take() {
        cancellation.cancel();
    }
    if let Some(background_discovery) = background_discovery {
        background_discovery.abort();
        let _ = background_discovery.await;
    }
    shutdown_live_tui_runtime_with_archive(runtime)
        .await
        .context("save registration archive during live TUI shutdown")
}

async fn shutdown_live_tui_runtime_with_archive(runtime: LiveTuiRuntime) -> Result<()> {
    let status = runtime.amm.subscribe_status().borrow().clone();
    let counts = graph_sync_counts(&status);
    let warm_generation_complete =
        warm_generation_ready_to_commit(counts, status.active_work_items().count());
    let _ = runtime.routes.shutdown().await;
    let _ = runtime.subscriber.shutdown().await;
    let mut cold_state = runtime.cold_start.subscribe_state();
    runtime.cold_start.shutdown();
    while *cold_state.borrow() != AmmColdStartWorkerState::Stopped {
        if cold_state.changed().await.is_err() {
            break;
        }
    }
    let prepared = if runtime.warm_session.persist_cache() && !warm_generation_complete {
        eprintln!(
            "warm generation remains incomplete; preserving the previous committed checkpoint"
        );
        Ok(None)
    } else {
        prepare_live_warm_generation(&runtime).await.map(Some)
    };
    let shutdown = runtime.amm.shutdown().await;

    match (prepared, shutdown) {
        (Ok(checkpoint), Ok(())) => {
            if let Some(checkpoint) = checkpoint
                && runtime.warm_session.persist_cache()
            {
                let mut warm_session = runtime.warm_session;
                tokio::task::spawn_blocking(move || warm_session.commit(checkpoint))
                    .await
                    .context("join AMM warm generation commit")??;
            }
            Ok(())
        }
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error).context("shut down AMM runtime before warm commit"),
    }
}

async fn prepare_live_warm_generation(
    runtime: &LiveTuiRuntime,
) -> Result<warm_store::WarmCheckpoint> {
    if runtime.warm_session.persist_cache() {
        runtime
            .amm
            .flush_persistent_cache()
            .await
            .context("flush actor-owned cache for warm checkpoint")?;
    }
    let snapshot = runtime.amm.latest_snapshot();
    let point = snapshot.point();
    let archive = AmmRegistrationArchive::capture(point.chain_id(), snapshot.registry())?;
    let checkpoint = warm_store::WarmCheckpoint {
        chain_id: point.chain_id(),
        block_number: point.block_number(),
        block_hash: point.block_hash(),
    };
    let path = runtime.warm_session.registration_archive().to_path_buf();
    tokio::task::spawn_blocking(move || archive.save(&path))
        .await
        .context("join registration archive writer")??;
    Ok(checkpoint)
}

async fn shutdown_live_tui_runtime(runtime: LiveTuiRuntime) {
    let _ = runtime.routes.shutdown().await;
    let _ = runtime.subscriber.shutdown().await;
    runtime.cold_start.shutdown();
    let _ = runtime.amm.shutdown().await;
}

fn replace_live_route(
    app: &mut AppState,
    routes: &LiveRouteRuntimeHandle,
    sim_config: SimConfig,
    tx: &mpsc::Sender<LiveRouteUiEvent>,
    controller: &mut LiveRouteUiController,
) {
    let generation = controller.begin_replacement();
    let token_in = app.token_in();
    let token_out = app.token_out();
    let amount_in = match parse_units(&app.amount, token_in.decimals) {
        Ok(amount) => amount,
        Err(error) => {
            app.quote = None;
            app.quote_loading = false;
            app.quote_error = Some(error.clone());
            set_route_work(app, RouteWorkPhase::Error, error, None);
            return;
        }
    };
    if token_in.address == token_out.address {
        let error = "input and output token must differ".to_owned();
        app.quote = None;
        app.quote_loading = false;
        app.quote_error = Some(error.clone());
        set_route_work(app, RouteWorkPhase::Error, error, None);
        return;
    }

    let request = RouteRequest::new(token_in.address, token_out.address, amount_in)
        .with_config(tui_search_config(app))
        .with_sim_config(sim_config);
    let spec = RouteSubscriptionSpec::new(request, tui_streaming_config());
    app.quote_loading = true;
    app.quote_error = None;
    set_route_work(
        app,
        RouteWorkPhase::Quoting,
        format!("subscribing {} -> {}", token_in.symbol, token_out.symbol),
        None,
    );
    app.status = "previous route remains visible while the replacement is computed".to_owned();

    let request = LiveRouteDriverRequest { generation, spec };
    if let Some(requests) = &controller.requests
        && requests.receiver_count() > 0
    {
        requests.send_replace(request);
        return;
    }

    controller.requests = None;
    controller.cancellation = None;
    let (requests, request_rx) = watch::channel(request);
    controller.requests = Some(requests);
    tokio::spawn(run_live_route_driver(
        routes.clone(),
        request_rx,
        tx.clone(),
    ));
}

async fn run_live_route_driver(
    routes: LiveRouteRuntimeHandle,
    mut requests: watch::Receiver<LiveRouteDriverRequest>,
    tx: mpsc::Sender<LiveRouteUiEvent>,
) {
    let initial = requests.borrow_and_update().clone();
    let mut generation = initial.generation;
    let mut subscription = match routes.subscribe(initial.spec).await {
        Ok(subscription) => subscription,
        Err(error) => {
            let _ = tx
                .send(LiveRouteUiEvent::Failed {
                    generation,
                    message: format!("live route subscription failed: {error}"),
                })
                .await;
            return;
        }
    };
    if tx
        .send(LiveRouteUiEvent::Attached {
            cancellation: subscription.cancellation_token(),
        })
        .await
        .is_err()
    {
        return;
    }
    if tx
        .send(LiveRouteUiEvent::Snapshot {
            generation,
            snapshot: subscription.latest(),
        })
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            biased;
            changed = requests.changed() => {
                if changed.is_err() {
                    break;
                }
                let request = requests.borrow_and_update().clone();
                generation = request.generation;
                if let Err(error) = subscription.replace(request.spec).await {
                    if tx.send(LiveRouteUiEvent::Failed {
                        generation,
                        message: format!("live route replacement failed: {error}"),
                    }).await.is_err() {
                        break;
                    }
                    continue;
                }
                if tx.send(LiveRouteUiEvent::Snapshot {
                    generation,
                    snapshot: subscription.latest(),
                }).await.is_err() {
                    break;
                }
            }
            changed = subscription.changed() => match changed {
                Ok(snapshot) => {
                    if tx.send(LiveRouteUiEvent::Snapshot {
                        generation,
                        snapshot,
                    }).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }
}

fn apply_live_route_snapshot(app: &mut AppState, snapshot: &RouteSubscriptionSnapshot) {
    let view = snapshot.view();
    let state_snapshot = view.snapshot();
    app.last_block = state_snapshot.point().block_number();
    app.graph_sync.routing_pools = routing_pool_count(view.graph());
    app.pools = state_snapshot
        .registry()
        .pools()
        .filter_map(|(_, instance)| state_snapshot.registry().pool(instance))
        .cloned()
        .map(|registration| pool_info_from_registration(registration, &app.tokens))
        .collect();
    sort_pool_infos(&mut app.pools);
    app.ready_pools = app.graph_sync.routing_pools;
    app.topology_updates = view.graph().version().revision();

    match snapshot.state() {
        RouteSubscriptionState::Pending { .. } => {
            app.quote_loading = true;
            set_route_work(
                app,
                RouteWorkPhase::Quoting,
                "waiting for route worker",
                None,
            );
        }
        RouteSubscriptionState::Searching { provisional, .. } => {
            set_route_work(
                app,
                RouteWorkPhase::Quoting,
                "streaming current route search",
                None,
            );
            if let Some(quote) = provisional {
                let token_in = app.token_in();
                let token_out = app.token_out();
                let should_replace = parse_units(&app.amount, token_in.decimals)
                    .ok()
                    .is_some_and(|amount_in| {
                        should_replace_displayed_provisional(
                            app.quote.as_ref().map(|displayed| &displayed.best),
                            token_in.address,
                            token_out.address,
                            amount_in,
                            quote.quote(),
                        )
                    });
                if should_replace {
                    install_live_quote(app, view, quote, &[], 1);
                }
            }
            app.quote_loading = true;
        }
        RouteSubscriptionState::Ready { best, report, .. } => {
            app.quote_loading = false;
            if let Some(best) = best {
                install_live_quote(app, view, best, &report.top_routes, report.routes_observed);
                set_route_work(
                    app,
                    RouteWorkPhase::Ready,
                    format!("ready {} route(s)", report.routes_observed),
                    None,
                );
                app.quote_error = None;
            } else {
                let error = "no viable route in the currently ready AMM set".to_owned();
                app.quote = None;
                app.quote_error = Some(error.clone());
                set_route_work(app, RouteWorkPhase::Error, error, None);
            }
        }
        RouteSubscriptionState::Failed { failure, .. } => {
            app.quote_loading = false;
            app.quote = None;
            app.quote_error = Some(failure.message().to_owned());
            set_route_work(
                app,
                RouteWorkPhase::Error,
                fit_to_width(failure.message(), 48),
                None,
            );
        }
        RouteSubscriptionState::RuntimeFailed { failure } => {
            app.quote_loading = false;
            app.quote = None;
            app.quote_error = Some(failure.message().to_owned());
            app.chain_sync.phase = ChainSyncPhase::Degraded;
            set_route_work(
                app,
                RouteWorkPhase::Error,
                fit_to_width(failure.message(), 48),
                None,
            );
        }
        RouteSubscriptionState::Cancelled | RouteSubscriptionState::Closed => {}
        _ => {}
    }
}

fn should_replace_displayed_provisional(
    displayed: Option<&RouteQuote>,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    provisional: &RouteQuote,
) -> bool {
    if !route_quote_matches_request(provisional, token_in, token_out, amount_in) {
        return false;
    }
    displayed
        .is_none_or(|current| !route_quote_matches_request(current, token_in, token_out, amount_in))
}

fn route_quote_matches_request(
    quote: &RouteQuote,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> bool {
    quote.amount_in == amount_in
        && quote
            .path
            .hops
            .first()
            .is_some_and(|hop| hop.token_in == token_in)
        && quote
            .path
            .hops
            .last()
            .is_some_and(|hop| hop.token_out == token_out)
}

fn route_panel_shows_loading(
    quote_loading: bool,
    displayed: Option<&RouteQuote>,
    token_in: Address,
    token_out: Address,
    amount_in: Option<U256>,
) -> bool {
    quote_loading
        && !amount_in.is_some_and(|amount_in| {
            displayed.is_some_and(|quote| {
                route_quote_matches_request(quote, token_in, token_out, amount_in)
            })
        })
}

fn install_live_quote(
    app: &mut AppState,
    view: &Arc<LiveSearchView>,
    best: &Arc<VersionedRouteQuote>,
    top_routes: &[RouteQuote],
    routes: usize,
) {
    let token_out = app.token_out();
    let mut stream_lines = app
        .quote
        .as_ref()
        .map(|quote| quote.stream_lines.clone())
        .unwrap_or_default();
    let elapsed = app
        .route_work
        .started_at
        .map(|started| started.elapsed())
        .unwrap_or_default();
    let gas = estimate_live_route_gas(app, view, best, &mut stream_lines);
    let best_quote = best.quote();
    app.quote_updates = app.quote_updates.saturating_add(1);
    let mut quote = quote_view(
        view.graph(),
        best_quote,
        app,
        &token_out,
        gas,
        stream_lines,
        QuoteViewStats {
            routes,
            graph_pools: routing_pool_count(view.graph()),
            elapsed,
            block_number: view.snapshot().point().block_number(),
        },
    );
    quote.live_tenderly = Some(LiveTenderlyQuote {
        view: Arc::clone(view),
        quote: Arc::clone(best),
    });
    quote.quoted_routes = top_routes
        .iter()
        .take(3)
        .enumerate()
        .map(|(index, route)| quoted_route_summary(app, route, index + 1))
        .collect();
    quote.quoted_venues = quoted_route_venue_coverage(app, top_routes);
    app.quote = Some(quote);
}

fn quoted_route_summary(app: &AppState, route: &RouteQuote, rank: usize) -> String {
    let venues = route
        .hops
        .iter()
        .map(|hop| pool_label(&app.pools, &hop.hop.pool))
        .collect::<Vec<_>>()
        .join(" -> ");
    format!(
        "#{rank}  {}  via {venues}",
        format_amount_with_token(route.amount_out, &app.token_out(), &app.prices, 8),
    )
}

fn quoted_route_venue_coverage(app: &AppState, routes: &[RouteQuote]) -> String {
    let venues = routes
        .iter()
        .flat_map(|route| route.hops.iter())
        .filter_map(|hop| {
            app.pools
                .iter()
                .find(|pool| pool.registration.key == hop.hop.pool)
                .map(|pool| protocol_label(&pool.registration))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    if venues.is_empty() {
        String::new()
    } else {
        format!("venues quoted: {venues}")
    }
}

fn apply_live_runtime_status(app: &mut AppState, status: &AmmRuntimeStatusSnapshot) {
    apply_graph_sync_status(app, status);
    let completed = status
        .active_work_items()
        .map(|(_, progress)| progress.completed())
        .sum::<u64>();
    let total = status
        .active_work_items()
        .filter_map(|(_, progress)| progress.total())
        .sum::<u64>();
    if app.graph_sync.phase == GraphSyncPhase::Warming {
        set_route_work(
            app,
            RouteWorkPhase::Discovering,
            app.graph_sync.detail.clone(),
            (total > 0).then_some((completed as usize, total as usize)),
        );
    } else if !app.quote_loading && app.quote.is_some() {
        let routes = app.quote.as_ref().map_or(0, |quote| quote.routes);
        set_route_work(
            app,
            RouteWorkPhase::Ready,
            format!("ready {routes} route(s)"),
            None,
        );
    }
    app.chain_sync.phase = match status.health() {
        evm_amm_state::adapters::AmmRuntimeHealth::Healthy
        | evm_amm_state::adapters::AmmRuntimeHealth::Degraded => ChainSyncPhase::Synced,
        evm_amm_state::adapters::AmmRuntimeHealth::Untrusted
        | evm_amm_state::adapters::AmmRuntimeHealth::ShuttingDown => ChainSyncPhase::Degraded,
        _ => ChainSyncPhase::Degraded,
    };
}

fn apply_graph_sync_status(app: &mut AppState, status: &AmmRuntimeStatusSnapshot) {
    let counts = graph_sync_counts(status);
    app.ready_pools = counts.routing_pools;
    app.graph_sync.routing_pools = counts.routing_pools;
    app.graph_sync.discovered_pools = counts.discovered_pools;
    app.graph_sync.loading_pools = counts.loading_pools;
    app.graph_sync.queued_loads = counts.queued_loads;
    app.graph_sync.pending_state_updates = counts.pending_state_updates;
    app.graph_sync.degraded_pools = counts.degraded_pools;
    app.graph_sync.failed_pools = counts.failed_pools;

    let warming =
        counts.loading_pools > 0 || counts.queued_loads > 0 || counts.pending_state_updates > 0;
    app.graph_sync.phase = match status.health() {
        evm_amm_state::adapters::AmmRuntimeHealth::Healthy if warming => GraphSyncPhase::Warming,
        evm_amm_state::adapters::AmmRuntimeHealth::Healthy
            if counts.routing_pools == 0
                && (counts.failed_pools > 0 || counts.degraded_pools > 0) =>
        {
            GraphSyncPhase::Error
        }
        evm_amm_state::adapters::AmmRuntimeHealth::Healthy => GraphSyncPhase::Synced,
        evm_amm_state::adapters::AmmRuntimeHealth::Degraded if warming => GraphSyncPhase::Warming,
        evm_amm_state::adapters::AmmRuntimeHealth::Degraded if counts.routing_pools > 0 => {
            GraphSyncPhase::Synced
        }
        evm_amm_state::adapters::AmmRuntimeHealth::Degraded => GraphSyncPhase::Error,
        evm_amm_state::adapters::AmmRuntimeHealth::Untrusted
        | evm_amm_state::adapters::AmmRuntimeHealth::ShuttingDown => GraphSyncPhase::Error,
        _ => GraphSyncPhase::Error,
    };
    app.graph_sync.detail = graph_sync_detail(status, counts);
}

fn graph_sync_detail(status: &AmmRuntimeStatusSnapshot, counts: GraphSyncCounts) -> String {
    let mut discovery_steps = None::<(u64, u64)>;
    for (_, progress) in status.active_work_items() {
        if progress.kind() != AmmWorkKind::Discovery {
            continue;
        }
        let (completed, total) = discovery_steps.get_or_insert((0, 0));
        *completed = completed.saturating_add(progress.completed());
        *total = total.saturating_add(progress.total().unwrap_or_default());
    }
    if let Some((completed, total)) = discovery_steps {
        let progress = if total > 0 {
            format!(" ({completed}/{total} discovery steps)")
        } else {
            format!(" ({completed} discovery steps complete)")
        };
        return format!("finding V2, Uniswap/Sushi V3, and Pancake V3 pools{progress}");
    }

    let pools_loading = counts
        .loading_pools
        .saturating_add(counts.queued_loads)
        .saturating_add(counts.pending_state_updates);
    if pools_loading > 0 {
        let mut protocols = BTreeMap::new();
        for (pool, state) in latest_pool_lifecycle_states(status) {
            if matches!(
                state,
                PoolRuntimeState::Queued
                    | PoolRuntimeState::Hydrating
                    | PoolRuntimeState::CatchingUp
            ) {
                *protocols
                    .entry(protocol_id_label(pool.protocol()))
                    .or_default() += 1;
            }
        }
        return format!(
            "loading {pools_loading} pools ({}); {} routing",
            format_count_map(protocols.into_iter()),
            counts.routing_pools
        );
    }

    format!(
        "{} pools routing, {} discovered, {} degraded, {} failed to load",
        counts.routing_pools, counts.discovered_pools, counts.degraded_pools, counts.failed_pools,
    )
}

fn graph_sync_counts(status: &AmmRuntimeStatusSnapshot) -> GraphSyncCounts {
    let mut counts = GraphSyncCounts::default();

    for state in latest_pool_lifecycle_states(status).into_values() {
        match state {
            PoolRuntimeState::Searchable | PoolRuntimeState::Live => counts.routing_pools += 1,
            PoolRuntimeState::Discovered => counts.discovered_pools += 1,
            PoolRuntimeState::Queued => counts.queued_loads += 1,
            PoolRuntimeState::Hydrating => counts.loading_pools += 1,
            PoolRuntimeState::CatchingUp => counts.pending_state_updates += 1,
            PoolRuntimeState::Degraded => counts.degraded_pools += 1,
            PoolRuntimeState::Failed => counts.failed_pools += 1,
            PoolRuntimeState::Removing | PoolRuntimeState::Removed => {}
            _ => {}
        }
    }

    counts
}

fn latest_pool_lifecycle_states(
    status: &AmmRuntimeStatusSnapshot,
) -> BTreeMap<&PoolKey, PoolRuntimeState> {
    let mut latest = BTreeMap::<&PoolKey, (u64, PoolRuntimeState)>::new();
    for (pool, state) in status.lifecycles().pools() {
        let generation = pool.generation().get();
        match latest.entry(pool.key()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((generation, state));
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if generation > entry.get().0 =>
            {
                entry.insert((generation, state));
            }
            _ => {}
        }
    }
    latest
        .into_iter()
        .map(|(pool, (_, state))| (pool, state))
        .collect()
}

fn routing_pool_count(graph: &AmmGraph) -> usize {
    graph
        .graph()
        .edge_references()
        .map(|edge| edge.weight().pool.clone())
        .collect::<HashSet<_>>()
        .len()
}

fn graph_token_pool_count(graph: &AmmGraph, token: Address) -> usize {
    graph
        .node_index(&token)
        .into_iter()
        .flat_map(|node| graph.graph().edges(node))
        .map(|edge| edge.weight().pool.clone())
        .collect::<HashSet<_>>()
        .len()
}

fn graph_coverage_summary(
    graph: &AmmGraph,
    token_in: &TokenInfo,
    token_out: &TokenInfo,
    graph_pools: usize,
) -> String {
    let background = if env_bool("AMM_ROUTE_TUI_BACKGROUND_DISCOVERY", true) {
        "focused + idle discovery enabled"
    } else {
        "focused discovery enabled"
    };
    format!(
        "COVERAGE  {}={} pools | {}={} pools | graph={} pools | {background}",
        token_in.symbol,
        graph_token_pool_count(graph, token_in.address),
        token_out.symbol,
        graph_token_pool_count(graph, token_out.address),
        graph_pools,
    )
}

fn mark_graph_synced(app: &mut AppState, routing_pools: usize) {
    app.graph_sync.phase = GraphSyncPhase::Synced;
    app.graph_sync.routing_pools = routing_pools;
    app.graph_sync.discovered_pools = 0;
    app.graph_sync.loading_pools = 0;
    app.graph_sync.queued_loads = 0;
    app.graph_sync.pending_state_updates = 0;
    app.graph_sync.degraded_pools = 0;
    app.graph_sync.failed_pools = 0;
    app.graph_sync.detail = format!("routing {routing_pools}, loading 0, queued 0, updates 0");
}

fn apply_live_subscriber_state(app: &mut AppState, state: &AmmSubscriberDriverState) {
    match state {
        AmmSubscriberDriverState::Paused => {
            app.chain_sync.phase = ChainSyncPhase::Applying;
            app.chain_sync.detail = "canonical subscriber paused for lifecycle update".to_owned();
        }
        AmmSubscriberDriverState::Running { point, .. } => {
            app.last_block = point.block_number();
            app.chain_sync.phase = ChainSyncPhase::Synced;
            app.chain_sync.detail =
                format!("canonical subscriber at block {}", point.block_number());
        }
        AmmSubscriberDriverState::Failed(error) => {
            app.chain_sync.phase = ChainSyncPhase::Degraded;
            app.chain_sync.detail =
                format!("canonical subscriber failed: {}", fit_to_width(error, 64));
        }
        AmmSubscriberDriverState::Stopped => {
            app.chain_sync.phase = ChainSyncPhase::Degraded;
            app.chain_sync.detail = "canonical subscriber stopped".to_owned();
        }
        _ => {}
    }
}

fn apply_live_price_event(app: &mut AppState, event: PriceEvent) {
    match event {
        PriceEvent::Completed { result: Ok(prices) } => {
            app.prices = prices;
            app.status = format!("updated {}", app.prices.coverage_label(app.tokens.len()));
        }
        PriceEvent::Completed { result: Err(error) } => {
            app.prices.last_error = Some(error.clone());
            app.status = format!("price refresh failed: {}", fit_to_width(&error, 80));
        }
    }
}

fn start_live_dynamic_discovery(
    app: &mut AppState,
    runtime: &LiveTuiRuntime,
    side: Side,
    raw_query: String,
    tx: mpsc::Sender<DynamicDiscoveryEvent>,
    generations: &mut [u64; 2],
) {
    let generation = advance_dynamic_discovery_generation(generations, side);
    let query = raw_query.trim().to_owned();
    if query.is_empty() {
        app.status = "enter a token address or symbol before pressing Enter".to_owned();
        return;
    }
    let provider = Arc::clone(&runtime.provider);
    let amm = runtime.amm.clone();
    let owners = runtime.discovery.clone();
    let known_tokens = app.tokens.clone();
    app.status = format!("looking up token {query}");
    set_route_work(
        app,
        RouteWorkPhase::Discovering,
        format!("resolving token {query}"),
        None,
    );
    tokio::spawn(async move {
        let outcome = match resolve_token_selector(provider.as_ref(), &known_tokens, &query).await {
            Ok(token) => {
                let connectors = connector_addresses(&known_tokens, token.address);
                let discovery = queue_dynamic_token_discovery(
                    &amm,
                    owners,
                    token.address,
                    connectors.iter().copied(),
                )
                .await;
                Ok(DynamicDiscoveryOutcome { token, discovery })
            }
            Err(error) => Err(format!("{error:#}")),
        };
        let _ = tx
            .send(DynamicDiscoveryEvent {
                generation,
                side,
                query,
                outcome,
            })
            .await;
    });
}

fn selected_token_discovery_plan(app: &AppState, side: Side) -> SelectedTokenDiscoveryPlan {
    let (token, counterpart) = match side {
        Side::Input => (app.token_in(), app.token_out()),
        Side::Output => (app.token_out(), app.token_in()),
    };
    let connectors = focused_discovery_connectors(&app.tokens, token.address, counterpart.address);
    SelectedTokenDiscoveryPlan { token, connectors }
}

fn start_selected_token_discovery(
    app: &mut AppState,
    runtime: &LiveTuiRuntime,
    side: Side,
    tx: mpsc::Sender<DynamicDiscoveryEvent>,
    generations: &mut [u64; 2],
) {
    let generation = advance_dynamic_discovery_generation(generations, side);
    let plan = selected_token_discovery_plan(app, side);
    let query = plan.token.symbol.clone();
    let amm = runtime.amm.clone();
    let owners = runtime.discovery.clone();
    app.status = format!("discovering {} connector pools", plan.token.symbol);
    tokio::spawn(async move {
        let discovery =
            queue_dynamic_token_discovery(&amm, owners, plan.token.address, plan.connectors).await;
        let _ = tx
            .send(DynamicDiscoveryEvent {
                generation,
                side,
                query,
                outcome: Ok(DynamicDiscoveryOutcome {
                    token: plan.token,
                    discovery,
                }),
            })
            .await;
    });
}

async fn queue_dynamic_token_discovery(
    amm: &AmmRuntimeHandle,
    owners: Vec<(ProtocolId, DiscoveryOwnerId)>,
    token: Address,
    connectors: impl IntoIterator<Item = Address>,
) -> Result<usize, String> {
    let connectors = connectors.into_iter().collect::<Vec<_>>();
    if connectors.is_empty() {
        return Ok(0);
    }

    let mut accepted = 0usize;
    let mut last_error = None;
    for (protocol, owner) in owners {
        match amm
            .queue_token_discovery(
                owner,
                TokenEdgeDiscoveryRequest::new(token, connectors.iter().copied())
                    .with_protocol(protocol),
                AmmDiscoveryOptions::default(),
            )
            .await
        {
            Ok(_) => accepted += 1,
            Err(error) => last_error = Some(error.to_string()),
        }
    }

    if accepted > 0 {
        Ok(accepted)
    } else {
        Err(last_error.unwrap_or_else(|| "no compatible factory watcher".to_owned()))
    }
}

fn apply_live_dynamic_discovery(
    app: &mut AppState,
    event: DynamicDiscoveryEvent,
    generations: &[u64; 2],
) -> bool {
    if generations[side_index(event.side)] != event.generation {
        return false;
    }
    let outcome = match event.outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            app.status = format!(
                "token lookup failed for {}: {}",
                event.query,
                fit_to_width(&error, 72)
            );
            set_route_work(app, RouteWorkPhase::Error, fit_to_width(&error, 72), None);
            return false;
        }
    };
    let index = app.upsert_token(outcome.token.clone());
    app.select_token_index(index, event.side);
    app.custom_address.clear();
    app.active = event.side.active_field();
    app.status = match outcome.discovery {
        Ok(0) => format!(
            "{} selected; no connector tokens available",
            outcome.token.symbol
        ),
        Ok(accepted) => format!(
            "{} selected; {accepted} focused discovery job(s) accepted",
            outcome.token.symbol
        ),
        Err(error) => format!(
            "{} selected; focused discovery unavailable: {error}",
            outcome.token.symbol
        ),
    };
    true
}

const fn side_index(side: Side) -> usize {
    match side {
        Side::Input => 0,
        Side::Output => 1,
    }
}

fn advance_dynamic_discovery_generation(generations: &mut [u64; 2], side: Side) -> u64 {
    let index = side_index(side);
    generations[index] = generations[index].saturating_add(1);
    generations[index]
}

async fn bootstrap_tui<B: Backend>(
    terminal: &mut Terminal<B>,
    ws_url: String,
) -> Result<BootstrappedTui>
where
    B::Error: Send + Sync + 'static,
{
    let mut timings = BootstrapTimings::default();
    // Live per-phase progress to stderr under the headless bench, so a slow or
    // hanging phase is visible even if bootstrap never reaches the final report.
    let bench = env_bool("AMM_ROUTE_TUI_BENCH", false);
    let phase_done = |name: &str, d: Duration| {
        if bench {
            eprintln!("[bootstrap] {name:<20} {d:?}");
        }
    };
    let user_config = load_tui_user_config().context("load TUI config")?;
    let tokens = token_list(&user_config).context("load token list")?;
    let mut app = AppState::new(tokens.clone(), Vec::new(), 0, 0, 0);
    app.quote_loading = true;
    app.status = "connecting websocket endpoint...".to_owned();
    app.chain_sync.phase = ChainSyncPhase::Applying;
    app.chain_sync.detail = "connecting websocket".to_owned();
    set_route_work(
        &mut app,
        RouteWorkPhase::Discovering,
        "connecting websocket endpoint",
        Some((1, STARTUP_STEPS)),
    );
    draw_now(terminal, &app)?;

    let (ui_tx, ui_rx) = mpsc::channel(128);
    spawn_input_thread(ui_tx);

    let ws_connect_start = Instant::now();
    let provider = Arc::new(
        RootProvider::<AnyNetwork>::connect(&ws_url)
            .await
            .context("connect websocket endpoint")?,
    );
    timings.ws_connect = ws_connect_start.elapsed();
    phase_done("ws connect", timings.ws_connect);

    app.status = "fetching latest block...".to_owned();
    app.chain_sync.detail = "fetching latest block".to_owned();
    set_route_work(
        &mut app,
        RouteWorkPhase::Discovering,
        "fetching latest block",
        Some((2, STARTUP_STEPS)),
    );
    draw_now(terminal, &app)?;
    let chain_meta_start = Instant::now();
    let latest = provider.get_block_number().await.context("latest block")?;
    let chain_id = provider.get_chain_id().await.context("chain id")?;
    app.last_block = latest;
    app.chain_sync.phase = ChainSyncPhase::Synced;
    app.chain_sync.detail = format!("synced block {latest}");
    match provider.get_gas_price().await {
        Ok(gas_price) => {
            app.gas_price_wei = Some(gas_price);
            app.gas_router_status = format!("gas price {} gwei", format_gwei(gas_price));
        }
        Err(error) => {
            app.gas_router_status = format!("gas price unavailable: {error}");
        }
    }
    timings.chain_meta = chain_meta_start.elapsed();
    phase_done("chain meta", timings.chain_meta);

    let sim_config = SimConfig::default()
        .with_v2_router(V2_ROUTER_02)
        .with_v3_quoter(V3_QUOTER_V2);

    let cache_dir = tui_cache_dir();
    let persist_cache = env_bool("AMM_ROUTE_TUI_PERSIST_CACHE", true);
    timings.persist_cache = persist_cache;
    timings.warm_cache = persist_cache
        && cache_dir
            .join(format!("chain_{chain_id}"))
            .join("evm_state.bin")
            .exists();
    app.status = if persist_cache {
        format!(
            "chain {chain_id}; latest block {latest}; loading fork cache from {}...",
            cache_dir.display()
        )
    } else {
        format!("chain {chain_id}; latest block {latest}; initializing in-memory fork cache...")
    };
    set_route_work(
        &mut app,
        RouteWorkPhase::Discovering,
        if persist_cache {
            format!("loading cache at block {latest}")
        } else {
            format!("initializing cache at block {latest}")
        },
        Some((3, STARTUP_STEPS)),
    );
    draw_now(terminal, &app)?;
    let cache_build_start = Instant::now();
    let mut cache = build_tui_cache(
        provider.clone(),
        latest,
        chain_id,
        persist_cache,
        &cache_dir,
    )
    .await;
    if gas_estimates_enabled() {
        match install_demo_router(&mut cache) {
            Ok(code_hash) => {
                app.gas_router_ready = true;
                app.gas_router_status = format!(
                    "{}; demo router installed {code_hash:?}",
                    app.gas_router_status
                );
            }
            Err(error) => {
                app.gas_router_ready = false;
                app.gas_router_status = format!("gas unavailable: {error:#}");
            }
        }
    } else {
        app.gas_router_ready = false;
        app.gas_router_status = "gas estimates disabled".to_owned();
    }
    let cache_build_elapsed = cache_build_start.elapsed();
    timings.cache_build = cache_build_elapsed;
    phase_done(
        if timings.warm_cache {
            "cache build (warm)"
        } else {
            "cache build (cold)"
        },
        timings.cache_build,
    );
    if persist_cache {
        app.status = format!(
            "loaded persistent fork cache from {} in {:?}",
            cache_dir.display(),
            cache_build_elapsed
        );
    } else {
        app.status = format!("initialized in-memory fork cache in {cache_build_elapsed:?}");
    }

    app.status = format!("latest block {latest}; discovering AMM pools...");
    set_route_work(
        &mut app,
        RouteWorkPhase::Discovering,
        "discovering startup pools",
        Some((4, STARTUP_STEPS)),
    );
    draw_now(terminal, &app)?;
    let startup_focus = startup_focus_pair(&tokens);
    let mut registry = build_registry(sim_config)?;
    let discovery_start = Instant::now();
    let discovered_pool_infos =
        discover_initial_pools(&registry, &mut cache, &tokens, startup_focus, &user_config)
            .context("discover AMM pools")?;
    timings.discovery = discovery_start.elapsed();
    if discovered_pool_infos.is_empty() {
        bail!("pool discovery returned no candidate pools");
    }
    timings.discovered_pools = discovered_pool_infos.len();
    phase_done(
        &format!("discovery ({} pools)", timings.discovered_pools),
        timings.discovery,
    );

    app.status = format!(
        "cold-starting {} discovered/manual pool(s)...",
        discovered_pool_infos.len()
    );
    set_route_work(
        &mut app,
        RouteWorkPhase::Discovering,
        format!("cold-starting {} pool(s)", discovered_pool_infos.len()),
        Some((5, STARTUP_STEPS)),
    );
    draw_now(terminal, &app)?;
    let mut pools = discovered_pool_infos
        .iter()
        .map(|pool| pool.registration.clone())
        .collect::<Vec<_>>();
    let cold_start_start = Instant::now();
    let outcomes = registry
        .cold_start_many(
            &mut pools,
            &mut cache,
            provider.as_ref(),
            ColdStartPolicy::Eager,
        )
        .await
        .context("cold-start discovered pools")?;
    timings.cold_start = cold_start_start.elapsed();
    let (fast_pools, fallback_pools) = classify_cold_start_outcomes(&outcomes);
    timings.fast_pools = fast_pools;
    timings.fallback_pools = fallback_pools;
    phase_done(
        &format!("cold_start ({fast_pools} fast, {fallback_pools} fallback)"),
        timings.cold_start,
    );

    let register_start = Instant::now();
    let mut ready_pool_infos = Vec::new();
    for (pool, outcome) in pools.into_iter().zip(outcomes) {
        if is_ready_outcome(&outcome) {
            registry.register_pool(pool.clone())?;
            ready_pool_infos.push(pool_info_from_registration(pool, &tokens));
        }
    }
    timings.register_ready = register_start.elapsed();
    phase_done("register ready", timings.register_ready);
    if ready_pool_infos.is_empty() {
        bail!("no discovered pools reached Ready");
    }
    timings.ready_pools = ready_pool_infos.len();

    let (tokens, dropped_tokens) = filter_connected_tokens(tokens, &registry);
    if tokens.len() < 2 {
        bail!("fewer than two listed tokens have a ready graph edge");
    }

    let sync = AmmSyncEngine::new(registry.clone())?;
    let ready_pools = ready_pool_infos.len();
    let skipped_pools = discovered_pool_infos.len().saturating_sub(ready_pools);
    let gas_price_wei = app.gas_price_wei;
    let gas_router_ready = app.gas_router_ready;
    let gas_router_status = app.gas_router_status.clone();
    app = AppState::new(tokens, ready_pool_infos, latest, ready_pools, skipped_pools);
    app.gas_price_wei = gas_price_wei;
    app.gas_router_ready = gas_router_ready;
    app.gas_router_status = gas_router_status;
    app.topology_updates = 1;
    app.quote_loading = true;
    app.chain_sync.phase = ChainSyncPhase::Synced;
    app.chain_sync.detail = format!("synced block {latest}");
    set_route_work(
        &mut app,
        RouteWorkPhase::Discovering,
        "building liquidity index",
        Some((6, STARTUP_STEPS)),
    );
    app.status = "refreshing liquidity index for warmed pools...".to_owned();
    draw_now(terminal, &app)?;
    let liquidity_start = Instant::now();
    refresh_liquidity_index(&mut app, &sync, &mut cache, provider.as_ref()).await;
    timings.liquidity_index = liquidity_start.elapsed();
    phase_done("liquidity index", timings.liquidity_index);

    // Prime the base cache with the state route searches read, so the following
    // `cache.flush()` persists it and warm restarts skip the first-search storm.
    if env_bool("AMM_ROUTE_TUI_PRIME_CACHE", true) {
        let prime_start = Instant::now();
        let primed = prime_search_cache(&app, &sync, &mut cache, &sim_config);
        timings.prime = prime_start.elapsed();
        phase_done(&format!("prime cache ({primed} pairs)"), timings.prime);
    }

    let subscribe_start = Instant::now();
    let topics = sync.registry().subscription_topics();
    if topics.is_empty() {
        bail!("registered pools produced no subscription topics");
    }

    app.status = "subscribing to websocket logs and blocks...".to_owned();
    app.chain_sync.phase = ChainSyncPhase::Applying;
    app.chain_sync.detail = "subscribing logs/blocks".to_owned();
    set_route_work(
        &mut app,
        RouteWorkPhase::Discovering,
        "subscribing logs and blocks",
        Some((7, STARTUP_STEPS)),
    );
    draw_now(terminal, &app)?;
    let (chain_tx, chain_rx) = mpsc::channel(1024);
    spawn_chain_task(provider.clone(), topics, chain_tx);
    app.chain_sync.phase = ChainSyncPhase::Synced;
    app.chain_sync.detail = format!("synced block {latest}");
    timings.subscribe = subscribe_start.elapsed();
    phase_done("subscribe", timings.subscribe);

    app.status = if dropped_tokens == 0 {
        format!("graph synced: routing_pools={ready_pools}, subscribed over websocket")
    } else {
        format!(
            "graph synced: routing_pools={ready_pools}, dropped {dropped_tokens} disconnected token(s)"
        )
    };
    let startup_input_symbol = app.token_in().symbol;
    let startup_output_symbol = app.token_out().symbol;
    set_route_work(
        &mut app,
        RouteWorkPhase::Quoting,
        format!(
            "quoting {} -> {}",
            startup_input_symbol, startup_output_symbol
        ),
        Some((8, STARTUP_STEPS)),
    );
    draw_now(terminal, &app)?;
    let first_search_start = Instant::now();
    let quote_ok = refresh_quote(&mut app, &sync, &mut cache, &sim_config, QuoteRefresh::Full);
    timings.first_search = first_search_start.elapsed();
    phase_done("first search", timings.first_search);
    if quote_ok {
        draw_now(terminal, &app)?;
        let gas_sim_start = Instant::now();
        refresh_displayed_route_gas(&mut app, &sync, &mut cache);
        timings.gas_sim = gas_sim_start.elapsed();
        phase_done("gas simulation", timings.gas_sim);
    }
    if persist_cache {
        let flush_start = Instant::now();
        let flush_result = cache.flush();
        timings.flush = flush_start.elapsed();
        phase_done("cache flush", timings.flush);
        match flush_result {
            Ok(()) => {
                let line = format!(
                    "persistent cache saved to {} in {:?}",
                    cache_dir.display(),
                    timings.flush
                );
                app.status = line.clone();
                if let Some(quote) = &mut app.quote {
                    push_stream_line(&mut quote.stream_lines, line);
                }
            }
            Err(error) => {
                let line = format!("cache persistence failed: {error:#}");
                app.status = line.clone();
                if let Some(quote) = &mut app.quote {
                    push_stream_line(&mut quote.stream_lines, line);
                }
            }
        }
    }

    let graph_report = AmmGraph::from_registry(sync.registry(), GraphBuildOptions::default());
    timings.token_nodes = graph_report.graph.node_count();
    timings.directed_edges = graph_report.graph.edge_count();

    Ok(BootstrappedTui {
        app,
        sync,
        cache,
        provider,
        sim_config,
        ui_rx,
        chain_rx,
        timings,
    })
}

async fn build_tui_cache(
    provider: Arc<RootProvider<AnyNetwork>>,
    block: u64,
    chain_id: u64,
    persist_cache: bool,
    cache_dir: &Path,
) -> EvmCache {
    let mut builder = EvmCache::builder(provider)
        .block(BlockId::Number(BlockNumberOrTag::Number(block)))
        .chain_id(chain_id)
        .speed_mode(CacheSpeedMode::Fast);

    if persist_cache {
        builder = builder
            .cache_config(CacheConfig::new(
                cache_dir.to_path_buf(),
                chain_id,
                Default::default(),
                Default::default(),
            ))
            .shared_memory_capacity(SharedMemoryCapacity::Auto);
    }

    builder.build().await
}

async fn prepare_tui_quote_targets(
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
            .with_context(|| format!("fetch shared quote entrypoint {address}"))?;
            if code.is_empty() {
                bail!("shared quote entrypoint {address} has no runtime code");
            }
            let actual = keccak256(&code);
            if actual != proof.code_hash {
                bail!(
                    "shared quote entrypoint {address} code hash mismatch: code={actual}, proof={}",
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
        .buffer_unordered(3)
        .collect::<Vec<Result<PreparedAccountValue>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedAccountPatch::new(block_hash, block_number, values))
}

fn draw_now<B: Backend>(terminal: &mut Terminal<B>, app: &AppState) -> Result<()>
where
    B::Error: Send + Sync + 'static,
{
    terminal.draw(|frame| draw(frame, app))?;
    Ok(())
}

async fn refresh_liquidity_index(
    app: &mut AppState,
    sync: &AmmSyncEngine,
    cache: &mut EvmCache,
    provider: &RootProvider<AnyNetwork>,
) {
    let graph_report = AmmGraph::from_registry(sync.registry(), GraphBuildOptions::default());
    let (mut index, build) = PoolLiquidityIndex::from_registry_with_scope(
        sync.registry(),
        &graph_report.graph,
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs,
    );
    if index.is_empty() {
        app.liquidity_index = None;
        app.status = "liquidity index disabled: no trackable balances".to_owned();
        return;
    }

    let refresh = index.refresh_all(cache, provider).await;
    app.status = format!(
        "liquidity index: tracked {}, refreshed {}, unknown {}, failures {}",
        build.tracked_balances,
        refresh.refreshed_balances,
        refresh.unknown_balances,
        refresh.failures.len()
    );
    app.liquidity_index = Some(index);
}

/// Warm the base [`EvmCache`] with the pool state route searches read, so it is
/// captured by the subsequent `cache.flush()` — making warm restarts (and every
/// in-process search) skip the first-search lazy-RPC storm. Uses the
/// single-threaded *injecting* [`AmmSearcher::find_routes`] path (which fetches
/// AND writes touched slots into the base cache; the streaming session path used
/// for live quotes runs on a throwaway overlay and never persists what it reads).
///
/// Primes the startup pair — whose multi-hop search alone reaches roughly half
/// the graph — plus every `token <-> WETH` hub pair so every pool is touched by a
/// primed search. Overlap across searches is served from the in-process backend
/// cache, so all but the first search are cheap. Best-effort: a pair with no
/// route or a bad amount is skipped. Returns the number of pairs primed.
fn prime_search_cache(
    app: &AppState,
    sync: &AmmSyncEngine,
    cache: &mut EvmCache,
    sim_config: &SimConfig,
) -> usize {
    let graph_report = AmmGraph::from_registry(sync.registry(), GraphBuildOptions::default());
    let mut searcher = AmmSearcher::new(sync.registry(), &graph_report.graph);
    if let Some(index) = app.liquidity_index.as_ref() {
        searcher = searcher.with_liquidity_index(index);
    }

    let mut pairs: Vec<(usize, usize)> = vec![(app.input_index, app.output_index)];
    if let Some(weth) = app.tokens.iter().position(|token| token.symbol == "WETH") {
        for other in 0..app.tokens.len() {
            if other != weth {
                pairs.push((other, weth));
                pairs.push((weth, other));
            }
        }
    }
    let mut seen = HashSet::new();
    pairs.retain(|pair| seen.insert(*pair));

    let mut primed = 0;
    for (input, output) in pairs {
        let token_in = &app.tokens[input];
        let token_out = &app.tokens[output];
        if token_in.address == token_out.address {
            continue;
        }
        let Ok(amount_in) = parse_units(&app.amount, token_in.decimals) else {
            continue;
        };
        let request = RouteRequest::new(token_in.address, token_out.address, amount_in)
            .with_config(tui_search_config(app))
            .with_sim_config(*sim_config);
        if searcher.find_routes(&request, cache).is_ok() {
            primed += 1;
        }
    }
    primed
}

#[allow(clippy::too_many_arguments)]
async fn tui_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut AppState,
    sync: &mut AmmSyncEngine,
    cache: &mut EvmCache,
    provider: Arc<RootProvider<AnyNetwork>>,
    sim_config: &SimConfig,
    ui_rx: &mut mpsc::Receiver<UiEvent>,
    chain_rx: &mut mpsc::Receiver<ChainEvent>,
) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut pending_quote_refresh: Option<Instant> = None;
    let (price_tx, mut price_rx) = mpsc::channel(8);
    let mut price_refresh_in_flight = false;
    let mut next_price_refresh = price_refresh_enabled().then_some(Instant::now());
    let (gas_price_tx, mut gas_price_rx) = mpsc::channel(4);
    let mut gas_price_refresh_in_flight = false;
    let (simulation_tx, mut simulation_rx) = mpsc::channel(4);

    loop {
        terminal.draw(|frame| draw(frame, app))?;

        tokio::select! {
            _ = tick.tick() => {
                if pending_quote_refresh.is_some_and(|due| Instant::now() >= due) {
                    pending_quote_refresh = None;
                    run_full_quote_refresh(terminal, app, sync, cache, sim_config).await?;
                    next_price_refresh = price_refresh_enabled().then_some(Instant::now());
                }
                if should_start_price_refresh(next_price_refresh, price_refresh_in_flight) {
                    price_refresh_in_flight = start_price_refresh(app, &price_tx);
                    next_price_refresh = price_refresh_enabled()
                        .then_some(Instant::now() + price_refresh_interval());
                }
            }
            Some(event) = price_rx.recv() => {
                price_refresh_in_flight = false;
                apply_price_event(app, sync, event);
            }
            Some(event) = gas_price_rx.recv() => {
                gas_price_refresh_in_flight = false;
                apply_gas_price_event(app, event);
            }
            Some(event) = simulation_rx.recv() => {
                apply_simulation_event(app, event);
            }
            Some(event) = ui_rx.recv() => {
                match handle_ui_event(app, event) {
                    UiAction::Quit => break,
                    UiAction::Continue => {}
                    UiAction::RequestQuote => {
                        schedule_quote_refresh(app, &mut pending_quote_refresh);
                    }
                    UiAction::SelectToken { .. } => {
                        schedule_quote_refresh(app, &mut pending_quote_refresh);
                    }
                    UiAction::SimulateSwap => {
                        start_tenderly_simulation(
                            app,
                            Arc::clone(&provider),
                            simulation_tx.clone(),
                        );
                    }
                    UiAction::DiscoverToken { side, query } => {
                        terminal.draw(|frame| draw(frame, app))?;
                        if let Err(error) = discover_custom_token(app, sync, cache, provider.as_ref(), side, query).await {
                            app.status = format!("token discovery failed: {error:#}");
                            app.quote_error = Some(error.to_string());
                        }
                        schedule_quote_refresh(app, &mut pending_quote_refresh);
                        next_price_refresh = price_refresh_enabled().then_some(Instant::now());
                    }
                }
            }
            Some(event) = chain_rx.recv() => {
                begin_chain_sync_indicator(app, &event);
                terminal.draw(|frame| draw(frame, app))?;

                let refresh_gas_price = matches!(event, ChainEvent::Block(_));
                let result = handle_chain_event(app, sync, cache, event);
                finish_chain_sync_indicator(app, &result);
                if refresh_gas_price && !gas_price_refresh_in_flight {
                    gas_price_refresh_in_flight =
                        start_gas_price_refresh(provider.clone(), gas_price_tx.clone());
                }

                match result.refresh {
                    QuoteRefresh::None => {}
                    refresh if pending_quote_refresh.is_some() => {
                        app.status = match refresh {
                            QuoteRefresh::Full => "received live update; pending full quote refresh".to_owned(),
                            QuoteRefresh::Incremental(_) => {
                                "received live update; pending selected-route refresh".to_owned()
                            }
                            QuoteRefresh::None => app.status.clone(),
                        };
                    }
                    refresh => {
                        begin_route_quoting(app, &refresh);
                        terminal.draw(|frame| draw(frame, app))?;
                        if refresh_quote(app, sync, cache, sim_config, refresh) {
                            terminal.draw(|frame| draw(frame, app))?;
                            refresh_displayed_route_gas(app, sync, cache);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn begin_chain_sync_indicator(app: &mut AppState, event: &ChainEvent) {
    app.chain_sync.phase = ChainSyncPhase::Applying;
    app.chain_sync.detail = match event {
        ChainEvent::Log(log) => {
            let block = log.block_number.unwrap_or(app.last_block);
            format!("applying 1 log from block {block}")
        }
        ChainEvent::Block(header) => format!("advancing block {}", header.number()),
        ChainEvent::Error(_) => "stream error".to_owned(),
    };
}

fn finish_chain_sync_indicator(app: &mut AppState, result: &ChainProcessResult) {
    app.chain_sync.phase = result.phase;
    app.chain_sync.detail = result.detail.clone();
}

fn should_start_price_refresh(next_refresh: Option<Instant>, in_flight: bool) -> bool {
    !in_flight && next_refresh.is_some_and(|next| Instant::now() >= next)
}

fn start_price_refresh(app: &mut AppState, tx: &mpsc::Sender<PriceEvent>) -> bool {
    if !price_refresh_enabled() {
        app.prices = PriceBook::disabled();
        return false;
    }

    let tokens = prioritized_price_tokens(app);
    if tokens.is_empty() {
        return false;
    }

    app.status = format!("refreshing USD prices for {} token(s)", tokens.len());
    let previous = app.prices.clone();
    let tx = tx.clone();
    tokio::spawn(async move {
        let result = fetch_price_book(&tokens, previous)
            .await
            .map_err(|error| format!("{error:#}"));
        let _ = tx.send(PriceEvent::Completed { result }).await;
    });
    true
}

fn apply_price_event(app: &mut AppState, sync: &AmmSyncEngine, event: PriceEvent) {
    match event {
        PriceEvent::Completed { result } => match result {
            Ok(prices) => {
                app.prices = prices;
                app.status = format!("updated {}", app.prices.coverage_label(app.tokens.len()));
                rebuild_quote_price_labels(app, sync);
            }
            Err(error) => {
                if app.prices.usd_by_token.is_empty() {
                    app.prices = PriceBook {
                        usd_by_token: HashMap::new(),
                        source: format!("prices unavailable: {}", fit_to_width(&error, 48)),
                        last_updated: None,
                        last_error: Some(error.clone()),
                    };
                } else {
                    app.prices.last_error = Some(error.clone());
                }
                app.status = format!("price refresh failed: {}", fit_to_width(&error, 80));
            }
        },
    }
}

fn start_gas_price_refresh(
    provider: Arc<RootProvider<AnyNetwork>>,
    tx: mpsc::Sender<GasPriceEvent>,
) -> bool {
    tokio::spawn(async move {
        let result = provider
            .get_gas_price()
            .await
            .map_err(|error| format!("{error:#}"));
        let _ = tx.send(GasPriceEvent::Completed { result }).await;
    });
    true
}

fn apply_gas_price_event(app: &mut AppState, event: GasPriceEvent) {
    match event {
        GasPriceEvent::Completed { result } => match result {
            Ok(gas_price) => {
                app.gas_price_wei = Some(gas_price);
                app.gas_router_status = format!("gas price {} gwei", format_gwei(gas_price));
                refresh_displayed_gas_price(app);
            }
            Err(error) => {
                app.gas_router_status = format!("gas price refresh failed: {error}");
            }
        },
    }
}

fn refresh_displayed_gas_price(app: &mut AppState) {
    let Some(gas_price_wei) = app.gas_price_wei else {
        return;
    };
    let Some(quote) = &mut app.quote else {
        return;
    };
    let Some(existing) = quote.gas.as_ref().and_then(|gas| gas.estimate.clone()) else {
        return;
    };
    let mut refreshed = existing;
    refreshed.gas_price_wei = Some(gas_price_wei);
    refreshed.gas_cost_native = Some(U256::from(refreshed.gas_used) * U256::from(gas_price_wei));
    quote.gas = Some(gas_estimate_view(&refreshed, &app.prices));
}

fn start_tenderly_simulation(
    app: &mut AppState,
    provider: Arc<RootProvider<AnyNetwork>>,
    tx: mpsc::Sender<SimulationEvent>,
) {
    if app.tenderly.in_flight {
        app.status = "Tenderly simulation already in flight".to_owned();
        return;
    }

    let request = match build_tenderly_simulation_request(app) {
        Ok(request) => request,
        Err(error) => {
            let message = format!(
                "Tenderly simulation unavailable: {}",
                fit_to_width(&error, 96)
            );
            app.tenderly.status = message.clone();
            app.tenderly.ok = Some(false);
            app.status = message;
            return;
        }
    };

    app.tenderly.in_flight = true;
    app.tenderly.ok = None;
    app.tenderly.status = "submitting Tenderly simulation".to_owned();
    app.status = "submitting Tenderly simulation".to_owned();
    tokio::spawn(async move {
        let result = submit_tenderly_simulation(request, provider.as_ref()).await;
        let _ = tx.send(SimulationEvent::Completed { result }).await;
    });
}

fn apply_simulation_event(app: &mut AppState, event: SimulationEvent) {
    app.tenderly.in_flight = false;
    match event {
        SimulationEvent::Completed { result } => match result {
            Ok(outcome) => {
                let gas = outcome
                    .gas_used
                    .map(|gas| format!(" gas={}", comma_digits(&gas.to_string())))
                    .unwrap_or_default();
                let output = format_units(
                    outcome.amount_out,
                    outcome.output_decimals,
                    outcome.output_decimals.min(8) as usize,
                );
                app.tenderly.ok = Some(true);
                app.tenderly.status = format!(
                    "validated block {} end-of-block={} hash=matched output={} {} delta={}{}",
                    comma_digits(&outcome.block_number.to_string()),
                    comma_digits(&outcome.transaction_index.to_string()),
                    output,
                    outcome.output_symbol,
                    outcome.output_delta,
                    gas,
                );
                app.status = format!("Tenderly validated route: {}", outcome.url);
            }
            Err(error) => {
                app.tenderly.ok = Some(false);
                app.tenderly.status = format!("Tenderly simulation failed: {error}");
                app.status = format!("Tenderly simulation failed: {}", fit_to_width(&error, 96));
            }
        },
    }
}

fn build_tenderly_simulation_request(app: &AppState) -> Result<TenderlySimulationRequest, String> {
    let config = app
        .tenderly
        .config
        .as_ref()
        .ok_or_else(|| app.tenderly.status.clone())?
        .clone();
    let displayed_quote = app
        .quote
        .as_ref()
        .ok_or_else(|| "no displayed route to simulate yet".to_owned())?;
    let live = displayed_quote.live_tenderly.as_ref().ok_or_else(|| {
        "the displayed route is not paired with an immutable live snapshot".to_owned()
    })?;
    let quote = live.quote.quote();
    let source = live.quote.source();
    let snapshot = live.view.snapshot();
    let point = snapshot.point();
    if source.runtime_id() != snapshot.runtime_id()
        || source.state_version() != snapshot.version()
        || source.point() != point
        || source.graph_version() != live.view.graph().version()
        || quote != &displayed_quote.best
        || displayed_quote.block_number != point.block_number()
    {
        return Err("displayed route provenance no longer matches its live snapshot".to_owned());
    }
    let token_in = quote
        .hops
        .first()
        .map(|hop| hop.hop.token_in)
        .ok_or_else(|| "cannot simulate an empty route".to_owned())?;
    let token_out = quote
        .hops
        .last()
        .map(|hop| hop.hop.token_out)
        .ok_or_else(|| "cannot simulate an empty route".to_owned())?;
    let output_token = app
        .tokens
        .iter()
        .find(|token| token.address == token_out)
        .ok_or_else(|| {
            format!(
                "missing output-token metadata for {}",
                address_hex(token_out)
            )
        })?;
    let local_gas_used = displayed_quote
        .gas
        .as_ref()
        .and_then(|gas| gas.gas_used)
        .unwrap_or(500_000);
    let gas_limit = tenderly_gas_limit(local_gas_used);
    let network_id = point.chain_id().to_string();

    let calldata = encode_demo_router_execute_calldata(snapshot.registry().registry(), quote)
        .map_err(|error| format!("{error:#}"))?;
    let runtime = demo_router_runtime().map_err(|error| format!("{error:#}"))?;
    let balance_mapping = app.gas_balance_mappings.get(&token_in).ok_or_else(|| {
        format!(
            "input token {} has no verified balance layout",
            address_hex(token_in)
        )
    })?;
    let balance_slot = tenderly_balance_slot(token_in, DEMO_ROUTER, balance_mapping)?;
    let state_objects = tenderly_state_objects(token_in, balance_slot, quote.amount_in, &runtime);

    let mut payload = json!({
        "save": true,
        "save_if_fails": true,
        "simulation_type": "full",
        "network_id": network_id,
        "block_number": point.block_number(),
        "from": address_hex(config.from),
        "to": address_hex(DEMO_ROUTER),
        "gas": gas_limit,
        "gas_price": 0,
        "value": 0,
        "input": bytes_hex(&calldata),
    });
    payload
        .as_object_mut()
        .expect("payload object")
        .insert("state_objects".to_owned(), Value::Object(state_objects));

    let endpoint = format!(
        "https://api.tenderly.co/api/v1/account/{}/project/{}/simulate",
        config.account_slug, config.project_slug
    );
    let dashboard_url = format!(
        "https://dashboard.tenderly.co/{}/{}/simulator",
        config.account_slug, config.project_slug
    );

    Ok(TenderlySimulationRequest {
        api_key: config.api_key,
        endpoint,
        dashboard_url,
        payload,
        block_number: point.block_number(),
        block_hash: point.block_hash(),
        expected_amount_out: quote.amount_out,
        output_symbol: output_token.symbol.clone(),
        output_decimals: output_token.decimals,
        output_tolerance_bps: env_usize(
            "AMM_ROUTE_TUI_TENDERLY_OUTPUT_TOLERANCE_BPS",
            DEFAULT_TENDERLY_OUTPUT_TOLERANCE_BPS as usize,
        )
        .min(10_000) as u16,
    })
}

fn tenderly_state_objects(
    token_in: Address,
    balance_slot: B256,
    amount_in: U256,
    runtime: &Bytes,
) -> serde_json::Map<String, Value> {
    let mut token_storage = serde_json::Map::new();
    token_storage.insert(
        b256_hex(balance_slot),
        Value::String(u256_word_hex(amount_in)),
    );

    let mut state_objects = serde_json::Map::new();
    state_objects.insert(
        address_hex(DEMO_ROUTER),
        json!({ "code": bytes_hex(runtime) }),
    );
    state_objects.insert(
        address_hex(token_in),
        json!({ "storage": Value::Object(token_storage) }),
    );
    state_objects
}

fn tenderly_gas_limit(gas_used: u64) -> u64 {
    gas_used
        .saturating_mul(120)
        .saturating_div(100)
        .saturating_add(75_000)
        .max(500_000)
}

fn tenderly_balance_slot(
    token: Address,
    owner: Address,
    mapping: &TrackedMapping,
) -> Result<B256, String> {
    if mapping.contract != token {
        return Err(format!(
            "balance layout contract {} does not match input token {}",
            address_hex(mapping.contract),
            address_hex(token),
        ));
    }
    mapping.slot_for(owner.into_word()).ok_or_else(|| {
        format!(
            "unsupported balance layout for token {} owner {}",
            address_hex(token),
            address_hex(owner),
        )
    })
}

async fn submit_tenderly_simulation(
    mut request: TenderlySimulationRequest,
    provider: &RootProvider<AnyNetwork>,
) -> Result<TenderlySimulationOutcome, String> {
    let canonical_state_root =
        verify_canonical_tenderly_block(provider, request.block_number, request.block_hash).await?;
    let transaction_index = provider
        .get_block_transaction_count_by_hash(request.block_hash)
        .await
        .map_err(|error| {
            format!(
                "fetch transaction count for canonical block {}: {error}",
                request.block_number
            )
        })?
        .ok_or_else(|| {
            format!(
                "transaction count for canonical block {} is unavailable",
                request.block_number
            )
        })?;
    request
        .payload
        .as_object_mut()
        .ok_or_else(|| "Tenderly request payload is not an object".to_owned())?
        .insert(
            "transaction_index".to_owned(),
            Value::from(transaction_index),
        );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(45))
        .build()
        .map_err(|error| format!("build Tenderly client: {error}"))?;
    let response = client
        .post(&request.endpoint)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header("X-Access-Key", &request.api_key)
        .json(&request.payload)
        .send()
        .await
        .map_err(|error| format!("submit Tenderly simulation: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("read Tenderly response: {error}"))?;
    let value = serde_json::from_str::<Value>(&body).unwrap_or(Value::Null);

    let post_state_root =
        verify_canonical_tenderly_block(provider, request.block_number, request.block_hash).await?;
    if post_state_root != canonical_state_root {
        return Err(format!(
            "canonical state root changed while simulating block {}",
            request.block_number
        ));
    }

    if !status.is_success() {
        return Err(format!(
            "HTTP {status}: {}",
            tenderly_error_message(&value).unwrap_or_else(|| fit_to_width(&body, 180))
        ));
    }

    let url = tenderly_simulation_url(&request.dashboard_url, &value);
    if let Some(state_root) = tenderly_state_root(&value)
        && state_root != canonical_state_root
    {
        let _ = open_url(&url);
        return Err(format!(
            "Tenderly state root {state_root} does not match canonical state root {canonical_state_root}; url={url}"
        ));
    }
    let validation = match validate_tenderly_response(
        &value,
        request.block_number,
        transaction_index,
        request.expected_amount_out,
        request.output_tolerance_bps,
    ) {
        Ok(validation) => validation,
        Err(error) => {
            let _ = open_url(&url);
            return Err(format!("{error}; url={url}"));
        }
    };
    let _ = open_url(&url);
    Ok(TenderlySimulationOutcome {
        url,
        gas_used: validation.gas_used,
        block_number: request.block_number,
        transaction_index,
        amount_out: validation.amount_out,
        output_delta: validation.output_delta,
        output_symbol: request.output_symbol,
        output_decimals: request.output_decimals,
    })
}

async fn verify_canonical_tenderly_block(
    provider: &RootProvider<AnyNetwork>,
    block_number: u64,
    expected_hash: B256,
) -> Result<B256, String> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .await
        .map_err(|error| format!("fetch canonical block {block_number}: {error}"))?
        .ok_or_else(|| format!("canonical block {block_number} is unavailable"))?;
    let header = block.header();
    if header.hash != expected_hash {
        return Err(format!(
            "snapshot block hash {} no longer matches canonical block {} hash {}",
            expected_hash, block_number, header.hash,
        ));
    }
    Ok(header.inner.state_root)
}

fn validate_tenderly_response(
    value: &Value,
    expected_block_number: u64,
    expected_transaction_index: u64,
    expected_amount_out: U256,
    tolerance_bps: u16,
) -> Result<TenderlyValidation, String> {
    match tenderly_success(value) {
        Some(true) => {}
        Some(false) => {
            let reason = tenderly_error_message(value)
                .unwrap_or_else(|| "execution reverted without an error message".to_owned());
            return Err(format!("Tenderly execution reverted: {reason}"));
        }
        None => return Err("Tenderly response omitted execution status".to_owned()),
    }

    let block_number = tenderly_block_number(value)
        .ok_or_else(|| "Tenderly response omitted simulation block number".to_owned())?;
    if block_number != expected_block_number {
        return Err(format!(
            "Tenderly used block {block_number}, expected {expected_block_number}"
        ));
    }

    let transaction_index = tenderly_transaction_index(value)
        .ok_or_else(|| "Tenderly response omitted simulation transaction index".to_owned())?;
    if transaction_index != expected_transaction_index {
        return Err(format!(
            "Tenderly used transaction index {transaction_index}, expected end-of-block index {expected_transaction_index}"
        ));
    }

    let amount_out = tenderly_amount_out(value)?;
    let output_delta = amount_out.abs_diff(expected_amount_out);
    let tolerance = expected_amount_out
        .checked_mul(U256::from(tolerance_bps))
        .unwrap_or(U256::MAX)
        / U256::from(10_000);
    if output_delta > tolerance {
        return Err(format!(
            "Tenderly output mismatch: expected {expected_amount_out}, got {amount_out}, delta {output_delta} exceeds {tolerance_bps} bps tolerance"
        ));
    }

    Ok(TenderlyValidation {
        gas_used: tenderly_gas_used(value),
        amount_out,
        output_delta,
    })
}

fn tenderly_amount_out(value: &Value) -> Result<U256, String> {
    let raw = [
        "/transaction/transaction_info/call_trace/output",
        "/transaction/call_trace/output",
        "/simulation/transaction/transaction_info/call_trace/output",
    ]
    .into_iter()
    .find_map(|path| value.pointer(path).and_then(Value::as_str));
    if let Some(raw) = raw {
        let encoded = raw.strip_prefix("0x").unwrap_or(raw);
        let bytes =
            hex::decode(encoded).map_err(|error| format!("decode Tenderly amountOut: {error}"))?;
        if bytes.len() != 32 {
            return Err(format!(
                "Tenderly amountOut must be one ABI word, received {} bytes",
                bytes.len()
            ));
        }
        return Ok(U256::from_be_slice(&bytes));
    }

    let decoded = value
        .pointer("/transaction/transaction_info/call_trace/decoded_output/0/value")
        .and_then(Value::as_str)
        .ok_or_else(|| "Tenderly response omitted the router amountOut".to_owned())?;
    parse_config_u256(decoded).map_err(|error| format!("decode Tenderly amountOut: {error:#}"))
}

fn tenderly_block_number(value: &Value) -> Option<u64> {
    value
        .pointer("/simulation/block_number")
        .or_else(|| value.pointer("/transaction/block_number"))
        .and_then(json_u64)
}

fn tenderly_transaction_index(value: &Value) -> Option<u64> {
    value
        .pointer("/simulation/transaction_index")
        .or_else(|| value.pointer("/transaction/index"))
        .and_then(json_u64)
}

fn tenderly_state_root(value: &Value) -> Option<B256> {
    [
        "/simulation/block_header/stateRoot",
        "/simulation/block_header/state_root",
    ]
    .into_iter()
    .find_map(|path| value.pointer(path).and_then(Value::as_str))
    .and_then(|root| B256::from_str(root).ok())
}

fn tenderly_error_message(value: &Value) -> Option<String> {
    value
        .pointer("/error/message")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/message").and_then(Value::as_str))
        .or_else(|| {
            value
                .pointer("/transaction/error_message")
                .and_then(Value::as_str)
        })
        .or_else(|| {
            value
                .pointer("/simulation/error_message")
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned)
}

fn tenderly_simulation_url(base_url: &str, value: &Value) -> String {
    value
        .pointer("/simulation/id")
        .and_then(Value::as_str)
        .map(|id| format!("{base_url}/{id}"))
        .unwrap_or_else(|| base_url.to_owned())
}

fn tenderly_gas_used(value: &Value) -> Option<u64> {
    value
        .pointer("/transaction/gas_used")
        .or_else(|| value.pointer("/simulation/gas_used"))
        .and_then(json_u64)
}

fn tenderly_success(value: &Value) -> Option<bool> {
    value
        .pointer("/transaction/status")
        .or_else(|| value.pointer("/simulation/status"))
        .and_then(Value::as_bool)
}

fn json_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        let raw = value.as_str()?;
        if let Some(hex) = raw.strip_prefix("0x") {
            u64::from_str_radix(hex, 16).ok()
        } else {
            raw.parse::<u64>().ok()
        }
    })
}

fn open_url(url: &str) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        command.arg(url);
        command
    };
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(url);
        command
    };
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command.args(["/C", "start", "", url]);
        command
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    let mut command = {
        let mut command = Command::new("open");
        command.arg(url);
        command
    };

    command.spawn()?.wait()?;
    Ok(())
}

fn rebuild_quote_price_labels(app: &mut AppState, sync: &AmmSyncEngine) {
    let (Some(active), Some(existing_quote)) = (&app.route_session, &app.quote) else {
        return;
    };
    let Some(best) = active.session.best().cloned() else {
        return;
    };
    let graph_report = AmmGraph::from_registry(sync.registry(), GraphBuildOptions::default());
    let token_out = app.token_out();
    let stream_lines = existing_quote.stream_lines.clone();
    let gas = existing_quote
        .gas
        .as_ref()
        .and_then(|gas| gas.estimate.as_ref())
        .map(|estimate| gas_estimate_view(estimate, &app.prices))
        .or_else(|| existing_quote.gas.clone());
    let stats = QuoteViewStats {
        routes: existing_quote.routes,
        graph_pools: graph_report.indexed_pools.len(),
        elapsed: existing_quote.elapsed,
        block_number: existing_quote.block_number,
    };
    app.quote = Some(quote_view(
        &graph_report.graph,
        &best,
        app,
        &token_out,
        gas,
        stream_lines,
        stats,
    ));
}

fn prioritized_price_tokens(app: &AppState) -> Vec<TokenInfo> {
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();
    push_price_token(&mut tokens, &mut seen, app.token_in());
    push_price_token(&mut tokens, &mut seen, app.token_out());

    if let Some(quote) = &app.quote {
        for token in &quote.route.tokens {
            push_price_token(
                &mut tokens,
                &mut seen,
                token_by_address(&app.tokens, token.address),
            );
        }
    }

    for token in &app.tokens {
        push_price_token(&mut tokens, &mut seen, token.clone());
    }

    tokens
}

fn push_price_token(tokens: &mut Vec<TokenInfo>, seen: &mut HashSet<Address>, token: TokenInfo) {
    if seen.insert(token.address) {
        tokens.push(token);
    }
}

fn price_refresh_enabled() -> bool {
    env_bool("AMM_ROUTE_TUI_PRICES", true)
        && !matches!(price_source().as_str(), "none" | "off" | "disabled")
}

fn price_refresh_interval() -> Duration {
    let default = if coingecko_api_key().is_some() {
        DEFAULT_KEYED_PRICE_REFRESH_SECS
    } else {
        DEFAULT_KEYLESS_PRICE_REFRESH_SECS
    };
    Duration::from_secs(env_usize("AMM_ROUTE_TUI_PRICE_REFRESH_SECS", default).max(1) as u64)
}

fn set_route_work(
    app: &mut AppState,
    phase: RouteWorkPhase,
    detail: impl Into<String>,
    progress: Option<(usize, usize)>,
) {
    app.route_work.phase = phase;
    app.route_work.detail = detail.into();
    app.route_work.progress = progress;
    app.route_work.started_at = match phase {
        RouteWorkPhase::Ready | RouteWorkPhase::Error => None,
        RouteWorkPhase::Discovering | RouteWorkPhase::Quoting => Some(Instant::now()),
    };
}

fn begin_route_quoting(app: &mut AppState, refresh: &QuoteRefresh) {
    let detail = match refresh {
        QuoteRefresh::Full => format!(
            "quoting {} -> {}",
            app.token_in().symbol,
            app.token_out().symbol
        ),
        QuoteRefresh::Incremental(affected) => {
            format!("requoting {} affected pool(s)", affected.pools().len())
        }
        QuoteRefresh::None => "ready".to_owned(),
    };
    set_route_work(app, RouteWorkPhase::Quoting, detail, None);
}

async fn run_full_quote_refresh(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut AppState,
    sync: &mut AmmSyncEngine,
    cache: &mut EvmCache,
    sim_config: &SimConfig,
) -> Result<()> {
    app.quote_loading = true;
    let token_in_symbol = app.token_in().symbol;
    let token_out_symbol = app.token_out().symbol;
    set_route_work(
        app,
        RouteWorkPhase::Quoting,
        format!("quoting {} -> {}", token_in_symbol, token_out_symbol),
        None,
    );
    app.status = format!(
        "quoting warmed route data for {} -> {}...",
        token_in_symbol, token_out_symbol
    );
    terminal.draw(|frame| draw(frame, app))?;
    if refresh_quote(app, sync, cache, sim_config, QuoteRefresh::Full) {
        terminal.draw(|frame| draw(frame, app))?;
        refresh_displayed_route_gas(app, sync, cache);
    }
    Ok(())
}

fn schedule_quote_refresh(app: &mut AppState, pending_quote_refresh: &mut Option<Instant>) {
    app.quote = None;
    app.quote_error = None;
    app.quote_loading = true;
    app.route_session = None;
    let token_in_symbol = app.token_in().symbol;
    let token_out_symbol = app.token_out().symbol;
    set_route_work(
        app,
        RouteWorkPhase::Quoting,
        format!("queued {} -> {}", token_in_symbol, token_out_symbol),
        None,
    );
    app.status = format!(
        "queued route refresh for {} -> {}",
        token_in_symbol, token_out_symbol
    );
    *pending_quote_refresh = Some(Instant::now() + QUOTE_REFRESH_DEBOUNCE);
}

fn handle_ui_event(app: &mut AppState, event: UiEvent) -> UiAction {
    let UiEvent::Key(key) = event;
    if app.active == ActiveField::TokenAddress {
        return handle_custom_token_event(app, key);
    }
    if app.active == ActiveField::TokenSearch {
        return handle_token_search_event(app, key);
    }
    if app.amount_editing {
        return handle_amount_edit_event(app, key);
    }

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => return UiAction::Quit,
        KeyCode::Tab => app.select_next_field(),
        KeyCode::BackTab => app.select_prev_field(),
        KeyCode::Left => app.select_prev_field(),
        KeyCode::Right => app.select_next_field(),
        KeyCode::Up => app.select_prev_field(),
        KeyCode::Down => app.select_next_field(),
        KeyCode::Enter if matches!(app.active, ActiveField::Input | ActiveField::Output) => {
            app.begin_token_search(active_token_side(app.active), None);
        }
        KeyCode::Enter if app.active == ActiveField::Amount => app.begin_amount_edit(),
        KeyCode::Char('i') => app.active = ActiveField::Input,
        KeyCode::Char('o') => app.active = ActiveField::Output,
        KeyCode::Char('a') => app.active = ActiveField::Amount,
        KeyCode::Char('n') => app.begin_custom_token(),
        KeyCode::Char('t') => return UiAction::SimulateSwap,
        KeyCode::Char('r') => return UiAction::RequestQuote,
        _ => {}
    }
    UiAction::Continue
}

fn handle_amount_edit_event(app: &mut AppState, key: KeyEvent) -> UiAction {
    match key.code {
        KeyCode::Esc | KeyCode::Enter => {
            app.finish_amount_edit();
            UiAction::RequestQuote
        }
        KeyCode::Tab | KeyCode::Right | KeyCode::Down => {
            app.finish_amount_edit();
            app.select_next_field();
            UiAction::RequestQuote
        }
        KeyCode::BackTab | KeyCode::Left | KeyCode::Up => {
            app.finish_amount_edit();
            app.select_prev_field();
            UiAction::RequestQuote
        }
        KeyCode::Char('q') => UiAction::Quit,
        KeyCode::Char(c) if c.is_ascii_digit() || (c == '.' && !app.amount.contains('.')) => {
            app.amount.push(c);
            UiAction::RequestQuote
        }
        KeyCode::Backspace => {
            app.amount.pop();
            UiAction::RequestQuote
        }
        _ => UiAction::Continue,
    }
}

fn handle_token_search_event(app: &mut AppState, key: KeyEvent) -> UiAction {
    match key.code {
        KeyCode::Esc => {
            app.cancel_token_search();
            UiAction::Continue
        }
        KeyCode::Tab | KeyCode::Right => {
            app.cancel_token_search();
            app.select_next_field();
            UiAction::Continue
        }
        KeyCode::BackTab | KeyCode::Left => {
            app.cancel_token_search();
            app.select_prev_field();
            UiAction::Continue
        }
        KeyCode::Up => {
            app.move_token_search_selection(-1);
            UiAction::Continue
        }
        KeyCode::Down => {
            app.move_token_search_selection(1);
            UiAction::Continue
        }
        KeyCode::Backspace => {
            app.pop_token_search_char();
            UiAction::Continue
        }
        KeyCode::Enter => commit_token_search(app),
        KeyCode::Char(c) if is_token_search_char(c) => {
            app.append_token_search_char(c);
            UiAction::Continue
        }
        _ => UiAction::Continue,
    }
}

fn commit_token_search(app: &mut AppState) -> UiAction {
    let Some(search) = &app.token_search else {
        return UiAction::Continue;
    };
    let side = search.side;
    let query = search.query.trim().to_owned();
    let matches = token_search_matches(&app.tokens, &query);
    if let Some(index) = matches.get(search.selected.min(matches.len().saturating_sub(1))) {
        app.select_token_index(*index, side);
        return UiAction::SelectToken { side };
    }
    if !query.is_empty() {
        app.status = format!("looking up routes for {query}");
        app.token_search = None;
        app.active = side.active_field();
        return UiAction::DiscoverToken { side, query };
    }
    app.status = format!("no token matched \"{query}\"");
    UiAction::Continue
}

fn handle_custom_token_event(app: &mut AppState, key: KeyEvent) -> UiAction {
    match key.code {
        KeyCode::Esc => {
            app.cancel_custom_token();
            UiAction::Continue
        }
        KeyCode::Enter => {
            let query = app.custom_address.trim().to_owned();
            if query.is_empty() {
                app.status = "enter a token address or symbol before pressing Enter".to_owned();
                UiAction::Continue
            } else {
                app.status = format!("looking up routes for {query}");
                UiAction::DiscoverToken {
                    side: app.custom_side,
                    query,
                }
            }
        }
        KeyCode::Backspace => {
            app.custom_address.pop();
            UiAction::Continue
        }
        KeyCode::Char(c) => {
            if is_token_lookup_char(c) {
                app.custom_address.push(c);
            }
            UiAction::Continue
        }
        _ => UiAction::Continue,
    }
}

fn handle_chain_event(
    app: &mut AppState,
    sync: &mut AmmSyncEngine,
    cache: &mut EvmCache,
    event: ChainEvent,
) -> ChainProcessResult {
    match event {
        ChainEvent::Log(log) => {
            let block_number = log.block_number.unwrap_or(app.last_block);
            let affected = AffectedPools::from_rpc_logs(sync.registry(), [&log]);
            let should_refresh = !affected.pools().is_empty() || affected.removed_logs() > 0;
            let ctx = ctx_from_log(&log, cache.chain_id());
            let batch = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
                ReactiveInput::Log(log),
                ctx,
            )]);
            match sync.ingest_batch(cache, batch) {
                Ok(report) => {
                    let routed = !report.reactive.applied.is_empty()
                        || report.resync_state_updates > 0
                        || report.resync_failures > 0;
                    if routed {
                        app.routed_logs += 1;
                    } else {
                        app.ignored_logs += 1;
                    }
                    if !report.reactive.applied.is_empty() {
                        app.applied_logs += report.reactive.applied.len() as u64;
                    }
                    app.resync_updates += report.resync_state_updates as u64;
                    app.resync_failures += report.resync_failures as u64;
                    app.degraded_pools += report.degraded_pools.len() as u64;
                    app.recovered_pools += report.recovered_pools.len() as u64;
                    app.last_block = block_number.max(app.last_block);
                    app.status = format!("processed log at block {block_number}");
                    let refresh = if should_refresh {
                        QuoteRefresh::Incremental(affected)
                    } else {
                        QuoteRefresh::None
                    };
                    ChainProcessResult {
                        refresh,
                        phase: ChainSyncPhase::Synced,
                        detail: format!("synced 1 log from block {block_number}"),
                    }
                }
                Err(error) => {
                    app.status = format!("sync error: {error}");
                    ChainProcessResult {
                        refresh: QuoteRefresh::None,
                        phase: ChainSyncPhase::Degraded,
                        detail: format!("sync error at block {block_number}"),
                    }
                }
            }
        }
        ChainEvent::Block(header) => {
            let block_number = header.number();
            match cache.advance_block(header.as_ref()) {
                Ok(()) => {
                    app.last_block = block_number;
                    app.status = format!("new block {block_number}");
                    ChainProcessResult {
                        refresh: QuoteRefresh::None,
                        phase: ChainSyncPhase::Synced,
                        detail: format!("synced block {block_number}"),
                    }
                }
                Err(error) => {
                    app.last_block = block_number;
                    app.status = format!("block sync error at {block_number}: {error}");
                    ChainProcessResult {
                        refresh: QuoteRefresh::None,
                        phase: ChainSyncPhase::Degraded,
                        detail: format!("block sync error at {block_number}"),
                    }
                }
            }
        }
        ChainEvent::Error(error) => {
            app.status = error;
            ChainProcessResult {
                refresh: QuoteRefresh::None,
                phase: ChainSyncPhase::Degraded,
                detail: "stream error".to_owned(),
            }
        }
    }
}

fn refresh_quote(
    app: &mut AppState,
    sync: &AmmSyncEngine,
    cache: &mut EvmCache,
    sim_config: &SimConfig,
    refresh: QuoteRefresh,
) -> bool {
    app.quote_updates += 1;
    app.quote_loading = false;
    match quote_current(app, sync, cache, sim_config, refresh) {
        Ok(view) => {
            set_route_work(
                app,
                RouteWorkPhase::Ready,
                format!("ready {} route(s)", view.routes),
                None,
            );
            app.quote = Some(view);
            app.quote_error = None;
            true
        }
        Err(error) => {
            set_route_work(app, RouteWorkPhase::Error, fit_to_width(&error, 48), None);
            app.quote = None;
            app.quote_error = Some(error);
            false
        }
    }
}

fn active_token_side(active: ActiveField) -> Side {
    match active {
        ActiveField::Output => Side::Output,
        _ => Side::Input,
    }
}

fn is_token_search_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, 'x' | 'X')
}

fn is_token_lookup_char(c: char) -> bool {
    !c.is_control() && !c.is_whitespace()
}

fn token_search_matches(tokens: &[TokenInfo], query: &str) -> Vec<usize> {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return (0..tokens.len()).collect();
    }
    let mut matches = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            let symbol = token.symbol.to_ascii_lowercase();
            let address = format!("{:#x}", token.address).to_ascii_lowercase();
            let rank = if symbol == query {
                Some(0)
            } else if symbol.starts_with(&query) {
                Some(1)
            } else if address.starts_with(&query) {
                Some(2)
            } else if symbol.contains(&query) {
                Some(3)
            } else if address.contains(&query) {
                Some(4)
            } else {
                None
            }?;
            Some((rank, symbol.len(), index))
        })
        .collect::<Vec<_>>();
    matches.sort_unstable();
    matches.into_iter().map(|(_, _, index)| index).collect()
}

fn quote_current(
    app: &mut AppState,
    sync: &AmmSyncEngine,
    cache: &mut EvmCache,
    sim_config: &SimConfig,
    refresh: QuoteRefresh,
) -> Result<QuoteView, String> {
    let token_in = app.token_in();
    let token_out = app.token_out();
    if token_in.address == token_out.address {
        return Err("input and output token must differ".to_owned());
    }
    let amount_in = parse_units(&app.amount, token_in.decimals)?;

    let graph_report = AmmGraph::from_registry(sync.registry(), GraphBuildOptions::default());
    let mut searcher = AmmSearcher::new(sync.registry(), &graph_report.graph);
    if let Some(liquidity_index) = app.liquidity_index.as_ref() {
        searcher = searcher.with_liquidity_index(liquidity_index);
    }
    let request = RouteRequest::new(token_in.address, token_out.address, amount_in)
        .with_config(tui_search_config(app))
        .with_sim_config(*sim_config);

    let started = Instant::now();
    let streaming_config = tui_streaming_config();
    let mut stream_lines = app
        .quote
        .as_ref()
        .map(|quote| quote.stream_lines.clone())
        .unwrap_or_default();
    let session_matches = app.route_session.as_ref().is_some_and(|active| {
        active.topology_updates == app.topology_updates
            && active.session.request() == &request
            && active.session.streaming_config() == streaming_config
    });

    if !session_matches {
        app.route_session = None;
    }

    if let QuoteRefresh::Incremental(affected) = refresh
        && app.route_session.is_some()
    {
        let report = {
            let active = app
                .route_session
                .as_mut()
                .expect("checked active route session");
            active
                .session
                .refresh_affected(&searcher, cache, affected, |event| {
                    push_route_update_line(&mut stream_lines, &event, &token_out, &app.prices);
                    SearchControl::Continue
                })
        };

        if report.status != IncrementalRouteUpdateStatus::RecomputeRequired {
            let (best, route_count) =
                {
                    let active = app
                        .route_session
                        .as_ref()
                        .expect("checked active route session");
                    (
                        active.session.best().cloned().ok_or_else(|| {
                            "no route returned after incremental refresh".to_owned()
                        })?,
                        active.session.materialized_route_count(),
                    )
                };
            let elapsed = started.elapsed();
            app.status = format!(
                "incremental quote refresh: {:?}, requoted {}, probes {}",
                report.status, report.routes_requoted, report.probe_routes_quoted
            );
            return Ok(quote_view(
                &graph_report.graph,
                &best,
                app,
                &token_out,
                None,
                stream_lines,
                QuoteViewStats {
                    routes: route_count,
                    graph_pools: graph_report.indexed_pools.len(),
                    elapsed,
                    block_number: app.last_block,
                },
            ));
        }

        if let Some(reason) = report.recompute_reason {
            push_stream_line(
                &mut stream_lines,
                format!("incremental fallback: {reason:?}; rebuilding session"),
            );
        }
        app.route_session = None;
    }

    let mut routes_observed = 0_usize;
    let session = searcher
        .start_route_session(&request, cache, streaming_config, |event| {
            if let RouteSearchEvent::Completed { report } = &event {
                routes_observed = report.routes_observed;
            }
            push_route_search_line(&mut stream_lines, &event, &token_out, &app.prices);
            SearchControl::Continue
        })
        .map_err(search_error_message)?;
    let elapsed = started.elapsed();
    let best = session
        .best()
        .cloned()
        .ok_or_else(|| "no route returned".to_owned())?;
    routes_observed = routes_observed.max(session.materialized_route_count());
    app.status = format!(
        "route session refreshed: {} tracked route(s), {} probe(s)",
        session.materialized_route_count(),
        session.parallel_probe_count()
    );
    app.route_session = Some(ActiveRouteSession {
        session,
        topology_updates: app.topology_updates,
    });

    Ok(quote_view(
        &graph_report.graph,
        &best,
        app,
        &token_out,
        None,
        stream_lines,
        QuoteViewStats {
            routes: routes_observed,
            graph_pools: graph_report.indexed_pools.len(),
            elapsed,
            block_number: app.last_block,
        },
    ))
}

fn tui_streaming_config() -> StreamingSearchConfig {
    let parallel = match env_usize("AMM_ROUTE_TUI_SEARCH_WORKERS", 0) {
        0 => ParallelSearchConfig::default(),
        workers => ParallelSearchConfig::default().with_workers(workers),
    };
    let mut config = StreamingSearchConfig::default()
        .heuristic_only()
        .with_top_k(16)
        .with_parallel(parallel);
    if env_bool("AMM_ROUTE_TUI_EXHAUSTIVE_SEARCH", false) {
        config = config.exhaustive();
    }
    if let Some(confidence_bps) = env_bps("AMM_ROUTE_TUI_STOP_CONFIDENCE_BPS") {
        config = config.stop_at_confidence_bps(confidence_bps);
    }
    if let Some(fraction_bps) = env_bps("AMM_ROUTE_TUI_STOP_EXHAUSTIVE_FRACTION_BPS") {
        config = config.stop_at_exhaustive_fraction_bps(fraction_bps);
    }
    if let Some(confidence_bps) = env_bps("AMM_ROUTE_TUI_INITIAL_RESULT_CONFIDENCE_BPS") {
        config = config.emit_initial_results_at_confidence_bps(confidence_bps);
    }
    if let Some(fraction_bps) = env_bps("AMM_ROUTE_TUI_INITIAL_RESULT_EXHAUSTIVE_FRACTION_BPS") {
        config = config.emit_initial_results_at_exhaustive_fraction_bps(fraction_bps);
    }
    config
}

fn tui_search_config(app: &AppState) -> SearchConfig {
    tui_search_config_for_tokens(&app.tokens)
}

fn tui_search_config_for_tokens(tokens: &[TokenInfo]) -> SearchConfig {
    let heuristic = HeuristicSearchConfig::balanced()
        .with_auto_connectors(8, 4)
        .with_parallel_edge_limit(4)
        .with_fast_lane(
            FastLaneConfig::enabled()
                .with_direct_edges_per_pair(4)
                .with_connector_edges_per_pair(2),
        )
        .with_finalist_simulation(true, 16);
    SearchConfig::default()
        .with_hops(1, 3)
        .with_connector_tokens(tui_connector_tokens(tokens))
        .with_mode(SearchMode::Heuristic(heuristic))
        .with_liquidity_pruning(LiquidityPruningConfig::enabled())
}

fn tui_connector_tokens(tokens: &[TokenInfo]) -> Vec<Address> {
    let mut connectors = Vec::new();
    let mut seen = HashSet::new();
    for token in [WETH, USDC, USDT, DAI, WBTC] {
        if seen.insert(token) {
            connectors.push(token);
        }
    }
    for token in tokens {
        if seen.insert(token.address) {
            connectors.push(token.address);
        }
    }
    connectors
}

fn refresh_displayed_route_gas(app: &mut AppState, sync: &AmmSyncEngine, cache: &mut EvmCache) {
    let Some(best) = app.quote.as_ref().map(|quote| quote.best.clone()) else {
        return;
    };
    let mut stream_lines = app
        .quote
        .as_ref()
        .map(|quote| quote.stream_lines.clone())
        .unwrap_or_default();
    let gas = estimate_route_gas(app, sync, cache, &best, &mut stream_lines);
    if let Some(quote) = &mut app.quote {
        quote.stream_lines = stream_lines;
        quote.gas = gas;
    }
}

fn estimate_live_route_gas(
    app: &AppState,
    view: &evm_amm_search::LiveSearchView,
    best: &VersionedRouteQuote,
    stream_lines: &mut Vec<String>,
) -> Option<GasEstimateView> {
    if !gas_estimates_enabled() {
        return None;
    }
    if !app.gas_router_ready {
        return Some(GasEstimateView {
            summary: "GAS  unavailable".to_owned(),
            detail: fit_to_width(&app.gas_router_status, 120),
            gas_used: None,
            ok: false,
            estimate: None,
        });
    }
    let token_in = best.quote().hops.first().map(|hop| hop.hop.token_in);
    if token_in.is_none_or(|token| !app.gas_balance_mappings.contains_key(&token)) {
        return Some(GasEstimateView {
            summary: "GAS  benchmark unavailable for input token".to_owned(),
            detail: "the input token balance layout was not available in the immutable snapshot"
                .to_owned(),
            gas_used: None,
            ok: false,
            estimate: None,
        });
    }

    match simulate_versioned_route_gas_with_balance_mappings(
        view,
        best,
        DemoRouterConfig::default(),
        app.gas_price_wei,
        &app.gas_balance_mappings,
    ) {
        Ok(estimate) => Some(gas_estimate_view(&estimate, &app.prices)),
        Err(error) => {
            let line = format!("snapshot gas benchmark unavailable: {error:#}");
            push_stream_line(stream_lines, line.clone());
            Some(GasEstimateView {
                summary: "GAS  benchmark unavailable for selected route".to_owned(),
                detail: fit_to_width(&line, 120),
                gas_used: None,
                ok: false,
                estimate: None,
            })
        }
    }
}

fn estimate_route_gas(
    app: &AppState,
    sync: &AmmSyncEngine,
    cache: &mut EvmCache,
    best: &RouteQuote,
    stream_lines: &mut Vec<String>,
) -> Option<GasEstimateView> {
    if !gas_estimates_enabled() {
        return None;
    }
    if !app.gas_router_ready {
        return Some(GasEstimateView {
            summary: "GAS  unavailable".to_owned(),
            detail: fit_to_width(&app.gas_router_status, 120),
            gas_used: None,
            ok: false,
            estimate: None,
        });
    }

    match simulate_route_gas(
        sync.registry(),
        cache,
        best,
        DemoRouterConfig::default(),
        app.gas_price_wei,
    ) {
        Ok(estimate) => Some(gas_estimate_view(&estimate, &app.prices)),
        Err(error) => {
            let line = format!("gas estimate unavailable: {error:#}");
            push_stream_line(stream_lines, line.clone());
            Some(GasEstimateView {
                summary: "GAS  estimate unavailable for selected route".to_owned(),
                detail: fit_to_width(&line, 120),
                gas_used: None,
                ok: false,
                estimate: None,
            })
        }
    }
}

fn gas_estimate_view(estimate: &SwapGasEstimate, prices: &PriceBook) -> GasEstimateView {
    let gas_price = estimate
        .gas_price_wei
        .map(format_gwei)
        .unwrap_or_else(|| "?".to_owned());
    let native = estimate
        .gas_cost_native
        .map(|cost| format!("{} ETH", format_units(cost, 18, 6)))
        .unwrap_or_else(|| "? ETH".to_owned());
    let usd = estimate
        .gas_cost_native
        .and_then(|cost| {
            prices
                .usd_by_token
                .get(&WETH)
                .copied()
                .and_then(|price| amount_usd(cost, 18, price))
        })
        .map(|usd| format!(" ({})", format_usd(usd)))
        .unwrap_or_default();
    GasEstimateView {
        summary: format!(
            "GAS  {} gas @ {} gwei = {}{}",
            comma_digits(&estimate.gas_used.to_string()),
            gas_price,
            native,
            usd
        ),
        detail: format!(
            "gas sim output={} latency={:.2?}",
            estimate.gross_amount_out, estimate.latency
        ),
        gas_used: Some(estimate.gas_used),
        ok: true,
        estimate: Some(estimate.clone()),
    }
}

fn quote_view(
    graph: &AmmGraph,
    best: &RouteQuote,
    app: &AppState,
    token_out: &TokenInfo,
    gas: Option<GasEstimateView>,
    stream_lines: Vec<String>,
    stats: QuoteViewStats,
) -> QuoteView {
    let token_in = app.token_in();
    let max_loss_bps = env_usize(
        "AMM_ROUTE_TUI_MAX_VALUE_LOSS_BPS",
        DEFAULT_MAX_VALUE_LOSS_BPS as usize,
    )
    .min(10_000) as u16;
    let warnings = economic_value_warning(
        best.amount_in,
        &token_in,
        best.amount_out,
        token_out,
        &app.prices,
        max_loss_bps,
    )
    .into_iter()
    .collect();
    QuoteView {
        best: best.clone(),
        block_number: stats.block_number,
        live_tenderly: None,
        output: format_amount_with_token(best.amount_out, token_out, &app.prices, 8),
        gas,
        route: build_route_viz(graph, best, &app.tokens, &app.pools, &app.prices),
        quoted_routes: Vec::new(),
        quoted_venues: String::new(),
        warnings,
        coverage: graph_coverage_summary(graph, &token_in, token_out, stats.graph_pools),
        stream_lines,
        routes: stats.routes,
        graph_pools: stats.graph_pools,
        elapsed: stats.elapsed,
    }
}

fn push_route_search_line(
    lines: &mut Vec<String>,
    event: &RouteSearchEvent,
    token_out: &TokenInfo,
    prices: &PriceBook,
) {
    match event {
        RouteSearchEvent::Started { completion } => {
            push_stream_line(lines, format!("stream started: completion={completion:?}"));
        }
        RouteSearchEvent::RouteFound { phase, rank, quote } => {
            if let Some(rank) = rank {
                push_stream_line(
                    lines,
                    format!(
                        "top #{rank} from {}: {}",
                        phase_label(*phase),
                        format_amount_with_token(quote.amount_out, token_out, prices, 6),
                    ),
                );
            }
        }
        RouteSearchEvent::BestUpdated { phase, quote, .. } => {
            push_stream_line(
                lines,
                format!(
                    "best updated from {}: {}",
                    phase_label(*phase),
                    format_amount_with_token(quote.amount_out, token_out, prices, 6),
                ),
            );
        }
        RouteSearchEvent::Progress { progress } => {
            let fraction = progress
                .exhaustive_fraction_bps
                .map(format_bps)
                .unwrap_or_else(|| "pending".to_owned());
            push_stream_line(
                lines,
                format!(
                    "progress {}: evaluated={} viable={} failed={} confidence={} exhaustive={}",
                    progress.phase.map(phase_label).unwrap_or("startup"),
                    progress.candidates_evaluated,
                    progress.viable_routes_observed,
                    progress.failed_candidates,
                    format_bps(progress.confidence_bps),
                    fraction
                ),
            );
        }
        RouteSearchEvent::InitialResultsReady {
            progress,
            best,
            top_routes,
        } => {
            push_stream_line(
                lines,
                format!(
                    "initial results ready: best={} confidence={} top_routes={}",
                    format_amount_with_token(best.amount_out, token_out, prices, 6),
                    format_bps(progress.confidence_bps),
                    top_routes.len()
                ),
            );
        }
        RouteSearchEvent::PhaseCompleted { phase, stats } => {
            push_stream_line(
                lines,
                format!(
                    "{} phase done: viable={} dup_skipped={} quote_hits={}",
                    phase_label(*phase),
                    stats.routes_observed,
                    stats.duplicate_paths_skipped,
                    stats.quote_cache.hits
                ),
            );
        }
        RouteSearchEvent::Completed { report } => {
            push_stream_line(
                lines,
                format!(
                    "stream complete: finality={} viable={} exhaustive_improvements={}",
                    finality_label(report.finality),
                    report.routes_observed,
                    report.improvements_after_heuristic
                ),
            );
        }
    }
}

fn push_route_update_line(
    lines: &mut Vec<String>,
    event: &RouteUpdateEvent,
    token_out: &TokenInfo,
    prices: &PriceBook,
) {
    match event {
        RouteUpdateEvent::Started { affected_pools } => {
            push_stream_line(
                lines,
                format!(
                    "incremental started: affected_pool_count={}",
                    affected_pools.len()
                ),
            );
        }
        RouteUpdateEvent::RouteRequoted { quote } => {
            push_stream_line(
                lines,
                format!(
                    "requoted route: {}",
                    format_amount_with_token(quote.amount_out, token_out, prices, 6),
                ),
            );
        }
        RouteUpdateEvent::ProbeRouteFound { quote } => {
            push_stream_line(
                lines,
                format!(
                    "probe route: {}",
                    format_amount_with_token(quote.amount_out, token_out, prices, 6),
                ),
            );
        }
        RouteUpdateEvent::BestChanged { best, .. } => {
            let output = best
                .as_ref()
                .map(|quote| format_amount_with_token(quote.amount_out, token_out, prices, 6))
                .unwrap_or_else(|| "no viable route".to_owned());
            push_stream_line(lines, format!("incremental best changed: {output}"));
        }
        RouteUpdateEvent::RecomputeRequired { reason } => {
            push_stream_line(lines, format!("incremental recompute required: {reason:?}"));
        }
        RouteUpdateEvent::Completed { report } => {
            push_stream_line(
                lines,
                format!(
                    "incremental complete: status={:?} requoted={} probes={} quote_hits={}",
                    report.status,
                    report.routes_requoted,
                    report.probe_routes_quoted,
                    report.quote_cache.hits
                ),
            );
        }
    }
}

fn search_error_message(error: SearchError) -> String {
    match error {
        SearchError::NoViableRoute {
            candidates,
            failures,
        } => {
            let first = failures
                .first()
                .map(|failure| failure.reason.as_str())
                .unwrap_or("all candidates failed");
            format!("no viable route among {candidates} candidate(s): {first}")
        }
        other => other.to_string(),
    }
}

fn push_stream_line(lines: &mut Vec<String>, line: String) {
    lines.push(line);
    const MAX_STREAM_LINES: usize = 8;
    if lines.len() > MAX_STREAM_LINES {
        lines.remove(0);
    }
}

fn phase_label(phase: RouteSearchPhase) -> &'static str {
    match phase {
        RouteSearchPhase::Heuristic => "heuristic",
        RouteSearchPhase::Exhaustive => "exhaustive",
    }
}

fn finality_label(finality: SearchFinality) -> &'static str {
    match finality {
        SearchFinality::FastLaneOnly => "fast-lane-only",
        SearchFinality::HeuristicOnly => "heuristic-only",
        SearchFinality::Exhaustive => "exhaustive",
        SearchFinality::StopPolicySatisfied => "stop-policy",
        SearchFinality::Stopped => "stopped",
    }
}

async fn discover_custom_token(
    app: &mut AppState,
    sync: &mut AmmSyncEngine,
    cache: &mut EvmCache,
    provider: &RootProvider<AnyNetwork>,
    side: Side,
    raw_query: String,
) -> Result<()> {
    let token = resolve_token_selector(provider, &app.tokens, &raw_query).await?;
    let address = token.address;
    let existing_index = app.tokens.iter().position(|known| known.address == address);
    let symbol = token.symbol.clone();
    app.custom_address.clear();
    app.active = side.active_field();

    let connectors = connector_addresses(&app.tokens, address);
    if connectors.is_empty() {
        if let Some(index) = existing_index {
            app.select_token_index(index, side);
            app.status = format!("{symbol} selected; no connector tokens available");
        } else {
            app.status = format!("{symbol} not added; no connector tokens available");
        }
        return Ok(());
    }

    let discovery = PoolDiscovery::for_registry(sync.registry(), factory_config());
    let discovered = discovery
        .find(
            cache,
            PoolQuery::pairs(
                connectors
                    .iter()
                    .copied()
                    .map(|connector| (address, connector)),
            ),
        )
        .context("discover pools for token lookup")?;
    let existing_keys = sync
        .registry()
        .pools()
        .map(|pool| pool.key.clone())
        .collect::<HashSet<_>>();
    let mut label_tokens = app.tokens.clone();
    if existing_index.is_none() {
        label_tokens.push(token.clone());
    }
    let mut new_infos = discovered
        .into_iter()
        .filter(|pool| !existing_keys.contains(&pool.key))
        .map(|pool| pool_info_from_registration(pool.registration, &label_tokens))
        .collect::<Vec<_>>();

    dedup_pool_infos(&mut new_infos);
    sort_pool_infos(&mut new_infos);
    let max_dynamic = env_usize("AMM_ROUTE_TUI_MAX_DYNAMIC_POOLS", DEFAULT_MAX_DYNAMIC_POOLS);
    if max_dynamic > 0 && new_infos.len() > max_dynamic {
        new_infos.truncate(max_dynamic);
    }

    if new_infos.is_empty() {
        if let Some(index) = existing_index {
            app.select_token_index(index, side);
            app.status = format!("{symbol} selected; no new factory pools found");
        } else {
            app.status = format!("{symbol} not added; no ready graph edge found");
        }
        return Ok(());
    }

    let found = new_infos.len();
    let mut pools = new_infos
        .iter()
        .map(|pool| pool.registration.clone())
        .collect::<Vec<_>>();
    let cold_registry = sync.registry().clone();
    let outcomes = cold_registry
        .cold_start_many(&mut pools, cache, provider, ColdStartPolicy::Eager)
        .await
        .context("cold-start token lookup pools")?;

    let mut ready_regs = Vec::new();
    let mut ready_infos = Vec::new();
    for (pool, outcome) in pools.into_iter().zip(outcomes) {
        if is_ready_outcome(&outcome) {
            ready_infos.push(pool_info_from_registration(pool.clone(), &label_tokens));
            ready_regs.push(pool);
        }
    }

    let ready = ready_regs.len();
    if ready > 0 {
        sync.register_pools(ready_regs)
            .context("register token lookup pools")?;
        app.pools.extend(ready_infos);
        app.ready_pools += ready;
        let routing_pools = app.ready_pools;
        mark_graph_synced(app, routing_pools);
        app.topology_updates += 1;
        refresh_liquidity_index(app, sync, cache, provider).await;
        let connected = connected_token_addresses(sync.registry());
        if connected.contains(&address) {
            let index = app.upsert_token(token);
            app.select_token_index(index, side);
        } else if let Some(index) = existing_index {
            app.select_token_index(index, side);
        } else {
            app.status = format!(
                "{symbol} not added; discovered {found} pool(s), but none indexed into the graph"
            );
            return Ok(());
        }
    } else if let Some(index) = existing_index {
        app.select_token_index(index, side);
    }
    app.skipped_pools += found.saturating_sub(ready);
    app.status = if ready > 0 || existing_index.is_some() {
        format!("{symbol} selected; discovered {found} new pool(s), registered {ready}")
    } else {
        format!("{symbol} not added; discovered {found} pool(s), registered 0")
    };
    Ok(())
}

async fn resolve_token_selector(
    provider: &RootProvider<AnyNetwork>,
    known_tokens: &[TokenInfo],
    selector: &str,
) -> Result<TokenInfo> {
    let selector = selector.trim();
    if selector.is_empty() {
        bail!("token selector is empty");
    }
    if let Some(index) = token_index(known_tokens, selector) {
        return Ok(known_tokens[index].clone());
    }
    if let Ok(address) = Address::from_str(selector) {
        return Ok(fetch_token_info(provider, address).await);
    }
    lookup_token_symbol(provider, selector).await
}

async fn lookup_token_symbol(
    provider: &RootProvider<AnyNetwork>,
    symbol: &str,
) -> Result<TokenInfo> {
    let url = token_registry_url().context(
        "token registry disabled; enter an address or add the token to .amm-route-tui.toml",
    )?;
    let chain_id = provider
        .get_chain_id()
        .await
        .context("fetch chain id for token registry lookup")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(
            env_usize("AMM_ROUTE_TUI_TOKEN_REGISTRY_TIMEOUT_SECS", 5) as u64,
        ))
        .build()
        .context("build token registry client")?;
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetch token registry {url}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "token registry HTTP {}: {}",
            status.as_u16(),
            fit_to_width(&body, 160)
        );
    }
    let registry = response
        .json::<TokenRegistryList>()
        .await
        .with_context(|| format!("decode token registry {url}"))?;
    token_from_registry_entries(chain_id, symbol, registry.tokens)
}

fn token_from_registry_entries(
    chain_id: u64,
    symbol: &str,
    entries: Vec<TokenRegistryEntry>,
) -> Result<TokenInfo> {
    let symbol = symbol.trim();
    let matches = entries
        .into_iter()
        .filter(|entry| entry.chain_id == chain_id && entry.symbol.eq_ignore_ascii_case(symbol))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => bail!("token symbol {symbol} was not found in registry for chain {chain_id}"),
        [entry] => {
            let address = Address::from_str(entry.address.trim())
                .with_context(|| format!("registry token {} has invalid address", entry.symbol))?;
            Ok(TokenInfo::new(&entry.symbol, address, entry.decimals))
        }
        entries => {
            let choices = entries
                .iter()
                .take(4)
                .map(|entry| entry.address.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "token symbol {symbol} is ambiguous on chain {chain_id}; enter one address ({choices})"
            )
        }
    }
}

fn token_registry_url() -> Option<String> {
    let raw = std::env::var("AMM_ROUTE_TUI_TOKEN_REGISTRY_URL")
        .unwrap_or_else(|_| DEFAULT_TOKEN_REGISTRY_URL.to_owned());
    let raw = raw.trim();
    if raw.is_empty()
        || raw.eq_ignore_ascii_case("off")
        || raw.eq_ignore_ascii_case("none")
        || raw == "0"
    {
        None
    } else {
        Some(raw.to_owned())
    }
}

async fn fetch_token_info(provider: &RootProvider<AnyNetwork>, address: Address) -> TokenInfo {
    let symbol = match erc20_symbol(provider, address).await {
        Ok(Some(symbol)) => symbol,
        Ok(None) | Err(_) => short_address(address),
    };
    let decimals = match erc20_decimals(provider, address).await {
        Ok(Some(decimals)) if decimals <= 36 => decimals,
        Ok(_) | Err(_) => 18,
    };
    TokenInfo::new(&symbol, address, decimals)
}

async fn erc20_symbol(
    provider: &RootProvider<AnyNetwork>,
    address: Address,
) -> Result<Option<String>> {
    let response = erc20_call(provider, address, ERC20_SYMBOL_SELECTOR).await?;
    Ok(decode_abi_string(&response).and_then(sanitize_symbol))
}

async fn erc20_decimals(
    provider: &RootProvider<AnyNetwork>,
    address: Address,
) -> Result<Option<u8>> {
    let response = erc20_call(provider, address, ERC20_DECIMALS_SELECTOR).await?;
    Ok(decode_abi_u8(&response))
}

async fn erc20_call(
    provider: &RootProvider<AnyNetwork>,
    address: Address,
    selector: [u8; 4],
) -> Result<Bytes> {
    let tx = TransactionRequest::default()
        .to(address)
        .input(TransactionInput::from(Bytes::copy_from_slice(&selector)));
    provider
        .call(tx.into())
        .await
        .context("erc20 metadata call")
}

fn decode_abi_u8(data: &[u8]) -> Option<u8> {
    if data.len() >= 32 {
        Some(data[31])
    } else if data.len() == 1 {
        Some(data[0])
    } else {
        None
    }
}

fn decode_abi_string(data: &[u8]) -> Option<String> {
    if data.len() >= 64 {
        let offset = abi_word_to_usize(data.get(0..32)?)?;
        let length_word = data.get(offset..offset.checked_add(32)?)?;
        let length = abi_word_to_usize(length_word)?;
        let start = offset.checked_add(32)?;
        let end = start.checked_add(length)?;
        if end <= data.len() {
            return String::from_utf8(data[start..end].to_vec()).ok();
        }
    }

    if data.len() >= 32 {
        let word = data.get(0..32)?;
        let end = word
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(word.len());
        if end > 0 {
            return String::from_utf8(word[..end].to_vec()).ok();
        }
    }

    None
}

fn abi_word_to_usize(word: &[u8]) -> Option<usize> {
    if word.len() != 32 || word[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    Some(u64::from_be_bytes(bytes) as usize)
}

fn sanitize_symbol(symbol: String) -> Option<String> {
    let symbol = symbol
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(16)
        .collect::<String>();
    (!symbol.is_empty()).then_some(symbol)
}

fn build_route_viz(
    graph: &AmmGraph,
    route: &RouteQuote,
    tokens: &[TokenInfo],
    pools: &[PoolInfo],
    prices: &PriceBook,
) -> RouteViz {
    let mut route_tokens = Vec::new();
    if let Some(first_hop) = route.hops.first() {
        route_tokens.push(route_token_view(graph, tokens, first_hop.hop.token_in));
    }
    route_tokens.extend(
        route
            .hops
            .iter()
            .map(|hop| route_token_view(graph, tokens, hop.hop.token_out)),
    );

    let selected_legs = route
        .hops
        .iter()
        .map(|hop| route_leg_view(hop, tokens, pools, prices))
        .collect::<Vec<_>>();
    let mut alternatives = Vec::new();
    for (index, hop) in route.hops.iter().enumerate() {
        for (pool_label, pool_address) in alternate_leg_labels(
            graph,
            hop.hop.token_in,
            hop.hop.token_out,
            &hop.hop.pool,
            pools,
        ) {
            let mut legs = selected_legs.clone();
            let Some(leg) = legs.get_mut(index) else {
                continue;
            };
            leg.pool = pool_label;
            leg.pool_address = pool_address;
            alternatives.push(AlternativePathView {
                label: format!(
                    "ALT {}  leg {} venue  {} -> {}",
                    alternatives.len() + 1,
                    index + 1,
                    token_symbol(tokens, hop.hop.token_in),
                    token_symbol(tokens, hop.hop.token_out)
                ),
                replaced_leg: index,
                legs,
            });
            if alternatives.len() >= MAX_ALTERNATIVE_PATHS {
                break;
            }
        }
        if alternatives.len() >= MAX_ALTERNATIVE_PATHS {
            break;
        }
    }

    RouteViz {
        tokens: route_tokens,
        selected_legs,
        alternatives,
    }
}

fn route_token_view(graph: &AmmGraph, tokens: &[TokenInfo], address: Address) -> RouteTokenView {
    let edge_count = graph
        .node_index(&address)
        .map(|node| graph.graph().edges(node).count())
        .unwrap_or_default();
    RouteTokenView {
        symbol: token_symbol(tokens, address),
        address,
        edge_count,
    }
}

fn route_leg_view(
    hop: &evm_amm_search::HopQuote,
    tokens: &[TokenInfo],
    pools: &[PoolInfo],
    prices: &PriceBook,
) -> RouteLegView {
    let token_in = token_by_address(tokens, hop.hop.token_in);
    let token_out = token_by_address(tokens, hop.hop.token_out);
    RouteLegView {
        token_in: token_in.symbol.clone(),
        token_out: token_out.symbol.clone(),
        token_in_address: token_in.address,
        token_out_address: token_out.address,
        amount_in: format_amount_with_token(hop.amount_in, &token_in, prices, 6),
        amount_out: format_amount_with_token(hop.amount_out, &token_out, prices, 6),
        pool: pool_label(pools, &hop.hop.pool),
        pool_address: hop.hop.pool.address(),
    }
}

fn alternate_leg_labels(
    graph: &AmmGraph,
    token_in: Address,
    token_out: Address,
    selected_pool: &PoolKey,
    pools: &[PoolInfo],
) -> Vec<(String, Option<Address>)> {
    let mut seen = HashSet::new();
    let mut labels = Vec::new();
    for edge in graph.graph().edge_references() {
        if edge.weight().pool == *selected_pool || !seen.insert(edge.weight().pool.clone()) {
            continue;
        }
        let Some(source) = graph.node_token(edge.source()) else {
            continue;
        };
        let Some(target) = graph.node_token(edge.target()) else {
            continue;
        };
        if source == token_in && target == token_out {
            labels.push((
                pool_label(pools, &edge.weight().pool),
                edge.weight().pool.address(),
            ));
        }
    }
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    labels
}

fn route_panel_lines(quote: &QuoteView, width: usize, height: usize) -> Vec<GraphLine> {
    let content_width = width.saturating_sub(2).max(24);
    let budget = height.saturating_sub(2).max(1);
    let stream_budget = if budget >= 20 {
        5
    } else if budget >= 15 {
        3
    } else if budget >= 11 {
        1
    } else {
        0
    };
    let stream_overhead = if stream_budget > 0 && !quote.stream_lines.is_empty() {
        2
    } else {
        0
    };
    let route_budget = budget.saturating_sub(stream_budget + stream_overhead);
    let mut lines = route_body_lines(quote, content_width, route_budget);

    if stream_budget > 0 && !quote.stream_lines.is_empty() && lines.len() + 2 < budget {
        push_graph_line(&mut lines, budget, String::new(), GraphStyle::Normal);
        push_graph_line(
            &mut lines,
            budget,
            "STREAM".to_owned(),
            GraphStyle::Highlight,
        );
        for line in quote.stream_lines.iter().rev().take(stream_budget).rev() {
            push_graph_line(
                &mut lines,
                budget,
                fit_to_width(line, content_width),
                GraphStyle::Secondary,
            );
        }
    }

    lines
}

fn route_body_lines(quote: &QuoteView, width: usize, budget: usize) -> Vec<GraphLine> {
    let mut lines = Vec::new();
    let route = &quote.route;
    if route.selected_legs.is_empty() {
        push_graph_line(
            &mut lines,
            budget,
            "no route selected".to_owned(),
            GraphStyle::Dim,
        );
        return lines;
    }

    let first_leg = &route.selected_legs[0];
    let hop_count = route.selected_legs.len();
    let hop_word = if hop_count == 1 { "hop" } else { "hops" };
    push_graph_line(
        &mut lines,
        budget,
        fit_to_width(
            &format!(
                "BEST  {} -> {}  |  {} {}",
                first_leg.amount_in, quote.output, hop_count, hop_word
            ),
            width,
        ),
        GraphStyle::Highlight,
    );
    for warning in &quote.warnings {
        push_graph_line(
            &mut lines,
            budget,
            fit_to_width(warning, width),
            GraphStyle::Warning,
        );
    }
    push_graph_line(
        &mut lines,
        budget,
        fit_to_width(&quote.coverage, width),
        GraphStyle::Dim,
    );
    if !quote.quoted_routes.is_empty() {
        push_graph_line(
            &mut lines,
            budget,
            fit_to_width("QUOTED TOP ROUTES", width),
            GraphStyle::Normal,
        );
        if !quote.quoted_venues.is_empty() {
            push_graph_line(
                &mut lines,
                budget,
                fit_to_width(&quote.quoted_venues, width),
                GraphStyle::Dim,
            );
        }
        for quoted in &quote.quoted_routes {
            push_graph_line(
                &mut lines,
                budget,
                fit_to_width(quoted, width),
                GraphStyle::Secondary,
            );
        }
    }
    if let Some(gas) = &quote.gas {
        push_graph_line(
            &mut lines,
            budget,
            fit_to_width(&gas.summary, width),
            if gas.ok {
                GraphStyle::Highlight
            } else {
                GraphStyle::Dim
            },
        );
        push_graph_line(
            &mut lines,
            budget,
            fit_to_width(&gas.detail, width),
            GraphStyle::Dim,
        );
    }
    push_graph_line(
        &mut lines,
        budget,
        render_path_lane(&route.tokens, &route.selected_legs, None, width),
        GraphStyle::Highlight,
    );
    push_graph_line(
        &mut lines,
        budget,
        fit_to_width(&node_edge_line(&route.tokens), width),
        GraphStyle::Dim,
    );
    for (index, leg) in route.selected_legs.iter().enumerate() {
        push_graph_line(
            &mut lines,
            budget,
            fit_to_width(&selected_leg_line(index, leg), width),
            GraphStyle::Normal,
        );
    }
    if route.alternatives.is_empty() {
        push_graph_line(&mut lines, budget, String::new(), GraphStyle::Normal);
        push_graph_line(
            &mut lines,
            budget,
            fit_to_width(
                "ALTERNATE PATHS  no registered same-pair venue substitutions",
                width,
            ),
            GraphStyle::Dim,
        );
    } else {
        push_graph_line(&mut lines, budget, String::new(), GraphStyle::Normal);
        push_graph_line(
            &mut lines,
            budget,
            fit_to_width("ALTERNATE PATHS", width),
            GraphStyle::Normal,
        );

        for (alt_index, alternative) in route.alternatives.iter().enumerate() {
            if lines.len() + 3 > budget {
                let remaining = route.alternatives.len().saturating_sub(alt_index);
                push_graph_line(
                    &mut lines,
                    budget,
                    fit_to_width(&format!("... {remaining} more alternate path(s)"), width),
                    GraphStyle::Dim,
                );
                break;
            }
            push_graph_line(
                &mut lines,
                budget,
                fit_to_width(&alternative.label, width),
                GraphStyle::Secondary,
            );
            push_graph_line(
                &mut lines,
                budget,
                render_path_lane(
                    &route.tokens,
                    &alternative.legs,
                    Some(alternative.replaced_leg),
                    width,
                ),
                GraphStyle::Secondary,
            );
            if let Some(leg) = alternative.legs.get(alternative.replaced_leg) {
                push_graph_line(
                    &mut lines,
                    budget,
                    fit_to_width(
                        &format!(
                            "      leg {} venue: {} -> {} via {}",
                            alternative.replaced_leg + 1,
                            leg.token_in,
                            leg.token_out,
                            leg.pool
                        ),
                        width,
                    ),
                    GraphStyle::Dim,
                );
            }
        }
    }

    append_route_link_lines(&mut lines, budget, route, width);
    lines
}

fn append_route_link_lines(
    lines: &mut Vec<GraphLine>,
    budget: usize,
    route: &RouteViz,
    width: usize,
) {
    if width < 76 || lines.len() + 2 >= budget {
        return;
    }

    let mut link_lines = Vec::new();
    let mut seen_tokens = HashSet::new();
    for token in &route.tokens {
        if seen_tokens.insert(token.address) {
            link_lines.push(format!(
                "token {:<8} {}",
                token.symbol,
                etherscan_token_url(token.address)
            ));
        }
    }
    for (index, leg) in route.selected_legs.iter().enumerate() {
        if let Some(address) = leg.pool_address {
            link_lines.push(format!(
                "pool  leg {:<2} {}",
                index + 1,
                etherscan_address_url(address)
            ));
        }
        for (symbol, address) in [
            (&leg.token_in, leg.token_in_address),
            (&leg.token_out, leg.token_out_address),
        ] {
            if seen_tokens.insert(address) {
                link_lines.push(format!(
                    "token {:<8} {}",
                    symbol,
                    etherscan_token_url(address)
                ));
            }
        }
    }
    let link_lines = link_lines
        .into_iter()
        .filter(|line| char_len(line) <= width)
        .take(6)
        .collect::<Vec<_>>();
    if link_lines.is_empty() {
        return;
    }

    if !push_graph_line(lines, budget, String::new(), GraphStyle::Normal) {
        return;
    }
    if !push_graph_line(
        lines,
        budget,
        "LINKS  terminal URL detection / Command-click".to_owned(),
        GraphStyle::Dim,
    ) {
        return;
    }
    for line in link_lines {
        if !push_graph_line(lines, budget, line, GraphStyle::Secondary) {
            break;
        }
    }
}

fn push_graph_line(
    lines: &mut Vec<GraphLine>,
    budget: usize,
    text: String,
    style: GraphStyle,
) -> bool {
    if lines.len() >= budget {
        return false;
    }
    lines.push(GraphLine { text, style });
    true
}

fn render_path_lane(
    tokens: &[RouteTokenView],
    legs: &[RouteLegView],
    alternate_leg: Option<usize>,
    width: usize,
) -> String {
    if tokens.is_empty() || legs.is_empty() {
        return "(no route)".to_owned();
    }

    let node_labels = tokens
        .iter()
        .map(|token| format!("({})", token.symbol))
        .collect::<Vec<_>>();
    let node_width = node_labels
        .iter()
        .map(|label| char_len(label))
        .sum::<usize>();
    let separators = legs.len() * 2;
    let minimum_connector_width = if width >= 96 { 18 } else { 10 };
    let connector_total = width
        .saturating_sub(node_width + separators)
        .max(minimum_connector_width * legs.len());
    let base_connector_width = connector_total / legs.len();
    let extra_connectors = connector_total % legs.len();

    let mut line = node_labels[0].clone();
    for (index, leg) in legs.iter().enumerate() {
        let connector_width = base_connector_width + usize::from(index < extra_connectors);
        line.push(' ');
        line.push_str(&leg_connector(
            &leg.pool,
            connector_width,
            connector_mode(alternate_leg, index),
        ));
        line.push(' ');
        if let Some(node) = node_labels.get(index + 1) {
            line.push_str(node);
        }
    }

    fit_to_width(&line, width)
}

fn connector_mode(alternate_leg: Option<usize>, index: usize) -> ConnectorMode {
    match alternate_leg {
        None => ConnectorMode::Selected,
        Some(alternate) if alternate == index => ConnectorMode::Alternate,
        Some(_) => ConnectorMode::Context,
    }
}

#[derive(Clone, Copy)]
enum ConnectorMode {
    Selected,
    Alternate,
    Context,
}

fn leg_connector(pool: &str, width: usize, mode: ConnectorMode) -> String {
    if width <= 1 {
        return ">".to_owned();
    }

    let fill = match mode {
        ConnectorMode::Selected => '=',
        ConnectorMode::Alternate => '~',
        ConnectorMode::Context => '-',
    };
    if width <= 5 {
        return format!("{}>", fill.to_string().repeat(width.saturating_sub(1)));
    }

    let arrow_width = 1;
    let body_width = width.saturating_sub(arrow_width);
    let label = fit_to_width(&format!("[{pool}]"), body_width.saturating_sub(2).max(1));
    let label_width = char_len(&label);
    let fill_width = body_width.saturating_sub(label_width);
    let left = fill_width / 2;
    let right = fill_width.saturating_sub(left);
    let mut connector = String::with_capacity(width);
    connector.push_str(&fill.to_string().repeat(left));
    connector.push_str(&label);
    connector.push_str(&fill.to_string().repeat(right));
    connector.push('>');
    connector
}

fn node_edge_line(tokens: &[RouteTokenView]) -> String {
    let nodes = tokens
        .iter()
        .map(|token| format!("{} {} edges", token.symbol, token.edge_count))
        .collect::<Vec<_>>()
        .join("  |  ");
    format!("nodes  {nodes}")
}

fn selected_leg_line(index: usize, leg: &RouteLegView) -> String {
    format!(
        "leg {}  {} -> {}  via {}",
        index + 1,
        leg.amount_in,
        leg.amount_out,
        leg.pool
    )
}

fn fit_to_width(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let length = char_len(value);
    if length <= width {
        return value.to_owned();
    }
    if width <= 3 {
        return value.chars().take(width).collect();
    }
    let mut clipped = value.chars().take(width - 3).collect::<String>();
    clipped.push_str("...");
    clipped
}

fn char_len(value: &str) -> usize {
    value.chars().count()
}

fn draw(frame: &mut Frame<'_>, app: &AppState) {
    let area = frame.area();
    let input_height = if app.token_search.is_some() {
        12
    } else if app.active == ActiveField::TokenAddress || !app.custom_address.is_empty() {
        10
    } else {
        8
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(input_height),
            Constraint::Min(10),
            Constraint::Length(8),
        ])
        .split(area);

    let header_block = Block::default().borders(Borders::ALL);
    let header_inner = header_block.inner(chunks[0]);
    frame.render_widget(header_block, chunks[0]);
    let header_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(46), Constraint::Percentage(54)])
        .split(header_inner);
    let header_left_width = header_chunks[0].width as usize;
    let header_left = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                "amm-route-tui",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "  block={}  routing_pools={}  {}",
                app.last_block,
                app.graph_sync.routing_pools,
                app.prices.coverage_label(app.tokens.len())
            )),
        ]),
        Line::from(fit_to_width(
            &format!("status={}", app.status),
            header_left_width,
        )),
    ]);
    frame.render_widget(header_left, header_chunks[0]);
    frame.render_widget(
        Paragraph::new(status_indicator_lines(app, header_chunks[1].width as usize))
            .alignment(Alignment::Right),
        header_chunks[1],
    );

    let token_in = app.token_in();
    let token_out = app.token_out();
    let search_side = app.token_search.as_ref().map(|search| search.side);
    let amount_value = if app.amount_editing {
        format!("{}_", app.amount)
    } else {
        app.amount.clone()
    };
    let mut controls = vec![
        field_line(
            "Input",
            &token_in.symbol,
            app.active == ActiveField::Input || search_side == Some(Side::Input),
        ),
        field_line(
            "Output",
            &token_out.symbol,
            app.active == ActiveField::Output || search_side == Some(Side::Output),
        ),
        field_line("Amount", &amount_value, app.active == ActiveField::Amount),
    ];
    if let Some(search) = &app.token_search {
        controls.push(field_line(
            "Search",
            &format!("{} {}", search.side.label(), search.query),
            true,
        ));
        controls.extend(token_search_preview_lines(
            app,
            5,
            chunks[1].width.saturating_sub(4) as usize,
        ));
    }
    if app.active == ActiveField::TokenAddress || !app.custom_address.is_empty() {
        controls.push(field_line(
            "Token lookup",
            &format!("{} {}", app.custom_side.label(), app.custom_address),
            app.active == ActiveField::TokenAddress,
        ));
    }
    let mut shortcuts =
        "Tab/arrows move fields  Enter edits selected field  picker Up/Down selects  n token lookup  r requotes"
            .to_owned();
    if app.tenderly.config.is_some() {
        shortcuts.push_str("  t simulates");
    }
    shortcuts.push_str("  q quits");
    controls.push(Line::from(shortcuts));
    frame.render_widget(
        Paragraph::new(controls)
            .block(Block::default().title("Inputs").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        chunks[1],
    );

    let route_panel_loading = route_panel_shows_loading(
        app.quote_loading,
        app.quote.as_ref().map(|quote| &quote.best),
        token_in.address,
        token_out.address,
        parse_units(&app.amount, token_in.decimals).ok(),
    );
    let graph_title = if route_panel_loading {
        format!(
            "Route Layout  loading {} -> {}",
            token_in.symbol, token_out.symbol
        )
    } else if let Some(quote) = &app.quote {
        let sync_label = if app.quote_loading {
            "(syncing)"
        } else {
            "(synced) "
        };
        format!(
            "Route Layout  {} in {:.2?}  streamed={} routing_pools={}  {}",
            quote.output, quote.elapsed, quote.routes, quote.graph_pools, sync_label,
        )
    } else {
        "Route Layout".to_owned()
    };
    let graph_items = if route_panel_loading {
        loading_panel_items(app, &token_in, &token_out, chunks[2].width as usize)
    } else if let Some(quote) = &app.quote {
        route_panel_lines(quote, chunks[2].width as usize, chunks[2].height as usize)
            .into_iter()
            .map(|line| {
                ListItem::new(line.text).style(match line.style {
                    GraphStyle::Normal => Style::default(),
                    GraphStyle::Highlight => Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                    GraphStyle::Warning => {
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                    }
                    GraphStyle::Secondary => Style::default().fg(Color::Gray),
                    GraphStyle::Dim => Style::default().fg(Color::DarkGray),
                })
            })
            .collect::<Vec<_>>()
    } else {
        vec![ListItem::new(
            app.quote_error
                .as_deref()
                .unwrap_or("no quote yet; waiting for input"),
        )]
    };
    frame.render_widget(
        List::new(graph_items).block(Block::default().title(graph_title).borders(Borders::ALL)),
        chunks[2],
    );

    let mut stats_lines = vec![
        Line::from(format!(
            "graph: routing_pools={} discovered_pools={} loading_pools={} queued_loads={} pending_state_updates={}",
            app.graph_sync.routing_pools,
            app.graph_sync.discovered_pools,
            app.graph_sync.loading_pools,
            app.graph_sync.queued_loads,
            app.graph_sync.pending_state_updates
        )),
        Line::from(format!(
            "routed_logs={} ignored_logs={} applied_effects={} resync_updates={} resync_failures={}",
            app.routed_logs,
            app.ignored_logs,
            app.applied_logs,
            app.resync_updates,
            app.resync_failures
        )),
        Line::from(format!(
            "pool_events: degraded={} recovered={} failed_pool_loads={} skipped_loads={}",
            app.graph_sync
                .degraded_pools
                .max(app.degraded_pools as usize),
            app.recovered_pools,
            app.graph_sync.failed_pools,
            app.skipped_pools
        )),
        Line::from(format!(
            "quote_updates={} topology_updates={} token lookup metadata falls back to short address/18 decimals",
            app.quote_updates, app.topology_updates
        )),
        Line::from(
            "WebSocket block/log updates requote live prices; topology changes when discovery registers new pools.",
        ),
    ];
    if app.tenderly.config.is_some() || !app.tenderly.status.starts_with("Tenderly disabled") {
        stats_lines.push(Line::from(Span::styled(
            fit_to_width(
                &format!("Tenderly: {}", app.tenderly.status),
                chunks[3].width.saturating_sub(4) as usize,
            ),
            tenderly_status_style(app),
        )));
    }
    let stats = Paragraph::new(stats_lines)
        .block(Block::default().title("Live Sync").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(stats, chunks[3]);
}

fn loading_panel_items(
    app: &AppState,
    token_in: &TokenInfo,
    token_out: &TokenInfo,
    width: usize,
) -> Vec<ListItem<'static>> {
    let elapsed = app
        .route_work
        .started_at
        .map(|started| format!("  elapsed {:.1}s", started.elapsed().as_secs_f32()))
        .unwrap_or_default();
    vec![
        ListItem::new(format!(
            "{} loading swap data for {} -> {}{}",
            loading_spinner(app),
            token_in.symbol,
            token_out.symbol,
            elapsed
        ))
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        ListItem::new(loading_bar_text(app, width)).style(Style::default().fg(Color::Yellow)),
        ListItem::new(app.route_work.detail.clone()).style(Style::default().fg(Color::Gray)),
        ListItem::new("graph warming and quote work now run behind this screen")
            .style(Style::default().fg(Color::DarkGray)),
    ]
}

fn loading_spinner(app: &AppState) -> &'static str {
    const FRAMES: [&str; 4] = ["|", "/", "-", "\\"];
    let index = app
        .route_work
        .started_at
        .map(|started| (started.elapsed().as_millis() / 250) as usize)
        .unwrap_or(0);
    FRAMES[index % FRAMES.len()]
}

fn loading_bar_text(app: &AppState, width: usize) -> String {
    let bar_width = width.saturating_sub(18).clamp(12, 48);
    if let Some((step, total)) = app.route_work.progress {
        let total = total.max(1);
        let step = step.min(total);
        let filled = (bar_width * step) / total;
        return format!(
            "[{}{}] {step}/{total}",
            "#".repeat(filled),
            ".".repeat(bar_width.saturating_sub(filled))
        );
    }

    let pulse_width = (bar_width / 4).clamp(3, bar_width);
    let range = bar_width.saturating_sub(pulse_width).saturating_add(1);
    let offset = app
        .route_work
        .started_at
        .map(|started| ((started.elapsed().as_millis() / 250) as usize) % range.max(1))
        .unwrap_or(0);
    let mut cells = vec!['.'; bar_width];
    for cell in cells
        .iter_mut()
        .take(offset.saturating_add(pulse_width).min(bar_width))
        .skip(offset)
    {
        *cell = '=';
    }
    let bar = cells.into_iter().collect::<String>();
    format!("[{bar}] working")
}

fn status_indicator_lines(app: &AppState, width: usize) -> Vec<Line<'static>> {
    vec![
        status_indicator_line(
            "chain",
            chain_phase_label(app.chain_sync.phase),
            &app.chain_sync.detail,
            chain_phase_color(app.chain_sync.phase),
            width,
        ),
        status_indicator_line(
            "graph",
            graph_phase_label(app.graph_sync.phase),
            &app.graph_sync.detail,
            graph_phase_color(app.graph_sync.phase),
            width,
        ),
    ]
}

fn status_indicator_line(
    prefix: &'static str,
    phase: &'static str,
    detail: &str,
    color: Color,
    width: usize,
) -> Line<'static> {
    let marker = format!("● {phase}");
    let fixed_width = char_len(prefix) + 1 + char_len(&marker) + 1;
    let detail = compact_status_detail(phase, detail);
    let detail = fit_to_width(detail, width.saturating_sub(fixed_width));
    Line::from(vec![
        Span::styled(format!("{prefix} "), Style::default().fg(Color::Gray)),
        Span::styled(
            marker,
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::raw(detail),
    ])
}

fn compact_status_detail<'a>(phase: &str, detail: &'a str) -> &'a str {
    if detail == phase {
        return "";
    }
    detail
        .strip_prefix(phase)
        .and_then(|rest| rest.strip_prefix(' '))
        .unwrap_or(detail)
}

fn chain_phase_label(phase: ChainSyncPhase) -> &'static str {
    match phase {
        ChainSyncPhase::Synced => "synced",
        ChainSyncPhase::Applying => "applying",
        ChainSyncPhase::Degraded => "error",
    }
}

fn chain_phase_color(phase: ChainSyncPhase) -> Color {
    match phase {
        ChainSyncPhase::Synced => Color::Green,
        ChainSyncPhase::Applying => Color::Yellow,
        ChainSyncPhase::Degraded => Color::Red,
    }
}

fn graph_phase_label(phase: GraphSyncPhase) -> &'static str {
    match phase {
        GraphSyncPhase::Synced => "graph synced",
        GraphSyncPhase::Warming => "warming graph",
        GraphSyncPhase::Error => "graph error",
    }
}

fn graph_phase_color(phase: GraphSyncPhase) -> Color {
    match phase {
        GraphSyncPhase::Synced => Color::Green,
        GraphSyncPhase::Warming => Color::Yellow,
        GraphSyncPhase::Error => Color::Red,
    }
}

fn route_phase_label(phase: RouteWorkPhase) -> &'static str {
    match phase {
        RouteWorkPhase::Ready => "ready",
        RouteWorkPhase::Discovering => "discover",
        RouteWorkPhase::Quoting => "quoting",
        RouteWorkPhase::Error => "error",
    }
}

fn route_phase_color(phase: RouteWorkPhase) -> Color {
    match phase {
        RouteWorkPhase::Ready => Color::Green,
        RouteWorkPhase::Discovering | RouteWorkPhase::Quoting => Color::Yellow,
        RouteWorkPhase::Error => Color::Red,
    }
}

fn token_search_preview_lines(app: &AppState, limit: usize, width: usize) -> Vec<Line<'static>> {
    let Some(search) = &app.token_search else {
        return Vec::new();
    };
    let matches = token_search_matches(&app.tokens, &search.query);
    if matches.is_empty() {
        let query = search.query.trim();
        let message = if Address::from_str(query).is_ok() {
            "no listed token; Enter discovers this address".to_owned()
        } else if !query.is_empty() {
            "no listed token; Enter looks up this symbol".to_owned()
        } else {
            "no listed token match".to_owned()
        };
        return vec![Line::from(Span::styled(
            message,
            Style::default().fg(Color::Gray),
        ))];
    }

    let selected = search.selected.min(matches.len() - 1);
    let start = selected.saturating_sub(limit.saturating_sub(1));
    matches
        .into_iter()
        .skip(start)
        .take(limit)
        .enumerate()
        .map(|(row, token_index)| {
            let token = &app.tokens[token_index];
            let is_selected = start + row == selected;
            let marker = if is_selected { ">" } else { " " };
            let style = if is_selected {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            Line::from(Span::styled(
                format_token_preview_line(marker, token, width),
                style,
            ))
        })
        .collect()
}

fn format_token_preview_line(marker: &str, token: &TokenInfo, width: usize) -> String {
    let full_address = format!("{:#x}", token.address);
    let prefix = format!("{marker} {:<8} ", token.symbol);
    if char_len(&prefix) + char_len(&full_address) <= width {
        format!("{prefix}{full_address}")
    } else {
        format!("{prefix}{}", short_address(token.address))
    }
}

fn field_line(label: &str, value: &str, active: bool) -> Line<'static> {
    let style = if active {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    Line::from(vec![
        Span::styled(format!("{label}: "), style),
        Span::styled(value.to_owned(), style),
    ])
}

fn tenderly_status_style(app: &AppState) -> Style {
    if app.tenderly.in_flight {
        Style::default().fg(Color::Yellow)
    } else if app.tenderly.ok == Some(false) {
        Style::default().fg(Color::Red)
    } else if app.tenderly.config.is_some() {
        Style::default().fg(Color::Green)
    } else {
        Style::default().fg(Color::Gray)
    }
}

fn spawn_input_thread(tx: mpsc::Sender<UiEvent>) {
    thread::spawn(move || {
        loop {
            if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                match event::read() {
                    Ok(Event::Key(key)) => {
                        if tx.blocking_send(UiEvent::Key(key)).is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }
    });
}

fn spawn_chain_task(
    provider: Arc<RootProvider<AnyNetwork>>,
    topics: Vec<B256>,
    tx: mpsc::Sender<ChainEvent>,
) {
    tokio::spawn(async move {
        let filter = Filter::new().event_signature(topics);
        let mut logs = match provider.subscribe_logs(&filter).await {
            Ok(subscription) => subscription.into_stream(),
            Err(error) => {
                let _ = tx
                    .send(ChainEvent::Error(format!(
                        "log subscription failed: {error}"
                    )))
                    .await;
                return;
            }
        };
        let mut blocks = match provider.subscribe_blocks().await {
            Ok(subscription) => subscription.into_stream(),
            Err(error) => {
                let _ = tx
                    .send(ChainEvent::Error(format!(
                        "block subscription failed: {error}"
                    )))
                    .await;
                return;
            }
        };

        loop {
            tokio::select! {
                maybe_log = logs.next() => {
                    match maybe_log {
                        Some(log) => {
                            if tx.send(ChainEvent::Log(log)).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            let _ = tx.send(ChainEvent::Error("log stream ended".to_owned())).await;
                            break;
                        }
                    }
                }
                maybe_block = blocks.next() => {
                    match maybe_block {
                        Some(header) => {
                            if tx.send(ChainEvent::Block(Box::new(header))).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            let _ = tx.send(ChainEvent::Error("block stream ended".to_owned())).await;
                            break;
                        }
                    }
                }
            }
        }
    });
}

fn ctx_from_log(log: &RpcLog, chain_id: u64) -> ReactiveContext {
    let block = match (log.block_hash, log.block_number) {
        (Some(hash), Some(number)) => Some(BlockRef {
            number,
            hash,
            parent_hash: None,
            timestamp: log.block_timestamp,
        }),
        _ => None,
    };
    let chain_status = match (&block, log.removed) {
        (Some(block), true) => ChainStatus::Reorged {
            dropped_from: block.clone(),
        },
        (Some(block), false) => ChainStatus::Included {
            block: block.clone(),
            confirmations: 0,
        },
        (None, _) => ChainStatus::Pending,
    };
    ReactiveContext {
        chain_id: Some(chain_id),
        source: InputSource::Subscription,
        chain_status,
        block,
        transaction_index: log.transaction_index,
        log_index: log.log_index,
    }
}

fn endpoint_from_env() -> Option<String> {
    std::env::var("ETH_WS_URL")
        .ok()
        .or_else(|| std::env::var("WS_RPC_URL").ok())
        .or_else(|| {
            std::env::var("E2E_RPC_URL")
                .ok()
                .map(|url| http_to_ws(&url))
        })
        .or_else(|| std::env::var("RPC_URL").ok().map(|url| http_to_ws(&url)))
}

struct StateProviderConnection {
    provider: Arc<RootProvider<AnyNetwork>>,
    endpoint_count: usize,
    max_request_bytes: usize,
}

async fn connect_state_provider(
    ws_url: &str,
    network: &TuiNetworkConfig,
) -> Result<StateProviderConnection> {
    if let Some(urls) = state_rpc_urls_from_env()? {
        let endpoints = urls
            .into_iter()
            .map(profiled_rpc_endpoint)
            .collect::<Vec<_>>();
        return Ok(build_state_provider_connection(
            endpoints,
            network.rpc_batch_size,
        ));
    }
    if !network.rpc_endpoints.is_empty() {
        let endpoints = network
            .rpc_endpoints
            .iter()
            .map(configured_rpc_endpoint)
            .collect::<Result<Vec<_>>>()?;
        return Ok(build_state_provider_connection(
            endpoints,
            network.rpc_batch_size,
        ));
    }

    Ok(StateProviderConnection {
        provider: Arc::new(
            RootProvider::<AnyNetwork>::connect(ws_url)
                .await
                .context("connect state websocket endpoint")?,
        ),
        endpoint_count: 1,
        // Unknown WebSocket endpoints retain the conservative body budget.
        max_request_bytes: 1_450_000,
    })
}

fn state_rpc_urls_from_env() -> Result<Option<Vec<Url>>> {
    let Some(raw) = env_first(&["AMM_ROUTE_TUI_RPC_URLS"]) else {
        return Ok(None);
    };
    let urls = raw
        .split([',', ';'])
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(|url| {
            let parsed = Url::parse(url).with_context(|| format!("parse RPC URL {url}"))?;
            match parsed.scheme() {
                "http" | "https" => Ok(parsed),
                scheme => bail!(
                    "AMM_ROUTE_TUI_RPC_URLS must contain HTTP(S) RPC URLs, got scheme {scheme}"
                ),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if urls.is_empty() {
        bail!("AMM_ROUTE_TUI_RPC_URLS was set but no RPC URLs were provided");
    }
    Ok(Some(urls))
}

fn profiled_rpc_endpoint(url: Url) -> EndpointConfig {
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host.contains("alchemy") {
        EndpointConfig::new(url, Weight(150))
            .with_max_request_bytes(2_400_000)
            .with_max_in_flight(32)
    } else if host.contains("quicknode") || host.contains("quiknode") {
        EndpointConfig::new(url, Weight(100))
            .with_max_request_bytes(5_000_000)
            .with_max_in_flight(24)
    } else {
        EndpointConfig::new(url, Weight(50))
            .with_max_request_bytes(1_450_000)
            .with_max_in_flight(4)
    }
}

fn configured_rpc_endpoint(config: &TuiRpcEndpointConfig) -> Result<EndpointConfig> {
    let url = Url::parse(&config.url).with_context(|| format!("parse RPC URL {}", config.url))?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("network.rpc_endpoints must use HTTP(S): {}", config.url);
    }
    let profiled = profiled_rpc_endpoint(url);
    let mut endpoint = EndpointConfig::new(
        profiled.url,
        Weight(config.weight.unwrap_or(profiled.weight.0)),
    );
    endpoint.max_request_bytes = config.max_request_bytes.or(profiled.max_request_bytes);
    endpoint.max_in_flight = config.max_in_flight.or(profiled.max_in_flight);
    Ok(endpoint)
}

fn build_state_provider_connection(
    endpoints: Vec<EndpointConfig>,
    configured_batch_size: usize,
) -> StateProviderConnection {
    let endpoint_count = endpoints.len();
    let max_request_bytes = endpoints
        .iter()
        .filter_map(|endpoint| endpoint.max_request_bytes)
        .max()
        .unwrap_or(1_450_000);
    let balanced = LoadBalancedTransport::builder_with_endpoints(endpoints)
        .http_client_config(HttpClientConfig {
            gzip: true,
            ..Default::default()
        })
        .build();
    let profiled = RpcProfileTransport::http(balanced);
    let batched = BatchingTransport::new(
        profiled,
        BatchingConfig {
            max_batch_size: env_usize("AMM_ROUTE_TUI_RPC_BATCH_SIZE", configured_batch_size),
            ..Default::default()
        },
    );
    StateProviderConnection {
        provider: Arc::new(RootProvider::<AnyNetwork>::new(RpcClient::new(
            batched, false,
        ))),
        endpoint_count,
        max_request_bytes,
    }
}

fn http_to_ws(url: &str) -> String {
    url.replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1)
}

fn build_registry(sim_config: SimConfig) -> Result<AdapterRegistry> {
    let mut registry = AdapterRegistry::new().with_sim_config(sim_config);
    registry.register_adapter(Arc::new(UniswapV2Adapter::default()))?;
    registry.register_adapter(Arc::new(ConcentratedLiquidityAdapter::default()))?;
    registry.register_adapter(Arc::new(CurveAdapter::default()))?;
    Ok(registry)
}

fn factory_config() -> FactoryConfig {
    FactoryConfig::default()
        .with_uniswap_v2(UniswapV2FactoryConfig::uniswap_v2(UNISWAP_V2_FACTORY).with_fee_bps(30))
        .with_uniswap_v2(UniswapV2FactoryConfig::uniswap_v2(SUSHISWAP_V2_FACTORY).with_fee_bps(30))
        .with_uniswap_v3(UniswapV3FactoryConfig::uniswap_v3(UNISWAP_V3_FACTORY))
        .with_uniswap_v3(
            UniswapV3FactoryConfig::sushi_v3(SUSHISWAP_V3_FACTORY)
                .with_quoter(SUSHISWAP_V3_QUOTER_V2),
        )
        .with_pancake_v3_factory(PANCAKESWAP_V3_FACTORY)
        .with_verify_derivations(false)
}

fn load_tui_user_config() -> Result<TuiUserConfig> {
    if let Ok(path) = std::env::var("AMM_ROUTE_TUI_CONFIG") {
        let path = PathBuf::from(path);
        if !path.exists() {
            bail!("AMM_ROUTE_TUI_CONFIG does not exist: {}", path.display());
        }
        ensure_toml_config_path(&path)?;
        return parse_tui_user_config(&path);
    }

    let Some(path) = tui_config_path() else {
        return Ok(TuiUserConfig::default());
    };
    parse_tui_user_config(&path)
}

fn parse_tui_user_config(path: &Path) -> Result<TuiUserConfig> {
    ensure_toml_config_path(path)?;
    let raw =
        fs::read_to_string(path).with_context(|| format!("read TUI config {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("parse TOML TUI config {}", path.display()))
}

fn ensure_toml_config_path(path: &Path) -> Result<()> {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("toml") => Ok(()),
        _ => bail!("TUI config must be a .toml file: {}", path.display()),
    }
}

fn tui_config_path() -> Option<PathBuf> {
    [".amm-route-tui.toml", "amm-route-tui.toml"]
        .into_iter()
        .map(PathBuf::from)
        .find(|path| path.exists())
}

fn token_list(config: &TuiUserConfig) -> Result<Vec<TokenInfo>> {
    let mut tokens = if config.replace_default_tokens {
        Vec::new()
    } else {
        default_token_list()
    };
    for token in &config.tokens {
        let address = parse_config_address(&token.address)
            .with_context(|| format!("parse token {} address", token.symbol))?;
        upsert_token_info(
            &mut tokens,
            TokenInfo::new(&token.symbol, address, token.decimals),
        );
    }
    if tokens.is_empty() {
        bail!("TUI token list is empty");
    }
    Ok(tokens)
}

fn default_token_list() -> Vec<TokenInfo> {
    vec![
        TokenInfo::new("USDC", USDC, 6),
        TokenInfo::new("WETH", WETH, 18),
        TokenInfo::new("DAI", DAI, 18),
        TokenInfo::new("WBTC", WBTC, 8),
        TokenInfo::new("USDT", USDT, 6),
        TokenInfo::new("USDe", USDE, 18),
        TokenInfo::new("sUSDe", SUSDE, 18),
        TokenInfo::new("LINK", LINK, 18),
        TokenInfo::new("UNI", UNI, 18),
        TokenInfo::new("AAVE", AAVE, 18),
        TokenInfo::new("MKR", MKR, 18),
        TokenInfo::new("CRV", CRV, 18),
        TokenInfo::new("LDO", LDO, 18),
        TokenInfo::new("FRAX", FRAX, 18),
        TokenInfo::new("SUSHI", SUSHI, 18),
        TokenInfo::new("PEPE", PEPE, 18),
    ]
}

fn upsert_token_info(tokens: &mut Vec<TokenInfo>, token: TokenInfo) {
    if let Some(existing) = tokens
        .iter_mut()
        .find(|existing| existing.address == token.address)
    {
        *existing = token;
    } else {
        tokens.push(token);
    }
}

fn startup_focus_pair(tokens: &[TokenInfo]) -> Option<(Address, Address)> {
    let input_index = env_token_index(tokens, "AMM_ROUTE_TUI_INPUT")
        .or_else(|| tokens.iter().position(|token| token.symbol == "USDC"))?;
    let output_index = env_token_index(tokens, "AMM_ROUTE_TUI_OUTPUT")
        .or_else(|| tokens.iter().position(|token| token.symbol == "WETH"))?;
    if input_index == output_index {
        None
    } else {
        Some((tokens[input_index].address, tokens[output_index].address))
    }
}

fn discover_initial_pools(
    registry: &AdapterRegistry,
    cache: &mut EvmCache,
    tokens: &[TokenInfo],
    focus: Option<(Address, Address)>,
    config: &TuiUserConfig,
) -> Result<Vec<PoolInfo>> {
    let discovery = PoolDiscovery::for_registry(registry, factory_config());
    let discovered = discovery
        .find(
            cache,
            PoolQuery::basket(tokens.iter().map(|token| token.address)),
        )
        .context("factory basket discovery")?;
    let mut infos = discovered
        .into_iter()
        .map(|pool| pool_info_from_registration(pool.registration, tokens))
        .collect::<Vec<_>>();
    infos.extend(manual_config_pools(config, tokens)?);
    dedup_pool_infos(&mut infos);
    if let Some((token_in, token_out)) = focus {
        sort_pool_infos_for_focus(&mut infos, token_in, token_out);
    } else {
        sort_pool_infos(&mut infos);
    }

    let max_pools = env_usize("AMM_ROUTE_TUI_MAX_POOLS", DEFAULT_MAX_STARTUP_POOLS);
    if max_pools > 0 && infos.len() > max_pools {
        infos.truncate(max_pools);
    }
    Ok(infos)
}

fn filter_connected_tokens(
    tokens: Vec<TokenInfo>,
    registry: &AdapterRegistry,
) -> (Vec<TokenInfo>, usize) {
    let connected = connected_token_addresses(registry);
    let original_len = tokens.len();
    let tokens = tokens
        .into_iter()
        .filter(|token| connected.contains(&token.address))
        .collect::<Vec<_>>();
    let dropped = original_len.saturating_sub(tokens.len());
    (tokens, dropped)
}

fn connected_token_addresses(registry: &AdapterRegistry) -> HashSet<Address> {
    let graph_report = AmmGraph::from_registry(registry, GraphBuildOptions::default());
    let mut connected = HashSet::new();
    for edge in graph_report.graph.graph().edge_references() {
        if let Some(source) = graph_report.graph.node_token(edge.source()) {
            connected.insert(source);
        }
        if let Some(target) = graph_report.graph.node_token(edge.target()) {
            connected.insert(target);
        }
    }
    connected
}

fn manual_config_pools(config: &TuiUserConfig, tokens: &[TokenInfo]) -> Result<Vec<PoolInfo>> {
    let mut pools = default_manual_curve_pools();
    for pool in &config.curve_pools {
        pools.push(user_curve_pool(pool)?);
    }
    for pool in &config.pools {
        pools.push(user_pool(pool, tokens)?);
    }
    Ok(pools)
}

fn default_manual_curve_pools() -> Vec<PoolInfo> {
    vec![
        curve_pool(
            "Curve 3pool DAI/USDC/USDT",
            CURVE_3POOL,
            [DAI, USDC, USDT],
            CurveVariant::StableSwap,
            curve_3pool_read_set(),
        ),
        curve_pool(
            "Curve FRAX/USDC",
            CURVE_FRAX_USDC,
            [FRAX, USDC],
            CurveVariant::StableSwap,
            curve_frax_usdc_read_set(),
        ),
        curve_pool(
            "Curve tricryptoUSDC-ng USDC/WBTC/WETH",
            TRICRYPTO_USDC_NG,
            [USDC, WBTC, WETH],
            CurveVariant::CryptoSwapNG,
            curve_tricrypto_usdc_ng_read_set(),
        ),
    ]
}

fn curve_pool(
    label: &str,
    pool: Address,
    coins: impl IntoIterator<Item = Address>,
    variant: CurveVariant,
    discovered_slots: impl IntoIterator<Item = U256>,
) -> PoolInfo {
    PoolInfo {
        label: label.to_owned(),
        registration: PoolRegistration::new(PoolKey::Curve(pool))
            .with_state_address(pool)
            .with_metadata(ProtocolMetadata::Curve(
                CurveMetadata::default()
                    .with_coins(coins)
                    .with_variant(variant)
                    .with_discovered_slots(discovered_slots),
            )),
    }
}

fn curve_3pool_read_set() -> Vec<U256> {
    vec![
        U256::from(2),
        U256::from(7),
        U256::from(9),
        U256::from_be_slice(&hex!(
            "b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6"
        )),
        U256::from_be_slice(&hex!(
            "b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf7"
        )),
        U256::from_be_slice(&hex!(
            "b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf8"
        )),
    ]
}

fn curve_frax_usdc_read_set() -> Vec<U256> {
    [3_u64, 4, 5, 10, 12].into_iter().map(U256::from).collect()
}

fn curve_tricrypto_usdc_ng_read_set() -> Vec<U256> {
    [1_u64, 2, 3, 9, 10, 11, 12, 13, 14, 20, 25]
        .into_iter()
        .map(U256::from)
        .collect()
}

fn user_curve_pool(pool: &UserCurvePoolConfig) -> Result<PoolInfo> {
    let address = parse_config_address(&pool.address)
        .with_context(|| format!("parse curve pool {} address", pool.label))?;
    let coins = pool
        .coins
        .iter()
        .map(|coin| parse_config_address(coin))
        .collect::<Result<Vec<_>>>()
        .with_context(|| format!("parse curve pool {} coins", pool.label))?;
    if coins.len() < 2 {
        bail!("curve pool {} must list at least two coins", pool.label);
    }
    let variant = parse_curve_variant(pool.variant.as_deref())?;
    let discovered_slots = pool
        .discovered_slots
        .iter()
        .map(|slot| parse_config_u256(slot))
        .collect::<Result<Vec<_>>>()
        .with_context(|| format!("parse curve pool {} discovered slots", pool.label))?;
    Ok(curve_pool(
        &pool.label,
        address,
        coins,
        variant,
        discovered_slots,
    ))
}

fn user_pool(pool: &UserPoolConfig, tokens: &[TokenInfo]) -> Result<PoolInfo> {
    let address = parse_config_address(&pool.address)
        .with_context(|| format!("parse pool {} address", pool_label_from_config(pool)))?;
    let token_addresses = pool
        .tokens
        .as_ref()
        .map(|configured| {
            configured
                .iter()
                .map(|token| parse_config_address(token))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
        .with_context(|| format!("parse pool {} tokens", pool_label_from_config(pool)))?;
    let protocol = pool.protocol.trim().to_ascii_lowercase();
    let registration = match protocol.as_str() {
        "curve"
        | "curve_stable"
        | "curve_stableswap"
        | "curve_crypto"
        | "curve_cryptoswap"
        | "curve_crypto_ng"
        | "curve_cryptoswap_ng" => {
            let coins = token_addresses.ok_or_else(|| {
                anyhow::anyhow!(
                    "manual curve pool {} must set tokens",
                    pool_label_from_config(pool)
                )
            })?;
            if coins.len() < 2 {
                bail!(
                    "manual curve pool {} must list at least two tokens",
                    pool_label_from_config(pool)
                );
            }
            let variant = parse_curve_variant(
                pool.variant
                    .as_deref()
                    .or_else(|| protocol.strip_prefix("curve_")),
            )?;
            PoolRegistration::new(PoolKey::Curve(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::Curve(
                    CurveMetadata::default()
                        .with_coins(coins)
                        .with_variant(variant),
                ))
        }
        "uniswap_v2" | "sushiswap_v2" | "v2" => {
            let mut metadata =
                UniswapV2Metadata::default().with_fee_bps(pool.fee_bps.unwrap_or(30));
            if let Some(pair) = token_pair(&token_addresses, pool)? {
                metadata = metadata.with_token0(pair.0).with_token1(pair.1);
            }
            PoolRegistration::new(PoolKey::UniswapV2(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::UniswapV2(metadata))
        }
        "uniswap_v3" | "v3" => {
            let metadata = v3_metadata(pool, token_addresses, None)?;
            PoolRegistration::new(PoolKey::UniswapV3(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::UniswapV3(metadata))
        }
        "pancake_v3" | "pancakeswap_v3" => {
            let metadata = v3_metadata(pool, token_addresses, Some(PANCAKESWAP_V3_FACTORY))?;
            PoolRegistration::new(PoolKey::PancakeV3(address))
                .with_state_address(address)
                .with_metadata(ProtocolMetadata::PancakeV3(metadata))
        }
        other => bail!("unsupported manual pool protocol {other}"),
    };

    Ok(PoolInfo {
        label: pool
            .label
            .clone()
            .unwrap_or_else(|| registration_label(&registration, tokens)),
        registration,
    })
}

fn token_pair(
    configured: &Option<Vec<Address>>,
    pool: &UserPoolConfig,
) -> Result<Option<(Address, Address)>> {
    let Some(tokens) = configured else {
        return Ok(None);
    };
    if tokens.len() != 2 {
        bail!(
            "manual {} pool {} must list exactly two tokens",
            pool.protocol,
            pool_label_from_config(pool)
        );
    }
    Ok(Some((tokens[0], tokens[1])))
}

fn v3_metadata(
    pool: &UserPoolConfig,
    configured_tokens: Option<Vec<Address>>,
    factory: Option<Address>,
) -> Result<V3Metadata> {
    let fee = pool.fee.or(pool.fee_bps.map(|bps| bps * 100));
    let Some(fee) = fee else {
        bail!(
            "manual {} pool {} must set fee, e.g. fee = 500 for 0.05%",
            pool.protocol,
            pool_label_from_config(pool)
        );
    };
    let mut metadata = V3Metadata::default().with_fee(fee);
    if let Some(factory) = factory {
        metadata = metadata.with_factory(factory);
    }
    if let Some(pair) = token_pair(&configured_tokens, pool)? {
        metadata = metadata.with_token0(pair.0).with_token1(pair.1);
    }
    Ok(metadata)
}

fn parse_curve_variant(value: Option<&str>) -> Result<CurveVariant> {
    match value
        .unwrap_or("stable")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "stable" | "stableswap" => Ok(CurveVariant::StableSwap),
        "crypto" | "cryptoswap" => Ok(CurveVariant::CryptoSwap),
        "crypto_ng" | "cryptoswap_ng" | "ng" => Ok(CurveVariant::CryptoSwapNG),
        other => bail!("unsupported curve variant {other}"),
    }
}

fn pool_label_from_config(pool: &UserPoolConfig) -> String {
    pool.label
        .clone()
        .unwrap_or_else(|| format!("{} {}", pool.protocol, pool.address))
}

fn parse_config_address(value: &str) -> Result<Address> {
    Address::from_str(value.trim()).with_context(|| format!("invalid address {value}"))
}

fn parse_config_u256(value: &str) -> Result<U256> {
    let value = value.trim();
    let (digits, radix) = value
        .strip_prefix("0x")
        .map_or((value, 10), |digits| (digits, 16));
    U256::from_str_radix(digits, radix).with_context(|| format!("parse U256 value {value}"))
}

fn pool_info_from_registration(registration: PoolRegistration, tokens: &[TokenInfo]) -> PoolInfo {
    PoolInfo {
        label: registration_label(&registration, tokens),
        registration,
    }
}

fn registration_label(registration: &PoolRegistration, tokens: &[TokenInfo]) -> String {
    let token_path = registration
        .tokens()
        .map(|pool_tokens| {
            pool_tokens
                .into_iter()
                .map(|token| token_symbol(tokens, token))
                .collect::<Vec<_>>()
                .join("/")
        })
        .unwrap_or_else(|| {
            registration
                .key
                .address()
                .map(short_address)
                .unwrap_or_else(|| format!("{:?}", registration.key))
        });
    let fee = match &registration.metadata {
        ProtocolMetadata::UniswapV2(metadata) => metadata
            .fee_bps
            .map(|fee_bps| format!(" {fee_bps}bps"))
            .unwrap_or_default(),
        ProtocolMetadata::UniswapV3(metadata)
        | ProtocolMetadata::PancakeV3(metadata)
        | ProtocolMetadata::Slipstream(metadata) => metadata
            .fee
            .map(|fee| format!(" {}", format_v3_fee(fee)))
            .unwrap_or_default(),
        _ => String::new(),
    };
    format!("{} {}{}", protocol_label(registration), token_path, fee)
}

fn protocol_label(registration: &PoolRegistration) -> &'static str {
    match registration.key {
        PoolKey::UniswapV2(_) => "V2",
        PoolKey::UniswapV3(_) => match v3_factory(registration) {
            Some(factory) if factory == UNISWAP_V3_FACTORY => "Uniswap V3",
            Some(factory) if factory == SUSHISWAP_V3_FACTORY => "Sushi V3",
            _ => "V3",
        },
        PoolKey::PancakeV3(_) => "Pancake V3",
        PoolKey::Slipstream(_) => "Slipstream",
        PoolKey::SolidlyV2(_) => "SolidlyV2",
        PoolKey::BalancerV2(_) => "BalancerV2",
        PoolKey::Curve(_) => "Curve",
        PoolKey::Custom(_) => "Custom",
        _ => "Pool",
    }
}

fn v3_factory(registration: &PoolRegistration) -> Option<Address> {
    match &registration.metadata {
        ProtocolMetadata::UniswapV3(metadata)
        | ProtocolMetadata::PancakeV3(metadata)
        | ProtocolMetadata::Slipstream(metadata) => metadata.factory,
        _ => None,
    }
}

fn format_v3_fee(fee: u32) -> String {
    let mut text = format!("{:.4}", fee as f64 / 10_000.0);
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    format!("{text}%")
}

fn dedup_pool_infos(infos: &mut Vec<PoolInfo>) {
    let mut seen = HashSet::new();
    infos.retain(|info| seen.insert(info.registration.key.clone()));
}

fn sort_pool_infos(infos: &mut [PoolInfo]) {
    infos.sort_by(|a, b| {
        pool_priority(a)
            .cmp(&pool_priority(b))
            .then_with(|| a.label.cmp(&b.label))
    });
}

fn sort_pool_infos_for_focus(infos: &mut [PoolInfo], token_in: Address, token_out: Address) {
    infos.sort_by(|a, b| {
        pool_focus_priority(a, token_in, token_out)
            .cmp(&pool_focus_priority(b, token_in, token_out))
            .then_with(|| a.label.cmp(&b.label))
    });
}

fn pool_focus_priority(info: &PoolInfo, token_in: Address, token_out: Address) -> (u8, u8, u8, u8) {
    let tokens = info.registration.tokens().unwrap_or_default();
    let has_input = tokens.contains(&token_in);
    let has_output = tokens.contains(&token_out);
    let selected_rank = match (has_input, has_output) {
        (true, true) => 0,
        (true, false) | (false, true) => 1,
        (false, false) => 2,
    };
    let connector_rank = if tokens
        .iter()
        .any(|token| [WETH, USDC, USDT, DAI, WBTC].contains(token))
    {
        0
    } else {
        1
    };
    let protocol_rank = match info.registration.key {
        PoolKey::PancakeV3(_) => 0,
        PoolKey::UniswapV3(_) => 1,
        PoolKey::Curve(_) => 2,
        PoolKey::UniswapV2(_) => 3,
        _ => 4,
    };
    let token_count = tokens.len().min(u8::MAX as usize) as u8;
    (
        selected_rank,
        connector_rank,
        protocol_rank,
        u8::MAX.saturating_sub(token_count),
    )
}

fn pool_priority(info: &PoolInfo) -> (u8, u8, u8) {
    let tokens = info.registration.tokens().unwrap_or_default();
    let has_weth = tokens.contains(&WETH);
    let has_core_stable = tokens.iter().any(|token| [USDC, USDT, DAI].contains(token));
    let core_count = tokens
        .iter()
        .filter(|token| [WETH, USDC, USDT, DAI, WBTC].contains(token))
        .count() as u8;
    let route_rank = match (has_weth, has_core_stable) {
        (true, true) => 0,
        (true, false) | (false, true) => 1,
        (false, false) => 2,
    };
    let protocol_rank = match info.registration.key {
        PoolKey::PancakeV3(_) => 0,
        PoolKey::Curve(_) => 0,
        PoolKey::UniswapV3(_) => 1,
        PoolKey::UniswapV2(_) => 2,
        _ => 3,
    };
    (route_rank, 10_u8.saturating_sub(core_count), protocol_rank)
}

fn connector_addresses(tokens: &[TokenInfo], address: Address) -> Vec<Address> {
    let mut connectors = Vec::new();
    let mut seen = HashSet::new();
    for connector in [WETH, USDC, USDT, DAI, WBTC] {
        if connector != address && seen.insert(connector) {
            connectors.push(connector);
        }
    }
    for token in tokens {
        if token.address != address && seen.insert(token.address) {
            connectors.push(token.address);
        }
    }
    let max_connectors = env_usize("AMM_ROUTE_TUI_CONNECTORS", DEFAULT_DYNAMIC_CONNECTORS);
    if max_connectors > 0 && connectors.len() > max_connectors {
        connectors.truncate(max_connectors);
    }
    connectors
}

fn focused_discovery_connectors(
    tokens: &[TokenInfo],
    selected: Address,
    counterpart: Address,
) -> Vec<Address> {
    let mut connectors = connector_addresses(tokens, selected);
    if counterpart != selected && !connectors.contains(&counterpart) {
        connectors.push(counterpart);
    }
    connectors
}

fn background_discovery_requests(tokens: &[TokenInfo]) -> Vec<TokenEdgeDiscoveryRequest> {
    let addresses = tokens
        .iter()
        .map(|token| token.address)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    addresses
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, token)| {
            let connectors = addresses
                .iter()
                .copied()
                .skip(index + 1)
                .collect::<Vec<_>>();
            (!connectors.is_empty()).then(|| TokenEdgeDiscoveryRequest::new(token, connectors))
        })
        .collect()
}

fn background_discovery_options() -> AmmDiscoveryOptions {
    AmmDiscoveryOptions::default().with_class(AmmWorkClass::Deferred)
}

async fn run_idle_background_discovery(
    amm: AmmRuntimeHandle,
    owners: Vec<(ProtocolId, DiscoveryOwnerId)>,
    tokens: Vec<TokenInfo>,
) {
    let mut status = amm.subscribe_status();
    for request in background_discovery_requests(&tokens) {
        for (protocol, owner) in &owners {
            loop {
                while status
                    .borrow_and_update()
                    .active_work_items()
                    .next()
                    .is_some()
                {
                    if status.changed().await.is_err() {
                        return;
                    }
                }
                let queued = amm
                    .queue_token_discovery(
                        owner.clone(),
                        request.clone().with_protocol(*protocol),
                        background_discovery_options(),
                    )
                    .await;
                match queued {
                    Ok(_) => {
                        if status.changed().await.is_err() {
                            return;
                        }
                        break;
                    }
                    Err(error) if error.to_string().contains("full") => {
                        if status.changed().await.is_err() {
                            return;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn is_ready_outcome(outcome: &ColdStartOutcome) -> bool {
    matches!(
        outcome,
        ColdStartOutcome::Ready(_) | ColdStartOutcome::ReadyWithDeferred(_, _)
    )
}

fn cycle_index(index: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }
    let len = len as isize;
    ((index as isize + delta).rem_euclid(len)) as usize
}

fn token_by_address(tokens: &[TokenInfo], address: Address) -> TokenInfo {
    tokens
        .iter()
        .find(|token| token.address == address)
        .cloned()
        .unwrap_or_else(|| TokenInfo::new(&short_address(address), address, 18))
}

fn token_symbol(tokens: &[TokenInfo], address: Address) -> String {
    tokens
        .iter()
        .find(|token| token.address == address)
        .map(|token| token.symbol.clone())
        .unwrap_or_else(|| short_address(address))
}

fn pool_label(pools: &[PoolInfo], key: &PoolKey) -> String {
    pools
        .iter()
        .find(|pool| pool.registration.key == *key)
        .map(|pool| pool.label.clone())
        .unwrap_or_else(|| format!("{key:?}"))
}

async fn fetch_price_book(tokens: &[TokenInfo], previous: PriceBook) -> Result<PriceBook> {
    if !env_bool("AMM_ROUTE_TUI_PRICES", true) {
        return Ok(PriceBook::disabled());
    }

    let source = price_source();
    match source.as_str() {
        "none" | "off" | "disabled" => Ok(PriceBook::disabled()),
        "coingecko" => fetch_coingecko_prices(tokens, previous).await,
        other => bail!("unsupported price source {other}"),
    }
}

async fn fetch_coingecko_prices(tokens: &[TokenInfo], previous: PriceBook) -> Result<PriceBook> {
    let mut seen = HashSet::new();
    let addresses = tokens
        .iter()
        .filter_map(|token| {
            let address = format!("{:#x}", token.address).to_ascii_lowercase();
            seen.insert(address.clone()).then_some(address)
        })
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Ok(PriceBook::disabled());
    }

    let settings = coingecko_price_settings();
    let client = reqwest::Client::builder()
        .timeout(settings.timeout)
        .build()
        .context("build CoinGecko client")?;

    let mut usd_by_token = previous.usd_by_token;
    let mut last_error = None;
    let mut fetched = 0_usize;
    let limit = settings.requests_per_refresh.min(addresses.len());
    for (index, address) in addresses.into_iter().take(limit).enumerate() {
        if index > 0 && !settings.request_delay.is_zero() {
            tokio::time::sleep(settings.request_delay).await;
        }
        match fetch_coingecko_token_price(&client, &settings, &address).await {
            Ok(Some((address, usd))) => {
                usd_by_token.insert(address, usd);
                fetched += 1;
            }
            Ok(None) => {}
            Err(error) => {
                let error = error.to_string();
                let rate_limited = error.contains("429");
                last_error = Some(error);
                if rate_limited {
                    break;
                }
            }
        }
    }

    if fetched == 0 && usd_by_token.is_empty() {
        let error = last_error
            .clone()
            .unwrap_or_else(|| "no CoinGecko prices returned".to_owned());
        bail!("{error}");
    }

    Ok(PriceBook {
        usd_by_token,
        source: match settings.api_key.as_ref().map(|key| key.tier) {
            Some("pro") => "coingecko-pro".to_owned(),
            Some("demo") => "coingecko-demo".to_owned(),
            _ => "coingecko-keyless".to_owned(),
        },
        last_updated: (fetched > 0)
            .then_some(Instant::now())
            .or(previous.last_updated),
        last_error,
    })
}

async fn fetch_coingecko_token_price(
    client: &reqwest::Client,
    settings: &CoingeckoPriceSettings,
    address: &str,
) -> Result<Option<(Address, f64)>> {
    let mut request = client.get(&settings.base_url).query(&[
        ("contract_addresses", address.to_owned()),
        ("vs_currencies", "usd".to_owned()),
    ]);
    if let Some(api_key) = &settings.api_key {
        request = request.header(api_key.header_name, &api_key.value);
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("fetch CoinGecko token price for {address}"))?;
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(|value| format!(" retry_after={value}s"))
            .unwrap_or_default();
        bail!("CoinGecko HTTP 429 rate limited{retry_after}");
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "CoinGecko HTTP {}: {}",
            status.as_u16(),
            fit_to_width(&body, 160)
        );
    }

    let body = response
        .json::<HashMap<String, CoingeckoTokenPrice>>()
        .await
        .with_context(|| format!("decode CoinGecko token price for {address}"))?;
    for (address, price) in body {
        let Ok(address) = Address::from_str(&address) else {
            continue;
        };
        let Some(usd) = price.usd else {
            continue;
        };
        if usd.is_finite() && usd > 0.0 {
            return Ok(Some((address, usd)));
        }
    }
    Ok(None)
}

fn coingecko_price_settings() -> CoingeckoPriceSettings {
    let api_key = coingecko_api_key();
    let keyed = api_key.is_some();
    let default_requests = if keyed {
        DEFAULT_KEYED_COINGECKO_REQUESTS_PER_REFRESH
    } else {
        DEFAULT_KEYLESS_COINGECKO_REQUESTS_PER_REFRESH
    };
    let default_delay_ms = if keyed {
        DEFAULT_KEYED_COINGECKO_REQUEST_DELAY_MS
    } else {
        DEFAULT_KEYLESS_COINGECKO_REQUEST_DELAY_MS
    };
    CoingeckoPriceSettings {
        base_url: std::env::var("AMM_ROUTE_TUI_COINGECKO_URL").unwrap_or_else(|_| {
            if api_key.as_ref().is_some_and(|key| key.tier == "pro") {
                "https://pro-api.coingecko.com/api/v3/simple/token_price/ethereum".to_owned()
            } else {
                "https://api.coingecko.com/api/v3/simple/token_price/ethereum".to_owned()
            }
        }),
        timeout: Duration::from_secs(env_usize("AMM_ROUTE_TUI_PRICE_TIMEOUT_SECS", 5) as u64),
        request_delay: Duration::from_millis(env_usize(
            "AMM_ROUTE_TUI_COINGECKO_REQUEST_DELAY_MS",
            default_delay_ms,
        ) as u64),
        requests_per_refresh: env_usize(
            "AMM_ROUTE_TUI_COINGECKO_REQUESTS_PER_REFRESH",
            default_requests,
        )
        .max(1),
        api_key,
    }
}

fn coingecko_api_key() -> Option<CoingeckoApiKey> {
    std::env::var("AMM_ROUTE_TUI_COINGECKO_PRO_API_KEY")
        .ok()
        .or_else(|| std::env::var("COINGECKO_PRO_API_KEY").ok())
        .map(|key| key.trim().to_owned())
        .filter(|key| !key.is_empty())
        .map(|value| CoingeckoApiKey {
            value,
            header_name: "x-cg-pro-api-key",
            tier: "pro",
        })
        .or_else(|| {
            std::env::var("AMM_ROUTE_TUI_COINGECKO_API_KEY")
                .ok()
                .or_else(|| std::env::var("COINGECKO_API_KEY").ok())
                .map(|key| key.trim().to_owned())
                .filter(|key| !key.is_empty())
                .map(|value| CoingeckoApiKey {
                    value,
                    header_name: "x-cg-demo-api-key",
                    tier: "demo",
                })
        })
}

fn price_source() -> String {
    std::env::var("AMM_ROUTE_TUI_PRICE_SOURCE")
        .unwrap_or_else(|_| DEFAULT_PRICE_SOURCE.to_owned())
        .trim()
        .to_ascii_lowercase()
}

fn format_amount_with_token(
    value: U256,
    token: &TokenInfo,
    prices: &PriceBook,
    max_fractional: usize,
) -> String {
    let amount = format_units(value, token.decimals, max_fractional);
    match prices
        .usd_by_token
        .get(&token.address)
        .and_then(|price| amount_usd(value, token.decimals, *price))
    {
        Some(usd) => format!("{amount} {} ({})", token.symbol, format_usd(usd)),
        None => format!("{amount} {}", token.symbol),
    }
}

fn amount_usd(value: U256, decimals: u8, price: f64) -> Option<f64> {
    let raw = value.to_string().parse::<f64>().ok()?;
    let amount = raw / 10_f64.powi(decimals as i32);
    let usd = amount * price;
    usd.is_finite().then_some(usd)
}

fn economic_value_warning(
    amount_in: U256,
    token_in: &TokenInfo,
    amount_out: U256,
    token_out: &TokenInfo,
    prices: &PriceBook,
    max_loss_bps: u16,
) -> Option<String> {
    let input_usd = prices
        .usd_by_token
        .get(&token_in.address)
        .and_then(|price| amount_usd(amount_in, token_in.decimals, *price))?;
    let output_usd = prices
        .usd_by_token
        .get(&token_out.address)
        .and_then(|price| amount_usd(amount_out, token_out.decimals, *price))?;
    if input_usd <= 0.0 || output_usd >= input_usd {
        return None;
    }
    let loss_fraction = 1.0 - output_usd / input_usd;
    let loss_bps = (loss_fraction * 10_000.0).round().clamp(0.0, 10_000.0) as u16;
    if loss_bps < max_loss_bps {
        return None;
    }
    Some(format!(
        "UNSAFE ROUTE  estimated value loss {:.1}% ({} -> {}); graph may be incomplete or liquidity insufficient",
        loss_fraction * 100.0,
        format_usd(input_usd),
        format_usd(output_usd),
    ))
}

fn format_usd(value: f64) -> String {
    if !value.is_finite() {
        return "$?".to_owned();
    }
    if value.abs() >= 1_000.0 {
        return format!("${}", comma_digits(&format!("{value:.0}")));
    }
    if value.abs() >= 1.0 {
        return format!("${}", comma_decimal(&format!("{value:.2}")));
    }
    if value.abs() >= 0.01 {
        return format!("${}", comma_decimal(&format!("{value:.4}")));
    }
    format!("${}", comma_decimal(&format!("{value:.6}")))
}

fn etherscan_token_url(address: Address) -> String {
    format!("https://etherscan.io/token/{address:#x}")
}

fn etherscan_address_url(address: Address) -> String {
    format!("https://etherscan.io/address/{address:#x}")
}

fn short_address(address: Address) -> String {
    let value = format!("{address:?}");
    if value.len() <= 12 {
        return value;
    }
    format!("{}...{}", &value[..6], &value[value.len() - 4..])
}

fn tenderly_ui_state_from_env() -> TenderlyUiState {
    let api_key = env_first(&["TENDERLY_API_KEY", "TENDERLY_ACCESS_KEY"]);
    let account_slug = env_first(&["TENDERLY_ACCOUNT_SLUG"]);
    let project_slug = env_first(&["TENDERLY_PROJECT_SLUG"]);
    let from = match env_first(&["AMM_ROUTE_TUI_SIMULATE_FROM", "TENDERLY_SIMULATE_FROM"]) {
        Some(value) => match Address::from_str(value.trim()) {
            Ok(address) if address != Address::ZERO => address,
            Ok(_) => {
                return TenderlyUiState {
                    config: None,
                    status: "Tenderly disabled: simulate-from address cannot be zero".to_owned(),
                    in_flight: false,
                    ok: None,
                };
            }
            Err(error) => {
                return TenderlyUiState {
                    config: None,
                    status: format!("Tenderly disabled: invalid simulate-from address ({error})"),
                    in_flight: false,
                    ok: None,
                };
            }
        },
        None => DEFAULT_SIMULATE_FROM,
    };

    match (api_key, account_slug, project_slug) {
        (Some(api_key), Some(account_slug), Some(project_slug)) => {
            let config = TenderlyConfig {
                api_key,
                account_slug,
                project_slug,
                from,
            };
            TenderlyUiState {
                status: format!("ready; from={}", address_hex(from)),
                config: Some(config),
                in_flight: false,
                ok: None,
            }
        },
        (None, None, None) => TenderlyUiState {
            config: None,
            status: "Tenderly disabled; set TENDERLY_API_KEY, TENDERLY_ACCOUNT_SLUG, TENDERLY_PROJECT_SLUG".to_owned(),
            in_flight: false,
            ok: None,
        },
        (api_key, account_slug, project_slug) => {
            let mut missing = Vec::new();
            if api_key.is_none() {
                missing.push("TENDERLY_API_KEY");
            }
            if account_slug.is_none() {
                missing.push("TENDERLY_ACCOUNT_SLUG");
            }
            if project_slug.is_none() {
                missing.push("TENDERLY_PROJECT_SLUG");
            }
            TenderlyUiState {
                config: None,
                status: format!("Tenderly setup incomplete; set {}", missing.join(", ")),
                in_flight: false,
                ok: None,
            }
        }
    }
}

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

fn tenderly_credentials_configured() -> bool {
    [
        &["TENDERLY_API_KEY", "TENDERLY_ACCESS_KEY"][..],
        &["TENDERLY_ACCOUNT_SLUG"][..],
        &["TENDERLY_PROJECT_SLUG"][..],
    ]
    .into_iter()
    .all(|names| env_first(names).is_some())
}

fn address_hex(address: Address) -> String {
    format!("{address:#x}")
}

fn b256_hex(value: B256) -> String {
    format!("{value:#x}")
}

fn bytes_hex(bytes: &Bytes) -> String {
    format!("0x{}", hex::encode(bytes.as_ref()))
}

fn u256_word_hex(value: U256) -> String {
    format!("0x{}", hex::encode(value.to_be_bytes::<32>()))
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => Some(true),
            "0" | "false" | "no" | "n" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn gas_estimates_enabled() -> bool {
    if std::env::var_os("AMM_ROUTE_TUI_GAS_ESTIMATES").is_some() {
        env_bool("AMM_ROUTE_TUI_GAS_ESTIMATES", true)
    } else {
        // Compatibility with the original benchmark documentation.
        env_bool("AMM_ROUTE_TUI_GAS", true)
    }
}

fn prepare_live_gas_router(cache: &mut EvmCache, enabled: bool) -> (bool, String) {
    if !enabled {
        return (false, "gas estimates disabled".to_owned());
    }
    match install_demo_router(cache) {
        Ok(code_hash) => (
            true,
            format!("route execution runtime ready ({code_hash:?})"),
        ),
        Err(error) => (false, format!("route execution unavailable: {error:#}")),
    }
}

fn prepare_live_gas_tokens(
    cache: &mut EvmCache,
    tokens: &[TokenInfo],
) -> (HashMap<Address, TrackedMapping>, String) {
    let mut ready = HashMap::new();
    let mut failures = Vec::new();
    for token in tokens {
        match cache.track_erc20_balances(token.address, [DEMO_ROUTER]) {
            Ok(Some((mapping, _))) => {
                match prewarm_demo_router_token_transfer(cache, token.address, &mapping) {
                    Ok(()) => {
                        ready.insert(token.address, mapping);
                    }
                    Err(error) => failures.push(format!("{} transfer: {error}", token.symbol)),
                }
            }
            Ok(None) => failures.push(format!(
                "{} has no discoverable balance layout",
                token.symbol
            )),
            Err(error) => failures.push(format!("{}: {error}", token.symbol)),
        }
    }
    let status = if failures.is_empty() {
        format!(
            "route execution validation ready for {}/{} tokens",
            ready.len(),
            tokens.len()
        )
    } else {
        format!(
            "route execution validation ready for {}/{} tokens; {} unavailable",
            ready.len(),
            tokens.len(),
            failures.len()
        )
    };
    (ready, status)
}

fn tui_cache_dir() -> PathBuf {
    std::env::var("AMM_ROUTE_TUI_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_TUI_CACHE_DIR))
}

fn env_bps(name: &str) -> Option<u16> {
    let value = std::env::var(name).ok()?.parse::<u16>().ok()?;
    (1..=10_000).contains(&value).then_some(value)
}

fn format_bps(bps: u16) -> String {
    format!("{}.{:02}%", bps / 100, bps % 100)
}

fn format_gwei(wei: u128) -> String {
    let gwei = wei as f64 / 1_000_000_000.0;
    if gwei >= 100.0 {
        format!("{gwei:.0}")
    } else if gwei >= 10.0 {
        format!("{gwei:.1}")
    } else {
        format!("{gwei:.2}")
    }
}

fn env_token_index(tokens: &[TokenInfo], name: &str) -> Option<usize> {
    let selector = std::env::var(name).ok()?;
    token_index(tokens, &selector)
}

fn token_index(tokens: &[TokenInfo], selector: &str) -> Option<usize> {
    let selector = selector.trim();
    if selector.is_empty() {
        return None;
    }
    if let Ok(address) = Address::from_str(selector) {
        return tokens.iter().position(|token| token.address == address);
    }
    tokens
        .iter()
        .position(|token| token.symbol.eq_ignore_ascii_case(selector))
}

fn parse_units(value: &str, decimals: u8) -> Result<U256, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("amount is empty".to_owned());
    }
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty() && fractional.is_empty() {
        return Err("amount is empty".to_owned());
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) || !fractional.chars().all(|c| c.is_ascii_digit())
    {
        return Err("amount must be a positive decimal number".to_owned());
    }
    if fractional.len() > decimals as usize {
        return Err(format!(
            "amount has too many decimals for token precision ({decimals})"
        ));
    }

    let whole = if whole.is_empty() {
        U256::ZERO
    } else {
        U256::from_str_radix(whole, 10).map_err(|error| error.to_string())?
    };
    let scale = ten_pow(decimals);
    let mut fractional_padded = fractional.to_owned();
    while fractional_padded.len() < decimals as usize {
        fractional_padded.push('0');
    }
    let fractional = if fractional_padded.is_empty() {
        U256::ZERO
    } else {
        U256::from_str_radix(&fractional_padded, 10).map_err(|error| error.to_string())?
    };
    Ok(whole * scale + fractional)
}

fn format_units(value: U256, decimals: u8, max_fractional: usize) -> String {
    let plain = format_units_plain(value, decimals, max_fractional);
    comma_decimal(&plain)
}

fn format_units_plain(value: U256, decimals: u8, max_fractional: usize) -> String {
    if decimals == 0 {
        return value.to_string();
    }

    let scale = ten_pow(decimals);
    let whole = value / scale;
    let fractional = value % scale;
    if fractional.is_zero() {
        return whole.to_string();
    }

    let mut fractional = fractional.to_string();
    while fractional.len() < decimals as usize {
        fractional.insert(0, '0');
    }
    fractional.truncate(max_fractional.min(fractional.len()));
    while fractional.ends_with('0') {
        fractional.pop();
    }
    if fractional.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fractional}")
    }
}

fn comma_decimal(value: &str) -> String {
    let Some((whole, fractional)) = value.split_once('.') else {
        return comma_digits(value);
    };
    if fractional.is_empty() {
        comma_digits(whole)
    } else {
        format!("{}.{}", comma_digits(whole), fractional)
    }
}

fn comma_digits(value: &str) -> String {
    let (sign, digits) = value
        .strip_prefix('-')
        .map(|digits| ("-", digits))
        .unwrap_or(("", value));
    let mut grouped = String::new();
    let len = digits.chars().count();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (len - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    format!("{sign}{grouped}")
}

fn ten_pow(decimals: u8) -> U256 {
    let mut value = U256::from(1);
    for _ in 0..decimals {
        value *= U256::from(10);
    }
    value
}

#[cfg(test)]
mod live_tui_tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    #[test]
    fn bootstrap_does_not_attach_subscriber_while_startup_loads_are_active() {
        let warming = GraphSyncCounts {
            routing_pools: 1,
            loading_pools: 12,
            queued_loads: 40,
            pending_state_updates: 3,
            ..GraphSyncCounts::default()
        };
        assert!(!initial_graph_ready_for_routes(warming, 7, None));
        assert!(!initial_graph_ready_for_routes(warming, 7, Some(false)));
        assert!(!initial_graph_ready_for_routes(warming, 7, Some(true)));

        let no_route_yet = GraphSyncCounts {
            loading_pools: 1,
            ..GraphSyncCounts::default()
        };
        assert!(!initial_graph_ready_for_routes(no_route_yet, 1, None));
        assert!(initial_graph_ready_for_routes(
            GraphSyncCounts::default(),
            0,
            Some(false),
        ));
    }

    #[test]
    fn startup_pool_default_is_bounded_for_interactive_boots() {
        assert_eq!(DEFAULT_MAX_STARTUP_POOLS, 128);
    }

    #[test]
    fn graph_progress_names_the_protocols_and_pool_count_being_loaded() {
        use evm_amm_state::adapters::{
            AmmRuntimeHealth, AmmStateVersion, PoolGeneration, PoolInstanceId, RuntimeLifecycleMap,
        };

        let mut lifecycles = RuntimeLifecycleMap::default();
        for (key, generation, state) in [
            (
                PoolKey::UniswapV2(Address::repeat_byte(0x11)),
                1,
                PoolRuntimeState::Searchable,
            ),
            (
                PoolKey::UniswapV2(Address::repeat_byte(0x12)),
                2,
                PoolRuntimeState::Queued,
            ),
            (
                PoolKey::UniswapV3(Address::repeat_byte(0x13)),
                3,
                PoolRuntimeState::Hydrating,
            ),
            (
                PoolKey::PancakeV3(Address::repeat_byte(0x14)),
                4,
                PoolRuntimeState::CatchingUp,
            ),
        ] {
            lifecycles.set_pool(
                PoolInstanceId::new(key, PoolGeneration::new(generation)),
                state,
            );
        }
        let status = AmmRuntimeStatusSnapshot::new(
            1,
            AmmStateVersion::new(1),
            lifecycles,
            [],
            Default::default(),
            AmmRuntimeHealth::Healthy,
        );
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);

        apply_live_runtime_status(&mut app, &status);

        assert_eq!(
            app.graph_sync.detail,
            "loading 3 pools (Pancake V3=1, V2=1, V3=1); 1 routing"
        );
    }

    #[test]
    fn graph_progress_explains_factory_discovery_work() {
        use evm_amm_state::adapters::{
            AmmRuntimeHealth, AmmStateVersion, AmmWorkKind, AmmWorkProgress, DiscoveryGeneration,
            DiscoveryOwnerId, DiscoveryOwnerKey, RuntimeLifecycleMap, RuntimeOwnerId,
            RuntimeWorkId, WorkId,
        };

        let discovery = RuntimeWorkId::new(
            RuntimeOwnerId::Discovery(DiscoveryOwnerId::new(
                DiscoveryOwnerKey::new("tui-factories"),
                DiscoveryGeneration::new(1),
            )),
            WorkId::new(1),
        );
        let progress = AmmWorkProgress::new(AmmWorkKind::Discovery, 2, Some(6))
            .expect("valid discovery progress");
        let status = AmmRuntimeStatusSnapshot::new(
            1,
            AmmStateVersion::new(1),
            RuntimeLifecycleMap::default(),
            [(discovery, progress)],
            Default::default(),
            AmmRuntimeHealth::Healthy,
        );
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);

        apply_live_runtime_status(&mut app, &status);

        assert_eq!(
            app.graph_sync.detail,
            "finding V2, Uniswap/Sushi V3, and Pancake V3 pools (2/6 discovery steps)"
        );
    }

    #[test]
    fn graph_counts_only_the_latest_generation_of_each_logical_pool() {
        use evm_amm_state::adapters::{
            AmmRuntimeHealth, AmmStateVersion, PoolGeneration, PoolInstanceId, RuntimeLifecycleMap,
        };

        let retried = PoolKey::UniswapV3(Address::repeat_byte(0x21));
        let terminal = PoolKey::UniswapV2(Address::repeat_byte(0x22));
        let mut lifecycles = RuntimeLifecycleMap::default();
        for (key, generation, state) in [
            (retried.clone(), 1, PoolRuntimeState::Failed),
            (retried.clone(), 2, PoolRuntimeState::Failed),
            (retried, 3, PoolRuntimeState::Searchable),
            (terminal, 1, PoolRuntimeState::Failed),
        ] {
            lifecycles.set_pool(
                PoolInstanceId::new(key, PoolGeneration::new(generation)),
                state,
            );
        }
        let status = AmmRuntimeStatusSnapshot::new(
            1,
            AmmStateVersion::new(1),
            lifecycles,
            [],
            Default::default(),
            AmmRuntimeHealth::Healthy,
        );

        let counts = graph_sync_counts(&status);

        assert_eq!(counts.routing_pools, 1);
        assert_eq!(counts.failed_pools, 1);
    }

    #[test]
    fn usable_degraded_graph_is_synced_with_coverage_detail() {
        use evm_amm_state::adapters::{
            AmmRuntimeHealth, AmmStateVersion, PoolGeneration, PoolInstanceId, RuntimeLifecycleMap,
        };

        let mut lifecycles = RuntimeLifecycleMap::default();
        for (key, generation, state) in [
            (
                PoolKey::UniswapV2(Address::repeat_byte(0x41)),
                1,
                PoolRuntimeState::Searchable,
            ),
            (
                PoolKey::UniswapV3(Address::repeat_byte(0x42)),
                1,
                PoolRuntimeState::Degraded,
            ),
        ] {
            lifecycles.set_pool(
                PoolInstanceId::new(key, PoolGeneration::new(generation)),
                state,
            );
        }
        let status = AmmRuntimeStatusSnapshot::new(
            1,
            AmmStateVersion::new(1),
            lifecycles,
            [],
            Default::default(),
            AmmRuntimeHealth::Degraded,
        );
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);

        apply_live_runtime_status(&mut app, &status);

        assert_eq!(graph_phase_label(app.graph_sync.phase), "graph synced");
        assert_eq!(graph_phase_color(app.graph_sync.phase), Color::Green);
        assert_eq!(app.chain_sync.phase, ChainSyncPhase::Synced);
        assert_eq!(
            app.graph_sync.detail,
            "1 pools routing, 0 discovered, 1 degraded, 0 failed to load"
        );
    }

    #[test]
    fn partial_pool_failures_keep_a_usable_graph_synced() {
        use evm_amm_state::adapters::{
            AmmRuntimeHealth, AmmStateVersion, PoolGeneration, PoolInstanceId, RuntimeLifecycleMap,
        };

        let mut lifecycles = RuntimeLifecycleMap::default();
        for index in 0..296_u16 {
            let mut bytes = [0_u8; 20];
            bytes[18..].copy_from_slice(&index.to_be_bytes());
            let state = if index < 280 {
                PoolRuntimeState::Searchable
            } else {
                PoolRuntimeState::Failed
            };
            lifecycles.set_pool(
                PoolInstanceId::new(
                    PoolKey::UniswapV2(Address::from(bytes)),
                    PoolGeneration::new(1),
                ),
                state,
            );
        }
        let status = AmmRuntimeStatusSnapshot::new(
            1,
            AmmStateVersion::new(1),
            lifecycles,
            [],
            Default::default(),
            AmmRuntimeHealth::Degraded,
        );
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);

        apply_live_runtime_status(&mut app, &status);

        assert!(matches!(app.graph_sync.phase, GraphSyncPhase::Synced));
        assert_eq!(app.graph_sync.routing_pools, 280);
        assert_eq!(app.graph_sync.failed_pools, 16);
        assert!(app.graph_sync.detail.contains("16 failed to load"));
    }

    #[test]
    fn graph_is_an_error_when_every_pool_load_failed() {
        use evm_amm_state::adapters::{
            AmmRuntimeHealth, AmmStateVersion, PoolGeneration, PoolInstanceId, RuntimeLifecycleMap,
        };

        let key = PoolKey::UniswapV2(Address::repeat_byte(0x51));
        let mut lifecycles = RuntimeLifecycleMap::default();
        lifecycles.set_pool(
            PoolInstanceId::new(key, PoolGeneration::new(1)),
            PoolRuntimeState::Failed,
        );
        let status = AmmRuntimeStatusSnapshot::new(
            1,
            AmmStateVersion::new(1),
            lifecycles,
            [],
            Default::default(),
            AmmRuntimeHealth::Healthy,
        );
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);

        apply_live_runtime_status(&mut app, &status);

        assert!(matches!(app.graph_sync.phase, GraphSyncPhase::Error));
        assert_eq!(app.graph_sync.failed_pools, 1);
    }

    #[test]
    fn header_uses_the_original_colored_status_bubbles() -> Result<()> {
        let mut terminal = Terminal::new(TestBackend::new(160, 32))?;
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);
        terminal.draw(|frame| draw(frame, &app))?;
        let synced = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(synced.contains("chain ● synced"), "{synced}");
        assert!(synced.contains("graph ● graph synced"), "{synced}");

        app.chain_sync.phase = ChainSyncPhase::Applying;
        app.graph_sync.phase = GraphSyncPhase::Warming;
        terminal.draw(|frame| draw(frame, &app))?;
        let syncing = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(syncing.contains("chain ● applying"), "{syncing}");
        assert!(syncing.contains("graph ● warming graph"), "{syncing}");
        Ok(())
    }

    #[test]
    fn route_header_uses_a_fixed_width_synced_or_syncing_marker() -> Result<()> {
        let mut terminal = Terminal::new(TestBackend::new(160, 32))?;
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);
        let token_in = app.token_in();
        let token_out = app.token_out();
        let amount_in = parse_units(&app.amount, token_in.decimals).expect("valid test amount");
        let best = RouteQuote {
            path: RoutePath::from_hops(vec![Hop::new(
                PoolKey::UniswapV3(Address::repeat_byte(0x81)),
                token_in.address,
                token_out.address,
            )]),
            amount_in,
            amount_out: ten_pow(token_out.decimals),
            hops: Vec::new(),
        };
        app.quote = Some(QuoteView {
            best,
            block_number: 1,
            live_tenderly: None,
            output: "0.56388145 WETH ($997.02)".to_owned(),
            gas: None,
            route: RouteViz {
                tokens: Vec::new(),
                selected_legs: Vec::new(),
                alternatives: Vec::new(),
            },
            quoted_routes: Vec::new(),
            quoted_venues: String::new(),
            warnings: Vec::new(),
            coverage: String::new(),
            stream_lines: Vec::new(),
            routes: 89,
            graph_pools: 212,
            elapsed: Duration::from_micros(194_640),
        });

        terminal.draw(|frame| draw(frame, &app))?;
        let synced = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(synced.contains("routing_pools=212  (synced)"), "{synced}");
        assert!(!synced.contains("refreshing"), "{synced}");

        app.quote_loading = true;
        terminal.draw(|frame| draw(frame, &app))?;
        let syncing = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(
            syncing.contains("routing_pools=212  (syncing)"),
            "{syncing}"
        );
        assert!(!syncing.contains("refreshing"), "{syncing}");
        Ok(())
    }

    #[test]
    fn untrusted_graph_is_reported_as_an_error() {
        use evm_amm_state::adapters::{AmmRuntimeHealth, AmmStateVersion, RuntimeLifecycleMap};

        let status = AmmRuntimeStatusSnapshot::new(
            1,
            AmmStateVersion::new(1),
            RuntimeLifecycleMap::default(),
            [],
            Default::default(),
            AmmRuntimeHealth::Untrusted,
        );
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);

        apply_live_runtime_status(&mut app, &status);

        assert_eq!(graph_phase_label(app.graph_sync.phase), "graph error");
        assert_eq!(graph_phase_color(app.graph_sync.phase), Color::Red);
        assert_eq!(app.chain_sync.phase, ChainSyncPhase::Degraded);
    }

    #[test]
    fn sushi_v3_discovery_uses_its_factory_specific_quoter() {
        let config = factory_config();
        let sushi = config
            .concentrated_liquidity
            .iter()
            .find(|spec| spec.factory == SUSHISWAP_V3_FACTORY)
            .expect("Sushi V3 factory is configured");

        assert_eq!(sushi.quoter, Some(SUSHISWAP_V3_QUOTER_V2));
    }

    #[test]
    fn warm_resume_migrates_sushi_registrations_to_the_sushi_quoter() {
        let mut registration = PoolRegistration::new(PoolKey::UniswapV3(Address::repeat_byte(
            0x31,
        )))
        .with_metadata(ProtocolMetadata::UniswapV3(
            V3Metadata::default().with_factory(SUSHISWAP_V3_FACTORY),
        ));

        normalize_tui_registration(&mut registration);

        let ProtocolMetadata::UniswapV3(metadata) = registration.metadata else {
            panic!("registration changed protocol")
        };
        assert_eq!(metadata.quoter, Some(SUSHISWAP_V3_QUOTER_V2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn live_gas_setup_installs_the_demo_router_in_runtime_state() {
        use alloy_transport::mock::Asserter;
        use evm_fork_cache::cache::CodeSeedState;

        let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
        let mut cache = EvmCache::new(Arc::new(provider)).await;

        let (ready, status) = prepare_live_gas_router(&mut cache, true);

        assert!(ready, "{status}");
        assert!(matches!(
            cache.code_seed_state(&DEMO_ROUTER),
            Some(CodeSeedState::Etched { .. })
        ));
    }

    #[test]
    fn discovery_candidate_budget_is_distributed_without_overshoot() {
        let quotas = discovery_candidate_quotas(125, 42);
        assert_eq!(quotas.iter().sum::<usize>(), 125);
        assert_eq!(quotas.len(), 42);
        assert!(quotas.iter().all(|quota| [2, 3].contains(quota)));
        assert!(
            discovery_candidate_quotas(0, 3)
                .iter()
                .all(|quota| *quota == 0)
        );
        assert!(discovery_candidate_quotas(5, 0).is_empty());
    }

    #[test]
    fn selecting_an_existing_token_discovers_its_core_route_connectors() {
        let connectors = focused_discovery_connectors(&default_token_list(), LINK, UNI);

        assert!(connectors.contains(&WETH));
        assert!(connectors.contains(&USDC));
        assert!(connectors.contains(&DAI));
        assert!(connectors.contains(&UNI));
        assert!(!connectors.contains(&LINK));
    }

    #[test]
    fn existing_token_selection_plans_discovery_for_the_new_pair() {
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);
        app.input_index = app
            .tokens
            .iter()
            .position(|token| token.address == LINK)
            .expect("LINK token");
        app.output_index = app
            .tokens
            .iter()
            .position(|token| token.address == UNI)
            .expect("UNI token");

        let plan = selected_token_discovery_plan(&app, Side::Input);

        assert_eq!(plan.token.address, LINK);
        assert!(plan.connectors.contains(&WETH));
        assert!(plan.connectors.contains(&UNI));
    }

    #[test]
    fn idle_background_discovery_covers_pairs_once_at_deferred_priority() {
        let requests = background_discovery_requests(&default_token_list());
        let link_weth = requests
            .iter()
            .filter(|request| {
                (request.token() == LINK && request.connectors().contains(&WETH))
                    || (request.token() == WETH && request.connectors().contains(&LINK))
            })
            .count();

        assert_eq!(link_weth, 1);
        assert_eq!(
            background_discovery_options().class(),
            AmmWorkClass::Deferred
        );
    }

    #[test]
    fn severe_usd_value_loss_is_rendered_as_an_unsafe_route_warning() {
        let mut prices = PriceBook::disabled();
        prices.usd_by_token.insert(LINK, 7.98);
        prices.usd_by_token.insert(UNI, 3.56);
        let input = TokenInfo::new("LINK", LINK, 18);
        let output = TokenInfo::new("UNI", UNI, 18);

        let warning = economic_value_warning(
            ten_pow(18) * U256::from(1_000),
            &input,
            U256::from(211_737_702_380_000_000_000_u128),
            &output,
            &prices,
            1_000,
        )
        .expect("90% value loss must be called out");

        assert!(warning.contains("UNSAFE"));
        assert!(warning.contains("90.6%"));
        assert!(warning.contains("$7,980"));
    }

    #[test]
    fn default_token_list_includes_ethena_dollar_assets() {
        let tokens = default_token_list();
        let expected = [
            ("USDe", address!("4c9EDD5852cd905f086C759E8383e09bff1E68B3")),
            (
                "sUSDe",
                address!("9d39a5de30e57443bff2a8307a4256c8797a3497"),
            ),
        ];

        for (symbol, address) in expected {
            let token = tokens
                .iter()
                .find(|token| token.symbol == symbol)
                .unwrap_or_else(|| panic!("missing {symbol} from the default token list"));
            assert_eq!(token.address, address);
            assert_eq!(token.decimals, 18);
        }
    }

    #[test]
    fn same_request_provisional_route_never_replaces_the_accepted_display() {
        let token_in = Address::repeat_byte(0x61);
        let token_out = Address::repeat_byte(0x62);
        let amount_in = U256::from(1_000);
        let route = |amount_out| RouteQuote {
            path: RoutePath::from_hops(vec![Hop::new(
                PoolKey::UniswapV3(Address::repeat_byte(0x63)),
                token_in,
                token_out,
            )]),
            amount_in,
            amount_out: U256::from(amount_out),
            hops: Vec::new(),
        };
        let displayed = route(2_240);

        assert!(!should_replace_displayed_provisional(
            Some(&displayed),
            token_in,
            token_out,
            amount_in,
            &route(1_781),
        ));
        assert!(!should_replace_displayed_provisional(
            Some(&displayed),
            token_in,
            token_out,
            amount_in,
            &route(2_241),
        ));
    }

    #[test]
    fn route_panel_only_switches_to_loading_for_a_new_request() {
        let token_in = Address::repeat_byte(0x71);
        let token_out = Address::repeat_byte(0x72);
        let amount_in = U256::from(1_000);
        let displayed = RouteQuote {
            path: RoutePath::from_hops(vec![Hop::new(
                PoolKey::UniswapV3(Address::repeat_byte(0x73)),
                token_in,
                token_out,
            )]),
            amount_in,
            amount_out: U256::from(2_000),
            hops: Vec::new(),
        };

        assert!(!route_panel_shows_loading(
            true,
            Some(&displayed),
            token_in,
            token_out,
            Some(amount_in),
        ));
        assert!(route_panel_shows_loading(
            true,
            Some(&displayed),
            token_out,
            token_in,
            Some(amount_in),
        ));
        assert!(!route_panel_shows_loading(
            false,
            None,
            token_in,
            token_out,
            Some(amount_in),
        ));
    }

    #[test]
    fn restored_registrations_respect_the_global_startup_cap() {
        let focus = (USDC, WETH);
        let configured_key = PoolKey::Curve(Address::repeat_byte(0x91));
        let focus_key = PoolKey::UniswapV2(Address::repeat_byte(0x92));
        let mut registrations = vec![
            PoolRegistration::new(PoolKey::UniswapV2(Address::repeat_byte(0x90))).with_metadata(
                ProtocolMetadata::UniswapV2(
                    UniswapV2Metadata::default()
                        .with_token0(DAI)
                        .with_token1(USDT),
                ),
            ),
            PoolRegistration::new(focus_key.clone()).with_metadata(ProtocolMetadata::UniswapV2(
                UniswapV2Metadata::default()
                    .with_token0(USDC)
                    .with_token1(WETH),
            )),
            PoolRegistration::new(configured_key.clone()).with_metadata(ProtocolMetadata::Curve(
                CurveMetadata::default()
                    .with_coins([DAI, USDT])
                    .with_variant(CurveVariant::StableSwap),
            )),
        ];

        cap_startup_registrations(
            &mut registrations,
            2,
            &HashSet::from([configured_key.clone()]),
            Some(focus),
        );

        assert_eq!(registrations.len(), 2);
        assert!(registrations.iter().any(|pool| pool.key == configured_key));
        assert!(registrations.iter().any(|pool| pool.key == focus_key));
    }

    #[test]
    fn interactive_routes_finish_at_the_heuristic_boundary_by_default() {
        assert_eq!(
            tui_streaming_config().completion,
            evm_amm_search::StreamingCompletion::HeuristicExhausted,
        );
    }

    #[test]
    fn interactive_search_quotes_multiple_parallel_venues() {
        let config = tui_search_config_for_tokens(&default_token_list());
        let SearchMode::Heuristic(heuristic) = config.mode else {
            panic!("TUI search is not heuristic");
        };
        assert!(heuristic.parallel_edge_limit >= 4);
        assert!(heuristic.fast_lane.direct_edges_per_pair >= 4);
        assert!(heuristic.fast_lane.connector_edges_per_pair >= 2);
        assert!(tui_streaming_config().top_k >= 16);
    }

    #[test]
    fn benchmark_failure_summary_preserves_observer_lag() {
        let mut summary = BenchFailureSummary::default();
        summary.record_observer_lag(37);
        summary.record_observer_lag(5);
        assert_eq!(summary.skipped_events, 42);
    }

    #[test]
    fn warm_checkpoint_catchup_limit_is_inclusive_and_rejects_future_points() {
        assert!(warm_checkpoint_is_resumable(1_000, 1_256, 256));
        assert!(!warm_checkpoint_is_resumable(1_000, 1_257, 256));
        assert!(!warm_checkpoint_is_resumable(1_001, 1_000, 256));
    }

    #[test]
    fn warm_generation_cannot_commit_while_background_work_remains() {
        let idle = GraphSyncCounts::default();
        assert!(warm_generation_ready_to_commit(idle, 0));

        let mut queued = idle;
        queued.queued_loads = 1;
        assert!(!warm_generation_ready_to_commit(queued, 0));
        assert!(!warm_generation_ready_to_commit(idle, 1));
    }

    #[test]
    fn curve_read_sets_cover_builtins_and_parse_custom_hex_slots() {
        for pool in default_manual_curve_pools() {
            let ProtocolMetadata::Curve(metadata) = pool.registration.metadata else {
                panic!("default manual pool is not Curve");
            };
            assert!(!metadata.discovered_slots.is_empty());
        }

        let config: TuiUserConfig = toml::from_str(
            r#"
                [[curve_pools]]
                label = "Custom"
                address = "0x4444444444444444444444444444444444444444"
                coins = [
                    "0x1111111111111111111111111111111111111111",
                    "0x2222222222222222222222222222222222222222",
                ]
                discovered_slots = ["0x2a", "43"]
            "#,
        )
        .expect("curve config");
        let pool = user_curve_pool(&config.curve_pools[0]).expect("curve pool");
        let ProtocolMetadata::Curve(metadata) = pool.registration.metadata else {
            panic!("custom pool is not Curve");
        };
        assert_eq!(
            metadata.discovered_slots,
            vec![U256::from(42), U256::from(43)]
        );
    }

    #[test]
    fn networking_config_deserializes_provider_capabilities() {
        let config: TuiUserConfig = toml::from_str(
            r#"
                [network]
                cold_start_concurrency = 24
                bulk_max_slots_per_call = 25000
                bulk_max_request_bytes = 2400000

                [[network.rpc_endpoints]]
                url = "https://example.invalid"
                weight = 75
                max_request_bytes = 1800000
                max_in_flight = 6
            "#,
        )
        .expect("network config");

        assert_eq!(config.network.cold_start_concurrency, Some(24));
        assert_eq!(config.network.bulk_max_slots_per_call, 25_000);
        assert_eq!(config.network.rpc_endpoints[0].weight, Some(75));
        assert_eq!(config.network.rpc_endpoints[0].max_in_flight, Some(6));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap_screen_accepts_quit_while_runtime_work_is_blocked() -> Result<()> {
        let mut terminal = Terminal::new(TestBackend::new(120, 32))?;
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);
        let (ui_tx, mut ui_rx) = mpsc::channel(4);
        let (_bootstrap_tx, mut bootstrap_rx) = mpsc::channel(4);
        ui_tx
            .send(UiEvent::Key(KeyEvent::new(
                KeyCode::Char('q'),
                KeyModifiers::NONE,
            )))
            .await?;

        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            wait_for_live_bootstrap(&mut terminal, &mut app, &mut ui_rx, &mut bootstrap_rx),
        )
        .await??;

        assert!(matches!(outcome, LiveBootstrapOutcome::Quit));
        Ok(())
    }

    #[test]
    fn newer_request_reuses_the_subscription_and_rejects_older_ui_updates() {
        let mut controller = LiveRouteUiController::default();
        let first = controller.begin_replacement();
        let cancellation = RouteCancellationToken::default();
        controller.cancellation = Some(cancellation.clone());
        let second = controller.begin_replacement();

        assert!(!cancellation.is_cancelled());
        assert!(!controller.accepts(first));
        assert!(controller.accepts(second));
    }

    #[test]
    fn stale_dynamic_discovery_cannot_replace_a_newer_token_selection() {
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);
        let mut generations = [0, 0];
        let discovery = advance_dynamic_discovery_generation(&mut generations, Side::Output);
        let original_output = app.token_out().address;
        let stale = DynamicDiscoveryEvent {
            generation: discovery,
            side: Side::Output,
            query: "STALE".to_owned(),
            outcome: Ok(DynamicDiscoveryOutcome {
                token: TokenInfo::new("STALE", Address::repeat_byte(0x51), 18),
                discovery: Ok(1),
            }),
        };

        let manual_selection = advance_dynamic_discovery_generation(&mut generations, Side::Output);

        assert!(!apply_live_dynamic_discovery(&mut app, stale, &generations));
        assert_eq!(app.token_out().address, original_output);

        let current_address = Address::repeat_byte(0x52);
        let current = DynamicDiscoveryEvent {
            generation: manual_selection,
            side: Side::Output,
            query: "CURRENT".to_owned(),
            outcome: Ok(DynamicDiscoveryOutcome {
                token: TokenInfo::new("CURRENT", current_address, 18),
                discovery: Ok(1),
            }),
        };
        assert!(apply_live_dynamic_discovery(
            &mut app,
            current,
            &generations
        ));
        assert_eq!(app.token_out().address, current_address);
    }

    #[test]
    fn unresolved_dynamic_discovery_does_not_add_a_token() {
        let mut app = AppState::new(default_token_list(), Vec::new(), 0, 0, 0);
        let mut generations = [0, 0];
        let generation = advance_dynamic_discovery_generation(&mut generations, Side::Input);
        let original_len = app.tokens.len();
        let original_input = app.token_in().address;
        let event = DynamicDiscoveryEvent {
            generation,
            side: Side::Input,
            query: "NOPE".to_owned(),
            outcome: Err("not found".to_owned()),
        };

        assert!(!apply_live_dynamic_discovery(&mut app, event, &generations));
        assert_eq!(app.tokens.len(), original_len);
        assert_eq!(app.token_in().address, original_input);
    }

    #[test]
    fn token_registry_lookup_is_chain_scoped_and_rejects_ambiguous_symbols() {
        let entries = vec![
            TokenRegistryEntry {
                chain_id: 1,
                address: format!("{:#x}", Address::repeat_byte(0x11)),
                symbol: "AAA".to_owned(),
                decimals: 18,
            },
            TokenRegistryEntry {
                chain_id: 10,
                address: format!("{:#x}", Address::repeat_byte(0x22)),
                symbol: "AAA".to_owned(),
                decimals: 6,
            },
        ];

        let token = token_from_registry_entries(10, "aaa", entries).expect("chain-scoped match");
        assert_eq!(token.address, Address::repeat_byte(0x22));
        assert_eq!(token.decimals, 6);

        let ambiguous = match token_from_registry_entries(
            1,
            "BBB",
            vec![
                TokenRegistryEntry {
                    chain_id: 1,
                    address: format!("{:#x}", Address::repeat_byte(0x33)),
                    symbol: "BBB".to_owned(),
                    decimals: 18,
                },
                TokenRegistryEntry {
                    chain_id: 1,
                    address: format!("{:#x}", Address::repeat_byte(0x44)),
                    symbol: "BBB".to_owned(),
                    decimals: 18,
                },
            ],
        ) {
            Ok(_) => panic!("ambiguous symbol unexpectedly resolved"),
            Err(error) => error,
        };
        assert!(ambiguous.to_string().contains("ambiguous"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap_cancellation_interrupts_pending_work() -> Result<()> {
        let (cancel, cancelled) = watch::channel(false);
        let waiting = tokio::spawn(wait_for_bootstrap_cancellation(cancelled));
        cancel.send_replace(true);

        tokio::time::timeout(Duration::from_millis(100), waiting).await??;
        Ok(())
    }

    #[test]
    fn warm_resume_does_not_queue_disabled_or_unsupported_registrations() {
        use evm_amm_state::adapters::PoolStatus;

        let mut registrations = vec![
            PoolRegistration::new(PoolKey::UniswapV2(Address::repeat_byte(1))),
            PoolRegistration::new(PoolKey::UniswapV2(Address::repeat_byte(2)))
                .with_status(PoolStatus::Disabled),
            PoolRegistration::new(PoolKey::UniswapV2(Address::repeat_byte(3)))
                .with_status(PoolStatus::Unsupported),
        ];

        retain_queueable_registrations(&mut registrations);

        assert_eq!(registrations.len(), 1);
        assert_eq!(
            registrations[0].key,
            PoolKey::UniswapV2(Address::repeat_byte(1))
        );
    }

    #[test]
    fn tenderly_state_override_only_prefunds_the_demo_router() {
        let token = Address::repeat_byte(0x71);
        let slot = B256::repeat_byte(0x72);
        let amount = U256::from(1_000_000_u64);
        let runtime = Bytes::from_static(&[0x60, 0x00]);

        let state_objects = tenderly_state_objects(token, slot, amount, &runtime);

        assert_eq!(state_objects.len(), 2);
        assert_eq!(
            state_objects
                .get(&address_hex(token))
                .and_then(|token| token.pointer(&format!("/storage/{}", b256_hex(slot))))
                .and_then(Value::as_str),
            Some(u256_word_hex(amount).as_str()),
        );
        assert_eq!(
            state_objects
                .get(&address_hex(token))
                .and_then(|token| token.pointer("/storage"))
                .and_then(Value::as_object)
                .map(serde_json::Map::len),
            Some(1),
            "the token override must not include an allowance slot",
        );
        assert!(state_objects.contains_key(&address_hex(DEMO_ROUTER)));
    }

    #[test]
    fn tenderly_validation_requires_success_block_and_matching_output() {
        let expected = U256::from(211_737_702_380_000_000_000_u128);
        let response = json!({
            "transaction": {
                "status": true,
                "gas_used": 149_382,
                "transaction_info": {
                    "call_trace": {
                        "output": u256_word_hex(expected),
                    }
                }
            },
            "simulation": {
                "block_number": 25_524_123,
                "transaction_index": 391,
            }
        });

        let validation = validate_tenderly_response(&response, 25_524_123, 391, expected, 0)
            .expect("matching Tenderly execution");
        assert_eq!(validation.amount_out, expected);
        assert_eq!(validation.gas_used, Some(149_382));

        let missing_output = json!({
            "transaction": { "status": true },
            "simulation": { "block_number": 25_524_123, "transaction_index": 391 },
        });
        assert!(
            validate_tenderly_response(&missing_output, 25_524_123, 391, expected, 0)
                .unwrap_err()
                .contains("amountOut")
        );

        let wrong_output = json!({
            "transaction": {
                "status": true,
                "transaction_info": {
                    "call_trace": { "output": u256_word_hex(expected / U256::from(2)) }
                }
            },
            "simulation": { "block_number": 25_524_123, "transaction_index": 391 },
        });
        assert!(
            validate_tenderly_response(&wrong_output, 25_524_123, 391, expected, 1)
                .unwrap_err()
                .contains("output mismatch")
        );

        let wrong_position = json!({
            "transaction": {
                "status": true,
                "transaction_info": {
                    "call_trace": { "output": u256_word_hex(expected) }
                }
            },
            "simulation": { "block_number": 25_524_123, "transaction_index": 0 },
        });
        assert!(
            validate_tenderly_response(&wrong_position, 25_524_123, 391, expected, 0)
                .unwrap_err()
                .contains("expected end-of-block index 391")
        );
    }
}
