use std::{collections::HashSet, env, fs, path::Path, str::FromStr, time::Duration};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use url::Url;

const MAINNET_V2_ROUTER: &str = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D";
const MAINNET_V3_QUOTER: &str = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e";

/// Fully resolved sidecar configuration.
#[derive(Clone, Debug)]
pub struct SidecarConfig {
    pub server: ServerConfig,
    pub chain: ChainConfig,
    pub rpc: RpcConfig,
    pub storage: StorageConfig,
    pub routing: RoutingConfig,
    pub discovery: DiscoveryConfig,
    pub tokens: Vec<TokenConfig>,
    pub factories: Vec<FactoryConfig>,
    pub pools: Vec<PoolConfig>,
    pub profile_fingerprint: B256,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listen: String,
    pub max_request_bytes: usize,
    pub max_in_flight_quotes: usize,
    pub json_logs: bool,
    pub admin_bearer_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ChainConfig {
    pub expected_chain_id: u64,
    pub v2_router: Address,
    pub v3_quoter: Address,
}

#[derive(Clone, Debug)]
pub struct RpcConfig {
    pub canonical_ws: String,
    pub state: Vec<RpcEndpointConfig>,
    pub batch_size: usize,
    pub cold_start_concurrency: usize,
    pub max_log_addresses_per_subscription: usize,
    pub point_read_slots_per_batch: usize,
    pub point_read_concurrency: usize,
    pub bulk_max_slots_per_call: usize,
    pub bulk_max_slots_per_request: usize,
    pub bulk_max_request_bytes: usize,
    pub bulk_max_concurrent_calls: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RpcEndpointConfig {
    pub url: String,
    #[serde(default = "default_rpc_weight")]
    pub weight: u32,
    pub max_request_bytes: Option<usize>,
    pub max_in_flight: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub path: String,
    pub persist_cache: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchQuality {
    Fast,
    Balanced,
    Exhaustive,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMode {
    Off,
    IfMissing,
    Refresh,
}

#[derive(Clone, Debug)]
pub struct RoutingConfig {
    pub default_quality: SearchQuality,
    pub default_max_hops: usize,
    pub default_top_k: usize,
    pub default_timeout: Duration,
    pub max_hops: usize,
    pub max_top_k: usize,
    pub max_timeout: Duration,
    pub max_candidates: usize,
    pub allow_exhaustive: bool,
    pub route_worker_threads: usize,
    pub max_subscriptions: usize,
}

#[derive(Clone, Debug)]
pub struct DiscoveryConfig {
    pub quote_default: DiscoveryMode,
    pub max_startup_pools: usize,
    pub max_concurrent_requests: usize,
    pub negative_ttl: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TokenConfig {
    pub symbol: String,
    pub address: String,
    pub decimals: u8,
    #[serde(default)]
    pub connector: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl TokenConfig {
    pub fn parsed_address(&self) -> Result<Address> {
        parse_address(&self.address).with_context(|| format!("token {}", self.symbol))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FactoryConfig {
    pub name: String,
    pub protocol: String,
    pub address: String,
    pub quoter: Option<String>,
    pub fee_bps: Option<u32>,
    pub get_pair_base_slot: Option<String>,
    pub init_code_hash: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl FactoryConfig {
    pub fn parsed_address(&self) -> Result<Address> {
        parse_address(&self.address).with_context(|| format!("factory {}", self.name))
    }

    pub fn normalized_protocol(&self) -> String {
        self.protocol.trim().to_ascii_lowercase().replace('_', "-")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PoolConfig {
    pub protocol: String,
    pub address: String,
    #[serde(default)]
    pub tokens: Vec<String>,
    pub fee_bps: Option<u32>,
    pub fee: Option<u32>,
    pub variant: Option<String>,
    pub factory: Option<String>,
    pub quoter: Option<String>,
    #[serde(default)]
    pub discovered_slots: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl PoolConfig {
    pub fn normalized_protocol(&self) -> String {
        self.protocol.trim().to_ascii_lowercase().replace('-', "_")
    }
}

#[derive(Default, Deserialize)]
struct RawConfig {
    extends: Option<String>,
    #[serde(default)]
    replace_tokens: bool,
    #[serde(default)]
    replace_factories: bool,
    #[serde(default)]
    replace_pools: bool,
    server: Option<ServerOverrides>,
    chain: Option<ChainOverrides>,
    rpc: Option<RpcOverrides>,
    storage: Option<StorageOverrides>,
    routing: Option<RoutingOverrides>,
    discovery: Option<DiscoveryOverrides>,
    #[serde(default)]
    tokens: Vec<TokenConfig>,
    #[serde(default)]
    factories: Vec<FactoryConfig>,
    #[serde(default)]
    pools: Vec<PoolConfig>,
}

#[derive(Default, Deserialize)]
struct ServerOverrides {
    listen: Option<String>,
    max_request_bytes: Option<usize>,
    max_in_flight_quotes: Option<usize>,
    json_logs: Option<bool>,
    admin_bearer_token: Option<String>,
}

#[derive(Default, Deserialize)]
struct ChainOverrides {
    expected_chain_id: Option<u64>,
    v2_router: Option<String>,
    v3_quoter: Option<String>,
}

#[derive(Default, Deserialize)]
struct RpcOverrides {
    canonical_ws: Option<String>,
    state: Option<Vec<RpcEndpointConfig>>,
    batch_size: Option<usize>,
    cold_start_concurrency: Option<usize>,
    max_log_addresses_per_subscription: Option<usize>,
    point_read_slots_per_batch: Option<usize>,
    point_read_concurrency: Option<usize>,
    bulk_max_slots_per_call: Option<usize>,
    bulk_max_slots_per_request: Option<usize>,
    bulk_max_request_bytes: Option<usize>,
    bulk_max_concurrent_calls: Option<usize>,
}

#[derive(Default, Deserialize)]
struct StorageOverrides {
    path: Option<String>,
    persist_cache: Option<bool>,
}

#[derive(Default, Deserialize)]
struct RoutingOverrides {
    default_quality: Option<SearchQuality>,
    default_max_hops: Option<usize>,
    default_top_k: Option<usize>,
    default_timeout_ms: Option<u64>,
    max_hops: Option<usize>,
    max_top_k: Option<usize>,
    max_timeout_ms: Option<u64>,
    max_candidates: Option<usize>,
    allow_exhaustive: Option<bool>,
    route_worker_threads: Option<usize>,
    max_subscriptions: Option<usize>,
}

#[derive(Default, Deserialize)]
struct DiscoveryOverrides {
    quote_default: Option<DiscoveryMode>,
    max_startup_pools: Option<usize>,
    max_concurrent_requests: Option<usize>,
    negative_ttl_secs: Option<u64>,
}

impl SidecarConfig {
    pub fn parse(source: &str) -> Result<Self> {
        Self::parse_with(source, |key| env::var(key).ok())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let source = fs::read_to_string(path)
            .with_context(|| format!("read sidecar config {}", path.display()))?;
        Self::parse(&source).with_context(|| format!("load sidecar config {}", path.display()))
    }

    fn parse_with<F>(source: &str, lookup: F) -> Result<Self>
    where
        F: Fn(&str) -> Option<String> + Copy,
    {
        let mut value: toml::Value = toml::from_str(source).context("parse sidecar TOML")?;
        expand_toml_value(&mut value, lookup)?;
        let raw: RawConfig = value.try_into().context("deserialize sidecar TOML")?;
        let mut config = match raw.extends.as_deref().unwrap_or("ethereum-mainnet") {
            "ethereum-mainnet" => Self::ethereum_mainnet(),
            "none" => Self::empty(),
            profile => bail!("unsupported built-in profile {profile}"),
        };
        config.apply(raw)?;
        config.validate()?;
        config.profile_fingerprint = config.topology_fingerprint()?;
        Ok(config)
    }

    fn empty() -> Self {
        Self {
            server: ServerConfig {
                listen: "0.0.0.0:8080".to_owned(),
                max_request_bytes: 65_536,
                max_in_flight_quotes: 64,
                json_logs: false,
                admin_bearer_token: None,
            },
            chain: ChainConfig {
                expected_chain_id: 0,
                v2_router: Address::ZERO,
                v3_quoter: Address::ZERO,
            },
            rpc: RpcConfig {
                canonical_ws: String::new(),
                state: Vec::new(),
                batch_size: 150,
                cold_start_concurrency: 16,
                max_log_addresses_per_subscription: 1_024,
                point_read_slots_per_batch: 150,
                point_read_concurrency: 8,
                bulk_max_slots_per_call: 25_000,
                bulk_max_slots_per_request: 25_000,
                bulk_max_request_bytes: 2_400_000,
                bulk_max_concurrent_calls: 4,
            },
            storage: StorageConfig {
                path: "/data".to_owned(),
                persist_cache: false,
            },
            routing: RoutingConfig {
                default_quality: SearchQuality::Balanced,
                default_max_hops: 3,
                default_top_k: 3,
                default_timeout: Duration::from_millis(5_000),
                max_hops: 4,
                max_top_k: 5,
                max_timeout: Duration::from_millis(15_000),
                max_candidates: 50_000,
                allow_exhaustive: true,
                route_worker_threads: std::thread::available_parallelism().map_or(1, usize::from),
                max_subscriptions: 1_024,
            },
            discovery: DiscoveryConfig {
                quote_default: DiscoveryMode::IfMissing,
                max_startup_pools: 128,
                max_concurrent_requests: 8,
                negative_ttl: Duration::from_secs(300),
            },
            tokens: Vec::new(),
            factories: Vec::new(),
            pools: Vec::new(),
            profile_fingerprint: B256::ZERO,
        }
    }

    fn ethereum_mainnet() -> Self {
        let mut config = Self::empty();
        config.chain.expected_chain_id = 1;
        config.chain.v2_router = parse_address(MAINNET_V2_ROUTER).expect("valid mainnet router");
        config.chain.v3_quoter = parse_address(MAINNET_V3_QUOTER).expect("valid mainnet quoter");
        config.tokens = vec![
            token(
                "USDC",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                6,
                true,
            ),
            token(
                "WETH",
                "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                18,
                true,
            ),
            token(
                "DAI",
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                18,
                true,
            ),
            token(
                "WBTC",
                "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                8,
                false,
            ),
            token(
                "USDT",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                6,
                true,
            ),
        ];
        config.factories = vec![
            factory(
                "uniswap-v2",
                "uniswap-v2",
                "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
                Some(30),
                None,
            ),
            factory(
                "sushiswap-v2",
                "uniswap-v2",
                "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",
                Some(30),
                None,
            ),
            factory(
                "uniswap-v3",
                "uniswap-v3",
                "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                None,
                Some(MAINNET_V3_QUOTER),
            ),
            factory(
                "sushiswap-v3",
                "sushi-v3",
                "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F",
                None,
                Some("0x64e8802FE490fa7cc61d3463958199161Bb608A7"),
            ),
            factory(
                "pancakeswap-v3",
                "pancake-v3",
                "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
                None,
                Some("0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997"),
            ),
        ];
        config
    }

    fn apply(&mut self, raw: RawConfig) -> Result<()> {
        if let Some(server) = raw.server {
            set_if_some(&mut self.server.listen, server.listen);
            set_if_some(&mut self.server.max_request_bytes, server.max_request_bytes);
            set_if_some(
                &mut self.server.max_in_flight_quotes,
                server.max_in_flight_quotes,
            );
            set_if_some(&mut self.server.json_logs, server.json_logs);
            if server.admin_bearer_token.is_some() {
                self.server.admin_bearer_token = server.admin_bearer_token;
            }
        }
        if let Some(chain) = raw.chain {
            set_if_some(&mut self.chain.expected_chain_id, chain.expected_chain_id);
            if let Some(router) = chain.v2_router {
                self.chain.v2_router = parse_address(&router).context("chain.v2_router")?;
            }
            if let Some(quoter) = chain.v3_quoter {
                self.chain.v3_quoter = parse_address(&quoter).context("chain.v3_quoter")?;
            }
        }
        if let Some(rpc) = raw.rpc {
            set_if_some(&mut self.rpc.canonical_ws, rpc.canonical_ws);
            if let Some(state) = rpc.state {
                self.rpc.state = state;
            }
            set_if_some(&mut self.rpc.batch_size, rpc.batch_size);
            set_if_some(
                &mut self.rpc.cold_start_concurrency,
                rpc.cold_start_concurrency,
            );
            set_if_some(
                &mut self.rpc.max_log_addresses_per_subscription,
                rpc.max_log_addresses_per_subscription,
            );
            set_if_some(
                &mut self.rpc.point_read_slots_per_batch,
                rpc.point_read_slots_per_batch,
            );
            set_if_some(
                &mut self.rpc.point_read_concurrency,
                rpc.point_read_concurrency,
            );
            set_if_some(
                &mut self.rpc.bulk_max_slots_per_call,
                rpc.bulk_max_slots_per_call,
            );
            set_if_some(
                &mut self.rpc.bulk_max_slots_per_request,
                rpc.bulk_max_slots_per_request,
            );
            set_if_some(
                &mut self.rpc.bulk_max_request_bytes,
                rpc.bulk_max_request_bytes,
            );
            set_if_some(
                &mut self.rpc.bulk_max_concurrent_calls,
                rpc.bulk_max_concurrent_calls,
            );
        }
        if let Some(storage) = raw.storage {
            set_if_some(&mut self.storage.path, storage.path);
            set_if_some(&mut self.storage.persist_cache, storage.persist_cache);
        }
        if let Some(routing) = raw.routing {
            set_if_some(&mut self.routing.default_quality, routing.default_quality);
            set_if_some(&mut self.routing.default_max_hops, routing.default_max_hops);
            set_if_some(&mut self.routing.default_top_k, routing.default_top_k);
            if let Some(ms) = routing.default_timeout_ms {
                self.routing.default_timeout = Duration::from_millis(ms);
            }
            set_if_some(&mut self.routing.max_hops, routing.max_hops);
            set_if_some(&mut self.routing.max_top_k, routing.max_top_k);
            if let Some(ms) = routing.max_timeout_ms {
                self.routing.max_timeout = Duration::from_millis(ms);
            }
            set_if_some(&mut self.routing.max_candidates, routing.max_candidates);
            set_if_some(&mut self.routing.allow_exhaustive, routing.allow_exhaustive);
            set_if_some(
                &mut self.routing.route_worker_threads,
                routing.route_worker_threads,
            );
            set_if_some(
                &mut self.routing.max_subscriptions,
                routing.max_subscriptions,
            );
        }
        if let Some(discovery) = raw.discovery {
            set_if_some(&mut self.discovery.quote_default, discovery.quote_default);
            set_if_some(
                &mut self.discovery.max_startup_pools,
                discovery.max_startup_pools,
            );
            set_if_some(
                &mut self.discovery.max_concurrent_requests,
                discovery.max_concurrent_requests,
            );
            if let Some(secs) = discovery.negative_ttl_secs {
                self.discovery.negative_ttl = Duration::from_secs(secs);
            }
        }

        merge_items(&mut self.tokens, raw.tokens, raw.replace_tokens, |item| {
            item.address.to_ascii_lowercase()
        });
        merge_items(
            &mut self.factories,
            raw.factories,
            raw.replace_factories,
            |item| {
                format!(
                    "{}:{}",
                    item.normalized_protocol(),
                    item.address.to_ascii_lowercase()
                )
            },
        );
        merge_items(&mut self.pools, raw.pools, raw.replace_pools, |item| {
            format!(
                "{}:{}",
                item.protocol.to_ascii_lowercase(),
                item.address.to_ascii_lowercase()
            )
        });
        self.tokens.retain(|item| item.enabled);
        self.factories.retain(|item| item.enabled);
        self.pools.retain(|item| item.enabled);
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.server.max_request_bytes == 0 || self.server.max_in_flight_quotes == 0 {
            bail!("server bounds must be non-zero");
        }
        if self
            .server
            .admin_bearer_token
            .as_deref()
            .is_some_and(|token| token.trim().is_empty())
        {
            bail!("server.admin_bearer_token cannot be empty when configured");
        }
        if self.storage.persist_cache {
            bail!(
                "storage.persist_cache is not supported by this sidecar release; verified startup always rebuilds from the canonical chain"
            );
        }
        if self.chain.expected_chain_id == 0 {
            bail!("chain.expected_chain_id must be non-zero");
        }
        validate_url(&self.rpc.canonical_ws, &["ws", "wss"], "rpc.canonical_ws")?;
        if [
            self.rpc.batch_size,
            self.rpc.cold_start_concurrency,
            self.rpc.max_log_addresses_per_subscription,
            self.rpc.point_read_slots_per_batch,
            self.rpc.point_read_concurrency,
            self.rpc.bulk_max_slots_per_call,
            self.rpc.bulk_max_slots_per_request,
            self.rpc.bulk_max_request_bytes,
            self.rpc.bulk_max_concurrent_calls,
        ]
        .contains(&0)
        {
            bail!("RPC bounds must be non-zero");
        }
        for endpoint in &self.rpc.state {
            validate_url(&endpoint.url, &["http", "https"], "rpc.state.url")?;
            if endpoint.weight == 0 {
                bail!("RPC endpoint weight must be non-zero");
            }
            if endpoint.max_request_bytes == Some(0) || endpoint.max_in_flight == Some(0) {
                bail!("RPC endpoint bounds must be non-zero when configured");
            }
        }
        if self.tokens.is_empty() {
            bail!("profile must configure at least one token");
        }
        if !self.tokens.iter().any(|token| token.connector) {
            bail!("profile must configure at least one connector token");
        }
        let mut token_addresses = HashSet::new();
        for token in &self.tokens {
            let address = token.parsed_address()?;
            if !token_addresses.insert(address) {
                bail!("duplicate configured token address {address}");
            }
        }
        for factory in &self.factories {
            factory.parsed_address()?;
            match factory.normalized_protocol().as_str() {
                "uniswap-v2" | "uniswap-v3" | "sushi-v3" | "pancake-v3" => {}
                other => bail!("unsupported factory protocol {other}"),
            }
            if let Some(quoter) = &factory.quoter {
                parse_address(quoter)
                    .with_context(|| format!("factory {} quoter", factory.name))?;
            }
            if let Some(slot) = &factory.get_pair_base_slot {
                parse_u256(slot)
                    .with_context(|| format!("factory {} get_pair_base_slot", factory.name))?;
            }
            if let Some(hash) = &factory.init_code_hash {
                B256::from_str(hash)
                    .with_context(|| format!("factory {} init_code_hash", factory.name))?;
            }
        }
        for pool in &self.pools {
            parse_address(&pool.address)
                .with_context(|| format!("pool {} address", pool.address))?;
            for token in &pool.tokens {
                parse_address(token).with_context(|| format!("pool {} token", pool.address))?;
            }
            for slot in &pool.discovered_slots {
                parse_u256(slot)
                    .with_context(|| format!("pool {} discovered slot", pool.address))?;
            }
            if let Some(factory) = &pool.factory {
                parse_address(factory).with_context(|| format!("pool {} factory", pool.address))?;
            }
            if let Some(quoter) = &pool.quoter {
                parse_address(quoter).with_context(|| format!("pool {} quoter", pool.address))?;
            }
            match pool.normalized_protocol().as_str() {
                "uniswap_v2" | "sushiswap_v2" | "v2" => {
                    if !pool.tokens.is_empty() && pool.tokens.len() != 2 {
                        bail!(
                            "manual V2 pool {} must list exactly two tokens",
                            pool.address
                        );
                    }
                }
                "uniswap_v3" | "sushi_v3" | "v3" | "pancake_v3" | "pancakeswap_v3" => {
                    if pool.fee.is_none() && pool.fee_bps.is_none() {
                        bail!("manual V3 pool {} must set fee", pool.address);
                    }
                    if !pool.tokens.is_empty() && pool.tokens.len() != 2 {
                        bail!(
                            "manual V3 pool {} must list exactly two tokens",
                            pool.address
                        );
                    }
                }
                "curve" | "curve_stable" | "curve_crypto" | "curve_crypto_ng" => {
                    if pool.tokens.len() < 2 {
                        bail!(
                            "manual Curve pool {} must list at least two tokens",
                            pool.address
                        );
                    }
                }
                protocol => bail!("unsupported manual pool protocol {protocol}"),
            }
        }
        if self.routing.default_max_hops == 0
            || self.routing.default_max_hops > self.routing.max_hops
        {
            bail!("routing.default_max_hops must be within 1..=routing.max_hops");
        }
        if self.routing.default_top_k == 0 || self.routing.default_top_k > self.routing.max_top_k {
            bail!("routing.default_top_k must be within 1..=routing.max_top_k");
        }
        if self.routing.default_timeout.is_zero() || self.routing.max_timeout.is_zero() {
            bail!("routing timeouts must be non-zero");
        }
        if self.routing.default_timeout > self.routing.max_timeout {
            bail!("routing.default_timeout_ms exceeds routing.max_timeout_ms");
        }
        if self.routing.max_candidates == 0
            || self.routing.route_worker_threads == 0
            || self.routing.max_subscriptions == 0
        {
            bail!("routing bounds must be non-zero");
        }
        if self.discovery.max_concurrent_requests == 0 {
            bail!("discovery.max_concurrent_requests must be non-zero");
        }
        Ok(())
    }

    fn topology_fingerprint(&self) -> Result<B256> {
        let encoded = serde_json::to_vec(&(
            self.chain.expected_chain_id,
            format!("{:#x}", self.chain.v2_router),
            format!("{:#x}", self.chain.v3_quoter),
            &self.tokens,
            &self.factories,
            &self.pools,
        ))?;
        Ok(keccak256(encoded))
    }

    pub fn connector_addresses(&self) -> Result<Vec<Address>> {
        self.tokens
            .iter()
            .filter(|token| token.connector)
            .map(TokenConfig::parsed_address)
            .collect()
    }
}

fn token(symbol: &str, address: &str, decimals: u8, connector: bool) -> TokenConfig {
    TokenConfig {
        symbol: symbol.to_owned(),
        address: address.to_owned(),
        decimals,
        connector,
        enabled: true,
    }
}

fn factory(
    name: &str,
    protocol: &str,
    address: &str,
    fee_bps: Option<u32>,
    quoter: Option<&str>,
) -> FactoryConfig {
    FactoryConfig {
        name: name.to_owned(),
        protocol: protocol.to_owned(),
        address: address.to_owned(),
        quoter: quoter.map(str::to_owned),
        fee_bps,
        get_pair_base_slot: None,
        init_code_hash: None,
        enabled: true,
    }
}

fn set_if_some<T>(target: &mut T, value: Option<T>) {
    if let Some(value) = value {
        *target = value;
    }
}

fn merge_items<T, F>(target: &mut Vec<T>, incoming: Vec<T>, replace: bool, key: F)
where
    F: Fn(&T) -> String,
{
    if replace {
        target.clear();
    }
    for item in incoming {
        let item_key = key(&item);
        if let Some(existing) = target.iter_mut().find(|existing| key(existing) == item_key) {
            *existing = item;
        } else {
            target.push(item);
        }
    }
}

fn expand_toml_value<F>(value: &mut toml::Value, lookup: F) -> Result<()>
where
    F: Fn(&str) -> Option<String> + Copy,
{
    match value {
        toml::Value::String(value) => *value = expand_string_with(value, lookup)?,
        toml::Value::Array(values) => {
            for value in values {
                expand_toml_value(value, lookup)?;
            }
        }
        toml::Value::Table(values) => {
            for (_, value) in values.iter_mut() {
                expand_toml_value(value, lookup)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn expand_string_with<F>(input: &str, lookup: F) -> Result<String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut output = String::with_capacity(input.len());
    let mut remaining = input;
    while let Some(start) = remaining.find("${") {
        output.push_str(&remaining[..start]);
        let variable = &remaining[start + 2..];
        let end = variable
            .find('}')
            .ok_or_else(|| anyhow!("unterminated environment placeholder in {input:?}"))?;
        let name = &variable[..end];
        if name.is_empty()
            || !name.bytes().enumerate().all(|(index, byte)| {
                byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
            })
        {
            bail!("invalid environment variable name {name:?}");
        }
        output.push_str(
            &lookup(name)
                .ok_or_else(|| anyhow!("required environment variable {name} is not set"))?,
        );
        remaining = &variable[end + 1..];
    }
    output.push_str(remaining);
    Ok(output)
}

fn validate_url(value: &str, schemes: &[&str], label: &str) -> Result<()> {
    let parsed = Url::parse(value).with_context(|| format!("parse {label}"))?;
    if !schemes.contains(&parsed.scheme()) {
        bail!("{label} must use one of {schemes:?}");
    }
    Ok(())
}

pub fn parse_address(value: &str) -> Result<Address> {
    Address::from_str(value.trim()).with_context(|| format!("invalid address {value}"))
}

pub fn parse_u256(value: &str) -> Result<U256> {
    let value = value.trim();
    let (digits, radix) = value
        .strip_prefix("0x")
        .map_or((value, 10), |digits| (digits, 16));
    U256::from_str_radix(digits, radix).with_context(|| format!("invalid U256 {value}"))
}

const fn default_rpc_weight() -> u32 {
    100
}
const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolates_environment_inside_toml_strings() {
        let config = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example/${API_KEY}"
            "#,
            |key| (key == "API_KEY").then(|| "secret".to_owned()),
        )
        .unwrap();
        assert_eq!(config.rpc.canonical_ws, "wss://rpc.example/secret");
    }

    #[test]
    fn missing_environment_variable_is_an_error() {
        let error = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "${MISSING_RPC}"
            "#,
            |_| None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("MISSING_RPC"));
    }

    #[test]
    fn profile_items_merge_by_identity_and_can_be_replaced() {
        let config = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                replace_tokens = true
                [rpc]
                canonical_ws = "wss://rpc.example"

                [[tokens]]
                symbol = "WETH"
                address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
                decimals = 18
                connector = true
            "#,
            |_| None,
        )
        .unwrap();
        assert_eq!(config.tokens.len(), 1);
        assert_eq!(config.tokens[0].symbol, "WETH");
    }

    #[test]
    fn request_limits_are_validated() {
        let error = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
                [routing]
                default_max_hops = 5
                max_hops = 4
            "#,
            |_| None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("default_max_hops"));
    }

    #[test]
    fn zero_server_capacity_is_rejected_during_config_validation() {
        let error = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
                [server]
                max_in_flight_quotes = 0
            "#,
            |_| None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("server bounds"));
    }

    #[test]
    fn zero_rpc_capacity_is_rejected_during_config_validation() {
        let error = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
                batch_size = 0
            "#,
            |_| None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("RPC bounds"));
    }

    #[test]
    fn configured_admin_token_cannot_be_empty() {
        let error = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
                [server]
                admin_bearer_token = ""
            "#,
            |_| None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("admin_bearer_token"));
    }

    #[test]
    fn zero_default_quote_timeout_is_rejected() {
        let error = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
                [routing]
                default_timeout_ms = 0
            "#,
            |_| None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("routing timeouts"));
    }

    #[test]
    fn unsupported_cache_persistence_fails_config_validation() {
        let error = SidecarConfig::parse_with(
            r#"
                extends = "ethereum-mainnet"
                [rpc]
                canonical_ws = "wss://rpc.example"
                [storage]
                persist_cache = true
            "#,
            |_| None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("persist_cache"));
    }
}
