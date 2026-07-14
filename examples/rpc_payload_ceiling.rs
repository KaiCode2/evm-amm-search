//! Provider-by-provider ceiling probe for the bulk storage `eth_call` used by
//! cold-start. This is intentionally a standalone benchmark: it never changes
//! the TUI/cache defaults and never lets load balancing hide which endpoint
//! accepted or rejected a payload.
//!
//! Environment:
//! - `AMM_NETWORK_BENCH_RPC_URLS` (falls back to `AMM_ROUTE_TUI_RPC_URLS`)
//! - `AMM_NETWORK_BENCH_SIZES` (default `1000,5000,...,40000`)
//! - `AMM_NETWORK_BENCH_BLOCK` (optional pinned mainnet block)
//! - `AMM_NETWORK_BENCH_MAX_PROVIDERS` (default all configured endpoints)
//! - `AMM_NETWORK_BENCH_PROVIDER_INDEX` (optional one-based endpoint selector)

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::Instant,
};

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_json_rpc::{RequestPacket, ResponsePacket};
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, U256, address};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_client::RpcClient;
use alloy_transport::{TransportError, TransportFut};
use alloy_transport_balancer::{HttpClientConfig, LoadBalancedTransport, Weight};
use anyhow::{Context, Result, bail};
use evm_fork_cache::{BulkCallConfig, CallDispatch, fetch_slots_bulk};
use reqwest::Url;
use tower::Service;

const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    let urls = rpc_urls()?;
    let selected_provider = std::env::var("AMM_NETWORK_BENCH_PROVIDER_INDEX")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .context("parse AMM_NETWORK_BENCH_PROVIDER_INDEX")
        })
        .transpose()?;
    let max_providers = env_usize("AMM_NETWORK_BENCH_MAX_PROVIDERS", urls.len()).min(urls.len());
    let sizes = requested_sizes()?;
    println!(
        "rpc_payload_ceiling: providers={}, sizes={:?}, target=WETH, dispatch=one eth_call",
        selected_provider.map_or(max_providers, |_| 1),
        sizes
    );
    println!(
        "Each row reads N unique slots through the same state-override extractor used by evm-fork-cache."
    );

    for (index, url) in urls
        .into_iter()
        .enumerate()
        .filter(|(index, _)| selected_provider.is_none_or(|selected| *index + 1 == selected))
        .take(max_providers)
    {
        let label = provider_label(index, &url);
        let (provider, payload_stats) = provider(url);
        let block = match std::env::var("AMM_NETWORK_BENCH_BLOCK") {
            Ok(value) => value
                .parse::<u64>()
                .with_context(|| format!("parse AMM_NETWORK_BENCH_BLOCK={value}"))?,
            Err(_) => provider
                .get_block_number()
                .await
                .with_context(|| format!("fetch latest block from {label}"))?,
        };
        println!("\nprovider={label} block={block}");
        println!(
            "{:<10}{:>24}{:>24}{:>14}  result",
            "slots", "request JSON", "response JSON", "elapsed"
        );

        for &slots in &sizes {
            let requests = (0..slots)
                .map(|slot| (WETH, U256::from(slot)))
                .collect::<Vec<_>>();
            let config = BulkCallConfig {
                max_slots_per_call: slots,
                max_targets_per_call: 1,
                max_concurrent_calls: 1,
                point_read_threshold: 0,
                pre_shanghai_extractor: false,
                dispatch: CallDispatch::PerCall,
                max_slots_per_request: slots,
                max_request_bytes: usize::MAX,
            };
            let started = Instant::now();
            let results = fetch_slots_bulk(
                &provider,
                requests,
                BlockId::Number(BlockNumberOrTag::Number(block)),
                config,
            )
            .await;
            let elapsed = started.elapsed();
            let failures = results
                .iter()
                .filter(|(_, _, result)| result.is_err())
                .count();
            let outcome = if failures == 0 && results.len() == slots {
                "ok".to_owned()
            } else {
                let sample = results
                    .iter()
                    .find_map(|(_, _, result)| result.as_ref().err())
                    .map(classify_error)
                    .unwrap_or_else(|| "missing results".to_owned());
                format!("failed {failures}/{} ({sample})", results.len())
            };
            println!(
                "{slots:<10}{:>24}{:>24}{:>14?}  {outcome}",
                human_bytes_exact(payload_stats.request_bytes.load(Ordering::Relaxed)),
                human_bytes_exact(payload_stats.response_bytes.load(Ordering::Relaxed)),
                elapsed,
            );
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }
    Ok(())
}

fn provider(url: Url) -> (RootProvider<AnyNetwork>, Arc<PayloadStats>) {
    let transport = LoadBalancedTransport::builder(vec![(url, Weight::default())])
        .http_client_config(HttpClientConfig {
            gzip: true,
            ..Default::default()
        })
        .build();
    let stats = Arc::new(PayloadStats::default());
    let transport = PayloadSizeTransport {
        inner: transport,
        stats: Arc::clone(&stats),
    };
    (RootProvider::new(RpcClient::new(transport, false)), stats)
}

#[derive(Default)]
struct PayloadStats {
    request_bytes: AtomicUsize,
    response_bytes: AtomicUsize,
}

#[derive(Clone)]
struct PayloadSizeTransport<T> {
    inner: T,
    stats: Arc<PayloadStats>,
}

impl<T> Service<RequestPacket> for PayloadSizeTransport<T>
where
    T: Service<RequestPacket, Response = ResponsePacket, Error = TransportError>
        + Clone
        + Send
        + 'static,
    T::Future: Send + 'static,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        self.stats
            .request_bytes
            .store(request_json_bytes(&request), Ordering::Relaxed);
        let stats = Arc::clone(&self.stats);
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await;
            stats.response_bytes.store(
                response.as_ref().map_or(0, response_json_bytes),
                Ordering::Relaxed,
            );
            response
        })
    }
}

fn request_json_bytes(request: &RequestPacket) -> usize {
    let payload = request
        .requests()
        .iter()
        .map(|request| request.serialized().get().len())
        .sum::<usize>();
    if request.as_batch().is_some() {
        payload + 2 + request.len().saturating_sub(1)
    } else {
        payload
    }
}

fn response_json_bytes(response: &ResponsePacket) -> usize {
    let payload = response
        .responses()
        .iter()
        .map(|response| serde_json::to_vec(response).map_or(0, |json| json.len()))
        .sum::<usize>();
    if response.as_batch().is_some() {
        payload + 2 + response.responses().len().saturating_sub(1)
    } else {
        payload
    }
}

fn rpc_urls() -> Result<Vec<Url>> {
    let raw = std::env::var("AMM_NETWORK_BENCH_RPC_URLS")
        .or_else(|_| std::env::var("AMM_ROUTE_TUI_RPC_URLS"))
        .context("set AMM_NETWORK_BENCH_RPC_URLS or AMM_ROUTE_TUI_RPC_URLS")?;
    let urls = raw
        .split([',', ';'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Url::parse)
        .collect::<Result<Vec<_>, _>>()?;
    if urls.is_empty() {
        bail!("no HTTP RPC URLs configured");
    }
    Ok(urls)
}

fn requested_sizes() -> Result<Vec<usize>> {
    let raw = std::env::var("AMM_NETWORK_BENCH_SIZES")
        .unwrap_or_else(|_| "1000,5000,10000,15000,20000,25000,30000,35000,40000".to_owned());
    let sizes = raw
        .split([',', ';'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::parse::<usize>)
        .collect::<Result<BTreeSet<_>, _>>()?
        .into_iter()
        .filter(|size| *size > 0)
        .collect::<Vec<_>>();
    if sizes.is_empty() {
        bail!("AMM_NETWORK_BENCH_SIZES did not contain a positive slot count");
    }
    Ok(sizes)
}

fn provider_label(index: usize, url: &Url) -> String {
    let host = url.host_str().unwrap_or("unknown-host");
    let brand = if host.contains("alchemy") {
        "alchemy"
    } else if host.contains("infura") {
        "infura"
    } else if host.contains("quicknode") || host.contains("quiknode") {
        "quicknode"
    } else if host.contains("drpc") {
        "drpc"
    } else {
        "provider"
    };
    format!("{brand}#{}", index + 1)
}

fn classify_error(error: &evm_fork_cache::StorageFetchError) -> String {
    let message = error.to_string();
    for (needle, label) in [
        ("413", "HTTP 413 payload too large"),
        ("429", "HTTP 429 rate limited"),
        ("execution aborted", "execution aborted"),
        ("out of gas", "out of gas"),
        ("response too large", "response too large"),
        ("timed out", "timeout"),
        ("timeout", "timeout"),
    ] {
        if message.to_ascii_lowercase().contains(needle) {
            return label.to_owned();
        }
    }
    "provider rejected or malformed the call".to_owned()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn human_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn human_bytes_exact(bytes: usize) -> String {
    format!("{} ({bytes} B)", human_bytes(bytes))
}
