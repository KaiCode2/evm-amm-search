//! Benchmark-only observation of the physical HTTP JSON-RPC packets emitted
//! after transparent batching and immediately before load balancing.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use alloy_json_rpc::{Id, RequestPacket, ResponsePacket};
use alloy_primitives::B256;
use alloy_transport::{TransportError, TransportFut};
use tower::{Layer, Service};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
enum TransportKind {
    #[default]
    Http,
    WebSocket,
}

impl TransportKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Http => "state-http",
            Self::WebSocket => "canonical-ws",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RpcProfilePhase {
    #[default]
    Connect,
    CacheBuild,
    InitialWarmup,
    SubscriberAttach,
    FirstRoute,
}

impl RpcProfilePhase {
    const fn as_u8(self) -> u8 {
        self as u8
    }

    const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::CacheBuild,
            2 => Self::InitialWarmup,
            3 => Self::SubscriberAttach,
            4 => Self::FirstRoute,
            _ => Self::Connect,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::CacheBuild => "cache-build",
            Self::InitialWarmup => "initial-warmup",
            Self::SubscriberAttach => "subscriber-attach",
            Self::FirstRoute => "first-route",
        }
    }
}

#[derive(Clone)]
pub(crate) struct RpcProfileTransport<T> {
    inner: T,
    profile: Option<Arc<RpcProfile>>,
    transport: TransportKind,
}

impl<T> RpcProfileTransport<T> {
    pub(crate) fn http(inner: T) -> Self {
        Self::new(inner, TransportKind::Http)
    }

    fn new(inner: T, transport: TransportKind) -> Self {
        Self {
            inner,
            profile: benchmark_profile(),
            transport,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RpcProfileLayer;

impl RpcProfileLayer {
    pub(crate) const fn websocket() -> Self {
        Self
    }
}

impl<T> Layer<T> for RpcProfileLayer {
    type Service = RpcProfileTransport<T>;

    fn layer(&self, inner: T) -> Self::Service {
        RpcProfileTransport::new(inner, TransportKind::WebSocket)
    }
}

impl<T> Service<RequestPacket> for RpcProfileTransport<T>
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

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        let Some(profile) = self.profile.clone() else {
            return Box::pin(self.inner.call(request));
        };

        let observed = ObservedRequest::new(&request, profile.phase(), self.transport);
        let future = self.inner.call(request);
        profile.begin();
        Box::pin(async move {
            let started = Instant::now();
            let response = future.await;
            profile.finish(observed, started.elapsed(), response.as_ref().ok());
            response
        })
    }
}

#[derive(Default)]
struct MethodStats {
    calls: usize,
    packets: usize,
    duplicate_calls: usize,
    request_bytes: usize,
    response_bytes: usize,
    max_request_bytes: usize,
    max_response_bytes: usize,
}

#[derive(Default)]
struct PhaseStats {
    packets: usize,
    calls: usize,
    request_bytes: usize,
    response_bytes: usize,
    elapsed: Duration,
}

#[derive(Default)]
struct ProfileStats {
    packets: usize,
    logical_calls: usize,
    request_bytes: usize,
    response_bytes: usize,
    response_error_packets: usize,
    transport_errors: usize,
    duplicates: usize,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_packet_latency: Duration,
    batch_histogram: BTreeMap<usize, usize>,
    methods: BTreeMap<String, MethodStats>,
    phases: BTreeMap<RpcProfilePhase, PhaseStats>,
    transports: BTreeMap<TransportKind, PhaseStats>,
    fingerprints: HashMap<(String, B256), usize>,
}

struct RpcProfile {
    started: Instant,
    phase: AtomicU8,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    stats: Mutex<ProfileStats>,
}

impl RpcProfile {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            phase: AtomicU8::new(RpcProfilePhase::Connect.as_u8()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            stats: Mutex::new(ProfileStats::default()),
        }
    }

    fn phase(&self) -> RpcProfilePhase {
        RpcProfilePhase::from_u8(self.phase.load(Ordering::Relaxed))
    }

    fn begin(&self) {
        let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_in_flight.fetch_max(current, Ordering::Relaxed);
    }

    fn finish(
        &self,
        request: ObservedRequest,
        elapsed: Duration,
        response: Option<&ResponsePacket>,
    ) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        let response_bytes = response.map_or(0, response_json_bytes);
        let response_by_id = response.map(response_bytes_by_id).unwrap_or_default();
        let mut stats = self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stats.packets += 1;
        stats.logical_calls += request.calls.len();
        stats.request_bytes += request.json_bytes;
        stats.response_bytes += response_bytes;
        stats.max_request_bytes = stats.max_request_bytes.max(request.json_bytes);
        stats.max_response_bytes = stats.max_response_bytes.max(response_bytes);
        stats.max_packet_latency = stats.max_packet_latency.max(elapsed);
        *stats
            .batch_histogram
            .entry(request.calls.len())
            .or_default() += 1;
        if response.is_none() {
            stats.transport_errors += 1;
        } else if response.is_some_and(ResponsePacket::is_error) {
            stats.response_error_packets += 1;
        }

        let phase = stats.phases.entry(request.phase).or_default();
        phase.packets += 1;
        phase.calls += request.calls.len();
        phase.request_bytes += request.json_bytes;
        phase.response_bytes += response_bytes;
        phase.elapsed += elapsed;

        let transport = stats.transports.entry(request.transport).or_default();
        transport.packets += 1;
        transport.calls += request.calls.len();
        transport.request_bytes += request.json_bytes;
        transport.response_bytes += response_bytes;
        transport.elapsed += elapsed;

        let mut methods_in_packet = HashSet::new();
        for call in request.calls {
            let key = (call.method.clone(), call.params_hash);
            let seen = stats.fingerprints.entry(key).or_default();
            let duplicate = *seen > 0;
            *seen += 1;
            if duplicate {
                stats.duplicates += 1;
            }

            let method = stats.methods.entry(call.method.clone()).or_default();
            method.calls += 1;
            method.request_bytes += call.json_bytes;
            method.max_request_bytes = method.max_request_bytes.max(call.json_bytes);
            if let Some(response_bytes) = response_by_id.get(&call.id) {
                method.response_bytes += response_bytes;
                method.max_response_bytes = method.max_response_bytes.max(*response_bytes);
            }
            method.duplicate_calls += usize::from(duplicate);
            if methods_in_packet.insert(call.method) {
                method.packets += 1;
            }
        }
    }
}

struct ObservedCall {
    id: Id,
    method: String,
    params_hash: B256,
    json_bytes: usize,
}

struct ObservedRequest {
    phase: RpcProfilePhase,
    transport: TransportKind,
    json_bytes: usize,
    calls: Vec<ObservedCall>,
}

impl ObservedRequest {
    fn new(request: &RequestPacket, phase: RpcProfilePhase, transport: TransportKind) -> Self {
        let calls = request
            .requests()
            .iter()
            .map(|call| ObservedCall {
                id: call.id().clone(),
                method: call.method().to_owned(),
                params_hash: call.params_hash(),
                json_bytes: call.serialized().get().len(),
            })
            .collect::<Vec<_>>();
        let payload_bytes = calls.iter().map(|call| call.json_bytes).sum::<usize>();
        let json_bytes = if request.as_batch().is_some() {
            payload_bytes + 2 + calls.len().saturating_sub(1)
        } else {
            payload_bytes
        };
        Self {
            phase,
            transport,
            json_bytes,
            calls,
        }
    }
}

fn response_json_bytes(response: &ResponsePacket) -> usize {
    match response {
        ResponsePacket::Single(response) => {
            serde_json::to_vec(response).map_or(0, |json| json.len())
        }
        ResponsePacket::Batch(responses) => {
            responses
                .iter()
                .map(|response| serde_json::to_vec(response).map_or(0, |json| json.len()))
                .sum::<usize>()
                + 2
                + responses.len().saturating_sub(1)
        }
    }
}

fn response_bytes_by_id(response: &ResponsePacket) -> HashMap<Id, usize> {
    response
        .responses()
        .iter()
        .map(|response| {
            (
                response.id.clone(),
                serde_json::to_vec(response).map_or(0, |json| json.len()),
            )
        })
        .collect()
}

static PROFILE: OnceLock<Arc<RpcProfile>> = OnceLock::new();

fn benchmark_profile() -> Option<Arc<RpcProfile>> {
    let bench = env_enabled("AMM_ROUTE_TUI_BENCH", false);
    let network = env_enabled("AMM_ROUTE_TUI_NETWORK_PROFILE", true);
    (bench && network).then(|| PROFILE.get_or_init(|| Arc::new(RpcProfile::new())).clone())
}

pub(crate) fn set_phase(phase: RpcProfilePhase) {
    if let Some(profile) = PROFILE.get() {
        profile.phase.store(phase.as_u8(), Ordering::Relaxed);
    }
}

pub(crate) fn print_report() {
    let Some(profile) = PROFILE.get() else {
        return;
    };
    let stats = profile
        .stats
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let elapsed = profile.started.elapsed();
    let batch_ratio = if stats.packets == 0 {
        0.0
    } else {
        stats.logical_calls as f64 / stats.packets as f64
    };

    println!("\n=== cold-start JSON-RPC network profile ===");
    println!("window:                 {elapsed:?}");
    println!("physical RPC packets:   {}", stats.packets);
    println!(
        "logical RPC calls:      {} ({batch_ratio:.2} calls/packet)",
        stats.logical_calls
    );
    println!(
        "request JSON bytes:     {}",
        human_bytes(stats.request_bytes)
    );
    println!(
        "response JSON bytes:    {}",
        human_bytes(stats.response_bytes)
    );
    println!(
        "max request/response:   {} / {}",
        human_bytes(stats.max_request_bytes),
        human_bytes(stats.max_response_bytes)
    );
    println!("max packet latency:     {:?}", stats.max_packet_latency);
    println!(
        "max packets in flight:  {}",
        profile.max_in_flight.load(Ordering::Relaxed)
    );
    println!(
        "transport/rpc errors:   {} / {} packets",
        stats.transport_errors, stats.response_error_packets
    );
    println!("exact duplicate calls:  {}", stats.duplicates);
    println!(
        "batch-size histogram:   {}",
        stats
            .batch_histogram
            .iter()
            .map(|(size, count)| format!("{size}x:{count}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    println!(
        "\n{:<20}{:>10}{:>10}{:>13}{:>13}{:>13}",
        "phase", "packets", "calls", "request", "response", "sum latency"
    );
    for (phase, phase_stats) in &stats.phases {
        println!(
            "{:<20}{:>10}{:>10}{:>13}{:>13}{:>13?}",
            phase.label(),
            phase_stats.packets,
            phase_stats.calls,
            human_bytes(phase_stats.request_bytes),
            human_bytes(phase_stats.response_bytes),
            phase_stats.elapsed,
        );
    }

    println!(
        "\n{:<20}{:>10}{:>10}{:>13}{:>13}{:>13}",
        "transport", "packets", "calls", "request", "response", "sum latency"
    );
    for (transport, transport_stats) in &stats.transports {
        println!(
            "{:<20}{:>10}{:>10}{:>13}{:>13}{:>13?}",
            transport.label(),
            transport_stats.packets,
            transport_stats.calls,
            human_bytes(transport_stats.request_bytes),
            human_bytes(transport_stats.response_bytes),
            transport_stats.elapsed,
        );
    }

    let mut methods = stats.methods.iter().collect::<Vec<_>>();
    methods.sort_by(|(left_name, left), (right_name, right)| {
        right
            .calls
            .cmp(&left.calls)
            .then_with(|| left_name.cmp(right_name))
    });
    println!(
        "\n{:<24}{:>8}{:>9}{:>10}{:>12}{:>12}{:>12}",
        "method", "calls", "packets", "duplicate", "request", "response", "max response"
    );
    for (method, method_stats) in methods {
        println!(
            "{:<24}{:>8}{:>9}{:>10}{:>12}{:>12}{:>12}",
            method,
            method_stats.calls,
            method_stats.packets,
            method_stats.duplicate_calls,
            human_bytes(method_stats.request_bytes),
            human_bytes(method_stats.response_bytes),
            human_bytes(method_stats.max_response_bytes),
        );
    }
}

fn human_bytes(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    if bytes as f64 >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB)
    } else if bytes as f64 >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn env_enabled(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(default)
}
