use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver as StdReceiver, SyncSender, TrySendError, sync_channel},
    },
    thread,
};

use evm_amm_state::adapters::{
    AmmChangeSet, AmmRuntimeCommandError, AmmRuntimeHandle, AmmRuntimeId, AmmStatePoint,
    AmmStateVersion,
};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::{
    GraphBuildOptions, GraphDelta, GraphVersion, LiveAmmGraph, LiveGraphError, LiveSearchView,
    RouteQuote, RouteRequest, RouteSearchEvent, SearchControl, SearchError, StreamingSearchConfig,
    StreamingSearchReport,
};

/// Stable identity of one logical live route subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RouteSubscriptionId(u64);

impl RouteSubscriptionId {
    /// Numeric process-local subscription identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic identity of one search attempt within a subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RouteSearchJobId(u64);

impl RouteSearchJobId {
    /// Numeric subscription-local search identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Complete AMM/search provenance represented by a route result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RouteProvenance {
    runtime_id: AmmRuntimeId,
    state_version: AmmStateVersion,
    point: AmmStatePoint,
    graph_version: GraphVersion,
}

impl RouteProvenance {
    fn from_view(view: &LiveSearchView) -> Self {
        Self {
            runtime_id: view.snapshot().runtime_id(),
            state_version: view.snapshot().version(),
            point: view.snapshot().point(),
            graph_version: view.graph().version(),
        }
    }

    /// Source AMM runtime lineage.
    pub const fn runtime_id(self) -> AmmRuntimeId {
        self.runtime_id
    }

    /// Source coherent AMM state version.
    pub const fn state_version(self) -> AmmStateVersion {
        self.state_version
    }

    /// Source complete chain-state point.
    pub const fn point(self) -> AmmStatePoint {
        self.point
    }

    /// Source graph topology version.
    pub const fn graph_version(self) -> GraphVersion {
        self.graph_version
    }
}

/// Complete fence carried by one worker search and every progressive result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RouteJobStamp {
    subscription: RouteSubscriptionId,
    epoch: u64,
    job: RouteSearchJobId,
    source: RouteProvenance,
}

impl RouteJobStamp {
    /// Logical route subscription.
    pub const fn subscription(self) -> RouteSubscriptionId {
        self.subscription
    }

    /// Subscription incarnation used to reject work from a replacement.
    pub const fn epoch(self) -> u64 {
        self.epoch
    }

    /// Search attempt within the subscription.
    pub const fn job(self) -> RouteSearchJobId {
        self.job
    }

    /// Immutable AMM/search source of the attempt.
    pub const fn source(self) -> RouteProvenance {
        self.source
    }
}

/// Route quote paired with the exact state and graph that produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedRouteQuote {
    source: RouteProvenance,
    quote: RouteQuote,
}

impl VersionedRouteQuote {
    /// Exact result provenance.
    pub const fn source(&self) -> RouteProvenance {
        self.source
    }

    /// Underlying route quote.
    pub const fn quote(&self) -> &RouteQuote {
        &self.quote
    }
}

#[derive(Debug)]
struct RouteCancellationState {
    cancelled: AtomicBool,
    cleanup: Mutex<
        Option<(
            mpsc::UnboundedSender<RouteSubscriptionId>,
            RouteSubscriptionId,
        )>,
    >,
}

/// Cloneable cooperative cancellation shared with the active route job.
#[derive(Clone, Debug)]
pub struct RouteCancellationToken(Arc<RouteCancellationState>);

impl Default for RouteCancellationToken {
    fn default() -> Self {
        Self(Arc::new(RouteCancellationState {
            cancelled: AtomicBool::new(false),
            cleanup: Mutex::new(None),
        }))
    }
}

impl RouteCancellationToken {
    fn attached(
        id: RouteSubscriptionId,
        cleanup: mpsc::UnboundedSender<RouteSubscriptionId>,
    ) -> Self {
        Self(Arc::new(RouteCancellationState {
            cancelled: AtomicBool::new(false),
            cleanup: Mutex::new(Some((cleanup, id))),
        }))
    }

    fn request(&self) -> bool {
        !self.0.cancelled.swap(true, Ordering::AcqRel)
    }

    /// Request cancellation. This operation is immediate and idempotent.
    pub fn cancel(&self) {
        if !self.request() {
            return;
        }
        if let Some((cleanup, id)) = self
            .0
            .cleanup
            .lock()
            .expect("route cancellation cleanup poisoned")
            .take()
        {
            let _ = cleanup.send(id);
        }
    }

    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }
}

/// Live route-runtime controls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveRouteRuntimeConfig {
    /// Long-lived route worker threads.
    pub worker_threads: usize,
    /// Bounded worker admission queue.
    pub job_queue_capacity: usize,
    /// Bounded actor command queue.
    pub command_capacity: usize,
    /// Bounded internal worker-result queue.
    pub result_capacity: usize,
    /// Lossy observer event capacity.
    pub event_capacity: usize,
    /// Hard route-subscription limit.
    pub max_subscriptions: usize,
}

impl Default for LiveRouteRuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: thread::available_parallelism().map_or(1, usize::from),
            job_queue_capacity: 64,
            command_capacity: 64,
            result_capacity: 256,
            event_capacity: 512,
            max_subscriptions: 1_024,
        }
    }
}

impl LiveRouteRuntimeConfig {
    /// Override the persistent route worker count.
    pub const fn with_worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = worker_threads;
        self
    }

    /// Override bounded worker queue capacity.
    pub const fn with_job_queue_capacity(mut self, capacity: usize) -> Self {
        self.job_queue_capacity = capacity;
        self
    }

    /// Override the maximum live route subscriptions.
    pub const fn with_max_subscriptions(mut self, max_subscriptions: usize) -> Self {
        self.max_subscriptions = max_subscriptions;
        self
    }

    /// Override lossy observer capacity.
    pub const fn with_event_capacity(mut self, event_capacity: usize) -> Self {
        self.event_capacity = event_capacity;
        self
    }

    fn validate(self) -> Result<Self, LiveRouteRuntimeError> {
        if self.worker_threads == 0
            || self.job_queue_capacity == 0
            || self.command_capacity == 0
            || self.result_capacity == 0
            || self.event_capacity == 0
            || self.max_subscriptions == 0
        {
            return Err(LiveRouteRuntimeError::InvalidConfig);
        }
        Ok(self)
    }
}

/// Request and search controls retained by a live route subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteSubscriptionSpec {
    /// Route request to recompute at each coherent source point.
    pub request: RouteRequest,
    /// Progressive search policy used by each attempt.
    pub streaming: StreamingSearchConfig,
}

impl RouteSubscriptionSpec {
    /// Construct a route subscription.
    pub const fn new(request: RouteRequest, streaming: StreamingSearchConfig) -> Self {
        Self { request, streaming }
    }
}

/// Failure isolated to one route search attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteSearchFailure {
    message: String,
    worker_panicked: bool,
}

/// Terminal coordinator failure while consuming/applying reliable AMM state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveRouteRuntimeFailure {
    message: String,
}

impl LiveRouteRuntimeFailure {
    /// Human-readable terminal failure.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl RouteSearchFailure {
    /// Human-readable search failure.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether a panic was caught at the reusable worker boundary.
    pub const fn worker_panicked(&self) -> bool {
        self.worker_panicked
    }
}

/// Recoverable current state of one route subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteSubscriptionState {
    /// Waiting for bounded worker admission.
    Pending {
        /// Newest source the subscription must represent.
        source: RouteProvenance,
        /// Last accepted quote, retained only as historical context.
        previous: Option<Arc<VersionedRouteQuote>>,
    },
    /// A current search attempt is running or queued.
    Searching {
        /// Current complete job fence.
        stamp: RouteJobStamp,
        /// Best progressive result accepted for this job.
        provisional: Option<Arc<VersionedRouteQuote>>,
        /// Last fully accepted quote from an older source.
        previous: Option<Arc<VersionedRouteQuote>>,
    },
    /// Search completed for the exact current source.
    Ready {
        /// Exact accepted source.
        source: RouteProvenance,
        /// Best route, or `None` when no route was viable.
        best: Option<Arc<VersionedRouteQuote>>,
        /// Complete search report.
        report: Box<StreamingSearchReport>,
    },
    /// Current-source search failed without affecting other subscriptions.
    Failed {
        /// Source that failed.
        source: RouteProvenance,
        /// Isolated failure.
        failure: Arc<RouteSearchFailure>,
    },
    /// Explicitly cancelled subscription.
    Cancelled,
    /// Route runtime stopped.
    Closed,
    /// Route runtime stopped because reliable state could no longer be applied.
    RuntimeFailed {
        /// Recoverable terminal failure details.
        failure: Arc<LiveRouteRuntimeFailure>,
    },
}

impl RouteSubscriptionState {
    /// Current required/accepted source, when the state is provenance-bearing.
    pub const fn source(&self) -> Option<RouteProvenance> {
        match self {
            Self::Pending { source, .. }
            | Self::Ready { source, .. }
            | Self::Failed { source, .. } => Some(*source),
            Self::Searching { stamp, .. } => Some(stamp.source()),
            Self::Cancelled | Self::Closed | Self::RuntimeFailed { .. } => None,
        }
    }
}

/// Recoverable latest-value publication for one route subscription.
#[derive(Clone)]
pub struct RouteSubscriptionSnapshot {
    sequence: u64,
    id: RouteSubscriptionId,
    epoch: u64,
    view: Arc<LiveSearchView>,
    state: RouteSubscriptionState,
}

impl std::fmt::Debug for RouteSubscriptionSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteSubscriptionSnapshot")
            .field("sequence", &self.sequence)
            .field("id", &self.id)
            .field("epoch", &self.epoch)
            .field("runtime_id", &self.view.snapshot().runtime_id())
            .field("state_version", &self.view.snapshot().version())
            .field("point", &self.view.snapshot().point())
            .field("graph_version", &self.view.graph().version())
            .field("state", &self.state)
            .finish()
    }
}

impl PartialEq for RouteSubscriptionSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
            && self.id == other.id
            && self.epoch == other.epoch
            && self.view.snapshot().runtime_id() == other.view.snapshot().runtime_id()
            && self.view.snapshot().version() == other.view.snapshot().version()
            && self.view.snapshot().point() == other.view.snapshot().point()
            && self.view.graph().version() == other.view.graph().version()
            && self.state == other.state
    }
}

impl Eq for RouteSubscriptionSnapshot {}

impl RouteSubscriptionSnapshot {
    /// Actor publication sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Route subscription identity.
    pub const fn id(&self) -> RouteSubscriptionId {
        self.id
    }

    /// Request incarnation represented by this authoritative publication.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Exact immutable graph, registry, liquidity, and AMM snapshot for this state.
    pub const fn view(&self) -> &Arc<LiveSearchView> {
        &self.view
    }

    /// Recoverable current route state.
    pub const fn state(&self) -> &RouteSubscriptionState {
        &self.state
    }
}

/// Why a previously accepted/provisional quote became historical.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteInvalidationReason {
    /// A newer coherent AMM state was applied.
    AmmStateAdvanced,
    /// The logical subscription request or search policy changed.
    RequestChanged,
    /// Subscription was explicitly cancelled.
    Cancelled,
    /// Route runtime is shutting down.
    RuntimeClosed,
}

/// Trigger for a fresh route search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteSearchTrigger {
    /// Initial subscription result.
    Initial,
    /// New coherent AMM commit.
    AmmCommit,
    /// Logical subscription request replacement.
    RequestChanged,
}

/// Typed lifecycle and pipeline events emitted by the route runtime.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LiveRouteRuntimeEventKind {
    /// Logical route subscription accepted.
    SubscriptionAccepted,
    /// Reliable AMM commit applied to the live search universe.
    AmmCommitApplied {
        /// Typed AMM state changes.
        changes: Arc<AmmChangeSet>,
        /// Search graph/liquidity consequence.
        graph_delta: Arc<GraphDelta>,
    },
    /// Previously published/provisional result became historical.
    RouteInvalidated {
        /// Source that is no longer current.
        previous: RouteProvenance,
        /// Invalidation cause.
        reason: RouteInvalidationReason,
    },
    /// Fresh work admitted to the reusable worker pool.
    SearchScheduled { trigger: RouteSearchTrigger },
    /// Existing streaming search event accepted through the current fence.
    SearchEvent(RouteSearchEvent),
    /// Current-source final result installed into the recoverable watch state.
    RoutePublished {
        /// Previously accepted quote.
        previous: Option<Arc<VersionedRouteQuote>>,
        /// Newly accepted best quote.
        current: Option<Arc<VersionedRouteQuote>>,
        /// Complete current-source report.
        report: StreamingSearchReport,
    },
    /// Late worker completion was rejected by the provenance/job fence.
    StaleResultRejected {
        /// Source used by the worker.
        produced: RouteProvenance,
        /// Current required source.
        current: RouteProvenance,
    },
    /// Current-source search failed.
    SearchFailed { failure: Arc<RouteSearchFailure> },
    /// Subscription cancelled; no later result can become authoritative.
    SubscriptionCancelled,
    /// Reliable AMM consumption or graph application failed terminally.
    RuntimeFailed {
        /// Terminal failure details.
        failure: Arc<LiveRouteRuntimeFailure>,
    },
    /// Route runtime stopped.
    RuntimeClosed,
}

/// Sequenced event assigned only by the route coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveRouteRuntimeEvent {
    sequence: u64,
    subscription: Option<RouteSubscriptionId>,
    stamp: Option<RouteJobStamp>,
    kind: LiveRouteRuntimeEventKind,
}

impl LiveRouteRuntimeEvent {
    /// Global route-runtime event sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Associated logical subscription, when any.
    pub const fn subscription(&self) -> Option<RouteSubscriptionId> {
        self.subscription
    }

    /// Associated complete worker fence, when any.
    pub const fn stamp(&self) -> Option<RouteJobStamp> {
        self.stamp
    }

    /// Typed event payload.
    pub const fn kind(&self) -> &LiveRouteRuntimeEventKind {
        &self.kind
    }
}

/// Observer receive error. Event lag never affects recoverable route state.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum LiveRouteObserverError {
    /// Observer fell behind by this many events.
    #[error("route observer lagged by {0} events")]
    Lagged(u64),
    /// Route runtime closed.
    #[error("route runtime closed")]
    Closed,
}

/// Lossy typed route-event observer with explicit lag reporting.
pub struct LiveRouteObserver {
    events: broadcast::Receiver<Arc<LiveRouteRuntimeEvent>>,
    exited: watch::Receiver<bool>,
}

impl LiveRouteObserver {
    /// Receive the next typed route event.
    pub async fn next_event(
        &mut self,
    ) -> Result<Arc<LiveRouteRuntimeEvent>, LiveRouteObserverError> {
        loop {
            tokio::select! {
                event = self.events.recv() => return match event {
                    Ok(event) => Ok(event),
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        Err(LiveRouteObserverError::Lagged(count))
                    }
                    Err(broadcast::error::RecvError::Closed) => Err(LiveRouteObserverError::Closed),
                },
                changed = self.exited.changed() => {
                    if changed.is_err() || (*self.exited.borrow() && self.events.is_empty()) {
                        return Err(LiveRouteObserverError::Closed);
                    }
                }
            }
        }
    }
}

/// Stage 8 live route-runtime failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LiveRouteRuntimeError {
    /// Runtime configuration contains a zero bound.
    #[error("live route runtime capacities and worker count must be non-zero")]
    InvalidConfig,
    /// AMM critical subscription could not be acquired.
    #[error(transparent)]
    AmmSubscription(#[from] AmmRuntimeCommandError),
    /// Baseline or commit could not be represented by the live graph.
    #[error(transparent)]
    LiveGraph(#[from] LiveGraphError),
    /// Snapshot and live graph could not form a coherent worker view.
    #[error("live route search view rejected: {0}")]
    SearchView(String),
    /// An operating-system route worker could not be created.
    #[error("failed to spawn live route worker: {0}")]
    WorkerSpawn(#[source] std::io::Error),
    /// Actor command channel closed.
    #[error("live route runtime closed")]
    Closed,
    /// Configured route subscription limit was reached.
    #[error("live route subscription capacity reached")]
    SubscriptionCapacity,
    /// Requested route subscription no longer exists.
    #[error("route subscription not found: {0:?}")]
    SubscriptionNotFound(RouteSubscriptionId),
    /// Runtime/subscription/job sequence exhausted.
    #[error("live route runtime sequence exhausted")]
    SequenceExhausted,
}

/// Marker namespace for spawning the search-owned live route actor.
pub struct LiveRouteRuntime;

impl LiveRouteRuntime {
    /// Acquire the single reliable AMM subscription and start the live route actor.
    pub async fn spawn(
        amm: &AmmRuntimeHandle,
        graph_options: GraphBuildOptions,
        config: LiveRouteRuntimeConfig,
    ) -> Result<LiveRouteRuntimeHandle, LiveRouteRuntimeError> {
        let config = config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|_| LiveRouteRuntimeError::Closed)?;
        let changes = amm.subscribe_changes().await?;
        let live = LiveAmmGraph::from_snapshot(changes.snapshot(), graph_options)?;
        let view = Arc::new(
            LiveSearchView::new(Arc::clone(changes.snapshot()), &live)
                .map_err(|error| LiveRouteRuntimeError::SearchView(error.to_string()))?,
        );
        let (commands, command_rx) = mpsc::channel(config.command_capacity);
        let (cleanup, cleanup_rx) = mpsc::unbounded_channel();
        let (events, _) = broadcast::channel(config.event_capacity);
        let (exited, exited_rx) = watch::channel(false);
        let (worker_tx, worker_rx) = mpsc::channel(config.result_capacity);
        let workers =
            RouteWorkerPool::new(config, worker_tx).map_err(LiveRouteRuntimeError::WorkerSpawn)?;
        let actor = LiveRouteActor {
            changes,
            live,
            view,
            commands: command_rx,
            cleanup: cleanup_rx,
            worker_results: worker_rx,
            workers: Some(workers),
            events: events.clone(),
            entries: BTreeMap::new(),
            pending: VecDeque::new(),
            pending_set: BTreeSet::new(),
            next_subscription: 0,
            sequence: 0,
            max_subscriptions: config.max_subscriptions,
            shutdown_response: None,
            terminal_failure: None,
            exited,
        };
        tokio::spawn(actor.run());
        Ok(LiveRouteRuntimeHandle {
            commands,
            cleanup,
            events,
            exited: exited_rx,
        })
    }
}

/// Cheap cloneable control and event handle for the live route actor.
#[derive(Clone)]
pub struct LiveRouteRuntimeHandle {
    commands: mpsc::Sender<RouteCommand>,
    cleanup: mpsc::UnboundedSender<RouteSubscriptionId>,
    events: broadcast::Sender<Arc<LiveRouteRuntimeEvent>>,
    exited: watch::Receiver<bool>,
}

impl LiveRouteRuntimeHandle {
    /// Create a logical route subscription over the actor's current immutable view.
    pub async fn subscribe(
        &self,
        spec: RouteSubscriptionSpec,
    ) -> Result<LiveRouteSubscription, LiveRouteRuntimeError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(RouteCommand::Subscribe {
                spec: Box::new(spec),
                runtime: self.clone(),
                response,
            })
            .await
            .map_err(|_| LiveRouteRuntimeError::Closed)?;
        result.await.map_err(|_| LiveRouteRuntimeError::Closed)?
    }

    /// Cancel and remove one logical route subscription.
    pub async fn unsubscribe(&self, id: RouteSubscriptionId) -> Result<(), LiveRouteRuntimeError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(RouteCommand::Cancel { id, response })
            .await
            .map_err(|_| LiveRouteRuntimeError::Closed)?;
        result.await.map_err(|_| LiveRouteRuntimeError::Closed)?
    }

    /// Replace one subscription's request in place while retaining its identity.
    pub async fn replace(
        &self,
        id: RouteSubscriptionId,
        spec: RouteSubscriptionSpec,
    ) -> Result<(), LiveRouteRuntimeError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(RouteCommand::Replace {
                id,
                spec: Box::new(spec),
                response,
            })
            .await
            .map_err(|_| LiveRouteRuntimeError::Closed)?;
        result.await.map_err(|_| LiveRouteRuntimeError::Closed)?
    }

    /// Subscribe to lossy typed events with explicit lag errors.
    pub fn subscribe_events(&self) -> LiveRouteObserver {
        LiveRouteObserver {
            events: self.events.subscribe(),
            exited: self.exited.clone(),
        }
    }

    /// Stop the route actor and reusable workers without stopping the AMM runtime.
    pub async fn shutdown(&self) -> Result<(), LiveRouteRuntimeError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(RouteCommand::Shutdown { response })
            .await
            .map_err(|_| LiveRouteRuntimeError::Closed)?;
        result.await.map_err(|_| LiveRouteRuntimeError::Closed)
    }
}

/// Logical route subscription with recoverable state and cooperative cancellation.
pub struct LiveRouteSubscription {
    id: RouteSubscriptionId,
    snapshots: watch::Receiver<Arc<RouteSubscriptionSnapshot>>,
    observer: LiveRouteObserver,
    cancellation: RouteCancellationToken,
    runtime: LiveRouteRuntimeHandle,
}

impl LiveRouteSubscription {
    /// Logical subscription identity.
    pub const fn id(&self) -> RouteSubscriptionId {
        self.id
    }

    /// Latest recoverable route state.
    pub fn latest(&self) -> Arc<RouteSubscriptionSnapshot> {
        self.snapshots.borrow().clone()
    }

    /// Wait for and return the next recoverable route state.
    pub async fn changed(
        &mut self,
    ) -> Result<Arc<RouteSubscriptionSnapshot>, LiveRouteRuntimeError> {
        self.snapshots
            .changed()
            .await
            .map_err(|_| LiveRouteRuntimeError::Closed)?;
        Ok(self.snapshots.borrow_and_update().clone())
    }

    /// Receive the next typed route-runtime event.
    pub async fn next_event(
        &mut self,
    ) -> Result<Arc<LiveRouteRuntimeEvent>, LiveRouteObserverError> {
        self.observer.next_event().await
    }

    /// Clone the immediate cooperative cancellation token.
    pub fn cancellation_token(&self) -> RouteCancellationToken {
        self.cancellation.clone()
    }

    /// Cancel this subscription and wait until the actor removes it.
    pub async fn cancel(&self) -> Result<(), LiveRouteRuntimeError> {
        self.cancellation.request();
        self.runtime.unsubscribe(self.id).await
    }

    /// Replace this logical request without changing subscription identity.
    pub async fn replace(&self, spec: RouteSubscriptionSpec) -> Result<(), LiveRouteRuntimeError> {
        self.runtime.replace(self.id, spec).await
    }
}

impl Drop for LiveRouteSubscription {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let _ = self.runtime.cleanup.send(self.id);
    }
}

enum RouteCommand {
    Subscribe {
        spec: Box<RouteSubscriptionSpec>,
        runtime: LiveRouteRuntimeHandle,
        response: oneshot::Sender<Result<LiveRouteSubscription, LiveRouteRuntimeError>>,
    },
    Cancel {
        id: RouteSubscriptionId,
        response: oneshot::Sender<Result<(), LiveRouteRuntimeError>>,
    },
    Replace {
        id: RouteSubscriptionId,
        spec: Box<RouteSubscriptionSpec>,
        response: oneshot::Sender<Result<(), LiveRouteRuntimeError>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

struct RouteEntry {
    spec: RouteSubscriptionSpec,
    epoch: u64,
    next_job: u64,
    active: Option<ActiveRouteJob>,
    dirty: bool,
    trigger: RouteSearchTrigger,
    last_ready: Option<Arc<VersionedRouteQuote>>,
    snapshots: watch::Sender<Arc<RouteSubscriptionSnapshot>>,
    cancellation: RouteCancellationToken,
}

struct ActiveRouteJob {
    stamp: RouteJobStamp,
    cancellation: RouteCancellationToken,
}

struct LiveRouteActor {
    changes: evm_amm_state::adapters::AmmChangeSubscription,
    live: LiveAmmGraph,
    view: Arc<LiveSearchView>,
    commands: mpsc::Receiver<RouteCommand>,
    cleanup: mpsc::UnboundedReceiver<RouteSubscriptionId>,
    worker_results: mpsc::Receiver<RouteWorkerMessage>,
    workers: Option<RouteWorkerPool>,
    events: broadcast::Sender<Arc<LiveRouteRuntimeEvent>>,
    entries: BTreeMap<RouteSubscriptionId, RouteEntry>,
    pending: VecDeque<RouteSubscriptionId>,
    pending_set: BTreeSet<RouteSubscriptionId>,
    next_subscription: u64,
    sequence: u64,
    max_subscriptions: usize,
    shutdown_response: Option<oneshot::Sender<()>>,
    terminal_failure: Option<Arc<LiveRouteRuntimeFailure>>,
    exited: watch::Sender<bool>,
}

const MAX_COMMIT_BURST: usize = 16;

enum LiveRouteActorInput {
    Commit(Option<Arc<evm_amm_state::adapters::AmmStateCommit>>),
    Command(Option<Box<RouteCommand>>),
    Cleanup(Option<RouteSubscriptionId>),
    Worker(Option<Box<RouteWorkerMessage>>),
}

impl LiveRouteActor {
    async fn run(mut self) {
        let mut commit_streak = 0_usize;
        loop {
            match self.next_input(commit_streak >= MAX_COMMIT_BURST).await {
                LiveRouteActorInput::Commit(commit) => {
                    let Some(commit) = commit else { break };
                    if let Err(error) = self.apply_commit(commit) {
                        self.fail(error);
                        break;
                    }
                    commit_streak = commit_streak.saturating_add(1);
                }
                LiveRouteActorInput::Command(command) => {
                    let Some(command) = command else { break };
                    if self.handle_command(*command) {
                        break;
                    }
                    commit_streak = 0;
                }
                LiveRouteActorInput::Cleanup(cleanup) => {
                    if let Some(id) = cleanup {
                        let _ = self.cancel_subscription(id);
                    }
                    commit_streak = 0;
                }
                LiveRouteActorInput::Worker(result) => {
                    let Some(result) = result else { break };
                    // A fairness turn may select a worker while a canonical
                    // commit is also ready. Apply one such commit first: any
                    // result from the actor's previous view is then fenced out.
                    if let Some(commit) = self.changes.try_next_commit()
                        && let Err(error) = self.apply_commit(commit)
                    {
                        self.fail(error);
                        break;
                    }
                    self.handle_worker_message(*result);
                    commit_streak = 0;
                }
            }
            self.schedule_pending();
        }
        self.close().await;
    }

    async fn next_input(&mut self, prefer_fairness: bool) -> LiveRouteActorInput {
        if prefer_fairness {
            tokio::select! {
                biased;
                command = self.commands.recv() => {
                    LiveRouteActorInput::Command(command.map(Box::new))
                }
                cleanup = self.cleanup.recv() => LiveRouteActorInput::Cleanup(cleanup),
                result = self.worker_results.recv() => {
                    LiveRouteActorInput::Worker(result.map(Box::new))
                }
                commit = self.changes.next_commit() => LiveRouteActorInput::Commit(commit),
            }
        } else {
            tokio::select! {
                biased;
                commit = self.changes.next_commit() => LiveRouteActorInput::Commit(commit),
                command = self.commands.recv() => {
                    LiveRouteActorInput::Command(command.map(Box::new))
                }
                cleanup = self.cleanup.recv() => LiveRouteActorInput::Cleanup(cleanup),
                result = self.worker_results.recv() => {
                    LiveRouteActorInput::Worker(result.map(Box::new))
                }
            }
        }
    }

    fn fail(&mut self, error: LiveRouteRuntimeError) {
        self.terminal_failure = Some(Arc::new(LiveRouteRuntimeFailure {
            message: error.to_string(),
        }));
    }

    fn apply_commit(
        &mut self,
        commit: Arc<evm_amm_state::adapters::AmmStateCommit>,
    ) -> Result<(), LiveRouteRuntimeError> {
        let delta = Arc::new(self.live.apply_commit(&commit)?);
        let view = Arc::new(
            LiveSearchView::new(Arc::clone(commit.snapshot()), &self.live)
                .map_err(|error| LiveRouteRuntimeError::SearchView(error.to_string()))?,
        );
        self.view = view;
        self.publish_event(
            None,
            None,
            LiveRouteRuntimeEventKind::AmmCommitApplied {
                changes: Arc::clone(commit.changes()),
                graph_delta: delta,
            },
        );
        let current = RouteProvenance::from_view(&self.view);
        let ids = self.entries.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let previous = self.entries.get(&id).and_then(entry_published_source);
            if let Some(entry) = self.entries.get_mut(&id) {
                if let Some(active) = &entry.active {
                    active.cancellation.cancel();
                }
                entry.dirty = true;
                entry.trigger = RouteSearchTrigger::AmmCommit;
            }
            self.publish_snapshot(
                id,
                RouteSubscriptionState::Pending {
                    source: current,
                    previous: self
                        .entries
                        .get(&id)
                        .and_then(|entry| entry.last_ready.clone()),
                },
            );
            if let Some(previous) = previous {
                self.publish_event(
                    Some(id),
                    None,
                    LiveRouteRuntimeEventKind::RouteInvalidated {
                        previous,
                        reason: RouteInvalidationReason::AmmStateAdvanced,
                    },
                );
            }
            if self
                .entries
                .get(&id)
                .is_some_and(|entry| entry.active.is_none())
            {
                self.enqueue_pending(id);
            }
        }
        Ok(())
    }

    fn handle_command(&mut self, command: RouteCommand) -> bool {
        match command {
            RouteCommand::Subscribe {
                spec,
                runtime,
                response,
            } => {
                let result = self.add_subscription(*spec, runtime);
                let _ = response.send(result);
                false
            }
            RouteCommand::Cancel { id, response } => {
                let result = self.cancel_subscription(id);
                let _ = response.send(result);
                false
            }
            RouteCommand::Replace { id, spec, response } => {
                let result = self.replace_subscription(id, *spec);
                let _ = response.send(result);
                false
            }
            RouteCommand::Shutdown { response } => {
                self.shutdown_response = Some(response);
                true
            }
        }
    }

    fn add_subscription(
        &mut self,
        spec: RouteSubscriptionSpec,
        runtime: LiveRouteRuntimeHandle,
    ) -> Result<LiveRouteSubscription, LiveRouteRuntimeError> {
        if self.entries.len() >= self.max_subscriptions {
            return Err(LiveRouteRuntimeError::SubscriptionCapacity);
        }
        let raw = self.next_subscription;
        self.next_subscription = self
            .next_subscription
            .checked_add(1)
            .ok_or(LiveRouteRuntimeError::SequenceExhausted)?;
        let id = RouteSubscriptionId(raw);
        let source = RouteProvenance::from_view(&self.view);
        let sequence = self.next_sequence();
        let initial = Arc::new(RouteSubscriptionSnapshot {
            sequence,
            id,
            epoch: 0,
            view: Arc::clone(&self.view),
            state: RouteSubscriptionState::Pending {
                source,
                previous: None,
            },
        });
        let (snapshots, snapshot_rx) = watch::channel(initial);
        let cancellation = RouteCancellationToken::attached(id, runtime.cleanup.clone());
        self.entries.insert(
            id,
            RouteEntry {
                spec,
                epoch: 0,
                next_job: 0,
                active: None,
                dirty: true,
                trigger: RouteSearchTrigger::Initial,
                last_ready: None,
                snapshots,
                cancellation: cancellation.clone(),
            },
        );
        self.enqueue_pending(id);
        let observer = LiveRouteObserver {
            events: runtime.events.subscribe(),
            exited: runtime.exited.clone(),
        };
        self.publish_event(
            Some(id),
            None,
            LiveRouteRuntimeEventKind::SubscriptionAccepted,
        );
        Ok(LiveRouteSubscription {
            id,
            snapshots: snapshot_rx,
            observer,
            cancellation,
            runtime,
        })
    }

    fn cancel_subscription(
        &mut self,
        id: RouteSubscriptionId,
    ) -> Result<(), LiveRouteRuntimeError> {
        let Some(entry) = self.entries.remove(&id) else {
            return Err(LiveRouteRuntimeError::SubscriptionNotFound(id));
        };
        entry.cancellation.cancel();
        if let Some(active) = entry.active {
            active.cancellation.cancel();
        }
        let sequence = self.next_sequence();
        let epoch = entry.epoch;
        entry
            .snapshots
            .send_replace(Arc::new(RouteSubscriptionSnapshot {
                sequence,
                id,
                epoch,
                view: Arc::clone(&self.view),
                state: RouteSubscriptionState::Cancelled,
            }));
        self.publish_event(
            Some(id),
            None,
            LiveRouteRuntimeEventKind::SubscriptionCancelled,
        );
        Ok(())
    }

    fn replace_subscription(
        &mut self,
        id: RouteSubscriptionId,
        spec: RouteSubscriptionSpec,
    ) -> Result<(), LiveRouteRuntimeError> {
        let source = RouteProvenance::from_view(&self.view);
        let (previous, previous_quote, active) = {
            let Some(entry) = self.entries.get_mut(&id) else {
                return Err(LiveRouteRuntimeError::SubscriptionNotFound(id));
            };
            let next_epoch = entry
                .epoch
                .checked_add(1)
                .ok_or(LiveRouteRuntimeError::SequenceExhausted)?;
            let previous = entry_published_source(entry);
            if let Some(active) = &entry.active {
                active.cancellation.cancel();
            }
            entry.spec = spec;
            entry.epoch = next_epoch;
            entry.dirty = true;
            entry.trigger = RouteSearchTrigger::RequestChanged;
            (previous, entry.last_ready.clone(), entry.active.is_some())
        };
        self.publish_snapshot(
            id,
            RouteSubscriptionState::Pending {
                source,
                previous: previous_quote,
            },
        );
        if let Some(previous) = previous {
            self.publish_event(
                Some(id),
                None,
                LiveRouteRuntimeEventKind::RouteInvalidated {
                    previous,
                    reason: RouteInvalidationReason::RequestChanged,
                },
            );
        }
        if !active {
            self.enqueue_pending(id);
        }
        Ok(())
    }

    fn enqueue_pending(&mut self, id: RouteSubscriptionId) {
        if self.pending_set.insert(id) {
            self.pending.push_back(id);
        }
    }

    fn schedule_pending(&mut self) {
        while let Some(id) = self.pending.pop_front() {
            self.pending_set.remove(&id);
            let should_schedule = self
                .entries
                .get(&id)
                .is_some_and(|entry| entry.dirty && entry.active.is_none());
            if !should_schedule {
                continue;
            }
            let source = RouteProvenance::from_view(&self.view);
            let (job, stamp, trigger, cancellation, next_job) = {
                let entry = self.entries.get_mut(&id).expect("entry exists");
                let raw_job = entry.next_job;
                let Some(next_job) = raw_job.checked_add(1) else {
                    continue;
                };
                let stamp = RouteJobStamp {
                    subscription: id,
                    epoch: entry.epoch,
                    job: RouteSearchJobId(raw_job),
                    source,
                };
                let cancellation = RouteCancellationToken::default();
                let mut streaming = entry.spec.streaming;
                streaming.parallel = streaming.parallel.with_workers(1);
                (
                    RouteWorkerJob {
                        stamp,
                        view: Arc::clone(&self.view),
                        request: entry.spec.request.clone(),
                        streaming,
                        subscription_cancel: entry.cancellation.clone(),
                        cancellation: cancellation.clone(),
                    },
                    stamp,
                    entry.trigger,
                    cancellation,
                    next_job,
                )
            };
            let Some(workers) = self.workers.as_ref() else {
                return;
            };
            match workers.try_submit(job) {
                Ok(()) => {
                    let entry = self.entries.get_mut(&id).expect("entry exists");
                    entry.next_job = next_job;
                    entry.active = Some(ActiveRouteJob {
                        stamp,
                        cancellation,
                    });
                    entry.dirty = false;
                    self.publish_snapshot(
                        id,
                        RouteSubscriptionState::Searching {
                            stamp,
                            provisional: None,
                            previous: self
                                .entries
                                .get(&id)
                                .and_then(|entry| entry.last_ready.clone()),
                        },
                    );
                    self.publish_event(
                        Some(id),
                        Some(stamp),
                        LiveRouteRuntimeEventKind::SearchScheduled { trigger },
                    );
                }
                Err(()) => {
                    self.enqueue_pending(id);
                    break;
                }
            }
        }
    }

    fn handle_worker_message(&mut self, message: RouteWorkerMessage) {
        match message {
            RouteWorkerMessage::Event { stamp, event } => {
                if !self.accepts(stamp) {
                    return;
                }
                let progressive = event_best(&event).map(|quote| {
                    Arc::new(VersionedRouteQuote {
                        source: stamp.source,
                        quote,
                    })
                });
                if let Some(progressive) = progressive {
                    let previous = self
                        .entries
                        .get(&stamp.subscription)
                        .and_then(|entry| entry.last_ready.clone());
                    self.publish_snapshot(
                        stamp.subscription,
                        RouteSubscriptionState::Searching {
                            stamp,
                            provisional: Some(progressive),
                            previous,
                        },
                    );
                }
                self.publish_event(
                    Some(stamp.subscription),
                    Some(stamp),
                    LiveRouteRuntimeEventKind::SearchEvent(event),
                );
            }
            RouteWorkerMessage::Completed { stamp, result } => {
                let current = RouteProvenance::from_view(&self.view);
                let stale_dirty = {
                    let Some(entry) = self.entries.get_mut(&stamp.subscription) else {
                        return;
                    };
                    let was_active = entry
                        .active
                        .as_ref()
                        .is_some_and(|active| active.stamp == stamp);
                    if !was_active {
                        return;
                    }
                    entry.active = None;
                    if stamp.source != current
                        || entry.epoch != stamp.epoch
                        || entry.cancellation.is_cancelled()
                    {
                        entry.dirty = !entry.cancellation.is_cancelled();
                        Some(entry.dirty)
                    } else {
                        None
                    }
                };
                if let Some(dirty) = stale_dirty {
                    self.publish_event(
                        Some(stamp.subscription),
                        Some(stamp),
                        LiveRouteRuntimeEventKind::StaleResultRejected {
                            produced: stamp.source,
                            current,
                        },
                    );
                    if dirty {
                        self.enqueue_pending(stamp.subscription);
                    }
                    return;
                }
                let entry = self
                    .entries
                    .get_mut(&stamp.subscription)
                    .expect("accepted route entry exists");
                match result {
                    Ok(report) => {
                        let previous = entry.last_ready.clone();
                        let best = report.best.clone().map(|quote| {
                            Arc::new(VersionedRouteQuote {
                                source: stamp.source,
                                quote,
                            })
                        });
                        entry.last_ready = best.clone();
                        self.publish_snapshot(
                            stamp.subscription,
                            RouteSubscriptionState::Ready {
                                source: stamp.source,
                                best: best.clone(),
                                report: Box::new(report.clone()),
                            },
                        );
                        self.publish_event(
                            Some(stamp.subscription),
                            Some(stamp),
                            LiveRouteRuntimeEventKind::RoutePublished {
                                previous,
                                current: best,
                                report,
                            },
                        );
                    }
                    Err(failure) => {
                        let failure = Arc::new(failure);
                        self.publish_snapshot(
                            stamp.subscription,
                            RouteSubscriptionState::Failed {
                                source: stamp.source,
                                failure: Arc::clone(&failure),
                            },
                        );
                        self.publish_event(
                            Some(stamp.subscription),
                            Some(stamp),
                            LiveRouteRuntimeEventKind::SearchFailed { failure },
                        );
                    }
                }
            }
        }
    }

    fn accepts(&self, stamp: RouteJobStamp) -> bool {
        let current = RouteProvenance::from_view(&self.view);
        self.entries.get(&stamp.subscription).is_some_and(|entry| {
            stamp.source == current
                && entry.epoch == stamp.epoch
                && !entry.cancellation.is_cancelled()
                && entry.active.as_ref().is_some_and(|active| {
                    active.stamp == stamp && !active.cancellation.is_cancelled()
                })
        })
    }

    fn publish_snapshot(&mut self, id: RouteSubscriptionId, state: RouteSubscriptionState) {
        let sequence = self.next_sequence();
        if let Some(entry) = self.entries.get(&id) {
            entry
                .snapshots
                .send_replace(Arc::new(RouteSubscriptionSnapshot {
                    sequence,
                    id,
                    epoch: entry.epoch,
                    view: Arc::clone(&self.view),
                    state,
                }));
        }
    }

    fn publish_event(
        &mut self,
        subscription: Option<RouteSubscriptionId>,
        stamp: Option<RouteJobStamp>,
        kind: LiveRouteRuntimeEventKind,
    ) {
        let sequence = self.next_sequence();
        let _ = self.events.send(Arc::new(LiveRouteRuntimeEvent {
            sequence,
            subscription,
            stamp,
            kind,
        }));
    }

    fn next_sequence(&mut self) -> u64 {
        let current = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        current
    }

    async fn close(mut self) {
        let ids = self.entries.keys().copied().collect::<Vec<_>>();
        for id in ids {
            if let Some(entry) = self.entries.remove(&id) {
                entry.cancellation.cancel();
                if let Some(active) = entry.active {
                    active.cancellation.cancel();
                }
                let sequence = self.next_sequence();
                let epoch = entry.epoch;
                entry
                    .snapshots
                    .send_replace(Arc::new(RouteSubscriptionSnapshot {
                        sequence,
                        id,
                        epoch,
                        view: Arc::clone(&self.view),
                        state: self.terminal_failure.as_ref().map_or(
                            RouteSubscriptionState::Closed,
                            |failure| RouteSubscriptionState::RuntimeFailed {
                                failure: Arc::clone(failure),
                            },
                        ),
                    }));
                if self.terminal_failure.is_none() {
                    self.publish_event(Some(id), None, LiveRouteRuntimeEventKind::RuntimeClosed);
                }
            }
        }
        if let Some(failure) = self.terminal_failure.clone() {
            self.publish_event(
                None,
                None,
                LiveRouteRuntimeEventKind::RuntimeFailed { failure },
            );
        }
        self.worker_results.close();
        if let Some(workers) = self.workers.take() {
            let _ = tokio::task::spawn_blocking(move || workers.shutdown()).await;
        }
        if let Some(response) = self.shutdown_response.take() {
            self.exited.send_replace(true);
            let _ = response.send(());
        } else {
            self.exited.send_replace(true);
        }
    }
}

fn entry_published_source(entry: &RouteEntry) -> Option<RouteProvenance> {
    match entry.snapshots.borrow().state() {
        RouteSubscriptionState::Searching {
            stamp,
            provisional: Some(_),
            ..
        } => Some(stamp.source()),
        RouteSubscriptionState::Ready { source, .. } => Some(*source),
        RouteSubscriptionState::Pending { .. }
        | RouteSubscriptionState::Searching {
            provisional: None, ..
        }
        | RouteSubscriptionState::Failed { .. }
        | RouteSubscriptionState::Cancelled
        | RouteSubscriptionState::Closed
        | RouteSubscriptionState::RuntimeFailed { .. } => {
            entry.last_ready.as_ref().map(|quote| quote.source())
        }
    }
}

fn event_best(event: &RouteSearchEvent) -> Option<RouteQuote> {
    match event {
        RouteSearchEvent::BestUpdated { quote, .. }
        | RouteSearchEvent::InitialResultsReady { best: quote, .. } => Some(quote.clone()),
        _ => None,
    }
}

struct RouteWorkerJob {
    stamp: RouteJobStamp,
    view: Arc<LiveSearchView>,
    request: RouteRequest,
    streaming: StreamingSearchConfig,
    subscription_cancel: RouteCancellationToken,
    cancellation: RouteCancellationToken,
}

enum RouteWorkerCommand {
    Search(Box<RouteWorkerJob>),
    Shutdown,
}

enum RouteWorkerMessage {
    Event {
        stamp: RouteJobStamp,
        event: RouteSearchEvent,
    },
    Completed {
        stamp: RouteJobStamp,
        result: Result<StreamingSearchReport, RouteSearchFailure>,
    },
}

struct RouteWorkerPool {
    sender: SyncSender<RouteWorkerCommand>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl RouteWorkerPool {
    fn new(
        config: LiveRouteRuntimeConfig,
        results: mpsc::Sender<RouteWorkerMessage>,
    ) -> Result<Self, std::io::Error> {
        let (sender, receiver) = sync_channel(config.job_queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut threads = Vec::new();
        for worker in 0..config.worker_threads {
            let receiver = Arc::clone(&receiver);
            let results = results.clone();
            match thread::Builder::new()
                .name(format!("amm-route-worker-{worker}"))
                .spawn(move || route_worker(receiver, results))
            {
                Ok(thread) => threads.push(thread),
                Err(error) => {
                    Self::stop_threads(&sender, threads);
                    return Err(error);
                }
            }
        }
        Ok(Self { sender, threads })
    }

    fn try_submit(&self, job: RouteWorkerJob) -> Result<(), ()> {
        match self
            .sender
            .try_send(RouteWorkerCommand::Search(Box::new(job)))
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(RouteWorkerCommand::Search(job)))
            | Err(TrySendError::Disconnected(RouteWorkerCommand::Search(job))) => {
                let _ = job;
                Err(())
            }
            Err(_) => Err(()),
        }
    }

    fn shutdown(self) {
        Self::stop_threads(&self.sender, self.threads);
    }

    fn stop_threads(sender: &SyncSender<RouteWorkerCommand>, threads: Vec<thread::JoinHandle<()>>) {
        for _ in &threads {
            let _ = sender.send(RouteWorkerCommand::Shutdown);
        }
        for thread in threads {
            let _ = thread.join();
        }
    }
}

fn route_worker(
    receiver: Arc<Mutex<StdReceiver<RouteWorkerCommand>>>,
    results: mpsc::Sender<RouteWorkerMessage>,
) {
    loop {
        let command = receiver
            .lock()
            .expect("route worker receiver poisoned")
            .recv();
        let Ok(command) = command else { break };
        let RouteWorkerCommand::Search(job) = command else {
            break;
        };
        let job = *job;
        let stamp = job.stamp;
        let result = if job.subscription_cancel.is_cancelled() || job.cancellation.is_cancelled() {
            Err(RouteSearchFailure {
                message: "route search cancelled before worker execution".to_owned(),
                worker_panicked: false,
            })
        } else {
            let run = catch_unwind(AssertUnwindSafe(|| {
                let searcher = job.view.searcher();
                searcher.stream_routes_snapshot_cancellable(
                    &job.request,
                    job.streaming,
                    || job.subscription_cancel.is_cancelled() || job.cancellation.is_cancelled(),
                    |event| {
                        if job.subscription_cancel.is_cancelled() || job.cancellation.is_cancelled()
                        {
                            return SearchControl::Stop;
                        }
                        if results
                            .blocking_send(RouteWorkerMessage::Event { stamp, event })
                            .is_err()
                        {
                            return SearchControl::Stop;
                        }
                        SearchControl::Continue
                    },
                )
            }));
            match run {
                Ok(Ok(report)) => Ok(report),
                Ok(Err(error)) => Err(RouteSearchFailure {
                    message: route_search_failure_message(&error),
                    worker_panicked: false,
                }),
                Err(_) => Err(RouteSearchFailure {
                    message: "route worker panicked".to_owned(),
                    worker_panicked: true,
                }),
            }
        };
        let _ = results.blocking_send(RouteWorkerMessage::Completed { stamp, result });
    }
}

fn route_search_failure_message(error: &SearchError) -> String {
    if let SearchError::NoViableRoute {
        candidates,
        failures,
    } = error
    {
        let first = failures
            .first()
            .map(|failure| failure.reason.as_str())
            .unwrap_or("all candidates failed");
        format!("no viable route among {candidates} candidates: {first}")
    } else {
        error.to_string()
    }
}
