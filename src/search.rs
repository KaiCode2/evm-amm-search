use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

use alloy_primitives::{Address, Log, U256};
use alloy_rpc_types_eth::Log as RpcLog;
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, AmmStatePoint, PoolKey, PoolRegistration, PoolStatus,
    ProtocolId, ProtocolMetadata, SimConfig, SimError,
};
#[cfg(feature = "live-runtime")]
use evm_amm_state::adapters::{
    AdapterRegistrySnapshot, AmmStateSnapshot, PoolInstanceId, PoolRevisionMap, PoolStateRef,
};
use evm_fork_cache::cache::EvmCache;
#[cfg(feature = "live-runtime")]
use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot};
use petgraph::Direction;

#[cfg(feature = "live-runtime")]
use crate::live_graph::LiveAmmGraph;
use crate::{
    AmmGraph, GraphVersion,
    liquidity::{BalanceState, LiquidityPruneStats, LiquidityPruningConfig, PoolLiquidityIndex},
    overlay_cache::OverlayAdapterCache,
};

/// Search controls shared by route and cycle requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchConfig {
    /// Minimum number of hops a candidate must contain.
    pub min_hops: usize,
    /// Maximum number of hops a candidate may contain.
    pub max_hops: usize,
    /// Maximum candidate paths to evaluate.
    pub max_candidates: Option<usize>,
    /// Optional allowlist for intermediate connector tokens.
    pub connector_tokens: Option<HashSet<Address>>,
    /// Search strategy used to produce candidate routes.
    pub mode: SearchMode,
    /// Optional balance-aware heuristic controls. Disabled by default.
    pub liquidity_pruning: LiquidityPruningConfig,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            min_hops: 1,
            max_hops: 3,
            max_candidates: None,
            connector_tokens: None,
            mode: SearchMode::Exhaustive,
            liquidity_pruning: LiquidityPruningConfig::default(),
        }
    }
}

impl SearchConfig {
    /// Override the hop range.
    pub fn with_hops(mut self, min_hops: usize, max_hops: usize) -> Self {
        self.min_hops = min_hops;
        self.max_hops = max_hops;
        self
    }

    /// Limit how many candidate paths are evaluated.
    pub fn with_max_candidates(mut self, max_candidates: usize) -> Self {
        self.max_candidates = Some(max_candidates);
        self
    }

    /// Restrict intermediate hops to `tokens`.
    pub fn with_connector_tokens(mut self, tokens: impl IntoIterator<Item = Address>) -> Self {
        self.connector_tokens = Some(tokens.into_iter().collect());
        self
    }

    /// Use exhaustive path enumeration.
    pub fn exhaustive(mut self) -> Self {
        self.mode = SearchMode::Exhaustive;
        self
    }

    /// Use heuristic route search with default heuristic controls.
    pub fn heuristic(mut self) -> Self {
        self.mode = SearchMode::Heuristic(HeuristicSearchConfig::default());
        self
    }

    /// Set the route search mode.
    pub fn with_mode(mut self, mode: SearchMode) -> Self {
        self.mode = mode;
        self
    }

    /// Configure balance-aware ordering and pruning for heuristic search.
    pub fn with_liquidity_pruning(mut self, liquidity_pruning: LiquidityPruningConfig) -> Self {
        self.liquidity_pruning = liquidity_pruning;
        self
    }

    fn validate(&self) -> Result<(), SearchError> {
        if self.min_hops == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "min_hops must be at least 1",
            });
        }
        if self.min_hops > self.max_hops {
            return Err(SearchError::InvalidConfig {
                reason: "min_hops cannot exceed max_hops",
            });
        }
        if self.max_candidates == Some(0) {
            return Err(SearchError::InvalidConfig {
                reason: "max_candidates must be at least 1",
            });
        }
        if let SearchMode::Heuristic(heuristic) = self.mode {
            heuristic.validate()?;
        }
        Ok(())
    }
}

/// Candidate generation strategy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchMode {
    /// Enumerate every valid simple path up to the configured bounds.
    #[default]
    Exhaustive,
    /// Evaluate and prune candidate prefixes while searching.
    Heuristic(HeuristicSearchConfig),
}

/// Controls for [`SearchMode::Heuristic`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeuristicSearchConfig {
    /// Maximum automatically selected connector tokens when no explicit
    /// `connector_tokens` allowlist is supplied.
    pub max_auto_connectors: usize,
    /// Minimum graph degree for a token to be selected as an automatic connector.
    pub min_auto_connector_degree: usize,
    /// Maximum prefix states retained after each hop depth.
    pub beam_width: Option<usize>,
    /// Maximum quoted parallel edges retained for the same token-to-token step.
    pub parallel_edge_limit: usize,
    /// Re-run exact quoting for the best finalist paths before returning.
    pub simulate_finalists: bool,
    /// Maximum finalist paths returned after heuristic scoring.
    pub max_finalists: usize,
    /// Evaluate target-closing branch groups before intermediate groups.
    pub target_first: bool,
    /// Prune worse prefixes that reach the same token with stricter path
    /// constraints.
    pub prefix_dominance: bool,
    /// Fast direct and central-connector route search before broad expansion.
    pub fast_lane: FastLaneConfig,
    /// Adaptive shortlist controls for same-pair parallel edges.
    pub edge_shortlist: AdaptiveEdgeShortlistConfig,
    /// Conservative upper-bound pruning controls.
    pub upper_bound_pruning: UpperBoundPruningConfig,
}

impl Default for HeuristicSearchConfig {
    fn default() -> Self {
        Self::balanced()
    }
}

impl HeuristicSearchConfig {
    /// Balanced heuristic defaults.
    ///
    /// This keeps ordering and conservative pruning defaults enabled while
    /// leaving adaptive edge shortlisting disabled. It is the default preset
    /// because the latency-first shortlist can change results under bounded
    /// heuristic search.
    pub const fn balanced() -> Self {
        Self {
            max_auto_connectors: 8,
            min_auto_connector_degree: 6,
            beam_width: Some(64),
            parallel_edge_limit: 1,
            simulate_finalists: true,
            max_finalists: 16,
            target_first: true,
            prefix_dominance: true,
            fast_lane: FastLaneConfig::enabled(),
            edge_shortlist: AdaptiveEdgeShortlistConfig::ordering_only(),
            upper_bound_pruning: UpperBoundPruningConfig::conservative(),
        }
    }

    /// Latency-first heuristic preset.
    ///
    /// This enables adaptive parallel-edge shortlisting and refinement. It can
    /// surface strong quotes faster, but it is approximate under bounded
    /// heuristic search and should be paired with streaming/exhaustive audit
    /// when exactness matters.
    pub const fn latency_first() -> Self {
        Self {
            max_auto_connectors: 8,
            min_auto_connector_degree: 6,
            beam_width: Some(64),
            parallel_edge_limit: 1,
            simulate_finalists: true,
            max_finalists: 16,
            target_first: true,
            prefix_dominance: true,
            fast_lane: FastLaneConfig::enabled(),
            edge_shortlist: AdaptiveEdgeShortlistConfig::enabled(),
            upper_bound_pruning: UpperBoundPruningConfig::conservative(),
        }
    }

    /// Override automatic connector selection.
    pub fn with_auto_connectors(
        mut self,
        max_auto_connectors: usize,
        min_auto_connector_degree: usize,
    ) -> Self {
        self.max_auto_connectors = max_auto_connectors;
        self.min_auto_connector_degree = min_auto_connector_degree;
        self
    }

    /// Override the per-depth beam width. `None` keeps all prefix states.
    pub fn with_beam_width(mut self, beam_width: Option<usize>) -> Self {
        self.beam_width = beam_width;
        self
    }

    /// Keep up to `parallel_edge_limit` quoted edges for each token-to-token step.
    pub fn with_parallel_edge_limit(mut self, parallel_edge_limit: usize) -> Self {
        self.parallel_edge_limit = parallel_edge_limit;
        self
    }

    /// Configure finalist exact simulation.
    pub fn with_finalist_simulation(
        mut self,
        simulate_finalists: bool,
        max_finalists: usize,
    ) -> Self {
        self.simulate_finalists = simulate_finalists;
        self.max_finalists = max_finalists;
        self
    }

    /// Configure whether target-closing branch groups are evaluated first.
    pub fn with_target_first(mut self, target_first: bool) -> Self {
        self.target_first = target_first;
        self
    }

    /// Configure same-token prefix dominance pruning.
    pub fn with_prefix_dominance(mut self, prefix_dominance: bool) -> Self {
        self.prefix_dominance = prefix_dominance;
        self
    }

    /// Configure fast-lane direct and central-connector route search.
    pub fn with_fast_lane(mut self, fast_lane: FastLaneConfig) -> Self {
        self.fast_lane = fast_lane;
        self
    }

    /// Configure adaptive same-pair edge shortlisting.
    pub fn with_edge_shortlist(mut self, edge_shortlist: AdaptiveEdgeShortlistConfig) -> Self {
        self.edge_shortlist = edge_shortlist;
        self
    }

    /// Configure conservative upper-bound pruning.
    pub fn with_upper_bound_pruning(
        mut self,
        upper_bound_pruning: UpperBoundPruningConfig,
    ) -> Self {
        self.upper_bound_pruning = upper_bound_pruning;
        self
    }

    fn validate(self) -> Result<(), SearchError> {
        if self.parallel_edge_limit == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "parallel_edge_limit must be at least 1",
            });
        }
        if self.max_finalists == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "max_finalists must be at least 1",
            });
        }
        if self.beam_width == Some(0) {
            return Err(SearchError::InvalidConfig {
                reason: "beam_width must be at least 1",
            });
        }
        self.fast_lane.validate()?;
        self.edge_shortlist.validate()?;
        Ok(())
    }
}

/// Fast initial heuristic route search before broad frontier expansion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FastLaneConfig {
    /// Whether the fast-lane route scheduler is enabled.
    pub enabled: bool,
    /// Maximum central connectors considered for two-hop fast-lane routes.
    pub max_connectors: usize,
    /// Number of ranked direct `token_in -> token_out` edges quoted first.
    pub direct_edges_per_pair: usize,
    /// Number of ranked edges used for each fast-lane connector leg.
    pub connector_edges_per_pair: usize,
    /// Quote direct routes before connector routes.
    pub evaluate_direct_best_first: bool,
}

impl Default for FastLaneConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_connectors: 8,
            direct_edges_per_pair: 1,
            connector_edges_per_pair: 1,
            evaluate_direct_best_first: true,
        }
    }
}

impl FastLaneConfig {
    /// Disabled fast-lane search.
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            max_connectors: 8,
            direct_edges_per_pair: 1,
            connector_edges_per_pair: 1,
            evaluate_direct_best_first: true,
        }
    }

    /// Enabled fast-lane search with default limits.
    pub const fn enabled() -> Self {
        Self {
            enabled: true,
            max_connectors: 8,
            direct_edges_per_pair: 1,
            connector_edges_per_pair: 1,
            evaluate_direct_best_first: true,
        }
    }

    /// Toggle fast-lane scheduling.
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the maximum central connectors considered for two-hop fast-lane
    /// routes.
    pub const fn with_max_connectors(mut self, max_connectors: usize) -> Self {
        self.max_connectors = max_connectors;
        self
    }

    /// Set the number of ranked direct edges quoted in the fast lane.
    pub const fn with_direct_edges_per_pair(mut self, direct_edges_per_pair: usize) -> Self {
        self.direct_edges_per_pair = direct_edges_per_pair;
        self
    }

    /// Set the number of ranked connector-leg edges quoted in the fast lane.
    pub const fn with_connector_edges_per_pair(mut self, connector_edges_per_pair: usize) -> Self {
        self.connector_edges_per_pair = connector_edges_per_pair;
        self
    }

    /// Configure whether direct routes are quoted before connector routes.
    pub const fn with_evaluate_direct_best_first(
        mut self,
        evaluate_direct_best_first: bool,
    ) -> Self {
        self.evaluate_direct_best_first = evaluate_direct_best_first;
        self
    }

    fn validate(self) -> Result<(), SearchError> {
        if self.enabled && self.direct_edges_per_pair == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "fast_lane.direct_edges_per_pair must be at least 1",
            });
        }
        if self.enabled && self.connector_edges_per_pair == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "fast_lane.connector_edges_per_pair must be at least 1",
            });
        }
        Ok(())
    }
}

/// Controls for adaptive same-pair parallel-edge search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdaptiveEdgeShortlistConfig {
    /// Whether adaptive edge shortlisting is enabled.
    pub enabled: bool,
    /// Parallel edges per token pair quoted in the first broad heuristic pass.
    pub initial_edges_per_pair: usize,
    /// Parallel edges per token pair quoted in the refinement pass.
    pub refinement_edges_per_pair: usize,
    /// Whether to run the wider refinement pass after the first broad pass.
    pub refine_parallel_edges: bool,
    /// Whether protocol/liquidity-aware edge ordering is used.
    pub protocol_ordering: bool,
}

impl Default for AdaptiveEdgeShortlistConfig {
    fn default() -> Self {
        Self::ordering_only()
    }
}

impl AdaptiveEdgeShortlistConfig {
    /// Disabled adaptive shortlisting.
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            initial_edges_per_pair: 1,
            refinement_edges_per_pair: 3,
            refine_parallel_edges: false,
            protocol_ordering: false,
        }
    }

    /// Protocol-aware ordering without adaptive edge truncation.
    pub const fn ordering_only() -> Self {
        Self {
            enabled: false,
            initial_edges_per_pair: 1,
            refinement_edges_per_pair: 3,
            refine_parallel_edges: false,
            protocol_ordering: true,
        }
    }

    /// Enabled adaptive shortlisting with default limits.
    pub const fn enabled() -> Self {
        Self {
            enabled: true,
            initial_edges_per_pair: 1,
            refinement_edges_per_pair: 3,
            refine_parallel_edges: true,
            protocol_ordering: true,
        }
    }

    /// Toggle adaptive shortlisting.
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the initial per-pair edge count.
    pub const fn with_initial_edges_per_pair(mut self, initial_edges_per_pair: usize) -> Self {
        self.initial_edges_per_pair = initial_edges_per_pair;
        self
    }

    /// Set the refinement per-pair edge count.
    pub const fn with_refinement_edges_per_pair(
        mut self,
        refinement_edges_per_pair: usize,
    ) -> Self {
        self.refinement_edges_per_pair = refinement_edges_per_pair;
        self
    }

    /// Configure whether a wider refinement pass runs after the initial pass.
    pub const fn with_refine_parallel_edges(mut self, refine_parallel_edges: bool) -> Self {
        self.refine_parallel_edges = refine_parallel_edges;
        self
    }

    /// Configure protocol/liquidity-aware ordering inside parallel groups.
    pub const fn with_protocol_ordering(mut self, protocol_ordering: bool) -> Self {
        self.protocol_ordering = protocol_ordering;
        self
    }

    fn validate(self) -> Result<(), SearchError> {
        if self.enabled && self.initial_edges_per_pair == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "edge_shortlist.initial_edges_per_pair must be at least 1",
            });
        }
        if self.enabled && self.refinement_edges_per_pair == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "edge_shortlist.refinement_edges_per_pair must be at least 1",
            });
        }
        Ok(())
    }
}

/// Controls for conservative optimistic pruning after an incumbent route exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpperBoundPruningConfig {
    /// Whether upper-bound pruning is enabled.
    pub enabled: bool,
    /// Use fresh target-token balance caps as a conservative route upper bound.
    pub balance_cap_pruning: bool,
    /// Reserved for a future approximate rate-bound mode. Disabled by default.
    pub estimated_rate_pruning: bool,
    /// Do not prune when any required bound input is unknown or stale.
    pub fail_open_on_unknown: bool,
}

impl Default for UpperBoundPruningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            balance_cap_pruning: true,
            estimated_rate_pruning: false,
            fail_open_on_unknown: true,
        }
    }
}

impl UpperBoundPruningConfig {
    /// Disabled upper-bound pruning.
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            balance_cap_pruning: false,
            estimated_rate_pruning: false,
            fail_open_on_unknown: true,
        }
    }

    /// Conservative enabled upper-bound pruning.
    pub const fn conservative() -> Self {
        Self {
            enabled: true,
            balance_cap_pruning: true,
            estimated_rate_pruning: false,
            fail_open_on_unknown: true,
        }
    }

    /// Toggle upper-bound pruning.
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Configure fresh balance-cap pruning.
    pub const fn with_balance_cap_pruning(mut self, balance_cap_pruning: bool) -> Self {
        self.balance_cap_pruning = balance_cap_pruning;
        self
    }

    /// Configure approximate estimated-rate pruning.
    ///
    /// This is reserved for future use and remains disabled by default.
    pub const fn with_estimated_rate_pruning(mut self, estimated_rate_pruning: bool) -> Self {
        self.estimated_rate_pruning = estimated_rate_pruning;
        self
    }

    /// Configure whether unknown/stale bound inputs fail open.
    pub const fn with_fail_open_on_unknown(mut self, fail_open_on_unknown: bool) -> Self {
        self.fail_open_on_unknown = fail_open_on_unknown;
        self
    }
}

/// Worker controls for overlay-backed parallel candidate evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParallelSearchConfig {
    /// Number of worker threads used to evaluate candidate paths.
    pub workers: usize,
}

impl Default for ParallelSearchConfig {
    fn default() -> Self {
        Self {
            workers: thread::available_parallelism().map_or(1, usize::from),
        }
    }
}

impl ParallelSearchConfig {
    /// Override the worker count.
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    fn validate(&self) -> Result<(), SearchError> {
        if self.workers == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "workers must be at least 1",
            });
        }
        Ok(())
    }
}

/// When a streaming search is considered complete.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamingCompletion {
    /// Stop once the fast direct/connector lane is exhausted.
    FastLaneExhausted,
    /// Stop once the heuristic-first phase has exhausted its frontier.
    HeuristicExhausted,
    /// Continue after the heuristic phase until every configured simple route
    /// candidate has been evaluated.
    #[default]
    Exhaustive,
}

/// Controls for heuristic-first route streaming.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamingSearchConfig {
    /// Completion target for the search.
    pub completion: StreamingCompletion,
    /// Worker controls used by the exhaustive remainder phase.
    pub parallel: ParallelSearchConfig,
    /// Emit every viable route, not only routes entering the retained top set.
    pub emit_all_viable: bool,
    /// Number of top routes that should trigger [`RouteSearchEvent::RouteFound`]
    /// when they enter the retained set.
    pub top_k: usize,
    /// Optional policy that stops the search once threshold conditions are met.
    pub stop_policy: Option<StreamingThresholdPolicy>,
    /// Optional policy that gates initial quote-bearing events until threshold
    /// conditions are met while search continues.
    pub initial_result_policy: Option<StreamingThresholdPolicy>,
}

impl Default for StreamingSearchConfig {
    fn default() -> Self {
        Self {
            completion: StreamingCompletion::Exhaustive,
            parallel: ParallelSearchConfig::default(),
            emit_all_viable: false,
            top_k: 1,
            stop_policy: None,
            initial_result_policy: None,
        }
    }
}

impl StreamingSearchConfig {
    /// Stop after the fast direct/central-connector lane.
    pub fn fast_lane_only(mut self) -> Self {
        self.completion = StreamingCompletion::FastLaneExhausted;
        self
    }

    /// Stop after the heuristic phase.
    pub fn heuristic_only(mut self) -> Self {
        self.completion = StreamingCompletion::HeuristicExhausted;
        self
    }

    /// Continue after the heuristic phase until configured exhaustive search is
    /// complete.
    pub fn exhaustive(mut self) -> Self {
        self.completion = StreamingCompletion::Exhaustive;
        self
    }

    /// Override worker controls for the exhaustive remainder phase.
    pub fn with_parallel(mut self, parallel: ParallelSearchConfig) -> Self {
        self.parallel = parallel;
        self
    }

    /// Emit all viable routes instead of only retained top routes.
    pub fn with_emit_all_viable(mut self, emit_all_viable: bool) -> Self {
        self.emit_all_viable = emit_all_viable;
        self
    }

    /// Retain and emit routes that enter the top `top_k` by gross output.
    pub fn with_top_k(mut self, top_k: usize) -> Self {
        self.top_k = top_k;
        self
    }

    /// Stop the search once `policy` is satisfied.
    pub fn with_stop_policy(mut self, policy: StreamingThresholdPolicy) -> Self {
        self.stop_policy = Some(policy);
        self
    }

    /// Gate initial quote-bearing events until `policy` is satisfied.
    pub fn with_initial_result_policy(mut self, policy: StreamingThresholdPolicy) -> Self {
        self.initial_result_policy = Some(policy);
        self
    }

    /// Stop once heuristic confidence reaches `bps`.
    pub fn stop_at_confidence_bps(mut self, bps: u16) -> Self {
        self.stop_policy = Some(policy_with_confidence(self.stop_policy, bps));
        self
    }

    /// Stop once exhaustive search fraction reaches `bps`.
    pub fn stop_at_exhaustive_fraction_bps(mut self, bps: u16) -> Self {
        self.stop_policy = Some(policy_with_exhaustive_fraction(self.stop_policy, bps));
        self
    }

    /// Stop once confidence and exhaustive fraction both reach their thresholds.
    pub fn stop_at_confidence_and_exhaustive_fraction_bps(
        self,
        confidence_bps: u16,
        exhaustive_fraction_bps: u16,
    ) -> Self {
        self.with_stop_policy(
            StreamingThresholdPolicy::all()
                .with_min_confidence_bps(confidence_bps)
                .with_min_exhaustive_fraction_bps(exhaustive_fraction_bps),
        )
    }

    /// Release initial quote-bearing events once heuristic confidence reaches
    /// `bps`, then continue streaming normally.
    pub fn emit_initial_results_at_confidence_bps(mut self, bps: u16) -> Self {
        self.initial_result_policy = Some(policy_with_confidence(self.initial_result_policy, bps));
        self
    }

    /// Release initial quote-bearing events once exhaustive search fraction
    /// reaches `bps`, then continue streaming normally.
    pub fn emit_initial_results_at_exhaustive_fraction_bps(mut self, bps: u16) -> Self {
        self.initial_result_policy = Some(policy_with_exhaustive_fraction(
            self.initial_result_policy,
            bps,
        ));
        self
    }

    /// Release initial quote-bearing events once confidence and exhaustive
    /// fraction both reach their thresholds, then continue streaming normally.
    pub fn emit_initial_results_at_confidence_and_exhaustive_fraction_bps(
        self,
        confidence_bps: u16,
        exhaustive_fraction_bps: u16,
    ) -> Self {
        self.with_initial_result_policy(
            StreamingThresholdPolicy::all()
                .with_min_confidence_bps(confidence_bps)
                .with_min_exhaustive_fraction_bps(exhaustive_fraction_bps),
        )
    }

    fn validate(&self) -> Result<(), SearchError> {
        self.parallel.validate()?;
        if self.top_k == 0 {
            return Err(SearchError::InvalidConfig {
                reason: "streaming top_k must be at least 1",
            });
        }
        if let Some(policy) = self.stop_policy {
            policy.validate()?;
        }
        if let Some(policy) = self.initial_result_policy {
            policy.validate()?;
        }
        Ok(())
    }
}

/// How multiple streaming threshold conditions are combined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingThresholdMode {
    /// Every configured threshold must be satisfied.
    All,
    /// Any configured threshold may be satisfied.
    Any,
}

/// Thresholds used to stop search or gate initial quote-bearing events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamingThresholdPolicy {
    /// How configured thresholds are combined.
    pub mode: StreamingThresholdMode,
    /// Minimum heuristic confidence in basis points (`1..=10_000`).
    pub min_confidence_bps: Option<u16>,
    /// Minimum exhaustive search fraction in basis points (`1..=10_000`).
    pub min_exhaustive_fraction_bps: Option<u16>,
}

impl StreamingThresholdPolicy {
    /// Construct a policy that requires all configured thresholds.
    pub const fn all() -> Self {
        Self {
            mode: StreamingThresholdMode::All,
            min_confidence_bps: None,
            min_exhaustive_fraction_bps: None,
        }
    }

    /// Construct a policy that requires any configured threshold.
    pub const fn any() -> Self {
        Self {
            mode: StreamingThresholdMode::Any,
            min_confidence_bps: None,
            min_exhaustive_fraction_bps: None,
        }
    }

    /// Set a confidence threshold in basis points (`1..=10_000`).
    pub const fn with_min_confidence_bps(mut self, bps: u16) -> Self {
        self.min_confidence_bps = Some(bps);
        self
    }

    /// Set an exhaustive-fraction threshold in basis points (`1..=10_000`).
    pub const fn with_min_exhaustive_fraction_bps(mut self, bps: u16) -> Self {
        self.min_exhaustive_fraction_bps = Some(bps);
        self
    }

    fn validate(self) -> Result<(), SearchError> {
        if self.min_confidence_bps.is_none() && self.min_exhaustive_fraction_bps.is_none() {
            return Err(SearchError::InvalidConfig {
                reason: "streaming threshold policy must configure at least one threshold",
            });
        }
        if self
            .min_confidence_bps
            .is_some_and(|bps| !(1..=10_000).contains(&bps))
        {
            return Err(SearchError::InvalidConfig {
                reason: "streaming confidence threshold must be between 1 and 10000 bps",
            });
        }
        if self
            .min_exhaustive_fraction_bps
            .is_some_and(|bps| !(1..=10_000).contains(&bps))
        {
            return Err(SearchError::InvalidConfig {
                reason: "streaming exhaustive fraction threshold must be between 1 and 10000 bps",
            });
        }
        Ok(())
    }

    fn is_satisfied(self, progress: &RouteSearchProgress) -> bool {
        let confidence_satisfied = self
            .min_confidence_bps
            .map(|threshold| progress.confidence_bps >= threshold);
        let exhaustive_satisfied = self.min_exhaustive_fraction_bps.map(|threshold| {
            progress
                .exhaustive_fraction_bps
                .is_some_and(|fraction| fraction >= threshold)
        });

        match self.mode {
            StreamingThresholdMode::All => {
                confidence_satisfied.unwrap_or(true) && exhaustive_satisfied.unwrap_or(true)
            }
            StreamingThresholdMode::Any => {
                confidence_satisfied.unwrap_or(false) || exhaustive_satisfied.unwrap_or(false)
            }
        }
    }
}

fn policy_with_confidence(
    policy: Option<StreamingThresholdPolicy>,
    bps: u16,
) -> StreamingThresholdPolicy {
    policy
        .unwrap_or_else(StreamingThresholdPolicy::all)
        .with_min_confidence_bps(bps)
}

fn policy_with_exhaustive_fraction(
    policy: Option<StreamingThresholdPolicy>,
    bps: u16,
) -> StreamingThresholdPolicy {
    policy
        .unwrap_or_else(StreamingThresholdPolicy::all)
        .with_min_exhaustive_fraction_bps(bps)
}

/// Phase that produced a streamed route event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteSearchPhase {
    /// Heuristic-first prefix expansion.
    Heuristic,
    /// Exhaustive evaluation of routes not already evaluated by the heuristic
    /// phase.
    Exhaustive,
}

/// Whether a streamed route is still provisional.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamedRouteStatus {
    /// Search can still discover a better route.
    Provisional,
    /// Search has reached the caller's configured completion target.
    FinalWithinConfiguredSearch,
}

/// Why a streaming search stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchFinality {
    /// The fast lane exhausted and the caller did not request broader search.
    FastLaneOnly,
    /// The heuristic phase exhausted its frontier and the caller did not request
    /// an exhaustive remainder.
    HeuristicOnly,
    /// Every configured route candidate was evaluated.
    Exhaustive,
    /// Search stopped because the configured stop policy was satisfied.
    StopPolicySatisfied,
    /// The event callback requested an early stop.
    Stopped,
}

/// Callback return value for streaming search events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchControl {
    /// Keep searching.
    #[default]
    Continue,
    /// Stop scheduling and evaluating more candidates.
    Stop,
}

/// Cumulative stats at a streaming phase boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamingPhaseStats {
    /// Viable route quotes observed so far.
    pub routes_observed: usize,
    /// Duplicate complete paths skipped before exhaustive evaluation.
    pub duplicate_paths_skipped: usize,
    /// Exact-hop quote-cache counters observed so far.
    pub quote_cache: QuoteCacheStats,
    /// Balance-aware pruning counters observed so far.
    pub liquidity_pruning: LiquidityPruneStats,
}

/// Current progress through the configured streaming route-search universe.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteSearchProgress {
    /// Current streaming phase.
    pub phase: Option<RouteSearchPhase>,
    /// Complete-path candidates evaluated so far, successful or failed.
    pub candidates_evaluated: usize,
    /// Viable route quotes observed so far.
    pub viable_routes_observed: usize,
    /// Complete-path candidates that failed to quote.
    pub failed_candidates: usize,
    /// Duplicate complete paths skipped before exhaustive evaluation.
    pub duplicate_paths_skipped: usize,
    /// Total configured unique candidate universe, once known.
    pub total_candidates: Option<usize>,
    /// Candidate evaluation fraction in basis points, once total is known.
    pub exhaustive_fraction_bps: Option<u16>,
    /// Heuristic confidence score in basis points.
    pub confidence_bps: u16,
    /// Best amount out observed so far.
    pub best_amount_out: Option<U256>,
}

/// Final report from a streaming route search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingSearchReport {
    /// Best route observed before the search stopped.
    pub best: Option<RouteQuote>,
    /// Retained top route quotes, ordered by gross output descending.
    pub top_routes: Vec<RouteQuote>,
    /// Best route found during the heuristic phase.
    pub heuristic_best: Option<RouteQuote>,
    /// Completion state reached by the search.
    pub finality: SearchFinality,
    /// Whether the heuristic best remained the final best. `None` when the
    /// exhaustive phase did not run or no best route was found.
    pub heuristic_was_final_best: Option<bool>,
    /// Number of times the best route improved after the heuristic phase.
    pub improvements_after_heuristic: usize,
    /// Viable route quotes observed.
    pub routes_observed: usize,
    /// Duplicate complete paths skipped before exhaustive evaluation.
    pub duplicate_paths_skipped: usize,
    /// Exact-hop quote-cache counters.
    pub quote_cache: QuoteCacheStats,
    /// Balance-aware pruning counters.
    pub liquidity_pruning: LiquidityPruneStats,
    /// Final progress snapshot.
    pub progress: RouteSearchProgress,
    /// Whether initial quote-bearing results were released to the callback.
    pub initial_results_released: bool,
}

/// Event emitted during a heuristic-first streaming route search.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteSearchEvent {
    /// Search has started.
    Started {
        /// Configured completion target.
        completion: StreamingCompletion,
    },
    /// A viable route was observed. Emission depends on
    /// [`StreamingSearchConfig::emit_all_viable`] and
    /// [`StreamingSearchConfig::top_k`].
    RouteFound {
        /// Phase that produced the quote.
        phase: RouteSearchPhase,
        /// Current gross-output rank among retained top routes, when this
        /// route entered the retained top set.
        rank: Option<usize>,
        /// Quote result.
        quote: RouteQuote,
    },
    /// The best route improved.
    BestUpdated {
        /// Phase that produced the improvement.
        phase: RouteSearchPhase,
        /// New best quote.
        quote: RouteQuote,
        /// Previous best quote, if any.
        previous_best: Option<RouteQuote>,
        /// Provisional/final status of this update.
        status: StreamedRouteStatus,
    },
    /// Search progress changed.
    Progress {
        /// Current progress snapshot.
        progress: RouteSearchProgress,
    },
    /// Initial quote-bearing results were released after the configured result
    /// gate was satisfied.
    InitialResultsReady {
        /// Current progress snapshot.
        progress: RouteSearchProgress,
        /// Current best quote.
        best: RouteQuote,
        /// Retained top routes at release time.
        top_routes: Vec<RouteQuote>,
    },
    /// A search phase completed.
    PhaseCompleted {
        /// Completed phase.
        phase: RouteSearchPhase,
        /// Cumulative stats at this boundary.
        stats: StreamingPhaseStats,
    },
    /// Search reached the configured completion target.
    Completed {
        /// Final streaming report.
        report: StreamingSearchReport,
    },
}

/// Pools whose state may have changed after a live-sync batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AffectedPools {
    pools: HashSet<PoolKey>,
    unknown_logs: usize,
    removed_logs: usize,
}

impl AffectedPools {
    /// Build affected-pool input from explicit pool keys.
    pub fn from_pool_keys(keys: impl IntoIterator<Item = PoolKey>) -> Self {
        Self {
            pools: keys.into_iter().collect(),
            unknown_logs: 0,
            removed_logs: 0,
        }
    }

    /// Route primitive logs through the AMM registry and collect affected pools.
    ///
    /// Unknown logs are retained as diagnostics. They force a conservative
    /// incremental-session fallback because the search layer cannot know whether
    /// they affected the active route universe.
    pub fn from_logs<'log>(
        registry: &AdapterRegistry,
        logs: impl IntoIterator<Item = &'log Log>,
    ) -> Self {
        let mut affected = Self::default();
        for log in logs {
            if let Some(pool) = registry.route_log(log) {
                affected.pools.insert(pool.key.clone());
            } else {
                affected.unknown_logs += 1;
            }
        }
        affected
    }

    /// Route RPC logs through the AMM registry and collect affected pools.
    ///
    /// Removed logs force a conservative fallback because the session cannot
    /// rollback prior route quotes by itself.
    pub fn from_rpc_logs<'log>(
        registry: &AdapterRegistry,
        logs: impl IntoIterator<Item = &'log RpcLog>,
    ) -> Self {
        let mut affected = Self::default();
        for log in logs {
            if log.removed {
                affected.removed_logs += 1;
                continue;
            }
            if let Some(pool) = registry.route_log(&log.inner) {
                affected.pools.insert(pool.key.clone());
            } else {
                affected.unknown_logs += 1;
            }
        }
        affected
    }

    /// Borrow the routed pool set.
    pub fn pools(&self) -> &HashSet<PoolKey> {
        &self.pools
    }

    /// Number of logs that were not routed to a tracked pool.
    pub fn unknown_logs(&self) -> usize {
        self.unknown_logs
    }

    /// Number of removed logs seen in this input.
    pub fn removed_logs(&self) -> usize {
        self.removed_logs
    }

    /// Whether no routed, unknown, or removed log was provided.
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty() && self.unknown_logs == 0 && self.removed_logs == 0
    }
}

/// Final status of an incremental route-session refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IncrementalRouteUpdateStatus {
    /// No simulations were needed or the best route remained unchanged.
    Unchanged,
    /// Affected routes/probes were re-evaluated successfully.
    Updated,
    /// The session could not safely update incrementally.
    RecomputeRequired,
}

/// Why an incremental route session declined to update in place.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecomputeReason {
    /// Removed/reorged logs were supplied.
    RemovedLogs { count: usize },
    /// Logs were supplied but could not be routed to tracked AMM pools.
    UnknownLogs { count: usize },
    /// The search graph changed after the session was created.
    TopologyChanged,
    /// A live searcher moved to a different complete chain-state point.
    StatePointChanged {
        previous: AmmStatePoint,
        current: AmmStatePoint,
    },
    /// The session and searcher disagree on whether quote keys are snapshot-scoped.
    StateScopeChanged,
    /// An affected pool is no longer registered.
    UnknownAffectedPool(PoolKey),
    /// An affected pool is degraded.
    DegradedAffectedPool(PoolKey),
    /// More probe paths were needed than the conservative refresh budget allows.
    ProbeBudgetExceeded { requested: usize, limit: usize },
}

/// Report from a route-session incremental refresh.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncrementalRouteUpdateReport {
    /// Best route before the refresh.
    pub previous_best: Option<RouteQuote>,
    /// Best route after the refresh, when the session could update in place.
    pub best: Option<RouteQuote>,
    /// Refresh status.
    pub status: IncrementalRouteUpdateStatus,
    /// Routed affected pools supplied by the caller.
    pub affected_pools: HashSet<PoolKey>,
    /// Number of materialized routes re-quoted.
    pub routes_requoted: usize,
    /// Number of parallel-probe routes quoted.
    pub probe_routes_quoted: usize,
    /// Quote-cache counters after the refresh.
    pub quote_cache: QuoteCacheStats,
    /// Conservative fallback reason.
    pub recompute_reason: Option<RecomputeReason>,
}

/// Event emitted while refreshing a route-search session.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteUpdateEvent {
    /// Incremental refresh has started.
    Started {
        /// Routed affected pools supplied by the caller.
        affected_pools: HashSet<PoolKey>,
    },
    /// A materialized route containing an affected pool was re-quoted.
    RouteRequoted {
        /// Updated route quote.
        quote: RouteQuote,
    },
    /// A local parallel replacement path was quoted.
    ProbeRouteFound {
        /// Probe quote.
        quote: RouteQuote,
    },
    /// The session's best route changed.
    BestChanged {
        /// Previous best route.
        previous_best: Option<RouteQuote>,
        /// New best route.
        best: Option<RouteQuote>,
    },
    /// Incremental refresh cannot safely continue.
    RecomputeRequired {
        /// Fallback reason.
        reason: RecomputeReason,
    },
    /// Incremental refresh completed.
    Completed {
        /// Final refresh report.
        report: IncrementalRouteUpdateReport,
    },
}

/// Request for best-route search from one token to another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteRequest {
    /// Input token.
    pub token_in: Address,
    /// Output token.
    pub token_out: Address,
    /// Exact input amount.
    pub amount_in: U256,
    /// Route search controls.
    pub config: SearchConfig,
    /// Adapter simulation config.
    pub sim_config: SimConfig,
}

impl RouteRequest {
    /// Construct a route request with default search and simulation config.
    pub fn new(token_in: Address, token_out: Address, amount_in: U256) -> Self {
        Self {
            token_in,
            token_out,
            amount_in,
            config: SearchConfig::default(),
            sim_config: SimConfig::default(),
        }
    }

    /// Set the search config.
    pub fn with_config(mut self, config: SearchConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the simulation config.
    pub fn with_sim_config(mut self, sim_config: SimConfig) -> Self {
        self.sim_config = sim_config;
        self
    }
}

/// Request for bounded cycle search starting and ending at `base_token`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CycleRequest {
    /// Token the cycle starts and ends with.
    pub base_token: Address,
    /// Exact cycle input amount.
    pub amount_in: U256,
    /// Cycle search controls.
    pub config: SearchConfig,
    /// Adapter simulation config.
    pub sim_config: SimConfig,
}

impl CycleRequest {
    /// Construct a cycle request with default simulation config and min 2 hops.
    pub fn new(base_token: Address, amount_in: U256) -> Self {
        Self {
            base_token,
            amount_in,
            config: SearchConfig::default().with_hops(2, 3),
            sim_config: SimConfig::default(),
        }
    }

    /// Set the search config.
    pub fn with_config(mut self, config: SearchConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the simulation config.
    pub fn with_sim_config(mut self, sim_config: SimConfig) -> Self {
        self.sim_config = sim_config;
        self
    }
}

/// One directed pool hop.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Hop {
    /// Pool used for the hop.
    pub pool: PoolKey,
    /// Input token for the hop.
    pub token_in: Address,
    /// Output token for the hop.
    pub token_out: Address,
}

impl Hop {
    /// Construct a hop.
    pub fn new(pool: PoolKey, token_in: Address, token_out: Address) -> Self {
        Self {
            pool,
            token_in,
            token_out,
        }
    }
}

/// Candidate route path.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RoutePath {
    /// Ordered hops in this path.
    pub hops: Vec<Hop>,
}

impl RoutePath {
    /// Construct an empty path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a path from `hops`.
    pub fn from_hops(hops: Vec<Hop>) -> Self {
        Self { hops }
    }

    /// Number of hops.
    pub fn len(&self) -> usize {
        self.hops.len()
    }

    /// Whether this path has no hops.
    pub fn is_empty(&self) -> bool {
        self.hops.is_empty()
    }

    fn push(&mut self, hop: Hop) {
        self.hops.push(hop);
    }
}

/// Quote result for a single hop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HopQuote {
    /// The quoted hop.
    pub hop: Hop,
    /// Input amount passed to the hop.
    pub amount_in: U256,
    /// Output amount returned by the hop.
    pub amount_out: U256,
}

/// Quote result for a full route path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteQuote {
    /// Candidate path that was evaluated.
    pub path: RoutePath,
    /// Initial route input amount.
    pub amount_in: U256,
    /// Final route output amount.
    pub amount_out: U256,
    /// Per-hop quote trace.
    pub hops: Vec<HopQuote>,
}

/// Quote result for a cycle path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CycleQuote {
    /// Underlying route quote, starting and ending at the base token.
    pub route: RouteQuote,
    /// Gross cycle profit when profitable.
    pub profit: Option<U256>,
}

impl CycleQuote {
    /// Whether the cycle returns more base token than it starts with.
    pub fn is_profitable(&self) -> bool {
        self.profit.is_some()
    }
}

/// One failed candidate-path evaluation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathFailure {
    /// Candidate path that failed.
    pub path: RoutePath,
    /// Human-readable failure reason.
    pub reason: String,
}

/// Results and quote-cache diagnostics from a batch search.
#[derive(Debug)]
pub struct BatchSearchReport<T> {
    /// Per-request batch results, preserving input request order.
    pub results: Vec<Result<Vec<T>, SearchError>>,
    /// Quote-cache counters collected while evaluating the batch.
    pub quote_cache: QuoteCacheStats,
    /// Balance-aware heuristic pruning counters collected while evaluating the
    /// batch.
    pub liquidity_pruning: LiquidityPruneStats,
}

/// Counters for the exact-hop quote cache used by DAG evaluation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QuoteCacheStats {
    /// Ready quote entries reused without running adapter simulation.
    pub hits: usize,
    /// Quote keys that were absent and therefore executed by this search.
    pub misses: usize,
    /// Times a worker waited for another worker's in-flight quote.
    pub waits: usize,
    /// Adapter simulations actually executed by this search.
    pub executed: usize,
    /// Executed adapter simulations that returned a quote failure.
    pub failed: usize,
}

/// Search and quote errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SearchError {
    /// Invalid search configuration.
    #[error("invalid search config: {reason}")]
    InvalidConfig {
        /// Why the configuration is invalid.
        reason: &'static str,
    },
    /// Token was not present in the graph.
    #[error("token not found in graph: {0:?}")]
    TokenNotFound(Address),
    /// No graph path was found between the requested tokens.
    #[error("no graph path from {from:?} to {to:?}")]
    NoPath {
        /// Source token.
        from: Address,
        /// Destination token.
        to: Address,
    },
    /// The path was empty.
    #[error("cannot quote an empty path")]
    EmptyPath,
    /// A path referenced a pool not registered in the adapter registry.
    #[error("pool not found in registry: {0:?}")]
    MissingPool(PoolKey),
    /// A path referenced a protocol with no registered adapter.
    #[error("adapter not found for protocol: {0:?}")]
    MissingAdapter(ProtocolId),
    /// An active snapshot pool had no published quote-state revision.
    #[cfg(feature = "live-runtime")]
    #[error("active pool has no published quote-state revision: {0:?}")]
    MissingPoolRevision(PoolInstanceId),
    /// A snapshot-scoped searcher was paired with a different live graph state.
    #[cfg(feature = "live-runtime")]
    #[error("live graph does not represent the supplied AMM runtime snapshot")]
    SnapshotGraphMismatch,
    /// A snapshot-only search API was called on a static searcher.
    #[cfg(feature = "live-runtime")]
    #[error("snapshot-only search requires AmmSearcher::from_snapshot")]
    SnapshotRequired,
    /// One hop failed during adapter simulation.
    #[error("quote failed for hop {hop:?}: {source}")]
    QuoteFailed {
        /// Hop that failed.
        hop: Hop,
        /// Underlying adapter simulation error.
        #[source]
        source: Box<SimError>,
    },
    /// Candidate paths existed, but every evaluated path failed to quote.
    #[error("no viable route among {candidates} candidates")]
    NoViableRoute {
        /// Number of candidate paths evaluated.
        candidates: usize,
        /// Per-path failure diagnostics.
        failures: Vec<PathFailure>,
    },
    /// A parallel search worker panicked while evaluating candidate paths.
    #[error("parallel search worker panicked")]
    WorkerPanic,
}

const DEFAULT_INCREMENTAL_PROBE_LIMIT: usize = 256;

/// Stateful route search that can be refreshed after live AMM events.
pub struct RouteSearchSession {
    request: RouteRequest,
    streaming_config: StreamingSearchConfig,
    finality: SearchFinality,
    route_table: HashMap<RoutePath, RouteSessionEntry>,
    pool_to_routes: HashMap<PoolKey, HashSet<RoutePath>>,
    parallel_probe_index: HashMap<PoolKey, HashSet<RoutePath>>,
    best: Option<RouteQuote>,
    quote_cache: SharedQuoteCache,
    graph_version: GraphVersion,
    state_point: Option<AmmStatePoint>,
    max_probe_paths_per_refresh: usize,
}

impl RouteSearchSession {
    /// Original route request for this session.
    pub fn request(&self) -> &RouteRequest {
        &self.request
    }

    /// Completion state reached by the initial streamed search.
    pub fn finality(&self) -> SearchFinality {
        self.finality
    }

    /// Streaming controls used to create the session.
    pub fn streaming_config(&self) -> StreamingSearchConfig {
        self.streaming_config
    }

    /// Best route currently known to the session.
    pub fn best(&self) -> Option<&RouteQuote> {
        self.best.as_ref()
    }

    /// Quote-cache counters currently held by the session.
    pub fn quote_cache_stats(&self) -> QuoteCacheStats {
        self.quote_cache.stats()
    }

    /// Number of materialized route paths tracked by this session.
    pub fn materialized_route_count(&self) -> usize {
        self.route_table.len()
    }

    /// Number of local parallel-replacement probe paths tracked by this session.
    pub fn parallel_probe_count(&self) -> usize {
        self.parallel_probe_index
            .values()
            .map(HashSet::len)
            .sum::<usize>()
    }

    /// Refresh affected route candidates after the caller has applied live sync
    /// updates to `cache`.
    pub fn refresh_affected(
        &mut self,
        searcher: &AmmSearcher<'_>,
        cache: &mut EvmCache,
        affected: AffectedPools,
        mut on_event: impl FnMut(RouteUpdateEvent) -> SearchControl,
    ) -> IncrementalRouteUpdateReport {
        let previous_best = self.best.clone();
        let affected_pools = affected.pools.clone();
        let early_recompute_reason = if affected.removed_logs > 0 {
            Some(RecomputeReason::RemovedLogs {
                count: affected.removed_logs,
            })
        } else if affected.unknown_logs > 0 {
            Some(RecomputeReason::UnknownLogs {
                count: affected.unknown_logs,
            })
        } else {
            match (self.state_point, searcher.state_point()) {
                (Some(previous), Some(current)) if previous != current => {
                    Some(RecomputeReason::StatePointChanged { previous, current })
                }
                (Some(_), None) | (None, Some(_)) => Some(RecomputeReason::StateScopeChanged),
                _ if self.graph_version != searcher.graph.version() => {
                    Some(RecomputeReason::TopologyChanged)
                }
                _ => None,
            }
        };
        if !emit_route_update_event(
            &mut on_event,
            RouteUpdateEvent::Started {
                affected_pools: affected_pools.clone(),
            },
        ) {
            return self.incremental_report(
                previous_best,
                if early_recompute_reason.is_some() {
                    IncrementalRouteUpdateStatus::RecomputeRequired
                } else {
                    IncrementalRouteUpdateStatus::Unchanged
                },
                affected_pools,
                0,
                0,
                early_recompute_reason,
            );
        }
        if let Some(reason) = early_recompute_reason {
            return self.recompute_required(previous_best, affected_pools, reason, on_event);
        }
        if affected.pools.is_empty() {
            let report = self.incremental_report(
                previous_best,
                IncrementalRouteUpdateStatus::Unchanged,
                affected_pools,
                0,
                0,
                None,
            );
            emit_route_update_event(
                &mut on_event,
                RouteUpdateEvent::Completed {
                    report: report.clone(),
                },
            );
            return report;
        }
        for pool in &affected.pools {
            let Some(registration) = searcher.registry.pool(pool) else {
                return self.recompute_required(
                    previous_best,
                    affected_pools,
                    RecomputeReason::UnknownAffectedPool(pool.clone()),
                    on_event,
                );
            };
            if registration.status == PoolStatus::Degraded {
                return self.recompute_required(
                    previous_best,
                    affected_pools,
                    RecomputeReason::DegradedAffectedPool(pool.clone()),
                    on_event,
                );
            }
        }
        self.quote_cache.set_context(searcher.quote_context.clone());

        let mut materialized_paths = HashSet::new();
        let mut probe_paths = HashSet::new();
        for pool in &affected.pools {
            if let Some(paths) = self.pool_to_routes.get(pool) {
                materialized_paths.extend(paths.iter().cloned());
            }
            if let Some(paths) = self.parallel_probe_index.get(pool) {
                probe_paths.extend(paths.iter().cloned());
            }
        }
        for path in &materialized_paths {
            probe_paths.remove(path);
        }

        if materialized_paths.is_empty() && probe_paths.is_empty() {
            let report = self.incremental_report(
                previous_best,
                IncrementalRouteUpdateStatus::Unchanged,
                affected_pools,
                0,
                0,
                None,
            );
            emit_route_update_event(
                &mut on_event,
                RouteUpdateEvent::Completed {
                    report: report.clone(),
                },
            );
            return report;
        }

        if probe_paths.len() > self.max_probe_paths_per_refresh {
            return self.recompute_required(
                previous_best,
                affected_pools,
                RecomputeReason::ProbeBudgetExceeded {
                    requested: probe_paths.len(),
                    limit: self.max_probe_paths_per_refresh,
                },
                on_event,
            );
        }

        self.quote_cache.invalidate_pools(&affected.pools);
        let mut overlay_cache = searcher.new_overlay_cache(cache);
        let mut routes_requoted = 0_usize;
        let mut probe_routes_quoted = 0_usize;

        let materialized = sorted_paths(materialized_paths);
        if !materialized.is_empty() {
            let mut on_quote = |quote: &RouteQuote| {
                emit_route_update_event(
                    &mut on_event,
                    RouteUpdateEvent::RouteRequoted {
                        quote: quote.clone(),
                    },
                )
            };
            let report = quote_paths_with_registry_observed(
                searcher.registry,
                materialized.clone(),
                self.request.amount_in,
                &self.request.sim_config,
                &mut overlay_cache,
                &self.quote_cache,
                Some(&mut on_quote),
                None,
            );
            for path in materialized {
                self.route_table.insert(path, RouteSessionEntry::Failed);
            }
            for failure in report.failures {
                self.route_table
                    .insert(failure.path, RouteSessionEntry::Failed);
            }
            for quote in report.quotes {
                routes_requoted += 1;
                self.insert_quote(quote, searcher);
            }
        }

        let probes = sorted_paths(probe_paths);
        if !probes.is_empty() {
            let mut on_quote = |quote: &RouteQuote| {
                emit_route_update_event(
                    &mut on_event,
                    RouteUpdateEvent::ProbeRouteFound {
                        quote: quote.clone(),
                    },
                )
            };
            let report = quote_paths_with_registry_observed(
                searcher.registry,
                probes,
                self.request.amount_in,
                &self.request.sim_config,
                &mut overlay_cache,
                &self.quote_cache,
                Some(&mut on_quote),
                None,
            );
            for quote in report.quotes {
                probe_routes_quoted += 1;
                self.insert_quote(quote, searcher);
            }
        }

        self.best = self.best_from_table();
        let best_changed = self.best != previous_best;
        if best_changed {
            emit_route_update_event(
                &mut on_event,
                RouteUpdateEvent::BestChanged {
                    previous_best: previous_best.clone(),
                    best: self.best.clone(),
                },
            );
        }

        let status = if routes_requoted == 0 && probe_routes_quoted == 0 && !best_changed {
            IncrementalRouteUpdateStatus::Unchanged
        } else {
            IncrementalRouteUpdateStatus::Updated
        };
        let report = self.incremental_report(
            previous_best,
            status,
            affected_pools,
            routes_requoted,
            probe_routes_quoted,
            None,
        );
        emit_route_update_event(
            &mut on_event,
            RouteUpdateEvent::Completed {
                report: report.clone(),
            },
        );
        report
    }

    fn insert_quote(&mut self, quote: RouteQuote, searcher: &AmmSearcher<'_>) {
        let path = quote.path.clone();
        self.route_table
            .insert(path.clone(), RouteSessionEntry::Quoted(quote));
        index_route_path(&path, &mut self.pool_to_routes);
        index_probe_paths_for_route(searcher, &path, &mut self.parallel_probe_index);
    }

    fn best_from_table(&self) -> Option<RouteQuote> {
        self.route_table
            .values()
            .filter_map(RouteSessionEntry::quote)
            .max_by_key(|quote| quote.amount_out)
            .cloned()
    }

    fn incremental_report(
        &self,
        previous_best: Option<RouteQuote>,
        status: IncrementalRouteUpdateStatus,
        affected_pools: HashSet<PoolKey>,
        routes_requoted: usize,
        probe_routes_quoted: usize,
        recompute_reason: Option<RecomputeReason>,
    ) -> IncrementalRouteUpdateReport {
        IncrementalRouteUpdateReport {
            previous_best,
            best: self.best.clone(),
            status,
            affected_pools,
            routes_requoted,
            probe_routes_quoted,
            quote_cache: self.quote_cache.stats(),
            recompute_reason,
        }
    }

    fn recompute_required(
        &self,
        previous_best: Option<RouteQuote>,
        affected_pools: HashSet<PoolKey>,
        reason: RecomputeReason,
        mut on_event: impl FnMut(RouteUpdateEvent) -> SearchControl,
    ) -> IncrementalRouteUpdateReport {
        emit_route_update_event(
            &mut on_event,
            RouteUpdateEvent::RecomputeRequired {
                reason: reason.clone(),
            },
        );
        let report = self.incremental_report(
            previous_best,
            IncrementalRouteUpdateStatus::RecomputeRequired,
            affected_pools,
            0,
            0,
            Some(reason),
        );
        emit_route_update_event(
            &mut on_event,
            RouteUpdateEvent::Completed {
                report: report.clone(),
            },
        );
        report
    }
}

/// Immutable, O(1)-clone search input shared by live route workers.
#[cfg(feature = "live-runtime")]
#[derive(Clone)]
pub struct LiveSearchView {
    snapshot: Arc<AmmStateSnapshot>,
    graph: Arc<AmmGraph>,
    liquidity: Arc<PoolLiquidityIndex>,
    quote_context: QuoteKeyContext,
}

#[cfg(feature = "live-runtime")]
impl LiveSearchView {
    /// Capture one coherent AMM snapshot, graph generation, and liquidity view.
    pub fn new(
        snapshot: Arc<AmmStateSnapshot>,
        live_graph: &LiveAmmGraph,
    ) -> Result<Self, SearchError> {
        if !live_graph.matches_snapshot(&snapshot) {
            return Err(SearchError::SnapshotGraphMismatch);
        }
        let quote_context = live_quote_context(&snapshot);
        Ok(Self {
            snapshot,
            graph: live_graph.graph_snapshot(),
            liquidity: live_graph.liquidity_snapshot(),
            quote_context,
        })
    }

    /// Exact immutable AMM snapshot represented by this view.
    pub fn snapshot(&self) -> &Arc<AmmStateSnapshot> {
        &self.snapshot
    }

    /// Graph topology snapshot represented by this view.
    pub fn graph(&self) -> &Arc<AmmGraph> {
        &self.graph
    }

    /// Liquidity sidecar snapshot represented by this view.
    pub fn liquidity(&self) -> &Arc<PoolLiquidityIndex> {
        &self.liquidity
    }

    /// Construct a borrowing searcher without rebuilding live quote scope.
    pub fn searcher(&self) -> AmmSearcher<'_> {
        AmmSearcher {
            registry: self.snapshot.registry().registry(),
            graph: &self.graph,
            liquidity_index: Some(&self.liquidity),
            quote_context: self.quote_context.clone(),
            snapshot_cache: Some(self.snapshot.cache_snapshot()),
        }
    }
}

enum SearchOverlaySource<'a> {
    Cache(&'a mut EvmCache),
    #[cfg(feature = "live-runtime")]
    Snapshot(Arc<EvmSnapshot>),
}

impl<'a> SearchOverlaySource<'a> {
    fn new(_searcher: &AmmSearcher<'_>, cache: &'a mut EvmCache) -> Self {
        #[cfg(feature = "live-runtime")]
        if let Some(snapshot) = &_searcher.snapshot_cache {
            return Self::Snapshot(Arc::clone(snapshot));
        }
        Self::Cache(cache)
    }

    fn overlay(&mut self) -> OverlayAdapterCache {
        match self {
            Self::Cache(cache) => OverlayAdapterCache::new(cache.mock_overlay()),
            #[cfg(feature = "live-runtime")]
            Self::Snapshot(snapshot) => {
                OverlayAdapterCache::new(EvmOverlay::new(Arc::clone(snapshot), None))
            }
        }
    }
}

/// Route and cycle searcher over an [`AmmGraph`] and [`AdapterRegistry`].
pub struct AmmSearcher<'a> {
    registry: &'a AdapterRegistry,
    graph: &'a AmmGraph,
    liquidity_index: Option<&'a PoolLiquidityIndex>,
    quote_context: QuoteKeyContext,
    #[cfg(feature = "live-runtime")]
    snapshot_cache: Option<Arc<EvmSnapshot>>,
}

impl<'a> AmmSearcher<'a> {
    /// Construct a searcher from a registry and graph built from that registry.
    pub fn new(registry: &'a AdapterRegistry, graph: &'a AmmGraph) -> Self {
        Self {
            registry,
            graph,
            liquidity_index: None,
            quote_context: QuoteKeyContext::Static,
            #[cfg(feature = "live-runtime")]
            snapshot_cache: None,
        }
    }

    /// Construct a searcher whose quote cache is scoped to one immutable AMM snapshot.
    #[cfg(feature = "live-runtime")]
    pub fn from_snapshot(
        snapshot: &'a AmmStateSnapshot,
        live_graph: &'a LiveAmmGraph,
    ) -> Result<Self, SearchError> {
        if !live_graph.matches_snapshot(snapshot) {
            return Err(SearchError::SnapshotGraphMismatch);
        }
        let quote_context = live_quote_context(snapshot);
        Ok(Self {
            registry: snapshot.registry().registry(),
            graph: live_graph.graph(),
            liquidity_index: None,
            quote_context,
            snapshot_cache: Some(snapshot.cache_snapshot()),
        })
    }

    /// Complete chain-state point used by versioned quote keys, when present.
    pub const fn state_point(&self) -> Option<AmmStatePoint> {
        self.quote_context.point()
    }

    fn new_quote_cache(&self) -> SharedQuoteCache {
        SharedQuoteCache::with_context(self.quote_context.clone())
    }

    fn new_overlay_cache(&self, cache: &mut EvmCache) -> OverlayAdapterCache {
        if let Some(overlay) = self.snapshot_overlay_cache() {
            return overlay;
        }
        OverlayAdapterCache::new(cache.mock_overlay())
    }

    fn snapshot_overlay_cache(&self) -> Option<OverlayAdapterCache> {
        #[cfg(feature = "live-runtime")]
        if let Some(snapshot) = &self.snapshot_cache {
            return Some(OverlayAdapterCache::new(EvmOverlay::new(
                Arc::clone(snapshot),
                None,
            )));
        }
        None
    }

    /// Attach an optional liquidity index used by heuristic searches whose
    /// [`SearchConfig::liquidity_pruning`] is enabled.
    pub fn with_liquidity_index(mut self, liquidity_index: &'a PoolLiquidityIndex) -> Self {
        self.liquidity_index = Some(liquidity_index);
        self
    }

    /// Return the best route by gross output.
    pub fn find_best_route(
        &self,
        request: &RouteRequest,
        cache: &mut dyn AdapterCache,
    ) -> Result<RouteQuote, SearchError> {
        self.find_routes(request, cache)?
            .into_iter()
            .next()
            .ok_or(SearchError::NoPath {
                from: request.token_in,
                to: request.token_out,
            })
    }

    /// Return the best route using only the immutable cache carried by a live snapshot.
    #[cfg(feature = "live-runtime")]
    pub fn find_best_route_snapshot(
        &self,
        request: &RouteRequest,
    ) -> Result<RouteQuote, SearchError> {
        self.find_routes_snapshot(request)?
            .into_iter()
            .next()
            .ok_or(SearchError::NoPath {
                from: request.token_in,
                to: request.token_out,
            })
    }

    /// Find and quote routes using only the immutable cache carried by a live snapshot.
    #[cfg(feature = "live-runtime")]
    pub fn find_routes_snapshot(
        &self,
        request: &RouteRequest,
    ) -> Result<Vec<RouteQuote>, SearchError> {
        let mut overlay = self
            .snapshot_overlay_cache()
            .ok_or(SearchError::SnapshotRequired)?;
        let quote_cache = self.new_quote_cache();
        self.find_routes_with_quote_cache(request, &mut overlay, &quote_cache)
    }

    /// Return the best route by gross output using overlay-backed worker threads.
    pub fn find_best_route_parallel(
        &self,
        request: &RouteRequest,
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<RouteQuote, SearchError> {
        self.find_routes_parallel(request, cache, parallel_config)?
            .into_iter()
            .next()
            .ok_or(SearchError::NoPath {
                from: request.token_in,
                to: request.token_out,
            })
    }

    /// Find and quote routes, sorted by gross output descending.
    pub fn find_routes(
        &self,
        request: &RouteRequest,
        cache: &mut dyn AdapterCache,
    ) -> Result<Vec<RouteQuote>, SearchError> {
        let quote_cache = self.new_quote_cache();
        if let Some(mut overlay) = self.snapshot_overlay_cache() {
            return self.find_routes_with_quote_cache(request, &mut overlay, &quote_cache);
        }
        self.find_routes_with_quote_cache(request, cache, &quote_cache)
    }

    /// Find and quote routes on isolated [`EvmOverlay`](evm_fork_cache::cache::EvmOverlay)
    /// instances, sorted by gross output descending.
    pub fn find_routes_parallel(
        &self,
        request: &RouteRequest,
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<Vec<RouteQuote>, SearchError> {
        if matches!(request.config.mode, SearchMode::Heuristic(_)) {
            parallel_config.validate()?;
            let quote_cache = self.new_quote_cache();
            let mut overlay_cache = self.new_overlay_cache(cache);
            return self.find_routes_with_quote_cache(request, &mut overlay_cache, &quote_cache);
        }

        let paths =
            self.enumerate_routes(request.token_in, request.token_out, &request.config, false)?;
        self.quote_and_rank_parallel(
            paths,
            request.amount_in,
            &request.sim_config,
            cache,
            parallel_config,
        )
    }

    pub fn stream_routes_parallel(
        &self,
        request: &RouteRequest,
        cache: &mut EvmCache,
        streaming_config: StreamingSearchConfig,
        mut on_event: impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> Result<StreamingSearchReport, SearchError> {
        let mut overlays = SearchOverlaySource::new(self, cache);
        Ok(self
            .stream_routes_parallel_with_cache(
                request,
                &mut overlays,
                streaming_config,
                self.new_quote_cache(),
                &mut on_event,
                None,
            )?
            .report)
    }

    /// Start a stateful route-search session that can be refreshed after live
    /// AMM events.
    pub fn start_route_session(
        &self,
        request: &RouteRequest,
        cache: &mut EvmCache,
        streaming_config: StreamingSearchConfig,
        mut on_event: impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> Result<RouteSearchSession, SearchError> {
        let quote_cache = self.new_quote_cache();
        let mut overlays = SearchOverlaySource::new(self, cache);
        let run = self.stream_routes_parallel_with_cache(
            request,
            &mut overlays,
            streaming_config.with_emit_all_viable(true),
            quote_cache.clone(),
            &mut on_event,
            None,
        )?;
        let mut session = RouteSearchSession {
            request: request.clone(),
            streaming_config,
            finality: run.report.finality,
            route_table: HashMap::new(),
            pool_to_routes: HashMap::new(),
            parallel_probe_index: HashMap::new(),
            best: run.report.best.clone(),
            quote_cache,
            graph_version: self.graph.version(),
            state_point: self.state_point(),
            max_probe_paths_per_refresh: DEFAULT_INCREMENTAL_PROBE_LIMIT,
        };
        for quote in run.quotes {
            session.insert_quote(quote, self);
        }
        if let Some(best) = run.report.best
            && !session.route_table.contains_key(&best.path)
        {
            session.insert_quote(best, self);
        }
        Ok(session)
    }

    /// Stream a snapshot-bound route search without borrowing a mutable cache.
    #[cfg(feature = "live-runtime")]
    pub fn stream_routes_snapshot(
        &self,
        request: &RouteRequest,
        streaming_config: StreamingSearchConfig,
        mut on_event: impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> Result<StreamingSearchReport, SearchError> {
        let snapshot = self
            .snapshot_cache
            .as_ref()
            .ok_or(SearchError::SnapshotRequired)?;
        let mut overlays = SearchOverlaySource::Snapshot(Arc::clone(snapshot));
        Ok(self
            .stream_routes_parallel_with_cache(
                request,
                &mut overlays,
                streaming_config,
                self.new_quote_cache(),
                &mut on_event,
                None,
            )?
            .report)
    }

    /// Search one immutable snapshot while allowing its owning runtime to
    /// interrupt candidate enumeration between bounded graph expansions.
    #[cfg(feature = "live-runtime")]
    pub(crate) fn stream_routes_snapshot_cancellable(
        &self,
        request: &RouteRequest,
        streaming_config: StreamingSearchConfig,
        is_cancelled: impl Fn() -> bool,
        mut on_event: impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> Result<StreamingSearchReport, SearchError> {
        let snapshot = self
            .snapshot_cache
            .as_ref()
            .ok_or(SearchError::SnapshotRequired)?;
        let mut overlays = SearchOverlaySource::Snapshot(Arc::clone(snapshot));
        let should_continue = || !is_cancelled();
        Ok(self
            .stream_routes_parallel_with_cache(
                request,
                &mut overlays,
                streaming_config,
                self.new_quote_cache(),
                &mut on_event,
                Some(&should_continue),
            )?
            .report)
    }

    /// Stream route results in heuristic-first order, optionally continuing
    /// with an exhaustive remainder.
    ///
    /// The callback is invoked on the coordinating thread, not on worker
    /// threads. Returning [`SearchControl::Stop`] stops scheduling new work and
    /// returns a report with [`SearchFinality::Stopped`].
    fn stream_routes_parallel_with_cache(
        &self,
        request: &RouteRequest,
        overlays: &mut SearchOverlaySource<'_>,
        streaming_config: StreamingSearchConfig,
        quote_cache: SharedQuoteCache,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
        should_continue: Option<&dyn Fn() -> bool>,
    ) -> Result<StreamingRunReport, SearchError> {
        streaming_config.validate()?;
        request.config.validate()?;

        let mut stream_state = StreamingRouteState::new(streaming_config);
        let mut evaluated_paths = HashSet::<RoutePath>::new();
        let mut observed_quotes = Vec::<RouteQuote>::new();

        if !emit_search_event(
            on_event,
            RouteSearchEvent::Started {
                completion: streaming_config.completion,
            },
        ) {
            return Ok(StreamingRunReport {
                report: stream_state.report(
                    SearchFinality::Stopped,
                    quote_cache.stats(),
                    quote_cache.liquidity_prune_stats(),
                ),
                quotes: observed_quotes,
            });
        }

        let heuristic = match request.config.mode {
            SearchMode::Exhaustive => HeuristicSearchConfig::default(),
            SearchMode::Heuristic(heuristic) => heuristic,
        };
        let mut heuristic_config = request.config.clone();
        heuristic_config.mode = SearchMode::Heuristic(heuristic);

        let mut heuristic_overlay = overlays.overlay();
        let mut heuristic_error = None;
        let mut stopped = false;
        {
            let mut on_quote = |quote: &RouteQuote| {
                evaluated_paths.insert(quote.path.clone());
                observed_quotes.push(quote.clone());
                stream_state.observe_quote(RouteSearchPhase::Heuristic, quote.clone(), on_event)
            };

            match self.find_routes_heuristic_with_cache_observed(
                request.token_in,
                request.token_out,
                request.amount_in,
                &heuristic_config,
                heuristic,
                &request.sim_config,
                &mut heuristic_overlay,
                &quote_cache,
                false,
                if streaming_config.completion == StreamingCompletion::FastLaneExhausted {
                    HeuristicRunLimit::FastLaneOnly
                } else {
                    HeuristicRunLimit::Full
                },
                Some(&mut on_quote),
            ) {
                Ok(report) => {
                    if !stream_state.observe_failed_candidates(
                        RouteSearchPhase::Heuristic,
                        report.failed_candidates,
                        on_event,
                    ) {
                        stopped = true;
                    }
                    stopped |= report.stopped;
                    for quote in report.quotes {
                        evaluated_paths.insert(quote.path);
                    }
                }
                Err(err @ (SearchError::InvalidConfig { .. } | SearchError::TokenNotFound(_))) => {
                    return Err(err);
                }
                Err(err) => {
                    heuristic_error = Some(err);
                }
            }
        }
        if !stopped && !stream_state.finish_heuristic_phase(on_event) {
            stopped = true;
        }
        if should_continue.is_some_and(|should_continue| !should_continue()) {
            stopped = true;
        }

        if stopped {
            let finality = stream_state.stop_finality();
            let emit_completed = stream_state.should_emit_completion_for_stop();
            let report = finish_stream_report(
                &mut stream_state,
                finality,
                &quote_cache,
                on_event,
                emit_completed,
            );
            return Ok(StreamingRunReport {
                report,
                quotes: observed_quotes,
            });
        }

        if !emit_search_event(
            on_event,
            RouteSearchEvent::PhaseCompleted {
                phase: RouteSearchPhase::Heuristic,
                stats: stream_state
                    .phase_stats(quote_cache.stats(), quote_cache.liquidity_prune_stats()),
            },
        ) {
            let report = finish_stream_report(
                &mut stream_state,
                SearchFinality::Stopped,
                &quote_cache,
                on_event,
                false,
            );
            return Ok(StreamingRunReport {
                report,
                quotes: observed_quotes,
            });
        }

        if matches!(
            streaming_config.completion,
            StreamingCompletion::FastLaneExhausted | StreamingCompletion::HeuristicExhausted
        ) {
            if stream_state.best.is_none()
                && let Some(err) = heuristic_error
            {
                return Err(err);
            }
            let finality = if streaming_config.completion == StreamingCompletion::FastLaneExhausted
            {
                SearchFinality::FastLaneOnly
            } else {
                SearchFinality::HeuristicOnly
            };
            let report =
                finish_stream_report(&mut stream_state, finality, &quote_cache, on_event, true);
            return Ok(StreamingRunReport {
                report,
                quotes: observed_quotes,
            });
        }

        let mut exhaustive_config = request.config.clone();
        exhaustive_config.mode = SearchMode::Exhaustive;
        let remaining_budget = request.config.max_candidates.map(|max_candidates| {
            max_candidates.saturating_sub(stream_state.candidates_evaluated())
        });
        let enumeration = match self.enumerate_routes_filtered(
            request.token_in,
            request.token_out,
            &exhaustive_config,
            false,
            &evaluated_paths,
            remaining_budget,
            should_continue,
        ) {
            Ok(enumeration) => enumeration,
            Err(err) => {
                if stream_state.best.is_none() {
                    return Err(heuristic_error.unwrap_or(err));
                }
                let report = finish_stream_report(
                    &mut stream_state,
                    SearchFinality::Exhaustive,
                    &quote_cache,
                    on_event,
                    true,
                );
                return Ok(StreamingRunReport {
                    report,
                    quotes: observed_quotes,
                });
            }
        };

        stream_state.duplicate_paths_skipped = stream_state
            .duplicate_paths_skipped
            .saturating_add(enumeration.duplicates_skipped);
        let remaining_paths = enumeration.paths;
        if enumeration.stopped {
            let report = finish_stream_report(
                &mut stream_state,
                SearchFinality::Stopped,
                &quote_cache,
                on_event,
                false,
            );
            return Ok(StreamingRunReport {
                report,
                quotes: observed_quotes,
            });
        }
        if !stream_state.set_total_candidates(
            stream_state
                .candidates_evaluated()
                .saturating_add(remaining_paths.len()),
            on_event,
        ) {
            stopped = true;
        }

        let mut exhaustive_failures = Vec::new();
        if !stopped && !remaining_paths.is_empty() {
            let mut on_quote = |quote: &RouteQuote| {
                observed_quotes.push(quote.clone());
                stream_state.observe_quote(RouteSearchPhase::Exhaustive, quote.clone(), on_event)
            };
            let report = self.quote_and_stream_parallel(
                remaining_paths,
                request.amount_in,
                &request.sim_config,
                overlays,
                streaming_config.parallel,
                &quote_cache,
                &mut on_quote,
            )?;
            stopped = report.stopped;
            if !stopped
                && !stream_state.observe_failed_candidates(
                    RouteSearchPhase::Exhaustive,
                    report.failures.len(),
                    on_event,
                )
            {
                stopped = true;
            }
            exhaustive_failures = report.failures;
        }

        if stopped {
            let finality = stream_state.stop_finality();
            let emit_completed = stream_state.should_emit_completion_for_stop();
            let report = finish_stream_report(
                &mut stream_state,
                finality,
                &quote_cache,
                on_event,
                emit_completed,
            );
            return Ok(StreamingRunReport {
                report,
                quotes: observed_quotes,
            });
        }
        stream_state.mark_exhaustive_complete();

        if !emit_search_event(
            on_event,
            RouteSearchEvent::PhaseCompleted {
                phase: RouteSearchPhase::Exhaustive,
                stats: stream_state
                    .phase_stats(quote_cache.stats(), quote_cache.liquidity_prune_stats()),
            },
        ) {
            let report = finish_stream_report(
                &mut stream_state,
                SearchFinality::Stopped,
                &quote_cache,
                on_event,
                false,
            );
            return Ok(StreamingRunReport {
                report,
                quotes: observed_quotes,
            });
        }

        if stream_state.best.is_none() {
            if let Some(err) = heuristic_error {
                return Err(err);
            }
            return Err(SearchError::NoViableRoute {
                candidates: exhaustive_failures.len(),
                failures: exhaustive_failures,
            });
        }

        let report = finish_stream_report(
            &mut stream_state,
            SearchFinality::Exhaustive,
            &quote_cache,
            on_event,
            true,
        );
        Ok(StreamingRunReport {
            report,
            quotes: observed_quotes,
        })
    }

    /// Find and quote cycles, sorted by gross output descending.
    pub fn find_cycles(
        &self,
        request: &CycleRequest,
        cache: &mut dyn AdapterCache,
    ) -> Result<Vec<CycleQuote>, SearchError> {
        let quote_cache = self.new_quote_cache();
        if let Some(mut overlay) = self.snapshot_overlay_cache() {
            return self.find_cycles_with_quote_cache(request, &mut overlay, &quote_cache);
        }
        self.find_cycles_with_quote_cache(request, cache, &quote_cache)
    }

    /// Find and quote cycles on isolated [`EvmOverlay`](evm_fork_cache::cache::EvmOverlay)
    /// instances, sorted by gross output descending.
    pub fn find_cycles_parallel(
        &self,
        request: &CycleRequest,
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<Vec<CycleQuote>, SearchError> {
        if matches!(request.config.mode, SearchMode::Heuristic(_)) {
            parallel_config.validate()?;
            let quote_cache = self.new_quote_cache();
            let mut overlay_cache = self.new_overlay_cache(cache);
            return self.find_cycles_with_quote_cache(request, &mut overlay_cache, &quote_cache);
        }

        let paths = self.enumerate_routes(
            request.base_token,
            request.base_token,
            &request.config,
            true,
        )?;
        let routes = self.quote_and_rank_parallel(
            paths,
            request.amount_in,
            &request.sim_config,
            cache,
            parallel_config,
        )?;

        Ok(routes
            .into_iter()
            .map(|route| {
                let profit = (route.amount_out > route.amount_in)
                    .then_some(route.amount_out - route.amount_in);
                CycleQuote { route, profit }
            })
            .collect())
    }

    /// Find and quote many route requests over a fixed worker pool.
    ///
    /// Each worker gets one isolated `EvmOverlay` and processes whole route
    /// requests serially. This is the fixed-worker-pool path to prefer when the
    /// caller has many independent searches, because it avoids spawning threads
    /// per request.
    pub fn find_routes_batch_parallel(
        &self,
        requests: &[RouteRequest],
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<Vec<Result<Vec<RouteQuote>, SearchError>>, SearchError> {
        Ok(self
            .find_routes_batch_parallel_with_stats(requests, cache, parallel_config)?
            .results)
    }

    /// Find and quote many route requests over a fixed worker pool, returning
    /// quote-cache diagnostics with the ordered per-request results.
    pub fn find_routes_batch_parallel_with_stats(
        &self,
        requests: &[RouteRequest],
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<BatchSearchReport<RouteQuote>, SearchError> {
        parallel_config.validate()?;
        if requests.is_empty() {
            return Ok(BatchSearchReport {
                results: Vec::new(),
                quote_cache: QuoteCacheStats::default(),
                liquidity_pruning: LiquidityPruneStats::default(),
            });
        }

        let worker_count = parallel_config.workers.min(requests.len());
        let chunks = split_indexed_items(requests, worker_count);
        let quote_cache = self.new_quote_cache();

        if worker_count == 1 {
            let overlay_cache = self.new_overlay_cache(cache);
            let worker_quote_cache = quote_cache.clone();
            let results = find_route_request_chunk(
                self.registry,
                self.graph,
                self.liquidity_index,
                chunks[0].clone(),
                overlay_cache,
                worker_quote_cache,
            );
            return Ok(BatchSearchReport {
                results: collect_ordered_results(results, requests.len()),
                quote_cache: quote_cache.stats(),
                liquidity_pruning: quote_cache.liquidity_prune_stats(),
            });
        }

        let mut overlay_caches = Vec::with_capacity(chunks.len());
        for _ in 0..chunks.len() {
            overlay_caches.push(self.new_overlay_cache(cache));
        }

        let registry = self.registry;
        let graph = self.graph;
        let liquidity_index = self.liquidity_index;
        let worker_results = thread::scope(|scope| {
            let handles: Vec<_> = chunks
                .into_iter()
                .zip(overlay_caches)
                .map(|(chunk, overlay_cache)| {
                    let quote_cache = quote_cache.clone();
                    scope.spawn(move || {
                        find_route_request_chunk(
                            registry,
                            graph,
                            liquidity_index,
                            chunk,
                            overlay_cache,
                            quote_cache,
                        )
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|handle| handle.join())
                .collect::<Vec<_>>()
        });

        let mut results = Vec::with_capacity(requests.len());
        for result in worker_results {
            results.extend(result.map_err(|_| SearchError::WorkerPanic)?);
        }

        Ok(BatchSearchReport {
            results: collect_ordered_results(results, requests.len()),
            quote_cache: quote_cache.stats(),
            liquidity_pruning: quote_cache.liquidity_prune_stats(),
        })
    }

    /// Find and quote many cycle requests over a fixed worker pool.
    pub fn find_cycles_batch_parallel(
        &self,
        requests: &[CycleRequest],
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<Vec<Result<Vec<CycleQuote>, SearchError>>, SearchError> {
        Ok(self
            .find_cycles_batch_parallel_with_stats(requests, cache, parallel_config)?
            .results)
    }

    /// Find and quote many cycle requests over a fixed worker pool, returning
    /// quote-cache diagnostics with the ordered per-request results.
    pub fn find_cycles_batch_parallel_with_stats(
        &self,
        requests: &[CycleRequest],
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<BatchSearchReport<CycleQuote>, SearchError> {
        parallel_config.validate()?;
        if requests.is_empty() {
            return Ok(BatchSearchReport {
                results: Vec::new(),
                quote_cache: QuoteCacheStats::default(),
                liquidity_pruning: LiquidityPruneStats::default(),
            });
        }

        let worker_count = parallel_config.workers.min(requests.len());
        let chunks = split_indexed_items(requests, worker_count);
        let quote_cache = self.new_quote_cache();

        if worker_count == 1 {
            let overlay_cache = self.new_overlay_cache(cache);
            let worker_quote_cache = quote_cache.clone();
            let results = find_cycle_request_chunk(
                self.registry,
                self.graph,
                self.liquidity_index,
                chunks[0].clone(),
                overlay_cache,
                worker_quote_cache,
            );
            return Ok(BatchSearchReport {
                results: collect_ordered_results(results, requests.len()),
                quote_cache: quote_cache.stats(),
                liquidity_pruning: quote_cache.liquidity_prune_stats(),
            });
        }

        let mut overlay_caches = Vec::with_capacity(chunks.len());
        for _ in 0..chunks.len() {
            overlay_caches.push(self.new_overlay_cache(cache));
        }

        let registry = self.registry;
        let graph = self.graph;
        let liquidity_index = self.liquidity_index;
        let worker_results = thread::scope(|scope| {
            let handles: Vec<_> = chunks
                .into_iter()
                .zip(overlay_caches)
                .map(|(chunk, overlay_cache)| {
                    let quote_cache = quote_cache.clone();
                    scope.spawn(move || {
                        find_cycle_request_chunk(
                            registry,
                            graph,
                            liquidity_index,
                            chunk,
                            overlay_cache,
                            quote_cache,
                        )
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|handle| handle.join())
                .collect::<Vec<_>>()
        });

        let mut results = Vec::with_capacity(requests.len());
        for result in worker_results {
            results.extend(result.map_err(|_| SearchError::WorkerPanic)?);
        }

        Ok(BatchSearchReport {
            results: collect_ordered_results(results, requests.len()),
            quote_cache: quote_cache.stats(),
            liquidity_pruning: quote_cache.liquidity_prune_stats(),
        })
    }

    /// Quote an explicit path by dispatching each hop to the registered adapter.
    #[cfg(feature = "live-runtime")]
    pub fn quote_path_snapshot(
        &self,
        path: &RoutePath,
        amount_in: U256,
        sim_config: &SimConfig,
    ) -> Result<RouteQuote, SearchError> {
        let mut overlay = self
            .snapshot_overlay_cache()
            .ok_or(SearchError::SnapshotRequired)?;
        quote_path_with_registry(self.registry, path, amount_in, &mut overlay, sim_config)
    }

    /// Quote an explicit path by dispatching each hop to the registered adapter.
    pub fn quote_path(
        &self,
        path: &RoutePath,
        amount_in: U256,
        cache: &mut dyn AdapterCache,
        sim_config: &SimConfig,
    ) -> Result<RouteQuote, SearchError> {
        if let Some(mut overlay) = self.snapshot_overlay_cache() {
            return quote_path_with_registry(
                self.registry,
                path,
                amount_in,
                &mut overlay,
                sim_config,
            );
        }
        quote_path_with_registry(self.registry, path, amount_in, cache, sim_config)
    }

    fn quote_and_rank_with_cache(
        &self,
        paths: Vec<RoutePath>,
        amount_in: U256,
        sim_config: &SimConfig,
        cache: &mut dyn AdapterCache,
        quote_cache: &SharedQuoteCache,
    ) -> Result<Vec<RouteQuote>, SearchError> {
        let candidates = paths.len();
        let (quotes, failures) = quote_paths_with_registry(
            self.registry,
            paths,
            amount_in,
            sim_config,
            cache,
            quote_cache,
        );

        rank_or_no_viable(candidates, quotes, failures)
    }

    fn find_routes_with_quote_cache(
        &self,
        request: &RouteRequest,
        cache: &mut dyn AdapterCache,
        quote_cache: &SharedQuoteCache,
    ) -> Result<Vec<RouteQuote>, SearchError> {
        match request.config.mode {
            SearchMode::Exhaustive => {
                let paths = self.enumerate_routes(
                    request.token_in,
                    request.token_out,
                    &request.config,
                    false,
                )?;
                self.quote_and_rank_with_cache(
                    paths,
                    request.amount_in,
                    &request.sim_config,
                    cache,
                    quote_cache,
                )
            }
            SearchMode::Heuristic(heuristic) => self.find_routes_heuristic_with_cache(
                request.token_in,
                request.token_out,
                request.amount_in,
                &request.config,
                heuristic,
                &request.sim_config,
                cache,
                quote_cache,
                false,
            ),
        }
    }

    fn find_cycles_with_quote_cache(
        &self,
        request: &CycleRequest,
        cache: &mut dyn AdapterCache,
        quote_cache: &SharedQuoteCache,
    ) -> Result<Vec<CycleQuote>, SearchError> {
        let routes = match request.config.mode {
            SearchMode::Exhaustive => {
                let paths = self.enumerate_routes(
                    request.base_token,
                    request.base_token,
                    &request.config,
                    true,
                )?;
                self.quote_and_rank_with_cache(
                    paths,
                    request.amount_in,
                    &request.sim_config,
                    cache,
                    quote_cache,
                )?
            }
            SearchMode::Heuristic(heuristic) => self.find_routes_heuristic_with_cache(
                request.base_token,
                request.base_token,
                request.amount_in,
                &request.config,
                heuristic,
                &request.sim_config,
                cache,
                quote_cache,
                true,
            )?,
        };

        Ok(routes
            .into_iter()
            .map(|route| {
                let profit = (route.amount_out > route.amount_in)
                    .then_some(route.amount_out - route.amount_in);
                CycleQuote { route, profit }
            })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn find_routes_heuristic_with_cache(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        config: &SearchConfig,
        heuristic: HeuristicSearchConfig,
        sim_config: &SimConfig,
        cache: &mut dyn AdapterCache,
        quote_cache: &SharedQuoteCache,
        allow_final_source_revisit: bool,
    ) -> Result<Vec<RouteQuote>, SearchError> {
        self.find_routes_heuristic_with_cache_observed(
            token_in,
            token_out,
            amount_in,
            config,
            heuristic,
            sim_config,
            cache,
            quote_cache,
            allow_final_source_revisit,
            HeuristicRunLimit::Full,
            None,
        )
        .map(|report| report.quotes)
    }

    #[allow(clippy::too_many_arguments)]
    fn find_routes_heuristic_with_cache_observed(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        config: &SearchConfig,
        heuristic: HeuristicSearchConfig,
        sim_config: &SimConfig,
        cache: &mut dyn AdapterCache,
        quote_cache: &SharedQuoteCache,
        allow_final_source_revisit: bool,
        run_limit: HeuristicRunLimit,
        mut on_quote: Option<&mut dyn FnMut(&RouteQuote) -> bool>,
    ) -> Result<HeuristicRoutesReport, SearchError> {
        config.validate()?;

        let source = self
            .graph
            .node_index(&token_in)
            .ok_or(SearchError::TokenNotFound(token_in))?;
        let target = self
            .graph
            .node_index(&token_out)
            .ok_or(SearchError::TokenNotFound(token_out))?;
        let mut run_cache = HeuristicRunCache::default();
        let connector_tokens = self.connector_tokens_for_heuristic(
            token_in,
            token_out,
            config,
            heuristic,
            &mut run_cache,
        );

        let mut quotes = Vec::new();
        let mut failures = Vec::new();
        let mut stopped = false;
        let mut evaluated_paths = HashSet::<RoutePath>::new();
        let mut incumbent_amount = None;

        if !allow_final_source_revisit && heuristic.fast_lane.enabled {
            let fast_lane_paths = self.fast_lane_paths(
                token_in,
                token_out,
                config,
                heuristic,
                connector_tokens.as_ref(),
                quote_cache,
                &mut run_cache,
            );
            if !fast_lane_paths.is_empty() {
                quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
                    fast_lane_routes: fast_lane_paths.len(),
                    ..LiquidityPruneStats::default()
                });
                for path in &fast_lane_paths {
                    evaluated_paths.insert(path.clone());
                }
                let report = if let Some(on_quote) = on_quote.as_mut() {
                    quote_paths_with_registry_observed(
                        self.registry,
                        fast_lane_paths,
                        amount_in,
                        sim_config,
                        cache,
                        quote_cache,
                        Some(&mut **on_quote),
                        None,
                    )
                } else {
                    quote_paths_with_registry_observed(
                        self.registry,
                        fast_lane_paths,
                        amount_in,
                        sim_config,
                        cache,
                        quote_cache,
                        None,
                        None,
                    )
                };
                stopped = report.stopped;
                failures.extend(report.failures);
                quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
                    fast_lane_quotes: report.quotes.len(),
                    ..LiquidityPruneStats::default()
                });
                for quote in report.quotes {
                    incumbent_amount = Some(
                        incumbent_amount
                            .map_or(quote.amount_out, |best: U256| best.max(quote.amount_out)),
                    );
                    quotes.push(quote);
                    if max_candidates_reached(quotes.len(), config) {
                        stopped = true;
                        break;
                    }
                }
            }
        }

        if stopped || run_limit == HeuristicRunLimit::FastLaneOnly {
            quotes.sort_by_key(|quote| Reverse(quote.amount_out));
            let finalist_count = heuristic.max_finalists.min(quotes.len());
            quotes.truncate(finalist_count);
            if quotes.is_empty() && !failures.is_empty() && stopped {
                return Err(SearchError::NoViableRoute {
                    candidates: failures.len(),
                    failures,
                });
            }
            return Ok(HeuristicRoutesReport {
                quotes,
                stopped,
                failed_candidates: failures.len(),
            });
        }

        let mut prefix_dominance = PrefixDominanceIndex::default();

        let pass_modes = heuristic_pass_modes(heuristic);
        let has_refinement_pass = pass_modes.contains(&HeuristicPassMode::Refinement);
        for pass_mode in pass_modes {
            let mut frontier = vec![HeuristicState {
                node: source,
                path: RoutePath::new(),
                hop_quotes: Vec::new(),
                amount: amount_in,
                visited_tokens: {
                    let mut visited = HashSet::new();
                    visited.insert(token_in);
                    visited
                },
                used_pools: HashSet::new(),
            }];
            let mut deferred_edges_seen = false;

            'search: while !frontier.is_empty() {
                let mut next_frontier = Vec::new();

                for state in frontier {
                    if state.path.len() >= config.max_hops {
                        continue;
                    }
                    if self.should_prune_upper_bound_prefix(
                        &state,
                        target,
                        token_out,
                        config.max_hops,
                        incumbent_amount,
                        heuristic.upper_bound_pruning,
                        allow_final_source_revisit,
                        quote_cache,
                    ) {
                        continue;
                    }
                    if heuristic.prefix_dominance {
                        let Some(token) = self.graph.node_token(state.node) else {
                            continue;
                        };
                        let dominance_used_pools = self.future_blocked_pools(
                            state.node,
                            source,
                            target,
                            token_in,
                            token_out,
                            connector_tokens.as_ref(),
                            &state.visited_tokens,
                            &state.used_pools,
                            allow_final_source_revisit,
                        );
                        if prefix_dominance.is_strictly_dominated(
                            token,
                            state.amount,
                            &state.visited_tokens,
                            &dominance_used_pools,
                        ) {
                            quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
                                prefix_dominated_states: 1,
                                ..LiquidityPruneStats::default()
                            });
                            continue;
                        }
                    }

                    let grouped_hops = self.group_heuristic_hops(
                        &state,
                        source,
                        target,
                        token_in,
                        token_out,
                        connector_tokens.as_ref(),
                        config.max_hops,
                        heuristic,
                        config.liquidity_pruning,
                        quote_cache,
                        allow_final_source_revisit,
                        &mut run_cache,
                    );

                    for group in grouped_hops {
                        let closes_target = group.closes_target;
                        let ranked_hops = self.liquidity_ranked_hops(
                            group.hops,
                            heuristic.edge_shortlist,
                            config.liquidity_pruning,
                            pass_mode,
                            quote_cache,
                        );
                        if pass_mode == HeuristicPassMode::Initial
                            && has_refinement_pass
                            && heuristic.edge_shortlist.enabled
                            && ranked_hops.len() > pass_mode.edge_limit(heuristic)
                        {
                            deferred_edges_seen = true;
                        }
                        let mut quoted_hops = Vec::new();
                        let mut best_quoted_output = None;
                        for ranked in self.shortlisted_ranked_hops(
                            ranked_hops,
                            heuristic,
                            pass_mode,
                            quote_cache,
                        ) {
                            if self.should_prune_liquidity_hop(
                                &ranked,
                                best_quoted_output,
                                config.liquidity_pruning,
                                closes_target,
                                quote_cache,
                            ) {
                                continue;
                            }

                            let hop = ranked.hop;
                            match quote_hop_with_cache(
                                self.registry,
                                &hop,
                                state.amount,
                                cache,
                                sim_config,
                                quote_cache,
                            ) {
                                Ok(amount_out) => {
                                    best_quoted_output = Some(
                                        best_quoted_output
                                            .map_or(amount_out, |best: U256| best.max(amount_out)),
                                    );
                                    quoted_hops.push(QuotedHeuristicHop { hop, amount_out })
                                }
                                Err(reason) => {
                                    let mut path = state.path.clone();
                                    path.push(hop);
                                    failures.push(PathFailure { path, reason });
                                }
                            }
                        }

                        quoted_hops.sort_by_key(|quoted| Reverse(quoted.amount_out));
                        let retain_limit = if heuristic.edge_shortlist.enabled {
                            pass_mode.edge_limit(heuristic)
                        } else {
                            heuristic.parallel_edge_limit
                        };
                        quoted_hops.truncate(retain_limit);

                        for quoted in quoted_hops {
                            let next_node = self
                                .graph
                                .node_index(&quoted.hop.token_out)
                                .expect("heuristic hop output token is indexed");
                            let closes_target = next_node == target;
                            let mut path = state.path.clone();
                            path.push(quoted.hop.clone());
                            let mut hop_quotes = state.hop_quotes.clone();
                            hop_quotes.push(HopQuote {
                                hop: quoted.hop.clone(),
                                amount_in: state.amount,
                                amount_out: quoted.amount_out,
                            });

                            if closes_target {
                                if path.len() >= config.min_hops {
                                    if !evaluated_paths.insert(path.clone()) {
                                        continue;
                                    }
                                    let quote = RouteQuote {
                                        path,
                                        amount_in,
                                        amount_out: quoted.amount_out,
                                        hops: hop_quotes,
                                    };
                                    incumbent_amount = Some(
                                        incumbent_amount.map_or(quote.amount_out, |best: U256| {
                                            best.max(quote.amount_out)
                                        }),
                                    );
                                    let keep_searching = on_quote
                                        .as_deref_mut()
                                        .is_none_or(|on_quote| on_quote(&quote));
                                    quotes.push(quote);
                                    if !keep_searching {
                                        stopped = true;
                                        break 'search;
                                    }
                                    if max_candidates_reached(quotes.len(), config) {
                                        break 'search;
                                    }
                                }
                                continue;
                            }

                            if path.len() < config.max_hops {
                                let mut visited_tokens = state.visited_tokens.clone();
                                visited_tokens.insert(quoted.hop.token_out);
                                let mut used_pools = state.used_pools.clone();
                                used_pools.insert(quoted.hop.pool.clone());
                                let dominance_used_pools = self.future_blocked_pools(
                                    next_node,
                                    source,
                                    target,
                                    token_in,
                                    token_out,
                                    connector_tokens.as_ref(),
                                    &visited_tokens,
                                    &used_pools,
                                    allow_final_source_revisit,
                                );
                                if heuristic.prefix_dominance
                                    && prefix_dominance.insert_or_dominated(
                                        quoted.hop.token_out,
                                        quoted.amount_out,
                                        &visited_tokens,
                                        &dominance_used_pools,
                                    )
                                {
                                    quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
                                        prefix_dominated_states: 1,
                                        ..LiquidityPruneStats::default()
                                    });
                                    continue;
                                }
                                next_frontier.push(HeuristicState {
                                    node: next_node,
                                    path,
                                    hop_quotes,
                                    amount: quoted.amount_out,
                                    visited_tokens,
                                    used_pools,
                                });
                            }
                        }
                    }
                }

                if heuristic.prefix_dominance {
                    let before = next_frontier.len();
                    next_frontier.retain(|state| {
                        let Some(token) = self.graph.node_token(state.node) else {
                            return true;
                        };
                        !prefix_dominance.is_strictly_dominated(
                            token,
                            state.amount,
                            &state.visited_tokens,
                            &self.future_blocked_pools(
                                state.node,
                                source,
                                target,
                                token_in,
                                token_out,
                                connector_tokens.as_ref(),
                                &state.visited_tokens,
                                &state.used_pools,
                                allow_final_source_revisit,
                            ),
                        )
                    });
                    let removed = before.saturating_sub(next_frontier.len());
                    if removed > 0 {
                        quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
                            prefix_dominated_states: removed,
                            ..LiquidityPruneStats::default()
                        });
                    }
                }
                let preserve_liquidity_branch_order = config.liquidity_pruning.enabled
                    && config.liquidity_pruning.rank_branches_by_liquidity
                    && self.liquidity_index.is_some();
                apply_beam_width(
                    &mut next_frontier,
                    heuristic.beam_width,
                    !preserve_liquidity_branch_order,
                );
                frontier = next_frontier;
            }
            if stopped {
                break;
            }
            if pass_mode == HeuristicPassMode::Initial
                && has_refinement_pass
                && !deferred_edges_seen
            {
                break;
            }
        }

        if stopped {
            quotes.sort_by_key(|quote| Reverse(quote.amount_out));
            let finalist_count = heuristic.max_finalists.min(quotes.len());
            quotes.truncate(finalist_count);
            return Ok(HeuristicRoutesReport {
                quotes,
                stopped,
                failed_candidates: failures.len(),
            });
        }

        if quotes.is_empty() {
            if failures.is_empty() {
                return Err(SearchError::NoPath {
                    from: token_in,
                    to: token_out,
                });
            }
            return Err(SearchError::NoViableRoute {
                candidates: failures.len(),
                failures,
            });
        }

        quotes.sort_by_key(|quote| Reverse(quote.amount_out));
        let finalist_count = heuristic.max_finalists.min(quotes.len());
        quotes.truncate(finalist_count);

        if !heuristic.simulate_finalists {
            return Ok(HeuristicRoutesReport {
                quotes,
                stopped: false,
                failed_candidates: failures.len(),
            });
        }

        let paths = quotes
            .iter()
            .map(|quote| quote.path.clone())
            .collect::<Vec<_>>();
        let (finalist_quotes, mut finalist_failures) = quote_paths_with_registry(
            self.registry,
            paths,
            amount_in,
            sim_config,
            cache,
            quote_cache,
        );
        let failed_candidates = failures.len() + finalist_failures.len();
        failures.append(&mut finalist_failures);
        rank_or_no_viable(finalist_count, finalist_quotes, failures).map(|quotes| {
            HeuristicRoutesReport {
                quotes,
                stopped: false,
                failed_candidates,
            }
        })
    }

    fn fast_lane_paths(
        &self,
        token_in: Address,
        token_out: Address,
        config: &SearchConfig,
        heuristic: HeuristicSearchConfig,
        connector_tokens: Option<&HashSet<Address>>,
        quote_cache: &SharedQuoteCache,
        run_cache: &mut HeuristicRunCache,
    ) -> Vec<RoutePath> {
        let mut paths = Vec::new();
        let mut seen = HashSet::new();
        if config.max_hops == 0 {
            return paths;
        }

        if heuristic.fast_lane.evaluate_direct_best_first && config.min_hops <= 1 {
            for ranked in self
                .ranked_hops_between(
                    token_in,
                    token_out,
                    heuristic.edge_shortlist,
                    config.liquidity_pruning,
                    HeuristicPassMode::Initial,
                    quote_cache,
                )
                .into_iter()
                .take(heuristic.fast_lane.direct_edges_per_pair)
            {
                let path = RoutePath::from_hops(vec![ranked.hop]);
                if seen.insert(path.clone()) {
                    paths.push(path);
                }
            }
        }

        if config.max_hops >= 2 && config.min_hops <= 2 {
            for connector in self
                .ranked_central_connectors(token_in, token_out, connector_tokens, run_cache)
                .into_iter()
                .take(heuristic.fast_lane.max_connectors)
            {
                let first_hops = self
                    .ranked_hops_between(
                        token_in,
                        connector,
                        heuristic.edge_shortlist,
                        config.liquidity_pruning,
                        HeuristicPassMode::Initial,
                        quote_cache,
                    )
                    .into_iter()
                    .take(heuristic.fast_lane.connector_edges_per_pair)
                    .collect::<Vec<_>>();
                let second_hops = self
                    .ranked_hops_between(
                        connector,
                        token_out,
                        heuristic.edge_shortlist,
                        config.liquidity_pruning,
                        HeuristicPassMode::Initial,
                        quote_cache,
                    )
                    .into_iter()
                    .take(heuristic.fast_lane.connector_edges_per_pair)
                    .collect::<Vec<_>>();

                for first in &first_hops {
                    for second in &second_hops {
                        if first.hop.pool == second.hop.pool {
                            continue;
                        }
                        let path =
                            RoutePath::from_hops(vec![first.hop.clone(), second.hop.clone()]);
                        if seen.insert(path.clone()) {
                            paths.push(path);
                        }
                    }
                }
            }
        }

        paths
    }

    fn ranked_hops_between(
        &self,
        token_in: Address,
        token_out: Address,
        edge_shortlist: AdaptiveEdgeShortlistConfig,
        liquidity_config: LiquidityPruningConfig,
        pass_mode: HeuristicPassMode,
        quote_cache: &SharedQuoteCache,
    ) -> Vec<LiquidityRankedHop> {
        let Some(node) = self.graph.node_index(&token_in) else {
            return Vec::new();
        };
        let hops = self
            .graph
            .outgoing_edges(node)
            .iter()
            .filter_map(|edge| {
                (edge.token_out == token_out)
                    .then(|| Hop::new(edge.pool.clone(), edge.token_in, edge.token_out))
            })
            .collect::<Vec<_>>();
        self.liquidity_ranked_hops(
            hops,
            edge_shortlist,
            liquidity_config,
            pass_mode,
            quote_cache,
        )
    }

    fn ranked_central_connectors(
        &self,
        token_in: Address,
        token_out: Address,
        connector_tokens: Option<&HashSet<Address>>,
        run_cache: &mut HeuristicRunCache,
    ) -> Vec<Address> {
        let mut candidates = connector_tokens
            .map(|tokens| tokens.iter().copied().collect::<Vec<_>>())
            .unwrap_or_else(|| {
                self.auto_connector_tokens(
                    token_in,
                    token_out,
                    HeuristicSearchConfig::default(),
                    run_cache,
                )
                .into_iter()
                .collect()
            });
        candidates.retain(|token| *token != token_in && *token != token_out);
        candidates.sort_by_cached_key(|token| {
            let score = self.connector_liquidity_score_cached(*token, run_cache);
            (
                Reverse(score.known_balances),
                Reverse(score.degree),
                Reverse(score.log_sum),
                *token,
            )
        });
        candidates.dedup();
        candidates
    }

    fn connector_liquidity_score_cached(
        &self,
        token: Address,
        run_cache: &mut HeuristicRunCache,
    ) -> ConnectorScore {
        if let Some(score) = run_cache.connector_scores.get(&token).copied() {
            return score;
        }
        let score = self.connector_liquidity_score(token, run_cache);
        run_cache.connector_scores.insert(token, score);
        score
    }

    fn connector_liquidity_score(
        &self,
        token: Address,
        run_cache: &mut HeuristicRunCache,
    ) -> ConnectorScore {
        let Some(node) = self.graph.node_index(&token) else {
            return ConnectorScore::default();
        };
        let degree = self.token_degree_cached(node, run_cache);
        let Some(liquidity_index) = self.liquidity_index else {
            return ConnectorScore {
                degree,
                ..ConnectorScore::default()
            };
        };

        let mut seen = HashSet::new();
        let mut score = ConnectorScore {
            degree,
            ..ConnectorScore::default()
        };
        for edge in self
            .graph
            .graph()
            .edges_directed(node, Direction::Incoming)
            .chain(self.graph.graph().edges_directed(node, Direction::Outgoing))
        {
            let pool = edge.weight().pool.clone();
            if !seen.insert(pool.clone()) {
                continue;
            }
            if let Some(balance) = liquidity_index.balance_state(&pool, token).fresh() {
                score.known_balances += 1;
                score.log_sum += u256_log2(balance) as usize;
            }
        }
        score
    }

    #[allow(clippy::too_many_arguments)]
    fn group_heuristic_hops(
        &self,
        state: &HeuristicState,
        source: petgraph::graph::NodeIndex,
        target: petgraph::graph::NodeIndex,
        token_in: Address,
        token_out: Address,
        connector_tokens: Option<&HashSet<Address>>,
        max_hops: usize,
        heuristic: HeuristicSearchConfig,
        liquidity_config: LiquidityPruningConfig,
        quote_cache: &SharedQuoteCache,
        allow_final_source_revisit: bool,
        run_cache: &mut HeuristicRunCache,
    ) -> Vec<HeuristicHopGroup> {
        let mut grouped = HashMap::<Address, Vec<Hop>>::new();
        let remaining_after_hop = max_hops.saturating_sub(state.path.len() + 1);

        for edge in self.graph.outgoing_edges(state.node) {
            let next_node = edge.target;
            let next_token = edge.token_out;
            let current_token = edge.token_in;
            let pool = edge.pool.clone();

            if state.used_pools.contains(&pool) {
                continue;
            }

            let closes_target = next_node == target;
            let closes_source_cycle =
                allow_final_source_revisit && source == target && next_node == source;

            if state.visited_tokens.contains(&next_token) && !closes_source_cycle {
                continue;
            }

            if !closes_target
                && !connector_allowed_with_tokens(next_token, token_in, token_out, connector_tokens)
            {
                continue;
            }

            if !closes_target {
                if remaining_after_hop == 0 {
                    continue;
                }

                let mut visited_tokens = state.visited_tokens.clone();
                visited_tokens.insert(next_token);
                let mut used_pools = state.used_pools.clone();
                used_pools.insert(pool.clone());

                if !self.has_static_route_to_target(
                    next_node,
                    source,
                    target,
                    token_in,
                    token_out,
                    connector_tokens,
                    visited_tokens,
                    used_pools,
                    remaining_after_hop,
                    allow_final_source_revisit,
                ) {
                    continue;
                }
            }

            grouped
                .entry(next_token)
                .or_default()
                .push(Hop::new(pool, current_token, next_token));
        }

        self.sorted_heuristic_hop_groups(
            grouped,
            target,
            connector_tokens,
            heuristic,
            liquidity_config,
            quote_cache,
            run_cache,
        )
    }

    fn sorted_heuristic_hop_groups(
        &self,
        grouped: HashMap<Address, Vec<Hop>>,
        target: petgraph::graph::NodeIndex,
        connector_tokens: Option<&HashSet<Address>>,
        heuristic: HeuristicSearchConfig,
        liquidity_config: LiquidityPruningConfig,
        quote_cache: &SharedQuoteCache,
        run_cache: &mut HeuristicRunCache,
    ) -> Vec<HeuristicHopGroup> {
        let mut stats = LiquidityPruneStats::default();
        let mut groups = grouped
            .into_iter()
            .map(|(next_token, hops)| {
                let closes_target = self.graph.node_index(&next_token) == Some(target);
                if closes_target && heuristic.target_first {
                    stats.target_first_groups += 1;
                }
                let liquidity_score =
                    self.branch_liquidity_score(&hops, closes_target, liquidity_config, &mut stats);
                let connector_priority = usize::from(
                    connector_tokens.is_some_and(|tokens| tokens.contains(&next_token)),
                );
                let degree = self
                    .graph
                    .node_index(&next_token)
                    .map_or(0, |node| self.token_degree_cached(node, run_cache));
                (
                    HeuristicHopGroup {
                        next_token,
                        hops,
                        closes_target,
                    },
                    liquidity_score,
                    connector_priority,
                    degree,
                )
            })
            .collect::<Vec<_>>();

        groups.sort_by(|left, right| {
            let (left_group, left_liquidity, left_connector, left_degree) = left;
            let (right_group, right_liquidity, right_connector, right_degree) = right;

            if heuristic.target_first && left_group.closes_target != right_group.closes_target {
                return right_group.closes_target.cmp(&left_group.closes_target);
            }

            if liquidity_config.enabled
                && liquidity_config.rank_branches_by_liquidity
                && !left_group.closes_target
                && !right_group.closes_target
            {
                match (left_liquidity.is_known(), right_liquidity.is_known()) {
                    (true, true) => {
                        let ordering = right_liquidity
                            .max_input_balance
                            .cmp(&left_liquidity.max_input_balance)
                            .then_with(|| {
                                right_liquidity
                                    .sum_input_balance
                                    .cmp(&left_liquidity.sum_input_balance)
                            });
                        if !ordering.is_eq() {
                            return ordering;
                        }
                    }
                    (true, false) => return std::cmp::Ordering::Less,
                    (false, true) => return std::cmp::Ordering::Greater,
                    (false, false) => {}
                }
            }

            right_connector
                .cmp(left_connector)
                .then_with(|| right_degree.cmp(left_degree))
                .then_with(|| {
                    left_group
                        .next_token
                        .as_slice()
                        .cmp(right_group.next_token.as_slice())
                })
        });

        quote_cache.record_liquidity_prune_stats(stats);
        groups
            .into_iter()
            .map(|(group, _liquidity, _connector, _degree)| group)
            .collect()
    }

    fn branch_liquidity_score(
        &self,
        hops: &[Hop],
        closes_target: bool,
        config: LiquidityPruningConfig,
        stats: &mut LiquidityPruneStats,
    ) -> BranchLiquidityScore {
        if closes_target || !config.enabled || !config.rank_branches_by_liquidity {
            return BranchLiquidityScore::default();
        }

        let Some(liquidity_index) = self.liquidity_index else {
            return BranchLiquidityScore::default();
        };

        let mut score = BranchLiquidityScore::default();
        stats.balance_reads += hops.len();
        for hop in hops {
            match liquidity_index
                .balance_state(&hop.pool, hop.token_in)
                .fresh()
            {
                Some(balance) => {
                    score.max_input_balance = Some(
                        score
                            .max_input_balance
                            .map_or(balance, |max| max.max(balance)),
                    );
                    score.sum_input_balance =
                        Some(score.sum_input_balance.unwrap_or_default() + balance);
                }
                None => {
                    stats.stale_or_unknown_skipped_for_pruning += 1;
                }
            }
        }

        if score.is_known() {
            stats.liquidity_ranked_branch_groups += 1;
        } else {
            stats.liquidity_unknown_branch_groups += 1;
        }
        score
    }

    fn liquidity_ranked_hops(
        &self,
        mut hops: Vec<Hop>,
        edge_shortlist: AdaptiveEdgeShortlistConfig,
        config: LiquidityPruningConfig,
        _pass_mode: HeuristicPassMode,
        quote_cache: &SharedQuoteCache,
    ) -> Vec<LiquidityRankedHop> {
        let liquidity_active = config.enabled && self.liquidity_index.is_some() && hops.len() >= 2;
        let protocol_ordering = edge_shortlist.protocol_ordering && hops.len() >= 2;
        let mut stats = LiquidityPruneStats {
            ordered_groups: usize::from(liquidity_active && config.order_by_output_balance),
            balance_reads: if liquidity_active { hops.len() * 2 } else { 0 },
            protocol_ranked_edges: if protocol_ordering { hops.len() } else { 0 },
            ..LiquidityPruneStats::default()
        };

        let mut ranked = hops
            .drain(..)
            .map(|hop| {
                let input_balance = self
                    .liquidity_index
                    .filter(|_| liquidity_active)
                    .map_or(BalanceState::Unknown, |index| {
                        index.balance_state(&hop.pool, hop.token_in)
                    });
                let output_balance = self
                    .liquidity_index
                    .filter(|_| liquidity_active)
                    .map_or(BalanceState::Unknown, |index| {
                        index.balance_state(&hop.pool, hop.token_out)
                    });
                if liquidity_active
                    && (!matches!(input_balance, BalanceState::Fresh(_))
                        || !matches!(output_balance, BalanceState::Fresh(_)))
                {
                    stats.stale_or_unknown_skipped_for_pruning += 1;
                }
                let protocol_bonus = protocol_bonus(hop.pool.protocol());
                let fee_sort = self.fee_sort(&hop.pool);
                let rank_known =
                    input_balance.fresh().is_some() && output_balance.fresh().is_some();
                let liquidity_bucket = match (input_balance.fresh(), output_balance.fresh()) {
                    (Some(input), Some(output)) => u256_log2(input.min(output)),
                    _ => 0,
                };
                let rank_score = liquidity_bucket
                    .saturating_mul(100)
                    .saturating_add(if protocol_ordering { protocol_bonus } else { 0 });
                LiquidityRankedHop {
                    hop,
                    input_balance,
                    output_balance,
                    liquidity_active,
                    rank_known,
                    rank_score,
                    protocol_bonus,
                    fee_sort,
                }
            })
            .collect::<Vec<_>>();

        if protocol_ordering || (liquidity_active && config.order_by_output_balance) {
            ranked.sort_by(|left, right| {
                match (left.rank_known, right.rank_known) {
                    (true, false) => return std::cmp::Ordering::Less,
                    (false, true) => return std::cmp::Ordering::Greater,
                    (true, true) => {
                        let ordering = right
                            .rank_score
                            .cmp(&left.rank_score)
                            .then_with(|| {
                                right
                                    .output_balance
                                    .fresh()
                                    .cmp(&left.output_balance.fresh())
                            })
                            .then_with(|| {
                                right.input_balance.fresh().cmp(&left.input_balance.fresh())
                            })
                            .then_with(|| left.fee_sort.cmp(&right.fee_sort))
                            .then_with(|| hop_sort_key(&left.hop).cmp(&hop_sort_key(&right.hop)));
                        if !ordering.is_eq() {
                            return ordering;
                        }
                    }
                    (false, false) => {}
                }

                right
                    .protocol_bonus
                    .cmp(&left.protocol_bonus)
                    .then_with(|| left.fee_sort.cmp(&right.fee_sort))
                    .then_with(|| hop_sort_key(&left.hop).cmp(&hop_sort_key(&right.hop)))
            });
        }

        quote_cache.record_liquidity_prune_stats(stats);
        ranked
    }

    fn shortlisted_ranked_hops(
        &self,
        ranked_hops: Vec<LiquidityRankedHop>,
        heuristic: HeuristicSearchConfig,
        pass_mode: HeuristicPassMode,
        quote_cache: &SharedQuoteCache,
    ) -> Vec<LiquidityRankedHop> {
        if !heuristic.edge_shortlist.enabled || ranked_hops.len() <= 1 {
            return ranked_hops;
        }

        let limit = pass_mode.edge_limit(heuristic);
        let considered = ranked_hops.len().min(limit);
        let deferred = ranked_hops.len().saturating_sub(limit);
        quote_cache.record_liquidity_prune_stats(match pass_mode {
            HeuristicPassMode::Initial => LiquidityPruneStats {
                shortlist_initial_edges: considered,
                shortlist_deferred_edges: deferred,
                ..LiquidityPruneStats::default()
            },
            HeuristicPassMode::Refinement => LiquidityPruneStats {
                shortlist_refinement_edges: considered,
                ..LiquidityPruneStats::default()
            },
        });

        ranked_hops.into_iter().take(limit).collect()
    }

    fn should_prune_liquidity_hop(
        &self,
        ranked: &LiquidityRankedHop,
        best_quoted_output: Option<U256>,
        config: LiquidityPruningConfig,
        closes_target: bool,
        quote_cache: &SharedQuoteCache,
    ) -> bool {
        if !ranked.liquidity_active || !config.enabled || !config.prune_balance_dominated {
            return false;
        }
        let Some(best_quoted_output) = best_quoted_output else {
            return false;
        };
        let Some(balance) = ranked.output_balance.fresh() else {
            return false;
        };

        let can_apply_hard_prune = closes_target
            || self
                .liquidity_index
                .is_some_and(|index| index.is_two_token_pool(&ranked.hop.pool));
        if !can_apply_hard_prune || balance > best_quoted_output {
            return false;
        }

        quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
            pruned_edges: 1,
            ..LiquidityPruneStats::default()
        });
        true
    }

    fn fee_sort(&self, pool: &PoolKey) -> u64 {
        self.registry.pool(pool).map_or(u64::MAX, pool_fee_sort)
    }

    #[allow(clippy::too_many_arguments)]
    fn should_prune_upper_bound_prefix(
        &self,
        state: &HeuristicState,
        target: petgraph::graph::NodeIndex,
        target_token: Address,
        max_hops: usize,
        incumbent_amount: Option<U256>,
        config: UpperBoundPruningConfig,
        allow_final_source_revisit: bool,
        quote_cache: &SharedQuoteCache,
    ) -> bool {
        if allow_final_source_revisit
            || !config.enabled
            || !config.balance_cap_pruning
            || incumbent_amount.is_none()
            || state.node == target
            || state.path.len() >= max_hops
        {
            return false;
        }

        let Some(incumbent_amount) = incumbent_amount else {
            return false;
        };
        let remaining_hops = max_hops.saturating_sub(state.path.len());
        match self.target_balance_cap(state.node, target, target_token, remaining_hops) {
            UpperBoundCap::Known(cap) if cap <= incumbent_amount => {
                quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
                    upper_bound_pruned_prefixes: 1,
                    ..LiquidityPruneStats::default()
                });
                true
            }
            UpperBoundCap::Unknown if config.fail_open_on_unknown => {
                quote_cache.record_liquidity_prune_stats(LiquidityPruneStats {
                    upper_bound_unknown_prefixes: 1,
                    ..LiquidityPruneStats::default()
                });
                false
            }
            UpperBoundCap::Known(_) | UpperBoundCap::Unknown => false,
        }
    }

    fn target_balance_cap(
        &self,
        start: petgraph::graph::NodeIndex,
        target: petgraph::graph::NodeIndex,
        target_token: Address,
        max_hops: usize,
    ) -> UpperBoundCap {
        let Some(liquidity_index) = self.liquidity_index else {
            return UpperBoundCap::Unknown;
        };
        if max_hops == 0 {
            return UpperBoundCap::Unknown;
        }

        let mut cap = None;
        let mut queue = VecDeque::from([(start, 0_usize)]);
        let mut seen = HashSet::new();

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_hops || !seen.insert((node, depth)) {
                continue;
            }

            for edge in self.graph.outgoing_edges(node) {
                let next_node = edge.target;
                let next_depth = depth + 1;
                if next_depth > max_hops {
                    continue;
                }

                if next_node == target {
                    match liquidity_index
                        .balance_state(&edge.pool, target_token)
                        .fresh()
                    {
                        Some(balance) => {
                            cap = Some(cap.map_or(balance, |current: U256| current.max(balance)));
                        }
                        None => return UpperBoundCap::Unknown,
                    }
                } else if next_depth < max_hops {
                    queue.push_back((next_node, next_depth));
                }
            }
        }

        cap.map_or(UpperBoundCap::Unknown, UpperBoundCap::Known)
    }

    #[allow(clippy::too_many_arguments)]
    fn future_blocked_pools(
        &self,
        node: petgraph::graph::NodeIndex,
        source: petgraph::graph::NodeIndex,
        target: petgraph::graph::NodeIndex,
        token_in: Address,
        token_out: Address,
        connector_tokens: Option<&HashSet<Address>>,
        visited_tokens: &HashSet<Address>,
        used_pools: &HashSet<PoolKey>,
        allow_final_source_revisit: bool,
    ) -> HashSet<PoolKey> {
        let mut blocked = HashSet::new();
        for edge in self.graph.outgoing_edges(node) {
            let pool = &edge.pool;
            if !used_pools.contains(pool) {
                continue;
            }
            let next_node = edge.target;
            let next_token = edge.token_out;
            let closes_target = next_node == target;
            let closes_source_cycle =
                allow_final_source_revisit && source == target && next_node == source;
            if visited_tokens.contains(&next_token) && !closes_source_cycle {
                continue;
            }
            if !closes_target
                && !connector_allowed_with_tokens(next_token, token_in, token_out, connector_tokens)
            {
                continue;
            }
            blocked.insert(pool.clone());
        }
        blocked
    }

    #[allow(clippy::too_many_arguments)]
    fn has_static_route_to_target(
        &self,
        start: petgraph::graph::NodeIndex,
        source: petgraph::graph::NodeIndex,
        target: petgraph::graph::NodeIndex,
        token_in: Address,
        token_out: Address,
        connector_tokens: Option<&HashSet<Address>>,
        mut visited_tokens: HashSet<Address>,
        mut used_pools: HashSet<PoolKey>,
        max_hops: usize,
        allow_final_source_revisit: bool,
    ) -> bool {
        if start == target {
            return true;
        }
        if max_hops == 0 {
            return false;
        }

        self.has_static_route_to_target_dfs(
            start,
            source,
            target,
            token_in,
            token_out,
            connector_tokens,
            &mut visited_tokens,
            &mut used_pools,
            max_hops,
            allow_final_source_revisit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn has_static_route_to_target_dfs(
        &self,
        node: petgraph::graph::NodeIndex,
        source: petgraph::graph::NodeIndex,
        target: petgraph::graph::NodeIndex,
        token_in: Address,
        token_out: Address,
        connector_tokens: Option<&HashSet<Address>>,
        visited_tokens: &mut HashSet<Address>,
        used_pools: &mut HashSet<PoolKey>,
        remaining_hops: usize,
        allow_final_source_revisit: bool,
    ) -> bool {
        for edge in self.graph.outgoing_edges(node) {
            let next_node = edge.target;
            let next_token = edge.token_out;
            let pool = &edge.pool;

            if used_pools.contains(pool) {
                continue;
            }

            let closes_target = next_node == target;
            let closes_source_cycle =
                allow_final_source_revisit && source == target && next_node == source;

            if visited_tokens.contains(&next_token) && !closes_source_cycle {
                continue;
            }

            if !closes_target
                && !connector_allowed_with_tokens(next_token, token_in, token_out, connector_tokens)
            {
                continue;
            }

            if closes_target {
                return true;
            }
            if remaining_hops <= 1 {
                continue;
            }

            let inserted_token = visited_tokens.insert(next_token);
            let inserted_pool = used_pools.insert(pool.clone());
            debug_assert!(inserted_token);
            debug_assert!(inserted_pool);
            let found = self.has_static_route_to_target_dfs(
                next_node,
                source,
                target,
                token_in,
                token_out,
                connector_tokens,
                visited_tokens,
                used_pools,
                remaining_hops - 1,
                allow_final_source_revisit,
            );
            if inserted_pool {
                used_pools.remove(pool);
            }
            if inserted_token {
                visited_tokens.remove(&next_token);
            }
            if found {
                return true;
            }
        }

        false
    }

    fn connector_tokens_for_heuristic(
        &self,
        token_in: Address,
        token_out: Address,
        config: &SearchConfig,
        heuristic: HeuristicSearchConfig,
        run_cache: &mut HeuristicRunCache,
    ) -> Option<HashSet<Address>> {
        if let Some(tokens) = &config.connector_tokens {
            return Some(tokens.clone());
        }
        Some(self.auto_connector_tokens(token_in, token_out, heuristic, run_cache))
    }

    fn auto_connector_tokens(
        &self,
        token_in: Address,
        token_out: Address,
        heuristic: HeuristicSearchConfig,
        run_cache: &mut HeuristicRunCache,
    ) -> HashSet<Address> {
        if heuristic.max_auto_connectors == 0 {
            return HashSet::new();
        }

        let mut ranked = self
            .graph
            .graph()
            .node_indices()
            .filter_map(|node| {
                let token = self.graph.node_token(node)?;
                if token == token_in || token == token_out {
                    return None;
                }
                let degree = self.token_degree_cached(node, run_cache);
                (degree >= heuristic.min_auto_connector_degree).then_some((degree, token))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_degree, left_token), (right_degree, right_token)| {
            right_degree
                .cmp(left_degree)
                .then_with(|| left_token.as_slice().cmp(right_token.as_slice()))
        });

        ranked
            .into_iter()
            .take(heuristic.max_auto_connectors)
            .map(|(_degree, token)| token)
            .collect()
    }

    fn token_degree(&self, node: petgraph::graph::NodeIndex) -> usize {
        self.graph
            .graph()
            .edges_directed(node, Direction::Incoming)
            .count()
            + self
                .graph
                .graph()
                .edges_directed(node, Direction::Outgoing)
                .count()
    }

    fn token_degree_cached(
        &self,
        node: petgraph::graph::NodeIndex,
        run_cache: &mut HeuristicRunCache,
    ) -> usize {
        if let Some(degree) = run_cache.token_degrees.get(&node).copied() {
            return degree;
        }
        let degree = self.token_degree(node);
        run_cache.token_degrees.insert(node, degree);
        degree
    }

    fn quote_and_rank_parallel(
        &self,
        paths: Vec<RoutePath>,
        amount_in: U256,
        sim_config: &SimConfig,
        cache: &mut EvmCache,
        parallel_config: ParallelSearchConfig,
    ) -> Result<Vec<RouteQuote>, SearchError> {
        parallel_config.validate()?;

        let candidates = paths.len();
        let worker_count = parallel_config.workers.min(candidates.max(1));
        let chunks = split_paths(paths, worker_count);
        let quote_cache = self.new_quote_cache();

        if worker_count == 1 {
            let mut overlay_cache = self.new_overlay_cache(cache);
            let (quotes, failures) = quote_paths_with_registry(
                self.registry,
                chunks.into_iter().next().unwrap_or_default(),
                amount_in,
                sim_config,
                &mut overlay_cache,
                &quote_cache,
            );
            return rank_or_no_viable(candidates, quotes, failures);
        }

        let mut overlay_caches = Vec::with_capacity(chunks.len());
        for _ in 0..chunks.len() {
            overlay_caches.push(self.new_overlay_cache(cache));
        }

        let registry = self.registry;
        let sim_config = *sim_config;
        let worker_results = thread::scope(|scope| {
            let handles: Vec<_> = chunks
                .into_iter()
                .zip(overlay_caches)
                .map(|(chunk, mut overlay_cache)| {
                    let quote_cache = quote_cache.clone();
                    scope.spawn(move || {
                        quote_paths_with_registry(
                            registry,
                            chunk,
                            amount_in,
                            &sim_config,
                            &mut overlay_cache,
                            &quote_cache,
                        )
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|handle| handle.join())
                .collect::<Vec<_>>()
        });

        let mut quotes = Vec::new();
        let mut failures = Vec::new();
        for result in worker_results {
            let (mut worker_quotes, mut worker_failures) =
                result.map_err(|_| SearchError::WorkerPanic)?;
            quotes.append(&mut worker_quotes);
            failures.append(&mut worker_failures);
        }

        rank_or_no_viable(candidates, quotes, failures)
    }

    #[allow(clippy::too_many_arguments)]
    fn quote_and_stream_parallel(
        &self,
        paths: Vec<RoutePath>,
        amount_in: U256,
        sim_config: &SimConfig,
        overlays: &mut SearchOverlaySource<'_>,
        parallel_config: ParallelSearchConfig,
        quote_cache: &SharedQuoteCache,
        on_quote: &mut dyn FnMut(&RouteQuote) -> bool,
    ) -> Result<QuotePathsReport, SearchError> {
        parallel_config.validate()?;

        let candidates = paths.len();
        if candidates == 0 {
            return Ok(QuotePathsReport {
                quotes: Vec::new(),
                failures: Vec::new(),
                stopped: false,
            });
        }

        let worker_count = parallel_config.workers.min(candidates);
        let chunks = split_paths(paths, worker_count);

        if worker_count == 1 {
            let mut overlay_cache = overlays.overlay();
            return Ok(quote_paths_with_registry_observed(
                self.registry,
                chunks.into_iter().next().unwrap_or_default(),
                amount_in,
                sim_config,
                &mut overlay_cache,
                quote_cache,
                Some(on_quote),
                None,
            ));
        }

        let mut overlay_caches = Vec::with_capacity(chunks.len());
        for _ in 0..chunks.len() {
            overlay_caches.push(overlays.overlay());
        }

        let (tx, rx) = mpsc::channel::<StreamingWorkerMessage>();
        let cancelled = Arc::new(AtomicBool::new(false));
        let registry = self.registry;
        let sim_config = *sim_config;
        let worker_results = thread::scope(|scope| {
            let handles: Vec<_> = chunks
                .into_iter()
                .zip(overlay_caches)
                .map(|(chunk, mut overlay_cache)| {
                    let quote_cache = quote_cache.clone();
                    let tx = tx.clone();
                    let cancelled = Arc::clone(&cancelled);
                    scope.spawn(move || {
                        let mut on_worker_quote = |quote: &RouteQuote| {
                            if cancelled.load(Ordering::Relaxed) {
                                return false;
                            }
                            tx.send(StreamingWorkerMessage::Quote(quote.clone()))
                                .is_ok()
                                && !cancelled.load(Ordering::Relaxed)
                        };
                        quote_paths_with_registry_observed(
                            registry,
                            chunk,
                            amount_in,
                            &sim_config,
                            &mut overlay_cache,
                            &quote_cache,
                            Some(&mut on_worker_quote),
                            None,
                        )
                    })
                })
                .collect();
            drop(tx);

            for message in rx {
                match message {
                    StreamingWorkerMessage::Quote(quote) => {
                        if !on_quote(&quote) {
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }

            handles
                .into_iter()
                .map(|handle| handle.join())
                .collect::<Vec<_>>()
        });

        let mut quotes = Vec::new();
        let mut failures = Vec::new();
        let mut stopped = cancelled.load(Ordering::Relaxed);
        for result in worker_results {
            let mut worker_report = result.map_err(|_| SearchError::WorkerPanic)?;
            stopped |= worker_report.stopped;
            quotes.append(&mut worker_report.quotes);
            failures.append(&mut worker_report.failures);
        }

        Ok(QuotePathsReport {
            quotes,
            failures,
            stopped,
        })
    }

    fn enumerate_routes(
        &self,
        token_in: Address,
        token_out: Address,
        config: &SearchConfig,
        allow_final_source_revisit: bool,
    ) -> Result<Vec<RoutePath>, SearchError> {
        Ok(self
            .enumerate_routes_filtered(
                token_in,
                token_out,
                config,
                allow_final_source_revisit,
                &HashSet::new(),
                config.max_candidates,
                None,
            )?
            .paths)
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_routes_filtered(
        &self,
        token_in: Address,
        token_out: Address,
        config: &SearchConfig,
        allow_final_source_revisit: bool,
        excluded: &HashSet<RoutePath>,
        max_new_paths: Option<usize>,
        should_continue: Option<&dyn Fn() -> bool>,
    ) -> Result<RouteEnumeration, SearchError> {
        config.validate()?;

        let source = self
            .graph
            .node_index(&token_in)
            .ok_or(SearchError::TokenNotFound(token_in))?;
        let target = self
            .graph
            .node_index(&token_out)
            .ok_or(SearchError::TokenNotFound(token_out))?;

        if max_new_paths == Some(0) {
            return Ok(RouteEnumeration::default());
        }

        let mut paths = Vec::new();
        let mut queue = VecDeque::new();
        let mut duplicates_skipped = 0_usize;
        let mut found_path = false;
        let mut stopped = false;
        let mut visited = HashSet::new();
        visited.insert(token_in);
        queue.push_back(SearchState {
            node: source,
            path: RoutePath::new(),
            visited_tokens: visited,
            used_pools: HashSet::new(),
        });

        'search: while let Some(state) = queue.pop_front() {
            if should_continue.is_some_and(|should_continue| !should_continue()) {
                stopped = true;
                break;
            }
            if state.path.len() >= config.max_hops {
                continue;
            }

            for edge in self.graph.outgoing_edges(state.node) {
                if should_continue.is_some_and(|should_continue| !should_continue()) {
                    stopped = true;
                    break 'search;
                }
                let next_node = edge.target;
                let next_token = edge.token_out;
                let current_token = edge.token_in;
                let pool = edge.pool.clone();

                if state.used_pools.contains(&pool) {
                    continue;
                }

                let closes_target = next_node == target;
                let closes_source_cycle =
                    allow_final_source_revisit && source == target && next_node == source;

                if state.visited_tokens.contains(&next_token) && !closes_source_cycle {
                    continue;
                }

                if !closes_target && !connector_allowed(next_token, token_in, token_out, config) {
                    continue;
                }

                let mut path = state.path.clone();
                path.push(Hop::new(pool.clone(), current_token, next_token));

                if closes_target {
                    if path.len() >= config.min_hops {
                        found_path = true;
                        if excluded.contains(&path) {
                            duplicates_skipped = duplicates_skipped.saturating_add(1);
                        } else {
                            paths.push(path);
                        }
                        if max_new_paths.is_some_and(|limit| paths.len() >= limit) {
                            break 'search;
                        }
                    }
                    continue;
                }

                if path.len() < config.max_hops {
                    let mut visited_tokens = state.visited_tokens.clone();
                    visited_tokens.insert(next_token);
                    let mut used_pools = state.used_pools.clone();
                    used_pools.insert(pool);
                    queue.push_back(SearchState {
                        node: next_node,
                        path,
                        visited_tokens,
                        used_pools,
                    });
                }
            }
        }

        if !found_path && !stopped {
            return Err(SearchError::NoPath {
                from: token_in,
                to: token_out,
            });
        }

        Ok(RouteEnumeration {
            paths,
            duplicates_skipped,
            stopped,
        })
    }
}

fn quote_path_with_registry(
    registry: &AdapterRegistry,
    path: &RoutePath,
    amount_in: U256,
    cache: &mut dyn AdapterCache,
    sim_config: &SimConfig,
) -> Result<RouteQuote, SearchError> {
    if path.is_empty() {
        return Err(SearchError::EmptyPath);
    }

    let mut amount = amount_in;
    let mut hops = Vec::with_capacity(path.len());

    for hop in &path.hops {
        let pool = registry
            .pool(&hop.pool)
            .ok_or_else(|| SearchError::MissingPool(hop.pool.clone()))?;
        let adapter = registry
            .adapter(hop.pool.protocol())
            .ok_or_else(|| SearchError::MissingAdapter(hop.pool.protocol()))?;

        let quote = adapter
            .simulate_swap(pool, cache, hop.token_in, hop.token_out, amount, sim_config)
            .map_err(|source| SearchError::QuoteFailed {
                hop: hop.clone(),
                source: Box::new(source),
            })?;

        let amount_out = quote.amount_out;
        hops.push(HopQuote {
            hop: hop.clone(),
            amount_in: amount,
            amount_out,
        });
        amount = amount_out;
    }

    Ok(RouteQuote {
        path: path.clone(),
        amount_in,
        amount_out: amount,
        hops,
    })
}

fn quote_paths_with_registry(
    registry: &AdapterRegistry,
    paths: Vec<RoutePath>,
    amount_in: U256,
    sim_config: &SimConfig,
    cache: &mut dyn AdapterCache,
    quote_cache: &SharedQuoteCache,
) -> (Vec<RouteQuote>, Vec<PathFailure>) {
    let report = quote_paths_with_registry_observed(
        registry,
        paths,
        amount_in,
        sim_config,
        cache,
        quote_cache,
        None,
        None,
    );
    (report.quotes, report.failures)
}

struct QuotePathsReport {
    quotes: Vec<RouteQuote>,
    failures: Vec<PathFailure>,
    stopped: bool,
}

#[allow(clippy::too_many_arguments)]
fn quote_paths_with_registry_observed<'quote, 'failure>(
    registry: &AdapterRegistry,
    paths: Vec<RoutePath>,
    amount_in: U256,
    sim_config: &SimConfig,
    cache: &mut dyn AdapterCache,
    quote_cache: &SharedQuoteCache,
    on_quote: Option<&'quote mut dyn FnMut(&RouteQuote) -> bool>,
    on_failure: Option<&'failure mut dyn FnMut(&PathFailure) -> bool>,
) -> QuotePathsReport {
    let mut failures = Vec::new();
    let mut dag = QuoteDag::default();
    let mut on_failure = on_failure;
    let mut stopped = false;

    for path in paths {
        if path.is_empty() {
            let failure = PathFailure {
                path,
                reason: SearchError::EmptyPath.to_string(),
            };
            let keep_searching = on_failure
                .as_deref_mut()
                .is_none_or(|on_failure| on_failure(&failure));
            failures.push(failure);
            if !keep_searching {
                stopped = true;
                break;
            }
        } else {
            dag.insert(path);
        }
    }

    let mut evaluator = QuoteDagEvaluator {
        registry,
        sim_config,
        cache,
        quote_cache,
        amount_in,
        quotes: Vec::new(),
        failures,
        on_quote,
        on_failure,
        stopped,
    };
    let mut hop_quotes = Vec::new();
    if !evaluator.stopped {
        evaluator.evaluate(&dag.root, amount_in, &mut hop_quotes);
    }

    QuotePathsReport {
        quotes: evaluator.quotes,
        failures: evaluator.failures,
        stopped: evaluator.stopped,
    }
}

#[derive(Default)]
struct QuoteDag {
    root: QuoteDagNode,
}

impl QuoteDag {
    fn insert(&mut self, path: RoutePath) {
        let mut node = &mut self.root;

        for hop in &path.hops {
            node = node.child_for_hop(hop);
        }

        node.terminals.push(path);
    }
}

#[derive(Default)]
struct QuoteDagNode {
    children: Vec<QuoteDagChild>,
    terminals: Vec<RoutePath>,
}

impl QuoteDagNode {
    fn child_for_hop(&mut self, hop: &Hop) -> &mut QuoteDagNode {
        let index = self
            .children
            .iter()
            .position(|child| child.hop == *hop)
            .unwrap_or_else(|| {
                self.children.push(QuoteDagChild {
                    hop: hop.clone(),
                    node: QuoteDagNode::default(),
                });
                self.children.len() - 1
            });

        &mut self.children[index].node
    }

    fn collect_failures(&self, reason: &str, failures: &mut Vec<PathFailure>) {
        for path in &self.terminals {
            failures.push(PathFailure {
                path: path.clone(),
                reason: reason.to_string(),
            });
        }

        for child in &self.children {
            child.node.collect_failures(reason, failures);
        }
    }
}

struct QuoteDagChild {
    hop: Hop,
    node: QuoteDagNode,
}

struct QuoteDagEvaluator<'a, 'cache, 'quote, 'failure> {
    registry: &'a AdapterRegistry,
    sim_config: &'a SimConfig,
    cache: &'cache mut dyn AdapterCache,
    quote_cache: &'a SharedQuoteCache,
    amount_in: U256,
    quotes: Vec<RouteQuote>,
    failures: Vec<PathFailure>,
    on_quote: Option<&'quote mut dyn FnMut(&RouteQuote) -> bool>,
    on_failure: Option<&'failure mut dyn FnMut(&PathFailure) -> bool>,
    stopped: bool,
}

impl QuoteDagEvaluator<'_, '_, '_, '_> {
    fn evaluate(&mut self, node: &QuoteDagNode, amount: U256, hop_quotes: &mut Vec<HopQuote>) {
        for path in &node.terminals {
            if self.stopped {
                return;
            }

            let quote = RouteQuote {
                path: path.clone(),
                amount_in: self.amount_in,
                amount_out: amount,
                hops: hop_quotes.clone(),
            };
            let keep_searching = self
                .on_quote
                .as_deref_mut()
                .is_none_or(|on_quote| on_quote(&quote));
            self.quotes.push(quote);
            if !keep_searching {
                self.stopped = true;
                return;
            }
        }

        for child in &node.children {
            if self.stopped {
                return;
            }

            match self.quote_hop(&child.hop, amount) {
                Ok(amount_out) => {
                    hop_quotes.push(HopQuote {
                        hop: child.hop.clone(),
                        amount_in: amount,
                        amount_out,
                    });
                    self.evaluate(&child.node, amount_out, hop_quotes);
                    hop_quotes.pop();
                }
                Err(reason) => {
                    let mut failures = Vec::new();
                    child.node.collect_failures(&reason, &mut failures);
                    for failure in failures {
                        if !self.push_failure(failure) {
                            return;
                        }
                    }
                }
            }
        }
    }

    fn push_failure(&mut self, failure: PathFailure) -> bool {
        let keep_searching = self
            .on_failure
            .as_deref_mut()
            .is_none_or(|on_failure| on_failure(&failure));
        self.failures.push(failure);
        if !keep_searching {
            self.stopped = true;
            return false;
        }
        true
    }

    fn quote_hop(&mut self, hop: &Hop, amount_in: U256) -> Result<U256, String> {
        let key = self.quote_cache.key(hop, amount_in, self.sim_config)?;
        let registry = self.registry;
        let sim_config = self.sim_config;
        let cache = &mut *self.cache;

        self.quote_cache.get_or_quote(key, || {
            quote_hop_uncached(registry, hop, amount_in, cache, sim_config)
        })
    }
}

fn quote_hop_uncached(
    registry: &AdapterRegistry,
    hop: &Hop,
    amount_in: U256,
    cache: &mut dyn AdapterCache,
    sim_config: &SimConfig,
) -> Result<U256, String> {
    let pool = registry
        .pool(&hop.pool)
        .ok_or_else(|| SearchError::MissingPool(hop.pool.clone()).to_string())?;
    let adapter = registry
        .adapter(hop.pool.protocol())
        .ok_or_else(|| SearchError::MissingAdapter(hop.pool.protocol()).to_string())?;

    adapter
        .simulate_swap(
            pool,
            cache,
            hop.token_in,
            hop.token_out,
            amount_in,
            sim_config,
        )
        .map(|quote| quote.amount_out)
        .map_err(|source| {
            SearchError::QuoteFailed {
                hop: hop.clone(),
                source: Box::new(source),
            }
            .to_string()
        })
}

fn quote_hop_with_cache(
    registry: &AdapterRegistry,
    hop: &Hop,
    amount_in: U256,
    cache: &mut dyn AdapterCache,
    sim_config: &SimConfig,
    quote_cache: &SharedQuoteCache,
) -> Result<U256, String> {
    let key = quote_cache.key(hop, amount_in, sim_config)?;

    quote_cache.get_or_quote(key, || {
        quote_hop_uncached(registry, hop, amount_in, cache, sim_config)
    })
}

#[derive(Clone, Default)]
enum QuoteKeyContext {
    #[default]
    Static,
    #[cfg(feature = "live-runtime")]
    Live {
        point: AmmStatePoint,
        registry: Arc<AdapterRegistrySnapshot>,
        revisions: Arc<PoolRevisionMap>,
    },
    #[cfg(all(test, feature = "live-runtime"))]
    FixedLive {
        point: AmmStatePoint,
        pools: Arc<HashMap<PoolKey, PoolStateRef>>,
    },
}

#[cfg(feature = "live-runtime")]
fn live_quote_context(snapshot: &AmmStateSnapshot) -> QuoteKeyContext {
    QuoteKeyContext::Live {
        point: snapshot.point(),
        registry: snapshot.registry_snapshot(),
        revisions: snapshot.pool_revisions_snapshot(),
    }
}

impl QuoteKeyContext {
    const fn point(&self) -> Option<AmmStatePoint> {
        match self {
            Self::Static => None,
            #[cfg(feature = "live-runtime")]
            Self::Live { point, .. } => Some(*point),
            #[cfg(all(test, feature = "live-runtime"))]
            Self::FixedLive { point, .. } => Some(*point),
        }
    }

    fn pool_scope(&self, pool: &PoolKey) -> Result<QuotePoolScope, String> {
        match self {
            Self::Static => Ok(QuotePoolScope::Static(pool.clone())),
            #[cfg(feature = "live-runtime")]
            Self::Live {
                point,
                registry,
                revisions,
            } => registry
                .pool_instance(pool)
                .and_then(|instance| {
                    revisions
                        .get(instance)
                        .map(|revision| PoolStateRef::new(instance.clone(), *revision, *point))
                })
                .map(QuotePoolScope::Versioned)
                .ok_or_else(|| format!("live quote context is missing pool state ref: {pool:?}")),
            #[cfg(all(test, feature = "live-runtime"))]
            Self::FixedLive { pools, .. } => pools
                .get(pool)
                .cloned()
                .map(QuotePoolScope::Versioned)
                .ok_or_else(|| format!("live quote context is missing pool state ref: {pool:?}")),
        }
    }
}

#[derive(Clone)]
struct SharedQuoteCache {
    inner: Arc<SharedQuoteCacheInner>,
    context: Arc<Mutex<QuoteKeyContext>>,
}

impl Default for SharedQuoteCache {
    fn default() -> Self {
        Self::with_context(QuoteKeyContext::Static)
    }
}

#[derive(Default)]
struct SharedQuoteCacheInner {
    entries: Mutex<HashMap<HopQuoteKey, QuoteCacheState>>,
    keys_by_pool: Mutex<HashMap<PoolKey, HashSet<HopQuoteKey>>>,
    ready: Condvar,
    stats: AtomicQuoteCacheStats,
    liquidity_pruning: Mutex<LiquidityPruneStats>,
}

impl SharedQuoteCache {
    fn with_context(context: QuoteKeyContext) -> Self {
        Self {
            inner: Arc::new(SharedQuoteCacheInner::default()),
            context: Arc::new(Mutex::new(context)),
        }
    }

    fn set_context(&self, context: QuoteKeyContext) {
        *self.context.lock().expect("quote context poisoned") = context;
    }

    fn key(
        &self,
        hop: &Hop,
        amount_in: U256,
        sim_config: &SimConfig,
    ) -> Result<HopQuoteKey, String> {
        Ok(HopQuoteKey {
            pool: self
                .context
                .lock()
                .expect("quote context poisoned")
                .pool_scope(&hop.pool)?,
            token_in: hop.token_in,
            token_out: hop.token_out,
            amount_in,
            sim_config: SimConfigKey::from(sim_config),
        })
    }

    fn stats(&self) -> QuoteCacheStats {
        self.inner.stats.snapshot()
    }

    fn liquidity_prune_stats(&self) -> LiquidityPruneStats {
        *self
            .inner
            .liquidity_pruning
            .lock()
            .expect("liquidity stats poisoned")
    }

    fn record_liquidity_prune_stats(&self, stats: LiquidityPruneStats) {
        if stats == LiquidityPruneStats::default() {
            return;
        }
        self.inner
            .liquidity_pruning
            .lock()
            .expect("liquidity stats poisoned")
            .merge(stats);
    }

    fn invalidate_pools(&self, pools: &HashSet<PoolKey>) {
        if pools.is_empty() {
            return;
        }
        let mut entries = self.inner.entries.lock().expect("quote cache poisoned");
        let mut keys_by_pool = self
            .inner
            .keys_by_pool
            .lock()
            .expect("quote cache pool index poisoned");
        for pool in pools {
            if let Some(keys) = keys_by_pool.remove(pool) {
                for key in keys {
                    entries.remove(&key);
                }
            }
        }
        self.inner.ready.notify_all();
    }

    fn get_or_quote(
        &self,
        key: HopQuoteKey,
        quote: impl FnOnce() -> Result<U256, String>,
    ) -> Result<U256, String> {
        {
            let mut entries = self.inner.entries.lock().expect("quote cache poisoned");
            loop {
                match entries.get(&key) {
                    Some(QuoteCacheState::Ready(entry)) => {
                        self.inner.stats.hits.fetch_add(1, Ordering::Relaxed);
                        return entry.clone().into_result();
                    }
                    Some(QuoteCacheState::InFlight) => {
                        self.inner.stats.waits.fetch_add(1, Ordering::Relaxed);
                        entries = self
                            .inner
                            .ready
                            .wait(entries)
                            .expect("quote cache poisoned");
                    }
                    None => {
                        self.inner.stats.misses.fetch_add(1, Ordering::Relaxed);
                        entries.insert(key.clone(), QuoteCacheState::InFlight);
                        self.inner
                            .keys_by_pool
                            .lock()
                            .expect("quote cache pool index poisoned")
                            .entry(key.logical_pool().clone())
                            .or_default()
                            .insert(key.clone());
                        break;
                    }
                }
            }
        }

        let mut guard = InFlightQuoteGuard::new(self, key.clone());
        let entry = match quote() {
            Ok(amount_out) => QuoteCacheEntry::Success(amount_out),
            Err(reason) => QuoteCacheEntry::Failure(reason),
        };
        self.inner.stats.executed.fetch_add(1, Ordering::Relaxed);
        if matches!(entry, QuoteCacheEntry::Failure(_)) {
            self.inner.stats.failed.fetch_add(1, Ordering::Relaxed);
        }

        {
            let mut entries = self.inner.entries.lock().expect("quote cache poisoned");
            if matches!(entries.get(&key), Some(QuoteCacheState::InFlight)) {
                entries.insert(key, QuoteCacheState::Ready(entry.clone()));
            }
            self.inner.ready.notify_all();
        }
        guard.complete();

        entry.into_result()
    }

    #[cfg(all(test, feature = "live-runtime"))]
    fn entry_count(&self) -> usize {
        self.inner
            .entries
            .lock()
            .expect("quote cache poisoned")
            .len()
    }
}

#[derive(Default)]
struct AtomicQuoteCacheStats {
    hits: AtomicUsize,
    misses: AtomicUsize,
    waits: AtomicUsize,
    executed: AtomicUsize,
    failed: AtomicUsize,
}

impl AtomicQuoteCacheStats {
    fn snapshot(&self) -> QuoteCacheStats {
        QuoteCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            executed: self.executed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

struct InFlightQuoteGuard<'a> {
    cache: &'a SharedQuoteCache,
    key: Option<HopQuoteKey>,
}

impl<'a> InFlightQuoteGuard<'a> {
    fn new(cache: &'a SharedQuoteCache, key: HopQuoteKey) -> Self {
        Self {
            cache,
            key: Some(key),
        }
    }

    fn complete(&mut self) {
        self.key = None;
    }
}

impl Drop for InFlightQuoteGuard<'_> {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };

        let mut entries = self
            .cache
            .inner
            .entries
            .lock()
            .expect("quote cache poisoned");
        entries.remove(&key);
        remove_quote_key_from_pool_index(&self.cache.inner, &key);
        self.cache.inner.ready.notify_all();
    }
}

fn remove_quote_key_from_pool_index(inner: &SharedQuoteCacheInner, key: &HopQuoteKey) {
    let pool = key.logical_pool();
    let mut keys_by_pool = inner
        .keys_by_pool
        .lock()
        .expect("quote cache pool index poisoned");
    let remove_pool = keys_by_pool.get_mut(pool).is_some_and(|keys| {
        keys.remove(key);
        keys.is_empty()
    });
    if remove_pool {
        keys_by_pool.remove(pool);
    }
}

#[derive(Clone, Debug)]
enum QuoteCacheState {
    InFlight,
    Ready(QuoteCacheEntry),
}

#[derive(Clone, Debug)]
enum QuoteCacheEntry {
    Success(U256),
    Failure(String),
}

impl QuoteCacheEntry {
    fn into_result(self) -> Result<U256, String> {
        match self {
            Self::Success(amount_out) => Ok(amount_out),
            Self::Failure(reason) => Err(reason),
        }
    }
}

fn rank_or_no_viable(
    candidates: usize,
    mut quotes: Vec<RouteQuote>,
    failures: Vec<PathFailure>,
) -> Result<Vec<RouteQuote>, SearchError> {
    quotes.sort_by_key(|quote| Reverse(quote.amount_out));

    if quotes.is_empty() {
        return Err(SearchError::NoViableRoute {
            candidates,
            failures,
        });
    }

    Ok(quotes)
}

fn split_paths(paths: Vec<RoutePath>, workers: usize) -> Vec<Vec<RoutePath>> {
    let mut chunks = vec![Vec::new(); workers];
    for (index, path) in paths.into_iter().enumerate() {
        chunks[index % workers].push(path);
    }
    chunks.retain(|chunk| !chunk.is_empty());
    chunks
}

fn split_indexed_items<T: Clone>(items: &[T], workers: usize) -> Vec<Vec<(usize, T)>> {
    let mut chunks: Vec<Vec<(usize, T)>> = (0..workers).map(|_| Vec::new()).collect();
    for (index, item) in items.iter().cloned().enumerate() {
        chunks[index % workers].push((index, item));
    }
    chunks.retain(|chunk| !chunk.is_empty());
    chunks
}

fn find_route_request_chunk(
    registry: &AdapterRegistry,
    graph: &AmmGraph,
    liquidity_index: Option<&PoolLiquidityIndex>,
    chunk: Vec<(usize, RouteRequest)>,
    mut cache: OverlayAdapterCache,
    quote_cache: SharedQuoteCache,
) -> Vec<(usize, Result<Vec<RouteQuote>, SearchError>)> {
    let mut searcher = AmmSearcher::new(registry, graph);
    if let Some(liquidity_index) = liquidity_index {
        searcher = searcher.with_liquidity_index(liquidity_index);
    }
    chunk
        .into_iter()
        .map(|(index, request)| {
            (
                index,
                searcher.find_routes_with_quote_cache(&request, &mut cache, &quote_cache),
            )
        })
        .collect()
}

fn find_cycle_request_chunk(
    registry: &AdapterRegistry,
    graph: &AmmGraph,
    liquidity_index: Option<&PoolLiquidityIndex>,
    chunk: Vec<(usize, CycleRequest)>,
    mut cache: OverlayAdapterCache,
    quote_cache: SharedQuoteCache,
) -> Vec<(usize, Result<Vec<CycleQuote>, SearchError>)> {
    let mut searcher = AmmSearcher::new(registry, graph);
    if let Some(liquidity_index) = liquidity_index {
        searcher = searcher.with_liquidity_index(liquidity_index);
    }
    chunk
        .into_iter()
        .map(|(index, request)| {
            (
                index,
                searcher.find_cycles_with_quote_cache(&request, &mut cache, &quote_cache),
            )
        })
        .collect()
}

fn collect_ordered_results<T>(mut results: Vec<(usize, T)>, expected_len: usize) -> Vec<T> {
    results.sort_by_key(|(index, _)| *index);
    debug_assert_eq!(results.len(), expected_len);
    results
        .into_iter()
        .map(|(_, result)| result)
        .collect::<Vec<_>>()
}

#[derive(Clone, Debug)]
struct SearchState {
    node: petgraph::graph::NodeIndex,
    path: RoutePath,
    visited_tokens: HashSet<Address>,
    used_pools: HashSet<PoolKey>,
}

#[derive(Debug, Default)]
struct RouteEnumeration {
    paths: Vec<RoutePath>,
    duplicates_skipped: usize,
    stopped: bool,
}

#[derive(Clone, Debug)]
struct HeuristicState {
    node: petgraph::graph::NodeIndex,
    path: RoutePath,
    hop_quotes: Vec<HopQuote>,
    amount: U256,
    visited_tokens: HashSet<Address>,
    used_pools: HashSet<PoolKey>,
}

#[derive(Clone, Debug)]
struct HeuristicHopGroup {
    next_token: Address,
    hops: Vec<Hop>,
    closes_target: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BranchLiquidityScore {
    max_input_balance: Option<U256>,
    sum_input_balance: Option<U256>,
}

impl BranchLiquidityScore {
    const fn is_known(self) -> bool {
        self.max_input_balance.is_some()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ConnectorScore {
    known_balances: usize,
    degree: usize,
    log_sum: usize,
}

#[derive(Debug, Default)]
struct HeuristicRunCache {
    token_degrees: HashMap<petgraph::graph::NodeIndex, usize>,
    connector_scores: HashMap<Address, ConnectorScore>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpperBoundCap {
    Known(U256),
    Unknown,
}

#[derive(Clone, Debug, Default)]
struct PrefixDominanceIndex {
    labels: HashMap<Address, Vec<PrefixLabel>>,
}

#[derive(Clone, Debug)]
struct PrefixLabel {
    amount: U256,
    visited_tokens: HashSet<Address>,
    used_pools: HashSet<PoolKey>,
}

impl PrefixDominanceIndex {
    fn insert_or_dominated(
        &mut self,
        token: Address,
        amount: U256,
        visited_tokens: &HashSet<Address>,
        used_pools: &HashSet<PoolKey>,
    ) -> bool {
        let labels = self.labels.entry(token).or_default();
        if labels.iter().any(|label| {
            label.amount >= amount
                && label.visited_tokens.is_subset(visited_tokens)
                && label.used_pools.is_subset(used_pools)
        }) {
            return true;
        }

        labels.retain(|label| {
            !(amount >= label.amount
                && visited_tokens.is_subset(&label.visited_tokens)
                && used_pools.is_subset(&label.used_pools))
        });
        labels.push(PrefixLabel {
            amount,
            visited_tokens: visited_tokens.clone(),
            used_pools: used_pools.clone(),
        });
        false
    }

    fn is_strictly_dominated(
        &self,
        token: Address,
        amount: U256,
        visited_tokens: &HashSet<Address>,
        used_pools: &HashSet<PoolKey>,
    ) -> bool {
        let Some(labels) = self.labels.get(&token) else {
            return false;
        };
        labels.iter().any(|label| {
            label.amount >= amount
                && label.visited_tokens.is_subset(visited_tokens)
                && label.used_pools.is_subset(used_pools)
                && (label.amount > amount
                    || label.visited_tokens.len() < visited_tokens.len()
                    || label.used_pools.len() < used_pools.len())
        })
    }
}

#[derive(Clone, Debug)]
struct HeuristicRoutesReport {
    quotes: Vec<RouteQuote>,
    stopped: bool,
    failed_candidates: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeuristicRunLimit {
    FastLaneOnly,
    Full,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeuristicPassMode {
    Initial,
    Refinement,
}

impl HeuristicPassMode {
    const fn edge_limit(self, heuristic: HeuristicSearchConfig) -> usize {
        if !heuristic.edge_shortlist.enabled {
            return usize::MAX;
        }
        match self {
            Self::Initial => heuristic.edge_shortlist.initial_edges_per_pair,
            Self::Refinement => heuristic.edge_shortlist.refinement_edges_per_pair,
        }
    }
}

fn heuristic_pass_modes(heuristic: HeuristicSearchConfig) -> Vec<HeuristicPassMode> {
    if heuristic.edge_shortlist.enabled
        && heuristic.edge_shortlist.refine_parallel_edges
        && heuristic.edge_shortlist.refinement_edges_per_pair
            > heuristic.edge_shortlist.initial_edges_per_pair
    {
        vec![HeuristicPassMode::Initial, HeuristicPassMode::Refinement]
    } else {
        vec![HeuristicPassMode::Initial]
    }
}

struct StreamingRunReport {
    report: StreamingSearchReport,
    quotes: Vec<RouteQuote>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RouteSessionEntry {
    Quoted(RouteQuote),
    Failed,
}

impl RouteSessionEntry {
    fn quote(&self) -> Option<&RouteQuote> {
        match self {
            Self::Quoted(quote) => Some(quote),
            Self::Failed => None,
        }
    }
}

struct StreamingRouteState {
    config: StreamingSearchConfig,
    best: Option<RouteQuote>,
    heuristic_best: Option<RouteQuote>,
    top_routes: Vec<RouteQuote>,
    routes_observed: usize,
    failed_candidates: usize,
    duplicate_paths_skipped: usize,
    improvements_after_heuristic: usize,
    heuristic_phase_completed: bool,
    progress_phase: Option<RouteSearchPhase>,
    total_candidates: Option<usize>,
    initial_results_released: bool,
    stop_finality: Option<SearchFinality>,
}

impl StreamingRouteState {
    fn new(config: StreamingSearchConfig) -> Self {
        Self {
            config,
            best: None,
            heuristic_best: None,
            top_routes: Vec::new(),
            routes_observed: 0,
            failed_candidates: 0,
            duplicate_paths_skipped: 0,
            improvements_after_heuristic: 0,
            heuristic_phase_completed: false,
            progress_phase: None,
            total_candidates: None,
            initial_results_released: config.initial_result_policy.is_none(),
            stop_finality: None,
        }
    }

    fn observe_quote(
        &mut self,
        phase: RouteSearchPhase,
        quote: RouteQuote,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> bool {
        let results_were_released = self.initial_results_released;
        self.progress_phase = Some(phase);
        self.routes_observed += 1;

        let top_rank = self.record_top_route(quote.clone());
        let improves_best = self
            .best
            .as_ref()
            .is_none_or(|best| quote.amount_out > best.amount_out);

        if improves_best {
            if phase == RouteSearchPhase::Exhaustive && self.heuristic_best.is_some() {
                self.improvements_after_heuristic += 1;
            }
            let previous_best = self.best.replace(quote.clone());
            if !self.emit_progress_and_apply_policies(phase, on_event) {
                return false;
            }
            if !results_were_released || self.just_released_initial_results(results_were_released) {
                return true;
            }
            if !emit_search_event(
                on_event,
                RouteSearchEvent::BestUpdated {
                    phase,
                    quote: quote.clone(),
                    previous_best,
                    status: StreamedRouteStatus::Provisional,
                },
            ) {
                self.stop_finality = Some(SearchFinality::Stopped);
                return false;
            }
        } else if !self.emit_progress_and_apply_policies(phase, on_event) {
            return false;
        } else if !results_were_released
            || self.just_released_initial_results(results_were_released)
        {
            return true;
        }

        if (self.config.emit_all_viable || top_rank.is_some())
            && !emit_search_event(
                on_event,
                RouteSearchEvent::RouteFound {
                    phase,
                    rank: top_rank,
                    quote,
                },
            )
        {
            self.stop_finality = Some(SearchFinality::Stopped);
            return false;
        }

        true
    }

    fn observe_failed_candidates(
        &mut self,
        phase: RouteSearchPhase,
        count: usize,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> bool {
        if count == 0 {
            return true;
        }
        self.progress_phase = Some(phase);
        self.failed_candidates += count;
        self.emit_progress_and_apply_policies(phase, on_event)
    }

    fn finish_heuristic_phase(
        &mut self,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> bool {
        self.heuristic_best = self.best.clone();
        self.heuristic_phase_completed = true;
        self.progress_phase = Some(RouteSearchPhase::Heuristic);
        self.emit_progress_and_apply_policies(RouteSearchPhase::Heuristic, on_event)
    }

    fn set_total_candidates(
        &mut self,
        total_candidates: usize,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> bool {
        self.total_candidates = Some(total_candidates.max(self.candidates_evaluated()));
        self.progress_phase = Some(RouteSearchPhase::Exhaustive);
        self.emit_progress_and_apply_policies(RouteSearchPhase::Exhaustive, on_event)
    }

    fn mark_exhaustive_complete(&mut self) {
        self.progress_phase = Some(RouteSearchPhase::Exhaustive);
    }

    fn stop_finality(&self) -> SearchFinality {
        self.stop_finality.unwrap_or(SearchFinality::Stopped)
    }

    fn should_emit_completion_for_stop(&self) -> bool {
        self.stop_finality == Some(SearchFinality::StopPolicySatisfied)
    }

    fn record_top_route(&mut self, quote: RouteQuote) -> Option<usize> {
        if self
            .top_routes
            .iter()
            .any(|existing| existing.path == quote.path)
        {
            return None;
        }

        let path = quote.path.clone();
        self.top_routes.push(quote);
        self.top_routes
            .sort_by_key(|quote| Reverse(quote.amount_out));
        let rank = self
            .top_routes
            .iter()
            .position(|candidate| candidate.path == path)
            .map(|index| index + 1);
        if self.top_routes.len() > self.config.top_k {
            self.top_routes.truncate(self.config.top_k);
        }

        rank.filter(|rank| *rank <= self.config.top_k)
    }

    fn candidates_evaluated(&self) -> usize {
        self.routes_observed + self.failed_candidates
    }

    fn progress(&self) -> RouteSearchProgress {
        let candidates_evaluated = self.candidates_evaluated();
        let exhaustive_fraction_bps = self.total_candidates.and_then(|total| {
            if total == 0 {
                return Some(10_000);
            }
            let capped = candidates_evaluated.min(total);
            let bps = capped.saturating_mul(10_000) / total;
            u16::try_from(bps.min(10_000)).ok()
        });
        let confidence_bps = self.confidence_bps(exhaustive_fraction_bps);

        RouteSearchProgress {
            phase: self.progress_phase,
            candidates_evaluated,
            viable_routes_observed: self.routes_observed,
            failed_candidates: self.failed_candidates,
            duplicate_paths_skipped: self.duplicate_paths_skipped,
            total_candidates: self.total_candidates,
            exhaustive_fraction_bps,
            confidence_bps,
            best_amount_out: self.best.as_ref().map(|quote| quote.amount_out),
        }
    }

    fn final_progress(&self, finality: SearchFinality) -> RouteSearchProgress {
        let mut progress = self.progress();
        if finality == SearchFinality::Exhaustive {
            progress.exhaustive_fraction_bps = Some(10_000);
            progress.confidence_bps = if self.best.is_some() { 10_000 } else { 0 };
        }
        progress
    }

    fn confidence_bps(&self, exhaustive_fraction_bps: Option<u16>) -> u16 {
        if self.best.is_none() {
            return 0;
        }
        if self
            .total_candidates
            .is_some_and(|total| total > 0 && self.candidates_evaluated() >= total)
        {
            return 9_999;
        }
        if let Some(fraction) = exhaustive_fraction_bps {
            return (9_000 + (fraction / 10)).min(9_999);
        }
        if self.heuristic_phase_completed {
            return 9_000;
        }
        7_000
    }

    fn emit_progress_and_apply_policies(
        &mut self,
        phase: RouteSearchPhase,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> bool {
        self.progress_phase = Some(phase);
        let progress = self.progress();
        if !emit_search_event(
            on_event,
            RouteSearchEvent::Progress {
                progress: progress.clone(),
            },
        ) {
            self.stop_finality = Some(SearchFinality::Stopped);
            return false;
        }

        if !self.initial_results_released
            && self.best.is_some()
            && self
                .config
                .initial_result_policy
                .is_some_and(|policy| policy.is_satisfied(&progress))
            && !self.release_initial_results(progress.clone(), on_event)
        {
            return false;
        }

        if self.best.is_some()
            && self
                .config
                .stop_policy
                .is_some_and(|policy| policy.is_satisfied(&progress))
        {
            self.stop_finality = Some(SearchFinality::StopPolicySatisfied);
            return false;
        }

        true
    }

    fn release_initial_results(
        &mut self,
        progress: RouteSearchProgress,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> bool {
        let Some(best) = self.best.clone() else {
            return true;
        };
        self.initial_results_released = true;
        if !emit_search_event(
            on_event,
            RouteSearchEvent::InitialResultsReady {
                progress,
                best,
                top_routes: self.top_routes.clone(),
            },
        ) {
            self.stop_finality = Some(SearchFinality::Stopped);
            return false;
        }
        true
    }

    fn release_initial_results_at_completion(
        &mut self,
        finality: SearchFinality,
        on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    ) -> bool {
        if self.initial_results_released || self.config.initial_result_policy.is_none() {
            return true;
        }
        let progress = self.final_progress(finality);
        self.release_initial_results(progress, on_event)
    }

    fn just_released_initial_results(&self, released_before: bool) -> bool {
        !released_before && self.initial_results_released
    }

    fn phase_stats(
        &self,
        quote_cache: QuoteCacheStats,
        liquidity_pruning: LiquidityPruneStats,
    ) -> StreamingPhaseStats {
        StreamingPhaseStats {
            routes_observed: self.routes_observed,
            duplicate_paths_skipped: self.duplicate_paths_skipped,
            quote_cache,
            liquidity_pruning,
        }
    }

    fn report(
        &self,
        finality: SearchFinality,
        quote_cache: QuoteCacheStats,
        liquidity_pruning: LiquidityPruneStats,
    ) -> StreamingSearchReport {
        let heuristic_was_final_best = if finality == SearchFinality::Exhaustive {
            self.best
                .as_ref()
                .map(|best| self.heuristic_best.as_ref() == Some(best))
        } else {
            None
        };

        StreamingSearchReport {
            best: self.best.clone(),
            top_routes: self.top_routes.clone(),
            heuristic_best: self.heuristic_best.clone(),
            finality,
            heuristic_was_final_best,
            improvements_after_heuristic: self.improvements_after_heuristic,
            routes_observed: self.routes_observed,
            duplicate_paths_skipped: self.duplicate_paths_skipped,
            quote_cache,
            liquidity_pruning,
            progress: self.final_progress(finality),
            initial_results_released: self.initial_results_released,
        }
    }
}

fn finish_stream_report(
    stream_state: &mut StreamingRouteState,
    finality: SearchFinality,
    quote_cache: &SharedQuoteCache,
    on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    emit_completed: bool,
) -> StreamingSearchReport {
    let mut finality = finality;
    if !stream_state.release_initial_results_at_completion(finality, on_event) {
        finality = SearchFinality::Stopped;
    }
    let report = stream_state.report(
        finality,
        quote_cache.stats(),
        quote_cache.liquidity_prune_stats(),
    );
    if emit_completed {
        emit_search_event(
            on_event,
            RouteSearchEvent::Completed {
                report: report.clone(),
            },
        );
    }
    report
}

fn emit_search_event(
    on_event: &mut impl FnMut(RouteSearchEvent) -> SearchControl,
    event: RouteSearchEvent,
) -> bool {
    on_event(event) == SearchControl::Continue
}

fn emit_route_update_event(
    on_event: &mut impl FnMut(RouteUpdateEvent) -> SearchControl,
    event: RouteUpdateEvent,
) -> bool {
    on_event(event) == SearchControl::Continue
}

fn index_route_path(path: &RoutePath, pool_to_routes: &mut HashMap<PoolKey, HashSet<RoutePath>>) {
    for hop in &path.hops {
        pool_to_routes
            .entry(hop.pool.clone())
            .or_default()
            .insert(path.clone());
    }
}

fn index_probe_paths_for_route(
    searcher: &AmmSearcher<'_>,
    path: &RoutePath,
    parallel_probe_index: &mut HashMap<PoolKey, HashSet<RoutePath>>,
) {
    for (hop_index, hop) in path.hops.iter().enumerate() {
        let Some(node) = searcher.graph.node_index(&hop.token_in) else {
            continue;
        };
        for edge in searcher.graph.outgoing_edges(node) {
            if edge.token_out != hop.token_out {
                continue;
            }
            let replacement_pool = edge.pool.clone();
            if replacement_pool == hop.pool {
                continue;
            }
            if path
                .hops
                .iter()
                .enumerate()
                .any(|(index, existing)| index != hop_index && existing.pool == replacement_pool)
            {
                continue;
            }

            let mut probe = path.clone();
            probe.hops[hop_index] = Hop::new(replacement_pool.clone(), hop.token_in, hop.token_out);
            parallel_probe_index
                .entry(hop.pool.clone())
                .or_default()
                .insert(probe.clone());
            parallel_probe_index
                .entry(replacement_pool)
                .or_default()
                .insert(probe);
        }
    }
}

fn sorted_paths(paths: HashSet<RoutePath>) -> Vec<RoutePath> {
    let mut paths = paths.into_iter().collect::<Vec<_>>();
    paths.sort_by_key(route_path_sort_key);
    paths
}

fn route_path_sort_key(path: &RoutePath) -> String {
    path.hops
        .iter()
        .map(hop_sort_key)
        .collect::<Vec<_>>()
        .join("|")
}

enum StreamingWorkerMessage {
    Quote(RouteQuote),
}

#[derive(Clone, Debug)]
struct QuotedHeuristicHop {
    hop: Hop,
    amount_out: U256,
}

#[derive(Clone, Debug)]
struct LiquidityRankedHop {
    hop: Hop,
    input_balance: BalanceState,
    output_balance: BalanceState,
    liquidity_active: bool,
    rank_known: bool,
    rank_score: u16,
    protocol_bonus: u16,
    fee_sort: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct HopQuoteKey {
    pool: QuotePoolScope,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    sim_config: SimConfigKey,
}

impl HopQuoteKey {
    fn logical_pool(&self) -> &PoolKey {
        match &self.pool {
            QuotePoolScope::Static(pool) => pool,
            #[cfg(feature = "live-runtime")]
            QuotePoolScope::Versioned(state) => state.pool().key(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum QuotePoolScope {
    Static(PoolKey),
    #[cfg(feature = "live-runtime")]
    Versioned(PoolStateRef),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SimConfigKey {
    v3_quoter: Address,
    v2_router: Address,
    from: Address,
}

impl From<&SimConfig> for SimConfigKey {
    fn from(config: &SimConfig) -> Self {
        Self {
            v3_quoter: config.v3_quoter,
            v2_router: config.v2_router,
            from: config.from,
        }
    }
}

fn connector_allowed(
    token: Address,
    token_in: Address,
    token_out: Address,
    config: &SearchConfig,
) -> bool {
    connector_allowed_with_tokens(token, token_in, token_out, config.connector_tokens.as_ref())
}

fn connector_allowed_with_tokens(
    token: Address,
    token_in: Address,
    token_out: Address,
    connector_tokens: Option<&HashSet<Address>>,
) -> bool {
    if token == token_in || token == token_out {
        return true;
    }

    connector_tokens.is_none_or(|tokens| tokens.contains(&token))
}

fn max_candidates_reached(count: usize, config: &SearchConfig) -> bool {
    config
        .max_candidates
        .is_some_and(|max_candidates| count >= max_candidates)
}

fn apply_beam_width(
    frontier: &mut Vec<HeuristicState>,
    beam_width: Option<usize>,
    sort_without_truncation: bool,
) {
    let should_sort =
        sort_without_truncation || beam_width.is_some_and(|beam_width| frontier.len() > beam_width);
    if should_sort {
        frontier.sort_by(|left, right| {
            right.amount.cmp(&left.amount).then_with(|| {
                route_path_sort_key(&left.path).cmp(&route_path_sort_key(&right.path))
            })
        });
    }
    if let Some(beam_width) = beam_width
        && frontier.len() > beam_width
    {
        frontier.truncate(beam_width);
    }
}

fn hop_sort_key(hop: &Hop) -> String {
    format!("{:?}:{:?}:{:?}", hop.pool, hop.token_in, hop.token_out)
}

const fn protocol_bonus(protocol: ProtocolId) -> u16 {
    match protocol {
        ProtocolId::UniswapV3 | ProtocolId::PancakeV3 | ProtocolId::Slipstream => 30,
        ProtocolId::Curve => 25,
        ProtocolId::BalancerV2 => 22,
        ProtocolId::UniswapV2 | ProtocolId::SolidlyV2 => 10,
        ProtocolId::Custom(_) => 0,
        #[cfg(feature = "experimental-protocols")]
        ProtocolId::BalancerV3 | ProtocolId::Erc4626 | ProtocolId::UniswapV4 => 0,
        _ => 0,
    }
}

fn pool_fee_sort(pool: &PoolRegistration) -> u64 {
    match &pool.metadata {
        ProtocolMetadata::UniswapV2(metadata) => metadata.fee_bps.map_or(u64::MAX, u64::from),
        ProtocolMetadata::UniswapV3(metadata)
        | ProtocolMetadata::PancakeV3(metadata)
        | ProtocolMetadata::Slipstream(metadata) => metadata.fee.map_or(u64::MAX, u64::from),
        _ => u64::MAX,
    }
}

fn u256_log2(mut value: U256) -> u16 {
    if value == U256::ZERO {
        return 0;
    }

    let mut bits = 0_u16;
    while value > U256::ZERO {
        value >>= 1;
        bits += 1;
    }
    bits.saturating_sub(1)
}

#[cfg(all(test, feature = "live-runtime"))]
mod live_quote_key_tests {
    use super::*;
    use alloy_primitives::B256;
    use evm_amm_state::adapters::{PoolGeneration, PoolInstanceId, PoolStateRevision};

    fn context(
        key: &PoolKey,
        generation: u64,
        revision: u64,
        point: AmmStatePoint,
    ) -> QuoteKeyContext {
        QuoteKeyContext::FixedLive {
            point,
            pools: Arc::new(
                [(
                    key.clone(),
                    PoolStateRef::new(
                        PoolInstanceId::new(key.clone(), PoolGeneration::new(generation)),
                        PoolStateRevision::new(revision),
                        point,
                    ),
                )]
                .into_iter()
                .collect(),
            ),
        }
    }

    #[test]
    fn live_quote_keys_isolate_revision_generation_and_complete_point() {
        let pool = PoolKey::UniswapV2(Address::repeat_byte(0x41));
        let hop = Hop::new(
            pool.clone(),
            Address::repeat_byte(0x01),
            Address::repeat_byte(0x02),
        );
        let point_a = AmmStatePoint::post_block(1, 500, B256::repeat_byte(0xa1));
        let point_b = AmmStatePoint::post_block(1, 501, B256::repeat_byte(0xb1));
        let cache = SharedQuoteCache::with_context(context(&pool, 0, 0, point_a));
        let first = cache
            .key(&hop, U256::from(10_u64), &SimConfig::default())
            .expect("pool state ref is present");
        assert_eq!(
            first,
            cache
                .key(&hop, U256::from(10_u64), &SimConfig::default())
                .expect("pool state ref is present")
        );

        cache.set_context(context(&pool, 0, 1, point_a));
        assert_ne!(
            first,
            cache
                .key(&hop, U256::from(10_u64), &SimConfig::default())
                .expect("pool state ref is present")
        );
        cache.set_context(context(&pool, 1, 0, point_a));
        assert_ne!(
            first,
            cache
                .key(&hop, U256::from(10_u64), &SimConfig::default())
                .expect("pool state ref is present")
        );
        cache.set_context(context(&pool, 0, 0, point_b));
        assert_ne!(
            first,
            cache
                .key(&hop, U256::from(10_u64), &SimConfig::default())
                .expect("pool state ref is present")
        );

        cache.set_context(QuoteKeyContext::FixedLive {
            point: point_b,
            pools: Arc::new(HashMap::new()),
        });
        assert!(
            cache
                .key(&hop, U256::from(10_u64), &SimConfig::default())
                .expect_err("live cache keys must never fall back to static identity")
                .contains("missing pool state ref")
        );
    }

    #[test]
    fn live_quote_cache_reuses_results_and_evicts_obsolete_pool_revisions() {
        let pool = PoolKey::UniswapV2(Address::repeat_byte(0x42));
        let hop = Hop::new(
            pool.clone(),
            Address::repeat_byte(0x01),
            Address::repeat_byte(0x02),
        );
        let point = AmmStatePoint::post_block(1, 500, B256::repeat_byte(0xa2));
        let cache = SharedQuoteCache::with_context(context(&pool, 0, 0, point));
        let success_key = cache
            .key(&hop, U256::from(10_u64), &SimConfig::default())
            .expect("revision zero key");
        assert_eq!(
            cache.get_or_quote(success_key.clone(), || Ok(U256::from(20_u64))),
            Ok(U256::from(20_u64))
        );
        assert_eq!(
            cache.get_or_quote(success_key, || panic!("ready success must be reused")),
            Ok(U256::from(20_u64))
        );
        let failure_key = cache
            .key(&hop, U256::from(11_u64), &SimConfig::default())
            .expect("failure key");
        assert_eq!(
            cache.get_or_quote(failure_key.clone(), || Err("quote failed".to_owned())),
            Err("quote failed".to_owned())
        );
        assert_eq!(
            cache.get_or_quote(failure_key, || panic!("ready failure must be reused")),
            Err("quote failed".to_owned())
        );
        assert_eq!(cache.entry_count(), 2);

        cache.set_context(context(&pool, 0, 1, point));
        cache.invalidate_pools(&[pool.clone()].into_iter().collect());
        assert_eq!(cache.entry_count(), 0);
        let revised_key = cache
            .key(&hop, U256::from(10_u64), &SimConfig::default())
            .expect("revision one key");
        assert_eq!(
            cache.get_or_quote(revised_key, || Ok(U256::from(21_u64))),
            Ok(U256::from(21_u64))
        );
        assert_eq!(cache.entry_count(), 1);
    }
}
