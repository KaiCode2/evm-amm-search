use std::collections::{HashMap, HashSet};

use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, Log, U256, keccak256};
use alloy_provider::Provider;
use evm_amm_state::adapters::{
    AdapterCache, AdapterRegistry, PoolKey, PoolRegistration, ProtocolMetadata, SlotDelta,
    StateUpdate, StorageSyncEncoding, StorageSyncError, StorageSyncSpec, run_storage_syncs,
};
use evm_fork_cache::cache::{EvmCache, EvmSnapshot};
use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::AmmGraph;

const BALANCER_CASH_BITS: usize = 112;
const PARALLEL_EDGE_THRESHOLD: usize = 2;

/// Controls for balance-aware ordering and pruning in heuristic search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiquidityPruningConfig {
    /// Whether the liquidity sidecar should affect heuristic search.
    pub enabled: bool,
    /// Retained for compatibility. Liquidity pruning now applies to every
    /// parallel group, so values above `2` are ignored.
    pub min_parallel_edges: usize,
    /// Order candidate parallel edges by known output-token balance descending.
    pub order_by_output_balance: bool,
    /// Skip candidates whose known output-token balance cannot beat the best
    /// already quoted output for the same group.
    pub prune_balance_dominated: bool,
    /// Reserved for callers that want a stricter future policy. V1 keeps unknown
    /// and stale balances fail-open even when this is true.
    pub prune_unknown: bool,
    /// Rank non-target branch groups by fresh current-token pool balance when a
    /// broader liquidity index is attached to the searcher.
    pub rank_branches_by_liquidity: bool,
}

impl Default for LiquidityPruningConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_parallel_edges: PARALLEL_EDGE_THRESHOLD,
            order_by_output_balance: true,
            prune_balance_dominated: true,
            prune_unknown: false,
            rank_branches_by_liquidity: false,
        }
    }
}

impl LiquidityPruningConfig {
    /// Disabled liquidity pruning.
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            min_parallel_edges: PARALLEL_EDGE_THRESHOLD,
            order_by_output_balance: true,
            prune_balance_dominated: true,
            prune_unknown: false,
            rank_branches_by_liquidity: false,
        }
    }

    /// Enabled liquidity pruning with conservative defaults.
    pub const fn enabled() -> Self {
        Self {
            enabled: true,
            min_parallel_edges: PARALLEL_EDGE_THRESHOLD,
            order_by_output_balance: true,
            prune_balance_dominated: true,
            prune_unknown: false,
            rank_branches_by_liquidity: true,
        }
    }

    /// Compatibility setter. Values below or above `2` are normalized to `2`
    /// because the heuristic now runs for any parallel edge group.
    pub const fn with_min_parallel_edges(mut self, min_parallel_edges: usize) -> Self {
        let _ = min_parallel_edges;
        self.min_parallel_edges = PARALLEL_EDGE_THRESHOLD;
        self
    }

    /// Toggle output-balance ordering within same-prefix parallel edge groups.
    pub const fn with_order_by_output_balance(mut self, order_by_output_balance: bool) -> Self {
        self.order_by_output_balance = order_by_output_balance;
        self
    }

    /// Toggle conservative balance-dominated edge pruning.
    pub const fn with_prune_balance_dominated(mut self, prune_balance_dominated: bool) -> Self {
        self.prune_balance_dominated = prune_balance_dominated;
        self
    }

    /// Configure whether unknown balances may be pruned.
    ///
    /// Current behavior remains fail-open for unknown/stale balances; this is
    /// retained as a forward-compatible policy knob.
    pub const fn with_prune_unknown(mut self, prune_unknown: bool) -> Self {
        self.prune_unknown = prune_unknown;
        self
    }

    /// Toggle liquidity-ranked branch expansion.
    pub const fn with_rank_branches_by_liquidity(
        mut self,
        rank_branches_by_liquidity: bool,
    ) -> Self {
        self.rank_branches_by_liquidity = rank_branches_by_liquidity;
        self
    }
}

/// Counters for balance-aware heuristic pruning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiquidityPruneStats {
    /// Same-prefix parallel groups sorted by liquidity.
    pub ordered_groups: usize,
    /// Candidate edges skipped without simulation because a known balance cap
    /// could not beat the best output already quoted for the group.
    pub pruned_edges: usize,
    /// Candidate edges that remained fail-open because their balance was stale or
    /// unknown.
    pub stale_or_unknown_skipped_for_pruning: usize,
    /// Balance lookups performed by the search heuristic.
    pub balance_reads: usize,
    /// Balance refresh failures observed by the liquidity index.
    pub refresh_failures: usize,
    /// Heuristic expansion groups where target-closing branches were moved
    /// ahead of intermediate branches.
    pub target_first_groups: usize,
    /// Prefix states skipped because an equal-or-better prefix reached the same
    /// token with looser route constraints.
    pub prefix_dominated_states: usize,
    /// Non-target branch groups sorted by current-token liquidity.
    pub liquidity_ranked_branch_groups: usize,
    /// Non-target branch groups that remained fail-open because no fresh branch
    /// liquidity was known.
    pub liquidity_unknown_branch_groups: usize,
    /// Fast-lane route templates attempted before broad heuristic expansion.
    pub fast_lane_routes: usize,
    /// Fast-lane route templates that produced viable quotes.
    pub fast_lane_quotes: usize,
    /// Parallel edges considered during the first adaptive shortlist pass.
    pub shortlist_initial_edges: usize,
    /// Parallel edges considered during the adaptive refinement pass.
    pub shortlist_refinement_edges: usize,
    /// Parallel edges deferred by adaptive shortlisting.
    pub shortlist_deferred_edges: usize,
    /// Parallel edges ordered with protocol-aware ranking.
    pub protocol_ranked_edges: usize,
    /// Prefixes pruned because a conservative upper bound could not beat the
    /// incumbent route.
    pub upper_bound_pruned_prefixes: usize,
    /// Prefixes left fail-open because an upper bound could not be proven.
    pub upper_bound_unknown_prefixes: usize,
}

impl LiquidityPruneStats {
    pub(crate) fn merge(&mut self, other: Self) {
        self.ordered_groups += other.ordered_groups;
        self.pruned_edges += other.pruned_edges;
        self.stale_or_unknown_skipped_for_pruning += other.stale_or_unknown_skipped_for_pruning;
        self.balance_reads += other.balance_reads;
        self.refresh_failures += other.refresh_failures;
        self.target_first_groups += other.target_first_groups;
        self.prefix_dominated_states += other.prefix_dominated_states;
        self.liquidity_ranked_branch_groups += other.liquidity_ranked_branch_groups;
        self.liquidity_unknown_branch_groups += other.liquidity_unknown_branch_groups;
        self.fast_lane_routes += other.fast_lane_routes;
        self.fast_lane_quotes += other.fast_lane_quotes;
        self.shortlist_initial_edges += other.shortlist_initial_edges;
        self.shortlist_refinement_edges += other.shortlist_refinement_edges;
        self.shortlist_deferred_edges += other.shortlist_deferred_edges;
        self.protocol_ranked_edges += other.protocol_ranked_edges;
        self.upper_bound_pruned_prefixes += other.upper_bound_pruned_prefixes;
        self.upper_bound_unknown_prefixes += other.upper_bound_unknown_prefixes;
    }
}

/// Pool-token balance tracking scope used when constructing a liquidity index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LiquidityIndexScope {
    /// Track only output-token balances for token pairs that have parallel AMM
    /// edges. This is the low-cost default used by
    /// [`PoolLiquidityIndex::from_registry`].
    #[default]
    ParallelEdgeOutputs,
    /// Track every token that appears as a directed graph input or output for
    /// each indexed pool. Use this when enabling liquidity-ranked branch
    /// expansion.
    AllDirectedEdgeInputsAndOutputs,
}

/// Latest known liquidity state for a tracked pool/token balance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BalanceState {
    /// No trustworthy balance is available.
    #[default]
    Unknown,
    /// A previously known balance may be stale and must not be used for pruning.
    Stale,
    /// Fresh balance sampled or event-updated at the current cache point.
    Fresh(U256),
}

impl BalanceState {
    /// Return the fresh amount, if this balance is usable for pruning.
    pub const fn fresh(self) -> Option<U256> {
        match self {
            Self::Fresh(value) => Some(value),
            Self::Unknown | Self::Stale => None,
        }
    }
}

/// One token/holder pair callers should subscribe to for ERC-20 `Transfer`
/// freshness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferEventSource {
    /// ERC-20 token contract whose `Transfer` logs matter.
    pub token: Address,
    /// Holders whose balance changes affect indexed pool liquidity.
    pub holders: Vec<Address>,
}

/// Summary from constructing a liquidity index.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PoolLiquidityBuildReport {
    /// Number of pool-token balances tracked by the index.
    pub tracked_balances: usize,
    /// Number of pool-token balances that could not be mapped to a refreshable
    /// storage source.
    pub unknown_balances: usize,
}

/// Summary from incrementally reconciling liquidity targets for selected pools.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PoolLiquidityMutationReport {
    /// Newly tracked pool/token targets.
    pub added_targets: usize,
    /// Targets no longer represented by the graph/scope.
    pub removed_targets: usize,
    /// Fresh balances retained because their source identity was unchanged.
    pub preserved_fresh_targets: usize,
    /// Desired targets whose balance source remains unknown.
    pub unknown_targets: usize,
}

/// Summary from a balance refresh.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PoolLiquidityRefreshReport {
    /// Storage words read from the chain.
    pub storage_reads: usize,
    /// Balance entries updated to fresh.
    pub refreshed_balances: usize,
    /// Balance entries left unknown.
    pub unknown_balances: usize,
    /// Balance entries marked stale because refresh failed.
    pub stale_balances: usize,
    /// Per-target refresh failures.
    pub failures: Vec<PoolLiquidityRefreshFailure>,
}

/// One failed liquidity refresh group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolLiquidityRefreshFailure {
    /// Storage target that failed to refresh.
    pub target: Address,
    /// Human-readable failure reason.
    pub reason: String,
}

/// Result of applying a transfer log to the liquidity index.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransferApplyReport {
    /// Whether the log matched a tracked ERC-20 transfer.
    pub matched: bool,
    /// Number of in-memory balances updated.
    pub updated_balances: usize,
    /// Number of tracked balances marked stale.
    pub stale_balances: usize,
    /// Number of cache state updates emitted for tracked holder balances.
    pub cache_updates: usize,
    /// Number of cache updates that were skipped because their current slot was
    /// cold.
    pub skipped_cache_updates: usize,
}

/// Mutable liquidity sidecar for pool-held token balances.
#[derive(Clone, Debug, Default)]
pub struct PoolLiquidityIndex {
    targets: Vec<PoolLiquidityTarget>,
    by_pool_token: HashMap<(PoolKey, Address), usize>,
    by_token_holder: HashMap<(Address, Address), Vec<usize>>,
    by_storage_slot: HashMap<(Address, U256), Vec<usize>>,
    by_pool: HashMap<PoolKey, Vec<usize>>,
    pool_token_counts: HashMap<PoolKey, usize>,
    free_targets: Vec<usize>,
}

/// Alias kept for callers that prefer a stateful "tracker" name for the same
/// sidecar.
pub type PoolLiquidityTracker = PoolLiquidityIndex;

impl PoolLiquidityIndex {
    /// Build a liquidity index for pools that are already present in `graph`.
    pub fn from_registry(
        registry: &AdapterRegistry,
        graph: &AmmGraph,
    ) -> (Self, PoolLiquidityBuildReport) {
        Self::from_registry_with_scope(registry, graph, LiquidityIndexScope::ParallelEdgeOutputs)
    }

    /// Build a liquidity index for pools that are already present in `graph`,
    /// using an explicit balance tracking scope.
    pub fn from_registry_with_scope(
        registry: &AdapterRegistry,
        graph: &AmmGraph,
        scope: LiquidityIndexScope,
    ) -> (Self, PoolLiquidityBuildReport) {
        let mut index = Self::default();
        let mut unknown_balances = 0;
        let tracked_tokens = tracked_tokens_by_pool(graph, scope);

        let mut pools: Vec<&PoolRegistration> = registry.pools().collect();
        pools.sort_by_key(|pool| format!("{:?}", pool.key));

        for pool in pools {
            let Some(needed_tokens) = tracked_tokens.get(&pool.key) else {
                continue;
            };

            let Some(tokens) = pool.tokens() else {
                continue;
            };
            let tokens = normalize_tokens(tokens);
            if tokens.len() < 2 {
                continue;
            }
            index
                .pool_token_counts
                .insert(pool.key.clone(), tokens.len());
            let tokens = tokens
                .into_iter()
                .filter(|token| needed_tokens.contains(token))
                .collect::<Vec<_>>();
            if tokens.is_empty() {
                continue;
            }

            match &pool.metadata {
                ProtocolMetadata::UniswapV2(_)
                | ProtocolMetadata::UniswapV3(_)
                | ProtocolMetadata::PancakeV3(_)
                | ProtocolMetadata::Slipstream(_)
                | ProtocolMetadata::SolidlyV2(_)
                | ProtocolMetadata::Curve(_) => {
                    let Some(holder) = pool.key.address() else {
                        unknown_balances += tokens.len();
                        for token in tokens {
                            index.push_target(pool.key.clone(), token, LiquiditySource::Unknown);
                        }
                        continue;
                    };
                    for token in tokens {
                        index.push_target(
                            pool.key.clone(),
                            token,
                            LiquiditySource::Erc20Balance { holder, slot: None },
                        );
                    }
                }
                ProtocolMetadata::BalancerV2(metadata) => {
                    let token_cash: HashMap<Address, _> = metadata
                        .token_cash
                        .iter()
                        .map(|cash| (cash.token, *cash))
                        .collect();
                    for token in tokens {
                        let source = metadata
                            .vault
                            .zip(token_cash.get(&token).copied())
                            .map(|(vault, cash)| LiquiditySource::BalancerV2VaultCash {
                                vault,
                                slot: cash.slot,
                                high_field: cash.high_field,
                            })
                            .unwrap_or(LiquiditySource::Unknown);
                        if matches!(source, LiquiditySource::Unknown) {
                            unknown_balances += 1;
                        }
                        index.push_target(pool.key.clone(), token, source);
                    }
                }
                ProtocolMetadata::Unknown | ProtocolMetadata::Custom(_) => {}
                #[allow(unreachable_patterns)]
                _ => {}
            }
        }

        let report = PoolLiquidityBuildReport {
            tracked_balances: index.by_pool_token.len(),
            unknown_balances,
        };
        (index, report)
    }

    /// Number of tracked pool-token balances.
    pub fn len(&self) -> usize {
        self.by_pool_token.len()
    }

    /// Whether the index has no tracked balances.
    pub fn is_empty(&self) -> bool {
        self.by_pool_token.is_empty()
    }

    /// Reconcile only the selected pools against the current graph and registry.
    ///
    /// Existing fresh balances survive when the exact token/source identity is
    /// unchanged. Removed target slots are tombstoned so unrelated reverse
    /// indexes and balances do not need to be rebuilt.
    pub fn reconcile_pools(
        &mut self,
        registry: &AdapterRegistry,
        graph: &AmmGraph,
        scope: LiquidityIndexScope,
        pools: impl IntoIterator<Item = PoolKey>,
    ) -> PoolLiquidityMutationReport {
        let mut pools = pools.into_iter().collect::<Vec<_>>();
        pools.sort_by_key(|pool| format!("{pool:?}"));
        pools.dedup();
        let mut report = PoolLiquidityMutationReport::default();

        for pool in pools {
            let previous = self
                .by_pool
                .get(&pool)
                .into_iter()
                .flatten()
                .filter_map(|index| self.targets.get(*index))
                .filter(|target| target.active)
                .map(|target| (target.token, (target.source, target.balance)))
                .collect::<HashMap<_, _>>();
            let needed = graph
                .tracked_tokens_for_pool(&pool, scope == LiquidityIndexScope::ParallelEdgeOutputs);
            let desired = registry
                .pool(&pool)
                .filter(|_| !needed.is_empty())
                .map(|registration| desired_targets(registration, &needed))
                .unwrap_or_default()
                .into_iter()
                .map(|(token, source)| {
                    let source = previous.get(&token).map_or(source, |(previous_source, _)| {
                        preserve_discovered_source(*previous_source, source)
                    });
                    (token, source)
                })
                .collect::<Vec<_>>();

            let mut matched = HashSet::new();
            for (token, source) in &desired {
                match previous.get(token) {
                    Some((previous_source, BalanceState::Fresh(_)))
                        if previous_source == source =>
                    {
                        report.preserved_fresh_targets += 1;
                        matched.insert(*token);
                    }
                    Some((previous_source, _)) if previous_source == source => {
                        matched.insert(*token);
                    }
                    Some(_) => {
                        report.removed_targets += 1;
                        report.added_targets += 1;
                    }
                    None => report.added_targets += 1,
                }
                if matches!(source, LiquiditySource::Unknown) {
                    report.unknown_targets += 1;
                }
            }
            report.removed_targets += previous
                .keys()
                .filter(|token| {
                    !matched.contains(*token) && !desired.iter().any(|(next, _)| next == *token)
                })
                .count();

            self.remove_pool_targets(&pool);
            if let Some(registration) = registry.pool(&pool) {
                let token_count = registration
                    .tokens()
                    .map(normalize_tokens)
                    .map_or(0, |tokens| tokens.len());
                if token_count >= 2 && !desired.is_empty() {
                    self.pool_token_counts.insert(pool.clone(), token_count);
                }
            }
            for (token, source) in desired {
                let balance = previous
                    .get(&token)
                    .filter(|(previous_source, _)| *previous_source == source)
                    .map_or(BalanceState::Unknown, |(_, balance)| *balance);
                self.push_target_with_balance(pool.clone(), token, source, balance);
            }
        }

        report
    }

    /// Return the current balance state for `pool`'s `token`.
    pub fn balance_state(&self, pool: &PoolKey, token: Address) -> BalanceState {
        self.by_pool_token
            .get(&(pool.clone(), token))
            .and_then(|index| self.targets.get(*index))
            .map(|target| target.balance)
            .unwrap_or(BalanceState::Unknown)
    }

    /// Return the fresh balance for `pool`'s `token`, if available.
    pub fn fresh_balance(&self, pool: &PoolKey, token: Address) -> Option<U256> {
        self.balance_state(pool, token).fresh()
    }

    /// Whether `pool` was indexed as a two-token pool.
    pub fn is_two_token_pool(&self, pool: &PoolKey) -> bool {
        self.pool_token_counts.get(pool).copied() == Some(2)
    }

    /// Set a fresh balance for an already tracked pool/token. This is useful for
    /// custom balance sources and deterministic tests.
    pub fn set_balance(
        &mut self,
        pool: &PoolKey,
        token: Address,
        balance: U256,
    ) -> Result<(), PoolLiquidityError> {
        let index =
            self.target_index(pool, token)
                .ok_or_else(|| PoolLiquidityError::UnknownPoolToken {
                    pool: pool.clone(),
                    token,
                })?;
        self.targets[index].balance = BalanceState::Fresh(balance);
        Ok(())
    }

    /// Seed a discovered ERC-20 balance slot for every tracked pool balance
    /// backed by `token.balanceOf(holder)`.
    pub fn set_erc20_balance_slot(&mut self, token: Address, holder: Address, slot: U256) -> usize {
        let before = self.by_storage_slot.get(&(token, slot)).map_or(0, Vec::len);
        self.set_erc20_slot(token, holder, slot);
        self.by_storage_slot
            .get(&(token, slot))
            .map_or(0, Vec::len)
            .saturating_sub(before)
    }

    /// Mark a tracked pool/token balance stale.
    pub fn mark_stale(&mut self, pool: &PoolKey, token: Address) -> Result<(), PoolLiquidityError> {
        let index =
            self.target_index(pool, token)
                .ok_or_else(|| PoolLiquidityError::UnknownPoolToken {
                    pool: pool.clone(),
                    token,
                })?;
        self.targets[index].balance = BalanceState::Stale;
        Ok(())
    }

    /// Return ERC-20 transfer subscriptions needed to keep normal pool balances
    /// fresh.
    pub fn transfer_event_sources(&self) -> Vec<TransferEventSource> {
        let mut by_token = HashMap::<Address, HashSet<Address>>::new();
        for target in &self.targets {
            if !target.active {
                continue;
            }
            if let LiquiditySource::Erc20Balance { holder, .. } = target.source {
                by_token.entry(target.token).or_default().insert(holder);
            }
        }

        let mut sources = by_token
            .into_iter()
            .map(|(token, holders)| {
                let mut holders = holders.into_iter().collect::<Vec<_>>();
                holders.sort_unstable_by(|left, right| left.as_slice().cmp(right.as_slice()));
                TransferEventSource { token, holders }
            })
            .collect::<Vec<_>>();
        sources.sort_by(|left, right| left.token.as_slice().cmp(right.token.as_slice()));
        sources
    }

    /// Refresh every known balance source in one batched storage-sync operation.
    pub async fn refresh_all<P: Provider<AnyNetwork>>(
        &mut self,
        cache: &mut EvmCache,
        provider: &P,
    ) -> PoolLiquidityRefreshReport {
        self.refresh(cache, provider, RefreshMode::All).await
    }

    /// Refresh only balances currently marked stale in one batched storage-sync
    /// operation.
    pub async fn refresh_stale<P: Provider<AnyNetwork>>(
        &mut self,
        cache: &mut EvmCache,
        provider: &P,
    ) -> PoolLiquidityRefreshReport {
        self.refresh(cache, provider, RefreshMode::StaleOnly).await
    }

    /// Refresh known storage-backed balances from one immutable cache snapshot.
    ///
    /// Targets without a discovered storage slot remain unknown. This method
    /// performs no provider reads and therefore cannot cross the snapshot's
    /// block/hash boundary.
    pub fn refresh_from_snapshot(&mut self, snapshot: &EvmSnapshot) -> PoolLiquidityRefreshReport {
        let mut report = PoolLiquidityRefreshReport::default();
        for target in &mut self.targets {
            if !target.active {
                continue;
            }
            let Some((address, slot)) = target.storage_slot() else {
                target.balance = BalanceState::Unknown;
                report.unknown_balances += 1;
                continue;
            };
            let Some(value) = snapshot.storage_value(address, slot) else {
                target.balance = BalanceState::Unknown;
                report.unknown_balances += 1;
                continue;
            };
            target.balance = BalanceState::Fresh(match target.source {
                LiquiditySource::Erc20Balance { .. } => value,
                LiquiditySource::BalancerV2VaultCash { high_field, .. } => {
                    decode_balancer_cash(value, high_field)
                }
                LiquiditySource::Unknown => {
                    report.unknown_balances += 1;
                    continue;
                }
            });
            report.refreshed_balances += 1;
        }
        report
    }

    /// Apply a normal ERC-20 transfer log to both the cache and the in-memory
    /// balance index. Removed/reorged logs should call
    /// [`mark_transfer_log_stale`](Self::mark_transfer_log_stale) instead.
    pub fn apply_transfer_log(
        &mut self,
        cache: &mut dyn AdapterCache,
        log: &Log,
    ) -> TransferApplyReport {
        let Some(transfer) = parse_transfer(log) else {
            return TransferApplyReport::default();
        };

        let mut updates = Vec::new();
        let mut touched = Vec::new();
        self.collect_transfer_updates(
            transfer.token,
            transfer.from,
            SlotDelta::Sub(transfer.value),
            &mut updates,
            &mut touched,
        );
        self.collect_transfer_updates(
            transfer.token,
            transfer.to,
            SlotDelta::Add(transfer.value),
            &mut updates,
            &mut touched,
        );

        if updates.is_empty() {
            return TransferApplyReport::default();
        }

        let diff = cache.apply_updates(&updates);
        let mut report = TransferApplyReport {
            matched: true,
            cache_updates: updates.len(),
            skipped_cache_updates: diff.skipped.len() + diff.skipped_masks.len(),
            ..TransferApplyReport::default()
        };

        if diff.has_skipped() {
            let skipped_slots = diff
                .skipped
                .iter()
                .map(|skipped| (skipped.address, skipped.slot))
                .collect::<HashSet<_>>();
            for (target_index, address, slot, delta) in touched {
                if skipped_slots.contains(&(address, slot)) {
                    self.targets[target_index].balance = BalanceState::Stale;
                    report.stale_balances += 1;
                } else if let Some(current) = self.targets[target_index].balance.fresh() {
                    self.targets[target_index].balance = BalanceState::Fresh(delta.apply(current));
                    report.updated_balances += 1;
                } else {
                    self.targets[target_index].balance = BalanceState::Stale;
                    report.stale_balances += 1;
                }
            }
        } else {
            for (target_index, _address, _slot, delta) in touched {
                if let Some(current) = self.targets[target_index].balance.fresh() {
                    self.targets[target_index].balance = BalanceState::Fresh(delta.apply(current));
                    report.updated_balances += 1;
                } else {
                    self.targets[target_index].balance = BalanceState::Stale;
                    report.stale_balances += 1;
                }
            }
        }

        report
    }

    /// Mark tracked balances touched by a transfer log stale. Use this for
    /// removed logs, reorg notifications, or other cases where event deltas should
    /// not be trusted directionally.
    pub fn mark_transfer_log_stale(&mut self, log: &Log) -> TransferApplyReport {
        let Some(transfer) = parse_transfer(log) else {
            return TransferApplyReport::default();
        };

        let mut report = TransferApplyReport {
            matched: true,
            ..TransferApplyReport::default()
        };
        for holder in [transfer.from, transfer.to] {
            if holder == Address::ZERO {
                continue;
            }
            let Some(indices) = self.by_token_holder.get(&(transfer.token, holder)).cloned() else {
                continue;
            };
            for index in indices {
                self.targets[index].balance = BalanceState::Stale;
                report.stale_balances += 1;
            }
        }
        report
    }

    /// Update Balancer V2 vault-cash balances from absolute slot writes emitted
    /// by an AMM sync path.
    pub fn apply_storage_updates(&mut self, updates: &[StateUpdate]) -> usize {
        let mut updated = 0;
        for update in updates {
            let StateUpdate::Slot {
                address,
                slot,
                value,
            } = update
            else {
                continue;
            };
            updated += self.update_slot_value(*address, *slot, *value);
        }
        updated
    }

    fn push_target(&mut self, pool: PoolKey, token: Address, source: LiquiditySource) {
        self.push_target_with_balance(pool, token, source, BalanceState::Unknown);
    }

    fn push_target_with_balance(
        &mut self,
        pool: PoolKey,
        token: Address,
        source: LiquiditySource,
        balance: BalanceState,
    ) {
        let target_index = self.free_targets.pop().unwrap_or(self.targets.len());
        if let LiquiditySource::Erc20Balance { holder, .. } = source {
            self.by_token_holder
                .entry((token, holder))
                .or_default()
                .push(target_index);
        }
        self.by_pool_token
            .insert((pool.clone(), token), target_index);
        self.by_pool
            .entry(pool.clone())
            .or_default()
            .push(target_index);
        let target = PoolLiquidityTarget {
            token,
            source,
            balance,
            active: true,
        };
        if target_index == self.targets.len() {
            self.targets.push(target);
        } else {
            self.targets[target_index] = target;
        }
        if let Some((address, slot)) = self.targets[target_index].storage_slot() {
            self.by_storage_slot
                .entry((address, slot))
                .or_default()
                .push(target_index);
        }
    }

    fn target_index(&self, pool: &PoolKey, token: Address) -> Option<usize> {
        self.by_pool_token.get(&(pool.clone(), token)).copied()
    }

    async fn refresh<P: Provider<AnyNetwork>>(
        &mut self,
        cache: &mut EvmCache,
        provider: &P,
        mode: RefreshMode,
    ) -> PoolLiquidityRefreshReport {
        let mut report = PoolLiquidityRefreshReport::default();
        self.discover_erc20_slots(cache, mode, &mut report);

        let mut slots_by_target = HashMap::<Address, Vec<U256>>::new();
        for target in &self.targets {
            if !target.active {
                continue;
            }
            if !mode.includes(target.balance) {
                continue;
            }
            if let Some((address, slot)) = target.storage_slot() {
                slots_by_target.entry(address).or_default().push(slot);
            } else if matches!(target.balance, BalanceState::Unknown) {
                report.unknown_balances += 1;
            }
        }

        let mut specs = slots_by_target
            .into_iter()
            .filter_map(|(target, mut slots)| {
                slots.sort_unstable();
                slots.dedup();
                (!slots.is_empty()).then(|| {
                    StorageSyncSpec::new(target, slots)
                        .with_encoding(StorageSyncEncoding::CalldataSlots)
                })
            })
            .collect::<Vec<_>>();
        specs.sort_by(|left, right| left.target.as_slice().cmp(right.target.as_slice()));

        report.storage_reads = specs.iter().map(|spec| spec.slots.len()).sum();
        if specs.is_empty() {
            return report;
        }

        let snapshots = run_storage_syncs(provider, cache.block(), &specs).await;
        for (spec, snapshot) in specs.iter().zip(snapshots) {
            match snapshot {
                Ok(snapshot) => {
                    snapshot.inject_fresh(cache);
                    for (slot, value) in snapshot.entries {
                        report.refreshed_balances +=
                            self.update_slot_value(snapshot.target, slot, value);
                    }
                }
                Err(error) => {
                    let stale = self.mark_storage_spec_stale(spec);
                    report.stale_balances += stale;
                    report.failures.push(PoolLiquidityRefreshFailure {
                        target: spec.target,
                        reason: storage_sync_error_to_string(&error),
                    });
                }
            }
        }

        report
    }

    fn discover_erc20_slots(
        &mut self,
        cache: &mut EvmCache,
        mode: RefreshMode,
        report: &mut PoolLiquidityRefreshReport,
    ) {
        let mut holders_by_token = HashMap::<Address, HashSet<Address>>::new();
        for target in &self.targets {
            if !target.active {
                continue;
            }
            if !mode.includes(target.balance) {
                continue;
            }
            if let LiquiditySource::Erc20Balance { holder, slot: None } = target.source {
                holders_by_token
                    .entry(target.token)
                    .or_default()
                    .insert(holder);
            }
        }

        for (token, holders) in holders_by_token {
            let holders = holders.into_iter().collect::<Vec<_>>();
            match cache.track_erc20_balances(token, holders.iter().copied()) {
                Ok(Some((_tracked, pairs))) => {
                    for (holder, slot) in pairs {
                        self.set_erc20_slot(token, holder, U256::from_be_bytes(slot.0));
                    }
                }
                Ok(None) => {
                    for holder in holders {
                        report.unknown_balances +=
                            self.mark_token_holder_unknown_or_stale(token, holder, false);
                    }
                }
                Err(error) => {
                    for holder in holders {
                        report.stale_balances +=
                            self.mark_token_holder_unknown_or_stale(token, holder, true);
                    }
                    report.failures.push(PoolLiquidityRefreshFailure {
                        target: token,
                        reason: error.to_string(),
                    });
                }
            }
        }
    }

    fn set_erc20_slot(&mut self, token: Address, holder: Address, slot: U256) {
        let Some(indices) = self.by_token_holder.get(&(token, holder)).cloned() else {
            return;
        };
        for index in indices {
            if let LiquiditySource::Erc20Balance {
                holder: source_holder,
                slot: source_slot,
            } = &mut self.targets[index].source
            {
                debug_assert_eq!(*source_holder, holder);
                *source_slot = Some(slot);
                let indices = self.by_storage_slot.entry((token, slot)).or_default();
                if !indices.contains(&index) {
                    indices.push(index);
                }
            }
        }
    }

    fn mark_token_holder_unknown_or_stale(
        &mut self,
        token: Address,
        holder: Address,
        stale: bool,
    ) -> usize {
        let Some(indices) = self.by_token_holder.get(&(token, holder)).cloned() else {
            return 0;
        };
        for index in &indices {
            self.targets[*index].balance = if stale {
                BalanceState::Stale
            } else {
                BalanceState::Unknown
            };
        }
        indices.len()
    }

    fn update_slot_value(&mut self, address: Address, slot: U256, value: U256) -> usize {
        let Some(indices) = self.by_storage_slot.get(&(address, slot)).cloned() else {
            return 0;
        };
        let mut updated = 0;
        for index in indices {
            let balance = match self.targets[index].source {
                LiquiditySource::Erc20Balance { .. } => value,
                LiquiditySource::BalancerV2VaultCash { high_field, .. } => {
                    decode_balancer_cash(value, high_field)
                }
                LiquiditySource::Unknown => continue,
            };
            self.targets[index].balance = BalanceState::Fresh(balance);
            updated += 1;
        }
        updated
    }

    fn mark_storage_spec_stale(&mut self, spec: &StorageSyncSpec) -> usize {
        let mut stale = 0;
        for slot in &spec.slots {
            let Some(indices) = self.by_storage_slot.get(&(spec.target, *slot)).cloned() else {
                continue;
            };
            for index in indices {
                self.targets[index].balance = BalanceState::Stale;
                stale += 1;
            }
        }
        stale
    }

    fn collect_transfer_updates(
        &self,
        token: Address,
        holder: Address,
        delta: SlotDelta,
        updates: &mut Vec<StateUpdate>,
        touched: &mut Vec<(usize, Address, U256, SlotDelta)>,
    ) {
        if holder == Address::ZERO {
            return;
        }
        let Some(indices) = self.by_token_holder.get(&(token, holder)) else {
            return;
        };

        let mut pushed_slots = HashSet::<U256>::new();
        for index in indices {
            let LiquiditySource::Erc20Balance {
                slot: Some(slot), ..
            } = self.targets[*index].source
            else {
                continue;
            };
            touched.push((*index, token, slot, delta));
            if pushed_slots.insert(slot) {
                updates.push(StateUpdate::slot_delta(token, slot, delta));
            }
        }
    }

    fn remove_pool_targets(&mut self, pool: &PoolKey) {
        let Some(indices) = self.by_pool.remove(pool) else {
            self.pool_token_counts.remove(pool);
            return;
        };
        for index in indices {
            let Some(target) = self.targets.get_mut(index) else {
                continue;
            };
            if !target.active {
                continue;
            }
            target.active = false;
            self.by_pool_token.remove(&(pool.clone(), target.token));
            if let LiquiditySource::Erc20Balance { holder, .. } = target.source {
                remove_reverse_index(&mut self.by_token_holder, &(target.token, holder), index);
            }
            if let Some(storage_slot) = target.storage_slot() {
                remove_reverse_index(&mut self.by_storage_slot, &storage_slot, index);
            }
            self.free_targets.push(index);
        }
        self.pool_token_counts.remove(pool);
    }
}

/// Errors returned by direct liquidity-index mutation helpers.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PoolLiquidityError {
    /// The requested pool/token is not tracked.
    #[error("pool/token balance is not tracked: pool={pool:?}, token={token:?}")]
    UnknownPoolToken {
        /// Pool key.
        pool: PoolKey,
        /// Token address.
        token: Address,
    },
}

#[derive(Clone, Debug)]
struct PoolLiquidityTarget {
    token: Address,
    source: LiquiditySource,
    balance: BalanceState,
    active: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LiquiditySource {
    Erc20Balance {
        holder: Address,
        slot: Option<U256>,
    },
    BalancerV2VaultCash {
        vault: Address,
        slot: U256,
        high_field: bool,
    },
    Unknown,
}

fn preserve_discovered_source(
    previous: LiquiditySource,
    desired: LiquiditySource,
) -> LiquiditySource {
    match (previous, desired) {
        (
            previous @ LiquiditySource::Erc20Balance {
                holder: previous_holder,
                slot: Some(_),
            },
            LiquiditySource::Erc20Balance {
                holder: desired_holder,
                ..
            },
        ) if previous_holder == desired_holder => previous,
        _ => desired,
    }
}

fn desired_targets(
    pool: &PoolRegistration,
    needed_tokens: &HashSet<Address>,
) -> Vec<(Address, LiquiditySource)> {
    let Some(tokens) = pool.tokens() else {
        return Vec::new();
    };
    let tokens = normalize_tokens(tokens)
        .into_iter()
        .filter(|token| needed_tokens.contains(token))
        .collect::<Vec<_>>();
    match &pool.metadata {
        ProtocolMetadata::UniswapV2(_)
        | ProtocolMetadata::UniswapV3(_)
        | ProtocolMetadata::PancakeV3(_)
        | ProtocolMetadata::Slipstream(_)
        | ProtocolMetadata::SolidlyV2(_)
        | ProtocolMetadata::Curve(_) => tokens
            .into_iter()
            .map(|token| {
                let source = pool
                    .key
                    .address()
                    .map_or(LiquiditySource::Unknown, |holder| {
                        LiquiditySource::Erc20Balance { holder, slot: None }
                    });
                (token, source)
            })
            .collect(),
        ProtocolMetadata::BalancerV2(metadata) => {
            let token_cash = metadata
                .token_cash
                .iter()
                .map(|cash| (cash.token, *cash))
                .collect::<HashMap<_, _>>();
            tokens
                .into_iter()
                .map(|token| {
                    let source = metadata.vault.zip(token_cash.get(&token).copied()).map_or(
                        LiquiditySource::Unknown,
                        |(vault, cash)| LiquiditySource::BalancerV2VaultCash {
                            vault,
                            slot: cash.slot,
                            high_field: cash.high_field,
                        },
                    );
                    (token, source)
                })
                .collect()
        }
        ProtocolMetadata::Unknown | ProtocolMetadata::Custom(_) => Vec::new(),
        #[allow(unreachable_patterns)]
        _ => Vec::new(),
    }
}

fn remove_reverse_index<K: std::hash::Hash + Eq>(
    index: &mut HashMap<K, Vec<usize>>,
    key: &K,
    removed: usize,
) {
    let remove_key = if let Some(indices) = index.get_mut(key) {
        indices.retain(|index| *index != removed);
        indices.is_empty()
    } else {
        false
    };
    if remove_key {
        index.remove(key);
    }
}

impl PoolLiquidityTarget {
    fn storage_slot(&self) -> Option<(Address, U256)> {
        match self.source {
            LiquiditySource::Erc20Balance {
                slot: Some(slot), ..
            } => Some((self.token, slot)),
            LiquiditySource::BalancerV2VaultCash { vault, slot, .. } => Some((vault, slot)),
            LiquiditySource::Erc20Balance { slot: None, .. } | LiquiditySource::Unknown => None,
        }
    }
}

#[derive(Clone, Copy)]
enum RefreshMode {
    All,
    StaleOnly,
}

impl RefreshMode {
    const fn includes(self, state: BalanceState) -> bool {
        match self {
            Self::All => true,
            Self::StaleOnly => matches!(state, BalanceState::Stale),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TokenTransfer {
    token: Address,
    from: Address,
    to: Address,
    value: U256,
}

fn parse_transfer(log: &Log) -> Option<TokenTransfer> {
    let topics = log.topics();
    if topics.len() < 3 || topics[0] != transfer_event_signature() {
        return None;
    }
    let data = log.data.data.as_ref();
    if data.len() < 32 {
        return None;
    }

    Some(TokenTransfer {
        token: log.address,
        from: Address::from_word(topics[1]),
        to: Address::from_word(topics[2]),
        value: U256::from_be_slice(&data[..32]),
    })
}

fn transfer_event_signature() -> B256 {
    keccak256(b"Transfer(address,address,uint256)")
}

pub(crate) fn tracked_tokens_by_pool(
    graph: &AmmGraph,
    scope: LiquidityIndexScope,
) -> HashMap<PoolKey, HashSet<Address>> {
    match scope {
        LiquidityIndexScope::ParallelEdgeOutputs => parallel_output_tokens_by_pool(graph),
        LiquidityIndexScope::AllDirectedEdgeInputsAndOutputs => {
            all_directed_edge_tokens_by_pool(graph)
        }
    }
}

fn parallel_output_tokens_by_pool(graph: &AmmGraph) -> HashMap<PoolKey, HashSet<Address>> {
    let mut groups = HashMap::<(Address, Address), Vec<PoolKey>>::new();
    for edge in graph.graph().edge_references() {
        let Some(token_in) = graph.node_token(edge.source()) else {
            continue;
        };
        let Some(token_out) = graph.node_token(edge.target()) else {
            continue;
        };
        groups
            .entry((token_in, token_out))
            .or_default()
            .push(edge.weight().pool.clone());
    }

    let mut output_tokens = HashMap::<PoolKey, HashSet<Address>>::new();
    for ((_, token_out), pools) in groups {
        if pools.len() < PARALLEL_EDGE_THRESHOLD {
            continue;
        }
        for pool in pools {
            output_tokens.entry(pool).or_default().insert(token_out);
        }
    }
    output_tokens
}

fn all_directed_edge_tokens_by_pool(graph: &AmmGraph) -> HashMap<PoolKey, HashSet<Address>> {
    let mut tokens_by_pool = HashMap::<PoolKey, HashSet<Address>>::new();
    for edge in graph.graph().edge_references() {
        let Some(token_in) = graph.node_token(edge.source()) else {
            continue;
        };
        let Some(token_out) = graph.node_token(edge.target()) else {
            continue;
        };
        let tokens = tokens_by_pool
            .entry(edge.weight().pool.clone())
            .or_default();
        tokens.insert(token_in);
        tokens.insert(token_out);
    }
    tokens_by_pool
}

fn normalize_tokens(mut tokens: Vec<Address>) -> Vec<Address> {
    tokens.sort_unstable_by(|left, right| left.as_slice().cmp(right.as_slice()));
    tokens.dedup();
    tokens
}

fn decode_balancer_cash(word: U256, high_field: bool) -> U256 {
    let mask = (U256::from(1_u8) << BALANCER_CASH_BITS) - U256::from(1_u8);
    if high_field {
        (word >> BALANCER_CASH_BITS) & mask
    } else {
        word & mask
    }
}

fn storage_sync_error_to_string(error: &StorageSyncError) -> String {
    error.to_string()
}
