use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy_primitives::{Address, U256};
use evm_amm_state::adapters::{
    AmmChangeSet, AmmRuntimeId, AmmStateCommit, AmmStatePoint, AmmStateSnapshot, AmmStateVersion,
    PoolInstanceId, PoolKey, PoolStateRevision,
};

use crate::graph::{is_indexable_status, tokens_for_pool};
use crate::{
    AmmGraph, GraphBuildOptions, GraphVersion, LiquidityIndexScope, PoolLiquidityError,
    PoolLiquidityIndex, PoolLiquidityMutationReport, PoolLiquidityRefreshReport,
};

/// One active pool represented in the live search universe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedPool {
    instance: PoolInstanceId,
    revision: PoolStateRevision,
    tokens: Vec<Address>,
}

impl IndexedPool {
    /// Exact active pool generation.
    pub const fn instance(&self) -> &PoolInstanceId {
        &self.instance
    }

    /// Quote-relevant revision at the source state version.
    pub const fn revision(&self) -> PoolStateRevision {
        self.revision
    }

    /// Canonically sorted distinct graph tokens.
    pub fn tokens(&self) -> &[Address] {
        &self.tokens
    }
}

/// Search-universe consequence of one AMM state commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphTopologyImpact {
    /// Only quote state/revisions changed.
    #[default]
    Unchanged,
    /// A logical pool retained its token edges but changed active generation.
    IdentityChanged,
    /// A bounded set of pool/token edges changed.
    Localized,
    /// The runtime required reconciliation against the complete snapshot.
    FullReconciliation,
}

/// Before/after search representation for one logical pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphPoolDelta {
    pool: PoolKey,
    before: Option<IndexedPool>,
    after: Option<IndexedPool>,
}

impl GraphPoolDelta {
    /// Logical pool key affected by the commit.
    pub const fn pool(&self) -> &PoolKey {
        &self.pool
    }

    /// Search representation before the commit.
    pub const fn before(&self) -> Option<&IndexedPool> {
        self.before.as_ref()
    }

    /// Search representation after the commit.
    pub const fn after(&self) -> Option<&IndexedPool> {
        self.after.as_ref()
    }
}

/// Canonical delta emitted after applying one AMM state commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDelta {
    source_state_version: AmmStateVersion,
    source_point: AmmStatePoint,
    from_graph_version: GraphVersion,
    to_graph_version: GraphVersion,
    impact: GraphTopologyImpact,
    pool_changes: Vec<GraphPoolDelta>,
    liquidity: PoolLiquidityMutationReport,
}

impl GraphDelta {
    /// AMM state version whose snapshot produced this delta.
    pub const fn source_state_version(&self) -> AmmStateVersion {
        self.source_state_version
    }

    /// Complete chain-state point whose snapshot produced this delta.
    pub const fn source_point(&self) -> AmmStatePoint {
        self.source_point
    }

    /// Graph topology version before the commit.
    pub const fn from_graph_version(&self) -> GraphVersion {
        self.from_graph_version
    }

    /// Graph topology version after the commit.
    pub const fn to_graph_version(&self) -> GraphVersion {
        self.to_graph_version
    }

    /// Highest topology impact represented by the commit.
    pub const fn impact(&self) -> GraphTopologyImpact {
        self.impact
    }

    /// Logical pool identity/revision changes represented by the commit.
    pub fn pool_changes(&self) -> &[GraphPoolDelta] {
        &self.pool_changes
    }

    /// Incremental liquidity-target changes applied with this graph delta.
    pub const fn liquidity(&self) -> &PoolLiquidityMutationReport {
        &self.liquidity
    }

    /// Whether topology or an active pool generation changed.
    pub const fn topology_changed(&self) -> bool {
        !matches!(self.impact, GraphTopologyImpact::Unchanged)
    }
}

/// Rejected live graph transition.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum LiveGraphError {
    #[error("non-contiguous AMM state version: expected {expected:?}, received {actual:?}")]
    NonContiguousStateVersion {
        /// State version required by the current graph.
        expected: AmmStateVersion,
        /// State version supplied by the commit.
        actual: AmmStateVersion,
    },
    #[error("state commit snapshot/change version mismatch")]
    CommitVersionMismatch,
    #[error("AMM runtime lineage mismatch: expected {expected:?}, received {actual:?}")]
    RuntimeMismatch {
        /// Runtime lineage used to initialize the live graph.
        expected: AmmRuntimeId,
        /// Runtime lineage carried by the supplied snapshot/commit.
        actual: AmmRuntimeId,
    },
    #[error("snapshot does not advance AMM state: current {current:?}, received {actual:?}")]
    StaleSnapshot {
        /// Current applied AMM state version.
        current: AmmStateVersion,
        /// Equal or older recovery snapshot version.
        actual: AmmStateVersion,
    },
    #[error(
        "snapshot does not match live graph state: expected {expected_version:?} at {expected_point:?}, received {actual_version:?} at {actual_point:?}"
    )]
    SnapshotStateMismatch {
        /// Current applied AMM state version.
        expected_version: AmmStateVersion,
        /// Current applied complete state point.
        expected_point: AmmStatePoint,
        /// Supplied snapshot state version.
        actual_version: AmmStateVersion,
        /// Supplied snapshot complete state point.
        actual_point: AmmStatePoint,
    },
    #[error("active pool has no published revision: {0:?}")]
    MissingPoolRevision(PoolInstanceId),
    #[error("active pool generation has no matching registration: {0:?}")]
    MissingPoolRegistration(PoolInstanceId),
    #[error("AMM graph version exhausted")]
    GraphVersionExhausted,
}

/// Incrementally maintained search universe backed by immutable AMM commits.
#[derive(Clone, Debug)]
pub struct LiveAmmGraph {
    graph: Arc<AmmGraph>,
    runtime_id: AmmRuntimeId,
    state_version: AmmStateVersion,
    point: AmmStatePoint,
    options: GraphBuildOptions,
    liquidity_scope: LiquidityIndexScope,
    liquidity: Arc<PoolLiquidityIndex>,
    indexed: BTreeMap<PoolKey, IndexedPool>,
}

impl LiveAmmGraph {
    /// Build the initial live graph from one coherent AMM snapshot.
    pub fn from_snapshot(
        snapshot: &AmmStateSnapshot,
        options: GraphBuildOptions,
    ) -> Result<Self, LiveGraphError> {
        Self::from_snapshot_with_liquidity_scope(
            snapshot,
            options,
            LiquidityIndexScope::ParallelEdgeOutputs,
        )
    }

    /// Build with an explicit incremental liquidity-target scope.
    pub fn from_snapshot_with_liquidity_scope(
        snapshot: &AmmStateSnapshot,
        options: GraphBuildOptions,
        liquidity_scope: LiquidityIndexScope,
    ) -> Result<Self, LiveGraphError> {
        let graph = AmmGraph::from_registry(snapshot.registry().registry(), options).graph;
        let (liquidity, _) = PoolLiquidityIndex::from_registry_with_scope(
            snapshot.registry().registry(),
            &graph,
            liquidity_scope,
        );
        let indexed = indexed_from_snapshot(snapshot, options)?;
        Ok(Self {
            graph: Arc::new(graph),
            runtime_id: snapshot.runtime_id(),
            state_version: snapshot.version(),
            point: snapshot.point(),
            options,
            liquidity_scope,
            liquidity: Arc::new(liquidity),
            indexed,
        })
    }

    /// Apply one contiguous reliable state commit atomically.
    pub fn apply_commit(&mut self, commit: &AmmStateCommit) -> Result<GraphDelta, LiveGraphError> {
        self.validate_runtime(commit.snapshot())?;
        if commit.snapshot().version() != commit.changes().version()
            || commit.snapshot().point() != commit.changes().point()
        {
            return Err(LiveGraphError::CommitVersionMismatch);
        }
        let expected = self.state_version.checked_next().map_err(|_| {
            LiveGraphError::NonContiguousStateVersion {
                expected: self.state_version,
                actual: commit.snapshot().version(),
            }
        })?;
        if commit.snapshot().version() != expected {
            return Err(LiveGraphError::NonContiguousStateVersion {
                expected,
                actual: commit.snapshot().version(),
            });
        }

        self.apply_snapshot_changes(commit.snapshot(), commit.changes())
    }

    /// Reconcile against a newer snapshot when a reliable consumer recovers.
    pub fn reconcile_snapshot(
        &mut self,
        snapshot: &AmmStateSnapshot,
    ) -> Result<GraphDelta, LiveGraphError> {
        self.validate_runtime(snapshot)?;
        if snapshot.version() <= self.state_version {
            return Err(LiveGraphError::StaleSnapshot {
                current: self.state_version,
                actual: snapshot.version(),
            });
        }
        let next_indexed = indexed_from_snapshot(snapshot, self.options)?;
        let changed_keys = self
            .indexed
            .keys()
            .chain(next_indexed.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        self.replace_from_snapshot(
            snapshot,
            next_indexed,
            changed_keys,
            GraphTopologyImpact::FullReconciliation,
        )
    }

    /// Current immutable-search graph view.
    pub fn graph(&self) -> &AmmGraph {
        self.graph.as_ref()
    }

    /// Cheap immutable graph snapshot for concurrent search workers.
    pub fn graph_snapshot(&self) -> Arc<AmmGraph> {
        Arc::clone(&self.graph)
    }

    /// Incrementally maintained liquidity-target sidecar.
    pub fn liquidity(&self) -> &PoolLiquidityIndex {
        self.liquidity.as_ref()
    }

    /// Cheap immutable liquidity snapshot for concurrent search workers.
    pub fn liquidity_snapshot(&self) -> Arc<PoolLiquidityIndex> {
        Arc::clone(&self.liquidity)
    }

    /// Seed a discovered ERC-20 balance slot without exposing target-topology mutation.
    pub fn set_erc20_liquidity_slot(
        &mut self,
        token: Address,
        holder: Address,
        slot: U256,
    ) -> usize {
        Arc::make_mut(&mut self.liquidity).set_erc20_balance_slot(token, holder, slot)
    }

    /// Mark one already-tracked pool/token balance stale.
    pub fn mark_liquidity_stale(
        &mut self,
        pool: &PoolKey,
        token: Address,
    ) -> Result<(), PoolLiquidityError> {
        Arc::make_mut(&mut self.liquidity).mark_stale(pool, token)
    }

    /// Refresh known liquidity slots from the exact immutable live snapshot.
    pub fn refresh_liquidity_from_snapshot(
        &mut self,
        snapshot: &AmmStateSnapshot,
    ) -> Result<PoolLiquidityRefreshReport, LiveGraphError> {
        self.validate_snapshot(snapshot)?;
        Ok(Arc::make_mut(&mut self.liquidity).refresh_from_snapshot(snapshot.cache()))
    }

    /// Current graph topology version.
    pub fn version(&self) -> GraphVersion {
        self.graph.version()
    }

    /// Process-unique lineage of the source AMM runtime.
    pub const fn runtime_id(&self) -> AmmRuntimeId {
        self.runtime_id
    }

    /// Last applied AMM state version.
    pub const fn state_version(&self) -> AmmStateVersion {
        self.state_version
    }

    /// Complete chain-state point of the current graph snapshot.
    pub const fn point(&self) -> AmmStatePoint {
        self.point
    }

    /// Active generation/revision indexed for one logical pool.
    pub fn indexed_pool(&self, key: &PoolKey) -> Option<&IndexedPool> {
        self.indexed.get(key)
    }

    pub(crate) fn matches_snapshot(&self, snapshot: &AmmStateSnapshot) -> bool {
        self.runtime_id == snapshot.runtime_id()
            && self.state_version == snapshot.version()
            && self.point == snapshot.point()
    }

    fn validate_runtime(&self, snapshot: &AmmStateSnapshot) -> Result<(), LiveGraphError> {
        if snapshot.runtime_id() != self.runtime_id {
            return Err(LiveGraphError::RuntimeMismatch {
                expected: self.runtime_id,
                actual: snapshot.runtime_id(),
            });
        }
        Ok(())
    }

    fn validate_snapshot(&self, snapshot: &AmmStateSnapshot) -> Result<(), LiveGraphError> {
        self.validate_runtime(snapshot)?;
        if snapshot.version() != self.state_version || snapshot.point() != self.point {
            return Err(LiveGraphError::SnapshotStateMismatch {
                expected_version: self.state_version,
                expected_point: self.point,
                actual_version: snapshot.version(),
                actual_point: snapshot.point(),
            });
        }
        Ok(())
    }

    fn apply_snapshot_changes(
        &mut self,
        snapshot: &AmmStateSnapshot,
        changes: &AmmChangeSet,
    ) -> Result<GraphDelta, LiveGraphError> {
        if changes.requires_full_refresh() {
            return self.reconcile_snapshot(snapshot);
        }

        let mut pool_deltas = Vec::new();
        let mut impact = GraphTopologyImpact::Unchanged;

        let changed_keys = changes
            .pool_changes()
            .iter()
            .map(|change| change.pool().key().clone())
            .collect::<BTreeSet<_>>();
        for key in changed_keys {
            let before = self.indexed.get(&key).cloned();
            let after = snapshot
                .registry()
                .pool_instance(&key)
                .map(|instance| indexed_pool(snapshot, instance, self.options))
                .transpose()?
                .flatten();

            impact = impact.max(match (&before, &after) {
                (Some(previous), Some(next)) if previous.tokens() == next.tokens() => {
                    if previous.instance() == next.instance() {
                        GraphTopologyImpact::Unchanged
                    } else {
                        GraphTopologyImpact::IdentityChanged
                    }
                }
                (None, None) => GraphTopologyImpact::Unchanged,
                _ => GraphTopologyImpact::Localized,
            });
            if before != after {
                pool_deltas.push(GraphPoolDelta {
                    pool: key,
                    before,
                    after,
                });
            }
        }

        let from_graph_version = self.graph.version();
        if impact == GraphTopologyImpact::Unchanged {
            apply_index_deltas(&mut self.indexed, &pool_deltas);
            self.state_version = snapshot.version();
            self.point = snapshot.point();
            return Ok(GraphDelta {
                source_state_version: snapshot.version(),
                source_point: snapshot.point(),
                from_graph_version,
                to_graph_version: from_graph_version,
                impact,
                pool_changes: pool_deltas,
                liquidity: PoolLiquidityMutationReport::default(),
            });
        }

        let mut next_graph = self.graph.as_ref().clone();
        let previous_graph = Arc::clone(&self.graph);
        for delta in &pool_deltas {
            match &delta.after {
                Some(indexed) => {
                    let registration =
                        snapshot
                            .registry()
                            .pool(indexed.instance())
                            .ok_or_else(|| {
                                LiveGraphError::MissingPoolRegistration(indexed.instance().clone())
                            })?;
                    next_graph.apply_pool(registration, self.options);
                }
                None => {
                    next_graph.remove_pool_compacting(&delta.pool);
                }
            }
        }
        let to_graph_version = from_graph_version
            .checked_next()
            .ok_or(LiveGraphError::GraphVersionExhausted)?;
        next_graph.set_version(to_graph_version);
        let liquidity_pools = liquidity_affected_pools(
            &previous_graph,
            &next_graph,
            self.liquidity_scope,
            pool_deltas.iter().map(|delta| delta.pool.clone()),
        );
        let mut next_liquidity = self.liquidity.as_ref().clone();
        let liquidity = next_liquidity.reconcile_pools(
            snapshot.registry().registry(),
            &next_graph,
            self.liquidity_scope,
            liquidity_pools,
        );
        self.graph = Arc::new(next_graph);
        self.liquidity = Arc::new(next_liquidity);
        apply_index_deltas(&mut self.indexed, &pool_deltas);
        self.state_version = snapshot.version();
        self.point = snapshot.point();
        Ok(GraphDelta {
            source_state_version: snapshot.version(),
            source_point: snapshot.point(),
            from_graph_version,
            to_graph_version,
            impact,
            pool_changes: pool_deltas,
            liquidity,
        })
    }

    fn replace_from_snapshot(
        &mut self,
        snapshot: &AmmStateSnapshot,
        next_indexed: BTreeMap<PoolKey, IndexedPool>,
        changed_keys: BTreeSet<PoolKey>,
        requested_impact: GraphTopologyImpact,
    ) -> Result<GraphDelta, LiveGraphError> {
        let mut next_graph =
            AmmGraph::from_registry(snapshot.registry().registry(), self.options).graph;
        let mut pool_changes = Vec::new();
        for key in changed_keys {
            let before = self.indexed.get(&key).cloned();
            let after = next_indexed.get(&key).cloned();
            if before != after {
                pool_changes.push(GraphPoolDelta {
                    pool: key,
                    before,
                    after,
                });
            }
        }
        let topology_changed = semantic_index_changed(&self.indexed, &next_indexed);
        let from_graph_version = self.graph.version();
        let to_graph_version = if topology_changed {
            from_graph_version
                .checked_next()
                .ok_or(LiveGraphError::GraphVersionExhausted)?
        } else {
            from_graph_version
        };
        next_graph.set_version(to_graph_version);
        let liquidity_pools = liquidity_affected_pools(
            self.graph.as_ref(),
            &next_graph,
            self.liquidity_scope,
            pool_changes.iter().map(|delta| delta.pool.clone()),
        );
        let mut next_liquidity = self.liquidity.as_ref().clone();
        let liquidity = next_liquidity.reconcile_pools(
            snapshot.registry().registry(),
            &next_graph,
            self.liquidity_scope,
            liquidity_pools,
        );
        self.graph = Arc::new(next_graph);
        self.liquidity = Arc::new(next_liquidity);
        self.indexed = next_indexed;
        self.state_version = snapshot.version();
        self.point = snapshot.point();
        Ok(GraphDelta {
            source_state_version: snapshot.version(),
            source_point: snapshot.point(),
            from_graph_version,
            to_graph_version,
            impact: if topology_changed {
                requested_impact
            } else {
                GraphTopologyImpact::Unchanged
            },
            pool_changes,
            liquidity,
        })
    }
}

fn indexed_from_snapshot(
    snapshot: &AmmStateSnapshot,
    options: GraphBuildOptions,
) -> Result<BTreeMap<PoolKey, IndexedPool>, LiveGraphError> {
    let mut indexed = BTreeMap::new();
    for (key, instance) in snapshot.registry().pools() {
        let registration = snapshot
            .registry()
            .pool(instance)
            .ok_or_else(|| LiveGraphError::MissingPoolRegistration(instance.clone()))?;
        if !is_indexable_status(registration.status, options) {
            continue;
        }
        let Ok(tokens) = tokens_for_pool(registration) else {
            continue;
        };
        let revision = snapshot
            .pool_revision(instance)
            .ok_or_else(|| LiveGraphError::MissingPoolRevision(instance.clone()))?;
        indexed.insert(
            key.clone(),
            IndexedPool {
                instance: instance.clone(),
                revision,
                tokens,
            },
        );
    }
    Ok(indexed)
}

fn indexed_pool(
    snapshot: &AmmStateSnapshot,
    instance: &PoolInstanceId,
    options: GraphBuildOptions,
) -> Result<Option<IndexedPool>, LiveGraphError> {
    let registration = snapshot
        .registry()
        .pool(instance)
        .ok_or_else(|| LiveGraphError::MissingPoolRegistration(instance.clone()))?;
    if !is_indexable_status(registration.status, options) {
        return Ok(None);
    }
    let Some(tokens) = tokens_for_pool(registration).ok() else {
        return Ok(None);
    };
    let revision = snapshot
        .pool_revision(instance)
        .ok_or_else(|| LiveGraphError::MissingPoolRevision(instance.clone()))?;
    Ok(Some(IndexedPool {
        instance: instance.clone(),
        revision,
        tokens,
    }))
}

fn semantic_index_changed(
    before: &BTreeMap<PoolKey, IndexedPool>,
    after: &BTreeMap<PoolKey, IndexedPool>,
) -> bool {
    before.iter().any(|(key, pool)| {
        after
            .get(key)
            .is_none_or(|next| pool.instance != next.instance || pool.tokens != next.tokens)
    }) || after.keys().any(|key| !before.contains_key(key))
}

fn apply_index_deltas(indexed: &mut BTreeMap<PoolKey, IndexedPool>, deltas: &[GraphPoolDelta]) {
    for delta in deltas {
        if let Some(after) = &delta.after {
            indexed.insert(delta.pool.clone(), after.clone());
        } else {
            indexed.remove(&delta.pool);
        }
    }
}

fn liquidity_affected_pools(
    before: &AmmGraph,
    after: &AmmGraph,
    scope: LiquidityIndexScope,
    directly_changed: impl IntoIterator<Item = PoolKey>,
) -> BTreeSet<PoolKey> {
    let mut affected = directly_changed.into_iter().collect::<BTreeSet<_>>();
    if scope == LiquidityIndexScope::ParallelEdgeOutputs {
        let directly_changed = affected.iter().cloned().collect::<Vec<_>>();
        for key in directly_changed {
            affected.extend(before.parallel_neighbors(&key));
            affected.extend(after.parallel_neighbors(&key));
        }
    }
    affected
}
