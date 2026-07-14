use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::Address;
use evm_amm_state::adapters::{
    AdapterRegistry, PoolKey, PoolRegistration, PoolStatus, ProtocolMetadata,
};
use petgraph::{
    Direction,
    graph::{EdgeIndex, NodeIndex},
    stable_graph,
};

/// Monotonic identity of one token-graph topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GraphVersion {
    lineage: u64,
    revision: u64,
}

static NEXT_GRAPH_LINEAGE: AtomicU64 = AtomicU64::new(1);

impl GraphVersion {
    /// Initial graph version produced by a full build.
    pub fn initial() -> Self {
        let lineage = NEXT_GRAPH_LINEAGE
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |lineage| {
                lineage.checked_add(1)
            })
            .expect("AMM graph lineage exhausted");
        Self {
            lineage,
            revision: 0,
        }
    }

    /// Process-unique graph lineage.
    pub const fn lineage(self) -> u64 {
        self.lineage
    }

    /// Monotonic topology revision within this lineage.
    pub const fn revision(self) -> u64 {
        self.revision
    }

    /// Return the next revision in this graph lineage.
    pub const fn checked_next(self) -> Option<Self> {
        match self.revision.checked_add(1) {
            Some(revision) => Some(Self {
                lineage: self.lineage,
                revision,
            }),
            None => None,
        }
    }

    fn next(self) -> Self {
        self.checked_next().expect("AMM graph version exhausted")
    }
}

impl Default for GraphVersion {
    fn default() -> Self {
        Self::initial()
    }
}

/// Observable result of applying one pool registration to an existing graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphPoolMutation {
    /// The registration already has exactly the indexed topology.
    Unchanged,
    /// A newly searchable pool was indexed.
    Added {
        /// Directed edges added for the pool.
        directed_edges: usize,
    },
    /// Existing pool token metadata changed and its edges were replaced.
    Updated {
        /// Directed edges removed from the previous topology.
        removed_edges: usize,
        /// Directed edges added for the replacement topology.
        added_edges: usize,
    },
    /// A no-longer-searchable pool was removed.
    Removed {
        /// Directed edges removed for the pool.
        directed_edges: usize,
    },
    /// The registration was not indexable and had no existing topology.
    Skipped {
        /// Why the registration could not be indexed.
        reason: SkippedPoolReason,
    },
}

impl GraphPoolMutation {
    /// Whether this mutation changed graph topology and advanced its version.
    pub const fn topology_changed(&self) -> bool {
        matches!(
            self,
            Self::Added { .. } | Self::Updated { .. } | Self::Removed { .. }
        )
    }
}

/// Stable directed graph used by the search layer.
pub type StableAmmDiGraph = stable_graph::StableDiGraph<Address, EdgeData>;

/// Data carried by each directed AMM edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeData {
    /// The pool that can execute this directed token pair.
    pub pool: PoolKey,
}

impl EdgeData {
    /// Construct edge data for `pool`.
    pub fn new(pool: PoolKey) -> Self {
        Self { pool }
    }
}

/// Options for building an [`AmmGraph`] from an adapter registry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GraphBuildOptions {
    include_degraded: bool,
}

impl GraphBuildOptions {
    /// Index both ready and degraded pools.
    pub fn include_degraded() -> Self {
        Self {
            include_degraded: true,
        }
    }

    /// Return whether degraded pools are indexed.
    pub fn indexes_degraded(self) -> bool {
        self.include_degraded
    }
}

/// Result of building an [`AmmGraph`].
#[derive(Clone, Debug)]
pub struct GraphBuildReport {
    /// The constructed graph.
    pub graph: AmmGraph,
    /// Pool keys that were indexed into the graph.
    pub indexed_pools: Vec<PoolKey>,
    /// Pools that were skipped and the reason each was skipped.
    pub skipped_pools: Vec<SkippedPool>,
}

/// Result of rebuilding an existing [`AmmGraph`] in place.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GraphBuildSummary {
    /// Pool keys that were indexed into the graph.
    pub indexed_pools: Vec<PoolKey>,
    /// Pools that were skipped and the reason each was skipped.
    pub skipped_pools: Vec<SkippedPool>,
}

/// A skipped pool and its skip reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkippedPool {
    /// The skipped pool key.
    pub pool: PoolKey,
    /// Why the pool was skipped.
    pub reason: SkippedPoolReason,
}

/// Reason a pool was not indexed into the graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkippedPoolReason {
    /// The pool status is not searchable under the chosen build options.
    Status(PoolStatus),
    /// Required token metadata was absent.
    MissingMetadata(&'static str),
    /// The metadata kind has no v1 graph extraction support.
    UnsupportedMetadata,
    /// Fewer than two distinct tokens were available.
    TooFewTokens {
        /// The number of distinct tokens found.
        count: usize,
    },
}

/// Stable token graph over AMM pool edges.
#[derive(Clone, Debug, Default)]
pub struct AmmGraph {
    graph: StableAmmDiGraph,
    node_map: HashMap<Address, NodeIndex>,
    edge_map: HashMap<PoolKey, Vec<EdgeIndex>>,
    pool_tokens: HashMap<PoolKey, Vec<Address>>,
    pair_pools: HashMap<(Address, Address), HashSet<PoolKey>>,
    version: GraphVersion,
}

impl AmmGraph {
    /// Construct an empty graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a graph from the pools currently registered in `registry`.
    pub fn from_registry(
        registry: &AdapterRegistry,
        options: GraphBuildOptions,
    ) -> GraphBuildReport {
        let mut graph = Self::new();
        let mut indexed_pools = Vec::new();
        let mut skipped_pools = Vec::new();

        let mut pools: Vec<&PoolRegistration> = registry.pools().collect();
        pools.sort_by_key(|pool| pool_key_sort_key(&pool.key));

        for pool in pools {
            if !is_indexable_status(pool.status, options) {
                skipped_pools.push(SkippedPool {
                    pool: pool.key.clone(),
                    reason: SkippedPoolReason::Status(pool.status),
                });
                continue;
            }

            let tokens = match tokens_for_pool(pool) {
                Ok(tokens) => tokens,
                Err(reason) => {
                    skipped_pools.push(SkippedPool {
                        pool: pool.key.clone(),
                        reason,
                    });
                    continue;
                }
            };

            graph.add_pool_edges(pool.key.clone(), &tokens);
            indexed_pools.push(pool.key.clone());
        }

        GraphBuildReport {
            graph,
            indexed_pools,
            skipped_pools,
        }
    }

    /// Rebuild this graph in place from `registry`, compacting any orphan nodes.
    pub fn rebuild_from_registry(
        &mut self,
        registry: &AdapterRegistry,
        options: GraphBuildOptions,
    ) -> GraphBuildSummary {
        let mut report = Self::from_registry(registry, options);
        let same_observable_topology = self.pool_tokens == report.graph.pool_tokens
            && self.node_map.len() == report.graph.node_map.len()
            && self
                .node_map
                .keys()
                .all(|token| report.graph.node_map.contains_key(token));
        report.graph.version = if same_observable_topology {
            self.version
        } else {
            self.version.next()
        };
        *self = report.graph;
        GraphBuildSummary {
            indexed_pools: report.indexed_pools,
            skipped_pools: report.skipped_pools,
        }
    }

    /// Remove every edge for `pool`, leaving token nodes in place.
    pub fn remove_pool(&mut self, pool: &PoolKey) -> usize {
        let (removed, _) = self.remove_pool_edges(pool);
        if removed > 0 {
            self.version = self.version.next();
        }
        removed
    }

    /// Apply one current registration without scanning unrelated pools.
    pub fn apply_pool(
        &mut self,
        registration: &PoolRegistration,
        options: GraphBuildOptions,
    ) -> GraphPoolMutation {
        let tokens = if is_indexable_status(registration.status, options) {
            match tokens_for_pool(registration) {
                Ok(tokens) => tokens,
                Err(reason) => {
                    return self.remove_or_skip(&registration.key, reason);
                }
            }
        } else {
            return self.remove_or_skip(
                &registration.key,
                SkippedPoolReason::Status(registration.status),
            );
        };

        match self.pool_tokens.get(&registration.key) {
            Some(current) if current == &tokens => GraphPoolMutation::Unchanged,
            Some(_) => {
                let (removed_edges, old_tokens) = self.remove_pool_edges(&registration.key);
                self.prune_orphan_tokens(&old_tokens);
                self.add_pool_edges(registration.key.clone(), &tokens);
                self.version = self.version.next();
                GraphPoolMutation::Updated {
                    removed_edges,
                    added_edges: directed_edge_count(tokens.len()),
                }
            }
            None => {
                self.add_pool_edges(registration.key.clone(), &tokens);
                self.version = self.version.next();
                GraphPoolMutation::Added {
                    directed_edges: directed_edge_count(tokens.len()),
                }
            }
        }
    }

    /// Remove one pool and compact token nodes that no remaining edge uses.
    pub fn remove_pool_compacting(&mut self, pool: &PoolKey) -> GraphPoolMutation {
        let (directed_edges, tokens) = self.remove_pool_edges(pool);
        if directed_edges == 0 {
            return GraphPoolMutation::Unchanged;
        }
        self.prune_orphan_tokens(&tokens);
        self.version = self.version.next();
        GraphPoolMutation::Removed { directed_edges }
    }

    /// Current topology version.
    pub const fn version(&self) -> GraphVersion {
        self.version
    }

    #[cfg(feature = "live-runtime")]
    pub(crate) const fn set_version(&mut self, version: GraphVersion) {
        self.version = version;
    }

    /// Borrow the underlying stable directed graph.
    pub fn graph(&self) -> &StableAmmDiGraph {
        &self.graph
    }

    /// The number of token nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// The number of directed AMM edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Return the graph node for `token`, if indexed.
    pub fn node_index(&self, token: &Address) -> Option<NodeIndex> {
        self.node_map.get(token).copied()
    }

    /// Return the token stored at `node`, if the node still exists.
    pub fn node_token(&self, node: NodeIndex) -> Option<Address> {
        self.graph.node_weight(node).copied()
    }

    /// Return the live edge indices for `pool`.
    pub fn edges_for_pool(&self, pool: &PoolKey) -> Vec<EdgeIndex> {
        self.edge_map
            .get(pool)
            .map(|edges| {
                edges
                    .iter()
                    .copied()
                    .filter(|edge| self.graph.edge_weight(*edge).is_some())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn tracked_tokens_for_pool(
        &self,
        pool: &PoolKey,
        parallel_only: bool,
    ) -> HashSet<Address> {
        let Some(tokens) = self.pool_tokens.get(pool) else {
            return HashSet::new();
        };
        if !parallel_only {
            return tokens.iter().copied().collect();
        }
        let mut tracked = HashSet::new();
        for_each_token_pair(tokens, |pair| {
            if self
                .pair_pools
                .get(&pair)
                .is_some_and(|pools| pools.len() >= 2)
            {
                tracked.insert(pair.0);
                tracked.insert(pair.1);
            }
        });
        tracked
    }

    #[cfg(feature = "live-runtime")]
    pub(crate) fn parallel_neighbors(&self, pool: &PoolKey) -> HashSet<PoolKey> {
        let Some(tokens) = self.pool_tokens.get(pool) else {
            return HashSet::new();
        };
        let mut neighbors = HashSet::new();
        for_each_token_pair(tokens, |pair| {
            if let Some(pools) = self.pair_pools.get(&pair) {
                neighbors.extend(pools.iter().cloned());
            }
        });
        neighbors
    }

    fn get_or_create_node(&mut self, token: Address) -> NodeIndex {
        if let Some(node) = self.node_map.get(&token) {
            return *node;
        }

        let node = self.graph.add_node(token);
        self.node_map.insert(token, node);
        node
    }

    fn add_pool_edges(&mut self, pool: PoolKey, tokens: &[Address]) {
        let nodes: Vec<NodeIndex> = tokens
            .iter()
            .copied()
            .map(|token| self.get_or_create_node(token))
            .collect();

        for (from_idx, from_node) in nodes.iter().enumerate() {
            for (to_idx, to_node) in nodes.iter().enumerate() {
                if from_idx == to_idx {
                    continue;
                }

                let edge = self
                    .graph
                    .add_edge(*from_node, *to_node, EdgeData::new(pool.clone()));
                self.edge_map.entry(pool.clone()).or_default().push(edge);
            }
        }
        self.add_pool_pairs(&pool, tokens);
        self.pool_tokens.insert(pool, tokens.to_vec());
    }

    fn remove_or_skip(&mut self, pool: &PoolKey, reason: SkippedPoolReason) -> GraphPoolMutation {
        match self.remove_pool_compacting(pool) {
            GraphPoolMutation::Removed { directed_edges } => {
                GraphPoolMutation::Removed { directed_edges }
            }
            GraphPoolMutation::Unchanged => GraphPoolMutation::Skipped { reason },
            _ => unreachable!("pool removal has only removed or unchanged outcomes"),
        }
    }

    fn remove_pool_edges(&mut self, pool: &PoolKey) -> (usize, Vec<Address>) {
        let tokens = self.pool_tokens.remove(pool).unwrap_or_default();
        self.remove_pool_pairs(pool, &tokens);
        let Some(edges) = self.edge_map.remove(pool) else {
            return (0, tokens);
        };
        let removed = edges.len();
        for edge in edges {
            self.graph.remove_edge(edge);
        }
        (removed, tokens)
    }

    fn add_pool_pairs(&mut self, pool: &PoolKey, tokens: &[Address]) {
        for_each_token_pair(tokens, |pair| {
            self.pair_pools
                .entry(pair)
                .or_default()
                .insert(pool.clone());
        });
    }

    fn remove_pool_pairs(&mut self, pool: &PoolKey, tokens: &[Address]) {
        for_each_token_pair(tokens, |pair| {
            let remove_pair = self.pair_pools.get_mut(&pair).is_some_and(|pools| {
                pools.remove(pool);
                pools.is_empty()
            });
            if remove_pair {
                self.pair_pools.remove(&pair);
            }
        });
    }

    fn prune_orphan_tokens(&mut self, tokens: &[Address]) {
        for token in tokens {
            let Some(node) = self.node_map.get(token).copied() else {
                continue;
            };
            let has_edges = self
                .graph
                .edges_directed(node, Direction::Outgoing)
                .next()
                .is_some()
                || self
                    .graph
                    .edges_directed(node, Direction::Incoming)
                    .next()
                    .is_some();
            if !has_edges {
                self.graph.remove_node(node);
                self.node_map.remove(token);
            }
        }
    }
}

const fn directed_edge_count(tokens: usize) -> usize {
    tokens.saturating_mul(tokens.saturating_sub(1))
}

fn for_each_token_pair(tokens: &[Address], mut apply: impl FnMut((Address, Address))) {
    for (left_index, left) in tokens.iter().copied().enumerate() {
        for right in tokens.iter().copied().skip(left_index + 1) {
            apply((left, right));
        }
    }
}

pub(crate) fn is_indexable_status(status: PoolStatus, options: GraphBuildOptions) -> bool {
    status == PoolStatus::Ready || (options.indexes_degraded() && status == PoolStatus::Degraded)
}

pub(crate) fn tokens_for_pool(pool: &PoolRegistration) -> Result<Vec<Address>, SkippedPoolReason> {
    let tokens = match &pool.metadata {
        ProtocolMetadata::UniswapV2(metadata) => {
            pair_tokens(metadata.token0, metadata.token1, "Uniswap V2 token0/token1")?
        }
        ProtocolMetadata::UniswapV3(metadata)
        | ProtocolMetadata::PancakeV3(metadata)
        | ProtocolMetadata::Slipstream(metadata) => {
            pair_tokens(metadata.token0, metadata.token1, "V3-family token0/token1")?
        }
        ProtocolMetadata::SolidlyV2(metadata) => {
            pair_tokens(metadata.token0, metadata.token1, "Solidly V2 token0/token1")?
        }
        ProtocolMetadata::BalancerV2(metadata) => metadata.tokens.clone(),
        ProtocolMetadata::Curve(metadata) => metadata.coins.clone(),
        ProtocolMetadata::Unknown | ProtocolMetadata::Custom(_) => {
            return Err(SkippedPoolReason::UnsupportedMetadata);
        }
        #[allow(unreachable_patterns)]
        _ => return Err(SkippedPoolReason::UnsupportedMetadata),
    };

    normalize_tokens(tokens)
}

fn pair_tokens(
    token0: Option<Address>,
    token1: Option<Address>,
    field: &'static str,
) -> Result<Vec<Address>, SkippedPoolReason> {
    match (token0, token1) {
        (Some(token0), Some(token1)) => Ok(vec![token0, token1]),
        _ => Err(SkippedPoolReason::MissingMetadata(field)),
    }
}

fn normalize_tokens(mut tokens: Vec<Address>) -> Result<Vec<Address>, SkippedPoolReason> {
    tokens.sort_unstable_by(|left, right| left.as_slice().cmp(right.as_slice()));
    tokens.dedup();

    if tokens.len() < 2 {
        return Err(SkippedPoolReason::TooFewTokens {
            count: tokens.len(),
        });
    }

    Ok(tokens)
}

fn pool_key_sort_key(pool: &PoolKey) -> String {
    format!("{pool:?}")
}
