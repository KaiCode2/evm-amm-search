//! Immutable request-path metadata for the live routing graph.

use std::collections::{HashMap, HashSet};

use alloy_primitives::Address;
use evm_amm_search::{AmmGraph, GraphDelta};
use evm_amm_state::adapters::{AmmStateVersion, PoolKey};
use petgraph::visit::{EdgeRef, IntoEdgeReferences};

/// Constant-time graph counters and token membership used by HTTP admission.
#[derive(Clone, Debug)]
pub struct GraphIndex {
    state_version: AmmStateVersion,
    edges: usize,
    pool_tokens: HashMap<PoolKey, Vec<Address>>,
    token_pools: HashMap<Address, usize>,
}

impl GraphIndex {
    /// Build the index once from the exact graph used by the route runtime.
    pub fn from_graph(graph: &AmmGraph, state_version: AmmStateVersion) -> Self {
        let mut tokens_by_pool = HashMap::<PoolKey, HashSet<Address>>::new();
        for edge in graph.graph().edge_references() {
            let tokens = tokens_by_pool
                .entry(edge.weight().pool.clone())
                .or_default();
            if let Some(token) = graph.node_token(edge.source()) {
                tokens.insert(token);
            }
            if let Some(token) = graph.node_token(edge.target()) {
                tokens.insert(token);
            }
        }
        let pool_tokens = tokens_by_pool
            .into_iter()
            .map(|(pool, tokens)| (pool, canonical_tokens(tokens)))
            .collect::<HashMap<_, _>>();
        let token_pools = token_pool_counts(&pool_tokens);
        Self {
            state_version,
            edges: graph.edge_count(),
            pool_tokens,
            token_pools,
        }
    }

    /// AMM state version whose searchable topology this index represents.
    pub const fn state_version(&self) -> AmmStateVersion {
        self.state_version
    }

    /// Replace the index from a newer coherent route graph.
    pub fn reconcile_graph(&mut self, graph: &AmmGraph, state_version: AmmStateVersion) {
        *self = Self::from_graph(graph, state_version);
    }

    /// Apply one contiguous route-runtime topology delta without scanning the graph.
    ///
    /// Returns `false` for an already represented stale/duplicate delta.
    pub fn apply_delta(&mut self, delta: &GraphDelta) -> Result<bool, GraphIndexError> {
        let actual = delta.source_state_version();
        if actual <= self.state_version {
            return Ok(false);
        }
        let expected = self
            .state_version
            .get()
            .checked_add(1)
            .map(AmmStateVersion::new)
            .ok_or(GraphIndexError::VersionExhausted)?;
        if actual != expected {
            return Err(GraphIndexError::NonContiguous { expected, actual });
        }

        let mut next_edges = self.edges;
        let mut next_pools = HashMap::<PoolKey, Option<Vec<Address>>>::new();
        let mut next_token_counts = HashMap::<Address, usize>::new();
        for change in delta.pool_changes() {
            let current = next_pools
                .get(change.pool())
                .cloned()
                .unwrap_or_else(|| self.pool_tokens.get(change.pool()).cloned());
            let before = change
                .before()
                .map(|pool| canonical_token_slice(pool.tokens()));
            if current != before {
                return Err(GraphIndexError::IncoherentPool {
                    pool: format!("{:?}", change.pool()),
                });
            }
            if let Some(before) = &before {
                next_edges = next_edges
                    .checked_sub(directed_edge_count(before.len())?)
                    .ok_or_else(|| GraphIndexError::IncoherentPool {
                        pool: format!("{:?}", change.pool()),
                    })?;
                update_token_counts(
                    &self.token_pools,
                    &mut next_token_counts,
                    before,
                    TokenCountChange::Remove,
                )?;
            }
            let after = change
                .after()
                .map(|pool| canonical_token_slice(pool.tokens()));
            if let Some(after) = &after {
                next_edges = next_edges
                    .checked_add(directed_edge_count(after.len())?)
                    .ok_or(GraphIndexError::EdgeCountOverflow)?;
                update_token_counts(
                    &self.token_pools,
                    &mut next_token_counts,
                    after,
                    TokenCountChange::Add,
                )?;
            }
            next_pools.insert(change.pool().clone(), after);
        }

        for (pool, tokens) in next_pools {
            match tokens {
                Some(tokens) => {
                    self.pool_tokens.insert(pool, tokens);
                }
                None => {
                    self.pool_tokens.remove(&pool);
                }
            }
        }
        for (token, count) in next_token_counts {
            if count == 0 {
                self.token_pools.remove(&token);
            } else {
                self.token_pools.insert(token, count);
            }
        }
        self.edges = next_edges;
        self.state_version = actual;
        Ok(true)
    }

    /// Return constant-time global and optional token-scoped graph metadata.
    pub fn stats(&self, token: Option<Address>) -> GraphStats {
        GraphStats {
            tokens: self.token_pools.len(),
            edges: self.edges,
            pools: self.pool_tokens.len(),
            token_present: token.is_some_and(|token| self.token_pools.contains_key(&token)),
            token_pools: token
                .and_then(|token| self.token_pools.get(&token).copied())
                .unwrap_or_default(),
        }
    }
}

fn canonical_tokens(tokens: HashSet<Address>) -> Vec<Address> {
    let mut tokens = tokens.into_iter().collect::<Vec<_>>();
    tokens.sort_unstable_by(|left, right| left.as_slice().cmp(right.as_slice()));
    tokens
}

fn canonical_token_slice(tokens: &[Address]) -> Vec<Address> {
    let mut tokens = tokens.to_vec();
    tokens.sort_unstable_by(|left, right| left.as_slice().cmp(right.as_slice()));
    tokens.dedup();
    tokens
}

fn directed_edge_count(tokens: usize) -> Result<usize, GraphIndexError> {
    tokens
        .checked_mul(tokens.saturating_sub(1))
        .ok_or(GraphIndexError::EdgeCountOverflow)
}

#[derive(Clone, Copy)]
enum TokenCountChange {
    Add,
    Remove,
}

fn update_token_counts(
    current: &HashMap<Address, usize>,
    next: &mut HashMap<Address, usize>,
    tokens: &[Address],
    change: TokenCountChange,
) -> Result<(), GraphIndexError> {
    for token in tokens {
        let count = next
            .entry(*token)
            .or_insert_with(|| current.get(token).copied().unwrap_or_default());
        *count = match change {
            TokenCountChange::Add => count
                .checked_add(1)
                .ok_or(GraphIndexError::TokenPoolCountOverflow)?,
            TokenCountChange::Remove => {
                count
                    .checked_sub(1)
                    .ok_or_else(|| GraphIndexError::IncoherentPool {
                        pool: format!("token {token}"),
                    })?
            }
        };
    }
    Ok(())
}

fn token_pool_counts(pool_tokens: &HashMap<PoolKey, Vec<Address>>) -> HashMap<Address, usize> {
    let mut counts = HashMap::new();
    for tokens in pool_tokens.values() {
        for token in tokens {
            *counts.entry(*token).or_default() += 1;
        }
    }
    counts
}

/// Observable graph metadata returned by [`GraphIndex`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GraphStats {
    tokens: usize,
    edges: usize,
    pools: usize,
    token_present: bool,
    token_pools: usize,
}

impl GraphStats {
    pub const fn tokens(self) -> usize {
        self.tokens
    }

    pub const fn edges(self) -> usize {
        self.edges
    }

    pub const fn pools(self) -> usize {
        self.pools
    }

    pub const fn token_present(self) -> bool {
        self.token_present
    }

    pub const fn token_pools(self) -> usize {
        self.token_pools
    }
}

/// A graph delta could not be applied safely to the request-path index.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum GraphIndexError {
    #[error("graph index expected state version {expected:?}, received {actual:?}")]
    NonContiguous {
        expected: AmmStateVersion,
        actual: AmmStateVersion,
    },
    #[error("graph index is incoherent for pool {pool}")]
    IncoherentPool { pool: String },
    #[error("graph index state version exhausted")]
    VersionExhausted,
    #[error("graph index directed-edge count overflowed")]
    EdgeCountOverflow,
    #[error("graph index token-pool count overflowed")]
    TokenPoolCountOverflow,
}
