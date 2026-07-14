use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use alloy_primitives::Address;
use serde::Serialize;
use tokio::sync::RwLock;

/// Observable preparation state for one token.
#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoverageState {
    #[default]
    Unknown,
    Queued,
    Discovering,
    Ready,
    Empty,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct TokenCoverage {
    pub token: String,
    pub state: CoverageState,
    pub configured: bool,
    pub graph_present: bool,
    pub protocols: Vec<String>,
    pub connectors: Vec<String>,
    pub pools: usize,
    pub jobs: usize,
    pub updated_at_unix_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl TokenCoverage {
    fn unknown(token: Address) -> Self {
        Self {
            token: format!("{token:#x}"),
            state: CoverageState::Unknown,
            configured: false,
            graph_present: false,
            protocols: Vec::new(),
            connectors: Vec::new(),
            pools: 0,
            jobs: 0,
            updated_at_unix_ms: now_unix_ms(),
            error: None,
        }
    }
}

/// Service-owned coverage ledger. The AMM runtime remains the source of truth;
/// this ledger records why discovery was requested and exposes it over HTTP.
#[derive(Clone, Default)]
pub struct CoverageLedger {
    entries: Arc<RwLock<BTreeMap<Address, TokenCoverage>>>,
}

impl CoverageLedger {
    pub async fn get(&self, token: Address) -> TokenCoverage {
        self.entries
            .read()
            .await
            .get(&token)
            .cloned()
            .unwrap_or_else(|| TokenCoverage::unknown(token))
    }

    pub async fn all(&self) -> Vec<TokenCoverage> {
        self.entries.read().await.values().cloned().collect()
    }

    pub async fn negative_is_fresh(&self, token: Address, ttl: Duration) -> bool {
        let entry = self.get(token).await;
        entry.state == CoverageState::Empty
            && now_unix_ms().saturating_sub(entry.updated_at_unix_ms) < ttl.as_millis()
    }

    pub async fn mark_configured(&self, token: Address) {
        self.mutate(token, |entry| entry.configured = true).await;
    }

    pub async fn mark_queued(
        &self,
        token: Address,
        protocols: Vec<String>,
        connectors: Vec<Address>,
        jobs: usize,
    ) {
        self.mutate(token, |entry| {
            entry.state = CoverageState::Queued;
            entry.protocols = protocols;
            entry.connectors = connectors
                .into_iter()
                .map(|connector| format!("{connector:#x}"))
                .collect();
            entry.jobs = jobs;
            entry.error = None;
        })
        .await;
    }

    pub async fn mark_discovering(&self, token: Address) {
        self.mutate(token, |entry| entry.state = CoverageState::Discovering)
            .await;
    }

    pub async fn mark_settled(&self, token: Address, pools: usize, graph_present: bool) {
        self.mutate(token, |entry| {
            entry.pools = pools;
            entry.graph_present = graph_present;
            entry.state = if graph_present {
                CoverageState::Ready
            } else {
                CoverageState::Empty
            };
        })
        .await;
    }

    pub async fn mark_failed(&self, token: Address, error: impl Into<String>) {
        let error = error.into();
        self.mutate(token, |entry| {
            entry.state = CoverageState::Failed;
            entry.error = Some(error);
        })
        .await;
    }

    pub async fn refresh_graph_state(&self, token: Address, pools: usize, graph_present: bool) {
        self.mutate(token, |entry| {
            entry.pools = pools;
            entry.graph_present = graph_present;
            if graph_present && matches!(entry.state, CoverageState::Unknown | CoverageState::Empty)
            {
                entry.state = CoverageState::Ready;
            }
        })
        .await;
    }

    async fn mutate(&self, token: Address, f: impl FnOnce(&mut TokenCoverage)) {
        let mut entries = self.entries.write().await;
        let entry = entries
            .entry(token)
            .or_insert_with(|| TokenCoverage::unknown(token));
        f(entry);
        entry.updated_at_unix_ms = now_unix_ms();
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ledger_tracks_idempotent_token_state() {
        let ledger = CoverageLedger::default();
        let token = Address::repeat_byte(1);
        ledger.mark_configured(token).await;
        ledger
            .mark_queued(token, vec!["uniswap_v2".into()], Vec::new(), 1)
            .await;
        ledger.mark_settled(token, 2, true).await;
        let state = ledger.get(token).await;
        assert!(state.configured);
        assert_eq!(state.state, CoverageState::Ready);
        assert_eq!(state.pools, 2);
    }
}
