//! HTTP and runtime composition for the standalone AMM routing sidecar.
//!
//! This package deliberately sits outside `evm-amm-search`'s library modules.
//! It composes the search crate with `evm-amm-state`, provider transports, and
//! an HTTP boundary without making deployment concerns part of the core API.

pub mod api;
pub mod config;
pub mod coverage;
pub mod execution;
pub mod graph_index;
pub mod node;

/// Sidecar package version embedded by Cargo.
pub const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Source revision embedded by the release builder, or `unknown` for ordinary
/// local Cargo builds.
pub const SOURCE_REVISION: &str = match option_env!("EVM_AMM_ROUTE_SOURCE_REVISION") {
    Some(revision) => revision,
    None => "unknown",
};
