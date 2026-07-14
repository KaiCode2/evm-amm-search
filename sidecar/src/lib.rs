//! HTTP and runtime composition for the standalone AMM routing sidecar.
//!
//! This package deliberately sits outside `evm-amm-search`'s library modules.
//! It composes the search crate with `evm-amm-state`, provider transports, and
//! an HTTP boundary without making deployment concerns part of the core API.

pub mod api;
pub mod config;
pub mod coverage;
pub mod node;
