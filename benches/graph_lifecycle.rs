//! Fully offline baseline for graph construction and mid-lifecycle mutation.
//!
//! The full-rebuild add workload is retained as the Stage 0 baseline. Stage 7
//! adds bounded incremental add/remove workloads against the same registries.
//!
//! ```text
//! cargo bench --bench graph_lifecycle
//! ```

use alloy_primitives::Address;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use evm_amm_search::{AmmGraph, GraphBuildOptions};
use evm_amm_state::adapters::{
    AdapterRegistry, PoolKey, PoolRegistration, PoolStatus, ProtocolMetadata, UniswapV2Metadata,
};

const EXISTING_POOL_COUNTS: [usize; 3] = [16, 64, 320];

fn address(value: u64) -> Address {
    let mut bytes = [0_u8; 20];
    bytes[12..].copy_from_slice(&value.to_be_bytes());
    Address::from(bytes)
}

fn pool(index: usize) -> PoolRegistration {
    let pool = address(index as u64 + 1);
    let token0 = address(10_000 + (index % 32) as u64);
    let token1 = address(10_000 + ((index + 1) % 32) as u64);
    PoolRegistration::new(PoolKey::UniswapV2(pool))
        .with_metadata(ProtocolMetadata::UniswapV2(
            UniswapV2Metadata::default()
                .with_token0(token0)
                .with_token1(token1)
                .with_fee_bps(30),
        ))
        .with_status(PoolStatus::Ready)
}

fn registry_with_pools(count: usize) -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    for index in 0..count {
        registry.register_pool(pool(index)).expect("unique pool");
    }
    registry
}

fn graph_lifecycle(c: &mut Criterion) {
    let mut build = c.benchmark_group("graph_lifecycle/full_build");
    for existing in EXISTING_POOL_COUNTS {
        build.throughput(Throughput::Elements(existing as u64));
        build.bench_with_input(
            BenchmarkId::from_parameter(existing),
            &existing,
            |b, &count| {
                let registry = registry_with_pools(count);
                b.iter_with_large_drop(|| {
                    AmmGraph::from_registry(black_box(&registry), GraphBuildOptions::default())
                });
            },
        );
    }
    build.finish();

    let mut rebuild = c.benchmark_group("graph_lifecycle/rebuild_existing");
    for existing in EXISTING_POOL_COUNTS {
        rebuild.throughput(Throughput::Elements(existing as u64));
        rebuild.bench_with_input(
            BenchmarkId::from_parameter(existing),
            &existing,
            |b, &count| {
                let registry = registry_with_pools(count);
                b.iter_batched(
                    || AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph,
                    |mut graph| {
                        let summary = graph.rebuild_from_registry(
                            black_box(&registry),
                            GraphBuildOptions::default(),
                        );
                        (graph, summary)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    rebuild.finish();

    let mut add = c.benchmark_group("graph_lifecycle/add_one_via_full_rebuild");
    add.throughput(Throughput::Elements(1));
    for existing in EXISTING_POOL_COUNTS {
        add.bench_with_input(
            BenchmarkId::from_parameter(existing),
            &existing,
            |b, &count| {
                b.iter_batched(
                    || {
                        let registry = registry_with_pools(count);
                        let graph =
                            AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
                        (registry, graph, pool(count))
                    },
                    |(mut registry, mut graph, registration)| {
                        registry
                            .register_pool(registration)
                            .expect("register one pool");
                        let summary = graph.rebuild_from_registry(
                            black_box(&registry),
                            GraphBuildOptions::default(),
                        );
                        (registry, graph, summary)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    add.finish();

    let mut incremental_add = c.benchmark_group("graph_lifecycle/add_one_incremental");
    incremental_add.throughput(Throughput::Elements(1));
    for existing in EXISTING_POOL_COUNTS {
        incremental_add.bench_with_input(
            BenchmarkId::from_parameter(existing),
            &existing,
            |b, &count| {
                b.iter_batched(
                    || {
                        let registry = registry_with_pools(count);
                        let graph =
                            AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
                        (graph, pool(count))
                    },
                    |(mut graph, registration)| {
                        let mutation = graph
                            .apply_pool(black_box(&registration), GraphBuildOptions::default());
                        (graph, mutation)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    incremental_add.finish();

    let mut remove = c.benchmark_group("graph_lifecycle/remove_one");
    remove.throughput(Throughput::Elements(1));
    for existing in EXISTING_POOL_COUNTS {
        remove.bench_with_input(
            BenchmarkId::from_parameter(existing),
            &existing,
            |b, &count| {
                b.iter_batched(
                    || {
                        let registry = registry_with_pools(count);
                        let graph =
                            AmmGraph::from_registry(&registry, GraphBuildOptions::default()).graph;
                        (graph, pool(count - 1).key)
                    },
                    |(mut graph, key)| {
                        let mutation = graph.remove_pool_compacting(black_box(&key));
                        (graph, mutation)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    remove.finish();
}

criterion_group!(benches, graph_lifecycle);
criterion_main!(benches);
