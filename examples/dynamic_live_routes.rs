//! Providerless dynamic topology: add/remove pools without reconstructing either runtime.

mod support;

use alloy_primitives::{Address, U256};
use anyhow::Result;
use evm_amm_search::{RouteRequest, RouteSubscriptionSpec, StreamingSearchConfig};
use evm_amm_state::adapters::AmmEvictionPolicy;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tokio::task::LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let token_a = Address::repeat_byte(0x01);
    let token_b = Address::repeat_byte(0x02);
    let token_c = Address::repeat_byte(0x03);
    let direct_key = evm_amm_state::adapters::PoolKey::UniswapV2(Address::repeat_byte(0xac));
    let connector_key = evm_amm_state::adapters::PoolKey::UniswapV2(Address::repeat_byte(0xbc));
    let (runtime, routes) = support::spawn_empty_system(910).await?;
    let initial = runtime.latest_snapshot();
    let runtime_id = initial.runtime_id();
    let adapter = initial
        .registry()
        .adapters()
        .next()
        .expect("route adapter")
        .1
        .clone();

    let direct_version = support::commit_pool(
        &runtime,
        support::ready_pool(
            direct_key.address().expect("address pool"),
            token_a,
            token_c,
        ),
    )
    .await?;
    let mut route = routes
        .subscribe(RouteSubscriptionSpec::new(
            RouteRequest::new(token_a, token_c, U256::from(10_u64)),
            StreamingSearchConfig::default(),
        ))
        .await?;
    let direct = support::wait_ready_at(&mut route, direct_version)
        .await?
        .expect("direct route");
    assert_eq!(direct.quote().path.len(), 1);
    assert_eq!(direct.quote().amount_out, U256::from(20_u64));

    support::commit_pool(
        &runtime,
        support::ready_pool(Address::repeat_byte(0xab), token_a, token_b),
    )
    .await?;
    let connector_version = support::commit_pool(
        &runtime,
        support::ready_pool(
            connector_key.address().expect("address pool"),
            token_b,
            token_c,
        ),
    )
    .await?;
    let connected = support::wait_ready_at(&mut route, connector_version)
        .await?
        .expect("two-hop route");
    assert_eq!(connected.quote().path.len(), 2);
    assert_eq!(connected.quote().amount_out, U256::from(40_u64));
    println!("dynamic add: best route changed from 10 -> 20 to 10 -> 40");

    let connector = runtime
        .latest_snapshot()
        .registry()
        .pool_instance(&connector_key)
        .expect("connector generation")
        .clone();
    let removed = runtime
        .remove_pool(connector, AmmEvictionPolicy::Retain)
        .await?;
    let fallback = support::wait_ready_at(&mut route, removed.version())
        .await?
        .expect("direct fallback route");
    assert_eq!(fallback.quote().path.len(), 1);
    assert_eq!(fallback.quote().amount_out, U256::from(20_u64));
    let final_snapshot = runtime.latest_snapshot();
    assert_eq!(final_snapshot.runtime_id(), runtime_id);
    assert_eq!(final_snapshot.registry().adapter_count(), 1);
    assert_eq!(
        final_snapshot
            .registry()
            .adapters()
            .next()
            .expect("route adapter")
            .1,
        &adapter,
    );
    println!(
        "dynamic remove: best route fell back to 10 -> 20 without rebuilding the runtime or adapter"
    );

    route.cancel().await?;
    routes.shutdown().await?;
    runtime.shutdown().await?;
    Ok(())
}
