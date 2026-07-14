//! Providerless progressive loading: a route becomes usable as its last pool commits.

mod support;

use alloy_primitives::{Address, U256};
use anyhow::Result;
use evm_amm_search::{RouteRequest, RouteSubscriptionSpec, StreamingSearchConfig};
use evm_amm_state::adapters::AmmStateVersion;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tokio::task::LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let token_a = Address::repeat_byte(0x01);
    let token_b = Address::repeat_byte(0x02);
    let token_c = Address::repeat_byte(0x03);
    let (runtime, routes) = support::spawn_empty_system(900).await?;
    let mut route = routes
        .subscribe(RouteSubscriptionSpec::new(
            RouteRequest::new(token_a, token_c, U256::from(10_u64)),
            StreamingSearchConfig::default(),
        ))
        .await?;

    println!(
        "startup at version {}: the empty runtime and route subscription are already responsive",
        AmmStateVersion::new(0).get()
    );

    let first = support::commit_pool(
        &runtime,
        support::ready_pool(Address::repeat_byte(0xa1), token_a, token_b),
    )
    .await?;
    println!(
        "version {}: A -> B committed independently; A -> C still lacks its second edge",
        first.get()
    );

    let second = support::commit_pool(
        &runtime,
        support::ready_pool(Address::repeat_byte(0xb2), token_b, token_c),
    )
    .await?;
    let quote = support::wait_ready_at(&mut route, second)
        .await?
        .expect("the second independently committed pool completes the route");
    assert_eq!(quote.quote().path.len(), 2);
    assert_eq!(quote.quote().amount_out, U256::from(40_u64));
    println!(
        "version {}: A -> B -> C became immediately usable (10 -> {})",
        second.get(),
        quote.quote().amount_out
    );

    route.cancel().await?;
    routes.shutdown().await?;
    runtime.shutdown().await?;
    Ok(())
}
