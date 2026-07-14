# Quote Quality Benchmarks

This benchmark compares `evm-amm-search` against external aggregator quote APIs
on Ethereum mainnet. The local search universe is intentionally focused:
Uniswap V2 and Uniswap V3 pools discovered for the eight-token README basket.

External providers covered here:

- [LI.FI quote API](https://docs.li.fi/api-reference/get-a-quote-for-a-token-transfer)
- [1inch Classic Swap quote API](https://business.1inch.com/portal/documentation/apis/swap/classic-swap/methods/v6.1/1/quote/method/get)

## What We Tested

Routes:

```text
10 WETH -> USDC
100 LINK -> AAVE
1000 DAI -> UNI
```

Local setup:

- paid Ethereum RPC via `LoadBalancedTransport` plus request batching and gzip
- Uniswap V2/V3 factory discovery over the benchmark basket
- `ColdStartPolicy::Eager`
- broad liquidity index scope for branch ranking and parallel-edge ordering
- warm streaming search with the balanced heuristic preset
- optional demo-router gas simulation through `contracts/DemoRouter.sol`

External setup:

- LI.FI and 1inch requests are sent alongside local search.
- External APIs quote live/current chain state and do not expose a block pin.
- The example can stagger one provider batch per new block.
- LI.FI returns `estimate.toAmount` and `estimate.gasCosts`.
- 1inch Classic quote supports `includeGas=true` and returns approximated gas.

## Focused Cold Start

Final gas-simulated run, observed at Ethereum block `25494941`:

| Step | Time |
| --- | ---: |
| `cold_start_many` | `997.2ms` |
| Liquidity refresh | `132.7ms` |
| Cold start plus liquidity refresh | `1.13s` |
| Warm prime searches | `38.1ms` |

The local graph stayed small and repeatable: `117` indexed pools, `8` token
nodes, and `234` directed AMM edges.

## Warm Local Search Latency

Same final gas-simulated run:

| Route | First quote | Best quote | Search total |
| --- | ---: | ---: | ---: |
| `10 WETH -> USDC` | `312µs` | `312µs` | `4.37ms` |
| `100 LINK -> AAVE` | `616µs` | `1.97ms` | `9.78ms` |
| `1000 DAI -> UNI` | `946µs` | `1.59ms` | `9.34ms` |

The broader 30-run README benchmark is still the better source for p50, p95,
and worst-case local search latency. This page focuses on quote quality and gas
comparison against external providers.

## Provider Comparison

Sign convention: positive provider difference means the external provider
quoted more output than the local route. Negative means local search quoted more
output.

The gross LI.FI and 1inch columns come from a three-run provider sample where
both providers returned quotes. The local router gas and LI.FI net-gas column
come from the final gas-simulated run. The LI.FI net comparison converts gas
into output-token units with the local WETH-to-output quote at the sampled gas
price.

| Route | Local best quote sample | Local best latency | Local router gas | LI.FI p50 gross diff / latency / gas | LI.FI net diff incl. gas | 1inch p50 gross diff / latency / gas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `10 WETH -> USDC` | `17,451.115307 USDC` | `381.5µs` | `139,308` | `-21.1002 bps / 874.8ms / 823,081` | `+18.3966 bps` | `-27.4128 bps / 253.6ms / 782,148` |
| `100 LINK -> AAVE` | `8.721006564750064259 AAVE` | `1.77ms` | `218,872` | `-23.0615 bps / 765.5ms / 686,113` | `-24.6706 bps` | `-27.1551 bps / 183.6ms / 453,075` |
| `1000 DAI -> UNI` | `297.574325332547588103 UNI` | `8.99ms` | `219,966` | `-18.6252 bps / 665.4ms / 934,730` | `-22.9711 bps` | `-25.4327 bps / 239.2ms / 954,862` |

Takeaways:

- Local warm search surfaced the best route in microseconds to single-digit
  milliseconds.
- In the three-run provider sample, local search beat both LI.FI and 1inch on
  all three routes on gross output.
- In the final gas-simulated LI.FI comparison, LI.FI beat the local route on
  `WETH -> USDC`; local search beat LI.FI on `LINK -> AAVE` and `DAI -> UNI`.

## How To Reproduce

```text
set -a; source .env; set +a
AGG_BENCH_RUNS=1 \
AGG_BENCH_STAGGER_QUOTES_BY_BLOCK=1 \
AGG_BENCH_SIMULATE_LOCAL_SWAP_GAS=1 \
AGG_BENCH_PRIME_SEARCHES=1 \
AGG_BENCH_PERSIST_CACHE=1 \
cargo run --release --example aggregator_quote_comparison
```

Relevant env vars:

```text
E2E_RPC_URL=<paid-mainnet-rpc>
LIFI_API_KEY=<key>
ONEINCH_API_KEY=<key>
AGG_BENCH_CACHE_DIR=.cache/aggregator-quote-comparison
AGG_BENCH_BLOCK_LAG=0
AGG_BENCH_MAX_HOPS=3
AGG_BENCH_WORKERS=0
AGG_BENCH_COMPLETION=heuristic
```
