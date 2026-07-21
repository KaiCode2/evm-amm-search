# Local release-gate fixtures

`uniswap_v2_pair_runtime.hex` is the canonical Uniswap V2 pair runtime embedded
by `evm-amm-state` 0.2.0. Its decoded SHA-256 is
`8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4`.
The deterministic Anvil gate installs this code so the same verified-code seed
contract used in production is exercised without an external archive RPC.
