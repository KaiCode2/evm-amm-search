# Experimental executor router

`ExperimentalExecutorRouter` is an unaudited demonstration contract for
executing exact-input routes produced by `evm-amm-search`. It is absolutely not
intended for public or production use. Its ABI and packed route format are
unstable.

The contract supports the graph's currently executable protocol families:

| ID | Family | Per-hop data after the 61-byte common header |
|---:|---|---|
| `0` | Uniswap V2-style | fee in basis points (`uint16`, big-endian) |
| `1` | Uniswap V3 | none |
| `2` | PancakeSwap V3 | none |
| `3` | Slipstream | none |
| `4` | Solidly V2 | none |
| `5` | Balancer V2 | pool ID (`bytes32`) |
| `6` | Curve StableSwap | input and output indices (`uint8`, `uint8`) |
| `7` | Curve CryptoSwap | input and output indices (`uint8`, `uint8`) |
| `8` | Curve CryptoSwapNG | input and output indices (`uint8`, `uint8`) |

Every route starts with the final token address. Each common hop header is
`protocol (1) | endpoint (20) | tokenIn (20) | tokenOut (20)`. This is a new
executor format: unlike the older `DemoRouter` format, V2 hops include their
configured fee so non-30-basis-point graph routes can execute correctly.

## Entry points and invariants

- `executeExactInput` uses an existing ERC-20 allowance.
- `executeExactInputWithPermit` applies a standard ERC-2612 permit and swaps in
  the same transaction.
- `executeExactInputWithPermit2` uses Permit2 SignatureTransfer. The owner must
  already have approved the token to the chain's Permit2 contract.
- `executeExactInputNative` requires `msg.value == amountIn` and `tokenIn ==
  WETH`, then wraps the native input before executing the same route.

All calls require distinct input/output tokens, a nonzero recipient, nonzero
`minAmountOut`, and an unexpired deadline. Only the final output is protected by `minAmountOut`; intermediate
legs intentionally have no per-hop minimum in this first version, and per-leg
protection is explicitly left as a future improvement. Choosing the
second-best quote as a default minimum is sidecar policy and must be resolved
offchain before calling the contract.

The sidecar-local `execution` module implements that policy and serializes graph
quotes into this packed format. It returns the executor target, transaction
value, calldata, and prerequisite approval rather than submitting anything.
The sidecar exposes it through a disabled-by-default experimental endpoint that
also pins simulation and gas estimation to the quote's block hash. A response
is returned only when the executor's decoded output exactly matches the selected
route quote and satisfies the final minimum. The service still does not sign or
submit transactions, and the contract and wire format remain unstable.

Each hop must consume exactly the entire output of the previous hop. The router
uses actual balance deltas as the next hop's input, sends only that transaction's
final output to the recipient, clears temporary venue approvals, authenticates
V3 callbacks against an active call context, and rejects reentrancy. Successful
calls require the exact input amount to arrive, verify the recipient's exact
final-output balance increase, and restore every touched router balance to its
pre-call baseline. Fee-on-transfer or otherwise nonconforming tokens therefore
fail closed. Tokens sent
to the router outside an execution are not attributed to a caller or swept.

Custom graph adapters are intentionally rejected. The permissionless router
does not expose an arbitrary external-call opcode; a new venue family needs a
fixed, reviewed encoder and contract implementation before it can be enabled.

## Deterministic deployment

`ExperimentalExecutorRouterFactory` deploys with CREATE2 and returns an existing
deployment when another caller requests the same configuration. The predicted
address depends on the chain, factory address, salt, WETH and Permit2 addresses,
compiler settings, and router bytecode. Cross-chain address equality therefore
also requires deploying the factory at the same address.

Run the contract suite with:

```sh
forge test
forge build --sizes
```

Deploy and emit the exact runtime code hash required by the sidecar with:

```sh
DEPLOYER_PRIVATE_KEY=... \
WETH_ADDRESS=0x... \
PERMIT2_ADDRESS=0x... \
EXECUTOR_SALT=0x... \
forge script script/DeployExperimentalExecutor.s.sol \
  --rpc-url "$EXECUTOR_RPC_URL" \
  --broadcast
```

Set `EXECUTOR_FACTORY` to reuse an existing factory. Verify a deployed router
and generate its TOML fields with `sidecar/scripts/executor-preflight.sh`.

With an Ethereum archive RPC, run the pinned real-protocol smoke matrix with:

```sh
ETHEREUM_RPC_URL=https://your-archive-node \
  sidecar/scripts/executor-fork-smoke.sh
```

Run the stronger generated-route matrix with the same archive RPC:

```sh
ETHEREUM_RPC_URL=https://your-archive-node \
  sidecar/scripts/executor-e2e-anvil.sh
```

That gate starts the daemon at pinned Ethereum block `21,000,000` and covers
Uniswap V2, Uniswap V3, PancakeSwap V3, and mixed Uniswap V2 to Curve. Every
case submits exactly the returned target, value, and calldata and requires
quote output = simulated output = mined recipient balance delta. Balancer V2
still passes the direct contract-fork case, but is intentionally outside the
default sidecar matrix until its upstream whole-account hydration gap is fixed.
