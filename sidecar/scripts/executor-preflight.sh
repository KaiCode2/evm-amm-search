#!/usr/bin/env bash
set -euo pipefail

rpc_url=${EXECUTOR_RPC_URL:-${ETHEREUM_RPC_URL:-}}
router=${EXECUTOR_ROUTER:-}

if [[ -z "$rpc_url" || -z "$router" ]]; then
  echo "usage: EXECUTOR_RPC_URL=... EXECUTOR_ROUTER=0x... executor-preflight.sh" >&2
  exit 2
fi

code=$(cast code --rpc-url "$rpc_url" "$router")
if [[ "$code" == "0x" ]]; then
  echo "executor router has no runtime code: $router" >&2
  exit 1
fi

actual_hash=$(cast keccak "$code")
weth=$(cast call --rpc-url "$rpc_url" "$router" 'WETH()(address)')
permit2=$(cast call --rpc-url "$rpc_url" "$router" 'PERMIT2()(address)')

if [[ -n "${EXPECTED_RUNTIME_CODE_HASH:-}" && "$actual_hash" != "$EXPECTED_RUNTIME_CODE_HASH" ]]; then
  echo "executor runtime hash mismatch: expected $EXPECTED_RUNTIME_CODE_HASH, received $actual_hash" >&2
  exit 1
fi

echo "[executor]"
echo "enabled = true"
echo "router = \"$router\""
echo "weth = \"$weth\""
echo "permit2 = \"$permit2\""
echo "expected_runtime_code_hash = \"$actual_hash\""
