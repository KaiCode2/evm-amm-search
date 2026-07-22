#!/usr/bin/env bash
set -euo pipefail

: "${ETHEREUM_RPC_URL:?set ETHEREUM_RPC_URL to a mainnet archive or full-history RPC}"

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
anvil_port=${ANVIL_E2E_PORT:-18545}
sidecar_port=${SIDECAR_E2E_PORT:-18080}
fork_block_number=${EXECUTOR_E2E_FORK_BLOCK:-21000000}
timeout_seconds=${EXECUTOR_E2E_TIMEOUT_SECONDS:-300}
evidence_json=${EXECUTOR_E2E_EVIDENCE_JSON:-}
only_scenarios=${EXECUTOR_E2E_ONLY:-}
anvil_rpc="http://127.0.0.1:${anvil_port}"
anvil_ws="ws://127.0.0.1:${anvil_port}"
sidecar_url="http://127.0.0.1:${sidecar_port}"
private_key="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
weth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
usdc="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
usdt="0xdAC17F958D2ee523a2206206994597C13D831ec7"
bal="0xba100000625a3754423978a60c9317c58a424e3D"
permit2="0x000000000022D473030F116dDEE9F6B43aC78BA3"
tmp_dir=$(mktemp -d)
sidecar_log="$tmp_dir/sidecar.log"
sidecar_binary="$root_dir/sidecar/target/debug/evm-amm-route-sidecar"

cleanup() {
  status=$?
  if (( status != 0 )); then
    echo "executor e2e failed; anvil log:" >&2
    tail -n 80 "$tmp_dir/anvil.log" >&2 || true
    echo "executor e2e failed; latest sidecar log:" >&2
    if [[ -f "$sidecar_log" ]]; then tail -n 160 "$sidecar_log" >&2 || true; fi
  fi
  if [[ -n "${sidecar_pid:-}" ]]; then kill "$sidecar_pid" 2>/dev/null || true; fi
  if [[ -n "${anvil_pid:-}" ]]; then kill "$anvil_pid" 2>/dev/null || true; fi
  rm -rf "$tmp_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

stop_sidecar() {
  if [[ -n "${sidecar_pid:-}" ]]; then
    kill "$sidecar_pid" 2>/dev/null || true
    wait "$sidecar_pid" 2>/dev/null || true
    unset sidecar_pid
  fi
}

start_sidecar() {
  local scenario=$1
  local allowed_protocols=$2
  local pool_protocols=$3
  local scenario_config="$tmp_dir/${scenario}.toml"
  sidecar_log="$tmp_dir/sidecar-${scenario}.log"

  sed "s/^allowed_protocols = .*/allowed_protocols = [${allowed_protocols}]/" \
    "$root_dir/sidecar/examples/anvil-mainnet-e2e.toml" \
    | awk -v allowed="$pool_protocols" '
      function protocol_allowed(protocol, count, protocol_list, cursor) {
        count = split(allowed, protocol_list, ",")
        for (cursor = 1; cursor <= count; cursor++) {
          if (protocol == protocol_list[cursor]) return 1
        }
        return 0
      }
      function flush_pool() {
        if (in_pool && keep_pool) printf "%s", pool_block
        pool_block = ""
        keep_pool = 0
      }
      /^\[\[pools\]\]$/ {
        flush_pool()
        in_pool = 1
        pool_block = $0 ORS
        next
      }
      in_pool && /^\[/ {
        flush_pool()
        in_pool = 0
      }
      in_pool {
        pool_block = pool_block $0 ORS
        if ($0 ~ /^protocol = "/) {
          protocol = $0
          sub(/^protocol = "/, "", protocol)
          sub(/".*/, "", protocol)
          keep_pool = protocol_allowed(protocol)
        }
        next
      }
      { print }
      END { flush_pool() }
    ' >"$scenario_config"

  "$sidecar_binary" --config "$scenario_config" >"$sidecar_log" 2>&1 &
  sidecar_pid=$!

  local deadline=$((SECONDS + timeout_seconds))
  until curl --fail --silent --max-time 2 "$sidecar_url/readyz" >/dev/null; do
    if ! kill -0 "$sidecar_pid" 2>/dev/null; then
      echo "sidecar exited before readiness for scenario $scenario" >&2
      exit 1
    fi
    if (( SECONDS >= deadline )); then
      echo "sidecar did not become ready within ${timeout_seconds}s for scenario $scenario" >&2
      exit 1
    fi
    sleep 1
  done
}

u256_delta() {
  python3 - "$1" "$2" <<'PY'
import sys
print(int(sys.argv[2], 0) - int(sys.argv[1], 0))
PY
}

assert_u256_ge() {
  python3 - "$1" "$2" <<'PY'
import sys
if int(sys.argv[1], 0) < int(sys.argv[2], 0):
    raise SystemExit(1)
PY
}

cargo build --locked --manifest-path "$root_dir/sidecar/Cargo.toml"

anvil_args=(
  --silent
  --host 127.0.0.1
  --port "$anvil_port"
  --fork-url "$ETHEREUM_RPC_URL"
)
if [[ "$fork_block_number" != "latest" ]]; then
  anvil_args+=(--fork-block-number "$fork_block_number")
fi
anvil "${anvil_args[@]}" >"$tmp_dir/anvil.log" 2>&1 &
anvil_pid=$!

deadline=$((SECONDS + 60))
until cast chain-id --rpc-url "$anvil_rpc" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    echo "anvil did not start within 60 seconds" >&2
    exit 1
  fi
  sleep 1
done

fork_block=$(cast block-number --rpc-url "$anvil_rpc")
if [[ "$fork_block_number" != "latest" && "$fork_block" != "$fork_block_number" ]]; then
  echo "anvil forked block $fork_block instead of pinned block $fork_block_number" >&2
  exit 1
fi

snapshot_id=$(cast rpc --rpc-url "$anvil_rpc" evm_snapshot)
deployment=$(forge create \
  --root "$root_dir" \
  --rpc-url "$anvil_rpc" \
  --private-key "$private_key" \
  --broadcast \
  --json \
  contracts/ExperimentalExecutorRouter.sol:ExperimentalExecutorRouter \
  --constructor-args "$weth" "$permit2")
router=$(jq -r '.deployedTo // .deployed_to // empty' <<<"$deployment")
if [[ -z "$router" || "$router" == "null" ]]; then
  echo "could not parse deployed executor address" >&2
  exit 1
fi
runtime_code=$(cast code --rpc-url "$anvil_rpc" "$router")
runtime_hash=$(cast keccak "$runtime_code")
sender=$(cast wallet address --private-key "$private_key")

# Keep the canonical block hash identical to the upstream fork so the AMM cache
# can fetch verified state from the archive provider. Restore the executor's
# exact immutable-bearing runtime code plus its single nonzero initialized
# storage value (the reentrancy lock in slot zero).
cast rpc --rpc-url "$anvil_rpc" evm_revert "$snapshot_id" >/dev/null
cast rpc --rpc-url "$anvil_rpc" anvil_setCode "$router" "$runtime_code" >/dev/null
cast rpc --rpc-url "$anvil_rpc" anvil_setStorageAt \
  "$router" \
  "0x0000000000000000000000000000000000000000000000000000000000000000" \
  "0x0000000000000000000000000000000000000000000000000000000000000001" \
  >/dev/null
if [[ "$(cast block-number --rpc-url "$anvil_rpc")" != "$fork_block" ]]; then
  echo "installing executor code unexpectedly changed the canonical block" >&2
  exit 1
fi
if [[ "$(cast code --rpc-url "$anvil_rpc" "$router")" != "$runtime_code" ]]; then
  echo "executor runtime code was not installed after fork restoration" >&2
  exit 1
fi

export ANVIL_RPC_URL="$anvil_rpc"
export ANVIL_WS_URL="$anvil_ws"
export ANVIL_SIDECAR_LISTEN="127.0.0.1:${sidecar_port}"
export AMM_ROUTE_ADMIN_TOKEN="anvil-e2e-only"
export EXECUTOR_ROUTER="$router"
export EXECUTOR_RUNTIME_CODE_HASH="$runtime_hash"

# Each row is name|output token|allowed protocols|expected hop protocols. The
# temporary profile retains only those pool families, making every assertion a
# deterministic test of the intended adapter and encoder instead of relying on
# the requested route surviving a global top-k cutoff.
# The chain is restored after every transaction so every scenario quotes and
# executes against the same pinned state and upstream-verifiable block hash.
scenarios=(
  "uniswap_v2|$usdc|\"uniswap_v2\"|uniswap_v2"
  "uniswap_v3|$usdc|\"uniswap_v3\"|uniswap_v3"
  "pancake_v3|$usdc|\"pancake_v3\"|pancake_v3"
  "mixed_v2_curve|$usdt|\"uniswap_v2\", \"curve\"|uniswap_v2,curve"
)
if [[ "${EXECUTOR_E2E_INCLUDE_BALANCER:-false}" == "true" \
  || ",${only_scenarios}," == *",balancer_v2,"* ]]; then
  scenarios+=("balancer_v2|$bal|\"balancer_v2\"|balancer_v2")
fi

scenario_snapshot=$(cast rpc --rpc-url "$anvil_rpc" evm_snapshot)
executed_scenarios=0
for scenario_row in "${scenarios[@]}"; do
  IFS='|' read -r scenario token_out allowed_protocols expected_hops <<<"$scenario_row"
  if [[ -n "$only_scenarios" && ",${only_scenarios}," != *",${scenario},"* ]]; then
    continue
  fi
  executed_scenarios=$((executed_scenarios + 1))
  start_sidecar "$scenario" "$allowed_protocols" "$expected_hops"

  quote_file="$tmp_dir/quote-${scenario}.json"
  quote_status=$(curl --silent --show-error --max-time 30 \
    --output "$quote_file" \
    --write-out '%{http_code}' \
    "$sidecar_url/v1/executable-quote" \
    -H 'content-type: application/json' \
    -d "{
      \"token_in\":\"$weth\",
      \"token_out\":\"$token_out\",
      \"amount_in\":\"100000000000000000\",
      \"sender\":\"$sender\",
      \"recipient\":\"$sender\",
      \"slippage_bps\":100,
      \"deadline_secs\":600,
      \"authorization\":{\"type\":\"native\"},
      \"options\":{\"quality\":\"exhaustive\",\"top_k\":64,\"timeout_ms\":20000,\"discovery\":\"off\"}
    }")
  quote=$(<"$quote_file")
  if [[ "$quote_status" != "200" ]]; then
    echo "$scenario executable quote returned HTTP $quote_status: $quote" >&2
    exit 1
  fi

  target=$(jq -r '.transaction.to // empty' <<<"$quote")
  data=$(jq -r '.transaction.data // empty' <<<"$quote")
  value=$(jq -r '.transaction.value // empty' <<<"$quote")
  quote_amount=$(jq -r '.route.amount_out // empty' <<<"$quote")
  simulation_amount=$(jq -r '.simulation.amount_out // empty' <<<"$quote")
  minimum_amount=$(jq -r '.min_amount_out // empty' <<<"$quote")
  source_timestamp=$(jq -r '.source.block_timestamp // empty' <<<"$quote")
  actual_hops=$(jq -r '[.route.hops[].protocol] | join(",")' <<<"$quote")
  if [[ -z "$target" || -z "$data" || -z "$value" ]]; then
    echo "$scenario returned no executable transaction: $quote" >&2
    exit 1
  fi
  if [[ -z "$source_timestamp" ]]; then
    echo "$scenario returned no source block timestamp" >&2
    exit 1
  fi
  if [[ -z "$quote_amount" || "$quote_amount" != "$simulation_amount" ]]; then
    echo "$scenario quote/simulation mismatch: quote=$quote_amount simulation=$simulation_amount" >&2
    exit 1
  fi
  if [[ "$actual_hops" != "$expected_hops" ]]; then
    echo "$scenario returned unexpected protocols: expected=$expected_hops actual=$actual_hops" >&2
    exit 1
  fi

  before=$(cast call --rpc-url "$anvil_rpc" "$token_out" 'balanceOf(address)(uint256)' "$sender" | cut -d' ' -f1)
  cast rpc --rpc-url "$anvil_rpc" evm_setNextBlockTimestamp "$((source_timestamp + 1))" >/dev/null
  receipt=$(cast send --json --rpc-url "$anvil_rpc" --private-key "$private_key" --value "$value" \
    "$target" --data "$data")
  receipt_status=$(jq -r '.status // empty' <<<"$receipt")
  transaction_hash=$(jq -r '.transactionHash // .transaction_hash // empty' <<<"$receipt")
  if [[ "$receipt_status" != "0x1" && "$receipt_status" != "1" ]]; then
    echo "$scenario transaction receipt was not successful: $receipt" >&2
    exit 1
  fi
  after=$(cast call --rpc-url "$anvil_rpc" "$token_out" 'balanceOf(address)(uint256)' "$sender" | cut -d' ' -f1)
  actual_amount=$(u256_delta "$before" "$after")

  if [[ "$actual_amount" != "$simulation_amount" ]]; then
    echo "$scenario recipient delta differs from simulation: actual=$actual_amount simulation=$simulation_amount" >&2
    exit 1
  fi
  if ! assert_u256_ge "$actual_amount" "$minimum_amount"; then
    echo "$scenario recipient delta is below minimum: actual=$actual_amount minimum=$minimum_amount" >&2
    exit 1
  fi

  block_number=$(jq -r '.source.block_number' <<<"$quote")
  block_hash=$(jq -r '.source.block_hash' <<<"$quote")
  route_rank=$(jq -r '.route_rank' <<<"$quote")
  gas_estimate=$(jq -r '.simulation.gas_estimate' <<<"$quote")
  jq -n \
    --arg scenario "$scenario" \
    --arg block_number "$block_number" \
    --arg block_hash "$block_hash" \
    --arg block_timestamp "$source_timestamp" \
    --arg route_rank "$route_rank" \
    --arg protocols "$actual_hops" \
    --arg amount_out "$actual_amount" \
    --arg min_amount_out "$minimum_amount" \
    --arg gas_estimate "$gas_estimate" \
    --arg transaction_hash "$transaction_hash" \
    '{scenario:$scenario, block_number:$block_number, block_hash:$block_hash, block_timestamp:$block_timestamp, route_rank:$route_rank, protocols:($protocols | split(",")), amount_out:$amount_out, min_amount_out:$min_amount_out, gas_estimate:$gas_estimate, transaction_hash:$transaction_hash, receipt_status:"success", quote_simulation_execution_parity:true}' \
    >"$tmp_dir/result-${scenario}.json"
  echo "executor e2e passed: scenario=$scenario block=$block_number protocols=$actual_hops rank=$route_rank gas=$gas_estimate output=$actual_amount tx=$transaction_hash"

  stop_sidecar
  revert_result=$(cast rpc --rpc-url "$anvil_rpc" evm_revert "$scenario_snapshot")
  if [[ "$revert_result" != "true" ]]; then
    echo "could not restore pinned state after scenario $scenario" >&2
    exit 1
  fi
  if [[ "$(cast block-number --rpc-url "$anvil_rpc")" != "$fork_block" ]]; then
    echo "state restoration after $scenario did not return to block $fork_block" >&2
    exit 1
  fi
  scenario_snapshot=$(cast rpc --rpc-url "$anvil_rpc" evm_snapshot)
done

if (( executed_scenarios == 0 )); then
  echo "EXECUTOR_E2E_ONLY selected no known scenarios" >&2
  exit 1
fi

if [[ -n "$evidence_json" ]]; then
  mkdir -p "$(dirname "$evidence_json")"
  jq -s \
    --arg fork_block "$fork_block" \
    --arg router "$router" \
    --arg runtime_code_hash "$runtime_hash" \
    '{fork_block:$fork_block, router:$router, runtime_code_hash:$runtime_code_hash, scenarios:.}' \
    "$tmp_dir"/result-*.json >"$evidence_json"
fi

echo "executor e2e matrix passed: scenarios=$executed_scenarios fork_block=$fork_block"
