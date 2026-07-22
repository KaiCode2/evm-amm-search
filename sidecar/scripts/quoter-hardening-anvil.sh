#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
chain_mode=${QUOTER_CHAIN_MODE:-local}
sidecar_image=${QUOTER_SIDECAR_IMAGE:-}
anvil_port=${ANVIL_QUOTER_PORT:-18645}
proxy_port=${ANVIL_PROXY_PORT:-18646}
fallback_proxy_port=${ANVIL_FALLBACK_PROXY_PORT:-18647}
state_proxy_port=${ANVIL_STATE_PROXY_PORT:-18648}
proxy_control_port=${ANVIL_PROXY_CONTROL_PORT:-18475}
sidecar_port=${SIDECAR_QUOTER_PORT:-18081}
startup_timeout=${QUOTER_STARTUP_TIMEOUT_SECONDS:-300}
short_outage_seconds=${QUOTER_SHORT_OUTAGE_SECONDS:-5}
short_outage_cycles=${QUOTER_SHORT_OUTAGE_CYCLES:-1}
long_outage_seconds=${QUOTER_LONG_OUTAGE_SECONDS:-40}
state_failure_seconds=${QUOTER_STATE_FAILURE_SECONDS:-5}
load_requests=${QUOTER_LOAD_REQUESTS:-128}
load_concurrency=${QUOTER_LOAD_CONCURRENCY:-32}
test_provider_restart=${QUOTER_TEST_PROVIDER_RESTART:-false}
provider_restart_outage_seconds=${QUOTER_PROVIDER_RESTART_OUTAGE_SECONDS:-20}
test_reorg=${QUOTER_TEST_REORG:-false}
reorg_depth=${QUOTER_REORG_DEPTH:-3}
max_memory_growth_bytes=${QUOTER_MAX_MEMORY_GROWTH_BYTES:-134217728}
evidence_json=${QUOTER_EVIDENCE_JSON:-}
anvil_rpc="http://127.0.0.1:${anvil_port}"
proxy_api="http://127.0.0.1:${proxy_control_port}"
sidecar_url="http://127.0.0.1:${sidecar_port}"
proxy_container="amm-route-toxiproxy-$$"
sidecar_container="amm-route-sidecar-gate-$$"
test_network="amm-route-gate-$$"
tmp_dir=$(mktemp -d)
anvil_state="$tmp_dir/anvil-state.json"

case "$chain_mode" in
  local)
    config_name="anvil-quoter-release-gate.toml"
    token_in="0x0000000000000000000000000000000000000011"
    token_out="0x0000000000000000000000000000000000000022"
    pool_address="0x0000000000000000000000000000000000000100"
    v2_router="0x0000000000000000000000000000000000000200"
    v3_quoter="0x0000000000000000000000000000000000000201"
    v2_factory="0x0000000000000000000000000000000000000300"
    ;;
  mainnet-fork)
    : "${ETHEREUM_RPC_URL:?set ETHEREUM_RPC_URL to a mainnet archive or full-history RPC}"
    config_name="anvil-quoter-hardening.toml"
    token_in="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    token_out="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    ;;
  *)
    echo "unsupported QUOTER_CHAIN_MODE: $chain_mode" >&2
    exit 2
    ;;
esac

sidecar_alive() {
  if [[ -n "$sidecar_image" ]]; then
    [[ "$(docker inspect --format '{{.State.Running}}' "$sidecar_container" 2>/dev/null || true)" == "true" ]]
  else
    kill -0 "$sidecar_pid" 2>/dev/null
  fi
}

start_anvil() {
  local anvil_host=127.0.0.1
  if [[ -n "$sidecar_image" ]]; then
    # Linux containers reach the host through the Docker bridge gateway, not
    # its loopback interface. Keep host-binary runs loopback-only, but expose
    # the ephemeral test chain to the bridge for exact-image gates.
    anvil_host=0.0.0.0
  fi
  local args=(--silent --host "$anvil_host" --port "$anvil_port")
  if [[ "$chain_mode" == "local" ]]; then
    args+=(--state "$anvil_state" --preserve-historical-states)
  else
    args+=(--fork-url "$ETHEREUM_RPC_URL")
  fi
  anvil "${args[@]}" >"$tmp_dir/anvil.log" 2>&1 &
  anvil_pid=$!
}

wait_for_anvil() {
  local deadline=$((SECONDS + 60))
  until cast chain-id --rpc-url "$anvil_rpc" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "anvil did not start within 60 seconds" >&2
      return 1
    fi
    sleep 1
  done
}

cleanup() {
  status=$?
  if (( status != 0 )); then
    echo "quoter hardening run failed; anvil log:" >&2
    tail -n 80 "$tmp_dir/anvil.log" >&2 || true
    echo "quoter hardening run failed; sidecar log:" >&2
    if [[ -n "$sidecar_image" ]]; then
      docker logs --tail 160 "$sidecar_container" >&2 || true
    else
      tail -n 160 "$tmp_dir/sidecar.log" >&2 || true
    fi
    echo "quoter hardening run failed; proxy log:" >&2
    docker logs "$proxy_container" >&2 || true
  fi
  if [[ -n "${sidecar_pid:-}" ]]; then kill "$sidecar_pid" 2>/dev/null || true; fi
  if [[ -n "${anvil_pid:-}" ]]; then kill "$anvil_pid" 2>/dev/null || true; fi
  docker rm --force "$sidecar_container" >/dev/null 2>&1 || true
  docker rm --force "$proxy_container" >/dev/null 2>&1 || true
  docker network rm "$test_network" >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

start_anvil
wait_for_anvil

if [[ "$chain_mode" == "local" && ! -s "$anvil_state" ]]; then
  token0_word="0x0000000000000000000000000000000000000000000000000000000000000011"
  token1_word="0x0000000000000000000000000000000000000000000000000000000000000022"
  reserves_word="0x0000000100000001a784379d99db4200000000000000d3c21bcecceda1000000"
  pool_runtime_code="0x$(tr -d '[:space:]' <"$root_dir/sidecar/fixtures/uniswap_v2_pair_runtime.hex")"
  v2_router_code=$(forge inspect \
    --root "$root_dir" \
    contracts/test/LocalV2Quoter.sol:LocalV2Quoter \
    deployedBytecode)
  cast rpc --rpc-url "$anvil_rpc" anvil_setStorageAt "$pool_address" \
    "0x0000000000000000000000000000000000000000000000000000000000000006" \
    "$token0_word" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_setStorageAt "$pool_address" \
    "0x0000000000000000000000000000000000000000000000000000000000000007" \
    "$token1_word" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_setStorageAt "$pool_address" \
    "0x0000000000000000000000000000000000000000000000000000000000000008" \
    "$reserves_word" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_setCode "$pool_address" "$pool_runtime_code" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_setCode "$v2_router" "$v2_router_code" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_setCode "$v3_quoter" "0x00" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_setCode "$v2_factory" "0x00" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_mine 0x1 >/dev/null
fi

docker network create "$test_network" >/dev/null
docker run --detach --rm \
  --name "$proxy_container" \
  --network "$test_network" \
  --add-host host.docker.internal:host-gateway \
  --publish "127.0.0.1:${proxy_control_port}:8474" \
  --publish "127.0.0.1:${proxy_port}:8666" \
  --publish "127.0.0.1:${fallback_proxy_port}:8667" \
  --publish "127.0.0.1:${state_proxy_port}:8668" \
  ghcr.io/shopify/toxiproxy:2.9.0@sha256:b44c283298cea49e2defaba1b3028783798346f2a926684e3a345fd8441af3b8 >/dev/null

deadline=$((SECONDS + 30))
until curl --fail --silent "$proxy_api/version" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "Toxiproxy did not start within 30 seconds" >&2
    exit 1
  fi
  sleep 1
done

curl --fail --silent --request POST "$proxy_api/proxies" \
  --header 'content-type: application/json' \
  --data '{"name":"canonical","listen":"0.0.0.0:8666","upstream":"host.docker.internal:'"$anvil_port"'"}' \
  >/dev/null
curl --fail --silent --request POST "$proxy_api/proxies" \
  --header 'content-type: application/json' \
  --data '{"name":"canonical-fallback","listen":"0.0.0.0:8667","upstream":"host.docker.internal:'"$anvil_port"'"}' \
  >/dev/null
curl --fail --silent --request POST "$proxy_api/proxies" \
  --header 'content-type: application/json' \
  --data '{"name":"state","listen":"0.0.0.0:8668","upstream":"host.docker.internal:'"$anvil_port"'"}' \
  >/dev/null

export ANVIL_PROXY_WS_URL="ws://127.0.0.1:${proxy_port}"
export ANVIL_FALLBACK_PROXY_WS_URL="ws://127.0.0.1:${fallback_proxy_port}"
export ANVIL_INVALID_WS_URL="ws://quoter-dns-failure.invalid:65535"
export ANVIL_STATE_RPC_URL="http://127.0.0.1:${state_proxy_port}"
export ANVIL_SIDECAR_LISTEN="127.0.0.1:${sidecar_port}"
export AMM_ROUTE_ADMIN_TOKEN="anvil-quoter-hardening-only"
export RUST_LOG=${QUOTER_RUST_LOG:-evm_amm_route_sidecar=info,tower_http=info}

if [[ -n "$sidecar_image" ]]; then
  docker image inspect "$sidecar_image" >/dev/null
  # Do not use --rm here: if startup fails, cleanup must still be able to
  # retain and print the container's diagnostic logs before removing it.
  docker run --detach \
    --name "$sidecar_container" \
    --network "$test_network" \
    --add-host host.docker.internal:host-gateway \
    --publish "127.0.0.1:${sidecar_port}:8080" \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges:true \
    --env ANVIL_PROXY_WS_URL="ws://${proxy_container}:8666" \
    --env ANVIL_FALLBACK_PROXY_WS_URL="ws://${proxy_container}:8667" \
    --env ANVIL_INVALID_WS_URL \
    --env ANVIL_STATE_RPC_URL="http://${proxy_container}:8668" \
    --env ANVIL_SIDECAR_LISTEN="0.0.0.0:8080" \
    --env AMM_ROUTE_ADMIN_TOKEN \
    --env ETHEREUM_RPC_URL \
    --env RUST_LOG \
    --volume "$root_dir/sidecar/examples:/config:ro" \
    "$sidecar_image" --config "/config/$config_name" >/dev/null
else
  cargo run --manifest-path "$root_dir/sidecar/Cargo.toml" -- \
    --config "$root_dir/sidecar/examples/$config_name" \
    >"$tmp_dir/sidecar.log" 2>&1 &
  sidecar_pid=$!
fi

deadline=$((SECONDS + startup_timeout))
until curl --fail --silent --max-time 2 "$sidecar_url/readyz" >/dev/null; do
  if ! sidecar_alive; then
    echo "sidecar exited before readiness" >&2
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo "sidecar did not become ready within ${startup_timeout}s" >&2
    exit 1
  fi
  sleep 1
done

quote() {
  local amount_in=$1
  local output=$2
  curl --silent --show-error --max-time 15 \
    --output "$output" \
    --write-out '%{http_code} %{time_total}' \
    "$sidecar_url/v1/quote" \
    --header 'content-type: application/json' \
    --data '{
      "token_in":"'"$token_in"'",
      "token_out":"'"$token_out"'",
      "amount_in":"'"$amount_in"'",
      "options":{"quality":"balanced","top_k":1,"timeout_ms":5000,"discovery":"off"}
    }'
}

set_proxy_enabled() {
  local proxy_name=$1
  local enabled=$2
  curl --fail --silent --request POST "$proxy_api/proxies/$proxy_name" \
    --header 'content-type: application/json' \
    --data '{"enabled":'"$enabled"'}' >/dev/null
}

initial_quote_code=$(quote 100000000000000000 "$tmp_dir/initial-quote.json" | awk '{print $1}')
if [[ "$initial_quote_code" != "200" ]] \
  || [[ "$(jq '.routes | length' "$tmp_dir/initial-quote.json")" -lt 1 ]]; then
  echo "initial quote failed with HTTP $initial_quote_code" >&2
  cat "$tmp_dir/initial-quote.json" >&2
  curl --silent "$sidecar_url/v1/status" >&2 || true
  exit 1
fi

: >"$tmp_dir/load-results"
for ((start = 1; start <= load_requests; start += load_concurrency)); do
  pids=()
  for ((offset = 0; offset < load_concurrency && start + offset <= load_requests; offset++)); do
    index=$((start + offset))
    (
      result=$(quote "$((100000000000000000 + index))" "$tmp_dir/load-${index}.json")
      printf '%s\n' "$result" >>"$tmp_dir/load-results"
    ) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
done

load_ok=$(awk '$1 == 200 { count += 1 } END { print count + 0 }' "$tmp_dir/load-results")
if (( load_ok != load_requests )); then
  echo "parallel quote load completed only $load_ok/$load_requests requests" >&2
  sort "$tmp_dir/load-results" | uniq -c >&2
  exit 1
fi
awk '{ print $2 * 1000 }' "$tmp_dir/load-results" | sort -n >"$tmp_dir/load-ms"
p50_index=$(((load_ok * 50 + 99) / 100))
p95_index=$(((load_ok * 95 + 99) / 100))
p99_index=$(((load_ok * 99 + 99) / 100))
p50_ms=$(sed -n "${p50_index}p" "$tmp_dir/load-ms")
p95_ms=$(sed -n "${p95_index}p" "$tmp_dir/load-ms")
p99_ms=$(sed -n "${p99_index}p" "$tmp_dir/load-ms")
echo "parallel quote load passed: requests=$load_ok concurrency=$load_concurrency p50_ms=$p50_ms p95_ms=$p95_ms p99_ms=$p99_ms"

initial_generation=$(curl --fail --silent "$sidecar_url/v1/status" | jq -r '.node.routing_generation')
set_proxy_enabled canonical false
set_proxy_enabled canonical-fallback false
long_failed_after="none"
stale_quote_http="none"
stale_quote_code="none"
for ((elapsed = 1; elapsed <= long_outage_seconds; elapsed++)); do
  ready_code=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
  if [[ "$ready_code" != "200" && "$long_failed_after" == "none" ]]; then
    long_failed_after=$elapsed
    stale_quote_http=$(quote 100000000000000000 "$tmp_dir/stale-quote.json" | awk '{print $1}')
    stale_quote_code=$(jq -r '.error.code // "missing"' "$tmp_dir/stale-quote.json")
  fi
  sleep 1
done
if [[ "$long_failed_after" == "none" ]] || [[ "$stale_quote_http" != "503" ]]; then
  echo "extended outage did not fail readiness and quote admission closed" >&2
  exit 1
fi
case "$stale_quote_code" in
  canonical_stale|canonical_reconnecting|runtime_untrusted) ;;
  *)
    echo "extended outage returned unexpected quote error $stale_quote_code" >&2
    exit 1
    ;;
esac

if [[ "$chain_mode" == "local" ]]; then
  set_proxy_enabled state false
fi
set_proxy_enabled canonical-fallback true
if [[ "$chain_mode" == "local" ]]; then
  sleep "$state_failure_seconds"
  state_failure_ready=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
  state_failure_quote_http=$(quote 100000000000000000 "$tmp_dir/state-failure-quote.json" | awk '{print $1}')
  if [[ "$state_failure_ready" == "200" || "$state_failure_quote_http" != "503" ]]; then
    echo "state-provider outage reopened quote traffic before a verified rebuild" >&2
    exit 1
  fi
  set_proxy_enabled state true
fi
long_recovered=false
deadline=$((SECONDS + startup_timeout))
while (( SECONDS < deadline )); do
  ready_code=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
  recovered_status=$(curl --silent "$sidecar_url/v1/status")
  recovered_generation=$(jq -r '.node.routing_generation // 0' <<<"$recovered_status")
  recovered_endpoint=$(jq -r '.node.canonical_endpoint_index // -1' <<<"$recovered_status")
  if [[ "$ready_code" == "200" ]] \
    && (( recovered_generation > initial_generation )) \
    && [[ "$recovered_endpoint" == "1" ]]; then
    long_recovered=true
    break
  fi
  sleep 1
done
if [[ "$long_recovered" != "true" ]]; then
  echo "sidecar did not rebuild after the extended websocket outage" >&2
  exit 1
fi
outage_recovered_generation=$recovered_generation
outage_recovered_endpoint=$recovered_endpoint
echo "extended websocket outage and endpoint failover recovered: readiness_failed_after_seconds=$long_failed_after stale_quote_http=$stale_quote_http stale_quote_code=$stale_quote_code initial_generation=$initial_generation recovered_generation=$recovered_generation recovered_endpoint_index=$recovered_endpoint"

if [[ -n "$sidecar_image" ]]; then
  memory_before=$(docker exec "$sidecar_container" cat /sys/fs/cgroup/memory.current)
fi

for ((cycle = 1; cycle <= short_outage_cycles; cycle++)); do
  before_short=$(jq -r '.node.block_number' <<<"$recovered_status")
  set_proxy_enabled canonical-fallback false
  cast rpc --rpc-url "$anvil_rpc" anvil_mine 0x3 >/dev/null
  sleep "$short_outage_seconds"
  during_short_ready=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
  set_proxy_enabled canonical-fallback true
  sleep 1
  cast rpc --rpc-url "$anvil_rpc" anvil_mine 0x1 >/dev/null
  short_target=$(cast block-number --rpc-url "$anvil_rpc")
  deadline=$((SECONDS + 30))
  short_recovered=false
  while (( SECONDS < deadline )); do
    ready_code=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
    recovered_status=$(curl --silent "$sidecar_url/v1/status")
    observed=$(jq -r '.node.block_number // 0' <<<"$recovered_status")
    if [[ "$ready_code" == "200" ]] && (( observed >= short_target )); then
      short_recovered=true
      break
    fi
    sleep 1
  done
  if [[ "$short_recovered" != "true" ]]; then
    echo "sidecar did not recover from short websocket outage cycle $cycle" >&2
    exit 1
  fi
  echo "short websocket outage recovered: cycle=$cycle before_block=$before_short target_block=$short_target ready_during_outage_http=$during_short_ready"
done

if [[ "$test_provider_restart" == "true" ]]; then
  if [[ "$chain_mode" != "local" ]]; then
    echo "provider restart test requires QUOTER_CHAIN_MODE=local" >&2
    exit 2
  fi
  restart_generation=$(jq -r '.node.routing_generation' <<<"$recovered_status")
  kill "$anvil_pid"
  wait "$anvil_pid" || true
  unset anvil_pid
  sleep "$provider_restart_outage_seconds"
  restart_ready=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
  restart_quote_http=$(quote 100000000000000000 "$tmp_dir/provider-restart-quote.json" | awk '{print $1}')
  if [[ "$restart_ready" == "200" || "$restart_quote_http" != "503" ]]; then
    echo "provider restart outage did not keep quote traffic fail-closed" >&2
    exit 1
  fi
  start_anvil
  wait_for_anvil
  cast rpc --rpc-url "$anvil_rpc" anvil_mine 0x1 >/dev/null
  restart_target=$(cast block-number --rpc-url "$anvil_rpc")
  deadline=$((SECONDS + startup_timeout))
  provider_recovered=false
  while (( SECONDS < deadline )); do
    recovered_status=$(curl --silent "$sidecar_url/v1/status")
    ready_code=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
    recovered_generation=$(jq -r '.node.routing_generation // 0' <<<"$recovered_status")
    observed=$(jq -r '.node.block_number // 0' <<<"$recovered_status")
    if [[ "$ready_code" == "200" ]] \
      && (( recovered_generation > restart_generation )) \
      && (( observed >= restart_target )); then
      provider_recovered=true
      break
    fi
    sleep 1
  done
  if [[ "$provider_recovered" != "true" ]]; then
    echo "sidecar did not recover after the canonical/state provider process restart" >&2
    exit 1
  fi
  echo "provider process restart recovered: initial_generation=$restart_generation recovered_generation=$recovered_generation target_block=$restart_target"
fi

if [[ "$test_reorg" == "true" ]]; then
  if [[ "$chain_mode" != "local" ]]; then
    echo "reorg test requires QUOTER_CHAIN_MODE=local" >&2
    exit 2
  fi
  snapshot_id=$(cast rpc --rpc-url "$anvil_rpc" evm_snapshot | tr -d '"')
  cast rpc --rpc-url "$anvil_rpc" anvil_mine "$(printf '0x%x' "$reorg_depth")" >/dev/null
  old_hash=$(cast block latest --rpc-url "$anvil_rpc" --json | jq -r '.hash')
  sleep 1
  cast rpc --rpc-url "$anvil_rpc" evm_revert "$snapshot_id" >/dev/null
  latest_timestamp=$(cast block latest --rpc-url "$anvil_rpc" --json | jq -r '.timestamp' | xargs printf '%d')
  cast rpc --rpc-url "$anvil_rpc" evm_setNextBlockTimestamp "$((latest_timestamp + 60))" >/dev/null
  cast rpc --rpc-url "$anvil_rpc" anvil_mine "$(printf '0x%x' "$reorg_depth")" >/dev/null
  reorg_target=$(cast block-number --rpc-url "$anvil_rpc")
  new_hash=$(cast block latest --rpc-url "$anvil_rpc" --json | jq -r '.hash')
  if [[ "$new_hash" == "$old_hash" ]]; then
    echo "local reorg fixture did not produce a distinct canonical hash" >&2
    exit 1
  fi
  deadline=$((SECONDS + 60))
  reorg_recovered=false
  while (( SECONDS < deadline )); do
    recovered_status=$(curl --silent "$sidecar_url/v1/status")
    ready_code=$(curl --silent --output /dev/null --write-out '%{http_code}' "$sidecar_url/readyz")
    observed_hash=$(jq -r '.node.block_hash // empty' <<<"$recovered_status")
    observed=$(jq -r '.node.block_number // 0' <<<"$recovered_status")
    if [[ "$ready_code" == "200" ]] \
      && (( observed == reorg_target )) \
      && [[ "${observed_hash,,}" == "${new_hash,,}" ]]; then
      reorg_recovered=true
      break
    fi
    sleep 1
  done
  if [[ "$reorg_recovered" != "true" ]]; then
    echo "sidecar did not converge to the replacement canonical branch" >&2
    exit 1
  fi
  echo "shallow reorg recovered: depth=$reorg_depth block=$reorg_target old_hash=$old_hash new_hash=$new_hash"
fi

if [[ -n "$sidecar_image" ]]; then
  memory_after=$(docker exec "$sidecar_container" cat /sys/fs/cgroup/memory.current)
  memory_growth=$((memory_after - memory_before))
  if (( memory_growth > max_memory_growth_bytes )); then
    echo "sidecar memory grew by $memory_growth bytes across recovery cycles (limit $max_memory_growth_bytes)" >&2
    exit 1
  fi
  echo "container memory check passed: before_bytes=$memory_before after_bytes=$memory_after growth_bytes=$memory_growth limit_bytes=$max_memory_growth_bytes"
fi

final_status=$(curl --fail --silent "$sidecar_url/v1/status")
final_health=$(jq -r '.node.runtime_health' <<<"$final_status")
final_block=$(jq -r '.node.block_number' <<<"$final_status")
post_failure_quote_http=$(quote 100000000000000000 "$tmp_dir/post-failure-quote.json" | awk '{print $1}')
if [[ "$post_failure_quote_http" != "200" ]]; then
  echo "quote service did not reopen after canonical recovery" >&2
  exit 1
fi
echo "quoter recovery contract passed: outage_seconds=$long_outage_seconds observed_block=$final_block runtime_health=$final_health post_recovery_quote_http=$post_failure_quote_http"

if [[ -n "$evidence_json" ]]; then
  mkdir -p "$(dirname "$evidence_json")"
  jq -n \
    --arg chain_mode "$chain_mode" \
    --arg image "${sidecar_image:-host-binary}" \
    --arg stale_quote_code "$stale_quote_code" \
    --arg final_runtime_health "$final_health" \
    --argjson load_requests "$load_ok" \
    --argjson load_concurrency "$load_concurrency" \
    --argjson p50_ms "$p50_ms" \
    --argjson p95_ms "$p95_ms" \
    --argjson p99_ms "$p99_ms" \
    --argjson long_outage_seconds "$long_outage_seconds" \
    --argjson readiness_failed_after_seconds "$long_failed_after" \
    --argjson initial_generation "$initial_generation" \
    --argjson recovered_generation "$outage_recovered_generation" \
    --argjson recovered_endpoint_index "$outage_recovered_endpoint" \
    --argjson short_outage_cycles "$short_outage_cycles" \
    --argjson provider_restart_tested "$test_provider_restart" \
    --argjson reorg_tested "$test_reorg" \
    --argjson final_block "$final_block" \
    '{
      chain_mode: $chain_mode,
      image: $image,
      parallel_load: {
        requests: $load_requests,
        concurrency: $load_concurrency,
        p50_ms: $p50_ms,
        p95_ms: $p95_ms,
        p99_ms: $p99_ms
      },
      extended_outage: {
        duration_seconds: $long_outage_seconds,
        readiness_failed_after_seconds: $readiness_failed_after_seconds,
        stale_quote_http: 503,
        stale_quote_code: $stale_quote_code,
        initial_generation: $initial_generation,
        recovered_generation: $recovered_generation,
        recovered_endpoint_index: $recovered_endpoint_index
      },
      short_outage_cycles: $short_outage_cycles,
      provider_restart_tested: $provider_restart_tested,
      reorg_tested: $reorg_tested,
      final_block: $final_block,
      final_runtime_health: $final_runtime_health,
      post_recovery_quote_http: 200
    }' | tee "$evidence_json"
fi
