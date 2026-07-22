#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: live-smoke-container.sh IMAGE}
: "${ETHEREUM_WS_URL:?set ETHEREUM_WS_URL to a private mainnet websocket}"
admin_token=${AMM_ROUTE_ADMIN_TOKEN:-live-smoke-only-token}
timeout_seconds=${LIVE_SMOKE_TIMEOUT_SECONDS:-240}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
config="$script_dir/../examples/ethereum-mainnet.toml"
container="amm-route-live-smoke-$$"

cleanup() {
  status=$?
  if (( status != 0 )); then
    docker logs "$container" >&2 || true
  fi
  docker rm --force "$container" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM

docker run --detach \
  --name "$container" \
  --publish 127.0.0.1::8080 \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  -e ETHEREUM_WS_URL \
  -e AMM_ROUTE_ADMIN_TOKEN="$admin_token" \
  -v "$config:/config/config.toml:ro" \
  "$image" >/dev/null

port=$(docker port "$container" 8080/tcp | head -n 1 | awk -F: '{print $NF}')
base_url="http://127.0.0.1:$port"
started_at=$SECONDS
deadline=$((SECONDS + timeout_seconds))
until curl --fail --silent --max-time 2 "$base_url/readyz" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "sidecar did not become ready within ${timeout_seconds}s" >&2
    exit 1
  fi
  sleep 1
done

status=$(curl --fail --silent --max-time 10 "$base_url/v1/status")
if [[ ! "$status" =~ \"graph_pools\":[1-9][0-9]* ]]; then
  echo "sidecar became ready without a non-empty routing graph: $status" >&2
  exit 1
fi

quote=$(curl --fail --silent --max-time 30 \
  "$base_url/v1/quote" \
  -H 'content-type: application/json' \
  -d '{
    "token_in":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "token_out":"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "amount_in":"1000000",
    "options":{
      "quality":"balanced",
      "top_k":1,
      "timeout_ms":15000,
      "discovery":"off"
    }
  }')
if [[ "$quote" != *'"routes":[{'* ]]; then
  echo "balanced live smoke returned no route: $quote" >&2
  exit 1
fi

echo "live container smoke passed: image=$image ready_seconds=$((SECONDS - started_at))"
