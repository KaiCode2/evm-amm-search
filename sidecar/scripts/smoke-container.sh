#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: smoke-container.sh IMAGE}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
config="$script_dir/../examples/ethereum-mainnet.toml"

image_id=$(docker image ls --no-trunc --quiet "$image" | head -n 1)
if [[ -z "$image_id" ]]; then
  echo "container image not found: $image" >&2
  exit 1
fi
docker image inspect "$image_id" >/dev/null

runtime_uid=$(docker run --rm --entrypoint /usr/bin/id "$image" -u)
if [[ "$runtime_uid" != "10001" ]]; then
  echo "expected container runtime uid 10001, got $runtime_uid" >&2
  exit 1
fi

healthcheck=$(docker image inspect --format '{{json .Config.Healthcheck.Test}}' "$image_id")
if [[ "$healthcheck" != *"/readyz"* ]]; then
  echo "container healthcheck does not target /readyz" >&2
  exit 1
fi

docker run --rm "$image" --help | grep -Fq -- '--check-config'

docker run --rm \
  --read-only \
  -e ETHEREUM_WS_URL=wss://rpc.example.invalid \
  -e AMM_ROUTE_ADMIN_TOKEN=container-smoke-only \
  -v "$config:/config/config.toml:ro" \
  "$image" --check-config

echo "container smoke passed: $image"
