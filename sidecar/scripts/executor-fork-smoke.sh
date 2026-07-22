#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_FILE=$(mktemp)

cleanup() {
  rm -f "$OUTPUT_FILE"
}
trap cleanup EXIT

if [[ -z "${ETHEREUM_RPC_URL:-}" ]]; then
  echo "executor-fork-smoke: ETHEREUM_RPC_URL is required" >&2
  exit 2
fi

cd "$ROOT_DIR"
set -o pipefail
forge test --match-contract ExperimentalExecutorRouterForkTest -vv | tee "$OUTPUT_FILE"

if grep -q '\[SKIP' "$OUTPUT_FILE"; then
  echo "executor-fork-smoke: fork tests were skipped" >&2
  exit 1
fi

if ! grep -Eq '7 passed; 0 failed; 0 skipped' "$OUTPUT_FILE"; then
  echo "executor-fork-smoke: expected all 7 pinned fork tests to pass without skips" >&2
  exit 1
fi
