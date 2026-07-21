#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: build-release-image.sh IMAGE [EVIDENCE_JSON]}
evidence=${2:-sidecar-release-evidence.json}
root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
vcs_ref=${GITHUB_SHA:-$(git -C "$root_dir" rev-parse HEAD)}
source_date_epoch=${SOURCE_DATE_EPOCH:-$(git -C "$root_dir" show -s --format=%ct HEAD)}
build_created=${BUILD_CREATED:-$(git -C "$root_dir" show -s --format=%cI HEAD)}
image_version=$(cargo metadata \
  --format-version 1 \
  --no-deps \
  --manifest-path "$root_dir/sidecar/Cargo.toml" \
  | jq -r '.packages[0].version')
build_jobs=${CARGO_BUILD_JOBS:-4}
workspace_dirty=false
if [[ -n "$(git -C "$root_dir" status --porcelain)" ]]; then
  workspace_dirty=true
fi
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
started_seconds=$SECONDS

docker build \
  --progress=plain \
  --provenance=false \
  --build-arg VCS_REF="$vcs_ref" \
  --build-arg IMAGE_VERSION="$image_version" \
  --build-arg BUILD_CREATED="$build_created" \
  --build-arg SOURCE_DATE_EPOCH="$source_date_epoch" \
  --build-arg CARGO_BUILD_JOBS="$build_jobs" \
  --file "$root_dir/sidecar/Dockerfile" \
  --tag "$image" \
  "$root_dir"

duration_seconds=$((SECONDS - started_seconds))
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
inspect=$(docker image inspect "$image")
image_id=$(jq -r '.[0].Id' <<<"$inspect")
image_size=$(jq -r '.[0].Size' <<<"$inspect")
architecture=$(jq -r '.[0].Architecture' <<<"$inspect")
platform_os=$(jq -r '.[0].Os' <<<"$inspect")
runtime_user=$(jq -r '.[0].Config.User' <<<"$inspect")

jq -n \
  --arg image "$image" \
  --arg image_id "$image_id" \
  --arg image_version "$image_version" \
  --arg vcs_ref "$vcs_ref" \
  --arg build_created "$build_created" \
  --arg started_at "$started_at" \
  --arg finished_at "$finished_at" \
  --arg architecture "$architecture" \
  --arg platform_os "$platform_os" \
  --arg runtime_user "$runtime_user" \
  --argjson workspace_dirty "$workspace_dirty" \
  --argjson cargo_build_jobs "$build_jobs" \
  --argjson duration_seconds "$duration_seconds" \
  --argjson image_size_bytes "$image_size" \
  '{
    image: $image,
    image_id: $image_id,
    image_version: $image_version,
    vcs_ref: $vcs_ref,
    build_created: $build_created,
    workspace_dirty: $workspace_dirty,
    started_at: $started_at,
    finished_at: $finished_at,
    duration_seconds: $duration_seconds,
    cargo_build_jobs: $cargo_build_jobs,
    image_size_bytes: $image_size_bytes,
    platform: {os: $platform_os, architecture: $architecture},
    runtime_user: $runtime_user
  }' | tee "$evidence"
