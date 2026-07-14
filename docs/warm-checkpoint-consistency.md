# Crash-consistent TUI warm checkpoints

The live TUI restores ready pools only when generic EVM cache state,
`AmmRegistrationArchive`, and a canonical block checkpoint come from one
committed generation. A small manifest is the commit record; no mutable cache
file is trusted independently.

## On-disk layout

For chain `1` under `AMM_ROUTE_TUI_CACHE_DIR`:

```text
chain_1/
  amm_manifest.json
  generations/
    gen-<block>-<hash-prefix>-<unique>/
      chain_1/
        evm_state.bin
        bytecodes.bin
        code_seeds.bin
        immutable_data.bin
        amm_registrations.bin
```

Each running persistent TUI writes to a private `.staging-*` directory. The
manifest contains schema version, chain ID, immutable generation name, and
`{ block_number, block_hash }`. Files directly under the legacy `chain_1/`
namespace are not a certified generation.

## Startup

1. Parse the manifest and validate its chain, generation name, state file, and
   registration archive.
2. Copy the selected immutable generation into a private staging generation and
   point `EvmCache` at the staging base directory.
3. Fetch the latest canonical block. A checkpoint is eligible for exact replay
   only when it is not in the future and is at most
   `AMM_ROUTE_TUI_MAX_WARM_CATCHUP_BLOCKS` behind head (256 by default).
4. Fetch an eligible checkpoint block and require the exact stored hash. A
   missing or reorged checkpoint is rejected.
5. On success, restore ready registrations at that point, establish live
   subscriptions, replay intervening canonical blocks in order, and wait for
   repairs before exposing route handles.
6. When verification fails or the checkpoint exceeds the replay ceiling,
   remove copied EVM cache files and perform a normal cold start at
   the verified latest baseline. Registration metadata may remain only as
   pending read-set/layout hints and is rehydrated before publication.

The first run after the legacy layout has no certified checkpoint. It may import
the legacy registration archive as cold-start hints, but deliberately ignores
legacy EVM state. Initial worker admission expands to fit that restored batch so
archives larger than the normal 256-job floor still migrate atomically.

Before the cache moves behind the actor, bootstrap also warms the small shared
set of configured Router02/QuoterV2 accounts. Progressive per-pool hydration does
not use `AdapterRegistry::cold_start_many`'s batch-level quote-target warming;
without this explicit step, immutable search snapshots would contain ready pool
state but no executable quote entrypoint code. The verified accounts are part of
the committed generation and are reused without RPC on later verified resumes.

## Orderly commit

Shutdown first checks that discovery, cold-start, and pending state-update work
is quiescent. If work remains, it stops the runtime and abandons the private
staging generation, leaving the previous manifest-selected generation intact.
This prevents an early interactive quit or failed headless milestone from
replacing a complete registration archive with only the pools that happened to
finish first. Once quiescent, shutdown stops route work, canonical delivery,
and cold-start dispatch before capturing the final coherent AMM snapshot. It
then:

1. explicitly flushes the actor-owned cache into staging;
2. saves the registration archive from the same snapshot into staging;
3. shuts down the AMM runtime and waits for its cache drop/final flush;
4. syncs every staging file and directory;
5. renames staging to a sealed immutable generation and syncs `generations/`;
6. atomically writes, syncs, and renames `amm_manifest.json` **last**;
7. prunes generations not selected by the committed manifest.

`AMM_ROUTE_TUI_PERSIST_CACHE=0` keeps the existing manifest and generations
untouched. It may still update the small legacy registration archive, but a
later persistent run continues from the last manifest-selected generation.

## Failure behavior

| Failure point | Next startup |
|---|---|
| Before staging exists | Previous manifest generation |
| Orderly quit while background work remains | Previous manifest generation; incomplete staging discarded |
| While cache/archive is written | Previous manifest generation; orphan staging ignored |
| After staging sync, before seal rename | Previous manifest generation |
| After seal rename, before manifest replace | Previous manifest generation; unreferenced generation ignored |
| After manifest replace | New complete generation |
| Manifest missing/corrupt or required file absent | Cold start; no persisted EVM state trusted |
| Checkpoint hash no longer canonical | Cold start with registration hints only |
| Checkpoint is more than the configured replay ceiling behind head | Rebuild at latest with registration hints; do not replay an unbounded block range |

The store assumes a single writer per chain namespace. It is process-crash
consistent, not a multi-process database and not an authenticated artifact
format. The registration archive checksum detects accidental corruption; a
party with write access to the cache directory can replace both data and
metadata.

## Verification

Focused tests model process death by abandoning a populated staging generation,
reject incomplete commits, verify canonical-rejection cleanup, and confirm that
cache-disabled sessions do not replace the committed manifest:

```text
cargo test --bin amm-route-tui warm_store::tests
```

The end-to-end headless benchmark additionally exercises canonical checkpoint
verification and replay; see
[`network-cold-start-benchmark.md`](network-cold-start-benchmark.md).
