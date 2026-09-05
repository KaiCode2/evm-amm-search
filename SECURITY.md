# Security policy

## Supported releases

Security fixes are applied to the latest `0.1.x` release of `evm-amm-search`
and the latest published `evm-amm-route-sidecar` beta. Older prerelease images
may be removed or superseded rather than patched in place.

## Reporting a vulnerability

Please report suspected vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/KaiCode2/evm-amm-search/security/advisories/new).
Do not open a public issue for an undisclosed vulnerability or include secrets,
private RPC endpoints, keys, signatures, or transaction credentials in a report.

Include the affected crate or image version, image digest when applicable,
configuration boundary, reproduction steps, impact, and any suggested
mitigation. We will coordinate disclosure after the issue has been understood
and a safe remediation is available.

## Beta execution boundary

The sidecar and executor are unaudited beta software. Executable quoting is
disabled by default, the service never holds keys or submits transactions, and
operators remain responsible for token policy, signing, fee policy, submission,
and MEV protection. The current limitations and release blockers are tracked in
[`sidecar/PRODUCTION_READINESS.md`](sidecar/PRODUCTION_READINESS.md).

## Alloy LRU advisory scope

`RUSTSEC-2026-0253` concerns `lru::LruCache::pop` when a stored key's destructor
panics and execution subsequently continues after catching that panic. The
remaining `lru 0.16.4` copy comes only from `alloy-provider 1.6.3`. Its block
stream uses `BlockNumber` (`u64`) keys and its cache layer uses `B256` keys;
neither has a custom destructor. This dependency remains affected as a library,
but the key-destructor trigger is absent from these selected Alloy uses.
Reassess on an Alloy or key-type change. The fix is in lru 0.18.2, outside
Alloy's 0.16 dependency range; no registry patch is hidden in this release.

The TUI dependency set uses the patched `lru 0.18.2` where that version range
is supported. Do not generalize the Alloy assessment to other lru consumers.
