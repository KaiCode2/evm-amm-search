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
