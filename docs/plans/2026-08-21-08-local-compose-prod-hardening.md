# 08 Local Compose — Production-Hardening Extension

## Scope
Beyond the 120 canonical IDs, add PROD-HARDENING tests that prove local
Compose *cannot* be mistaken for production and that its gaps are explicit.
These do NOT make local prod-ready (spec: Prohibited use — HA evidence),
but they make misuse impossible and Swarm readiness auditable.

## New suites
- PROD-001: Single-node simplification acknowledged (ZK 1 vs 3, one Fluss tablet)
- PROD-002: Checkpoints are local volume (not S3) — Swarm needs s3://
- PROD-003: No prod endpoint/bucket/creds accepted by local profile
- PROD-004: No secret in git history / .env.example
- PROD-005: No :latest, digests pinned
- PROD-006: Resource limits + JVM 65/35/85 wiring
- PROD-007: Restart policy safe (>= unless-stopped, no silent disable)
- PROD-008: DDL apply idempotency + evidence 2775/664 ownership
- PROD-009: OTel retry_on_failure + O2 auth held by collector (ingestion cred-free)
- PROD-010: Gate monotonic HALTED→RECONCILING→APPROVAL→ENABLED, safety_halt only regress
- PROD-011: execution-t3 disabled by default (no accidental live)
- PROD-012: No AWS creds in FLUSS_PROPERTIES (env only)
- PROD-013: Lake snapshot shared volume (coordinator+tablet readable)
- PROD-014: 10-instrument smoke vs 1024-instrument live manifest (1024 cap documented)
- PROD-015: Checkpoint + restart constants pinned in PlatformConfig
- PROD-016: Ingestion backpressure guards (MAX_PENDING, PENDING_WARNING, reconnect 1s/30s)
- PROD-017: Audit full-audit / docs-audit gates exist and pin-check exists
- PROD-018: Implementation gate (no CEP in order path) + stale-table scan

## Files
- `code/01_platform/04_scripts/tests/test_08_local_compose_prod.py` — 18 PROD tests, offline-contract, gated live where noted
- `Makefile` — `test-prod-hardening`
- This plan

## Verification
```
make test-prod-hardening
pytest code/01_platform/04_scripts/tests/test_08_local_compose_prod.py -v
```
Offline PASS, live gates skip when Swarm not present.
