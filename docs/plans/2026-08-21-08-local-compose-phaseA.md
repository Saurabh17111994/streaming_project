# 08 Local Compose — Phase A Implementation Plan

## Scope
Implements `docs/08_implementation/08-local-compose.md` Phase A (Must exist before usable):
CONFIG-001..006, HEALTH-001..008, NETWORK-001..010 + SEC-001..015, START-001..006, SCHEMA-001..006, JOB-001..005

Later phases: B (STREAM+EXEC+LOCAL-INT), C (FAIL+SAFETY), D (OBS+RES+PERF) — separate commits.

## Files to create/modify
- `code/01_platform/04_scripts/tests/test_08_local_compose_l0.py` — L0 offline static checks (CONFIG)
- `code/01_platform/04_scripts/tests/test_08_local_compose_l1_l3.py` — L1 HEALTH + L3 START (compose healthcheck/readiness contract, dependency ordering)
- `code/01_platform/04_scripts/tests/test_08_local_compose_l2.py` — L2 NETWORK + SEC (extends execution_network_check; offline + container probes when available)
- `code/01_platform/04_scripts/tests/test_08_local_compose_l4.py` — L4 SCHEMA + L5 JOB subset (manifest, DDL, table existence contract)
- `Makefile` — add `test-local`, `test-network`, `test-08-phaseA` targets
- `code/01_platform/04_scripts/08_local_compose_checks.py` — shared helpers (compose JSON loader, image tag lint, secret scan, DDL catalog)

## Acceptance mapping
Every test asserts its spec ID in the failure message so `pytest -v` maps 1:1 to the doc table.
Offline tests PASS without containers; container-gated tests SKIP (not FAIL) when docker unavailable — CI still green, `COMPOSE_GATED=1` runs them.

## Verification
```
python -m pytest code/01_platform/04_scripts/tests/test_08_local_compose* -v
make test-local        # L0 offline only
make test-network      # L2 (offline + gated container probes)
make test-08-phaseA    # all Phase A
```
