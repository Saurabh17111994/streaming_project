# 08 Local Compose — Phase B Implementation Plan

## Scope
STREAM-001..010 + EXEC-001..013 + LOCAL-INT-004 (10-instrument fake-broker smoke)
Per spec: same images and execution-net isolation will later run unchanged on the Swarm (09 v1 4 VMs Manager+Worker, v2 7 VMs Manager-ONLY + Workers); only placement changes.

## Files to create
- `code/01_platform/04_scripts/local_int_004_smoke.py` — the harness: 10 random instruments → fake bridge (Place/Modify/Cancel + UNKNOWN/REJECT + fill stream) → Nautilus order lifecycle + position → Fluss projections → Babysitter zero-action proof. Offline-contract mode always passes; live mode (execution-t3 fake) does real HTTP/Fluss checks.
- `code/01_platform/04_scripts/tests/test_08_local_compose_l6_l7.py` — STREAM + EXEC lifecycle (offline contracts + gated container probes)
- `code/01_platform/04_scripts/tests/test_08_local_compose_l10.py` — LOCAL-INT-004 smoke (contract + live)
- `Makefile` — add `test-execution` and extend `test-08-phaseA` to `test-08-phaseB`

## Design
- Fake bridge semantics derived from `code/02_services/06_execution_bridge/go-bridge/models.go` and the Rust `FakeBridge` — no live Arrow creds required.
- EXEC gate is the Rust `crate::gate::Gate` (HALTED → RECONCILING → APPROVAL_PENDING → ENABLED, safety_halt→HALTED); HEALTH `trading_ready = gate==ENABLED` — proven both in Rust unit tests and via the new Python contract tests (cargo test + HTTP probes when execution-t3 is up).
- STREAM path is ingestion → Fluss (raw_table_1 LOG) → Flink Signal/Babysitter; offline proof is DDL + job topology + ingestion quarantine/throughput contract; live proof is TCP/Fluss append + Flink checkpoint when the stack is up.

## Verification
```
make test-execution              # STREAM+EXEC+LOCAL-INT-004 offline contracts
python3 code/01_platform/04_scripts/local_int_004_smoke.py --offline   # contract only
python3 code/01_platform/04_scripts/local_int_004_smoke.py --live      # requires execution-t3 fake
pytest code/01_platform/04_scripts/tests/test_08_local_compose_l10.py -v
```
