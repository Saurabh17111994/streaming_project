# 08 Local Compose — Phase C Implementation Plan

## Scope
FAIL-001..010 + SAFETY-001 (fail-closed on ambiguity)

Per 08-local-compose.md §L8: restart/recovery for ingestion, Flink JM/TM,
Fluss tablet, Nautilus (critical: durable state + gate stays HALTED), bridge
reconnect, network partition, fake broker timeout/UNKNOWN, double delivery
idempotency; SAFETY-001: ambiguous broker/correlation/position/schema/
credential/gateway/order-status/restart-mid-lifecycle → DO NOT place order.

## Files to create
- `code/01_platform/04_scripts/tests/test_08_local_compose_l8.py` — L8 FAIL+SAFETY (offline contracts: gate safety_halt, deduplicator, idempotent projection, restart docs; gated live probes when stack is up)
- Update `Makefile` — add `test-failure` and extend phase aggregates

## Invariants
- Gate::safety_halt() always returns to HALTED from any state and increments only on non-HALTED halt (proven in gate.rs).
- IntentDeduplicator: FIRST vs DUPLICATE vs HASH_VIOLATION; gateway fail-closed on HASH_VIOLATION.
- PositionProjectorDriver / FlussProjectionWriter idempotency: same event → DUPLICATE, not double-apply.
- Nautilus EXECUTION_ENABLED=false survives restart (compose contract, HEALTH-008).

## Verification
```
make test-failure
pytest code/01_platform/04_scripts/tests/test_08_local_compose_l8.py -v
```
