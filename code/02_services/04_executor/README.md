# Executor — implementation handoff

> **Status:** scaffold only; `main.py` raises `NotImplementedError`. **2026-08-18:** the Executor
> scope is now part of the integrated **Execution Core** dossier
> [`../../../docs/08_implementation/05-execution-core.md`](../../../docs/08_implementation/05-execution-core.md)
> (order path — Nautilus execution engine + go-arrow bridge as the sole Arrow-facing
> component). The checklist below describes the superseded standalone-Executor design.
>
> **Live money:** prohibited. The default and uncertain state is `HALTED`.

## Current target

```text
immutable Trade_Decisions
  → validation and duplicate/request-hash guard
  → durable Execution_Gate check
  → durable Execution_Attempt + client reference
  → fencing check
  → Arrow REST broker call (POST /order/regular)
  → verified mapping or UNKNOWN
  → reconciliation and immutable Execution_Audit
```

Executor writes only execution-owned state:

- `Execution_Gate`
- `Execution_Attempts`
- `Order_Correlation`
- `Execution_Audit`

It is not read-only. It never mutates strategy/candidate/ranking fields and
never treats broker REST as exactly-once.

## Implementation checklist

- [ ] Pin Arrow REST request, response, timeout, identity, and idempotency evidence.
- [ ] Implement immutable decision intake and validation.
- [ ] Implement durable gate and epoch checks.
- [ ] Implement attempt/request-hash/client-reference protocol.
- [ ] Implement account/order-partition fencing.
- [ ] Implement Arrow REST adapter and explicit result classification.
- [ ] Implement mapping, UNKNOWN state, reconciliation, and quarantine interaction.
- [ ] Implement single-operator authenticated resume (Saurabh, DEC-044).
- [ ] Implement immutable audit and readiness/telemetry.
- [ ] Pass crash-window, duplicate, fencing, reconciliation, approval, and reconstruction tests.

## References

- Requirements: `../../../docs/02_requirements/02-functional/07-executor.md`
- Contract: `../../../docs/04_contracts/07-executor.md`
- Implementation dossier: `../../../docs/08_implementation/05-execution-core.md` (integrated Execution Core — order path)
- Release evidence: `../../../docs/08_implementation/11-testing-and-release.md`
