# Executor — implementation handoff

> **Status:** scaffold only; `main.py` raises `NotImplementedError`. Use [`../../../docs/08_implementation/components/05-executor.md`](../../../docs/08_implementation/components/05-executor.md) as the implementation contract.
>
> **Live money:** prohibited. The default and uncertain state is `HALTED`.

## Current target

```text
immutable Trade_Decisions
  → validation and duplicate/request-hash guard
  → durable Execution_Gate check
  → durable Execution_Attempt + client reference
  → fencing check
  → OpenAlgo broker adapter call
  → verified mapping or UNKNOWN
  → reconciliation and immutable Execution_Audit
```

Executor writes only execution-owned state:

- `Execution_Gate`
- `Execution_Attempts`
- `Order_Correlation`
- `Execution_Audit`

It is not read-only. It never mutates strategy/candidate/ranking fields and never treats broker REST as exactly-once.

## Implementation checklist

- [ ] Pin OpenAlgo/broker request, response, timeout, identity, and idempotency evidence.
- [ ] Implement immutable decision intake and validation.
- [ ] Implement durable gate and epoch checks.
- [ ] Implement attempt/request-hash/client-reference protocol.
- [ ] Implement account/order-partition fencing.
- [ ] Implement OpenAlgo adapter and explicit result classification.
- [ ] Implement mapping, UNKNOWN state, reconciliation, and quarantine interaction.
- [ ] Implement two-person authenticated resume.
- [ ] Implement immutable audit and readiness/telemetry.
- [ ] Pass crash-window, duplicate, fencing, reconciliation, approval, and reconstruction tests.

## References

- Requirements: `../../../docs/02_requirements/02-functional/07-executor.md`
- Contract: `../../../docs/04_contracts/07-executor.md`
- Implementation dossier: `../../../docs/08_implementation/components/05-executor.md`
- Release evidence: `../../../docs/08_implementation/testing/02-release-evidence.md`
