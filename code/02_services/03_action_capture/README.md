# Action Capture — implementation handoff

> **Status:** implementation not started. Use the implementation dossier at [`../../../docs/08_implementation/components/03-action-capture.md`](../../../docs/08_implementation/components/03-action-capture.md).
>
> **Live money:** disabled until postback identity, correlation, projection, and reconciliation evidence passes.

## Current target

```text
Evidence-approved broker postback stream
  → original payload + hash + postback_event_id/fingerprint
  → verified correlation or Postback_Quarantine
  → Fills_table immutable append
  → Order_Lifecycle projection
  → fill-derived Positions projection
```

The current architecture does **not** assume Kite, `order_id`, `postback_seq`, or proximity-based correlation. Order lifecycle and position lifecycle are separate aggregates.

## Implementation checklist

- [ ] Pin postback protocol/status/identity behavior and versions.
- [ ] Implement payload preservation, decoder, identity, and fingerprint.
- [ ] Implement broker-ID/client-reference/evidence-based correlation.
- [ ] Implement immutable Fills audit and quarantine.
- [ ] Implement projection ledger for partial writes/restart recovery.
- [ ] Implement lifecycle precedence and terminal-state protection.
- [ ] Implement fill-derived position projection.
- [ ] Implement readiness, backlog, metrics, and alerts.
- [ ] Pass duplicate, out-of-order, conflict, crash-window, rebuild, and audit tests.

## References

- Requirements: `../../../docs/02_requirements/02-functional/06-action-capture.md`
- Contract: `../../../docs/04_contracts/06-action-capture.md`
- Implementation dossier: `../../../docs/08_implementation/components/03-action-capture.md`
- Schema lifecycle: `../../../docs/08_implementation/03-schema-lifecycle.md`
