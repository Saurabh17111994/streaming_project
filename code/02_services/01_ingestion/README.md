# Ingestion — implementation handoff

> **Status:** implementation not started. Use the implementation dossier at [`../../../docs/08_implementation/components/01-ingestion.md`](../../../docs/08_implementation/components/01-ingestion.md) as the build contract.
>
> **Live money:** disabled. Broker protocol, decoder, credentials, limits, and exact versions remain evidence-gated.

## Current target

```text
Evidence-approved broker market stream
  → versioned decoder
  → original packet + normalized typed fields + fingerprint
  → bounded Fluss Java client append
  → raw_table_1

Connection/subscription/heartbeat/time evidence
  → suspected_discontinuities
```

The current architecture does **not** assume Zerodha/Kite, `seq_no`, exact sequence gaps, or a verified broker event ID. Use the explicit placeholders and evidence workflow in the dossier until the broker protocol is approved.

## Implementation checklist

- [ ] Pin broker protocol/decoder and Fluss client versions.
- [ ] Build golden packet corpus and byte/hash fixtures.
- [ ] Implement manifest loading and subscription completeness checks.
- [ ] Implement decode, normalize, validity, packet preservation, and fingerprint modules.
- [ ] Implement bounded append/retry/backpressure behavior.
- [ ] Implement suspected-discontinuity and quarantine evidence.
- [ ] Implement liveness/readiness/telemetry.
- [ ] Pass ingestion unit, integration, failure, and workload tests.

## References

- Requirements: `../../../docs/02_requirements/02-functional/01-ingestion.md`
- Contract: `../../../docs/04_contracts/01-ingestion.md`
- Implementation dossier: `../../../docs/08_implementation/components/01-ingestion.md`
- Cross-cutting invariants: `../../../docs/08_implementation/04-cross-cutting-invariants.md`
- Version matrix: `../../../docs/08_implementation/02-version-compatibility.md`
