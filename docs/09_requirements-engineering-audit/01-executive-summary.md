# Executive Summary

## Review verdict

- **Scope:** `docs/02_requirements`
- **Review lens:** integrated Flink engineering, Flink + Fluss trading design, state/recovery, latency, schema, operations, and requirements quality
- **Status:** `OPEN / BLOCKED`
- **Live-money status:** correctly remains disabled
- **Implementation status:** requirements preserve the intended architecture but are not fully implementation-ready

## What is already strong

The requirements correctly establish:

- two Flink jobs: Signal and strict no-op Babysitter;
- in-operator ranking rather than a separate Ranking job;
- immutable instructions;
- separate order lifecycle and position aggregates;
- durable Executor gate, attempts, mapping, reconciliation, and audit state;
- bounded best-effort fingerprints instead of assumed broker sequences;
- independent postback writes with quarantine;
- fail-closed behavior for unknown money-moving outcomes;
- three workload campaigns: 75k, 112.5k, and 150k ticks/s;
- explicit live-money evidence gates.

## Main conclusion

Several critical mechanisms are described as intentions but lack a complete owner, state model, interface, ordering rule, or binary acceptance test. The most important gaps are:

1. Global ranking and reservation topology.
2. Reservation ownership and durability.
3. Safety-halt propagation to Executor.
4. Executor fencing protocol.
5. Action Capture projection ledger.
6. Account/portfolio scope identities.
7. Instruction visibility versus checkpoint commit latency.
8. Position and lifecycle version semantics.
9. RPO and failure-detection boundaries.
10. N+1 resource and recovery capacity.

## Recommended next action

Resolve ranking/reservation ownership first. It affects the Signal job graph, Flink state, Fluss tables, instruction publication, Executor interaction, latency, and recovery. Do not generate final physical DDLs until this decision and the version matrix are closed.

## Classification

| Category | Status |
| --- | --- |
| Semantics | `FAIL` |
| Topology | `FAIL` |
| State | `FAIL` |
| Latency/throughput | `UNVERIFIED` |
| Memory | `UNVERIFIED` |
| Checkpoint/recovery | `FAIL` |
| Capacity | `UNVERIFIED` |
| Operations | `FAIL` |
| Verification | `FAIL` |
