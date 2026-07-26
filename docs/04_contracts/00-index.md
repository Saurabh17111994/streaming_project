# Build Contracts — Index

These contracts translate the production requirements into implementation boundaries. They are derived from the active project decisions and requirements; they do not replace executable code, tests, reconciled DDLs, or evidence-gated external protocol specifications.

## Status

All contracts are **blocked for live-money release** until exact Arrow/OpenAlgo/Flink/Fluss versions and protocol evidence pass. The platform is pre-production, so stale schemas and contracts are replaced as a clean break.

## Contract reconciliation rule

When `docs/01_project` or `docs/02_requirements` changes, the affected contract, cross-cutting requirements, DDLs, tests, metrics, and runbooks SHALL be reconciled together. A contract is not considered current merely because its segment file still exists. Any unresolved contradiction is a release blocker and SHALL be recorded as evidence-gated rather than guessed.

Cross-cutting requirements apply to every contract:

- `../02_requirements/03-non-functional.md` — SLOs, delivery semantics, safety, durability, and security
- `../02_requirements/04-data.md` — identities, schemas, ownership, retention, and evolution
- `../02_requirements/05-interfaces.md` — interface versions, payloads, failure behavior, and compatibility
- `../02_requirements/06-operational.md` — startup, health, recovery, deployment, and release gates

## Contracts

| Segment | Contract | Mandatory boundary |
| --- | --- | --- |
| Ingestion | `01-ingestion.md` | Original packet bytes + typed fields + bounded fingerprint |
| Storage | `02-storage.md` | Three-node Fluss, reconciled schemas, retention/offload gate |
| Compute | `03-compute.md` | Final 15-second candles and tested checkpoint boundary |
| Business Logic | `04-business-logic.md` | Forming-bar candidates, reservations, immutable instructions |
| Babysitter | `05-babysitter.md` | Separate checkpointed no-op in MVP |
| Action Capture | `06-action-capture.md` | Immutable postbacks, lifecycle and position projections |
| Executor | `07-executor.md` | Durable gate, attempts, mappings, reconciliation, OpenAlgo |
| Observability | `08-observability.md` | Proof of every safety/release gate |
| Platform | `09-platform-runtime.md` | Compose local, four-VM Swarm production |
| Ranking | `10-ranking.md` | Pure in-operator scoring/selection; no separate job |

The identity model is `candidate_id`, `instruction_id`, `execution_attempt_id`, `client_order_ref`, `broker_order_id`, `trade_context_id`, `position_id`, `postback_event_id`, and future `action_id`. Generic `order_id` is prohibited across domain boundaries.

Every contract must define its owner, inputs, outputs, state/identity rules, delivery guarantee, failure behavior, observability, and acceptance evidence. Exact external protocol values, connector semantics, and unsupported DDL properties remain evidence-gated until pinned tests pass.
