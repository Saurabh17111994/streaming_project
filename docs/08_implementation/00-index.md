# Implementation Dossiers — Index

<!-- markdownlint-disable MD013 -->

## Purpose

This directory converts the stable project requirements and build contracts into implementation-ready engineering dossiers. It defines **how an implementer must realize and prove the system**, while the upstream documents remain authoritative for **what the system must do**.

No document in this directory proves that code exists or that a runtime guarantee has passed. Runtime claims require the evidence records defined here.

## Authority and conflict rule

Use the repository authority order from [`../01_project/00-index.md`](../01_project/00-index.md):

1. Executable implementation and tests
2. Active architectural decisions
3. Validated authoritative DDLs
4. Build contracts
5. Detailed requirements
6. Implementation dossiers and summaries

An implementation dossier must not silently redefine an upstream requirement. If implementation detail exposes a conflict, record it in [`01-documentation-governance.md`](./01-documentation-governance.md) and keep the affected work blocked until the authoritative layer is reconciled.

## Documentation-first workflow

```text
plan.md task
  → authoritative requirement/decision identified
  → implementation dossier completed
  → unresolved external facts recorded as evidence gates
  → design review approved
  → code and DDL implementation
  → focused tests and operational evidence
  → plan.md task verified and struck through
```

Documentation-complete is not code-complete. A checklist item may be marked documentation-complete only when its dossier, traceability, test cases, and unresolved evidence gates are explicit.

## Dossier map

| Dossier | Purpose | Primary plan phases |
| --- | --- | --- |
| [`01-documentation-governance.md`](./01-documentation-governance.md) | Status model, evidence records, placeholders, change control, completion rules | 0, 1, 12 |
| [`02-version-compatibility.md`](./02-version-compatibility.md) | Exact-version matrix, protocol evidence, compatibility classifications | 2, 12 |
| [`03-schema-lifecycle.md`](./03-schema-lifecycle.md) | DDL authority, checksums, clean creation, evolution, reset/replay, retention/offload | 3, 12 |
| [`04-cross-cutting-invariants.md`](./04-cross-cutting-invariants.md) | Identity, immutability, ordering, time, consistency, ownership, safety invariants | 3–7 |
| [`components/01-ingestion.md`](./components/01-ingestion.md) | Broker decode, manifest, packet preservation, fingerprint, raw append, discontinuity evidence | 4 |
| [`components/02-signal-job.md`](./components/02-signal-job.md) | Flink source, dedup, event time, candles, business logic, in-operator ranking, sinks | 5 |
| [`components/03-action-capture.md`](./components/03-action-capture.md) | Postback evidence, correlation, lifecycle and position projections, recovery | 6 |
| [`components/04-babysitter.md`](./components/04-babysitter.md) | Separate checkpointed MVP no-op job and future action boundary | 6 |
| [`components/05-executor.md`](./components/05-executor.md) | Immutable intake, gate, attempts, fencing, OpenAlgo, reconciliation, approvals, audit | 7 |
| [`deployment/01-local-compose.md`](./deployment/01-local-compose.md) | Local-only topology, readiness, effective config, networks, secrets | 8 |
| [`deployment/02-production-swarm.md`](./deployment/02-production-swarm.md) | Four-VM topology, placement, HA, storage, security, rollout | 9 |
| [`deployment/03-observability-operations.md`](./deployment/03-observability-operations.md) | Telemetry envelope, health dimensions, alerts, runbooks, clocks | 10 |
| [`testing/01-test-catalog.md`](./testing/01-test-catalog.md) | Requirement-linked test IDs, fixtures, failure matrix, evidence outputs | 11 |
| [`testing/02-release-evidence.md`](./testing/02-release-evidence.md) | Evidence package and live-money release decision record | 12 |
| [`99-traceability.md`](./99-traceability.md) | Plan issue → dossier → requirements → tests mapping | All |

## Document status vocabulary

Every dossier uses one of these statuses:

| Status | Meaning |
| --- | --- |
| `Draft` | Structure exists but material behavior is unresolved |
| `Design-ready` | Internal behavior is specified; external evidence gates may remain |
| `Implementation-ready` | Required decisions, interfaces, failure behavior, and tests are specified |
| `Evidence-blocked` | Design is usable but exact external/runtime fact remains unproven |
| `Validated` | Pinned implementation and tests prove the documented behavior |
| `Superseded` | Replaced by a newer approved document |

A dossier may be both `Implementation-ready` and `Evidence-blocked`: implementation can proceed behind an adapter or disabled gate, but live-money release cannot.

## Current readiness

| Area | Documentation status | Code status | Release status |
| --- | --- | --- | --- |
| Architecture and ownership | Design-ready | Scaffold | Blocked |
| Broker protocols | Evidence-blocked | Not implemented | Blocked |
| DDL/schema | Design-ready, version validation pending | Generated proposals | Blocked |
| Ingestion | Implementation-ready dossier | Not implemented | Blocked |
| Signal job | Implementation-ready dossier | Not implemented | Blocked |
| Action Capture | Implementation-ready dossier | Not implemented | Blocked |
| Babysitter | Implementation-ready dossier | Not implemented | Blocked |
| Executor | Implementation-ready dossier | Scaffold raises `NotImplementedError` | Blocked |
| Local runtime | Implementation-ready dossier | Scaffold | Blocked |
| Production runtime | Implementation-ready dossier | Not implemented | Blocked |
| Test/evidence program | Implementation-ready dossier | Not implemented | Blocked |

## Mandatory implementation order

1. Approve documentation governance and cross-cutting invariants.
2. Record version/protocol evidence gates.
3. Validate DDL capability and schema lifecycle.
4. Implement ingestion and test fixtures.
5. Implement the Signal job.
6. Implement Action Capture and the no-op Babysitter.
7. Implement Executor last among functional services, with broker calls disabled until safety tests pass.
8. Implement local integration runtime.
9. Implement production Swarm and operational controls.
10. Produce the release evidence package.

Implementation may overlap where dependencies permit, but no downstream task may invent an upstream contract.
