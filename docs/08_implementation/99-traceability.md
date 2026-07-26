# Implementation Traceability Matrix

<!-- markdownlint-disable MD013 -->

## Purpose

This matrix maps audit findings and `plan.md` phases to the implementation dossiers and executable evidence families. It prevents an issue from disappearing during documentation or code work.

## Audit issue traceability

| Audit issues | Primary dossier | Test/evidence families |
| --- | --- | --- |
| P0-1 | `components/05-executor.md` | `EXE-*`, `REL-EXE-*`, `REL-CRASH-*`, `REL-HALT-*` |
| P0-2 | All component dossiers | `ING-*`, `SIG-*`, `AC-*`, `BAB-*`, `EXE-*` |
| P0-3 | `components/02-signal-job.md` | Job submission/readiness integration tests |
| P0-4 | `01-documentation-governance.md`, ingestion/action dossiers | `BROKER-MD-*`, `BROKER-PB-*`, stale-term CI gate |
| P1-1 | Component dossiers, `02-version-compatibility.md` | Build entry-point and artifact tests |
| P1-2 | Governance and cross-cutting invariants | Stale-term CI gate |
| P1-3 | `03-schema-lifecycle.md` | `COMPAT-FLUSS-*`, schema workflow tests |
| P1-4, P1-5 | `03-schema-lifecycle.md`, release evidence | `PERF-EOD-*`, `REL-RET-*` |
| P1-6, P1-18 | Local/production deployment dossiers | Health/readiness/startup tests |
| P1-7, P1-19 | Local and production deployment dossiers | Volume/replication/one-VM tests |
| P1-8 | `deployment/02-production-swarm.md` | `REL-HA-*` |
| P1-9, P2-1 | Version dossier, production deployment | Image/digest/SBOM CI gates |
| P1-10, P1-11 | Version/schema/local/production dossiers | Effective-config/S3/checkpoint tests |
| P1-12 | `03-schema-lifecycle.md` | Routing/null/skew tests |
| P1-13 | Schema and cross-cutting invariants | Immutable duplicate/mutation tests |
| P1-14 | Schema and Action Capture dossiers | State precedence/stale/conflict tests |
| P1-15, P1-16 | Executor and cross-cutting invariants | Correlation/attempt/concurrency tests |
| P1-17 | Schema, Executor, release evidence | `EXE-AUDIT-*`, `REL-RET-*` |
| P1-20, P1-22 | Local/production deployment dossiers | Network exposure/deny-path tests |
| P1-21 | Governance/local/production/security dossiers | Secret scan/rotation/least privilege |
| P2-2 | Local dossier | Documentation and effective-mount checks |
| P2-3 | Signal dossier/version compatibility | Pinned connector checkpoint tests |
| P2-4 | Ingestion dossier | Discontinuity/no-sequence tests |
| P2-5 | Action Capture dossier | Duplicate/no-sequence postback tests |
| P2-6, P2-7, P2-8, P2-9 | Ingestion and schema dossiers | Manifest parser/validation/injection/projection tests |
| P2-10 | Test catalog | CI test-family coverage gate |
| P2-11, P2-12 | Schema lifecycle | Manifest/checksum/parity/reset/replay tests |
| P2-13 | Signal dossier | Two-job topology/job lifecycle tests |
| P2-14 | Executor dossier | Owned-state write tests |
| P2-15 | Action Capture/Babysitter/Executor dossiers | Aggregate ownership tests |
| P2-16, P2-17 | Observability dossier | Telemetry, clock, SLO and alert tests |
| P3-1, P3-2, P3-3, P3-4 | Governance/local/version/test dossiers | Docs links/build commands/test-stage/local-only CI checks |

## Plan phase traceability

| Plan phase | Dossiers |
| --- | --- |
| 0 Governance | `01-documentation-governance.md`, `testing/02-release-evidence.md` |
| 1 Reconciliation | Governance, cross-cutting invariants, component dossiers |
| 2 Versions | `02-version-compatibility.md` |
| 3 Data model | `03-schema-lifecycle.md`, `04-cross-cutting-invariants.md` |
| 4 Ingestion | `components/01-ingestion.md` |
| 5 Signal job | `components/02-signal-job.md` |
| 6 Action/Babysitter | `components/03-action-capture.md`, `components/04-babysitter.md` |
| 7 Executor | `components/05-executor.md` |
| 8 Local runtime | `deployment/01-local-compose.md` |
| 9 Production runtime | `deployment/02-production-swarm.md` |
| 10 Observability | `deployment/03-observability-operations.md` |
| 11 Testing | `testing/01-test-catalog.md` |
| 12 Release | `testing/02-release-evidence.md` |

## Requirements traceability

| Requirement family | Owning dossier |
| --- | --- |
| `REQ-ING-*` | Ingestion |
| `REQ-ST-*` / data requirements | Schema lifecycle |
| `REQ-FC-*` | Signal job |
| `REQ-BL-*` | Signal job |
| `REQ-RNK-*` | Signal job |
| `REQ-AC-*` | Action Capture |
| `REQ-BAB-*` | Babysitter |
| `REQ-EXE-*` | Executor |
| `REQ-OBS-*` | Observability/operations |
| `REQ-PF-*` | Local/production deployment and version compatibility |

## Documentation completion statement

The dossiers specify implementation behavior but do not prove that code, DDL, deployments, or tests exist. Corresponding `plan.md` implementation checkboxes remain unchecked until executable evidence is produced. Documentation tasks may record these dossier paths as evidence and move to documentation-complete status.
