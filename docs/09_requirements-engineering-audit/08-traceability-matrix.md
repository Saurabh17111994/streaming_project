# Traceability Matrix

<!-- markdownlint-disable MD013 -->

This matrix maps audit findings to affected artifacts and the minimum evidence required to close them.

| Finding | Primary requirements | Contracts/dossiers | Data/DDL impact | Minimum validation | Owner | Status |
| --- | --- | --- | --- | --- | --- | --- |
| AUD-C01 | `REQ-SS-001`, `REQ-SS-005`, `REQ-RNK-005` | Business Logic, Ranking, Compute | Reservation scope/state/table | Concurrent global-capacity test | Strategy/Platform | OPEN |
| AUD-C02 | `REQ-SS-005`, `REQ-FLS-009` | Business Logic, Storage, Schema lifecycle | Reservation state and rebuild | Crash/replay transition test | Strategy/Storage | OPEN |
| AUD-C03 | `REQ-FC-009`, `REQ-AC-003`, `REQ-EXE-002` | Compute, Action Capture, Executor | Safety-halt event/audit | Fault-to-halt timing test | Execution/Platform | OPEN |
| AUD-C04 | Identity sections, `REQ-FLS-004`, `REQ-EXE-001` | Cross-cutting invariants, Executor | Scope columns and keys | Cross-account isolation test | Platform/Execution | OPEN |
| AUD-C05 | `REQ-EXE-008`, `REQ-EXE-010` | Executor, platform runtime | Fence token/lease state | Split-brain and stale-owner test | Execution | EVIDENCE-BLOCKED |
| AUD-C06 | `REQ-AC-006`, `REQ-FLS-003` | Action Capture, schema lifecycle | Projection ledger/equivalent | Crash-after-step and recovery test | Action Capture/Storage | OPEN |
| AUD-C07 | `REQ-FLS-008`, `REQ-EXE-004`, `REQ-EXE-006` | Storage, Executor | Immutable feed and mapping | Mutation/replay test | Signal/Execution | RECONCILIATION-REQUIRED |
| AUD-C08 | `REQ-FC-008`, `REQ-RNK-006`, NFR latency | Compute, Ranking, version dossier | Visibility/checkpoint metadata | Pinned connector latency test | Platform/Signal | EVIDENCE-BLOCKED |
| AUD-H01 | `REQ-FC-004`, `REQ-FC-006` | Compute | Window/finality metadata | Event-time finality test | Compute | OPEN |
| AUD-H02 | `REQ-ING-003`, `REQ-FC-004` | Ingestion, Compute | Source split/partition metadata | Idle/reconnect test | Ingestion/Compute | OPEN |
| AUD-H03 | `REQ-ING-008`, `REQ-FC-003` | Ingestion, Compute | Dedup config/state budget | TTL and state-growth test | Ingestion/Compute | OPEN |
| AUD-H04 | `REQ-SS-001`, interfaces | Business Logic, Compute | Lifecycle/reservation source | Stale/partial-state test | Strategy/Platform | OPEN |
| AUD-H05 | `REQ-SS-004`, `REQ-RNK-004`, `REQ-EXE-004` | Business Logic, Ranking, Executor | Supersession/cancel metadata | Replacement race test | Signal/Execution | OPEN |
| AUD-H06 | `REQ-AC-007`, `REQ-BB-002` | Action Capture, Babysitter | Position arithmetic/schema | Fill replay/rebuild test | Action Capture | OPEN |
| AUD-H07 | `REQ-AC-005` | Action Capture | Version/evidence fields | Out-of-order conflict test | Action Capture | EVIDENCE-BLOCKED |
| AUD-H08 | NFR durability, operational recovery | Storage, release evidence | RPO fields/manifest | Per-boundary RPO test | Platform | OPEN |
| AUD-H09 | `REQ-FLS-001`, `REQ-PF-002`, `REQ-PF-009` | Storage, production runtime | Resource/replication config | One-VM loss test | Platform | OPEN |
| AUD-H10 | NFR security, `REQ-PF-003` | Platform/security | TLS and exception policy | Network encryption test | Security/Platform | RECONCILIATION-REQUIRED |
| AUD-H11 | `REQ-OBS-004`, readiness requirements | Observability/runtime | Buffer/readiness state | Backend outage test | Operations | OPEN |
| AUD-H12 | NFR latency/recovery | Observability, operations | Detector timestamps | Fault timeline test | Operations | OPEN |
| AUD-M01–M14 | Index, traceability, segment requirements | All affected dossiers | Naming/ID/metadata cleanup | CI link/format checks | Platform | OPEN |

## Closure rule

A finding is `CLOSED` only when:

1. the authoritative requirement/decision is updated;
2. affected contracts and DDL proposals are reconciled;
3. implementation and test references are updated;
4. acceptance evidence exists with version/environment metadata;
5. the release owner accepts residual risk or confirms the risk is resolved.
