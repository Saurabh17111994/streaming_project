# Remediation Roadmap

<!-- markdownlint-disable MD013 -->

This roadmap is ordered by dependency. Do not skip ahead to physical DDL or live-money behavior while an upstream contract remains unresolved.

## Phase A — Architectural reconciliation

| Order | Work item | Resolves | Required output | Exit gate |
| ---: | --- | --- | --- | --- |
| 1 | Define account, portfolio, and execution scopes | AUD-C04 | Identity and ownership update | Isolation tests specified |
| 2 | Define ranking/reservation topology | AUD-C01, AUD-C02 | Signal graph, keying, state owner, reservation contract | Concurrent capacity test design approved |
| 3 | Define safety-halt control interface | AUD-C03 | Versioned durable halt event/API | Fault-to-gate timing test defined |
| 4 | Define Executor fencing | AUD-C05 | Lease/fence protocol and interleaving model | Split-brain test design approved |
| 5 | Define immutable Trade_Decisions feed | AUD-C07 | Final table/interface contract | No Executor mutation path |
| 6 | Add projection ledger/equivalent | AUD-C06 | Logical schema and recovery protocol | Crash-after-each-step test defined |

## Phase B — Semantics and state

| Order | Work item | Resolves | Required output | Exit gate |
| ---: | --- | --- | --- | --- |
| 7 | Canonicalize watermark/finalization semantics | AUD-H01, AUD-H02 | Time and source-partition contract | Deterministic event-time fixtures |
| 8 | Define dedup/replay horizon | AUD-H03 | Horizon calculation and state budget | Config rejection and growth tests |
| 9 | Define Signal external-state ingress | AUD-H04 | Changelog/source/freshness contract | Stale/partial visibility test |
| 10 | Define supersession protocol | AUD-H05 | Immutable cancellation/supersession interface | Old/new instruction race test |
| 11 | Complete position/fill arithmetic | AUD-H06 | Position state machine and decimal policy | Fill replay/rebuild test |
| 12 | Define lifecycle precedence without assumed broker sequence | AUD-H07 | Evidence-based transition algorithm | Out-of-order conflict test |

## Phase C — Quantitative and operational closure

| Order | Work item | Resolves | Required output | Exit gate |
| ---: | --- | --- | --- | --- |
| 13 | Split visibility, sink acknowledgement, and checkpoint SLOs | AUD-C08 | Timestamp/boundary specification | Pinned connector benchmark |
| 14 | Define per-boundary RPO and failure clocks | AUD-H08, AUD-H12 | RPO/RTO matrix and detector definitions | Fault timeline evidence plan |
| 15 | Define N+1 capacity and recovery | AUD-H09 | Resource, slots, bandwidth, catch-up model | One-VM test acceptance criteria |
| 16 | Tighten transport encryption policy | AUD-H10 | Security exception policy | Security gate becomes binary |
| 17 | Define observability degradation | AUD-H11 | Component-specific readiness/degradation matrix | Backend outage tests |
| 18 | Define OpenAlgo reconciliation capabilities | AUD-M09 | API capability contract and sandbox evidence | Unknown-outcome recovery test |
| 19 | Define control-plane interfaces | AUD-M10 | Authenticated commands and audit rules | Authorization/concurrency tests |
| 20 | Define EOD controller ownership | AUD-M12 | Manifest/retry/expiry controller contract | Failed-offload expiry test |

## Phase D — Traceability and cleanup

| Order | Work item | Resolves | Required output |
| ---: | --- | --- | --- |
| 21 | Normalize requirement IDs | AUD-M01 | One ID family across requirements/contracts/tests |
| 22 | Fix functional index and table names | AUD-M02, AUD-M03, AUD-M04 | Consistent navigation and vocabulary |
| 23 | Rename timestamp fields | AUD-M05 | Canonical event timestamp vocabulary |
| 24 | Add state bounds and candidate lifecycle | AUD-M06, AUD-M08 | State budgets and cleanup rules |
| 25 | Add closed-candle interface | AUD-M07 | Complete in-job event contracts |
| 26 | Convert acceptance lists to unique evidence IDs | AUD-M13 | Binary traceability matrix |
| 27 | Remove formatting/history debris | AUD-M14 | Clean authoritative requirements |

## Release-blocking exit criteria

The requirements are ready to drive implementation only when:

- all critical findings have an accepted resolution;
- all evidence-gated external facts have owners and evidence artifacts;
- every table/state has owner, key, writer, reader, retention, cleanup, and recovery source;
- every interface has version, payload, failure, idempotency, and compatibility behavior;
- every SLO has boundary, percentile, workload, duration, clock, and acceptance threshold;
- every critical failure has detection, safe state, recovery, and rollback behavior;
- all requirement IDs map to contracts and acceptance tests;
- DDL generation is allowed only after the pinned Flink/Fluss capability suite passes.
