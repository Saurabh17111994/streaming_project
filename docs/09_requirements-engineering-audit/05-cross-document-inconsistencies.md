# Cross-Document Inconsistencies

<!-- markdownlint-disable MD013 -->

## Active decisions versus requirements

| Topic | Current state | Required reconciliation |
| --- | --- | --- |
| Ranking | Decisions and requirements correctly specify in-operator ranking; older prose may describe a separate job | Remove remaining stale separate-ranking references outside the active authority layer |
| Order identity | Active model uses instruction, attempt, client reference, and broker order identities | Ensure no new generic `order_id` appears; add account/portfolio scope identities |
| Trade Decisions | Requirements still say `LOG or KV` and allow possible Executor reference assignment | Fix to one immutable Signal-owned feed plus Executor-owned attempt/reference state |
| Reservation | States are defined but owner and durable storage are not | Add authoritative owner, key, transitions, versions, recovery, and interface |
| Action Capture ledger | Required by functional behavior but absent from logical table list | Add durable ledger or equivalent inbox/outbox contract |
| DDL authority | Requirements refer to reconciled DDLs while project decisions block stale DDL use | Keep physical schema evidence-gated and do not call proposals authoritative |
| Retention | Three-day minimum plus extension while offload is unverified | Add executable extension owner/mechanism and test expiry protection |
| RPO | Requirements demand scenario evidence but no target is specified | Add per-boundary RPO or explicit unresolved gate |
| Security | TLS is required “where supported” | Make sensitive and money-moving transport encryption mandatory |

## Requirements versus contracts/dossiers

| Topic | Locations | Problem |
| --- | --- | --- |
| Requirement IDs | `02-functional/*`, `08_implementation/99-traceability.md` | `REQ-FLS/SS/BB` versus `REQ-ST/BL/BAB` families break traceability |
| Positions ownership | `02-functional/0.0-index.md`, `06-action-capture.md`, `04-data.md` | Index omits a required Action Capture output |
| Ranking boundary | `04-business-logic.md`, `10-ranking.md`, contracts | Logical boundary is clear, but global reservation topology is not |
| Lifecycle ordering | `06-action-capture.md`, `05-interfaces.md` | Source version is required without broker sequence/version evidence |
| Latency | `03-non-functional.md`, `03-compute.md`, `10-ranking.md` | Instruction “commit” and checkpoint/visibility semantics are not the same defined event |
| Time semantics | `01-ingestion.md`, `03-compute.md`, contracts | Watermark delay, allowed lateness, finalization, and source idleness need one canonical model |
| State recovery | Action Capture requirements/dossier versus data model | Projection recovery is specified but ledger storage is missing |

## Vocabulary cleanup

The requirements should use one canonical vocabulary for:

- `account_scope_id`, `portfolio_id`, and `execution_partition_id`;
- `event_time`, `receive_time`, `sink_ack_time`, `visible_time`, and `checkpoint_commit_time`;
- `dedup_horizon` and `dedup_ttl`;
- reservation versus execution state;
- broker event evidence versus platform event identity;
- order lifecycle versus position lifecycle;
- `UNKNOWN`, `HALTED`, `DEGRADED`, and `NOT_READY`.
