# Architectural Decisions

This file indexes active project decisions. Detailed implementation belongs in contracts, DDLs, and code. A revised decision explicitly supersedes older wording rather than leaving both active.

## Active decisions

| ID | Decision | Reason and trade-off |
| --- | --- | --- |
| DEC-001 | Fluss is the live streaming bus and storage layer. LOG tables hold immutable events; KV tables hold current operational state. | One data plane and native changelog consumption. Requires strict table and column ownership. |
| DEC-002 | Flink performs candle computation, signal detection, ranking, and babysitting. | Stateful event-time processing and checkpointed recovery. Operational complexity is higher than stateless services. |
| DEC-003 | Ingestion decodes Arrow's documented protocol and appends through the supported Fluss Java client path. | Arrow has no verified Go SDK and Fluss has no supported Go writer. The custom decoder must be compatibility-tested against captured broker packets. |
| DEC-004 | There are two Flink jobs: one signal job with in-operator ranking and one separate babysitter job. | Avoids a candidate round-trip and ranking-window delay while retaining a separate position-management failure domain. There is no separate Ranking deployment. |
| DEC-005 | KV tables use explicit writer/column ownership and `partial_update` where supported. | Prevents unrelated column clobbering. It does not guarantee external exactly-once behavior or solve stale writes within one owner. |
| DEC-006 | The Executor consumes Fluss, owns the durable order gate and reconciliation, and calls OpenAlgo. OpenAlgo is only the broker REST adapter. | Places safety and idempotency in one component. Executor state must be durable and highly observable. |
| DEC-007 | Order correlation uses `instruction_id`, `client_order_ref`, and `broker_order_id`. | Removes identity ambiguity and supports one instruction producing multiple broker orders. Requires a durable mapping. |
| DEC-008 | Eligible immutable data is offloaded to Iceberg/S3 at EOD. | Avoids live-hour S3 contention. Fluss retention must provide a multi-day retry buffer and expiry must follow verified offload completion. |
| DEC-009 | OpenObserve provides operational logs, metrics, traces, and alerts. | One observability backend. Business analytics remains out of MVP scope. |
| DEC-010 | Local development uses Docker Compose; production uses four-VM Docker Swarm. | Keeps local setup simple while retaining a production HA target. The two environment definitions must not be mixed. |
| DEC-011 | The data path auto-recovers; the order path uses a durable safe-halt gate and manual reconciliation before resume. | Missing a trade opportunity is safer than duplicating or placing a stale order. |
| DEC-012 | Tick identity is a bounded best-effort event fingerprint until Arrow supplies a verified event or packet ID. | Prevents false exactness. Hash collisions and identical legitimate ticks remain a documented limitation. |
| DEC-013 | Order lifecycle and position lifecycle are separate aggregates. | A filled order is not equivalent to a closed position. Babysitting operates on position state derived from fills. |
| DEC-014 | Ingestion preserves original broker packet bytes plus normalized typed fields; canonical JSON cannot replace the original bytes. | Supports replay and protocol evolution. Increases raw storage and requires payload redaction/access controls. |
| DEC-015 | Broker tick and postback sequence IDs are unavailable until evidence proves otherwise. Tick and postback duplicate handling uses versioned bounded fingerprints and explicit limitations. | Avoids false exactness. Logical duplicates may remain and identical legitimate events may be collapsed in bounded projections. |
| DEC-016 | Instructions are immutable. Executor stores gate, attempts, identity mappings, and immutable execution audit in dedicated Fluss state and never mutates strategy fields. | Makes crash-window reconciliation and ownership explicit. Adds state tables and projection/recovery work. |
| DEC-017 | Position state is a separate fill-derived aggregate keyed by `position_id` and grouped by `trade_context_id`; MVP Babysitter is a strict no-op. | Prevents order/position conflation. Real position actions require a later structured-action contract. |
| DEC-018 | Production Flink checkpoints/savepoints use encrypted S3; Fluss uses three-node replication across workload VMs; eligible live source tables retain at least three complete trading days and extend retention while EOD offload is unverified. | Supports one-VM tolerance and safe EOD retries. Increases storage and operational cost. |
| DEC-019 | Resume from order safe-halt requires reconciliation and two distinct authenticated operator approvals for the same gate epoch/evidence hash. | Reduces unsafe unilateral resume. Increases operational response time. |
| DEC-020 | Money-moving execution, gate, order, fill, mapping, approval, and action audit is encrypted and retained for seven years. | Supports live-money auditability. Requires access control, lifecycle governance, and storage cost. |
| DEC-021 | The current schema transition is a pre-production clean break. Exact Flink, Fluss, Arrow/OpenAlgo protocol, SDK, and image versions are evidence-gated release inputs. | Avoids compatibility work for stale undeployed schemas. Blocks DDL finalization until versions and protocol evidence are available. |

## Event fingerprint decision

The canonical fingerprint includes stable normalized values available in the decoded packet:

```text
event_fingerprint = hash(
  connection_id,
  instrument_token,
  event_time,
  tick_type,
  price and quantity fields,
  depth fields present in the packet
)
```

The fingerprint algorithm, numeric canonicalization, field order, and TTL are versioned. The raw canonical payload is retained for audit. Two byte-equivalent market events inside the TTL may be collapsed; therefore the guarantee is best-effort, not exact.

Without a broker sequence number, the platform cannot calculate exact missing sequence ranges. It may report suspected discontinuities from connection drops, heartbeat gaps, exchange-time jumps, or feed-health signals.

## Superseded statements

The following statements are no longer active:

- Ranking is a separate Flink job reading `Signal_Candidates`.
- OpenAlgo directly consumes Fluss KV changelogs.
- One overloaded `order_id` represents both platform instruction and broker order.
- Broker ticks provide a sequence number suitable for exact deduplication and gap ranges.
- Docker Compose is the production orchestrator.
- Flink exactly-once checkpoints make external broker REST calls exactly-once.
- Original broker payload may be replaced by canonical decoded JSON.
- `postback_seq` is assumed to be broker-provided and monotonic.
- Babysitter writes free-form action strings into order-lifecycle state.
- Executor is strictly read-only and may keep only an in-memory duplicate set.
- One-day raw retention is sufficient for EOD offload safety.

## Related documents

- Technology choices: [`../03_architecture/01-technology-choices.md`](../03_architecture/01-technology-choices.md)
- Contracts: [`../04_contracts/`](../04_contracts/)
- Detailed requirements: [`../02_requirements/`](../02_requirements/)
