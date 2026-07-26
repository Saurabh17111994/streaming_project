# 05 — Interface Requirements

## Interface policy

All interfaces are versioned contracts. Exact Arrow/OpenAlgo/Fluss protocol values are evidence-gated until official artifacts or sandbox captures pass. External calls are at-least-once/uncertain until reconciled; Flink exactly-once is limited to tested state/sink boundaries.

## 5.1 Ingestion → Fluss

- **Protocol:** evidence-approved Fluss Java client
- **Table:** `raw_table_1` LOG
- **Direction:** write
- **Guarantee:** at-least-once
- **Payload:** original packet bytes plus normalized typed fields, hash, decoder/protocol version, fingerprint/version, timestamps, and validity state
- **Failure:** bounded retry under pinned client policy; uncertainty is counted and alerting/readiness is affected

Ingestion never claims raw logical deduplication. Compute owns bounded fingerprint deduplication.

## 5.2 Fluss → Signal Flink job

- **Source:** version-pinned Fluss connector
- **Table:** `raw_table_1`
- **Timestamp:** verified UTC `event_time`
- **State key:** instrument plus fingerprint scope
- **Watermark:** tested bounded out-of-orderness profile

The job filters eligible trades, deduplicates best-effort, emits final candles, and passes forming-bar state in-process to Business Logic. It does not read feature tables back for strategy execution.

## 5.3 Signal job → Fluss

The Signal job writes:

- `feature_candles_15s` final LOG rows
- `Signal_Candidates` immutable LOG rows
- `Ranking_Results` immutable LOG rows
- Immutable `Trade_Decisions` instruction records

One checkpoint covers the job, but no cross-table atomic visibility is promised without a connector test. Consumers must tolerate partial visibility and use IDs/reconciliation.

## 5.4 In-operator Business Logic ↔ Ranking

This is an in-process typed state/event interface, not a network or Fluss interface. The contract includes candidate identity, active setup snapshot, normalized score inputs, strategy/configuration version, reservation state/version, evaluation ID, and deterministic tie-break data.

Ranking writes audit and immutable instruction records. Same-winner reevaluation is audit-only; changed parameters create a new immutable instruction.

## 5.5 Action Capture → audit and projections

Action Capture consumes broker postbacks and writes independently:

1. Immutable `Fills_table` event/audit.
2. `Order_Lifecycle` KV projection.
3. `Positions` projection for uniquely correlated fill events.
4. `Postback_Quarantine` for missing/ambiguous/invalid events.

These writes are not atomic unless proven. A durable projection/reconciliation worker retries incomplete projections. No postback may infer a mapping from proximity or overloaded IDs.

## 5.6 Signal/Action state → Babysitter

Babysitter consumes the versioned `Positions` changelog. It may also consume lifecycle/audit projections needed for freshness, but it does not own them. MVP emits no actions.

Future actions are immutable `Position_Actions` events with `action_id`, `position_id`, `trade_context_id`, typed action fields, source version, expiry, reason, and configuration version.

## 5.7 Instructions/actions → Executor

Executor consumes immutable `Trade_Decisions` and, after MVP, `Position_Actions`. For each event it verifies schema/version, identity, expiry, reservation, gate state, and duplicate request hash.

A modified instruction under the same `instruction_id` is a contract violation and causes halt/quarantine. Executor writes only execution-owned state: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and `Execution_Audit`.

## 5.8 Executor → OpenAlgo

The endpoint, authentication, payload fields, timeout, response schema, retry classes, client-reference behavior, and broker-order response are evidence-gated. The required semantic contract is:

- Prepare durable attempt before call.
- Verify gate epoch immediately before call.
- Record request hash and client reference.
- Treat ambiguous response/timeout/crash window as `UNKNOWN`.
- Halt and reconcile before retry.
- Never claim REST exactly-once from Flink checkpointing.

OpenAlgo SHALL return or expose enough evidence to correlate broker order identity, acceptance/rejection, and status. If not, live-money readiness remains blocked.

## 5.9 Gate/resume interface

Gate transitions are authenticated control operations recorded in `Execution_Audit`. Resume requires broker/order reconciliation, position/fill reconciliation, changelog continuity, job/checkpoint health, resolution of unknown attempts, and two distinct authorized approvals for the same gate epoch/evidence hash.

## 5.10 Observability interface

All components emit structured logs, metrics, and traces/correlation IDs to OpenObserve using the supported ingestion contract. Trace propagation is capability-gated; correlation IDs and audit IDs are mandatory even if distributed tracing is unavailable.

## 5.11 Compatibility and acceptance

Every interface has a schema/protocol version and compatibility test. Tests cover malformed input, unknown versions, duplicate delivery, out-of-order events, partial writes, retries, timeout/unknown outcomes, restart, changelog gaps, authorization failure, and rollback. The exact version matrix is owned by Platform and Execution teams and is a live-money release blocker until pinned.

 
