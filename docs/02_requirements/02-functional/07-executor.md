# 02.7 — Executor

## Purpose

Executor is the only platform component permitted to initiate money-moving OpenAlgo calls. It consumes immutable instructions and, after MVP, structured position actions; enforces a durable order gate before every call; persists attempts and identity mappings; reconciles uncertain broker outcomes; and writes only execution-owned state.

OpenAlgo is a REST adapter. It does not consume Fluss, own strategy, capture fills, or decide whether order placement is safe.

## REQ-EXE-001: Dedicated execution state

Executor SHALL own dedicated Fluss state with exact physical DDLs for:

- `Execution_Gate` KV: singleton/account-scoped gate state, epoch, reason, timestamps, approvals
- `Execution_Attempts` KV: one row per `execution_attempt_id`, immutable request hash, phase, outcome, retry eligibility
- `Order_Correlation` KV: `instruction_id` ↔ `client_order_ref` ↔ `broker_order_id` mapping and verification state
- `Execution_Audit` LOG: immutable gate transitions, attempt events, reconciliation evidence, approval events, and broker response summaries

Executor SHALL not mutate strategy/candidate/ranking fields. Any execution status exposed on `Trade_Decisions` is derived/read-only or removed in favor of dedicated execution state.

## REQ-EXE-002: Gate state machine

```text
HALTED → RECONCILING → APPROVAL_PENDING → ENABLED
ENABLED → HALTED
```

A fresh installation and any restart with unverifiable durable state begin `HALTED`. Every money-moving call SHALL atomically verify the current gate epoch/state before submission.

The gate enters `HALTED` for unknown submission outcome, duplicate risk, missing/ambiguous correlation, stale instruction/reservation state, changelog discontinuity, checkpoint failure affecting order correctness, missing fill, failed reconciliation, unauthorized action, or security incident.

Safe-halt SHALL block new calls within five seconds of detection.

## REQ-EXE-003: Two-person resume

Resume requires:

1. Broker order reconciliation.
2. Open position/fill reconciliation.
3. Consumer offset/changelog continuity verification.
4. Signal job/checkpoint health verification.
5. Resolution of every unknown attempt and reservation.
6. Two distinct authenticated authorized operators approving the same gate epoch and evidence hash.

The second approval transitions `APPROVAL_PENDING` to `ENABLED`. Automatic resume is prohibited. Approvals and denied/unauthorized attempts are immutable audit events.

## REQ-EXE-004: Immutable instruction intake

Executor consumes new immutable `instruction_id` records. It verifies schema version, reservation, expiry/freshness, action fields, configuration compatibility, and that no attempt exists for the same instruction/request hash.

A modified row under an existing `instruction_id` is a contract violation: halt, quarantine, and alert. A superseded/cancelled instruction is not submitted.

## REQ-EXE-005: Attempt protocol

Before a broker call Executor SHALL durably create `execution_attempt_id`, deterministic evidence-gated `client_order_ref`, request hash, gate epoch, and `PREPARED` audit state. It then transitions to `SUBMITTING` and calls OpenAlgo.

Outcomes:

- Verified rejection/no acceptance: terminal failure; reconcile and release reservation according to policy.
- Verified broker acknowledgement: persist `broker_order_id` mapping and terminal accepted state.
- Timeout, disconnect, malformed response, process crash window, or ambiguous status: mark `UNKNOWN`, halt, and reconcile before any retry.

An unknown attempt is never automatically retried as a new order. A retry requires proof the broker did not accept the prior request or a verified broker idempotency contract; otherwise manual disposition is required.

## REQ-EXE-006: Correlation

The broker-facing reference format, length, character set, uniqueness, and echo behavior are evidence-gated. The mapping is durable before an attempt may be considered safely reconciled.

Postbacks without a unique mapping are quarantined; they halt affected new order flow and never create an inferred instruction mapping silently.

## REQ-EXE-007: Structured position actions

MVP consumes no Babysitter actions. Future actions SHALL be immutable versioned records containing `action_id`, `position_id`, `trade_context_id`, action type, side, quantity, optional price, reason, source state/version, event/expiry timestamps, and correlation IDs.

They pass through the same gate, attempt, identity, and reconciliation protocol as entry instructions.

## REQ-EXE-008: Availability and single-writer ownership

Only one active Executor may own an account/order partition at a time. Production deployment SHALL define leadership/fencing so two instances cannot submit the same partition concurrently. Losing leadership or durable-state connectivity halts submissions.

## REQ-EXE-009: Health and observability

Readiness requires durable state connectivity, changelog continuity, valid schema/version, OpenAlgo reachability, current gate state known, no unresolved invariant violation, acceptable clock offset, and observability delivery. `HALTED` may be process-healthy but trading-not-ready.

Metrics/logs include gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, duplicate suppressions, reconciliation duration/results, mapping/quarantine counts, approval events, consumer lag, OpenAlgo latency/status, and security events. Secrets and raw credentials are never logged.

## REQ-EXE-010: Acceptance gates

1. Crash injection before call, during call, after broker acceptance, and before durable acknowledgement produces no duplicate broker order.
2. Unknown outcomes halt within five seconds and cannot retry automatically.
3. Restart with missing/corrupt state defaults to HALTED.
4. Two-person approval enforces distinct identities and matching evidence epoch/hash.
5. Concurrent-instance/fencing tests prevent dual submission.
6. Mapping, quarantine, broker rejection, timeout, malformed response, and changelog discontinuity tests pass.
7. Every money-moving call is reconstructable from seven-year immutable audit data.
8. The current `NotImplementedError` scaffold is replaced and all gates pass before live-money release.

 



> 
> 
> 
