# 02.5 — Babysitter

## Purpose and MVP boundary

Babysitter is the second Flink job. It consumes the separate position projection and validates checkpoint/recovery wiring. In MVP it is a strict no-op: it SHALL emit no trim, exit, re-entry, or other money-moving action.

Real position management is Phase 4.3+ and cannot be enabled until the position model, structured action schema, current-price source, strategy rules, Executor integration, and failure tests are approved.

## REQ-BB-001: Separate job

The platform has two Flink jobs:

| Job            | Responsibility                                                                    |
| -------------- | --------------------------------------------------------------------------------- |
| Signal job     | Compute, forming-bar detection, in-operator ranking, immutable entry instructions |
| Babysitter job | Post-entry position-management evaluation; no-op in MVP                           |

Babysitter has its own checkpoint/restart boundary so position-management failures cannot corrupt the Signal job.

## REQ-BB-002: Position input

Babysitter SHALL consume a versioned `Positions` KV changelog keyed by `position_id`, with `trade_context_id` linking related entry, trim, exit, and re-entry orders. It SHALL NOT treat an order lifecycle row as a position.

A position projection is derived from uniquely correlated fills and contains at minimum instrument, side, opened/closed quantities, average prices, state, source version, last correlated fill, and update timestamp.

Order lifecycle remains owned by Action Capture. Position projection remains owned by the fill-derived position projector. Babysitter is read-only on both.

## REQ-BB-003: MVP no-op

MVP SHALL:

1. Subscribe to the position changelog.
2. Validate schema/version and consumer continuity.
3. Maintain minimal checkpointed observation state.
4. Emit metrics for positions observed and state freshness.
5. Emit **zero** position actions under every input.

Any non-zero action output in MVP is a release-blocking test failure.

## REQ-BB-004: Future structured actions

After MVP, a position-management decision SHALL append an immutable versioned `Position_Actions` record containing:

- `action_id`
- `position_id`
- `trade_context_id`
- Action type (`TRIM`, `EXIT`, `REENTRY`, or future versioned enum)
- Side, quantity, optional limit/trigger price
- Rule/strategy/configuration version
- Source position state/version
- Event, creation, and expiry timestamps
- Human- and machine-readable reason
- Supersedes/cancels relation where applicable

Free-form command strings such as `trim:<pct>:<qty>` are prohibited. A changed action creates a new `action_id`. Executor applies the same durable gate, attempt, correlation, and reconciliation protocol as entry instructions.

## REQ-BB-005: Future eligibility

Only an unambiguous `OPEN` position with current correlated state may be evaluated. Missing/stale state, unknown broker outcome, unresolved fill correlation, or order-gate halt suppresses action creation and emits an alert. A closed position cannot create a new action unless an approved re-entry strategy explicitly creates a new instruction/action and reservation.

## REQ-BB-006: Checkpoint and recovery

Production checkpoints/savepoints use S3. Version-specific tests SHALL prove source offset, position observation state, and future action sink semantics. Checkpointing does not make Executor/OpenAlgo/broker effects exactly-once.

On state corruption, schema mismatch, or changelog discontinuity, Babysitter becomes not ready and emits no action.

## REQ-BB-007: Observability

MVP metrics include consumer lag, positions observed by state, source state version/freshness, checkpoint duration/size/failures, restart count, schema errors, and emitted-action count (which must remain zero).

Future metrics add actions by type/state, suppressions by reason, evaluation latency, and action expiry.

## REQ-BB-008: Acceptance

MVP tests SHALL prove two-job deployment, input/schema handling, checkpoint restore, changelog discontinuity response, stale-state readiness failure, bounded backpressure, and zero action output for all fixtures. Future action behavior requires a separate approval and complete safety suite before activation.

# 
