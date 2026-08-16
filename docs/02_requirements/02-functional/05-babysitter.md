# 02.5 — Babysitter

## Purpose and MVP boundary

Babysitter is the second Flink job. It consumes the separate position projection and validates checkpoint/recovery wiring. In MVP it is a strict no-op: it SHALL emit no trim, exit, re-entry, or other money-moving action.

Real position management is Phase 4.3+ and cannot be enabled until the position model, structured action schema, current-price source, strategy rules, Executor integration, and failure tests are approved.

## Constraints

- Babysitter SHALL emit zero money-moving actions in MVP. Any non-zero action output is a release-blocking test failure.
- `POSITION_ACTIONS_ENABLED` SHALL be hard-coded `false` for the MVP build. Startup SHALL fail if any environment variable or configuration attempts to set it to `true`.
- Babysitter SHALL NOT mutate `Order_Lifecycle`, `Positions`, or any execution-owned state. It is read-only on the position changelog and lifecycle/audit projections.
- Babysitter SHALL maintain its own checkpoint/restart boundary, separate from the Signal job. A Babysitter failure SHALL NOT corrupt entry signals or instructions.
- Babysitter SHALL NOT treat an order lifecycle row as a position. Position state is the authoritative fill-derived aggregate keyed by `position_id`.
- Babysitter SHALL NOT create free-form command strings for future actions. All future actions use the versioned structured `Position_Actions` schema.
- Babysitter SHALL enter NOT_READY and emit no action on state corruption, schema mismatch, or changelog discontinuity.
- A closed position SHALL NOT create a new action unless an approved re-entry strategy explicitly creates a new action. **(Instruction/reservation clause REMOVED 2026-08-15, CHG-005.)**

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-BB-001 | The `Positions` KV changelog is complete, versioned, and delivers current state with fresh enough latency to inform position-management decisions (post-MVP). | REQ-BB-002 |
| ASM-BB-002 | Action Capture's fill-derived position projector produces correct, uniquely correlated position aggregates keyed by `position_id` with `trade_context_id` linkage. | REQ-BB-002 |
| ASM-BB-003 | The versioned `Position_Actions` structured schema will be approved before post-MVP activation. Free-form command strings will not be used. | REQ-BB-004 |
| ASM-BB-004 | The Executor applies the same durable gate, attempt, correlation, and reconciliation protocol to `Position_Actions` as it does to ~~`Trade_Decisions`~~ **(REMOVED 2026-08-15, CHG-005 — decision feed out of scope; the gate/attempt/correlation protocol itself stands)**. | REQ-BB-004 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **MVP is a strict no-op:** Babysitter validates input schemas, maintains checkpointed observation state, and emits metrics — but produces zero actions. This proves the deployment, checkpoint, and changelog-consumption wiring without money-moving risk.
- **Separate checkpoint boundary:** Babysitter failures, checkpoint corruption, or restart loops cannot affect the Signal job's entry-signal path. This isolation is mandated by the two-job topology.
- **Read-only on external state:** Babysitter consumes position and lifecycle projections but never writes them. This prevents accidental corruption of fill-derived or execution-owned state.
- **Future actions are structured and immutable:** Post-MVP `Position_Actions` are versioned, typed records with `action_id`, supersession, and expiry — not free-form command strings. A changed action creates a new `action_id`.

## Out of Scope

The following capabilities are explicitly NOT owned by Babysitter:

- **Candle computation, signal detection, candidate creation:** Owned by the Signal Flink job. **(Ranking and entry-instruction publication REMOVED 2026-08-15, CHG-005.)**
- **Broker order submission, execution, gate management, and Arrow REST integration:** Owned by the Executor.
- **Postback capture, fill audit, order lifecycle projection, and position projection:** Owned by Action Capture.
- **Market data ingestion and broker connection management:** Owned by Ingestion.
- **Position action execution:** Babysitter emits actions; the Executor owns the durable gate, attempt, correlation, and broker submission for position actions.
- **Money-moving position management decisions in MVP:** Explicitly out of scope. Zero actions are emitted.
- **Strategy authoring, backtesting, or configuration UI for position-management rules:** Deferred; not in MVP scope.
- **Real-time current-price sourcing for position evaluation (e.g., LTP-based exit triggers):** Deferred; not in MVP scope.

## REQ-BB-001: Separate job

The platform has two Flink jobs:

| Job            | Responsibility                                                                    |
| -------------- | --------------------------------------------------------------------------------- |
| Signal job     | Compute, forming-bar detection (**in-operator ranking and immutable entry instructions REMOVED 2026-08-15, CHG-005**) |
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
3. Maintain minimal checkpointed observation state containing only: latest accepted position version, last consumed source offset, freshness timestamp, schema version, and no-op reason counters. BABYSITTER STATE SHALL NOT contain historical position snapshots, market ticks, candles, candidates, or strategy state.
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

Only an unambiguous `OPEN` position with current correlated state may be evaluated. Missing/stale state, unknown broker outcome, unresolved fill correlation, or order-gate halt suppresses action creation and emits an alert. A closed position cannot create a new action unless an approved re-entry strategy explicitly creates a new action. **(Instruction/reservation clause REMOVED 2026-08-15, CHG-005.)**

## REQ-BB-006: Checkpoint and recovery

Production checkpoints/savepoints use S3. Version-specific tests SHALL prove source offset, position observation state, and future action sink semantics. Checkpointing does not make Executor/Arrow REST/broker effects exactly-once.

On state corruption, schema mismatch, or changelog discontinuity, Babysitter becomes not ready and emits no action.

## REQ-BB-007: Observability

MVP metrics include consumer lag, positions observed by state, source state version/freshness, checkpoint duration/size/failures, restart count, schema errors, and emitted-action count (which must remain zero).

Future metrics add actions by type/state, suppressions by reason, evaluation latency, and action expiry.

## REQ-BB-008: Acceptance

MVP tests SHALL prove two-job deployment, input/schema handling, checkpoint restore, changelog discontinuity response, stale-state readiness failure, bounded backpressure, and zero action output for all fixtures. Future action behavior requires a separate approval and complete safety suite before activation.
