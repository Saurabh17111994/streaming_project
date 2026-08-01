# 02.4 — Business Logic

## Purpose

Business Logic is a stateful operator inside the Signal Flink job. It consumes Compute's in-job closed-candle and forming-bar events, detects patterns, creates immutable candidates, requests portfolio reservations, and passes eligible candidates to in-operator Ranking. It never calls a broker and never mutates lifecycle or position state.

## Constraints

- Business Logic SHALL NOT call a broker, submit an order, or interact with Arrow REST. Order execution is owned by the Executor.
- Business Logic SHALL NOT mutate lifecycle (`Order_Lifecycle`) or position (`Positions`) state. These are owned by Action Capture and the position projector.
- Business Logic SHALL NOT publish an executable instruction when the portfolio reservation view is missing, stale, conflicting, or contains unresolved `UNKNOWN` state.
- Business Logic SHALL NOT reuse `instruction_id` for different quantity, side, symbol, price, strategy version, or trade context. A changed winning parameter creates a new `instruction_id`.
- Business Logic SHALL NOT treat cross-table visibility as atomic ordering. Executor is authoritative for submission eligibility and supersession sequencing.
- `Signal_Candidates` records SHALL NOT be updated. Corrections are new records with an explicit supersession relation.
- The ranking/reservation operator SHALL be a serialized scope per `portfolio_id` inside the Signal Flink job. It SHALL NOT become a separate deployment or Fluss round trip.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-BL-001 | The Compute operator delivers typed closed-candle and forming-bar events in a deterministic order within each instrument key group after deduplication. | REQ-FC-013, REQ-SS-007 |
| ASM-BL-002 | The in-job portfolio reservation state interface delivers a consistent, versioned view of current reservations, lifecycle state, and position state for the ranking operator's portfolio scope. | REQ-SS-001, REQ-SS-009 |
| ASM-BL-003 | Executor respects the supersession contract: a replacement instruction carrying `supersedes_instruction_id` is held or rejected until the predecessor is terminally disposed or explicitly reconciled. | REQ-SS-010 |
| ASM-BL-004 | Fingerprint collisions at the dedup stage do not cause candidate identity collisions or spurious supersession at the Business Logic layer. | RISK-001 |
| ASM-BL-005 | The strategy versions, configuration hashes, and scoring parameters are deterministic and replayable from the checkpointed state. | REQ-SS-007 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Forming-bar detection fires on incomplete data:** Patterns may fire on the forming bar before window close. This enables low-latency entry but means a signal may be invalidated later if the bar reverses. The operator defines one-shot, repeatable, or updated detection semantics per strategy.
- **Immutable instructions are never corrected in-place:** A `Signal_Candidates` record is never updated. If parameters change, a new candidate and new `instruction_id` are created with a supersession relation. The old candidate remains as audit evidence.
- **Reservations are conservative:** Capacity is held through `UNKNOWN` states and only released after terminal correlation or explicit reconciliation. This prevents over-trading at the cost of potentially unused capacity during uncertain states.
- **Deterministic replay is bounded:** Replay determinism is guaranteed only under identical ordered input, fingerprint version, strategy version, configuration version, and lifecycle/reservation snapshot. Different arrival order or missing external state may produce different results.
- **In-operator ranking with no Fluss round trip:** Ranking is an in-memory function call within the Signal job, not a separate deployment. This keeps latency low but means ranking state lives entirely in the Signal job's checkpoint.
- **Same winner, unchanged parameters = audit only:** A repeated evaluation of the same setup with identical executable content produces only an audit record, not a new instruction.

## Out of Scope

The following capabilities are explicitly NOT owned by Business Logic:

- **Candle computation, event-time watermarking, and deduplication:** Owned by Compute within the same Signal job.
- **Broker order submission, execution, gate management, and Arrow REST integration:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection:** Owned by Action Capture.
- **Position monitoring and position-action emission:** Owned by the Babysitter Flink job.
- **Order lifecycle state mutation (`Order_Lifecycle`):** Owned by Action Capture. Business Logic reads reservation/lifecycle state for eligibility but does not write it.
- **Strategy authoring, backtesting, or configuration UI:** Deferred; not in MVP scope.
- **ML-based ranking or dynamic weight adjustment:** Deferred; not in MVP scope.
- **Multi-broker or multi-account strategy routing:** Deferred; not in MVP scope.

## REQ-SS-001: State and ownership

The operator SHALL maintain versioned state for:

- Per-instrument closed-candle ring buffer
- Current forming-bar accumulator
- Active setup descriptors
- Candidate/evaluation identity
- Portfolio reservation view received through the tested in-job/materialized state interface

State restoration SHALL be checkpointed with the Signal job. If required lifecycle, position, or reservation state is unavailable or stale beyond its configured bound, the operator SHALL suppress new instruction publication and expose the reason.

## REQ-SS-002: Forming-bar detection

Patterns MAY fire on the forming bar as soon as a verified condition is met. Every candidate includes instrument, strategy, rule, event timestamp, formation state, entry parameters, and the strategy version/configuration hash.

The operator SHALL define whether a setup is one-shot, repeatable after invalidation, or updated. A repeated evaluation of the same setup is audit-only unless it creates a new immutable instruction under the instruction lifecycle rules.

## REQ-SS-003: Candidate audit

Every detected candidate, selected or rejected, SHALL append an immutable `Signal_Candidates` record with:

- `candidate_id`
- `instruction_id` when an instruction is created, otherwise null
- `trade_context_id` when known
- Instrument, strategy, rule, action, price, quantity, product, and order type
- Event and evaluation timestamps
- Score inputs and strategy/configuration version
- Detection reason and validity state

A candidate record is never updated. Corrections are new records with an explicit supersession relation.

## REQ-SS-004: Immutable instruction lifecycle

`instruction_id` is immutable. A candidate whose winning parameters change SHALL create a new instruction with a new `instruction_id`; the prior instruction becomes `SUPERSEDED` or `CANCELLED` in execution-owned state before a replacement can be submitted.

The Signal job SHALL not reuse `instruction_id` for different quantity, side, symbol, price, strategy version, or trade context. Exact identifier encoding is versioned and collision-tested.

## REQ-SS-005: Portfolio reservation

Before publishing an executable instruction, the operator SHALL obtain a reservation for the relevant instrument, strategy, trade context, and capacity class. Reservations are conservative:

- `RESERVED`: candidate accepted but no broker attempt yet
- `SUBMITTING`: Executor has begun an attempt
- `PENDING`: broker outcome not terminal
- `OPEN`: correlated fill opened exposure
- `RELEASE_PENDING`: terminal broker/position event is awaiting correlation
- `RELEASED`: capacity available again
- `UNKNOWN`: uncertainty; capacity remains held and the order path halts

A rejected, cancelled, or failed instruction releases capacity only after the terminal state is correlated or an explicitly authorized reconciliation disposition is recorded. Unknown outcomes never release capacity automatically.

## REQ-SS-006: Ranking handoff

Business Logic passes all active candidate records to the in-operator Ranking function. There is no Fluss round trip, separate Ranking deployment, or separate ranking checkpoint.

## REQ-SS-007: Deterministic replay

Replay determinism is defined relative to an identical ordered input snapshot, fingerprint algorithm/version, strategy/configuration version, and lifecycle/reservation snapshot. It is not promised across different arrival order, fingerprint collisions, missing external state, or changed configuration.

## REQ-SS-009: Portfolio-keyed ranking and reservation topology

Compute and candidate detection SHALL remain keyed by `instrument_token`. Eligible candidate events SHALL then be repartitioned by `portfolio_id` to a serialized ranking/reservation scope inside the same Signal Flink job. Ranking SHALL not become a separate deployment or Fluss round trip.

For one `portfolio_id`, the ranking/reservation operator SHALL own the authoritative in-job reservation state and process candidate and lifecycle/position evidence in a deterministic order. The operator SHALL define the behavior for simultaneous candidates, stale external state, duplicate candidate events, partial output visibility, and recovery.

The operator SHALL not publish a new executable instruction when the portfolio view is missing, stale, conflicting, or contains unresolved `UNKNOWN` state.

## REQ-SS-010: Reservation lifecycle and supersession

Every reservation SHALL have `reservation_id`, `portfolio_id`, capacity class, candidate/instruction identity, state, transition version, creation time, expiry time, and source evidence. Legal transitions, expected prior version, stale-update handling, and rebuild source SHALL be explicit.

A replacement instruction SHALL carry `supersedes_instruction_id`. Executor is authoritative for submission eligibility and SHALL reject or hold the replacement until the predecessor is terminally disposed or explicitly reconciled. Cross-table visibility SHALL not be treated as atomic ordering.

## REQ-SS-011: Candidate and evaluation bounds

Business Logic SHALL define candidate expiry, invalidation, repeat/evaluation triggers, maximum active candidates per instrument and portfolio, timer bounds, and cleanup behavior. Every evaluation SHALL have a stable `evaluation_id` and source state/version snapshot.

## REQ-SS-008: Acceptance

Tests SHALL prove pattern detection on forming bars, closed-candle handoff, one-shot/repeat behavior, immutable instruction creation, supersession, missing-state suppression, global capacity under concurrent candidates, reservation transitions, restart restore, candidate audit completeness, bounded candidate state, and deterministic replay under a fixed input snapshot.
