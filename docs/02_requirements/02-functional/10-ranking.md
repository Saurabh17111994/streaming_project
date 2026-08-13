# 02.10 — Ranking (In-operator)

## Purpose

Rating is a pure, versioned function inside the Business Logic operator of the Signal Flink job. It scores currently active candidates, applies portfolio/reservation constraints, writes immutable ranking audit records, and creates at most one new immutable winning instruction per evaluation transition.

> **MVP note (2026-07-23):** Single account + single strategy → exactly one `portfolio_id`. Candidates are repartitioned by `portfolio_id` before ranking; with one portfolio this produces a single partition operationally. Ranking executes per `portfolio_id` in a serialized scope, not as a single global scope.

## Constraints

- Ranking SHALL NOT read `Signal_Candidates` from Fluss, create a separate evaluation window, run as a separate Flink job, or own a separate checkpoint boundary. It is an in-process function inside the Signal job.
- Ranking SHALL NOT produce NaN or infinite values. Invalid, null, non-finite, or out-of-range score inputs SHALL reject the candidate with a documented reason.
- Ranking SHALL NOT select a candidate when the portfolio reservation view is missing, stale, conflicting, or contains unresolved `UNKNOWN` state.
- Ranking SHALL NOT publish a new instruction for a same-winner, unchanged-parameters evaluation. Identical content produces audit-only; changed parameters require a new `instruction_id` after prior disposition.
- The final tie-breaker SHALL be a stable candidate identity, not processing-time arrival order. Ranking SHALL NOT use wall-clock time, hash order, or connection order as a sole deterministic tie-breaker.
- Portfolio capacity limits SHALL be versioned configuration with a hash recorded in every evaluation. Changing limits requires controlled deployment and SHALL NOT retroactively release existing reservations.
- `RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` SHALL consume capacity. `RELEASED` does not. UNKNOWN states SHALL NOT release capacity automatically.
- `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` SHALL be exactly `1`. Before ranking, reject a candidate if its instrument has an active reservation, active open trade, or unchanged active candidate. Do not forward another active candidate for that instrument. Every rejected candidate SHALL include a single rejection reason code: `ACTIVE_RESERVATION`, `ACTIVE_OPEN_TRADE`, or `UNCHANGED_ACTIVE_CANDIDATE`.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-RNK-001 | The in-job portfolio reservation state interface delivers a consistent, versioned view of current reservations, lifecycle state, and position state for the ranking operator's portfolio scope. | REQ-SS-001, REQ-SS-009 |
| ASM-RNK-002 | Candidate events are delivered to the ranking operator in a deterministic order after repartition by `portfolio_id`. | REQ-SS-009, REQ-RNK-008 |
| ASM-RNK-003 | Executor respects the supersession contract: a replacement instruction carrying `supersedes_instruction_id` is held or rejected until the predecessor is terminally disposed or explicitly reconciled. | REQ-SS-010 |
| ASM-RNK-004 | The strategy versions, configuration hashes, and scoring parameters (weights, normalization functions) are deterministic and replayable from the checkpointed state. | REQ-SS-007 |
| ASM-RNK-005 | The illustrative MVP weights (confidence 0.5, risk/reward 0.3, expected move 0.2) are replaced with production-approved values before live money. Input normalization is explicit and tested. | REQ-RNK-002 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **In-operator ranking with no Fluss round trip:** Ranking is an in-memory function within the Signal job, sharing its checkpoint. This minimizes latency at the cost of coupling ranking state to the Signal job's lifecycle.
- **Single serialized ranking scope per portfolio:** All candidates for one `portfolio_id` are processed serially. This guarantees deterministic capacity evaluation within a portfolio at the cost of intra-portfolio parallelism.
- **Same winner, unchanged = audit only:** A valid strategy that fires repeatedly on the same setup without parameter change produces one immutable instruction and subsequent audit-only evaluation records.
- **Invalid inputs reject, not skip:** A candidate with a null, NaN, or out-of-range score input is explicitly rejected with a reason recorded in `Ranking_Results`. It is not silently dropped or placed at the bottom of the ranking.
- **MVP weights are illustrative:** The initial weight values are placeholder configuration. Production weights require strategy approval, normalization specification, and acceptance testing.
- **Conservative capacity model:** `UNKNOWN` and transitional states hold capacity. This prevents over-trading during uncertainty at the cost of potentially unused capacity.

## Out of Scope

The following capabilities are explicitly NOT owned by Ranking:

- **Candle computation, event-time watermarking, and deduplication:** Owned by Compute within the same Signal job.
- **Signal detection, candidate creation, and strategy evaluation:** Owned by Business Logic within the same Signal job.
- **Broker order submission, execution, gate management, and Arrow REST integration:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection:** Owned by Action Capture.
- **Babysitter position monitoring and action emission:** Owned by the Babysitter Flink job.
- **Market data ingestion and broker connection management:** Owned by Ingestion.
- **ML-based ranking, dynamic weight adjustment, or online learning:** Deferred; not in MVP scope.
- **Strategy authoring, backtesting, or configuration UI for ranking parameters:** Deferred; not in MVP scope.

## REQ-RNK-001: No separate deployment

Ranking SHALL NOT read `Signal_Candidates` from Fluss, create a separate evaluation window, or run as a separate Flink job. It shares the Signal job checkpoint and receives candidates through in-process typed state/events.

## REQ-RNK-002: Versioned score contract

Every ranking record SHALL include `ranking_model_version`, configuration hash, score-input snapshot, normalized components, weights, composite result, and validation status.

Before a score is computed, each input SHALL define:

- Unit and allowed range
- Null, non-finite, and missing behavior
- Normalization/clipping function
- Weight range and sum constraint
- Decimal/rounding behavior

The illustrative MVP weights of confidence 0.5, risk/reward 0.3, and expected move 0.2 are not production authority until their input normalization and strategy approval are recorded. Invalid score inputs reject the candidate with a reason; they never silently produce NaN ordering.

## REQ-RNK-003: Deterministic ordering

Candidates are ordered by normalized composite score descending, followed by explicit versioned tie-breakers. The final tie-breaker SHALL be a stable candidate identity, not processing-time arrival.

Every active candidate receives a `Ranking_Results` record with evaluation identity, rank, selection result, rejection reason, and score breakdown.

## REQ-RNK-004: Selection and churn

A ranking evaluation SHALL distinguish:

- Same winner, unchanged parameters: audit-only; no new instruction
- Same setup, changed executable parameters: new immutable instruction after prior disposition/supersession
- Different winner: reservation transition and new instruction only if capacity is safely available
- Uncertain lifecycle/reservation state: reject/suppress and halt instruction publication as required

The Executor's duplicate guard is a safety backstop, not the mechanism that defines valid ranking churn.

## REQ-RNK-005: Portfolio constraints

MVP defaults are maximum one reserved/open trade per instrument, three total reserved/open positions, and one per strategy. `RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` consume capacity. `RELEASED` does not.

Limits are configuration with version/hash recorded in every evaluation. Changing limits requires controlled deployment and does not retroactively release existing reservations.

## REQ-RNK-006: Latency target

Trigger-tick consumption to winning instruction commit SHALL have **p99 below 100 ms** at 50,000 ticks/s variable average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average). This is the single release target. Reports SHALL include p50/p95/p99, UTC clock source, test duration, instrument count (3,000), total tick rate (50,000/s), failure/restart inclusion, software versions, and VM specification. Internal diagnostic timestamps (source receipt, raw visibility, Signal-job consumption, candidate/ranking evaluation, winner commit) are recorded for diagnosis but are not independent release gates. No unmeasured microsecond or cross-job latency claim is permitted.

## REQ-RNK-007: Acceptance

Tests SHALL cover score ranges, null/NaN inputs, ties, identical reevaluation, changed parameters, winner transitions, stale reservation state, global capacity limits across parallel candidates, restart/replay, audit completeness, bounded ranking state, and latency under the production workload.

## REQ-RNK-008: Global capacity serialization

Ranking SHALL execute after candidate events are repartitioned by `portfolio_id`. The ranking/reservation state for one portfolio SHALL be serialized and versioned. Capacity checks SHALL include all configured scopes, including instrument, strategy, portfolio, and account limits.

A candidate cannot be selected solely from a local instrument view. The operator SHALL use the latest valid portfolio execution view and SHALL suppress publication when that view is stale, conflicting, or incomplete.

## REQ-RNK-009: Ranking evidence

Every ranking evaluation SHALL record `portfolio_id`, account scope, candidate snapshot version, reservation version before/after, capacity configuration hash, evaluation trigger, and deterministic tie-break data. The result SHALL distinguish audit-only reevaluation from instruction creation, supersession, rejection, and safety suppression.
