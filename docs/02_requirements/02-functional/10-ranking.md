# 02.10 — Ranking (In-operator)

## Purpose

Ranking is a pure, versioned function inside the Business Logic operator of the Signal Flink job. It scores currently active candidates, applies portfolio/reservation constraints, writes immutable ranking audit records, and creates at most one new immutable winning instruction per evaluation transition.

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

Trigger-tick consumption to winning instruction commit SHALL have p99 below 100 ms at the 75,000 ticks/s full-session baseline. Reports SHALL include p50/p95/p99, UTC clock source, sample duration, failure inclusion, and workload profile. No unmeasured microsecond or cross-job latency claim is permitted.

## REQ-RNK-007: Acceptance

Tests SHALL cover score ranges, null/NaN inputs, ties, identical reevaluation, changed parameters, winner transitions, stale reservation state, capacity limits, restart/replay, audit completeness, and latency under the production workload.

## 

> **✅ RESOLVED [HIGH]**: Score inputs not normalized. **Fix applied in:** `docs/02_requirements/02-functional/10-ranking.md` — REQ-RNK-002 (lines 14–26): each input defines unit/allowed range, null/NaN/missing behavior, normalization/clipping, weight range/sum constraint, decimal/rounding; "Invalid score inputs reject the candidate with a reason; they never silently produce NaN ordering."
> 
> **✅ RESOLVED [HIGH]**: Sub-millisecond claim lacks evidence. **Fix applied in:** `docs/02_requirements/02-functional/10-ranking.md` — REQ-RNK-006 (lines 53–55): "No unmeasured microsecond or cross-job latency claim is permitted." p99 <100 ms at 75k ticks/s with full statistical reporting.
> 
> **✅ RESOLVED [HIGH]**: Selection churn/idempotency incomplete. **Fix applied in:** `docs/02_requirements/02-functional/10-ranking.md` — REQ-RNK-004 (lines 38–48): explicit four selection states (same winner=audit-only, changed parameters=new instruction+supersession, different winner=reservation transition, uncertain state=suppress/halt).
