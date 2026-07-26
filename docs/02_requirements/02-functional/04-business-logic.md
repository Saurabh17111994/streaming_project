# 02.4 — Business Logic

## Purpose

Business Logic is a stateful operator inside the Signal Flink job. It consumes Compute's in-job closed-candle and forming-bar events, detects patterns, creates immutable candidates, requests portfolio reservations, and passes eligible candidates to in-operator Ranking. It never calls a broker and never mutates lifecycle or position state.

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

## REQ-SS-008: Acceptance

Tests SHALL prove pattern detection on forming bars, one-shot/repeat behavior, immutable instruction creation, supersession, missing-state suppression, reservation transitions, restart restore, candidate audit completeness, and deterministic replay under a fixed input snapshot.

## 
