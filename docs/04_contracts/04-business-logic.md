# Segment Build Contract — Business Logic

## Boundary

A stateful operator inside the Signal Flink job consumes closed-candle and forming-bar events. Candidate detection is keyed by `instrument_token`. Eligible candidates are then repartitioned by `portfolio_id` to a serialized ranking/reservation scope inside the same job. The operator never calls Arrow REST or mutates broker/position lifecycle.

> **MVP scope (Slice 2.1, DEC-034):** the implemented subset is closed-candle signal detection → `Signal_Candidates` KV records (side BUY / action ENTRY / order MARKET, execution-engine-ready; ranking fields NULL by design). Forming-bar events, candidate lifecycle (bounds, supersession, expiry), ranking/reservation repartitioning, and `Trade_Decisions` are postponed — the sections below define the full-phase contract.

## Identities and state

- Immutable `candidate_id` and `evaluation_id`
- Immutable `instruction_id` only when a winning executable request is created
- Stable `trade_context_id`
- Versioned strategy/configuration and setup identity
- `portfolio_id` for ranking and capacity scope
- Reservation lifecycle keyed by `reservation_id` and `portfolio_id`
- Reservation states: `RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, `RELEASED`, `UNKNOWN`
- Legal transitions and expected prior versions are explicit; stale updates are rejected
- Candidate bounds: expiry, invalidation, maximum active per instrument and portfolio, timer bounds, cleanup

Reservations are rebuildable from a durable event feed or KV projection. Missing, stale, conflicting, or `UNKNOWN` portfolio evidence suppresses instruction publication.

Unknown/stale external state suppresses publication. Unknown attempts keep capacity reserved.

## Outputs

- `Signal_Candidates`: immutable candidate audit
- In-process candidate input to Ranking
- Immutable `Trade_Decisions` instruction after ranking/reservation

Changed executable parameters create a new instruction. The old instruction is disposed/superseded before replacement submission. Same unchanged winner is audit-only.

## Acceptance

Forming-bar detection, setup repetition rules, identity collision tests, stale-state suppression, reservation transitions, immutable instruction/supersession, checkpoint restore, and deterministic replay from a fixed snapshot pass.

## Requirement traceability

- Functional: `REQ-SS-001` through `REQ-SS-011`
- Cross-cutting: `03-non-functional.md` §§3.1–3.5, 3.8; `04-data.md` §§4.2–4.4; `05-interfaces.md` §§5.3–5.4, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/04-business-logic.md`.
