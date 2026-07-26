# Segment Build Contract — Business Logic

## Boundary

A stateful operator inside the Signal Flink job consumes closed-candle and forming-bar events, detects candidates, checks a materialized reservation/lifecycle view, and passes eligible candidates to in-operator Ranking. It never calls OpenAlgo or mutates broker/position lifecycle.

## Identities and state

- Immutable `candidate_id`
- Immutable `instruction_id` only when a winning executable request is created
- Stable `trade_context_id`
- Versioned strategy/configuration and setup identity
- Reservation states: `RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, `RELEASED`, `UNKNOWN`

Unknown/stale external state suppresses publication. Unknown attempts keep capacity reserved.

## Outputs

- `Signal_Candidates`: immutable candidate audit
- In-process candidate input to Ranking
- Immutable `Trade_Decisions` instruction after ranking/reservation

Changed executable parameters create a new instruction. The old instruction is disposed/superseded before replacement submission. Same unchanged winner is audit-only.

## Acceptance

Forming-bar detection, setup repetition rules, identity collision tests, stale-state suppression, reservation transitions, immutable instruction/supersession, checkpoint restore, and deterministic replay from a fixed snapshot pass.

## Requirement traceability

- Functional: `REQ-SS-001` through `REQ-SS-008`
- Cross-cutting: `03-non-functional.md` §§3.1–3.5, 3.8; `04-data.md` §§4.2–4.4; `05-interfaces.md` §§5.3–5.4, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/04-business-logic.md`.
