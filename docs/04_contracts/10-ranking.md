# Segment Build Contract — Ranking

## Boundary

Ranking is a pure versioned function inside Business Logic in the Signal Flink job. Candidate detection is instrument-keyed. Eligible candidates and versioned lifecycle/position/reservation evidence are repartitioned by `portfolio_id` inside the Signal job. One serialized ranking/reservation state owner evaluates capacity for each portfolio scope. It is not a deployment, Fluss consumer, window, or checkpoint boundary.

## Inputs

Active candidates, normalized/versioned score inputs, strategy/configuration version, evaluation ID, and reservation/lifecycle snapshot/version.

## Behavior

Validate and normalize every score component; reject null/non-finite/invalid inputs with a reason. Apply versioned weights and deterministic tie-breakers ending with stable candidate identity. Enforce conservative reservation capacity with global scope serialization. Ranking SHALL execute after candidate events are repartitioned by `portfolio_id`. Capacity checks SHALL include all configured scopes, including instrument, strategy, portfolio, and account limits. A candidate SHALL not be selected solely from a local instrument view. The operator SHALL use the latest valid portfolio execution view and SHALL suppress publication when that view is stale, conflicting, or incomplete. MVP defaults are at most one reserved/open trade per instrument, three total reserved/open positions, and one per strategy. `RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` consume capacity; `RELEASED` does not. Limits and their configuration hash are recorded with each evaluation.

Same winner with unchanged parameters creates audit only. Changed parameters create a new immutable instruction after prior disposition. Uncertain state suppresses publication.

## Outputs

`Ranking_Results` contains all candidates, score breakdown, model/configuration version, rank, selection, rejection reason, `portfolio_id`, account scope, capacity configuration hash, reservation version before/after, evaluation trigger, and deterministic tie-break data. `Trade_Decisions` receives at most one new immutable instruction per valid evaluation transition.

## SLO and acceptance

Trigger tick to instruction commit p99 is <100 ms at variable 60,000 ticks/s average baseline and 90,000 ticks/s peak (20 ticks/s/instrument), with p50/p95/p99 and full measurement context. Tests cover ranges, NaN/null, ties, churn, capacity, stale state, replay, audit, and workload latency.

## Requirement traceability

- Functional: `REQ-RNK-001` through `REQ-RNK-009`
- Cross-cutting: `03-non-functional.md` §§3.1, 3.3, 3.5, 3.8; `04-data.md` §§4.2–4.4; `05-interfaces.md` §§5.3–5.4, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/10-ranking.md`.
