# Segment Build Contract — Ranking

## Boundary

Ranking is a pure versioned function inside Business Logic in the Signal Flink job. It is not a deployment, Fluss consumer, window, or checkpoint boundary.

## Inputs

Active candidates, normalized/versioned score inputs, strategy/configuration version, evaluation ID, and reservation/lifecycle snapshot/version.

## Behavior

Validate and normalize every score component; reject null/non-finite/invalid inputs with a reason. Apply versioned weights and deterministic tie-breakers ending with stable candidate identity. Enforce conservative reservation capacity. MVP defaults are at most one reserved/open trade per instrument, three total reserved/open positions, and one per strategy. `RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` consume capacity; `RELEASED` does not. Limits and their configuration hash are recorded with each evaluation.

Same winner with unchanged parameters creates audit only. Changed parameters create a new immutable instruction after prior disposition. Uncertain state suppresses publication.

## Outputs

`Ranking_Results` contains all candidates, score breakdown, model/configuration version, rank, selection, rejection reason, and reservation snapshot. `Trade_Decisions` receives at most one new immutable instruction per valid evaluation transition.

## SLO and acceptance

Trigger tick to instruction commit p99 is <100 ms at 75,000 ticks/s, with p50/p95/p99 and full measurement context. Tests cover ranges, NaN/null, ties, churn, capacity, stale state, replay, audit, and workload latency.

## Requirement traceability

- Functional: `REQ-RNK-001` through `REQ-RNK-007`
- Cross-cutting: `03-non-functional.md` §§3.1, 3.3, 3.5, 3.8; `04-data.md` §§4.2–4.4; `05-interfaces.md` §§5.3–5.4, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/10-ranking.md`.
