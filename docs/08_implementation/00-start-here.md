# Implementation Guide — Start Here

<!-- markdownlint-disable MD013 -->

## Purpose

This directory converts the stable project requirements and build contracts into implementation-ready engineering dossiers. It defines **how an implementer must realize and prove the system**, while the upstream documents remain authoritative for **what the system must do**.

No document in this directory proves that code exists or that a runtime guarantee has passed. Runtime claims require the evidence records defined here.

## How to use this folder

Build the project in numeric order. Each phase file contains **what to build** and a **verification mapping**; [`11-testing-and-release.md`](./11-testing-and-release.md) contains the detailed test procedures, evidence requirements, and final release evidence. Finish the mapped tests for one phase before moving to the next. `01-foundation.md` contains shared rules needed before building.

## Authority and conflict rule

Use the repository authority order from [`../01_project/00-index.md`](../01_project/00-index.md):

1. Executable implementation and tests
2. Active architectural decisions
3. Validated authoritative DDLs
4. Build contracts
5. Detailed requirements
6. Implementation dossiers and summaries

An implementation dossier must not silently redefine an upstream requirement. If implementation detail exposes a conflict, record it in [`01-foundation.md`](./01-foundation.md) and keep the affected work blocked until the authoritative layer is reconciled.

## Documentation-first workflow

```text
implementation task
  → authoritative requirement/decision identified
  → implementation dossier completed
  → unresolved external facts recorded as evidence gates
  → design review approved
  → code and DDL implementation
  → focused tests and operational evidence
  → implementation task verified and struck through
```

Documentation-complete is not code-complete. A checklist item may be marked documentation-complete only when its dossier, traceability, test cases, and unresolved evidence gates are explicit.

## Dossier map

| Dossier | Purpose | Primary plan phases |
| --- | --- | --- |
| [`01-foundation.md`](./01-foundation.md) | Plan, governance, software versions, storage rules, and shared safety rules | 0–2 |
| [`02-schema-storage.md`](./02-schema-storage.md) | Data tables/storage instructions and their test design | 3 |
| [`03-ingestion.md`](./03-ingestion.md) | Ingestion instructions and test design | 4 |
| [`04-signal-job.md`](./04-signal-job.md) | Signal-job instructions and test design | 5 |
| [`05-action-capture.md`](./05-action-capture.md) | Action Capture instructions and test design | 6 |
| [`06-babysitter.md`](./06-babysitter.md) | Babysitter instructions and test design | 6 |
| [`07-executor.md`](./07-executor.md) | Executor instructions and test design | 7 |
| [`08-local-compose.md`](./08-local-compose.md) | Local runtime instructions and test design | 8 |
| [`09-production-swarm.md`](./09-production-swarm.md) | Production runtime instructions and test design | 9 |
| [`10-observability.md`](./10-observability.md) | Monitoring/operations instructions and test design | 10 |
| [`11-testing-and-release.md`](./11-testing-and-release.md) | Master test list, traceability, and final release evidence | 11–12 |
| **Roadmap** | Step-by-step plans live INSIDE their dossiers (2026-08-13 merge): current build plan → `04-signal-job.md` (appended section); P7 bench + completed gaps → `11-testing-and-release.md`; P10 rehearsal → `09-production-swarm.md`; agent-2 executor plan → `07-executor.md` | — |

## Document status vocabulary

Every dossier SHALL include a status banner with these four dimensions (defined in [`01-foundation.md`](./01-foundation.md)):

| Dimension | Options |
| --- | --- |
| **Design status** | `Draft`, `Design-ready`, `Evidence-blocked`, `Superseded` |
| **Implementation status** | `Not-implemented`, `Implementing`, `Implemented`, `Validated` |
| **Evidence status** | `Untested`, `Tested-in-sandbox`, `Production-validated` |
| **Live-money status** | `Blocked`, `Approved-for-testing`, `Live` |

A dossier may be Design-ready, Evidence-blocked, and Live-money blocked simultaneously: implementation can proceed behind an adapter or disabled gate, but live-money release cannot.

The previous single-axis status vocabulary (`Draft`, `Design-ready`, `Implementation-ready`, `Evidence-blocked`, `Validated`, `Superseded`) is superseded by the 4-dimension banner above. Existing dossiers using the old vocabulary remain valid until their next revision.

## Current readiness

| Area | Design status | Implementation status | Evidence status | Live-money status |
| --- | --- | --- | --- | --- |
| Architecture and ownership | Design-ready | Implemented | Tested-in-sandbox | Blocked |
| Broker protocols | Evidence-blocked | Implementing | Untested | Blocked |
| DDL/schema | Design-ready | Implementing | Untested | Blocked |
| Ingestion | Design-ready | Implemented | Tested-in-sandbox | Blocked |
| Signal job | Design-ready | Implementing (Slice 1 + Slice 2.1 MVP detection live-verified; candle tables converted to **KV-only** 2026-08-13 — `feature_candles_15s` KV PK `(instrument_token, window_start)`, see `04-signal-job.md` banner; 183 compute tests green + 7-test gated battery) | Tested-in-sandbox (Slice 1 smoke + SAFETY-INT-001 + Slice 2.1 live + candle conversion battery 2026-08-13) | Blocked |
| Action Capture | Design-ready | Not-implemented | Untested | Blocked |
| Babysitter | Design-ready | Not-implemented | Untested | Blocked |
| Executor | Design-ready | Not-implemented | Untested | Blocked |
| Local runtime | Design-ready | Implementing | Tested-in-sandbox | Blocked |
| Production runtime | Design-ready | Not-implemented | Untested | Blocked |
| Test/evidence program | Design-ready | Implementing | Tested-in-sandbox | Blocked |

## Mandatory implementation order

1. Approve documentation governance and cross-cutting invariants.
2. Record version/protocol evidence gates.
3. Validate DDL capability and schema lifecycle.
4. Implement ingestion and test fixtures.
5. Implement the Signal job.
6. Implement Action Capture and the no-op Babysitter.
7. Implement Executor last among functional services, with broker calls disabled until safety tests pass.
8. Implement local integration runtime.
9. Implement production Swarm and operational controls.
10. Produce the release evidence package.

Implementation may overlap where dependencies permit, but no downstream task may invent an upstream contract.

## Per-phase execution guide

Each phase below follows a **contracts-first** workflow:

```text
for each phase:
  1. read the contracts in docs/04_contracts/   ← WHAT (binding spec)
  2. read the dossier in docs/08_implementation/ ← HOW (engineering plan)
  3. write code and DDL
  4. run focused tests + record evidence
  5. mark phase complete before moving on
```

If contract and dossier disagree, the contract wins. Flag the conflict in
[`01-foundation.md`](./01-foundation.md) and block the affected work.

---

### Phase 1: Mock Broker ✅ COMPLETED

| Step | Artifact | Status |
| --- | --- | --- |
| Contract | [`../04_contracts/arrow_broker.md`](../04_contracts/arrow_broker.md) (MKT data section) | Read |
| Dossier | [`11-testing-and-release.md`](./11-testing-and-release.md#performance-benchmark-procedure) | Read |
| Code | `code/02_services/05_mock_arrow/src/.../MockArrowServer.java` | ✅ Implemented |
| Tests | `MOCK-UNIT-001`, `MOCK-UNIT-002`, `MOCK-UNIT-003`, `MOCK-PERF-001` | Suite present (`SyntheticWorkloadTest`); executed indirectly via ingestion E2E; standalone MOCK-* evidence report pending |

---

### Phase 2: Ingestion ✅ COMPLETED (2026-08-09)

Status: Phases 2a-2g done — 296 tests (184 ingestion + 112 common), 0 failures, 7 env-gated skips; E2E fake-broker → Fluss green (10,716 rows persisted, 58,951 ticks/s baseline probe on the 1,024-instrument envelope). Open items: 60k/90k perf gates not achieved (measured feed ceiling ≈58.9k rows/s); ING-RES-001 real-backoff soak not yet run; 3,000-instrument / 3-connection envelope deferred — see [`03-ingestion.md`](./03-ingestion.md) Status.

Read these **in order** before writing any code:

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/01-ingestion.md`](../04_contracts/01-ingestion.md) | What ingestion must do — per-tick append, backpressure, batching rules |
| **Contract** | [`../04_contracts/02-storage.md`](../04_contracts/02-storage.md) | `raw_table_1` contract — routing key, retention, append semantics |
| **Contract** | [`../04_contracts/arrow_broker.md`](../04_contracts/arrow_broker.md) (market section) | Broker protocol — binary frames, zstd, auth, subscription |
| **Dossier** | [`03-ingestion.md`](./03-ingestion.md) | How to build — state machines, config, failure modes, test IDs |
| **DDL** | `code/01_platform/02_sql/ddl/02_raw_table_1.sql` | Physical table schema |

**What to build:**

1. Go `arrow-bridge` (Arrow Go SDK → stdout NDJSON) pipes into Java `IngestionService` (stdin → validate → fingerprint → Fluss `raw_table_1` writer)
2. Reconnect loop with exponential backoff (epoch bump on reconnect)
3. Per-tick append latency tracking
4. Backpressure: stop accepting at `MAX_PENDING_APPEND_RECORDS` (50k) / `MAX_PENDING_APPEND_BYTES` (64MB)
5. Readiness probe: returns not-ready when backpressured

**Acceptance tests:** `FAIL-PENDING-001`, `STATE-DEDUP-001`

---

### Phase 3: Signal Job 🔴 NEXT

- > Head start: Slice 1 (raw source → validation → dedup → 15 s candles → `feature_candles_15s`) is implemented with 25 green tests and live-smoke-verified 2026-08-09 (205,146 candles, 1,074 instruments, 48 checkpoints); see [`04-signal-job.md`](./04-signal-job.md) §Slice 1 evidence. The slot-scoped safety consumer shell (`SafetyHaltJob` + `SafetyStateTracker` + `SuppressionGate` in `common`) is also implemented and live-verified — SAFETY-INT-001 passed 2026-08-09. Remaining: forming-bar handoff + Business Logic (Slice 2), Ranking/Reservations/Decisions (Slice 3).

Read these **in order** before writing any code:

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/03-compute.md`](../04_contracts/03-compute.md) | Signal job contract — sources, sinks, checkpointing |
| **Contract** | [`../04_contracts/04-business-logic.md`](../04_contracts/04-business-logic.md) | Feature compute, candidate detection, filtering rules |
| **Contract** | [`../04_contracts/10-ranking.md`](../04_contracts/10-ranking.md) | In-operator ranking, no Fluss round-trip, MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT=1 |
| **Dossier** | [`04-signal-job.md`](./04-signal-job.md) | How to build — state layout, dedup, candle, ranking, reservation, decisions |
| **DDL** | `code/01_platform/02_sql/ddl/03_feature_candles_15s.sql`, `05_signal_candidates.sql`, `06_ranking_results.sql`, `07_trade_decisions.sql`, `15_portfolio_reservations.sql` | Physical schemas |

**What to build (in-order, inside one Flink job):**

1. Feature compute: `raw_table_1` source → per-instrument `Candle15s` + `FormingBar`
2. Candidate detection: feature state → `SignalCandidate` (max 1 active per instrument)
3. Ranking: in-operator, deterministic tie-break, rejection codes
4. Reservation: portfolio capacity check → `ReservationState`
5. Decision publish: write `TradeDecisions` to Fluss changelog

**Acceptance tests:** `STATE-CANDLE-001`, `STATE-DEDUP-001` (at Flink level), perf tests

---

### Phase 4: Action Capture + Babysitter (parallelizable)

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/06-action-capture.md`](../04_contracts/06-action-capture.md) | Postback consumer, lifecycle, position projection |
| **Contract** | [`../04_contracts/05-babysitter.md`](../04_contracts/05-babysitter.md) | Babysitter contract (MVP no-op) |
| **Contract** | [`../04_contracts/arrow_broker.md`](../04_contracts/arrow_broker.md) (postback section) | Postback protocol — WS JSON, identity mapping |
| **Dossier** | [`05-action-capture.md`](./05-action-capture.md) | Action Capture engineering plan |
| **Dossier** | [`06-babysitter.md`](./06-babysitter.md) | Babysitter engineering plan |
| **DDL** | `09_order_lifecycle.sql`, `10_positions.sql`, `16_postback_quarantine.sql`, `17_postback_projection_ledger.sql` | Physical schemas |

**Action Capture — what to build:**

1. Arrow postback WebSocket consumer
2. Order lifecycle state machine (`OrderLifecycleState` transitions)
3. Position projector (fill aggregation into `PositionState`)
4. Postback quarantine (unknown/malformed events)
5. Postback projection ledger

**Babysitter (MVP = no-op):**

- Already scaffolded (`BabysitterJob.java`) — wire the Positions changelog source and emit zero actions
- `POSITION_ACTIONS_ENABLED` must remain `false`

**Acceptance tests:** `BABYSITTER-001`

---

### Phase 5: Executor (LAST — money-moving code)

Read these **in order** before writing any code:

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/07-executor.md`](../04_contracts/07-executor.md) | Executor contract — gates, attempts, audit, safe-halt |
| **Contract** | [`../04_contracts/arrow_broker.md`](../04_contracts/arrow_broker.md) (REST section) | Arrow REST — `POST /order/regular`, auth, response, idempotency |
| **Dossier** | [`07-executor.md`](./07-executor.md) | How to build — gates, fencing, changelog consumer, retry rules |
| **DDL** | `11_execution_gate.sql`, `12_execution_attempts.sql`, `13_order_correlation.sql`, `14_execution_audit.sql`, `18_safety_halt_requests.sql` | Physical schemas |

**What to build:**

1. `TradeDecisions` changelog consumer (tail from Fluss)
2. Pre-call safety gate checks (identity, epoch, fencing, hash persistence)
3. Arrow REST call (`POST /order/regular`) with client_order_ref ≤ 16 chars
4. Attempt/correlation/audit persistence
5. Safe-halt on `UNKNOWN` outcome (no auto-resume)

**Starts `HALTED`.** `EXECUTION_ENABLED=false` in `.env.example`. Money calls disabled until all gate checks + acceptance tests pass.

**Acceptance tests:** gate transitions, fencing expiry, duplicate/replay, unknown-outcome halt

---

### Phase 6: Integration & Evidence

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/openobserve.md`](../04_contracts/openobserve.md) | OpenObserve OTLP metrics/logs/traces, alert thresholds |
| **Contract** | [`../04_contracts/09-platform-runtime.md`](../04_contracts/09-platform-runtime.md) | Compose/Swarm topology, health checks |
| **Dossier** | [`08-local-compose.md`](./08-local-compose.md) | Local Docker Compose integration |
| **Dossier** | [`09-production-swarm.md`](./09-production-swarm.md) | Production Swarm deployment |
| **Dossier** | [`10-observability.md`](./10-observability.md) | Dashboards, alerts, runbooks |
| **Dossier** | [`11-testing-and-release.md`](./11-testing-and-release.md) | Master test list, traceability, release evidence |

1. Wire all services in Docker Compose (already scaffolded)
2. Run the 30-min variable-baseline perf test — current phase envelope: 1,024 instruments / 20,480 ticks/s average; the 3,000-instrument / 60k ticks/s average baseline is the deferred production target
3. Run the peak-capacity campaign — current phase: sustained 20,480 ticks/s with ≤30 ticks/s per instrument; the 90k ticks/s peak campaign is the deferred production target
4. Capability evidence: Fluss LOG/KV/changelog, Flink checkpoint/savepoint/rescale, Arrow REST sandbox
5. Record evidence per `EvidenceRecord` format
6. Produce release evidence package (`11-testing-and-release.md`)

---

### Staleness note

If any contract changes during implementation, re-read the dossier and code before the downstream phase — the dossier may be stale. Record the discrepancy in [`01-foundation.md`](./01-foundation.md).
