# Live-Readiness Master Plan — Close All Gaps to Approved-for-Live Trading

**Date:** 2026-08-21
**Status:** Execution contract (self-contained; a fresh coding agent can execute it with only this file + the repository)
**Scope:** Everything between today (well-tested market-data → candles → placeholder-signals machine) and a single real, approved, safe order.
**Authority:** Docs win over this plan on conflict; this plan implements `docs/08_implementation/00-start-here.md` mandatory order and the audit at `docs/plans/2026-08-21-*-audit` (see Review Handoff).

---

## Overview

The platform today proves the **data path** end-to-end (ticks → `raw_table_1` → 15 s candles → `Signal_Candidates`) with strong crash-recovery and losslessness evidence. The **order path** (the part that moves money) is built but **switched off by design**: the Nautilus execution service boots `HALTED`, real broker authentication is proven live for login only, and no real order has ever been placed.

This plan closes the remaining gaps in five phases:

1. **Phase A — Sandbox order-path proof** (paper-trade round-trip against the broker sandbox; capture confirmations; reconcile).
2. **Phase B — Execution engine enablement** (live event loop, exactly-once crash proof, full gate lifecycle, signal→intent→fill flow, babysitter observation).
3. **Phase C — Scale-up** (multi-connection bridge so the full 2,433-instrument / 50k ticks-s manifest can run under your current subscription).
4. **Phase D — Production deployment** (7-VM Swarm, HA drills, perf + DR evidence).
5. **Phase E — Release evidence package** (final Monday gate, version-matrix completion, DEC-044 single-operator sign-off, go/no-go).

**Completion of all five phases = the system is Approved-for-Testing (sandbox) and gated for Live.** Live money stays blocked until the human release review in Phase E5 passes.

### Acceptance criteria (whole plan is done when ALL hold)
- [ ] A paper order (broker sandbox, `BI-EQ x1`) placed through `SignalJob → Execution_Intent → Gateway → Nautilus → go-arrow bridge → Arrow REST` returns a broker order id, and the matching postback projects into `Fills`/`Order_Lifecycle`/`Positions` with zero duplicates and reconciles against `GET /user/orders|trades|positions`.
- [ ] `Nautilus` runs its live event loop; a `kill -9` mid-order does not create a duplicate broker order (fence proof).
- [ ] The gate reaches `ENABLED` only through `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED` via the DEC-044 Saurabh approval path; a `Safety_Halt_Requests` row returns it to `HALTED` within 5 s.
- [ ] The 2,433-instrument manifest runs at ≥50k ticks/s synthetic envelope with `0` wire loss and p99 append `< 5 ms` (SIG-PERF-001 50k baseline unblocked).
- [ ] Production Swarm survives one workload-VM loss (FAIL-VM-LOSS-60000-001) and meets p99 `< 100 ms` trigger-to-commit at 50k (PERF-PROD-60000-001).
- [ ] `make full-audit && make gate && make pin-check && make cep-check-module` all exit 0; version-matrix rows 7/8/9/10 read `VERIFIED`; a release evidence package exists per `docs/08_implementation/11-testing-and-release.md`.
- [ ] DEC-044 single-operator (Saurabh) release review executed and recorded; system flips from `Blocked` to `Approved-for-Testing` with live money still gated.

---

## Context (verified during planning)

**Repo layout**
- `docs/` — numbered spec layers `01_project` → `08_implementation`; `docs/04_contracts/` are binding build contracts; `docs/08_implementation/` are engineering dossiers; `docs/05_deployment/change-records/` holds `CHG-*.md` decision-change records.
- `code/common` — shared Java lib (models, schema, identity, safety, observability).
- `code/02_services/01_ingestion` — Go `go-bridge` (Arrow SDK → NDJSON stdout) → `IngestionService.java` (stdin → validate → fingerprint → Fluss `raw_table_1`).
- `code/02_services/02_compute` — `SignalJob` (FlussSource → dedup → candle window → KV sink → signal dual-sink), `BabysitterJob` (Positions observer), `SafetyHaltJob`.
- `code/02_services/04_executor` — Rust Nautilus service: `src/engine.rs` (`LiveNodeRuntime` hosted run loop, clean stop, fail-closed duplicate-run guard), `src/executiongate.rs`, `src/gate.rs`, `src/bridge/`, `src/projection/`, `src/http.rs` (`/healthz`, `/readyz`).
- `code/02_services/06_execution_gateway` — Java Flink/Flink-adjacent gateway: `GatewayHttpServer`, `FlussControlStateStore`, `FlussIntentDedupStore`, `FlussProjectionLedgerStore`, `FlussProjectionWriter`, `DurableIntentDispatcher`, `NautilusIntentClient`.
- `code/02_services/06_execution_bridge/go-bridge` — Go wrapper around pinned `go-arrow`: `/v1/commands`, `/v1/events`, `/healthz`, `/readyz` (`server.go`).
- `code/01_platform/04_scripts/` — ~30 ops/gate tools backing the `Makefile`.
- `code/01_platform/01_docker/` — `docker-compose.yml`, `docker-stack.yml`, `versions.pin`.

**Verified current status**
- Ingestion: 576 tests green, losslessness + reconnect soak proven (`docs/08_implementation/03-ingestion.md`).
- Signal job: Slice 1 (candles) + Slice 2.1 (placeholder signal) live-verified; `SIG-PERF-001` envelope half done; **50k baseline half BLOCKED** by single-connection cap (bridge `ARROW_HFT_CONNECTIONS` pinned to 1, `ARROW_HFT_MAX_TOKENS_PER_CONNECTION` = 1024).
- Execution core: WP-0..7 DONE, live-verified (`docs/08_implementation/20-close-execution-service-gaps-plan.md`). `LiveNodeRuntime` run loop now exists in `engine.rs`. **T9 (live-Arrow order/fill/reconciliation evidence) NOT yet run** — IP gate accepted 2026-08-21, awaiting auth + DEC-044 review.
- Babysitter: now a real `Positions` changelog observer (not the MVP `fromElements(0L)` shell) — `BabysitterJob.java` current source.
- Versions pinned in `code/01_platform/04_scripts/versions.pin`: Flink 2.2.1, Fluss 0.9.1-incubating, ZK 3.9.2, Go 1.24.5, Rust 1.97.1, Nautilus commit `74d57e7e…`.

**Relevant commands (run from repo root unless noted)**
- `make full-audit` — docs-vs-code truth gate (must stay green).
- `make gate` — Monday verification gate (static + compose + go + java + schema/perf).
- `make gate-order` — mandatory 7-task implementation order gate.
- `make static-check` — `bash -n` + shellcheck every shell script.
- `make pin-check` — version pin discipline (`versions.pin`, no SNAPSHOT/floating tags).
- `make cep-check-module` — fail if Flink CEP referenced (project policy).
- `make ddl` / `make ddl-apply-smoke` — DDL apply exit-code contract.
- `make disaster-drills ARGS="--dry-run"` / `ARGS="--approve"` — Item F drills DR-001..006.
- `make eod-controller ARGS="status"` — EOD controller CLI.
- `cd code && mvn -q test -pl common,02_services/01_ingestion,02_services/02_compute`
- `cd code/02_services/04_executor && cargo test --offline` (currently 79 lib pass)
- `cd code/02_services/06_execution_bridge/go-bridge && go test -race ./...`
- `python3 code/01_platform/04_scripts/t8_sandbox_contract_check.py` (12/12 target)
- `python3 code/01_platform/04_scripts/execution_network_check.py --compose code/01_platform/01_docker/docker-compose.yml`
- `docker compose -f code/01_platform/01_docker/docker-compose.yml --profile execution-t3 up`

**Known gotchas (must respect)**
- `make full-audit` / `make stale-tables` compare live surefire test counts against **hardcoded truth numbers** (current truth: 341/236/294 for compute/ingestion/common; cited in `Makefile` `stale-tables` help text and `docs_audit.py`). **Every time you add/remove a test, update the truth numbers** in `Makefile` `stale-tables` description + any `docs_audit.py` constant, or `full-audit` goes red.
- Fluss 0.9.1 does **not** allow altering `table.log.ttl` after create (verified boundary) — retention is set at DDL create time. Do not design features that change retention live.
- KV tables are single-replica in Fluss 0.9.1; durability is via remote storage + rebuild from audit LOG. Never treat KV as loss-proof.
- All money-adjacent writes default **off** (`EXECUTION_INTENT_ENABLED=false`, `POSITION_ACTIONS_ENABLED=false`). Keep them off until the task says otherwise.

---

## Review Handoff

- **Original request:** "audit full project … now build a very detailed plan that any coding agent can implement, with atomic tasks, goals, constraints, needs, completion criteria, and necessary tests."
- **Scope decision (defaulted — user did not answer interactively):** Full roadmap to live-ready (all 5 phases). If a narrower scope is wanted, execute Phase A+B first; C/D/E are independent follow-ons.
- **Scale decision (defaulted):** Plan builds **multi-connection bridge support** in Phase C as the path to 50k. The premium-broker-tier alternative is documented as a manual fallback; no subscription purchase is assumed. Activation of the full 2,433-instrument envelope is left as a user choice (Phase C5).
- **Key decisions:** (1) live-money stays blocked until Phase E5 human review; (2) every phase ends with `make full-audit` green; (3) evidence artifacts go to `logs/<topic>/<name>-<yyyymmdd>.md` (gitignored by convention) + a `CHG-*` record in `docs/05_deployment/change-records/`.
- **Explicit non-goals:** ranking/scoring/reservations/decisions (removed by CHG-005); multi-broker; BSE/currency; backtesting; strategy authoring; Kubernetes; auto gap backfill; auto order-path resume.
- **Out of scope by user decision (2026-08-17):** the signal-side p99 decision-latency measurement half. The Phase D5 target (`PERF-PROD-60000-001`, trigger-tick-to-winner-commit p99 < 100 ms) is a different, still-required measurement — do not conflate them.
- **Open questions:** (a) exact broker sandbox credentials + approval token for Phase A (provided by human); (b) premium-tier vs multi-connection final choice (Phase C5); (c) cloud VM provisioning (Phase D1 — human).
- **Hidden context:** none — this plan is self-contained.

---

## Development Approach

- Complete one task at a time. Do not start a task until the previous task's tests + `make full-audit` pass.
- Tests are written **in the same task** as the code change.
- Keep feature flags off by default; enable only when a task explicitly requires it and re-disable after the test run proves the behavior.
- Record every evidence claim as a dated artifact + CHG record before marking the task done.
- If a task cannot fully pass until a later task, write the test, note the dependency in the task, and revisit when the dependency lands.
- Update this plan (mark `[x]`) as work completes; add discovered tasks with `+` prefix; mark blockers `BLOCKED:`.

## Testing Strategy
- Rust: `cargo test --offline` (unit + integration), `cargo test --offline --doc`.
- Go: `go test -race ./...` in both bridge modules.
- Java: `mvn -q test -pl <module> -am` plus env-gated integration tests (set the `COMPUTE_INT_TEST_*` / `SIGNAL_CHAIN_E2E` / `FLUSS_BOOTSTRAP` gate as the task specifies).
- Python ops: `pytest code/01_platform/04_scripts/tests/...`, `python3 -m unittest discover -s code/01_platform/04_scripts/tests`.
- After every task: `make static-check && make pin-check && make full-audit`.

## Human vs Agent work
- **AGENT-DOABLE:** all code, unit/integration tests, sandbox contract checks, compose/swarm config, drift/DR scripts, evidence assembly, gate runs.
- **HUMAN-ONLY (marked 🔒):** providing broker sandbox + real credentials; approving the DEC-044 release; provisioning cloud VMs; choosing premium-tier vs multi-connection at Phase C5; pressing the final "go live" switch. These are gates the agent prepares for and verifies mechanically, but cannot satisfy alone.

---

# Phase A — Sandbox Order-Path Proof (T9 continuation)

> Goal: Prove the full order round-trip against the broker **sandbox** (no real money). All order placement requires the env flag `T9_APPROVED_BY=saurabh` (or equivalent human approval token) AND sandbox-mode config. Without that flag the bridge MUST refuse to place orders (fail closed).

### Task A1 — Sandbox auth + auto re-auth harness
**Why:** Login (TOTP) is proven live, but automatic re-auth on token expiry (T3) was deferred; the round-trip needs a stable session.
- [ ] Read `code/02_services/06_execution_bridge/go-bridge/broker.go` + `fake_arrow_broker_test.go` to confirm the auth interface and the existing re-auth stub.
- [ ] Implement automatic re-auth on 401/`token_expired`: refresh TOTP, retry once, then surface `UP disabled` via `/healthz` if refresh fails (do NOT loop forever).
- [ ] Add Go test `TestSandboxAutoReauth` in `go-bridge/broker_test.go`: fake clock expiry → exactly one re-auth → success; repeated failure → `/healthz` reports disabled, no order attempted.
- [ ] Run `cd code/02_services/06_execution_bridge/go-bridge && go test -race -run TestSandboxAutoReauth ./...`.
- [ ] Write evidence `logs/tracker-14/t9-sandbox-reauth-<yyyymmdd>.md` + `CHG-057` record.
**DoD:** re-auth unit-tested; `/healthz` reflects disabled on auth failure; no test regression.

### Task A2 — Paper-order placement smoke (full chain, sandbox)
**Why:** Proves gateway → Nautilus → bridge → Arrow `POST /order/regular` works with a real broker sandbox order.
- [ ] Verify `EXECUTION_INTENT_ENABLED=true` is settable on `SignalJob` (`SignalJobConfig.java` line 120) and that the gateway dispatch path is wired (`DurableIntentDispatcher` + `NautilusIntentClient`).
- [ ] Stand up compose with `--profile execution-t3`; confirm `gateway:9180/healthz` 200, `nautilus:9190/healthz` reports `HALTED`, `bridge:8787/healthz` reports `UP disabled` until approval.
- [ ] Place ONE sandbox order `BI-EQ x1` via `POST /v1/intents` with `T9_APPROVED_BY=saurabh` + sandbox broker config (`execution-auth-001` token pattern, len 238 proven live 2026-08-21).
- [ ] Assert: Arrow returns `broker_order_id`; `Execution_Intent` LOG + `Order_Lifecycle` KV + `Execution_Attempts` KV populated; `client_order_ref` echoed.
- [ ] Add `T9_ORDER_SANDBOX` Python integration in `code/01_platform/04_scripts/tests/` (reuses `t8_sandbox_contract_check.py` harness): place→poll→assert, then cancel.
- [ ] Run `python3 code/01_platform/04_scripts/t8_sandbox_contract_check.py` (expect 12/12) + the new test.
- [ ] Evidence `logs/tracker-14/t9-order-sandbox-<yyyymmdd>.md` + `CHG-058`.
**DoD:** one sandbox order placed end-to-end; attempt/lifecycle tables populated; cancel succeeds; no real order possible without `T9_APPROVED_BY`.

### Task A3 — Live postback capture evidence (VM-BROKER-PBK-009)
**Why:** Postback WebSocket behavior is currently `TO_BE_VERIFIED`; the capture path must be proven against real broker confirmations.
- [ ] Subscribe to the sandbox order-updates WebSocket (`/v1/events`) via the bridge; capture a fill postback for the A2 order.
- [ ] Assert `Postback_Quarantine` stays empty for the well-formed postback, `Fills` LOG gets one immutable row, `Order_Lifecycle` transitions to filled, `Positions` KV updates.
- [ ] Add `TestPostbackCapture` (Go) + a `T9_POSTBACK` Python integration; run `cargo test -race` + `pytest`.
- [ ] Evidence `logs/tracker-14/t9-postback-<yyyymmdd>.md` (status `VERIFIED`) + `CHG-059`.
**DoD:** postback identity/correlation (`client_order_ref`/`broker_order_id`) proven; matrix row 7 → VERIFIED.

### Task A4 — Arrow REST capability matrix (VM-ARROW-010)
**Why:** `POST /order/regular` request/response/auth/timeout must be captured and correlated to one broker order.
- [ ] Capture: request shape, success/failure response codes, auth failure (401 → re-auth), 15 s UNKNOWN timeout behavior, one-attempt-to-one-order correlation.
- [ ] Encode as an `ArrowRestCapabilityReport` and pin it; add `TestArrowRestCapability` (Go, fake broker).
- [ ] Evidence `logs/tracker-14/t9-arrow-rest-<yyyymmdd>.md` (status `VERIFIED`) + `CHG-060`.
**DoD:** matrix row 8 → VERIFIED; tests green.

### Task A5 — Reconciliation read-back (DEC-023)
**Why:** Confirms `GET /user/orders|trades|positions` matches Fluss projections so the gate can trust local state.
- [ ] After A2+A3, read back via the reconciliation REST endpoints and diff against `Fills`/`Order_Lifecycle`/`Positions`.
- [ ] Add `T9_RECON` Python integration asserting counts + key fields match (allowing only documented latency).
- [ ] Evidence `logs/tracker-14/t9-reconciliation-<yyyymmdd>.md` + `CHG-061`.
**DoD:** reconciliation delta = 0 (within documented window); no unmatched fills.

### Task A6 — Phase A gate
- [ ] `make full-audit && make gate && make pin-check && make cep-check-module` all exit 0.
- [ ] `make static-check`.
- [ ] Update `docs/08_implementation/12-version-compatibility-evidence.md` rows 7 & 8 status `TO_BE_VERIFIED` → `VERIFIED` (row 9 OpenObserve may stay pending to Phase D).
**DoD:** Phase A complete; order path proven in sandbox; live money still blocked.

> **Caution for executors:** the existing `t9_paper`, `t9_paper_25`, `t9_paper_25_full` bins
> (`code/02_services/04_executor/src/t9paper/mod.rs`) are OFFLINE scripted-scenario harnesses —
> per their own honesty contract, `engine_exercised: false` and no broker round-trip occurs.
> They do NOT satisfy Phase A. Only a live sandbox round-trip via Tasks A2–A5 does.

---

# Phase B — Execution Engine Enablement

> Goal: make Nautilus run its live loop, prove exactly-once under crash, and prove the gate lifecycle + signal→intent→fill flow end-to-end on the local stack.

### Task B1 — `LiveNodeRuntime` long-run soak (FakeBridge)
**Why:** The run loop exists (`engine.rs` `LiveNodeRuntime`) but needs a stability soak to catch leaks/hangs before real wiring.
- [ ] Add `LiveNodeRuntimeSoakTest` in `code/02_services/04_executor/tests/`: run ≥30 min against `FakeBridge`, assert no goroutine/fd leak, clean `stop` via `LiveNodeHandle`, fail-closed duplicate-run guard holds.
- [ ] `cargo test --offline --test LiveNodeRuntimeSoakTest`.
- [ ] Evidence `logs/tracker-14/b1-livenode-soak-<yyyymmdd>.md` + `CHG-062`.
**DoD:** soak green; leaks = 0; duplicate-run guard proven.

### Task B2 — Crash-exactly-once (T5 fence proof)
**Why:** A `kill -9` mid-order must not create a duplicate broker order.
- [ ] Add `CrashExactlyOnceTest`: inject kill after intent accepted but before bridge ack; restart node; assert exactly one `broker_order_id` and one `Execution_Attempts` record; fence (`Execution_Gate`) epoch increments; second run is rejected.
- [ ] Wire to the existing `FlussGateStateStore` + `FlussAttemptStore` (`attemptRefreshOnRecovery`).
- [ ] `cargo test --offline` + env-gated compose drill (`make disaster-drills` style harness optional).
- [ ] Evidence `logs/tracker-14/b2-crash-exactly-once-<yyyymmdd>.md` + `CHG-063`.
**DoD:** duplicate-order impossible by construction; fence test green.

### Task B3 — Gate lifecycle E2E on compose
**Why:** The order path must only reach `ENABLED` via `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED`, gated by DEC-044 Saurabh approval, and return to `HALTED` on a safety halt within 5 s.
- [ ] Add `T9_GATE_LIFECYCLE` integration: publish `Safety_Halt_Requests` → assert `nautilus:9190/healthz` flips to `HALTED` within 5 s; submit Saurabh approval → `APPROVAL_PENDING` → `ENABLED`; publish halt → `HALTED`.
- [ ] Confirm the single-operator gate logic in `src/gate.rs` / `executiongate.rs` checks `saurabh` identity + evidence hash + fencing (DEC-044).
- [ ] Run via `--profile execution-t3`; assert via `t8_sandbox_contract_check.py` extended.
- [ ] Evidence `logs/tracker-14/b3-gate-lifecycle-<yyyymmdd>.md` + `CHG-064`.
**DoD:** full lifecycle proven; fails closed on missing/forged approval; 5 s halt met.

### Task B4 — Signal→Intent→Fill flow E2E
**Why:** Closes the loop: a fired signal becomes an immutable `Execution_Intent` the gateway consumes and Nautilus executes (sandbox).
- [ ] Run `SignalJob` with `EXECUTION_INTENT_ENABLED=true` (fake bridge/sandbox), confirm `Execution_Intent` LOG rows appear.
- [ ] Confirm gateway dispatches → `NautilusIntentClient` → bridge → fill → `Fills`/`Order_Lifecycle`/`Positions`.
- [ ] Add `B4_SIGNAL_INTENT_E2E` env-gated Python integration (`SIGNAL_CHAIN_E2E` pattern + `run-signal-chain-e2e.sh`).
- [ ] Evidence `logs/tracker-14/b4-signal-intent-e2e-<yyyymmdd>.md` + `CHG-065`.
**DoD:** signal → intent → fill round-trip green in sandbox.

### Task B5 — Babysitter live observation drill
**Why:** `BabysitterJob` now observes `Positions` changelog; prove it survives a position write storm + restart without emitting any `Position_Actions`.
- [ ] Drive position writes (from B4), run `BabysitterJob`, assert `POSITION_ACTIONS_ENABLED=false` enforced at startup (fails closed if set), zero action rows, observation `ValueState` survives cancel+restore (`BABYSITTER_STATE_RECOVERY_PATH`).
- [ ] Reuse `submit-jobs.sh` launcher + `COMPUTE_INT_TEST_T7` MiniCluster restore green.
- [ ] Evidence `logs/tracker-14/b5-babysitter-observe-<yyyymmdd>.md` + `CHG-066`.
**DoD:** babysitter observes silently; restart-safe; fails closed if enabled.

### Task B6 — 🔒 DEC-044 release-review checklist (human-prepared doc, agent assembles)
**Why:** The single-operator review must be a documented, signable checklist before any live enablement.
- [ ] Agent assembles `docs/05_deployment/change-records/CHG-067.md` listing every A/B evidence artifact, the gate-flow proof, and the Saurabh sign-off slot.
- [ ] 🔒 Human (Saurabh) signs after reviewing; agent marks Phase B complete only after signature recorded.
**DoD:** review doc complete and signed; Phase B complete.

### Task B7 — In-service durable write path (four permanent clients)
**Why:** The Nautilus service currently keeps its duplicate-send memory in-process. Four durable clients (Fluss state stores, local event journal, R2 backup, OTeL feed) are designed but flag-gated unbuilt — without them an in-process crash can lose the "did I send this?" guard even though Fluss-backed gateway stores exist.
- [ ] Read `docs/08_implementation/19-nautilus-execution-service-implementation-plan.md` §durable-clients design + `CHG-052.md` for the intended client set and flags.
- [ ] Implement each client behind a dedicated feature flag (default OFF), reusing `FlussGateStateStore`/`FlussAttemptStore`/`FlussProjectionWriter` where they already cover a client.
- [ ] Add Rust integration tests per client: write → restart → state recovered; flag off → behavior identical to today (no regression).
- [ ] `cargo test --offline`; evidence `logs/tracker-14/b7-durable-clients-<yyyymmdd>.md` + next free `CHG-*`.
- [ ] 🔒 Enabling the flags in compose requires explicit user approval (recorded in the CHG); default deployment keeps them OFF until B6 sign-off.
**DoD:** four clients implemented + tested behind flags; enabling is a recorded human decision.

### Task B8 — Clock-drift safety enforcement
**Why:** A clock-drift limit is declared in configuration but no code enforces it — an unenforced safety switch. (Ingestion already has `NtpClockChecker` + `TimeJumpMonitor`; the gap is on the execution side.)
- [ ] **Discover:** locate where the clock-drift setting is declared — search `code/02_services/04_executor/src/config.rs`, `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayConfig.java`, `code/common/src/main/java/com/trading/common/config/`, and `code/01_platform/01_docker/docker-compose.yml` env blocks for drift/skew/NTP keys. Record the exact key(s) found.
- [ ] Implement enforcement at startup AND runtime: measured drift beyond the configured limit → service transitions to `HALTED` (fail closed), emits an alert log via the existing telemetry path, and refuses new orders until drift returns within limits and the gate re-enables.
- [ ] Tests: unit test for threshold boundary (just inside = pass, just outside = halt); integration test that a simulated clock jump halts the engine and recovery re-enables only through the normal gate flow.
- [ ] Run `cargo test --offline` (+ gateway `mvn -q test` if Java-side); evidence `logs/tracker-14/b8-clock-drift-<yyyymmdd>.md` + next free `CHG-*`.
**DoD:** declared limit actually enforced end-to-end; halt-on-drift proven by test; no silent tolerance of skew.

---

# Phase C — Scale-Up (Multi-Connection Bridge)

> Goal: lift the 1,024-instrument single-socket cap so the 2,433-instrument manifest runs at the 50k ticks/s envelope under your current subscription.

### Task C1 — Bridge fan-out implementation
**Why:** `ARROW_HFT_CONNECTIONS=1` + `ARROW_HFT_MAX_TOKENS_PER_CONNECTION=1024` cap throughput at 1,024 instruments; multi-connection sharding removes the blocker without a new subscription.
- [ ] In `code/02_services/01_ingestion/go-bridge/` (and mirror for `06_execution_bridge` if it consumes market data), implement `N = ARROW_HFT_CONNECTIONS` sockets, round-robin token sharding across sockets, per-socket reconnect backoff, and aggregate `ARROW_TICK_COUNTS` (file-persisted `/tmp/arrow-tick-counts.txt`).
- [ ] Keep single-connection behavior bit-identical when `N=1` (no regression to current 1,024 path).
- [ ] Add Go tests: `TestTokenShardingDeterministic` (same manifest → same socket assignment), `TestPerSocketReconnectIsolation` (one socket down → others keep streaming), `TestAggregateCounts`.
- [ ] `go test -race ./...` in both bridge modules.
- [ ] Evidence `logs/tracker-14/c1-multiconn-<yyyymmdd>.md` + `CHG-068`.
**DoD:** `N>1` streams N×1,024 instruments; single-socket path unchanged; tests green.

### Task C2 — Config parity + pin update
**Why:** Project requires Java↔Go config parity (`ING-UNIT-018` pattern); new keys must be mirrored and rejected consistently.
- [ ] Add `ARROW_HFT_CONNECTIONS`, `ARROW_HFT_MAX_TOKENS_PER_CONNECTION` to the parity table in `ConfigParityTest` (Java) and `hft_policy_test.go` (Go); reject non-integer/out-of-range with Go FATAL 2 / Java throw.
- [ ] Confirm `ARROW_HFT_LATENCY_MS`/`ARROW_HFT_CONNECTIONS` still flow through `hftRange`/`hftPin`.
- [ ] Evidence + `CHG-069`.
**DoD:** parity tests green; divergent config fails in both languages.

### Task C3 — Losslessness re-validation at scale
**Why:** Multi-connection must not break the count-based losslessness guarantee (`ING-TCP-001`).
- [ ] Run `reconcile-compare.py` multi-epoch across N sockets; assert 0 lost / 0 extra / 0 vanished per token.
- [ ] Evidence `logs/tracker-14/c3-losslessness-multiconn-<yyyymmdd>.md` + `CHG-070`.
**DoD:** losslessness holds across sockets.

### Task C4 — SIG-PERF-001 50k baseline unblocked
**Why:** The 50k baseline was BLOCKED by the 1,024 cap; now runnable.
- [ ] Run the 2,433-instrument / 20 Hz manifest (`NSE_CM_EQUITY.csv`) with `N` connections; assert ≥50k ticks/s synthetic envelope, `0` wire loss, p99 append `< 5 ms` (mirror `sig-perf-001-50k-baseline` envelope proof).
- [ ] Update `docs/08_implementation/04-signal-job.md` pending-item `SIG-PERF-001` 50k half → DONE.
- [ ] Evidence `logs/tracker-14/c4-sig-perf-50k-<yyyymmdd>.md` + `CHG-071`.
**DoD:** 50k baseline certified; Signal job perf gate closed.

### Task C5 — 🔒 Scale-path decision record
- [ ] Agent writes `CHG-072` recording: multi-connection built (C1–C4) AND the premium-tier alternative documented as fallback; **activation of the full 2,433 envelope is left to the human** (set activation env at human discretion). Do not auto-enable.
- [ ] 🔒 Human chooses: (a) run multi-connection at N×1,024 within subscription, or (b) purchase premium tier. Decision recorded; no subscription purchase assumed.
**DoD:** decision recorded; envelope activation gated on human choice.

---

# Phase D — Production Deployment

### Task D1 — 🔒 VM provisioning + agent-verifiable checklist
- [ ] Agent writes `docs/05_deployment/PROD_VM_PROVISIONING.md`: 7 VMs (v1 4 → v2 7: 3 Managers + N≥3 Workers + 1 O2), 500 GB SSD, role labels (`role=worker`,`role=observability`), no hostname pinning.
- [ ] Agent provides `code/01_platform/04_scripts/prod_node_check.py` that, given SSH/API access constants, verifies per-VM disk/label/role and exits non-zero on drift.
- [ ] 🔒 Human creates the VMs; agent's `prod_node_check.py` must pass before D2.
**DoD:** checklist + checker exist; human provisions.

### Task D2 — Swarm bootstrap + stack deploy
- [ ] `make stack-selfcheck` (single-node mimic) passes; then `make stack-config DEPLOY=1` on the real swarm once D1 done.
- [ ] Verify `docker-stack.yml` (already present) uses role-label placement, encrypted overlays, external secrets, durable volumes, pinned digests.
- [ ] Evidence `logs/tracker-14/d2-swarm-deploy-<yyyymmdd>.md` + `CHG-073`.
**DoD:** prod stack deploys; no hostname-pinned placement.

### Task D3 — SWARM-* HA tests live (M3 quorum)
- [ ] Run `make test-09` + the swarm quorum battery: 3-node ZK ensemble, Fluss replication ≥2 across 3 VMs (anti-co-location), Flink HA via ZK.
- [ ] Evidence `logs/tracker-14/d3-swarm-ha-<yyyymmdd>.md` + `CHG-074`.
**DoD:** quorum + anti-co-location proven on real VMs.

### Task D4 — FAIL-VM-LOSS-60000-001 drill
- [ ] Drain one workload VM; measure data-path recovery `< 30 s` and order-path safe-halt `< 5 s`; assert no duplicate orders during failover.
- [ ] Evidence `logs/tracker-14/d4-vmloss-<yyyymmdd>.md` + `CHG-075`.
**DoD:** one-VM loss tolerated; recovery within targets.

### Task D5 — PERF-PROD-60000-001 (p99 < 100 ms @ 50k)
- [ ] Run the production perf campaign at 50k ticks/s, 3,000 instruments; assert p99 trigger-to-commit `< 100 ms` (DEC-029).
- [ ] Evidence `logs/tracker-14/d5-perf-prod-<yyyymmdd>.md` + `CHG-076`.
**DoD:** release latency target met on prod-like stack.

### Task D6 — Disaster drills DR-001..006 on prod stack
- [ ] `make disaster-drills ARGS="--dry-run"` then `--approve` against the prod stack; assert recovery assertions for coordinator/tablet/ZK quorum/O2/gateway/network-partition faults.
- [ ] Evidence under `logs/disaster-drills/` + `CHG-077`.
**DoD:** all Item F drills green on prod stack.

### Task D7 — Observability finalization
- [ ] Set alert thresholds from measured data (D5/D4); seed prod OpenObserve dashboards (`seed_dashboards.py`); verify O2 outage leaves durable local audit (`OPS-FAIL-001`).
- [ ] Evidence `logs/tracker-14/d7-observability-<yyyymmdd>.md` + `CHG-078`.
**DoD:** thresholds data-derived; dashboards live; O2-independent audit proven.

---

# Phase E — Release Evidence Package

### Task E1 — Version matrix completion (rows 7–10)
- [ ] `docs/08_implementation/12-version-compatibility-evidence.md`: rows 7 (postback) + 8 (Arrow REST) → VERIFIED (from A3/A4); row 9 (OpenObserve) → VERIFIED (from D7); row 10 (base images) → VERIFIED (pin-check).
- [ ] No `TO_BE_VERIFIED` row remains.
**DoD:** matrix fully VERIFIED.

### Task E2 — Full Monday gate green
- [ ] `make gate` (static + compose + go + java + schema/perf) exits 0; capture output to `logs/tracker-14/e2-monday-gate-<yyyymmdd>.log`.
- [ ] `make full-audit` + `make stale-tables` + `make pin-check` + `make cep-check-module` all exit 0 (update hardcoded test-count truth numbers if tests were added — see Gotchas).
**DoD:** every gate green.

### Task E3 — Release evidence package assembly
- [ ] Assemble per `docs/08_implementation/11-testing-and-release.md` final-release-evidence format: every acceptance ID (AC-*) mapped to an evidence artifact; `EvidenceRecord` entries for each.
- [ ] Produce `docs/08_implementation/RELEASE_EVIDENCE_2026-08-21.md`.
**DoD:** every required AC has an evidence pointer.

### Task E4 — Docs consistency reconciliation
- [ ] Run `make docs-audit`; fix any drift (status banners, table counts, ownership matrix). Update `docs/08_implementation/00-start-here.md` "Current readiness" table to reflect A–E completion.
**DoD:** docs-audit green; readiness table truthful.

### Task E5 — 🔒 DEC-044 single-operator release review + sign-off
- [ ] Review `RELEASE_EVIDENCE_2026-08-21.md` + `CHG-067` checklist.
- [ ] 🔒 Saurabh executes the single-operator approval (evidence hash + gate epoch) and records `CHG-079` (status flip `Blocked` → `Approved-for-Testing`, live money still gated).
- [ ] Agent flips the dossier status banners to `Approved-for-testing` after signature.
**DoD:** reviewed, signed, status flipped; live money remains blocked until a separate explicit go-live decision.

### Task E5b — Audit legal-hold / immutability evidence (Cloudflare R2)
**Why:** Money-path audit must be provably immutable and retrievable. On R2 the WORM mechanism is prefix bucket-locks (S3 Object Lock API is unsupported — outside our control); the API token for programmatic verification was never issued.
- [ ] 🔒 Human creates a scoped Cloudflare API token (R2 read + retention admin).
- [ ] Agent extends `code/01_platform/04_scripts/audit_r2.py` (or adds `r2_legal_hold_check.py`) to verify: prefix retention rules active on audit buckets, retrieval of a sampled object, hash-chain spot check against `AuditHashChain`.
- [ ] Record limitation honestly: R2 bucket-locks ≠ S3 Object Lock; document residual risk in the CHG.
- [ ] Evidence `logs/tracker-14/e5b-legalhold-<yyyymmdd>.md` + next free `CHG-*`.
**DoD:** retention rules verified programmatically; retrieval + integrity sampled; limitation documented.

### Task E5c — Missing named E2E fixture artifacts
**Why:** Several ingestion/network scenarios are named in `docs/02_requirements/09-acceptance-matrix.md` whose building blocks and unit tests exist, but the dated end-to-end evidence artifacts were never produced (audit finding B5).
- [ ] Grep the acceptance matrix for rows still marked `EVIDENCE_BLOCKED` / `NOT_IMPLEMENTED` whose implementing tests exist green.
- [ ] For each, run the env-gated integration test against the dev cluster and save the artifact under `logs/tracker-14/<test-id>-<yyyymmdd>/`.
- [ ] Flip the matrix cell to point at the artifact; rerun `make full-audit`.
**DoD:** zero matrix rows remain blocked solely for lack of a produced artifact (any still-blocked row cites an external cause).

### Task E6 — Final verification
- [ ] Run: `make full-audit && make gate && make pin-check && make cep-check-module && make static-check` — all exit 0.
- [ ] `cd code && mvn -q test -pl common,02_services/01_ingestion,02_services/02_compute` green; `cargo test --offline` green; `go test -race ./...` green.
- [ ] Mark every checkbox above `[x]`.
**DoD:** whole-plan acceptance criteria satisfied.

---

## Cross-Cutting Constraints (every task must obey)
- **No live money without `T9_APPROVED_BY` + sandbox config for A/B; live enablement requires E5 human sign-off.**
- **No Flink CEP** anywhere (project policy; `cep_guard.sh` fails the build).
- **Pin discipline:** `versions.pin` exact digests; no floating tags, no external `SNAPSHOT` (CI runs `make pin-check`).
- **Fail-closed defaults:** feature flags off; missing/forged approval → halt; unknown broker outcome → halt.
- **Evidence-first:** every claim → dated artifact in `logs/<topic>/` + `CHG-*` in `docs/05_deployment/change-records/` (allocate the next free number).
- **Repo gates stay green:** run `make full-audit` after each task; update hardcoded test-count truth numbers when tests change.
- **No scope creep:** ranking/reservations/decisions/multi-broker/BSE/K8s are explicitly out of scope.

## Post-Completion (manual / external)
- 🔒 Provision cloud VMs (D1) — human.
- 🔒 Provide broker sandbox + (later) real credentials (A) — human.
- 🔒 Premium-tier subscription decision (C5) — human.
- 🔒 Final go-live switch beyond Approved-for-Testing (E5) — human, separate decision.
- Broker-side contract drift (new API versions) — monitor via `make pin-check` + `version_matrix_verify.py`.

---

Created plan: `docs/plans/2026-08-21-live-readiness-master-plan.md`
