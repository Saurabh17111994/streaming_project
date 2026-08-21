# Live-Readiness Master Plan — Close All Gaps to Approved-for-Live Trading

**Date:** 2026-08-21
**Status:** Execution contract (self-contained; a fresh coding agent can execute it with only this file + the repository)
**Scope:** Everything between today (well-tested market-data → candles → placeholder-signals machine) and a single real, approved, safe order.
**Authority:** Docs win over this plan on conflict; this plan implements `docs/08_implementation/00-start-here.md` mandatory order and the audit at `docs/plans/2026-08-21-*-audit` (see Review Handoff).

---

---

## 📌 LIVE TRACKER (this file is the execution ledger — mark it as you go)

**How to use**
1. One step = one checkbox. Flip `- [ ] \`X.n\`` to `- [x] \`X.n\`` the moment that step is verified — never batch.
2. When ALL steps of a task are `[x]`, set its row below to `DONE`. Starting a task → `IN-PROGRESS`. Blocked → `BLOCKED: <reason>`. Cut by user decision → `SKIPPED: <CHG ref>`.
3. 🔒 rows need a human action — the agent does everything except the 🔒 step, then stops.
4. Never delete a row. Add discovered work as a new numbered task (`Task B9 — …`) + row here.
5. After each session: run `make full-audit` — if red, fix doc/count drift before closing.

**Status legend:** `TODO` (ready to execute) · `IN-PROGRESS` · `DONE` · `BLOCKED: reason` (`market-hours`, `needs prod VMs`, `awaiting human`) · `SKIPPED: CHG-ref`

| ID | Task | Depends on | Status |
| --- | --- | --- | --- |
| `A1` | Sandbox auth + auto re-auth harness | — | TODO |
| `A2` | Paper-order placement smoke (full chain, sandbox) | A1 BLOCKED: market-hours (harness buildable now) |
| `A3` | Live postback capture evidence (VM-BROKER-PBK-009) | A2 BLOCKED: market-hours + A2 |
| `A4` | Arrow REST capability matrix (VM-ARROW-010) | A2 | TODO |
| `A5` | Reconciliation read-back (DEC-023) | A3+A4 BLOCKED: market-hours + A3/A4 |
| `A6` | Phase A gate | A1–A5 | TODO |
| `B1` | `LiveNodeRuntime` long-run soak (FakeBridge) | — | DONE |
| `B2` | Crash-exactly-once (T5 fence proof) | B1 | DONE |
| `B3` | Gate lifecycle E2E on compose | B2 | TODO |
| `B4` | Signal→Intent→Fill flow E2E | B3+A2 | TODO |
| `B5` | Babysitter live observation drill | B4 | TODO |
| `B6` | 🔒 DEC-044 release-review checklist (human-prepared doc, agent assembles) | 🔒 B1–B5 BLOCKED: awaiting human signature |
| `B7` | In-service durable write path (four permanent clients) | — (enable: 🔒) | DONE |
| `B8` | Clock-drift safety enforcement | — | DONE |
| `C1` | Bridge fan-out implementation | — | TODO |
| `C2` | Config parity + pin update | C1 | TODO |
| `C3` | Losslessness re-validation at scale | C1+C2 | TODO |
| `C4` | SIG-PERF-001 50k baseline unblocked | C3 | TODO |
| `C5` | 🔒 Scale-path decision record | 🔒 C4 BLOCKED: awaiting human decision |
| `D1` | 🔒 VM provisioning + agent-verifiable checklist | 🔒 human VMs BLOCKED: awaiting human VM provisioning |
| `D2` | Swarm bootstrap + stack deploy | D1 BLOCKED: needs prod VMs (single-node mimic OK) |
| `D3` | SWARM-* HA tests live (M3 quorum) | D2 BLOCKED: needs prod VMs |
| `D4` | FAIL-VM-LOSS-60000-001 drill | D3 BLOCKED: needs prod VMs |
| `D5` | PERF-PROD-60000-001 (p99 < 100 ms @ 50k) | C4+D3 BLOCKED: needs prod VMs |
| `D6` | Disaster drills DR-001..006 on prod stack | D2 | TODO |
| `D7` | Observability finalization | D5+D6 BLOCKED: needs D4/D5 measured data |
| `E1` | Version matrix completion (rows 7–10) | A3–A5+D7 BLOCKED: needs A3–A5+D7 |
| `E2` | Full Monday gate green | all gates green | TODO |
| `E3` | Release evidence package assembly | E1+E2 | TODO |
| `E4` | Docs consistency reconciliation | E3 | TODO |
| `E5` | 🔒 DEC-044 single-operator release review + sign-off | 🔒 E3+E4+E5b+E5c+B6 BLOCKED: awaiting human review |
| `E5b` | Audit legal-hold / immutability evidence (Cloudflare R2) | 🔒 CF token BLOCKED: awaiting human CF token |
| `E5c` | Missing named E2E fixture artifacts | E2 | TODO |
| `E6` | Final verification | E5 | TODO |

### 🚦 Execution readiness triage — dev laptop · market closed · no VMs (set 2026-08-21)

**✅ Implementable NOW on this laptop** — B1, B2, B3, B4, B5, B7, B8, C1, C2, C3, C4, E2, E5c
(all run on FakeBridge / synthetic frames / local compose / dev cluster — zero live-market or VM need)

**🟡 PREP-only now** — build code/tests/harness/docs today, real execution waits:
| ID | Do now | Waits for |
| --- | --- | --- |
| `A1` | auto-re-auth code + unit tests (fake clock) | one live re-auth check (auth endpoint works off-hours) |
| `A2` | sandbox-order integration harness + contract checks | 🔒 credentials + market hours (no fills while closed) |
| `A4` | error-path evidence: 401 auth-fail, 15 s UNKNOWN timeout | success-response half of matrix |
| `B6` | assemble DEC-044 checklist doc | 🔒 Saurabh signature |
| `C5` | draft scale-decision CHG record | 🔒 premium-vs-multi-connection choice |
| `D1` | `PROD_VM_PROVISIONING.md` + `prod_node_check.py` | 🔒 you creating the VMs |
| `D6` | re-run drills on local compose (already green there) | prod-stack rerun |
| `E4` | docs-audit hygiene pass | final truthfulness flip post-phases |

**❌ Blocked outright**
| IDs | Blocker |
| --- | --- |
| A3, A5 | real fill needed → market hours (+A2 done) |
| D2-real, D3, D4, D5 | real multi-VM rig — laptop cannot honestly produce these numbers |
| D7 | needs D4/D5 measurements as input |
| E1 | rows wait on A3–A5 + D7 |
| E3, E6 | wait on everything above |
| E5b, E5, PC items | 🔒 human token / sign-offs / purchases |

**Suggested order for a laptop session:** B1→B2→B8→B7 → C1→C2 → B3→B4→B5 → C3→C4 (long, background) → E2→E5c → A1-code/B6/C5/D1/E4 drafts. Afterward only two bundles remain: **one market-session batch** (A1-live → A2 → A3 → A5 → A4-success) and **the VM era** (D1→…→D7).


**Progress:** 0 / 34 tasks done · AC criteria 0 / 7 met

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
- [ ] `AC.1` A paper order (broker sandbox, `BI-EQ x1`) placed through `SignalJob → Execution_Intent → Gateway → Nautilus → go-arrow bridge → Arrow REST` returns a broker order id, and the matching postback projects into `Fills`/`Order_Lifecycle`/`Positions` with zero duplicates and reconciles against `GET /user/orders|trades|positions`.
- [ ] `AC.2` `Nautilus` runs its live event loop; a `kill -9` mid-order does not create a duplicate broker order (fence proof).
- [ ] `AC.3` The gate reaches `ENABLED` only through `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED` via the DEC-044 Saurabh approval path; a `Safety_Halt_Requests` row returns it to `HALTED` within 5 s.
- [ ] `AC.4` The 2,433-instrument manifest runs at ≥50k ticks/s synthetic envelope with `0` wire loss and p99 append `< 5 ms` (SIG-PERF-001 50k baseline unblocked).
- [ ] `AC.5` Production Swarm survives one workload-VM loss (FAIL-VM-LOSS-60000-001) and meets p99 `< 100 ms` trigger-to-commit at 50k (PERF-PROD-60000-001).
- [ ] `AC.6` `make full-audit && make gate && make pin-check && make cep-check-module` all exit 0; version-matrix rows 7/8/9/10 read `VERIFIED`; a release evidence package exists per `docs/08_implementation/11-testing-and-release.md`.
- [ ] `AC.7` DEC-044 single-operator (Saurabh) release review executed and recorded; system flips from `Blocked` to `Approved-for-Testing` with live money still gated.

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
- [ ] `A1.1` Read `code/02_services/06_execution_bridge/go-bridge/broker.go` + `fake_arrow_broker_test.go` to confirm the auth interface and the existing re-auth stub.
- [ ] `A1.2` Implement automatic re-auth on 401/`token_expired`: refresh TOTP, retry once, then surface `UP disabled` via `/healthz` if refresh fails (do NOT loop forever).
- [ ] `A1.3` Add Go test `TestSandboxAutoReauth` in `go-bridge/broker_test.go`: fake clock expiry → exactly one re-auth → success; repeated failure → `/healthz` reports disabled, no order attempted.
- [ ] `A1.4` Run `cd code/02_services/06_execution_bridge/go-bridge && go test -race -run TestSandboxAutoReauth ./...`.
- [ ] `A1.5` Write evidence `logs/tracker-14/t9-sandbox-reauth-<yyyymmdd>.md` + `CHG-057` record.
**DoD:** re-auth unit-tested; `/healthz` reflects disabled on auth failure; no test regression.

### Task A2 — Paper-order placement smoke (full chain, sandbox)
**Why:** Proves gateway → Nautilus → bridge → Arrow `POST /order/regular` works with a real broker sandbox order.
- [ ] `A2.1` Verify `EXECUTION_INTENT_ENABLED=true` is settable on `SignalJob` (`SignalJobConfig.java` line 120) and that the gateway dispatch path is wired (`DurableIntentDispatcher` + `NautilusIntentClient`).
- [ ] `A2.2` Stand up compose with `--profile execution-t3`; confirm `gateway:9180/healthz` 200, `nautilus:9190/healthz` reports `HALTED`, `bridge:8787/healthz` reports `UP disabled` until approval.
- [ ] `A2.3` Place ONE sandbox order `BI-EQ x1` via `POST /v1/intents` with `T9_APPROVED_BY=saurabh` + sandbox broker config (`execution-auth-001` token pattern, len 238 proven live 2026-08-21).
- [ ] `A2.4` Assert: Arrow returns `broker_order_id`; `Execution_Intent` LOG + `Order_Lifecycle` KV + `Execution_Attempts` KV populated; `client_order_ref` echoed.
- [ ] `A2.5` Add `T9_ORDER_SANDBOX` Python integration in `code/01_platform/04_scripts/tests/` (reuses `t8_sandbox_contract_check.py` harness): place→poll→assert, then cancel.
- [ ] `A2.6` Run `python3 code/01_platform/04_scripts/t8_sandbox_contract_check.py` (expect 12/12) + the new test.
- [ ] `A2.7` Evidence `logs/tracker-14/t9-order-sandbox-<yyyymmdd>.md` + `CHG-058`.
**DoD:** one sandbox order placed end-to-end; attempt/lifecycle tables populated; cancel succeeds; no real order possible without `T9_APPROVED_BY`.

### Task A3 — Live postback capture evidence (VM-BROKER-PBK-009)
**Why:** Postback WebSocket behavior is currently `TO_BE_VERIFIED`; the capture path must be proven against real broker confirmations.
- [ ] `A3.1` Subscribe to the sandbox order-updates WebSocket (`/v1/events`) via the bridge; capture a fill postback for the A2 order.
- [ ] `A3.2` Assert `Postback_Quarantine` stays empty for the well-formed postback, `Fills` LOG gets one immutable row, `Order_Lifecycle` transitions to filled, `Positions` KV updates.
- [ ] `A3.3` Add `TestPostbackCapture` (Go) + a `T9_POSTBACK` Python integration; run `cargo test -race` + `pytest`.
- [ ] `A3.4` Evidence `logs/tracker-14/t9-postback-<yyyymmdd>.md` (status `VERIFIED`) + `CHG-059`.
**DoD:** postback identity/correlation (`client_order_ref`/`broker_order_id`) proven; matrix row 7 → VERIFIED.

### Task A4 — Arrow REST capability matrix (VM-ARROW-010)
**Why:** `POST /order/regular` request/response/auth/timeout must be captured and correlated to one broker order.
- [ ] `A4.1` Capture: request shape, success/failure response codes, auth failure (401 → re-auth), 15 s UNKNOWN timeout behavior, one-attempt-to-one-order correlation.
- [ ] `A4.2` Encode as an `ArrowRestCapabilityReport` and pin it; add `TestArrowRestCapability` (Go, fake broker).
- [ ] `A4.3` Evidence `logs/tracker-14/t9-arrow-rest-<yyyymmdd>.md` (status `VERIFIED`) + `CHG-060`.
**DoD:** matrix row 8 → VERIFIED; tests green.

### Task A5 — Reconciliation read-back (DEC-023)
**Why:** Confirms `GET /user/orders|trades|positions` matches Fluss projections so the gate can trust local state.
- [ ] `A5.1` After A2+A3, read back via the reconciliation REST endpoints and diff against `Fills`/`Order_Lifecycle`/`Positions`.
- [ ] `A5.2` Add `T9_RECON` Python integration asserting counts + key fields match (allowing only documented latency).
- [ ] `A5.3` Evidence `logs/tracker-14/t9-reconciliation-<yyyymmdd>.md` + `CHG-061`.
**DoD:** reconciliation delta = 0 (within documented window); no unmatched fills.

### Task A6 — Phase A gate
- [ ] `A6.1` `make full-audit && make gate && make pin-check && make cep-check-module` all exit 0.
- [ ] `A6.2` `make static-check`.
- [ ] `A6.3` Update `docs/08_implementation/12-version-compatibility-evidence.md` rows 7 & 8 status `TO_BE_VERIFIED` → `VERIFIED` (row 9 OpenObserve may stay pending to Phase D).
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
- [x] `B1.1` Add soak test in `code/02_services/04_executor/tests/live_node_soak.rs` (as `live_node_runtime_sustained_soak`, env-tunable `SOAK_SECS`, default 30 s dev leg / 1800 s evidence leg): gate-boots-HALTED, liveness sampling, clean stop via handle, duplicate-run guard, fd/RSS leak bounds. Validation leg green; full suite 131 lib + integration targets green.
- [x] `B1.2` Evidence leg: `SOAK_SECS=1800 cargo test --offline --test live_node_soak` — exit 0 in 1810.01 s (boot 12:23:40Z → stop signal at exactly 1800 s → graceful shutdown). Dev leg + full suite green (136/0).
- [x] `B1.3` Evidence `logs/nautilus-execution/b1-livenode-soak-20260821.md` + CHG-062 filed.
**DoD:** soak green; leaks = 0; duplicate-run guard proven.

### Task B2 — Crash-exactly-once (T5 fence proof)
**Why:** A `kill -9` mid-order must not create a duplicate broker order.
- [x] `B2.1` Add crash-exactly-once composite test: `tests/crash_exactly_once.rs` — mid-flight kill, restart on shared durable memory, replay halts with ZERO duplicate calls, single durable attempt record. Protocol finding: epoch bump CANNOT fence an unresolved attempt (quarantine holds); reconciliation-by-evidence then re-enable resumes flow.
- [x] `B2.2` Runs against the existing durable protocol (`AttemptStore`/`GateStateStore`/`BridgeCaller` traits + `InMemory*` stores that mirror the Java Fluss-backed stores). Fluss-backed store swap is Workstream D; traits are unchanged by D.
- [x] `B2.3` Full suite green: 136 passed / 0 failed. Compose drill not needed (pure in-process proof; DR-005 already covers gateway crash/restart at compose level).
- [x] `B2.4` Evidence `logs/nautilus-execution/b2-crash-exactly-once-20260821.md` + CHG-063 filed.
**DoD:** duplicate-order impossible by construction; fence test green.

### Task B3 — Gate lifecycle E2E on compose
**Why:** The order path must only reach `ENABLED` via `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED`, gated by DEC-044 Saurabh approval, and return to `HALTED` on a safety halt within 5 s.
- [ ] `B3.1` Add `T9_GATE_LIFECYCLE` integration: publish `Safety_Halt_Requests` → assert `nautilus:9190/healthz` flips to `HALTED` within 5 s; submit Saurabh approval → `APPROVAL_PENDING` → `ENABLED`; publish halt → `HALTED`.
- [ ] `B3.2` Confirm the single-operator gate logic in `src/gate.rs` / `executiongate.rs` checks `saurabh` identity + evidence hash + fencing (DEC-044).
- [ ] `B3.3` Run via `--profile execution-t3`; assert via `t8_sandbox_contract_check.py` extended.
- [ ] `B3.4` Evidence `logs/tracker-14/b3-gate-lifecycle-<yyyymmdd>.md` + `CHG-064`.
**DoD:** full lifecycle proven; fails closed on missing/forged approval; 5 s halt met.

### Task B4 — Signal→Intent→Fill flow E2E
**Why:** Closes the loop: a fired signal becomes an immutable `Execution_Intent` the gateway consumes and Nautilus executes (sandbox).
- [ ] `B4.1` Run `SignalJob` with `EXECUTION_INTENT_ENABLED=true` (fake bridge/sandbox), confirm `Execution_Intent` LOG rows appear.
- [ ] `B4.2` Confirm gateway dispatches → `NautilusIntentClient` → bridge → fill → `Fills`/`Order_Lifecycle`/`Positions`.
- [ ] `B4.3` Add `B4_SIGNAL_INTENT_E2E` env-gated Python integration (`SIGNAL_CHAIN_E2E` pattern + `run-signal-chain-e2e.sh`).
- [ ] `B4.4` Evidence `logs/tracker-14/b4-signal-intent-e2e-<yyyymmdd>.md` + `CHG-065`.
**DoD:** signal → intent → fill round-trip green in sandbox.

### Task B5 — Babysitter live observation drill
**Why:** `BabysitterJob` now observes `Positions` changelog; prove it survives a position write storm + restart without emitting any `Position_Actions`.
- [ ] `B5.1` Drive position writes (from B4), run `BabysitterJob`, assert `POSITION_ACTIONS_ENABLED=false` enforced at startup (fails closed if set), zero action rows, observation `ValueState` survives cancel+restore (`BABYSITTER_STATE_RECOVERY_PATH`).
- [ ] `B5.2` Reuse `submit-jobs.sh` launcher + `COMPUTE_INT_TEST_T7` MiniCluster restore green.
- [ ] `B5.3` Evidence `logs/tracker-14/b5-babysitter-observe-<yyyymmdd>.md` + `CHG-066`.
**DoD:** babysitter observes silently; restart-safe; fails closed if enabled.

### Task B6 — 🔒 DEC-044 release-review checklist (human-prepared doc, agent assembles)
**Why:** The single-operator review must be a documented, signable checklist before any live enablement.
- [ ] `B6.1` Agent assembles `docs/05_deployment/change-records/CHG-067.md` listing every A/B evidence artifact, the gate-flow proof, and the Saurabh sign-off slot.
- [ ] `B6.2` 🔒 Human (Saurabh) signs after reviewing; agent marks Phase B complete only after signature recorded.
**DoD:** review doc complete and signed; Phase B complete.

### Task B7 — In-service durable write path (four permanent clients)
**Why:** The Nautilus service currently keeps its duplicate-send memory in-process. Four durable clients (Fluss state stores, local event journal, R2 backup, OTeL feed) are designed but flag-gated unbuilt — without them an in-process crash can lose the "did I send this?" guard even though Fluss-backed gateway stores exist.
- [x] `B7.1` Read §durable-clients design + CHG-052: no `DURABLE_*` flags existed; executor used `InMemory*` stores (mirrors of Java `InMemory*`); Fluss-backed stores exist on Java gateway side (CHG-051/052) but not consumed durably on Rust side.
- [x] `B7.2` New `src/durable.rs`: 4 clients (gate/attempts/journal/audit) behind `DURABLE_GATE/ATTEMPTS/JOURNAL/AUDIT_ENABLED` (all default OFF). Gate/attempts reuse `GateStateStore`/`AttemptStore` traits + `InMemory*` stores; journal/audit are new append-only traits with in-memory offline impls. `DurableClients` bundles as `Rc`-shared handles for restart sharing; live Fluss/file/OTel impls swap at Workstream D.
- [x] `B7.3` 8 durable tests (gate/attempt/journal/audit write→restart→recovered; flag OFF vs ON identical — no regression) + 1 config test (defaults + selective enable).
- [x] `B7.4` Suite green 153 passed / 0 failed (was 144). Evidence `logs/nautilus-execution/b7-durable-clients-20260821.md` + CHG-065.
- [x] `B7.5` Documented: default deployment keeps all OFF until B6 sign-off (DEC-044); enabling any flag in compose requires explicit user approval recorded in the enabling CHG (CHG-065 notes the gate).
**DoD:** four clients implemented + tested behind flags; enabling is a recorded human decision.

### Task B8 — Clock-drift safety enforcement
**Why:** A clock-drift limit is declared in configuration but no code enforces it — an unenforced safety switch. (Ingestion already has `NtpClockChecker` + `TimeJumpMonitor`; the gap is on the execution side.)
- [x] `B8.1` Found: `CLOCK_OFFSET_LIMIT_MS` (default 200) exists ONLY in compose's ingestion service block (enforced there by Java `NtpClockChecker`); executor + gateway configs had zero drift/skew/NTP keys. Evidence in `logs/nautilus-execution/b8-clock-drift-20260821.md`.
- [x] `B8.2` New `src/clockwatch.rs`: `DriftMonitor.enforce()` — `Beyond`/`Unmeasurable` → `Gate::safety_halt()` + `tracing::error!` alert; recovery ONLY via sanctioned path (proven by test). Config now reads `CLOCK_OFFSET_LIMIT_MS` (default 200); bootstrap exposes `Runtime::enforce_clock_drift()`. Production NTP source = Workstream D behind `OffsetSource` trait.
- [x] `B8.3` 7 monitor tests (±200/±201 boundary symmetric, fail-closed on probe error, halt clears approvals, idempotent breaches, within-limit no-op, sanctioned-path-only recovery incl. rejected direct transition + zero auto-recovery) + config default/override test.
- [x] `B8.4` Suite green 144 passed / 0 failed (was 136). Java-side not needed (gap was execution-side only). Evidence `logs/nautilus-execution/b8-clock-drift-20260821.md` + CHG-064.
**DoD:** declared limit actually enforced end-to-end; halt-on-drift proven by test; no silent tolerance of skew.

---

# Phase C — Scale-Up (Multi-Connection Bridge)

> Goal: lift the 1,024-instrument single-socket cap so the 2,433-instrument manifest runs at the 50k ticks/s envelope under your current subscription.

### Task C1 — Bridge fan-out implementation
**Why:** `ARROW_HFT_CONNECTIONS=1` + `ARROW_HFT_MAX_TOKENS_PER_CONNECTION=1024` cap throughput at 1,024 instruments; multi-connection sharding removes the blocker without a new subscription.
- [ ] `C1.1` In `code/02_services/01_ingestion/go-bridge/` (and mirror for `06_execution_bridge` if it consumes market data), implement `N = ARROW_HFT_CONNECTIONS` sockets, round-robin token sharding across sockets, per-socket reconnect backoff, and aggregate `ARROW_TICK_COUNTS` (file-persisted `/tmp/arrow-tick-counts.txt`).
- [ ] `C1.2` Keep single-connection behavior bit-identical when `N=1` (no regression to current 1,024 path).
- [ ] `C1.3` Add Go tests: `TestTokenShardingDeterministic` (same manifest → same socket assignment), `TestPerSocketReconnectIsolation` (one socket down → others keep streaming), `TestAggregateCounts`.
- [ ] `C1.4` `go test -race ./...` in both bridge modules.
- [ ] `C1.5` Evidence `logs/tracker-14/c1-multiconn-<yyyymmdd>.md` + `CHG-068`.
**DoD:** `N>1` streams N×1,024 instruments; single-socket path unchanged; tests green.

### Task C2 — Config parity + pin update
**Why:** Project requires Java↔Go config parity (`ING-UNIT-018` pattern); new keys must be mirrored and rejected consistently.
- [ ] `C2.1` Add `ARROW_HFT_CONNECTIONS`, `ARROW_HFT_MAX_TOKENS_PER_CONNECTION` to the parity table in `ConfigParityTest` (Java) and `hft_policy_test.go` (Go); reject non-integer/out-of-range with Go FATAL 2 / Java throw.
- [ ] `C2.2` Confirm `ARROW_HFT_LATENCY_MS`/`ARROW_HFT_CONNECTIONS` still flow through `hftRange`/`hftPin`.
- [ ] `C2.3` Evidence + `CHG-069`.
**DoD:** parity tests green; divergent config fails in both languages.

### Task C3 — Losslessness re-validation at scale
**Why:** Multi-connection must not break the count-based losslessness guarantee (`ING-TCP-001`).
- [ ] `C3.1` Run `reconcile-compare.py` multi-epoch across N sockets; assert 0 lost / 0 extra / 0 vanished per token.
- [ ] `C3.2` Evidence `logs/tracker-14/c3-losslessness-multiconn-<yyyymmdd>.md` + `CHG-070`.
**DoD:** losslessness holds across sockets.

### Task C4 — SIG-PERF-001 50k baseline unblocked
**Why:** The 50k baseline was BLOCKED by the 1,024 cap; now runnable.
- [ ] `C4.1` Run the 2,433-instrument / 20 Hz manifest (`NSE_CM_EQUITY.csv`) with `N` connections; assert ≥50k ticks/s synthetic envelope, `0` wire loss, p99 append `< 5 ms` (mirror `sig-perf-001-50k-baseline` envelope proof).
- [ ] `C4.2` Update `docs/08_implementation/04-signal-job.md` pending-item `SIG-PERF-001` 50k half → DONE.
- [ ] `C4.3` Evidence `logs/tracker-14/c4-sig-perf-50k-<yyyymmdd>.md` + `CHG-071`.
**DoD:** 50k baseline certified; Signal job perf gate closed.

### Task C5 — 🔒 Scale-path decision record
- [ ] `C5.1` Agent writes `CHG-072` recording: multi-connection built (C1–C4) AND the premium-tier alternative documented as fallback; **activation of the full 2,433 envelope is left to the human** (set activation env at human discretion). Do not auto-enable.
- [ ] `C5.2` 🔒 Human chooses: (a) run multi-connection at N×1,024 within subscription, or (b) purchase premium tier. Decision recorded; no subscription purchase assumed.
**DoD:** decision recorded; envelope activation gated on human choice.

---

# Phase D — Production Deployment

### Task D1 — 🔒 VM provisioning + agent-verifiable checklist
- [ ] `D1.1` Agent writes `docs/05_deployment/PROD_VM_PROVISIONING.md`: 7 VMs (v1 4 → v2 7: 3 Managers + N≥3 Workers + 1 O2), 500 GB SSD, role labels (`role=worker`,`role=observability`), no hostname pinning.
- [ ] `D1.2` Agent provides `code/01_platform/04_scripts/prod_node_check.py` that, given SSH/API access constants, verifies per-VM disk/label/role and exits non-zero on drift.
- [ ] `D1.3` 🔒 Human creates the VMs; agent's `prod_node_check.py` must pass before D2.
**DoD:** checklist + checker exist; human provisions.

### Task D2 — Swarm bootstrap + stack deploy
- [ ] `D2.1` `make stack-selfcheck` (single-node mimic) passes; then `make stack-config DEPLOY=1` on the real swarm once D1 done.
- [ ] `D2.2` Verify `docker-stack.yml` (already present) uses role-label placement, encrypted overlays, external secrets, durable volumes, pinned digests.
- [ ] `D2.3` Evidence `logs/tracker-14/d2-swarm-deploy-<yyyymmdd>.md` + `CHG-073`.
**DoD:** prod stack deploys; no hostname-pinned placement.

### Task D3 — SWARM-* HA tests live (M3 quorum)
- [ ] `D3.1` Run `make test-09` + the swarm quorum battery: 3-node ZK ensemble, Fluss replication ≥2 across 3 VMs (anti-co-location), Flink HA via ZK.
- [ ] `D3.2` Evidence `logs/tracker-14/d3-swarm-ha-<yyyymmdd>.md` + `CHG-074`.
**DoD:** quorum + anti-co-location proven on real VMs.

### Task D4 — FAIL-VM-LOSS-60000-001 drill
- [ ] `D4.1` Drain one workload VM; measure data-path recovery `< 30 s` and order-path safe-halt `< 5 s`; assert no duplicate orders during failover.
- [ ] `D4.2` Evidence `logs/tracker-14/d4-vmloss-<yyyymmdd>.md` + `CHG-075`.
**DoD:** one-VM loss tolerated; recovery within targets.

### Task D5 — PERF-PROD-60000-001 (p99 < 100 ms @ 50k)
- [ ] `D5.1` Run the production perf campaign at 50k ticks/s, 3,000 instruments; assert p99 trigger-to-commit `< 100 ms` (DEC-029).
- [ ] `D5.2` Evidence `logs/tracker-14/d5-perf-prod-<yyyymmdd>.md` + `CHG-076`.
**DoD:** release latency target met on prod-like stack.

### Task D6 — Disaster drills DR-001..006 on prod stack
- [ ] `D6.1` `make disaster-drills ARGS="--dry-run"` then `--approve` against the prod stack; assert recovery assertions for coordinator/tablet/ZK quorum/O2/gateway/network-partition faults.
- [ ] `D6.2` Evidence under `logs/disaster-drills/` + `CHG-077`.
**DoD:** all Item F drills green on prod stack.

### Task D7 — Observability finalization
- [ ] `D7.1` Set alert thresholds from measured data (D5/D4); seed prod OpenObserve dashboards (`seed_dashboards.py`); verify O2 outage leaves durable local audit (`OPS-FAIL-001`).
- [ ] `D7.2` Evidence `logs/tracker-14/d7-observability-<yyyymmdd>.md` + `CHG-078`.
**DoD:** thresholds data-derived; dashboards live; O2-independent audit proven.

---

# Phase E — Release Evidence Package

### Task E1 — Version matrix completion (rows 7–10)
- [ ] `E1.1` `docs/08_implementation/12-version-compatibility-evidence.md`: rows 7 (postback) + 8 (Arrow REST) → VERIFIED (from A3/A4); row 9 (OpenObserve) → VERIFIED (from D7); row 10 (base images) → VERIFIED (pin-check).
- [ ] `E1.2` No `TO_BE_VERIFIED` row remains.
**DoD:** matrix fully VERIFIED.

### Task E2 — Full Monday gate green
- [ ] `E2.1` `make gate` (static + compose + go + java + schema/perf) exits 0; capture output to `logs/tracker-14/e2-monday-gate-<yyyymmdd>.log`.
- [ ] `E2.2` `make full-audit` + `make stale-tables` + `make pin-check` + `make cep-check-module` all exit 0 (update hardcoded test-count truth numbers if tests were added — see Gotchas).
**DoD:** every gate green.

### Task E3 — Release evidence package assembly
- [ ] `E3.1` Assemble per `docs/08_implementation/11-testing-and-release.md` final-release-evidence format: every acceptance ID (AC-*) mapped to an evidence artifact; `EvidenceRecord` entries for each.
- [ ] `E3.2` Produce `docs/08_implementation/RELEASE_EVIDENCE_2026-08-21.md`.
**DoD:** every required AC has an evidence pointer.

### Task E4 — Docs consistency reconciliation
- [ ] `E4.1` Run `make docs-audit`; fix any drift (status banners, table counts, ownership matrix). Update `docs/08_implementation/00-start-here.md` "Current readiness" table to reflect A–E completion.
**DoD:** docs-audit green; readiness table truthful.

### Task E5 — 🔒 DEC-044 single-operator release review + sign-off
- [ ] `E5.1` Review `RELEASE_EVIDENCE_2026-08-21.md` + `CHG-067` checklist.
- [ ] `E5.2` 🔒 Saurabh executes the single-operator approval (evidence hash + gate epoch) and records `CHG-079` (status flip `Blocked` → `Approved-for-Testing`, live money still gated).
- [ ] `E5.3` Agent flips the dossier status banners to `Approved-for-testing` after signature.
**DoD:** reviewed, signed, status flipped; live money remains blocked until a separate explicit go-live decision.

### Task E5b — Audit legal-hold / immutability evidence (Cloudflare R2)
**Why:** Money-path audit must be provably immutable and retrievable. On R2 the WORM mechanism is prefix bucket-locks (S3 Object Lock API is unsupported — outside our control); the API token for programmatic verification was never issued.
- [ ] `E5b.1` 🔒 Human creates a scoped Cloudflare API token (R2 read + retention admin).
- [ ] `E5b.2` Agent extends `code/01_platform/04_scripts/audit_r2.py` (or adds `r2_legal_hold_check.py`) to verify: prefix retention rules active on audit buckets, retrieval of a sampled object, hash-chain spot check against `AuditHashChain`.
- [ ] `E5b.3` Record limitation honestly: R2 bucket-locks ≠ S3 Object Lock; document residual risk in the CHG.
- [ ] `E5b.4` Evidence `logs/tracker-14/e5b-legalhold-<yyyymmdd>.md` + next free `CHG-*`.
**DoD:** retention rules verified programmatically; retrieval + integrity sampled; limitation documented.

### Task E5c — Missing named E2E fixture artifacts
**Why:** Several ingestion/network scenarios are named in `docs/02_requirements/09-acceptance-matrix.md` whose building blocks and unit tests exist, but the dated end-to-end evidence artifacts were never produced (audit finding B5).
- [ ] `E5c.1` Grep the acceptance matrix for rows still marked `EVIDENCE_BLOCKED` / `NOT_IMPLEMENTED` whose implementing tests exist green.
- [ ] `E5c.2` For each, run the env-gated integration test against the dev cluster and save the artifact under `logs/tracker-14/<test-id>-<yyyymmdd>/`.
- [ ] `E5c.3` Flip the matrix cell to point at the artifact; rerun `make full-audit`.
**DoD:** zero matrix rows remain blocked solely for lack of a produced artifact (any still-blocked row cites an external cause).

### Task E6 — Final verification
- [ ] `E6.1` Run: `make full-audit && make gate && make pin-check && make cep-check-module && make static-check` — all exit 0.
- [ ] `E6.2` `cd code && mvn -q test -pl common,02_services/01_ingestion,02_services/02_compute` green; `cargo test --offline` green; `go test -race ./...` green.
- [ ] `E6.3` Mark every checkbox above `[x]`.
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
- [ ] `PC.1` 🔒 Provision cloud VMs (D1) — human.
- [ ] `PC.2` 🔒 Provide broker sandbox + (later) real credentials (A) — human.
- [ ] `PC.3` 🔒 Premium-tier subscription decision (C5) — human.
- [ ] `PC.4` 🔒 Final go-live switch beyond Approved-for-Testing (E5) — human, separate decision.
- [ ] `PC.5` Broker-side contract drift (new API versions) — monitor via `make pin-check` + `version_matrix_verify.py`.

---

Created plan: `docs/plans/2026-08-21-live-readiness-master-plan.md`
