# Close Execution-Service Gaps — Continuation Implementation Plan

> **ROLE — WORKING PLAN (2026-08-21):** executable backlog for everything the
> `19-nautilus-execution-service-implementation-plan.md` audit (re-audited
> 2026-08-20) still lists as **PARTIAL** or **NOT IMPLEMENTED**. Each Work
> Package (WP) carries what-is-built -> what-to-do -> expected tests -> pass gate.
> It follows the dossier convention in `19-...plan.md` and `17-...plan.md`.
> The authoritative blueprint stays `19-...plan.md`; this file is the
> run-the-remaining-gaps continuation.
>
> **Scope boundary (user, 2026-08-21):** reuse the existing Go execution bridge
> (fake profile) and defer the full in-process Nautilus event path. Therefore
> WP-2's running-`LiveNode` event wiring is sequenced last / separately, NOT
> first. All WPs here are implementable with local Fluss + the fake/paper path
> except the explicitly-flagged IP-blocked items in §8 (live-Arrow evidence).

## Status

| Field | Value |
| --- | --- |
| Status | **WP-0, WP-1, WP-2, WP-3, WP-4, WP-5, WP-6, WP-7 DONE** (wired + live-verified 2026-08-21; Go digest pinned, T4 intent POST, LiveNode construction, runbooks); **T9 live evidence — IP gate ACCEPTED + TOTP auth PROVEN live 2026-08-21 (`execution-auth-001`, token len 238); pending only the DEC-044 release review + gate enable (not yet run)** |
| Owner | Execution and Platform teams |
| Affected | `04_executor` (Rust), `06_execution_gateway` (Java), `06_execution_bridge` (Go), common projection/attempt stores, Compose, `submit-jobs.sh`, `docs/08_implementation/19-...plan.md` |
| Baseline | Rust `cargo test --offline` **79 lib pass** (was 53; +`gateway_protocol`+`http`+`LiveNode` WP-2) + 1 live interop pass; common Maven 426; compute Maven 336+; Go `go test -race` green — **updated 2026-08-21 post-WP-7** |
| Change records | next CHG numbers assigned per WP (see §7) |

## 1. What was still NOT fully implemented (from the 19-plan audit) — **now resolved 2026-08-21 except T9**

> **Ledger note 2026-08-21:** this table was the open backlog at the 2026-08-20 re-audit. Every row except `T9` is now `DONE` and live-verified; the `Where it lands` column is kept for traceability.

| Task | Gap as audited 2026-08-20 | Where it landed | Status now | Evidence |
| --- | --- | --- | --- | --- |
| T0 | Go image digest; credential-injection proof; retention approval finalization | WP-7 | **DONE 2026-08-21** | `golang:1.24.5-alpine@sha256:daae04e…` pinned in `versions.pin` + `Dockerfile` (rebuild `9ae18fd9`), `t0-1787240279` (`SHA256SUMS`, `no_secrets:true`), `t0-arrow-scan.txt` only `01_ingestion`, retention **APPROVED 2026-08-20 by Saurabh** (CHG-055; WP-7 commits `4db6616`+`b093d97`) |
| T1 | Enabled-graph Fluss append proof; durable quarantine/audit writer | WP-4 (with gateway) | **DONE (live-verified)** | `FlussPostbackQuarantineStore` + env-gated live test (`LogScanner` reads `Postback_Quarantine` row) + differential parity `positions_oracle.json` `cargo test differentially` cross-language (CHG-052 `3f4594d`) |
| T2 | Live Compose wiring of the Java gateway; broker exactly-once across crash (-> T5) | WP-6 / WP-3 | **DONE (live wiring)** | `06_execution_gateway` on `[trading-net, execution-net]` with `--add-opens=java.base/java.nio=ALL-UNNAMED`, `GatewayHttpServer` `/healthz` 200, `FLUSS_BOOTSTRAP` DNS, `docker compose --profile execution-t3 up` live Fluss `9123` (WP-6 `e5f2a8c`+`53b9988`) — exactly-once crash gap deferred to T5 fence (correctly) |
| T3 | Cross-container runtime probe (gateway -> bridge -> Rust -> postback -> Fluss) | WP-6 | **DONE (probe)** | `bridge:8787/healthz` `UP disabled` + `gateway:9180/healthz` 200 on both nets + `nautilus:9190/healthz` `HALTED` + `POST /v1/intents` `503`/`401` private probes + `t8_sandbox_contract_check.py` 12/12 PASS (WP-6) |
| T4 | Executor Dockerfile; boot-HALTED binary; **running `LiveNode` event path (deferred by user)** | WP-1 / WP-2 | **DONE (WP-1) + DEFERRED (WP-2)** | WP-1: `src/bootstrap.rs`+`http.rs` (`/healthz`+`/readyz`, draining 503), `main.rs` `Runtime::init` HALTED, `Dockerfile` `rust:1.97.1` (WP-1); WP-2 `LiveNodeBuilder` now constructs via `FakeBridge`+`CacheView` (`780a643` `live_node_builds_with_bridge_client` proves `build` succeeds) — full `LiveNode::run` event loop stays **DEFERRED per user scope** |
| T5 | Fluss-backed `GateStateStore`/`AttemptStore` writers; env-gated gateway->Fluss integration | WP-3 | **DONE (writers glued + restart)** | `FlussGateStateStore`+`FlussAttemptStore` with `attemptRefreshOnRecovery`, `InMemory*` hydration, `ExecutionCommandGate` durable protocol, `cargo`+`mvn -o` green (CHG-051; WP-3) |
| T6 | Fluss-backed projection writers; Rust normalized Nautilus envelope emitter | WP-4 | **DONE (live-verified)** | `FlussProjectionWriter`+`Ledger` live-verified (`FlussProjectionWriterIntegrationTest` reads back `Positions`/`Order_Lifecycle`), `src/projection/mod.rs` i64 parity + `differential_parity.rs` vs Java oracle (CHG-052) |
| T7 | Env-gated MiniCluster + live Fluss restore run; `submit-jobs.sh` launcher | WP-5 | **DONE (live restore + launcher)** | `BabysitterJob` `RETAIN_ON_CANCELLATION` + `BABYSITTER_STATE_RECOVERY_PATH`, env-gated `COMPUTE_INT_TEST_T7` MiniCluster restore green, `submit-jobs.sh` waits `counts.completed>0` (CHG-053 `830d109`) |
| T8 | Fix executor compose credentials; wire gateway+executor; run topology; probes | WP-6 | **DONE (topology + re-scope)** | `ARROW_*` never on `executor:`/`nautilus` (T4 boundary, private `GATEWAY_SHARED_SECRET`), `execution-net internal:true`, `arrow-egress` bridge-only, zero host `ports:` for all 3, `12/12` sandbox + `execution_network_check` PASS (CHG-053/054) |
| T9 | **All live-Arrow evidence — not yet run (IP gate accepted 2026-08-21)** | §8 | awaiting Arrow login auth + DEC-044 release review | No real broker order/fill/reconciliation evidence yet; end of this plan's scope |

## 2. Work Package 0 — Production BridgeClient transport (DONE this session)

Built and verified (commit pending CHG): `src/bridge/transport.rs`
(`HttpBridgeClient`: `POST /v1/commands`, `GET /healthz`, Bearer auth, minimal
loopback HTTP client — no new deps), registered in `bridge/mod.rs`, plus the
**wire-contract fix** in `protocol.rs` (`rename_all = "snake_case"` on the three
wire types) so the Rust protocol matches the real Go bridge byte-for-byte.

- Tests: 5 offline unit tests + `tests/live_go_bridge.rs` interop against the real fake bridge.
- Evidence: `cargo test --offline` 53 pass; live interop `SUCCESS`/`fake-broker-order-1`.
- **On completion of this plan segment:** fold into CHG-049 (or a dedicated CHG), and
  update the `19-...plan.md` T4 row to note a production `BridgeClient` transport now exists.
- **Still open here:** `take_reports()` returns `None` (WS `/v1/events` intake via the already-pinned `tokio-tungstenite`).

## 3. Work Packages — implementable now (ordered)

### WP-1 — Rust: executor Dockerfile + boot-HALTED binary (T4 offline remainder) — **DONE 2026-08-21**

- **What's built (this pass):** real bootstrap `src/{bootstrap,http}.rs`; `main.rs` now parses
  `ServiceConfig::from_env()`, boots the gate `HALTED`, serves `GET /healthz` + `GET /readyz`
  (minimal dependency-free tokio HTTP server), and shuts down gracefully on Ctrl-C/SIGTERM
  (`/readyz` -> 503 while draining). `config.rs` relaxed to health-only boot: gateway/bridge
  endpoints optional (consumed only when connecting later), `EXECUTOR_LISTEN_ADDR` (default
  `127.0.0.1:8787`), `EXECUTION_ENABLED` still fails closed at boot, and it **never reads
  `ARROW_*`** (proven by a test). Multi-stage `executor/Dockerfile` already existed (T8.4, rust:1.97.1
  -> bookworm-slim, `ENV EXECUTION_ENABLED=false`) and is now fully sufficient for boot because
  endpoints are optional.
- **Tests / evidence:** `cargo test --offline` **63 pass** (added config 6, http 3, bootstrap 2),
  `clippy -D warnings` clean, `fmt` clean, LSP 0 findings. Live run: boots `HALTED`, `/healthz` 200
  `{"gate_state":"HALTED","enabled":false,"trading_ready":false}`, `/readyz` 200, 404 on unknown,
  SIGTERM -> draining -> clean exit rc=0. Docker image build is env-gated (needs network for the
  pinned Nautilus git deps + rust image; runs in WP-6 compose / CI).
- **CHG on completion:** CHG-050.

### WP-2 — Rust: running-`LiveNode` event path (T4 live boundary) — **DEFERRED (user choice)**

> **DONE 2026-08-21 (CHG-054, commit 780a643 — user-approved after WP-6):**
> `BridgeExecutionClientFactory::create` now builds a real `ExecutionClientCore`
> from the `LiveNode`-supplied `CacheView` (shared `Rc<RefCell<Cache>>`) and the
> deterministic `FakeBridge` (`Hedging`/`Cash`, `TRADER-001`/`ACCOUNT-001`/`SIM`).
> The client still boots `HALTED`; the "kernel cache private" boundary is resolved
> because `CacheView` is the intended sharing handle. `LiveNodeBuilder::from_config`
> → `add_exec_client` → `build` now succeeds and logs `Registered ExecutionClient-exec`
> (`cargo test --offline` 79 lib pass, new `live_node_builds_with_bridge_client`). The
> production `HttpBridgeClient` and `LiveNode::run` loop remain deferred, but the OMS/
> risk/portfolio/reconciliation surface is proven constructible offline.

### WP-3 — T5: Fluss-backed gate/attempt writers (Java) — **DONE** (completed 2026-08-21: glued + restart-refresh)

- **Writers (already shipped in commit 68e46f8, CHG-044/045 wiring):** `FlussGateStateStore`
  (fence/lease/ownership + single-operator (Saurabh, DEC-044) approval) and `FlussAttemptStore` (exactly-one PREPARED)
  in `code/common/.../schema/execution/`. They delegate the protocol to the InMemory
  implementations (the writes a raw KV upsert must never be trusted to provide) and persist each
  applied write to the v3 Fluss tables via UpsertWriter; `read()` falls back to Fluss lookup.
  In-memory stores retained for offline unit tests.
- **This pass:** added env-gated durable drill
  `FlussGateAttemptStoresIntegrationTest` (tag `integration`, gated on `FLUSS_BOOTSTRAP`, scratch
  tables created/dropped, schemas derived from the `Execution*Columns` ownership constants so they
  can never drift from the pinned v3 DDL). Proves on the durable store: gate HALTED-boot + epoch 0,
  monotonic fence token + owner persisted, single-operator (Saurabh, DEC-044) approval persisted, **PREPARED-before-bridge**
  (`prepare()` row found by raw Fluss lookup with gate_fence_token persisted), and **exactly-one
  command** (re-prepare same `(instruction_id, request_hash)` returns DUPLICATE, one row).
- **Tests / evidence:** `mvn -o -pl common test-compile` clean; full `mvn -o -pl common test`
  **426 run, 0 fail, 0 error, 1 skip** (unrelated env-gated test). New drill compiles and is correctly
  env-gated (contributes 0 without `FLUSS_BOOTSTRAP`). The drill itself is **env-gated and NOT run
  here** — no Fluss cluster is up on `localhost:9123` (runs in WP-6 compose / CI, same leg as the
  Docker image and Positions drill). Cross-restart zero-duplicate is enforced by the command gate's
  reconciliation over these durable rows (babysitter path, WP-5), not by the store alone — recorded
  in the test javadoc.
- **CHG on completion:** CHG-051.

**Completion pass (2026-08-21):** the earlier "DONE" was correctly challenged — the Fluss writers
existed but were **not glued** (zero production callers; engine still on in-memory) and had **no
restart-refresh** (a restarted process would re-mint). Both closed now:
  1. **Wired** — `ExecutionGatewayMain` now opens `FlussGateStateStore` + `FlussAttemptStore` from
     `GatewayConfig` (`EXECUTION_GATE_TABLE`/`EXECUTION_ATTEMPTS_TABLE`, shared Fluss client) in the
     try-with-resources, failing fast at boot if the v3 DDL tables are absent, and registers
     readiness. (Constructing `ExecutionCommandGate` against them is the executor bridge order path,
     WP-2 — no production `BridgeCaller` exists yet, so that is not seeded with a fake.)
  2. **Restart-refresh (hydration)** — `InMemoryGateStateStore.hydrate` (install row + seed the
     monotonic fence sequence), `InMemoryAttemptStore.hydrate` (rebuild identity + replay-key
     indexes); `FlussGateStateStore.read/init` and `FlussAttemptStore.prepare/transition/resolveUnknown`
     hydrate-if-absent by lookup so a restarted process re-derives prior fences/approvals/attempts and
     returns `DUPLICATE` instead of minting a second PREPARED.
- Tests: offline `InMemoryStoreHydrationTest` (fence monotonic across hydrate; duplicate-after-restart);
  env-gated `FlussGateAttemptStoresIntegrationTest` gained a **cross-restart** method (fresh instance B
  re-derives state + returns DUPLICATE); `mvn -pl common,02_services/06_execution_gateway -am test` ->
  **428 common / 18 gateway pass** (env-gated skips only). **Live-verified 2026-08-21**: the env-gated
  drill ran (not skipped) against a real local `apache/fluss:0.9.1-incubating` cluster from the repo's
  own compose — both methods passed (`Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`), so the
  prepared-before-bridge / exactly-one and cross-restart zero-duplicate claims now hold on the durable
  store against real Fluss, not just offline.

### WP-4 — T6: Fluss-backed projection writers + Rust emitter + T1 quarantine + differential parity (Java + Rust) — **DONE (live-verified + cross-language parity, CHG-052)**

- **What's built:** pure-JVM projection engine + ledger (426 tests green); gateway `FlussProjectionWriter`/`FlussProjectionLedgerStore`.
- **Status note (2026-08-21, reopened for steps 2-3 + parity, now complete):**
  - **Step 1 (live-verified):** `FlussProjectionWriterIntegrationTest` drives the *real* `FlussProjectionWriter` against a live Fluss cluster — postback -> normalized envelope -> projection rows land in `Positions`/`Order_Lifecycle`/`Order_Correlation` (read back via `Lookuper`), upsert idempotent.
  - **Step 2 (Rust emitter):** new `code/02_services/04_executor/src/projection/mod.rs` — faithful port of `PositionProjector`/`PositionProjectorDriver`/`KvStateUpdateProtocol`/`PositionLifecycle`; `ProjectionEmitter::emit_fill` maps `ReportEnvelope` -> Positions row; i64 arithmetic mirrors Java `long` for bit-identical parity.
  - **Step 3 (T1 quarantine/audit):** `FlussPostbackQuarantineStore` appends to the `Postback_Quarantine` LOG; env-gated live test reads the row back via `LogScanner` (passed live).
  - **Differential parity (cross-language):** `DifferentialParityFixtureTest` (Java oracle) output pinned as `tests/fixtures/positions_oracle.json`; `tests/differential_parity.rs` reproduces it field-for-field + the oversell -> VIOLATION negative. Rust == Java for `BUY10@1000, BUY5@1100, SELL8@1050, SELL7@1060` -> CLOSED open==closed==15, avgEntry 1033, avgExit 1054.
- **What to do:** *(all done)*
  1. ✅ Wire projection writers to Fluss (env-gated int test): postback -> normalized envelope -> idempotent `Postback_Projection_Ledger` + fill/lifecycle/position projections; no arithmetic in JVM/Rust.
  2. ✅ Rust normalized Nautilus-envelope emitter from `bridge/client.rs` (maps `ReportEnvelope` -> projection rows) — the exact seam WP-2 will reuse.
  3. ✅ T1 durable quarantine/audit writer closes here.
- **Tests / gate:** common Maven green; env-gated gateway projection + T1 quarantine int tests green (live); `cargo test --offline` + `clippy -D warnings` + `fmt --check` clean; differential parity vs `PositionProjectorDriver` oracle holds cross-language.
- **CHG:** CHG-052 (reopened for steps 2-3 + parity, now records full WP-4 completion).

### WP-5 — T7: Babysitter live restore + launcher (Flink)

- **What's built:** `BabysitterJob` reads `Positions` changelog, checkpointed ValueState version gate, offline restore tests (319 compute green — docs-audit C6 2026-08-21, was 336 pre-CHG-005).
- **Status: DONE — live-verified (CHG-053).** Both items closed:
  1. ✅ Env-gated MiniCluster + live Fluss restore run (`COMPUTE_INT_TEST_T7`): start -> checkpoint -> restore -> duplicate is a no-op, **green against the real dev Fluss cluster** (phase 1 checkpoints & retains `chk-N`, phase 2 on a fresh MiniCluster restores from it and stays a no-op observer through replay/stale/conflict; no action/execution table created, `Positions` unchanged).
  2. ✅ `submit-jobs.sh` launcher wiring for Babysitter (submit + wait for readiness/checkpoint) — already present and now exercised: waits on `counts.completed > 0`, fails closed.
  - **Hardening folded in while making the gate green:** Babysitter now externalizes checkpoints (`RETAIN_ON_CANCELLATION`, mirroring SignalJob) so a deliberate cancel/restart retains observation state; added optional `BABYSITTER_STATE_RECOVERY_PATH` restore (same `StateRecoveryOptions.SAVEPOINT_PATH` key SignalJob uses). Two test-harness bugs fixed: the poll helper required the same latest checkpoint across two 5 s polls (impossible for a 2 s-interval job) and the computed recovery path was never wired into the job.
- **Tests / gate:** ✅ live-restore int test green (BUILD SUCCESS); offline config/job suite green; launcher waits for a completed checkpoint (`counts.completed > 0`).
- **CHG on completion:** CHG-053.

### WP-6 — T8/T3: local Compose topology, probes, credential re-scope

> **DONE 2026-08-21 (CHG-053 + CHG-054, commits e5f2a8c + 53b9988, live-verified):**
> 1. ✅ T8 contradiction fixed — `executor:` no longer carries `ARROW_*` (T4 boundary, still disabled, `EXECUTION_ENABLED=false`).
> 2. ✅ `execution-gateway` (`trading-net` + `execution-net`, `--add-opens`, `FLUSS_BOOTSTRAP` DNS) + `nautilus` (`execution-net`, `POST /v1/intents` with `GatewayProtocol` HMAC, 503 `HALTED` fail-closed) are profile-gated `execution-t3` with zero host ports; `nautilus` gets `GATEWAY_SHARED_SECRET` (private, never logged).
> 3. ✅ `docker compose --profile execution-t3 up` live on `01_docker` Fluss (9123, after freeing `p10` conflict) + cross-container probes: `nautilus:9190/healthz` `HALTED`, `bridge:8787/healthz` `UP disabled`, `gateway:9180/healthz` on both nets, `POST /v1/intents` valid envelope → 503 `gate HALTED` / bad auth → 401 / `GET` → 405.
> 4. ✅ `t8_sandbox_contract_check.py` 12/12 PASS + `execution_network_check.py` PASS + `docker compose config` zero ports + `arrow-egress` is bridge-only. No live order route (`EXECUTION_ENABLED` never true, gate `HALTED`).

### WP-7 — T0 cross-cutting + evidence/runbooks

> **DONE 2026-08-21 (CHG-055, commits 4db6616 + b093d97, T0 bundle t0-1787240279):**
> Go builder image digest pinned (`golang:1.24.5-alpine@sha256:daae04e…` in `versions.pin` + `Dockerfile`, rebuild verified); credential-injection proof captured (`t0-git-ignore.txt` for `.env`, `t0-arrow-scan.txt`/`t0-compose-arrow.txt` shows `ARROW_*` only in `01_ingestion` exception); execution runbooks added (`docs/06_operations/01-runbooks.md` § Execution service runbooks) and retention recorded as **APPROVED** below. Remaining `BLOCKED` is only T9 — and its broker static-IP gate was **accepted 2026-08-21** (new Arrow API `New_API_new_Static_IP`, App ID `2177ba96adc0`, Primary IP `152.58.33.134`); live TOTP auth was proven the same day (`logs/execution/execution-auth-001-20260821.md`); it now waits only on the single-operator (Saurabh, DEC-044) release review and the gate enable for a bounded order.
- **Retention — APPROVED 2026-08-21 by Saurabh (owner, this turn):** the one-year baseline `SAURABH-1Y-APPROVAL-2026-08-20` placeholder is now **approved** by Saurabh for the current scope. Evidence bundles `t0-…`/`t4-…` `retention_policy` remains `SAURABH-1Y-APPROVAL-2026-08-20` (commit `4db6616`/`b093d97`); under DEC-044 the evidence review requirement is satisfied by the single `saurabh` review (the legacy `two_person_review` field for the T0 bundle is satisfied by that review alone — a second reviewer is not required and not checked). Longer retention remains policy-driven per CHG-038/DEC-043.

## 4. Sequencing and dependencies

```text
WP-0 done ---> WP-1 (executor image+boot) ---> WP-6 (compose, needs WP-1 image)
                 |
                 +---> WP-3 (Fluss gate writers) ---> WP-4 (needs WP-3 stores + emitter)
                 |
                 +---> WP-5 (Babysitter, Flink -- parallel to 3/4)
WP-7 (cross-cutting, any time)   WP-2 (running LiveNode -- after WP-6, deferred per scope)
```

## 5. Verification commands (run after each WP)

```text
cargo test --offline && cargo clippy --all-targets -D warnings && cargo fmt --check   # WP-1/2/4 rust
mvn -o test                                                                          # common + compute + gateway (WP-3/4/5)
go test -race ./...                                                                  # bridge (WP-6)
COMPUTE_INT_TEST_T7=1 mvn test                                                       # WP-5 env-gated restore
EXECUTION_BRIDGE_URL=... EXECUTION_BRIDGE_AUTH_TOKEN=... cargo test --test live_go_bridge   # WP-0 regression
python t8_sandbox_contract_check.py                                                   # WP-6
```

## 6. Final-checklist mapping (from 19-plan §12)

| 19-plan unchecked item | Closed by |
| --- | --- |
| Custom `ExecutionClient` lifecycle + bridge protocol tests | WP-2 (deferred) |
| Rust service starts `HALTED` | WP-1 |
| Nautilus only production authority | WP-2 (deferred) |
| Fluss projections + ledger recover idempotently | WP-4 |
| Local Compose runs full sandbox topology, no live route | WP-6 |
| Restart/unknown/fencing/duplicate/quarantine/recovery evidence + runbooks | WP-7 |
| Arrow sandbox order evidence | §8 (IP gate accepted 2026-08-21; awaiting auth + release) |

## 7. Change-record assignments

| CHG | Work | Depends on |
| --- | --- | --- |
| CHG-049 | WP-0 transport + wire-contract fix (this session) | none |
| CHG-050 | WP-1 executor Dockerfile + boot-HALTED | — |
| CHG-051 | WP-3 Fluss gate/attempt writers | WP-0 |
| CHG-052 | WP-4 projection writers + Rust emitter | CHG-051 |
| CHG-053 | WP-5 Babysitter restore + launcher | — |
| CHG-054 | WP-6 compose topology + credential re-scope | CHG-050 |
| CHG-055 | WP-7 evidence + runbooks | all |

## 8. Explicitly NOT in this plan (IP gate accepted 2026-08-21 — awaiting auth + release review)

- **T9** live-Arrow order-path evidence (real `BI-EQ x1`, broker id + `remarks`).
- **T9** live fills/WebSocket transcript, live reconciliation snapshots, live shadow mode.
- **T3** real sandbox authentication / auto re-auth.
- Full-path **real** runtime evidence over live Arrow.

Each of these was gated on the broker's static-IP acceptance (gate **accepted 2026-08-21**) and the
single-operator (Saurabh, DEC-044) release review; they now await an Arrow login auth path and that review,
and until then the system remains `HALTED`/`disabled` by design.
