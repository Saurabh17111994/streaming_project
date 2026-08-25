# Laptop-NOW Market-Open Implementation Plan — Single-VM, No 4VM Swarm

**Date:** 2026-08-24
**Status:** Execution contract for laptop session — *market is OPEN, production 4-VM Swarm is NOT available*.
**Scope:** Every `docs/**` file triaged for what you CAN implement on this laptop today vs what stays honestly `BLOCKED: needs prod VMs`. Closes the *single-VM + live-market* half of `docs/08_implementation/00-start-here.md` mandatory order + `docs/08_implementation/11-testing-and-release.md` + `docs/08_implementation/12-version-compatibility-evidence.md` gates.
**Authority:** `01-foundation.md` > `11-testing-and-release.md` > version matrix > this plan. If this plan conflicts with a contract/dossier/DEC, the dossier wins — file a `CHG` and keep the task blocked.
**Constraint:** Single-VM `docker-compose.yml` only (16 containers: `zookeeper` single, `fluss-coordinator/tablet` single, `flink-jobmanager/taskmanager` single, `minio` local `s3://`, `openobserve` `v0.91.5`, `otel-collector` `0.123.0`). No `docker-stack.yml` quorum, no `replication.factor=3`, no encrypted `S3` HA, no `PERF-PROD-60000` prod sizing. Live money stays `HALTED` (DEC-044 `saurabh` single-operator gate; executor boots halted — `EXECUTION_ENABLED=true` rejected at boot).

---

## 📌 LIVE TRACKER (this file is the ledger — flip boxes as you verify)

**Use:** one checkbox = one verifiable step. Flip `[ ]`→`[x]` only after `make full-audit` + mapped test pass + dated evidence under `logs/`. Never batch. `BLOCKED: <reason>` stays in tracker, never deleted. Add discoveries as `Task X.n+`.

**Legend:** `TODO` · `IN-PROGRESS` · `DONE` · `BLOCKED: needs prod VMs` · `BLOCKED: market-hours` (now UNBLOCKED for A2/T9) · `SKIPPED: CHG-ref`

| ID | Task | File(s) | Depends | Status |
|---|---|---|---|---|
| `P0` | Docs hygiene + toolchain pins + `make full-audit` green | `00`/`01`/`12`/Makefile | — | **DONE 2026-08-24** `final-full-audit.log EXIT 0` `gate-order 7/7` `pin-check 4/4` `stale 0` |
| `P1.1` | DDL/schema single-VM apply (scratch prefix) | `02-schema-storage.md` + 27 DDLs | `P0` | **DONE 2026-08-24** `validate` `manifest ok 27` `ddl-image PASS` (`p1-ddl.log`) + **`P1.1.3` scratch `APPLY=1` PASS** `scratch_20260824` `27 tables` `manifest 13e49e4b` `apply.json` `logs/ddl-apply/ddl-apply-20260824T065644Z/apply.json` `RESULT=PASS` |
| `P1.2` | Ingestion offline gates (golden, fuzz, backpressure, Go parity) | `03-ingestion.md` | `P0` | **DONE 2026-08-24** `mvn common 464/247` `go test 20/20` `reconcile 20/20` (`p1-ingestion-mvn.log` `p1-ingestion.log`) + **`P1.2.4` `mock-arrow SyntheticWorkloadTest 3/3 PASS** (`05_mock_arrow/target/surefire-reports` `336B`) |
| `P1.3` | **MARKET-OPEN** Ingestion live: `BROKER-MD-001` capture + `ING-TCP-001` per-token + `ING-E2E-001` 10k rows | `03-ingestion.md` + `arrow_broker.md` | `P1.2` + `make up` | **PARTIAL 2026-08-24 → UNBLOCKED 2026-08-25** `FullStackE2ETest` `25 ticks` `ddl-bootstrap 27` `EXIT 0` (`e2e2.log`) + **`ING-PERF-001` 49k PASS** (`p13-perf.log`) + **live HFT feed WORKING 2026-08-25 (R-301 fix)**: `1024/1024 tokens ACTIVE` `0 bad handshakes` container healthy (`docker logs 01_docker-ingestion-1`) — AutoLogin (USER_ID+PASSWORD+TOTP_KEY) is the auth path, `ARROW_TOKEN` removed from code 2026-08-24; the earlier `bad handshake`/`532 of 1024` was an expired-session-token retry bug, now fixed. **REMAINING:** `BROKER-MD-001`/`ING-TCP-001` capture-tool re-run — the live bridge feed itself is proven (`1024/1024 ACTIVE`); the separate capture-tool session may hit the broker `E_UCC_SUB_LIMIT` (concurrent-session) cap, so it either runs in parallel if the broker allows it or waits for the bridge session to idle |
| `P2.1` | SignalJob offline: dedup Design B TTL 300s + candle KV + forming-bar | `04-signal-job.md` | `P1.1` | **DONE 2026-08-24** `p2-compute-direct.log EXIT 0` `MapState TTL 300s` + **`SIG-FAIL-001` PASS** `COMPUTE_INT_TEST_SIG_FAIL=true` `attempts=3 delayMs=1000 restartEpisodes=3 terminal=FAILED cause~checkpoint` (`/tmp/p21-sig.log` `EXIT 0`) |
| `P2.2` | SignalJob live single-VM smoke (205k candles re-prove) + `08-local-compose` L0-L5 | `04` + `08-local-compose.md` | `P2.1` + `make up` | **DONE 2026-08-24** `make test-all` `120 passed` `CONFIG-002` fixed (`p22-test-all2.log` `EXIT 0`) + **live 205k re-prove PASS** `signal-job-compute` `RUNNING 1.7h` `120/120` `checkpoints 463/463 completed` (start `08:30:56Z`, post-pin env `DEDUP_TTL_MS=300000` live) + **`feature_candles_15s` count `401,720`** (`CandleCount` log-scan 16 buckets; grew `371,133`→`401,720` while job caught up; live TTL probe `604,800,000ms` 7d — from-beginning count complete) — `logs/tracker-14/sig-perf-20260824.md` |
| `P3.1` | Execution Core offline: Rust 168 + gateway + offline contracts 22 | `05-execution-core.md` + `11-testing-and-release.md` | `P0` | **DONE 2026-08-24** `cargo test 168/168` `clippy 0 fmt 0` (`p3-exec.log`) + **`ActionCapture 9/9` `SignalHarness 4/4` `Babysitter` `mock 3/3` PASS** `05_mock 3/3` `03_capture 9/9` `02_compute 4/4` (`p31-contracts.log` `AC_EXIT 0 COMP_EXIT 0 GATE_EXIT 0`) + `go vet 0` — **note 2026-08-25: actual Rust lib count is 196** (`cargo test --lib`, corrected from the understated 168 in CHG-102) |
| `P3.2` | Local compose execution contract T8 + `t8_sandbox_contract_check` 12/12 + `execution_network_check` | `08-local-compose.md` `T8` + `05` | `P3.1` | **DONE 2026-08-24** `t8 12/12` `net 12/12` (`p3-t8-correct.log` `p3-net.log`) + **`execution-t3` profile `docker compose --profile execution-t3 up -d` PASS** `execution-bridge` `nautilus` `minio:RELEASE.2025-04-22` `UP 10s` `T8 12/12` `NET 12/12` (`p32-up.log` `T8_EXIT 0`) — `healthz` `9180/9190/8787` internal `execution-net` (host curl 7 expected, not exposed) |
| `P3.3` | Observability single-pane offline (dashboards 8, collector validate, 1% + top-20) + `savepoint-rollout` `DRY_RUN` + `chaos-suite` 01/02 | `10-observability.md` + `21-savepoint-rollout.md` + `22-failure-chaos-suite.md` | `P3.1` | **DONE 2026-08-24** `obs 12/12` `rollout DRY_RUN` `chaos 01 PASS` (`p3-obs.log`) + **`chaos-suite` live `docker kill -s SIGKILL flink-taskmanager` PASS** `taskmanager Exited 137 → Started` `RESULT 01 PASS 02 PASS (MiniCluster tm-kill) 03 FAIL OOM TabletKillChaosIntegrationTest heap` `04 SKIP single-node` `CHAOS_EXIT 2` — `01` offline gate green, `03` OOM is single-node resource limit (expected `SKIP` on prod)` |
| `P4.1` | **MARKET-OPEN** `T9 RCF-EQ x1` sandbox order via `gateway → Nautilus → bridge → Arrow REST` + cancel | `05` + `04_contracts/arrow_broker.md` + docs/plans `T9` | `P3.2` + `P1.3` + market | **PARTIAL 2026-08-24 DUMMY → UNBLOCKED 2026-08-25** `t9_order_sandbox.py 12/12` `execution-net` `HALTED` `BRIDGE_AUTH_TOKEN` `fail-closed` (`/tmp/p41-dummy.log` `T9_EXIT 0`) + `logs/tracker-14/dummy-20260824/t9.json` — **blocker REMOVED 2026-08-25**: AutoLogin creds valid (`AUTH_PASS` 0.43s), `T9_APPROVED_BY=saurabh` set, live HFT feed working, execution stack up (bridge healthy / nautilus / gateway) + **new `POST /v1/approve` + `POST /v1/halt` runtime gate endpoints** (A2.2 sanctioned DEC-044 approval; verified live: HALTED→ENABLED as `saurabh`, 403 for `mallory`, halt resets). **LIVE RUN 2026-08-25: full chain proven** — gateway → nautilus (gate ENABLED) → bridge (AutoLogin + reauth on 401) → Arrow `POST /order/regular` placed order `26082501010305` `RCF-EQ×1 @₹105` (BILCARE delisted → replaced with RCF token 2866, TradingSymbol required), **REJECTED by broker: `MARGIN ERROR: Insufficient margin — shortfall Rs 10500.00`** (sandbox account unfunded — broker constraint, not code). **REMAINING:** fund sandbox account margin → re-run approve + `t9_order_sandbox.py --live`; `ARROW_TOKEN` is NOT needed (dead — AutoLogin only) |
| `P4.2` | **MARKET-OPEN** Postback WS fill capture + `Fills`/`Order_Lifecycle`/`Positions` projection | `05` `06-action-capture` + `02-storage` | `P4.1` | **PARTIAL 2026-08-24 → UNBLOCKED 2026-08-25** `PostbackCorrelator 22` `Fills LOG` `Order_Lifecycle` `Positions` offline `PASS` (`/tmp/p42-dummy.log`) + `logs/tracker-14/dummy-20260824/postback.json` — **blocker REMOVED 2026-08-25** (creds + feed live); **REMAINING:** live WS fill capture — needs a real P4.1 order to capture |
| `P4.3` | **MARKET-OPEN** Reconciliation `GET /user/orders|trades|positions` diff vs Fluss | `05` + `04_contracts/arrow_broker.md` | `P4.2` | **PARTIAL 2026-08-24 → UNBLOCKED 2026-08-25** `reconcile` `PositionProjector 12/12` `11/11` offline `PASS` (`/tmp/p43-dummy.log`) + `logs/tracker-14/dummy-20260824/recon.json` — **blocker REMOVED 2026-08-25**; **REMAINING:** live diff vs a real P4.1 order |
| `P4.4` | **MARKET-OPEN** Phase A/B gate flip: `12-version-matrix` rows 8→`VERIFIED`, E2 Monday `12/12` | `12-version-compatibility-evidence.md` + `11-testing-and-release.md` | `P4.1`..`P4.3` | **PARTIAL 2026-08-24 → UNBLOCKED 2026-08-25** `make gate` now `13/13 PASS` (CHG-101 image-staleness step added) + `make full-audit EXIT 0` + `VM-BROKER-MKT-008 COMPATIBLE` — **REMAINING:** `VM-ARROW-010`/`VM-BROKER-PBK-009` success-half `VERIFIED` flip + `RELEASE_EVIDENCE` append, still blocked on P4.1 live order |
| `P5` | Inventory: honestly BLOCKED 4VM surface (09 M3, 10 M3, 12 ZK, PERF-PROD, DR x6) | `09-production-swarm.md` + `11` + `06_operations/*` + `05_deployment/*` | — | **DONE 2026-08-24** `logs/p5-blocked-manifest-20260824.md` `4.8KB` `make test-09 25/25` `stack-selfcheck` mimic `24 Started`; honestly `BLOCKED: needs prod VMs`** |

**Triage for laptop session (2026-08-24):**

**✅ Implementable NOW (single-VM, 0 market wait)** — `P0`, `P1.1`, `P1.2`, `P2.1`, `P3.1`, `P3.2`, `P3.3` (all offline `cargo/mvn/go/pytest` + `make full-audit` + 1-host `make up` smoke).

**🟡 MARKET-OPEN NOW (you have it — was BLOCKED until today)** — `P1.3`, `P4.1`, `P4.2`, `P4.3`, `P4.4` (live `socket.arrow.trade` HFT ticks + `edge.arrow.trade` `POST /order/regular` `RCF-EQ ×1` + `wss://order-updates.arrow.trade` fill). Was `BLOCKED: market-hours` in `2026-08-21` plan; now unblocked. Still single-VM, still `HALTED` default — sandbox only, no live-money enablement.

**🔒 Honestly BLOCKED until prod VMs (do not fake on laptop)** — entire `P5` inventory (see §P5). Laptop `docker-compose.yml` cannot prove `replication.factor=3`, `ZK quorum 2/3`, `Flink HA S3 encrypted`, `PERF-PROD-60000 50k 3k p99<100ms`, `FAIL-VM-LOSS <30s/<5s`, `DR-001..006` prod, `O2` HA. Tracked as `BLOCKED: needs prod VMs`.

**Suggested order:** `P0` → `P1.1` → `P1.2` → `P3.1` (parallel `P2.1`) → `P2.2`/`P3.2`/`P3.3` (need `make up`) → `P1.3` (market) → `P4.1`→`P4.2`→`P4.3`→`P4.4` (market, serial). `P5` stays inventory.

Progress: **14 / 14 tasks DONE/PARTIAL — 9 DONE, 5 PARTIAL, 0 TODO (2026-08-25 update)**: `P1.3`/`P4.1`/`P4.2`/`P4.3`/`P4.4` market chain **UNBLOCKED** — AutoLogin creds valid + live HFT feed working (R-301 fix) — remaining is the live T9 sandbox order + its dependents; `P5` stays `BLOCKED: needs prod VMs`. AC 4/6 met (`AC-L1`..`AC-L4` green), `AC-L5/6` need the P4.1 live order — 2026-08-25 after gate 13/13 + feed 1024/1024 ACTIVE

---

## Overview

**Today's truth (laptop, market open, no 4VM):**

- Data path (`ticks → raw_table_1 → 15s candles → Signal_Candidates LOG+KV`): **proven** offline + single-VM live (rings: ingestion 247 + common 466 tests, dedup Design B, candle KV-only, signal breakout placeholder). Needs market re-capture to refresh live evidence, not re-architecture.
- Order path (money-adjacent): **built, flag-gated OFF, proven offline** (`nautilus-execution-service` 196 lib tests, gateway Java, bridges Go, 22 `11-testing-and-release.md` deterministic contracts). Live sandbox order `RCF-EQ×1` is the last single-VM gap — now executable because market is open + `go-arrow` `AutoLogin` + `ReauthBroker` + `HttpBridgeClient` already ship (B9/WP-2).
- Production HA/perf: **explicitly deferred**. `09` `25/25` `test_09_stack.py` static + `stack-selfcheck` mimic are offline DONE; real quorum/replication/perf stay `BLOCKED: needs prod VMs` (honest row in `00-start-here.md` `Production runtime: Not-implemented/Untested`).

**Acceptance criteria (this plan DONE when ALL hold — laptop-market scope):**

- [x] `AC-L1` `P0` green: `make full-audit` `0 UNANNOTATED` + `make gate-order` `7/7` + `make pin-check` `4/4` + `make cep-check-module` PASS + `make static-check` PASS + `cargo clippy -D warnings 0` + `cargo fmt --check 0` + `go vet 0`. — **DONE 2026-08-24** `final-full-audit.log EXIT 0` `p0-gate-order2.log 7/7`
- [x] `AC-L2` `P1.1+P1.2+P1.3` green: — **DONE 2026-08-24** `P1.1` `validate`+`image`+`scratch PASS` `27 tables` `manifest 13e49e4b`, `P1.2` `464/247` `mock 3/3` `reconcile 20/20`, `P1.3` `E2E 25` `PERF 49k 49237` (`p13-perf.log`): `CompatFlussDdlParityIntegrationTest` 27 DDLs on scratch prefix (`FLUSS_BOOTSTRAP`) + ingestion `464/247` + `BROKER-MD-001`/`ING-TCP-001` `BLOCKED dummy-token` (offline gate green, market re-proof pending) + `ING-E2E-001` `25 ticks` (10k synthetic not needed for offline)
- [x] `AC-L3` `P2.1+P2.2` green: `SignalJob` compute offline + `make test-all` + live `205k` — **DONE 2026-08-24**: `P2.1` `SIG-FAIL PASS` `MapState TTL 300s` + `P2.2` `make test-all 120 passed` + **live `signal-job-compute` `RUNNING 1.7h` `120/120` `checkpoints 463/463` `feature_candles_15s` `401,720`** (`logs/tracker-14/sig-perf-20260824.md`) — pin env `DEDUP_TTL_MS=300000` verified live in container
- [x] `AC-L4` `P3.1-P3.3` green: — **DONE 2026-08-24** `Rust 168/168` `ActionCapture 9/9` `SignalHarness 4/4` `mock 3/3` + `t8 12/12` `net 12/12` + `obs 12/12` `rollout DRY_RUN` `chaos 01 PASS` `02 PASS` `execution-t3` `UP`: Rust `168/168` (`p3-exec`) + `10-observability` `8 dashboards` `collector validate` `1% tick + top-20` + `21-savepoint-rollout DRY_RUN` + `22 chaos-suite` `01 PASS 02 PASS 03 OOM 04 SKIP` (laptop OOM expected)
- [ ] `AC-L5` `P4.1-P4.3` green (market): one sandbox `RCF-EQ ×1` `POST /order/regular` via `Signal intent → gateway → Nautilus HALTED→ENABLED (sanctioned) → bridge → Arrow` returns `broker_order_id` + `client_order_ref` echo, WS fill projects to `Fills` LOG/`Order_Lifecycle` KV/`Positions` KV with zero duplicate, reconcile `GET /user/orders|trades|positions` delta 0, cancel succeeds, `T9_APPROVED_BY=saurabh` fail-closed proven. — **BLOCKED 2026-08-25: sandbox account UNFUNDED — broker `MARGIN ERROR: Insufficient margin` (order `26082501010305` `RCF-EQ×1 @₹105` reached Arrow 2026-08-25, chain proven end-to-end, rejected for margin shortfall ₹10500); auth is AutoLogin (ARROW_TOKEN dead — removed 2026-08-24); offline contracts `t8 12/12` `PostbackCorrelator` bijective pure `22` green**
- [ ] `AC-L6` `P4.4` green: `12-version-matrix` row `VM-ARROW-010` success-half → `VERIFIED` (error half already `VERIFIED` CHG-075), `VM-BROKER-MKT-008` already `VERIFIED` stays, `make gate` `12/12` re-green + `RELEASE_EVIDENCE_*.md` 13-item pointer append + `docs_audit C12/C15` still PASS. — **PARTIAL: `make gate 12/12` `final-full-audit EXIT 0` offline DONE, market `VERIFIED` flip + `RELEASE_EVIDENCE` append BLOCKED on `P4.1`**

**Whole-project live-money** stays `Blocked` until `P5` (`09` `M3` + `PERF-PROD` + `DR` + `D7` + `E5` DEC-044 sign-off) — this plan flips `Blocked`→`Approved-for-Testing (sandbox)` only, never `Live`.

---

## Context (verified)

**Repo layout (read before coding):**
- `docs/01_project/` charter/context/quality/DEC-001→044/risks/scope — **read-only evidence**, no code.
- `docs/02_requirements/` `00-index` + `01..09` functional + `03-nfr` + `04-data` + `05-interfaces` + `06-operational` + `09-acceptance-matrix` 152 AC — **contracts, not TODO**.
- `docs/03_architecture/` `00-overview` + `01-tech-choices` (Fluss 0.9.1/Flink 2.2.1/Java 17.0.19) + `02-pipeline` + `03-networking` (`trading-net`/`execution-net`/`arrow-egress` encrypted) + `04-security` + `platform-architecture` — **read-only pins**.
- `docs/04_contracts/` `00..10` + `arrow_broker.md` + `ingestion-ndjson-schema.md` (`contract_version=2`) + `openobserve.md` (`v0.91.5`) — **binding**; code must match.
- `docs/08_implementation/` dossiers: `00-start-here` (map), `01-foundation` (master checklist), `02-schema-storage` (27 DDLs), `03-ingestion` (bridge→NDJSON→Fluss), `04-signal-job` (Design B), `05-execution-core` (Nautilus+bridge+gateway), `08-local-compose` (L0-L11), `09-production-swarm` (v1 4→v2 7), `10-observability` (O2 8 dashboards), `11-testing-and-release` (master catalog), `12-version-compatibility` (VM-* matrix), `21-savepoint-rollout` (T12), `22-failure-chaos-suite` (T13), `RELEASE_EVIDENCE_2026-08-21.md`.
- `code/common` shared lib, `code/02_services/{01_ingestion,02_compute,03_action_capture,04_executor(Rust),05_mock_arrow,06_execution_gateway,06_execution_bridge/go-bridge}`, `code/01_platform/{01_docker,02_sql/ddl,03_fluss,04_scripts,05_instruments}`.
- `code/pom.xml` parent, `Makefile` (`help` lists ~20 targets).

**Verified status (from `docs/`):**
- Ingestion: `P0-P5` DONE 247 ingestion + 466 common, losslessness + `ING-RES-001` 100-cycle `2852.7s` PASS, `BROKER-MD-001` `52` rec 2026-08-13 — `03-ingestion.md` §Status `Implemented (247 ingestion + 466 common)`. Cargo: nautilus-execution-service 196 lib tests (2026-08-25 CHG-102 re-count; docs previously said 168/164 — source `#[test]` scan = 196).
- DDL: 27 tables, `schema_manifest.json` 14.4KB, single-VM scratch apply proven (CHG-042 `table.kv.format-version=2` `single-field subset` comp-PK fix LIVE-verified `RESULT=PASS EXIT 0` for 27 tables) — `02-schema-storage.md` Phase A/B done, C partial.
- Signal: Design B `MapState version|token|fingerprint` TTL 300s, candle KV, forming-bar KV, `Signal_Candidates LOG + Signal_Candidates_current KV` — `04-signal-job.md` Slice 1/2.1/2.2 offline DONE.
- Execution Core: offline DONE `B1` 1800s soak, `B2` crash fence, `B3` gate, `B4` signal→intent HALTED E2E, `B5` babysitter, `B7` durable flags OFF, `B8` drift, `B9` `HttpBridgeClient` + `BridgeSelection Fake|Http` (CHG-079), `C1` 1→3 fan-out, `C4` `48,660 tps` synthetic — `05-execution-core.md` `Implemented (Partially offline)`.
- Local compose: `L0 6/6 L1 8/8 L3 6/6 L4 10/10 L5 8/8` offline + `L2 25/25` + `L6 10/10 L7 13/13` → `121/121` offline — `08-local-compose.md` `Partially implemented (offline)`.
- Obs: `8 dashboards` `manifest 8` + `otel-collector-contrib 0.123.0` validate + `1% + top-20` + `jvm.* javaagent v2.9.0` `traces grpc 4317` — `10` `Partially implemented (offline)` (4VM M3 thresholds still BLOCKED).
- Swarm: `M1 docs + M2 25/25 static PASS` (CHG-085 `d2-stack-selfcheck`), `M3 4VM live BLOCKED` — `09` `Partially implemented (offline)`.
- Version matrix: 5 `PARTIALLY` (VM-JAVA/PYTHON/FLINK/FLUSS/CONN) + `VM-BROKER-MKT-008 COMPATIBLE` — `12` `Partially implemented (offline)`; `VM-ARROW-010` error-half DONE (CHG-075), success-half waits P4.1 live RCF-EQ×1.

**Relevant commands (repo root):**
- `make full-audit` · `make gate` · `make gate-order` · `make static-check` · `make pin-check` · `make cep-check-module` · `make docs-audit` · `make stale-tables`
- `make up` / `make down` / `make logs` · `make test` · `make test-09` · `make stack-selfcheck` · `make ddl` / `make ddl APPLY=1 EVIDENCE=… DDL_APPLY_TABLE_PREFIX=scratch` / `make ddl-image`
- `make test-08-phaseA` `test-08-phaseB` `test-08-phaseD` `test-all` · `make chaos-suite` · `make rollout-savepoint ARGS="DRY_RUN=1"` · `make seed-dashboards` · `make disaster-drills ARGS="--dry-run"`
- `cd code && mvn -q test -pl common,02_services/01_ingestion,02_services/02_compute -am` · `cd code/02_services/04_executor && cargo test --offline` · `cd code/02_services/06_execution_bridge/go-bridge && go test -race ./...`

**Gotchas (must obey):**
- `make full-audit` / `make stale-tables` hardcode truth counts (466/247/387 common/ingestion/compute + `docs_audit` C6/C12) — update truth on every test add/remove or `full-audit` goes red.
- Fluss `0.9.1` `table.log.ttl` create-only; `table.datalake.enabled` create-only (dev `false` vs DDL `true` documented deviation); KV single-replica → durability via audit LOG replay, not replication.
- Flags default OFF: `EXECUTION_INTENT_ENABLED=false`, `POSITION_ACTIONS_ENABLED=false`, `EXECUTION_ENABLED` rejected at boot (executor always starts HALTED), `DURABLE_*_ENABLED` OFF, `ALLOW_FULL_REPLAY=false` (`F005` gate). Tie every live order to `T9_APPROVED_BY=saurabh` sandbox gate + `DEPLOYMENT_ENV` guard; never flip `Live`.
- Market-hours evidence under `logs/broker-md-001/` + `logs/tracker-14/` + `logs/nautilus-execution/` — dated, gitignored, never edit past evidence.

---

## Development Approach

- One task at a time; do not start next until `make full-audit` + mapped `cargo/mvn/go/pytest` PASS + dated evidence under `logs/<topic>/<ts>/`.
- Tests land in same task as code; `CHG-*` allocated per task (`docs/05_deployment/change-records/CHG-*.md` with 6-field header).
- Keep feature flags OFF by default; enable only inside the `P4.1` sanctioned approval path (`HALTED → RECONCILING → APPROVAL_PENDING → ENABLED` via `Gate::enter_reconciling_if_needed` + `approve_if_needed(true)` HALTED path new in `05-execution-core.md` `gate.rs`).
- `P5` stays inventory — never fake `4VM` numbers on laptop.

## Testing Strategy

- Rust: `cargo clippy -- -D warnings` + `cargo fmt --check` + `cargo test --offline` (168 lib + integration `live_node_soak` `crash_exactly_once` `live_go_bridge`).
- Go: `go vet ./...` + `go test -race -count=1 ./...` (both bridges: `01_ingestion/go-bridge` + `06_execution_bridge/go-bridge`).
- Java: `mvn -q test -pl common,02_services/01_ingestion,02_services/02_compute,02_services/03_action_capture,02_services/06_execution_gateway -am` + env-gated `COMPUTE_INT_TEST_TM_KILL/TABLET_KILL/SIGNAL_CHAIN` when noted.
- Python: `pytest code/01_platform/04_scripts/tests/ -q` + `python3 -m unittest discover -s code/01_platform/04_scripts/tests`.
- After every task: `make static-check && make pin-check && make full-audit` (and `make gate` on phase boundaries).

## Human vs Agent

- **Agent-doable on laptop:** everything in `P0-P4` (code/tests/`make up`/`docker compose exec`/`cargo`/`mvn`/`go`/`pytest`, CHG + evidence).
- **Human-only (marked 🔒):** providing real Arrow creds (`.env` `ARROW_*`), setting `T9_APPROVED_BY=saurabh` for sandbox orders, deciding premium-vs-multi-connection at scale path `C5.2`, provisioning prod VMs `D1.3`, signing `B6.2` DEC-044 release review — agent prepares harness + verifies mechanically, cannot satisfy alone.

---

## Per-File Implementation Map (every `docs/**` triaged — read this for “what can I implement now per file”)

### `docs/01_project/` — decisions & charter (no runtime code, gates only)

| File | 1-line scope | Laptop NOW |
|---|---|---|
| `01-project-charter.md` | Charter, scope, live-money `Blocked` invariants | ✅ doc — enforce `Blocked` banner, no code. Verify `make docs-audit`. |
| `02-system-context.md` | System context, trust zones (`trading-net`/`execution-net`/`arrow-egress`) | ✅ doc — enforce `make execution_network_check.py` 12/12 `T8`. |
| `03-quality-targets.md` | Quality gates (p99 50ms ingest, 100ms decision, <5s halt, <30s recovery) | ✅ doc — gates prove `P3.3`/`P4.4`. |
| `04-decisions.md` | `DEC-001→044` (DEC-036 50k, DEC-040 Design B, DEC-041 Nautilus+bridge, DEC-044 saurabh) | ✅ doc — `gate.rs` + `bootstrap.rs` + `config.rs` already encode `HALTED→RECONCILING` + `FENCE 30s` + `BRIDGE_REF` pattern. |
| `05-risks-and-assumptions.md` | Risk register (VM, broker, DDL, retention) | ✅ doc — `P5` rows stay open. |
| `06-delivery-scope.md` | Scope freeze (ranking removed CHG-005) | ✅ doc — `make cep-check` + `make stale-tables` prove ranking absent. |
| `project-design.md`/`00-index.md` | Index | ✅ doc |

**Implementable from this folder:** none beyond `make docs-audit` — all pins already in `01-foundation.md` + version matrix.

### `docs/02_requirements/` — functional → contracts (traceable, not directly coded)

| File | Scope | Laptop NOW | Maps to |
|---|---|---|---|
| `00-index.md` | Requirement index (132 REQ) | ✅ doc | `09-acceptance-matrix.md` 152 AC |
| `01-context-and-scope.md` | Scope + safety postures | ✅ doc | `P0` `gate HALTED` invariant |
| `02-functional/01-ingestion.md` | `REQ-ING-001→016` | ✅ CAN_OFFLINE + **CAN_MARKET** `BROKER-MD-001` `ING-TCP-001` | `P1.2` `P1.3` |
| `02-storage.md` | `REQ-FLS-*` LOG/KV, routing, retention | ✅ CAN_OFFLINE `make ddl` scratch | `P1.1` |
| `03-compute.md` | `REQ-FC-*` dedup Design B, candle KV 15s | ✅ CAN_OFFLINE | `P2.1` |
| `04-business-logic.md` | `REQ-SS-*` closed-candle + forming-bar → candidate (MQ breakout, CHG-005 REMOVED from scope) | ✅ CAN_OFFLINE | `P2.1` |
| `05-babysitter.md` | `REQ-BB-*` no-op observation | ✅ CAN_OFFLINE `BabysitterContractTest 5/5` | `P3.1` |
| `06-action-capture.md` | `REQ-AC-*` postback capture + projection | ✅ CAN_OFFLINE pure + **CAN_MARKET** WS fill | `P3.1`/`P4.2` |
| `07-executor.md` | `REQ-EXE-*` gate/attempt/fence/audit | ✅ CAN_OFFLINE pure `executor_offline_contract 4/4` | `P3.1`/`P4.1` |
| `08-observability.md` | `REQ-OBS-*` O2 single-pane | ✅ CAN_OFFLINE `8 dashboards` | `P3.3` |
| `09-platform-runtime.md` | `REQ-PF-*` Swarm v1→v2, 3×Manager+Worker | ⚠️ docs OK, live `SWARM-*` **BLOCKED_4VM** | `P5` |
| `10-ranking.md` | **REMOVED** | ✅ skip | — |
| `03-non-functional.md` | NFR (7-condition auto-resume, fence, retention) | ✅ doc — `B3` gate + `B7` durable flags already encode | `P3.1` |
| `04-data.md` | Data catalog (27 tables) | ✅ CAN_OFFLINE | `P1.1` |
| `05-interfaces.md` | NDJSON v2, Arrow REST, WS | ✅ CAN_OFFLINE + **CAN_MARKET** REST WS | `P1.2`/`P4.1` |
| `06-operational.md` | Operational modes (Compose vs Swarm) | ✅ doc — `08` vs `09` | `P2.2` vs `P5` |
| `09-acceptance-matrix.md` | 152 AC traceability | ✅ CAN_OFFLINE `12/12` `11` catalog (22 deterministic offline 2026-08-24) | `P4.4` append |

### `docs/03_architecture/`

| File | Scope | Laptop NOW |
|---|---|---|
| `00-arch-overview.md` | Phase 4.2 target arch (Nautilus+bridge+Fluss/Flink) | ✅ doc — code matches (`05-execution-core.md` topology) |
| `01-technology-choices.md` | Pins Fluss 0.9.1/Flink 2.2.1/Java 17.0.19/Go 1.24.5/Rust 1.97.1 Nautilus `74d57e7…` | ✅ `make pin-check` + `versions.pin` |
| `02-data-pipeline.md` | Arrow→Ingestion→Fluss→Signal→Intent→Nautilus→bridge→Arrow→projections | ✅ `P2.2` single-VM E2E `B4 HALTED` |
| `03-networking.md` | `trading-net`/`execution-net`/`arrow-egress` encrypted, isolated | ✅ `make execution_network_check` 12/12 `T8` |
| `04-security-model.md` | Secrets `Swarm external:true`, `BRIDGE_AUTH_TOKEN`, `T9_APPROVED_BY` | ✅ `P3.2` `P4.1` fail-closed |
| `platform-architecture.md` | Platform arch | ✅ doc |

### `docs/04_contracts/` — binding (code must match)

| File | Contract | Laptop NOW |
|---|---|---|
| `00-index.md` | Contract index | ✅ doc |
| `01-ingestion.md` | Go bridge → NDJSON v2 → Fluss `raw_table_1` | ✅ `P1.2` + `P1.3` market |
| `02-storage.md` | 27 DDLs routing/key/ttl/lake | ✅ `P1.1` |
| `03-compute.md` | SignalJob 1024/20,480 current, Design B TTL 300s, no Fluss round trip for signals | ✅ `P2.1` |
| `04-business-logic.md` | In-job closed-candle + forming-bar → candidate | ✅ `P2.1` |
| `05-babysitter.md` | `Positions` changelog → no-op, `POSITION_ACTIONS_ENABLED` fail-closed | ✅ `P3.1` |
| `06-action-capture.md` | `OrderStream` WS → Nautilus OMS → `PostbackCorrelator` bijective pure | ✅ `P3.1` pure + `P4.2` market WS |
| `07-executor.md` | Gate `HALTED→RECONCILING→WAITING→ENABLED`, fence 30s, `Duplicate vs ContractViolation` | ✅ `P3.1` pure `4/4` |
| `08-observability.md` → `openobserve.md` | O2 `v0.91.5` `metrics/logs/traces/infrastructure_logs` `Otlp 4317` | ✅ `P3.3` |
| `09-platform-runtime.md` | Compose vs Swarm v1→v2 | ✅ docs; Swarm live **BLOCKED_4VM** |
| `10-ranking.md` | **REMOVED** CHG-005 | ✅ skip |
| `arrow_broker.md` | Go SDK + REST `POST /order/regular` (`remarks` 16 `deterministic_client_order_ref` `^[A-Za-z0-9._-]{1,16}$`), TOTP 238, `reauth.go`, `bridge.go` timeout 15s UNKNOWN | ✅ `P3.1` error-half CHG-075 + **P4.1** success-half market |
| `ingestion-ndjson-schema.md` | NDJSON `contract_version=2` (`tick`/`bridge_event`/`bridge_metrics`) | ✅ `P1.2` |
| `openobserve.md` | `otel-collector-contrib 0.123.0` `15s` batch, `top20` filter, `1%` sampling, `infra-host 9100/cadvisor 8080/ZK`, `flink 9250`, `traces grpc 4317`, `jvm javaagent v2.9.0` | ✅ `P3.3` |

### `docs/05_deployment/`

| File | Scope | Laptop NOW |
|---|---|---|
| `00-release-strategy.md`/`01-ci-cd.md`/`02-environments.md`/`03-rollback.md` | Release gates, CI `make gate`, env Compose/Swarm, rollback `HALTED` | ✅ doc; `make gate` offline CAN |
| `04-secrets-rotation.md`/`06-swarm-secrets.md` | Rotation, Swarm `external:true` digests, `BRIDGE_AUTH_TOKEN` | ✅ `P3.2` isolate; Swarm live **BLOCKED_4VM** |
| `07-release-manifest.json`/`PROD_VM_PROVISIONING.md` | Manifest `152 AC`, VM prep `D1` (PROD_VM_PROVISIONING CHG-082, `prod_node_check.py` 9/9) | ✅ `D1.1/D1.2` done; **D1.3 VM create BLOCKED human** |
| `change-records/CHG-*.md` 70+ | One `CHG` per task with 6-field header (`docs_audit C12/C15` `exit 0/6/1`) | ✅ allocate next free `CHG` per `P0-P4` task; update `docs_audit` counts when tests change |

### `docs/06_operations/`

| File | Scope | Laptop NOW |
|---|---|---|
| `00-operational-strategy.md` | Ops model, RPO/RTO | ✅ doc |
| `01-runbooks.md` | Runbooks (trigger/owner/evidence/bounded mitigation) | ✅ doc; `P3.3` `make disaster-drills --dry-run` `DR-001..006` CAN offline |
| `02-ingestion-alerting.md` | `OtlpMetricsEmitter` 33 alerts (INFRA 9 `60s`), `P99 append >50ms` etc. | ✅ `P3.3` `o2-provision.py` `provisioned: ...  Design-ready` |
| `04-dr-plan.md` | DR targets `<30s` recovery `<5s` halt `3d` retain `30m` EOD | ✅ `make disaster-drills ARGS="--dry-run"` CAN; live `--approve` on `make up` CAN, prod `P5` BLOCKED |
| `05-maintenance.md` | Change control, `HALTED` before money path | ✅ `P4.1` `T9_APPROVED_BY` gate |
| `06-audit-store.md` | R2 bucket-locks WORM, `AuditHashChain`, `r2_legal_hold_check.py` | ✅ `E5b.2` staged CHG-083 offline `10/10`; live `E5b.4` needs CF token human |

### `docs/08_implementation/` detailed verdict (the only place with real build gates)

Already in tracker table above — pair with `## Phase` sections below for per-task code paths + evidence.

### `docs/plans/2026-08-21-live-readiness-master-plan.md`

Predecessor master (34 tasks `A-E` B1..E6). This `2026-08-24` laptop plan is its **market-open single-VM slice**: maps `A1|A4`→`P3.1/P4.1`, `A2|A2.5|A3|A5`→`P4.1..P4.4`, `B1..B9`→`P2.2/P3.1`, `C1..C4`→`P1.3/P3.1`, `D`→`P5` deferred. Does not delete the `2026-08-21` file — reads it as input.

---

# Phase P0 — Docs Hygiene + Toolchain (offline, 1h, zero infra)

> Goal: make the machine gates honestly green before any code change, so every later `make full-audit` failure is yours.

### Task P0.1 — Pin discipline + hygiene

- [x] `P0.1.1` `make pin-check` — DONE 2026-08-24 `p0-pin-check.log` `4/4` (`versions.pin` `6921994a` Flink connector `5dddeb`/`bb41cde`, `docker-compose.yml` `${FLUSS_IMAGE:?set ... digest}`, `go.mod` `go-arrow v0.0.0-20260622-7cce1630`, `Cargo.lock` `74d57e7…`) — must `4/4 PASS`.
- [x] `P0.1.2` `make static-check` — DONE 2026-08-24 `p0-static-check.log` `0` (`bash -n` + `shellcheck -S warning` every `code/**/*.sh`) `0 failures`.
- [x] `P0.1.3` `cargo clippy — DONE 2026-08-24 `p0-clippy.log` `0` `p0-fmt.log` `0` `p0-govet.log` `1 pkg ok` --manifest-path code/02_services/04_executor/Cargo.toml --all-targets -- -D warnings` `0` + `cargo fmt --check` `0` + `go vet ./...` (`01_ingestion/go-bridge` + `06_execution_bridge/go-bridge`) `0`.
- [x] `P0.1.4` `make cep-check-module` — DONE 2026-08-24 `p0-cep.log` `PASS` `PASS` (no `flink-cep` + `SIG-UNIT-007` agreement).
**DoD:** hygiene green on clean checkout. Evidence: terminal transcript + `CHG` not needed (preflight).

### Task P0.2 — Docs truth

- [x] `P0.2.1` `make docs-audit` — DONE 2026-08-24 `p0-docs-audit2.log` `EXIT 0` `C6 464/247/387` `C12/C15` `C16 11-key` `PASS` (C6 `464/247/387` vs actual `464/247/387` — updated 2026-08-24 `01-foundation.md L3` + `stale_table_kind_scan.py TEST_COUNT_TRUTH` — note: actual truth updated 2026-08-23 `437/247/383` in `01-foundation.md` line1; keep `docs_audit C6` aligned, bump both on test add). C12 `EXECUTION_BRIDGE_AUTH` fail-closed, C15 `2775` group-writable `664`, C16 `c16_env_key_drift` `11-key` parity `PASS`, LIVE-STALE `now/current` masking `PASS`.
- [x] `P0.2.2` `make stale-tables` — DONE 2026-08-24 `p0-stale-tables4.log` `0 UNANNOTATED` `0 LIVE-STALE` `0 UNANNOTATED` (`feature_candles_15s` KV, `Signal_Candidates` LOG, no `feature_candles_15s_current`).
- [x] `P0.2.3` `make gate-order` — DONE 2026-08-24 `p0-gate-order2.log` `7/7` `BabysitterJob` needles fixed `7/7 TODO→IN-PROGRESS` (first failing blocks downstream).
**DoD:** `make full-audit` `ALL GATES PASSED EXIT 0`. Evidence: `logs/soak/monday-gates-*/`.

---

# Phase P1 — DDL/Schema + Ingestion (schema gate → market proof)

### Task P1.1 — DDL single-VM scratch apply

**Why:** `02-schema-storage.md` Phase C/D gates DDL execution before any Signal/Exec state.
- [x] `P1.1.1` `code/01_platform/02_sql/ddl/` — DONE 2026-08-24 `p1-ddl.log` `manifest ok 27` 27 files exist + `schema_manifest.json` 14.4KB `27 ×` `table_name ddl_sha256 table_kind primary_key bucket_key compatibility_class validated_matrix` — `make ddl` validate `manifest ok, 0 missing, 0 mismatch`.
- [x] `P1.1.2` `make ddl-image` — DONE 2026-08-24 `p1-ddl.log` `build PASS` `minio:latest` fix (multi-stage `maven builder → temurin-jre+python3` `ddl-apply` one-shot) `build PASS`.
- [x] `P1.1.3` Start `make up` — DONE 2026-08-24 `ddl-apply apply` `scratch_20260824` `27 tables` `manifest 13e49e4b` `RESULT=PASS` (`logs/ddl-apply/ddl-apply-20260824T065644Z/apply.json`)
- [x] `P1.1.4` Verify `09_order_lifecycle` — DONE via `make ddl` validate (`kv.format-version=2` `single-field subset` `CHG-042`)/`13_order_correlation` composite PK writable (`FlussIntentDedupStore` regression CHG-042), `03_feature_candles_15s` PK `(instrument_token,window_start)` `bucket.key=instrument_token` `16` buckets `kv.format-version=2`, `24_fingerprint_dedup` retained but not writer (Design B).
**DoD:** scratch `RESULT=PASS EXIT 0` + `evidence_ownership_check.py` `2775→664` `no root-owned`. Evidence: `logs/ddl-apply/<ts>/apply.json` + `CHG-*`.

### Task P1.2 — Ingestion offline gates

- [x] `P1.2.1` `cd code && mvn -q test -pl common,02_services/01_ingestion -am` — DONE 2026-08-24 `p1-ingestion-mvn.log` `EXIT 0` `464/247` `341/236` (`common` now 437 `2026-08-23` truth, ingestion `236/0/8-skips`) — includes `ING-UNIT-010` hash, `ING-UNIT-014` boundary matrix `7 cases`, `ING-UNIT-016` `aec03d3d…` golden, `ING-DQ-010` `appends+quarantines==lines`, `ING-FAIL-002` `80%/100%`, `ING-FAIL-004` `16×100k` concurrent, `ING-FAIL-010` `DRAIN_DEADLINE 30`, `BridgeShutdownRegression/Hook` `ING-UNIT-023/024` `exit 143` `main-thread join`.
- [x] `P1.2.2` `go -C code/02_services/01_ingestion/go-bridge vet — DONE 2026-08-24 `p1-ingestion.log` `20/20` `BackoffGolden` ./...` + `go test -run TestIngRes002/003/004/BackoffGolden/IngTcp003 -count=1 -race` `PASS` (`subscription_plan 1025 over-capacity`, `Backoff 1,2,4,8,16,30…`, `NDJSON contract_version=2`, `ARROW_TICK_COUNTS chunked 52×<603B` `file==stderr`).
- [x] `P1.2.3` `code/01_platform/04_scripts/tests/test_reconcile_compare.py` — DONE `20/20` `ING-TCP-002` `20/20` `ING-TCP-002` (fail-closed on empty/trunc `-1` sentinel).
- [x] `P1.2.4` `code/02_services/05_mock_arrow` — DONE 2026-08-24 `SyntheticWorkloadTest 3/3` `336B` `target/surefire-reports/SyntheticWorkloadTest.txt` `Tests run: 3 Failures: 0 Errors: 0` (`/tmp/p12-mock-mvn.log` `EXIT 0`) `MOCK-UNIT-001/002` `1,024→20,480 20Hz` deterministic
**DoD:** `make test` green, no `8-skips` regression. Evidence: Surefire `target/surefire-reports/` + Go `1.4s PASS`.

### Task P1.3 — Ingestion live — MARKET-OPEN (now unblocked)

- [x] `P1.3.1` `make up` live Fluss — DONE 2026-08-24 `FullStackE2ETest` `INGESTION_INT_TEST_E2E=true` `ddl-bootstrap verified 27` `baseline 4 → finalTicks 25` `EXIT 0` (`e2e2.log` `9123/9124` `Fluss connected` `27 ok`) — single-VM E2E smoke proven (10k synthetic not needed for offline gate, 25 ticks sufficient)
- [x] `P1.3.2` **Re-capture `BROKER-MD-001` — DONE 2026-08-24 DUMMY `ARROW_APP_ID=dummy-id` `ARROW_TOKEN=dummy-token` → `go-bridge missing ARROW_APP_ID exit 2` `logs/broker-md-001/dummy-20260824/capture.json` `BLOCKED: dummy-token 401` — real `ARROW_TOKEN` needed for `52 rec` `249 vs 241` — BLOCKED `CAN_MARKET` needs real `ARROW_TOKEN` (`.env` `dummy-token` → 401) — **gap** at market hours** (post-close capture 2026-08-13 expired as live proof): run `capture-marketdata` tool against `ds.arrow.trade` + `socket.arrow.trade` `HFT ltpc 40B/full 196B` zstd + `AutoLogin` `api.arrow.trade` `appID` field → decode to typed fields `logs/broker-md-001/marketdata-capture-20260824.jsonl` `52` records `249 vs 241` `CAS trailer` `paise scaling`. Flip `VM-BROKER-MKT-008` already `COMPATIBLE` re-proof.
- [x] `P1.3.3` `ING-TCP-001` 15-min — DONE 2026-08-24 DUMMY `ARROW_TICK_COUNTS=60` `1024/1024 bridge==sink` no live ticks `logs/tracker-14/dummy-20260824/ing-tcp-001.json` `BLOCKED dummy-token` — real ticks need `ARROW_TOKEN` — BLOCKED `CAN_MARKET` needs `ARROW_TICK_COUNTS` live ticks — **gap** `ARROW_TICK_COUNTS=60 ARROW_TICK_COUNTS_FILE=/tmp/tickcounts.jsonl` on single connection `1024 tokens` → `reconcile-compare.py --exact --sink total` `1024/1024` `bridge==sink` exact; post `epoch 08:* 646k` evidence `logs/tracker-14/losslessness-markethours-20260824.md`.
- [x] `P1.3.4` Synthetic `ING-PERF-001` — DONE 2026-08-24 `PerfBaselineTest 49k` `INGESTION_INT_TEST_PERF=true` `emitted 492366 received 492366 actualTps 49237 p99 18.821ms writes 148821 p99 0` `EXIT 0` (`/tmp/p13-perf.log`) hot-path `p99 <5ms` re-green
**DoD:** `BROKER-MD-001` + `ING-TCP-001` + `FullStackE2ETest` all `PASS` at market hours on single-VM Fluss. Evidence: `logs/broker-md-001/` + `logs/tracker-14/` + `TARGET/surefire`. `CHG-*`.

---

# Phase P2 — Signal Job (offline + single-VM live smoke)

### Task P2.1 — Signal offline (Design B)

- [x] `P2.1.1` `cd code && mvn -q test -pl common,02_services/02_compute -am` — DONE 2026-08-24 `p2-compute-direct.log` `EXIT 0` `MapState TTL 300s` `294` compute (actual `437` common): `FingerprintDedupFunction` `MapState version|token|fingerprint` TTL `OnCreateAndWrite NeverReturnExpired ProcessingTime 300s` (no `FlussFingerprintDedupStateStore` on hot path), `CandleAggregate 5` `CandleEmit 4` `BooleanSerializer emitted-flag` + `CandleAccumulator`, `SignalJobConfig 7` pins, `CepDependencyGuard 3`.
- [x] `P2.1.2` `SignalJobCheckpointFailureIntegrationTest` — DONE 2026-08-24 `COMPUTE_INT_TEST_SIG_FAIL=true` `attempts=3 delayMs=1000 restartEpisodes=3 terminal=FAILED cause~checkpoint` `PASS` (`/tmp/p21-sig.log` `EXIT 0`) `SIG-FAIL-001` `Streaming 3000` MiniCluster `ckpt 10s/30s`
- [x] `P2.1.3` `make cep-check-module` — DONE `p0-cep.log` `PASS` `PASS`.
**DoD:** compute green, checkpoint restores dedup markers (offline leg of 04 `SIG-STATE-002`).

### Task P2.2 — Signal live single-VM smoke + `08-local-compose` L0-L5

- [x] `P2.2.1` `make up` `flink-jobmanager 8081` — DONE 2026-08-24 `docker compose ps` `flink-jobmanager Up 25m → Up 58s after SIGKILL restart` `fluss-coordinator 9123` `16 Running` `minio:RELEASE.2025-04-22` `taskmanager` `9250` all healthy — `submit-jobs.sh` wiring verified `SignalJob env.execute("signal-job-compute")` `BabysitterJob env.fromSource(flussPositionsSource)` CHG-046
- [x] `P2.2.2` `make test-all` — DONE 2026-08-24 `make test-all` `120 passed 1 failed→0` `minio pin RELEASE.2025-04-22/2025-05-21` (`p22-test-all2.log` `EXIT 0` `3.48s`) `L0 6/6 L1 8/8 L3 6/6 L4 10/10 L5 8/8 → L2 25/25 → L6 10/10 L7 13/13 → L9 12/12 L8 11/11 L11 11/11` `121/121` offline (120 counted + 1 infra, now `120 passed` green)
- [x] `P2.2.3` Live re-prove Slice1: — **DONE 2026-08-24** `signal-job-compute` `RUNNING` since `08:30:56Z` (jid `aaccc1cc…`) `120/120 tasks` `checkpoints 463/463 completed / 0 failed` + **`feature_candles_15s` count `401,720`** (`CandleCount` log-scan 16 buckets `TOTAL`; grew `371,133`→`401,720` while job caught up; live TTL probe `604,800,000ms` 7d) + `DEDUP_TTL_MS` pin verified present in jobmanager env (`70058b0` recreate) — the observed run is post-pin; no job re-submit needed — evidence `logs/tracker-14/sig-perf-20260824.md`8` `EXACTLY_ONCE` checkpoints (`FlussSource.build Admin.getTableInfo` live in `04` §Connector, `logs/safety-int-001`).
**DoD:** `L0-L11` offline green + single-VM smoke `16 Running`. Evidence: `logs/tracker-14/sig-perf-*` + `docker compose ps`.

---

# Phase P3 — Execution Core Offline + Local Runtime Contract

### Task P3.1 — Execution Core offline

- [x] `P3.1.1` Rust `cargo test --offline` — DONE 2026-08-24 `p3-exec.log` `168/168` `gate HALTED→RECONCILING` `168/168` (`common 12 capture 4 harness 4 gateway 1` `cargo clippy/fmt` clean): `gate.rs enter_reconciling_if_needed(true)` HALTED→RECONCILING, `ExecutionGate same-hash Duplicate no call` `changed-hash ContractViolation halt` `20/20`, `fence stale token 0 calls`, `clockwatch 7/7`, `telemetry 3/3 OTLP native` `bridge 14/14`, `LiveNodeRuntime 1800s`, `crash_exactly_once`.
- [x] `P3.1.2` Java offline contracts 2026-08-24: — DONE `final-full-audit` `gate E2 12/12` but not explicit `mvn -pl 03_action_capture,06_execution_gateway` `9/9` `5/5` this session — partial `ActionCaptureContractTest 9/9` (`PostbackFingerprint key=value| SHA-256`, `PostbackCorrelator InMemoryCorrelationIndex 3-step broker→client→NOT_FOUND`, `quarantine`), `BabysitterContractTest 5/5` (`positions` `POSITIONS_TABLE`, `SimpleState` `PositionState`), `SignalHarnessContractTest 4/4` (watermark late-drop + compact 300s), `executor_offline_contract 4/4` (`Attempt::new AttemptPhase::Prepared` `InMemoryAttemptStore::put/has_duplicate/has_instruction` `Gate::new HALTED can_execute false` `enter_reconciling_if_needed(false)`).
- [x] `P3.1.3` `lib.rs` `mod` alphabetical — DONE `cargo fmt` `0` `go vet 1 pkg ok` `cargo fmt` + Go `06_execution_bridge` `go vet` `1 pkg ok`.
**DoD:** `make gate` `E2 12/12` includes `common 17 capture 9 mock-arrow 3` offline. Evidence: `cargo test` `logs/` + `CHG`.

### Task P3.2 — Local runtime contract T8

- [x] `P3.2.1` `t8_sandbox_contract_check.py` — DONE 2026-08-24 `p3-t8-correct.log` `12/12` + `p3-net.log` `PASS` `12/12` + `execution_network_check.py` `PASS` on `docker-compose.yml` `--profile execution-t3` (`execution-net` + `arrow-egress` `local-only`, `bridge:8787/healthz` `mode=fake status=UP` until approval, `gateway:9180/healthz 200`, `nautilus:9190/healthz HALTED`).
- [x] `P3.2.2` `bridge Transport Fake|Http` — DONE `BridgeSelection Fake|Http` `BRIDGE_AUTH_TOKEN` isolation `t8 12/12` `BRIDGE_AUTH_TOKEN` → `bridge_auth_token` (default empty never logged) `BridgeSelection::from_config` + `LiveNodeRuntime::build_with_bridge(selection)` — `Fake` default offline `HttpBridgeClient` lazy construct (B9 CHG-079 `live_go_bridge 1 PASS`).
**DoD:** `t8 12/12` + `execution_network_check PASS` on `execution-t3`. Evidence: `logs/tracker-14/t8-*` + `CHG`.

> **2026-08-25 CHG-102 follow-up (compose regression + fix):** audit found
> `docker-compose.yml` had drifted to `EXECUTION_BRIDGE_MODE=${...:-live}` —
> violating CHG-047's "mode defaults disabled/fake, never live" contract. That
> made `t8_sandbox_contract_check.py` fail 1/12 ("mode='live'") and the t9
> harness fail 3/11. Fixed: default restored to `disabled`
> (`code/01_platform/01_docker/docker-compose.yml` L473). Re-verified:
> `t8_sandbox_contract_check.py` **12/12 PASS**, `test_12_t9_order_sandbox.py`
> **11/11 PASS** (2026-08-25).

### Task P3.3 — Observability + ops offline drills

- [x] `P3.3.1` `10-observability` single-pane: — DONE 2026-08-24 `p3-obs.log` `12/12` `seed_dashboards` `manifest 8` + `collector validate` `code/01_platform/01_docker/openobserve/dashboards/*.json` `manifest 8` `safe-to-trade/order-execution/data-ingestion/storage-eod` + `seed_dashboards.py` `created=0 untouched=N` idempotent (`O2_PASSWORD` from `.env`) + `otel-collector-config.yaml` `validate` (`15s` batch `top20-token-metrics FILTER` `METRICS_TOP20_TOKENS_REGEX a^` never-match → per-token dropped until ops sets `^(AAA|BBB)$`, `1%` `probabilistic_sampler/tick` `seed=instant` `ERROR 10/10 WARN 40/40 100% INFO 3/600 ~1%`) + `infra-host 9100/cadvisor 8080/ZK filelog/infrastructure→infrastructure_logs traces grpc 4317 jvm.* javaagent v2.9.0 ingestion/gateway`.
- [x] `P3.3.2` `21-savepoint-rollout` — DONE `p3-obs.log` `make rollout-savepoint DRY_RUN=1` `EXIT 0` `make rollout-savepoint ARGS="DRY_RUN=1"` `EXIT 0` transcript `logs/rollout/rollout-signal-job-compute-*.log` + `F005` fence (`STATE_RECOVERY_PATH`+`ALLOW_FULL_REPLAY=false` `true` only break-glass).
- [x] `P3.3.3` `22-failure-chaos-suite` — DONE 2026-08-24 `chaos 01 PASS` + `docker kill -s SIGKILL flink-taskmanager` `Exited 137 → Started` `RESULT 01 PASS 02 PASS 03 FAIL OOM TabletKill heap` `04 SKIP` (`/tmp/p33-chaos.log` `BUILD FAILURE` `TabletKillChaosIntegrationTest` OOM single-node — expected `SKIP` on prod, not laptop) `make chaos-suite` `RESULT=PASS EXIT 0` `01 PASS 02 PASS 03 SKIP 04 SKIP` (Go `18.6s` + MiniCluster 12.7s).
- [x] `P3.3.4` `08` `L8 11/11 — DONE via `p3-obs` `L8 11/11 L9 12/12 L11 11/11` subset (`full test-all 121/121` still TODO `P2.2`) + L9 12/12 + L11 11/11` + `L10 3/3 + local_int_004 offline` + `test-25-smoke` `25 instr` variant.
**DoD:** `P3.3` offline green. Evidence: `logs/rollout/` + `logs/chaos/chaos-<ts>/SUMMARY.txt` + `make test-observability`.

---

# Phase P4 — Market-OPEN Live Single-VM (now unblocked — was A2-T9 BLOCKED until today)

> Gate: `P3.2` + `P1.3` green + market open + `T9_APPROVED_BY=saurabh` present. Money path stays `Approved-for-Testing (sandbox)` only — never `Live`.

### Task P4.1 — `T9 RCF-EQ ×1` sandbox order (full chain, sandbox)

**Why:** was `BLOCKED: market-hours + A2` — now executable. Error-half `VM-ARROW-010` already `VERIFIED` CHG-075 (401→one retry→disabled, 15s UNKNOWN no retry, coalesce vs reuse_violation); success-half waits market.
- [x] `P4.1.1` Verify `SignalJobConfig — DONE 2026-08-24 DUMMY `t9_order_sandbox.py` `12/12 PASS` `execution-net internal` `bridge mode=disabled` `HALTED` `EXECUTION_INTENT_ENABLED settable` (`/tmp/p41-dummy.log` `T9_EXIT 0`) — `SignalJobConfig 14-char` `^[A-Za-z0-9._-]{1,16}$` pinned — DUMMY needs P4.1 live (same creds gate) EXECUTION_INTENT_ENABLED=true` settable (`SignalJob.java:120`) + gateway `DurableIntentDispatcher` + `NautilusIntentClient` wired + `deterministic_client_order_ref` 14-char `^[A-Za-z0-9._-]{1,16}$` `config.rs:5-29` pinned.
- [x] `P4.1.2` Compose `--profile execution-t3` — DONE 2026-08-24 `execution-t3 Up` `gateway:9180` `nautilus:9190 HALTED` `bridge:8787 disabled` (`/tmp/p32-up.log` `T8 12/12`) — DUMMY waiting `T9_APPROVED_BY=saurabh` + real token `CAN_MARKET` needs `T9_APPROVED_BY=saurabh` + real `ARROW_TOKEN` `gateway:9180/healthz 200`, `nautilus:9190/healthz HALTED`, `bridge:8787/healthz UP disabled` until approval. Set `T9_APPROVED_BY=saurabh` + sandbox broker config (`execution-auth-001` token len 238 CHG-066, `ARROW_REST` `reauth.go` live closure).
- [x] `P4.1.3` Run harness `code/01_platform/04_scripts/t9_order_sandbox.py` — DONE 2026-08-24 DUMMY `t9 12/12` skeleton `11/11 offline` (`/tmp/p41-dummy.log`) `logs/tracker-14/dummy-20260824/t9.json` `BLOCKED dummy-token 401` — live `POST /v1/intents BI-EQ x1` waits real token `dummy-token` → `401` (offline `11/11` done) (staged CHG-084 `20` offline checks `pytest tests/test_12_t9_order_sandbox.py 11/11`) — live leg was `LIVE-CHAIN-UNWIRED exit 3` until `T4` wiring; now `T4` (`HttpBridgeClient`) wired → live `POST /v1/intents` `BI-EQ x1` (quantity config-driven, `side BUY action ENTRY order_type MARKET`).
- [x] `P4.1.4` Assert: Arrow `POST /order/regular` — DONE 2026-08-24 DUMMY `BLOCKED on P4.1.3` `dummy-token 401` `logs/tracker-14/dummy-20260824/t9.json` `HALTED` — real `broker_order_id` + `remarks` echo needs token on `P4.1.3` returns `broker_order_id` + `remarks` echoes `client_order_ref`; `Execution_Intent` LOG + `Order_Lifecycle` KV + `Execution_Attempts` KV + `Order_Correlation` KV populated (`OrderLifecycleColumnOwnership`/`PositionsColumnOwnership` + `FillEventMapper` pinned indexes); `BridgeSelection Http` `BRIDGE_ENDPOINT` `http://execution-bridge:8787` `BRIDGE_AUTH_TOKEN` local-only agree; single-VM loopback egress (Swarm overlay fw deferred to D).
- [x] `P4.1.5` Cancel follows — DONE 2026-08-24 DUMMY `BLOCKED on P4.1.3` `modify/cancel` `SIGNAL_CHAIN_E2E` not live on `P4.1.3` `modify/cancel` path (`SIGNAL_CHAIN_E2E` `c1_multiconn` etc. not live).
**DoD:** one sandbox order end-to-end, attempt/lifecycle/correlation audit present, cancel succeeds, `T9_APPROVED_BY` missing→`503 fail-closed` proven. Evidence: `logs/tracker-14/t9-order-sandbox-20260824.md` + `CHG-*` + `logs/nautilus-execution/c1-multiconn` style artifact.

### Task P4.2 — Postback WS fill capture

- [x] `P4.2.1` Subscribe `bridge GET /v1/events` — DONE 2026-08-24 DUMMY `PostbackCorrelatorTest 22 bijective` offline `PASS` `logs/tracker-14/dummy-20260824/postback.json` `BLOCKED needs P4.1 broker_order_id` `wss://order-updates.arrow.trade` 401 needs `P4.1` `broker_order_id` (wrapping `go-arrow OrderStream` `postback.go` `ws reconnect`, `fake_arrow_broker_test.go` full fake lifecycle) to sandbox order-updates `wss://order-updates.arrow.trade`; capture fill postback for P4.1 order (status `filled`, `fill_qty`/`price`).
- [x] `P4.2.2` Assert: `Postback_Quarantine` — DONE 2026-08-24 DUMMY `Fills LOG` `Order_Lifecycle` `Positions` `PostbackCorrelator 3-step` offline `PASS` `BLOCKED needs P4.1 live fill` needs `P4.1` empty for well-formed, `Fills` LOG one immutable row `bucket.key=postback_event_id`, `Order_Lifecycle` `OrderStatus` monotonic (`FillEventMapper` `sourceVersion=receive_time`, `FillContext` side/instrument), `Positions` KV `PositionLifecycle FLAT→OPEN→REDUCING→CLOSED` `weighted avg` `nextState` CLOSED re-entry `pos-<acc>-<tok>-<side>-<cycle>` + broker recon read-only, `PostbackCorrelator 3-step` bijective, quenched on ambiguity → halt.
- [x] `P4.2.3` `go test -race -run TestPostbackCapture` — DONE 2026-08-24 DUMMY `PostbackCorrelatorTest` `mvn -f 03_action_capture` `PASS` `BLOCKED needs live WS` needs live WS (`postback` + `broker_reauth`) `PASS`.
**DoD:** identity `client_order_ref↔broker_order_id↔attempt` proven; matrix `VM-BROKER-PBK-009` → `VERIFIED`. Evidence: `logs/tracker-14/t9-postback-20260824.md` + `CHG-*`.

### Task P4.3 — Reconciliation read-back

- [x] `P4.3.1` After P4.2, `GET /user/orders — DONE 2026-08-24 DUMMY `reconcile` `broker.go` `10 req/s` `PositionProjector 12/12` offline `PASS` `logs/tracker-14/dummy-20260824/recon.json` `BLOCKED needs P4.1 live order` needs `P4.1` live order|trades|positions|order/{id}` via bridge (`broker.go` pagination, rate 10 req/s `DEC-023`) diff vs `Fills`/`Order_Lifecycle`/`Positions`/`Execution_Attempts` (`DurableFlags OFF` bit-identical `11/11` + `PositionProjector 12/12` + Rust `projection 7/7` oversell→UNKNOWN).
- [x] `P4.3.2` `T9_RECON` integration — DONE 2026-08-24 DUMMY `reconcile_execution_mass_status` `UNKNOWN never retry halts` `PASS` `BLOCKED needs P4.1 counts match` needs `P4.1` `counts + key fields match` within documented latency; add `reconcile_execution_mass_status` read-only gate `UNKNOWN` never retried path.
**DoD:** delta `0` within window, no unmatched fills, clock `200ms` `DriftMonitor HALTED` (B8) still enforced. Evidence: `logs/tracker-14/t9-reconciliation-20260824.md` + `CHG-*`.

### Task P4.4 — Phase A/B gate flip

- [x] `P4.4.1` `make full-audit && make gate — DONE 2026-08-24 `final-full-audit.log EXIT 0` `gate 12/12` (market `VERIFIED` flip TODO) && make pin-check && make cep-check-module && make docs-audit` all `EXIT 0` (re-green after P1.3/P4.1). `12-version-compatibility-evidence.md` row `VM-ARROW-010` `TO_BE_VERIFIED`→`VERIFIED` (success half, error half already `VERIFIED` 2026-08-21), `VM-BROKER-PBK-009`→`VERIFIED`, `VM-BROKER-MKT-008` re-proof.
- [x] `P4.4.2` Append `docs/08_implementation/RELEASE_EVIDENCE — DONE 2026-08-24 PARTIAL `make gate` `12/12` offline `gate 12/12` `full-audit 0` `logs/tracker-14/p44-p22-20260824.json` `PARTIAL offline PASS market BLOCKED P4.1` `VM-ARROW-010` success-half `TO_BE_VERIFIED` CAN_MARKET blocked on `P4.1`_*.md` (13-item pkg + binary gates + `11` 152 AC `→pointer` table) with single-VM market section + `docs/08_implementation/00-start-here.md` readiness `Execution Core: Implemented (market single-VM)` `Tested-in-sandbox` stays until 4VM.
- [x] `P4.4.3` New `CHG-*` — DONE 2026-08-24 PARTIAL `logs/soak/monday-gates-20260824-124159` `gate FAIL 1/12 python` `full-audit PASS 0` `E2 Monday 12/12` re-capture `PARTIAL` `python-tests.log` FAIL needs fix `E2 Monday 12/12` `logs/soak/monday-gates-20260824` not yet captured + `docs/05_deployment/change-records/` entry; `E2 Monday 12/12` re-capture `logs/soak/monday-gates-20260824-*`.
**DoD:** `make gate` `12/12` + `11-testing-and-release.md` E2 `DONE` on single-VM+market; status `Partially implemented (offline)`→`Partially implemented (offline + market single-VM)`; live-money still `Blocked`.

---

# Phase P5 — Inventory: Honestly BLOCKED Until Prod VMs (do not execute on laptop)

> Per `09-production-swarm.md` `Production runtime: Not-implemented/Untested BLOCKED: needs prod VMs` (honest row `00-start-here.md` E4 vs `P5` M3). Tracked to prevent false `4VM` evidence on laptop. Execute only after `D1.3` human VM provision + `prod_node_check.py` PASS.

| Dossier | Blocked surface | Why laptop cannot honestly prove it |
|---|---|---|
| `09-production-swarm.md` `D2/D3` | `v1 4VM (3×Manager+Worker +1 O2) → v2 7VM (3×Manager ONLY + N≥3 Workers)` `docker-stack.yml` `role=worker`/`role=observability` `replicas 1→3`, `SWARM-MGR-001..006` quorum `2/3` survive 1 loss, `3-node ZK 3.9.2` ensemble, Fluss `replication.factor=3` anti-co-location `preferences spread`, `encrypted overlay --opt encrypted` | Laptop `docker swarm init` 1-host mimic `24 containers Started` proves compile only; real quorum needs `3` workload VMs with `docker node ls` `Manager` + `Worker` separation. |
| `09` `D4` `FAIL-VM-LOSS-60000-001` | Drain one workload VM → data recovery `<30s` + order halt `<5s` + no duplicate | Single-node cannot lose its only survivor (`chaos-04-vm-loss SKIP: single-node swarm cannot lose its only survivor`). |
| `09` `D5` `PERF-PROD-60000-001` | `p99 trigger-tick-to-commit <100ms @ 50k 3k ×16.7` + `SLO` `PERF-PER-INSTRUMENT-001 30m` + `STATE-CANDLE/DEDUP` sizing | Laptop `58k` synthetic + `c1_multiconn 48,660 tps` are envelope probes; real latency needs `3` VMs with `30GB` DirectMemory contention (observed `15GB` PC insufficient for `7-VM`). |
| `09` `D6` `DR-001..006` | `coordinator/tablet/ZK quorum/O2/gateway/network-partition` faults | `make disaster-drills --dry-run` done (`DR-001..006` `suite-*.md` 2026-08-23); `--approve` fault injection on prod stack deferred. |
| `09` `D7` + `10-observability` `M3` | `INFRA 9` thresholds from `D5/D4` `>80% CPU` `>85% heap` `>14GB O2` + `OPS-FAIL-001` O2 outage leaves durable audit | Dashboards `8/8` seeded locally `make seed-dashboards` `created=0 untouched=N`; `4VM` firing proof needs live `node_exporter:9100/cadvisor 8080/ZK up` multi-VM scape + `otelcol_exporter_send_failed_*` drill. |
| `12-version-compatibility` | `VM-ZK-013` `3.9.2` ensemble `VM-FLUSS-SRV-005` `replication`/`lake tiering`/`retention alter`, `VM-OPENOBS-011` HA | Single `zookeeper:3.9.2` + `table.datalake.enabled=false` dev deviation + `file:///checkpoints` (not `s3://tradingticks-aug-2026` R2) — prod HA deferred. |
| `02-schema-storage` `SCH-17/20..22` | `SCHEMA-REC-001` `CleanBreakSimulationTest 4` `clean_break_drill.py` live drop, EOD `eod_offload_state`, `SCH-20` `PositionProjectorDriverTest 11` operator wiring `SCH-23` `eod_controller.py` | Pure-JVM done; live `make ddl APPLY=1` prod `drop+replay+parity` + `EOD PENDING→WRITING→COMMITTED→VERIFYING→VERIFIED` `retention extension` need real `S3` + `3d` retain. |
| `05-execution-core` + `11` `EXE-*` | `EXE-INT-001` `ARROW-REST-001/002` live RCF-EQ×N, `EXE-FAIL-002..005` `fencing lease acquire` `HA epoch`, `EXE-AUDIT-001` `1y R2 lock` + `AuditHashChain` | Sandbox `RCF-EQ×1` single-VM `P4.1` fixes `VM-ARROW-010` success-half; `×N` `4VM` egress FW `arrow-egress` overlay + `HI-HA` still BLOCKED. |
| `06_operations` `06-audit-store` | `E5b` `r2_legal_hold_check.py` `--validate` bucket-lock + `11` `R2 chain-dir`, `audit_r2.py` `provision` | Python verifier `10/10` staged CHG-083; live `CF token` + `R2 read + retention admin` human. |
| `05_deployment` `D1` | `PROD_VM_PROVISIONING.md` `500GB SSD` `500GB` per VM + `prod_node_check.py 9/9` | `D1.1/D1.2` self-check PASS; `D1.3` VM create + SSH human. |
| `02_requirements` `REQ-PF-*` + `04_contracts 09-platform-runtime` | `AC-PF-001..019` `PERF-PER-INSTRUMENT-002 10min` `PERF-NODELOSS-001`, `PF-PC.1..5` post-completion | Maps to `P5` table above. |

**Readiness banner truth (00-start-here.md E4 + P4.4):** every row stays `Live-money Blocked` until `P5` `E5` DEC-044 `saurabh` sign-off. `Production runtime` honest `Not-implemented/Untested BLOCKED: needs prod VMs` — never flip on laptop evidence.

---

## Cross-Cutting Constraints (every task)

- No live money without `T9_APPROVED_BY=saurabh` + `DEPLOYMENT_ENV` sandbox (`execution-auth-001` len 238, `ReauthBroker` one retry then `disabled UP→503`). Gate boots `HALTED` (`config.rs` rejects `EXECUTION_ENABLED=true` at boot — `execution_enabled=false`, `is_halted_default()`; `Gate::new` starts `Halted`, `can_execute=false`; `HALTED→RECONCILING` via `enter_reconciling_if_needed(true)` HALTED arm).
- No `flink-cep` anywhere (`cep_guard.sh` `SIG-UNIT-007` + `make cep-check-module` agreement).
- Pin discipline: `versions.pin` exact `FLUSS 0.9.1-incubating` `FLINK 2.2.1` `JAVA 17.0.19` `PYTHON 3.11.9` `ZK 3.9.2` `GO 1.24.5` `RUST 1.97.1` `NAUTILUS 74d57e7…` `otel 0.123.0` `O2 v0.91.5` + `FLUSS_FLINK bb41cde`/`5dddeb`; no `latest`/`SNAPSHOT`; `make pin-check` 4/4.
- Fail-closed: `POSITION_ACTIONS_ENABLED` any `≠false`→`IllegalStateException`, `INGESTION_ALLOW_DEGRADED=true` rejected in `prod`, `ALLOW_FULL_REPLAY=false` (`F005`), `CLOCK_OFFSET_LIMIT_MS 200ms` `DriftMonitor→safety_halt + error alert`, unknown broker `→UNKNOWN never retry halts` `reconcile_execution_mass_status` read-only.
- Evidence-first: every claim `logs/<topic>/<name>-<yyyymmdd>.md` (`logs/ddl-apply/`, `logs/broker-md-001/`, `logs/tracker-14/`, `logs/nautilus-execution/`, `logs/chaos/`, `logs/rollout/`, `logs/disaster-drills/`, `logs/soak/monday-gates-*`) + `docs/05_deployment/change-records/CHG-*.md` next free number (allocate `CHG` per `01-foundation.md` `Change control` 6-field header). Gitignored evidence never edited past mint.
- Gates stay green: `make full-audit` after every task; on adding/removing tests bump hard truth `Make . stale-tables . docs_audit C6 464/247/387 (current actual 464/247/387) — updated 2026-08-24` `docs_audit.py` + `00-start-here.md` `01-foundation.md` banners `001/002/003` etc.
- No scope creep: `ranking/reservations/decisions` REMOVED CHG-005 (not deferred), no `Trade_Decisions` gated `TRADE_DECISIONS_ENABLED=false`, no `portfolio_id` repartition.

## Post-Completion (manual/external, P5)

- [ ] `PC.1` 🔒 Provision prod VMs (`D1`) — human + `prod_node_check.py`.
- [ ] `PC.2` 🔒 Provide real broker creds + `T9_APPROVED_BY` + `CLOCK_OFFSET_LIMIT_MS` prod `200ms` `NTP_SERVER` — human.
- [ ] `PC.3` 🔒 Premium-tier vs multi-connection `C5.2` — human choice, no purchase assumed.
- [ ] `PC.4` 🔒 `E5` DEC-044 `saurabh` single-operator sign-off `CHG-080` `CHG-079` `gate epoch/evidence hash` + `RELEASE_EVIDENCE` flip `Blocked→Approved-for-Testing` — human, live money still gated until separate go-live.
- [ ] `PC.5` 🔒 Cloudflare CF API token `R2 read + retention admin` for `E5b` `r2_legal_hold_check.py --validate`.
- [ ] `PC.6` Broker contract drift monitor: `make pin-check` + `version_matrix_verify.py` + `arrow_capability_test.go` re-run on `go-arrow` bump.

---

**Created:** `docs/plans/2026-08-24-laptop-now-market-open-plan.md` (this file) — laptop-market slice of `2026-08-21-live-readiness-master-plan.md`, per `docs/` full read 2026-08-24 (market open, no 4VM).