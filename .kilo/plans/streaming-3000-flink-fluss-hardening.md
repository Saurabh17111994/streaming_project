# Streaming 3000 — Flink+Fluss Hardening Plan
**Status:** Draft v2 — A+B updated 2026-08-22 — 7d retention, simple fixes table added. Not implemented.
**Scale:** 3000 instruments @ 50k tps avg / 90k cap. Now: 1 slot (1024) — design for 3 slots (1024+1024+952). No code modified yet.

## 1. Context and Current Architecture

**What exists (Observed):**
Broker Arrow HFT WS (zstd binary, 40B ltpc / 196B full) → Go arrow-bridge (stdout NDJSON, per-slot goroutine) —pipe→ Java IngestionService → Fluss LOG raw_table_1 (16 buckets by instrument_token, Bloom/retry, bounded 50k/64MiB) → Flink SignalJob (watermark BoundedOoO 5s + idle 15s, FingerprintDedup keyed MapState TTL 300s, 15s tumbling windows) → Fluss KV feature_candles_15s + forming_bar + LOG+KV Signal_Candidates + Signal_Candidates_current + disabled execution_intent → Nautilus Rust + go-arrow-bridge → Arrow REST edge.arrow.trade. SafetyHaltJob + Babysitter (RETAIN_ON_CANCELLATION). OTel :4318 → O2 :5080, Prometheus :9249, 7 dashboards. Deploy Compose dev (single host, trading-net/execution-net internal:true) vs Stack Swarm v1 4 VMs / v2 7 VMs (encrypted overlays, external secrets, JM1600m/TM3g).

**Fluss 26 DDL:** LOG raw_table_1(20c), Signal_Candidates(22c), Fills etc; KV feature_candles_15s(16bkts, avail 7d (was 2d), future 7d, format-v2), forming_bar(11c), Signal_Candidates_current(PK token), fingerprint_dedup(16bkts, 2d, dormant) etc. Keys subset of PK, composite PK needs kv.format-version=2.

**Flink:** EXACTLY_ONCE 10s/30s/1concurrent, fixed-delay 3x30s, RetainOnCancellation, rocksDB/heaps via Configuration, CheckpointingOptions dirs, StateRecoveryOptions savpoint OR ALLOW_FULL_REPLAY else fail. No silent offset-0 replay.

## 2. Problems Discovered

| # | Conflict | Docs | Code | Risk |
|---|---|---|---|---|
| C1 | Scale | 3000 @50k | Go 1024/conn, ARROW_HFT_CONNECTIONS=1, manifest 1024 → max 1024 | Cannot meet SLO |
| C2 | Dedup authority | Fluss KV master | Flink MapState native TTL master, Fluss DDL unused | Doc/code mismatch |
| C3 | Retention vs 3 trading days | >=3 trading days live | Derived 2d calendar TTL | Weekend EOD fail → loss |
| C4 | Exactly-once | claim exactly-once | Flink state EO, Fluss sink AL, multi-sink not atomic | Duplicate visible |
| C5 | Checkpoint durability | S3 prod | dev volume | Prod not HA |
| C6 | Candle invariants | — | No OHLC invariant | Bad candle → bad signal |
| C7 | Network buffers | — | p16 needs 17 have 13 | p8 fails start |

Assumptions at risk: no broker sequence id (fingerprint+event_time tie-break), 15s UTC event-time, partial_update not atomic, calendar TTL blind to holidays.

## 3. Decisions Made (30)

| # | Topic | Decision | Note |
|---|---|---|---|
|1|Scale|3k design, run 1 slot now|N=3 sharding token→slot|
|2|Dedup|Flink MapState TTL 300s, Fluss dormant|15M entries ~1GB at 3k|
|3|Late ticks|5s allow, beyond drop counted, empty=no row|deterministic (event_time,fingerprint)|
|4|Restart|Strict gate: need STATE_RECOVERY_PATH or ALLOW_FULL_REPLAY|fail closed|
|5|Retention|**All 7d (updated 2026-08-22 from 2d)** + block-delete-unverified|now meets 3 trading days + weekend buffer|
|6|Fluss slow|Bounded halt, tunable 50k/64M (1k) →150k/192M (3k)|80% warn,100% halt|
|7|Checkpoint store|Dev local volume, Prod encrypted S3 mandatory|fail start if missing|
|8|Candles|Only 15s now|forming_bar roll-up later|
|9|Failure tests|4 mandatory before prod|slot,TM,tablet,VM loss|
|10|Alerts|6 must alerts|ckp,Fluss,lag,late%,queue,ready|
|11|Spread|16 bkts, p=8|hash token|
|12|Market hours|No filter now, flag later||
|13|Replay|Separate tables, not prod|deterministic|
|14|Backend|RocksDB incremental prod, Heap dev|SSD dirs, 0.4 managed|
|15|Signal dup|LOG append, KV upsert||
|16|Manifest change|Fixed/session, controlled restart|savepoint|
|17|Metrics|Aggregated + top-20 sampled|avoid 30k series|
|18|Clock|NTP 2s gate, not-ready if drift|+ future skew drop|
|19|Schema|Versioned + fail-closed validator|controlled deploy|
|20|Auth|Shared token, per-slot epoch isolated|terminal per slot|
|21|S3 fail|Block Fluss TTL delete until manifest verified|extend|
|22|Network mem|256m for p=8||
|23|Replication|LOG x3, KV 1-2 rebuildable||
|24|OHLC checks|5 invariants → quarantine||
|25|Dup window|First-write-wins, skip second||
|26|Observability|Sampled 1% ticks, 100% errors, 7d||
|27|Job update|Savepoint rollout||
|28|Disk full|80% alert, 90% halt ingestion||
|29|Gap halt|Ignored this plan — log only||
|30|Dedup lost|Fail closed, no signals if state lost||

## 4. Architecture / Data-Flow Changes

**Slot sharding (no rewrite):**
- Env `ARROW_HFT_CONNECTIONS=N` (1 now,3 for 3k), `ARROW_INSTRUMENT_MANIFESTS=man1,man2,man3` or shard-by-hash at startup. SubscriptionPlan token→slot deterministic (mod N or range). Go supervisor per-slot epoch isolated, shared refreshAuth still 3 attempts but terminal per slot, not kill-all. Java IngestionService per-slot parsers feeding same Fluss sink but labeled slotId for discontinuity/safety scope. Backpressure PendingTracker per-slot + global 150k/192M.

**Flink p=8:**
- Fluss 16 buckets unchanged, Flink keyBy token → natural 16→8 rebalance safe (tested via CompatFlinkCheckpointRescale). RocksDB incremental, taskmanager.memory.network.max=256m, taskmanager.memory.managed.fraction=0.4, RocksDB dirs on SSD. NativeMapState TTL 300s neverReturnExpired. Watermark 5s+5s lateness+15s idle.

**Retention/block-delete (updated 7d):**
- Change DDL ttl 2d → 7d for all tables, EOD controller adds guard: before Fluss TTL delete executes, check ice-berg manifest verified for that day; if unverified → extend (setTableConfigOverride or hold). Alert critical.

**Candle guards:**
- Post-window OHLC 5 checks → quarantine table, counter compute.candles.invalid.*. First-write-wins map for (token,window_start) → duplicate_window count.


## Simple Fixes Table (added 2026-08-22 — A)

| No | Problem today | Fix | File |
|---|---|---|---|
| T0 | Only 1 slot 1024 | Make slots env-driven 1→3 | docker-compose.yml, docker-stack.yml |
| T1 | 1024 hard limit | Shard 3000 by token 1024+1024+952, per-slot epoch | supervisor.go |
| T2 | Fluss slow floods | 50k/64M → 150k/192M for 3k, 80% warn 100% halt | IngestionService |
| T3 | p=1 too slow | p=8 + 256m network + RocksDB | SignalJob.java |
| T4 | Dedup size blind | Add gauge alert 800MB | FingerprintDedupFunction |
| T5 | Late overwrite | Drop >5s to quarantine, first-write-wins | CandleEmitFunction |
| T6 | Bad candle | 5 OHLC checks → quarantine | CandleEmitFunction |
| T7 | Silent replay | Strict gate | SignalJobConfig |
| T8 | 2d→7d retention | TTL 7d + block delete | 8 DDLs + EOD controller |
| T9 | Checkpoint lost | Prod S3 mandatory | Configuration |
| T10 | Clock wrong | NTP 2s gate | NtpClockChecker |
| T11 | Too many metrics | Aggregated + top-20 | otel-collector-config.yaml |
| T12 | Update loses dedup | Savepoint rollout | scripts/deploy |
| T13 | No HA proof | 4 failure tests | 08-local-compose |


## 5. Ordered Implementation Tasks — Grouped by Category/Dependency (updated 2026-08-22)

**Groups:** G1 Safety Gates (no scale) → G2 Ingestion Scale → G3 Flink Compute → G4 Storage/Durability → G5 Operations/HA. Phases 1-3 map to live rollout.

| Phase | Group | Ord | Task | Affected files/components | Required change | Depends | Validate | Test at |
|---|---|---|---|---|---|---|---|---|
| 1 | G1 Safety | T7 | Strict gate keep | SignalJobConfig validator, env | Retain gate, no silent replay | — | Start without STATE_RECOVERY_PATH → fail F005 explicit | 1k |
| 1 | G1 Safety | T10 | NTP gate 2s | IngestionService NtpClockChecker | 2s limit, not-ready + futureSkew drop | — | Clock off 3s → not-ready, no bad window | 1k |
| 1 | G1 Safety | T4 | Dedup keep Flink + metrics | FingerprintDedupFunction.java, dedup-state dashboard | Keep native TTL 300s, add gauge dedup_state_size alert 800MB, doc Fluss dormant | — | Checkpoint size p8·3k ~1GB budget | 1k mock 15M |
| 1 | G4 Storage | T8 | Retention 7d + block-guard | 02_raw_table_1.sql + 7 other DDLs 2d→7d, EOD controller | Change ttl 7d, add verified-guard extend + alert | — | EOD fail Fri → Fri-Sun kept 7d, alert fired | 1k |
| 2 | G2 Ingest | T0 | Parameterize slot count | code/01_platform/01_docker/docker-compose.yml, docker-stack.yml, 05_instruments/manifests | ARROW_HFT_CONNECTIONS=N env-driven, manifest list, docs | — | `docker compose config` shows N=1 now N=3 candidate | 1k |
| 2 | G2 Ingest | T1 | Go bridge slot sharding | executor/go-bridge/supervisor.go, SubscriptionPlan, bridge_events | Deterministic token→slot 1024+1024+952, per-slot epoch isolated, shared token 3-attempts per slot | T0 | Mock 3×1024, kill 1 slot → other 2 healthy | 1k mock 3k |
| 2 | G2 Ingest | T2 | Tunable backpressure | IngestionService, PendingTracker, configs | 50k/64M → env overrides 80/100%, 150k/192M for 3k | T0 | D8 slow-fluss: 50k burst → not-ready exact, no drop | 1k→3k |
| 2 | G3 Compute | T3 | Flink p8 + network + RocksDB | code/02_services/02_compute/SignalJob.java, SignalJobConfig, application.conf, stack env | p=8, network max 256m, RocksDB incremental, managed 0.4, SSD dirs | T0 | MiniCluster p8 start OK, CompatRescale pass | 1k p8 |
| 2 | G3 Compute | T5 | Late/duplicate guards | CandleAggregateFunction, CandleEmitFunction, KV sink wrapper | 5s lateness dropped+quarantine, emitted first-write-wins, duplicate_window counter | T3 | Late 5.1s not counted, quarantine row present | 1k |
| 2 | G3 Compute | T6 | OHLC invariants | CandleEmitFunction validation | 5 checks → quarantine + alert | T5 | Fuzz bad H/L → quarantined, not in feature_candles_15s | 1k |
| 2 | G3 Compute | T11 | Metrics top-N sampled | otel-collector-config.yaml, 7 dashboards | Global+slot metrics, top-20 per-token sampled, 1% tick logs | T3 | O2 cardinality < threshold at mock 3k | 1k |
| 3 | G4 Durab. | T9 | S3 checkpoint mandatory | Configuration, stack secrets, s3 endpoint | Prod requires CHECKPOINT_DIR s3://, fail start if missing, dev keeps volume | — | Prod-like without S3 → fail clear msg | Swarm |
| 3 | G5 Ops | T12 | Savepoint rollout script | scripts/deploy, Makefile | Savepoint → stop → deploy with recovery path | T3 | Rolling update retains dedup hit rate | 1k |
| 3 | G5 Ops | T13 | Failure chaos suite (4 tests) | 08-local-compose L-tests | Add slot kill, TM kill, tablet kill, VM loss as L11 gates | T1,T3 | 4 failures: no dup candle/signal, halt <5s | Swarm 3k ×30 |

**Dependency rule:** G1 → G2 → G3 → G5. G4 (T8,T9) independent but T8 do early phase 1. Parallel safe: G1+T0 can start together, G2 parallel with T4/T8, G3 parallel T11 with T6. Plan phases = rollout phases.

## 6. Failure / Recovery Behavior

| Scenario | Observed A | Planned B | Invariant |
|---|---|---|---|
| Go slot down (1 of 3) | slot terminal, others continue | sampled: 1024 stocks missing, signal blocked for those tokens | No cross-slot kill |
| Fluss LOG slow | retry 100/200/400 ×3 then uncertain | bounded halt + not-ready, no silent drop | never drop silently |
| Flink TM death p8 | job fails, retain checkpoint | restore from last checkpoint → no dup window (first-write-wins) | dedup preserved |
| Fluss tablet death (LOG x3) | single tablet → loss | replication 3 → no loss, KV rebuildable | LOG durable |
| VM loss (Swarm) | — | <30s data recov, <5s order halt | overlay re-route |
| Disk full 90% | — | 80 alert,90 halt ingestion+savepoint | no auto delete |
| Clock drift >2s | not-ready unchecked | not-ready, future tick quarantine | no bad window |
| Checkpoint bad / dedup lost | fail start ambiguous | fail closed, no signals until fix | no dup orders |
| S3 offload fail | delete even if unverified (old 2d) | block delete until verified with 7d | no weekend loss* |
| Duplicate window replay | last-write-wins risk | first-write-wins skip | deterministic |

*Was 2d + guard; now fixed to 7d + guard = safe for weekend.

## 7. Testing and Validation

- **Contract:** pin-check (versions.pin), TableContractValidator F005, execution_network_check internal:true.
- **Unit:** FingerprintDedup native TTL 300s exact, CandleAggregate deterministic tie-break, OHLC guards.
- **Local Compose L0-L11:** CONFIG/HEALTH/NETWORK 15 checks, p8 network 256m, slow-fluss 1s window, late-drop side-output, first-write-wins, NTP gate, backpressure D8.
- **Flink:** CompatFlinkCheckpointRescale p1→p8, SignalChainLiveE2E 205k candles rescale, CheckpointFailureIntegration.
- **Ingestion:** 341+236 etc common/ingestion suites, COMPAT-FLUSS-006 earliest/latest.
- **Proto live:** Arrow HFT single-slot 1k baseline, then 3-slot 3k ×16.7 and 3k ×30 soak 30 min on Swarm v2 with S3, 6 alerts firing evaluated.
- **Replay:** separate table deterministic check `(event_time,fingerprint)`.

## 8. Rollout / Migration Strategy

1. Phase 0 (now): run N=1, p=1 or 4, RocksDB dev optional, 7d ttl + block guard off. Prove 1k baseline, all gates green.
2. Phase 1: enable T0-T2 tunable limits, deploy 3 manifests but N=1 active (others idle). Verify sharding deterministic offline.
3. Phase 2: raise to N=2 (2048) controlled restart with savepoint (T12), p=4→8, network 256m, replication LOG x3 live. Soak 16.7 avg.
4. Phase 3: N=3 (3000) cap 30/instrument burst test, 6 alerts, 4 failure chaos (T13) on prod-like Swarm with S3. Must pass before live-money flag.
5. Premium subscription purchased → switch N=1→3 without code change (env only). Manifest fixed/session; change needs savepoint restart.
6. Cutover: verified Iceberg manifests must cover 7d before allowing TTL delete (T8). Keep fallback savepoint retain.

No schema drop in rollout; DDL ALTER nullable only.

## 9. Explicitly Out-of-Scope (not in this plan)

- Gap per-stock auto halt (Q29 ignored) — stays log-only.
- Multi-timeframe 1m/5m roll-up, execution path enablement, position snapshot hardening, historical Iceberg backfill, broker sequence id, per-tick tracing, PME clustering, full Swarm 7-VM HA proof beyond 4 failures.

## 10. Remaining Risks

- **7d retention = ~3.5× storage vs 2d — higher disk cost; block-guard may extend beyond 7d when S3 down ⇒ prod may hold >2d when S3 down** — cost surprise; monitor held-bytes alert.
- **1GB dedup checkpoint at p8×3k → 30s timeout tight** → may need 45s or incremental tuning after soak measurement.
- **Shared Arrow token** → slot auth thundering herd on expiry; refresh 3 attempts must jitter.
- **Calendar TTL vs trading days** — holiday Monday still counts against 7d but 7d buffer covers it; guard mitigates but spec mismatch remains.
- **Sampling 1%** may hide single-stock stall; top-N mitigates but not full coverage.
- **No per-stock halt on gap** — gap-candle may still drive signal until human triage.

---
**Verification:** Plan is planning-only. No source modified. Verify with `cat .kilo/plans/streaming-3000-flink-fluss-hardening.md` and `git diff -- .kilo/plans/`.

**Next:** Implement T0→T13 in order. T3 (p8) blocked until T0 env ready. Do not start T13 chaos until T1/T3 green.

