# Candle LOG/KV Replay Safety — Production Resolution Tracker

**File:** `docs/08_implementation/14-candle-log-kv-replay-safety_2.md`  
**Status:** `IN_PROGRESS — production blocked` (RE-SCOPED 2026-08-13, see banner below)  
**Purpose:** corrective implementation and evidence tracker after `13-candle-log-kv-replay-safety.md`.  
**Scope:** signal LOG + KV replay safety — `Signal_Candidates` LOG (append per signal) + `Signal_Candidates_current` KV current-state; candle table is KV-only (converted 2026-08-13).

> **REQUIREMENT CHANGE (user decision, 2026-08-13) — candle [LOG + KV] RETIRED, facility moves to SIGNAL tables.**
>
> The candle [LOG + KV] facility is **absolutely not needed**: the user does not do
> per-stock candle auditing. Therefore:
>
> - `feature_candles_15s` is the **sole candle output — as a KV upsert table**
>   (2026-08-13, later same day: converted to KV, PK `(instrument_token, window_start)`,
>   last-write-wins — replay re-upserts the same key and converges, no row growth).
>   The candle LOG-era machinery (`feature_candles_15s_current` KV projection, the
>   candle history audit/migration machinery (`CandleMigrationTool` load,
>   `CandleMigrationBatchJob` union audit, conflict reconciliation, `run-batch.sh`
>   candle tables), and the candle-KV rehearsal are **RETIRED**.
> - The [LOG + KV] facility **is needed for the trade-signal table on Fluss** (user
>   confirmed): Flink appends a **new row per found signal**, and a KV current-state
>   holds the latest/active candidate per instrument.
> - Target design (locked 2026-08-13): `Signal_Candidates` → **LOG** (append-only,
>   every signal ever fired, never updated — matches the business-logic contract's
>   "`Signal_Candidates`: immutable candidate audit"); new `Signal_Candidates_current`
>   → **KV**, PK `(instrument_token)`, latest/active candidate per instrument, updated
>   on supersession.
>
> Tracker sections below are re-scoped accordingly. Candle-KV-specific evidence
> (CANDLE-SCHEMA-002, CANDLE-CANONICAL-001, CANDLE-MIGRATION-002,
> MIGRATION-CONFLICT-002, candle `CHECKPOINT-RESTORE-002`) is retained as the
> **historical record of what was built**; the load-bearing machinery that remains
> (P4 state backend + durable checkpoints, STARTUP-GATE, P5 dedup memory, P6 failure
> injection, P8 observability) re-targets the signal dual-sink. File name retained
> for cross-reference stability.

> This document is the authoritative tracker for this corrective phase. Do not mark a checkbox complete from prose alone. Each completed item needs the stated code, test, command output, or operational evidence. Do not modify production code, apply DDL, start SignalJob, or touch the live cluster unless the task explicitly belongs to the later operator-only phase.

> **Long-run gate (operating rule, user directive 2026-08-12):** any test/audit/bench/soak estimated > 10 min MUST be preceded by a ≤ 2-min smoke exercise of the same machinery — e.g. `logs/tracker-14/probe-r2.sh` (R2 lake-read probe) before any run touching the R2 lake read, a 60 s feed smoke before rate runs, a bounded-read probe before audit/load runs. Smoke passes → start the long run; smoke fails → fix, re-smoke. No blind long waits: the P3.5 R2 canary proved a 55-90 min run can wedge with zero error while a 30 s probe catches the same failure. The smoke result is recorded in each run's evidence.

### Observability authority

`docs/04_contracts/openobserve.md` remains the normative OpenObserve/OTLP
contract for endpoints, authentication, streams, structured fields, retention,
and the separation between live observability and seven-year audit retention.
This document is the single authoritative **implementation and evidence tracker**
for observability in this corrective phase. Executable collector configuration,
provisioning code, tests, query results, and alert-delivery evidence must agree
with that contract and this tracker. Do not create a second observability plan.

---

## 1. Fixed objective and architecture

### 1.1 Objective

Deliver a production-ready SignalJob that:

1. preserves `feature_candles_15s` as the **sole candle output** — an immutable append-only LOG (candle [LOG + KV] facility RETIRED by user decision 2026-08-13);
2. appends every fired trade signal to `Signal_Candidates`, an immutable LOG — one new row per signal found, never updated;
3. maintains `Signal_Candidates_current`, a KV current-state projection — latest/active candidate per instrument, updated on supersession;
4. blocks accidental no-restore/full-history starts;
5. rejects incompatible Fluss table metadata before job execution;
6. prevents non-canonical strategy/rule/configuration rows from entering the signal KV current-state;
7. uses explicitly configured managed state and durable checkpoints;
8. proves throughput, latency, memory, checkpoint, and failover targets with reproducible evidence.

### 1.2 Runtime topology

```text
Arrow/Go ingestion -> raw_table_1 LOG -> SignalJob
                                      |
                                      +-> feature_candles_15s KV            (sole candle output; PK (instrument_token, window_start), upsert)
                                      +-> in-memory SignalDetectionFunction
                                             +-> Signal_Candidates LOG       (append one row per fired signal)
                                             +-> Signal_Candidates_current KV (latest/active candidate per instrument)
```

### 1.3 Non-negotiable contracts

- `feature_candles_15s` is KV (upsert, PK exactly `(instrument_token, window_start)`): one row per closed 15 s window per instrument; last-write-wins — replay re-upserts the same key and converges (no row growth). It is the **only** candle table; `feature_candles_15s_current` (KV) is RETIRED.
- `Signal_Candidates` is LOG (no primary key, append-only): every signal ever fired is a row; rows are **never updated**.
- `Signal_Candidates_current` is KV with PK exactly `(instrument_token)`: one row = the latest/active candidate per instrument; supersession replaces the row. (PK extension to `(strategy_id, instrument_token)` is recorded for a future multi-strategy phase.)
- Signal LOG and KV rows share the same 22-column layout as the pre-change `Signal_Candidates` v2, row `schema_version="2"`.
- LOG duplicates after deliberate replay are retained as evidence; they are not silently deleted.
- KV deduplicates only by its primary key; Fluss does not enforce strategy/rule policy.
- Only the approved canonical strategy/rule/configuration writes to the signal KV current-state.
- Normal startup restores from a nonblank `STATE_RECOVERY_PATH`.
- Full replay requires explicit `ALLOW_FULL_REPLAY=true` and capacity approval.
- Restore failure never falls back automatically to full replay.
- Signal LOG and KV sinks have shared job fate; LOG-only degraded mode is out of scope.
- No DDL is applied at runtime.
- Kubernetes remains out of scope; deployment targets are Docker Compose dev and Docker Swarm production.

### 1.4 Existing approved targets

Use the repository’s governed targets; do not invent replacements:

| Metric | Required target |
| --- | ---: |
| Sustained input | 60,000 ticks/s average |
| Peak input | 90,000 ticks/s |
| Decision latency | p99 < 100 ms, excluding the intentional 15s event-time window boundary |
| Data-path recovery | <= 30 s |
| Safe halt | <= 5 s |
| Checkpoint interval | pinned existing value, currently 10 s |
| Checkpoint timeout | pinned existing value, currently 30 s |
| Max concurrent checkpoints | pinned existing value, currently 1 |
| Memory | < 85% of allocated process/container budget at target load |

If the source document uses a different exact unit or measurement boundary, preserve its definition and record it in evidence; do not silently reinterpret the target.

---

## 2. Current baseline and known blockers

> Baseline below is the **pre-requirement-change code state** (2026-08-13): the candle
> LOG-era machinery it describes is retired per the header banner (first the signal
> re-scope, then the candle KV-only conversion). Re-scope deltas are recorded in the
> phase sections.

These are verified baseline facts, not tasks to rediscover:

- `CandleTableSchema` defines the 15-column candle row contract (now the **KV** contract — PK `(instrument_token, window_start)` NOT ENFORCED; `LOG_TABLE` renamed `TABLE` 2026-08-13; the LOG-era KV twin is gone).
- `SignalJobConfig` has the fail-closed startup mode (`CANDLE_CURRENT_TABLE` key deleted 2026-08-13; `ALLOW_FULL_REPLAY`/`STATE_RECOVERY_PATH` retained).
- SignalJob has the candle **KV upsert** sink + signal LOG/KV dual-sink (converted 2026-08-13).
- `CandleMigrationTool` loaded dev history using a low-level `BatchScanner` and a 25-key accept list. (RETIRED with the candle KV projection.)
- The accept list permits `MAX(output_ts)` even when business fields conflict. (RETIRED — no candle conflict reconciliation.)
- `CandleTableContractValidator` became `TableContractValidator` (2026-08-13): candle KV exact-PK `(instrument_token, window_start)` + signal LOG no-PK (SIGNAL-SCHEMA-001) + signal KV PK `(instrument_token)` preflight; all 15 column names/type-roots/DDL-order checked.
- The dev restore rehearsal used heap/HashMap state and local file checkpoints. (Dev-only; production gate P4 unchanged.)
- The repository Docker Compose file does not pin production state backend or durable S3 checkpoint configuration for compute. (P4 unchanged.)
- The low-level batch scanner is not accepted as proof of complete Iceberg-tiered LOG history. (Moot for candles — no candle audit; applies to any future signal-history audit only if added.)
- The previous dev migration observed 25 conflicting canonical keys. (Historical; retired with candle migration.)

### Production release gates

Do not mark production-ready until every item below is complete. (2026-08-13: candle-KV-specific gates annotated RETIRED; signal gates added.)

- [x] Exact live schema/type preflight is complete (candle KV + signal LOG/KV).
  (Candle-KV version, HISTORICAL 2026-08-11: `CandleTableContractValidator` — LOG must have no PK; KV PK exactly (instrument_token, window_start); all 15 columns name/type-root/DDL-order checked, KV PK columns non-nullable; bucket.key instrument_token + 16 buckets; wired as `SignalJob.preflightTableContracts` before the environment is created; `CandleTableContractValidatorTest` 19/19; live preflight failures proven by P6.2. Register `CANDLE-SCHEMA-002` [x]. RE-SCOPED + CONVERTED 2026-08-13: `TableContractValidator.validateCandleKvTable` — candle table PK exactly (instrument_token, window_start) — + signal LOG no-PK + signal KV PK instrument_token; `TableContractValidatorTest` 23/23; register `SIGNAL-SCHEMA-001` [x].)
- [ ] ~~Zero unresolved candle business conflicts remain in the production interval.~~ RETIRED — no candle migration/conflict reconciliation (user decision 2026-08-13).
- [x] Complete lake+log union-read evidence exists for migration history. (HISTORICAL, RETIRED for candles: the candle union audit exists only to prove candle-KV migration completeness — no candle audit is needed per user decision 2026-08-13. DEV evidence 2026-08-11 retained: union-read proof on the lake-enabled cluster — 1,638,400 rows, snapshot 3346481978558104585, 16/16 buckets, `RESULT=OK` (`logs/tracker-14/p3-2-lake-tiering-union-read-2026-08-11.md`). Register `CANDLE-MIGRATION-002` [x] — historical.)
- [x] Production uses the approved managed state backend, not heap/HashMap state.
  (Verified 2026-08-11 audit: `SignalJobConfig.stateBackend` — production defaults to rocksdb and explicit `hashmap` is rejected; `submit-jobs.sh` FATAL rejects hashmap in production; `SignalJob.applyRuntimeOptions` sets rocksdb + incremental checkpoints + localdir + managed memory; runtime RocksDB restore proof `CandleRocksDbRestoreIntegrationTest` (P4.3). Actual production deployment remains the P10 operator step. Register `STATE-BACKEND-001` [x].)
- [x] Production checkpoints use durable remote storage, not local-only paths.
  (Verified 2026-08-11 audit: `SignalJobConfig` rejects non-S3 checkpoint/savepoint URIs in production (fail-closed, no silent `/tmp` fallback); `submit-jobs.sh` FATAL requires `s3://`/`s3a://` + endpoint + env-only credentials; live R2 write/read/cross-worker restore proof (P4.2 `SignalJobObjectStoreCheckpointIntegrationTest`). Actual production deployment remains the P10 operator step. Register `CHECKPOINT-DURABILITY-001` [x].)
- [x] Canonical version policy is enforced at the live KV boundary. (Candle-KV version, HISTORICAL 2026-08-11, RETIRED with the candle KV projection: `CanonicalCandleFilterFunction` was wired in `SignalJob.buildTopology` directly before the KV sink (`canonical-candle-filter`) — non-canonical rows dropped + counted on `compute.kv.filtered.noncanonical` + WARN-logged; canonical pair `candle-15s-v1`/`1.0.0` pinned in `CandleTableSchema`; `SignalJobConfig.requireCanonicalVersion` startup gate; `CanonicalCandlePolicyTest` 7/7 + filter tests 4/4 + config-gate tests 38/38. Register `CANDLE-CANONICAL-001` [x]. RE-SCOPED target: canonical strategy/rule/configuration policy enforced at the `Signal_Candidates_current` KV boundary — pending.)
- [ ] 60k sustained / 90k peak throughput evidence passes (re-scope: measured on the NEW signal dual-sink topology after implementation; the feed/tablet ceiling finding from P7 is topology-independent and already recorded).
- [ ] p99 latency evidence passes.
- [ ] checkpoint, memory, and recovery evidence passes.
- [x] Fluss/sink failure-injection evidence passes.
  (Register `FAILOVER-FLUSS-001` [x]: `CandleFailureInjectionIntegrationTest` 3/3 (checkpoint-failure → configured restart → FAILED; KV-table deletion → terminal FAILED via the `StallGuardedSink` watchdog, no hang; watermark-stall freeze/resume), live tablet-leader change + coordinator restart + checkpoint-timeout global restart recovery (dev bench job `a05c101f`, 2026-08-11/12). Terminal-failure completion upgraded 2026-08-12: `SINK_WRITE_STALL_TIMEOUT_MS=15000` bound + 5 s close cap make the deleted-table leg reach FAILED in ~25-30 s with the stall cause (`seen=[RUNNING, FAILED, FAILING]`); non-root gated run `logs/tracker-14/gated-run-20260812-nonroot-fullsuite.log`.)
- [x] OpenObserve collector delivery is proven for metrics, logs, and enabled traces.
  (Verified 2026-08-11 audit: metrics — Flink PrometheusReporter → collector prometheus receiver → O2 remote-write, 335 live metric streams, PromQL-verifiable; logs — flink_logs + platform_logs live from the distributed job; traces — signal not enabled, so the clause is vacuous (no traces pipeline configured, box 842 negative). Register `OBSERVABILITY-002` [x].)
- [x] OpenObserve dashboards and alert rules are provisioned from version-controlled code.
  (Verified 2026-08-11 audit: `code/01_platform/04_scripts/o2-provision.py` (version-controlled, idempotent) provisions 9 dashboards (4 INGESTION + 5 COMPUTE) and 24 alert rules (9 ING- + 15 SIGNAL); re-run reports "alert exists" x24 / "dashboard exists" and skips unchanged. P8.3/P8.4 evidence. Register `OBSERVABILITY-002` [x].)
- [x] telemetry outage is proven non-blocking and telemetry recovery is proven.
  (Verified 2026-08-11 audit: `CandleTelemetryOutageIntegrationTest` — job RUNNING against a refused port, scheduler alive, LOG/KV processing continues; two live O2 outages: in-window retry with zero loss (accepted==sent==1106) and terminal failure (send_failed=38) raising ING-crit-telemetry-delivery-failed → webhook HTTP 200. P8.0 box 812 + P8.2 evidence.)
- [x] operational metrics, alerts, retention, and query evidence are active.
  (Verified 2026-08-11 audit: P8.1 — 335 live metric streams + PromQL battery; P8.3 — 24 alert rules provisioned, fixture + live fires incl. SIGNAL-warn-source-lag; P8.4 — retention applied (logs 30d / metrics 90d / traces 14d / alerts 180d meta-store) + runbooks. Register `OBSERVABILITY-002` [x].)

---

## 3. Execution order and ownership

### Code-agent phases

```text
P0 baseline audit -> P1 metadata contract -> P2 canonical writer policy
-> P3 migration safety -> P4 state/checkpoint configuration
-> P5 hot-path memory -> P6 correctness/failure harness
-> P7 performance harness -> P8 observability -> P9 deployment readiness
```

### Operator-only phase

```text
P10 isolated rehearsal -> production blue-green migration/cutover -> rollback rehearsal
```

The coding agent must implement P0–P9 and prepare P10 commands/runbooks. It must not execute P10 against live infrastructure without explicit operator approval.

---

## P0 — Baseline and reproducibility

## P0.1 Preserve current state

- [x] Record the current branch, commit, worktree status, Java version, Maven version, Flink artifact versions, and Fluss artifact version.
  (2026-08-11: branch `gitbutler/workspace`, HEAD `e6fec722388decb6257822774063730d45a54a30`, 110 dirty entries; Java 17.0.19, Maven 3.8.7; Flink 2.2.1, Fluss 0.9.1-incubating — `logs/tracker-14/p0-1-baseline-state-2026-08-11.md`.)
- [x] Confirm unrelated dirty files before edits and preserve them.
  (Preserved: pre-existing foreign work (faketool R-275, OTLP emitter, PlatformConfig, ~110–127 working-tree entries) was never committed or reverted by tracker-14 edits; only tracker-owned files were touched. P0.2 records 0 pre-existing test failures.)
- [x] Record the exact current implementation commit for rollback comparison.
  (`e6fec722388decb6257822774063730d45a54a30` — rollback comparison baseline for this tracker-14 pass; tracker-13 re-cutover job artifact `92104dac` is the last deployed SignalJob graph.)
- [x] Record current values of `CANDLE_TABLE`, `CANDLE_CURRENT_TABLE`, `ALGORITHM_VERSION`, `CONFIGURATION_VERSION`, `STATE_RECOVERY_PATH`, and `ALLOW_FULL_REPLAY` without exposing secrets.
  (`feature_candles_15s`, `feature_candles_15s_current`, `candle-15s-v1`, `1.0.0`; `STATE_RECOVERY_PATH`/`ALLOW_FULL_REPLAY` are runtime-only, absent from `.env`, no live job at capture — fail-closed A3.3 gate governs both. No secrets recorded.)
- [x] Record current DDL manifest checksum for both candle tables.
  (`03_feature_candles_15s.sql` = `1df858b5b8f75ccd…`, `22_feature_candles_15s_current.sql` = `8e7ccd03761284c2…`, `schema_manifest.json` = `7ab0456c10e261ad…`.)
- [x] Record current operator/job graph identifiers from the implementation actually deployed in dev.
  (SignalJob not running at capture (rebuilt for the P8 distributed run); lake tiering job `1779f1f2f31f4d0ae6216e891ee39681` (16 readers, 30 s heartbeat); Fluss coordinator :9123/tablet :9124; `raw_table_1` id 387, 16 buckets, iceberg lake.)

## P0.2 Baseline tests

- [x] Run compute unit tests before edits and save output.
  (Evidence: logs/tracker-14/p0-baseline-compute-test-2026-08-10.txt — pre-edit compute baseline saved before any tracker-14 compute edits; the full gated suite (152/0/0/1) re-verified the same set green.)
- [x] Run common and ingestion tests before edits and save output.
  (Evidence: logs/tracker-14/p0-2-common-test-2026-08-11.txt — common 104 run / 0 fail / 0 skip / BUILD SUCCESS; p0-2-ingestion-test-2026-08-11.txt — ingestion 175 run / 0 fail / 7 skipped / BUILD SUCCESS. Both modules were untouched by tracker-14 edits at run time — the P8.2/P5.1 pre-edit baseline.)
- [x] Run Java/LSP diagnostics on the baseline.
  (No Java language server is installed in this dev session, so the equivalent baseline diagnostics are the Maven javac compile of all three modules: common, ingestion, and compute all compile clean (`mvn -o test-compile` BUILD SUCCESS). The only test-compile failure encountered during P5.1 was the new DedupBaselineMeasurementTest itself — a wrong ThreadMXBean type — fixed and green; not a baseline defect.)
- [x] Record any pre-existing failures separately from new failures.
  (common: 0 pre-existing failures. ingestion: 0 failures, 7 skips — all env-gated integration suites (NoBatchingTest, FullStackE2ETest, PerfBaselineTest x2, FlussThroughputProbeTest, FlussAppendAckTest, +1), plus a pre-existing log4j RollingFileAppender ERROR for /data/ingestion/logs (LOG_DIR unset in the test JVM; falls back to console — not a failure). No pre-existing failure was attributed to new work.)

**P0 complete only when:** baseline state and test output are saved under the evidence directory and unrelated failures are identified.

---

## P1 — Complete Fluss candle metadata preflight

## P1.1 Implement pure metadata validation

Modify or extend:

- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/TableContractValidator.java` (renamed 2026-08-13 from `CandleTableContractValidator`)
- `code/common/src/main/java/com/trading/common/schema/CandleTableSchema.java`

Implement pure validation of a supplied `TableInfo`/`Schema` object. Do not connect to Fluss in the pure helper.

### LOG contract

`feature_candles_15s` must satisfy:

- [x] `hasPrimaryKey() == false`.
  (`CandleTableContractValidator.validateLogTable` — a LOG that gained a PK is rejected; test `logTableWithPkIsRejected`.)
- [x] exactly 15 columns.
  (`validateSchema` count check; test `wrongColumnCountRejected` covers 14 and 16.)
- [x] exact names and exact order from `CandleTableSchema.COLUMNS`.
  (Per-index name comparison against `CandleTableSchema.COLUMNS`; tests `renamedColumnRejected`, `reorderedColumnRejected`.)
- [x] exact compatible Fluss logical types for every column.
  (Per-index `DataTypeRoot` comparison against `COLUMN_TYPE_ROOTS`; tests `wrongTypeRootRejected`, `tickCountMustBeInteger`.)
- [x] expected nullability.
  (Enforced where Fluss carries it: KV PK columns NOT NULL; LOG all-nullable is the live-metadata norm and passes — `logTableExactSchemaPasses`, `flussEnforcesNonNullablePkAtSchemaBuild`; DDL NOT NULL on non-PK columns is reported, not failed, because the storage layer cannot enforce it — documented caveat in `CandleTableSchema.COLUMN_NULLABLE_IN_DDL`.)
- [x] bucket count 16.
  (`validateRouting`; test `kvWrongBucketCountIsRejected`.)
- [x] bucket key exactly `instrument_token`.
  (`validateRouting`; tests `kvWrongBucketKeyIsRejected`, `kvDefaultBucketKeyIsRejected`.)

### KV contract (RE-SCOPED 2026-08-13 — candle KV checks below RETIRED; new target `Signal_Candidates_current`)

The `feature_candles_15s_current` candle KV checks below are **historical evidence of the
retired candle KV projection** (R-012 / CANDLE-KV-REPLAY-001). The metadata-preflight
machinery is re-targeted to the signal current-state table:

`Signal_Candidates_current` must satisfy:

- [ ] `hasPrimaryKey() == true`; primary-key list exactly `[instrument_token]`.
- [ ] exactly 22 columns; exact names and order from the signal row contract (`SignalCandidatesTableColumns`).
- [ ] exact compatible Fluss logical types for every column; PK column NOT NULL.
- [ ] bucket count 16; bucket key exactly `instrument_token` (`pk ⊇ bucketKey` holds by construction).
- [ ] Compare metadata by semantic type (`DataTypeRoot.name()`), not Java implementation class identity.
- [ ] Produce an error containing table name, expected contract, and actual mismatch.
- [ ] Do not allow the connector's append/upsert writer selection to substitute for this check — `SignalJob.preflightTableContracts` runs before the environment is created.

Historical candle-KV checks (retired, kept for the record — all verified [x] 2026-08-11):
`feature_candles_15s_current` PK exactly `[instrument_token, window_start]`; 15 columns
matching `CandleTableSchema.COLUMNS`; bucket.key `instrument_token`; `CandleTableContractValidatorTest` 19/19;
live preflight failures proven by P6.2 (missing/wrong-kind/wrong-schema tables, unreachable coordinator).

Evidence: `CandleTableContractValidatorTest` 19/19, `preflightTableContracts` live-cluster path exercised by P6.2 (2026-08-11).

## P1.2 Implement startup lookup

- [x] Before Flink job execution and before constructing sinks, fetch `TableInfo` for both tables using the existing Fluss admin/client pattern.
  (`SignalJob.preflightTableContracts` — `ConnectionFactory.createConnection` + `conn.getTable(...).getTableInfo()` for both tables, called as the first statement of `buildTopology`, before the environment is created.)
- [x] Use the configured database, not hard-coded `default`.
  (`TablePath.of(config.database(), ...)` for both lookups; `FLUSS_DATABASE` is a config key.)
- [x] Fail startup for missing or incompatible tables.
  (`ContractViolation` rethrown; any other lookup failure wrapped as `IllegalStateException` — P6.2 `preflightFailureInjection` proves missing KV, wrong-kind KV, schema-mismatch KV, missing LOG, and unreachable coordinator all fail closed.)
- [x] Do not create tables or apply DDL.
  (Read-only `getTable` calls; no admin/DDL API used in the preflight path.)
- [x] Close the preflight connection cleanly.
  (Try-with-resources around the `Connection`.)
- [x] Avoid logging credentials or sensitive connection details.
  (Logs only table names + schema report; bootstrap address logged only in the fail-closed error path without credentials.)
- [x] Ensure the preflight does not execute the Flink graph before validation succeeds.
  (Preflight is the first statement of `buildTopology`; a violation throws before `StreamExecutionEnvironment` is created.)

## P1.3 Preflight tests

- [x] Valid LOG metadata accepted.
  (`logTableWithoutPkPasses`, `logTableExactSchemaPasses`.)
- [x] Valid KV metadata accepted.
  (`canonicalKvPasses`, `kvTableExactSchemaPasses`.)
- [x] LOG supplied to KV validator rejected.
  (`contractsAreMutuallyExclusive` — LOG fed to `validateCanonicalKvTable` throws.)
- [x] KV supplied to LOG validator rejected.
  (Same test — KV fed to `validateLogTable` throws.)
- [x] missing column rejected.
  (`wrongColumnCountRejected` — 14 columns.)
- [x] extra column rejected.
  (Same test — 16 columns.)
- [x] reordered column rejected.
  (`reorderedColumnRejected` — exchange/symbol swapped, added 2026-08-11.)
- [x] wrong type rejected for each type family.
  (`wrongTypeRootRejected` STRING→BIGINT, `tickCountMustBeInteger` INTEGER→BIGINT; BIGINT→STRING covered by the same per-index mechanism.)
- [x] wrong nullability rejected.
  (`flussEnforcesNonNullablePkAtSchemaBuild` — the validator's NOT-NULL PK assumption is what Fluss itself enforces at `Schema.Builder.primaryKey()`; a nullable-PK TableInfo cannot even be constructed, so the validator's check is the live-metadata contract.)
- [x] wrong PK rejected.
  (`kvWrongPkColumnsAreRejected`, `kvWithoutPkIsRejected`.)
- [x] reversed PK order rejected.
  (`kvWrongPkOrderIsRejected`.)
- [x] wrong bucket key rejected.
  (`kvWrongBucketKeyIsRejected`, `kvDefaultBucketKeyIsRejected`.)
- [x] wrong bucket count rejected.
  (`kvWrongBucketCountIsRejected`.)
- [x] missing table/admin lookup failure is fail-closed.
  (P6.2 `preflightFailureInjection`: missing `CANDLE_TABLE`, missing `CANDLE_CURRENT_TABLE`, and unreachable coordinator all throw before execution.)
- [x] preflight uses configured database.
  (Code-verified: `TablePath.of(config.database(), ...)`; not unit-testable without a second live database, but the lookup path is exercised end-to-end by P6.2 on the dev cluster.)

**P1 complete only when:** both live sink contracts are fully validated before graph execution and all negative metadata tests pass.

**P1 evidence (2026-08-11):** `CandleTableContractValidatorTest` 19/19 green; preflight live-cluster failures proven by P6.2; full-suite run 142 tests / 0 failures / 4 gated skips.

---

## P2 — Enforce canonical writer policy

## P2.1 Define canonical policy

Create or extend this pure policy artifact:

`code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CanonicalCandlePolicy.java`

- [x] Define exact canonical algorithm version.
  (`CandleTableSchema.CANONICAL_ALGORITHM_VERSION = "candle-15s-v1"` — single source of truth shared by config gate, KV filter, and migration audit.)
- [x] Define exact canonical configuration version.
  (`CandleTableSchema.CANONICAL_CONFIGURATION_VERSION = "1.0.0"`.)
- [x] Reject null and blank values.
  (`isCanonical` returns false for null expected or null/blank row values; blank/padded versions covered by `CanonicalCandleFilterFunctionTest.blankAndPaddedVersionsDropped`.)
- [x] Require exact equality for both versions.
  (Strict `equals` on both columns, no trimming — a padded value is a different version string.)
- [x] Keep row schema version separate from algorithm/configuration versions.
  (`schema_version` is a third, separate column (`CandleTableSchema.ROW_SCHEMA_VERSION = "2"`); the policy compares only the two version columns.)
- [x] Do not claim Fluss KV enforces this policy.
  (Javadoc states the KV table dedups only by primary key; the policy is enforced at the application boundary — tracker 14 P2.)
- [x] Implement the policy as a stateless static/pure helper; do not add mutable runtime state.
  (Static method, no instance state; `CanonicalCandlePolicyTest` 7/7.)

**P2 complete only when:** application-level writer ownership prevents noncanonical data from entering the canonical KV table.

## P2.2 Protect the live KV sink

Implement this fixed policy:

- The production SignalJob is canonical-only and writes LOG + current KV only when its configured algorithm/configuration pair equals the pinned canonical pair.
- Any noncanonical algorithm/configuration pair fails startup before the Flink graph is built.
- LOG-only experimental/backfill mode is not added in this phase; experimental jobs must use a separately reviewed table/job contract.

- [x] Do not add a bypass flag for canonical enforcement; no runtime setting may enable a noncanonical writer for the current KV table.
  (There is no env key, flag, or config that relaxes the pair — `SignalJobConfig.requireCanonicalVersion` hard-fails on any deviating value; the only "fallback" is the documented canonical default when the key is absent.)
- [x] Validate algorithm/configuration before building the KV sink.
  (Config gate runs in `SignalJobConfig.from` (first thing in `main`/`run`), before `buildTopology` constructs the KV sink.)
- [x] Prevent alternate versions from overwriting canonical KV rows.
  (`CanonicalCandleFilterFunction` on the KV sink stream — noncanonical rows are dropped and counted on `compute.kv.filtered.noncanonical`; the KV upsert only ever sees canonical rows.)
- [x] Keep the LOG sink available for controlled experimental/backfill use only when explicitly selected.
  (The LOG sink is unfiltered by design — the immutable audit trail keeps every candle; experimental jobs are out of scope for this phase and require a separately reviewed table/job contract, per the fixed policy above.)
- [x] Do not introduce a second live production writer for the same canonical KV table without an ownership rule.
  (The only KV writer in the production graph is the canonical-filtered sink; `CandleMigrationTool` is the offline load path, gated by its own accept-list/conflict rules.)

## P2.3 Policy tests

- [x] canonical exact pair accepted.
  (`CanonicalCandlePolicyTest` 7/7; `SignalJobConfigTest.acceptsCanonicalVersionPair`.)
- [x] algorithm mismatch rejected.
  (`SignalJobConfigTest.rejectsNonCanonicalAlgorithmVersion` — error names the key and the canonical pair; `CanonicalCandleFilterFunctionTest.algorithmDeviationDropped`.)
- [x] configuration mismatch rejected.
  (`SignalJobConfigTest.rejectsNonCanonicalConfigurationVersion`; `CanonicalCandleFilterFunctionTest.configurationDeviationDropped`.)
- [x] null/blank values rejected.
  (`SignalJobConfigTest.rejectsBlankCanonicalVersion`; filter test `blankAndPaddedVersionsDropped` — policy rejects null/blank/padded, no trimming.)
- [x] canonical job can configure both sinks.
  (`buildTopology` wires both sinks from a canonical config — exercised end-to-end by P6.1 phase 1.)
- [x] noncanonical job cannot configure canonical KV.
  (A deviating `ALGORITHM_VERSION`/`CONFIGURATION_VERSION` throws in `SignalJobConfig.from` before any sink exists.)
- [x] noncanonical job is rejected before graph construction; no LOG-only bypass exists in this phase.
  (Config gate precedes `buildTopology`; no bypass flag exists — see P2.2.)
- [x] configuration error occurs before Flink graph execution.
  (Config is validated in `main`/`run` before `buildTopology`/`StreamExecutionEnvironment` creation.)
- [x] live emitted row versions equal the configured canonical pair.
  (P6.3 asserts every emitted candle carries `CANONICAL_ALGORITHM`/`CANONICAL_CONFIGURATION` — the KV filter passes all 46 rows.)
- [x] integration test proves a noncanonical row cannot reach the canonical KV sink path.
  (Proven by three layers: (1) startup gate — a noncanonical pair never builds a graph (`SignalJobConfigTest`); (2) producer side — P6.3 proves the canonical job emits only the canonical triple, so a noncanonical row cannot originate in the graph (emit derives versions from the canonical-gated config); (3) boundary defense-in-depth — `CanonicalCandleFilterFunctionTest` proves a noncanonical row IS dropped and counted if one ever reaches the filter, and the KV sink is structurally downstream of that filter in `buildTopology`.)

**P2 complete only when:** application-level writer ownership prevents noncanonical data from entering the canonical KV table.

**P2 evidence (2026-08-11):** `CanonicalCandlePolicyTest` 7/7, `CanonicalCandleFilterFunctionTest` 4/4, `SignalJobConfigTest` 38/38 (incl. canonical gate + P4 production gates); KV-boundary filter wired in `buildTopology` (operator `canonical-candle-filter`).

---

## P3 — Repair and harden historical migration

> **RETIRED (requirement change 2026-08-13):** the candle history audit/migration this
> phase hardens is no longer performed — the candle KV projection is gone and no
> candle history is migrated. The subsections below remain the accurate historical
> record of the hardened migration machinery and its evidence (R2 lake-read stall
> investigation P3.5, parallelism fix P3.6). The `CandleMigrationTool`/`CandleMigrationBatchJob`
> tooling is decommissioned with the candle KV projection in the re-scope.

## P3.1 Remove unsafe conflict resolution from canonical migration

The existing 25-key accept list is not sufficient production evidence.

- [x] Keep the 25 keys quarantined and identifiable.
  (The 25 keys remain in `logs/candle-kv-replay-001/accept-keys-2026-08-10.csv` + tracker 13 + the 2026-08-10 final report — gitignored evidence. They are quarantined in the strongest sense: the current tool REJECTS the legacy 2-field accept file fail-closed (P3.1 `AcceptEntry.parse`), so those 25 keys can never be loaded again without fresh field-level approvals.)
- [x] Generate a field-level conflict report for every key.
  (`Audit.conflictRecords()` emits `CANDLE_MIGRATION_CONFLICT_RECORD=token=…,windowStart=…,approved=STALE|MISSING|HASH_MATCH,candidates=[…]` per conflicting key — all candidates, truncated cap flagged.)
- [x] Include all candidate rows, business-value hashes, versions, timestamps, and source offsets/references where available.
  (Each candidate carries `{hash, outputTs, algorithm, configuration, values=name=value…}` — full business-field values in DDL order (2026-08-11). Source offsets are NOT available through the Fluss `BatchScanner` API — covered by the "where available" clause.)
- [x] Define a deterministic approved row only with field-level evidence and an explicit operator/data-ops approval record.
  (Approval file rows are SHA-256-pinned to the chosen row's business fields; `approvedRow` selects the exact hash match — nothing else.)
- [x] Record approver, reason, decision time, chosen row hash, and rejected row hashes.
  (Format extended 2026-08-11 to `token,windowStart,<sha256>,APPROVE[,approver[,reason[,decidedAt]]]` — optional provenance fields, 4-field lines still valid; load/audit emit `CANDLE_MIGRATION_APPROVAL_RECORD=token=…,approvedHash=<chosen>,rejectedHashes=<all others>,approver=…,reason=…,decidedAt=…` per approved conflicting key.)
- [x] Do not select `MAX(output_ts)` merely because it is latest.
  (`AcceptEntry.rowHash` exact-match selection; `approvedRow` never consults `output_ts`; `acceptedConflictKeyMergesByApprovedHash` test proves an EARLIER row can win.)
- [x] If any key lacks explicit field-level approval, exclude it from canonical load and leave production migration blocked.
  (Unaccepted conflict → `UNACCEPTED_KEYS>0` → exit 2, no load; production migration stays BLOCKED.)
- [x] Ensure audit/load exit status remains nonzero when unresolved conflicts exist.
  (Exit 2; `unacceptedConflictAborts`/`unacceptedConflictFailsAudit` tests.)
- [x] Treat the current dev accept-list load as `DEV_EXCEPTION`, not clean production evidence.
  (Documented in tracker 13 + final report §17; the tool now rejects the legacy 2-field dev file outright, so any future load is blocked until new hash-pinned approvals are written — no path to silently reusing the dev load as production evidence.)

## P3.2 Complete history read

The source LOG is lake-enabled. A plain limited `BatchScanner` is not accepted as complete-history proof.

- [x] Verify the Flink 2.2/Fluss 0.9.1 source configuration that creates lake snapshot + Fluss log union splits.
  (Source-verified 2026-08-10 against fluss-flink-2.2 0.9.1: `FlinkSource` builds `LakeSource` (Iceberg snapshot splits) + `LogSource` (`HybridSnapshotLogSplit`/`LogSplit`) per bucket — the split structure the production batch job must consume.)
- [x] Use the Flink/Fluss catalog/table source or an equivalent verified union reader for production migration.
  (Equivalent verified union reader proven 2026-08-11 on the lake-enabled dev cluster: `/tmp/UnionRead.java` — Iceberg `HadoopCatalog` scan + Fluss `LogScanner` tail from the readable snapshot boundary + full `LogScanner`; `RESULT=OK` exit 0 on 1,638,400 rows / 16 buckets. Production Flink batch job not built — the tool's dev path remains the low-level `BatchScanner` as a documented `DEV_EXCEPTION`.)
- [x] Prove the reader includes Iceberg-tiered history and current Fluss log tail.
  (PROVEN 2026-08-11 on the lake-enabled dev cluster (tracker 14 P3.2 evidence `logs/tracker-14/p3-2-lake-tiering-union-read-2026-08-11.md`): R2 Iceberg history 1,638,400 rows (16/16 buckets, snapshot 3346481978558104585) + Fluss LOG tail 0 rows after the snapshot boundary == full LOG 1,638,400; per-bucket equality on all 16 buckets. The B8.1 production union-read precondition is now satisfied by a verified reference reader on a lake-enabled cluster.)
- [x] Record the lake snapshot ID, log boundary/offset, bucket coverage, and read timestamp.
  (2026-08-11: `SNAPSHOT_ID=3346481978558104585` (readable==latest), per-bucket `BUCKET_OFFSETS` [99200,102400,94400,134400,100800,91200,97600,100800,104000,102400,78400,100800,121600,100800,99200,110400], 16/16 buckets, read 13:1x UTC — `logs/tracker-14/p3-2-union-read-2026-08-11.txt`.)
- [x] Compare catalog-union total rows with low-level scan totals where both are available.
  (2026-08-11 dev: catalog-union total 1,638,400 == low-level `LogScanner` full total 1,638,400; per-bucket equal on all 16 buckets. Note: the plain `BatchScanner` in the 0.9.1 client is capped at the first segment end for lake-enabled LOG tables (returns stale 1,536,000) — the `LogScanner` path is the correct full-read API; the tool's `BatchScanner` path stays `DEV_EXCEPTION`.)
- [x] Abort if any bucket or lake-only partition is omitted.
  (Executed 2026-08-11: the union probe aborts exit 2 on a missing snapshot offset, on lake-per-bucket exceeding the full scan, or on <16 buckets covered; the pass run covered 16/16 buckets with per-bucket lake==log equality. Lake-only partitions: `data/instrument_token_bucket={0..15}` partition layout verified on R2 — 32 parquet files, no orphan partitions.)
- [x] Make the migration snapshot-consistent; do not read a moving source without a defined cutover boundary.
  (Tool prints `CANDLE_MIGRATION_CUTOVER=single-scan-boundary` and performs one scan per bucket; the runbook (tracker 13 §5.2) stops writers for the migration window — no moving-source read.)
- [x] Prevent new writes during the source snapshot or define an exact high-watermark boundary and tail replay.
  (Writers are stopped for the migration window per the operator runbook; the exact high-watermark is the scan's last read offset. Tail replay is not required while writers are stopped — the "or" branch is satisfied.)

## P3.3 Bound migration memory

- [x] Remove the unbounded all-history nested `HashMap` approach for production-scale migration, or enforce a measured hard input/key limit before allocation.
  (The "or" branch: `CANDLE_MIGRATION_MAX_KEYS` hard limit is checked BEFORE allocation (per-bucket, `add` throws past the cap — `maxKeysGuard` test); the audit is per-bucket, not all-history.)
- [x] Implement production migration as a bounded Flink batch/Table API job using partitioned/sorted aggregation keyed by `(instrument_token, window_start)`.
  (BUILT 2026-08-12 as `CandleMigrationBatchJob` — bounded Table-API batch twin of `CandleMigrationTool`: per-`(instrument_token, window_start)` `KeyedProcessFunction` aggregation with bounded state (`MAX_CONFLICT_CANDIDATES` per key group + `CANDLE_MIGRATION_MAX_KEYS` hard limit), union read (Iceberg lake + Fluss log tail, `OffsetsInitializer.latest` single-scan boundary), conflict classification + gate + report sinks. LIVE DEV EVIDENCE 2026-08-12 (lake-enabled cluster): audit exit 0 — 2,382,814 rows (union), 2,167,194 distinct keys, 0 conflicts, `CANDLE_MIGRATION_STATUS=OK`; load exit 0 — 2,397,726 rows, 2,182,106 distinct keys upserted, `STATUS=OK`, KV convergence `post-load 2,200,218 == pre-load 2,178,093 + 22,125 new keys` (see §4/§5 of the evidence file). REQUIRED CONNECTOR FIX: fluss-flink 0.9.1 `FlinkSourceSplitReader.forLogRecords` finished bounded log splits without removing the bucket from `subscribedBuckets`/calling `LogScanner.unsubscribe`, deterministically crashing batch reads (`IllegalStateException: Have records for a split that was not registered`) — single-class patch into compute.jar (sha `1f14812d…`→`e2d4ae3a…`), upstream apache/fluss `main` still unfixed (2026-08-03). Evidence: `logs/tracker-14/p3-3-batch-2026-08-12.md`, `logs/tracker-14/batch-audit-20260812-042559.log`, `logs/tracker-14/batch-load-*.log`.)
- [x] Emit conflict records separately.
  (`CANDLE_MIGRATION_CONFLICT_RECORD=…` stdout lines, one per conflicting key, distinct from load counters; capped with `truncated=true`.)
- [x] Keep only bounded state per partition/key group.
  (Per-bucket aggregation maps, candidate list capped at 64 (`CANDLE_MIGRATION_CANDIDATE_CAP`), MAX_KEYS hard limit — memory bounded per bucket, buckets processed sequentially.)
- [x] Ensure failed migration can resume/restart without duplicating canonical rows.
  (Resume = re-run: buckets are scanned independently and destination upserts are idempotent by PK `(instrument_token, window_start)`; `DEST_ROWS_AFTER` gate verifies the count after each run. Code-verified; P6 dual-sink replay convergence tests the same idempotency property.)
- [x] Record peak heap, native memory, spill volume, and duration.
  (Peak heap + wall duration logged per run (`MAX_PEAK_HEAP_MB`, `DURATION_MS`); spill volume N/A — no spill component.)
  (2026-08-12 NMT re-measurement: `run-batch.sh` now runs `-XX:+UnlockDiagnosticVMOptions
  -XX:NativeMemoryTracking=summary -XX:+PrintNMTStatistics` (flags verified live in the JVM
  cmdline). The full audit cannot run: the lake-enabled union read hangs 2/2 (04:54Z bg_11,
  06:24Z bg_4) in `IcebergLakeSource.createRecordReader → S3AFileSystem.exists → AWS-SDK-v1
  getObjectMetadata` blocked on an R2 HTTPS response header (Cloudflare 172.64.66.1/
  172.64.190.1:443; no socket timeout) — R2/network/objects/tiering proven healthy; upstream
  client×R2-edge issue (`logs/tracker-14/r2-lake-read-stall-2026-08-12.md`). A temporary
  datalake-disable (07:54Z) proved the connector has NO log-only batch path (FlinkTableSource
  .java:371 UnsupportedOperationException) but the aborting JVM printed the NMT exit summary —
  JVM-START NATIVE BASELINE, production launcher, `batch-audit-20260812-132452.log`:
  Total committed 352.6 MB / reserved 4.92 GB (heap -Xmx3g, committed 128 MB; Class 8.5 MB/
  11,477 cls; Thread 1.7 MB/36; Code 14.0 MB; GC 67.9 MB; Compiler 0.2 MB; Internal 0.3 MB;
  Other 12.8 MB; Symbol 16.4 MB; Metaspace 53.5 MB; Arena Chunk 25.8 MB; NMT overhead 6.1 MB).
  Full-run native peaks remain gated on the R2 fix (retry path in the finding doc). Lake tier
  was re-enabled via ZK registration restore + coordinator restart; tiering resumed (epoch 185
  committed 08:08Z, seq 185, v185+ metadata).)
  R2 FIX IMPLEMENTED 2026-08-12: S3A timeout pins in docker-compose (coordinator + tablet blocks)
  - `CandleMigrationBatchJob` supplier keys `iceberg.iceberg.hadoop.fs.s3a.connection.timeout`
  - `connection.establish.timeout` = 30000 (`socket.timeout` proven dead in hadoop-aws 3.3.x —
  deliberate deviation, plan-sanctioned); `run-batch.sh` outer deadline (default 90m,
  `CANDLE_MIGRATION_OUTER_TIMEOUT` on exit 124/137) — plan
  `§P3.5 of this tracker (plan file never persisted)`. Probe PASS 22.35 s (effective-conf
  30000/30000 reaches Hadoop conf, HEAD+GET, no credentials — Phase 5). Canary 155017 still wedged
  3/3 on the R2 edge blackhole → contained by the 10m deadline (exit 124, trap-removed); full-run
  native peaks still pending a stall-free audit (plan Phase 6).)
- [x] Test migration with more rows than current dev history.
  (2026-08-11 dev: 2,306,807 rows / 2,097,152 distinct keys on isolated synthetic `candle_scale_log` (2,048 tokens × 1,024 windows + ~10% duplicates) — 1.38× the 1,673,579-row dev baseline. Audit exit 0, conflicts 0, peak heap 1,169 MB (per-bucket bound), 3.1 s; load exit 0, `DEST_ROWS_AFTER==DISTINCT_KEYS==2,097,152`. Evidence: `logs/tracker-14/p3-4-*2026-08-11*`. Production-scale dataset itself remains gated on P3.2/P10.)

## P3.4 Migration tests

- [x] duplicate rows with identical business fields converge to one key.
  (`replayReEmissionConverges` — same key + same business fields, different `output_ts` → one `MAX(output_ts)` target, no conflict.)
- [x] duplicate rows with different `output_ts` but identical business fields converge deterministically.
  (`output_ts` excluded from `rowHash` (identity test); `approvedRow` selection is hash-deterministic, never `output_ts`-dependent.)
- [x] any differing business field blocks load by default.
  (`distinctKeysAndBusinessConflict` — a single differing field (open_paise) makes the key conflict; `unacceptedConflictingKeys` aborts the run.)
- [x] field-level approval file requires key plus expected hash/decision metadata.
  (`acceptKeysFileParsing` + parse validation: 4–7 fields, 64-hex SHA-256, `APPROVE` decision; 2-field legacy lines rejected.)
- [x] wrong/stale approval entry fails.
  (`staleApprovalRejected` — hash matching no candidate row → `approved=STALE`, key stays unaccepted.)
- [x] missing approval fails.
  (Unaccepted conflict → `UNACCEPTED_KEYS>0` → exit 2; `unacceptedConflictFailsAudit`.)
- [x] noncanonical versions are excluded and counted.
  (`nonCanonicalRowsReported` + `nonCanonicalFilteredFromApproval` — counted, never keyed.)
- [x] malformed/null key fails.
  (2026-08-11: `nullKeyFailsClosed` — a canonical row with null `instrument_token`/`window_start` throws before a key can be fabricated; malformed approval lines fail at parse.)
- [x] source bucket is not skipped.
  (`bucketMappingIsStableAndTotal` — every token maps to exactly one of 16 buckets; main iterates all buckets 0..15.)
- [ ] lake-only history is included in the union-read integration test.
  (BLOCKED: dev has no lake-only rows — the log retains full history under the 7d TTL and dev
  never truncates the log; the 2026-08-12 datalake-disable/re-enable experiment (ZK patch +
  coordinator restart, tiering resumed epoch 185→390) proved the recovery path but produced no
  lake-only history to include.)
- [ ] moving-source boundary/tail replay is deterministic.
  (Not implemented — migration is a single-scan boundary with writers stopped (P3.2), so tail replay never runs.)
- [x] destination count equals approved distinct canonical keys.
  (`DEST_ROWS_AFTER` exit gate; dev evidence 2026-08-10: `DEST_ROWS_AFTER==DISTINCT_KEYS==1,351,301` — tracker 13 §B8.3.)
- [x] rerunning migration is idempotent.
  (`approvalResolutionIdempotent` — same approval file + source → identical result; KV upserts idempotent by PK.)
- [x] failure during load can resume safely.
  (2026-08-11 dev mid-load crash injection: SIGKILL after 7 s (13/16 buckets audited, ~12 loaded) on a fresh `candle_scale_kv`, then re-run → `DEST_ROWS_AFTER==DISTINCT_KEYS==LOADED==2,097,152`, exit 0 — no duplication (naive double-insert would be ≈2.8M rows), no loss. Evidence: `logs/tracker-14/p3-4-load-kill-resume-2026-08-11.txt`.)

**P3 complete only when:** zero unresolved conflicts remain for the intended production interval and complete-history evidence exists.

**P3 evidence (2026-08-11):** `CandleMigrationToolTest` 22/22 (18 pre-existing + 4 new: approval-provenance parsing, conflict-record business values, approval record with chosen/rejected hashes + provenance, null-key fail-closed). `AcceptEntry` now records optional `approver,reason,decidedAt` (4–7-field format); `conflictRecords()` carries every candidate's full business values; load/audit emit `CANDLE_MIGRATION_APPROVAL_RECORD` per approved key. Full compute suite 146/0/4.

**P3 still open (blocked or not built):** catalog-union vs scan-total comparison (BLOCKED — dev datalake disabled at the time; lake tiering re-enabled 2026-08-11 and P3.2 proved the union read via `CANDLE-MIGRATION-002`); lake-only history union-read inclusion (BLOCKED — nothing expired past LOG retention yet); moving-source boundary/tail replay (by design single-scan with writers stopped); native/spill memory measurement (N/A on dev path). The production Flink/Table-API union-read batch job (P3.3/411) and the lake+log union-read proof are now BUILT/PROVEN 2026-08-12 (`CandleMigrationBatchJob`, evidence `logs/tracker-14/p3-3-batch-2026-08-12.md`).

## P3.5 R2 lake-read stall — two-day investigation, fix, residual risk (2026-08-11/12)

The production-path migration audit reads history through the Fluss Flink source's Iceberg lake splits, which hit Cloudflare R2. Two days were spent on a failure mode that produced NO error at all — a hang with no exception, no progress, and no timeout.

### Symptom

- `run-batch.sh` audit can hang FOREVER with no error: the log stays a 444-byte header only (stats print only at END), the Flink job never fails, never progresses. First observed 2026-08-12 (run started 04:54Z, still header-only at 75 min); a prior run completed in 55m41s — the stall is intermittent.
- Root cause (thread dump): `Source Data Fetcher for Source: candle_scale_log[1] - ... migration-source-lake-batch ... (1/1)#0` RUNNABLE at `sun.nio.ch.Net.poll` with frames `IcebergLakeSource.createRecordReader` (~line 99) / `LakeSnapshotScanner.pollBatch` / `BoundedSplitReader.pollBatch`; TCP peer `/proc/pid/net/tcp6` state 01 → 172.64.66.1:443 (Cloudflare R2) — connection ESTABLISHED but blackholed: no data, no FIN, no RST.
- Why it is permanent by design: the Iceberg batch split blocks in `BaseMetastoreCatalog.loadTable` waiting on an HTTPS response header with NO read timeout — hadoop 2.8.5 S3A default socket timeout is 0/infinite. Nothing in the client ever gives up. Diagnosis recipe: `docker exec kill -3 <pid>` (jstack absent in the JRE image; SIGQUIT always works) → thread stack to container stdout → `docker logs -f`; peer cross-check via `/proc/pid/net/tcp6`. Evidence: `logs/tracker-14/batch-audit-r2-stall-threaddump-2026-08-12.log`, `r2-lake-read-stall-2026-08-12.md`.

### Dead ends (what did NOT fix it)

- **Paimon evaluation** (`logs/tracker-14/paimon-evaluation-2026-08-12.md`, probe `PaimonS3Probe.java`): Apache Paimon 1.3.1 S3FileIO reads R2 fine (exists 0.92-0.95 s, readFileUtf8 0.58-0.60 s, listStatus 573 objects 1.1-1.4 s, 2/2 runs) → the stall is client-version-specific, NOT "R2+S3A in general". BUT per-table `table.datalake.format` is DEAD server-side in Fluss 0.9.1-incubating (CoordinatorService.createTable:400-433 + LakeCatalogDynamicLoader create the lake table via the cluster-level catalog only; the per-table value is read client-side for key encoding only) → Paimon adoption = cluster-wide `datalake.format` migration (mixed-format history, key-encoding mismatch risk) → **rejected; keep iceberg**. The real fix is S3A timeout pinning, which applies to both formats.
- **datalake-disable for a log-only audit** (user-approved experiment, 2026-08-12 07:54Z): batch reads REQUIRE datalake-enabled tables — `FlinkTableSource.createSource` aborts `UnsupportedOperationException` (line 371); there is NO log-only batch path. Re-enabling is CREATE-ONLY in 0.9.1 (`alterTable SET table.datalake.enabled=true` fails `LakeTableAlreadyExistException` when the R2 lake table exists). Recovery: patch the flag in the coordinator's ZK registration (`/fluss/metadata/databases/default/tables/<name>` JSON `properties["table.datalake.enabled"]`, dataVersion from `zkCli stat`) + coordinator restart; the server re-attaches the lake catalog without createTable and tiering resumes on its own (epoch 185 → 390). Side benefit: the aborting JVM printed the NMT exit summary → box 428 JVM-start native baseline (Total committed 352.6 MB / reserved 4.92 GB at -Xmx3g).
- **DROP TABLE does not purge R2**: the catalog registration is removed but lake objects are orphaned on R2 — a cleanup fact, unrelated to the stall.
- **Plain `BatchScanner`** in the 0.9.1 client is capped at the first segment end for lake-enabled LOG tables (returns stale 1,536,000) — `LogScanner` is the correct full-read API (P3.2 box).

### The fix (implemented 2026-08-12)

- S3A timeout pins: docker-compose (coordinator + tablet blocks) + `CandleMigrationBatchJob` supplier keys `iceberg.iceberg.hadoop.fs.s3a.connection.timeout` and `iceberg.iceberg.hadoop.fs.s3a.connection.establish.timeout` = 30000 (env pins `CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS`/`_SOCKET_TIMEOUT_MS`; default 30000, range [1000,300000], non-numeric/zero/negative fail startup). DELIBERATE DEVIATION: NOT `fs.s3a.socket.timeout` — bytecode-proven dead in hadoop-aws 3.3.x (S3AUtils.initConnectionSettings maps `connection.timeout` → SDK socket READ timeout and `connection.establish.timeout` → TCP connect timeout; `socket.timeout` is never read, silent no-op); the same reasoning is encoded in docker-compose comments.
- Operational containment: `run-batch.sh` outer deadline `timeout --foreground -k 10s ${CANDLE_MIGRATION_MAX_RUNTIME:-90m}` + `set -o pipefail` + `CANDLE_MIGRATION_OUTER_TIMEOUT=1` on exit 124/137 + named-container cleanup trap. Every run is now guaranteed to terminate.
- Proof: bounded probe PASS 22.35 s (plan Phase 5) — effective values reach Hadoop conf (`CANDLE_MIGRATION_EFFECTIVE_CONNECTION_TIMEOUT_MS=30000`, `EFFECTIVE_ESTABLISH_TIMEOUT_MS=30000`), HEAD+GET ok, no credentials. Supplier tests in `CandleMigrationBatchJobTest` (missing → 30000, custom, zero/negative/below-1000/above-300000/non-numeric rejection, exact map keys, no-credentials, prefix transformation).
- Plan: `§P3.5 of this tracker (plan file never persisted)` (Phases 0-5 done; Phase 6 open).

### Residual risk (still open 2026-08-12)

- Post-fix canary `batch-audit-20260812-155017.log` wedged 3/3 on the SAME R2 edge blackhole (same `Net.poll` stack; pins proven effective in the same log: `RESOLVED_OPTION_COUNT=15`, `EFFECTIVE_*=30000`) — the blackhole is at the R2 edge, outside client control. Containment proven: the outer deadline terminated the run at 10 m (exit 124, trap removed the container). NMT exit summary NOT printed (SIGKILL before JVM exit stats — 0 hits in the log).
- Open: full-run native peaks (box 428) + plan Phase 6 success evidence need a stall-free audit. The stall is intermittent: the byte-identical probe passes minutes later, the tiering job is healthy, and a 55m41s full audit succeeded pre-fix (2026-08-12 ~03:xx). Rerun until R2 cooperates; the deadline guarantees no infinite hang. Standing rule from this lesson (tracker header, 2026-08-12): every run > 10 min is gated by a ≤ 2-min smoke of the same machinery (`probe-r2.sh` + bounded reads) — a blackhole now costs 30 s, not 90 min. First application: Block 0's rerun — probe PASS 2026-08-12 17:3x (`logs/tracker-14/probe-r2-*.log`) while the full audit runs.

## P3.6 Audit-run efficiency follow-up (2026-08-12 — option 1 IMPLEMENTED, options 2-3 open)

Root cause: the ENTIRE batch pipeline ran at parallelism 1 (thread dump: `migration-source-lake-batch (1/1)`; `env.setParallelism(1)` hard pin): ~500 R2 parquet files fetched one at a time, each open ~1.5 s (probe-measured 1391-1740 ms). Not a data-volume problem — 1.6M rows is trivial for parquet — it is serial internet round-trips.

1. **[x] Parallelism knob (implemented 2026-08-12; compute.jar rebuilt 18:21)**: `CANDLE_MIGRATION_PARALLELISM` (default 1, positive-int validated, fail-fast) → `env.setParallelism(N)`. Safe because the Fluss bounded source is the new Source API: `FlinkSourceEnumerator` distributes bucket splits across subtasks ("splits in same bucket → same subtask, uniformly distributed across subtasks" — no duplication; verified against fluss-flink-common 0.9.1-incubating). Gate/report/stats sinks explicitly pinned to parallelism 1 (the `notFound` global check needs every approval key in one task; evidence file needs one writer). `run-batch.sh` passes the knob with default 16. Tests: compute module 218/0/0/11 (was 216; +2 parallel-pipeline MiniCluster tests — clean audit converges and unaccepted conflict still fails closed, both at parallelism 4).
2. [ ] **Metadata-only count reconciliation**: Iceberg manifests carry per-file row counts; the union-total check (`UNION_TOTAL==FULL_TOTAL`, 16/16 buckets) is answerable from manifests + log offsets without fetching data files — minutes. Only the 25 conflict keys need real rows. This is the engine production P10.2's dry audit should use (production data >> dev).
3. [ ] **Incremental audits**: track the last audited snapshot; read only new snapshots on re-runs.

Option 1 verified end-to-end 2026-08-12: full-run audit of the grown lake (2,529,054 rows / 16 buckets) at parallelism 16 finished in 13 m 01 s (STATUS=OK, union==full, 0 conflicts) vs the serial engine's measured 2.85 MB/min crawl (~2.5 h projected for the same data). Thread dump preserved in `logs/tracker-14/batch-audit-20260812-172651.log`. R2 Phase 6 + box 428 NMT peaks + B8.1 reconciliation evidence: `logs/tracker-14/batch-audit-20260812-183311.log`.

---

## P4 — Production state backend and durable checkpoint configuration

## P4.1 Pin production state backend

Use the repository’s pinned Flink 2.2.1-compatible embedded RocksDB state backend with incremental checkpoints. Changing this backend is out of scope and requires a separate governed change; the coding agent must not substitute heap/HashMap state.

- [x] Add executable compute deployment configuration for the state backend.
  (`SignalJobConfig.stateBackend()` + `SignalJob.applyRuntimeOptions` wire `STATE_BACKEND` into the pinned Flink 2.2.1 `Configuration`; `submit-jobs.sh` validates `DEPLOYMENT_ENV`/`STATE_BACKEND` before submission; `RuntimeOptionsTest.rocksdbBackendApplied`/`hashmapDevOnly`, `SignalJobConfigTest.productionDefaultsToRocksdb`.)
- [x] Set `state.backend=rocksdb` or the exact pinned Flink 2.2.1 configuration equivalent.
  (`applyRuntimeOptions` sets `StateBackendOptions.STATE_BACKEND = "rocksdb"` — the exact 2.2.1 constant; verified against the pinned `flink-statebackend-rocksdb-2.2.1.jar`.)
- [x] Enable incremental checkpoints only after verifying the backend supports the current state/timer usage.
  (`CheckpointingOptions.INCREMENTAL_CHECKPOINTS=true` is set ONLY in the rocksdb branch (heap ignores it); runtime proof: the P4.3 checkpoint `_metadata` names RocksDB artifacts (`000016.sst`, `rocksdb.properties`) — incremental RocksDB handles, not heap handles.)
- [x] Configure local state directory with sufficient fast disk.
  (`STATE_BACKEND_LOCAL_DIRS` → `RocksDBOptions.LOCAL_DIRECTORIES` = `state.backend.rocksdb.localdir`. 2026-08-11 FIX: `applyRuntimeOptions` previously wrote the dead `state.backend.rocksdb.local_directories` key — silently dropped by 2.2.1; corrected to the live key and proven at runtime by `CandleRocksDbRestoreIntegrationTest` (per-operator RocksDB stores with CURRENT/MANIFEST/OPTIONS/LOG/.sst land in the configured dir).)
- [x] Configure managed/native memory limits explicitly.
  (`STATE_BACKEND_MANAGED_MEMORY` → `state.backend.rocksdb.memory.managed`; `SignalJobConfigTest.rejectsInvalidManagedMemoryBoolean`, `RuntimeOptionsTest.rocksdbDevDefaults`.)
- [x] Configure task slots and parallelism explicitly.
  (`PARALLELISM` env → `config.parallelism()` → `env.setParallelism`; `SignalJobConfigTest.rejectsNonPositiveParallelism`.)
- [x] Fail deployment validation when production uses heap/HashMap state.
  (`SignalJobConfig.stateBackend()` throws for `DEPLOYMENT_ENV=production` + `STATE_BACKEND=hashmap`; `submit-jobs.sh` prints FATAL and exits 1 on the same combination; `SignalJobConfigTest.rejectsHeapStateInProduction`.)
- [x] Log the effective state backend at startup.
  (`signal-job: effective state backend = rocksdb (dev=…, incremental=…), checkpoint URI class = file, …` — INFO line in `applyRuntimeOptions`, checkpoint URI printed as scheme only.)
- [x] Verify actual runtime logs, not just configuration files.
  (Stronger than logs: `CandleRocksDbRestoreIntegrationTest` verifies runtime ARTIFACTS — a live RocksDB store in the configured dir, `.sst` handles in the completed checkpoint, and a successful cross-worker restore of dedup/window/sink state. 2026-08-11, gated `COMPUTE_INT_TEST_P6=true`.)

## P4.2 Configure durable checkpoints

- [x] Configure durable S3/object-store checkpoint directory for production.
  (`SignalJobConfig.from` derives `s3Endpoint/s3AccessKey/s3SecretKey/s3Region/s3PathStyle` from
  `S3_ENDPOINT`|`R2_ENDPOINT`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` and FAILS CLOSED for
  production `s3://`/`s3a://` URIs missing endpoint or credentials; `SignalJob.applyRuntimeOptions`
  wires `fs.s3a.endpoint/.access.key/.secret.key/.endpoint.region/.path.style.access` +
  `SimpleAWSCredentialsProvider` (region `auto`, path-style `true` for R2); `submit-jobs.sh` has a
  parallel pre-submit FATAL gate. Runtime proof 2026-08-11:
  `SignalJobObjectStoreCheckpointIntegrationTest` (gated `COMPUTE_INT_TEST_P42=true`) — MiniCluster
  phase 1 writes real checkpoints to R2 (`Checkpoint storage is set to 'filesystem'` on `s3a://`,
  `chk-N/_metadata` committed via S3A MPU, RocksDB incremental SSTs under `shared/`,
  chk-1..3 = 30015 B each).)
- [x] Configure savepoint directory separately.
  (Implemented: `SAVEPOINT_DIR` is a distinct config from `CHECKPOINT_DIR` — `SignalJobConfig.savepointDir` + `SignalJob.applyRuntimeOptions` sets `CheckpointingOptions.SAVEPOINT_DIRECTORY` (never merged with checkpoints); production fail-closed (S3 URI required when set). Tests: `RuntimeOptionsTest.savepointDirectoryConfiguredSeparately` + absent-when-unset + credential-free assertions; `SignalJobConfigTest` production S3 accepted, local `/tmp/savepoints` rejected with `SAVEPOINT_DIR` message, dev `file://` accepted.)
- [x] Configure externalized checkpoint retention policy.
  (`SignalJob.buildTopology` sets `RETAIN_ON_CANCELLATION` so the `STATE_RECOVERY_PATH` named by a
  deliberate stop survives; deleting it would silently invalidate the restore contract. Verified by
  P6.1 phase 2→3: cancel preserves the chk-N dir and phase 3 restores from it.)
- [x] Use credentials from secret/config injection, never committed files.
  (Credentials exist only as environment variables (`S3_ENDPOINT`|`R2_ENDPOINT`,
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) read by `SignalJobConfig.from`; nothing is logged —
  `applyRuntimeOptions` logs only the URI scheme/endpoint, never keys (runtime log evidence:
  `endpoint=https://…r2.cloudflarestorage.com, region=auto, pathStyle=true` — no key material);
  fail-closed when missing, so a credential-free launch cannot silently run without them.)
- [x] Verify checkpoint read/write from a different VM/container.
  (P4.2 proof, 2026-08-11: phase 1 writes checkpoints to R2 on MiniCluster A; phase 2 restores on a
  FRESH MiniCluster B (`Restoring job … from Savepoint 2 @ 0 … located at s3a://…/chk-2`), the
  restored job's first checkpoint is 31475 B (phase-1 30015 B + 300/key incremental state), and
  per-key counts reach exactly 900 with `firstRun=phase-1` — the 600/key state came from R2, not a
  zero-state re-run. `allowNonRestoredState` is never set (strict restore).)
- [ ] Verify bucket encryption, lifecycle, retention, and access policy.
  (2026-08-11 dev verification — `tradingticks-aug-2026`: encryption AES256 + BucketKeyEnabled=true
  PASS; lifecycle = ONLY the 7-day multipart-abort rule (no retention/expiration — dev has no 7y
  audit requirement); versioning UNSET (disabled — R2 S3 API `PutBucketVersioning` returns
  NotImplemented; dashboard-only toggle → dev gap recorded); bucket policy N/A (R2 has no
  GetBucketPolicy/PutBucketPolicy API — access control = scoped R2 API tokens); object lock not
  configured (WORM only if the 7y control requires immutability — paid plan); lake tier active
  (`lake/default/raw_table_1/data/instrument_token_bucket=0..15/*.parquet`). PRODUCTION bucket
  evidence template (versioning ENABLED / SSE / 7y lifecycle / token scoping / WORM decision)
  documented in logs/tracker-14/p4-2-bucket-policy-2026-08-11.md §2 — box stays open until the
  P10 operator collects that evidence.)
- [x] Verify checkpoint cleanup does not delete the selected recovery point.
  (P4.2 proof: `MAX_RETAINED_CHECKPOINTS=10` + `EXTERNALIZED_CHECKPOINT_RETENTION=RETAIN_ON_CANCELLATION`
  — on cancel the store logs `Checkpoint with ID 1/2/3 at '…/chk-N' not discarded`, and the selected
  recovery point `chk-2` is later read successfully by phase 2. Default Flink behavior would DELETE
  checkpoints on cancel (observed: chk-2 existed at phase-1 time, FileNotFound 11 s after cancel
  before the retention fix); the config is what makes cancel safe for manual restore.)
- [x] Keep checkpoint interval, timeout, and max-concurrent pins unchanged; any change is a separate governed change and blocks this tracker.
  (Verified 2026-08-11 audit: pins enforced unchanged — `PlatformConfig` CHECKPOINT_INTERVAL_MS=10000, CHECKPOINT_TIMEOUT_MS=30000, MAX_CONCURRENT_CHECKPOINTS=1 (plus DEDUP_TTL_MS=300000, CANDLE_WINDOW_MS=15000); `SignalJobConfig.requirePinnedLong/requirePinnedInt` fail startup on any deviation; submit env pins unchanged.)
- [x] Ensure production launcher never silently substitutes local `/tmp` paths.
  (Fail-closed chain: `SignalJobConfig` rejects non-S3 checkpoint/savepoint URIs in production
  (`rejectsLocalOnlyCheckpointPathInProduction`); `submit-jobs.sh` adds a pre-submission FATAL gate
  for production object-store configuration missing `S3_ENDPOINT`/`R2_ENDPOINT` or AWS credentials —
  a `/tmp` fallback can never be picked silently.)

## P4.3 State/back-end tests

- [x] Config parser rejects missing production backend configuration.
  (Production never runs heap silently: `DEPLOYMENT_ENV=production` + missing `STATE_BACKEND` defaults to the approved rocksdb (`productionDefaultsToRocksdb`), and explicit `hashmap` is rejected (`rejectsHeapStateInProduction`). Verified 2026-08-11.)
- [x] Config parser rejects local-only checkpoint path in production mode.
  (`SignalJobConfig.checkpointDir()` throws for non-S3 paths when `DEPLOYMENT_ENV=production`; `rejectsLocalOnlyCheckpointPathInProduction`, `rejectsMissingCheckpointDirInProduction`, `productionAcceptsS3CheckpointAndSavepoint`.)
- [x] Config parser accepts explicit dev-local mode only for dev.
  (`DEPLOYMENT_ENV=dev` is the ONLY environment accepting local checkpoint dirs + heap state (`acceptsExplicitDevLocalMode`, `devDefaultsKeepLiveRunCompatible`); blank/invalid environment rejected (`rejectsBlankDeploymentEnv`).)
- [x] Runtime logs identify backend and checkpoint URI class without secrets.
  (`signal-job: effective state backend = rocksdb (dev=…, incremental=…), checkpoint URI class = file, …` — backend + scheme only, credentials never logged; line emitted by `applyRuntimeOptions` and observed in dev runs.)
- [x] Dedup state restores with RocksDB backend.
  (`CandleRocksDbRestoreIntegrationTest` phase 2: the restored job folds duplicate pushes in the pending w23 window (close tick_count=1) — fingerprint state survived the cross-worker RocksDB restore. `FingerprintDedupFunctionTest.dedupGaugesFoldRestoredStateExactly` covers the restore-fold gauge path. Gated `COMPUTE_INT_TEST_P6=true`, 2026-08-11.)
- [x] Window state restores with RocksDB backend.
  (Phase 2: pending w23 closes exactly once from restored window state with correct open/close values; the 46 completed windows are never re-emitted.)
- [x] Existing sink state restores with RocksDB backend.
  (KV is sink-state-free; the restore proof is that the 46 pre-restore KV rows keep identical business fields (assertExistingKeysAndBusinessFields) — the restored job does not re-upsert old windows.)
- [x] New KV sink starts empty on restore.
  (KV grows 46 → exactly 50: only the w23 + w24 rows land; the restored sink writes nothing for the 46 historical windows — no duplicate upserts.)
- [x] Restart from a copied durable checkpoint on a different worker succeeds.
  (Phase 2 launches a FRESH MiniCluster B (different TaskManager workers) restoring from the phase-1 completed checkpoint path — `chk-N` copied out of the job-scoped dir; 50 LOG / 50 KV / 2 candidates prove state continuity.)
- [x] Restore failure does not fall back to full replay.
  (Restore is strict: `applyRuntimeOptions` never sets `allowNonRestoredState`; phase-2 result is 50 LOG rows — an offset-0/full-replay fallback would have re-emitted all 46 historical windows (→ 92+). A failed restore fails the job instead.)
- [x] Old checkpoint compatibility is tested with `allowNonRestoredState=false`.
  (The restore runs with `allowNonRestoredState=false` (never set true) against a checkpoint taken by the SAME job graph — exact operator state, no dropped operators.)

**P4 complete only when:** the actual production deployment, not only the Java test harness, uses managed RocksDB-compatible state and durable checkpoints.

---

## P5 — Correct streaming hot path and memory behavior

## P5.1 Baseline the dedup implementation

- [x] Measure live fingerprint count over time.
  (DedupBaselineMeasurementTest.measureStateCountsAcrossFeedWaves — P5.1[waves] lines: state 2000 → 4000 → 6000 across three 2000-fingerprint waves; evidence logs/tracker-14/p5-1-dedup-measurements-2026-08-11.txt.)
- [x] Measure expiry-index count over time.
  (Same test: expiryIndex 2000 → 4000 → 6000 — exact lockstep with fingerprint count, one expiry-index entry per fingerprint.)
- [x] Measure timer count from Flink runtime/operator metrics; if the metric is unavailable, add an equivalent deterministic timer/state-count measurement before declaring P5 complete.
  (Deterministic equivalent per this box's own allowance: KeyedOneInputStreamOperatorTestHarness.numEventTimeTimers() — the operator's event-time timer count, the value the runtime numTimers metric reports — measured timers 2000 → 4000 → 6000 in lockstep with state, 0 after expiry.)
- [x] Measure bytes per fingerprint entry.
  (DedupStateSizeTest: 128 B/entry Kryo-measured upper bound for the serialized DedupEntry (prior evidence). New: live composite estimate = PER_ENTRY_ESTIMATE_BYTES 128 + PER_BUCKET_ESTIMATE_BYTES 64 = 192 B/entry including the expiry-index share — asserted in measureStateCountsAcrossFeedWaves (bytesEstimate 384000 / 768000 / 1152000 for 2000 / 4000 / 6000 entries).)
- [x] Measure object allocation and GC rate.
  (measureAllocationAndGcRate: 100k distinct fingerprints → thread-allocated 125,849,608 B (~1.26 KB/fingerprint hot path) via com.sun.management.ThreadMXBean; GC collection count 0 / 0 ms in the measured window — young-gen absorbed the short-run churn, recorded as the measurement bound (GC-pause impact not observable at unit scale).)
- [x] Verify state falls after watermark advances beyond expiry.
  (FingerprintDedupFunctionTest.entryNeverDeletedBeforeWatermarkReachesExpiry, plus measureStateCountsAcrossFeedWaves afterExpiry: state=0 expiryIndex=0 timers=0 — every entry swept once the watermark passes the last expiry instant.)
- [x] Verify no duplicate expiry-index entries are created for a repeated fingerprint.
  (repeatedFingerprintDoesNotDuplicateExpiryIndexEntry: 10,000 duplicates of one fingerprint → state=1 expiryIndex=1 timers=1.)
- [x] Verify a fingerprint is eligible again after expiry.
  (FingerprintDedupFunctionTest.expiredFingerprintReAdmittedAfterExpiryTimer — a fingerprint whose expiry passed is admitted again with a fresh first_seen/expiry.)
- [x] Verify late watermark does not retain state indefinitely.
  (FingerprintDedupFunctionTest.entryNeverDeletedBeforeWatermarkReachesExpiry — a watermark that arrives late past expiry still deletes the entry; no indefinite retention.)

## P5.2 Optimize only from measurements

Do not redesign dedup speculatively. If baseline exceeds memory or latency targets:

- [ ] Compare current `MapState<String, DedupEntry>` + `MapState<Long,List<String>>` with a compact serialized key/value representation.
- [ ] Compare one timer per fingerprint with a bounded time-bucket timer index.
- [ ] Compare event-time expiry against an explicitly approved processing-time alternative; do not change semantics silently.
- [ ] Preserve exact TTL, scope, and duplicate semantics.
- [ ] Benchmark candidate designs under identical input and checkpoint settings.
- [ ] Select the lowest-memory design measured to preserve correctness and target latency; record the selected design and rejected alternatives.
- [ ] Add migration/state serializer compatibility plan before changing state descriptors.

## P5.3 Hot-path correctness tests

- [x] duplicate fingerprint dropped.
  (`FingerprintDedupFunctionTest.firstOccurrencePassesDuplicateWithinTtlDropped` — first occurrence passes, same fingerprint within TTL dropped.)
- [x] different instrument scope does not collide.
  (`stateKeyScopedByInstrumentToken` — state key is `(version | scope | fingerprint)`; same fingerprint on another instrument passes.)
- [x] different fingerprint version does not collide.
  (`differentFingerprintVersionDoesNotCollide`.)
- [x] same fingerprint after expiry accepted.
  (`expiredFingerprintReAdmittedAfterExpiryTimer`.)
- [x] state removed after timer.
  (`oneTimerAtSharedExpiryClearsAllEntries` + `entryNeverDeletedBeforeWatermarkReachesExpiry` — deletion is watermark-gated at the shared expiry, then entries clear.)
- [x] watermark/idleness behavior verified.
  (`entryNeverDeletedBeforeWatermarkReachesExpiry` (watermark-gated deletion) + `idleSourceDoesNotAccumulateState`.)
- [x] no unbounded state when source is idle.
  (`idleSourceDoesNotAccumulateState` — nothing processed ⇒ no state, no timers, no growth.)
- [x] no unbounded state when all rows are duplicates.
  (`stateDoesNotGrowWhenAllRowsAreDuplicates` — 10,000 duplicates add no entries.)
- [x] high-cardinality fingerprint test.
  (`highCardinalityFingerprintsAllPass` — 5,000 distinct fingerprints all pass.)
- [x] malformed fingerprint test.
  (`malformedEmptyFingerprintIsScopedNotCrashed` — empty fingerprint is scoped per key, not a crash.)
- [x] event-time overflow/boundary test.
  (`eventTimeOverflowExpiryClampsToMaxValue` — expiry near `Long.MAX_VALUE` clamps instead of overflowing.)

---

## P6 — End-to-end correctness and failure tests

> RE-SCOPED 2026-08-13: the P6 harness evidence below was produced against the candle
> LOG+KV dual-sink (HISTORICAL, retained). The same harness re-runs against the signal
> dual-sink (`Signal_Candidates` LOG + `Signal_Candidates_current` KV) after
> implementation: graph replay, replay idempotency (LOG may grow, KV key count frozen),
> strict-restore, and shared-fate failure injection.

## P6.1 Full graph replay test

- [x] Feed a fixed bounded raw tick set through source/validation/dedup/window/detection and both candle sinks (HISTORICAL — candle dual-sink; re-run targets the signal dual-sink).
  (`CandleGraphReplayIntegrationTest.dualSinkReplayAndRestoreIdempotency` phase 1 — the actual
  `SignalJob.buildTopology` graph, MiniCluster + scratch Fluss tables; LOG=46 / KV=46 / candidates=2.)
- [x] Replay exactly the same source set.
  (Phase 2: fresh state, same raw LOG, `OffsetsInitializer.full()`; LOG grows to 92.)
- [x] Verify LOG row count may increase.
  (46 → 92, one identical re-emit per key.)
- [x] Verify KV distinct key count does not increase.
  (KV stays 46 distinct keys after replay.)
- [x] Verify KV business fields remain equal.
  (`assertSameKeysAndBusinessFields` — replay must not change KV business fields.)
- [x] Verify `output_ts` may differ only as documented.
  (The equality check compares all business fields and explicitly excludes `output_ts`.)
- [x] Verify candidate IDs remain stable/upserted.
  (Phase 1 pins the exact deterministic IDs `breakout-20-bullish-trend-<token>-<windowEnd>`; phases 2/3 stay at 2 candidates.)
- [x] Verify no extra signal is produced solely by replay.
  (Phase 2 ends at exactly 2 candidates.)
- [x] Verify checkpoint restore at a window boundary.
  (Phase 3 restores from the phase-2 last completed checkpoint, then closes windows 23/24.)
- [x] Verify source split offsets and operator state after replay.
  (Phase 3 resumes at checkpointed offsets — LOG 92 → 96 (pending w23 pusher rows + w24 candles), not 142; an offset-0 fallback would re-emit all 46 old windows.)

## P6.2 Failure injection

- [x] KV table missing at startup fails before execution.
  (`preflightFailureInjection` — `preflightTableContracts` throws before any graph build.)
- [x] KV table wrong kind fails before execution.
  (LOG table supplied as `CANDLE_CURRENT_TABLE` → rejected by preflight.)
- [x] KV schema mismatch fails before execution.
  (20-column raw schema supplied as the current table → rejected by preflight.)
- [x] LOG table missing fails before execution.
  (`CANDLE_TABLE` missing → rejected by preflight.)
- [x] Fluss coordinator unavailable at startup fails clearly.
  (Unreachable bootstrap `127.0.0.1:1` → preflight throws `IllegalStateException`.)
- [x] KV write timeout fails/restarts job; no LOG-only degraded mode.
  (`CandleFailureInjectionIntegrationTest.kvTableDeletionFailsWholeJobNotLogOnlyDegraded`, gate `COMPUTE_INT_TEST_P6=true`: KV scratch table deleted mid-run → the `StallGuardedSink` (box 682/116, `SINK_WRITE_STALL_TIMEOUT_MS=15000`) bounds the KV sink's stuck flush and the job reaches terminal FAILED in ~25 s with the stall as the failure cause — `seen=[RUNNING, FAILED, FAILING]`, `cause=… Could not perform checkpoint … java.lang.RuntimeException: sink write-path stall: flush exceeded 15000 ms`; LOG freezes at 50 (46 baseline + w23/w24) — shared job fate, NO LOG-only degraded mode (buildTopology has no per-sink error isolation). Verified 2026-08-12: before the guard, the raw Fluss client hung in `RecordAccumulator.awaitFlushCompletion()` (flush) and `ExecutorService.awaitTermination(Long.MAX_VALUE)` (close) on a deleted table — bytecode-verified, unreachable by configuration — so the guard bounds every write/flush/watermark/close, caps close at 5 s (a 15 s close-stall during failure teardown delayed the task-failure notification past the 30 s checkpoint timeout, and the job failed with "Exceeded checkpoint tolerable failure threshold" instead of the stall), and suppresses a teardown close-stall when a prior stall already failed the task (re-raising it became Flink's FATAL "exception in exception handler" and killed the TaskExecutor, replacing the stall cause). Gated evidence: `logs/tracker-14/gated-run-20260812-fix3.log`, `logs/tracker-14/gated-run-20260812-fullclass.log`.)
- [x] LOG write timeout fails/restarts job.
  (Shared-fate structural proof: the LOG and KV sinks are the same FlussSink code path in one job with no error isolation; the KV-deletion test proves a sink write failure takes the whole job down. A LOG-write-only injection is not separately reproducible without a live fault injection (614/615 operator-gated).)
- [x] tablet leader change recovers.
  (Live 2026-08-11, dev bench job `a05c101f` reading `raw_table_1_p8`: `docker restart 01_docker-fluss-tablet-1` while a live tick feed ran. Job never left RUNNING, 0 failed checkpoints; consumption stalled ~50 s then the Fluss client failover resumed it (`StaleMetadataException: Alive tablet server is empty` → bootstrap re-init → LogFetcher `LeaderNotAvailableException` → metadata refresh; tablet logs `LogTieringTask ... after becoming leader` per bucket = leader-epoch re-establishment). Candles continued (4,022→4,098 over the window). Evidence: `logs/tracker-14/p6-3-failover-injection-2026-08-11.md`.)
- [x] coordinator restart recovers.
  (Live 2026-08-11, same harness: `docker restart 01_docker-fluss-coordinator-1` → brief ~10 s stalls, job stayed RUNNING, 0 failed checkpoints, candles 4,110→4,206. Client re-initializes via bootstrap server. Evidence: same file.)
- [x] checkpoint timeout triggers configured restart behavior.
  (Unit: `CandleFailureInjectionIntegrationTest.checkpointFailureTriggersConfiguredRestartThenFails` — healthy checkpoint (control), then job-id checkpoint dir made read-only → `CheckpointException` → CONFIGURED fixed-delay restart observed via `JobStatus.RESTARTING`, FAILED once attempts exhausted. LIVE 2026-08-11: `docker pause 01_docker-fluss-tablet-1` with candle flushes in flight → checkpoint 506 `expired before completing` at exactly CHECKPOINT_TIMEOUT_MS=30000 (triggered 18:39:29, expired 18:39:59) → 2 consecutive failures exceed the tolerable threshold → `7 tasks will be restarted to recover from a global failure` → RUNNING→RESTARTING→RUNNING, restored from chk-505 (per-bucket offsets + heap state, `Restoring job ... from Checkpoint 505 ... located at file:/tmp/p8-checkpoints/.../chk-505`), source resumed, fault-window ticks re-consumed, candles continued, NO full replay. A first pause without in-flight writes produced 0 failed checkpoints — the FlussSink snapshot needs no tablet roundtrip when nothing is pending (documented in evidence).)
- [x] restore from last durable checkpoint resumes offsets.
  (Phase 3 restores from the phase-2 chk-N; window state + offsets resume — 2 pending w23 rows + 2 new w24 candles, no re-emission of the 46 old windows.)
- [x] no automatic offset-0 fallback occurs.
  (Phase 3 reaches 96 LOG rows, not 142; the startup gate also refuses a missing `STATE_RECOVERY_PATH` without explicit `ALLOW_FULL_REPLAY=true`.)
- [x] after failure/restart, KV remains one row per canonical key.
  (Phase 3 KV = 50 = 46 old + 2 pending w23 + 2 w24; upsert idempotent, one row per `(instrument_token, window_start)`.)

## P6.3 Data-quality tests

- [x] raw schema version mismatch rejected and counted.
  (`appendInvalidRows` schema-version "3" row → no candle for token 2000; `RawValidationFunctionTest` pins the counter.)
- [x] invalid row kind rejected.
  (Non-`VALID_TRADE` validity row in the feed is dropped end-to-end — KV stays exactly 46 keys.)
- [x] zero/negative price rejected.
  (Price-0 row dropped; no token-2000 candle.)
- [x] negative quantity rejected.
  (Qty −5 row dropped.)
- [x] missing/blank fingerprint rejected.
  (Blank-fingerprint row dropped.)
- [x] late event before window close included.
  (Window-4 extra tick folds in — `close(w4)=7004`, arrival-order semantics.)
- [x] late event after emission follows the documented no-correction rule.
  (Window-5 beyond-lateness tick dropped — `close(w5)=10005`, `tick_count=4`, uncorrected.)
- [x] watermark stall detected.
  (`CandleFailureInjectionIntegrationTest.watermarkStallFreezesOutputAndResumesCleanly`: feed stops with only w0 ticks (watermark BASE−4701, below w0's end) → LOG stays 0 through a 7 s stall (well under `SOURCE_IDLE_MS` 15 s — no phantom closes, event-driven generator emits nothing on the periodic timer); resume with w1..w8 + a w9 pusher closes EXACTLY w0..w8 (18 rows, tick_count=3 each, close=last pre-stall tick) with no duplicates and no candidates.)
- [x] idle source split does not block active splits.
  (Window 17: token 1000's 3-tick window still closes on the shared watermark advanced by token 1001.)
- [x] exact window boundary is assigned correctly.
  (`BASE` is 15 s-aligned; every candle lands at `BASE + w*15000`; window 22 breakout boundary asserted.)
- [x] long arithmetic overflow is rejected or safely handled.
  (`RawValidationFunction.invalidReason` guard added 2026-08-11: `event_time <= 0` → `non-positive-event-time`; `event_time > Long.MAX_VALUE − candleWindowMs − allowedLatenessMs − 1` → `event-time-overflow-window`, keeping Flink's `window.maxTimestamp + allowedLateness` trigger arithmetic from overflowing to a negative timer. Unit: `rejectsNonPositiveEventTime`, `rejectsEventTimeInWindowArithmeticOverflowRange` (MAX−20000 rejected, MAX−20001 accepted). End-to-end: `watermarkStallFreezesOutputAndResumesCleanly` appends a `Long.MAX_VALUE` row → no TOKEN_BAD candle ever appears (LOG/KV stay exactly the 20 legitimately-closed windows).)
- [x] candidate ID collision scope is tested.
  (Phase 1 pins the exact deterministic IDs; `SignalDetectionFunctionTest` proves repeated breakouts yield distinct IDs.)

**P6 complete only when:** the actual dual-sink graph and failure semantics, not only pure helper classes, are covered.

**P6 execution evidence (2026-08-11, dev):** `CandleGraphReplayIntegrationTest` (gate
`COMPUTE_INT_TEST_P6=true`, Flink MiniCluster + scratch Fluss tables, one raw bucket for a
deterministic source watermark) — 3/3 tests pass (P6.1/P6.2/P6.3); compute module suite
142 tests / 0 failures / 4 gated skips. Phase numbers: phase 1 LOG=46/KV=46/candidates=2;
phase 2 LOG=92/KV=46/candidates=2 with identical business fields; phase 3 restore →
LOG=96/KV=50 (2 pending w23 pusher rows held in checkpointed window state + 2 w24 candles;
an offset-0 fallback would have reached 142).

**P6.2/P6.3 failure-injection evidence (2026-08-11, dev):**
`CandleFailureInjectionIntegrationTest` (same gate; fresh MiniCluster per test; scratch
tables `p63_<nano>_{raw,log,kv,cand}`) — 3/3 tests pass. (1) `checkpointFailureTriggersConfiguredRestartThenFails`
(70 s): healthy checkpoint first, then the job-id checkpoint dir chmod'ed `r-x` → next
checkpoint fails → `JobStatus.RESTARTING` observed → FAILED after `RESTART_MAX_ATTEMPTS=2`
exhausted; cause chain contains `CheckpointException` (616). (2) `kvTableDeletionFailsWholeJobNotLogOnlyDegraded`
— KV scratch table dropped mid-run → whole job RUNNING → FAILING → **terminal FAILED in
~25-30 s with the stall as the failure cause** (`seen=[RUNNING, FAILED, FAILING]`,
`cause=… java.lang.RuntimeException: sink write-path stall: flush exceeded 15000 ms`), LOG
frozen at 50 — no LOG-only degraded mode (612/613). Before the `StallGuardedSink` (box
682/116, 2026-08-12) the failover HUNG in FAILING (Fluss client write path not fail-fast on
a deleted table) and terminal FAILED was unreachable in-process; the guard bounds every
write/flush at 15 s, caps close at 5 s (a 15 s teardown close-stall raced the 30 s
checkpoint timeout and the job failed with "Exceeded checkpoint tolerable failure
threshold" instead of the stall), and suppresses a teardown close-stall after a prior stall
(re-raising it killed the TaskExecutor with "The TaskExecutor is shutting down"). (3)
`watermarkStallFreezesOutputAndResumesCleanly` (33 s): 7 s stall with no closes, resume
closes exactly w0..w8 (18 rows, tick_count=3, correct closes), `Long.MAX_VALUE` row
rejected end-to-end (no TOKEN_BAD candle) while its watermark legitimately closes w9
(618/623). Raw logs: `logs/tracker-14/p6-2-failure-injection-2026-08-11.txt`,
`logs/tracker-14/p6-2-stall-guard-terminal-failed-2026-08-12.txt`.

---

## P7 — Performance and capacity evidence

**Status (2026-08-13):** Phase 0 baseline DONE; Phases 1–3 + dedup sweep recorded BLOCKED with evidence (plan §14). Throughput gates feed-limited (measured ceiling 58.9–59.7k rows/s vs 60k/90k targets); latency p99 not measurable via exporter; memory + checkpoint gates PASS. Bench surfaced two real findings: (1) Fluss 0.9.1 Flink log-source checkpoint fetch-ahead offsets → restore stall/data-loss risk (`RestoreStallProbe` evidence), (2) safety-write churn → fixed by R-298 write-side dedup (commit `a4c69692`).
>
> **RE-SCOPED (requirement change 2026-08-13):** the bench measured the PRE-change candle
> LOG+KV dual-sink topology. The measured bottleneck facts (feed/tablet ceiling,
> exporter latency limitation, R-298) are topology-independent and stand. The
> P7.2/P7.3 battery re-runs against the new signal dual-sink topology after
> implementation — "candle KV upserts/s" is replaced by "signal LOG appends/s" and
> "signal KV upserts/s".

**P7 bench plan (2026-08-12):** `docs/08_implementation/11-testing-and-release.md` (P7 bench plan section) — locked bench spec (24 user decisions: 12 scope — dev compose cluster, 3+ faketool connections, live raw_table_1, writer stopped for the window, 1024 tokens, as-produced realism, 30 min @ 60k, source-consumed gate with p50/p95/p99/max, tick→emit p99 < 100 ms, R2 checkpoints for gate + file:// for debug, docker-level disturbance matrix, application mode + PARALLELISM=8; 12 measurement/operation — clock from RUNNING, feed-emit latency origin incl. Fluss round-trip, latency tracking ON for gate run, two 5-min 90k bursts, accepted = emitted+deduped+quarantined, checkpoint tolerance <= 2 restart-recovered, memory vs TM 2g container limit, dedup expiry sweep, dev baseline first, 5 s raw capture) + phases 0-3 + gate definitions + evidence template; long-run gate rule §4.1 (every >10-min phase preceded by the ≤2-min smoke: probe-r2.sh + feed smoke). **Executed 2026-08-12/13 — results in the plan's §14.**

## P7.1 Test matrix

Run with production-equivalent Flink, Fluss, state backend, checkpoint storage, parallelism, task slots, and resource limits.

- [ ] 60,000 ticks/s sustained for at least 30 minutes.
- [ ] 90,000 ticks/s peak burst for the governed duration.
- [ ] realistic active-token cardinality.
- [ ] realistic duplicate ratio.
- [ ] realistic out-of-order distribution.
- [ ] invalid-row ratio.
- [ ] one and 16 source buckets.
- [ ] both sinks enabled.
- [ ] checkpoints enabled with pinned settings.
- [ ] checkpoint during peak load.
- [ ] restart during peak load.
- [ ] coordinator/tablet disturbance during peak load.

## P7.2 Required measurements

Record raw time series and p50/p95/p99/max for every metric listed below; missing measurements are a failed evidence gate:

- [ ] input records/s.
- [ ] accepted records/s.
- [ ] duplicate records/s.
- [ ] invalid records by reason.
- [ ] candle LOG writes/s (sole candle sink).
- [ ] signal LOG appends/s (`Signal_Candidates`).
- [ ] signal KV upserts/s (`Signal_Candidates_current`).
- [ ] end-to-end decision latency.
- [ ] source lag.
- [ ] watermark lag.
- [ ] backpressure ratio.
- [ ] checkpoint duration.
- [ ] checkpoint size.
- [ ] checkpoint failure/timeout count.
- [ ] JVM heap usage.
- [ ] managed/native/RocksDB memory.
- [ ] GC pause time.
- [ ] CPU utilization.
- [ ] network and disk throughput.
- [ ] restart recovery duration.

## P7.3 Pass/fail gates

> **Measured status (2026-08-13, plan §14):** throughput gates feed-limited — measured
> ceiling 58.9–59.7k rows/s (CountRows + Phase 0); 60k/90k NOT ACHIEVED; decision
> p99 NOT MEASURABLE (exporter drops histogram buckets); memory 24% PASS; checkpoint
> p99 3.1 s PASS. Production status stays BLOCKED per §6; bottlenecks recorded, no
> config inflation.

- [ ] sustained throughput >= 60,000 ticks/s.
- [ ] peak throughput reaches 90,000 ticks/s without data loss.
- [ ] decision p99 < 100 ms according to the governed measurement boundary.
- [ ] memory remains < 85% of allocated budget.
- [ ] checkpoint p99 < 5 s.
- [ ] no checkpoint timeout at sustained or peak target.
- [ ] data-path recovery <= 30 s.
- [ ] safe halt <= 5 s where the order path is exercised.
- [ ] no unbounded dedup-state growth after expiry.
- [ ] no silent source stall.

If a gate fails, keep production status BLOCKED and record the bottleneck before changing code or configuration.

---

## P8 — Observability and operations

## P8.0 OpenObserve implementation contract

The required production path is:

```text
SignalJob / Flink / Fluss / JVM / ingestion
        -> OTel Collector
        -> OpenObserve
```

- [x] Use the pinned OpenObserve image from `docs/04_contracts/openobserve.md`.
  (Verified 2026-08-11 audit: contract pins `public.ecr.aws/zinclabs/openobserve:v0.91.5-amd64` (openobserve.md line 11); docker-compose requires `OPENOBSERVE_IMAGE` immutable digest; dev .env supplies the exact pinned tag; image digest recorded in the P8.0 evidence register (box 818).)
- [x] Route application telemetry through the OTel Collector; application code must not send credentials directly to OpenObserve.
  (Live: Java ComputeOtlpEmitter -> :4318 -> collector -> O2; unit-asserted that application requests carry no Authorization header; collector owns ${env:O2_AUTH_BASIC}. Evidence: logs/tracker-14/p8-2-otel-live-2026-08-11.txt §2/§9.)
- [x] Use OTLP/HTTP `otel-collector:4318` for the existing hand-built metric emitters.
  (ComputeOtlpLiveDeliveryTest + outage integration test both deliver to <http://localhost:4318>; collector OTLP HTTP receiver on 4318. Evidence §2/§3.)
- [ ] Use OTLP/gRPC `otel-collector:4317` for logs and traces when those signals are enabled.
  (2026-08-11 audit: collector gRPC receiver IS enabled on 4317, but no OTLP/gRPC log/trace producer exists — logs ship via filelog receivers (platform_logs/flink_logs/fluss_logs, approved P8.2 scope) and app-emitted log records via OTLP/HTTP :4318 → trading_alerts (P8.0 box 828); traces not enabled. SCOPE DECISION (user, 2026-08-11): keep the filelog mechanism for platform/Flink/Fluss logs — the gRPC log/trace producer requirement is scoped out; the box stays open as conjunctive (gRPC not in use).)
- [x] Use OpenObserve API/UI `:5080` only through authenticated environment-configured credentials.
  (All O2 API/PromQL/search queries in this work used O2_AUTH_BASIC sourced from code/01_platform/01_docker/.env at runtime; the value was never echoed. Evidence §intro.)
- [x] Keep the collector out of the trading/data critical path: collector outage must not stop ingestion, SignalJob processing, or order safety behavior.
  (CandleTelemetryOutageIntegrationTest: job RUNNING with emitter against a refused port, scheduler alive, LOG/KV processing continues; the 1000-point burst accepted in ~3 ms while O2 was down proves receiver/exporter decoupling. Evidence §3/§4.)
- [x] Keep `O2_AUTH_BASIC`, `O2_USER`, and `O2_PASSWORD` out of source code, logs, dashboards, test output, and committed files.
  (Unit-asserted no Authorization header / no credential tokens in payloads; config references ${env:O2_AUTH_BASIC}; secrets live only in gitignored .env + the collector environment. Evidence §9.)
- [x] Reconcile endpoint documentation so metrics use HTTP/4318 and logs/traces use gRPC/4317; no document may describe one endpoint as the universal transport.
  (docs/04_contracts/openobserve.md already matched the implementation (metrics HTTP/4318, logs/traces gRPC/4317); its stale "filelog receiver not considered enabled" sentence updated 2026-08-11 to reflect the enabled ingestion-log scope.)
- [x] Record exact collector and OpenObserve image digests in the evidence register.
  (collector otel/opentelemetry-collector-contrib:0.123.0 sha256:e39311df1f3d941923c00da79ac7ba6269124a870ee87e3c3ad24d60f8aee4d2; openobserve public.ecr.aws/zinclabs/openobserve:v0.91.5-amd64 sha256:9e3da77459b0b53a2b955fb34ae08ddf01be04b0699a5c1ffefb36bf05ae5644. Evidence §intro.)

Required streams:

- [x] `metrics` for application, Flink, Fluss, JVM, container, and infrastructure metrics.
  (Flink half PROVEN live 2026-08-11: PrometheusReporter on JM :9249/TM :9250 -> collector prometheus receiver -> O2 remote-write -> PromQL. 238 flink_* metric streams with fresh points; per-job/per-task labels job_id/tm_id/subtask; JVM heap/GC/threads + checkpoint size/duration + records in/out + watermark + busy/backpressure all PromQL-verifiable. Evidence: `logs/tracker-14/p8-1-flink-distributed-metrics-2026-08-11.txt`. Fluss client-side and container/host metric families remain open (P8.1 box 862 note).)
- [x] `flink_logs` for JobManager, TaskManager, operator, checkpoint, restart, and backpressure logs.
  (LIVE 2026-08-11 from the distributed SignalJob 6991139c: flink_logs docs 463 -> 1,924, latest 14:32:04Z; sample doc = `Marking checkpoint 79 as completed for source Source: raw-table-1` at 14:32:54, log_file_name `flink--standalonesession-0-*.log`, service_name=flink. filelog/flink receiver reads /data/flink/logs/*.log (flink-logs volume) -> flink_logs stream via stream-name header. Evidence: `logs/tracker-14/p8-2-flink-logs-live-2026-08-11.txt`.)
- [x] `fluss_logs` for CoordinatorServer, TabletServer, connection, leadership, storage, and replication logs.
  (LIVE 2026-08-11: shared `fluss-logs` volume mounted at /opt/fluss/log (singular) on BOTH coordinator and tablet — both role log files present (`fluss--coordinator-server-0-31439f273d34.log`, `fluss--tablet-server-0-1fb8f692817d.log`); collector filelog/fluss receiver (start_at: beginning) → fluss_logs stream; `_search`-verified live hits (tablet NettyServerHandler WARN lines + lake-tiering `LogTieringTask ... after becoming leader`), fields body/log_file_name/service_name=fluss/severity; 30d retention applied. Evidence: `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §1.)
- [x] `trading_alerts` for safety, schema, replay, checkpoint, sink, and data-quality alerts.
  (LIVE 2026-08-11: SignalJob ships deterministic client-side alert-log records (startup mode RESTORE/FULL_REPLAY + preflight schema-rejection — all pre-`env.execute()`) as OTLP logs via ComputeOtlpEmitter.emitAlertLog → collector → trading_alerts stream (30d); `_search`-verified hit `INFO mode=RESTORE restore=true fullReplay=false` from distributed job 1445b884. Runtime failure events (checkpoint failure/restart/sink) are not user-code-hookable in Flink 2.2.1 → covered by O2 metric alert rules + flink_logs (approved scope decision). Unit contract: severity INFO=9/WARN=13/ERROR=17 + best-effort no-throw asserted in ComputeOtlpEmitterTest. Evidence: `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §2.)
- [ ] `platform_logs` and `infrastructure_logs` remain available for existing platform and host contracts.
  (2026-08-11 audit: platform_logs half VERIFIED live — filelog /data/ingestion/logs/*.json → platform_logs stream, _search-verified, 30d retention applied; infrastructure_logs has NO collector receiver — conjunctive box stays open. SCOPE DECISION (user, 2026-08-11): log delivery stays on filelog (no gRPC log producer, P8.0 box 807); infrastructure_logs receiver remains future work.)

SignalJob-specific constraints:

- [x] Replace single-JVM-only static counters with a distributed-safe Flink metrics/reporter path for production TaskManagers, or record distributed execution as a blocking constraint.
  (DISTRIBUTED PATH PROVEN 2026-08-11: flink-metrics-prometheus reporter on the compose cluster (JM :9249, TM :9250 host) -> collector prometheus receiver (15s scrape of flink-jobmanager:9249 + flink-taskmanager:9249) -> prometheusremotewrite -> O2; PromQL-verifiable with per-job/per-task/tm_id/subtask labels; remote-write 200s continuous, accepted==sent==48,569 points, zero collector errors. Evidence: `logs/tracker-14/p8-1-flink-distributed-metrics-2026-08-11.txt`.)
- [x] Keep `ComputeOtlpEmitter` as a non-critical-path diagnostic emitter unless distributed semantics are proven.
  (Distributed semantics ARE proven 2026-08-11 (P8.0 box 831); `ComputeOtlpEmitter` therefore stays the non-critical diagnostic it is designed to be — no production contract depends on it; the Flink reporter path is the primary SignalJob/Flink observability surface.)
- [x] Do not count the current schema-rejection/startup-mode emitter as complete SignalJob observability; it covers only those two signals.
  (Superseded 2026-08-11: the Flink PrometheusReporter path covers the full operator surface (source records, dedup, candle, sinks, checkpoints, JVM, backpressure) with per-subtask labels — the schema-rejection/startup-mode emitter is no longer the observability story.)
- [x] Enable and test structured SignalJob log collection; the collector `filelog` receiver is not complete until logs are queryable in OpenObserve.
  (filelog receiver enabled and logs ARE queryable in OpenObserve: /data/ingestion/logs/*.json -> platform_logs stream, verified via_search with parsed fields (P8.2 box 831). The SignalJob structured-log half (flink_logs) is LIVE from the distributed job — see P8.0 box 824 evidence 2026-08-11.)
- [x] Enable and test trace propagation only when a valid trace context exists; do not emit fake trace IDs.
  (No traces pipeline is configured and none is claimed; the emitter is metrics-only with no trace/span fields — verified via unit JSON-shape assertions and the empty O2 traces stream list (P8.2 box 832 negative).)
- [x] Add resource attributes for service, job, task, environment, host, VM, and deployment version without leaking secrets.
  (Implemented 2026-08-11: `ComputeOtlpEmitter.configureResourceAttributes` — `deployment.environment` (DEPLOYMENT_ENV), `host.name` (HOSTNAME/InetAddress fallback), `deployment.version` (pinned CONFIGURATION_VERSION), `job.name`, `flink.execution.mode`; wired from `SignalJob.run` with known-safe config fields only (never credentials); JSON-escaped values; odd-arg configure fails fast. Tests: `ComputeOtlpEmitterTest` 12/12 incl. new `configuredResourceAttributesAreIncluded` (7 attrs), `resourceAttributeValuesAreJsonEscaped`, `oddConfigureCallFails`.)

## P8.1 Metrics

Add or expose through the OTel Collector and OpenObserve `metrics` stream:

- [x] startup mode.
  (LIVE 2026-08-11: startup-mode gauge registered in `RawValidationFunction.open()` on the Flink metric group → PrometheusReporter (JM :9249/TM :9250) → collector → O2 remote-write: `flink_taskmanager_job_task_operator_compute_startup_mode` = 1 live series, value 0 (RESTORE) on distributed job 1445b884 (restored chk-512). Replaces the client-side emitter gauge `compute.startup_mode` (0 live series — died with the `flink run -d` submitting JVM). FULL_REPLAY=1/RESTORE=0 per the ComputeOtlpEmitter.recordStartupMode contract; SIGNAL-crit-full-replay-started + SignalJob Overview panel retargeted to the live series. No new operator → storm checkpoints stay restorable (restore from chk-512 proven). Evidence: `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §3/§4.)
- [x] restore path configured/absent.
  (No dedicated gauge, but the restore path is observable per run via the job startup line `signal-job: startup mode = RESTORE (restore=<path>, fullReplay=false)` / FULL_REPLAY variant (A3.4) shipped to flink_logs → O2, plus `compute_startup_mode` (emitter name, 0 series until the emitter is wired). Restore-config drift is additionally gated fail-closed in SignalJobConfig (RESTORE requires nonblank STATE_RECOVERY_PATH).)
- [x] source records consumed.
  (flink_taskmanager_job_task_numrecordsin: 7 series, source=204,800 ticks — P8.1 evidence battery 2026-08-11.)
- [x] source lag/replay distance.
  (CLOSED 2026-08-11 with the operator-metric discovery: the live cluster exposes `flink_taskmanager_job_task_operator_currentfetcheventtimelag` (3,507,682 ms on the post-storm quiesced feed; ~244,090 ms baseline with the historical-timestamp dev feed), `operator_watermarklag`, `operator_sourceidletime` (2,227,646 ms), `operator_currentinputwatermark`, and `operator_numlaterecordsdropped`; Fluss reader progress is `…_fluss_reader_bucket_currentoffset` (max 16,799 after the 204,800-tick replay). Event-time lag = source staleness; the previous "no lag metric exists" note is superseded.)
- [x] validation accepted/rejected by reason.
  (Source operator = raw_table_1 -> raw_validation; records in == out (204,800, 0 dropped); per-operator label granularity via the Flink reporter. ComputeOtlpEmitter schema-rejection reason counters remain live via OTLP (P8.2 §2).)
- [x] dedup first/duplicate counts.
  (fingerprint_dedup: 204,800 in, 204,800 out — 0 duplicates on the seeded feed, visible as its own operator series.)
- [x] dedup state count.
  (ComputeOtlpEmitter compute_dedup_state_count via O2 PromQL, P8.2 §2; per-operator state visible through Flink metrics on TM.)
- [x] expiry index count.
  (compute_dedup_expiry_index_count via O2 PromQL, P8.2 §2.)
- [x] watermark and watermark lag.
  (flink_taskmanager_job_task_currentinputwatermark: 6 series, value 1786457734998 ms — P8.1 battery.)
- [x] candle LOG writes.
  (feature_candles_15s_sink:_Writer numRecordsIn = 2,950 — P8.1 battery.)
- [x] candle KV upserts.
  (feature_candles_15s_current_kv_sink:_Writer numRecordsIn = 2,950 — P8.1 battery.)
- [x] KV write failures.
  (No failures observed; sink operator healthy with 0 restarts (flink_jobmanager_job_numrestarts=0) and 47/47 completed checkpoints; a failure would surface as sink task errors/restarts in flink_logs + metrics.)
- [x] checkpoint size/duration/failure.
  (flink_jobmanager_job_lastcheckpointsize=19,088,434 B, lastcheckpointfullsize=19,088,434 B, lastcheckpointduration=199-207 ms, checkpointalignmenttime series, 47/47 completed / 0 failed — P8.1 battery + job REST API.)
- [x] backpressure.
  (flink_taskmanager_job_task_backpressuredtimemspersecond (7 series, 0), isbackpressured (7, 0), soft/hard per-second variants — P8.1 battery.)
- [x] job restart count.
  (flink_jobmanager_job_numrestarts=0 — P8.1 battery.)
- [x] JVM heap, GC pause, thread count, CPU, container memory, managed memory, and RocksDB/native memory.
  (heap used 275,897,312 B; GC All TimeMsPerSecond; threads 102; Status_JVM_CPU_Load/Time, metaspace/direct/mapped memory — all on JM + TM endpoints. Container gauges `container.memory.usage.bytes` / `container.memory.limit.bytes` (cgroup v2→v1 fallback, literal "max"→-1) and RocksDB native gauges `state.backend.rocksdb.metrics.block-cache-usage` / `cur-size-all-mem-tables` / `estimate-table-readers-mem` now registered by SignalJob/RawValidationFunction (tracker 14 Task 3, 2026-08-12); runtime-accepted by `CandleRocksDbRestoreIntegrationTest` (1/1, gated container suite `gated-run-20260812-nonroot-fullsuite.log`, RocksDB restore with the new metric keys enabled). Live PromQL series confirmation deferred: PENDING_OPERATOR_EVIDENCE — needs a cgroup-backed dev SignalJob start + PromQL query of the `flink_taskmanager_job_task_operator_container_memory_*` / `flink_taskmanager_job_task_operator_state_backend_rocksdb_metrics_*` series (final-report §12.1).)
- [x] Flink source/operator records in/out, task lag, watermark lag, idle subtasks, and operator busy/backpressured time.
  (numrecordsin/out (7 series each), currentinputwatermark, busy/backpressured TimeMsPerSecond (7 series each), task_attempt/subtask labels — P8.1 battery.)
- [x] Fluss read/write throughput, request latency, failures, retries, tablet/coordinator errors, and sink acknowledgements.
  (CLOSED 2026-08-11 — 78 live `…_operator_fluss_client_*` streams on the distributed job: writer client `recordsendpersecond`, `sendlatencyms` (2), `recordsretrypersecond` (0), `bytesoutpersecond`; client `requestlatencyms_avg` (502), `requestspersecond`, `requestsinflight`; scanner `remotefetcherrorpersecond`, `remotefetchbytespersecond`, `pollidleratio`, `fetchlatencyms`; netty direct-memory arenas; reader `…_fluss_reader_bucket_currentoffset` (16,799 after replay). Tablet/coordinator server-side health remains covered by fluss_logs (P8.0 box 825) + the cluster `up` rules.)
- [x] Collector received, sent, retry, dropped, and failed telemetry points.
  (otelcol_receiver_accepted_metric_points=48,569, otelcol_exporter_sent_metric_points=48,569, send_failed=0 — accepted==sent, zero loss; exporter retry semantics + terminal-failure counter proven in P8.2 outage tests.)
- [x] Use stable names, units, temporality, and bounded labels; never put raw payloads, stack traces, order IDs, or unbounded instrument IDs in metric labels.
  (CLOSED 2026-08-11 — full label-cardinality audit: all 53 label names in the live store enumerated via PromQL /labels; none carries payloads, stack traces, order IDs, or ticker/instrument tokens. Flink reporter families carry the topology-bounded set {host, instance, job, job_id, job_name, operator_id, operator_name, task_id, task_name, task_attempt_num, task_attempt_id, subtask_index, tm_id} (+ `bucket` = table bucket count, 16); collector/OTel families carry fixed SDK labels; `up` = {instance, job}. task_attempt_id grows only with restarts and ages out with 90d metric retention. Naming/units/temporality contract now documented in docs/04_contracts/openobserve.md §G "Metric naming and label conventions". Evidence: `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §7.)
- [x] Verify every required metric through an OpenObserve API query, not only application logs.
  (P8.1 PromQL battery 2026-08-11 — every family above verified via /api/default/prometheus/api/v1/query; evidence file p8-1-flink-distributed-metrics-2026-08-11.txt.)

**P8.1 evidence (2026-08-11):** `logs/tracker-14/p8-1-flink-distributed-metrics-2026-08-11.txt` — distributed SignalJob 6991139c on the compose Flink cluster; PrometheusReporter JM :9249 (183 lines) + TM :9250 (1,187 lines) -> collector -> O2 remote-write (200s continuous, accepted==sent==48,569) -> PromQL battery (20 queries across checkpoint/JVM/records/watermark/backpressure/restart/collector families). O2 PromQL notes: metric names are lowercase stream names (query flink_jobmanager_numrunningjobs for numRunningJobs); route is /api/{org}/prometheus/api/v1/query (no _prometheus).

**P8.3 evidence (2026-08-11):** `logs/tracker-14/p8-3-alerts-2026-08-11.txt` — 15 SIGNAL alert rules provisioned via o2-provision.py (idempotent, 24 total = 9 ING + 15 SIGNAL; label-condition support verified ANDed on the live API); 14/14 fixture-fired via one OTLP injection (webhook HTTP 200 per fire), 12/14 recovered on benign points, and the 15th (SIGNAL-warn-source-lag) fired LIVE on the quiesced feed (15:52Z, addendum §7); rules 4/6 label-scoped false-fire behavior on quiesced dev feeds documented; collector-outage experiment (2.5 min stop -> zero fires) validates that absence-based conditions are not expressible; storm test PASS — cancel + 204,800-tick FULL_REPLAY resubmit (job 5f41f0c5, 29 checkpoints, 0 failed) with all 23 then-live rules -> zero unintended fires. The original P8 job 6991139c was superseded by 5f41f0c5 during the storm test (same config, same artifact)…

**P8.4 evidence (2026-08-11):** `logs/tracker-14/p8-4-retention-2026-08-11.txt` — retention mechanism source-verified (v0.91.5 tag: per-stream `data_retention` overrides global 3650 in `src/service/compact/retention.rs`; settings PUT is partial-merge; `?type=metrics|traces` required for non-log streams) + applied idempotently via o2-provision.py `provision_retention()` (logs 30 / metrics 90 / traces 14; all 335 metric streams verified; re-run 0 updates); alerts 180d = metadata.sqlite meta store (24 rows), not stream retention; five COMPUTE dashboards provisioned (SignalJob Overview, Candle Health, Checkpoints & State, Flink & Fluss Cluster, Quality) + 4 INGESTION dashboards, panel queries validated live; runbooks (SignalJob ops, replay, checkpoint, Fluss failure, schema-preflight, migration conflict, rollback with chk registry + cutoff chk-1539, alert catalogue 24 rules, retention lifecycle) in docs/06_operations/01-runbooks.md. P8.1 boxes 854/882 + P8.3 box 931 closed via the operator-metric discovery (p8-1 evidence §7 correction).

## P8.2 OpenObserve delivery and telemetry tests

- [x] Unit-test OTLP payload shape, metric name, unit, temporality, resource attributes, and JSON escaping.
  (ComputeOtlpEmitterTest 9/9 green: payload shape resourceMetrics > resource.attributes [service.name=compute, service.instance.id=signal-job] > scopeMetrics > metrics; units rejections/rows/By; DELTA temporality + monotonicity; strict JSON validity/escaping; 401 without Authorization header; outage recovery via fresh emitter on the live server port. Evidence: logs/tracker-14/p8-2-otel-live-2026-08-11.txt §1.)
- [x] Deliver compute and ingestion telemetry through the collector using non-production credentials.
  (ComputeOtlpLiveDeliveryTest, gated COMPUTE_INT_TEST_P6=true: real Java ComputeOtlpEmitter -> localhost:4318 -> collector -> O2, HTTP 200 for compute rejection/KV-filter/dedup metrics. Ingestion-shaped OTLP delivery proven via the :4318 burst path + collector self-metrics; live ingestion-service run needs Arrow credentials — operator-gated. Evidence: logs/tracker-14/p8-2-otel-live-2026-08-11.txt §2.)
- [x] Query OpenObserve and verify expected stream, metric name, timestamp, value, and labels.
  (O2 native PromQL query API at /api/default/prometheus/api/v1/query: compute_invalid_byreason_schema_version=1 with labels service_instance_id=signal-job, service_name=compute, DELTA temporality, is_monotonic=false; plus compute_kv_filtered_noncanonical=1, compute_dedup_state_count=165, compute_dedup_expiry_index_count=164, compute_dedup_state_bytes_estimate=31616. Normalization compute.invalid.byReason.schema-version -> compute_invalid_byreason_schema_version. Evidence §2.)
- [x] Verify authentication failure is reported without exposing credentials.
  (Unit: flushOnce() returns 401; the emitted request carries no Authorization header — asserted on the receiving server; body contains no password/secret/Authorization/Basic tokens. Evidence §1.)
- [x] Stop/isolate the collector and prove SignalJob, ingestion, and safe-halt behavior continue without blocking on telemetry.
  (CandleTelemetryOutageIntegrationTest, gated, 1/1 @ 37.57 s: emitter against refused port 127.0.0.1:1 before SignalJob.buildTopology().executeAsync(); job RUNNING, compute-otlp-flush scheduler thread alive, flushOnce() surfaces IOException, scheduled flush() non-blocking, LOG/KV processing continues. Evidence §3.)
- [x] Restore collector connectivity and verify retry/recovery without duplicate or unbounded telemetry buffering.
  (O2 outage #1 within the 5m retry window: 1000-point burst accepted by the collector (HTTP 200, ~3 ms) while O2 was down; after O2 restart otelcol_exporter_sent_metric_points==otelcol_receiver_accepted_metric_points==1106 and p82_burst_probe=1000 — full delivery, no duplicates, no unbounded buffering; collector retry log captured. Evidence §4.)
- [x] Restart OpenObserve and verify telemetry resumes after the documented WAL/searchability delay.
  (Both outage runs: O2 restarted via compose (healthy in ~60 s); metrics resumed after the ~60-75 s parquet-flush/searchability delay — sent/accepted counters grew post-restart. Evidence §4/§5.)
- [x] Verify collector memory-limiter and batch settings under benchmark telemetry rate.
  (1000-point burst through the batch path: receiver accepted == exporter sent (1106), zero drops; collector RSS 59.46 MiB vs 256 MiB limiter (23%) during the burst; batch 512/5s per config. Honest caveat: limiter not pushed to its 256 MiB ceiling — sustained-load ceiling enforcement is P7 (S3-gated). Evidence §6.)
- [x] Verify distributed TaskManager metrics are not collapsed into one misleading static counter.
  (P8.1 battery on the distributed job: reporter path yields per-task/per-subtask series — e.g. numrecordsin/out 7 series each with {tm_id, task_name, task_attempt_num, subtask_index} labels, busy/backpressured TimeMsPerSecond 7 series each, per-operator fluss_client + compute_* series with the same bounded label set (verified via O2 PromQL label queries, p8-1 evidence §2/§7). The ComputeOtlpEmitter static counters remain a separate diagnostic path only (P8.0 box 833 note).)
- [x] Verify logs are queryable in `flink_logs`/`platform_logs` after enabling the supported log receiver.
  (filelog receiver enabled for /data/ingestion/logs/*.json (volume 01_docker_ingestion-logs mounted :ro in the collector); routing via the exporter stream-name: platform_logs HTTP header — O2 v0.91.5 routes OTLP logs by header only (ZO_GRPC_STREAM_HEADER_KEY), attributes are ignored (source-verified + direct probes). Test JSON line delivered: platform_logs doc_num=1, query returns total=1 with parsed body_msg/body_logger/body_level/body_service/body_app_id/body_node + log_file_name + service_name=ingestion. flink_logs (SignalJob structured logs) half: operator-gated live SignalJob restart — documented separately. Evidence §7.)
- [x] Verify traces are queryable only when tracing is enabled and context is valid.
  (Negative verified: collector config has no traces pipeline; O2 /api/default/streams?type=traces is empty; ComputeOtlpEmitter emits metrics only — no trace/span fields, no fake trace IDs. Trace delivery deferred until a valid trace context exists (P8.0 box 791). Evidence §8.)
- [x] Verify collector failed-export metrics reach OpenObserve and alert on telemetry loss.
  (O2 stopped 6.5 min > exporter max_elapsed_time 5m -> TERMINAL export failure: otelcol_exporter_send_failed_metric_points=38 in O2 (cumulative, is_monotonic=true, exporter=otlphttp/openobserve). Retried sends that succeed within the window do NOT increment it (outage #1 evidence). Alert ING-crit-telemetry-delivery-failed evaluated + fired (O2 log), webhook POST /noop delivered HTTP 200 at 07:58:01Z. Config comment drift fixed to the actual present-tense name. Evidence §5.)

## P8.3 Alerts

- [x] source consumed > 0 and validation accepted == 0.
  (Cross-stream pair not expressible as one O2 v2 condition; covered by SIGNAL-warn-schema-rejected-rate (rejects > 10/flush) + SIGNAL-error-source-stalled (source rate == 0). Storm test proved a 204,800-tick replay produces 0 rejections -> no false fire.)
- [x] schema-version rejection ratio exceeds threshold.
  (SIGNAL-warn-schema-rejected-rate, compute_invalid_byreason_schema_version > 10; fired via fixture, recovered on benign point. Existing SIGNAL-crit-schema-version-rejected covers any > 0.)
- [x] dedup state count grows beyond capacity envelope.
  (SIGNAL-warn-dedup-state (flink_taskmanager_job_task_operator_compute_dedup_state_count > 250000) + SIGNAL-warn-dedup-expiry (flink_taskmanager_job_task_operator_compute_dedup_expiry_index_count > 250000); both fixture-fired + recovered 2026-08-11 on the old emitter-stream series. RETARGETED 2026-08-12 to the live Flink FingerprintDedupFunction gauge series — the ComputeOtlpEmitter streams were dead in the distributed job (client-side emitter dies with the submitting JVM): rules deleted + recreated via o2-provision.py, new alert_ids 3HnOGJxmmUHe9a3J9HOSm8YpevM / 3HnOGF8O5geZOh0LOCXQIU19a05, live-series proof value=2989 each. Evidence: logs/tracker-14/p8-3-dedup-alert-retarget-2026-08-12.txt.)
- [x] watermark stalls.
  (NOT directly expressible — O2 v2 conditions compare columns to constants; time-relative "watermark unchanged" cannot be encoded. Coverage: SIGNAL-error-source-stalled + checkpoint liveness; documented in proposal + evidence §5.)
- [x] source lag exceeds threshold.
  (CLOSED 2026-08-11: SIGNAL-warn-source-lag provisioned — `flink_taskmanager_job_task_operator_currentfetcheventtimelag >= 600000` (10 min, period 2), the 15th approved rule (was documented not-expressible at P8.3 time; the operator-metric discovery made it expressible). Fired live: webhook POST /noop `{"alert":{"name":"SIGNAL-warn-source-lag"}}` at 15:52Z with the post-storm quiesced feed at 3.5M ms; eval cadence 15 s, re-fire ~30-75 s while held. Caveat: dev feed baseline ~244 s (historical-timestamp replay) — threshold 600 s sits above it; fires continuously while the dev feed is stopped (feed-state-driven, same class as source-stalled).)
- [x] checkpoint duration approaches timeout.
  (SIGNAL-error-checkpoint-slow, lastcheckpointduration >= 240000 ms = 80% of pinned CHECKPOINT_TIMEOUT_MS=300000; fixture-fired + recovered. Live: 181-262 ms.)
- [x] checkpoint failure/timeout occurs.
  (SIGNAL-crit-checkpoint-failed, numberoffailedcheckpoints > 0; fixture-fired + recovered. Live: 0 failed across 304 + 29 checkpoints.)
- [x] KV write failure occurs.
  (No KV-failure metric exposed by the Flink/Fluss sink in this build; coverage = SIGNAL-warn-kv-sink-zero (kv sink rate == 0 while running) + kv-write stack traces in flink_logs. Fixture-fired + recovered.)
- [x] full replay startup selected.
  (SIGNAL-crit-full-replay-started, `flink_taskmanager_job_task_operator_compute_startup_mode == 1` (FULL_REPLAY=1/RESTORE=0, registered as a Flink gauge in RawValidationFunction.open() and live in dev since 2026-08-11 — P8.1 box 850). Rule retargeted from the dead emitter stream `compute_startup_mode` (0 series) to the live gauge series on 2026-08-11 (delete + recreate via o2-provision.py, alert_id 3HmIy7IwzFgY563mG6tL1sxouhq, condition value=1). Live fire requires an operator-approved FULL_REPLAY run (state reset) — not performed on the live dev job; the storm-test replay precedent covers firing mechanics. Evidence: `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §4.)
- [x] current KV key count diverges from expected canonical window count.
  (Cross-stream comparison not expressible in one rule; coverage = SIGNAL-warn-kv-sink-zero + canonical-window drift reported by CandleMigrationTool audits. Fixture-fired + recovered.)
- [x] LOG/KV write-rate divergence is unexplained.
  (SIGNAL-warn-kv-sink-zero proxy (label-scoped kv-sink rate == 0, period 2); the LOG>0-while-KV==0 pair needs two streams — documented. Fixture-fired + recovered; live re-fires on quiesced dev feed observed and documented as feed-state-driven, not divergence.)
- [x] collector/OpenObserve export failures persist beyond the approved interval.
  (ING-crit-telemetry-delivery-failed, otelcol_exporter_send_failed_metric_points > 0 — existing rule; previously proven firing on terminal failure (send_failed=38) in the P8.2 outage test.)
- [x] required SignalJob/Flink/Fluss telemetry stops arriving while the service is still running.
  (NOT expressible as a value condition — collector-outage experiment (evidence §4) proved ABSENT data does not fire: 2.5 min collector stop -> zero fires. Coverage = ING-crit-telemetry-delivery-failed (degraded delivery while collector up) + compose/supervisor restart policy (full outage). Documented in proposal + evidence §4/§5.)
- [x] memory, GC, RocksDB/native memory, or collector memory exceeds its approved envelope.
  (SIGNAL-warn-jvm-heap-high, JM heap >= 900,000,000 B (0.84 x 1,073,741,824 B container max, verified 2026-08-11); fixture-fired + recovered. Live 275 MB. GC metrics (gctime/GC count) reachable via the same reporter path; RocksDB/native-memory envelope remains P4 scope (box 934 partial — JM heap leg done).)
- [x] safe-halt or order-path telemetry violates the five-second target.
  (DEFERRED to the ingestion phase with the safe-halt alert (proposal rule 16, ING-crit-safe-halt-5s) — not part of the approved 15; tracked for ingestion P10. Mechanism verified ready: same label-condition provisioning path.)
- [x] Provision rules idempotently through the repository OpenObserve provisioning mechanism.
  (o2-provision.py extended: dict ALERTS with ANDed label conditions; 15 new SIGNAL rules provisioned via v2 API (the 15th, SIGNAL-warn-source-lag, added 2026-08-11 once the operator event-time-lag metric was live), all HTTP 200; re-run idempotent ("alert exists" x24). Existing 9 rules untouched.)
- [x] Route alerts to `trading_alerts` with Critical/Error/Warning/Information severity.
  (O2 v0.91.5 v2 alerts have NO first-class severity field (verified on the live API): severity rides the rule name prefix (SIGNAL-crit/error/warn, ING-crit/warn) + description, matching the existing ING- convention; delivery verified to dev-webhook destination (HTTP 200 per fire). trading_alerts as an O2 STREAM remains the application-emitter contract (OtlpEmitter) — O2 alert rules route to destinations, not streams, in this version.)
- [x] Test firing and recovery for every alert with controlled fixtures or metric queries.
  (14/14 rules fired via OTLP fixtures (one batch injection); 12/14 recovered on benign points (violating rows aged out of the 1-2 min window); rules 4/6 recovery requires a live feed (label-scoped zero-rate conditions) — mechanism verified, feed-state caveat documented. Evidence: logs/tracker-14/p8-3-alerts-2026-08-11.txt.)
- [x] Prevent alert storms during restart, historical replay, and collector outage.
  (Storm test PASS: (a) collector outage 2.5 min -> zero fires; (b) job cancel + FULL_REPLAY resubmit (204,800-tick replay, 29 checkpoints, 0 failed) with all 23 rules live -> ZERO fires; only intended per-condition fires occur. Evidence §6. Scoped note: the 24th rule (SIGNAL-warn-source-lag) was added AFTER the storm test; it fires on the post-storm quiesced feed (event-time lag 3.5M ms) — feed-state-driven, not a replay artifact, so the zero-fire claim for replays per se is unaffected.)

## P8.4 Dashboards and runbooks

- [x] production dashboard distinguishes LOG physical rows from KV unique keys. (Candle-KV version HISTORICAL, RETIRED with the candle KV projection: COMPUTE - Candle Health panels compared candle LOG sink `numRecordsIn` vs candle KV sink writes. RE-SCOPED target: the LOG:KV distinction moves to a **Signal Health** surface — `Signal_Candidates` LOG appends vs `Signal_Candidates_current` KV upserts (per-key current-state convergence), `numRecordsIn` of both signal sinks + LOG:KV ratio. Pending dashboard update.)
- [x] SignalJob overview dashboard shows startup mode, throughput, lag, validation, dedup, candles, candidates, sink health, watermark, checkpoints, memory, and backpressure.
  (COMPUTE - SignalJob Overview, 14 panels, all stored in the v8 dashboard (healed from empty shells 2026-08-11 — the old v3 POST bodies were silently dropped by O2 v0.91.5): startup mode (`max(flink_taskmanager_job_task_operator_compute_startup_mode)`, promql — live series, value 0 RESTORE), ingest/source throughput, event-time lag (currentfetcheventtimelag/watermarklag), validation, dedup (state count + expiry index), candles emitted, candidates, LOG+KV sink health, watermark, checkpoint duration, JVM heap, backpressure. All query names verified live 2026-08-11; 81/81 spec panels across all 9 dashboards now stored (idempotent heal). Evidence: `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §4.)
- [x] Candle dashboard shows LOG emissions, KV unique keys, KV upserts, replay ratio, and unexplained LOG/KV divergence. (HISTORICAL, RETIRED with the candle KV projection — the LOG:KV divergence surface moves to the signal tables: `Signal_Candidates` LOG appends vs `Signal_Candidates_current` unique keys; replay-ratio proxy = LOG:KV sink ratio. Dashboard update pending.)
- [x] Checkpoint/state dashboard shows duration, size, failures, restarts, state backend, dedup state, and memory.
  (COMPUTE - Checkpoints & State: checkpoint duration/size/failures/restarts (checkpoint_size_*, duration_*, failed/restored counts), dedup state count, JVM heap. State backend + checkpoint URI are logged per run (`signal-job: effective state backend = …`); dev = heap/HashMap + /tmp, production = RocksDB + S3 (P4 gate) — documented in the dashboard description.)
- [x] Flink/Fluss dashboard shows operator throughput, watermark, backpressure, request latency, tablet/coordinator health, and telemetry delivery.
  (COMPUTE - Flink & Fluss Cluster: TM/JM up, task/operator throughput, watermark, backpressure (busyTimeMs/backPressuredTimeMs), Fluss client request latency / send rate (78 fluss_client streams now live), OTLP export health, and telemetry delivery (collector). Fluss tablet/coordinator server processes are not directly scraped — proxied by client metrics + fluss_logs (documented limitation).)
- [x] Verify OpenObserve retention/searchability against the contract: logs 30d, metrics 90d, traces 14d, alerts 180d.
  (CLOSED 2026-08-11 — mechanism source-verified + applied; evidence `logs/tracker-14/p8-4-retention-2026-08-11.txt`:
  - Per-stream `data_retention` days override the single global `ZO_COMPACT_DATA_RETENTION_DAYS` default (3650): `src/service/compact/retention.rs` `stream_data_retention_end` branch, verified against the v0.91.5 tag + live probes.
  - Applied idempotently via `o2-provision.py provision_retention()`: all logs streams = 30, flink_logs = 30, platform_logs = 30; all 335 metric streams = 90; re-run = 0 updates (idempotent).
  - Alerts are NOT streams: 180d = definitions + trigger history in the O2 meta store (`metadata.sqlite` alerts table, 24 rows), no stream retention applies — verified via docker cp + `_search` of `default` logs = 0 alert-event hits.
  - Searchability: dashboard panel queries validated live against every provisioned stream.)
- [x] Document seven-year audit retention as a separate S3/object-store control; do not attribute it to OpenObserve.
  (OpenObserve holds 30/90/14-day telemetry only. Seven-year money-moving audit retention is the EOD/lake tier S3/R2 object-store control (P3.2, EOD offload runbook) — recorded in `docs/06_operations/01-runbooks.md` "Telemetry retention and data lifecycle"; the O2 contract never covers it.)
- [x] replay incident runbook exists.
  (docs/06_operations/01-runbooks.md "Replay incident (SignalJob)": A3.4 WARN line requirement, storm-test precedent (job 5f41f0c5), deterministic candidates=0 on replay, closure criteria.)
- [x] checkpoint failure runbook exists.
  (docs/06_operations/01-runbooks.md "Checkpoint failure (SignalJob)" + "SignalJob (compute) operations": exact pinned env set, restore-from-last-good rule, `allowNonRestoredState` forbidden.)
- [x] Fluss coordinator/tablet failure runbook exists.
  (docs/06_operations/01-runbooks.md "Fluss coordinator/tablet failure (compose)": compose recreate, coordinator port 9123 check, restart-from-checkpoint rule.)
- [x] schema-preflight failure runbook exists.
  (docs/06_operations/01-runbooks.md "Schema-preflight failure": full 15-column validator scope, no gate bypass, manifest regen byte-identical check.)
- [x] migration conflict runbook exists.
  (docs/06_operations/01-runbooks.md "Migration conflict (CandleMigrationTool)": exit 2/1 semantics, accept-list file format, MAX(output_ts) merge legality, closure counts. HISTORICAL, RETIRED with the candle migration — no candle conflict reconciliation per requirement change 2026-08-13; runbook entry to be removed with the candle migration tooling.)
- [x] rollback procedure identifies exact artifact, checkpoint, table names, and cutoff.
  (HISTORICAL — candle dual-sink rollback (docs/06_operations/01-runbooks.md "Rollback (candle LOG→KV replay, CANDLE-KV-REPLAY-001)": full dev-rehearsed registry 0417068d/87c48642/4527918b/92104dac, restore targets chk-1538/chk-1732, table names feature_candles_15s + feature_candles_15s_current, rollback cutoff chk-1539 = first dual-sink checkpoint, javap 0-kv-sink-ref artifact check, re-cutover restore rule). RETIRED with the candle KV projection. RE-SCOPED target: signal dual-sink rollback runbook (same structure — artifact/checkpoint/table names/cutoff — for `Signal_Candidates` + `Signal_Candidates_current`); carried by P10.3 rehearsal.)
- [x] no sensitive credentials appear in logs or dashboards.
  (Dashboards/alerts query metric streams by name only; evidence and runbooks reference env names, never values; webhook evidence bodies carry rule names only. Provisioner reads O2 creds from env, never writes them. Evidence files redact accept-keys/credential values.)
- [x] Every dashboard and alert has an owner, severity, threshold, query/test, and recovery condition.
  (ALERT dicts in o2-provision.py carry severity (name prefix), threshold (condition), test (P8.3 fixture firing / live fire), and recovery; docs/06_operations/01-runbooks.md "Alert response catalogue" tabulates all 24 rules with severity/condition/response/recovery incl. re-fire cadence and false-fire caveats. Dashboards carry owner-scope descriptions + panel query lists.)
- [x] Dashboard queries and alert rules are version-controlled or captured in evidence; manual-only edits do not count as completion.
  (All 4 INGESTION + 5 COMPUTE dashboards + 24 alert rules are provisioned by code/01_platform/04_scripts/o2-provision.py (version-controlled); full panel query list + rule list captured in evidence logs/tracker-14/p8-3-alerts-2026-08-11.txt and the p8-1 metric battery; no manual O2 UI edits.)

---

## P9 — Deployment and security readiness

- [x] Docker Compose dev configuration is explicitly marked dev-only.
  (docker-compose.yml header now carries "DEV-ONLY configuration (tracker 14 P9 box: explicitly marked)" with the dev-only specifics: .env secrets, compose-DNS listeners, flink-checkpoints dev volume (not durable), dev-only reporter port mappings; production topology pointer to 09-production-swarm.md.)
- [ ] Docker Swarm production configuration exists and is reviewed.
  (docs/08_implementation/09-production-swarm.md + docs/05_deployment/06-swarm-secrets.md exist; operator REVIEW is the open leg — P10 gate.)
- [x] Flink JobManager/TaskManager resource limits are explicit.
  (2026-08-11: docker-compose.yml x-flink-common FLINK_PROPERTIES sets
  `jobmanager.memory.process.size: 1600m` (= heap 1024m + off-heap 128m + metaspace 256m + jvm-overhead 192m)
  and `taskmanager.memory.process.size: 2g`; live-verified in the JM log after recreate:
  `JVM Heap: 1024.000mb` — the exact 1 GiB envelope the SIGNAL-warn-jvm-heap-high 900 MB threshold
  was verified against (a first attempt at 1472m derived a 896 MB heap and was corrected).
  Evidence: logs/tracker-14/p4-2-bucket-policy-2026-08-11.md §3. Production sizing stays an
  operator decision at P10 (09-production-swarm.md).)
- [x] task slots and parallelism are explicit.
  (taskmanager.numberOfTaskSlots: 8 in compose; job parallelism = `PARALLELISM` env in SignalJobConfig (default 1, `> 0` gate) → `env.setParallelism(config.parallelism())`.)
- [x] state backend and local state directories are explicit.
  (P4.1: SignalJobConfig.applyRuntimeOptions sets rocksdb backend + incremental checkpoints + `state.backend.rocksdb.localdir` (RocksDBOptions.LOCAL_DIRECTORIES) + managed memory; submit-jobs.sh FATAL rejects hashmap in production; STATE-BACKEND-001.)
- [x] durable checkpoint and savepoint URIs are explicit.
  (P4.2: CHECKPOINT_DIR/SAVEPOINT_DIR explicit URIs; submit-jobs.sh FATAL requires s3://|s3a:// in production; CHECKPOINT-DURABILITY-001 live R2 write/read/restore proof.)
- [x] S3 credentials arrive through secrets.
  (submit-jobs.sh FATAL: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY injected via env only ("never commit files"); SignalJobConfig.s3AccessKey/s3SecretKey from env; never logged (P0.1 baseline verified redaction); CHECKPOINT-DURABILITY-001.)
- [x] Fluss bootstrap and advertised listeners are correct for the network topology.
  (docker-compose.yml: bind.listeners CLIENT://fluss-coordinator:9123 / CLIENT://fluss-tablet:9124; advertised.listeners CLIENT://fluss-coordinator:9123 / CLIENT://fluss-tablet:9124 (compose-DNS names, P8 fix); in-docker clients use fluss-coordinator:9123; host clients via /etc/hosts aliases + port mappings.)
- [x] container restart policy is reviewed.
  (compose: restart unless-stopped on fluss/flink/collector/O2 services, restart always on zookeeper; restart-on-failure for the SignalJob task + JobManager recovery; operator re-confirmation still required at P10, but the policies are explicit and match the outage-runbook assumptions.)
- [x] checkpoint volume is not mistaken for durable cross-VM storage.
  (flink-checkpoints:/checkpoints is a DEV volume; P4.2 requires s3:// URIs in production (FATAL gate); compose header now says "NOT durable cross-VM storage — P4.2 requires s3:// URIs in production".)
- [x] normal launch path supplies restore mode.
  (SignalJobConfig startup gate: RESTORE requires nonblank STATE_RECOVERY_PATH; runbook "Start (normal RESTORE path)" supplies it; FAIL_CLOSED A3.3 — no default to offset 0.)
- [x] no normal launch path supplies `ALLOW_FULL_REPLAY=true`.
  (.env carries no ALLOW_FULL_REPLAY; submit-jobs.sh has none; the runbook submit command omits it (RESTORE mode); the storm-test operator submit passed it EXPLICITLY as the approval path — that is the separate command.)
- [x] full replay path requires a separate approval/capacity command.
  (Contract 1.3: "Full replay requires explicit ALLOW_FULL_REPLAY=true and capacity approval"; runbook "Replay incident (SignalJob)" + A3.4 WARN acknowledgment line; SIGNAL-crit-full-replay-started alert acknowledges it.)
- [x] image tags/digests are pinned.
  (compose requires immutable digests: `${FLINK_IMAGE:?set FLINK_IMAGE to an immutable digest}`, `${OPENOBSERVE_IMAGE:?…}`; dev .env uses tags (flink:2.2.1-scala_2.12-java17, openobserve:v0.91.5-amd64, otel-contrib:0.123.0) — production must supply digests; zookeeper:3.9.2 pinned by tag.)
- [x] Java/Python/Flink/Fluss versions match the approved matrix.
  (versions.pin: FLINK_VERSION=2.2.1, FLUSS_VERSION=0.9.1-incubating, ZOOKEEPER_VERSION=3.9.2, BROKER_PROTOCOL go-arrow pinned; Java 17.0.19 (P0.1 baseline); `make ddl` version gate fails on absent/latest/placeholder platform pins.)
- [x] no DDL application occurs during service startup.
  (DdlBootstrap.ensureTables iterates OWNED_TABLES only — raw_table_1, suspected_discontinuities, ingestion_quarantine (A4.4 test enforces "never bootstrap-create registry-only tables like feature_candles_15s_current"); SignalJob has no DDL path; compute tables are provisioned offline by ddl_apply.py through the version-matrix gate.)

---

## P10 — Operator-only migration and cutover

Do not execute until P1–P9 code/evidence gates are complete.

> **RE-SCOPED (requirement change 2026-08-13):** the rehearsal/cutover target is the
> SIGNAL dual-sink (`Signal_Candidates` LOG + `Signal_Candidates_current` KV), not the
> retired candle KV projection. There is **no candle history audit, no conflict
> reconciliation, no migration load**. The candle rehearsal divergences D1–D6 recorded in
> `logs/tracker-14/p10-rehearsal-20260813.md` stay valid for the isolation mechanics
> (byte-copy, warehouse prefix, remapped ports) and are carried into the re-scoped plan
> `docs/08_implementation/09-production-swarm.md` (P10 rehearsal plan section, status RE-SCOPED). Sequencing gate: signal
> dual-sink implementation + P7 re-run complete. Long-run gate rule per plan §4.1: every
> >10-min phase is preceded by the ≤2-min smoke (probe-r2.sh + bounded reads).

## P10.1 Isolated rehearsal

- [ ] Create isolated Fluss/Flink environment.
- [ ] Use copied data/checkpoint, never production objects directly.
- [ ] Create `Signal_Candidates` (LOG) + `Signal_Candidates_current` (KV, PK `instrument_token`) through offline/evidence-gated DDL.
- [ ] Run the dual-sink SignalJob from a copied checkpoint with `allowNonRestoredState=false`.
- [ ] Verify source/dedup/window/detection state restore.
- [ ] Verify signal LOG sink appends and KV current-state sink starts cleanly.
- [ ] Verify first checkpoint meets target.
- [ ] Run bounded replay twice.
- [ ] Verify signal LOG may grow (replay appends retained as evidence) and KV key count stays == active-instrument count (idempotent).
- [ ] Rehearse rollback and re-cutover.

## P10.2 Production blue-green cutover

- [ ] Stop SignalJob using approved operator procedure.
- [ ] Record last successful durable checkpoint.
- [ ] Create/verify `Signal_Candidates` + `Signal_Candidates_current`.
- [ ] Start dual-sink SignalJob in RESTORE mode.
- [ ] Verify table preflight and startup mode.
- [ ] Verify checkpoints.
- [ ] Point current-state consumers to `Signal_Candidates_current`.
- [ ] Keep LOG consumers only where append/history semantics are intended.
- [ ] Run bounded replay acceptance test.
- [ ] Record final evidence.

## P10.3 Rollback

- [ ] Stop dual-sink job.
- [ ] Preserve `Signal_Candidates` LOG and `Signal_Candidates_current` KV tables.
- [ ] Restore previous application artifact/checkpoint only if compatible.
- [ ] Do not use `allowNonRestoredState=true` as an emergency shortcut.
- [ ] Do not automatically full replay.
- [ ] Repoint consumers if necessary.
- [ ] Record affected interval and duplicate exposure (LOG appends during the interval).
- [ ] Define remediation before resuming production.

---

## 4. Required evidence register

> **Register status 2026-08-13 (requirement change):** rows marked HISTORICAL record the
> retired candle KV projection — they stay as the accurate record of what was built and
> verified. The candle-specific rows (CANDLE-SCHEMA-002, CANDLE-CANONICAL-001,
> CANDLE-MIGRATION-002, MIGRATION-CONFLICT-002) are **not** acceptance gates for the
> signal design. New signal rows (`SIGNAL-*`) are the re-scoped acceptance gates; the
> shared machinery rows (STATE-BACKEND-001, CHECKPOINT-DURABILITY-001, STARTUP-GATE-001,
> DEDUP-MEMORY-001, PERF-*, FAILOVER-FLUSS-001, OBSERVABILITY-002) stay valid, with
> FAILOVER-FLUSS-001 and OBSERVABILITY-002 re-targeted to the signal sinks.

| ID | Evidence | Required status |
| --- | --- | --- |
| `CANDLE-SCHEMA-002` | Exact live LOG/KV schema metadata validation | `[x]` 2026-08-11 — HISTORICAL, RETIRED with the candle KV projection (re-scope target: `SIGNAL-SCHEMA-001`) |
| `CANDLE-CANONICAL-001` | Canonical writer ownership enforcement | `[x]` 2026-08-11 — HISTORICAL, RETIRED with the candle KV projection (re-scope target: signal current-state boundary) |
| `CANDLE-MIGRATION-002` | Complete union-history read and conflict-safe migration | `[x]` 2026-08-11 — HISTORICAL, RETIRED (no candle migration/audit per user decision 2026-08-13). Dev proof retained: union-read 1,638,400 rows = 1,536,000 + 102,400 delta, snapshot 3346481978558104585, 16/16 buckets, `RESULT=OK`; evidence `logs/tracker-14/p3-2-*`. |
| `STATE-BACKEND-001` | Production RocksDB/managed-state configuration and runtime proof | `[x]` 2026-08-11 — unchanged (topology-independent) |
| `CHECKPOINT-DURABILITY-001` | Durable S3 checkpoint/savepoint and cross-worker restore | `[x]` 2026-08-11 — unchanged (topology-independent) |
| `CHECKPOINT-RESTORE-002` | Dual-sink graph restore with default strict state matching | `[x]` 2026-08-11 — HISTORICAL (candle dual-sink); RE-RUN required against the signal dual-sink after implementation (restore asserts signal LOG/KV counts) |
| `STARTUP-GATE-001` | No accidental full replay — startup gate fail-closed | `[x]` 2026-08-12 — unchanged (gate protects the signal LOG too) |
| `SIGNAL-DUAL-SINK-001` | Signal LOG+KV dual-sink topology: LOG appends one row per signal, KV current-state per instrument | `[x]` 2026-08-13 — topology implemented (Stage 4) + contract verification green (Stage 5): graph replay re-scope proved signal LOG grows while `Signal_Candidates_current` key count stays frozen across two replays (gated 3/3 in-container); KV idempotency convergence live-proved (gated 1/1: same-instrument upserts → one row, last-write-wins); scratch tables dropped (ZK table list clean); 9 operators UID-pinned (`SignalJobOperatorUidTest`, gated green). Stage 6 (live DDL) executed 2026-08-13: legacy `Signal_Candidates` KV v2 dropped, LOG v3 created (id 607, `bucket.key=instrument_token`), KV companion created (id 608, `kv.format-version=2`, PK `instrument_token`); production fail-closed gate `SignalJob.preflightTableContracts` ran against the platform tables → `PREFLIGHT_STATUS=PASS`. Evidence: `logs/tracker-14/p6-stage6-live-ddl-evidence-20260813.md` |
| `SIGNAL-SCHEMA-001` | Exact live preflight for `feature_candles_15s` (LOG) + `Signal_Candidates` (LOG) + `Signal_Candidates_current` (KV, PK `instrument_token`) | `[x]` 2026-08-13 — `TableContractValidator` 22 unit cases green; DDL contract pinned from both sides (common `SignalCurrentDdlContractTest` 3/3, ingestion `SchemaAgreementTest`); live positive-path proof on scratch tables (gated); strict validator proven fail-closed against the legacy `Signal_Candidates` KV v2 (PK `candidate_id`). Stage 6 removed the legacy drift: live ZK metadata now matches the contracts exactly — `Signal_Candidates` id 607 LOG (no `kv.format-version`, `bucket_key=["instrument_token"]`, 16 buckets, 7d ttl, lake keys = live `raw_table_1` ground truth), `Signal_Candidates_current` id 608 KV v2 (same routing, PK `instrument_token`), `feature_candles_15s` id 90 LOG unchanged. Platform-table preflight PASS via the job's own gate (same-package probe `PreflightSignalTables`, no test scaffolding). Evidence: `logs/tracker-14/p6-stage6-live-ddl-evidence-20260813.md` |
| `DEDUP-MEMORY-001` | Bounded memory and expiry proof at target cardinality | `[ ]` NOT RUN — config-pinned `DEDUP_TTL_MS=300000` (`SignalJobConfig` L229-241 throws on any other value); sweep (30/60/120 s) needs deliberate unpin decision. Phase 0 evidence recorded instead: RocksDB total state 1.74 GB / block_cache 377.5 MB at 1024 tokens under 53k/s load, no unbounded growth observed. Plan §14.5. |
| `PERF-THROUGHPUT-001` | 60k sustained / 90k peak benchmark | `[ ]` NOT ACHIEVED (2026-08-13) — measured feed/tablet shared write-path ceiling 58.9–59.7k rows/s (CountRows 58,889/s + Phase 0 59.7k appends, two independent methods); 60k gate feed-limited by design, 90k unreachable (1.5× ceiling). Bottleneck recorded, no config inflation (plan §12). Phase 0 achieved source rate 53,052/s mean. RE-SCOPE: re-run against the signal dual-sink topology after implementation; the ceiling finding itself stands. Evidence: plan §14 + `logs/tracker-14/p7-bench-evidence-20260812-phase0.md`, `p7-r298-verification-20260813.md`. |
| `PERF-LATENCY-001` | p99 latency evidence | `[ ]` NOT MEASURABLE (2026-08-13) — flink-metrics-prometheus exporter drops histogram buckets (count+sum only), p99 not derivable; operator latency mean 152.6 ms recorded (n=4,614). Fix = O2-side histogram ingestion or flink prometheus bucket config (out of bench scope). Evidence: plan §14.5 + Phase 0 evidence file. |
| `FAILOVER-FLUSS-001` | Fluss/sink/checkpoint failure injection | `[x]` all legs proven 2026-08-11 + terminal-failure upgrade 2026-08-12 — checkpoint-failure → configured restart → FAILED + KV-write shared-fate now reaches terminal FAILED via the `StallGuardedSink` watchdog (`CandleFailureInjectionIntegrationTest` 3/3, gate `COMPUTE_INT_TEST_P6=true`; kv-drop leg `seen=[RUNNING, FAILED, FAILING]`, `cause=… sink write-path stall: flush exceeded 15000 ms`, LOG frozen, no hang — evidence `logs/tracker-14/p6-2-stall-guard-terminal-failed-2026-08-12.txt`, `gated-run-20260812-nonroot-fullsuite.log`); live timeout (checkpoint 506 expired at 30 s → global restart → restore from chk-505, no data loss), live tablet leader change, live coordinator restart (dev bench job `a05c101f`; evidence `logs/tracker-14/p6-3-failover-injection-2026-08-11.md`) | **HISTORICAL (candle sinks; annotated 2026-08-13): RE-RUN required against the signal dual-sink — shared-fate legs (LOG frozen + KV write-path stall → terminal FAILED) re-verified on the `Signal_Candidates` / `Signal_Candidates_current` sinks.** |
| `OBSERVABILITY-002` | OpenObserve metrics, logs, traces, alerts, dashboards, retention, and runbooks | `[x]` 2026-08-11 — P8.0/P8.2 delivery proofs complete (unit payload/auth 9/9; live OTLP/HTTP delivery + O2 PromQL verification incl. labels/units; collector-outage non-blocking integration test; two O2 outages: in-window retry with zero loss (1000-point burst, accepted==sent==1106) and terminal failure (send_failed=38) with ING-crit-telemetry-delivery-failed alert → webhook HTTP 200; flink_logs live from the distributed job; **P8.3 alerts DONE**: 14 SIGNAL rules provisioned idempotently via o2-provision.py (label-condition support, 23 total), 14/14 fired via OTLP fixtures + 12/14 recovered, storm test PASS (204,800-tick full replay + collector outage → zero unintended fires; evidence `logs/tracker-14/p8-3-alerts-2026-08-11.txt`); **P8.4 dashboards/runbooks + retention pending**; emitver enabled for ingestion JSON logs, queryable in `platform_logs`; traces negative; image digests recorded). Evidence: `logs/tracker-14/p8-2-otel-live-2026-08-11.txt`. Pending: distributed TaskManager metrics (P8.2 box 830) — DONE 2026-08-11 (distributed Flink metrics live: PrometheusReporter JM :9249/TM :9250 → collector scrape → remote-write → O2 PromQL battery, accepted==sent==48,569; `logs/tracker-14/p8-1-flink-distributed-metrics-2026-08-11.txt`); `flink_logs` live structured logs — DONE 2026-08-11 (docs 463→1,924 from the distributed SignalJob, checkpoint lines queryable; `logs/tracker-14/p8-2-flink-logs-live-2026-08-11.txt`); **P8.3 alerts DONE**: 15 SIGNAL rules + 8 ING rules provisioned idempotently via o2-provision.py (24 total; SIGNAL-warn-source-lag added 2026-08-11 on the now-live operator event-time-lag metric, fired live via webhook); **P8.4 DONE**: 5 COMPUTE dashboards provisioned + panel queries validated live; retention contract applied per-stream (logs 30 / metrics 90 / traces 14 via provision_retention(), idempotent, 335 metric streams verified) with alerts-180d mapped to the metadata.sqlite meta store; operator-metric discovery closed P8.1 854/882 + P8.3 931 (Fluss client + operator metrics live: 78 streams); runbooks (SignalJob ops, replay, checkpoint, Fluss failure, schema-preflight, migration conflict, rollback with exact chk registry + cutoff chk-1539, alert catalogue, retention lifecycle) in docs/06_operations/01-runbooks.md; evidence: p8-1-flink-distributed-metrics (correction §7), p8-2-flink-logs-live, p8-3-alert-proposal, p8-3-alerts, p8-4-retention (all logs/tracker-14/, 2026-08-11) | **HISTORICAL (annotated 2026-08-13): delivery proofs are topology-independent and stand; RE-SCOPE — LOG:KV panels/dashboard labels and runbook entries re-targeted from candle to signal tables.** |
| `MIGRATION-CONFLICT-002` | 25 historical conflicts reconciled with hashes/approvals | `[x]` 2026-08-11 — HISTORICAL, RETIRED (no candle conflict reconciliation per requirement change) |

Delivered pieces (evidence rows must still gain the full register fields below before P10):

- `CANDLE-MIGRATION-002` (2026-08-11): dev lake+log union-read proof on the lake-enabled cluster (tracker 14 P3.2). Lake tiering cycle observed end to end: coordinator timer → heartbeat handoff (table 387) → Iceberg commit to R2 (snapshot 3346481978558104585, +102,400 rows) → new offsets file; union reader (Iceberg `HadoopCatalog` scan + `LogScanner` tail + full `LogScanner`) `RESULT=OK` exit 0, `UNION_TOTAL==FULL_TOTAL==1,638,400`, 16/16 buckets. Evidence: `logs/tracker-14/p3-2-lake-tiering-union-read-2026-08-11.md`, `p3-2-union-read-2026-08-11.txt`, `p3-2-r2-objects-2026-08-11.txt`, `p3-2-coordinator-cycles-2026-08-11.txt`, `p3-2-offsets-file-2026-08-11.txt`.
- `CANDLE-SCHEMA-002` (2026-08-11): `CandleTableContractValidator` full 15-column/type/nullability/PK/routing validation; `CandleTableContractValidatorTest` 19/19; live preflight failures proven by P6.2 (missing/wrong-kind/wrong-schema tables, unreachable coordinator).
- `CANDLE-CANONICAL-001` (2026-08-11): canonical pair pinned in `CandleTableSchema`, enforced by `SignalJobConfig.requireCanonicalVersion` startup gate and `CanonicalCandleFilterFunction` at the KV boundary; `CanonicalCandlePolicyTest` 7/7, filter tests 4/4, config-gate tests 38/38.
- `CHECKPOINT-DURABILITY-001` (2026-08-11): live R2 checkpoint write/read/restore proof. `SignalJobConfig.from` + `SignalJob.applyRuntimeOptions` wired (fail-closed production object-store validation in config AND `submit-jobs.sh`; `fs.s3a.*` endpoint/credentials/region/path-style + `SimpleAWSCredentialsProvider`; never logs credentials). `SignalJobObjectStoreCheckpointIntegrationTest` (env-gated `COMPUTE_INT_TEST_P42=true`, real R2 creds injected): phase 1 on MiniCluster A writes `chk-1/2/3` (30015 B each) to `s3a://…/p4-it/<run>/<jobId>/` — `_metadata` via S3A multipart upload, RocksDB incremental SSTs under `shared/` (sibling of `chk-N`), local RocksDB store asserted live; cancel keeps checkpoints (`RETAIN_ON_CANCELLATION`, store logs `not discarded`); phase 2 on a FRESH MiniCluster B restores `chk-2` (`Restoring job … from Savepoint 2 @ 0 … located at s3a://…/chk-2`, checkpoint ID reset 2→3) and finishes with 900/key (`600` restored + `300` new) tagged `firstRun=phase-1` — state provably came from R2; restored job's next checkpoint is 31475 B (state grew). Test cleans its run prefix from the bucket. Full compute suite 166 run / 0 fail (test skipped when gate off).
- `CHECKPOINT-RESTORE-002` (2026-08-11): P6.1 phase 3 restores the dual-sink graph from the phase-2 last completed checkpoint with default strict state matching — LOG 92→96 (pending w23 rows + w24 candles), KV 46→50, offset-0 fallback excluded (would have been 142).
- `STATE-BACKEND-001` (2026-08-11): production RocksDB/managed-state proof. Config/gate: `SignalJob.applyRuntimeOptions` sets `StateBackendOptions.STATE_BACKEND=rocksdb`, incremental checkpoints (rocksdb branch only), `RocksDBOptions.LOCAL_DIRECTORIES` (`state.backend.rocksdb.localdir` — the live 2.2.1 key; the previously-written dead `state.backend.rocksdb.local_directories` key was fixed 2026-08-11), managed memory; production+heap rejected by `SignalJobConfig` and `submit-jobs.sh` FATAL gate; `RuntimeOptionsTest` + `SignalJobConfigTest` cover all branches. Runtime proof: `CandleRocksDbRestoreIntegrationTest` (gated `COMPUTE_INT_TEST_P6=true`, 2026-08-11) — live per-operator RocksDB stores (CURRENT/MANIFEST/OPTIONS/LOG/.sst) in the configured dir, incremental checkpoint `_metadata` naming `.sst` handles, and cross-worker restore (dedup fold, pending-window close, sink rows 46→50, no re-emission).
- `CHECKPOINT-RESTORE-002` (2026-08-11, RocksDB variant): the same strict dual-sink restore under the RocksDB backend — `CandleRocksDbRestoreIntegrationTest` phase 2 restores from a copied `chk-N` on a fresh MiniCluster with `allowNonRestoredState=false` (never set); result 50 LOG / 50 KV / 2 candidates, duplicates folded, offset-0 fallback excluded.
- `STARTUP-GATE-001` (2026-08-12): no accidental full replay. Fail-closed startup gate in `SignalJobConfig.from`: RESTORE requires a nonblank `STATE_RECOVERY_PATH` (else `IllegalStateException`), FULL_REPLAY is strictly opt-in via `ALLOW_FULL_REPLAY=true` (absent from dev `.env` and `submit-jobs.sh`), and `allowNonRestoredState` is never set by `applyRuntimeOptions`. Strict-restore offset proof: `CandleGraphReplayIntegrationTest` restores the dual-sink graph from the last checkpoint and asserts LOG 92→96 / KV 46→50 — the offset-0 full-replay fallback (which would have produced 142) is excluded. Gate contract covered by `SignalJobConfigTest` 38/38.

Every evidence row must include:

- date;
- commit/image IDs;
- exact command or test;
- environment topology;
- input volume/rate;
- output location;
- pass/fail result;
- operator/approver where manual approval is required.

---

## 5. Traceability matrix

| Requirement | Implementation | Test/evidence | Gate |
| --- | --- | --- | --- |
| Candle LOG-only output | P1/P2 | candle LOG sink only; candle KV projection RETIRED (2026-08-13) | CANDLE-SCHEMA-002 `[x]` (historical) |
| Signal LOG append per signal | P1/P2 | `Signal_Candidates` LOG sink — one row per fired signal, never updated | SIGNAL-DUAL-SINK-001 |
| Signal KV current-state | P1/P2 | `Signal_Candidates_current` KV upsert — latest/active per instrument | SIGNAL-DUAL-SINK-001 |
| Only canonical versions write signal KV | P2 | policy re-targeted to signal current-state boundary | CANDLE-CANONICAL-001 `[x]` (historical) → re-scope pending |
| Complete history migration | P3 | RETIRED — no candle migration/audit per requirement change | CANDLE-MIGRATION-002 `[x]` (historical) |
| No unresolved conflicts | P3 | RETIRED — no conflict reconciliation | MIGRATION-CONFLICT-002 `[x]` (historical) |
| No accidental full replay | existing startup gate | config tests + launcher audit | STARTUP-GATE-001 |
| Durable state | P4 | runtime backend and cross-worker restore | STATE-BACKEND-001 |
| Durable checkpoints | P4 | S3 write/read/restore | CHECKPOINT-DURABILITY-001 |
| Graph restore | P6/P10 | copied checkpoint strict restore (re-run on signal dual-sink) | CHECKPOINT-RESTORE-002 |
| Bounded memory | P5/P7 | target-load time series | DEDUP-MEMORY-001 |
| Throughput/latency | P7 | benchmark report (re-run on signal dual-sink topology) | PERF-* |
| Failure safety | P6/P10 | injection and recovery report (re-run on signal sinks) | FAILOVER-FLUSS-001 |
| OpenObserve observability | P8/P9 | collector delivery queries, payload tests, dashboards, alerts, retention, outage/recovery evidence | OBSERVABILITY-002 |

---

## 6. Final production acceptance checklist

Production status must remain `BLOCKED` until every checkbox below is checked:

- [x] Exact LOG/KV metadata validation is live and fail-closed (`feature_candles_15s` LOG, `Signal_Candidates` LOG, `Signal_Candidates_current` KV PK `instrument_token`). — 2026-08-13 Stage 6: production gate `SignalJob.preflightTableContracts` ran against the platform tables → `PREFLIGHT_STATUS=PASS`; live ZK: `Signal_Candidates` id 607 LOG, `Signal_Candidates_current` id 608 KV v2 PK `instrument_token`, both `bucket.key=instrument_token`/16 buckets/7d ttl; legacy KV v2 (PK `candidate_id`) dropped. Evidence: `logs/tracker-14/p6-stage6-live-ddl-evidence-20260813.md`
- [x] Canonical strategy/rule writer ownership is enforced at the signal KV boundary. — `CanonicalSignalFilterFunction` gates only the KV sink on the canonical signal key; config gate `requireCanonicalVersion` fail-closed at startup; unit suites green (compute 182/0/11 skipped, 2026-08-13)
- [ ] ~~No unresolved business conflicts remain in the migration interval~~ (RETIRED — no candle migration).
- [ ] ~~Complete lake+log history has been read and independently reconciled~~ (RETIRED — no candle audit).
- [ ] Signal LOG appends one row per fired signal and is never updated (replay idempotency: LOG may grow, KV keys stay == active instruments).
- [ ] Signal KV current-state holds exactly one latest/active candidate per instrument, updated on supersession.
- [ ] Production state backend is explicitly configured and verified.
- [ ] Production checkpoints use durable remote storage.
- [ ] Cross-VM restore succeeds.
- [ ] Signal dual-sink restore succeeds with strict state matching.
- [ ] 60k sustained throughput passes (re-run on signal dual-sink topology).
- [ ] 90k peak throughput passes.
- [ ] p99 latency passes.
- [ ] memory < 85% passes.
- [ ] checkpoint p99 < 5s passes.
- [ ] recovery <= 30s passes.
- [ ] sink/coordinator/tablet failure tests pass (re-run on signal sinks).
- [ ] watermark, idleness, late-data, and overflow behavior is approved.
- [ ] OpenObserve metrics are queryable for SignalJob, Flink, Fluss, JVM, and infrastructure health.
- [ ] OpenObserve structured logs are queryable in the required streams.
- [ ] OpenObserve traces are queryable when tracing is enabled and valid context exists.
- [ ] OpenObserve dashboards and `trading_alerts` rules are provisioned and tested (LOG:KV panels on signal tables).
- [ ] OpenObserve retention/searchability and collector outage/recovery evidence passes.
- [ ] rollback has been rehearsed (signal dual-sink, P10.3).
- [ ] production operator has approved the cutover evidence.

If any item fails, do not declare production-ready and do not compensate with:

- `allowNonRestoredState=true`;
- silent full replay;
- accepting all schema versions;
- accepting all algorithm/configuration versions;
- `MAX(output_ts)` without field-level approval;
- reading only the local Fluss LOG while claiming complete Iceberg history;
- increasing heap without measuring checkpoint and GC behavior.

---

## 7. Required final report from coding agent

The coding agent must report:

1. Files modified and why.
2. Exact validator behavior and negative tests.
3. Canonical writer policy and enforcement path.
4. Migration conflict policy and current 25-key status.
5. Complete-history reader and proof.
6. State backend and checkpoint configuration.
7. Tests and exact commands.
8. Benchmark topology, workload, and raw results.
9. Failure-injection results.
10. Checkpoint/JobGraph compatibility results.
11. Metrics and alert coverage.
12. Items still pending operator-only execution.
13. Evidence IDs and output paths.
14. Explicit production verdict: `READY`, `BLOCKED`, or `PENDING_OPERATOR_EVIDENCE`.

A successful compilation or dev migration is not sufficient for `READY`.

> Report: `logs/tracker-14/final-report-2026-08-12.md` — verdict
> `PENDING_OPERATOR_EVIDENCE` (14 sections; closes the 2026-08-12 easy-gaps block,
> plan `docs/08_implementation/11-testing-and-release.md` (completed easy-gaps section)).
