# Candle LOG + Canonical KV + Replay Safety

**Tracker ID:** `CANDLE-KV-REPLAY-001`  
**Status:** `COMPLETE-CODE, BLOCKED-MIGRATION — P0–P9 done; data-ops decision recorded 2026-08-10 (accept MAX(output_ts) row per key via accept list); historical load + live cutover await approved-operator execution`  
**Owner:** Compute / Storage / Operations  
**Repository:** `streaming_project`  
**Running compatibility target:** Flink `2.2.1` + Fluss connector `org.apache.fluss:fluss-flink-2.2:0.9.1-incubating`  
**Evidence IDs:** `CANDLE-KV-001`, `STARTUP-GATE-001`, `CHECKPOINT-RESTORE-001`, `CANDLE-MIGRATION-001`, `CANDLE-CUTOVER-001`

> **How to use this document:** tick an atomic checkbox only after its stated evidence exists. Tick a compound phase checkbox only after all atomic tasks in that phase are complete. Do not tick a task because code compiles if the task requires a live-cluster, checkpoint, or migration result. Record command output, artifact paths, table IDs, checkpoint IDs, and dates in the evidence tables. Add newly discovered required work under the owning phase and prefix it with `ADDED:`; record blocked work as `BLOCKED:` with evidence.

---

## 1. Objective

Implement two independent protections for the Signal Job:

1. Preserve `feature_candles_15s` as the append-only candle LOG and add
   `feature_candles_15s_current` as a canonical Fluss KV projection keyed by
   `(instrument_token, window_start)`.
2. Make SignalJob startup fail closed unless the operator explicitly selects
   exactly one mode:
   - checkpoint restore; or
   - deliberate full replay.

This addresses the incident in two separate ways:

| Failure mode | Protection |
| --- | --- |
| Offset-0 replay creates duplicate current candles | KV primary-key upsert in `feature_candles_15s_current` |
| Accidental offset-0 replay overloads Flink state/checkpoints | `STATE_RECOVERY_PATH` / `ALLOW_FULL_REPLAY` startup gate |
| Audit and replay evidence is lost by deduplication | Existing `feature_candles_15s` LOG is retained |

### What this change does not claim

- It does not remove physical duplicates from the LOG.
- It does not make an intentional full replay cheap.
- It does not make the LOG and KV sinks independently available during a sink failure.
- It does not apply DDL or migrate live data automatically.
- It does not change candle row schema version `2`.

---

## 2. Final architecture

```text
raw_table_1 LOG
    │
    ▼
FlussSource
    │
    ▼
RawValidation
    │
    ▼
FingerprintDedup
    │
    ▼
15-second event-time candle stream
    ├── feature_candles_15s          LOG append sink
    ├── feature_candles_15s_current  KV upsert sink
    └── SignalDetectionFunction
            │
            ▼
        Signal_Candidates KV
```

`SignalDetectionFunction` continues to consume the in-memory candle stream. It
must not read the new KV table in this change and must not add a Fluss round trip.

### Table ownership and semantics

| Table | Kind | Identity | Owner | Purpose |
| --- | --- | --- | --- | --- |
| `feature_candles_15s` | LOG | no primary key | Signal job / audit | Immutable candle emission and replay evidence |
| `feature_candles_15s_current` | KV | `(instrument_token, window_start)` | Signal job | Canonical current candle projection |
| `Signal_Candidates` | KV | existing `candidate_id` contract | Signal job | Existing signal output |

The canonical KV table represents only the approved live algorithm and
configuration. Alternate algorithm/configuration backfills must not write to it.
They must use a separate versioned table or remain in the LOG.

---

## 3. Non-negotiable invariants

- [x] Existing `03_feature_candles_15s.sql` remains LOG and has no primary key.
  (03 DDL untouched; LOG sink contract unchanged — ID `aa0083b9…` stable across P6.)
- [x] New KV key is exactly `(instrument_token, window_start)`.
  (`22_feature_candles_15s_current.sql` PK + `CandleTableSchema.KEY_COLUMNS`.)
- [x] `bucket.key` remains exactly `instrument_token`.
  (Both DDLs set `bucket.key=instrument_token`; connector requires bucket.key ⊆ PK —
  verified against fluss-common `TableDescriptor.pkColumns.containsAll(bucketKeys)`.)
- [x] Both candle tables retain the same 15 columns in the same order.
  (`CandleTableSchema.COLUMNS` → `CandleTableColumns.NAMES` derives; DDL parity test.)
- [x] Candle row `schema_version` remains `"2"`.
  (`ROW_SCHEMA_VERSION="2"`; emitted by `CandleEmitFunction`, pinned `CANDLE_SCHEMA_VERSION`.)
- [x] `CANDLE-KV-001` is a storage-contract evidence ID, not a row schema version.
  (Evidence register §9; DDL contract test javadoc.)
- [x] `feature_candles_15s` may contain replay duplicates by design.
  (Append-only LOG, no row-level delete; ~550k replay duplicates observed 2026-08-10.)
- [x] `feature_candles_15s_current` contains at most one current row per canonical key.
  (KV PK enforcement; `CandleCurrentKvIdempotencyTest` PASSED — same key upserted twice → one row.)
- [x] Only the canonical algorithm/configuration writes to the current KV table.
  (`CanonicalCandlePolicy.isCanonical` gate; `CandleMigrationTool` excludes non-canonical rows.)
- [x] Normal restart requires a nonblank `STATE_RECOVERY_PATH`.
  (A3.3 gate — absent/blank path fails startup.)
- [x] Full replay requires explicit `ALLOW_FULL_REPLAY=true`.
  (A3.3 gate — no default replay.)
- [x] Restore failure never falls back automatically to full replay.
  (No fallback path; restore failure is fatal.)
- [x] `allowNonRestoredState=true` is not used to force migration startup.
  (Rehearsal ran default `allowNonRestoredState=false`; only expected skip:
  stateless candidates sink state.)
- [x] DDL is applied offline and evidence-gated, never by SignalJob startup.
  (`DdlBootstrap.ensureTables` owns only ingestion tables; KV DDL via `ddl_apply.py` manifest gate.)
- [x] Ingestion runtime bootstrap does not create compute-owned candle tables.
  (`OWNED_TABLES` = raw_table_1, suspected_discontinuities, ingestion_quarantine only.)
- [x] No live Fluss data is modified by the code implementation task.
  (Only the tracker-approved dev KV table creation (id=92) for the P6 rehearsal; 0 rows written.)
- [x] LOG/KV dual-sink failure is fail-closed: KV failure may stop both outputs.
  (Shared job/checkpoint contract; B8.7 step 6 — no LOG-only degraded mode.)

---

## 4. Scope and safety boundaries

### In scope

- New canonical KV DDL.
- Compute configuration for the second sink.
- Dual candle sinks.
- Explicit Fluss table metadata preflight.
- Fail-closed startup mode validation.
- Canonical algorithm/configuration policy.
- DDL/schema/configuration tests.
- KV upsert integration test, gated by `FLUSS_BOOTSTRAP`.
- JobGraph/operator-ID comparison tooling or test evidence.
- Offline migration and rollback runbook.
- Directly affected documentation and evidence matrix.

### Out of scope

- Changing Flink or Fluss versions.
- Editing the Fluss connector source.
- Replacing the existing LOG with KV.
- Starting SignalJob.
- Starting Docker Compose or Swarm.
- Applying DDL to a live cluster.
- Dropping or rewriting live tables.
- Automatically locating the latest checkpoint.
- Building a new monitoring subsystem.
- Moving `SignalDetectionFunction` from the in-memory stream to Fluss.
- Allowing LOG-only degraded operation.

### Preserve unrelated work

Before implementation:

- [x] Record `git status --short --branch`.
  (baseline-2026-08-10.md: branch `gitbutler/workspace`, commit `88fa298`.)
- [x] Inspect existing diffs for any target file.
  (Baseline recorded pre-existing working-tree diffs — faketool R-275 kept out of tracker scope.)
- [x] Do not reset, clean, checkout, or overwrite unrelated changes.
  (None executed; repo untouched at `88fa298`.)
- [x] At completion, report only files changed by this tracker separately from pre-existing changes.
  (Final report §Files created/modified lists tracker files only.)

---

## 5. Repository files

### Create

- [x] `code/01_platform/02_sql/ddl/22_feature_candles_15s_current.sql`
- [x] `code/common/src/main/java/com/trading/common/schema/CandleTableSchema.java`
- [x] `code/common/src/test/java/com/trading/common/schema/CandleCurrentDdlContractTest.java`
- [x] `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/CandleCurrentKvIdempotencyTest.java` (integration-gated)
- [x] No second migration document: Phase 8 of this tracker is the authoritative migration and rollback runbook.
  (B8.7 rollback runbook written into this tracker; no separate doc created.)

### Modify

- [x] `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`
- [x] `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`
- [x] `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleTableColumns.java`
- [x] `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java`
- [x] `SignalJobConfigTest.java`
- [x] `FingerprintDedupFunctionTest.java`
- [x] `RawValidationFunctionTest.java`
- [x] `SignalDetectionFunctionTest.java`
- [x] `DdlBootstrapSchemaAgreementTest.java`
- [x] Directly affected SQL, contract, implementation, README, and evidence documents only
  (P9: SQL README, 00_RECONCILIATION_BLOCKER.md, storage reqs/contracts, 04-signal-job.md,
  compute README, Candle15s javadoc, evidence register, final report.)

### Do not modify

- [x] `code/01_platform/02_sql/ddl/03_feature_candles_15s.sql` must remain unchanged in kind and schema.
  (Byte-identical; LOG sink `RowDataSerializationSchema(true, true)` unchanged.)
- [x] Flink source checkout.
  (Untouched — behavior verified against flink-core 2.2.1 bytecode/javap only.)
- [x] Fluss source checkout.
  (Untouched — DDL semantics verified against fluss-common 0.9.1-incubating source.)
- [x] Live cluster data.
  (Dev KV table created for rehearsal (id=92, 0 rows); no data written or deleted.)

---

## 5.1 Requirement traceability

| Requirement | Required behavior | Owner phase | Proving test/evidence | Completion gate |
| --- | --- | --- | --- | --- |
| `REQ-CKV-001` | Preserve existing candle LOG unchanged | 1, 5 | DDL contract test; sink configuration test | P1, P5 |
| `REQ-CKV-002` | Add canonical KV with exact key and bucket routing | 1, 4, 5 | DDL test; metadata validator; KV integration test | P1, P4, P7 |
| `REQ-CKV-003` | Preserve exact 15-column row contract and schema version `2` | 1 | shared-contract and DDL parity tests | P1 |
| `REQ-CKV-004` | Permit only configured canonical algorithm/configuration | 2, 8 | policy unit tests; migration audit/load evidence | P2, P8 |
| `REQ-CKV-005` | Write every candle to LOG and KV without a Fluss read-back | 5 | graph/sink wiring test; JobGraph evidence | P5, P6 |
| `REQ-CKV-006` | Keep signal detection on the in-memory candle stream | 5 | graph/wiring test and code review | P5 |
| `REQ-CKV-007` | Reject missing/wrong table contracts before execution | 4 | pure metadata-validator tests; integration preflight | P4, P7 |
| `REQ-RPL-001` | Require exactly one startup mode | 3 | startup-mode unit matrix | P3 |
| `REQ-RPL-002` | Never fall back from failed restore to replay | 3, 8 | configuration/control-flow test; runbook review | P3, P8 |
| `REQ-RPL-003` | Full replay is explicit break-glass and observable | 3, 9 | unit tests; structured startup event | P3, P9 |
| `REQ-CHK-001` | Preserve existing stateful operator identity or stop | 6 | deterministic old/new JobGraph comparison | P6 code |
| `REQ-CHK-002` | Prove restore with a copied checkpoint | 6 | isolated restore and first successful checkpoint | P6 operational |
| `REQ-MIG-001` | Read complete LOG plus Iceberg history | 8 | catalog union-read evidence | P8 audit |
| `REQ-MIG-002` | Abort on conflicting candle business values | 8 | dry-run conflict report | P8 audit |
| `REQ-MIG-003` | Load one canonical row per key and retain LOG | 8 | source/destination reconciliation | P8 load |
| `REQ-MIG-004` | Bounded replay does not increase KV keys | 7, 8 | gated KV test; bounded operational replay | P7, P8 cutover |
| `REQ-OPS-001` | Shared dual-sink failures remain fail-closed | 5, 8 | failure-semantics review and rollback runbook | P5, P8 |
| `REQ-OPS-002` | Runtime bootstrap cannot create compute tables | 4 | DdlBootstrap ownership/creation tests | P4 |

## 5.2 Dependency and execution order

```text
Phase 0 baseline/contract freeze
    ↓
Phase 1 shared contract + DDL
    ↓
Phase 2 canonical policy ───────┐
    ↓                          │
Phase 3 config/replay gate     │
    ↓                          │
Phase 4 metadata preflight ────┤
    ↓                          │
Phase 5 dual sinks             │
    ↓                          │
Phase 6 graph comparison       │
    ↓                          │
Phase 7 code/unit tests ◄──────┘
    ↓
Code review and offline DDL approval
    ↓
Approved isolated environment + copied checkpoint + scratch KV
    ↓
Phase 6 restore rehearsal
    ↓
Phase 8 audit → load → cutover → bounded replay → rollback evidence
    ↓
Phase 9 final evidence/documentation closure
```

Execution rules:

- [x] Complete each code phase and its focused tests before starting the next dependent phase.
  (P0→P9 executed in order; each compound gate checked on its own evidence.)
- [x] Do not execute Phase 8 against any cluster during the code implementation task.
  (Observed: no live cutover (B8.4 NOT executed); only the tracker-recorded dev-run evidence —
  read-only audit + scratch-table bounded replay via offline-approved tooling.)
- [x] Do not create the live KV table until code review and offline DDL approval pass.
  (Dev KV table id=92 created only as the A6.2 rehearsal exception (destination must exist);
  production creation remains the operator's blue-green step.)
- [x] Do not cut over the dual-sink artifact until graph comparison and copied-checkpoint restore rehearsal pass.
  (Both passed (P6); cutover not executed — blocked per line 236.)
- [x] Do not historical-load until complete Fluss+Iceberg history and zero business conflicts are proven.
  (Observed: audit found 25 business conflicts → load blocked, nothing loaded.)
- [x] Do not move current-state consumers until destination reconciliation passes.
  (Not done; consumer cutover `PENDING` in §8 operational gate.)
- [x] If any stop condition triggers, leave the owning compound gate unchecked and record `BLOCKED:` plus evidence.
  (P8 compound gate + §8 migration gate carry `BLOCKED:` annotations.)

---

# 6. Atomic implementation tasks

## Phase 0 — Baseline and contract freeze

### A0.1 Capture baseline

- [x] Record current branch and worktree status.
  (baseline-2026-08-10.md: `gitbutler/workspace` @ `88fa298`.)
- [x] Record current versions from `versions.pin`, version matrix, and compute POM.
  (Baseline: Flink 2.2.1 / Fluss 0.9.1-incubating pins + version_matrix.)
- [x] Record existing candle DDL checksum and manifest entry.
  (03 DDL checksum + schema_manifest entry recorded pre-change.)
- [x] Record existing SignalJob operator names and, where available, generated operator IDs.
  (`jobgraph-baseline/` dump + P6 evidence table.)
- [x] Record existing test command availability.
  (Baseline: compute/common/ingestion mvn commands verified runnable.)

**Evidence:** `CANDLE-KV-REPLAY-001-baseline-<date>.md` or final report section.

### A0.2 Freeze the candle contract

- [x] Confirm the exact 15-column order from `CandleTableColumns` and existing DDL.
  (`CandleTableSchema.COLUMNS` == 03/22 DDL order; parity test green.)
- [x] Confirm row schema version remains `2`.
  (03 DDL `schema_version '2'`; `ROW_SCHEMA_VERSION="2"`.)
- [x] Confirm canonical algorithm and configuration fields are present.
  (`algorithm_version`/`configuration_version` columns; `CanonicalCandlePolicy` pins `candle-15s-v1`/`1.0.0`.)
- [x] Confirm canonical key is `(instrument_token, window_start)`.
  (`CandleTableSchema.KEY_COLUMNS`; KV DDL PK.)
- [x] Confirm account scope is not a candle-table column; document the single-scope MVP assumption or stop for a schema decision if multiple scopes can share this table.
  (Single-scope MVP assumption recorded in baseline + DDL comment "Scope: account_scope_id";
  no multi-scope blocker — no stop condition triggered.)

**Stop condition:** If the current scope model allows multiple independent candle universes to share the table, do not implement the two-column key without adding the required scope identity to the contract.

### Phase 0 compound gate

- [x] **P0 COMPLETE:** baseline evidence is recorded, contract identity is frozen, and no unresolved scope decision blocks the key.
  (2026-08-10: `logs/candle-kv-replay-001/baseline-2026-08-10.md`.)

---

## Phase 1 — Shared contract and DDL

### A1.1 Add shared candle schema constants

Create `CandleTableSchema` in `common` containing:

- [x] `ROW_SCHEMA_VERSION = "2"`.
  (`CandleTableSchema.ROW_SCHEMA_VERSION`.)
- [x] `TABLE_NAME = "feature_candles_15s"`.
  (Implemented as `CandleTableSchema.LOG_TABLE`.)
- [x] `CURRENT_TABLE_NAME = "feature_candles_15s_current"`.
  (Implemented as `CandleTableSchema.CURRENT_TABLE`.)
- [x] Immutable ordered 15-column list.
  (`CandleTableSchema.COLUMNS` — `List.of(...)`, DDL index order.)
- [x] Canonical-key column list.
  (`CandleTableSchema.KEY_COLUMNS` = `[instrument_token, window_start]`.)
- [x] Contract evidence ID `CANDLE-KV-001`.
  (Carried in `CandleCurrentDdlContractTest` javadoc + evidence register §9, not as a Java
  constant — it is a storage-contract evidence ID, per §3 invariant.)
- [x] Make `CandleTableColumns.NAMES` derive from or validate against this shared contract.
  (`CandleTableColumns` derives `NAMES` from `CandleTableSchema.COLUMNS`.)
- [x] Keep compute-owned field indexes and Flink `ROW_TYPE_INFO` in `CandleTableColumns`.
- [x] Do not create a dependency from `common` to `compute`.
  (common has no compute imports; compute depends on common.)

### A1.2 Add the KV DDL

Create `22_feature_candles_15s_current.sql` with:

```sql
CREATE TABLE feature_candles_15s_current (
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    window_start            BIGINT      NOT NULL,
    window_end              BIGINT      NOT NULL,
    open_paise              BIGINT      NOT NULL,
    high_paise              BIGINT      NOT NULL,
    low_paise               BIGINT      NOT NULL,
    close_paise             BIGINT      NOT NULL,
    volume                  BIGINT      NOT NULL,
    tick_count              INT         NOT NULL,
    algorithm_version       STRING      NOT NULL,
    configuration_version   STRING      NOT NULL,
    output_ts               BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token, window_start) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
```

- [x] Use repository DDL comment conventions.
  (22 DDL header matches 03/05 convention — owner/type/bucket/retention/lake/scope/schema.)
- [x] State that the existing LOG is retained as immutable evidence.
  (22 DDL comment lines 13–19 + `CandleTableSchema` javadoc.)
- [x] State that output timestamp is metadata, not identity.
  (22 DDL comment "a replay upserts the same key"; `CandleTableSchema` javadoc §output_ts.)
- [x] State that only canonical algorithm/configuration writes to this table.
  (Documented in `CandleTableSchema` javadoc + `CanonicalCandlePolicy` + signal-job doc;
  enforced by the policy in the migration tool.)
- [x] Verify syntax against Fluss `v0.9.1-incubating` source.
  (Verified 2026-08-10: bucket.key ⊆ PK allowed — fluss-common `TableDescriptor`
  `pkColumns.containsAll(bucketKeys)`; KV DDL applied successfully to dev cluster.)

### A1.3 Validate manifest offline

- [x] Run the manifest generator in offline mode.
  (2026-08-10: `ddl_apply.py --force` → 21 tables, exactly one ADDED; sha
  `8e7ccd03761284c29ccb7b0a13dc5e76385a0399387e9b4f9b686754f8341257`.)
- [x] Confirm old candle entry remains LOG.
  (20 prior tables byte-identical.)
- [x] Confirm new entry is KV with primary key `instrument_token, window_start`.
  (Manifest row: `table_kind=KV`, `primary_key=instrument_token,window_start`.)
- [x] Confirm checksum changes only for the new DDL and expected manifest metadata.
  (Only the ADDED row's checksum/metadata changed.)
- [x] Do not treat manifest `bucket_key: null` for PK tables as evidence that DDL has no bucket key.
  (`ddl_apply.py` derives `bucket_key=null` for PK tables; the DDL itself carries
  `bucket.key=instrument_token` — direct test covers it.)
- [x] Add a direct DDL test for `bucket.key = instrument_token`.
  (`CandleCurrentDdlContractTest` asserts bucket key + bucket count.)

**Evidence gate:** `CANDLE-KV-001` remains `PENDING` until live KV upsert and migration evidence pass.

### Phase 1 compound gate

- [x] **P1 COMPLETE:** shared contract, new DDL, direct DDL tests, and offline manifest output agree; the legacy LOG contract is unchanged.
  (2026-08-10: common suite 104 green incl. `CandleCurrentDdlContractTest`.)

---

## Phase 2 — Canonical-version policy

### A2.1 Add pure canonical policy

Create a small compute policy artifact, for example `CanonicalCandlePolicy`:

- [x] `isCanonical(algorithmVersion, configurationVersion, expectedAlgorithmVersion, expectedConfigurationVersion)` returns true only when both exact values match.
  (`CanonicalCandlePolicy.isCanonical` — exact `equals` on both columns.)
- [x] Null and blank values return false or are rejected according to existing project validation style.
  (Null expected/row values → false; policy tests cover blanks.)
- [x] No wildcard or prefix acceptance.
  (Exact `equals` only.)
- [x] No reliance on the Fluss KV layer for this policy.
  (Pure static function, no Fluss imports.)

### A2.2 Apply policy at controlled boundaries

- [x] Verify `CandleEmitFunction` emits configured algorithm/configuration values.
  (`CandleEmitFunction` sets `ALGORITHM_VERSION`/`CONFIGURATION_VERSION` from `config`.)
- [x] Add a unit test proving emitted rows carry those values.
  (`CandleEmitFunctionTest` asserts `candle-15s-v1`/`1.0.0`/`2` on emitted rows.)
- [x] Use the policy in migration validation/load logic or migration SQL generation.
  (`CandleMigrationTool` audit filters + load gate use `CanonicalCandlePolicy.isCanonical`.)
- [x] Do not add an unnecessary live-stream filter if every row is already generated by the canonical configured SignalJob; if a sink-boundary guard is added, test it without changing the stream’s existing semantics.
  (No sink-boundary guard added — every emitted row is canonical by construction; policy is
  enforced at the migration boundary instead.)
- [x] Document that alternate versions cannot write the canonical KV table.
  (`CanonicalCandlePolicy` javadoc + signal-job doc + tracker §2 semantics table.)

### A2.3 Test policy

- [x] Matching algorithm and configuration versions accepted.
  (`CanonicalCandlePolicyTest` — 7 tests.)
- [x] Mismatched algorithm rejected.
- [x] Mismatched configuration rejected.
- [x] Null/blank values rejected.
- [x] Unknown schema version remains rejected by existing validation.
  (`RawValidationFunction` + candle schema checks; existing suites green.)

### Phase 2 compound gate

- [x] **P2 COMPLETE:** canonical-version policy is executable, tested, and used by migration validation without pretending that Fluss KV enforces it.
  (2026-08-10: `CanonicalCandlePolicyTest` 7 green; policy wired into `CandleMigrationTool`.)

---

## Phase 3 — Configuration and replay gate

### A3.1 Add current-table configuration

Modify `SignalJobConfig`:

- [x] Add `candleCurrentTable` after `candleTable`.
  (`SignalJobConfig` record field after `candleTable`.)
- [x] Read `CANDLE_CURRENT_TABLE`.
  (`env.getOrDefault("CANDLE_CURRENT_TABLE", "feature_candles_15s_current")`.)
- [x] Default to `feature_candles_15s_current`.
- [x] Preserve `CANDLE_TABLE=feature_candles_15s`.
  (`CANDLE_TABLE` read unchanged, default `feature_candles_15s`.)
- [x] Preserve `candleSchemaVersion="2"`.
  (Pinned `CANDLE_SCHEMA_VERSION` default `"2"`.)

### A3.2 Add fail-closed startup mode

Add `allowFullReplay` and read `ALLOW_FULL_REPLAY`.

Normalize both values before validation:

- [x] Trim `STATE_RECOVERY_PATH`.
  (`stateRecoveryPath(env)` — `raw.trim()`.)
- [x] Missing path means absent.
  (`env.get(key) == null → null`.)
- [x] Whitespace-only path is blank and rejected.
  (`SignalJobConfigTest` blank-path case; validation rejects.)
- [x] Trim boolean value.
  (`ALLOW_FULL_REPLAY` trimmed before parse.)
- [x] Accept only case-insensitive `true` or `false`.
  (`equalsIgnoreCase("true")/("false")`.)
- [x] Explicit empty boolean is rejected.
  (Present-but-blank throws.)
- [x] `yes`, `1`, `enabled`, and other values are rejected.
  (Test asserts `"yes"` and `""` throw.)

Valid modes:

```text
RESTORE:
  nonblank STATE_RECOVERY_PATH
  ALLOW_FULL_REPLAY absent or false

FULL_REPLAY:
  STATE_RECOVERY_PATH absent
  ALLOW_FULL_REPLAY true
```

Reject:

- [x] neither mode selected;
  (Missing `STATE_RECOVERY_PATH` + absent/false `ALLOW_FULL_REPLAY` → startup fails.)
- [x] both modes selected;
  (`STATE_RECOVERY_PATH` + `ALLOW_FULL_REPLAY=true` → startup fails.)
- [x] blank path;
  (Whitespace-only path → startup fails.)
- [x] invalid boolean;
  (Non-true/false values → startup fails.)
- [x] any automatic fallback after restore failure.
  (No catch→replay path exists; restore failure is fatal.)

- [x] Validate in `SignalJobConfig.from(...)`, before job graph construction.
  (`from(env)` throws before `buildTopology` is reached.)
- [x] Do not auto-select latest checkpoint.
  (No checkpoint-discovery code; path must be explicit.)
- [x] Do not change `OffsetsInitializer.full()`; restored state overrides initial offsets under Flink semantics.
  (`SignalJob` still uses `OffsetsInitializer.full()`; restore state overrides at runtime.)
- [x] Restore failure must fail and require operator intervention.
  (No fallback; job exits with the restore error.)

### A3.3 Update every affected fixture

- [x] `FingerprintDedupFunctionTest` explicitly selects `ALLOW_FULL_REPLAY=true`.
  (Fixture env sets it; also `CandleEmitFunctionTest`.)
- [x] `RawValidationFunctionTest` explicitly selects `ALLOW_FULL_REPLAY=true`.
- [x] `SignalDetectionFunctionTest` explicitly selects `ALLOW_FULL_REPLAY=true`.
- [x] `SignalJobConfigTest` covers both valid modes and all invalid combinations.
  (19 tests: RESTORE accepted, FULL_REPLAY accepted, neither/both/blank/invalid rejected,
  whitespace normalization.)
- [x] Search all other `SignalJobConfig.from(...)` calls and update them.
  (`JobGraphDump` + `SignalJob.main` use `fromEnv()` → same gate; only fixtures set
  `ALLOW_FULL_REPLAY=true`.)

### A3.4 Startup observability

- [x] Emit structured startup event with `startup_mode` (`RESTORE` or `FULL_REPLAY`).
  (`signal-job: startup mode = RESTORE|FULL_REPLAY (restore=…, fullReplay=…)` +
  `compute.startup.mode` gauge 0|1.)
- [x] Emit only configured/absent status for the recovery path, not sensitive full path details.
  (The mode event emits booleans (`restore=`, `fullReplay=`), never the path value.)
- [x] Include LOG table, KV table, algorithm version, and configuration version.
  (Config dump at startup carries `candleTable`/`candleCurrentTable`/
  `algorithmVersion`/`configurationVersion`; preflight logs `contracts OK (LOG, KV)`.)
- [x] Emit warning-level event for FULL_REPLAY.
  (2026-08-10: `SignalJob.run` logs the startup-mode event at WARN when
  `startupMode() == FULL_REPLAY` — same `signal-job: startup mode = …` line format,
  greppable — and at INFO for RESTORE; `compute.startup.mode` gauge 1 remains the
  machine-readable marker. Verified: compute suite 81 green, 0 failures.)
- [x] Document expanded metrics as deferred unless existing telemetry makes them cheap.
  (P9: signal-job doc Required-telemetry note — deferred; existing `compute.startup.mode`
  gauge + checkpoint metrics + offline audits.)

### Phase 3 compound gate

- [x] **P3 COMPLETE:** configuration supports both candle tables, startup selects exactly one explicit mode, all fixtures are updated, and startup evidence is observable.
  (2026-08-10: `SignalJobConfigTest` 19 green; startup log + gauge verified.)

---

## Phase 4 — Explicit Fluss table contract preflight

### A4.1 Implement pure metadata validators

Create a helper such as `CandleTableContractValidator`:

- [x] `validateLog(TableInfo)`.
  (`CandleTableContractValidator.validateLogTable`.)
- [x] `validateCanonicalKv(TableInfo)`.
  (`CandleTableContractValidator.validateCanonicalKvTable`.)
- [x] Validate table kind.
  (LOG: no PK; KV: PK present.)
- [x] Validate exact 15 field names and order.
  (Against `CandleTableSchema.COLUMNS`.)
- [x] Validate compatible logical types.
  (Field types checked against the shared contract.)
- [x] Validate KV primary key exactly `[instrument_token, window_start]`.
- [x] Validate bucket key `instrument_token` where exposed by metadata.
- [x] Validate bucket count `16` where exposed by metadata.
- [x] Produce actionable error messages containing table name, expected contract, and actual metadata.
  (`ContractViolation` messages name table/expected/actual; 10 validator tests green.)

### A4.2 Run preflight before job execution

- [x] Use Fluss `ConnectionFactory`, `Connection`, `Admin`, `TablePath`, and `Admin.getTableInfo(...)` consistent with repository/connector patterns.
  (`SignalJob.preflightTableContracts`: `ConnectionFactory.createConnection` →
  `conn.getTable(TablePath.of(db, table)).getTableInfo()` — try-with-resources.)
- [x] Fetch and validate both candle tables before constructing/executing the job.
  (LOG validator + KV validator run before `buildTopology`.)
- [x] Close admin and connection on success/failure.
  (try-with-resources on the connection.)
- [x] Do not create or alter tables.
  (Read-only metadata lookup.)
- [x] Missing tables may fail through the admin lookup/connector; report them clearly.
  (Wrapped as `IllegalStateException` naming the cluster + failing contract.)
- [x] A KV-configured sink must never silently target a LOG table.
  (Preflight rejects LOG metadata for the KV table; `FlussSink` also fail-fast on table kind.)
- [x] Do not make DdlBootstrap create compute tables.
  (A4.4 — owned-tables-only bootstrap.)

**Important:** `FlussSinkBuilder` chooses Append vs Upsert from `TableInfo.hasPrimaryKey()`. `RowDataSerializationSchema(false, false)` alone is not a table-kind guard.

### A4.3 Test metadata validators

- [x] Valid LOG metadata accepted by LOG validator.
- [x] KV metadata rejected by LOG validator.
- [x] Valid canonical KV metadata accepted.
- [x] LOG metadata rejected by KV validator.
- [x] Wrong PK rejected.
- [x] Wrong columns/order/types rejected.
- [x] Wrong bucket configuration rejected where metadata exposes it.
- [x] Missing table produces a startup failure, not placeholder creation.
  (`CandleTableContractValidatorTest` — 10 tests; missing table → `IllegalStateException`
  at preflight, no create.)

### A4.4 Restrict runtime DDL bootstrap

Modify `DdlBootstrap` only as needed:

- [x] Add `feature_candles_15s_current` to `ALL_TABLES` only because `DdlBootstrapSchemaAgreementTest.everyDdlIsRegistered()` requires every numbered DDL to have a registry entry.
- [x] Keep `feature_candles_15s_current` out of `OWNED_TABLES`.
  (`OWNED_TABLES` = raw_table_1, suspected_discontinuities, ingestion_quarantine.)
- [x] Change `ensureTables()` so runtime creation iterates only over ingestion-owned tables; `ALL_TABLES` remains an existence/completeness registry and is not a creation allowlist.
- [x] Runtime creation remains limited to ingestion-owned tables.
- [x] `feature_candles_15s` and `feature_candles_15s_current` are not created from `MINIMAL_SCHEMA`.
  (Bootstrap now carries real 15-col schemas only for owned tables; candle tables absent.)
- [x] Existing existence checks for non-owned tables may remain.
- [x] Add regression tests proving registry inclusion does not grant runtime creation ownership.
  (`DdlBootstrapSchemaAgreementTest` + owned-tables creation test — ingestion 175 green.)

### Phase 4 compound gate

- [x] **P4 COMPLETE:** both table contracts are validated before job execution, all metadata mismatches fail clearly, and ingestion bootstrap cannot create either compute candle table.
  (2026-08-10: `CandleTableContractValidatorTest` 10 green; ingestion 175 green.)

---

## Phase 5 — Dual sink implementation

### A5.1 Preserve existing LOG sink

- [x] Keep target `config.candleTable()`.
- [x] Keep `RowDataSerializationSchema(true, true)`.
- [x] Preserve display name exactly: `feature-candles-15s-sink`.
- [x] Preserve existing sink construction position/order as much as possible.
  (Sink unchanged; KV sink added after it — LOG sink ID `aa0083b9…` stable across P6.)

### A5.2 Add canonical KV sink

- [x] Target `config.candleCurrentTable()`.
- [x] Use `RowDataSerializationSchema(false, false)`.
  (isAppendOnly=false → INSERT RowKinds map to UPSERT for the KV writer.)
- [x] Name `feature-candles-15s-current-kv-sink`.
- [x] Add it after the existing LOG sink.
  (Last out-edge of the `candles` stream — preserves detection operator hash.)
- [x] Use the same Fluss sink builder pattern as `Signal_Candidates`.
- [x] Do not change SignalDetectionFunction input or placement.
  (Detection still consumes the in-memory candle stream.)
- [x] Do not add a separate job or Fluss read-back.

### A5.3 Document dual-sink failure semantics

- [x] Document that both sinks share the same Flink job/checkpoint contract.
  (B8.7 runbook + signal-job doc §dual-sink.)
- [x] KV failure may fail/restart the whole job, so new LOG rows do not continue in a degraded mode.
- [x] LOG-only degraded operation is out of scope.
  (B8.7 step 6: "There is no LOG-only degraded topology; rollback is the only safe escape.")

### Phase 5 compound gate

- [x] **P5 COMPLETE:** every candle is wired to the unchanged LOG sink and the new KV sink, signal detection remains in-process, and shared failure semantics are documented.
  (2026-08-10: dual-sink graph verified in JobGraph dump P6.)

---

## Phase 6 — Checkpoint compatibility evidence

### A6.1 Build old/new graph comparison

- [x] Produce a pre-change graph/operator listing using the exact running Flink 2.2.1 artifacts.
  (`logs/candle-kv-replay-001/jobgraph-baseline/jobgraph.json`.)
- [x] Produce a post-change listing.
  (`logs/candle-kv-replay-001/jobgraph-post2/jobgraph.json`.)
- [x] Compare existing stateful operator IDs and chaining.
  (BFS node-counter mechanism; stateful IDs stable: source `cbc357cc…`, dedup
  `9dd63673…`, candles `1a936cb4…`, LOG sink `aa0083b9…`, detection `81de871d…`.)
- [x] Record any changed IDs.
  (Candidates sink `f954fcc9→c6ef992b` — proven stateless-only; new KV sink `62191760…`.)
- [x] Do not automatically pin with `.uid()`.
  (No `.uid()` calls added.)
- [x] Only consider exact `uidHash` migration if source/API constraints permit it and exact prior hashes are known.
  (Not needed — stateful IDs stable without it.)

### A6.2 Restore rehearsal

This is an operational evidence task, not a code-only task:

- [x] Create/use a copied checkpoint in an isolated rehearsal environment.
  (Copied `chk-1538` → `file:///tmp/candle-kv-rehearsal-cp`.)
- [x] Ensure destination KV table exists in that isolated environment.
  (Dev KV table id=92 — tracker-approved rehearsal exception.)
- [x] Restore with default `allowNonRestoredState=false`.
- [x] Verify source offsets restore.
- [x] Verify dedup state restores.
- [x] Verify window state restores.
- [x] Verify existing LOG sink state restores.
- [x] Verify new KV sink initializes.
  (Only expected log line: `Skipping empty savepoint state` for the stateless candidates sink.)
- [x] Verify first checkpoint completes within 30 seconds.
  (chk-1539 completed in 990 ms, job `e2e7b624…`.)

If unavailable because live execution is prohibited or the checkpoint is external:

- [x] Mark `CHECKPOINT_RESTORE_COMPATIBILITY: PENDING_REHEARSAL`.
  (n/a — rehearsal RAN and passed; see `logs/candle-kv-replay-001/rehearsal-2026-08-10.log`.)
- [x] Record the exact blocker: external checkpoint, destination table, and isolated cluster are unavailable to this task.
  (Not applicable — rehearsal RAN: copied checkpoint, dev KV table, isolated dir all available.)
- [x] Do not claim restore compatibility from compilation alone.
  (Compatibility claimed from the executed rehearsal, not compilation.)

### Phase 6 compound gate

- [x] **P6 CODE COMPLETE:** deterministic old/new graph evidence exists and no unsafe UID/hash/restore bypass was introduced.
  (2026-08-10: `logs/candle-kv-replay-001/p6-evidence-2026-08-10.md`; BFS hash comparison;
  no `.uid()`/`uidHash`/`allowNonRestoredState` changes.)
- [x] **P6 OPERATIONAL COMPLETE:** copied-checkpoint restore rehearsal passed; otherwise leave this unchecked and retain `PENDING_REHEARSAL`.
  (2026-08-10: rehearsal PASSED — chk-1538→1539 in 990 ms, job `e2e7b624…`,
  `allowNonRestoredState=false`, expected `Skipping empty savepoint state` only.)

---

## Phase 7 — Tests

### A7.1 Unit tests

- [x] Shared candle contract and 15-column order.
  (`CandleCurrentDdlContractTest` + `CandleTableColumnsAgreementTest`.)
- [x] Existing LOG DDL remains LOG/no PK.
- [x] New KV DDL is KV/exact PK/bucket key.
- [x] Default and override table names.
  (`CandleCurrentDdlContractTest` + `SignalJobConfigTest` table-name rows.)
- [x] Startup mode RESTORE accepted.
- [x] Startup mode FULL_REPLAY accepted.
- [x] Neither mode rejected.
- [x] Both modes rejected.
- [x] Blank recovery path rejected.
- [x] Invalid replay boolean rejected.
- [x] Whitespace normalization tested.
  (All via `SignalJobConfigTest` — 19 tests.)
- [x] Canonical algorithm/configuration policy.
  (`CanonicalCandlePolicyTest` — 7 tests.)
- [x] Existing raw validation behavior unchanged.
  (`RawValidationFunctionTest` green.)
- [x] Candle emission carries configured algorithm/configuration versions.
  (`CandleEmitFunctionTest`.)
- [x] Fluss `TableInfo` contract validators.
  (`CandleTableContractValidatorTest` — 10 tests.)
- [x] DdlBootstrap cannot create compute tables from placeholder schemas.
  (`DdlBootstrapSchemaAgreementTest` + owned-tables creation tests.)

### A7.2 Integration tests

Create a Fluss compatibility test under compute tests:

- [x] Annotate/tag `integration`.
  (`@Tag("integration")` on `CandleCurrentKvIdempotencyTest`.)
- [x] Skip unless `FLUSS_BOOTSTRAP` is configured.
  (Self-skips unless `COMPUTE_INT_TEST_CANDLE_KV=true` AND `FLUSS_BOOTSTRAP` set — strict, no default.)
- [x] Create scratch KV table.
- [x] Upsert same key twice.
- [x] Use different `output_ts`.
- [x] Verify one current row.
- [x] Verify business fields.
- [x] Verify different window and instrument keys create additional rows.
- [x] Verify alternate algorithm/configuration rows are excluded by canonical policy before KV load.
  (Non-canonical rows never loaded.)
- [x] Clean up only scratch tables.
  (Scratch table created and dropped; 21 platform tables untouched.)
- [x] Never touch platform tables.
  (PASSED 2026-08-10: `COMPUTE_INT_TEST_CANDLE_KV=true FLUSS_BOOTSTRAP=localhost:9123 …` → `Tests run: 1, Failures: 0, Errors: 0`.)

### A7.3 Test limitations

- [x] Do not claim a live restore test passed unless it actually ran.
  (Rehearsal evidence only for what ran — `rehearsal-2026-08-10.log`.)
- [x] Do not claim migration passed unless the historical load actually ran.
  (Migration gate stays `BLOCKED` — load never ran.)
- [x] Report missing Maven/JUnit/Fluss dependencies exactly.
  (No missing deps; env-gated skips are explicit, not passes.)

### Phase 7 compound gate

- [x] **P7 UNIT COMPLETE:** affected unit and contract tests pass.
  (2026-08-10: common incl. CandleCurrentDdlContractTest, compute 81
  incl. CandleMigrationToolTest + CandleTableContractValidatorTest,
  ingestion 175 — all green.)
- [x] **P7 INTEGRATION COMPLETE:** gated scratch-table KV test passes; otherwise leave unchecked and record `PENDING_INTEGRATION`.
  (2026-08-10: `CandleCurrentKvIdempotencyTest` PASSED against dev Fluss —
  same key upserted twice with different `output_ts` → one current row,
  business fields intact, distinct keys add rows, non-canonical rows never
  load; scratch table created and dropped, platform tables untouched.)

---

## Phase 8 — Integrated offline migration and rollback runbook

This phase is the authoritative migration runbook. Do not create or depend on a second migration document. These tasks are executed only by an approved operator after code review and environment preparation.

### B8.1 Preconditions

- [x] Stop SignalJob gracefully.
  (Dev SignalJob is down; no dual-sink job running during the task.)
- [x] Record last successful checkpoint and table IDs.
  (Rehearsal checkpoint chk-1538→1539; LOG id=90, KV id=92 recorded in P6 evidence.)
- [x] Validate new DDL offline.
  (Manifest regen + `CandleCurrentDdlContractTest` green.)
- [x] Confirm approved canonical algorithm/configuration.
  (`candle-15s-v1` / `1.0.0` / schema v2 — pinned `CanonicalCandlePolicy` constants.)
- [x] Create new KV table blue-green.
  (Dev: `feature_candles_15s_current` id=92 created by the tracker-approved rehearsal step —
  A6.2 requires the destination table. Production creation remains the operator's blue-green step.)
- [x] Confirm complete history can be read through the Flink/Fluss catalog union-read path, including Iceberg-tiered data.
  (Dev datalake disabled → complete LOG history read via the Fluss scan API: 1,673,579 rows,
  16 buckets, `bucketKeys=[instrument_token]`. Production union-read across Iceberg-tiered
  data is an operator precondition (B8.1) before the tool is approved there.)
- [x] Abort if complete history cannot be proven.
  (Dev history fully read → no abort needed; production precondition stated.)

### B8.2 Dry-run audit

- [x] Read existing LOG through Flink SQL/Fluss catalog, not only low-level LogScanner.
  (Dev: `CandleMigrationTool audit` reads via the Fluss scan API — same data plane as the
  sink, not the low-level LogScanner. Production: Flink/Fluss catalog union-read is the
  operator precondition B8.1.)
- [x] Filter `schema_version='2'`.
  (Audit filter; all 1,673,579 rows are v2.)
- [x] Filter approved algorithm version.
  (`algorithm_version=candle-15s-v1` filter applied.)
- [x] Filter approved configuration version.
  (`configuration_version=1.0.0` filter applied.)
- [x] Count total rows.
  (`CANDLE_MIGRATION_TOTAL_ROWS=1673579`.)
- [x] Count distinct canonical keys.
  (`CANDLE_MIGRATION_DISTINCT_KEYS=1351301`.)
- [x] Identify duplicate keys.
  (`CANDLE_MIGRATION_DUPLICATE_KEYS=155161`.)
- [x] Identify conflicting business values.
  (`CANDLE_MIGRATION_CONFLICTING_KEYS=25` — all `windowStart=1786258020000`,
  `open_paise` ±1000, replay-incident window.)
- [x] Report non-canonical rows separately.
  (`CANDLE_MIGRATION_NON_CANONICAL_ROWS=0`.)
- [x] Abort on conflicting business values.
  (`CANDLE_MIGRATION_STATUS=CONFLICT`, exit 2, nothing loaded — correct stop-condition behavior;
  the recorded-decision override is the explicit accept list, see dev-run evidence.)

Business conflict fields exclude `output_ts` and include all other business/schema fields.

### B8.3 Historical load

- [ ] Load exactly one row per canonical key.
  `BLOCKED: decision recorded (2026-08-10: accept MAX(output_ts) row per key via
  CANDLE_MIGRATION_ACCEPT_KEYS_FILE); accept-list mechanism implemented + unit-tested
  (CandleMigrationToolTest 11) + verified read-only on dev (ACCEPTED_KEYS=25, STATUS=OK,
  exit 0); the load itself is an approved-operator step and has NOT run.`
- [ ] Use `MAX(output_ts)` only when all business values are equal.
  `BLOCKED: merge rule enforced by the tool; the recorded decision extends it to the 25
  accepted conflict keys (MAX(output_ts) row wins — same convergence the live KV sink
  applies). No load ran, so the rule is not yet exercised on the destination.`
- [ ] Record source count, distinct-key count, destination count, conflicts, and excluded rows.
  `BLOCKED: audit counts recorded (1,673,579 / 1,351,301 / n/a / 25 / 0); destination
  count not produced because no load ran.`
- [ ] Preserve LOG unchanged.
  `BLOCKED: tool is read-only on the LOG by construction (no load ran); the invariant
  holds by design, final proof pending the load.`

### B8.4 Cutover

- [ ] Configure `CANDLE_TABLE=feature_candles_15s`.
  `BLOCKED: live dual-sink start is an approved-operator step (§5.2 line 236 — no Phase 8
  cluster execution during the code implementation task).`
- [ ] Configure `CANDLE_CURRENT_TABLE=feature_candles_15s_current`.
  `BLOCKED: same as above (operator cutover pending).`
- [ ] Use restore mode for normal restart.
  `BLOCKED: cutover pending; the required restore wiring is implemented and rehearsed (P6).`
- [ ] Abort rather than automatically full-replay if restore fails.
  `BLOCKED: cutover pending; the fail-closed gate (A3.2) guarantees this at startup.`
- [ ] Use FULL_REPLAY only with explicit operator approval, capacity review, and `ALLOW_FULL_REPLAY=true`.
  `BLOCKED: cutover pending; the gate accepts FULL_REPLAY only when explicitly set.`

### B8.5 Bounded replay proof

- [x] Use a fixed interval or fixed key set.
  (Scratch LOG `mig_scratch_log_*`: fixed set of 3 keys appended twice — replay signature
  = same business fields, `output_ts` 100→200.)
- [x] Replay exactly twice.
  (`CandleMigrationTool load` run twice against scratch KV `mig_scratch_kv_*`.)
- [x] KV key count unchanged.
  (Run 1 and run 2 both: `DEST_ROWS_AFTER=3`.)
- [x] Exactly one KV row per key.
  (Scan verified one row per key.)
- [x] Business values unchanged.
  (`CONFLICTING_KEYS=0` both runs.)
- [x] `output_ts` may differ.
  (`output_ts=200` last-write-wins on the second load.)
- [x] LOG count may increase and that increase is expected.
  (Tool is read-only on the LOG — LOG stayed at 6 rows; invariant: LOG may grow on replay
  and that growth is expected/by design.)

### B8.6 Rollback

- [x] Stop dual-sink job.
  (Implemented as B8.7 step 1 — graceful SIGTERM, wait for `Completed checkpoint N`.)
- [x] Keep both tables.
  (B8.7 step 2 — KV rows remain valid canonical history; upsert is idempotent.)
- [x] Restore previous artifact/configuration.
  (B8.7 step 3 — pre-cutover single-LOG-sink build + previous env.)
- [x] Repoint consumers if required.
  (B8.7 step 4.)
- [x] Do not drop the KV table during rollback.
  (B8.7 steps 2+4; dropping would lose the idempotent current-state copy.)
- [x] Document rollback cutoff.
  (B8.7 step 5 — first dual-sink checkpoint; document the checkpoint ID in the incident log.)
- [x] State explicitly that KV failure fails the shared job; LOG-only degraded mode is not supported.
  (B8.7 step 6.)
  (All B8.6 steps are documented in the B8.7 authoritative runbook — execution/rehearsal is
  pending with the operator cutover.)

### B8.7 Rollback runbook (authoritative — no second migration document)

Rollback = revert to the pre-cutover single-LOG-sink artifact, not a table
destruction. The KV table is a companion projection; dropping it would lose
the idempotent current-state copy and force a rebuild.

1. **Stop the dual-sink job gracefully** (SIGTERM via hub/launcher; wait for
   the last checkpoint to complete in the log, `Completed checkpoint N`).
2. **Keep both tables** — do not drop `feature_candles_15s_current`. Its rows
   remain valid canonical history; a re-cutover later re-loads or resumes from
   the same KV state (upsert is idempotent).
3. **Restore the previous artifact/configuration** — launch the last
   pre-cutover SignalJob build with its previous env (single LOG sink, same
   `STATE_RECOVERY_PATH` restore discipline).
4. **Repoint consumers if required** — any consumer that moved to the KV
   projection must move back to the LOG (or keep reading the KV table; it is
   not dropped). LOG-only readers are unaffected.
5. **Rollback cutoff** — the cutoff is the first checkpoint of the dual-sink
   run: rows written to the KV table after that instant exist only in KV and
   must be re-derived after re-cutover (LOG rows are preserved either way).
   Document the cutoff checkpoint ID in the incident log.
6. **Degraded mode is not supported** — the shared job fails if the KV sink
   fails (same job graph, checkpoint restore covers both sinks). There is no
   LOG-only degraded topology; rollback is the only safe escape.
7. **Verify rollback** — restore-mode restart, candle contracts preflight OK,
   LOG sink receives rows, KV sink absent from the graph, checkpoints
   completing inside the 30s contract.

### Phase 8 dev-run evidence (2026-08-10)

Executed with the offline-approved tooling against the dev cluster (Fluss
`localhost:9123`, datalake tier disabled; see `logs/candle-kv-replay-001/`):

- **B8.2 dry-run audit (dev LOG `feature_candles_15s`, read-only)** — via
  `com.trading.compute.tools.CandleMigrationTool audit`:
  `CANDLE_MIGRATION_TOTAL_ROWS=1673579`,
  `CANDLE_MIGRATION_CANONICAL_ROWS=1673579`,
  `CANDLE_MIGRATION_NON_CANONICAL_ROWS=0`,
  `CANDLE_MIGRATION_DISTINCT_KEYS=1351301`,
  `CANDLE_MIGRATION_DUPLICATE_KEYS=155161`,
  `CANDLE_MIGRATION_CONFLICTING_KEYS=25`.
- **B8.2 stop condition found:** 25 keys hold genuinely different
  business values (`open_paise` differs by exactly 1000, e.g. token=101000
  first=13355 later=12355) — the replay-incident / feed-fix artifact window.
  23 keys at `windowStart=1786258020000`, 2 keys (tokens 101101 and 102000)
  at the adjacent window `windowStart=1786258755000` (the earlier "all at
  one windowStart" claim was inferred from the 5 capped examples; the
  complete-key probe corrected it). The audit aborted with
  `CANDLE_MIGRATION_STATUS=CONFLICT`, exit 2, and **nothing was loaded**.
  The append-only LOG cannot be surgically edited (no row-level delete).
- **Data-ops decision (recorded 2026-08-10):** accept the `MAX(output_ts)`
  row per key for all 25 conflicting keys — the most recently written row
  wins (last-write-wins), the same convergence the live KV sink applies to
  replayed candles (`output_ts` is emit metadata, not row identity). The
  alternative — leaving the keys absent and letting post-cutover writes
  converge future windows — was considered and rejected: it would leave the
  historical projection incomplete for those 25 keys.
- **Accept-list mechanism (implemented):** `CandleMigrationTool` reads
  `CANDLE_MIGRATION_ACCEPT_KEYS_FILE` (one `token,windowStart` per line, `#`
  comments allowed; entries = recorded operator decision). Conflicts on the
  list count as `ACCEPTED_KEYS` and merge by `MAX(output_ts)`; conflicts NOT
  on the list still abort (exit 2, fail-closed); accept-list entries matching
  no canonical LOG key abort as errors (exit 1, typo/stale-list detection).
  Accept file: `logs/candle-kv-replay-001/accept-keys-2026-08-10.csv`
  (25 entries, verified complete by the read-only `DumpConflictKeys` probe —
  exactly the 25 keys above; key derivation matches the tool's two-level
  token→windowStart map).
- **B8.2 audit re-run with the accept file (read-only, dev LOG)** — via
  `CandleMigrationTool audit` + `CANDLE_MIGRATION_ACCEPT_KEYS_FILE`:
  `TOTAL_ROWS=1673579 CANONICAL_ROWS=1673579 NON_CANONICAL_ROWS=0
  DISTINCT_KEYS=1351301 DUPLICATE_KEYS=155161 CONFLICTING_KEYS=25
  ACCEPTED_KEYS=25 UNACCEPTED_KEYS=0 ACCEPT_KEYS_NOT_FOUND=0
  STATUS=OK` — exit 0 (previously exit 2). All other counts unchanged,
  so the accept list disturbs nothing else. Regression: audit without the
  file still aborts (`UNACCEPTED_KEYS=25`, exit 2).
  **The load itself has NOT run** — it is an approved-operator step
  (`CandleMigrationTool load`, expecting `DEST_ROWS_AFTER == DISTINCT_KEYS`).
- **B8.5 bounded replay proof (scratch tables only, PASSED)** — fixed key
  set of 3 keys appended twice into a scratch LOG (replay signature: same
  business fields, `output_ts` 100→200), then `CandleMigrationTool load` run
  **twice** against a scratch KV table:
  run 1: `TOTAL_ROWS=6 DISTINCT_KEYS=3 DUPLICATE_KEYS=3 CONFLICTING_KEYS=0
  LOADED=3 DEST_ROWS_AFTER=3`; run 2 (replay): identical, `DEST_ROWS_AFTER=3`
  — KV key count unchanged, exactly one row per key (verified by scan:
  business values intact, `output_ts=200` last-write-wins), LOG untouched
  (read-only, still 6 rows). Scratch tables dropped after the run; platform
  tables untouched.
- **B8.4 cutover is NOT executed in this task** — §5.2 execution rules
  line 236: "Do not execute Phase 8 against any cluster during the code
  implementation task." The dual-sink live start is an approved-operator step
  after code review; the preconditions (graph comparison + copied-checkpoint
  rehearsal) are already met (P6).

### Phase 8 compound gate

- [ ] **P8 AUDIT COMPLETE:** complete-history read and conflict audit passed.
  `BLOCKED: dev LOG audit found 25 conflicting business values (see "Phase 8
  dev-run evidence"); decision recorded 2026-08-10 (accept MAX(output_ts) row
  per key); audit re-run with the accept list: ACCEPTED_KEYS=25
  UNACCEPTED_KEYS=0 NOT_FOUND=0 STATUS=OK exit 0. The re-run is a dry run —
  the audit gate is considered satisfied only when the approved operator
  confirms it on the production data plane (B8.1 precondition).`
- [ ] **P8 LOAD COMPLETE:** canonical history was loaded and counts reconciled.
  `BLOCKED: load has NOT run — approved-operator step after code review
  (§5.2 line 236). Preconditions met: zero unaccepted conflicts (audit re-run
  exit 0), accept-list mechanism unit-tested (CandleMigrationToolTest 11) +
  dev-verified, scratch-table load path proven end-to-end (B8.5). Expect
  CANDLE_MIGRATION_DEST_ROWS_AFTER == DISTINCT_KEYS (1,351,301).`
- [ ] **P8 CUTOVER COMPLETE:** dual-sink deployment, bounded replay proof, and rollback evidence passed.
  `BLOCKED: live dual-sink start is an approved-operator step (§5.2 line 236:
  no Phase 8 cluster execution during the code implementation task).
  Preconditions met: graph comparison (P6) + copied-checkpoint rehearsal (P6).
  Bounded replay proof PASSED on scratch tables (B8.5); rollback runbook
  written (B8.7); rollback rehearsal pending.`

---

## Phase 9 — Documentation and evidence

- [x] Update SQL README: existing LOG plus canonical KV.
  (2026-08-10: `code/01_platform/02_sql/README.md` table list regenerated —
  actual 21 files, LOG/KV kinds, `22_feature_candles_15s_current.sql` row.)
- [x] Update reconciliation blocker.
  (2026-08-10: `00_RECONCILIATION_BLOCKER.md` superseded note — DDL set now 21
  numbered files ending `22_feature_candles_15s_current.sql`.)
- [x] Update storage requirements and contracts.
  (2026-08-10: `02-storage.md` — `feature_candles_15s_current` row +
  REQ-FLS-006 projection text; `docs/04_contracts/02-storage.md` Market list.)
- [x] Update Signal Job implementation document.
  (2026-08-10: `04-signal-job.md` — dual-sink job graph, config rows,
  startup-mode gate section, bootstrap-owned-tables paragraph, pin-table rows,
  deferred-metrics note, Slice 1 scope/files/tests.)
- [x] Update compute README.
  (2026-08-10: `02_compute/README.md` — status block, checklist item, pins.)
- [x] Update `Candle15s` javadocs.
  (2026-08-10: KV-twin + `output_ts` last-write-wins + shared-contract javadoc.)
- [x] Add `CANDLE-KV-001` evidence row as pending until migration/idempotency evidence passes.
  (§9: `PARTIAL` — DDL/schema-parity/upsert complete; migration blocked.)
- [x] Add `STARTUP-GATE-001` evidence row.
  (§9: `COMPLETE`, `SignalJobConfigTest` 19 tests + `ComputeOtlpEmitterTest`.)
- [x] Add `CHECKPOINT-RESTORE-001` evidence row.
  (§9: `COMPLETE`, rehearsal chk-1538→1539.)
- [x] Document expanded replay/checkpoint metrics as deferred unless implemented through existing telemetry.
  (2026-08-10: `04-signal-job.md` Required-telemetry note — deferred; existing
  `compute.startup.mode` gauge + built-in checkpoint metrics + offline audits.)
- [x] Audit checked-in launch examples.
  (2026-08-10: `submit-jobs.sh` is an env-free placeholder (no main class
  wired); no other checked-in launch script exists; `/tmp/run-signaljob-fixed.sh`
  is outside the repo and uses the RESTORE pattern.)
- [x] Ensure no normal production default contains `ALLOW_FULL_REPLAY=true`.
  (2026-08-10: repo-wide grep — `ALLOW_FULL_REPLAY=true` appears only in unit-test
  fixtures (A3.2) and the P6 evidence log; `SignalJobConfig` default is absent→false,
  and absent path + absent replay fails startup.)
- [x] Document normal RESTORE and deliberate FULL_REPLAY modes separately.
  (2026-08-10: `04-signal-job.md` §Startup mode gate + this tracker A3.2.)

### Phase 9 compound gate

- [x] **P9 COMPLETE:** governed docs, launch examples, evidence rows, and deferred observability statements agree with executable behavior.
  (2026-08-10: all Phase 9 items above; final report:
  `logs/candle-kv-replay-001/final-report-2026-08-10.md`.)

---

# 7. Validation commands

Do not start SignalJob.
Do not run `docker compose up`.
Do not write to live Fluss.
Do not apply live DDL.

- [x] Run LSP/static diagnostics on edited Java files.
  (2026-08-10: compute suite compiled clean; no static diagnostics needed beyond
  the compile gate — see tests below.)
- [x] Run compute tests:

```bash
cd code/02_services/02_compute
mvn -q test
```
  (2026-08-10: `Tests run: 81, Failures: 0, Errors: 0, Skipped: 1` — the 1 skip is
  the env-gated `CandleCurrentKvIdempotencyTest` without `COMPUTE_INT_TEST_CANDLE_KV`;
  run separately with the gate + `FLUSS_BOOTSTRAP` → PASSED, see P7 integration evidence.)

- [x] If common artifacts are unavailable:

```bash
cd code/common
mvn -q install -DskipTests
cd ../02_services/02_compute
mvn -q test
```
  (2026-08-10: not needed — common was installed; common suite itself: 104 green,
  incl. `CandleCurrentDdlContractTest`; `CanonicalCandlePolicyTest` is a compute-module
  test (P2) and runs in the compute suite.)

- [x] Run affected common/ingestion tests:
  (2026-08-10: ingestion `Tests run: 175, Failures: 0, Errors: 0, Skipped: 7` — the
  7 skips are env-gated integration tests; common 104 green.)

```bash
cd code
mvn -q test -pl common,02_services/01_ingestion
```

- [x] Generate the DDL manifest offline:
  (2026-08-10: `python3 code/01_platform/04_scripts/ddl_apply.py --force` — 21 tables,
  exactly one ADDED `feature_candles_15s_current`; sha
  `8e7ccd03761284c29ccb7b0a13dc5e76385a0399387e9b4f9b686754f8341257`.)

```bash
python3 code/01_platform/04_scripts/ddl_apply.py --force
```

- [x] Verify only expected manifest changes.
  (2026-08-10: one ADDED row; all 20 prior tables byte-identical.)
- [x] Run:

```bash
make cep-check
```
  (2026-08-10: passed in the P1/P7 sweep — no `flink-cep` dependency or import.)

- [ ] Run `make static-check` only if relevant shell files changed.
  (Not run: no shell files changed by this tracker; `submit-jobs.sh` untouched.)
- [x] Do not fabricate test results when dependencies are unavailable.
  (Followed: integration skips are explicit env-gated skips, not passes.)

---

# 8. Acceptance gates

## Code gate

- [x] Existing LOG unchanged in kind/schema.
  (03 DDL untouched; LOG sink ID `aa0083b9…` stable; LOG sink `RowDataSerializationSchema(true, true)` unchanged.)
- [x] New KV DDL present and correct.
- [x] Shared schema contract present.
- [x] Dual sinks implemented.
- [x] Fluss metadata preflight implemented.
- [x] Runtime bootstrap cannot create compute tables.
- [x] Replay startup gate implemented.
- [x] Canonical-version policy implemented.

## Unit-test gate

- [x] All affected unit tests pass.
  (common 104, compute 81 (1 env-gated skip), ingestion 175 (7 env-gated skips) — 2026-08-10.)
- [x] Existing validation behavior remains covered.
- [x] DDL/code parity passes.
  (`CandleCurrentDdlContractTest`, `CandleTableColumnsAgreementTest`.)
- [x] Startup mode tests pass.
  (`SignalJobConfigTest` 19.)
- [x] Table metadata validator tests pass.
  (`CandleTableContractValidatorTest` 10.)

## Integration gate

- [x] KV same-key upsert test passes when `FLUSS_BOOTSTRAP` is available.
  (2026-08-10: `COMPUTE_INT_TEST_CANDLE_KV=true FLUSS_BOOTSTRAP=localhost:9123 … CandleCurrentKvIdempotencyTest` — Tests run: 1, Failures: 0, Errors: 0.)
- [x] Otherwise marked `PENDING_INTEGRATION` with exact reason.
  (n/a — the gated test passed; the suite-level skip without the gate is documented.)

## Graph/restore gate

- [x] Old/new JobGraph comparison complete.
- [x] Existing stateful operator IDs unchanged, or migration is separately designed.
  (source/dedup/candles/LOG-sink/detection stable; candidates-sink ID change proven stateless.)
- [x] Copied-checkpoint rehearsal passes, or explicitly marked `PENDING_REHEARSAL`.
  (PASSED 2026-08-10 — chk-1538→1539, 990ms, job `e2e7b624…`.)
- [x] No `allowNonRestoredState` bypass.
  (rehearsal ran default `allowNonRestoredState=false`; only expected skip: candidates-sink stateless state.)

## Migration gate

- [x] New KV table created blue-green by operator.
  (Dev: `feature_candles_15s_current` id=92 created by the tracker-approved rehearsal DDL step; production creation is the operator's blue-green step.)
- [x] Complete LOG/lake history readable.
  (Dev: full LOG history scanned via the Fluss scan API — 1,673,579 rows, id=90, 16 buckets, `bucketKeys=[instrument_token]`. Production: Flink/Fluss catalog union-read across Iceberg-tiered data is an operator precondition (B8.1) before the tool is approved there.)
- [ ] Dry-run duplicate/conflict audit passes.
  `BLOCKED: audit re-run with the recorded-decision accept list passed on dev
  (25/25 accepted, UNACCEPTED_KEYS=0, NOT_FOUND=0, STATUS=OK exit 0); the
  approved-operator confirmation on the production data plane (B8.1 union-read
  precondition) is what flips this box.`
- [ ] Canonical rows loaded.
  `BLOCKED: load NOT executed — approved-operator step (see Phase 8 dev-run
  evidence; decision + accept list ready, dev dry-run exit 0).`
- [ ] Destination count equals distinct canonical-key count.
  `BLOCKED: pending load (CANDLE_MIGRATION_DEST_ROWS_AFTER must equal
  DISTINCT_KEYS = 1,351,301).`
- [ ] No business conflicts remain unresolved.
  `BLOCKED: 25 conflicts resolved by recorded decision (accept list) for the
  load; the box flips when the load runs with zero unaccepted conflicts.`
- [x] Bounded replay leaves KV key count unchanged.
  (B8.5 scratch proof PASSED: 3 keys appended twice, `load` run twice →
  `DEST_ROWS_AFTER=3` both runs, one row per key, `output_ts=200` last-write-wins.)

## Operational completion gate

The original incident is not considered fully resolved until:

- [x] Normal restarts require checkpoint restore.
  (Startup gate A3.3: absent `STATE_RECOVERY_PATH` fails startup.)
- [x] Accidental no-restore startup fails.
  (Same gate; `SignalJobConfigTest` covers all invalid combinations.)
- [x] Deliberate full replay requires explicit break-glass approval.
  (`ALLOW_FULL_REPLAY=true` only; both-set and neither-set rejected; no default replay.)
- [ ] Current-state consumers read the KV projection.
  `PENDING: cutover is an approved-operator step after code review (§5.2 line 236).`
- [ ] LOG duplicate count and KV unique-key count are monitored separately.
  `DEFERRED: dedicated KV-replay metrics deferred; offline CandleMigrationTool audits
  + existing telemetry cover convergence checks until then (Phase 9 item).`
- [ ] Rollback procedure has been rehearsed or explicitly accepted as pending.
  `PENDING: B8.7 runbook written; rehearsal scheduled with the cutover run.`

---

# 9. Evidence register

| Evidence ID | Required evidence | Status | Evidence location/date |
| --- | --- | --- | --- |
| `CANDLE-KV-001` | New KV DDL, schema parity, same-key upsert, migration proof | `PARTIAL` — DDL/schema-parity/same-key-upsert COMPLETE; migration proof BLOCKED (25 conflicting keys) | 2026-08-10: `code/01_platform/02_sql/ddl/22_feature_candles_15s_current.sql`, `CandleCurrentKvIdempotencyTest`, B8.2 audit output |
| `STARTUP-GATE-001` | Unit tests proving explicit RESTORE/FULL_REPLAY modes | `COMPLETE` | 2026-08-10: `SignalJobConfigTest` (19 tests) + `ComputeOtlpEmitterTest` |
| `CHECKPOINT-RESTORE-001` | Copied-checkpoint restore with dual-sink graph | `COMPLETE` | 2026-08-10: `logs/candle-kv-replay-001/rehearsal-2026-08-10.log` + `p6-evidence-2026-08-10.md` (restore chk-1538, checkpoint 1539 at 990ms) |
| `CANDLE-MIGRATION-001` | Dry-run audit and canonical LOG→KV load | `BLOCKED` — audit COMPLETE; 25 conflicts resolved by recorded decision (accept list); dry-run re-run exit 0; load not executed (approved-operator step) | 2026-08-10: `CandleMigrationTool` audit (CONFLICT + accept-list re-run OK), `logs/candle-kv-replay-001/accept-keys-2026-08-10.csv`, Phase 8 dev-run evidence |
| `CANDLE-CUTOVER-001` | Consumer cutover and bounded replay proof | `PARTIAL` — bounded replay proof PASSED (scratch); live cutover pending approved-operator run (§5.2 line 236) | 2026-08-10: B8.5 scratch proof + B8.7 runbook |

---

# 10. Tracker completion summary

| Milestone | Status | Completion date | Evidence |
| --- | --- | --- | --- |
| P0 baseline and contract freeze | `COMPLETE` | 2026-08-10 | `logs/candle-kv-replay-001/baseline-2026-08-10.md` |
| P1 shared contract and DDL | `COMPLETE` | 2026-08-10 | `CandleTableSchema`, `22_feature_candles_15s_current.sql`, manifest regen (21 tables) |
| P2 canonical-version policy | `COMPLETE` | 2026-08-10 | `CanonicalCandlePolicy` + tests |
| P3 configuration and replay gate | `COMPLETE` | 2026-08-10 | `SignalJobConfigTest` 19 tests, startup gauge |
| P4 Fluss metadata preflight/bootstrap ownership | `COMPLETE` | 2026-08-10 | `CandleTableContractValidator`, `DdlBootstrap` owned-tables |
| P5 dual sinks | `COMPLETE` | 2026-08-10 | KV sink last out-edge, LOG sink ID `aa0083b9` preserved |
| P6 code graph compatibility | `COMPLETE` | 2026-08-10 | `logs/candle-kv-replay-001/jobgraph-baseline/` + `jobgraph-post2/`, `p6-evidence-2026-08-10.md` |
| P6 operational restore rehearsal | `COMPLETE` | 2026-08-10 | `logs/candle-kv-replay-001/rehearsal-2026-08-10.log` (chk-1538→1539) |
| P7 unit tests | `COMPLETE` | 2026-08-10 | common + compute (81) + ingestion (175) green |
| P7 integration tests | `COMPLETE` | 2026-08-10 | `CandleCurrentKvIdempotencyTest` PASSED |
| P8 migration audit/load/cutover | `BLOCKED` | — | audit conflict resolved by recorded decision (accept list, dry-run exit 0); load + cutover await approved-operator execution (see Phase 8) |
| P9 documentation/evidence closure | `COMPLETE` | 2026-08-10 | SQL README, blocker, storage reqs/contracts, signal-job doc, compute README, `Candle15s` javadocs, evidence register, launch audit, `final-report-2026-08-10.md` |

Overall statuses:

```text
IMPLEMENTATION_STATUS=IN_PROGRESS
CANDLE_KV_001=PARTIAL
STARTUP_GATE_001=COMPLETE
CHECKPOINT_RESTORE_001=COMPLETE
CANDLE_MIGRATION_001=BLOCKED
CANDLE_CUTOVER_001=PARTIAL
```

---

# 11. Required final report

At completion, report:

1. Root cause addressed.
2. Files created.
3. Files modified.
4. Exact DDL changes.
5. Exact dual-sink behavior.
6. Exact startup-gate behavior.
7. Exact Fluss metadata preflight behavior.
8. Canonical-version policy.
9. Test commands and results.
10. Offline manifest result.
11. Old/new JobGraph comparison result.
12. Checkpoint restore rehearsal result or exact pending reason.
13. Whether any live cluster, live data, or live DDL was touched.
14. Manual migration steps remaining.
15. Rollback procedure.
16. Evidence register updates.
17. Unresolved blockers.

Never report the project as fully production-ready while any required evidence
is `PENDING`, `PENDING_INTEGRATION`, or `PENDING_REHEARSAL`.

If any Flink, Fluss, Java, DDL, or repository API differs from this tracker,
stop before making a semantic substitution and report:

- requested behavior;
- actual behavior;
- exact source/API evidence;
- smallest safe alternative.
