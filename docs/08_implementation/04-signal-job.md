# Signal Job

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | **Slice 1 (raw source → validation → fingerprint dedup → 15 s event-time candles → `feature_candles_15s`) implemented; 25 signal-job tests green; live smoke verified 2026-08-09** (see Slice 1 evidence below). **Slice 2.1 (MVP signal detection → `Signal_Candidates`, DEC-034) implemented; 34 signal-job tests green; live smoke verified 2026-08-10** (see Slice 2.1 section below). Slot-scoped safety consumer (plan Amendment) implemented and live-verified — SAFETY-INT-001 passed 2026-08-09, see Safety consumer section below. **CANDLE-KV-REPLAY-001 (2026-08-10): candle dual-sink (`feature_candles_15s_current` KV projection) + fail-closed startup gate + offline migration tool implemented; unit/integration tests green; historical load and live cutover blocked pending data-ops decision on 25 replay-conflict keys — see `13-candle-log-kv-replay-safety.md`.** **Candle conversion (2026-08-13): `feature_candles_15s` converted to KV-only (PK `(instrument_token, window_start)`), candle-KV machinery deleted, signal tables remain LOG + KV — see banner; compute suite 185 tests green + 7-test gated battery.** Slice 2.2 forming-bar **handoff implemented 2026-08-16** (Q1=A handoff-only, Q2=B placeholder detector): `FormingBarBuilderFunction` (per-tick live forming bar, REQ-FC-007/AC-FC-014) → `FormingBar` typed event (record extended with `exchange`/`symbol`; custom `FormingBarTypeInfo` serializer, no Kryo) → `FormingBarDetectionFunction` (two-input keyed CoProcess: live forming bars + completed-candle lookback, mirrored-breakout placeholder rule, fire-once-per-window) → candidate rows unioned into the existing signal LOG + KV dual-sink (REQ-SS-003/DEC-035). **Forming-bar KV persistence implemented 2026-08-16:** the `forming_bar` KV current-state home is now live — the builder's `PERSIST_OUTPUT` side output feeds a coalescing `FormingBarWriterFunction` (keyed by instrument, one buffered row per instrument — the LATEST forming bar, replacing any previous state; flush on the `FORMING_BAR_WRITE_BATCH_MS` cadence) into the `forming_bar` KV upsert sink (PK `instrument_token`, INSERT→UPSERT, current-state only — never append-only, never per-tick history; the finalized candle stays the completed-candle pipeline's artifact). The live dev table was recreated from DDL 04 (the old id-7 table was a 7-column LOG of a prior era — no PK, wrong schema; LiveSync drop+recreate → id 3254 KV, preflight `validateFormingBarKvTable` PASS); the writer/table wiring is ALWAYS-ON in the preflight (DEC-038: forming bar is Fluss-authoritative durable state). JobGraphDump proof (2026-08-16): every pre-existing operator ID bit-identical (`d49b0765` source, `924023bd` candle window, `5b71ba6c` dedup, `f5ca0e06` signal-detection, all sinks); the only structural change is the stateless `canonical-signal-filter` splitting out of the `signal-detection` chain (union boundary); new stateful `forming-bar-builder` + `forming-bar-detection` appended. Slice 3 (Ranking/Reservations/Decisions) **removed from scope by decision (CHG-005, 2026-08-15 — not deferred)**; the SCH-19 decision dual-sink machinery is implemented but gated off (`TRADE_DECISIONS_ENABLED=false`, no producer in scope).  **2026-08-13 (DEC-035): candle tables KV-only; signal LOG+KV dual-sink implemented (Stages 3–6 executed, live DDL applied).** **2026-08-14 (DEC-038): state-ownership requirement — large durable hot Signal state (the dedup set) moves to a Fluss KV state table; Flink keeps small working + recovery state; checkpoint is not a second copy.** **2026-08-15 (DEC-038): design/DDL/pins + live wiring implemented — `24_fingerprint_dedup.sql` (6-col, PK `(instrument_token, fingerprint_version, event_fingerprint)`, 16 buckets), `FingerprintDedupStateStore` SPI + in-memory/Fluss twins, bounded cache (`DEDUP_CACHE_MAX_ENTRIES`/`_BYTES`) with query-on-miss rehydration, `FingerprintDedupWriterFunction` batched upsert wired via side output, `validateFingerprintDedupTable` ALWAYS-ON in the preflight (CHG-003/CHG-005).** **2026-08-15 (externalization benchmark landed — SIG-STATE-001/002 + SIG-PERF-001 evidence):** `FingerprintDedupExternalizationBenchmarkIT` (env-gated `COMPUTE_INT_TEST_DEDUP_EXT`) exercised the production store against the live dev cluster: durable write throughput (~9 upserts/s shared writer, ~325 ms/row `putFirstSeen`), cold-restart hydration (15/15 sampled lookups SEEN_LIVE from Fluss authority, SIG-STATE-002/003), bounded cleanup (all 15 stale rows deleted, plateau = live set, SIG-PERF-001). The run surfaced and fixed two evidence-gated cleanup bugs: (1) `bucketOf` was `token % numBuckets` but Fluss assigns buckets via `KeyEncoder.ofBucketKeyEncoder(rowType, bucketKeys, lake)` + `BucketingFunction.of(table lakeFormat)` (dev default `iceberg`) — fixed with `DedupBucketAssigner`, pinned by `DedupBucketAssignerTest`; (2) `UpsertWriter.delete` requires the full 6-column row (checkFieldCount), not the 3-field PK row. Ranking/reservation out of scope. **2026-08-15 (MiniCluster restore + checkpoint-size re-measurement landed — SIG-STATE-001 final leg):** `SignalJobCompactCheckpointRestoreIntegrationTest` (env-gated `COMPUTE_INT_TEST_SIG_STATE_RESTORE`, host-runnable) proved the checkpoint is bounded across 5x dedup cardinality (54465 vs 54482 bytes, ratio 1.00) and a strict restore resumes with no full replay in ~870 ms — and surfaced a third real DEC-038 violation (orphaned event-time timers on eviction, fixed by deleting the timer with its bucket; without it the checkpoint grew ratio 2.70). Compute suite **325 tests green / 17 env-gated skips** (verified 2026-08-16; +4 `DedupBucketAssignerTest`, +1 eviction timer-deletion unit test, +1 compact-restore IT, +3 `CandleWindowEmitHarnessTest` (emitted-flag no-op + SIG-HARNESS-002), +2 `CandleWatermarkIdlenessTest` (SIG-HARNESS-001 idleness half), +1 `FingerprintDedupFunctionTest.identicalLookingEventsCollapseAndEmitLimitationEvidence` (SIG-HARNESS-004), +1 `CompatFlinkCheckpointRescaleIntegrationTest` (COMPAT-FLINK-001 + STATE-COMPAT-001 serializer half), +1 `SigState002RehydrationRestoreIntegrationTest` (SIG-STATE-002 full job-restart half — env-gated `COMPUTE_INT_TEST_SIG_STATE_REHYDRATE`, PASSED live 2026-08-15), +1 `SigState003FailClosedPreflightIntegrationTest` (SIG-STATE-003 fail-closed preflight — env-gated `COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT`, class-level assumeTrue gate, reports 0/0, PASSED live 2026-08-15), +2 REQ-FC-010 metric tests (`ComputeOtlpEmitterTest.reqFc010SourceAndWatermarkMetrics` + `SourceIdleWatchdogGeneratorTest.recordsThroughputAndWatermarkLagMetrics`, Phase B item 9), +15 Slice 2.2 forming-bar handoff tests (`FormingBarBuilderFunctionTest` ×5, `FormingBarDetectionFunctionTest` ×7, `FormingBarTypeInfoTest` ×3), +12 forming-bar persistence tests 2026-08-16 (`FormingBarWriterFunctionTest` ×6, `TableContractValidatorTest` forming-bar legs ×4, `SignalJobConfigTest.rejectsNonPositiveFormingBarWriteBatchMs`, `FormingBarBuilderFunctionTest.persistSideOutputCarriesEverySnapshot` (the side output carries the same snapshot per accepted tick — the writer coalesces, so this is never a per-tick Fluss write), +1 `FormingBarWriterFunctionTest.checkpointRestoreResumesBufferedBar` (restart-rehydration Flink half: checkpoint → restore harness → the buffered bar resumes exactly, a new-window bar replaces it — DEC-038 recovery semantics), +3 live `FormingBarRehydrationIntegrationTest` legs (FORMING-BAR-REHYDRATE-001, 2026-08-16: latest-wins within a window, window rollover replaces on the same PK, cold-restart rehydration of the exact latest bar through the production `FlussFormingBarStateStore`, missing instrument reads empty — env-gated `COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE`, PASSED live 2026-08-16 against the dev cluster)).|
| Owner | Compute and Strategy Teams |
| Requirements | `REQ-FC-001`–`REQ-FC-013`, `REQ-SS-001`–`REQ-SS-011`, `REQ-RNK-001`–`REQ-RNK-009` → `AC-FC-001`–`AC-FC-016`, `AC-SS-001`–`AC-SS-012`, `AC-RNK-001`–`AC-RNK-009` |
| Contracts | `docs/04_contracts/03-compute.md`, `04-business-logic.md` (`10-ranking.md` REMOVED 2026-08-15, CHG-005) |
| Job topology | One Signal job containing Compute and Business Logic (**Ranking REMOVED 2026-08-15, CHG-005**) |
| Separate job | Babysitter only |

> **REQUIREMENT CHANGE (user decision, 2026-08-13) — candle tables are KV-only; the [LOG + KV] facility lives on the SIGNAL tables.**
>
> Per the user's requirement, `feature_candles_15s` is now **KV-only**: PK exactly
> `(instrument_token, window_start)`, `bucket.key=instrument_token`, 16 buckets,
> 15-column v2 schema. One row per closed 15 s window per instrument; upsert is
> last-write-wins, so replay re-upserts the same key and the table converges
> (no row growth). The candle LOG+KV dual-sink era (CANDLE-KV-REPLAY-001,
> implemented 2026-08-10) and the `feature_candles_15s_current` KV projection
> (DDL 22) are deleted; conversion (DDL 03 → KV, manifest, DdlBootstrap,
> `CandleTableSchema`, `TableContractValidator.validateCandleKvTable`, KV sink
> serialization, tests, o2-provision.py) is implemented and verified 2026-08-13.
> Live dev table recreated as KV the same day (LiveSync drop+recreate:
> id 90 LOG → id 693 KV; preflight `validateCandleKvTable` PASS — evidence
> `logs/tracker-14/p6-livesync-candle-kv-20260813.md`). Recreated again
> 2026-08-13 at the DDL blueprint TTL: id 693 → 697, `table.log.ttl` 7d → 2d,
datalake disabled on the live table (dev deviation vs DDL `enabled=true` —
avoids the 0.9.1 create-only lake re-enable trap), preflight PASS on 697 —
evidence `logs/tracker-14/ttl-live-recreate-2d-20260813.md`. The same
lake-off state applies to all ten recreated 2d tables (full id list +
consequences: log-scan reads only, lake re-enable = documented recovery —
see `02-schema-storage.md` Phase C lake-state note 2026-08-13).
>
> The [LOG + KV] facility on the **trade-signal table on Fluss** (user confirmed):
>
> - `Signal_Candidates` → **LOG** (append one new row per found signal; never updated) —
>   matches the business-logic contract "`Signal_Candidates`: immutable candidate audit";
>   reverses R-084's KV conversion, with supersession handled on the KV twin.
> - `Signal_Candidates_current` (new) → **KV current-state**, PK `(instrument_token)` —
>   latest/active candidate per instrument, updated on supersession.
>
> The dual-sink descriptions and test rows below (marked CANDLE-KV-REPLAY-001 / candle
> KV) are historical records of the pre-conversion build; the signal LOG + KV sinks are
> the current dual-sink. Section content below annotates the affected rows.
>
> **DOC-CONSISTENCY (2026-08-16):** the 2026-08-13 table-kind re-scope is reconciled across every documentation layer — the 16 `08_implementation` dossiers plus the `01_project`/`02_requirements`/`03_architecture`/`04_contracts` upstream layers all carry the current kinds (`Signal_Candidates` LOG, `Signal_Candidates_current` KV, `feature_candles_15s` KV; no `feature_candles_15s_current`), with every stale claim annotated historical — never rewritten. Verified 2026-08-16 by the new `stale_table_kind_scan.py` (`make stale-tables`; docs + `--ddl` + `--upstream` modes — **0 un-annotated hits, exit 0**) together with `make docs-audit` (all checks pass). Evidence: `logs/tracker-14/stale-table-kind-reconciliation-clean-20260816.md`.
>
> **DOC-CONSISTENCY (2026-08-16) — phase-status scan:** the doc scanner now also flags stale phase-status wording (forming-bar handoff scope, ranking/reservation scope, `Trade_Decisions` gating) across the same layers. The initial run found 3 un-annotated claims — the business-logic contract MVP-scope banner and the record-contract row — fixed the same day with status annotations (handoff 2026-08-16; removed from scope by CHG-005, not deferred; gated off, `TRADE_DECISIONS_ENABLED=false`). Re-run: **0 un-annotated, exit 0**; `make stale-tables` + `make docs-audit` both pass. Evidence: `logs/tracker-14/phase-status-scan-20260816.md`.

> **DOC-CONSISTENCY (2026-08-16) — numeric-drift scan:** the doc scanner now also flags drifted hard-coded counts — table counts against the current 24-table manifest and the acceptance-ID count in the requirements index against the matrix's 152 — across the dossiers and the upstream layers. The initial run found 6 un-annotated claims (the index count + 5 table-count claims, incl. this dossier's DDL-bootstrap rows and the local-startup verify example), fixed the same day in the annotate-don't-rewrite style with dated markers (2026-08-10 / 2026-08-13 / 2026-08-14, pre-CHG-003); the three section-tier counts in this dossier and 02-schema-storage were annotated the same way so they no longer rely on banner scope. Re-run: **0 un-annotated, exit 0**; `make stale-tables` + `make docs-audit` both pass. Evidence: `logs/tracker-14/numeric-drift-scan-20260816.md`. **Extended 2026-08-16 — test-count drift:** the class now also guards common/ingestion/compute test counts against the docs-audit C6 truth (340/234/325); the extension surfaced 3 stale dated counts (compute + ingestion, 2026-08-15 measurements) across 01-foundation and 15-ingestion-test-hardening, all annotated the same day with dated markers. Evidence: `logs/tracker-14/test-count-drift-scan-20260816.md`. Index of all four evidence files: `logs/tracker-14/stale-claim-reconciliation-index-20260816.md`.
>
> **DEC-038 (2026-08-14) — state ownership.** The canonical contract is [DEC-038 State Ownership and Recovery Contract](#dec-038-state-ownership-and-recovery-contract) directly below. In one line: **Fluss owns the large durable hot Signal-job state; Flink keeps only the small working state needed for active processing plus the minimal recovery/checkpoint state needed to restart safely; the Flink checkpoint is intentionally small and is not a second copy of the complete durable Signal state.** This reverses the pre-change design where ~1 GB of dedup state rode in Flink RocksDB and checkpoints (~1.74 GB measured at 53k t/s, 2026-08-12; ~986 MB dev-hashmap checkpoints 2026-08-14). **DDL + pins + pure core + live wiring landed 2026-08-15 (CHG-003/CHG-005) — see [Design — `fingerprint_dedup` dedup state table](#design--fingerprint_dedup-dedup-state-table-dec-038); the live-cluster externalization measurement remains.** **SUPERSEDED SAME-DAY (2026-08-15): the live-cluster externalization measurement LANDED — externalization benchmark executed against the live dev cluster, SIG-STATE-001/002 + SIG-PERF-001 evidence (see the dossier banner and the Design — `fingerprint_dedup` section below); this banner sentence was not updated when the benchmark landed.** Sections below that still describe the Flink-centric model are marked HISTORICAL (pre-DEC-038) and are superseded where they conflict. Ranking/reservation state is unchanged and out of scope.

### DEC-038 State Ownership and Recovery Contract

**This subsection is the single canonical statement of the DEC-038 state-ownership model for this dossier.** Sections below that predate the change (Dedup state budget, Checkpoint sizing, Concrete sizing) are HISTORICAL pre-DEC-038 baselines, superseded where they conflict; their measurements are retained as evidence, not as target bounds.

**Three state categories:**

1. **Authoritative durable state — Fluss.** Complete durable state for domains explicitly externalized: the dedup set (new Fluss KV state table, proposed `fingerprint_dedup`), closed candles (`feature_candles_15s` KV), current signal state (`Signal_Candidates_current` KV), and the durable forming bar (`forming_bar` KV, implemented 2026-08-16). Flink SHALL NOT retain a complete durable duplicate of any Fluss-authoritative domain.
2. **Transient working state — Flink.** Small state for efficient active processing: the active open-candle accumulator + `emitted` flag, in-flight timer context, signal-detection ring buffers, and the bounded dedup working cache. Not authoritative; derived; SHALL have explicit cardinality and/or byte bounds independent of the total Fluss-authoritative state.
3. **Recovery state — Flink checkpoint.** Source progress/offsets, watermarks, event-time timers, in-flight window state, and minimal working-state metadata. The checkpoint SHALL NOT be a second complete copy of Fluss-authoritative business state.

**State ownership matrix:**

| State | Durable authority | Flink working state | Checkpoint |
| --- | --- | --- | --- |
| Dedup set | Fluss KV state table (`fingerprint_dedup`) | bounded hot cache | cache metadata only |
| Closed candles | `feature_candles_15s` KV | none/history | no full copy |
| Open candle (active window) | Fluss final row after close; Flink while active | active accumulator | yes while active |
| Forming bar | Fluss `forming_bar` KV (implemented 2026-08-16) | small active context | minimal active context |
| Current signal | `Signal_Candidates_current` KV | small transient context | minimal context |
| Signal audit | `Signal_Candidates` LOG | none | no |
| Watermark | Flink | yes | yes |
| Event-time timers | Flink | yes | yes |
| Source progress | Flink/source | yes | yes |

**Dedup protocol (authoritative rule):** incoming tick → validation → bounded Flink cache lookup → cache says definite duplicate → reject; cache miss/uncertain → consult authoritative Fluss state (point lookup or state feed) → Fluss says seen → reject; Fluss says unseen → tested durable Fluss insert/upsert → accept. Fluss wins on any cache-vs-Fluss disagreement — "cache says unseen" never accepts against Fluss authority. The hot path is cache-first (no per-tick Fluss round trip); Fluss point lookups are for cache misses, recovery, and explicit state verification.

**Bounded cache rules (hard):** (1) the cache is never authoritative; (2) it has explicit maximum-entry and/or maximum-byte bounds independent of Fluss dedup cardinality, the 5-minute TTL, and instrument count; (3) cache eviction is a performance optimization and never determines logical duplicate status; (4) the cache never becomes a complete mirror of the Fluss table.

**Fluss authority rule (hard):** for every domain marked Fluss-authoritative, Fluss is the authority for duplicate/correctness determination. Flink state for that domain is bounded, derived, and rehydratable.

**Normal restart (routine):** restore the compact Flink checkpoint → verify Fluss authoritative-state availability and compatibility (startup preflight, extended from `preflightTableContracts` to the dedup KV table) → rehydrate only the required working state from Fluss (dedup cache; candles/signals already Fluss-owned) → resume from recovered source offsets. A normal restart SHALL NOT replay complete `raw_table_1` history to rebuild durable state that already exists in Fluss.

**Exceptional rebuild (controlled, not routine):** Fluss state missing/corrupt/incompatible/unavailable → fail closed or stay degraded (never treated as an empty state) → controlled bounded `raw_table_1` replay within the dedup TTL horizon → reconstruct the Fluss authoritative state → verify state/schema compatibility before resume. This is exceptional recovery, not a normal restart path.

**Open-window candle recovery:** if Flink crashes before the current 15-second window is finalized, the compact checkpoint restores the active window accumulator, timer context, and working data; the window continues and its final candle is written once at finalization. No full historical candle replay, and no historical candles are retained in Flink.

**Checkpoint invariant (hard):** checkpoint size scales with bounded working/recovery state — NOT the total cardinality of Fluss-authoritative durable state. The post-externalization size is measured, not asserted; no invented number replaces the old ~1 GB evidence.

**Anti-regression rule (hard):** no new Flink managed-state structure may be introduced for a Fluss-authoritative domain unless explicitly classified as bounded transient working state with an explicit entry/byte bound. "MapState is easier" is not a valid reason to re-duplicate durable state in Flink.

**Scope freeze:** Ranking and Reservation architecture (`Ranking_Results`, `Portfolio_Reservations`, portfolio capacity, ranking/reservation state and recovery, portfolio repartitioning) is UNCHANGED by DEC-038 and explicitly OUT OF SCOPE for this state-ownership refinement.

### Design — `fingerprint_dedup` dedup state table (DEC-038)

**Status:** design fixed; DDL + pins + pure core implemented 2026-08-15, live writer wiring remains. **SUPERSEDED SAME-DAY (2026-08-15): the live writer wiring LANDED — see the "Implemented 2026-08-15 (DDL + pin + pure core + LIVE WIRING)" block directly below; this status line was not updated when the final wiring session landed.** This section is the design stage of DEC-038 (tracker 14 P11.0): proposed DDL, buckets, expiry/cleanup mechanism, cache bound, and write cadence. It fixes the shape so implementation can proceed; every value marked *starting point* is a measurement target (DEC-038 §9), not an asserted bound. Items that depend on exact Fluss 0.9.1 connector behavior are marked **evidence-gated**.

The table is **authoritative durable state** — a Fluss state table, not an output table. The Signal job is its single writer owner. Logical semantics are pinned by the canonical contract above and by `04_contracts/02-storage.md` §Dedup state table contract; this section is the physical design.

**Implemented 2026-08-15 (DDL + pin + pure core + LIVE WIRING):** `24_fingerprint_dedup.sql` v1 is manifest-listed (`schema_manifest.json`, 24 tables) with the 6-column layout below (PK `(instrument_token, fingerprint_version, event_fingerprint)`, `bucket.key=instrument_token`, 16 buckets, `kv.format-version=2`), pinned cross-boundary by `FingerprintDedupTableColumns` + `FingerprintDedupTableColumnsAgreementTest`; `DedupExpiry` (logical TTL + bounded re-entrant cleanup selection) and the `DEDUP_STATE_TABLE` / `DEDUP_CACHE_*` / `DEDUP_WRITE_*` / `DEDUP_CLEANUP_INTERVAL_MS` config keys are unit-tested. **Live wiring (2026-08-15 final session):** `FingerprintDedupStateStore` SPI + in-memory/raw-client twins (the raw client works on this table — v2 + single-field-subset bucket key, COMPAT-FLUSS-005); `FingerprintDedupFunction` is now cache-first with the AUTHORITATIVE store on cache miss (query-on-miss lazy rehydration — a cold restart is correct, never an empty dedup set, SIG-STATE-003), a bounded cache (`DEDUP_CACHE_MAX_ENTRIES`/`DEDUP_CACHE_MAX_BYTES`, earliest-expiry eviction), first-seen rows emitted to the `fingerprint-dedup-write` side output, and grid-aligned processing-time cleanup timers (zero added keyed state) driving bounded `scanExpired`+`delete` on `DEDUP_CLEANUP_INTERVAL_MS`; `FingerprintDedupWriterFunction` batches the durable upsert on `DEDUP_WRITE_BATCH_MS`/`DEDUP_WRITE_BATCH_SIZE`; `validateFingerprintDedupTable` is ALWAYS-ON in `SignalJob.preflightTableContracts`. The Fluss reader for the SCH-24 bundle source is the documented integration. **Live evidence landed 2026-08-15 (externalization benchmark, SIG-STATE-001/002 + SIG-PERF-001):** the run exercised write/hydration/cleanup against the live cluster and exposed two cleanup bugs that are now fixed — (1) `scanExpired` bucket scoping (`token % n` ≠ Fluss's `KeyEncoder`+`BucketingFunction` assignment; fix = `DedupBucketAssigner`, verified predicted==actual buckets on the live cluster), and (2) `UpsertWriter.delete` full-row requirement. Delete/ack semantics on the read path are now evidenced (deleted keys → `NOT_SEEN`; plateau = live set).**

```sql
-- fingerprint_dedup: authoritative dedup state — one row per accepted fingerprint
--   within its logical TTL (DEC-038)
-- Owner: Signal job
-- Type: KV state table (PK instrument_token, fingerprint_version, event_fingerprint)
-- Bucket key: instrument_token (PK prefix — per-instrument colocation; the Fluss
--   connector requires bucket.key ⊆ primary key)
-- Retention: table.log.ttl = 2d bounds ONLY the underlying log; the logical dedup
--   lifetime is the column-based expiry (DEDUP_TTL_MS = 300000), enforced by the
--   writer + cleanup pass — never the log TTL
-- Lake: none — transient state (logical life ≤ 5 min); no EOD/audit value; avoids
--   lake churn at the write rate (datalake disabled, like forming_bar)
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE fingerprint_dedup (
    instrument_token     BIGINT      NOT NULL,
    fingerprint_version  STRING      NOT NULL,
    event_fingerprint    STRING      NOT NULL,
    first_seen_ms        BIGINT      NOT NULL,
    expiry_ms            BIGINT      NOT NULL,
    schema_version       STRING      NOT NULL,
    PRIMARY KEY (instrument_token, fingerprint_version, event_fingerprint) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '2d'
);
```

Design notes (mapping to REQ-FLS-017's eight pre-DDL design items):

- **Owner (item 1):** the Signal job is the single writer; provisioned only by the offline `make ddl` gate (manifest path) — a runtime bootstrap must never create it (same rule as all compute tables).
- **Keys (item 2):** PK `(instrument_token, fingerprint_version, event_fingerprint)` with `bucket.key=instrument_token` ⊆ PK — per-instrument colocation; key holds only identity; value holds only `first_seen_ms` + `expiry_ms` (SIG-UNIT-008) — no raw bytes, decoded fields, candle/candidate values, or event objects.
- **Update semantics (item 3):** first-seen insert/upsert; replay re-upserts the same key with identical `(first_seen_ms, expiry_ms)` — last-write-wins, no row growth, converges (same mechanism as `feature_candles_15s`).
- **TTL/cleanup (item 4):** `expiry_ms = first_seen_ms + DEDUP_TTL_MS` exactly; entries are never logically expired early; cleanup pass below (Fluss 0.9.1 has no per-key TTL).
- **Rebuild source (item 5):** `raw_table_1` replay within the dedup TTL horizon — **exceptional controlled rebuild only** (Fluss state missing/corrupt/incompatible), never a normal restart path.
- **Versioning (item 6):** `schema_version=1` column + the connector's RowData serialization of the 6-column schema; a schema change is additive-only or blocks before unsafe use (STATE-COMPAT-001, VM-FLUSS-SRV-005 row 5b).
- **Restart behavior (item 7)** and **consistency (item 8):** restart/rehydration and cache-vs-Fluss authority sections below.
- Buckets: 16, `bucket.key=instrument_token` — the per-instrument colocation pattern of `feature_candles_15s` / `Signal_Candidates_current`; *starting point* — bucket count is workload-tested configuration (REQ-FLS-004), not a copied assumption.
- Manifest entry (proposed): `table_kind=KV`, `primary_key="instrument_token, fingerprint_version, event_fingerprint"`, `bucket_key="instrument_token"`, `validated_matrix=VM-FLUSS-SRV-005` — regenerated by `ddl_apply.py --force`, never hand-edited.

**Read/write path:**

- Hot path (per tick): bounded cache lookup only — no Fluss round trip. Cache miss → Fluss point lookup by PK (evidence-gated: exact `lookupBy` semantics on the pinned connector) → seen → reject; unseen → buffered first-seen insert → accept.
- Durable writes: batched/async — buffered first-seen entries flush on the write cadence/batch size and on the checkpoint barrier, protected by the `StallGuardedSink` watchdog precedent (bounded flush, terminal failure on stall — a hung dedup table must fail the job, not hang it). "Fluss write uncertain" never claims first-seen success until the tested durable ack semantics are satisfied (evidence-gated; SIG-STATE-001/002).
- Fluss wins on any cache-vs-table disagreement; "cache says unseen" never accepts against the table.
- Cleanup: periodic pass on `DEDUP_CLEANUP_INTERVAL_MS` issues bounded batched key-deletes for rows with `expiry_ms` past (idempotent and re-entrant; exact KV delete/ack semantics evidence-gated). Growth is bounded: entries = accepted rate × TTL horizon.

**Cache bound and write cadence — new config keys (defined here; the config contract previously deferred them).** All are tuning keys — defaulted and validated at startup (reject missing/≤0), NOT `requirePinned` (pinned correctness keys are `DEDUP_TTL_MS` etc.). *Starting values are initial guesses to be validated by the externalization benchmark; the effective cache cap is min(entries, bytes).*

| Key | Default (starting point) | Meaning / rule |
| --- | --- | --- |
| `DEDUP_STATE_TABLE` | `fingerprint_dedup` | Dedup KV state table name; validated at startup by the extended preflight (schema v1, PK exact, 16 buckets, `bucket.key=instrument_token`) — `validateFingerprintDedupTable` implemented + unit-tested 2026-08-15, hooked into the preflight with the live writer wiring |
| `DEDUP_CACHE_MAX_ENTRIES` | 250000 | Hard entry bound of the working cache; independent of Fluss cardinality, the 5-min TTL, and instrument count; never a mirror of the table |
| `DEDUP_CACHE_MAX_BYTES` | 33554432 (32 MB) | Hard byte bound; the effective cap is min of the two |
| `DEDUP_WRITE_BATCH_MS` | 250 | Durable-write cadence: first-seen entries flush at most every N ms (and on the checkpoint barrier) |
| `DEDUP_WRITE_BATCH_SIZE` | 5000 | Durable-write batch size: flush when N dirty entries accumulate |
| `DEDUP_CLEANUP_INTERVAL_MS` | 60000 | Expired-row cleanup pass cadence |

Rules: (1) cache eviction is a performance optimization and never determines logical duplicate status; (2) a re-arriving fingerprint inside its logical TTL dedupes even if evicted — Fluss is the authority; (3) bounds are validated so the hot-path hit ratio stays high at the envelope while the cache never approaches Fluss cardinality — proven by the SIG-PERF-001 cache-hit-ratio row, not asserted.

**Expiry/cleanup mechanism (evidence-gated):** Fluss 0.9.1 has no per-key TTL, so logical expiry is writer-enforced: (1) the write path stores `expiry_ms = first_seen_ms + 300000`; (2) the read path treats a row as a duplicate iff the key exists AND `expiry_ms` is in the future — stale rows are harmless, never a false "seen" after expiry; (3) the cleanup pass deletes rows with `expiry_ms < now` in bounded batches on `DEDUP_CLEANUP_INTERVAL_MS`; (4) exact connector delete/ack semantics are evidence-gated — the mechanism is designed, implemented, and measured (cleanup rate + table-size plateau recorded in the externalization benchmark), not assumed.

**Restart and rehydration:** normal restart = compact checkpoint (source offsets, watermarks, timers, in-flight windows, cache metadata) → verify `fingerprint_dedup` availability/compatibility (extended `preflightTableContracts`) → rehydrate the bounded cache from Fluss (bounded read of live entries, `expiry_ms > now`, capped by the cache bound) → resume. No full `raw_table_1` replay (SIG-STATE-002). The rehydration read semantics (log-scan vs current-state read) are evidence-gated and their duration is measured (externalization benchmark hydration rows). Fluss table unavailable/incompatible → fail closed / degraded — never an empty dedup set (SIG-STATE-003).

**Sizing (starting points — DEC-038 §9: must be measured, not asserted):**

| Metric | Starting point | Validated by |
| --- | --- | --- |
| Fluss table steady-state entries | first-seen rate × TTL (measured at envelope) | externalization benchmark |
| Flink cache entries | ≤ `DEDUP_CACHE_MAX_ENTRIES` (250000 starting point) | externalization benchmark (hit ratio) |
| Flink checkpoint size | bounded working/recovery state only — re-measured, no invented number | externalization benchmark |
| Cleanup throughput | recorded (rows deleted/s) | externalization benchmark |

**Scope freeze:** this design touches only the dedup state domain. ~~Ranking/Reservation are unchanged and out of scope~~ — **REMOVED 2026-08-15 (CHG-005).**

**Current-phase envelope:** build and validate on the approved 1,024-instrument / single-connection configuration (20,480 ticks/s at 20 Hz per instrument). The 3,000-instrument / 50,000 ticks/s baseline stays deferred (`PERF-PROD-60000-001`, AC-FC-007/011; `PERF-PROD-90000-001` retired with the peak campaign, DEC-036); the budgets below are 3,000-instrument production targets, not this phase's acceptance.

- **DDL bootstrap:** production application of the compute output tables (`feature_candles_15s` KV, `Signal_Candidates` LOG v3 + `Signal_Candidates_current` KV — 2026-08-13 conversion: candle table is KV-only, DDL 03; DDL-22 `feature_candles_15s_current` deleted; ~~`Ranking_Results`, `Trade_Decisions`, `Portfolio_Reservations`~~ **REMOVED 2026-08-15, CHG-005**) stays behind the `make ddl` version gate (`12-version-compatibility-evidence.md`). For dev this is already solved: `DdlBootstrap.ALL_TABLES` registers the full platform table set (21 tables at 2026-08-13 — now 24 after CHG-003/CHG-004), but `ensureTables` creates **only the 3 ingestion-owned tables** (`raw_table_1` with the exact v2 schema, `suspected_discontinuities`, `ingestion_quarantine`); the compute tables are provisioned by the offline DDL gate (`ddl_apply.py` / `schema_manifest.json`) — a runtime bootstrap must never create them. `verifyTables` column-count-checks owned tables and existence-checks the rest. Note the offline-gate precedent used for `Safety_Halt_Requests` v3: datalake properties are skipped on the local cluster (no lake catalog).

### Job graph

```text
Fluss raw_table_1 source
→ schema/validity filter
→ event-time assignment and watermark
→ keyBy(instrument_token)
→ bounded fingerprint dedup
→ forming-bar/window state
├─ final 15-second candle sink            (feature_candles_15s — KV upsert, PK (instrument_token, window_start); sole candle output — 2026-08-13 requirement)
└─ closed-candle signal detection         (Slice 2.1, DEC-034: MVP placeholder rule)
   ├─ Signal_Candidates LOG append sink   (one new row per found signal, never updated — current)
   └─ Signal_Candidates_current KV sink   (current-state upsert PK instrument_token, supersession replaces — current)
   (Slice 2.2, 2026-08-16) typed forming-bar handoff — forming-bar-builder → FormingBar event → forming-bar-detection (mirrored breakout, fire-once-per-window placeholder) → union → same dual-sink
   (2026-08-16) forming-bar KV persistence — builder PERSIST_OUTPUT → forming-bar-writer (coalesce latest/instrument, cadence flush) → forming_bar KV upsert sink (current-state, PK instrument_token, never per-tick history)
   (future) business rules/candidate state (keyed by instrument_token)
   (future) candidate audit sink
   (future) repartition eligible candidates by portfolio_id
   (~~future in-operator ranking/reservation state~~ — REMOVED 2026-08-15, CHG-005)
```

No feature-table read-back, candidate Fluss round trip, or separate feature-compute job exists in MVP. Candidate detection is instrument-keyed. **(Ranking/reservation repartition REMOVED 2026-08-15, CHG-005.)**

MVP SHALL NOT use CEP. No `flink-cep` dependency, CEP operator, CEP job, or `org.apache.flink.cep` import is permitted. Configuration values in section 2 of [`01_plan.md`](./01-foundation.md) SHALL be validated at startup; any deviation SHALL fail job submission.

### Latency budget per operator

Approximate per-tick processing cost at 50,000 ticks/s baseline. Used to diagnose SLO misses (`p99 < 100 ms` trigger-tick-to-commit; `REQ-RNK-006`). Superseded by `PERF-PROD-60000-001`.

| Operator | Per-tick cost | Notes |
| --- | --- | --- |
| Fluss source read | ~0.1-0.5 ms | Deserialization + network from tablet server |
| Changelog filter | ~0.01 ms | Simple row-kind check (INSERT/UPDATE_AFTER only) |
| Row → typed mapping | ~0.05 ms | Field extraction + metric emission |
| Dedup lookup | ~0.01-0.05 ms | Flink working-cache hit (pre-DEC-038 row: "Dedup RocksDB lookup", L1 block cache — superseded; under DEC-038 the hot path hits the bounded in-Flink cache, not RocksDB or a Fluss round trip) |
| Window accumulator update | ~0.005 ms | In-memory hash map OHLCV update |
| Forming-bar detection | ~0.01 ms | Boundary check + typed event construction |
| Strategy evaluation | TBD (keep simple) | Instrument-keyed; fires per forming-bar update |
| **Per-tick hot path subtotal** | **~0.2-0.6 ms** | Everything above fires per tick |
| ~~Ranking burst (per portfolio)~~ | ~~~25-50 ms~~ | ~~Fires ~1×/instrument/15s; heap sort over 50-100 candidates~~ — **REMOVED 2026-08-15 (CHG-005)** |
| Winner commit to Fluss | ~0.5-2 ms | Append acknowledgement from tablet server |

~~The ranking burst is the dominant p99 term because it batches every 15 seconds. Per-tick overhead alone would support 1,000-5,000 ticks/s per core; the bottleneck is ranking, not ingestion or dedup~~ — **REMOVED 2026-08-15 (CHG-005).**

### Suggested operator boundaries

| Operator | Key/state | Responsibility |
| --- | --- | --- |
| `RawValidation` | Stateless | Schema version, validity, event classification |
| `FingerprintDedup` | Instrument + fingerprint scope | Bounded first-seen state and duplicate metrics |
| `CandleAndFormingBar` | Instrument | Event-time window and forming accumulator |
| `BusinessLogic` | Instrument/strategy | Active setup state and candidate emission |
| ~~`RankingAndReservation`~~ | ~~`portfolio_id`~~ | ~~Deterministic scoring, capacity, winner selection, reservation lifecycle~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `AuditAndDecisionSinks` | Connector transaction boundary | Immutable event/decision outputs |

Actual chaining is performance-tested; logical boundaries remain explicit for metrics and state ownership.

**Chaining guidance (starting recommendation from operator cost model):** Keep `RawValidation → FingerprintDedup → CandleAndFormingBar → BusinessLogic` chained — per-record overhead is negligible and a network shuffle provides no benefit for these instrument-keyed operators. Final decision is performance-test driven. **(The `RankingAndReservation` boundary and `AuditAndDecisionSinks` guidance are REMOVED 2026-08-15, CHG-005.)**

### Configuration contract

| Key | Requirement |
| --- | --- |
| `FLINK_VERSION` / `FLUSS_CONNECTOR_VERSION` | Exact compatibility matrix IDs |
| `RAW_SCHEMA_VERSION` | Accepted input version/range |
| `WATERMARK_OUT_OF_ORDER_MS` | Default 5000; change only through tested profile |
| `ALLOWED_LATENESS_MS` | Default 5000; change only through tested profile |
| `SOURCE_IDLE_MS` | Default 15000 per source partition |
| `DEDUP_TTL_MS` | Fixed at `300000` (5 minutes); reject startup for any other value in MVP |
| `DEDUP_STATE_TABLE` (new, DEC-038) | Fluss KV state table name for the authoritative dedup set (`fingerprint_dedup`); validated at startup by the extended table preflight — DDL/manifest entry + the live writer wiring landed 2026-08-15 (24-table manifest, CHG-003/CHG-005; `validateFingerprintDedupTable` ALWAYS-ON in `preflightTableContracts`); **live-cluster measurement landed 2026-08-15 (externalization benchmark, SIG-STATE-001/002 + SIG-PERF-001)** — bucket scoping fixed via `DedupBucketAssigner` (Fluss's `KeyEncoder`+`BucketingFunction` assignment, not `token % n`) and delete full-row contract fixed |
| `DEDUP_CACHE_*` (new, DEC-038) | Bounded working-cache bound and write cadence for the Flink side — keys and *starting* values defined in §Design — `fingerprint_dedup` dedup state table (tuning keys, validated at startup, re-derived from measurement) |
| `CANDLE_WINDOW_MS` | Fixed at `15000`; reject startup for any other value in MVP |
| `CANDLE_TABLE` | Candle KV sink table, default `feature_candles_15s` (sole candle output — KV upsert, PK `(instrument_token, window_start)`, converted 2026-08-13) |
| `CANDLE_CURRENT_TABLE` | **DELETED 2026-08-13** — key and `candleCurrentTable` config field removed with the candle KV projection; the fail-closed startup mode it governed (`ALLOW_FULL_REPLAY` / `STATE_RECOVERY_PATH`) is unchanged |
| `SIGNAL_CANDIDATES_TABLE` | Signal LOG sink table, default `Signal_Candidates` — append-only, one row per fired signal |
| `SIGNAL_CURRENT_TABLE` | Signal KV current-state table, default `Signal_Candidates_current` — PK `(instrument_token)`, 16 buckets, `bucket.key=instrument_token`, 22-column signal row contract; enforced at startup by `TableContractValidator` (SIGNAL-SCHEMA-001) |
| `STATE_RECOVERY_PATH` | Required for normal restarts (checkpoint/savepoint dir). Absent → startup fails closed unless `ALLOW_FULL_REPLAY=true` (CANDLE-KV-REPLAY-001 startup gate) |
| `ALLOW_FULL_REPLAY` | Explicit break-glass for deliberate offset-0 replay. **Never `true` in a normal production launch**; replay without restore is what caused the 2026-08-10 incident |
| `CHECKPOINT_INTERVAL_MS` | Fixed at `10000`; Signal and Babysitter jobs use this value |
| `CHECKPOINT_TIMEOUT_MS` | Fixed at `30000`; Signal and Babysitter jobs use this value |
| `MAX_CONCURRENT_CHECKPOINTS` | Fixed at `1`; Signal and Babysitter jobs use this value |
| `STATE_BACKEND_TO_BE_PINNED` | Version-compatible managed state backend |
| `S3_CHECKPOINT_URI_TO_BE_DEFINED` | Production encrypted checkpoint/savepoint storage |
| `STRATEGY_VERSION` | Included in candidates/decisions |
| ~~`RANKING_VERSION`~~ | ~~Included in ranking results/decisions~~ — **REMOVED 2026-08-15 (CHG-005)** |
| ~~`RESERVATION_POLICY_VERSION`~~ | ~~Included in audit and restored state~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` | Fixed at `1`; do not forward another active candidate for that instrument |
| `CHECKPOINT_RESTART_STRATEGY` | Fixed-delay: `RESTART_MAX_ATTEMPTS=3`, `RESTART_DELAY_MS=30000`, failure action = fail job. Deployment SHALL reject unbounded retry. **Production-pinned 2026-08-15:** in `DEPLOYMENT_ENV=production` both keys must be present and exactly equal the `PlatformConfig` pins (`RESTART_MAX_ATTEMPTS=3`, `RESTART_DELAY_MS=30000`) or startup fails — a deployment cannot silently raise the retry budget; dev keeps them as overridable tuning defaults (failure-injection integration tests use low attempts). |

Deployment SHALL reject unbounded or too-short `DEDUP_TTL`, missing production checkpoint storage, unbounded checkpoint restart retry, and any deviation from pinned values.

### Event-time contract

- Event time is the verified UTC broker timestamp.
- Events without verified event time do not advance watermarks.
- Watermark and idleness apply per source partition.
- One final candle emits at first window fire (watermark ≥ `window_end`). The `emitted` window-state flag makes any allowed-lateness re-trigger a no-op (late-within-lateness folds into the accumulator and is counted, never re-written); the row is final from its first write. (The "emits after `window_end + allowed_lateness`" phrasing in the contract means the finalization boundary — the candle is not corrected after that point.)
- Later records (beyond `window_end + allowed_lateness`) are dropped by the window operator to the `CandleLateDrop` side output and counted by the `compute.candles.late.dropped` counter carrying instrument/window-end/lateness/reason (REQ-FC-006, implemented 2026-08-15 — see Required telemetry below).
- Open/close ties use the versioned deterministic fingerprint ordering.
- Empty windows emit no row.

### Dedup state

Dedup set contents: key contains only fingerprint version, scope (instrument token), and event fingerprint; value contains only first-seen timestamp and expiry timestamp. Dedup state SHALL NOT contain raw bytes, decoded raw fields, candle values, candidate values, or an event object. The logical expiry instant is `first_seen + TTL` (`300000` ms) exactly; entries are never deleted early.

**Ownership (DEC-038, 2026-08-14):** the authoritative dedup set is a **Fluss KV state table** — key `(instrument_token, fingerprint_version, event_fingerprint)`, value `(first_seen_ms, expiry_ms)`, `bucket.key = instrument_token` — owned by the Signal job and rebuildable from `raw_table_1` replay within the TTL. Flink keeps only a **bounded working cache** for hot-path lookups (no Fluss round trip per tick — performance rule below). This supersedes the pre-DEC-038 Flink `MapState` layout described in the historical Slice-1 rows.

**Expiry (superseded Flink-2.2.1 mechanism, kept as history):** event-time state TTL was removed in Flink 2.2.1 (only `ProcessingTime` remains in `StateTtlConfig.TtlTimeCharacteristic`), so the old Flink-side design enforced expiry with explicit event-time timers (one per fingerprint) plus an expiry index (`expiry → state keys`). Under DEC-038 the dedup lifecycle moves to the Fluss table (expiry column + a tested cleanup path — Fluss 0.9.1 has no per-key TTL, so the mechanism must be designed and measured, not assumed). The Flink cache's own entries expire so a re-arriving fingerprint inside the TTL still dedupes (pinned by test).

First event proceeds; later candidate within TTL increments duplicate metrics and does not affect candle/business state.

Identical legitimate events may be collapsed; this limitation must remain visible in metrics and documentation.

**Performance rule (§7 of the DEC-038 requirement):** the hot path must not become `tick → Fluss read → Flink → Fluss write` per tick. Durable dedup writes are batched/async; lookups hit the bounded Flink cache; the design specifies cache bound, write cadence, and cleanup, and is measured.

### Dedup state budget (HISTORICAL — pre-DEC-038, superseded)

> This budget sized the ~1.74 GB RocksDB/checkpoint footprint DEC-038 removes. The target model is [DEC-038 State Ownership and Recovery Contract](#dec-038-state-ownership-and-recovery-contract); the post-externalization Fluss-side envelope and Flink cache size are **measured**, not asserted.

At the 50,000 ticks/s baseline workload (3,000 instruments; ≈16.7 ticks/s/instrument average) — **pre-externalization, superseded**:

| Metric | Value | Derivation |
| --- | --- | --- |
| Steady-state entries | ~18,000,000 | 50,000 ticks/s × 300s TTL |
| Raw state size (per entry ~32 bytes) | ~576 MB | Fingerprint + first-seen timestamp + expiry timestamp |
| Expiry index (`expiry → key list`) | ~+100-150% of raw state | One list entry per live fingerprint; bounded with the dedup state itself (measured on the 1,024-instrument dev envelope: dedup state ~2 MB total) |
| RocksDB overhead (LSM amplification) | ~1.7-2× | Block index, bloom filter, SST metadata |
| Estimated total state | **~1.3 GB** | Plateaus after warmup; does not grow unbounded |

Measured reference points (pre-externalization): RocksDB total state **1.74 GB** at 53k t/s on the 1,024-token bench (2026-08-12, 11-testing-and-release.md §14.1); dedup ballooned to **1.1 GB** during the 2026-08-10 full-replay incident; dev-hashmap checkpoints **986 MB / 22 s** (2026-08-14 E2E, blew the 30 s budget). These are the duplication this change eliminates — they are evidence for *why*, not a bound on the target. The post-externalization Fluss-side envelope and the Flink cache size **must be measured**, not asserted (DEC-038, §9 rule).

### Candle accumulator

Per instrument/window state SHALL contain only:

```text
first_order_key, open_price
last_order_key, close_price
high_price
low_price
volume
accepted_tick_count
window_start, window_end
algorithm/config version
```

Active candle state SHALL NOT contain a list, collection, array, or map of individual ticks. Window state (accumulator + `emitted` flag) is deleted by Flink's window cleanup when the watermark passes `window_end + allowed_lateness`; the final candle row has already been written at first fire and is never corrected.

Order key is `(event_time, deterministic_fingerprint_order)`. Price and quantity validation occurs before aggregation. Overflow/invalid numeric behavior is explicit and tested.

### Forming-bar and candidate interface

Typed in-process update includes:

- Instrument and routing identity
- Window boundaries
- Current OHLCV/tick count
- Trigger event time/fingerprint
- Strategy configuration version
- Source schema and manifest version

Business Logic maintains active setup state and emits one immutable candidate audit record per detected setup. Same candidate identity cannot change content.

### Ranking and reservation protocol — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** There is no ranking/reservation protocol in the current system.

Default MVP capacity:

- At most one reserved/open trade per instrument.
- At most three total reserved/open positions.
- At most one per strategy.

~~`RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` consume capacity. Reservation state uncertainty suppresses new decisions~~ — **REMOVED 2026-08-15 (CHG-005).**

A same winner with unchanged executable content produces audit only. Changed executable parameters require a new `instruction_id` and supersession relation.

### Output consistency

One Flink checkpoint covers job state and sinks, but cross-table atomic visibility is not assumed. Every output carries stable IDs and versions so consumers can tolerate partial visibility and reconcile.

~~`Trade_Decisions` is immutable. Executor never mutates it~~ — **REMOVED 2026-08-15 (CHG-005).**

### Restore and degradation

**Restore (DEC-038):** the job restores its **compact Flink checkpoint** (source offsets, watermarks, event-time timers, in-flight windows, dedup working-cache metadata), then **verifies Fluss authoritative-state availability and compatibility** (extend the existing `preflightTableContracts` pattern to the dedup KV table), then **rehydrates only the working state it needs** (dedup cache from the Fluss dedup table; signal ring buffers from `feature_candles_15s` if not checkpointed). No full raw-history replay is required merely to reconstruct durable hot state.

Restore must recover source offsets, windows, forming bars, active candidates, **and rehydrate the dedup cache from Fluss**. **(Ranking/reservation restore REMOVED 2026-08-15, CHG-005.)** If state compatibility or continuity cannot be proven:

1. Job reports not ready/degraded.
2. New decisions are suppressed.
3. Executor is signalled to halt.
4. Operator reconciliation/savepoint policy is invoked.

If the Fluss dedup table is unavailable or incompatible, the job **fails closed** (or stays degraded) rather than silently replaying with an empty dedup set — the existing `STATE_RECOVERY_PATH`/`ALLOW_FULL_REPLAY` startup gate is preserved and extended to cover Fluss state verification.

### Startup mode gate (CANDLE-KV-REPLAY-001 — shared machinery, unchanged by the 2026-08-13 re-scope; the gate protects the signal LOG + KV sinks too)

Normal restarts SHALL pass `STATE_RECOVERY_PATH` (a Flink checkpoint/savepoint dir); the job then restores offsets and state. If `STATE_RECOVERY_PATH` is absent the job fails closed at startup with an explicit mode error — unless the operator deliberately sets `ALLOW_FULL_REPLAY=true` (documented break-glass; logs and emits `compute.startup.mode` = `FULL_REPLAY`). RESTORE mode emits `compute.startup.mode` = `RESTORE`. The two modes are mutually exclusive and never defaulted to replay. This gate is what turns the 2026-08-10 incident's no-restore restart from a silent replay into a refused startup.

Completed checkpoints are externalized with `RETAIN_ON_CANCELLATION` (set in `SignalJob.buildTopology`), so a deliberate stop keeps the exact checkpoint named by the next `STATE_RECOVERY_PATH`; the default delete-on-cancel would silently invalidate the restore contract (P6.1 phase 2→3 verifies cancel preserves the `chk-N` directory and the restore resumes from it).

### Checkpoint sizing (HISTORICAL — pre-DEC-038, superseded)

> This ~600 MB – 1 GB estimate is exactly the duplicate-copy footprint DEC-038 removes; the target model is [DEC-038 State Ownership and Recovery Contract](#dec-038-state-ownership-and-recovery-contract). The post-externalization checkpoint size is **measured, not asserted** — §9 of the requirement forbids replacing "1 GB" with an invented number. Measurement target rows: checkpoint size/duration at the current 1,024-instrument envelope and the deferred 50k baseline.

Estimated checkpoint metrics at the 50,000 ticks/s baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) — **pre-externalization, superseded**:

| Metric | Estimate | Derivation |
| --- | --- | --- |
| Checkpoint size (steady state) | ~600 MB – 1 GB | Dominated by dedup state (~1 GB); window state ~120 KB (3K instruments × ~40 bytes); ~~candidate/ranking state ~5 MB~~ (**REMOVED 2026-08-15, CHG-005**) |
| Checkpoint write time | ~2-5 seconds | SSD write at ~500 MB/s; incremental checkpoints write only changed SST files |
| Restore time | ~5-15 seconds | Read back RocksDB state from S3/local checkpoint; well within 30s data-path recovery target (REQ-FC-008) |

Measured pre-externalization reference: dev-hashmap checkpoints 986 MB / 22 s at the 20,480 t/s envelope (2026-08-14 E2E) — the concrete failure (CP9 expired, 30 s budget) that motivated DEC-038. The configured 10s interval / 30s timeout contract is unchanged; its headroom after externalization is re-derived from measurement.

### Job submission contract

- Build one versioned job JAR.
- Upload once.
- Submit Signal job with explicit entry class/args.
- Submit Babysitter separately.
- Capture job IDs and artifact checksum.
- Treat repeated submission idempotently.
- READY requires both required jobs RUNNING and checkpointing.

### Required telemetry

Source throughput/lag, invalid events, dedup candidates/hits/state size, late events, watermark lag, window/candle rates, forming updates, candidate rates, operator busy/idle/backpressure, sink latency, checkpoint duration/size/failures, restore count, and state compatibility failures. **(Ranking/decision rates and reservation states/conflicts REMOVED 2026-08-15, CHG-005.)**

**Implemented in Slice 1** (per-operator counters): `compute.invalid.rows` + `compute.invalid.byReason.*` (RawValidationFunction), `compute.dedup.first` / `compute.dedup.duplicates` (FingerprintDedupFunction), `compute.candles.emitted` / `compute.candles.late.updates` (CandleEmitFunction). Checkpoint duration/size/failures come from Flink's built-in checkpoint metrics.

**Pending (Slice 2+ telemetry):** source throughput/lag, watermark lag, forming-update/candidate rates, sink latency, restore count, state compatibility failures. **(Ranking/decision rates and reservation states REMOVED 2026-08-15, CHG-005.)**

**Implemented 2026-08-15 (REQ-FC-006):** the beyond-lateness discard counter — the candle window now routes late drops to a side output (`.sideOutputLateData`) counted by a dedicated counter operator: `compute.candles.late.dropped` (Flink counter + OTLP DELTA sum) carrying the LATEST drop's instrument/window-end/lateness/reason as one bounded attribute set (`CandleLateDrop`, wired after the `candle-15s` UID; `SignalJobOperatorUidTest` updated).

**DEC-038 state-ownership telemetry (2026-08-14):** prove the architecture behaves as intended — Flink checkpoint size/duration/failure (existing), **Fluss dedup-table state size** (row/entry count + bytes), **Fluss dedup update rate**, **dedup cache hit ratio**, **rehydration latency**, **rehydration failures**, **state compatibility failures** (preflight on the dedup table), and **state continuity failures**. Bounded cardinality: per-table gauges and per-reason counters, no per-key labels.

**Implemented 2026-08-15:** the in-Flink half of the DEC-038 telemetry — **dedup cache hit ratio** (`compute.dedup.cache.hits` / `compute.dedup.cache.misses` DELTA sums + `compute.dedup.cache.hit.ratio` gauge in integer basis points, computed over the drained window), **rehydration latency** (`compute.dedup.rehydration.latency.ms` last-lookup gauge in `FingerprintDedupFunction`, timed around the query-on-miss store read), and **rehydration failures** (`compute.dedup.rehydration.failures` DELTA — counted before the task fails closed, SIG-STATE-003). **Still pending (benchmark, Phase A item 3):** Fluss-side dedup-table state size/update rate and the hydration-latency evidence rows (measured live, not asserted). **RESOLVED 2026-08-15 — the externalization benchmark recorded these rows live against the dev cluster: durable write throughput (~9 upserts/s shared writer), cold-restart hydration (15/15 SEEN_LIVE, avg 102 ms/lookup), and bounded cleanup (15/15 stale rows deleted, plateau = live set) — see the dossier banner and tracker 14 P11.3.**

**CANDLE-KV-REPLAY-001 observability (deferred through existing telemetry):** dedicated per-sink counters (LOG rows written vs KV upserts, KV duplicate/conflict count, replay-vs-live output_ts gap) are **deferred** — they are not implemented as new metrics. Existing telemetry covers the operational need: `compute.startup.mode` gauge (RESTORE/FULL_REPLAY), `compute.candles.emitted` / `compute.candles.late.updates` counters, Flink built-in checkpoint duration/size/failure metrics, and offline `CandleMigrationTool` audits for LOG-vs-KV convergence checks. A dedicated KV-replay metric can be added later without contract change. (2026-08-13 conversions: the candle table is now KV-only — its LOG-vs-KV convergence surface is gone; convergence/upsert observability for the signal tables — `Signal_Candidates` LOG appends vs `Signal_Candidates_current` KV unique keys, signal-sink `numRecordsIn` + LOG:KV ratio; `CandleMigrationTool` audits retire with the candle KV projection. Dashboard update tracked in tracker 14 P8.4.)

### Required tests

- `SIG-UNIT-001` deterministic fingerprint tie ordering.
- `SIG-UNIT-002` candle aggregation and empty windows.
- `SIG-UNIT-003` dedup TTL validation (exactly 300000 ms; reject other values).
- `SIG-UNIT-004` candidate identity/supersession.
- ~~`SIG-UNIT-005` deterministic ranking/tie-break~~ — **REMOVED 2026-08-15 (CHG-005)**
- ~~`SIG-UNIT-006` reservation capacity transitions~~ — **REMOVED 2026-08-15 (CHG-005)**
- `SIG-UNIT-007` no `flink-cep` dependency or `org.apache.flink.cep` import.
- `SIG-UNIT-008` dedup state contains only fingerprint + timestamps; no raw bytes or event objects (DEC-038: applies to the Fluss table AND the Flink working cache; the Flink checkpoint does not duplicate the set).
- `SIG-UNIT-009` active candle state contains only OHLCV fields; no tick list/collection.
- `SIG-HARNESS-001` out-of-order/watermark/idleness.
- `SIG-HARNESS-002` late-before-final versus late-after-final.
- `SIG-HARNESS-003` checkpoint/restore deterministic replay.
- `SIG-INT-001` pinned Fluss source/sink boundary.
- `SIG-INT-002` partial output visibility/reconciliation.
- `SIG-FAIL-001` checkpoint/state-continuity safe halt.
- `SIG-PERF-001` per-instrument workload envelope and p99 decision latency.

### Cross-boundary pin habit (DDL ↔ code ↔ wire)

Any change that crosses a module boundary SHALL be pinned by a test on **both** sides of the boundary, so a one-sided edit cannot silently drift. This repo's applied instances of the rule:

| Boundary | Left-side pin | Right-side pin |
| --- | --- | --- |
| Raw packet → persisted label | `TickPacketSchemaVersionTest` (ingestion module): builder label `String.valueOf(packet.schemaVersion())` equals shared `PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION` | `RawValidationFunctionTest` (compute): gate rejects `schema_version != 2` at runtime |
| DDL → Java row layout | `RawTable1DdlSchemaVersionTest` asserts DDL file column count/names/order against `RawTableColumns` field indexes | `SchemaAgreementTest` (ingestion module) asserts DdlBootstrap descriptors match the same DDL files — both sides converge on the manifest |
| Java row layout → candle sink | `CandleTableColumns` field indexes mirror `feature_candles_15s` DDL | `CandleAggregateFunctionTest` asserts OHLCV/volume semantics of the emitted row |
| Candle DDL → KV sink | `DdlBootstrapSchemaAgreementTest` / `CandleTableColumnsAgreementTest` assert DDL 03 (KV) matches the 15-column code schema with PK exactly `(instrument_token, window_start)` (HISTORICAL — the pre-conversion LOG+KV pair contract `CandleCurrentDdlContractTest` + `22_feature_candles_15s_current.sql` is deleted 2026-08-13) | `TableContractValidatorTest` asserts live candle-table metadata preflight: PK exactly `(instrument_token, window_start)` (rejects no-PK, narrower, wider), schema v2, 16 buckets, `bucket.key=instrument_token` |
| Candle versioning → KV upsert | `CanonicalCandlePolicyTest` asserts `(schema_version, algorithm_version, configuration_version)` canonical check at the candle boundary | `SignalCurrentKvIdempotencyTest` (env-gated) proves the signal KV table converges to one row per instrument, last-write-wins (2026-08-13; the candle KV idempotency proof retired with `CandleCurrentKvIdempotencyTest`) |
| Config → job behavior | `SignalJobConfigTest` asserts pinned values and reject-on-deviation | `SignalJob` reads the same config object; restore path pinned by `honorsStateRecoveryPathOverride` |

Rule of thumb: when you change a producer format (Go packet, DDL file, `RawTableColumns`, `CandleTableColumns`, a pinned config key), update the consumer-side pin test in the **same change**, not as a follow-up. One-sided changes fail CI or the live gate rather than drift silently. This is the documented habit for the process-rule pass (2026-08-10) and is the pattern `RawTable1DdlSchemaVersionTest` establishes.

### JVM and memory configuration

- Java max heap SHALL equal 65% of the container memory limit (`JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT=65`).
- Container limit minus Java max heap SHALL be at least 35% (`NON_HEAP_MEMORY_RESERVE_PERCENT=35`).
- Critical alert at or above 85% total container memory (`CONTAINER_MEMORY_ALERT_PERCENT=85`).
- Verify at startup that the container memory limit minus maximum heap is at least 35% of the container memory limit.

### Concrete sizing (48 GB VM) — pre-DEC-038, superseded

> This table sized a Flink TaskManager around ~1.3-1.74 GB of RocksDB dedup state — exactly the large Flink-side footprint DEC-038 removes. Do not reuse these numbers as the target; the post-externalization Flink-side memory split is **re-derived from measurement** (see [DEC-038 State Ownership and Recovery Contract](#dec-038-state-ownership-and-recovery-contract)). The 48 GB VM allocation and the 8 GB / 30 GB split are retained only as the pre-change baseline.

Derived for a Flink TaskManager on a 48 GB VM. All numbers are starting points, not measured — superseded by `PERF-PROD-60000-001` and, for the Flink-side split, by post-DEC-038 measurement.

For a 48 GB container memory limit:

| Resource | Value | Notes |
| --- | --- | --- |
| Container memory limit | 48 GB | Explicit Swarm/Compose limit |
| Java max heap (`-Xmx`) | **8 GB** | Modest because working state lives in RocksDB (direct memory), not heap — pre-DEC-038 rationale; re-derive after externalization |
| Direct memory (`-XX:MaxDirectMemorySize`) | **30 GB** | RocksDB block cache + Flink network buffers + Fluss client buffers — pre-DEC-038 rationale; re-derive after externalization |
| OS reserve | **~10 GB** | OS page cache, off-heap allocations, Fluss client overhead |
| JVM heap percent of container | ~17% | Intentionally lower than the generic 65% rule — RocksDB dominates; pre-DEC-038 rationale |
| GC | `-XX:+UseG1GC -XX:MaxGCPauseMillis=20` | Low pause target to protect p99 latency |
| Container memory alert at 85% | ~40.8 GB | `CONTAINER_MEMORY_ALERT_PERCENT` emits critical at this threshold |

For non-Flink Java containers (Ingestion, Action Capture, Executor), use the generic formula (`JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT=65`). The Flink TaskManager split is different because RocksDB uses direct/native memory for its block cache and SST file buffers, not heap — that reasoning's weight depends on how much RocksDB state remains after externalization.

### Implementation checklist — State compactness (from [`01_plan.md`](./01-foundation.md) Task 3) — DEC-038 re-scope

Before code is accepted, verify each item (items 2-5 are re-scoped by DEC-038: the duplicate state is Fluss-owned; the Flink side holds a bounded working cache):

1. Raw ticks keyed by `instrument_token` before duplicate checking and candle calculation.
2. Duplicate-state **Fluss table** key contains only fingerprint version, fingerprint scope (instrument token), and event fingerprint; the Flink working cache holds only the same key material.
3. Duplicate-state **Fluss table** value contains only first-seen timestamp and expiry timestamp; the Flink cache holds the same compact value.
4. Duplicate state does not contain raw bytes, decoded raw fields, candle values, candidate values, or an event object — in Fluss and in the Flink cache.
5. Duplicate state is removed when its 300000 ms expiry is reached (tested cleanup path; Fluss has no per-key TTL, so the mechanism is explicit and measured).
6. Active candle state contains only open, high, low, close, volume, tick count, first order key, last order key, window start, and window end.
7. Active candle state does not contain a list, collection, array, or map of individual ticks.
8. One final 15-second candle written only after configured watermark/finality rule passes.
9. Active candle state deleted by window cleanup after watermark passes `window_end + allowed_lateness` (final row written at first fire; no correction after finalization).
10. Forming-bar data passed directly to business logic in the same Signal job; not written and re-read through Fluss (REQ-FC-007 preserved; the durable forming-bar KV projection — implemented 2026-08-16 — is Fluss-owned). **(The no-round-trip ranking rationale is REMOVED 2026-08-15, CHG-005.)**
11. `flink-cep` dependency removed from `code/02_services/02_compute/pom.xml`.
12. No CEP API usage, CEP operator, CEP table, or CEP job in MVP.

**DEC-038 additions:** the Flink checkpoint is small and is not a second complete copy of Fluss-owned Signal business state; on restart the job rehydrates the dedup working cache from Fluss; Fluss state unavailability/incompatibility fails closed.

#### Task 3 acceptance checks

- 15-minute tests at the variable 50,000 ticks/s average baseline (3,000 instruments; every instrument ≤30 ticks/s; 90,000 ticks/s peak retired, DEC-036) report no state object containing raw packet bytes or a list of ticks.
- Duplicate state for expired tick fingerprint is absent after its expiry cleanup runs (Fluss-side and cache-side).
- One final candle per non-empty instrument/window and no correction candle after finalization.
- The Fluss dedup table holds the accepted dedup set; the Flink checkpoint does not duplicate it.
- Compute module has no `flink-cep` dependency and no `org.apache.flink.cep` import.

### Implementation checklist — Candidate bounding and in-job ranking (from [`01_plan.md`](./01-foundation.md) Task 4) — **ranking checklist REMOVED 2026-08-15 (CHG-005); retained as historical plan text**

Before code is accepted, verify each item:

1. ~~Before ranking, reject a candidate if its instrument has an active reservation, active open trade, or unchanged active candidate~~ — **REMOVED 2026-08-15 (CHG-005)**
2. Maintain no more than one active candidate per instrument.
3. ~~Emit an audit result for every rejected candidate with a single rejection reason code: `ACTIVE_RESERVATION`, `ACTIVE_OPEN_TRADE`, or `UNCHANGED_ACTIVE_CANDIDATE`~~ — **REMOVED 2026-08-15 (CHG-005)**
4. ~~Send eligible candidates directly to the in-job ranking operator~~ — **REMOVED 2026-08-15 (CHG-005)**
5. ~~Do not use `Signal_Candidates` as an input to ranking~~ — **REMOVED 2026-08-15 (CHG-005)**
6. ~~Do not create a separate ranking deployment or a separate ranking checkpoint boundary~~ — **REMOVED 2026-08-15 (CHG-005)**

#### Task 4 acceptance checks

- Repeated unchanged candidates for one instrument create one active candidate and one audit record per rejected repeat.
- ~~Candidate for an instrument with an active reservation is not sent to ranking~~ — **REMOVED 2026-08-15 (CHG-005)**
- ~~Search results contain no Fluss source/read of `Signal_Candidates` in ranking code~~ — **REMOVED 2026-08-15 (CHG-005)**

### Definition of done

The implementation is complete when exactly one Signal job performs the full path, deterministic replay is proven, state/checkpoint compatibility passes, cross-table limitations are documented and tested, readiness suppresses decisions on uncertainty, and no code/documentation claims external broker exactly-once behavior.

## Verification mapping

The required behavior above is verified by the canonical [Signal job test design](./11-testing-and-release.md#signal-job): `SIG-UNIT-001` to `SIG-UNIT-009`, `SIG-HARNESS-001` to `SIG-HARNESS-005`, `STATE-COMPAT-001`, `SIG-INT-001`, `SIG-INT-002`, `COMPAT-FLINK-001`, `SIG-FAIL-001`, and `SIG-PERF-001`, plus the DEC-038 state-boundary set `SIG-STATE-001` to `SIG-STATE-003` (2026-08-14: large dedup state observable in Fluss, bounded checkpoint, compact-restore + Fluss rehydration, fail-closed on Fluss unavailability). Implemented-test coverage of the SIG-* IDs is mapped in [Slice 1 evidence](#slice-1-evidence-implemented-2026-08-09) below.

## Slice 1 evidence (implemented 2026-08-09)

**Scope:** `raw_table_1` (Fluss LOG source, `OffsetsInitializer.full()`) → `RawValidationFunction` (schema/validity/price/qty gate) → `CandleWatermarkStrategy` (bounded out-of-orderness + idleness) → `keyBy(instrument_token)` → `FingerprintDedupFunction` (bounded first-seen state, event-time expiry timers) → 15 s event-time tumbling window (`CandleAggregateFunction` + `CandleEmitFunction`, emit-once final candles) → `feature_candles_15s` (Fluss KV upsert sink — PK `(instrument_token, window_start)`, sole candle output per requirement 2026-08-13; the Slice-1-era LOG sink and the CANDLE-KV-REPLAY-001 `feature_candles_15s_current` KV sink are HISTORICAL, removed 2026-08-13). Forming-bar handoff and Business Logic are Slice 2; ~~Ranking/Reservations/Decisions are…~~ — **REMOVED 2026-08-15 (CHG-005, not deferred)**

### Files

| File | Responsibility |
| --- | --- |
| `02_compute/.../signaljob/SignalJob.java` | Job topology: source → validation → dedup → 15 s window → sinks (`feature_candles_15s` KV upsert — sole candle output, 2026-08-13 requirement; `Signal_Candidates` LOG append + `Signal_Candidates_current` KV upsert); EXACTLY_ONCE checkpointing (interval/timeout/max-concurrent pinned, `RETAIN_ON_CANCELLATION` externalized retention so the `STATE_RECOVERY_PATH` restore point survives deliberate stops), fixed-delay restart 3 × 30 s (declarative `Configuration`, Flink 2.2.1); `preflightTableContracts(config)` metadata gate + startup-mode gate (RESTORE / explicit FULL_REPLAY). |
| `02_compute/.../signaljob/SignalJobConfig.java` | Pinned-load-bearing config: `DEDUP_TTL_MS=300000`, `CANDLE_WINDOW_MS=15000`, `CHECKPOINT_INTERVAL_MS=10000`, `CHECKPOINT_TIMEOUT_MS=30000`, `MAX_CONCURRENT_CHECKPOINTS=1` (reject any other value); tuning keys defaulted (`WATERMARK_OUT_OF_ORDER_MS=5000`, `ALLOWED_LATENESS_MS=5000`, `SOURCE_IDLE_MS=15000`); `RESTART_MAX_ATTEMPTS`/`RESTART_DELAY_MS` production-pinned 2026-08-15 (`requirePinnedInt`/`requirePinnedLong` in `DEPLOYMENT_ENV=production`, overridable tuning defaults in dev); replay gate keys (`ALLOW_FULL_REPLAY`, `STATE_RECOVERY_PATH` — the `CANDLE_CURRENT_TABLE` key deleted 2026-08-13; the gate keys outlive the candle KV facility they originally pinned). |
| `02_compute/.../signaljob/RawTableColumns.java` / `CandleTableColumns.java` | DDL v2 column layouts (20 / 15 fields) mirrored as field indexes; explicit `InternalTypeInfo` at the candle boundary (no Kryo fallback for RowData). `CandleTableColumns` drives the candle KV upsert sink (sole candle output; the `CandleMigrationTool` it once fed is deleted 2026-08-13). |
| `common/.../schema/CandleTableSchema.java` + `CanonicalCandlePolicy.java` | Shared 15-column candle KV schema contract (PK `(instrument_token, window_start)` NOT ENFORCED; `LOG_TABLE` constant renamed `TABLE` 2026-08-13) and canonical-version check `(schema_version, algorithm_version, configuration_version)` — asserted at the candle KV upsert boundary; signal row contract + canonical strategy/rule policy live at the `Signal_Candidates_current` boundary. |
| `02_compute/.../signaljob/TableContractValidator.java` | Startup preflight: live-table metadata must satisfy schema v2, 16 buckets, `bucket.key=instrument_token`; the candle table's PK must be **exactly** `(instrument_token, window_start)` (KV upsert contract); `Signal_Candidates` must have **no** primary key (append-only LOG — SIGNAL-SCHEMA-001); `Signal_Candidates_current` PK `(instrument_token)` — fail-fast before the job starts. |
| `02_compute/.../signaljob/RawValidationFunction.java` | Row-kind INSERT + `schema_version` pin + `validity_state` VALID-prefix + `last_price_paise > 0` + `last_qty >= 0`; per-reason invalid counters. |
| `02_compute/.../signaljob/CandleWatermarkStrategy.java` | Bounded out-of-orderness on `event_time` + per-partition idleness. |
| `02_compute/.../signaljob/FingerprintDedupFunction.java` | State key `version\|scope\|fingerprint`; value `(first_seen, expiry)`; event-time expiry timers + compact expiry index (`expiry → state keys`) because Flink 2.2.1 removed event-time state TTL; deletion fires when the watermark reaches expiry (never early). |
| `02_compute/.../signaljob/CandleAccumulator.java` / `CandleAggregateFunction.java` / `CandleEmitFunction.java` | Compact OHLCV accumulator (no tick list); trades + quotes contribute OHLC via `last_price_paise`, volume/tick_count only on `TRADE` with `last_qty > 0`; open/close by `(event_time, fingerprint)` order key; emit-once final candle with `emitted` window-state flag (late-within-lateness re-triggers counted, not re-written). |

### Tests (current suite 325 green / 17 env-gated skips — verified module-local 2026-08-16; rows are historical)

> Rows below are the Slice-1-era record (2026-08-09). Current compute suite: **325 tests, 0 failures, 17 env-gated skips** (module-local `mvn test`, verified 2026-08-16; grew from 253 via +6 `FingerprintDedupExternalizationTest` +3 `FingerprintDedupWriterFunctionTest` +6 `FormingBarTableColumnsAgreementTest`, CHG-005, +5 `SignalJobConfigTest` restart-pin legs, +2 `CandleLateDropTest` (REQ-FC-006) +2 `FingerprintDedupFunctionTest` cache/rehydration telemetry +3 `ComputeOtlpEmitterTest` payload legs, +4 `DedupBucketAssignerTest` (SIG-STATE-001 bucket fix), +1 `FingerprintDedupFunctionTest.evictionKeepsTimerStateBoundedByCacheCap` (SIG-STATE-001 timer-deletion fix) +1 `SignalJobCompactCheckpointRestoreIntegrationTest` (SIG-STATE-001 compact-restore + bounded-checkpoint IT), +3 `CandleWindowEmitHarnessTest` (Phase B item 1 emitted-flag no-op + item 3 SIG-HARNESS-002 late semantics, real WindowOperator harness), +2 `CandleWatermarkIdlenessTest` (Phase B item 2 SIG-HARNESS-001 idleness half), +1 `FingerprintDedupFunctionTest.identicalLookingEventsCollapseAndEmitLimitationEvidence` (Phase B item 4 SIG-HARNESS-004), +1 `CompatFlinkCheckpointRescaleIntegrationTest` (Phase B items 5/6: COMPAT-FLINK-001 checkpoint/restore/rescale + STATE-COMPAT-001 serializer-change block — env-gated `COMPUTE_INT_TEST_COMPAT_FLINK`, passes with the gate set 2026-08-15), +1 `SigState002RehydrationRestoreIntegrationTest` (Phase B item 7: SIG-STATE-002 full job-restart half — real Fluss store + sink, compact restore, re-sent dedupes — env-gated `COMPUTE_INT_TEST_SIG_STATE_REHYDRATE`, PASSED live 2026-08-15), +1 `SigState003FailClosedPreflightIntegrationTest` (Phase B item 8: SIG-STATE-003 fail-closed preflight — env-gated `COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT`, PASSED live 2026-08-15), +2 REQ-FC-010 metric tests (Phase B item 9: `ComputeOtlpEmitterTest.reqFc010SourceAndWatermarkMetrics` + `SourceIdleWatchdogGeneratorTest.recordsThroughputAndWatermarkLagMetrics` — source throughput DELTA + watermark-lag gauge, zero graph nodes)), +15 Slice 2.2 forming-bar handoff tests 2026-08-16: +5 `FormingBarBuilderFunctionTest` (first-tick O=H=L=C, per-tick updates keep O and move H/L/C, quote-rows OHLC-only, window-transition fresh bar, builder never finalizes; REQ-FC-010 `compute.forming.bar.updates` per-tick counter — the assertion reads the function's own counter field because the Flink 2.2.1 harness double-opens the user function and metric-group `counter(name)` name-collision keeps the first registration in the private map), +7 `FormingBarDetectionFunctionTest` (live event before candle close, contract payload, Business Logic receipt, warm-up gate until N completed candles, positive + negative placeholder rule, fire-once-per-window, strictly-prior lookback guard — a completed candle enters the lookback only when its window precedes the forming window), +3 `FormingBarTypeInfoTest` (custom TypeSerializer round-trip incl. nulls, no Kryo fallback)), +12 forming-bar KV persistence tests 2026-08-16: +6 `FormingBarWriterFunctionTest` (nothing emitted before the write-cadence timer, coalesce-latest-per-instrument on flush, quiet instrument writes nothing after its flush, window rollover replaces the durable state on the same PK, emitted row is the exact 11-column v1 layout, same-window repeats replace not append), +4 `TableContractValidatorTest` forming-bar legs (exact PK `[instrument_token]` passes, no-PK rejected, wider-PK rejected, 10-column schema drift rejected), +1 `SignalJobConfigTest.rejectsNonPositiveFormingBarWriteBatchMs`, +1 `FormingBarBuilderFunctionTest.persistSideOutputCarriesEverySnapshot` (the side output carries the same snapshot per accepted tick — the writer coalesces, so this is never a per-tick Fluss write)), +1 `FormingBarWriterFunctionTest.checkpointRestoreResumesBufferedBar` (restart-rehydration Flink half 2026-08-16: build + checkpoint via the harness, restore into a fresh harness via `initializeState`/`open` — the buffered bar for instrument X resumes exactly, a new-window bar replaces it, an unseen instrument restores empty — DEC-038: the checkpoint carries only the small working buffer, the durable authority is the `forming_bar` KV); the live half is `FormingBarRehydrationIntegrationTest` (FORMING-BAR-REHYDRATE-001, 3 legs, env-gated `COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE=true`, PASSED live 2026-08-16: latest-wins converge, rollover replaces on the same PK, cold-restart rehydrates the exact latest bar through the production `FlussFormingBarStateStore`, missing instrument reads empty); env-gated set (re-measured 2026-08-15, surefire): **17 skipped tests across 13 classes** — container battery (CandleGraphReplay 3, CandleFailureInjection 2, CandleRocksDbRestore 1, CandleTelemetryOutage 1, SignalJobOperatorUid 1, ComputeOtlpLiveDelivery 1 — the last skips without the collector reachable, SafetyHaltLiveIntegrationTest 1 — skips without `COMPUTE_INT_TEST_SAFETY`), plus `SignalJobObjectStoreCheckpointIntegrationTest` (1), `SignalJobSavepointRestoreIntegrationTest` (1, host-runnable: embedded MiniCluster + `file://` savepoints, passes 2026-08-13), `SignalJobCompactCheckpointRestoreIntegrationTest` (1, host-runnable: embedded MiniCluster + `file://` checkpoints, passes 2026-08-15 with `COMPUTE_INT_TEST_SIG_STATE_RESTORE=true`), `CompatFlinkCheckpointRescaleIntegrationTest` (1, host-runnable: embedded MiniCluster + `file://` checkpoints, passes 2026-08-15 with `COMPUTE_INT_TEST_COMPAT_FLINK=true`), `SigState002RehydrationRestoreIntegrationTest` (1, host-runnable: embedded MiniCluster + `file://` checkpoints + live Fluss, passes 2026-08-15 with `COMPUTE_INT_TEST_SIG_STATE_REHYDRATE=true`) and `SignalChainLiveE2ETest` (1 — skips without `SIGNAL_CHAIN_E2E=true`). `SignalCurrentKvIdempotencyTest`, `SigState003FailClosedPreflightIntegrationTest` and `FormingBarRehydrationIntegrationTest` are class-level env-gated (`COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT=true` / `COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE=true`) and report **0 run / 0 skipped** without the env var — not part of the 17. Counts in rows that were superseded by the 2026-08-13 conversion are annotated below.

| Test class | Count | Covers |
| --- | --- | --- |
| `CandleAggregateFunctionTest` | 5 | SIG-UNIT-001/002 core (tie ordering, mixed trades/quotes OHLCV, quote-only window, single-row window, merge) |
| `RawValidationFunctionTest` | 8 | Validation-gate rules (rowkind, schema version, validity state, price, qty) |
| `SignalJobConfigTest` | 19 | SIG-UNIT-003 core (pinned dedup TTL / window / checkpoint values; rejection of deviations; tuning defaults) + DEC-034 signal keys (defaults, overrides, `SIGNAL_LOOKBACK_CANDLES` < 2 and `SIGNAL_QUANTITY` ≤ 0 rejection) + restore path (`stateRecoveryPath` default null, `STATE_RECOVERY_PATH` override honored) + replay-gate keys (`ALLOW_FULL_REPLAY` fail-closed rules, mode selection RESTORE vs FULL_REPLAY — `CANDLE_CURRENT_TABLE` key removed 2026-08-13) |
| `FingerprintDedupFunctionTest` | 6 | SIG-UNIT-008/009 dedup half — Flink 2.2.1 operator harness (`KeyedOneInputStreamOperatorTestHarness`, no cluster): first occurrence passes / duplicate within TTL dropped; state stays exactly two rows per active key (dedup map + expiry index) regardless of fingerprint count; expiry timer deletes entries when the watermark reaches `first_seen + TTL` (never early), re-arriving expired fingerprint re-admitted; state key scoped by `version\|token\|fingerprint`; shared-expiry timer clears every listed key |
| `SignalDetectionFunctionTest` | 6 | DEC-034 rule via the same operator harness (no cluster): warm-up requires `SIGNAL_LOOKBACK_CANDLES` completed candles; fired candidate carries the full 22-column execution-ready payload (side/action/order_type/quantity/validity/detection_ts/formation ref/schema version; ~~ranking fields null~~ — REMOVED 2026-08-15 CHG-005); bearish candle never fires even on breakout; no fire without strict breakout; repeated breakouts emit distinct `candidate_id`s; state keyed per instrument |
| `ComputeOtlpEmitterTest` | 4 | OTLP/JSON payload shape: DELTA non-monotonic sum, `aggregationTemporality` + `isMonotonic` fields, per-flush delta drain (`getAndSet(0)`) |
| `TableContractValidatorTest` | 23 | Current-design preflight (2026-08-13; the Slice-1-era `CandleTableContractValidatorTest` 10 was renamed and expanded): candle KV table PK exactly `(instrument_token, window_start)` (rejects no-PK, narrower, wider), schema-v2 agreement, 16-bucket layout, `bucket.key=instrument_token`; signal LOG no-PK (SIGNAL-SCHEMA-001) + signal KV PK `(instrument_token)` |
| `CanonicalCandlePolicyTest` | 7 | CANDLE-KV-REPLAY-001: canonical `(schema_version, algorithm_version, configuration_version)` check, rejection of non-canonical rows (HISTORICAL — candle KV; re-scope target: canonical strategy/rule policy at the `Signal_Candidates_current` boundary) |
| `SignalCurrentKvIdempotencyTest` | 1 (env-gated) | 2026-08-13 replacement for the deleted `CandleCurrentKvIdempotencyTest`: `Signal_Candidates_current` PK `(instrument_token)` upsert converges to one row per instrument, last-write-wins; scratch table only, never touches platform tables |
| `CandleMigrationToolTest` | 5 | B8.2/B8.3 audit semantics (HISTORICAL — `CandleMigrationTool` and its test are DELETED 2026-08-13 with the candle KV projection) |

Not yet covered by an implementing test (pending): `SIG-UNIT-007` (dependency scan), `SIG-UNIT-008/009` emit half (`CandleEmitFunction` state-content assertions). ~~`SIG-HARNESS-005` (reservation/ranking recovery — lands with Slice 3), `SIG-FAIL-001` (decision-suppression half)~~ — **REMOVED 2026-08-15 (CHG-005 — reservation/ranking/decision state out of scope, not deferred)**; `SIG-PERF-001` (Phase 6 envelope half — see note below). **Covered 2026-08-15 (Phase B items 1–6): `SIG-HARNESS-001` idleness half (`CandleWatermarkIdlenessTest`), `SIG-HARNESS-002` (`CandleWindowEmitHarnessTest` late semantics), `SIG-HARNESS-004` (`FingerprintDedupFunctionTest.identicalLookingEventsCollapseAndEmitLimitationEvidence`), `STATE-COMPAT-001` serializer half + `COMPAT-FLINK-001` (`CompatFlinkCheckpointRescaleIntegrationTest`), and the `CandleEmitFunction` emitted-flag no-op (`CandleWindowEmitHarnessTest`).** **Covered by env-gated ITs 2026-08-13: `SIG-INT-001`/`SIG-INT-002` — `CandleGraphReplayIntegrationTest` (live Fluss source/sink boundary + dual-sink reconciliation); `SIG-HARNESS-003` and the `STATE-COMPAT-001` savepoint half — `SignalJobSavepointRestoreIntegrationTest` (stop-with-savepoint → strict restore at 2× parallelism, deterministic replay).**

### Pending work items: resolution plan

Each pending item below is a tracked work item with its solving method, prerequisite, and pass gate. Execution classes: **pure-JVM now** (needs only a compute-pom test-scope addition — no cluster), **live-dev now** (env-gated against the dev Fluss cluster, pattern of SAFETY-INT-001), **slice/phase-gated** (waits on Slice 3 or Phase 6).

| Pending item | How to solve | Prerequisite | Gate |
| --- | --- | --- | --- |
| Dedup unit tests — **DONE 2026-08-10** (`FingerprintDedupFunctionTest`, 6 green): harness-driven state-key/value/expiry assertions above. Emitted-flag half — **DONE 2026-08-15** (`CandleWindowEmitHarnessTest`, real `WindowOperator` via `WindowOperatorBuilder`): a re-trigger on an already-closed candle is a no-op — no correction row, no re-emission (window harness through the exact production `aggregate` path; `T0` wall-aligned to the 15000 ms boundary). **DEC-038 re-scope pending:** the dedup half re-targets to the Fluss-backed store + bounded cache (cache dedupes a re-sent fingerprint inside its staleness window; Fluss table holds the accepted set; checkpoint excludes it) | Harness infra **landed** in compute pom (test scope, no cluster); Fluss-side dedup tests follow the `SignalCurrentKvIdempotencyTest` scratch-table pattern | Expired fingerprint absent after its expiry cleanup runs — **proven on the old store**; Fluss-side expiry + cache-consistency — pending the DEC-038 implementation |
| **DEC-038 dedup cache — hit/miss semantics (unit, no cluster) — DONE 2026-08-15 (Phase B item 1; store prerequisite LANDED 2026-08-15, CHG-020)** — cache definite-duplicate → reject; cache miss/uncertain → consult the Fluss-backed store → seen → reject / unseen → durable first-seen insert → accept; Fluss wins on any cache-vs-store disagreement ("cache says unseen" never accepts against the store); eviction never determines duplicate status; cache never mirrors the table | Operator-harness test (Flink 2.2.1 harness already landed) around the bounded cache + Fluss-backed store (mock store for unit; scratch-table IT for the store half) | Fluss-backed dedup store implemented first — **landed 2026-08-15** (`FlussFingerprintDedupStateStore`, CHG-020) | Decision table matches the canonical DEC-038 protocol; cache bounds independent of TTL/cardinality/instrument count — **proven by `FingerprintDedupFunctionTest` cache telemetry legs (in-TTL re-delivery = cache-only definite duplicate, rehydration latency ≥ 0, 0 rehydration failures, eviction never determines status) + `FingerprintDedupExternalizationBenchmarkIT` live store half** |
| **DEC-038 duplicate race/retry (unit/IT)** — same fingerprint re-delivered concurrently or re-sent after restart → exactly one accepted first-seen, every later candidate counted as a duplicate (no double-accept, including after rehydration); the first-seen upsert is idempotent under retry | Re-delivery fixtures + a restart between deliveries (scratch-table pattern); assert accepted-count == 1 per fingerprint | Fluss-backed dedup store implemented first | Accepted exactly once; duplicates counted; state converges |
| **DEC-038 Fluss dedup unavailable / incompatible (fail-closed)** — **DONE 2026-08-15 (Phase B item 8)** — dedup table missing or schema-incompatible → job fails closed / stays degraded and never treats the set as empty (no silent replay with an empty dedup set); startup preflight rejects before unsafe use | **`SigState003FailClosedPreflightIntegrationTest` (env-gated `COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT=true`, live dev cluster, PASSED 2026-08-15):** `SignalJob.preflightTableContracts` accepts a DDL-24-shaped scratch dedup table (id recorded, 16 buckets, PK/bucketKeys exact) and FAILS CLOSED (IllegalStateException — `ContractViolation` extends it) on a MISSING dedup table and on a schema-drifted 5-col dedup table (STATE-COMPAT-001 / SIG-STATE-003); runtime unavailability half already pinned by `FingerprintDedupFunctionTest.rehydrationFailureFailsClosedAndIsCounted` | Preflight failure injection on the dedup table (extend the `TableContractValidator` fail-closed pattern; scratch tables) | Fluss-backed dedup store implemented first | SIG-STATE-003: fail closed / degraded, no silent replay — **proven live 2026-08-15** |
| **DEC-038 rehydration (env-gated live) — store-side half LANDED 2026-08-15 (CHG-020), full job-restart half LANDED 2026-08-15 (Phase B item 7)** — **store half:** externalization benchmark proved a fresh store (cold cache) rehydrates from Fluss authority — 15/15 sampled lookups SEEN_LIVE, avg 102 ms/lookup (evidence `logs/tracker-14/p11-dedup-externalization-benchmark-20260815.md`); **full job-restart half (`SigState002RehydrationRestoreIntegrationTest`, env-gated `COMPUTE_INT_TEST_SIG_STATE_REHYDRATE=true`, live dev cluster, PASSED 2026-08-15):** the REAL dedup sub-graph (real `FlussFingerprintDedupStateStore` factory + real `FingerprintDedupWriterFunction` → `StallGuardedSink(FlussSink)` → scratch `fingerprint_dedup` table — the exact SignalJob wiring) ran against a scratch table on the live cluster: phase 1 accepted 200 fingerprints (cache cap 100 → eviction ran), all 200 durably visible in the live table, one completed checkpoint retained; `preflightTableContracts` passed against the scratch dedup table (the "verifies Fluss dedup-table availability/compatibility" leg, `validateFingerprintDedupTable` ALWAYS-ON); phase 2 restored the compact checkpoint at 2× parallelism (no `allowNonRestoredState`) and re-fed all 200 + 2 new → exactly the 2 NEW emitted in **12.5 s restore-to-first-output (budget 30 s)**, table converged at 202 (200 original + 2 new — re-sends did NOT re-upsert), job stayed RUNNING. Hydration failures counted + fail-closed already pinned by `FingerprintDedupFunctionTest.rehydrationFailureFailsClosedAndIsCounted` | Scratch-table live test on the dev Fluss cluster (pattern: `SignalCurrentKvIdempotencyTest` + `SignalJobSavepointRestoreIntegrationTest`); store-half hydration latency recorded 2026-08-15 | Fluss-backed dedup store implemented first; dev cluster exists | SIG-STATE-002: resumes without full raw-history replay; re-sent fingerprint inside TTL dedupes; hydration failure → fail closed — **ALL legs evidenced 2026-08-15** |
| **DEC-038 compact-checkpoint restore (no full replay, bounded size)** — checkpoint carries source offsets/watermarks/timers/in-flight windows + bounded cache metadata only (never the full dedup set); restore resumes from the compact checkpoint without offset-0 replay; checkpoint size does not grow with Fluss dedup cardinality — **store-side cleanup/plateau evidence LANDED 2026-08-15 (CHG-020)** — externalization benchmark on a scratch table, then **confirmed on the REAL platform table `default.fingerprint_dedup` (id 3104) the same day**: production `FlussFingerprintDedupStateStore` (the class `SignalJob` opens) wrote 5 live + 5 stale rows across tokens 5000/7000/8000, `DedupBucketAssigner` on the real `TableInfo` predicted each token's bucket exactly (5000→15, 7000→4, 8000→5, NOT `token % 16`), the Signal-job cleanup pass (`scanExpired`+`delete`, batch 1000) deleted all 5/5 stale rows with 5/5 live rows surviving, post-delete lookups NOT_SEEN, and the table was reset to 0 rows (evidence `logs/tracker-14/p11-dedup-externalization-benchmark-20260815.md` §Real platform-table verification); **MiniCluster restore + checkpoint-size re-measurement LANDED 2026-08-15 (host-runnable, env-gated `COMPUTE_INT_TEST_SIG_STATE_RESTORE=true`)** — `SignalJobCompactCheckpointRestoreIntegrationTest` runs the real dedup sub-graph (dedup → side output → writer → shared store as the Fluss table) on embedded MiniClusters with `file://` checkpoints: two jobs with the SAME 500-entry cache cap and 5x the accepted fingerprints (2,000 vs 10,000; store grows to 12,000) produced checkpoints of **54465 vs 54482 bytes (ratio 1.00)** — the checkpoint does NOT grow with Fluss dedup cardinality; a strict restore (2x parallelism, no `allowNonRestoredState`) re-feeding all 10,000 + 2 new fingerprints passed exactly the 2 NEW ones (no full replay — a zero-state re-run would have emitted all 10,002) in **~870 ms restore-to-first-output**; the run also surfaced and fixed a real DEC-038 violation the measurement caught: `evictToBounds` left orphaned event-time timers for evicted buckets (without the fix the checkpoint grew 79984 → 215984 bytes, ratio 2.70), now deleted on eviction (`FingerprintDedupFunction`), pinned by `evictionKeepsTimerStateBoundedByCacheCap` + the IT's 1.5x ratio assertion | MiniCluster restore with the dedup table present (assert no full replay + bounded checkpoint); size/duration re-measured in the externalization benchmark (SIG-PERF-001 detail) | Fluss-backed dedup store implemented first; **externalization benchmark rows landed 2026-08-15 (CHG-020)** | SIG-STATE-001: checkpoint bounded and not duplicating the durable set; restore within the 30 s budget — **ALL THREE legs now evidenced (store cleanup/plateau, real-table confirmation, compact-restore + bounded checkpoint)** |
| `SIG-HARNESS-001/002/004` (~~005~~ — **REMOVED 2026-08-15, CHG-005: reservation/ranking recovery out of scope**) — `SIG-HARNESS-003` **COVERED 2026-08-13** (checkpoint-restore replay via `SignalJobSavepointRestoreIntegrationTest`, env-gated `COMPUTE_INT_TEST_SAVEPOINT`, green in-container: stop-with-savepoint → strict restore at 2× parallelism, dedup MapState continuity; re-feeding the same fingerprints emits nothing = deterministic replay equality). **001 idleness half DONE 2026-08-15** (`CandleWatermarkIdlenessTest`: `WatermarkStrategyWithIdleness` + `ManualClock` — no periodic emit → the generator marks idle after the idle timeout; a fresh event + periodic emit clears it, next periodic emit no longer marks idle; watermark half already covered by `CandleWatermarkStrategyTest`). **002 DONE 2026-08-15** (`CandleWindowEmitHarnessTest`: late-before-final events fold into the open window and are included in the final candle; late-after-final events hit the late-data path and emit nothing — no correction row). **004 DONE 2026-08-15** (`FingerprintDedupFunctionTest.identicalLookingEventsCollapseAndEmitLimitationEvidence`: the duplicate-vs-identical limitation — identical fingerprints collapse to one accepted first-seen; the duplicate counter records the re-sends and the limitation audit row is emitted). Remaining: ~~005 (reservation/ranking recovery, lands with Slice 3)~~ — **REMOVED 2026-08-15 (CHG-005)** | Same harness infra (Flink 2.2.1 API: `ProcessFunctionTestHarnesses.forKeyedProcessFunction` → `KeyedOneInputStreamOperatorTestHarness`): inject watermarks and processing time; assert correct event-time outcome; `snapshot`/`initializeState` for deterministic replay equality; ~~SIG-HARNESS-005 lands with Slice 3 (reservation/ranking state)~~ — **REMOVED 2026-08-15 (CHG-005)** | Harness infra **landed**; **no cluster** | Correct event-time outcome; late-before-final updates vs after-final discard; restored output equals expected deterministic output — **all proven 2026-08-15** |
| `STATE-COMPAT-001` (serializer-change half — savepoint half **COVERED 2026-08-13** by `SignalJobSavepointRestoreIntegrationTest`), `COMPAT-FLINK-001` (source/sink checkpoint-restore-rescale on pinned versions) — **BOTH DONE 2026-08-15** (`CompatFlinkCheckpointRescaleIntegrationTest`, env-gated `COMPUTE_INT_TEST_COMPAT_FLINK=true`, host-runnable, embedded MiniCluster + `file://` checkpoints, pinned Flink 2.2.1 + `fluss-flink-2.2:0.9.1-incubating`): checkpoint the dedup sub-graph, restore at 2× parallelism with strict state, assert state continuity and exactly-once accepted/duplicate counts across rescale (COMPAT-FLINK-001); then a **state-serializer change** (different cache value layout) blocks startup — the restore job fails with a state-compatibility error before unsafe use instead of silently re-reading wrong state (STATE-COMPAT-001 serializer half) | `MiniClusterWithClientResource` (`flink-test-utils`, **already in compute pom test scope**) with pinned Flink 2.2.1 + `fluss-flink-2.2:0.9.1-incubating`; run topology, checkpoint, restore, assert state continuity; serializer-change compatibility blocks startup before unsafe use | Harness infra **landed**; **no cluster** | Restore succeeds through the approved path, or startup blocks before unsafe use — **proven both ways 2026-08-15** |
| `SIG-INT-001/002` — **DONE 2026-08-13** (covered by `CandleGraphReplayIntegrationTest`, env-gated `COMPUTE_INT_TEST_P6`, green in-container 3/3, 134.9 s): real `SignalJob.buildTopology` against live dev Fluss with scratch tables — source/sink boundary on approved versions (001) + dual-sink partial-visibility reconciliation, signal LOG grows while `Signal_Candidates_current` KV key count stays frozen (002). The planned dedicated `COMPUTE_INT_TEST_SIGNAL` gate test was superseded — the P6 replay covers both | Env-gated live test on the dev Fluss cluster, same pattern as `SafetyHaltLiveIntegrationTest` (`COMPUTE_INT_TEST_SAFETY` → new `COMPUTE_INT_TEST_SIGNAL=true` gate): run the real `SignalJob` topology (or its source→sink shell) against live Fluss, assert candles land in `feature_candles_15s`; SIG-INT-002 uses two sinks + reconciliation to prove partial-visibility handling (as-built sinks were the candle LOG/KV pair — 2026-08-13: candle sink is the KV upsert, reconciliation targets the signal LOG/KV pair) | Live dev Fluss cluster (exists); Slice 2 sinks for the reconciliation half | Source/sink semantics work with approved versions; reconciliation identifies and handles partial visibility |
| `SIG-FAIL-001` (checkpoint/continuity failure → safe halt) | MiniCluster failure injection: force checkpoint failure, assert fixed-delay restart ×3 then fail-job; **the decision-suppression + safe-halt half is REMOVED 2026-08-15 (CHG-005 — decision operators out of scope)**; the shell-level checkpoint-failure test remains in scope | None (shell-level test can precede) | Job fails closed with a fixed-delay restart policy; no unsafe continuation |
| `SIG-PERF-001` (variable-baseline and peak workload) | Phase 6 perf campaign: soak suite (`run-full-suite.sh`) on the current 1,024-instrument / 20,480 t/s envelope; record state size, checkpoint duration/size, memory. **The decision-p99 half is REMOVED 2026-08-15 (CHG-005); the DEC-038 externalization-benchmark half LANDED 2026-08-15 (SIG-STATE-001/002 + SIG-PERF-001 evidence, see banner)** | Phase 6 | state, checkpoint, memory within defined limits (current-phase envelope; 50k deferred; 90k retired, DEC-036) |
| Beyond-lateness discard counter (REQ-FC-006) — **DONE 2026-08-15** (Phase B item 10): silent late-drop replaced with `.sideOutputLateData(tag)` on the window operator + a counting side-output operator; metric `compute.candles.late.dropped` (Flink counter + OTLP DELTA sum) carries the LATEST drop's instrument/window-end/lateness/reason as one bounded attribute set (`CandleLateDrop`; wired after the `candle-15s` UID; `SignalJobOperatorUidTest` updated). Tests: `CandleLateDropTest` + `CandleWindowEmitHarnessTest` late legs (in-lateness re-trigger emits nothing and is NOT dropped; beyond-final event → exactly one late-drop side-output row) | Code change — Slice 2 backlog | Discard metric emitted with instrument/window/lateness/reason — **proven** |
| Source throughput/lag, watermark lag, dedup state size (REQ-FC-010) — **DONE 2026-08-15 (Phase B item 9)** — dedup state-size gauges already landed (tracker 14 P5.1: `compute.dedup.state.count` / `.expiry.index.count` / `.state.bytes.estimate`); **source throughput + watermark lag added 2026-08-15** — the `SourceIdleWatchdogGenerator` watermark-level watchdog (inside the source operator, zero graph nodes — operator IDs stay bit-identical) now counts every consumed record into the `compute.source.records` DELTA sum (per-flush-window throughput) and records the last emitted watermark's processing-time staleness into the `compute.watermark.lag.ms` last-value gauge; payload-shape test (`ComputeOtlpEmitterTest.reqFc010SourceAndWatermarkMetrics`) + watchdog behavior test (`SourceIdleWatchdogGeneratorTest.recordsThroughputAndWatermarkLagMetrics`), suite 324/0/17 | Code: connector source-throughput metric, timestamp-assigner watermark-lag metric, dedup state-size probe (entry counter or sampled `MapState` size) | Code change — Slice 2 backlog | REQ-FC-010 metrics emitted — **proven 2026-08-15** (suite 325/0/17) |
| `RESTART_MAX_ATTEMPTS` / `RESTART_DELAY_MS` pinning — **DONE 2026-08-15** (`restartMaxAttempts`/`restartDelayMs` in `SignalJobConfig.from`, production-gated: `requirePinnedInt`/`requirePinnedLong` in `DEPLOYMENT_ENV=production`, dev stays overridable for the failure-injection ITs; constants `PlatformConfig.RESTART_MAX_ATTEMPTS`/`RESTART_DELAY_MS`; `SignalJobConfigTest` +5: rejects deviating/missing attempts and delay in production, accepts exact pins) | Code: `intValue` → production-pinned helpers in `SignalJobConfig.from` + rejection tests in `SignalJobConfigTest` | None | Config test rejects deviations from 3 / 30000 in production — **proven: compute suite 294/0/17** |

Fastest path: the harness infra is **already landed** (compute-pom test-scope addition, 2026-08-10). **Phase B Group 1 (items 1–6) landed 2026-08-15** — the pure-JVM rows (`CandleEmitFunctionTest` emitted-flag no-op, `SIG-HARNESS-001` idleness half, `SIG-HARNESS-002`, `SIG-HARNESS-004`, `STATE-COMPAT-001` serializer half, `COMPAT-FLINK-001`) are all covered by test code with no cluster needed (the two MiniCluster ITs are host-runnable with embedded clusters). **Phase B Group 2 (items 7–8) landed 2026-08-15** — the DEC-038 rehydration full-flow (`SigState002RehydrationRestoreIntegrationTest`, SIG-STATE-002) and fail-closed preflight (`SigState003FailClosedPreflightIntegrationTest`, SIG-STATE-003) both PASSED live against the dev Fluss cluster with their gates set. `SIG-FAIL-001`'s decision-suppression half and `SIG-PERF-001`'s decision-p99 half are **REMOVED 2026-08-15 (CHG-005 — Slice 3 removed, not deferred)**; the remaining shell-level checkpoint-failure and Phase-6 envelope halves wait on Phase 6.

### Live smoke (2026-08-09)

- **205,146 candle rows** written to `feature_candles_15s`; **1,074 distinct instruments**; **48 EXACTLY_ONCE checkpoints**; dedup state ~2 MB; OHLC/window-spacing **0 violations**; volume/tick histograms match REQ-FC-002 exactly.
- Dev overrides retained: `CHECKPOINT_DIR=file:///tmp/signaljob-checkpoints` (local heap checkpoints have a 5 MiB-per-state cap); full Flink-dist `--add-opens` set; `-Xmx4096m` for replays. (`RAW_SCHEMA_VERSION` stays at its default `2` — verified 2026-08-10 that the dev `raw_table_1` is the 20-column v2 schema.)

## Slice 2.1 — MVP signal detection → `Signal_Candidates` (implemented 2026-08-10)

Implements DEC-034: a closed-candle placeholder rule that produces
execution-engine-ready candidate records, so the record shape and the KV write
path are real before the user's own trading logic arrive. ~~Ranking,
reservations, `Trade_Decisions`,~~ and candidate lifecycle (max-one-active,
supersession, expiry) are intentionally postponed; ~~(ranking/reservation/decision portions REMOVED 2026-08-15, CHG-005)~~ a fired signal is appended as
an immutable `Signal_Candidates` record and nothing else happens downstream.

### Rule v1 — "20-candle breakout" (placeholder; replaceable via config)

Evaluated on each completed 15-second candle, keyed by `instrument_token`:

1. **Bullish**: `close > open` (strict; a flat candle never fires).
2. **Breakout**: `close > max(high of the previous`SIGNAL_LOOKBACK_CANDLES`completed candles)` (strict).
3. **Trend filter** (kept for contract fidelity): `close > mean(close of the previous lookback candles)` — exact integer compare `close * n > sum`.

Facts (documented so the frequency knob is understood, not guessed):

- **The trend filter is mathematically implied by the breakout**: every previous close is ≤ its own high ≤ `maxHigh`, so `close > maxHigh` already forces `close > mean`. It cannot fail independently — it is asserted only via the composite rule. The frequency knob is `SIGNAL_LOOKBACK_CANDLES` (default 20 = 5 minutes of history; a longer lookback fires less often).
- **Warm-up**: no signal until `lookback` completed candles exist per instrument (first 5 minutes of a session produce nothing).
- **Candidate identity**: `candidate_id = rule_id + "-" + instrument_token + "-" + window_end`; unique because exactly one candle closes per (instrument, window_end).
- **`formation_snapshot_ref`**: `candle:{window_start}:{window_end}:open=..:high=..:low=..:close=..:volume=..` — pins the exact closing candle that fired the rule.

### Record contract (execution-engine-ready — the engine punches with zero reasoning)

| Group | Columns | Value |
| --- | --- | --- |
| What to trade | `instrument_token`, `exchange`, `symbol` | From the closing candle |
| What to do | `side` = `BUY`, `action` = `ENTRY`, `quantity` = `SIGNAL_QUANTITY` (default 1 — config placeholder for real sizing), `order_type` = `MARKET`, `limit_price_paise` = NULL | Engine-ready defaults |
| Identity / audit | `candidate_id`, `detection_ts` = `evaluation_ts` = `window_end`, `strategy_id` = `simple-breakout`, `strategy_version` = `1.0.0`, `rule_id` = `breakout-20-bullish-trend`, `formation_snapshot_ref`, `validity_reason` = `VALID`, supersede chain empty, `schema_version` = `2` | Full audit trail |
| Removed from scope (CHG-005, 2026-08-15 — not deferred); empty by design | `instruction_id`, `trade_context_id`, `score_inputs` | NULL — no producer in scope writes them |

### Files

| File | Responsibility |
| --- | --- |
| `02_compute/.../signaljob/SignalDetectionFunction.java` | `KeyedProcessFunction<Long, RowData, RowData>` keyed by `instrument_token`; two bounded `ValueState<List<Long>>` ring buffers (highs, closes); emits 22-column `GenericRowData` candidates; metric `compute.signals.detected` |
| `02_compute/.../signaljob/SignalCandidatesTableColumns.java` | 22 column indexes mirroring DDL v2 exactly, `FIELD_COUNT`, `ROW_TYPE_INFO`, plus record constants (`ACTION_ENTRY`, `SIDE_BUY`, `ORDER_TYPE_MARKET`, `VALIDITY_REASON_VALID`, `SCHEMA_VERSION_V2`) |
| `02_compute/.../signaljob/SignalJob.java` | Wired after the 15s window: `keyBy(token).process(SignalDetectionFunction)` → `FlussSink` with `RowDataSerializationSchema(false, false)` (KV upsert; `isAppendOnly=false` maps INSERT → UPSERT; the Append vs Upsert writer is chosen from live table metadata — fail-fast startup if the KV table is missing) (HISTORICAL — 2026-08-10-era wiring into `Signal_Candidates` as KV; current dual-sink per DEC-035: LOG append into `Signal_Candidates` + KV upsert into `Signal_Candidates_current`) |
| `02_compute/.../signaljob/SignalJobConfig.java` | New tuning keys: `SIGNAL_CANDIDATES_TABLE` (`Signal_Candidates`), `SIGNAL_STRATEGY_ID` (`simple-breakout`), `SIGNAL_STRATEGY_VERSION` (`1.0.0`), `SIGNAL_RULE_ID` (`breakout-20-bullish-trend`), `SIGNAL_LOOKBACK_CANDLES` (20, must be ≥ 2), `SIGNAL_QUANTITY` (1, must be > 0) |
| `01_ingestion/.../DdlBootstrap.java` | `Signal_Candidates` now registered with a full 22-column KV descriptor (PK `candidate_id`, `distributedBy(16, "candidate_id")`) instead of the generic `logTable` descriptor — the sink needs a real KV table (HISTORICAL — pre-Stage-2; current: `DdlBootstrap` registers `Signal_Candidates` as a LOG descriptor (no PK) + `Signal_Candidates_current` as the 22-col KV descriptor, Stage 2) |
| Dev table | Created DDL-faithful on the dev cluster (22 cols, PK, 16 buckets; datalake props skipped — the dev Fluss server has datalake disabled, same precedent as `feature_candles_15s`) |

### Tests (38 green + 1 env-gated skip; no SIG-* ID carried in code yet)

`SignalDetectionFunctionTest` (6) + `SignalJobConfigTest` additions (2) — mapping rows in the Slice 1 test table above.

### Live evidence (2026-08-10)

- **Run:** full replay of the dev `raw_table_1` (15.2M rows, full offsets) through the complete topology — validation → dedup → candles → signal detection → `Signal_Candidates` KV upsert. **`FlussSink` picked the upsert writer** from live table metadata ("Initializing Fluss upsert sink writer"), proving the KV write path against the real dev table. — (HISTORICAL pre-DEC-035 wiring: the signal output is `Signal_Candidates` LOG + `Signal_Candidates_current` KV dual-sink since 2026-08-13.)
- **Live counts (2026-08-10, live run 7456cc1c):** `Signal_Candidates` grew through the run (925 → 3,220 by 16:50 as live windows fire) — the job consumes `raw_table_1` only (no feature read-back), and the KV upsert rewrites a re-detected same-window candidate in place by `candidate_id`, so the count tracks genuinely new windows, never replay duplication. `feature_candles_15s` (LOG, append-only — **HISTORICAL**: converted to KV-only 2026-08-13) grew to 323,930 candles before the replay-pollution incident — see pollution note in the process-rule section. 158+ EXACTLY_ONCE checkpoints at ~310 MB / 450–850 ms.
- **Metric-registration fix:** the live replay surfaced a pre-existing flood — `RawValidationFunction` registered its per-reason counter per invalid row, and Flink 2.2.1 logs a name-collision warning per re-registration (~5 GB/min log). Fixed by caching per-reason counters in `open()`; `RawValidationFunctionTest` 8/8 green.

### Process rules (approved 2026-08-10) — evidence

**Rule 1: DDL ↔ code version pin.** `RawTable1DdlSchemaVersionTest` (common module, 3 tests) parses the `02_raw_table_1.sql` header (`-- Schema version: 2`) and fails the build if it drifts from `PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION` — the single source both producer (`FlussClientAdapter.append`) and consumer gate (`RawValidationFunction`) derive from. The 2026-08-10 stall (builder defaulted "1" while compute validated "2") is the regression this prevents. Verified: common module suite green (`Tests run: 101, Failures: 0`).

**Rule 2: operational alert on schema-version rejection.** `ComputeOtlpEmitter` (compute telemetry) exports `compute.invalid.byReason.schema-version` as a DELTA non-monotonic sum — each 10s flush carries only rejections since the previous flush (`getAndSet(0)`), so historical replay/checkpoint restore never re-fires. `o2-provision.py` seeds the stream and provisions realtime alert `SIGNAL-crit-schema-version-rejected` (`value > 0`, period 1, threshold 1, destination `dev-webhook`). Live-verified 2026-08-10: stream `compute_invalid_byreason_schema_version` receiving points every 10s (275+ docs, growing); injected `value=1` test point fired the alert exactly once (webhook attempted; dev sink is a noop), zero-delta points never fired it.

**Rule 3: restore path for restart.** Pinned checkpoint contract (10s interval / 30s timeout / 1 concurrent) cannot absorb a full replay (the dead run ballooned dedup state to 1.1 GB and expired chk-9). `SignalJob` now honors `STATE_RECOVERY_PATH` → `StateRecoveryOptions.SAVEPOINT_PATH`; `SignalJobConfigTest` pins the override. Live: restart from chk-675 restored cleanly, checkpoints 676+ at ~327 MB / ~650 ms, no backlog replay.

## Slot-scoped safety consumer (plan Amendment — implemented 2026-08-09)

Implements plan.md §"Slot-scoped safety propagation" for the Signal Job: consume the immutable `Safety_Halt_Requests` KV changelog, bridge each current-value row, apply it to a per-slot state machine, and suppress decisions only for the affected slot's deterministic token set.

### Files

| File | Responsibility |
| --- | --- |
| `common/src/main/java/com/trading/common/safety/SlotSafetyStatus.java` | `UNSAFE` / `RECOVERED` vocabulary. |
| `common/src/main/java/com/trading/common/safety/SlotSafetyRequest.java` | Row contract record; mirrors `SafetyHaltWriter` validation (UNSAFE requires a reason). |
| `common/src/main/java/com/trading/common/safety/SafetyHaltRequestParser.java` | Map → request validation (contract_version = 2, state vocabulary, UNSAFE reason); `ParseException` for malformed rows — never fatal. |
| `common/src/main/java/com/trading/common/safety/SlotAssignment.java` | Deterministic manifest-derived slot→token-set mapping. |
| `common/src/main/java/com/trading/common/safety/SlotAssignmentResolver.java` | Go-parity assignment (sorted tokens, contiguous `connectionLimit` chunks into `hft-N`), byte-identical `TokenSetHash` vectors. |
| `common/src/main/java/com/trading/common/safety/TokenSetHash.java` | SHA-256 over sorted 8-byte-BE token longs (Go-parity). |
| `common/src/main/java/com/trading/common/safety/SafetyStateTracker.java` | State machine; 10-outcome `ApplyResult`; epoch = connection-instance boundary (same-epoch re-delivery is a duplicate); RECOVERED needs strictly greater epoch; gates on source component, contract version, manifest fingerprint, slot hash before any state change. |
| `common/src/main/java/com/trading/common/safety/SuppressionGate.java` | ALLOW / SUPPRESS_NEW / DISCARD_INFLIGHT per token; published decisions never retracted. |
| `02_compute/.../safetyhalt/SafetyHaltJob.java` | Flink shell: `FlussSource` (exactly-once, `OffsetsInitializer.full()`) → current-value filter (INSERT/UPDATE_AFTER) → per-task `SafetyHaltApplyFunction` (RichFlatMapFunction) with `safety.transitions.applied` / `safety.rows.malformed` / `safety.rows.skipped` metrics; malformed rows counted, never fatal. |
| `02_compute/.../safetyhalt/SafetyHaltRowDataBridge.java` | RowData → request via DDL v3 column positions. |
| `02_compute/src/test/.../SafetyHaltLiveIntegrationTest.java` | SAFETY-INT-001: env-gated (`COMPUTE_INT_TEST_SAFETY=true`) live harness — upsert UNSAFE/RECOVERED rows via the writer's KV write path, read back by primary key, bridge + parse + apply, assert suppression window opens/closes. **Passed against live Fluss 2026-08-09.** |

### Connector and compile evidence (T0)

- `org.apache.fluss:fluss-flink-2.2:0.9.1-incubating` resolves from Maven Central (shaded, 69 MB). `FlussSource<OUT>` + `FlussSource.<OUT>builder()` + `RowDataDeserializationSchema` + `OffsetsInitializer.full()`; `FlussSource.build()` performs a live `Admin.getTableInfo` (fail-fast startup). `flink-connector-base` is required as a provided dep (not transitive from `flink-streaming-java` 2.2.1).
- Flink 2.2.1 class locations (jar-verified): `RichFlatMapFunction` = `org.apache.flink.api.common.functions` (flink-core); `open(OpenContext)` (not `Configuration`); `RowKind` = `org.apache.flink.types`; `DataStream`/`StreamExecutionEnvironment` = flink-runtime. The user's local Flink source tree (`/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/flink`, 2.4-SNAPSHOT) confirms identical locations.
- `mvn -f 02_services/02_compute/pom.xml test` is green (main + test compile; SAFETY-INT-001 skips without the env gate). Compute is a standalone module outside the reactor; parent + `common` must be installed in `.m2` first (`mvn -N install`, `mvn install -pl common -DskipTests`).

### Live verification (2026-08-09)

- **v3 DDL applied live via the offline gate.** The dev cluster's `Safety_Halt_Requests` was still a LOG table (in-code `DdlBootstrap.SAFETY_HALT_SCHEMA` had no primary key), which rejects PK lookup. Dropped the pre-v3 LOG table and created the v3 KV table (`pk=[halt_request_id]`, 4 buckets, 21 columns). Datalake props from the DDL were skipped — this dev cluster has no lake catalog. `SafetyHaltWriter` switched `newAppend()` → `newUpsert()` (a KV table rejects `AppendWriter`; a duplicate `halt_request_id` is the R-089 upsert no-op), `observe` became generic, and `DdlBootstrap.SAFETY_HALT_SCHEMA` now declares the PK so a fresh cluster creates KV. Ingestion suite: 171 tests, 0 failures.
- **SAFETY-INT-001 passed** (`logs/safety-int-001/safety-int-001-20260809-122201.out`): KV upsert of an UNSAFE row → primary-key lookup (the client rejects `lookupBy` when lookup columns equal the physical PK) → production `SafetyHaltRowDataBridge` + `SafetyHaltRequestParser` + `SafetyStateTracker` → `NEW_UNSAFE`, tokens `[1000, 1001, 1]` suppressed and `999999` not; then RECOVERED at epoch+1 → `RECOVERED`, tokens admitted.

### Deferred (documented, not stubbed)

- The tracker lives per-task in the job shell; ~~when the decision operators land it moves to broadcast state so `isTokenSuppressed` / `SuppressionGate` gate candidates/rankings/reservations/decisions~~ — **REMOVED 2026-08-15 (CHG-005 — decision operators out of scope)**. A tick alone never clears unsafe state; RECOVERED admits only post-recovery input.
- The job itself (live `FlussSource` consume path end-to-end) runs only after production approval; SAFETY-INT-001 already exercises the production bridge/parser/tracker against live Fluss.

---

## Current build plan — Signal LOG/KV dual-sink (2026-08-13)

> **PARTIALLY SUPERSEDED (2026-08-13, later same day):** the candle half of this plan ("`feature_candles_15s` — LOG, sole candle output") was itself superseded by the user's candle-KV-only requirement: `feature_candles_15s` is now the **KV upsert table** (DDL 03 KV, PK `(instrument_token, window_start)`, converted and verified 2026-08-13). The signal half (`Signal_Candidates` LOG + `Signal_Candidates_current` KV) is exactly as planned below and remains current. Stage 2's DDL-22 deletion and the candle-KV retirement (Stages 3-4) are executed; the "live table 92 teardown" item below means the pre-conversion `feature_candles_15s_current` live table, already dropped.

### Signal LOG/KV Dual-Sink Implementation (Requirement Change 2026-08-13)

## Overview

Implement the 2026-08-13 requirement change: **retire the candle [LOG + KV] dual-sink** and **build the signal [LOG + KV] dual-sink**.

Target topology (source of truth: `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` §1, tracker header):

```
raw_table_1 → validation → dedup → 15s candles ── feature_candles_15s (KV upsert, PK (instrument_token, window_start) — sole candle output, 2026-08-13 conversion)
                                              └─ signal detection ── Signal_Candidates (LOG, append per signal)
                                                                  └─ Signal_Candidates_current (KV, PK instrument_token)
```

- `feature_candles_15s` — **KV** (converted 2026-08-13; was LOG): PK exactly `(instrument_token, window_start)`, 15 cols, `bucket.key=instrument_token`, 16 buckets; upsert is last-write-wins, so replay re-upserts the same key and converges (no row growth).
- `Signal_Candidates` — **LOG v3** (was KV v2, R-084): no primary key, `bucket.key=instrument_token`, append-only, one row per fired signal, never updated.
- `Signal_Candidates_current` — **NEW KV**: PK exactly `(instrument_token)`, `bucket.key=instrument_token`, 16 buckets, latest/active candidate per instrument, supersession overwrites in place.
- `feature_candles_15s_current` — **DELETED** (DDL 22 deleted + live dev table id 92 dropped 2026-08-13; the live `feature_candles_15s` LOG → KV recreation executed 2026-08-13 — LiveSync, id 90 → 693; then recreated at 2d TTL the same day — id 693 → 697, datalake disabled, preflight PASS).

**Row layout is FROZEN**: both signal tables share the pre-change 22-column `Signal_Candidates` v2 layout (`SignalCandidatesTableColumns`, `schema_version="2"` per tracker §1.3). `SignalDetectionFunction` and its emitted row do NOT change. Only storage kind, DDL, sink serialization, and config keys change.

### Acceptance criteria

1. Compute topology emits: candle KV upsert (PK `(instrument_token, window_start)` — sole candle output); signal LOG append + signal KV upsert (PK `instrument_token`).
2. `SchemaAgreementTest` (ingestion) pins `Signal_Candidates` LOG + `Signal_Candidates_current` KV; green.
3. Stateful Flink operator IDs (`raw-validation` chain, `fingerprint-dedup`, `candle-15s`, `signal-detection`) unchanged vs the Stage 0 baseline — proven by `JobGraphDump` diff. **SUPERSEDED 2026-08-13** (Stage 4 evidence): unsatisfiable as written — Flink IDs derive from the transitive topology hash and the baseline `candle-15s` vertex chained `canonical-candle-filter`, so the Stage 3 deletion alone shifted that vertex and all downstream IDs. Durable equivalent delivered: explicit `.uid(...)` on all 9 operators (UID-anchored restore, pinned by `SignalJobOperatorUidTest`); full analysis in `logs/tracker-14/jobgraph-signal-rescope-evidence-20260813.md`.
4. All retired candle-KV code deleted with no aliases/shims: `CanonicalCandleFilterFunction`, `CandleMigrationTool`, `CandleMigrationBatchJob`, candle-KV tests, DDL 22.
5. Full common + compute + ingestion suites green.
6. Live-cluster teardown (table 92, p10 trio) executed only after explicit user confirmation (Post-Completion).

## Context

### Files involved (HISTORICAL snapshot — verified 2026-08-13 BEFORE Stages 2–3 executed; describes the pre-change code, do NOT read as current. Current state: Overview + Stages 3–6 — candle KV upsert sole output, `TableContractValidator`, signal LOG/KV dual-sink)

- DDL: `code/01_platform/02_sql/ddl/05_signal_candidates.sql` (KV v2, PK `candidate_id`), `ddl/03_feature_candles_15s.sql` (LOG, unchanged), `ddl/22_feature_candles_15s_current.sql` (delete), `ddl/schema_manifest.json`, `code/01_platform/04_scripts/ddl_apply.py` (stub), `code/01_platform/02_sql/README.md` (rows already annotated).
- Ingestion: `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java` (Signal_Candidates 22-col KV descriptor), `src/test/java/com/trading/ingestion/SchemaAgreementTest.java` (L178–179 pins `PRIMARY KEY (candidate_id)` — breaks), `DdlBootstrapSchemaAgreementTest.java`.
- Compute main: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/` — `SignalJob.java` (topology L115–299: candle LOG sink L228–240; signal KV sink L254–261; candle KV block L263–296 with `CanonicalCandleFilterFunction`; `preflightTableContracts` L399–426), `SignalJobConfig.java` (record field `candleCurrentTable` L40, env `CANDLE_CURRENT_TABLE` L91, `signalCandidatesTable` L57/env L110), `CandleTableContractValidator.java`, `CanonicalCandleFilterFunction.java`, `StallGuardedSink.java` (retained), `SignalCandidatesTableColumns.java` (22-col, unchanged), `SignalDetectionFunction.java` (unchanged); `com/trading/compute/telemetry/ComputeOtlpEmitter.java` (`KV_FILTERED_NON_CANONICAL` + `recordKvFilteredNonCanonical()` — only called by the retiring filter); `com/trading/compute/tools/` — `CandleMigrationTool.java`, `CandleMigrationBatchJob.java`, `JobGraphDump.java` (reused for evidence).
- Common: `code/common/src/main/java/com/trading/common/schema/CandleTableSchema.java` (15-col candle contract + `CANONICAL_ALGORITHM_VERSION`/`CANONICAL_CONFIGURATION_VERSION` — constants stay, used by `SignalJobConfig.requireCanonicalVersion`), `CanonicalCandlePolicy.java` (verify references before deleting), `code/common/src/test/java/com/trading/common/schema/CandleCurrentDdlContractTest.java` (delete).
- Compute tests: `signaljob/` — `CandleTableContractValidatorTest.java`, `CanonicalCandleFilterFunctionTest.java` (delete), `CandleCurrentKvIdempotencyTest.java` (delete), `CandleEmitFunctionTest.java` (references `CanonicalCandlePolicy` — keep if policy kept), `CandleGraphReplayIntegrationTest.java` (re-scope), `SignalJobConfigTest.java` (`defaultsCandleCurrentTableAndHonorsOverride` — remove); `tools/` — `CandleMigrationToolTest.java`, `CandleMigrationBatchJobTest.java` (delete).

### Commands (verified)

- Build + test: `cd code && mvn -o test -pl 02_services/02_compute -am` — NOTE (2026-08-13): this exact invocation cannot run — compute is excluded from the root reactor (R-272); run module-locally in `code/02_services/02_compute` instead (same gate, see Stages 3–5)
- Ingestion pin tests: `cd code && mvn -o test -pl 02_services/01_ingestion -am -Dtest=SchemaAgreementTest,DdlBootstrapSchemaAgreementTest -Dsurefire.failIfNoSpecifiedTests=false`
- JobGraphDump (Stage 0 / post-change): build then run **inside the trading-net** — the host fails: the coordinator answers `localhost:9123` but advertises the tablet as `fluss-tablet:9124`, which does not resolve on the host. Verified recipe: `docker run --rm --network 01_docker_trading-net -v <repo>:/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project -v ~/.m2:/home/saurabh/.m2:ro -w <repo> maven:3.9-eclipse-temurin-17 sh -c 'env FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 OTEL_COLLECTOR_HOST=otel-collector:4318 DEDUP_TTL_MS=300000 CANDLE_WINDOW_MS=15000 CHECKPOINT_INTERVAL_MS=10000 CHECKPOINT_TIMEOUT_MS=30000 MAX_CONCURRENT_CHECKPOINTS=1 CHECKPOINT_DIR=file:///tmp/signaljob-checkpoints ALLOW_FULL_REPLAY=true PARALLELISM=16 java -cp "code/02_services/02_compute/target/classes:$(cat logs/tracker-14/jobgraph-signal-baseline/classpath.txt)" com.trading.compute.tools.JobGraphDump <out-dir>'`. The classpath file (host paths, valid in-container because `.m2` is mounted at the same absolute path) is produced by `cd code/02_services/02_compute && mvn -o dependency:build-classpath -Dmdep.outputFile=…`; `fluss-flink-2.2` is a shaded jar carrying `org.apache.fluss.client` (no separate fluss-client needed). The dump is read-only against the dev Fluss (metadata lookups only).
- Config defaults (from `SignalJobConfig.fromEnv`): `FLUSS_BOOTSTRAP_SERVERS=localhost:9123`, `FLUSS_DATABASE=default`, `CANDLE_TABLE=feature_candles_15s` (KV upsert — sole candle output; the `CANDLE_CURRENT_TABLE` key was deleted 2026-08-13 with `feature_candles_15s_current`), `SIGNAL_CANDIDATES_TABLE=Signal_Candidates`, `SIGNAL_CURRENT_TABLE=Signal_Candidates_current`, `PARALLELISM` default 1 (evidence convention: 16).

### Operator-ID mechanism (from `logs/candle-kv-replay-001/p6-evidence-2026-08-10.md`)

`StreamGraphHasherV2` BFS-walks from sorted source IDs; each node's hash folds in its BFS index + chain config + upstream hashes. New nodes shift every node processed after them. P6 final arrangement (candle KV sink emitted LAST) froze the stateful set for THAT code; the P6 historical IDs no longer match current code (post-P6 edits: StallGuardedSink wiring, R-298, checkpoint config). Sinks are stateless (`FlinkSinkWriter implements SinkWriter`, not `StatefulSinkWriter`) — sink ID changes orphan nothing. **Stage 0 baseline (2026-08-13, `logs/tracker-14/jobgraph-signal-baseline/job-vertices.txt`), the authoritative anchor:**

- `Source: raw-table-1 -> raw-validation | cbc357ccb763df2852fee8c4fc7d55f2` (STATEFUL — source)
- `candle-15s -> canonical-candle-filter | e883208d19e3c34f8aaf2a3168a63337` (STATEFUL — candle windows; chained vertex)
- `feature-candles-15s-current-kv-sink: Writer | 64ca712eb37af98fa4ecb0da1ab73dae` (stateless — Track A deletes)
- `feature-candles-15s-sink: Writer | 5810cf80e04eec437823a019a6af4c20` (stateless)
- `fingerprint-dedup | 9dd63673dd41ea021b896d5203f3ba7c` (STATEFUL — dedup cache)
- `signal-candidates-sink: Writer | 34ffd5126a7f2c583cc02eb4b474fdd0` (stateless)
- `signal-detection | 73cecb2424528f03e5e7967ff89ef48a` (STATEFUL — signal state)
  **Stateful set that must never move: `cbc357cc…`, `e883208d…`, `9dd63673…`, `73cecb24…`.**

### Constraints

- Governance (tracker-14 rule): no production code, DDL application, or live-cluster mutation until operator-gated steps. This plan's Stages 1–5 are repo-only.
- Live teardown (drop table id 92, stop p10 trio) requires explicit user confirmation at execution time.
- Long-run gate (user directive 2026-08-12): any test >10 min must be preceded by a ≤2-min smoke of the same machinery.
- Docs-first workflow: docs updated 2026-08-13 (trackers 13/14, P7/P10 plans, 04-signal-job.md, compute README, foundation, SQL README, business-logic contract); this plan covers the remaining authority docs in Stage 1.
- GitButler workspace (`gitbutler/workspace` branch): commit per stage via `but`.

## Review Handoff

- Original request: user approved implementation 2026-08-13 ("go ahead"); design locked: candles LOG-only, signals LOG + KV (`Signal_Candidates_current` PK `(instrument_token)`).
- Key decisions: 22-col layout frozen (no `SignalDetectionFunction` churn); Track A (retire) before Track B (new facility); one commit per stage; `StallGuardedSink` retained on all sinks; validator re-targeted (clean cutover, no alias).
- Non-goals: no candle migration/audit/conflict machinery (retired); no row-layout change; no live DDL application in Stages 1–5; no performance re-measurement (P7 authority unchanged).
- Open question (verify at Stage 3): whether `CanonicalCandlePolicy`/`CanonicalCandlePolicyTest` stay (used by `SignalJobConfig.requireCanonicalVersion` startup gate) or are deleted with the filter.
- Hidden context: none; this plan is self-contained.

## Development Approach

- Regular (not TDD): each stage edits code + its pin tests together, then runs the module suite before the next stage.
- Small focused changes; every code-change task updates or deletes its pin tests in the same stage.
- Update this plan when scope changes during execution.

## Testing Strategy

- Unit + pin tests per stage as listed; full module suites at Stages 3, 4, 5.
- JobGraphDump diff (stateful-ID stability) at Stages 3 and 4 against the Stage 0 baseline.
- Env-gated integration tests (live Fluss) remain gated; `SignalCurrentKvIdempotencyTest` follows the `CandleCurrentKvIdempotencyTest` pattern (scratch table, tagged `integration`, never touches platform tables).
- Long-run gate applies to any >10-min run.

## Progress Tracking

- `[x]` done; `+` new task; `BLOCKED:` blockers; keep in sync.

## Implementation Steps

### Stage 0: JobGraphDump baseline (BEFORE any code change)

**Why:** the checkpoint-restore anchor — prove later that stateful operator IDs never move.

- [x] Build compute + common (compute is NOT in the root reactor — R-272 comment in `code/pom.xml` L16–18; build it standalone from `code/02_services/02_compute`, parent + common already in `.m2`; rebuilt common first — the installed jar was stale vs source)
- [x] Build classpath: `cd code/02_services/02_compute && mvn -o dependency:build-classpath -Dmdep.outputFile=/tmp/compute-cp.txt` (52 flink/fluss/common/slf4j jars; staged copy in the baseline dir)
- [x] Run `JobGraphDump` **inside `01_docker_trading-net`** (host run fails: `fluss-tablet:9124` unresolvable from host) → `logs/tracker-14/jobgraph-signal-baseline/`
- [x] Verify 3 artifacts + record the stateful-ID set (see Operator-ID mechanism section): `cbc357cc…`, `e883208d…`, `9dd63673…`, `73cecb24…`

### Stage 1: Upstream authority docs (banner-annotate, no code)

**Why:** the conflict rule requires the requirement change recorded at requirements/contracts/decisions layers, not only implementation docs.

**Files (modify):**

- `docs/02_requirements/02-functional/02-storage.md` (table rows L84–86, REQ-FLS-006 heading + paragraph, REQ-FLS-007 paragraph — LOG v3 + current-state KV, candle KV RETIRED)
- `docs/04_contracts/02-storage.md` (L15 candle-KV retirement + L17 strategy sentence)
- `docs/01_project/04-decisions.md` (DEC-035 row added after DEC-034; historical DEC-034 text preserved)
- `docs/08_implementation/02-schema-storage.md` (L90 routing-key row + `Signal_Candidates_current` row + 2026-08-13 note after the 2026-08-10 note)
- `docs/02_requirements/04-data.md` (ownership-table row + `Signal_Candidates_current` subsection)
- `docs/02_requirements/02-functional/04-business-logic.md` (L16/L36 KV-overwrite caveats)
- `docs/03_architecture/00-arch-overview.md`, `01-technology-choices.md`, `02-data-pipeline.md`, `platform-architecture.md` (`Signal_Candidates_current` enumerations)

- [x] Annotate each stale site (KV→LOG v3 + new current-state table; candle KV retired; DEC-035 records the requirement change), same style as the 6 already-updated docs — executed 2026-08-13 across 10 files
- [x] Verify: no un-annotated `Signal_Candidates` KV / active candle-KV refs remain in `docs/` (grep clean; remaining matches are DEC-034 history, 04-business-logic L7 banner superseded by L9, and 04-signal-job implementation-history lines already covered by its re-scope banner)

### Stage 2: DDL + schema contract (one atomic commit)

**EXECUTED 2026-08-13** — commit `074f02f` (Change-ID `oqs`) on `stage2-signal-ddl-contract`: 9 files, ingestion pin tests 18/18 green. `ddl_apply.py` needed no change (no hardcoded table list — it derives the manifest from the DDL dir; manifest regenerated via `--force`).

**Why:** DDL and the descriptors/tests that pin it move together (cross-boundary pin habit) or pin tests break.

**Files:**

- Modify: `code/01_platform/02_sql/ddl/05_signal_candidates.sql`, `ddl/schema_manifest.json`, `code/01_platform/04_scripts/ddl_apply.py`, `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java`, `code/02_services/01_ingestion/src/test/java/com/trading/ingestion/SchemaAgreementTest.java`
- Create: `code/01_platform/02_sql/ddl/23_signal_candidates_current.sql`
- Delete: `code/01_platform/02_sql/ddl/22_feature_candles_15s_current.sql`

- [x] Rewrite `05_signal_candidates.sql` → LOG v3: drop `PRIMARY KEY (candidate_id) NOT ENFORCED`; `bucket.key` → `instrument_token`; header → v3 (supersede columns stay — 22-col layout frozen, audit linkage)
- [x] Create `23_signal_candidates_current.sql` → KV: PK `(instrument_token) NOT ENFORCED`, `bucket.num=16`, `bucket.key=instrument_token`, same 22 columns + datalake options as 05
- [x] Update `schema_manifest.json` (05 kind LOG + bucket key, add 23, remove 22) — regenerated by `ddl_apply.py --force` (21 tables at 2026-08-13 — now 24; no hardcoded list to edit)
- [x] Update `DdlBootstrap.java`: `Signal_Candidates` → LOG descriptor (no PK), register `Signal_Candidates_current` with the 22-col KV descriptor (`distributedBy(16, "instrument_token")`)
- [x] Update `SchemaAgreementTest` L178–179 → assert `Signal_Candidates` has NO primary key (LOG) + add `Signal_Candidates_current` KV pin (`PRIMARY KEY (instrument_token)`)
- [x] Run: `cd code && mvn -o test -pl 02_services/01_ingestion -am -Dtest=SchemaAgreementTest,DdlBootstrapSchemaAgreementTest -Dsurefire.failIfNoSpecifiedTests=false` — green (18 tests, 0 failures) before Stage 3

### Stage 3: Track A — retire candle KV (deletion)

**Why:** remove-before-add; pure deletion, no schema change to live LOG, smallest risk first.

**Files:**

- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`, `SignalJobConfig.java`, `CandleTableContractValidator.java`, `CandleTableColumns.java` (javadoc), `code/02_services/02_compute/src/main/java/com/trading/compute/telemetry/ComputeOtlpEmitter.java`, `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/SignalJobConfigTest.java`
- Delete: `signaljob/CanonicalCandleFilterFunction.java`, `tools/CandleMigrationTool.java`, `tools/CandleMigrationBatchJob.java`, `code/common/src/test/java/com/trading/common/schema/CandleCurrentDdlContractTest.java`, compute tests `CandleCurrentKvIdempotencyTest.java`, `CanonicalCandleFilterFunctionTest.java`, `tools/CandleMigrationToolTest.java`, `tools/CandleMigrationBatchJobTest.java`
- Keep (verify references first): `CanonicalCandlePolicy.java` + `CanonicalCandlePolicyTest.java` if `SignalJobConfig.requireCanonicalVersion` still calls `isCanonical`; `CandleTableSchema.java` (15-col LOG contract + version constants). If no caller remains, delete both.

- [ ] `SignalJob.java`: remove the candle KV block L263–296 (`canonical-candle-filter` + `feature-candles-15s-current-kv-sink`); update class javadoc L26–44 (candle LOG sole output)
- [ ] `SignalJobConfig.java`: remove `candleCurrentTable` field, `CANDLE_CURRENT_TABLE` env line, related javadoc; fix the record constructor call sites (fromEnv)
- [ ] `SignalJobConfigTest.java`: remove `defaultsCandleCurrentTableAndHonorsOverride`
- [ ] `ComputeOtlpEmitter.java`: remove `KV_FILTERED_NON_CANONICAL` + `recordKvFilteredNonCanonical()` (only caller deleted)
- [ ] `CandleTableContractValidator.java`: drop candle-KV validation; keep candle-LOG checks
- [ ] `CandleTableColumns.java`: update javadoc (no KV twin)
- [ ] Run: `cd code && mvn -o test -pl 02_services/02_compute -am` — green
- [ ] Re-run JobGraphDump → `logs/tracker-14/jobgraph-signal-post-track-a/`; diff `job-vertices.txt` vs Stage 0 baseline: stateful set unchanged (only candle KV sink + canonical filter gone)

### Stage 4: Track B — signal LOG/KV dual-sink

**Why:** the new facility — LOG audit + KV current-state per instrument.

**Files:**

- Modify: `SignalJobConfig.java`, `SignalJob.java`, `CandleTableContractValidator.java` → `TableContractValidator.java`, `ComputeOtlpEmitter.java`, `ComputeOtlpEmitterTest.java`, `SignalJobConfigTest.java`, `SignalCandidatesTableColumns.java`
- Create: `signaljob/CanonicalSignalPolicy.java` + `CanonicalSignalPolicyTest.java`, `signaljob/CanonicalSignalFilterFunction.java` + `CanonicalSignalFilterFunctionTest.java`, `signaljob/TableContractValidatorTest.java`

- [x] `SignalJobConfig.java`: added `signalCurrentTable` field + `SIGNAL_CURRENT_TABLE` env (default `Signal_Candidates_current`); canonical-identity defaults now reference `SignalCandidatesTableColumns` constants (`SCHEMA_VERSION_V2="2"`, `CANONICAL_STRATEGY_ID="simple-breakout"`, `CANONICAL_STRATEGY_VERSION="1.0.0"`, `CANONICAL_RULE_ID="breakout-20-bullish-trend"`); javadoc documents the dual-sink (LOG keeps every signal, KV filtered + counted); `SignalJobConfigTest` default + override legs green (41 tests, 0 failures)
- [x] `SignalJob.java`:
  - `signal-candidates-sink` serialization `(false,false)` → `(true,true)` (LOG append)
  - added `signal-candidates-current-sink` after it, `RowDataSerializationSchema(false,false)` (KV upsert), wrapped in `StallGuardedSink` with the same `client.request-timeout` / `client.writer.retries=2` options — LAST node of the `signals` stream (BFS-order discipline from P6)
  - class javadoc updated (topology → signal dual-sink DEC-035, serialization flags per sink kind)
  - `preflightTableContracts`: validates `feature_candles_15s` KV (exact PK `(instrument_token, window_start)`; RE-SCOPED as-built 2026-08-13 — the plan as written said LOG, superseded same-day by the KV-only conversion) + `Signal_Candidates` LOG (no PK, `bucket.key=instrument_token`) + `Signal_Candidates_current` KV (PK exactly `[instrument_token]`, bucket key subset of PK, 16 buckets, 22 cols); logs `schemaReport` per table (DDL-vs-live nullability divergence marker)
- [x] `CandleTableContractValidator.java`: re-targeted → three tables above (SIGNAL-SCHEMA-001 / CANDLE-SCHEMA-002); renamed to `TableContractValidator`, references migrated cleanly (no alias); `schemaReport(TableInfo, List<Boolean> nullableInDdl)`; `TableContractValidatorTest` 22 cases green
- [x] `CanonicalSignalPolicy.java`: canonical check on signal `(schema_version, strategy_id, strategy_version, rule_id)`; `CanonicalSignalFilterFunction` wired before the signal KV current sink only (LOG sink keeps every signal); counter `compute.signal.kv.filtered.noncanonical` added to `ComputeOtlpEmitter` (DELTA non-monotonic sum, drained pre-POST, present at 0); `CanonicalSignalPolicyTest` (4) + `CanonicalSignalFilterFunctionTest` (3) + `ComputeOtlpEmitterTest` (16) green
- [x] Run: compute suite green 2026-08-13 — 182 tests, 0 failures, 11 skipped (env-gated ITs, incl. `SignalJobOperatorUidTest`). Note: `mvn -o test -pl 02_services/02_compute -am` from `code/` cannot run — compute is excluded from the root reactor (R-272); executed module-locally in `code/02_services/02_compute` instead (same gate as Stage 3)
- [x] JobGraphDump re-run → `logs/tracker-14/jobgraph-signal-post-track-b/`. Result: the literal "stateful IDs unchanged vs baseline" criterion is **impossible** — Flink derives IDs from the transitive topology hash, and the baseline's `candle-15s` vertex CHAINED `canonical-candle-filter`, so Stage 3's filter deletion alone shifted that stateful vertex and everything downstream (post-track-a was never captured). Fix (documented in `logs/tracker-14/jobgraph-signal-rescope-evidence-20260813.md`): explicit `.uid(...)` on all 9 operators — restore anchors are now UID-derived and durable; `SignalJobOperatorUidTest` (env-gated) pins the UID set (green in-container 2026-08-13); pre-rescope checkpoints were invalidated by DEC-035 regardless (no restore-into-new-topology path existed); scratch tables `sig4_log_scratch`/`sig4_current_scratch` created for the preflight, used for the dump, then dropped

### Stage 5: Contract tests + verification

- [x] Create `code/common/src/test/java/com/trading/common/schema/SignalCurrentDdlContractTest.java` (3 cases, green 2026-08-13): LOG DDL stays LOG (no PK — a PK flips it to KV and breaks the audit trail), KV DDL PK exactly `(instrument_token)`, both DDLs agree column-for-column (22 cols, identical types), `bucket.key=instrument_token` + 16 buckets + 7d retention on both
- [x] Create `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/SignalCurrentKvIdempotencyTest.java` (env-gated `COMPUTE_INT_TEST_SIGNAL_KV=true`, scratch table, `@Tag("integration")`): same-instrument upserts converge to one row, last-write-wins, never touches platform tables — green in-container 2026-08-13 (1/1)
- [x] Re-scope `CandleGraphReplayIntegrationTest.java`: candle LOG-only + signal LOG/KV; replay twice → signal LOG grows, `Signal_Candidates_current` key count frozen — green in-container 2026-08-13 (3/3, 134.9 s: dual-sink replay, P6.2 preflight fail-closed, data-quality/window semantics). Shared scratch plumbing extracted to `ScratchTables` (single DDL-mirror source, both gated tests); `SignalJobOperatorUidTest` is now hermetic — the strict validator correctly rejects the dev cluster's legacy `Signal_Candidates` KV v2, so the UID test preflights contract-correct scratch tables instead of Stage 6-dependent platform metadata
- [x] Run full suites module-locally (R-272: compute is excluded from the root reactor): compute 182/0/11 skipped, common 104/0/0, ingestion 180/0/7 skipped — all green 2026-08-13. Re-measured 2026-08-13 (fresh module-local runs, surefire totals): compute **184 run / 0 fail / 12 skipped**, common **112 / 0 / 0**, ingestion **188 / 0 / 7 skipped** — skips are env-gated classes (see Tests note); `SignalCurrentKvIdempotencyTest` reports 0/0 under its class-level env gate
- [x] Final JobGraphDump diff evidence file: `logs/tracker-14/jobgraph-signal-rescope-evidence-20260813.md` (baseline vs post-A vs post-B tables) — written at Stage 4 closeout, 2026-08-13
- [x] Long-run gate: gated in-container battery (5 tests) finished in 155 s wall — below the 10-min smoke threshold, no pre-smoke required

### Stage 6 (Post-Completion, operator-gated): live cluster

- [x] Drop live dev table `feature_candles_15s_current` (id 92) — user-confirmed ("ok now go ahead for stage 6"); `DropTable.java` trading-net procedure, `DROP_STATUS=OK` 2026-08-13. Also dropped legacy `Signal_Candidates` KV v2 (id 91, PK `candidate_id`) for the v3 re-create — preflight showed neither carried `table.datalake.enabled` (no R2 orphan). Evidence: `logs/tracker-14/p6-stage6-live-ddl-evidence-20260813.md`
- [x] Apply new DDLs to dev (gated path: `schema_manifest.json` + evidence-gated application) — 2026-08-13: `ddl_apply.py --apply-verified --matrix-evidence logs/tracker-14/p6-stage5-contract-evidence-20260813.md` → version gate PASS, manifest current (21 tables at 2026-08-13 — now 24), apply stage reached (exit 0). Real application via new probe `logs/tracker-14/ApplySignalDdl.java` (Fluss 0.9.1 has no SQL client; the script's apply arm is a documented stub): `CREATE_STATUS=OK` for `Signal_Candidates` (LOG v3, id 607) + `Signal_Candidates_current` (KV v2, PK `instrument_token`, id 608), 22-col schemas mirroring `ScratchTables`, 16 buckets, `table.log.ttl=7d`, lake keys copied from live `raw_table_1` ZK (`table.datalake.{enabled,format,freshness,auto-compaction}`). Live proof: new same-package probe `PreflightSignalTables` ran the production fail-closed gate `SignalJob.preflightTableContracts` against the platform tables → `PREFLIGHT_STATUS=PASS tables=feature_candles_15s,Signal_Candidates,Signal_Candidates_current`
- [x] Stop the idle p10 rehearsal trio (`p10-zookeeper-1`, `p10-fluss-coordinator-1`, `p10-fluss-tablet-1`) — user-confirmed as part of Stage 6; all three `Exited (143)` (SIGTERM, graceful) 2026-08-13. Final ZK registry: 22 tables, no scratch leftovers, `feature_candles_15s_current` absent
- [x] Commit discipline: `but` commit per stage (0→5); teardown evidence recorded in tracker 14. — COMPLETE 2026-08-13: Stage 2 DDL contract `074f02f` (`stage2-signal-ddl-contract`); Stage 6 live-DDL doc closeout `6b08636` (`stage6-signal-live-ddl`); retro commits swept from the pre-existing uncommitted tree after user go-ahead — Stage 1 docs `a463d94`+`e0c2e31` (`stage1-dec035-docs`), Stage 3 Track-A `1a99521`+`8c1e9fa` (`stage3-dec035-candle-track-a`), Stage 4 Track-B `44d9b2a`+`cd868a5` (`stage4-dec035-signal-track-b`), Stage 5 tests `926208e` (`stage5-dec035-signal-tests`), all stacked on `stage6-signal-live-ddl`; foreign work (P7 bench, ingestion, compose, Eclipse metadata, open_code_review_reports) left uncommitted in zz. Teardown evidence: `logs/tracker-14/p6-stage6-live-ddl-evidence-20260813.md`.

## Technical Details

- Serialization: LOG append = `RowDataSerializationSchema(true, true)`; KV upsert = `RowDataSerializationSchema(false, false)` (INSERT RowKind → UPSERT for KV writer, chosen from live table metadata at `FlussSink.build()`).
- Stall guards: every sink stays inside `StallGuardedSink` with `client.request-timeout = SINK_WRITE_STALL_TIMEOUT_MS ms` + `client.writer.retries=2` (R-298-era hang containment; identical on all sinks).
- Operator-ID discipline: new sinks are appended LAST on their stream, after `signal-detection` is attached (P6 BFS-order evidence) so stateful IDs stay pinned. Sinks are stateless; ID changes there are checkpoint-safe.
- Startup gates preserved unchanged: `STATE_RECOVERY_PATH` / `ALLOW_FULL_REPLAY` fail-closed gate, `RETAIN_ON_CANCELLATION`, pinned checkpoint contract.

## Post-Completion

**Manual verification (after Stages 5, before Stage 6):**

- Review the three JobGraphDump diffs and confirm the stateful-ID set never moved. — **SUPERSEDED 2026-08-13** (see Stage 4 JobGraphDump item above): literal ID preservation is impossible (transitive topology hash); replaced by the explicit UID-anchor contract, enforced by `SignalJobOperatorUidTest` (gated green in-container)

**External system updates:**

- Live dev cluster: drop table id 92, apply 05-v3 + 23 DDLs, drop p10 trio — **EXECUTED 2026-08-13** (Stage 6, user-confirmed): id 92 + legacy `Signal_Candidates` KV v2 dropped, `Signal_Candidates` LOG v3 (id 607) + `Signal_Candidates_current` KV (id 608) created lake-enabled, p10 trio stopped, platform-table preflight PASS. Evidence: `logs/tracker-14/p6-stage6-live-ddl-evidence-20260813.md`. Commit state: Stage 6 doc closeout committed 2026-08-13 — `6b08636` on `stage6-signal-live-ddl` (README Stage 6 row, plan build-plan section, tracker register + production checklist, DDL blocker note); plan item 734 COMPLETE (per-stage retro-commits committed 2026-08-13, see item).
