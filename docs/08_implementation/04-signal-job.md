# Signal Job

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | **Slice 1 (raw source → validation → fingerprint dedup → 15 s event-time candles → `feature_candles_15s`) implemented; 25 signal-job tests green; live smoke verified 2026-08-09** (see Slice 1 evidence below). **Slice 2.1 (MVP signal detection → `Signal_Candidates` KV records, DEC-034) implemented; 34 signal-job tests green; live smoke verified 2026-08-10** (see Slice 2.1 section below). Slot-scoped safety consumer (plan Amendment) implemented and live-verified — SAFETY-INT-001 passed 2026-08-09, see Safety consumer section below. **CANDLE-KV-REPLAY-001 (2026-08-10): candle dual-sink (`feature_candles_15s_current` KV projection) + fail-closed startup gate + offline migration tool implemented; unit/integration tests green; historical load and live cutover blocked pending data-ops decision on 25 replay-conflict keys — see `13-candle-log-kv-replay-safety.md`.** Slices 2.2+ (forming-bar handoff, Business Logic internals) and 3 (Ranking/Reservations/Decisions) not started. |
| Owner | Compute and Strategy Teams |
| Requirements | `REQ-FC-*`, `REQ-SS-*`, `REQ-RNK-*` |
| Contracts | `docs/04_contracts/03-compute.md`, `04-business-logic.md`, `10-ranking.md` |
| Job topology | One Signal job containing Compute, Business Logic, and Ranking |
| Separate job | Babysitter only |

**Current-phase envelope:** build and validate on the approved 1,024-instrument / single-connection configuration (20,480 ticks/s at 20 Hz per instrument). The 3,000-instrument / 60,000 ticks/s baseline and 90,000 ticks/s peak stay deferred (`PERF-PROD-60000-001` / `PERF-PROD-90000-001`, AC-FC-007/011); the budgets below are 3,000-instrument production targets, not this phase's acceptance.

**DDL bootstrap:** production application of the compute output tables (`feature_candles_15s`, `feature_candles_15s_current`, `Signal_Candidates`, `Ranking_Results`, `Trade_Decisions`, `Portfolio_Reservations`) stays behind the `make ddl` version gate (`12-version-compatibility-evidence.md`). For dev this is already solved: `DdlBootstrap.ALL_TABLES` registers the full platform table set (21 tables, incl. the DDL-22 KV projection `feature_candles_15s_current`), but `ensureTables` creates **only the 3 ingestion-owned tables** (`raw_table_1` with the exact v2 schema, `suspected_discontinuities`, `ingestion_quarantine`); the compute tables are provisioned by the offline DDL gate (`ddl_apply.py` / `schema_manifest.json`) — a runtime bootstrap must never create them. `verifyTables` column-count-checks owned tables and existence-checks the rest. Note the offline-gate precedent used for `Safety_Halt_Requests` v3: datalake properties are skipped on the local cluster (no lake catalog).

### Job graph

```text
Fluss raw_table_1 source
→ schema/validity filter
→ event-time assignment and watermark
→ keyBy(instrument_token)
→ bounded fingerprint dedup
→ forming-bar/window state
├─ final 15-second candle sink            (feature_candles_15s — Slice 1, LOG)
├─ final 15-second candle KV sink         (feature_candles_15s_current — CANDLE-KV-REPLAY-001, upsert PK instrument_token+window_start; last out-edge of the candle branch)
└─ closed-candle signal detection         (Slice 2.1, DEC-034: MVP placeholder rule)
   └─ Signal_Candidates KV upsert sink    (side=BUY / action=ENTRY / order=MARKET)
   (future) typed forming-bar update (closed-candle + forming-bar)
   (future) business rules/candidate state (keyed by instrument_token)
   (future) candidate audit sink
   (future) repartition eligible candidates by portfolio_id
   (future) in-operator ranking/reservation state (serialized per portfolio_id)
   │  with Portfolio_Reservations, candidate bounds, supersession
   ├─ ranking audit sink
   └─ immutable Trade_Decisions sink
```

No feature-table read-back, candidate Fluss round trip, separate ranking job, or separate feature-compute job exists in MVP. Candidate detection is instrument-keyed; ranking/reservation is portfolio-keyed after repartition within the same job.

MVP SHALL NOT use CEP. No `flink-cep` dependency, CEP operator, CEP job, or `org.apache.flink.cep` import is permitted. Configuration values in section 2 of [`01_plan.md`](./01-foundation.md) SHALL be validated at startup; any deviation SHALL fail job submission.

### Latency budget per operator

Approximate per-tick processing cost at 60,000 ticks/s baseline. Used to diagnose SLO misses (`p99 < 100 ms` trigger-tick-to-commit; `REQ-RNK-006`). Superseded by `PERF-PROD-60000-001`.

| Operator | Per-tick cost | Notes |
| --- | --- | --- |
| Fluss source read | ~0.1-0.5 ms | Deserialization + network from tablet server |
| Changelog filter | ~0.01 ms | Simple row-kind check (INSERT/UPDATE_AFTER only) |
| Row → typed mapping | ~0.05 ms | Field extraction + metric emission |
| Dedup RocksDB lookup | ~0.01-0.05 ms | Point lookup; L1 block cache hit when state is RAM-resident |
| Window accumulator update | ~0.005 ms | In-memory hash map OHLCV update |
| Forming-bar detection | ~0.01 ms | Boundary check + typed event construction |
| Strategy evaluation | TBD (keep simple) | Instrument-keyed; fires per forming-bar update |
| **Per-tick hot path subtotal** | **~0.2-0.6 ms** | Everything above fires per tick |
| Ranking burst (per portfolio) | ~25-50 ms | Fires ~1×/instrument/15s; heap sort over 50-100 candidates; repartitioned by `portfolio_id` |
| Winner commit to Fluss | ~0.5-2 ms | Append acknowledgement from tablet server |

The ranking burst is the dominant p99 term because it batches every 15 seconds. Per-tick overhead alone would support 1,000-5,000 ticks/s per core; the bottleneck is ranking, not ingestion or dedup.

### Suggested operator boundaries

| Operator | Key/state | Responsibility |
| --- | --- | --- |
| `RawValidation` | Stateless | Schema version, validity, event classification |
| `FingerprintDedup` | Instrument + fingerprint scope | Bounded first-seen state and duplicate metrics |
| `CandleAndFormingBar` | Instrument | Event-time window and forming accumulator |
| `BusinessLogic` | Instrument/strategy | Active setup state and candidate emission |
| `RankingAndReservation` | `portfolio_id` | Deterministic scoring, capacity, winner selection, reservation lifecycle with `reservation_id`, transition versions, stale-update rejection |
| `AuditAndDecisionSinks` | Connector transaction boundary | Immutable event/decision outputs |

Actual chaining is performance-tested; logical boundaries remain explicit for metrics and state ownership.

**Chaining guidance (starting recommendation from operator cost model):** Disable chaining at the `RankingAndReservation` boundary to provide resource isolation for the ranking burst (dominant p99 latency term; see latency budget above). Consider disabling at `AuditAndDecisionSinks` to provide a separate checkpoint transaction boundary from compute. Keep `RawValidation → FingerprintDedup → CandleAndFormingBar → BusinessLogic` chained — per-record overhead is negligible and a network shuffle provides no benefit for these instrument-keyed operators. Final decision is performance-test driven.

### Configuration contract

| Key | Requirement |
| --- | --- |
| `FLINK_VERSION` / `FLUSS_CONNECTOR_VERSION` | Exact compatibility matrix IDs |
| `RAW_SCHEMA_VERSION` | Accepted input version/range |
| `WATERMARK_OUT_OF_ORDER_MS` | Default 5000; change only through tested profile |
| `ALLOWED_LATENESS_MS` | Default 5000; change only through tested profile |
| `SOURCE_IDLE_MS` | Default 15000 per source partition |
| `DEDUP_TTL_MS` | Fixed at `300000` (5 minutes); reject startup for any other value in MVP |
| `CANDLE_WINDOW_MS` | Fixed at `15000`; reject startup for any other value in MVP |
| `CANDLE_TABLE` | Candle LOG sink table, default `feature_candles_15s` (also used by `CandleMigrationTool`) |
| `CANDLE_CURRENT_TABLE` | Candle KV sink table, default `feature_candles_15s_current` (CANDLE-KV-REPLAY-001); schema must match `CandleTableSchema` v2, PK `(instrument_token, window_start)`, 16 buckets, `bucket.key=instrument_token` — enforced at startup by `CandleTableContractValidator` |
| `STATE_RECOVERY_PATH` | Required for normal restarts (checkpoint/savepoint dir). Absent → startup fails closed unless `ALLOW_FULL_REPLAY=true` (CANDLE-KV-REPLAY-001 startup gate) |
| `ALLOW_FULL_REPLAY` | Explicit break-glass for deliberate offset-0 replay. **Never `true` in a normal production launch**; replay without restore is what caused the 2026-08-10 incident |
| `CHECKPOINT_INTERVAL_MS` | Fixed at `10000`; Signal and Babysitter jobs use this value |
| `CHECKPOINT_TIMEOUT_MS` | Fixed at `30000`; Signal and Babysitter jobs use this value |
| `MAX_CONCURRENT_CHECKPOINTS` | Fixed at `1`; Signal and Babysitter jobs use this value |
| `STATE_BACKEND_TO_BE_PINNED` | Version-compatible managed state backend |
| `S3_CHECKPOINT_URI_TO_BE_DEFINED` | Production encrypted checkpoint/savepoint storage |
| `STRATEGY_VERSION` | Included in candidates/decisions |
| `RANKING_VERSION` | Included in ranking results/decisions |
| `RESERVATION_POLICY_VERSION` | Included in audit and restored state |
| `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` | Fixed at `1`; do not forward another active candidate for that instrument |
| `CHECKPOINT_RESTART_STRATEGY` | Fixed-delay: `RESTART_MAX_ATTEMPTS=3`, `RESTART_DELAY_MS=30000`, failure action = fail job. Deployment SHALL reject unbounded retry. Note: the current implementation defaults these two values (tuning keys, not `requirePinned`); pinning them is a follow-up so deployment cannot silently raise attempts. |

Deployment SHALL reject unbounded or too-short `DEDUP_TTL`, missing production checkpoint storage, unbounded checkpoint restart retry, and any deviation from pinned values.

### Event-time contract

- Event time is the verified UTC broker timestamp.
- Events without verified event time do not advance watermarks.
- Watermark and idleness apply per source partition.
- One final candle emits at first window fire (watermark ≥ `window_end`). The `emitted` window-state flag makes any allowed-lateness re-trigger a no-op (late-within-lateness folds into the accumulator and is counted, never re-written); the row is final from its first write. (The "emits after `window_end + allowed_lateness`" phrasing in the contract means the finalization boundary — the candle is not corrected after that point.)
- Later records (beyond `window_end + allowed_lateness`) are dropped by the window operator; they are not counted yet (beyond-lateness counter is pending telemetry, see Required telemetry below).
- Open/close ties use the versioned deterministic fingerprint ordering.
- Empty windows emit no row.

### Dedup state

State key contains only fingerprint version, scope, and event fingerprint. State value contains only first-seen timestamp and expiry timestamp. Dedup state SHALL NOT contain raw bytes, decoded raw fields, candle values, candidate values, or an event object. State is deleted when the event-time watermark reaches its `300000` ms expiry (implementation detail below).

**Expiry implementation (Flink 2.2.1 constraint):** event-time state TTL was removed in Flink 2.2.1 (only `ProcessingTime` remains in `StateTtlConfig.TtlTimeCharacteristic`), so expiry is enforced with explicit event-time timers: one timer per fingerprint entry registered at `first_seen + TTL`, plus a compact expiry index (`expiry → state keys`, `fingerprint-dedup-expiry` map) so the timer callback knows which fingerprints to delete. Deletion timing is watermark-driven — the timer fires when the watermark reaches the expiry instant, which with the bounded out-of-orderness watermark (max seen event time − 5 s) is at most `WATERMARK_OUT_OF_ORDER_MS` behind nominal expiry. Entries are never deleted early; the logical expiry instant is `first_seen + TTL` exactly. The index adds one list entry per live fingerprint — bounded with the dedup state itself (see budget below; it roughly doubles the raw per-entry estimate).

First event proceeds; later candidate within TTL increments duplicate metrics and does not affect candle/business state.

Identical legitimate events may be collapsed; this limitation must remain visible in metrics and documentation.

### Dedup state budget

At the 60,000 ticks/s baseline workload (3,000 instruments; 20 ticks/s/instrument average):

| Metric | Value | Derivation |
| --- | --- | --- |
| Steady-state entries | ~18,000,000 | 60,000 ticks/s × 300s TTL |
| Raw state size (per entry ~32 bytes) | ~576 MB | Fingerprint + first-seen timestamp + expiry timestamp |
| Expiry index (`expiry → key list`) | ~+100-150% of raw state | One list entry per live fingerprint; bounded with the dedup state itself (measured on the 1,024-instrument dev envelope: dedup state ~2 MB total) |
| RocksDB overhead (LSM amplification) | ~1.7-2× | Block index, bloom filter, SST metadata |
| Estimated total state | **~1.3 GB** | Plateaus after warmup; does not grow unbounded |

These are derived from the workload envelope and RocksDB state model, not from `PERF-PROD-60000-001`. The benchmark run is the authority. The estimate confirms that 48 GB VMs are over-provisioned for dedup state.

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

### Ranking and reservation protocol

Ranking consumes typed active candidate snapshots in-process. It records normalized inputs, weights, score, deterministic tie-break, rank, selected flag, rejection reason, ranking version, reservation ID/version, and evaluation ID.

Default MVP capacity:

- At most one reserved/open trade per instrument.
- At most three total reserved/open positions.
- At most one per strategy.

`RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` consume capacity. Reservation state uncertainty suppresses new decisions and signals Executor safety state.

A same winner with unchanged executable content produces audit only. Changed executable parameters require a new `instruction_id` and supersession relation.

### Output consistency

One Flink checkpoint covers job state and sinks, but cross-table atomic visibility is not assumed. Every output carries stable IDs and versions so consumers can tolerate partial visibility and reconcile.

`Trade_Decisions` is immutable. Executor never mutates it.

### Restore and degradation

Restore must recover source offsets, dedup state, windows, forming bars, active candidates, ranking, and reservation state consistently. If state compatibility or continuity cannot be proven:

1. Job reports not ready/degraded.
2. New decisions are suppressed.
3. Executor is signalled to halt.
4. Operator reconciliation/savepoint policy is invoked.

### Startup mode gate (CANDLE-KV-REPLAY-001)

Normal restarts SHALL pass `STATE_RECOVERY_PATH` (a Flink checkpoint/savepoint dir); the job then restores offsets and state. If `STATE_RECOVERY_PATH` is absent the job fails closed at startup with an explicit mode error — unless the operator deliberately sets `ALLOW_FULL_REPLAY=true` (documented break-glass; logs and emits `compute.startup.mode` = `FULL_REPLAY`). RESTORE mode emits `compute.startup.mode` = `RESTORE`. The two modes are mutually exclusive and never defaulted to replay. This gate is what turns the 2026-08-10 incident's no-restore restart from a silent replay into a refused startup.

### Checkpoint sizing

Estimated checkpoint metrics at the 60,000 ticks/s baseline (3,000 instruments; 20 ticks/s/instrument average):

| Metric | Estimate | Derivation |
| --- | --- | --- |
| Checkpoint size (steady state) | ~600 MB – 1 GB | Dominated by dedup state (~1 GB); window state ~120 KB (3K instruments × ~40 bytes); candidate/ranking state ~5 MB |
| Checkpoint write time | ~2-5 seconds | SSD write at ~500 MB/s; incremental checkpoints write only changed SST files |
| Restore time | ~5-15 seconds | Read back RocksDB state from S3/local checkpoint; well within 30s data-path recovery target (REQ-FC-008) |

These estimates confirm that the configured 10s checkpoint interval and 30s timeout are conservative even at peak (90,000 ticks/s). All figures are derived from the RocksDB state model, not measured — superseded by `PERF-PROD-60000-001`.

### Job submission contract

- Build one versioned job JAR.
- Upload once.
- Submit Signal job with explicit entry class/args.
- Submit Babysitter separately.
- Capture job IDs and artifact checksum.
- Treat repeated submission idempotently.
- READY requires both required jobs RUNNING and checkpointing.

### Required telemetry

Source throughput/lag, invalid events, dedup candidates/hits/state size, late events, watermark lag, window/candle rates, forming updates, candidate/ranking/decision rates, reservation states/conflicts, operator busy/idle/backpressure, sink latency, checkpoint duration/size/failures, restore count, and state compatibility failures.

**Implemented in Slice 1** (per-operator counters): `compute.invalid.rows` + `compute.invalid.byReason.*` (RawValidationFunction), `compute.dedup.first` / `compute.dedup.duplicates` (FingerprintDedupFunction), `compute.candles.emitted` / `compute.candles.late.updates` (CandleEmitFunction). Checkpoint duration/size/failures come from Flink's built-in checkpoint metrics.

**Pending (Slice 2+ telemetry):** source throughput/lag, dedup state size, beyond-lateness discard counter (REQ-FC-006 discard metric with instrument/window/lateness/reason), watermark lag, forming-update/candidate/ranking/decision rates, reservation states/conflicts, sink latency, restore count, state compatibility failures.

**CANDLE-KV-REPLAY-001 observability (deferred through existing telemetry):** dedicated per-sink counters (LOG rows written vs KV upserts, KV duplicate/conflict count, replay-vs-live output_ts gap) are **deferred** — they are not implemented as new metrics. Existing telemetry covers the operational need: `compute.startup.mode` gauge (RESTORE/FULL_REPLAY), `compute.candles.emitted` / `compute.candles.late.updates` counters, Flink built-in checkpoint duration/size/failure metrics, and offline `CandleMigrationTool` audits for LOG-vs-KV convergence checks. A dedicated KV-replay metric can be added later without contract change.

### Required tests

- `SIG-UNIT-001` deterministic fingerprint tie ordering.
- `SIG-UNIT-002` candle aggregation and empty windows.
- `SIG-UNIT-003` dedup TTL validation (exactly 300000 ms; reject other values).
- `SIG-UNIT-004` candidate identity/supersession.
- `SIG-UNIT-005` deterministic ranking/tie-break.
- `SIG-UNIT-006` reservation capacity transitions.
- `SIG-UNIT-007` no `flink-cep` dependency or `org.apache.flink.cep` import.
- `SIG-UNIT-008` dedup state contains only fingerprint + timestamps; no raw bytes or event objects.
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
| Candle DDL pair → both sinks | `CandleCurrentDdlContractTest` (common) asserts `22_feature_candles_15s_current.sql` matches the LOG DDL column-for-column with PK `(instrument_token, window_start)` + 16 buckets + `bucket.key=instrument_token` | `CandleTableContractValidatorTest` asserts live-table metadata preflight (schema v2, PK subset rule, bucket layout) rejects drift at startup |
| Candle versioning → KV upsert | `CanonicalCandlePolicyTest` asserts `(schema_version, algorithm_version, configuration_version)` canonical check | `CandleCurrentKvIdempotencyTest` (env-gated) proves same-key upsert converges to one row with last-write-wins `output_ts` |
| Config → job behavior | `SignalJobConfigTest` asserts pinned values and reject-on-deviation | `SignalJob` reads the same config object; restore path pinned by `honorsStateRecoveryPathOverride` |

Rule of thumb: when you change a producer format (Go packet, DDL file, `RawTableColumns`, `CandleTableColumns`, a pinned config key), update the consumer-side pin test in the **same change**, not as a follow-up. One-sided changes fail CI or the live gate rather than drift silently. This is the documented habit for the process-rule pass (2026-08-10) and is the pattern `RawTable1DdlSchemaVersionTest` establishes.

### JVM and memory configuration

- Java max heap SHALL equal 65% of the container memory limit (`JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT=65`).
- Container limit minus Java max heap SHALL be at least 35% (`NON_HEAP_MEMORY_RESERVE_PERCENT=35`).
- Critical alert at or above 85% total container memory (`CONTAINER_MEMORY_ALERT_PERCENT=85`).
- Verify at startup that the container memory limit minus maximum heap is at least 35% of the container memory limit.

### Concrete sizing (48 GB VM)

Derived for a Flink TaskManager on a 48 GB VM. All numbers are starting points, not measured — superseded by `PERF-PROD-60000-001`.

For a 48 GB container memory limit:

| Resource | Value | Notes |
| --- | --- | --- |
| Container memory limit | 48 GB | Explicit Swarm/Compose limit |
| Java max heap (`-Xmx`) | **8 GB** | Modest because working state lives in RocksDB (direct memory), not heap |
| Direct memory (`-XX:MaxDirectMemorySize`) | **30 GB** | RocksDB block cache + Flink network buffers + Fluss client buffers |
| OS reserve | **~10 GB** | OS page cache, off-heap allocations, Fluss client overhead |
| JVM heap percent of container | ~17% | Intentionally lower than the generic 65% rule — RocksDB dominates |
| GC | `-XX:+UseG1GC -XX:MaxGCPauseMillis=20` | Low pause target to protect p99 latency |
| Container memory alert at 85% | ~40.8 GB | `CONTAINER_MEMORY_ALERT_PERCENT` emits critical at this threshold |

For non-Flink Java containers (Ingestion, Action Capture, Executor), use the generic formula (`JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT=65`). The Flink TaskManager split is different because RocksDB uses direct/native memory for its block cache and SST file buffers, not heap.

### Implementation checklist — State compactness (from [`01_plan.md`](./01-foundation.md) Task 3)

Before code is accepted, verify each item:

1. Raw ticks keyed by `instrument_token` before duplicate checking and candle calculation.
2. Duplicate-state key contains only fingerprint version, fingerprint scope, and event fingerprint.
3. Duplicate-state value contains only first-seen timestamp and expiry timestamp.
4. Duplicate state does not contain raw bytes, decoded raw fields, candle values, candidate values, or an event object.
5. Duplicate state deleted exactly when its 300000 ms expiry is reached.
6. Active candle state contains only open, high, low, close, volume, tick count, first order key, last order key, window start, and window end.
7. Active candle state does not contain a list, collection, array, or map of individual ticks.
8. One final 15-second candle written only after configured watermark/finality rule passes.
9. Active candle state deleted by window cleanup after watermark passes `window_end + allowed_lateness` (final row written at first fire; no correction after finalization).
10. Forming-bar data passed directly to business logic in the same Signal job; not written and re-read through Fluss.
11. `flink-cep` dependency removed from `code/02_services/02_compute/pom.xml`.
12. No CEP API usage, CEP operator, CEP table, or CEP job in MVP.

#### Task 3 acceptance checks

- 15-minute tests at the variable 60,000 ticks/s average baseline and 90,000 ticks/s peak (3,000 instruments; every instrument ≤30 ticks/s) report no state object containing raw packet bytes or a list of ticks.
- Duplicate state for expired tick fingerprint is absent after its expiry timer runs.
- One final candle per non-empty instrument/window and no correction candle after finalization.
- Compute module has no `flink-cep` dependency and no `org.apache.flink.cep` import.

### Implementation checklist — Candidate bounding and in-job ranking (from [`01_plan.md`](./01-foundation.md) Task 4)

Before code is accepted, verify each item:

1. Before ranking, reject a candidate if its instrument has an active reservation, active open trade, or unchanged active candidate.
2. Maintain no more than one active candidate per instrument.
3. Emit an audit result for every rejected candidate with a single rejection reason code: `ACTIVE_RESERVATION`, `ACTIVE_OPEN_TRADE`, or `UNCHANGED_ACTIVE_CANDIDATE`.
4. Send eligible candidates directly to the in-job ranking operator.
5. Do not use `Signal_Candidates` as an input to ranking.
6. Do not create a separate ranking deployment or a separate ranking checkpoint boundary.

#### Task 4 acceptance checks

- Repeated unchanged candidates for one instrument create one active candidate and one audit record per rejected repeat.
- Candidate for an instrument with an active reservation is not sent to ranking.
- Search results contain no Fluss source/read of `Signal_Candidates` in ranking code.

### Definition of done

The implementation is complete when exactly one Signal job performs the full path, deterministic replay is proven, state/checkpoint compatibility passes, cross-table limitations are documented and tested, readiness suppresses decisions on uncertainty, and no code/documentation claims external broker exactly-once behavior.

## Verification mapping

The required behavior above is verified by the canonical [Signal job test design](./11-testing-and-release.md#signal-job): `SIG-UNIT-001` to `SIG-UNIT-009`, `SIG-HARNESS-001` to `SIG-HARNESS-005`, `STATE-COMPAT-001`, `SIG-INT-001`, `SIG-INT-002`, `COMPAT-FLINK-001`, `SIG-FAIL-001`, and `SIG-PERF-001`. Implemented-test coverage of the SIG-* IDs is mapped in [Slice 1 evidence](#slice-1-evidence-implemented-2026-08-09) below.

## Slice 1 evidence (implemented 2026-08-09)

**Scope:** `raw_table_1` (Fluss LOG source, `OffsetsInitializer.full()`) → `RawValidationFunction` (schema/validity/price/qty gate) → `CandleWatermarkStrategy` (bounded out-of-orderness + idleness) → `keyBy(instrument_token)` → `FingerprintDedupFunction` (bounded first-seen state, event-time expiry timers) → 15 s event-time tumbling window (`CandleAggregateFunction` + `CandleEmitFunction`, emit-once final candles) → `feature_candles_15s` (Fluss LOG sink, append-only) **and `feature_candles_15s_current` (Fluss KV upsert sink — added by CANDLE-KV-REPLAY-001, 2026-08-10; the KV sink is the last out-edge of the candle branch and is checkpoint-restorable with the same graph)**. Forming-bar handoff and Business Logic are Slice 2; Ranking/Reservations/Decisions are Slice 3 — both remain not started.

### Files

| File | Responsibility |
| --- | --- |
| `02_compute/.../signaljob/SignalJob.java` | Job topology: source → validation → dedup → 15 s window → sinks (LOG + KV); EXACTLY_ONCE checkpointing (interval/timeout/max-concurrent pinned), fixed-delay restart 3 × 30 s (declarative `Configuration`, Flink 2.2.1); `preflightTableContracts(config)` metadata gate + startup-mode gate (RESTORE / explicit FULL_REPLAY). |
| `02_compute/.../signaljob/SignalJobConfig.java` | Pinned-load-bearing config: `DEDUP_TTL_MS=300000`, `CANDLE_WINDOW_MS=15000`, `CHECKPOINT_INTERVAL_MS=10000`, `CHECKPOINT_TIMEOUT_MS=30000`, `MAX_CONCURRENT_CHECKPOINTS=1` (reject any other value); tuning keys defaulted (`WATERMARK_OUT_OF_ORDER_MS=5000`, `ALLOWED_LATENESS_MS=5000`, `SOURCE_IDLE_MS=15000`, `RESTART_MAX_ATTEMPTS=3`, `RESTART_DELAY_MS=30000`); replay gate keys (`CANDLE_CURRENT_TABLE`, `ALLOW_FULL_REPLAY`, `STATE_RECOVERY_PATH`). |
| `02_compute/.../signaljob/RawTableColumns.java` / `CandleTableColumns.java` | DDL v2 column layouts (20 / 15 fields) mirrored as field indexes; explicit `InternalTypeInfo` at the candle boundary (no Kryo fallback for RowData). The same `CandleTableColumns` layouts drive both candle sinks and `CandleMigrationTool`. |
| `common/.../schema/CandleTableSchema.java` + `CanonicalCandlePolicy.java` | Shared 15-column candle schema contract (LOG and KV DDL parity) and canonical-version check `(schema_version, algorithm_version, configuration_version)` (CANDLE-KV-REPLAY-001). |
| `02_compute/.../signaljob/CandleTableContractValidator.java` | Startup preflight: live-table metadata for the LOG and KV candle tables must satisfy schema v2, PK-subset rule, 16 buckets, `bucket.key=instrument_token` — fail-fast before the job starts (CANDLE-KV-REPLAY-001). |
| `02_compute/.../tools/CandleMigrationTool.java` | Offline dry-run audit (duplicate/conflict detection per `(instrument_token, window_start)`, canonical-version filter, `MAX(output_ts)` merge) and canonical historical load LOG→KV (CANDLE-KV-REPLAY-001, tracker B8.2/B8.3). |
| `02_compute/.../signaljob/RawValidationFunction.java` | Row-kind INSERT + `schema_version` pin + `validity_state` VALID-prefix + `last_price_paise > 0` + `last_qty >= 0`; per-reason invalid counters. |
| `02_compute/.../signaljob/CandleWatermarkStrategy.java` | Bounded out-of-orderness on `event_time` + per-partition idleness. |
| `02_compute/.../signaljob/FingerprintDedupFunction.java` | State key `version\|scope\|fingerprint`; value `(first_seen, expiry)`; event-time expiry timers + compact expiry index (`expiry → state keys`) because Flink 2.2.1 removed event-time state TTL; deletion fires when the watermark reaches expiry (never early). |
| `02_compute/.../signaljob/CandleAccumulator.java` / `CandleAggregateFunction.java` / `CandleEmitFunction.java` | Compact OHLCV accumulator (no tick list); trades + quotes contribute OHLC via `last_price_paise`, volume/tick_count only on `TRADE` with `last_qty > 0`; open/close by `(event_time, fingerprint)` order key; emit-once final candle with `emitted` window-state flag (late-within-lateness re-triggers counted, not re-written). |

### Tests (38 green + 1 env-gated skip; no SIG-* ID carried in code yet — mapping below)

| Test class | Count | Covers |
| --- | --- | --- |
| `CandleAggregateFunctionTest` | 5 | SIG-UNIT-001/002 core (tie ordering, mixed trades/quotes OHLCV, quote-only window, single-row window, merge) |
| `RawValidationFunctionTest` | 8 | Validation-gate rules (rowkind, schema version, validity state, price, qty) |
| `SignalJobConfigTest` | 19 | SIG-UNIT-003 core (pinned dedup TTL / window / checkpoint values; rejection of deviations; tuning defaults) + DEC-034 signal keys (defaults, overrides, `SIGNAL_LOOKBACK_CANDLES` < 2 and `SIGNAL_QUANTITY` ≤ 0 rejection) + restore path (`stateRecoveryPath` default null, `STATE_RECOVERY_PATH` override honored) + CANDLE-KV-REPLAY-001 gate keys (`CANDLE_CURRENT_TABLE` default/override, `ALLOW_FULL_REPLAY` fail-closed rules, mode selection RESTORE vs FULL_REPLAY) |
| `FingerprintDedupFunctionTest` | 6 | SIG-UNIT-008/009 dedup half — Flink 2.2.1 operator harness (`KeyedOneInputStreamOperatorTestHarness`, no cluster): first occurrence passes / duplicate within TTL dropped; state stays exactly two rows per active key (dedup map + expiry index) regardless of fingerprint count; expiry timer deletes entries when the watermark reaches `first_seen + TTL` (never early), re-arriving expired fingerprint re-admitted; state key scoped by `version\|token\|fingerprint`; shared-expiry timer clears every listed key |
| `SignalDetectionFunctionTest` | 6 | DEC-034 rule via the same operator harness (no cluster): warm-up requires `SIGNAL_LOOKBACK_CANDLES` completed candles; fired candidate carries the full 22-column execution-ready payload (side/action/order_type/quantity/validity/detection_ts/formation ref/schema version; ranking fields null); bearish candle never fires even on breakout; no fire without strict breakout; repeated breakouts emit distinct `candidate_id`s; state keyed per instrument |
| `ComputeOtlpEmitterTest` | 4 | OTLP/JSON payload shape: DELTA non-monotonic sum, `aggregationTemporality` + `isMonotonic` fields, per-flush delta drain (`getAndSet(0)`) |
| `CandleTableContractValidatorTest` | 10 | CANDLE-KV-REPLAY-001 preflight: schema-v2 agreement, PK subset of bucket key (and rejection when violated), 16-bucket layout, `bucket.key=instrument_token`, LOG vs KV parity |
| `CanonicalCandlePolicyTest` | 7 | CANDLE-KV-REPLAY-001: canonical `(schema_version, algorithm_version, configuration_version)` check, rejection of non-canonical rows |
| `CandleCurrentKvIdempotencyTest` | 1 (env-gated) | CANDLE-KV-001 integration: same-key upsert converges to one row, last-write-wins `output_ts`, business fields intact, non-canonical rows rejected |
| `CandleMigrationToolTest` | 5 | B8.2/B8.3 audit semantics: replay-convergence, distinct-keys/conflict counting, non-canonical filtering, `output_ts` excluded from conflicts, exact-match merge |

Not yet covered by an implementing test (pending): `SIG-UNIT-007` (dependency scan), `SIG-UNIT-008/009` emit half (`CandleEmitFunction` state-content assertions), `SIG-HARNESS-001..005`, `SIG-INT-001/002`, `SIG-FAIL-001`, `SIG-PERF-001`, `STATE-COMPAT-001`, `COMPAT-FLINK-001`.

### Pending work items: resolution plan

Each pending item below is a tracked work item with its solving method, prerequisite, and pass gate. Execution classes: **pure-JVM now** (needs only a compute-pom test-scope addition — no cluster), **live-dev now** (env-gated against the dev Fluss cluster, pattern of SAFETY-INT-001), **slice/phase-gated** (waits on Slice 3 or Phase 6).

| Pending item | How to solve | Prerequisite | Gate |
| --- | --- | --- | --- |
| Dedup unit tests — **DONE 2026-08-10** (`FingerprintDedupFunctionTest`, 6 green): harness-driven state-key/value/expiry assertions above. Remaining half: `CandleEmitFunction` direct-operator test — assert the `emitted` window-state flag makes a re-trigger a no-op and no correction row is emitted (needs `CandleEmitFunction` to expose its window-state access for the harness, or `OneInputStreamOperatorTestHarness` around the emit path) | Harness infra **landed** in compute pom (test scope, no cluster) | Expired fingerprint absent after its expiry timer runs — **proven**; emit no-op on re-trigger — pending |
| `SIG-HARNESS-001..005` (out-of-order/watermark/idleness, late-before vs after-final, checkpoint-restore replay, duplicate-vs-identical limitation, reservation/ranking recovery) | Same harness infra (Flink 2.2.1 API: `ProcessFunctionTestHarnesses.forKeyedProcessFunction` → `KeyedOneInputStreamOperatorTestHarness`): inject watermarks and processing time; assert correct event-time outcome; `snapshot`/`initializeState` for deterministic replay equality; SIG-HARNESS-004 asserts the fingerprint-limitation metric/audit is emitted; SIG-HARNESS-005 lands with Slice 3 (reservation/ranking state) | Harness infra **landed**; **no cluster** (005 additionally needs Slice 3) | Correct event-time outcome; late-before-final updates vs after-final discard; restored output equals expected deterministic output |
| `STATE-COMPAT-001`, `COMPAT-FLINK-001` (serializer/savepoint change, source/sink checkpoint-restore-rescale on pinned versions) | `MiniClusterWithClientResource` (`flink-test-utils`, **already in compute pom test scope**) with pinned Flink 2.2.1 + `fluss-flink-2.2:0.9.1-incubating`; run topology, checkpoint, restore, assert state continuity; serializer-change compatibility blocks startup before unsafe use | Harness infra **landed**; **no cluster** | Restore succeeds through the approved path, or startup blocks before unsafe use |
| `SIG-INT-001/002` (pinned Fluss source/sink boundary, partial visibility across outputs) | Env-gated live test on the dev Fluss cluster, same pattern as `SafetyHaltLiveIntegrationTest` (`COMPUTE_INT_TEST_SAFETY` → new `COMPUTE_INT_TEST_SIGNAL=true` gate): run the real `SignalJob` topology (or its source→sink shell) against live Fluss, assert candles land in `feature_candles_15s`; SIG-INT-002 uses two sinks + reconciliation to prove partial-visibility handling | Live dev Fluss cluster (exists); Slice 2 sinks for the reconciliation half | Source/sink semantics work with approved versions; reconciliation identifies and handles partial visibility |
| `SIG-FAIL-001` (checkpoint/continuity failure → safe halt) | MiniCluster failure injection: force checkpoint failure, assert fixed-delay restart ×3 then fail-job; decision-suppression + safe-halt half requires the decision operators | **Slice 3** for full semantics (shell-level checkpoint-failure test can precede) | New decisions suppressed; one idempotent safety halt published; no Arrow REST call from Flink |
| `SIG-PERF-001` (variable-baseline and peak workload, decision p99) | Phase 6 perf campaign: soak suite (`run-full-suite.sh`) on the current 1,024-instrument / 20,480 t/s envelope; record decision p99, state size, checkpoint duration/size, memory | **Phase 6** + decision path (Slice 3) | p99 decision latency, state, checkpoint, memory within defined limits (current-phase envelope; 60k/90k deferred) |
| Beyond-lateness discard counter (REQ-FC-006) | Code: replace silent late-drop with `.sideOutputLateData(tag)` on the window operator + counting side-output; metric `compute.candles.late.dropped` carrying instrument/window/lateness/reason | Code change — Slice 2 backlog | Discard metric emitted with instrument/window/lateness/reason |
| Source throughput/lag, watermark lag, dedup state size (REQ-FC-010) | Code: connector source-throughput metric, timestamp-assigner watermark-lag metric, dedup state-size probe (entry counter or sampled `MapState` size) | Code change — Slice 2 backlog | REQ-FC-010 metrics emitted |
| `RESTART_MAX_ATTEMPTS` / `RESTART_DELAY_MS` pinning | Code: `intValue` → `requirePinnedInt`/`requirePinnedLong` in `SignalJobConfig.from` + rejection tests in `SignalJobConfigTest` (10-minute change, no cluster) | None — implementable now | Config test rejects deviations from 3 / 30000 |

Fastest path: the harness infra is **already landed** (compute-pom test-scope addition, 2026-08-10) and the dedup half of the first row is done. The remaining pure-JVM rows (`CandleEmitFunctionTest`, `SIG-HARNESS-001..004`, `STATE-COMPAT-001`, `COMPAT-FLINK-001`) now need only test code — no further pom changes — and run with no cluster. The env-gated rows need only the dev cluster. `SIG-FAIL-001` (full semantics) and `SIG-PERF-001` genuinely wait on Slice 3 / Phase 6.

### Live smoke (2026-08-09)

- **205,146 candle rows** written to `feature_candles_15s`; **1,074 distinct instruments**; **48 EXACTLY_ONCE checkpoints**; dedup state ~2 MB; OHLC/window-spacing **0 violations**; volume/tick histograms match REQ-FC-002 exactly.
- Dev overrides retained: `CHECKPOINT_DIR=file:///tmp/signaljob-checkpoints` (local heap checkpoints have a 5 MiB-per-state cap); full Flink-dist `--add-opens` set; `-Xmx4096m` for replays. (`RAW_SCHEMA_VERSION` stays at its default `2` — verified 2026-08-10 that the dev `raw_table_1` is the 20-column v2 schema.)

## Slice 2.1 — MVP signal detection → `Signal_Candidates` (implemented 2026-08-10)

Implements DEC-034: a closed-candle placeholder rule that produces
execution-engine-ready candidate records, so the record shape and the KV write
path are real before the user's own trading logic (and ranking) arrive. Ranking,
reservations, `Trade_Decisions`, and candidate lifecycle (max-one-active,
supersession, expiry) are intentionally postponed; a fired signal is appended as
an immutable `Signal_Candidates` record and nothing else happens downstream.

### Rule v1 — "20-candle breakout" (placeholder; replaceable via config)

Evaluated on each completed 15-second candle, keyed by `instrument_token`:

1. **Bullish**: `close > open` (strict; a flat candle never fires).
2. **Breakout**: `close > max(high of the previous `SIGNAL_LOOKBACK_CANDLES` completed candles)` (strict).
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
| Postponed (empty by design) | `instruction_id`, `trade_context_id`, `score_inputs` | NULL until ranking resumes |

### Files

| File | Responsibility |
| --- | --- |
| `02_compute/.../signaljob/SignalDetectionFunction.java` | `KeyedProcessFunction<Long, RowData, RowData>` keyed by `instrument_token`; two bounded `ValueState<List<Long>>` ring buffers (highs, closes); emits 22-column `GenericRowData` candidates; metric `compute.signals.detected` |
| `02_compute/.../signaljob/SignalCandidatesTableColumns.java` | 22 column indexes mirroring DDL v2 exactly, `FIELD_COUNT`, `ROW_TYPE_INFO`, plus record constants (`ACTION_ENTRY`, `SIDE_BUY`, `ORDER_TYPE_MARKET`, `VALIDITY_REASON_VALID`, `SCHEMA_VERSION_V2`) |
| `02_compute/.../signaljob/SignalJob.java` | Wired after the 15s window: `keyBy(token).process(SignalDetectionFunction)` → `FlussSink` with `RowDataSerializationSchema(false, false)` (KV upsert; `isAppendOnly=false` maps INSERT → UPSERT; the Append vs Upsert writer is chosen from live table metadata — fail-fast startup if the KV table is missing) |
| `02_compute/.../signaljob/SignalJobConfig.java` | New tuning keys: `SIGNAL_CANDIDATES_TABLE` (`Signal_Candidates`), `SIGNAL_STRATEGY_ID` (`simple-breakout`), `SIGNAL_STRATEGY_VERSION` (`1.0.0`), `SIGNAL_RULE_ID` (`breakout-20-bullish-trend`), `SIGNAL_LOOKBACK_CANDLES` (20, must be ≥ 2), `SIGNAL_QUANTITY` (1, must be > 0) |
| `01_ingestion/.../DdlBootstrap.java` | `Signal_Candidates` now registered with a full 22-column KV descriptor (PK `candidate_id`, `distributedBy(16, "candidate_id")`) instead of the generic `logTable` descriptor — the sink needs a real KV table |
| Dev table | Created DDL-faithful on the dev cluster (22 cols, PK, 16 buckets; datalake props skipped — the dev Fluss server has datalake disabled, same precedent as `feature_candles_15s`) |

### Tests (38 green + 1 env-gated skip; no SIG-* ID carried in code yet)

`SignalDetectionFunctionTest` (6) + `SignalJobConfigTest` additions (2) — mapping rows in the Slice 1 test table above.

### Live evidence (2026-08-10)

- **Run:** full replay of the dev `raw_table_1` (15.2M rows, full offsets) through the complete topology — validation → dedup → candles → signal detection → `Signal_Candidates` KV upsert. **`FlussSink` picked the upsert writer** from live table metadata ("Initializing Fluss upsert sink writer"), proving the KV write path against the real dev table.
- **Live counts (2026-08-10, live run 7456cc1c):** `Signal_Candidates` grew through the run (925 → 3,220 by 16:50 as live windows fire) — the job consumes `raw_table_1` only (no feature read-back), and the KV upsert rewrites a re-detected same-window candidate in place by `candidate_id`, so the count tracks genuinely new windows, never replay duplication. `feature_candles_15s` (LOG, append-only) grew to 323,930 candles before the replay-pollution incident — see pollution note in the process-rule section. 158+ EXACTLY_ONCE checkpoints at ~310 MB / 450–850 ms.
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

- The tracker lives per-task in the job shell; when the decision operators land it moves to broadcast state so `isTokenSuppressed` / `SuppressionGate` gate candidates/rankings/reservations/decisions. A tick alone never clears unsafe state; RECOVERED admits only post-recovery input.
- The job itself (live `FlussSource` consume path end-to-end) runs only after production approval; SAFETY-INT-001 already exercises the production bridge/parser/tracker against live Fluss.

