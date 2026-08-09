# Signal Job

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | Main pipeline implementation-ready; slot-scoped safety consumer (plan Amendment) implemented, compiling, and live-verified — SAFETY-INT-001 passed 2026-08-09, see Safety consumer section below |
| Owner | Compute and Strategy Teams |
| Requirements | `REQ-FC-*`, `REQ-SS-*`, `REQ-RNK-*` |
| Contracts | `docs/04_contracts/03-compute.md`, `04-business-logic.md`, `10-ranking.md` |
| Job topology | One Signal job containing Compute, Business Logic, and Ranking |
| Separate job | Babysitter only |

### Job graph

```text
Fluss raw_table_1 source
→ schema/validity filter
→ event-time assignment and watermark
→ keyBy(instrument_token)
→ bounded fingerprint dedup
→ forming-bar/window state
├─ final 15-second candle sink
└─ typed forming-bar update (closed-candle + forming-bar)
   → business rules/candidate state (keyed by instrument_token)
   → candidate audit sink
   → repartition eligible candidates by portfolio_id
   → in-operator ranking/reservation state (serialized per portfolio_id)
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
| `CHECKPOINT_INTERVAL_MS` | Fixed at `10000`; Signal and Babysitter jobs use this value |
| `CHECKPOINT_TIMEOUT_MS` | Fixed at `30000`; Signal and Babysitter jobs use this value |
| `MAX_CONCURRENT_CHECKPOINTS` | Fixed at `1`; Signal and Babysitter jobs use this value |
| `STATE_BACKEND_TO_BE_PINNED` | Version-compatible managed state backend |
| `S3_CHECKPOINT_URI_TO_BE_DEFINED` | Production encrypted checkpoint/savepoint storage |
| `STRATEGY_VERSION` | Included in candidates/decisions |
| `RANKING_VERSION` | Included in ranking results/decisions |
| `RESERVATION_POLICY_VERSION` | Included in audit and restored state |
| `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` | Fixed at `1`; do not forward another active candidate for that instrument |
| `CHECKPOINT_RESTART_STRATEGY` | Max failures = 3, delay between attempts = 30s, failure action = fail job. Deployment SHALL reject unbounded retry. |

Deployment SHALL reject unbounded or too-short `DEDUP_TTL`, missing production checkpoint storage, unbounded checkpoint restart retry, and any deviation from pinned values.

### Event-time contract

- Event time is the verified UTC broker timestamp.
- Events without verified event time do not advance watermarks.
- Watermark and idleness apply per source partition.
- One final candle emits after watermark passes `window_end + allowed_lateness`.
- Later records are discarded and measured in MVP.
- Open/close ties use the versioned deterministic fingerprint ordering.
- Empty windows emit no row.

### Dedup state

State key contains only fingerprint version, scope, and event fingerprint. State value contains only first-seen timestamp and expiry timestamp. Dedup state SHALL NOT contain raw bytes, decoded raw fields, candle values, candidate values, or an event object. State is deleted exactly when its `300000` ms expiry is reached.

First event proceeds; later candidate within TTL increments duplicate metrics and does not affect candle/business state.

Identical legitimate events may be collapsed; this limitation must remain visible in metrics and documentation.

### Dedup state budget

At the 60,000 ticks/s baseline workload (3,000 instruments; 20 ticks/s/instrument average):

| Metric | Value | Derivation |
| --- | --- | --- |
| Steady-state entries | ~18,000,000 | 60,000 ticks/s × 300s TTL |
| Raw state size (per entry ~32 bytes) | ~576 MB | Fingerprint + first-seen timestamp + expiry timestamp |
| RocksDB overhead (LSM amplification) | ~1.7-2× | Block index, bloom filter, SST metadata |
| Estimated total state | **~1 GB** | Plateaus after warmup; does not grow unbounded |

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

Active candle state SHALL NOT contain a list, collection, array, or map of individual ticks. State is deleted after the final candle sink acknowledgement succeeds.

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
9. Active candle state deleted after final candle sink acknowledgement succeeds.
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

The required behavior above is verified by the canonical [Signal job test design](./11-testing-and-release.md#signal-job): `SIG-UNIT-001` to `SIG-UNIT-009`, `SIG-HARNESS-001` to `SIG-HARNESS-005`, `STATE-COMPAT-001`, `SIG-INT-001`, `SIG-INT-002`, `COMPAT-FLINK-001`, `SIG-FAIL-001`, and `SIG-PERF-001`.

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

