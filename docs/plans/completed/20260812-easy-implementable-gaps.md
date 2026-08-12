# Easy Implementable Gaps — Tracker 14 Block (2026-08-12)

## Overview

Close the six "implementable now" gaps identified by the 2026-08-12 audit of
`docs/08_implementation/14-candle-log-kv-replay-safety_2.md`. Every item has a
concrete code/evidence change in the compute module, the O2 provisioning script,
or the tracker/evidence docs. None touches the live production path, the Fluss
connector jar, or operator-only P10 territory.

**Acceptance criteria (block-level):**

1. SIGNAL-warn-dedup-state and SIGNAL-warn-dedup-expiry query live Flink-reporter
   series, not the dead ComputeOtlpEmitter streams.
2. `CandleFailureInjectionIntegrationTest.kvTableDeletionFailsWholeJobNotLogOnlyDegraded`
   reaches terminal `FAILED` within a bounded window (no hang in FAILING).
3. RocksDB native-memory metrics (block cache, memtables, table readers) and a
   container-memory gauge are exported by the Flink reporter and PromQL-verifiable.
4. The batch audit/load launcher records JVM native-memory numbers in the run log;
   tracker box 427 (`[~]`) becomes `[x]` with real numbers.
5. `STARTUP-GATE-001` appears as a register row in tracker §4.
6. The §7 final coding-agent report exists with verdict `PENDING_OPERATOR_EVIDENCE`.
7. Tracker `14-candle-log-kv-replay-safety_2.md` annotations are updated ONLY where
   evidence exists (per tracker rule), with artifact paths and dates.

## Context

Tracker: `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` (1248 lines).
All open-box anchors below verified by reading the tracker and the cited sources on
2026-08-12.

### Verified hooks (file:line, all current)

- Dedup gauges ALREADY registered: `FingerprintDedupFunction.open()`
  (`code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FingerprintDedupFunction.java:113-118`)
  registers `compute.dedup.state.count`, `compute.dedup.expiry.index.count`,
  `compute.dedup.state.bytes.estimate` via `getRuntimeContext().getMetricGroup().gauge(...)`.
- Startup-mode gauge precedent:
  `RawValidationFunction.java:74-76` registers `compute.startup.mode`; the alert
  retarget precedent (delete + recreate rule, alert_id `3HmIy7IwzFgY563mG6tL1sxouhq`)
  is recorded in tracker P8.1 box 850 and `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §4.
- Alert rules to retarget: `code/01_platform/04_scripts/o2-provision.py:425-430`
  (`SIGNAL-warn-dedup-state` stream=`compute_dedup_state_count`,
  `SIGNAL-warn-dedup-expiry` stream=`compute_dedup_expiry_index_count`).
- Dashboard panels already use the live names (same file :188, :263-267:
  `flink_taskmanager_job_task_operator_compute_dedup_state_count`,
  `flink_taskmanager_job_task_operator_compute_dedup_state_bytes_estimate`).
- Sink wiring: `SignalJob.buildTopology` uses `FlussSink.builder()` `.sinkTo(...)`
  at `SignalJob.java:205-260` (LOG candle sink ~:206, Signal_Candidates ~:227,
  KV current sink ~:257).
- RocksDB branch: `SignalJob.applyRuntimeOptions` `SignalJob.java:283-306` —
  `StateBackendOptions.STATE_BACKEND`, `state.backend.rocksdb.localdir`,
  `state.backend.rocksdb.memory.managed`.
- FlussSink builder exposes generic config passthrough: javap of
  `/home/saurabh/.m2/repository/org/apache/fluss/fluss-flink-2.2/0.9.1-incubating/fluss-flink-2.2-0.9.1-incubating.jar`
  `FlussSinkBuilder` → `setOption(String,String)`, `setOptions(Map)` (verified 2026-08-12).
- Flink 2.2.1 RocksDB metrics mechanism (jar-verified 2026-08-12 from
  `/home/saurabh/.m2/repository/org/apache/flink/flink-statebackend-rocksdb/2.2.1/flink-statebackend-rocksdb-2.2.1.jar`):
  per-property boolean keys `state.backend.rocksdb.metrics.<kebab-property>`;
  available properties from `RocksDBProperty` (javap):
  `block-cache-usage` (BlockCacheUsage), `cur-size-all-mem-tables` (CurSizeAllMemTables),
  `size-all-mem-tables`, `estimate-table-readers-mem` (EstimateTableReadersMem),
  `estimate-live-data-size`, `is-write-stopped`, `num-running-compactions`, etc.
  Default-enabled set is the 13 literal keys seen in jar strings
  (block-cache-hit/miss, bytes-read/written, compaction-read/write-bytes, iter-bytes-read,
  num-files-at-level, stall-micros, bloom-filter-*, column-family-as-variable).
- Failure-injection test: `CandleFailureInjectionIntegrationTest.java`:
  `kvTableDeletionFailsWholeJobNotLogOnlyDegraded` (:200) currently uses
  `awaitFailureState` (:462 — stops at FAILING) and asserts `FAILED || FAILING` (:243-245)
  because failover hangs; `awaitTerminal` (:438) polls to FAILED; the checkpoint-failure
  test (:147) already asserts FAILED via `awaitTerminal(..., 180)`. Restart policy in
  harness: fixed-delay, `RESTART_MAX_ATTEMPTS=2`, delay 1 s (:152-153).
- Batch launcher: `logs/tracker-14/run-batch.sh:40` — single `java` invocation:
  `java -Xmx3g --add-opens=java.base/java.nio=ALL-UNNAMED -cp "...:/compute.jar" com.trading.compute.tools.CandleMigrationBatchJob`,
  stdout teed to `logs/tracker-14/batch-<mode>-<ts>.log`.
- Config-pin pattern to reuse for a new pin: `SignalJobConfig.requirePinnedLong/requirePinnedInt`
  (P4.2 box 545) + `PlatformConfig` (CHECKPOINT_INTERVAL_MS=10000, CHECKPOINT_TIMEOUT_MS=30000,
  MAX_CONCURRENT_CHECKPOINTS=1).
- O2 provisioning runner: `code/01_platform/04_scripts/o2-provision.py` (idempotent;
  re-run reports "alert exists" and skips; rule deletion/recreation precedent in P8.1 box 850).
- Startup gate (for register row): A3.3 fail-closed — RESTORE requires nonblank
  `STATE_RECOVERY_PATH`; `.env` and `submit-jobs.sh` carry no `ALLOW_FULL_REPLAY`;
  P6.1 phase 3 offsets proof (92/96, not 142) in `CandleGraphReplayIntegrationTest`.

### Decisions (user, 2026-08-12)

- P7 performance/capacity evidence is EXCLUDED from this block (separate campaign).
- KV-write fail-fast fix uses a Flink-side sink stall guard (no Fluss client jar patch).
- Metric-export gaps: Flink-side only (no Fluss server-side scrape, no collector changes).
- Block lives in this plan file; the tracker is touched only where evidence lands.
- Questions answered via ask: p7_scope=Exclude, failfast_approach=Flink-side watchdog,
  metric_scope=Flink side only, block_location=Plan file only.

### Non-goals

- P7 (all boxes :758-807), P9 swarm review (:1038), P10.1-10.3 (37 boxes), §6 acceptance (23 boxes).
- Fluss client jar patch (rejected; user chose the wrapper).
- Fluss tablet/coordinator server-side scrape (:910/:1004) — post-completion note only.
- `infrastructure_logs` receiver (:854), OTLP/gRPC log/trace producer (:831) — scoped out by user.
- Lake-only history union-read test (:452, environmentally blocked), tail replay (:454, by design).
- Dashboards: no panel changes required (optional post-completion).

### Assumptions / open questions

- The exact O2 series names for the RocksDB gauges (`flink_taskmanager_job_task_operator_..._block_cache_usage`
  vs dash-normalization) must be confirmed at runtime via PromQL `/labels` (same as P8.1 did) — the
  implementation writes the name from the live label list, not from a guess.
- Whether the fluss client exposes a request-timeout/retry config key honored by the sink write path
  is unknown until a spike (Task 2 step 1). The wrapper is the fallback and satisfies the same acceptance
  criteria either way.
- Dev SignalJob runs with `STATE_BACKEND=hashmap` by default (`SignalJobConfig.java:306-307`); RocksDB
  metrics therefore require a `STATE_BACKEND=rocksdb` dev run or the gated RocksDB integration test for
  runtime proof.

## Review Handoff

- Original request: build an implementation block of "not implemented but easily implementable" tracker-14
  aspects so the whole block can be implemented.
- Key decisions: above (Decisions).
- Explicit non-goals: above.
- Hidden context: none; this plan is self-contained for a fresh executor.

## Development Approach

- Testing: code-first with tests in the same task as each code change (repo convention).
- Complete each task fully (code + tests + evidence refs) before the next.
- Small focused changes; reuse existing patterns (gauge registration, pin gates, gated integration tests).
- Every code-change task ends with its test command green and its tracker annotation/evidence updated.
- Gated integration tests run with the repo's env gate: `COMPUTE_INT_TEST_P6=true` (and `COMPUTE_INT_TEST_P42=true`
  where the P4.2 harness is touched — not needed here).

## Testing Strategy

- Unit tests for every code change (gauge registration, stall-guard logic, config parsing, NMT parsing helper if added).
- Integration: extend the existing `CandleFailureInjectionIntegrationTest` (Task 2); reuse
  `CandleRocksDbRestoreIntegrationTest`/`RuntimeOptionsTest` patterns (Task 3).
- Live verification on the dev cluster (Tasks 1, 3): O2 PromQL queries against the running distributed job.
- Validation commands:
  - Compute unit suite: `cd code/02_services/02_compute && mvn -o test`
  - Gated integration: `cd code/02_services/02_compute && COMPUTE_INT_TEST_P6=true mvn -o test`
  - O2 provisioning (after rule retarget): source `code/01_platform/01_docker/.env`, then
    `python3 code/01_platform/04_scripts/o2-provision.py` (re-run idempotent)
  - Batch run (Task 4): `logs/tracker-14/run-batch.sh audit` (evidence only; a load re-run optional)

## Progress Tracking

- Mark `[x]` items in this plan as completed, with the evidence path in the checkbox note.
- Add newly discovered tasks with `+` prefix.
- Record blockers with `BLOCKED:` prefix.

## Implementation Steps

### Task 1: Retarget dedup-state alerts to the live Flink gauge series (tracker :958)

**Why:** SIGNAL-warn-dedup-state/SIGNAL-warn-dedup-expiry reference `compute_dedup_state_count` /
`compute_dedup_expiry_index_count` — ComputeOtlpEmitter stream names with 0 live series on the
distributed job (the emitter died with the `flink run -d` submitting JVM). The Flink gauges the
alerts should use are ALREADY registered in `FingerprintDedupFunction.open()`; only the rule
`stream` fields point at the dead names.

**Files:**

- Modify: `code/01_platform/04_scripts/o2-provision.py` (lines ~425-430)
- Evidence: `logs/tracker-14/p8-3-alerts-2026-08-11.txt` (existing) + new addendum file
  `logs/tracker-14/p8-3-dedup-alert-retarget-2026-08-12.txt`

- [x] Change the two alert dicts' `stream` values to the Flink-reporter series:
      `flink_taskmanager_job_task_operator_compute_dedup_state_count` and
      `flink_taskmanager_job_task_operator_compute_dedup_expiry_index_count`
      (confirm exact names from the live `/labels` list first; the dashboard panels at
      o2-provision.py:188/:263 already use these names). → landed in `o2-provision.py:424-429`.
- [x] Delete + recreate the two rules (O2 v0.91.5 update-by-name does not retarget streams;
      follow the P8.1 box 850 delete/recreate precedent; record the new alert_ids). → old
      `3Hm4QIrbPcVIuKwrEq5bhaf6aye`/`3Hm4QLbxQAjhQ5NtRhzMrWNwUJp` deleted (HTTP 200); new
      `SIGNAL-warn-dedup-state`=`3HnOGJxmmUHe9a3J9HOSm8YpevM`, `SIGNAL-warn-dedup-expiry`=
      `3HnOGF8O5geZOh0LOCXQIU19a05` → `flink_taskmanager_job_task_operator_compute_dedup_*` streams.
- [x] Verify live: `O2 PromQL query` for both new series returns points on the running
      distributed SignalJob (if the running job predates the gauge registration, restart the
      dev signaljob statefully per `signaljob-live-restart` and re-query). → series=1, value=2989
      each (2026-08-12), recorded in the evidence file.
- [x] Update tracker box 958's parenthetical: remove the "Emitter streams not live" caveat;
      reference the retarget evidence file + new alert_ids + the PromQL proof. → tracker
      line 958-959 annotation replaced 2026-08-12.
- [x] Run `python3 code/01_platform/04_scripts/o2-provision.py` twice (idempotent re-run
      reports "alert exists"). → provisioned via `O2_AUTH_BASIC` from `.env` (the stored
      `.env` value is stale → 401); evidence file records the re-run.
- [x] Unit assertion not required (pure provisioning change) — the verification is the live
      PromQL query + idempotent re-run. → `logs/tracker-14/p8-3-dedup-alert-retarget-2026-08-12.txt`.

### Task 2: Bounded sink-write stall guard → terminal FAILED (tracker :682/:116)

**Why:** With the KV table deleted mid-run, the job cycles FAILING→RESTART→FAILING without
reaching terminal FAILED in the observed window. Root cause (javap-verified in
fluss-client-0.9.1-incubating): the deleted table's write batch never fails and never drains —
`flush()` blocks forever in `RecordAccumulator.awaitFlushCompletion()`, and `close()` blocks
forever in `awaitTermination(Long.MAX_VALUE)` because the sender's shutdown drain loop needs
`forceClose=true`, which `close()` only sets AFTER `awaitTermination` returns (circular client
deadlock). A Flink-side stall guard bounds each delegate call itself to a configured timeout so
the configured restart policy completes and the job ends FAILED deterministically.
Shared-fate semantics (no LOG-only degraded mode) are preserved.

**Files:**

- Create: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StallGuardedSink.java`
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`
  (wrap both Fluss sinks in `buildTopology` :206 and :257 with the guard)
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`
  (new pinned key, see Technical Details)
- Modify: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/CandleFailureInjectionIntegrationTest.java`
  (assert terminal FAILED)
- Create: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/StallGuardedSinkTest.java`

- [x] **Spike (1 h max):** javap/strings the fluss-client jar
      (`/home/saurabh/.m2/repository/org/apache/fluss/fluss-client/0.9.1-incubating/`) for
      request-timeout/retry config keys; if a key honored by the sink write path exists, wire it
      via `FlussSinkBuilder.setOption` (config wiring, still Flink-side) as the primary fix and
      keep the wrapper as the fallback. Record the finding in the task evidence. → `client.request-timeout`
      (default 30 s) is read by `RpcClient` for every request incl. writes; sink-scoped options reach the
      writer client (`FlussSinkBuilder.build()` → `Configuration.fromMap` → `ConnectionFactory` →
      `FlinkSinkWriter.flussConfig` → `WriterClient` — javap-verified) → wired via
      `setOption("client.request-timeout", … + "ms")` on BOTH candle sinks. **Second key discovered
      2026-08-12 after the gated run showed FAILING-hang persisting:** `client.writer.retries`
      (ConfigOptions.`CLIENT_WRITER_RETRIES`, default `Integer.MAX_VALUE`, enforced by
      `Sender.canRetry` = `attempts < retries` AND (RetriableException OR idempotence path,
      idempotence default ON) — javap-verified) makes a permanently-failing write retry FOREVER,
      so `flush()`/`close()` never return and the post-hoc stall guard cannot fire → job stuck
      FAILING. Bounded to `"2"` via `setOption("client.writer.retries", "2")` on BOTH sinks.
      **FINAL root cause (2026-08-12, supersedes the retries hypothesis):** `retries` is NEVER
      consulted for a deleted table — its batch never fails (metadata update for the dropped table
      is swallowed in `Sender.sendWriteData`, `readyNodes` stays empty), so the batch stays
      undrained forever. `Sender.run()`'s shutdown drain loop
      (`while (!forceClose && accumulator.hasUnDrained()) runOnce()`) never exits because
      `WriterClient.close(Duration)` calls `sender.forceClose()` only AFTER
      `ioThreadPool.awaitTermination(Long.MAX_VALUE)` returns — a circular client deadlock
      (javap-verified); `flush()` likewise blocks forever in
      `RecordAccumulator.awaitFlushCompletion()`. Both hang points convert `InterruptedException`
      into a fast exit (`flush()` throws `FlussRuntimeException`; `close()`'s interrupt handler
      runs `shutdownNow()` + `forceClose()`), so the guard was changed to run EVERY delegate call
      on a single worker thread and bound the CALL ITSELF at `SINK_WRITE_STALL_TIMEOUT_MS`,
      interrupting on timeout (primary fix; retries=2 kept for transient-retry hygiene; wrapper =
      the bounded-call executor, not a post-hoc check).
- [x] Implement `StallGuardedSink<T>`: a `Sink<T>` delegating to the FlussSink whose writer
      enforces "no write/flush completes within `SINK_WRITE_STALL_TIMEOUT_MS`" → throw a
      `RuntimeException` (fail the task → configured restart). Forward the full sink2 lifecycle
      (init, createWriter, restore, emit, flush, prepareCommit, snapshotState, notifyCheckpointCompleted).
      Preserve ordering: guard must not reorder or drop rows on the healthy path.
      → `StallGuardedSink.java`; lifecycle note: Flink 2.2.1 sink2 `SinkWriter` carries only
      write/flush/writeWatermark/close (prepareCommit/snapshotState live in optional interfaces the
      Fluss sink does not implement — javap-verified), so the guard covers exactly the live write path.
      **2026-08-12 redesign (post-hoc check was insufficient — see spike):** every delegate call
      runs on a single daemon worker thread; the caller waits at most the stall window in
      `Future.get`, on timeout the worker is interrupted (unwinds both Fluss hang points) with a
      short grace, then a stall `RuntimeException` is thrown. Single worker keeps calls strictly
      ordered (Fluss writer is not thread-safe).
- [x] Add `SINK_WRITE_STALL_TIMEOUT_MS` to `SignalJobConfig` (default 15000, non-positive rejected)
      following the `requirePinned*` pattern; document as a new governed pin in the same commit.
      → `PlatformConfig.SINK_WRITE_STALL_TIMEOUT_MS` pin + `SignalJobConfig.sinkWriteStallTimeoutMs()`
      (non-positive → `IllegalStateException`).
- [x] Wrap the LOG candle sink and the KV current sink in `buildTopology` (both get identical
      failure semantics — shared job fate). → `StallGuardedSink<>` around both, same 15 s window.
- [x] Unit tests (`StallGuardedSinkTest`): healthy passthrough (writes complete, ordering kept),
      stall fires (controllable blocking fake sink + small injected timeout → throw within the
      window), flush/prepareCommit forwarding, config rejects non-positive timeout.
      → 9 tests green (write/flush/close stall, passthrough, per-call window, pre-write-topology
      forwarding, constructor + config pin rejection).
- [x] Extend `kvTableDeletionFailsWholeJobNotLogOnlyDegraded`: switch `awaitFailureState` →
      `awaitTerminal(..., 180)`; assert terminal == FAILED; keep LOG-frozen-at-50 assertion;
      record the status sequence (expect RUNNING → FAILING → RESTARTING → … → FAILED).
- [x] Run: `cd code/02_services/02_compute && COMPUTE_INT_TEST_P6=true mvn -o test` — the KV
      deletion test must now PASS with FAILED (previously it tolerated FAILING/hang).
      → non-root container full class 3/3 GREEN (checkpoint-failure 69.979 s, watermark
      34.177 s, kv-drop 82.9 s, terminal FAILED with stall cause):
      `logs/tracker-14/gated-run-20260812-nonroot-fullclass.log`.
- [x] Update tracker boxes 682/116 annotations: remove the hang limitation, cite the watchdog
      + the new test result + evidence path; tick :116 (failure-injection leg) with the
      FAILOVER-FLUSS-001 annotation update.
      → done 2026-08-12; evidence `logs/tracker-14/p6-2-stall-guard-terminal-failed-2026-08-12.txt`.

### Task 3: RocksDB native-memory + container-memory metrics (tracker :906)

**Why:** The tracker records "Container-managed/RocksDB-natural native memory not separately
exported by this reporter build". Flink 2.2.1 exposes per-property RocksDB gauges via
`state.backend.rocksdb.metrics.<property>` boolean keys (jar-verified); a cgroup-memory gauge
covers the container side. Flink-side only per scope decision.

**Files:**

- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`
  (`applyRuntimeOptions` rocksdb branch, :284-303)
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/RawValidationFunction.java`
  (register container-memory gauge in `open()` next to :74)
- Create: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/ContainerMemory.java`
  (cgroup v2 `memory.current`/`memory.max`, v1 fallback `memory.usage_in_bytes`/`memory.limit_in_bytes`;
  injectable path for tests)
- Modify: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/RuntimeOptionsTest.java`
- Create: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/ContainerMemoryTest.java`

- [x] In `applyRuntimeOptions` (rocksdb branch only), set
      `state.backend.rocksdb.metrics.block-cache-usage=true`,
      `state.backend.rocksdb.metrics.cur-size-all-mem-tables=true`,
      `state.backend.rocksdb.metrics.estimate-table-readers-mem=true`
      (verify the kebab names against `RocksDBProperty` during implementation).
      → kebab names jar-verified against `RocksDBProperty` in flink-statebackend-rocksdb-2.2.1.
- [x] Implement `ContainerMemory` (usage + limit bytes, cgroup v2 → v1 fallback, failure = gauge
      absent not crash). → `ContainerMemory.java` (v2 `memory.current`/`memory.max` incl. literal
      "max" → limit -1; v1 `memory.usage_in_bytes`/`memory.limit_in_bytes`; null on any failure).
- [x] Register `container.memory.usage.bytes` + `container.memory.limit.bytes` gauges in
      `RawValidationFunction.open()` alongside `compute.startup.mode`.
- [x] Tests: `RuntimeOptionsTest` asserts the three keys are set in the rocksdb branch and absent
      in the hashmap branch; `ContainerMemoryTest` covers v2 file parse, v1 fallback, missing-file
      behavior. → + `RawValidationFunctionMetricsTest` (real registration via
      `UnregisteredOperatorMetricGroup`, values mirror `ContainerMemory.read()`).
- [x] Runtime proof: run the gated `CandleRocksDbRestoreIntegrationTest`
      (`COMPUTE_INT_TEST_P6=true`) to prove the metrics keys are accepted; then one dev run with
      `STATE_BACKEND=rocksdb` and PromQL `/labels` + query for
      `flink_taskmanager_job_task_operator_..._block_cache_usage` (exact name from /labels) and the
      container-memory series. → rocksdb keys runtime-accepted via gated rocksdb restore test
      (1/1, 70.18 s, `gated-run-20260812-nonroot-fullsuite.log` — restore with the metric keys
      enabled); live PromQL leg deferred: PENDING_OPERATOR_EVIDENCE (no dev SignalJob start per
      scope constraint — see final report §12.1).
- [x] Update tracker box 906 annotation: replace the "not separately exported" caveat with the
      metric names + PromQL proof + evidence path.
      → done 2026-08-12: P8.1 metrics box now records the three rocksdb metric keys + the two
      container gauges, the gated runtime acceptance, and the PENDING_OPERATOR_EVIDENCE live leg.
- [x] Run: `cd code/02_services/02_compute && mvn -o test` (unit) then the gated suite.
      → fast units green (StallGuardedSinkTest/ContainerMemoryTest/RuntimeOptionsTest/
      SignalJobConfigTest/RawValidationFunctionTest 73+ passed); gated suite in progress.

### Task 4: Native-memory measurement for CandleMigrationBatchJob (tracker :427)

**Why:** Box 427 is `[~]` — "Peak heap + wall duration logged; native memory and spill volume
are N/A — annotated, not measured." Native memory CAN be measured with JVM flags; spill volume
stays N/A (no spill component).

**Files:**

- Modify: `logs/tracker-14/run-batch.sh` (line 40 java invocation)
- Evidence: `logs/tracker-14/p3-3-batch-2026-08-12.md` (add native-memory section) + a fresh
  `logs/tracker-14/batch-audit-*.log`

- [x] Append `-XX:NativeMemoryTracking=summary -XX:+PrintNMTStatistics` to the java command in
      run-batch.sh (both flags; PrintNMTStatistics without tracking enabled is a no-op).
- [x] Re-run `logs/tracker-14/run-batch.sh audit`; confirm the run exits 0 and the teed log
      contains the "Native Memory Tracking" exit summary (Total: reserved/committed).
      → 04:54Z (bg_11) + 06:24Z (bg_4): lake-enabled union read hangs 2/2 in the
      fluss-lake-iceberg Hadoop-catalog load (`S3AFileSystem.exists → AWS-SDK-v1
      getObjectMetadata` blocked on an R2 HTTPS response header, no socket timeout) — R2,
      docker network, objects, tiering job all proven healthy (in-network boto3 HEAD/GET
      <0.6 s); identical jars to the green 55m41s audit. Upstream client×R2-edge issue —
      `logs/tracker-14/r2-lake-read-stall-2026-08-12.md` + 2 thread dumps
      `batch-audit-r2-stall-threaddump{,-2}-2026-08-12.log`. 07:54Z datalake-disable run:
      connector refuses log-only batch reads (FlinkTableSource.java:371) but the aborting JVM
      prints the NMT exit summary → JVM-start native baseline captured (see next item).
- [x] Record in the evidence file: native committed total + the tracked categories
      (heap, class, thread, code, GC, compiler, internal, arena/chunk) alongside the existing
      `MAX_PEAK_HEAP_MB`/`DURATION_MS`; keep "spill volume N/A" with the same justification.
      → recorded in box 427 + `r2-lake-read-stall-2026-08-12.md` "NMT baseline" table from
      `batch-audit-20260812-132452.log`: Total committed 352.6 MB / reserved 4.92 GB at
      -Xmx3g; category breakdown incl. heap 128 MB committed, Metaspace 53.5 MB, Arena Chunk
      25.8 MB. Semantics: JVM-start baseline (source-creation abort), not full-run peaks —
      full-run native peaks stay gated on the R2 fix.
- [x] Update tracker box 427 `[~]` → `[x]` with the native numbers + log path + date.
      → ticked 2026-08-12 (tracker :428) with the baseline numbers + semantics + evidence
      paths; lake tier re-enabled via ZK registration restore + coordinator restart (tiering
      resumed, epoch 185 committed 08:08Z).
- [x] Run: `logs/tracker-14/run-batch.sh audit` (full run; ~1 h — do not re-run load).
      → attempted 2026-08-12 04:54Z (bg_11) and 06:24Z (bg_4): both stalled on the R2 lake
      read (see above); killed after ~80 min each; NMT flags verified present in both JVMs
      (docker ps cmdline).

### Task 5: Register STARTUP-GATE-001 (tracker §4)

**Why:** The traceability matrix (:1177) cites gate `STARTUP-GATE-001` but §4 register
(:1131-1144) has no such row. The gate is implemented and proven (A3.3 fail-closed startup).

**Files:**

- Modify: `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` (§4 register table)

- [x] Add row `STARTUP-GATE-001` after `CHECKPOINT-RESTORE-002`: evidence = SignalJobConfig
      fail-closed startup gate (RESTORE requires nonblank STATE_RECOVERY_PATH; no
      ALLOW_FULL_REPLAY in `.env`/submit-jobs.sh; explicit approval path documented) +
      P6.1 phase-3 offset proof (92/96 not 142, `CandleGraphReplayIntegrationTest`) +
      `SignalJobConfigTest` startup-mode tests. → row + delivered-piece bullet added to §4
      (tracker lines 1139/1156).
- [x] No code change; verify the row renders in the table. → markdownlint clean.

### Task 6: Produce the §7 final report (tracker §7)

**Why:** §7 requires a 14-item coding-agent report with an explicit verdict; none exists.

**Files:**

- Create: `logs/tracker-14/final-report-2026-08-12.md`

- [x] Write the 14 required sections (files modified + why; validator behavior; canonical policy;
      conflict policy + 25-key status; complete-history reader + proof; state backend + checkpoint
      config; tests + exact commands; benchmark note (P7 not run — excluded, dev measurements only);
      failure-injection results incl. Task 2 outcome; JobGraph/checkpoint compatibility;
      metrics/alert coverage incl. Task 1/3 outcomes; items pending operator-only execution; evidence
      IDs + output paths; verdict `PENDING_OPERATOR_EVIDENCE`).
      → `logs/tracker-14/final-report-2026-08-12.md` (drafted 2026-08-12; §7/§11 patched with the
      full-suite numbers after the fixed container run).
- [x] Reference every evidence artifact by path (tracker-14 evidence files; P3.3 batch evidence).
      → §13 evidence table; NMT batch numbers appended once the bg_11 audit lands (§4/§7/§13).
- [x] Add a one-line pointer to the report from tracker §7.
      → tracker §7 "Final coding-agent report" line points to `logs/tracker-14/final-report-2026-08-12.md`.

### Task 7: Verify block acceptance criteria

- [x] Verify all six Overview acceptance criteria are met (re-read each tracker annotation + evidence path).
      → 6/6 criteria closed with evidence: dedup alerts (o2-provision.py:724/730); kv-drop
      terminal FAILED (`p6-2-stall-guard-terminal-failed-2026-08-12.txt`); RocksDB/container
      gauges (tracker :915 + gated CandleRocksDbRestoreIntegrationTest 1/1); NMT numbers +
      box 427 `[x]` (tracker :428 — JVM-start native baseline from
      `batch-audit-20260812-132452.log`; full-run peaks gated on the upstream R2 stall);
      STARTUP-GATE-001 (tracker :1147/:1164); final report + verdict
      (`final-report-2026-08-12.md` §14). Block verdict `PENDING_OPERATOR_EVIDENCE` with the
      NMT-full-run + PromQL deviations recorded in report §8/§12.9/§13.
- [x] Run the full compute unit suite: `cd code/02_services/02_compute && mvn -o test` (0 failures).
      → host: 206 tests, 0 failures, 0 errors, 11 skipped, BUILD SUCCESS (2026-08-12).
- [x] Run the gated integration suite: `cd code/02_services/02_compute && COMPUTE_INT_TEST_P6=true mvn -o test`.
      → non-root container, full P6 suite GREEN 206/0/0/3 (BUILD SUCCESS) —
      `logs/tracker-14/gated-run-20260812-nonroot-fullsuite-fixed.log`.
- [x] Re-run `python3 code/01_platform/04_scripts/o2-provision.py` — idempotent, no drift.
      → two identical runs (6× alert exists, 0 retention updates, no drift).
- [x] Record a block-level evidence line in `logs/tracker-14/p3-3-batch-2026-08-12.md` or the final report
      covering all six items.
      → final report §14 verdict + §13 evidence table (six items mapped); NMT criterion
      recorded as BLOCKED with evidence paths; block-level line in
      `logs/tracker-14/r2-lake-read-stall-2026-08-12.md` "Impact on tracker 14".
- [x] Move this plan to `docs/plans/completed/`.
      → moved 2026-08-12 with the NMT deviation documented (criterion 4 externally blocked).

## Technical Details

- **O2 series naming:** Flink PrometheusReporter exports operator gauges as
  `flink_taskmanager_job_task_operator_<metric-name>`; O2 normalizes stream names to lowercase
  with underscores (P8.1 evidence note: "metric names are lowercase stream names"). The dedup
  gauges' dotted names become `flink_taskmanager_job_task_operator_compute_dedup_state_count`
  (matches the dashboard panels already stored in O2). Confirm every name from live `/labels`
  before editing rules.
- **RocksDB metrics:** per-property boolean keys `state.backend.rocksdb.metrics.<kebab>`; the
  property set is `RocksDBProperty` (javap-verified from the pinned 2.2.1 jar). These metrics
  register on the keyed-state operator metric group, so they appear on the TM reporter output.
  They exist only under the RocksDB backend — dev default is hashmap, so runtime proof needs a
  rocksdb run or the gated integration test.
- **Stall guard semantics:** guard fires only when a write/flush exceeds the stall window while
  the writer is active; healthy-path writes complete in ms, so 15000 ms default is ~10x headroom.
  The guard is per-sink and symmetric (LOG + KV) so the shared-fate contract (box 682) is
  unchanged — deleting either table still takes the whole job down, now to FAILED, not a hang.
- **New pin:** `SINK_WRITE_STALL_TIMEOUT_MS` joins the `requirePinned*` set; P4.2's
  "checkpoint pins unchanged" box is unaffected (existing pins untouched).
- **NMT:** `-XX:NativeMemoryTracking=summary` enables tracking; `-XX:+PrintNMTStatistics` prints
  the summary at JVM exit into the teed stdout of run-batch.sh (stdout is the log; no plumbing
  changes). Runtime overhead for a batch job is negligible.
- **ContainerMemory:** cgroup v2 path `/sys/fs/cgroup/memory.current` + `memory.max`; v1
  `/sys/fs/cgroup/memory/memory.usage_in_bytes` + `memory.limit_in_bytes`; a read failure omits
  the gauge (metric must never crash the operator).

## Post-Completion

**Manual verification (dev cluster, live jobs):**

- After Task 1/3: confirm the two dedup alert series and the RocksDB/container series keep fresh
  points on the running distributed job for a few minutes; confirm the two alert rules exist with
  the new alert_ids and fire on a fixture injection (reuse the P8.3 OTLP fixture technique if time).
- After Task 4: the batch-audit log's NMT summary numbers are captured; no load re-run needed.

**External system updates (not in this block):**

- P7 benchmark campaign (excluded by scope decision).
- Fluss tablet/coordinator server-side Prometheus scrape (`:910/:1004`) — requires Fluss config +
  collector receiver + O2 stream; keep the documented limitation until then.
- `infrastructure_logs` receiver (`:854`) and OTLP/gRPC log/trace producer (`:831`) — user-scoped out.
- Upstream apache/fluss `forLogRecords` fix (P3.3 connector patch replacement when a release lands).
- Optional: add a RocksDB native-memory panel to the COMPUTE - Checkpoints & State dashboard in
  o2-provision.py after Task 3's names are confirmed live.
