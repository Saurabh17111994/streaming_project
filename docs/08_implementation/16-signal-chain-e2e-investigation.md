# 16 — SignalChainLiveE2ETest investigation (Design B tail-catch failure)

> **Role:** living investigation dossier for the failing env-gated E2E
> (`SignalChainLiveE2ETest`, SIGNAL-CHAIN-E2E-001) under the Design B
> (local-authoritative dedup) architecture. Records measurements, probe
> results, the narrowing-down of the blocker, and the remaining work plan.
> Written 2026-08-17 during the takeover session; all numbers below were
> measured live against the dev cluster, not estimated.
>
> Cross-links: [04-signal-job.md](04-signal-job.md) (SignalJob dossier),
> [13-candle-log-kv-replay-safety.md](13-candle-log-kv-replay-safety.md) /
> [14-candle-log-kv-replay-safety_2.md](14-candle-log-kv-replay-safety_2.md)
> (KV/LOG scan-safety rules the E2E now follows).

---

## 1. Objective

`SignalChainLiveE2ETest` must pass as the full-chain live proof for Design B:

broker → arrow-bridge → `raw_table_1` → SignalJob → `feature_candles_15s`
(plus signals / forming-bar), with the job staying RUNNING, completing
checkpoints, and **gaining rows in `feature_candles_15s` with
`window_end` after the run started**.

The env contract is defined in the test javadoc; the runner is
`code/01_platform/04_scripts/run-signal-chain-e2e.sh` (draft). Key env:
`SIGNAL_CHAIN_E2E=true` (gate), `E2E_BROKER=faketool|arrow-hft`,
`E2E_RUN_MINUTES`, `E2E_CHECKPOINT_DIR`, `INGESTION_CLASSPATH`,
`ARROW_BRIDGE_BIN`, `FAKETOOL_BIN`, `INSTRUMENT_MANIFEST_PATH`,
`STATE_BACKEND`, `STATE_BACKEND_LOCAL_DIRS`, `STATE_BACKEND_MANAGED_MEMORY`,
`PARALLELISM`.

**Constraint from the operator (2026-08-17):** the E2E must be allowed to run
as long as it needs — no artificial 8-minute / 10-minute caps, no
`timeout_seconds` games to squeeze the run into a tool window. Monitoring
waits are short (30 s), the run itself is open-ended.

---

## 2. Design B context (what is already proven, do not re-litigate)

- `FingerprintDedupFunction` is **state-authoritative**: no Fluss store, no
  query-on-miss, no bounded eviction. `MapState<String, DedupEntry>` (key
  `version|token|fingerprint`) + `MapState<Long, List<String>>` expiryIndex +
  one event-time timer per entry at `first_seen + DEDUP_TTL_MS` (300 000 ms).
- `SignalJob` keyBy's `instrument_token` before dedup — same-token records
  always land on one subtask, so parallelism scaling preserves the dedup
  invariant (verified by `CompatFlinkCheckpointRescaleIntegrationTest`,
  1 → 2, zero re-accepts).
- Full compute unit suite in the worktree: **311 run / 0 failures /
  16 skipped** (matches the handoff). Focused dedup/telemetry set:
  147 run / 0 failures.
- `DedupRocksDbThroughputMemoryIT` passed: steady-state ~51k records/s at p1
  (≈2.5× the 20 480/s target), checkpoints 0.9→14.4 MB, durations 20→120 ms
  — all far under the 30 s timeout. No `Checkpoint expired before
  completing`.
- The reworked checkpoint/restore IT (env-gated) proved: checkpoint carries
  the full dedup state; replayed fingerprints are not re-accepted;
  1 → 2 rescale restore works; ~1.1 s restore.

**Do NOT reintroduce**: synchronous Fluss lookup on the hot path, bounded-cache
eviction, processing-time TTL, or a raised checkpoint timeout as a fix.

---

## 3. The failing assertion and the KV-upsert insight

The E2E's core assertion is `featEnd > featStart` where both counts come from
`countRows(featureTable)` — a **KV batch scan row count** of
`feature_candles_15s`.

**Critical KV semantics (2026-08-17, verified in code + probes):**
`feature_candles_15s` is a **KV table** keyed by `(instrument_token,
window_start, ...)` and the sink is `RowDataSerializationSchema(false, false)`
(INSERT → UPSERT). Therefore:

- Replaying the backlog re-emits candles for windows that **already exist**
  as keys in the table → **upsert, row count unchanged**.
- The row count grows **only when the job emits a candle for a window that
  was never seen before** — i.e., a window whose `window_end` is newer than
  the table's current max (2.2 days old at the time of measurement).

Same reasoning applies to `forming_bar` (KV, PK `instrument_token`): replay
re-upserts existing keys; only live-tail data moves `last_event_time`.

**Consequence:** the E2E cannot pass by replaying the backlog alone. The job
must **catch the live tail** (process records with `event_time` newer than the
last previously-stored row) and let ≥1 fresh 15 s window complete. This is the
whole game.

### 3.1 The backlog's event-time shape (measured 2026-08-17) — old head + cliff

`raw_table_1` (16 buckets, 62M rows, all bucket tails fresh at age 0) is NOT
uniform in event time. Sampling bucket 0 at offset fractions:

| offset fraction | event-time age |
|---|---|
| 0.00 | 62.5 h |
| 0.10 | 56.3 h |
| 0.25 | 55.6 h |
| 0.50 | 3.38 h |
| 0.75 | 2.45 h |
| 0.90 | 2.27 h |
| 0.95 | 0.43 h |

Offsets 0-25% carry 55-62 h-old event times (the "old head", ~43% of every
bucket — measured 26.6M of 62M rows with event >24 h old, uniform 42-45%
across all 16 buckets), then a **cliff** to <3.4 h. Bucket sizes are even
(2.9M-4.7M), so the old head is bigger on the biggest buckets. This shape
matters because the window watermark = MIN over all source splits: a subtask
replaying a bucket's old head caps the whole job's watermark until it crosses
the cliff.

---

## 4. The failure mode (measured, not guessed)

### 4.1 E2E run log evidence (p8 + RocksDB + 15 min, `E2E_RUN_MINUTES=15`)

- Raw table end-offset sum grew **43 569 393 → 56 883 175** during the run
  (fresh ticks flowed; the E2E's own faketool + ingestion subprocess were the
  live feed — the dev docker ingestion was not appending, and the raw tail
  froze at run end when teardown killed the subprocess).
- `feature_candles_15s` stayed **exactly 282 382** across all 58 samples —
  not one new row in 15 minutes.
- **88/88 checkpoints completed**, job RUNNING the whole window, **0
  restarts, 0 checkpoint failures, no `Checkpoint expired`** (the healthy
  checkpoint machinery itself was re-verified on the real pipeline).
- Final assertion failure:
  `feature_candles_15s must gain rows: 282382 → 282382`.

### 4.2 Post-run table probes (same day, live cluster)

| Table | Probe result |
|---|---|
| `feature_candles_15s` KV | TOTAL_ROWS=282 382, **FRESH_AFTER_RUN_START=0**, MAX_WINDOW_END=1786714155000 (≈2.2 days before run start) |
| `forming_bar` KV | rows=1017 (one per instrument), **max last_event_time ≈ 2.25 days old** |
| `Signal_Candidates` LOG | end_offset_sum=115 753 |
| `Signal_Candidates_current` KV | rows=916 |

**The job wrote nothing newer than ~2.2–2.25 days to ANY Fluss table** during
the entire run. Since `forming_bar`'s writer uses **processing-time timers**
(flushes every `FORMING_BAR_WRITE_BATCH_MS` regardless of watermarks), even a
slow-but-progressing replay should have advanced its `last_event_time`
through the backlog. It did not move at all → **the full pipeline never
delivered any record with `event_time` newer than ~2.25 days ago**, i.e. it
consumed roughly the first ~9.2M records of the backlog (measured by
`BacklogCountProbe`: 9 200 619 records precede event time 1786712805275) and
then effectively stalled.

---

## 5. Probe results: what the source CAN do vs what the pipeline does

Standalone probes compiled against the compute module classpath, all p8 +
RocksDB + `OffsetsInitializer.full()` from offset 0, live `raw_table_1`
(backlog ≈ 43.5M end-offset sum at run start, ≈ 56.9M later).

### 5.1 Source-only probe (`SourceRateProbe`, 90 s)

Source → counting map, nothing else.

- Consumed **5.7–7.25M records per subtask** (~50M total) in 90 s.
- `max_event` reached **1786906392166** — the raw table's newest timestamp at
  run end. **The source can drain the entire backlog to the live tail in
  <90 s at p8. The source is NOT the bottleneck.**

### 5.2 Pipeline probe, no sinks (`TopologyProbe`, 90 s)

source → validation → keyBy → dedup → window(15 s) → counter. **No Fluss
sinks, no forming-bar branch, no signal branch.**

- validated=9 003 605, deduped=8 947 906 (dedup drops ~0.6 %), **candles
  emitted = 561 907**.
- **The candle emit path works** — windows fire, `CandleEmitFunction` emits.
- But `max_window_end` advanced only ~59 min of event time in 90 s wall
  (≈100k/s consumption): the pipeline stages throttle replay ~5.5× vs the
  source alone, and the job was still in the OLD part of the backlog at t=90 s.

### 5.3 Full topology with all real Fluss sinks (`SinkProbe`, ~150 s)

`SignalJob.buildTopology(config)` exactly as the E2E runs it.

- Checkpoint state reached 87 MB (job consumed *something*).
- **`forming_bar` still stale (2.25 days), feature still flat** — same as the
  E2E. **Reproduced the E2E failure in 2.5 minutes.** No exceptions in the
  log (checked for ERROR/WARN/backpressure/stall messages — none).

### 5.4 Incremental isolation (narrowing the blocker)

| Probe | Added to the working pipeline | Candles flow? | Tables move? |
|---|---|---|---|
| `FormingBranchProbe` (60 s) | forming-bar **builder** (no sink) | yes, 198 733 | — (no sinks) |
| `SinkStepProbe` (60 s) | real **feature-candles FlussSink** | yes, 356 118 | feature still 282 382, FRESH=0 |
| `FBSinkProbe` (60 s) | forming-bar **writer + real FlussSink** | yes, 346 041 | forming_bar still stale |

**Interpretation:** neither the forming-bar builder, nor the forming-bar
writer+sink, nor the feature sink alone stops candles from flowing. The
blocker that makes the full topology stall (5.3) must be in the **branches not
yet isolated**:

1. `signal-detection` (consumes candles, keyed, → signal LOG sink);
2. `forming-bar-detection` (the **connected two-input stream**
   `formingBars.connect(candles)`, keyed, → signal sinks);
3. `canonical-signal-filter` + `Signal_Candidates` LOG / `Signal_Candidates_current`
   KV sinks;
4. checkpoint-barrier cost of the combined graph (state at chk-88 was 628 MB
   in the E2E — see §6.2).

**RESOLVED — see §5.6 below: it was NONE of these. The topology flows at
~100k/s on heap and ~49k/s on RocksDB with realistic managed memory; the
~10-20k/s ceiling in every "stalled" run was the 128 MB managed-memory
default of local MiniCluster execution starving the RocksDB block cache.**

### 5.6 Root cause — CONFIRMED 2026-08-17: RocksDB managed-memory starvation

| Experiment (same full topology, p8) | Throughput | Tables move? |
|---|---|---|
| `SourceRateProbe` — source only | ~555k/s; drains 50M in <90 s, reaches tail | — |
| `RateBisectProbe` — full topology, **heap** backend | **~100k/s**, 4.8M records/60 s | — |
| `MemSizeProbe big` — full topology, RocksDB, `managed.size=2048m` | **~49k/s** | — |
| `MemSizeProbe small` — full topology, RocksDB, `managed.size=128m` | **~30k/s** | — |
| `SinkProbe` — **real `SignalJob.buildTopology`** (RocksDB, no memory config) | **~20-21k/s** (RocksDB +389M in ~3 min) | forming_bar STALE, feature flat |
| E2E (15-min run, p8) | ~20k/s (dedup state 628M = 6.1M-entry envelope) | both STALE/flat |

**The E2E log is the smoking gun** (local MiniCluster execution):

```
taskmanager.memory.managed.size required for local execution is not set,
setting it to its default value 128 mb
... managedMemory=128.000mb ... slot ... managedMemory=32.000mb
```

`SignalJob.applyRuntimeOptions` sets the state backend, incremental
checkpoints, localdir and managed-memory *toggle*, but **never sets
`taskmanager.memory.process.size` / `taskmanager.memory.managed.size`**.
Under local execution (no flink-conf.yaml), Flink defaults managed memory to
**128 MB total** — 32 MB per slot at p4, 16 MB at p8 — and the RocksDB block
cache + memtables live inside that pool.

**Why it fails:** the Design-B dedup envelope is 20 480 t/s × 300 s ≈
**6.1M entries ≈ 628 MB** of RocksDB state (the E2E's chk-88 was exactly
628 MB). A 16-32 MB block cache cannot hold even a small fraction of that
working set, so RocksDB reads/disks on every state access → sustained
throughput collapses to ~20k/s ≈ the 20 480/s live feed rate. Consequences,
all measured:

- net backlog drain ≈ 0 → the 43.5M→56.9M backlog never shrinks;
- the source never reaches the live tail → every candle emitted is an UPSERT
  of a historical window already in `feature_candles_15s` → row count stays
  282 382 (KV upsert never grows the count);
- `forming_bar` max event time stays ~2.25 days old (newest processed tick is
  still inside the old backlog);
- checkpoints complete fine (88/88) — the stall is throughput, not health.

The **passing** `DedupRocksDbThroughputMemoryIT` sets
`taskmanager.memory.process.size=1024m` + `managed.fraction=0.4` (~400 MB
managed) — that is the exact difference between the IT passing at 51k/s and
the E2E stalling at 20k/s. **This is a test-harness configuration gap, NOT a
Design B correctness bug**: the same full topology sustains ~100k/s on heap
and ~49k/s on RocksDB with realistic managed memory.

---

## 6. Hypotheses ranked (with evidence weight)

### 6.1 H1 — a downstream branch flow-locks the whole graph (strongest)

A slow/hung consumer on one branch backpressures the shared upstream: the
source can push ~500k/s, but the dedup → window output is throttled by
whatever blocks `signal-detection` / `forming-bar-detection` / the signal
sinks. Symptom match: pipeline-with-sinks ≈ 10k/s (9.2M in 15 min) vs
no-sink pipeline ≈ 100k/s vs source-only ≈ 500k/s. This mirrors the ORIGINAL
pre-Design-B failure mode (source flow-locked by a slow downstream), only the
culprit moved from the dedup store lookup to a signal/forming-bar branch.

*Test (next step):* add `signal-detection` → LOG/KV sinks to `SinkStepProbe`;
then add the `connect(candles)` branch. Whichever addition drops candle
throughput to ~0 is the blocker.

### 6.2 H2 — RocksDB/managed-memory starvation in the full graph

The E2E log shows the MiniCluster fell back to
`taskmanager.memory.managed.size ... default value 128 mb` for **all** RocksDB
operators (8 subtasks × dedup + window + forming-bar + signal state). The
E2E's chk-88 shared state was **628 MB** — far above the 14.4 MB envelope the
standalone RocksDB IT measured. Constant flush/compaction under 128 MB managed
memory would explain both the throughput collapse and the checkpoint growth.

*Test:* run the full topology with an explicit managed-memory increase (e.g.
`taskmanager.memory.managed.size` / `STATE_BACKEND_MANAGED_MEMORY` sizing) and
see whether consumption jumps to ~100k/s. Also re-check whether the RocksDB IT
configures managed memory explicitly (it may — the E2E doesn't).

### 6.3 H3 — connected-stream watermark coupling

`formingBars.connect(candles)` has two inputs; the operator's watermark is the
min of both. If the candles input (window output) lags, the connected operator
stalls forming-bar delivery, and the forming-bar writer's *processing-time*
flush still works — but forming_bar did not move, so H3 alone does not explain
the writer receiving nothing. Possible contributor, not primary.

### 6.4 H4 — event-time ordering of the backlog vs the tail

The backlog spans 2.5 days but is **not uniform**: the first 200 records at
offset 0 cover ~8 s of event time; 9.2M records cover the first ~2.25 days.
Not a blocker per se — the source drained it in 90 s — but relevant to sizing:
the backlog is dense near the tail, sparse at the head.

---

## 7. What is NOT the problem (ruled out by measurement)

- **Source throughput / Fluss storage capacity** — source drained 50M in 90 s.
- **Checkpoint timeout / duration** — 88/88 completed, sub-30 s always.
- **Dedup correctness under replay** — dedup drops only 0.6 % in the pipeline
  probe (fresh empty state, correct first-seen behavior).
- **Candle emit path** — 561k candles emitted by the no-sink pipeline probe.
- **The KV scan / LOG offset measurement in the test** — the LOG/KV split was
  already fixed by the previous agent (LOG → `listOffsets` end-offset sum, KV
  → batch scanner; never shared); the counts we used to diagnose are
  trustworthy and cross-checked by independent probes.
- **E2E runtime per se** — 15 min at p8 should have been enough IF the full
  graph consumed at the ~100k/s the no-sink pipeline sustains. It didn't;
  runtime is secondary to finding the flow-lock.

---

## 8. Do NOT / constraints (operator + dossier rules)

1. No artificial 8 min / 10 min caps on the E2E; no timeouts used to dodge a
   slow run. If a bounded wait is needed, use ~30 s and then act.
2. No synchronous Fluss lookup, no bounded-cache eviction, no processing-time
   TTL, no checkpoint-timeout inflation as a "fix".
3. Do NOT delete the legacy backlog; the job must catch the moving tail.
4. Preserve the corrected LOG/KV scan behavior in the test.
5. Keep RocksDB for the E2E (never the dev hashmap default at this envelope).
6. Trust the repository over the handoff where they disagree; say so.

---

## 9. Remaining work plan (ordered — root cause CONFIRMED, see §5.6)

1. **~~Isolate the flow-lock~~** — DONE, it was never a flow-lock. The full
   topology sustains ~100k/s on heap and ~49k/s on RocksDB with realistic
   managed memory; every "stall" was the 128 MB local-execution managed-memory
   default starving the RocksDB block cache.
2. **Test the memory hypothesis** — DONE (`MemSizeProbe`): 128m → ~30k/s,
   2048m → ~49k/s; real job without memory config → ~20k/s ≈ feed rate.
3. **Fix the harness** (the actual remaining change): give the E2E's
   RocksDB a realistic managed-memory budget. Options, in preference order:
   (a) the E2E runner sets `taskmanager.memory.process.size` +
   `taskmanager.memory.managed.size` — but the E2E runs
   `SignalJob.buildTopology` which builds its own `Configuration`, so the
   memory keys must ride in the env passthrough or in `applyRuntimeOptions`;
   (b) add a `TASK_MANAGER_MEMORY_MANAGED_SIZE`-style passthrough to
   `SignalJobConfig` + `applyRuntimeOptions` (mirrors the passing
   `DedupRocksDbThroughputMemoryIT`: `process.size=1024m`,
   `managed.fraction=0.4`), so both the E2E and any embedded run get real
   memory; keep the job-code change minimal and contract-documented.
4. **Fix the E2E count assertion** — DONE 2026-08-17. The 403 s abort was
   NOT a pipeline failure: `countRows` (KV batch scan) under-counted exactly
   one bucket's worth of rows (282 382 → 266 773 ≈ 282 382/16) under
   concurrent write load, tripping "must not shrink". KV rows are never
   deleted, so the true count is the **max over repeated scans**; the helper
   now scans 3 attempts and takes the max (never breaks early on a stable
   value), with a longer 5 s poll timeout. The final teardown count
   283 406 > 282 382 proved the rows were always there.
5. **Watermark-lag explanation (why +1 024 rows only)** — DONE 2026-08-17.
   The window watermark = min over source splits. The raw log has an
   old-head/cliff shape (offsets 0-25% carry 55-62 h-old event times; the
   remaining ~57% of records are <3.4 h old, cliff between 25% and 50%).
   Bucket sizes are even (2.9M-4.7M) but the old head is ~43% of each — the
   subtask holding the largest buckets (8+14 ≈ 8.97M records) lags furthest
   and holds the watermark at ~2.3 d until it clears its old head. The
   +1 024 rows = exactly the FIRST window past the old table max firing per
   instrument (1 024 instruments in the manifest). Once the laggard clears
   the cliff, the watermark jumps to fresh and candles for the fresh range
   fire in bulk.
6. **Re-run the E2E** — FIRST FIXED RUN (02:50-03:11, 20 min) was **clean and
   near-miss**: 120 checkpoints complete, 0 failures, 0 restarts, feature grew
   283 406 → 408 261 (+124 855 rows), forming_bar max reached 9.4 min from
   live. The ONLY failure was the final assertion "at least one candle window
   must complete during the run" — no row had `window_end > runStartMillis`.
   The measured split: forming_bar max age 9.4 min (fastest subtask) vs
   feature max window_end age 220 min (window watermark = MIN over source
   splits = slowest subtask). The backlog's old-head/cliff shape (§3.1) plus
   sequential 2-buckets-per-subtask replay means the subtask holding the two
   LARGEST buckets (8+14 ≈ 8.97M records) clears its old head last, and the
   20-min run ended just before its watermark jumped to fresh. The +124 855
   new rows are gap-fill candles for windows in the [3.7 h, 2.3 d] range —
   mostly empty (the cliff) plus old-head windows missing from the table;
   the min watermark had NOT yet crossed run start.
7. **Re-run again (03:23, E2E_RUN_MINUTES=30)** with: lazy count retry
   (1 scan/sample, re-scan only on a detected dip — removes the 3× full-table
   scan load that halves replay throughput), 30-min run so the slowest
   subtask clears its cliff, p8 + RocksDB + 2048m managed. Acceptance:
   feature gains rows with `window_end > run start`, job RUNNING, checkpoints
   complete, no `Checkpoint expired`, dedup correct, signals/forming-bar
   fresh.
7b. **Three MORE launch-environment root causes (10:00-10:20 session)** — the
   warmup (`raw_table_1 must grow`) began failing 100% after the docker
   stack restart, even though the ingestion service works standalone (smoke
   test appended 444k ticks in 21 s). Root causes, all in the RUNNER's env
   wiring, none in the test or job code:
   - **`INGESTION_CLASSPATH` was never set.** The runner exported the var
     but (i) the original computed it into `INGESTION_CP` and exported
     `INGESTION_CLASSPATH` — a name mismatch shipping an EMPTY classpath —
     and (ii) even the corrected `$(mvn -q … dependency:build-classpath)`
     stdout capture returns EMPTY because `-q` suppresses the classpath
     (it is printed at INFO level). The ingestion subprocess therefore got
     `target/classes:` with no dependency jars, died instantly on a missing
     fluss-client class, and the test's `drainStream` (→ null) hid it.
     Fix: capture via `-Dmdep.outputFile=…` + `cat`, prepend
     `target/classes`; verified len=1938 with fluss-client present. The
     90 s warmup timeout was a symptom of this, not a pipeline fault.
   - **Wrong `INSTRUMENT_MANIFEST_PATH` default.** The runner computed
     `$PROJ_ROOT/Arrow_broker/…` but `Arrow_broker` lives one level above
     the repo (`Flink_Fluss_Infrastructure/Arrow_broker`), so the exported
     path was invalid and overrode the test's correct relative default.
     Fix: launch env exports the real absolute path.
   - **`STATE_BACKEND_MANAGED_MEMORY` is a boolean** ('true'/'false', the
     RocksDB managed-memory toggle), not a byte-size; passing `2048m` threw
     `IllegalStateException` at config parse (caught by
     `SignalJobConfig.stateBackendManagedMemory`). The byte budget rides
     via `TASK_MANAGER_MEMORY_MANAGED_SIZE`. Fix: `=true`.
7c. **p16 attempt — network-memory deploy failure (10:51 run).** Raising
   parallelism to 16 (1 bucket per subtask, eliminating the p8 sequential
   two-bucket-per-subtask watermark reset of §9.5) exposed a SECOND harness
   gap: local MiniCluster defaults network memory to 64 MB (2048 × 32 KB
   buffers), and at 16 subtasks the connected forming-bar branch fails
   deploy with `Insufficient number of network buffers: required 17, but
   only 13 available`. Fix: `TASK_MANAGER_NETWORK_MEMORY_MAX` passthrough
   → `taskmanager.memory.network.max` (+ pin `.min` below it) in
   `SignalJob.applyRuntimeOptions`, guarded by a `RuntimeOptionsTest` case.
7d. **p16 checkpoint instability — UNRESOLVED (11:12 run, 4096m).** After
   the network fix, p16 with 2048m ran 66 clean checkpoints (4-11 s, ~850-
   995 MB) then **Checkpoint 67 expired (>30 s) → restart**; retried with
   4096m (256 MB/slot, matching the p8 per-slot budget that was
   checkpoint-stable for 179 checkpoints) and the run died EARLIER —
   **Checkpoint 15 expired at ~3.3 min**. So at p16 the checkpoint expiry is
   not purely managed-memory; the barrier across 16 RocksDB subtasks is the
   suspect. This was the state when the operator paused E2E work to reclaim
   disk (§11). OPEN: revert to p8 + longer run, or make p16 checkpoint
   stable (larger timeout is FORBIDDEN by §8; the fix must reduce barrier
   cost, not hide it).

---

## 11. Data hygiene — dev-cluster reset (2026-08-17, operator-approved)

All Fluss data is TEST data (probed: every sampled raw row carries
`connection=hft-0`, the faketool slot; event/ingest timestamps are all
current). DDL to recreate every table exists in
`code/01_platform/02_sql/ddl/*.sql`. The root-cause evidence lives in THIS
file (§3.1, §4, §5, §9) — deleting the dev data does not delete the
findings. On operator approval the two big volumes
(`01_docker_fluss-remote-data` ~112 GB, `01_docker_fluss-tablet-data`
~86 GB) are removed and the tables re-created via the DDL apply path. The
only non-reproducible artifact is the old-head/cliff event-time shape (§3.1)
which needs ~2.5 days wall-clock re-feed — its measurements are preserved
above.
   After all three fixes the job reached RUNNING with slots at
   `managedMemory=256.000mb` (p8 × 2048m) and samples flow; the 30-min
   full-replay proof is in progress (10:18 start).
8. **Regression sweep**: full compute suite (313) + focused dedup set +
   RocksDB IT + rescale/restore IT green before closing.
9. **Update dossiers** (04-signal-job.md, this file) with the confirmed
   root cause, the fix, and the final E2E evidence (checkpoint counts, row
   deltas, instruments covered).

---

## 10. Evidence index (files / probes / logs)

| Artifact | Where | What it shows |
|---|---|---|
| E2E run log (p8, 15 min) | `/tmp/e2e-run-p8-final.log` | 88/88 checkpoints, RUNNING, feature flat, final assertion failure |
| Earlier p4 run log | `/tmp/e2e-run.log` | same failure signature at p4 (feature flat 9 min) |
| `SourceRateProbe` | `/tmp/SourceRateProbe.java` | source drains backlog to tail in <90 s |
| `TopologyProbe` | `/tmp/TopologyProbe.java` | pipeline w/o sinks emits 561k candles, ~100k/s |
| `SinkProbe` | `/tmp/SinkProbe.java` | full topology reproduces E2E failure in 150 s |
| `SinkStepProbe` | `/tmp/SinkStepProbe.java` | feature sink alone does not stop candles |
| `FormingBranchProbe` | `/tmp/FormingBranchProbe.java` | forming-bar builder alone does not stop candles |
| `FBSinkProbe` | `/tmp/FBSinkProbe.java` | forming-bar writer+sink alone does not stop candles |
| `FeatureTailProbe` | `/tmp/FeatureTailProbe.java` | feature rows with window_end after run start = 0 |
| `FormingBarProbe` | `/tmp/FormingBarProbe.java` | forming_bar max last_event_time ≈ 2.25 days (stale) |
| `AllTablesProbe` | `/tmp/AllTablesProbe.java` | row/offset snapshot of all SignalJob tables |
| `BacklogCountProbe` | `/tmp/BacklogCountProbe.java` | 9.2M records precede the stale forming_bar timestamp |
| `RawHeadProbe` / `RawTailProbe` | `/tmp/RawHeadProbe.java` etc. | backlog event-time span; fresh tail present |
| E2E checkpoints | `/tmp/signal-chain-e2e-checkpoints-b2/` | chk-88 metadata + 628 MB shared RocksDB state |
