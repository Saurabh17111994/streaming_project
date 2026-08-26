# Performance Audit and Diagnosis — Flink 2.2.1 / Fluss 0.9.1 — Single-VM Dev Stack

**Date:** 2026-08-26  
**Scope:** `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project_New`  
**Mode:** Diagnosis only. No code, configuration, or infrastructure changes were made.  
**Current stack state at capture:** Flink JobManager/TaskManager, ingestion, otel-collector, webhook-receiver exited 127 due to Docker bind-mount initialization errors (`not a directory: Are you trying to mount a directory onto a file`). Fluss coordinator/tablet, MinIO, OpenObserve, Zookeeper, cAdvisor, node-exporter running; `oom=false` on all inspected containers.

---

## Executive diagnosis

The project does not yet have evidence that it meets all three targets simultaneously:

- **Low latency:** not proven because p99 latency data is unavailable.
- **High throughput:** the 50k sustained target was not achieved; the measured SignalJob path is feed/tablet limited in the tested envelope.
- **No OOM:** not proven. There is historical evidence of host-wide/container `Exit 137`, large RocksDB/checkpoint growth, a prior restore direct-memory OOM, and a Fluss client heap OOM.

The strongest bottlenecks and risks are:

1. **Input/storage throughput ceiling**
2. **Large Flink RocksDB dedup state and checkpoint amplification**
3. **Full-history replay on startup**
4. **Per-tick topology fan-out and forming-bar object/edge overhead**
5. **Unverified Fluss client/server buffer defaults**
6. **Single-node deployment and host resource contention**
7. **Missing runtime latency telemetry**
8. **Deployed-version and image-pin drift**

---

## Effective configuration (facts, verified)

### Flink / Fluss versions

- **Deployed:**
  - Flink `2.2.1-scala_2.12-java17`
  - Fluss `0.9.1-incubating`
- **Source checkouts:**
  - `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/flink` → `2.4-SNAPSHOT`
  - `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/fluss` → `0.9-SNAPSHOT`

> Source-derived defaults and code paths must be verified against the deployed 0.9.1 / 2.2.1 artifacts before tuning. The Fluss 0.9-SNAPSHOT source is the closest available reference.

### SignalJob runtime configuration

- **File:** `code/01_platform/01_docker/docker-compose.yml` (`x-flink-common`)
- **TaskManager:** `taskmanager.memory.process.size: 6g`, `taskmanager.memory.managed.fraction: 0.4`, `taskmanager.memory.task.off-heap.size: 512m`, `taskmanager.memory.network.max/min: 256m`, `taskmanager.numberOfTaskSlots: 8`
- **Env overrides:** `TASK_MANAGER_MEMORY_MANAGED_SIZE: 2g`, `STATE_BACKEND: rocksdb`, `STATE_BACKEND_MANAGED_MEMORY: true`
- **Job parallelism:** `env.setParallelism(8)` in `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:174`
- **State backend:** RocksDB incremental on SSD (`state.backend: rocksdb`, `state.backend.incremental: true`, `state.backend.rocksdb.localdir: /tmp/flink-rocksdb`)
- **Checkpointing:** `env.enableCheckpointing(10000, EXACTLY_ONCE)`, `CHECKPOINT_TIMEOUT_MS: 30000`, `MAX_CONCURRENT_CHECKPOINTS: 1`
- **Source:** `FlussSource` with `OffsetsInitializer.full()` and `ALLOW_FULL_REPLAY: true`

### Fluss cluster and table configuration

- **File:** `code/01_platform/03_fluss/fluss.properties` — only basic coordinator/tablet host, bucket, and lake properties
- **DDL bucketing:**
  - `raw_table_1` → `bucket.num=16`, LOG, `table.log.ttl=7d`
  - `feature_candles_15s` → `bucket.num=16`, KV `table.kv.format-version=2`
- **Most Fluss writer/server settings are implicit defaults** (not set in compose, `fluss.properties`, or sink builders)

### Fluss defaults from source

**Source:** `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java` (0.9-SNAPSHOT)

| Setting | Default |
|---|---:|
| `client.writer.buffer.memory-size` | 64 MB |
| `client.writer.buffer.page-size` | 128 KB |
| `client.writer.buffer.per-request-memory-size` | 16 MB |
| `client.writer.batch-size` | 2 MB |
| `client.writer.batch-timeout` | 100 ms |
| `client.writer.max-inflight-requests-per-bucket` | 5 |
| `client.writer.enable-idempotence` | enabled when compatible |
| `server.buffer.memory-size` | 256 MB |
| `server.buffer.page-size` | 128 KB |
| `server.buffer.per-request-memory-size` | 16 MB |
| `netty.server.num-network-threads` | 3 |
| `netty.server.num-worker-threads` | 8 |
| `netty.server.max-queued-requests` | 500 |
| `kv.rocksdb.thread.num` | 2 per bucket |
| `kv.rocksdb.writebuffer.size` | 64 MB |
| `kv.rocksdb.writebuffer.count` | 2 |
| `kv.rocksdb.block.cache-size` | 8 MB |

Effective values for the running 0.9.1 image have not yet been captured.

---

## Ranked findings

### 1. Proven throughput ceiling: Fluss/tablet or feed path — HIGH / HIGH

**Evidence**

- `logs/tracker-14/p7-battery-design-b-20260817.md:11` — replay consumption ~60,867 rows/s, tablet ceiling ~58.9–59.7k rows/s, steady live feed ~18,441 rows/s
- `logs/tracker-14/sig-perf-001-50k-baseline-20260817.md` — 50k baseline not achieved under single-socket 1,024-token policy
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:188` — full-history source (`OffsetsInitializer.full()`)

**Diagnosis**

The tested SignalJob path reaches a plateau around **59k rows/s at the Fluss tablet ceiling**. The 50k target leaves little headroom for checkpoint overlap, recovery, lake/tiering I/O, GC/native pressure, burst skew, or failure handling.

**Not proven to be:** Flink CPU saturation, network-credit backpressure, or Fluss writer-buffer exhaustion — no per-subtask metrics were available in the current stopped-stack state.

---

### 2. Large Flink dedup state creates the primary OOM/checkpoint risk — CRITICAL / HIGH

**Evidence**

- `logs/tracker-14/p7-battery-design-b-20260817.md:14` — ~25.6M dedup entries, ~3.5 GB estimated state during replay, steady-state ~628 MB at 20,480 rows/s, full checkpoint p50 ~1.6 GB / max ~1.9 GB
- `logs/tracker-14/e2e-30min-soak-20260817.md` — last checkpoint ~775.6 MB / 2.478 s (still large)
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FingerprintDedupFunction.java` — complete 5-minute dedup set in Flink keyed `MapState` with native TTL, checkpointed with source offset
- `code/01_platform/01_docker/docker-compose.yml:94-112` — 6g TM, 0.4 managed, 512m off-heap, incremental RocksDB

**Diagnosis**

Design-B makes the complete dedup set Flink-authoritative. At high replay volume, memory is spread across RocksDB state, block cache/memtables, managed memory, checkpoint buffers, direct/off-heap, and network memory. Recorded 1.6–1.9 GB checkpoints are large relative to the 6 GB TM envelope. Checkpoint duration already failed during replay in prior evidence. “No OOM” is not established for the 50k / 3,000-instrument target.

---

### 3. Full-history replay causes startup/recovery bursts — HIGH / HIGH

**Evidence**

- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:184-189` (`.setStartingOffsets(OffsetsInitializer.full())`)
- `code/01_platform/01_docker/docker-compose.yml:45` (`ALLOW_FULL_REPLAY: true`)
- `docs/08_implementation/04-signal-job.md` — replay-related state growth and checkpoint duration failures

**Diagnosis**

A new start or unsafe restore can reprocess the entire available `raw_table_1` history before reaching live tail. With 7-day retention, this bursts source load, RocksDB growth, checkpoint size, compaction, and time-to-live-tail, increasing checkpoint timeout and native-memory risk. Recovery is not equivalent to low-latency resumption while full replay is enabled/selected.

---

### 4. Multiple Fluss sinks multiply client resources — HIGH / MEDIUM-HIGH

**Evidence**

- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:256,288,376,424,440,462`
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/TradeDecisionsSinks.java:46,63`
- Normal topology has 5 active `FlussSink` definitions (6 if execution-intent enabled), at `parallelism=8` → up to **40 writer instances** (48 with execution-intent)

**Diagnosis**

Each sink writer may own its own connection, sender, `RecordAccumulator`, memory segment pool, and Arrow allocator. Fluss source confirms allocations via `LazyMemorySegmentPool` and `ArrowWriterPool` in `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java:120-144`, with readiness logic in `ready()` that evaluates accumulated batches and blocking conditions. The actual connector sharing must be verified from the deployed connector, but the topology clearly multiplies client heap/direct and request pressure. This is a strong candidate for client heap pressure and tail latency; first-saturated component is not yet proven without live writer metrics.

---

### 5. Per-tick forming-bar branch adds hot-path fan-out — MEDIUM-HIGH / MEDIUM

**Evidence**

- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FormingBarBuilderFunction.java` — emits `FormingBar` on every accepted tick (main + persistence side output)
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FormingBarWriterFunction.java` — correctly coalesces persistence writes
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:207-222,338-357,369-390` — at least 3 keyed shuffles before the connected detector

**Diagnosis**

Persistence coalescing avoids one Fluss write per tick, but every accepted tick still incurs forming-bar state update, object allocation, main emission, side-output emission, downstream connected processing, and keyed routing/serialization where edges are not chained. Architectural pressure is proven; runtime cost is unmeasured without per-operator metrics/flame.

---

### 6. Fluss writer/server settings are mostly implicit defaults — HIGH UNCERTAINTY / HIGH (not tuned)

**Evidence**

- No explicit `client.writer.buffer.*`, `client.writer.batch.*`, `client.writer.max-inflight-requests-per-bucket`, `server.buffer.*`, `netty.server.*`, or `kv.rocksdb.*` settings in `fluss.properties`, compose, or sink builders
- Source defaults listed above from `ConfigOptions.java`

**Diagnosis**

Possible unresolved bottlenecks include writer pages exhausted during bursts, writer blocking on acks, excessive in-flight requests, server buffer contention, Netty queueing, KV write-buffer/compaction stalls, and batching too small or too slow. This is an evidence gap, not yet a proven bottleneck.

---

### 7. Single-node Fluss and host contention make results non-representative — HIGH / HIGH

**Evidence**

- Single Fluss tablet/coordinator in compose
- `logs/eod-retention-extend-20260824T171204Z.md:158-159` and `docs/05_deployment/change-records/CHG-098.md:30-31` — host-wide `Exit 137` under 3-parallel workloads; SignalJob `5869c40e…` lost
- `logs/chaos/chaos-20260824-155057/SUMMARY.txt` — tablet-kill failed, VM-loss skipped (single-node)
- `code/01_platform/01_docker/docker-compose.yml:66-68,94` — managed `2g` vs `0.4×6g` ambiguity adds budgeting risk

**Diagnosis**

Many services compete on one host (Flink, Fluss, ZK, ingestion, MinIO, O2, OTEL, execution services). Local failures can reflect host pressure rather than an isolated Flink/Fluss pool. The recorded `Exit 137` without captured `memory.current/max` means the exact victim pool is unclassified. Production HA/replica/VM-loss remains unverified.

---

### 8. Low-latency target is not currently measurable — CRITICAL / HIGH

**Evidence**

- `logs/tracker-14/p7-battery-design-b-20260817.md:12` — p99 **NOT MEASURABLE** (histogram buckets dropped / OTel series no data)
- `docs/08_implementation/04-signal-job.md` — sink keep-up and checkpoint metrics available, but no p99 panel

**Diagnosis**

System cannot currently prove p50/p95/p99 source-to-candle, source-to-sink, Fluss append, Fluss writer wait, mailbox delay, or checkpoint-overlap impact. A mean exporter value is insufficient for a p99 SLO.

---

### 9. Memory configuration has competing declarations — MEDIUM / HIGH

**Evidence**

- `docker-compose.yml:94` (`6g`), `docker-compose.yml:108` (`0.4`), `docker-compose.yml:68` (`2g`), `SignalJob.java:527-542` (sets both fraction and explicit size)

**Diagnosis**

Project declares both a derived managed fraction and an explicit managed size. Code comments say explicit size wins, but the effective `Configuration` from the deployed TaskManager has not been captured. Verification is required for managed memory, heap, direct, network, RocksDB native, RSS, and cgroup limits.

---

### 10. Deployed runtime does not match skill source versions — HIGH / HIGH

- Deployed: Flink `2.2.1`, Fluss `0.9.1-incubating`
- Checkouts: Flink `2.4-SNAPSHOT`, Fluss `0.9-SNAPSHOT`

Source checkouts inform intended behavior but do not prove deployed behavior. All source-derived statements must be re-checked against the deployed JARs/image before tuning.

---

### 11. Image pin discipline not satisfied — MEDIUM / HIGH

**Evidence:** `code/01_platform/01_docker/.env:15-17` uses moving tags (`apache/fluss:0.9.1-incubating`, `flink:2.2.1-scala_2.12-java17`, `openobserve:v0.91.5-amd64`) instead of immutable digests.

This prevents reliable before/after comparison.

---

## OOM classification (verified)

| Event | Classification | Evidence |
|---|---|---|
| Current `Exit 127` (Flink/ingestion/otel/webhook) | Not OOM — Docker bind-mount init failure | `docker inspect` (`oom=false`, `not a directory` mount error) |
| Historical tablet-kill scan OOM | Fluss client Java heap OOM in `RecordAccumulator.ready()` | `logs/chaos/chaos-20260824-070137/03-tablet-kill.log:40-44,168-182` |
| 2026-08-24 full stack `Exit 137` | Host/global OOM or memory pressure; exact victim not captured | `CHG-098`, `logs/eod-retention-extend-20260824T171204Z.md:158-159` |
| Historical Flink restore OOM | Direct-buffer OOM during ~1.7 GB RocksDB restore | `docs/08_implementation/11-testing-and-release.md:1139` |
| Chaos-03 later failure (2026-08-24) | Recovery/scan timing failure; data invariants not reached | `logs/chaos/chaos-20260824-155057/03-tablet-kill.log` + `performance-audit-2026-08-26.md` prior finding |

---

## Bottleneck classification

| Area | Verdict |
|---|---|
| Feed/source capacity | Proven constraint |
| Fluss tablet throughput | Measured ceiling ~58.9–59.7k rows/s in tested replay path |
| Flink CPU saturation | Unproven — no busy-time/flame |
| Network-credit pressure | Unproven — no buffer usage |
| Fluss client buffer stall | Unproven — no writer wait metrics |
| Fluss server buffer stall | Unproven — no server buffer metrics |
| KV RocksDB compaction stall | Plausible, unproven |
| Dedup memory growth | Proven major OOM/checkpoint risk |
| Checkpoint duration pressure | Proven during replay |
| Full replay burst | Proven by config/topology |
| Forming-bar fan-out overhead | Design-proven, runtime unmeasured |
| Low-latency p99 | Not measurable |
| Host-level OOM | Observed Exit 137; exact pool unclassified for that event |
| Production-scale behavior | Blocked/unverified (needs prod VMs) |

---

## Overall verdict

Dominant risk envelope:

```text
full-history Fluss replay
  → high-volume Flink keyed dedup MapState
  → RocksDB state/checkpoint growth
  → multiple keyed branches + per-tick forming-bar events
  → multiple Fluss sinks (client accumulator/sender multiplication)
  → Fluss tablet throughput ceiling
  → checkpoint/recovery + host-memory contention
```

- **High throughput:** First hard measured ceiling is ~59k rows/s at the tablet path; 50k leaves little margin.
- **Low latency:** Lacks p99 evidence; forming-bar fan-out + checkpoint overlap + sink queueing are credible tail risks.
- **No OOM:** Failed historically (Fluss client heap OOM + Flink direct OOM + host OOM); not currently satisfied.

---

## Required evidence before changing one lever

No tuning change should be made yet. First capture one controlled run with:

1. Flink per-subtask `busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond`, `idleTimeMsPerSecond`, records in/out
2. Checkpoint sync/async phase durations, alignment, size, failed/expired, restore duration
3. Flink/container memory: RSS/current/max, heap, managed, direct/off-heap, RocksDB block-cache/memtables, GC
4. Fluss client: writer buffer usage, page waits, request latency, batch fill, in-flight, retries
5. Fluss server: buffer usage/waits, Netty queued, append latency, log flush latency, KV flush/compaction
6. Data distribution: per-subtask/bucket rate, hottest-bucket share, bucket count/replication, event size
7. Latency: source→candle, source→sink, p50/p95/p99 (not mean only)

**No source or configuration files were changed in this diagnosis.**

---

## Files referenced

- `code/01_platform/01_docker/docker-compose.yml`
- `code/01_platform/03_fluss/fluss.properties`
- `code/01_platform/02_sql/ddl/02_raw_table_1.sql`, `03_feature_candles_15s.sql`
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FingerprintDedupFunction.java`
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FormingBarBuilderFunction.java`
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FormingBarWriterFunction.java`
- `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/TradeDecisionsSinks.java`
- `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java` (0.9-SNAPSHOT)
- `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java`
- `logs/tracker-14/p7-battery-design-b-20260817.md`, `sig-perf-001-50k-baseline-20260817.md`, `e2e-30min-soak-20260817.md`
- `logs/chaos/chaos-20260824-070137/03-tablet-kill.log`, `logs/chaos/chaos-20260824-155057/03-tablet-kill.log`
- `logs/eod-retention-extend-20260824T171204Z.md`, `docs/05_deployment/change-records/CHG-098.md`
- `docs/08_implementation/04-signal-job.md`, `docs/08_implementation/11-testing-and-release.md`
