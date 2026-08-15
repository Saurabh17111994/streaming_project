# Production Swarm

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready, infrastructure/version evidence blocked |
| Owner | Platform Team |
| Topology | Four VMs: three workload/HA, one observability |
| EOD controller | Named service or scheduled job owning manifest lifecycle |
| Live-money | Disabled until Phase 12 evidence passes |
| Acceptance criteria | `REQ-PF-001`–`REQ-PF-012` → `AC-PF-001`–`AC-PF-019` (proving families: `SWARM-*`, `SEC-*`, `PERF-NODELOSS-001`; local subset in `08-local-compose.md`) |

### Placement model

| Node class | Required workload | Disk |
| --- | --- | --- |
| Workload VM 1 | Fluss replica/quorum, ZooKeeper ensemble member (1 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Workload VM 2 | Fluss replica/quorum, ZooKeeper ensemble member (2 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Workload VM 3 | Fluss replica/quorum, ZooKeeper ensemble member (3 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Observability VM | OpenObserve and telemetry storage/collection | 500 GB SSD |

Fluss replicas cannot co-locate, and ZooKeeper ensemble members cannot co-locate: exactly one ZooKeeper node per workload VM. All three replicas of any critical Fluss/Flink role SHALL be placed across separate workload VMs via anti-co-location constraints. The 3-node ZooKeeper ensemble (quorum 2-of-3) and 3-node Fluss LOG-table replication survive loss of any single workload VM. OpenObserve loss must not authorize orders or erase local durable audit.

The final service-to-node placement, CPU, RAM, SSD IOPS/throughput, and network bandwidth are `EVIDENCE-BLOCKED` until `PERF-PROD-60000-001` and `FAIL-VM-LOSS-60000-001` pass. Current allocations (500 GB SSD per VM) are a starting point, not a proven sizing result.

### Stack requirements

The production stack must define:

- Immutable image digests for every image.
- Exact Java/Python/Flink/Fluss/connector/protocol versions.
- 3-node ZooKeeper ensemble (`zookeeper:3.9.2`), one member per workload VM, quorum 2-of-3, durable per-node data volume, internal-only ports (2181 client, 2888/3888 peer/leader).
- Placement constraints and anti-co-location.
- Resource reservations/limits.
- Health checks and readiness dependencies.
- Restart, update, rollback, and shutdown policies.
- Encrypted overlay/TLS-protected transport mandatory for all sensitive paths (broker, Arrow REST, S3, operator control, secret delivery, money-moving/state traffic).
- N+1 resource budget: per-VM CPU, memory, network, disk, Flink slots, Fluss capacity, checkpoint bandwidth, and catch-up rate documented; post-loss validation at 50,000 ticks/s variable average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average).
- Internal-only Fluss/tablet/checkpoint ports.
- Swarm secrets and least-privilege identities.
- Durable per-node Fluss volumes.
- Encrypted/versioned S3 checkpoint, Iceberg, and audit storage.
- Executor fencing and one active owner per `execution_partition_id`.

### Readiness sequence

1. Verify image digests, Swarm secrets, encrypted networks, volumes, and S3.
2. Verify ZooKeeper ensemble quorum (2-of-3), then Fluss quorum, replication, tablets, placement, and schema manifest.
3. Verify Flink control/workers (JobManager HA leader elected via ZooKeeper), checkpoint storage, and HA metadata storage.
4. Deploy Signal and Babysitter artifacts; verify running/checkpointing.
5. Verify Ingestion manifest/subscriptions and Action Capture protocol readiness.
6. Start Executor `HALTED`; verify state, mappings, continuity, Arrow REST contract, fencing, and telemetry.
7. Complete broker/order/fill/position/attempt/reservation reconciliation.
8. Require two distinct approvals for the same gate epoch/evidence hash.
9. Enable only the approved gate epoch.

A container or service becoming healthy never enables order placement.

### Bootstrap and scaling (config-driven)

The production stack is config-driven: the same stack files, environment variables, secrets, and artifacts run at any node count. Bootstrap on one production-like VM first, then scale to three by configuration and node addition, not by rewriting the stack.

| Stage | Nodes | ZooKeeper | Fluss replication | Flink HA | Status |
| --- | --- | --- | --- | --- | --- |
| Bootstrap | 1 workload VM + observability VM | Single node (no quorum) | Replication factor 1 (LOG and KV) | JobManager HA disabled | Validates config/DDL/connectors/jobs only; NOT HA evidence |
| Target | 3 workload VMs + observability VM | 3-node ensemble, quorum 2-of-3 | LOG replication ≥2, anti-co-located | `high-availability.type: zookeeper`, standby JobManagers | Production HA topology |

Scale-out steps (1 → 3 VMs): add the two workload nodes and labels, convert ZooKeeper single-node to the 3-member ensemble (update `server.X` entries and quorum config), raise Fluss LOG-table replication factor, enable Flink ZK HA with the ensemble quorum, and re-verify the readiness sequence. The single-VM bootstrap stage SHALL NOT be cited as quorum, replication, or HA evidence.

### Storage and recovery

- Fluss data uses durable per-node volumes and tested replication (LOG tables; KV tables are single-replica in Fluss 0.9.1 — durability via Fluss remote storage + rebuild from audit (Flink checkpoints hold only small working/recovery state — DEC-038)).
- ZooKeeper ensemble members use durable per-node volumes; loss of one member is tolerated while quorum (2-of-3) holds.
- Flink checkpoints/savepoints use encrypted versioned S3; Flink JobManager HA metadata (`high-availability.storageDir`) uses the same encrypted S3 store, with leadership in ZooKeeper.
- Iceberg/audit storage uses encryption, versioning, and approved retention/lifecycle policy.
- Operational projections are rebuildable from immutable events/audit or a tested backup.
- Loss of any one workload VM is tested at 50,000 ticks/s variable average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average).
- RPO/RTO is recorded per failure scenario; no untested global claim is made.
- Flink JobManager HA: `high-availability.type: zookeeper` with `high-availability.zookeeper.quorum` = the 3-node ensemble, `high-availability.storageDir` = encrypted S3, `high-availability.zookeeper.path.root: /flink`, per-cluster `high-availability.cluster-id`. Multiple standby JobManagers run across workload VMs; ZooKeeper elects the leader, and a standby takes over JobManager failure without a full job re-submission.
- Checkpoint restart strategy: max 3 retries at 30s pause between attempts. After 3 consecutive checkpoint failures, the job fails. Swarm restarts it from the last successful checkpoint. If no valid checkpoint exists, the job stays down → critical alert → manual savepoint restore. Deployment SHALL reject unbounded retry. [Source: `REQ-FC-008`. The pre-DEC-038 headroom note ("estimated checkpoint size ~600 MB – 1 GB; 30s timeout provides 2-5× headroom over estimated write time") is **superseded 2026-08-14** — under DEC-038 the dedup set moves to Fluss and the checkpoint is small; the headroom statement is re-derived from measured post-externalization checkpoint size, not asserted.]

### Security and networking

Define separate logical networks for:

```text
data ingress
storage/control
compute
execution
observability
operator control
```

Only required ingress endpoints are exposed. Service identities are least-privilege. Broker/Arrow REST/S3/OpenObserve/TLS/operator credentials rotate through Swarm secrets or approved workload identity.

### Deployment and rollback

Every change record includes:

- Artifact digests and version matrix.
- DDL/schema/state compatibility.
- Savepoint/checkpoint impact.
- Gate halt and reconciliation requirement.
- Prechecks and acceptance evidence.
- Rollback artifact and state-readability path.
- Post-deployment verification.

Any uncertain rollback returns the Executor gate to `HALTED`. Schema-breaking clean break is permitted only before live-money release with reset/replay evidence.

### Capacity acceptance

The production-like topology must prove:

- variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) for a full session with decision p99 under 100 ms.
- One workload VM loss at the per-instrument production rate.
- Data recovery under 30 seconds for accepted scenarios.
- Safe-halt under five seconds.
- Full-volume EOD verification under 30 minutes.
- No duplicate broker order in crash-window tests.

### JVM and memory configuration

Every Java container (Ingestion, Flink TaskManager, Flink JobManager) SHALL enforce:

| Config key | Required value | Enforcement |
| --- | --- | --- |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap equals 65% of the container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap must be at least 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Emit critical alert at or above 85% total container memory |

- Set an explicit container memory limit for every Java container.
- Verify at startup that the container memory limit minus maximum heap is at least 35% of the container memory limit.
- Refuse production readiness when total container memory reaches or exceeds 85% for 60 consecutive seconds.

### Concrete sizing (48 GB VM) — pre-DEC-038, superseded

Derived for a Flink TaskManager on a 48 GB VM. All numbers are starting points — superseded by `PERF-PROD-60000-001` **and by the DEC-038 state-ownership change (2026-08-14)**. This table sized RocksDB around the ~1 GB dedup state budget; under DEC-038 the dedup set moves to Fluss and the Flink-side state is small (windows, timers, bounded working cache), so the RocksDB/direct-memory dominance and the split between the generic 65%/35% formula and a RocksDB-heavy split must be **re-derived from measured post-externalization memory**, not asserted. The rows below are the pre-change baseline.

| Resource | Value | Notes |
| --- | --- | --- |
| Container memory limit | 48 GB | Explicit Swarm limit |
| Java max heap (`-Xmx`) | **8 GB** | Modest — working state lives in RocksDB (direct memory); pre-DEC-038 rationale — re-derive after externalization |
| Direct memory (`-XX:MaxDirectMemorySize`) | **30 GB** | RocksDB block cache + Flink network buffers; pre-DEC-038 rationale — re-derive after externalization |
| OS reserve | **~10 GB** | OS page cache, off-heap, Fluss client |
| GC | `-XX:+UseG1GC -XX:MaxGCPauseMillis=20` | Protect p99 <100 ms decision SLO |
| Container memory alert at 85% | ~40.8 GB | Critical alert when hit for 60 consecutive seconds |

For non-Flink containers (Ingestion, Action Capture, Executor), use the generic 65%/35% formula above. The Flink TaskManager split is different because RocksDB uses direct memory for its block cache and SST buffers. Source (pre-DEC-038): dedup state budget ~1 GB, window + candidate + ranking state <10 MB, leaving substantial headroom for RocksDB block cache, write buffers, and network memory — the dedup term moves to Fluss under DEC-038 and this rationale is re-derived.

### Acceptance checklist

- [ ] Production stack is separate from Compose.
- [ ] No mutable image tags or unpinned dependencies remain.
- [ ] Placement prevents Fluss replica and ZooKeeper ensemble-member co-location.
- [ ] ZooKeeper ensemble quorum 2-of-3 verified; one ZK node loss tolerated.
- [ ] Secret/identity/network tests pass.
- [ ] S3 checkpoint/lake/audit recovery passes.
- [ ] One-VM loss passes with documented RPO/RTO.
- [ ] Job, service, durability, and trading readiness are separate.
- [ ] Rollback defaults to halted and preserves state readability.

## Verification mapping

The required behavior above is verified by the canonical [Production Swarm test design](./11-testing-and-release.md#production-swarm): `SWARM-INT-001`, `SWARM-INT-002`, `SWARM-FAIL-001`, `SWARM-FAIL-002`, `SWARM-REC-001`, `PERF-NODELOSS-001`, and `SEC-NET-001` to `SEC-AUDIT-001`.

---

## P10 rehearsal and cutover plan (RE-SCOPED 2026-08-13)

### P10 — Signal LOG/KV Dual-Sink Rehearsal & Cutover Runbook (tracker 14)

**Status:** `RE-SCOPED 2026-08-13 — previously PLANNED (not yet executed) as a candle KV migration rehearsal; target changed to the SIGNAL dual-sink per requirement change. Phase 0 isolation groundwork (overlay compose, empty rehearsal trio) remains valid; no data copy was ever performed.`
\*\*Location:\*\* `docs/08_implementation/09-production-swarm.md`
**Tracker:** `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` — `## P10 — Operator-only migration and cutover` (RE-SCOPED), `## P10.1 Isolated rehearsal`, `## P10.2 Production blue-green cutover`, `## P10.3 Rollback`.
**Sequencing gate:** the tracker says "Do not execute until P1–P9 code/evidence gates are complete" — this plan starts strictly AFTER (a) the signal dual-sink implementation (candle **KV-only** sink + `Signal_Candidates` LOG + `Signal_Candidates_current` KV) and (b) the P7.2/P7.3 battery re-run on the new topology (`docs/08_implementation/11-testing-and-release.md`).
**Recipe source:** `skill://candle-kv-rollback-rehearsal` (B8.7 rollback/re-cutover procedure pattern) + `candle-failure-injection-tests` (state-restore verification patterns) + `§P3.5 of 14-candle-log-kv-replay-safety_2.md (plan file never persisted)` (R2 containment).

> **REQUIREMENT CHANGE (user decision, 2026-08-13):** the candle [LOG + KV] facility is
> RETIRED (no per-stock candle audit). The [LOG + KV] facility moves to the trade-signal
> tables: `Signal_Candidates` → LOG (append one row per found signal, never updated) and
> `Signal_Candidates_current` → KV current-state (PK `instrument_token`, latest/active
> candidate per instrument, supersession overwrites). This rehearsal rehearses **that**
> dual-sink: cutover, bounded-replay idempotency (LOG grows / KV keys frozen), rollback,
> and re-cutover. There is **no candle history audit, no conflict reconciliation, no
> migration load**. The earlier Phase 0 record (`logs/tracker-14/p10-rehearsal-20260813.md`,
> divergences D1–D6) stays valid for the isolation mechanics (byte-copy, warehouse
> prefix, remapped ports); the candle-specific rehearsal steps below were removed.

## 1. Objective

Execute the full P10.1 isolated rehearsal on the dev host (all 10 re-scoped boxes), deliver P10.2/P10.3 as a ready-to-run production runbook (execution deferred — no production exists), and leave the register/evidence trail so production cutover is a command sequence, not a discovery exercise. Dev cluster is the qualification target (user decision); "production objects" in P10.1 wording = the live dev stack's objects.

## 2. Locked spec (user decisions, 2026-08-12 + 2026-08-13)

### 2.1 Scope decisions

| Dimension | Decision |
| --- | --- |
| Production definition | Dev cluster = qualification target; P10.2/10.3 delivered as a READY runbook, executed only when a real production deployment exists |
| Isolated env | Second compose project on this host (the `p10` overlay built 2026-08-13): separate project name → separate network, container names, remapped ports (12181/19123/19124/18081/19249/19250); live stack untouched |
| Rehearsal data | Full dev data + checkpoints (Fluss tablet segments + ZK metadata + archived known-good checkpoint); R2 lake objects shared read-only where the source reads the lake tier (§2.2) |
| Table provisioning | `feature_candles_15s` (KV, PK `(instrument_token, window_start)` — sole candle output, converted 2026-08-13), `Signal_Candidates` (LOG), `Signal_Candidates_current` (KV, PK `instrument_token`) via the identical gated DDL path; preflight validator re-targeted (SIGNAL-SCHEMA-001) |
| Signal LOG contract | Append one row per fired signal; never updated; replay appends are retained as evidence (never silently deleted) |
| Signal KV contract | Exactly one latest/active candidate per instrument; supersession replaces the row; KV key count == active instruments after replay |
| Bounded replay | Bounded replay run TWICE from the same source offsets — idempotency proof: signal LOG may grow, `Signal_Candidates_current` key count frozen |
| Checkpoint source | Archived known-good checkpoint (archived before live dev runs rotate it); restore reads the archive copy, never the live checkpoint dir |
| Rollback rehearsal | B8.7-style FULL: rollback (dual-sink → single-LOG artifact, pre-cutover checkpoint, signal KV frozen / LOG grows) AND re-cutover (dual-sink from its own checkpoint) |
| Operator | Autonomous execution by the agent; user reviews evidence + approves register rows |
| Sequencing | Strictly after signal dual-sink implementation + P7 re-run (no parallel override) |
| Consumers | P10.2 "point current-state consumers to `Signal_Candidates_current`" marked VACUOUS today (no consumers exist) — delivered as a runbook step for when the downstream pipeline lands |
| DDL path | Identical gated path: `ddl_apply.py` + version-matrix gate, against the isolated env's coordinator |
| Exposure record | Signal LOG + KV envelope: timestamp range + row counts + sampled keys written during the dual-sink window |

### 2.2 Isolation mechanism (design, verified against Fluss lake layout at execution)

- Fluss lake objects live at `s3://<bucket>/lake/<database>/<table>/...` (verified path layout: `lake/default/candle_scale_log/metadata/`). The isolated env creates its tables in a dedicated database (e.g. `rehearsal`) → its lake writes land at `lake/rehearsal/<table>/` — no R2 object collision with live `lake/default/...`.
- The rehearsal SignalJob reads the copied `raw_table_1` history (log segments + attached lake tier, read-only) and writes new candles/signals to the rehearsal env's own tables (db `rehearsal`, distinct lake path + separate tablet data dirs).
- Checkpoint restore reads the ARCHIVED COPY (`s3a://.../p10-rehearsal/<run>/chk-N` + `shared/`), never the live checkpoint dir.

## 3. Prerequisites (checked at Phase 0 entry)

- [ ] Signal dual-sink implemented: candle **KV upsert** sink (PK `(instrument_token, window_start)`), `Signal_Candidates` LOG append sink, `Signal_Candidates_current` KV sink; `CandleGraphReplayIntegrationTest` re-scoped and green.
- [ ] P7 battery re-run on the new topology with evidence registered (`PERF-*` + `DEDUP-MEMORY-001` rows annotated).
- [ ] Archived known-good checkpoint copied to a stable archive prefix (`s3a://…/p10-rehearsal/archive/`) BEFORE further live runs rotate it.
- [ ] Live stack healthy; no other rehearsal/bench in flight.
- [ ] R2 endpoint reachable; S3A timeout pins + outer-deadline containment available for every checkpoint step.
- [ ] Fast smoke gate probes available on the rehearsal network (§4.1): `probe-r2.sh` + bounded-read probe + KV-count probe.

## 4. Phase 0 — prepare isolated environment

1. **Archive the checkpoint:** copy the known-good R2 checkpoint tree (chk-N + `shared/` incremental SSTs) to the archive prefix; verify with `_metadata` read + a restore probe on a throwaway MiniCluster (CHECKPOINT-DURABILITY-001 recipe).
2. **Confirm the second compose project:** `docker compose -p p10 -f docker-compose.p10.yml` (overlay built 2026-08-13, remapped ports 12181/19123/19124/18081/19249/19250); same image digests; dev secrets. Trio health: `p10-zookeeper-1`, `p10-fluss-coordinator-1`, `p10-fluss-tablet-1` Up.
3. **Copy Fluss data:** copy the live tablet data dirs + ZK metadata into the rehearsal volumes while the live writer is stopped (brief ingestion stop; restart after copy — record start/stop timestamps). Verify segment integrity (torn-tail truncation procedure if needed — `fluss-tablet-crash-loop-repair` skill pattern).
4. **Database/table setup in the rehearsal env:** create database `rehearsal`; provision `raw_table_1`, `feature_candles_15s`, `Signal_Candidates`, `Signal_Candidates_current` via the gated DDL path (identical to production: `ddl_apply.py` + version-matrix gate — no bootstrap at service startup; DdlBootstrap owns only registry tables).
5. **Load the copied history:** copy log segments into the rehearsal `raw_table_1`; lake tier attaches to the existing R2 objects (shared read-only, §2.2).
6. **Environment health gate:** coordinator/tablet healthy in the rehearsal network; O2/collector reachable (or a rehearsal-scoped metric sink); rehearsal `raw_table_1` visible with the full copied history; `Signal_Candidates` empty LOG + `Signal_Candidates_current` empty KV visible.
7. **Fast smoke gate (≤ 2 min, §4.1):** `probe-r2.sh` against the rehearsal R2 path (`lake/rehearsal/...`) PASS + 30 s bounded log read on rehearsal `raw_table_1` returning the copied count + preflight validator smoke (expected PASS on the rehearsal tables). Smoke fails → fix + re-smoke before Phase 1.

### 4.1 Long-run gate rule (user directive, 2026-08-12)

Any phase estimated > 10 min MUST be preceded by a ≤ 2-min smoke exercise of the SAME machinery that phase depends on: R2 lake read (`probe-r2.sh`) for checkpoint steps, bounded log/batch reads for scan steps, checkpoint-restore probe for state steps. Smoke passes → run the long phase; smoke fails → fix + re-smoke. No blind long waits: the P3.5 R2 saga proved a 55-90 min audit can wedge with NO error while a 1-min probe catches the same failure in seconds. The smoke result (probe log path + exit code) is recorded in the evidence file as part of the run's proof.

## 5. Phase 1 — table preflight + dual-sink from copied checkpoint (P10.1 boxes 1-4, 6-7)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + the archived-checkpoint restore probe (Phase 0 step 1) re-run immediately before this phase — the checkpoint-restore machinery is what this phase exercises.
1. Run the re-targeted preflight validator against the rehearsal tables: `feature_candles_15s` KV PK exactly `(instrument_token, window_start)` (sole candle output — 2026-08-13 conversion), `Signal_Candidates` LOG (no PK), `Signal_Candidates_current` KV PK exactly `[instrument_token]`, 22 columns/type/nullability, bucket.key `instrument_token` + 16 buckets. Negative legs: wrong-kind and wrong-schema tables fail before environment creation (SIGNAL-SCHEMA-001).
2. Submit the SignalJob (application mode, rehearsal env, PARALLELISM from P7) in RESTORE mode from the ARCHIVED checkpoint copy; `allowNonRestoredState=false` (never set — STARTUP-GATE-001 contract).
3. Verify: table preflight passes (incl. the Fluss dedup state table under DEC-038); startup mode = RESTORE (no FULL_REPLAY); source/window/detection state restored (CHECKPOINT-RESTORE-002 recipe: offsets, window state) and the dedup working cache rehydrated from the Fluss dedup table (DEC-038 — no dedup restore from checkpoint); signal LOG sink appends and `Signal_Candidates_current` sink starts cleanly (first upserts from the restored detection state).
4. Verify first checkpoint meets target (30 s interval; duration recorded; R2 pins active).

## 6. Phase 2 — bounded replay twice (P10.1 boxes 8-9)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + rehearsal env health before the replay runs.
1. Run a bounded source replay twice (same offsets): 2nd run must leave `Signal_Candidates_current` **byte-identical in key count** (== active instrument count) while the `Signal_Candidates` LOG gains the replayed append rows (retained as evidence — duplicates after deliberate replay are never silently deleted).
2. Verify LOG may grow and KV keys do not: run the dual-sink job forward; confirm signal LOG row count grows while `Signal_Candidates_current` key count stays frozen.

## 7. Phase 3 — rollback + re-cutover rehearsal (P10.1 box 10, B8.7-style full)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + restore probe against BOTH checkpoints (pre-cutover single-LOG and dual-sink era) before the rollback direction starts.
1. **Rollback direction:** stop the dual-sink job (approved operator procedure); preserve the `Signal_Candidates` LOG and `Signal_Candidates_current` KV tables; reconstruct the single-LOG artifact (signal KV sink stripped, restore wiring kept); restore from the pre-cutover checkpoint; verify signal KV key count frozen and LOG grows; checkpoints complete <= 30 s.
2. **Re-cutover direction:** stop the single-LOG job; restore the dual-sink job from the dual-sink era's own checkpoint; verify full graph restore + `Signal_Candidates_current` sink resumes.
3. **Exposure record (P10.3 box):** capture the signal LOG + KV envelope of the dual-sink window — timestamp range, LOG rows appended, KV upserts, sampled keys (the data that a production rollback would expose as duplicates).
4. No `allowNonRestoredState=true` shortcut; no automatic full replay at any point (STARTUP-GATE-001 / P10.3 contract).

## 8. Phase 4 — evidence + runbook delivery

1. Check all re-scoped P10.1 boxes in the tracker with evidence annotations (date + artifact path, register-format fields: commit, commands, topology, volume, output, pass/fail, operator/approver line).
2. Deliver `docs/08_implementation/15-signal-log-kv-production-cutover.md` (or appendix in the tracker): the P10.2 (10 boxes) + P10.3 (8 boxes) ready-runbook — exact commands, stop/RESTORE sequence, consumer-repoint step (marked for when the downstream pipeline exists), rollback triggers, exposure-record format. Execution deferred until a real production deployment exists.
3. Register rows updated where rehearsal evidence contributes (`CHECKPOINT-RESTORE-002` gains a signal dual-sink rehearsal-env variant annotation; `SIGNAL-DUAL-SINK-001`, `SIGNAL-SCHEMA-001` where exercised).

## 9. Evidence template

Per tracker §4 fields: date; commit/image IDs; exact command or test; environment topology (isolated project, ports, db name); input volume/rate (copied row counts, offsets); output location (archive checkpoint path, rehearsal O2 queries, evidence file `logs/tracker-14/p10-rehearsal-<date>.md`); pass/fail per box; operator/approver line (user review).

## 10. Pass/fail handling

Any box failing mid-rehearsal: record the failure + root cause in the evidence file, fix (env/config/artifact), re-run the affected phase. The rehearsal env is throwaway — a failed phase never touches live data (isolation is the safety net). Production stays `BLOCKED` (tracker §6).

## 11. Cross-references

- Tracker: `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` P10 (RE-SCOPED), §4 register (`SIGNAL-DUAL-SINK-001`, `SIGNAL-SCHEMA-001`), §6 acceptance.
- Requirement change + retired candle machinery: tracker 14 header banner; `13-candle-log-kv-replay-safety.md` banner (SUPERSEDED scope); tracker 14 §4 register HISTORICAL rows.
- Recipe skills: `candle-kv-rollback-rehearsal` (B8.7 rollback/re-cutover pattern), `candle-failure-injection-tests` (state-restore verification patterns), `fluss-tablet-crash-loop-repair` (segment integrity).
- P7 bench: `docs/08_implementation/11-testing-and-release.md` (topology re-scope banner; sequencing gate).
- R2 fix + containment: `§P3.5 of 14-candle-log-kv-replay-safety_2.md (plan file never persisted)`; tracker P3.5.
- Prior Phase 0 evidence: `logs/tracker-14/p10-rehearsal-20260813.md` (divergences D1–D6; overlay compose + empty rehearsal trio; data copy never performed).
- Production target (future): `docs/08_implementation/09-production-swarm.md`, `docs/05_deployment/06-swarm-secrets.md` (P9 open review).

## 12. Execution results

*(to be appended after each phase run — date, commands, raw numbers, pass/fail, bottleneck notes. The 2026-08-13 Phase 0 record is `logs/tracker-14/p10-rehearsal-20260813.md`.)*
