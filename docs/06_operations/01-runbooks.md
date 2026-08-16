# Operational Runbooks

## Runbook standard

Every critical runbook records:

- Trigger, severity, and affected scope
- Owner role and acknowledgement target
- Evidence to collect before intervention
- Whether the Executor gate must halt
- Bounded mitigation steps
- Recovery and reconciliation procedure
- Verification and closure evidence
- Escalation path

Use this template:

```text
runbook_id/title
scope and severity
preconditions and safety posture
signals and evidence queries
immediate containment / gate action
diagnostic sequence
reconciliation steps
recovery procedure
validation and closure evidence
escalation and owner
rollback / abort criteria
```

Commands and API paths are version-specific implementation details. Validate them against the deployed release before execution.

## Gate halt

### Triggers

Halt for unknown broker outcome, duplicate-order risk, missing/ambiguous identity, ~~stale instruction/reservation state~~ (**REMOVED 2026-08-15, CHG-005**), changelog discontinuity, checkpoint failure affecting order correctness, missing fill, failed reconciliation, unauthorized action, security incident, or unverifiable Executor state.

### Procedure

1. Transition the affected gate scope to `HALTED` and increment/record the gate epoch.
2. Record reason, detection timestamp, affected attempts, actor/service, and evidence reference. (**Reservations REMOVED 2026-08-15, CHG-005.**)
3. Confirm new money-moving calls stop within five seconds.
4. Preserve attempts, request hashes, mappings, responses, offsets, fills, and logs.
5. Alert the owner and begin the matching reconciliation runbook.

Existing positions may remain monitored, but no new money-moving call is permitted while halted.

## Gate reconciliation and resume

1. Reconcile broker orders and Arrow REST responses.
2. Reconcile fills, order lifecycle, and positions.
3. Verify `instruction_id` ↔ `execution_attempt_id` ↔ `client_order_ref` ↔ `broker_order_id` mappings.
4. Resolve every `UNKNOWN` attempt through evidence-backed disposition. (**Reservations REMOVED 2026-08-15, CHG-005.**)
5. Verify Executor fencing and durable state.
6. Verify Fluss changelog offsets/continuity.
7. Verify Signal job and checkpoint health.
8. Verify projection completion and quarantine disposition.
9. Produce an evidence hash bound to the current gate epoch.
10. Require two distinct authenticated authorized operators to approve that same hash/epoch.
11. Transition to `ENABLED` only after the second approval.

Automatic resume and approval reuse across epochs are prohibited.

## Unknown execution outcome

1. Mark the attempt `UNKNOWN`; do not create a replacement attempt.
2. Halt the affected gate scope.
3. Query the broker through the evidence-approved reconciliation method using the durable client/broker reference.
4. If acceptance is proven, persist the `broker_order_id` mapping and reconcile fills/positions.
5. If non-acceptance is proven, record terminal evidence before retry/release according to policy.
6. If neither is proven, require manual disposition. (**Reservation-capacity clause REMOVED 2026-08-15, CHG-005.**)
7. Record immutable execution audit and follow the resume runbook.

## Broker market-data disconnect

1. Capture connection identity/epoch, affected manifest, last verified event/receive time, subscription acknowledgements, clock offset, and reconnect attempts.
2. Mark affected Ingestion readiness false.
3. Reconnect using the approved backoff/authentication policy and revalidate all required subscriptions.
4. Create suspected-discontinuity evidence; do not fabricate exact sequence gaps.
5. Do not perform inline historical backfill in MVP.
6. Suppress affected trading decisions unless an explicitly approved degraded mode proves safe.
7. Verify append acknowledgements, bounded backlog, and telemetry before ready.

## Flink job or checkpoint failure

1. Identify the affected job, checkpoint, source offsets, watermark, state size, failure, and sink status.
2. Halt the order path if state continuity or instruction correctness is uncertain.
3. Restore the compact checkpoint from the tested encrypted S3 checkpoint/savepoint (DEC-038: the checkpoint holds source offsets, watermarks, timers, in-flight windows, and working-cache metadata — not the full dedup set).
4. Verify Fluss authoritative-state availability and compatibility (dedup state table, candle/signal tables); rehydrate the dedup working cache from Fluss.
5. Verify window, forming-bar, and source state consistency; verify the Fluss dedup table holds the accepted set (checkpoint did not duplicate it). (**Ranking state REMOVED 2026-08-15, CHG-005.**)
6. Verify no duplicate immutable instruction and no unaccounted partial sink visibility.
7. If restoration or Fluss-state verification cannot be proven, remain not ready and execute the approved reset/replay procedure.
8. Reconcile before any gate resume.

## Fluss quorum, tablet, or workload VM loss

1. Record failed VM/tablet, leader/replica state, quorum, disk, backlog, and acknowledged-loss counters.
2. Halt the order path if changelog, durable state, or mappings are uncertain.
3. Verify replica placement and remaining quorum before restart/reassignment.
4. Restore Flink from S3 checkpoints as required.
5. Measure backlog recovery and data correctness at the current workload.
6. Verify recovery under the accepted RTO and run reconciliation before resume.

## Postback correlation or projection failure

1. Preserve the immutable postback and payload hash.
2. Write or verify `Postback_Quarantine` for missing/ambiguous identity or invalid schema/status.
3. Halt affected new order/action flow.
4. Inspect durable projection pending/completion state.
5. Retry lifecycle and position projections idempotently using source event/version.
6. Reject regressive terminal-state updates; conflicting evidence becomes `UNKNOWN`.
7. Verify `Order_Lifecycle`, `Positions`, and mappings before closure. (**Reservation impact REMOVED 2026-08-15, CHG-005.**)

## EOD offload failure

1. Mark the manifest unverified/retryable and emit a critical storage alert.
2. Extend or preserve Fluss retention for affected source data.
3. Capture table/schema version, date, source range, rows/bytes, checksums, commit state, S3 errors, and expiry margin.
4. Retry with approved bounded backoff.
5. Verify the Iceberg commit and manifest before marking success.
6. Do not expire the source day; maintain at least three complete trading days live.

## Observability outage

1. Confirm whether telemetry ingestion, storage, query, or alert delivery failed.
2. Preserve local structured logs and immutable execution audit.
3. Verify order safety using durable gate/attempt state, not a green dashboard.
4. If required safety evidence or alerting is unavailable, mark trading readiness false and halt according to policy.
5. Recover telemetry, replay buffered data where supported, and verify alert delivery before closure.

## SignalJob (compute) operations

Environment: the distributed SignalJob runs on the compose Flink cluster as a
`flink run -d` job. Pinned envs are REQUIRED at submit
(`requirePinnedLong` fails the job when missing):

| Env | Pinned value | Meaning |
| --- | --- | --- |
| `DEDUP_TTL_MS` | `300000` | dedup expiry TTL |
| `CANDLE_WINDOW_MS` | `15000` | candle aggregation window |
| `CHECKPOINT_INTERVAL_MS` | `10000` | checkpoint cadence |
| `CHECKPOINT_TIMEOUT_MS` | `30000` | checkpoint timeout |
| `MAX_CONCURRENT_CHECKPOINTS` | `1` | no concurrent checkpoints |

Additional required envs: `RAW_TABLE`, `CANDLE_TABLE`, `CANDLE_CURRENT_TABLE`,
`SIGNAL_CANDIDATES_TABLE`, `FLUSS_BOOTSTRAP_SERVERS` (coordinator port `9123` —
the tablet's client port is `9124` and `9123` on a tablet is nothing),
`CHECKPOINT_DIR` (MUST include a scheme: `file:///tmp/p8-checkpoints`; a bare
path throws `StringIndexOutOfBoundsException` at `SignalJob.java:310`).
`STATE_RECOVERY_PATH`/`ALLOW_FULL_REPLAY` are runtime-only and absent from
`.env`; the fail-closed A3.3 gate governs both (no normal launch path supplies
`ALLOW_FULL_REPLAY=true`).

### Start (normal RESTORE path)

1. Build: `mvn -f code/02_services/02_compute/pom.xml -pl . package` (jar lands
   at the path the launcher expects; the distributed cluster uses
   `/opt/flink/jobs/compute.jar`).
2. Copy the jar into the jobmanager container if rebuilt.
3. Submit with the pinned env set (exact working dev command):
   `docker exec -e RAW_TABLE=… -e CANDLE_TABLE=… -e SIGNAL_CANDIDATES_TABLE=… -e SIGNAL_CURRENT_TABLE=… -e STATE_RECOVERY_PATH=… -e CHECKPOINT_DIR=file:///tmp/p8-checkpoints -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 -e DEDUP_TTL_MS=300000 -e CANDLE_WINDOW_MS=15000 -e CHECKPOINT_INTERVAL_MS=10000 -e CHECKPOINT_TIMEOUT_MS=30000 -e MAX_CONCURRENT_CHECKPOINTS=1 01_docker-flink-jobmanager-1 flink run -d -c com.trading.compute.signaljob.SignalJob /opt/flink/jobs/compute.jar`
   (This is the normal RESTORE-mode command: `STATE_RECOVERY_PATH` names the
   previous run's last checkpoint and `ALLOW_FULL_REPLAY` is absent; the log
   line `signal-job: startup mode = RESTORE (restore=…, fullReplay=false)` at
   INFO confirms restore. The FULL_REPLAY break-glass variant sets
   `ALLOW_FULL_REPLAY=true` and omits `STATE_RECOVERY_PATH`.)
4. Health: verify via `ps`, log evidence (`Restoring job`, `Completed
   checkpoint`, sink `RUNNING`), and checkpoint continuity — NOT the hub
   readiness report, which false-alarms "NOT ready" while the job is healthy.

### Stop

Graceful: cancel via the Flink UI/REST with a savepoint/checkpoint capture, or
`flink cancel -s <target> <jobid>`. Never SIGKILL the jobmanager.

### Restart without losing state

Set `STATE_RECOVERY_PATH` to the PREVIOUS RUN's last checkpoint directory
(`file:///tmp/p8-checkpoints/<previous-jobid>/chk-N`). Scope any log grep to
new log lines after restart. Verify `Restoring job` appears, then completed
checkpoints advance.

Under DEC-038 (2026-08-14) the checkpoint is compact: it restores source
offsets, watermarks, timers, in-flight windows, and dedup working-cache
metadata. After restore, verify the Fluss authoritative state (dedup state
table, candle/signal tables) is reachable and compatible, and that the dedup
working cache rehydrates from Fluss — the checkpoint is not a second complete
copy of the durable Signal business state.

**JM/TM container recreate destroys container-local `/tmp` checkpoints** (not on a
named volume; 2026-08-11 incident: a `docker cp` preserve raced live checkpointing,
captured a metadata-only checkpoint, and the job had to FULL_REPLAY). Before any
`docker compose up -d flink-jobmanager flink-taskmanager`:
1. Stop the job (`flink cancel` — a running job keeps subsuming checkpoints while
   you copy, so the preserved snapshot may be incomplete).
2. Preserve `docker cp 01_docker-flink-jobmanager-1:/tmp/p8-checkpoints/. <host>`.
3. After recreate, copy back and `docker exec -u root … chown -R flink:flink
   /tmp/p8-checkpoints` (docker cp restores as root; flink cannot create the
   checkpoint store otherwise).
4. Re-deploy the jar (`docker cp compute.jar …:/opt/flink/jobs/` — also
   container-local) and resubmit with `STATE_RECOVERY_PATH` pointing at the
   preserved `chk-N`. Restore-only submit must NOT set `ALLOW_FULL_REPLAY`
   (A3.3 gate rejects the combination).

## Replay incident (SignalJob)

Trigger: an approved FULL_REPLAY is started, or a restart is forced to replay
history (`ALLOW_FULL_REPLAY=true` accepted via A3.4).

1. Confirm the A3.4 WARN log line: `signal-job: startup mode = FULL_REPLAY
   (restore=false, fullReplay=true)` — a full replay without this line is a
   configuration error.
2. Record JobID, artifact hash, checkpoint directory, and the replay reason
   (approved operator step).
3. The storm test (2026-08-11, job `5f41f0c5`, 204,800 ticks) proves a replay
   produces zero alert storms: checkpoints complete (0 failed), and the
   schema-rejection rules do not fire on valid historical data.
4. Verify the replayed candle/sink counts match the deterministic expectation
   for the input snapshot; candidates = 0 on replay is deterministic (no
   detection during historical catch-up).
5. Closure: replay drained, checkpoints advancing, no unintended alert fires.

## Checkpoint failure (SignalJob)

Trigger: `SIGNAL-crit-checkpoint-failed` (failed checkpoints > 0) or
`SIGNAL-error-checkpoint-slow` (duration >= 24000 ms = 80% of the pinned
30000 ms timeout).

1. Capture JobID, failed checkpoint count, last completed checkpoint, source
   offsets, state size, and sink status.
2. Verify the job is still producing completed checkpoints; one transient
   failure restarts the task automatically (configured restart strategy).
3. If checkpoints stop completing: restore from the last good checkpoint
   (`STATE_RECOVERY_PATH` = `file:///tmp/p8-checkpoints/<jobid>/chk-N`), never
   from a checkpoint at/after the failure.
4. `allowNonRestoredState` is forbidden — the restore must be exact.
5. Verify dedup, window, forming-bar, and source state consistency, then resume
   normal operation. Closure: checkpoints advancing, 0 failed, alert recovered.

## Schema-preflight failure

Trigger: DDL/version preflight blocks startup (validation/contract gate).

1. Capture the failing table, expected vs actual schema, and version matrix
   state.
2. Check the table contract fields: PK, routing, bucket count, and the full column/type/nullability set (`TableContractValidator`, re-targeted by the 2026-08-13 re-scope to `feature_candles_15s` KV PK exactly `(instrument_token, window_start)` + `Signal_Candidates` LOG + `Signal_Candidates_current` KV PK `[instrument_token]` — SIGNAL-SCHEMA-001, implemented).
3. Do NOT bypass the gate; reconcile the DDL/schema with the manifest
   (`ddl_apply.py --force` regeneration must be byte-identical) and re-run.
4. Closure: preflight passes, job starts in the intended mode.

## Migration conflict (CandleMigrationTool) — RETIRED

> **RETIRED (2026-08-13):** `CandleMigrationTool` is decommissioned with the candle KV projection (requirement change — `feature_candles_15s` is now the KV-only sole candle output). The procedure below is retained as the historical record of the implemented candle-KV migration conflict handling.

Trigger: `CandleMigrationTool` audit/load exits 2 (unaccepted conflicts) or 1
(accept-list entries match no canonical key).

1. Read the stdout fields: `ACCEPT_KEYS_FILE`, `ACCEPTED_KEYS`,
   `UNACCEPTED_KEYS`, `ACCEPT_KEYS_NOT_FOUND`.
2. Re-run the read-only audit; every conflict must be on the operator-approved
   accept list (`CANDLE_MIGRATION_ACCEPT_KEYS_FILE`: `token,windowStart` per
   line, `#` comments allowed) or the load fails closed.
3. Accepted keys merge by `MAX(output_ts)` (last-write-wins — the same
   convergence the live KV sink applies); unknown conflicts abort.
4. Entries matching no canonical key abort exit 1 (typo/stale list).
5. Closure: `UNACCEPTED_KEYS=0`, `NOT_FOUND=0`, exit 0, DEST_ROWS_AFTER ==
   DISTINCT_KEYS.

## Fluss coordinator/tablet failure (compose)

Trigger: `SIGNAL-error-flink-jm-scrape-down` / `SIGNAL-error-flink-tm-scrape-down`
(collector cannot scrape), or `up == 0` for a Fluss service, or
`SIGNAL-crit-taskmanager-down`.

1. Record the failed service, leader/replica state, and quorum.
2. Compose: `docker compose up -d --force-recreate <svc>` (coordinator/tablet);
   Fluss data persists in named volumes.
3. Verify the coordinator (`fluss-coordinator:9123`) and each tablet are
   reachable from the jobmanager container before restarting the job.
4. If the SignalJob lost its Fluss connections, restart it from its last
   checkpoint (see SignalJob restart) — never from offset 0 without the
   approved replay path.
5. Closure: scrapes return to 1, checkpoints advance, alerts recover.

## Rollback (candle LOG→KV replay, CANDLE-KV-REPLAY-001) — RETIRED

> **RETIRED (2026-08-13):** the candle dual-sink this runbook rolls back is retired (`feature_candles_15s` is now the KV-only sole candle output). The registry and chk-1539 cutoff below are historical dev-rehearsal evidence. The re-scope target is the SIGNAL dual-sink rollback runbook (`Signal_Candidates` LOG + `Signal_Candidates_current` KV), delivered by tracker 14 P10 (`docs/08_implementation/09-production-swarm.md`).

Exact dev-rehearsed registry (2026-08-10, tracker
`docs/08_implementation/13-candle-log-kv-replay-safety.md`):

| Job | Graph | Restore target | Last checkpoint |
| --- | --- | --- | --- |
| `0417068d` | pre-cutover single-LOG | — | chk-1538 (rollback restore target) |
| `87c48642` | dual-sink cutover | chk-1538 | chk-1732 (re-cutover restore target) |
| `4527918b` | rollback rehearsal (single-LOG, KV sink stripped, restore wiring + candle-contract preflight kept) | chk-1538 | chk-1572 |
| `92104dac` | current re-cutover | chk-1732 | chk-1827+ |

Tables: `feature_candles_15s` (LOG, never touched by rollback) and
`feature_candles_15s_current` (KV — writes freeze on rollback).
**Rollback cutoff = chk-1539** (the FIRST dual-sink checkpoint): restoring
any checkpoint at/after it into a single-LOG graph leaks KV state.

Rollback procedure:

1. Build the single-LOG artifact from CURRENT source with the KV-sink operator
   stripped (restore wiring + candle-contract preflight kept). Verify via
   `javap`: 0 kv-sink refs, restore + preflight present. The old pre-restore
   jars lack `STATE_RECOVERY_PATH` support and would offset-0 full-replay.
2. Submit with `STATE_RECOVERY_PATH=<pre-cutover job's last checkpoint>` =
   `file:///tmp/p8-checkpoints/0417068d…/chk-1538` (dev) — NEVER a checkpoint
   >= chk-1539.
3. Verify: KV frozen (key count constant), LOG continues to grow, checkpoints
   <= 30 s, `Restoring job` + completed checkpoints in the log.
4. Re-cutover (when safe): restore the dual-sink job from ITS OWN last
   checkpoint (`87c48642…/chk-1732`), not the rehearsal run's checkpoints
   (single-LOG graph → dual-sink restore fails).
5. Closure: KV count resumes growing, LOG/KV diverge only by design, alerts
   clear.

## Telemetry retention and data lifecycle

OpenObserve v0.91.5 retention contract (`docs/04_contracts/openobserve.md`):
logs 30 days, metrics 90 days, traces 14 days, alert definitions 180 days.

- Mechanism (source-verified, `src/service/compact/retention.rs`): per-stream
  `data_retention` (days) overrides the global
  `ZO_COMPACT_DATA_RETENTION_DAYS` default (3650 = 10 years). New metric
  streams inherit the global until the provisioning sync re-runs.
- Applied 2026-08-11 via `o2-provision.py` (idempotent `provision_retention`):
  all logs streams = 30, all 335 metric streams = 90. Re-run the provisioner
  after any new metric family appears.
- Alert rules are NOT streams: definitions + trigger history live in the O2
  meta store (`metadata.sqlite`), not subject to stream retention.
- The seven-year money-moving audit retention is a SEPARATE S3/object-store
  control (EOD/lake tier) and must NEVER be attributed to OpenObserve.
- When retention changes are needed: `PUT /api/{org}/streams/{stream}/settings`
  with `{"data_retention": N}` (partial update; other settings survive);
  minimum allowed is 3 days.

## Alert response catalogue

All rules route to the `dev-webhook` destination in dev (receiver logs:
`docker logs 01_docker-webhook-receiver-1 | grep 'POST /noop'`). O2 v0.91.5 v2
alerts have no first-class severity field — severity rides the rule-name prefix
(`SIGNAL-crit`/`error`/`warn`, `ING-crit`/`warn`). Realtime rules evaluate
continuously; while a condition holds, fires repeat on the ~30–75 s window
cadence. 26 rules provisioned (9 ING- ingestion + 17 SIGNAL- compute).

Compute/SignalJob rules:

| Rule | Severity | Condition | Response | Recovery |
| --- | --- | --- | --- | --- |
| SIGNAL-crit-checkpoint-failed | Critical | failed checkpoints > 0 | Checkpoint failure runbook | checkpoint completes, count resets |
| SIGNAL-error-checkpoint-slow | Error | duration >= 24000 ms | Checkpoint failure runbook | duration back under 80% of timeout |
| SIGNAL-error-job-restarting | Error | restarts > 0 | Check flink_logs for the restart cause | restarts stop |
| SIGNAL-error-source-stalled | Error | source rate == 0 (2 min) | Check feed/bridge; **false-fires on quiesced dev feed** | feed resumes |
| SIGNAL-warn-kv-sink-zero | Warning | kv-sink rate == 0 (2 min) | Check KV sink task; **false-fires on quiesced dev feed** | sink writes resume |
| SIGNAL-warn-dedup-state | Warning | Fluss dedup-table entry count > envelope (first-seen rate × TTL horizon) | Check first-seen rate and cleanup pace — DEC-038: the authoritative dedup set lives in `fingerprint_dedup` (DDL `24_fingerprint_dedup.sql` on file, applied with the DEC-038 implementation stage), not Flink state | table size plateaus back under envelope |
| SIGNAL-warn-dedup-expiry | Warning | expired-row cleanup backlog > bound | Check the dedup cleanup pass (`DEDUP_CLEANUP_INTERVAL_MS`) and delete throughput | backlog drains |
| SIGNAL-warn-dedup-cache-hit | Warning | dedup cache hit ratio below threshold (60 s) | Hot path degrading toward per-tick Fluss lookups — check cache bound vs envelope (DEC-038) | hit ratio recovers |
| SIGNAL-warn-schema-rejected-rate | Warning | rejects per flush > 10 | Check raw_table_1 schema vs validator | rejects stop |
| SIGNAL-crit-schema-version-rejected | Critical | any schema-version reject | Schema-preflight runbook | zero rejects |
| SIGNAL-crit-full-replay-started | Critical | startup mode == FULL_REPLAY | Replay incident runbook (fires only when the emitter ships in production) | replay drains, job runs |
| SIGNAL-error-flink-jm-scrape-down | Error | `up{instance=flink-jobmanager:9249} == 0` (2 min) | Check JM + collector scrape | scrape resumes |
| SIGNAL-error-flink-tm-scrape-down | Error | `up{instance=flink-taskmanager:9249} == 0` (2 min) | Check TM + collector scrape | scrape resumes |
| SIGNAL-warn-jvm-heap-high | Warning | JM heap >= 900 MB | GC/restart; check task slots | heap drops |
| SIGNAL-crit-taskmanager-down | Critical | registered TMs < 1 | Fluss/Flink cluster runbook | TM registers |
| SIGNAL-warn-scrape-slow | Warning | scrape duration >= 1 s | Reporter load check | duration drops |
| SIGNAL-warn-source-lag | Warning | event-time lag >= 600 s | Replay incident / source-stall check; **fires on stopped dev feed** (baseline ~244 s with the historical-timestamp dev feed) | feed resumes / replay drains |

Ingestion rules (existing): ING-crit-telemetry-delivery-failed (collector
export failures persist; degraded-delivery signal — absence-based conditions do
NOT fire, see the collector-outage limitation), ING-crit-fd-90,
ING-warn-fd-80, ING-crit-orphan-process, ING-warn-reconnect-streak-5,
ING-warn-capacity-80, and the slot/health rules defined in
`02-ingestion-alerting.md`.

Collector/O2 outage limitation (measured 2026-08-11): stopping the collector
2.5 minutes produces ZERO alert fires — O2 cannot alert on data it is not
receiving. Coverage for full outage = compose/supervisor restart policy +
`ING-crit-telemetry-delivery-failed` (degraded delivery while the collector is
up).

## Security or credential incident

1. Halt affected order flow.
2. Preserve access, gate, network, and execution evidence.
3. Revoke/rotate compromised credentials using `../05_deployment/04-secrets-rotation.md`.
4. Validate least privilege, transport, redaction, and unauthorized-control attempts.
5. Reconcile and require two-person resume.

## References

- Operational requirements: `../02_requirements/06-operational.md`
- Executor contract: `../04_contracts/07-executor.md`
- Rollback and recovery: `../05_deployment/03-rollback.md`
- Alert definitions and thresholds: `../08_implementation/10-observability.md`
