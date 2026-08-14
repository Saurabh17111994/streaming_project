# Observability

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready; metric names and data-derived capacity thresholds remain implementation inputs |
| Owner | Platform/Operations; component owners emit telemetry |
| Backend | OpenObserve target plus immutable local execution audit |
| Sources | `REQ-OBS-*`, `docs/01_project/03-quality-targets.md`, `docs/04_contracts/openobserve.md` |

### Common telemetry envelope

Every log/event/trace record contains where applicable:

```text
service_name
service_version
instance_id
environment
schema_version
protocol_version
configuration_hash
correlation_id
audit_id
event_time_utc
receive_time_utc
processing_time_utc
monotonic_duration
clock_offset_ms
severity
failure_class
bounded_scope
```

Do not include credentials, tokens, raw packets, or unnecessary account/person identifiers.

### Metric design rules

- Metric labels are bounded; instrument/order IDs belong in structured logs or audit, not labels.
- Every latency defines start/end boundary, unit, clock source, workload, and percentile.
- Every counter defines retry/duplicate semantics.
- Every health metric identifies the failing dependency.
- Missing telemetry is an observable failure, not a healthy zero.

### Health dimensions

| Dimension | Meaning |
| --- | --- |
| Liveness | Process/event loop responds |
| Readiness | Mandatory dependencies and data flow available |
| Job health | Required Flink job running/checkpointing |
| Trading readiness | Gate enabled, state known, reconciliation clean |
| Durability readiness | Replication/checkpoint/offload/audit posture passes |
| Telemetry readiness | Required metrics/logs/audit delivery works or approved buffer active |

Executor may be live while trading readiness is false.

### Required telemetry by area

#### Ingestion

Packet/byte rate, append acknowledgement latency, pending bytes, reconnects, connection epochs, subscription completeness, decoder/protocol/manifest versions, quarantine reasons, fingerprint counts, discontinuities, clock offset, acknowledged loss, readiness.

#### Signal job

Source lag/rate, watermark lag, idleness, invalid/late/discarded events, dedup hits/state size, candles/forming updates, candidates/rankings/decisions, reservations/conflicts, operator busy/idle/backpressure, sink latency, checkpoints, restores, state compatibility.

**DEC-038 state-ownership telemetry (2026-08-14):** prove the Fluss-owns-large-state model is behaving as intended — Flink checkpoint size/duration/failure (existing Flink built-ins); **Fluss dedup-table state size** (entry count + bytes) and **dedup update rate**; **dedup working-cache hit ratio** and cache size; **rehydration latency** and **rehydration failures**; **state compatibility failures** (dedup-table preflight) and **state continuity failures**. Bounded cardinality: per-table gauges and per-reason counters only, never per-key labels.

#### Action Capture and positions

Postback rate/bytes, decode failures, correlation/quarantine, stale/regressive/conflicting transitions, projection backlog/lag/retry, positions by state, incomplete writes, rebuild/recovery, readiness.

#### Executor

Gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, request conflicts, duplicate suppression, mapping/quarantine, reconciliation, fencing lease, consumer lag/continuity, Arrow REST latency/status, approvals/denials, audit append, readiness.

#### Storage/platform

Fluss quorum/replicas/leaders, ZooKeeper ensemble quorum/leader/latency, disk/volume, checkpoint S3, Flink HA leader/standby state, EOD manifest/retry/verification/expiry margin, retention extension, Iceberg commit/checksum, VM/node/container/job health, secret/certificate age, unauthorized controls, alert acknowledgement.

### Dashboards

OpenObserve is the target backend. A dashboard is not release evidence unless its record identifies the measurement boundary, workload, duration, UTC clock source/offset, exact software versions, sample count, and whether failures or restarts are included.

#### Data and ingestion dashboard

Show packet/tick and byte throughput; append acknowledgements and p50/p95/p99 write latency; active connections, subscription completeness, reconnects, and connection epochs; decoder/protocol/manifest versions; decode/quarantine counts; fingerprint candidates, dedup hits, and dedup-state size; pending bytes, blocked append duration, timeouts, acknowledged-loss count; clock offset/readiness; and suspected discontinuities.

#### Compute and decision dashboard

Show source throughput/lag; watermarks and allowed lateness; invalid, late, and discarded-after-emission events; candle/forming-bar rates; candidate/ranking/reservation/instruction rates; score validation and selection/rejection/churn reasons; trigger-tick-to-instruction p50/p95/p99; operator busy/idle/backpressure; and checkpoint duration, size, failure, restore, and state recovery. Report window waiting separately from processing latency. **DEC-038 additions:** Fluss dedup-table state size + update rate, dedup cache hit ratio, and rehydration latency/failures (proof the large state is in Fluss and the checkpoint is small).

#### Order safety dashboard

Show gate state/epoch; halt detection-to-block latency; attempts by phase/outcome; request hashes and unknown outcomes; unresolved reservations and duplicate suppression; identity mappings and postback quarantines; reconciliation results; changelog continuity; Executor fencing; Arrow REST latency/status and broker response classification; and two-person approvals, denials, mismatches, and unauthorized attempts. Trading readiness must never be calculated from process liveness alone.

#### Storage, EOD, and durability dashboard

Show Fluss replica/quorum/leader health; ZooKeeper ensemble quorum/leader health and latency; Flink HA leader/standby state; disk and volume pressure; checkpoint S3 availability and restore status; projection backlog/freshness; EOD rows/bytes, manifest status, retries, verification age, and expiry margin; live retention days/extension state; Iceberg commit/checksum verification; and one-workload-VM recovery state/backlog.

#### Security and platform dashboard

Show credential age/expiry/rotation/revocation; authentication/token-refresh failures; TLS/certificate status; secret scanning/redaction failures; unauthorized gate/control attempts; audit access and support-bundle generation; container, VM, Flink job, Swarm, and OpenObserve health; and alert acknowledgement/escalation state.

#### `Safe to Trade` operator dashboard

The primary operator dashboard is named **`Safe to Trade`**. It contains broker connection and subscription status; per-instrument freshness (`FRESH`, `STALE`, `UNKNOWN`, `MARKET_CLOSED`); current tick rate against the 50,000 ticks/s average baseline (the 90,000 ticks/s peak is retired, DEC-036; 3,000-instrument production targets; the current testing phase runs the 1,024-instrument / 20,480 ticks/s envelope); decision and Fluss append percentiles; Flink checkpoint/restart state; per-VM CPU/memory/SSD/network; Executor gate state/epoch; unknown-attempt count/age; and active scoped halts.

`GREEN` means nominal, `YELLOW` means attention is needed without blocking new orders, and `RED` means orders are blocked or safety is violated. These colours are summaries only; the Executor gate is the authority for order placement.

### Scale-up signals

After `PERF-PROD-60000-001` establishes the baseline, define the numeric review thresholds for sustained CPU, heap/non-heap memory, free SSD space, disk I/O, network use, checkpoint duration/size, Fluss append/fetch latency, critical-consumer lag, and decision p99 trending toward the 100 ms SLO. Every alert names a bounded affected scope: `global`, `account`, `portfolio`, `execution_partition`, or `instrument`. Fluss is a data platform, not the operator dashboard.

### SLO boundaries

| SLO | Boundary | Target |
| --- | --- | --- |
| Raw append | Broker packet received → Fluss append acknowledged | p99 <50 ms target (≤ 20 ms transport linger); evidence-gated |
| Decision | Trigger tick consumed → immutable decision committed | p99 <100 ms at variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) |
| Delivery | Decision committed → Executor received | Baseline then threshold |
| Broker REST | Arrow REST request start → verified broker response | Report separately; evidence-gated |
| Data recovery | Failure detected → processing resumed | <30 s accepted scenarios |
| Safe halt | Uncertainty detected → calls blocked | <5 s |
| EOD | Market close → verified manifest | <30 min target |

Acceptance uses a full session at the variable 50,000 ticks/s average baseline, with every instrument capped at 30 ticks/s. (The 90,000 ticks/s peak is retired, DEC-036.)

### Alert contract

Every alert config defines ID/version, condition, measurement window, threshold, severity, owner, acknowledgement target, affected scope, gate impact, evidence IDs, escalation, runbook, and closure criteria.

Critical categories:

- Unknown broker result, duplicate risk, mapping ambiguity, fencing loss, changelog gap, failed reconciliation, unsafe approval, halt latency breach.
- Job/checkpoint failure, watermark stall, sustained backpressure, append uncertainty, partial subscription, projection backlog.
- Fluss quorum/disk/S3/offload/retention/checksum/recovery failure.
- Credential/TLS/secret exposure/unauthorized control/public exposure/image policy/audit access failure.

#### Alert catalogue

Order-safety alerts cover unknown broker outcomes; duplicate-order risk or request-hash conflict; missing/ambiguous mapping; active-order postback quarantine; reconciliation failure; changelog discontinuity; Executor fencing loss; failed or unauthorized approval; safe-halt latency breach; unverifiable Executor state; and security incidents. Their response is to halt the affected gate, preserve evidence, notify the owners, and begin the linked runbook.

Streaming-health alerts cover Signal/Babysitter job failure; checkpoint failure, timeout, corruption, or restore failure; watermark stall/lag; sustained backpressure or memory breach; append uncertainty or acknowledged loss; broker disconnect/authentication exhaustion/partial subscription/protocol mismatch; projection backlog/freshness breach; and recovery-target breach. The affected path is not ready while correctness is uncertain.

Storage and durability alerts cover Fluss replica/quorum or leader failure; disk/volume/object-store pressure; S3 checkpoint loss; EOD manifest or verification failure; retry exhaustion; insufficient expiry margin; source retention risk; failed checksum/count/range validation; and one-workload-VM recovery failure. Failed offload extends retention; no source day expires before its manifest is verified.

Security alerts cover credential expiry/revocation/authentication exhaustion; TLS/certificate failure; secret exposure or redaction failure; unauthorized control operation; compromised identity; public exposure; image/SBOM/vulnerability policy failure; and audit-access anomalies. Affected money-moving paths halt until access, evidence, and reconciliation are verified.

#### Alert thresholds (per [Foundation task 7](./01-foundation.md))

Every threshold below uses a **60-second consecutive breach window** before escalating:

| Alert | Condition | Severity | Gate impact |
| --- | --- | --- | --- |
| Pending limits warning | Pending records or bytes ≥ 80% of configured limit for 60s | Warning | Readiness false |
| Pending limits critical | Pending records or bytes ≥ 100% of configured limit for 60s | Critical | Stop broker reads; acknowledge loss |
| Container memory critical | Total container memory ≥ 85% for 60 consecutive seconds | Critical | Safe halt |
| Append latency critical | Raw append p99 > 50 ms for 60 consecutive seconds | Critical | Readiness degraded |
| Decision latency critical | Trigger-tick-to-decision p99 > 100 ms for 60 consecutive seconds | Critical | Suppress new decisions |
| Checkpoint duration critical | Checkpoint p99 > 5 s for 60 consecutive seconds | Critical | Suppress new decisions |
| Checkpoint failure | Any checkpoint fails | Critical | Safety halt request |
| Source backlog critical | Source backlog > 100 ticks OR > 5 s for 60 consecutive seconds | Critical | Safety halt request |

On any critical checkpoint failure, state-continuity failure, or sustained source backlog, publish one idempotent `Safety_Halt_Request` with exact reason code and suppress new decision publication. Do not resume automatically — resume only through the Executor reconciliation and approval process.

Alert backend failure is itself alerted through an independent path where feasible and cannot authorize orders.

Each alert record contains its ID, schema/configuration version, UTC detection time, service/instance, bounded scope, condition/measured value, gate impact/current epoch, linked evidence IDs, severity, owner, acknowledgement, escalation, and closure state. It must not contain credentials, tokens, raw packets, or unnecessary account identifiers.

### Clock and evidence

Hosts maintain UTC synchronization and expose offset. Durations use monotonic clocks. Acceptance reports include exact versions, workload, duration, sample count, p50/p95/p99, clock evidence, and whether failures/restarts are included.

### OpenObserve resource budget

At the 50,000 ticks/s baseline on the 4-VM Swarm topology (Observability VM, 48 GB), allocate:

| Resource | Recommendation | Notes |
| --- | --- | --- |
| OpenObserve process RAM | **8-12 GB** | Adequate for declared retention (logs 30d, metrics 90d, traces 14d) at this throughput |
| Remaining for OTel Collector + OS | **~36 GB** | OTel Collector buffers telemetry during backend outages; OS needs disk cache |
| Memory cap | Set `ZO_MEMORY_LIMIT` or equivalent | Prevent runaway allocation starving the OTel Collector |
| Alert threshold | >14 GB for 60 seconds | OpenObserve exceeding its budget |

Without a RAM cap, OpenObserve's ClickHouse-like storage can consume all available memory and starve the OTel Collector, which is the sole buffer for telemetry during backend outages. Superseded by `PERF-PROD-60000-001`.

### Acceptance checklist

- [ ] Every mandatory requirement has proving telemetry.
- [ ] High-cardinality labels are bounded.
- [ ] Redaction tests cover logs, traces, alerts, and support bundles.
- [ ] Health dimensions are independently queryable.
- [ ] Critical alerts deliver, acknowledge, escalate, and link runbooks.
- [ ] OpenObserve outage does not erase execution audit or authorize orders.
- [ ] Component-specific degradation behavior is implemented: Ingestion/Action Capture continue bounded capture; Executor halts when mandatory audit/alert unavailable.
- [ ] Dashboard/query versions are included in release evidence.

## Verification mapping

The required behavior above is verified by the canonical [Observability and operations test design](./11-testing-and-release.md#observability-and-operations): `OPS-UNIT-001`, `OPS-UNIT-002`, `OPS-INT-001`, `OPS-INT-002`, `OPS-FAIL-001`, `OPS-FAIL-002`, `OPS-RUNBOOK-001`, and `OPS-REL-001`.

The test suite also proves EOD-expiry alerts, unauthorized-approval alerts, checkpoint/replication alerts, backend-outage buffering, and reconstruction of every live-money acceptance gate.
