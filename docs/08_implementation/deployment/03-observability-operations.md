# Observability and Operations Implementation Dossier

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready; metric names/thresholds remain implementation inputs |
| Owner | Platform/Operations; component owners emit telemetry |
| Backend | OpenObserve target plus immutable local execution audit |
| Sources | `REQ-OBS-*`, `docs/06_operations/`, `docs/01_project/03-quality-targets.md` |

## Common telemetry envelope

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

## Metric design rules

- Metric labels are bounded; instrument/order IDs belong in structured logs or audit, not labels.
- Every latency defines start/end boundary, unit, clock source, workload, and percentile.
- Every counter defines retry/duplicate semantics.
- Every health metric identifies the failing dependency.
- Missing telemetry is an observable failure, not a healthy zero.

## Health dimensions

| Dimension | Meaning |
| --- | --- |
| Liveness | Process/event loop responds |
| Readiness | Mandatory dependencies and data flow available |
| Job health | Required Flink job running/checkpointing |
| Trading readiness | Gate enabled, state known, reconciliation clean |
| Durability readiness | Replication/checkpoint/offload/audit posture passes |
| Telemetry readiness | Required metrics/logs/audit delivery works or approved buffer active |

Executor may be live while trading readiness is false.

## Required telemetry by area

### Ingestion

Packet/byte rate, append acknowledgement latency, pending bytes, reconnects, connection epochs, subscription completeness, decoder/protocol/manifest versions, quarantine reasons, fingerprint counts, discontinuities, clock offset, acknowledged loss, readiness.

### Signal job

Source lag/rate, watermark lag, idleness, invalid/late/discarded events, dedup hits/state size, candles/forming updates, candidates/rankings/decisions, reservations/conflicts, operator busy/idle/backpressure, sink latency, checkpoints, restores, state compatibility.

### Action Capture and positions

Postback rate/bytes, decode failures, correlation/quarantine, stale/regressive/conflicting transitions, projection backlog/lag/retry, positions by state, incomplete writes, rebuild/recovery, readiness.

### Executor

Gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, request conflicts, duplicate suppression, mapping/quarantine, reconciliation, fencing lease, consumer lag/continuity, OpenAlgo latency/status, approvals/denials, audit append, readiness.

### Storage/platform

Fluss quorum/replicas/leaders, disk/volume, checkpoint S3, EOD manifest/retry/verification/expiry margin, retention extension, Iceberg commit/checksum, VM/node/container/job health, secret/certificate age, unauthorized controls, alert acknowledgement.

## SLO boundaries

| SLO | Boundary | Target |
| --- | --- | --- |
| Raw append | Broker packet received → Fluss append acknowledged | p99 <5 ms target; evidence-gated |
| Decision | Trigger tick consumed → immutable decision committed | p99 <100 ms at 75k/s |
| Delivery | Decision committed → Executor received | Baseline then threshold |
| Broker REST | OpenAlgo request start → verified broker response | Report separately; evidence-gated |
| Data recovery | Failure detected → processing resumed | <30 s accepted scenarios |
| Safe halt | Uncertainty detected → calls blocked | <5 s |
| EOD | Market close → verified manifest | <30 min target |

## Alert contract

Every alert config defines ID/version, condition, measurement window, threshold, severity, owner, acknowledgement target, affected scope, gate impact, evidence IDs, escalation, runbook, and closure criteria.

Critical categories:

- Unknown broker result, duplicate risk, mapping ambiguity, fencing loss, changelog gap, failed reconciliation, unsafe approval, halt latency breach.
- Job/checkpoint failure, watermark stall, sustained backpressure, append uncertainty, partial subscription, projection backlog.
- Fluss quorum/disk/S3/offload/retention/checksum/recovery failure.
- Credential/TLS/secret exposure/unauthorized control/public exposure/image policy/audit access failure.

Alert backend failure is itself alerted through an independent path where feasible and cannot authorize orders.

## Runbook template

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

Mandatory runbooks: unknown broker outcome, duplicate risk, gate halt/resume, changelog gap, checkpoint restore, Fluss node/quorum loss, projection recovery, broker disconnect, S3/offload failure, credential compromise/rotation, OpenObserve outage, one-VM loss.

## Clock and evidence

Hosts maintain UTC synchronization and expose offset. Durations use monotonic clocks. Acceptance reports include exact versions, workload, duration, sample count, p50/p95/p99, clock evidence, and whether failures/restarts are included.

## Acceptance checklist

- [ ] Every mandatory requirement has proving telemetry.
- [ ] High-cardinality labels are bounded.
- [ ] Redaction tests cover logs, traces, alerts, and support bundles.
- [ ] Health dimensions are independently queryable.
- [ ] Critical alerts deliver, acknowledge, escalate, and link runbooks.
- [ ] OpenObserve outage does not erase execution audit or authorize orders.
- [ ] Dashboard/query versions are included in release evidence.
