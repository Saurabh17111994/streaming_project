# 02.8 — Observability

## Purpose

Observability proves data correctness, order safety, recovery, storage durability, security, and release gates. OpenObserve is the target backend, but correlation IDs and local durable audit remain mandatory if backend delivery is temporarily unavailable.

## REQ-OBS-001: Platform signals

All components SHALL emit structured JSON logs, metrics, health state, and correlation IDs. Distributed tracing is used where supported; it is not assumed that Flink or Fluss propagates trace headers without version evidence.

Required common fields: `timestamp` UTC RFC3339, `service`, `instance_id`, `level`, `schema_version`, and relevant `trace_id`/`correlation_id`/domain IDs. Secrets, tokens, original payload bytes, and unnecessary account identifiers are redacted.

## REQ-OBS-002: Metrics

### Data and compute

- packet/tick and byte throughput
- append acknowledgement and source/sink latency p50/p95/p99
- fingerprint candidates/dedup hits and state size
- invalid/quarantined/late events by reason
- watermark and source lag
- candle/forming-bar/candidate/ranking/instruction rates
- operator busy/idle and backpressure
- checkpoint duration/size/failure/restart/restore

### Order safety

- gate state/epoch and halt latency
- attempt counts by phase/outcome
- unknown outcomes and unresolved reservations
- duplicate suppression
- identity mappings and postback quarantines
- reconciliation duration/result
- two-person approvals/denials/unauthorized attempts
- changelog lag/continuity
- OpenAlgo request latency/status and broker response classification

### Storage and operations

- Fluss replica health/quorum/leader changes
- disk/volume/object-store pressure
- projection backlog and state freshness
- EOD offload bytes/rows, manifest status, retries, verification age, expiry margin
- secret/token age and rotation status
- node/VM/container/job health

MVP may not require unbounded per-instrument/per-order metric labels. High-cardinality details use bounded aggregation, sampling, or structured logs with retention policy.

## REQ-OBS-003: Alerts

The following alert groups SHALL be configured and tested in MVP:

| Group            | Critical conditions                                                                                                                  |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Order safety     | Gate halt, unknown outcome, duplicate risk, identity quarantine, reconciliation failure, unauthorized resume                         |
| Streaming health | Job failure, checkpoint failure, watermark stall, changelog discontinuity, sustained backlog/backpressure, data-path recovery breach |
| Storage safety   | Replica/quorum loss, disk pressure, EOD failure, manifest verification failure, expiry margin breach, checkpoint-store failure       |
| Security         | Credential expiry/auth exhaustion, TLS failure, secret exposure/redaction failure, unauthorized control operation                    |

Alert records include condition, detection time, affected scope, gate impact, evidence link, severity, and acknowledgement state. Alert thresholds are versioned configuration, not prose-only placeholders.

## REQ-OBS-004: Health dimensions

Health is multidimensional:

- **Liveness:** process/event loop responsive.
- **Readiness:** required dependencies and data flow are available.
- **Job health:** required Flink job is running and checkpointing.
- **Trading readiness:** Executor gate is `ENABLED`, state is known, continuity/reconciliation are clean.
- **Durability readiness:** replicas, checkpoints, offload, and retention safety pass.

A process may be alive while not ready or while trading is halted. Dashboards SHALL not collapse these states into one green indicator.

## REQ-OBS-005: SLO measurement

Each SLO metric reports p50/p95/p99, unit, boundary events, UTC clock source/offset, workload, duration, software versions, restart/failure inclusion, and sample count. Decision latency excludes broker REST latency and reports candle-window waiting separately.

## REQ-OBS-006: Audit and access

Money-moving logs, gate transitions, attempts, mappings, postback evidence, reconciliation, and approvals are immutable and retained seven years in encrypted lake storage. Audit queries are role-restricted and access is itself logged.

## REQ-OBS-007: Acceptance

Tests SHALL prove metric emission, redaction, cardinality limits, alert delivery/acknowledgement, backend outage buffering, health-state transitions, safe-halt alerts, offload expiry alerts, unauthorized control alerts, and reconstruction of each live-money acceptance gate from observability/audit evidence.

# 
