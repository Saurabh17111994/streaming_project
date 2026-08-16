# 02.8 — Observability

## Purpose

Observability proves data correctness, order safety, recovery, storage durability, security, and release gates. OpenObserve is the target backend, but correlation IDs and local durable audit remain mandatory if backend delivery is temporarily unavailable.

## Constraints

- Every SHALL-level metric, alert, and log field SHALL be emitted as structured JSON with schema version. Prose-only observability descriptions are not sufficient for production acceptance.
- Secrets, tokens, original payload bytes, and unnecessary account identifiers SHALL be redacted from logs and metrics. Secret exposure in observability is a security incident.
- OpenObserve outage SHALL NOT authorize orders, erase durable execution audit, or suppress safety-halt alerts. Executor SHALL halt new money-moving calls when mandatory execution audit or safety-control acknowledgement is unavailable.
- `HALTED` may be process-healthy but trading-not-ready. Dashboards and health checks SHALL distinguish process liveness, component readiness, job health, and trading readiness as separate dimensions.
- Alert thresholds SHALL be versioned configuration, not prose-only placeholders. Every alert carries condition, detection time, affected scope, gate impact, evidence link, severity, and acknowledgement state.
- High-cardinality labels (per-instrument, per-order) SHALL use bounded aggregation, sampling, or structured logs with retention policy. Uncontrolled label cardinality is a production defect.
- Only explicitly required operator/UI/API endpoints are exposed. Internal RPC, checkpoint, and service ports are not publicly exposed.
- S3 checkpoints, Fluss data volumes, and audit storage SHALL be encrypted at rest. Local named volumes are not production durability.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-OBS-001 | OpenObserve supports the required structured JSON ingestion, metric aggregation, alert rule evaluation, and role-based access control at the MVP scale. | REQ-OBS-001, REQ-OBS-003 |
| ASM-OBS-002 | Flink and Fluss emit sufficient metrics (checkpoint duration/size, watermark lag, consumer lag, replica health, disk pressure) through their versioned metric reporters. | REQ-OBS-002 |
| ASM-OBS-003 | S3 `ap-south-1` can complete verified EOD offload within 30 minutes, and observability can detect and alert on offload failure before retention expiry. | ASM-006 |
| ASM-OBS-004 | Docker Swarm secrets, encrypted overlay/TLS, S3 checkpoints, and three-node Fluss placement operate within the four-VM target. | ASM-009 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **OpenObserve is best-effort, not critical-path for order safety:** Executor halts independently when audit or safety-control delivery is unavailable. Observability backend failure does not silently authorize orders.
- **Distributed tracing is not assumed for Flink/Fluss:** Trace propagation is used where supported but not required for MVP. Correlation IDs and audit IDs are mandatory regardless.
- **High-cardinality metrics are bounded:** Per-instrument and per-order details use aggregation, sampling, or structured logs rather than unbounded metric label sets.
- **Health is multidimensional:** A single green/gray/red indicator cannot represent the platform. Dashboards SHALL separate liveness, readiness, job health, and trading readiness.
- **Local audit survives backend outage:** Local durable execution audit and evidence capture continue during OpenObserve unavailability. Backend recovery does not retroactively create missing audit records.

## Out of Scope

The following capabilities are explicitly NOT owned by Observability:

- **Candle computation, signal detection, strategy evaluation:** Owned by the Signal Flink job. **(Ranking REMOVED 2026-08-15, CHG-005.)**
- **Broker order submission, execution, and gate management:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection:** Owned by Action Capture.
- **Market data ingestion and broker connection management:** Owned by Ingestion.
- **EOD controller orchestration and manifest creation:** Owned by the EOD controller.
- **Schema lifecycle management and DDL application:** Owned by the schema lifecycle process.
- **Business analytics (P&L, win rate, trader dashboards):** Deferred; not in MVP scope.
- **End-user trading alerts and notifications:** Deferred; not in MVP scope (operational alerts remain in scope).

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
- candle/forming-bar/candidate rates **(ranking/instruction rates REMOVED 2026-08-15, CHG-005)**
- operator busy/idle and backpressure
- checkpoint duration/size/failure/restart/restore

### Order safety

- gate state/epoch and halt latency
- attempt counts by phase/outcome
- unknown outcomes (and, pre-2026-08-15, unresolved reservations — REMOVED CHG-005)
- duplicate suppression
- identity mappings and postback quarantines
- reconciliation duration/result
- two-person approvals/denials/unauthorized attempts
- changelog lag/continuity
- Arrow REST request latency/status and broker response classification

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

## REQ-OBS-008: Component-specific observability degradation

OpenObserve outage SHALL not erase durable execution audit or authorize orders. Ingestion and Action Capture MAY continue bounded evidence capture when durable source/audit writes, local buffering, and readiness policy remain healthy. Executor SHALL halt new money-moving calls when mandatory execution audit, safety-control acknowledgement, or alert visibility is unavailable. Each component SHALL expose its degraded reason and buffer bounds.
