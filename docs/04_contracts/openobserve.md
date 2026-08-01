# OpenObserve Integration Contract

> Source of truth for the platform observability backend. Derived from the
> OpenObserve Integration Specification (self-hosted, provide-only-OpenObserve
> rule). In authority order this is a *contract* layer; executable code and
> tests outrank it on conflict.

## A. Deployment

- **Type:** self-hosted.
- **Image (pin):** `public.ecr.aws/zinclabs/openobserve:v0.91.5-amd64`.
- **Topology:** OpenObserve + OTel Collector + (optional) Prometheus/Alertmanager on VM4. Flink/Fluss run on VM1–VM3.

## C. Ingestion protocol

- **Primary:** OpenTelemetry (**OTLP**) over gRPC, signals = logs + metrics + traces.
- **Fallback (only for non-OTLP components):** HTTP JSON ingest (`openobserve:5080/api/<org>/logs/_json`). The Java `common` services emit OTLP-shaped records via the collector; they do not call HTTP directly.

## D. Endpoints + auth

- **Collector (emit target):** `http://otel-collector:4317` (OTLP gRPC).
- **OpenObserve UI/API:** `http://openobserve:5080`.
- **Auth:** Basic Authentication, **read from environment variables** — never hardcoded.

## E. Stream / signal mapping (relevant to `common`)

| Stream | Our source |
| --- | --- |
| `platform_logs` | Executor, scheduler, trading/risk engine, REST APIs (incl. `common` StructuredLogger) |
| `flink_logs` | JobManager/TaskManager/operator/checkpoint events |
| `fluss_logs` | Coordinator/TabletServer/storage |
| `infrastructure_logs` | Docker/Linux/system |
| `trading_alerts` | **`AlertThresholds`** (8 categories), risk violations |
| `metrics` | Flink/Fluss/JVM/Python/infra metrics |
| `traces` | distributed requests, pipelines, API requests |

## F. Structured logging contract (every record)

**Required:** `timestamp, level, service, component, subsystem, host, vm_id, environment, correlation_id, trace_id, span_id, message`.
**Optional:** `symbol, timeframe, strategy, job_name, task_name, exception, stacktrace`.
Logs **must be structured JSON**; plain text is prohibited. Our `StructuredLogger`
already emits all required fields plus optional `symbol/strategy/exception`.

## G/H/I. Metrics / tracing / alerts

- **Metrics:** collect Flink (job health, checkpoints, backpressure, throughput, latency), Fluss (r/w throughput, tablet health, storage), Python (request/latency/queue/error), infra (CPU/mem/disk/net/Docker/VM).
- **Tracing:** OTel standard; every request propagates `trace_id` + `span_id`; traced logs include them.
- **Alerts:** sources = trading/risk/scheduler/executor/Flink/Fluss/infra; destination = `trading_alerts`; categories = Critical/Error/Warning/Information.

## K. Retention

- Logs 30d, Metrics 90d, Traces 14d, Alerts 180d.
- **Note:** this does **not** satisfy the project's **7-year audit retention**
  requirement. Audit retention is a separate S3-backed store (deferred item);
  OpenObserve is the *live* observability backend only.

## L. AI-agent rules (relevant to code)

- Never bypass the OTel Collector.
- Never write observability data to local files except temporary buffering.
- Never introduce additional logging systems.
- Emit structured JSON logs only; use OTel semantic conventions.
- Treat OpenObserve as the **only** observability backend.

## Matrix status

`openobserve` row in `version_matrix.yaml`: `proposed_version: v0.91.5-amd64`,
`evidence_source: docs/04_contracts/openobserve.md`,
`evidence_method: manual_spec_review`, `compatibility_class: UNKNOWN`
(pending capability-evidence run against a live collector).
