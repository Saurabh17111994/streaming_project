# Ingestion Alerting Contract

Applies to: `02_services/01_ingestion` (bridge + `IngestionService`).

Metrics are exported by `OtlpMetricsEmitter` via OTLP HTTP (`:4318`) to the
OpenObserve backend (see `01_platform/01_docker/otel-collector-config.yaml`).
Alerts are defined in OpenObserve (alerting rules UI / scheduled search).
This contract pins the **alert name, condition, severity, and runbook link**
so any operator can recreate them.

## Rule table

| # | Alert name | Condition (metric) | Threshold | Severity | Runbook |
|---|-----------|--------------------|-----------|----------|---------|
| 1 | Subscription headroom low | `bridge.slot.capacity_used_percent` (per `slot_id`) | `>= 90` for 5 min | warning | `01-runbooks.md` §Subscription |
| 2 | Subscription headroom critical | `bridge.slot.capacity_used_percent` | `>= 98` for 2 min | critical | `01-runbooks.md` §Subscription |
| 3 | Reconnect storm | `bridge.reconnects` (rate) | `>= 5 / 10 min` | critical | `01-runbooks.md` §Bridge recovery |
| 4 | Consecutive reconnects | `bridge.reconnect.consecutive` | `>= 5` | critical | `01-runbooks.md` §Bridge recovery |
| 5 | Stale feed | `bridge.slot.last_frame_age_ms` | `>= 5000` (5s, the freshness limit) for 1 min | critical | `01-runbooks.md` §Stale feed |
| 6 | Decode error burst | `decode.errors` (rate) | `>= 100 / 10 s` | critical | `01-runbooks.md` §Decode errors |
| 7 | Partial subscription | `bridge.slot.rejected` | `> 0` for 5 min | warning | `01-runbooks.md` §Subscription |
| 8 | Not ready | `ingestion.ready` | `== 0` for 2 min | critical | `01-runbooks.md` §Readiness |
| 9 | Bridge disconnected | `bridge.connected` | `== 0` for 1 min | critical | `01-runbooks.md` §Bridge recovery |
| 10 | Heartbeat failures | `heartbeat.failures` (counter delta) | `>= 3 / 5 min` | warning | `01-runbooks.md` §Heartbeat |
| 11 | OTLP collector unhealthy | `otel.collector.healthy` | `== 0` for 10 min | warning | `01-runbooks.md` §Telemetry |

## Notes

- Headroom = `acknowledged / assigned` per slot, exported as a percent gauge
  (`bridge.slot.capacity_used_percent`). The plan target is 1,024 tokens per
  connection; alert #1 fires when the account is 90% subscribed.
- All gauges are labeled by `slot_id` only — never by token/symbol (plan
  §Monitoring redaction rule).
- All 11 rules ARE wired into the local OpenObserve instance as of 2026-08-24
  (see `code/01_platform/04_scripts/o2-provision.py`, ALERTS catalog; CHG-093).
  This file remains the source of truth for names/thresholds/severity — any
  catalog drift is a defect. Note: with the pinned dev config (1 connection ×
  1024 tokens fully subscribed) the capacity gauges sit at 100%, so rules #1/#2
  fire continuously until the C5.2 multi-connection decision changes headroom.
