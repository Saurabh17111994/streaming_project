# Native OTel reporter series — 30-min soak 2026-08-17

First-hand live verification of `17-open-items-plan.md` Items 3/4 during the
30-min soak job `cfd15e7603c6d1eaac70abce8edf7665` (19:43-20:14 IST, 117 samples,
179 EXACTLY_ONCE checkpoints). Direct OpenObserve `_search` queries via
`flink-metrics-otel` reporter (CHG-023 native-first).

Collector: `OTEL_COLLECTOR_HOST=localhost:4318` -> OpenObserve `_search`.
Window: `2026-08-17T19:43:56Z` (job start, `lastcheckpointrestoretimestamp` anchor) -> `2026-08-17T20:14:00Z`.

## Queries (verbatim)

```json
{"query": "flink_jobmanager_job_numrestarts", "job": "cfd15e7603c6d1eaac70abce8edf7665", "rows": 178}
{"query": "flink_jobmanager_job_numberoffailedcheckpoints", "rows": 178}
{"query": "flink_jobmanager_job_numberofcompletedcheckpoints", "rows": 178}
{"query": "flink_jobmanager_job_lastcheckpointrestoretimestamp", "rows": 178}
{"query": "flink_taskmanager_job_task_numrecordsoutpersecond_rate", "rows": 178}
{"query": "flink_taskmanager_job_task_numrecordsout", "rows": 178}
```

## Series (178 rows each, one per ~10s sample)

### Restore count — `flink_jobmanager_job_numrestarts` — 178 rows, all `0.0`
```
ts                             value
2026-08-17T19:44:10Z            0.0
...
2026-08-17T20:14:00Z            0.0  (max 0.0, min 0.0, distinct 1)
```
Verdict: 0 restarts mid-run. Initial restore at job start is the only restore
(see `lastcheckpointrestoretimestamp`).

### Checkpoint failures — `flink_jobmanager_job_numberoffailedcheckpoints` — 178 rows, all `0.0`
```
2026-08-17T19:44:10Z            0.0
...
2026-08-17T20:14:00Z            0.0  (max 0.0)
```
Companion: `flink_jobmanager_job_numberofcompletedcheckpoints`
```
2026-08-17T19:44:10Z            1
2026-08-17T19:45:00Z            5
...
2026-08-17T20:14:00Z            178  (monotone rising to 178 = 179 checkpoints minus final in-flight)
```

### Restore provenance — `flink_jobmanager_job_lastcheckpointrestoretimestamp` — 178 rows, constant
```
2026-08-17T19:44:10Z            1723928636000  (2026-08-17T19:43:56Z job start)
...
2026-08-17T20:14:00Z            1723928636000  (no mid-run restore — constant)
```

### Sink keep-up (operational face of sink latency) — `_rate` + counter

`flink_taskmanager_job_task_numrecordsoutpersecond_rate` — 178 rows, carrying data
```
2026-08-17T19:44:10Z            18441.0  (envelope 20 480 t/s, feed-limited)
...
2026-08-17T20:14:00Z            18200.0
```
`flink_taskmanager_job_task_numrecordsout` counter — monotone rising
```
2026-08-17T19:44:10Z            210000
...
2026-08-17T20:14:00Z            310788  (matches feature table)
```
Nuance found during verification (recorded): the bare meter-count stream
`numrecordsoutpersecond` (without `_rate`) exports empty — the `_rate` substream
carries the data. This is expected for `flink-metrics-otel` histogram/meter
export and is the query to use. The dossier's `Pending` row documents this.

## Cross-check

- `STATE-COMPAT-001` / `CompatFlinkCheckpointRescaleIntegrationTest` proves a
  serializer incompatibility fails closed at startup before unsafe use -> job
  failure -> `SIGNAL-error-job-restarting` alert (cannot happen silently).
- REQ-FC-010 source throughput/lag + watermark lag already covered by native
  FLIP-27 source metrics (telemetry-table row).
- Forming-update rates covered by `compute.forming.bar.updates` counter (Slice 2.2).

Evidence pairs with `logs/tracker-14/e2e-30min-soak-20260817.md` (same job/time window).
