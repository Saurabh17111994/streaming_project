# 08 Local Compose — Phase D Implementation Plan

## Scope
OBS-001..012 + RES-001..006 + PERF-001..005 (production-confidence locally)

Per 08-local-compose.md §L9/L11. All tests pass offline (contract checks on
OTel collector config, PlatformConfig 65/35/85, Prometheus scrape, checkpoint
observability, throughput targets); live probes gate on containers so CI stays
green.

## Files to create
- `code/01_platform/04_scripts/tests/test_08_local_compose_l9.py` — L9 OBS-001..012
- `code/01_platform/04_scripts/tests/test_08_local_compose_l11.py` — L11 RES-001..006 + PERF-001..005
- Update `Makefile` — add `test-observability`, `test-performance`, `test-08-phaseD`, `test-all`

## Invariants
- Every service emits to OTel collector (OTLP/filelog/prometheus) → OpenObserve; collector holds O2 auth, ingestion is credential-free.
- Flink checkpoint failure observable via PrometheusReporter :9249 scrape + SignalJobObjectStoreCheckpointIntegrationTest.
- JVM heap 65 / non-heap 35 / alert 85 are pinned in PlatformConfig and wired via docs/08-local-compose.md.
- Telemetry failure (collector/O2 down) is explicitly non-critical — trading correctness is never gated on it.
- Throughput targets (1k/5k/10k/s, sustained, burst) are spec targets; ingestion baseline is 20 ticks/instrument/s (PlatformConfig.BROKER_BASELINE_...).

## Verification
```
make test-observability
make test-performance
pytest code/01_platform/04_scripts/tests/test_08_local_compose_l9.py code/01_platform/04_scripts/tests/test_08_local_compose_l11.py -v
```
