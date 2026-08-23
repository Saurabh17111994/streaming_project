# P7 battery — Design-B (30-min E2E + native OTel) — 2026-08-17

Evidence for 18-signal-job-remaining-work-plan.md Item C (P7.2/P7.3) on Design-B topology.

Topology: dedup = authoritative Flink keyed MapState + native StateTtlConfig (300000 ms), sinks = plain FlussSink, metrics = native flink-metrics-otel → OpenObserve.

Runner: E2E_RUN_MINUTES=30 OTEL_COLLECTOR_HOST=localhost:4318 bash code/01_platform/04_scripts/run-signal-chain-e2e.sh (smoke 2-min passed HEALTHY; feature-flat in smoke documented as deep-backlog artifact)

Metrics (43-metric P7.2 battery per 11-testing-and-release.md §P7):

- Throughput: feed replay consumption 60,867 rows/s at tablet ceiling (58.9–59.7k ceiling measured topology-independent, CHG-025); steady-state live feed 18,441 rows/s (feed-limited, 1024-instrument envelope 20,480 t/s); 179 checkpoints / 0 failed / 0 restarts (JobGraphDump 179→217 across runs)
- Latency: p99 NOT MEASURABLE — 2026-08-12 prometheus exporter dropped histogram buckets (mean 152.6 ms), 2026-08-17 native OTel series `latency_histogram_points` NO DATA (query recipe logs/tracker-14/o2-native-reporter-series-20260817.md) — accepted outcome per Item C delta 3
- Checkpoint: failures PASS (0 failed), duration FAIL (replay-inflated state, recorded not inflated)
- Dedup memory: Flink RocksDB MapState + native TTL — gauge compute.dedup.state.count + checkpoint size: peak ~25.6M entries / ~3.5GB estimate during 66.5M-row replay, steady-state ~628 MB at 20,480 t/s envelope; full checkpoint p50 1.6 GB / max 1.9 GB; 30/60/120s expiry sweep unmeasured (replay-dominated run), DEDUP_TTL_MS=300000 pinned

Gates P7.3:
- PERF-THROUGHPUT-001 (50k sustained) — NOT ACHIEVED (ceiling, recorded)
- PERF-LATENCY-001 (p99 <100ms) — NOT MEASURABLE (accepted)
- DEDUP-MEMORY-001 (bounded + expiry sweep) — MEASURED (replay envelope), steady-state sweep remains
- Checkpoint failures — PASS, duration — FAIL (replay), memory — N/A to harness topology

Register rows updated in 11-testing-and-release.md §11 (2026-08-17 Design-B marker).
Verdict: battery executed, all gates verdicted (PASS/FAIL/NOT-MEASURABLE recorded, no config inflation). Evidence series: p7-battery-design-b-series.tsv (native OTel series, see O2 transcripts).
