# SIG-PERF-001 50k t/s baseline — 2026-08-17 (Item F)

Evidence for 18-signal-job-remaining-work-plan.md Item F (50k baseline envelope).

Manifest: Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv = 2,433 instruments (2,434 lines incl header) → 48,660 ticks/s at 20 Hz (faketool -real-rate-hz 20) ≈ 50k baseline.

Smoke 2-min at full manifest (F.1): FAILED at 90s warmup — bridge FATAL token count 2433 exceeds capacity 1024 (single-socket production policy ARROW_HFT_CONNECTIONS=1 + ARROW_HFT_MAX_TOKENS_PER_CONNECTION=1024 → capacity 1,024 / 20,480 t/s). 0 ticks, 0 checkpoints. Bottleneck recorded → straight to F.4 per plan, F.2 30-min not run (structurally unreachable without multi-connection decision).

F.2 30-min: NOT RUN (blocked by F.1 policy ceiling — feed-side, not compute)
F.3 gates: NOT EVALUATED (no run) — would require feature_candles_15s gains with window_end after start, RUNNING every sample, 0 expired checkpoints, DESIGN-PERF[envelope] rawRowsPerSec ≥48.7k
F.4 bottleneck: RECORDED — single-socket policy is deliberate production constraint; baseline stays deferred territory (PERF-PROD-60000-001); no config inflation (host CPU/RocksDB/parallelism not raised)

Evidence: manifest count, exact FATAL lines, capacity math, root-cause code refs, reachable-envelope measurement from Item C (18,441 rows/s at 1024-inst envelope) — see CHG-025.
Verdict: DONE-as-recorded (bottleneck measured, deferred territory).
