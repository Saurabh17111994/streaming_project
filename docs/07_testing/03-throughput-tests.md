# Throughput and Latency Test Plan

<!-- markdownlint-disable MD013 -->

## Status

Implementation-ready performance plan. No workload evidence exists yet.

## Workload profiles

| Profile | Rate | Duration | Purpose |
| --- | ---: | ---: | --- |
| Baseline | 75,000 ticks/s | Full trading session | Production operating target |
| Burst | 112,500 ticks/s | At least 30 minutes | Open/volatile periods |
| Stress | 150,000 ticks/s | At least 60 minutes | Saturation/headroom/recovery |

Use production instrument universe, subscription mode, message-size distribution, and connection count. Record fixture/capture checksum.

## Measurements

- Decode → raw append acknowledgement p50/p95/p99.
- Trigger tick → immutable decision commit p50/p95/p99.
- Decision commit → Executor receipt baseline.
- OpenAlgo request → verified response separately.
- Throughput, backlog, source/sink lag, watermark lag, backpressure, state size, checkpoint duration/size, restart count, and acknowledged loss.
- Data-path recovery and safe-halt latency.
- EOD offload duration and expiry margin.

Every result includes exact versions/digests, configuration hash, duration, UTC clock source/offset, monotonic timer source, sample count, and failure/restart inclusion.

## Pass criteria

- Baseline meets the documented decision p99 target.
- Burst/stress backlog remains bounded and recovery is measurable.
- No acknowledged data loss.
- Checkpoints remain stable and restore is successful.
- Safe-halt remains under five seconds.
- Accepted data recovery remains under thirty seconds.
- EOD verification meets the thirty-minute target at full volume.

Performance results do not prove protocol correctness, duplicate safety, or live-money readiness by themselves.
