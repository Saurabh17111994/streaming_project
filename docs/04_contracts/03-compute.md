# Segment Build Contract — Compute

## Boundary

The Signal Flink job consumes `raw_table_1`, performs bounded fingerprint deduplication, computes final 15-second event-time candles, and passes forming-bar state directly to Business Logic in the same job.

**Tier-scoped deployment (current testing phase):** the current phase builds and validates the Signal job on the approved 1,024-instrument / single-connection envelope (20,480 ticks/s at 20 Hz per instrument). The 3,000-instrument / 50,000 ticks/s variable baseline remains the deferred production target (`PERF-PROD-60000-001`; `PERF-PROD-90000-001` retired with the peak campaign, DEC-036); per-instrument windowing, dedup, and candle logic are identical across envelopes — only the load/acceptance profile differs.

## State

- Fingerprint dedup state with configured, bounded TTL covering the declared ingestion retry/replay horizon plus watermark delay; deployment rejects shorter or unbounded values
- Per-instrument event-time window state
- Forming-bar accumulator
- Source offsets and version metadata

Production checkpoints/savepoints use encrypted S3. The exact state backend and connector versions are pinned and tested.

## Event-time contract

The deployed watermark, allowed-lateness, and source-idleness values are configuration parameters, not universal protocol constants. The default profile is bounded out-of-orderness of five seconds, allowed lateness of five seconds, and source idleness of fifteen seconds, as specified by the requirements; each value may be changed only through a tested deployment profile. A source without a verified event timestamp cannot advance the watermark.

A candle is final from its first write: the final row emits at first window fire (watermark ≥ `window_end`), an `emitted` window-state flag makes any allowed-lateness re-trigger a no-op (late-within-lateness folds into the accumulator and is counted, never re-written), and no correction/update row exists in MVP. The "final after `window_end + allowed_lateness`" phrasing means the finalization boundary — the candle is not corrected after that point.

Open/close tie ordering is deterministic from the versioned fingerprint specification, not broker `seq_no`.

## Event-time and finalization

The deployed watermark, allowed-lateness, and source-idleness values are configuration parameters, not universal protocol constants. The default profile is bounded out-of-orderness of five seconds, allowed lateness of five seconds, and source idleness of fifteen seconds. Each value may be changed only through a tested deployment profile. A source without a verified event timestamp cannot advance the watermark.

A candle is final from its first write: the final row emits at first window fire (watermark ≥ `window_end`), an `emitted` window-state flag makes any allowed-lateness re-trigger a no-op (late-within-lateness folds into the accumulator and is counted, never re-written), and no correction/update row exists in MVP. The "final after `window_end + allowed_lateness`" phrasing means the finalization boundary — the candle is not corrected after that point. The finalization contract SHALL separately name the watermark out-of-orderness bound, source-partition idleness threshold, allowed/finalization delay, source split identity, reconnect/reassignment behavior, and late-event classification. The term `allowed lateness` SHALL not imply correction/update rows.

## Deduplication horizon

The `dedup_horizon` is the maximum supported append retry, connector replay/rewind, checkpoint restore rewind, broker replay, and approved operational replay interval, plus a documented safety margin. `DEDUP_TTL_MS` SHALL be exactly `300000` (5 minutes) in MVP; deployment SHALL reject any other value. The implementation SHALL report accepted event rate, dedup entries, serialized entry bytes, physical backend/checkpoint bytes, and restore duration at the variable 50,000 ticks/s average baseline. (The 90,000 ticks/s peak is retired, DEC-036.)

## Typed handoff to Business Logic

Compute SHALL expose both typed closed-candle events and typed forming-bar events to Business Logic within the Signal job. Each event includes instrument, `portfolio_id` or routing scope when applicable, window boundaries, source schema/configuration versions, deterministic ordering metadata, and event/processing timestamps. Business Logic SHALL not reconstruct these events by reading the candle table back.

## Outputs

- `feature_candles_15s` immutable final rows
- Typed in-job forming-bar events to Business Logic
- Invalid, duplicate, late, watermark, checkpoint, and backpressure metrics

`Trade_Decisions` are immutable instruction records published by the Signal job; Compute and Ranking do not expose a Fluss round trip between forming-bar updates and Business Logic evaluation.

## Guarantees

Exactly-once applies only to the pinned integration-tested Flink state/sink boundary. It does not imply cross-table atomic visibility or broker-call exactly-once.

## Acceptance

Out-of-order, duplicate, identical-legitimate-event, empty/invalid window, late discard, deterministic replay, checkpoint restore, backpressure, node/process restart, and workload-envelope tests pass.

## Requirement traceability

- Functional: `REQ-FC-001` through `REQ-FC-013`
- Cross-cutting: `03-non-functional.md` §§3.1–3.3, 3.5, 3.8; `04-data.md` §§4.1, 4.3–4.7; `05-interfaces.md` §§5.2–5.3, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/03-compute.md`.
