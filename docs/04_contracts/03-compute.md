# Segment Build Contract — Compute

## Boundary

The Signal Flink job consumes `raw_table_1`, performs bounded fingerprint deduplication, computes final 15-second event-time candles, and passes forming-bar state directly to Business Logic in the same job.

## State

- Fingerprint dedup state with configured, bounded TTL covering the declared ingestion retry/replay horizon plus watermark delay; deployment rejects shorter or unbounded values
- Per-instrument event-time window state
- Forming-bar accumulator
- Source offsets and version metadata

Production checkpoints/savepoints use encrypted S3. The exact state backend and connector versions are pinned and tested.

## Event-time contract

The deployed watermark, allowed-lateness, and source-idleness values are configuration parameters, not universal protocol constants. The default profile is bounded out-of-orderness of five seconds, allowed lateness of five seconds, and source idleness of fifteen seconds, as specified by the requirements; each value may be changed only through a tested deployment profile. A source without a verified event timestamp cannot advance the watermark.

A candle emits once after `window_end + allowed_lateness`; later records are discarded and measured. No correction/update row exists in MVP.

Open/close tie ordering is deterministic from the versioned fingerprint specification, not broker `seq_no`.

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

- Functional: `REQ-FC-001` through `REQ-FC-010`
- Cross-cutting: `03-non-functional.md` §§3.1–3.3, 3.5, 3.8; `04-data.md` §§4.1, 4.3–4.7; `05-interfaces.md` §§5.2–5.3, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/03-compute.md`.
