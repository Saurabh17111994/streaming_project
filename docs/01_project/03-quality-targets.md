# Quality Targets and Delivery Guarantees

## Performance SLOs

Every SLO must report p50, p95, and p99, the workload profile, UTC clock source, sample duration, and whether failures/restarts are included.

| Stage | Target | Measurement boundary |
| --- | ---: | --- |
| Decode to raw append acknowledgement | < 5 ms target | Broker event received → `raw_table_1` acknowledgement |
| Trigger tick to winner commit | < 100 ms target | Signal-triggering tick consumed by Flink → `Trade_Decisions` commit |
| Winner commit to Executor receipt | Baseline required | Instruction commit → Executor changelog receipt; set release threshold from the pinned connector benchmark |
| Broker REST call | Baseline required | OpenAlgo request start → verified broker response; separate from stream SLO and evidence-gated |
| Data-path recovery | < 30 s target | Failure detection → ingestion and Flink processing resumed |
| Safe-halt | < 5 s target | Uncertain-state detection → order gate blocks new submissions |
| EOD offload | < 30 min target | Market close → verified Iceberg commit and manifest |

The 15-second event-time candle wait is a business/windowing characteristic, not hidden latency. Report it separately as `window_end → candle publication` and `trigger_tick → winner_commit`.

## Workload envelope

The normal day-to-day production workload is **75,000 ticks/second on average**. This is the sustained baseline, not a stress scenario. Capacity and latency tests must not use 10,000/s or 50,000/s as evidence of production readiness.

Until captured market-open data establishes higher empirical percentiles, use the following minimum planning envelope:

| Scenario | Tick rate | Duration | Purpose |
| --- | ---: | --- | --- |
| Normal production | 75,000/s average | Full trading session | Required sustained operating baseline |
| Burst capacity | 112,500/s minimum (1.5× baseline) | At least 30 minutes | Market open and volatile periods |
| Stress and headroom | 150,000/s minimum (2× baseline) | At least 60 minutes | Saturation behavior and recovery margin |

Tests must use the production instrument universe, subscription mode, message-size distribution, and connection count. The platform must meet production latency SLOs at 75,000/s; burst and stress tests additionally verify bounded backlog, backpressure, checkpoint stability, recovery, and absence of acknowledged data loss.

The 1.5× and 2× planning factors are minimum engineering headroom, not claims about measured broker peaks. Replace them with higher values if captured p99 and worst-case rates require it.

## Delivery guarantees

| Boundary | Guarantee |
| --- | --- |
| Broker WebSocket → ingestion | At-least-once; broker-provided sequence is not assumed |
| Ingestion → Fluss LOG | At-least-once with client retry behavior |
| Tick deduplication | Bounded best-effort using an event fingerprint; not proof of exact identity when Arrow supplies no sequence/event ID |
| Flink state and Fluss sinks | Exactly-once within the checkpointed Flink boundary, subject to connector semantics |
| Fluss command → Executor | At-least-once changelog delivery; Executor deduplicates by `instruction_id` / attempt identity |
| Executor → broker REST | At-least-once command submission unless broker idempotency/reconciliation proves effectively-once behavior |
| Fill capture | At-least-once postback processing with idempotent state updates and immutable audit events |

A broker REST call is never described as exactly-once merely because the producing Flink job is checkpointed.

## Order-gate safe-halt

The Executor owns the final gate before every new-order or position-changing broker call.

```text
HALTED
  → RECONCILING(evidence_hash, epoch)
  → APPROVAL_PENDING(first_approver)
  → ENABLED(second_distinct_approver)
  → HALTED(reason, detected_at, next_epoch)
```

The gate enters `HALTED` for duplicate-order risk, unknown submission outcome, checkpoint failure affecting the order path, stale signal state, missing fill correlation, or failed changelog continuity. A restart with unverifiable gate state defaults to `HALTED`.

Resumption requires:

1. Broker order reconciliation
2. Open position and fill reconciliation
3. Changelog offset / consumer-health verification
4. Confirmation that the signal job and checkpoints are healthy
5. Resolution of every unknown attempt and reservation
6. Two distinct authenticated operators approving the same gate epoch and evidence hash

Existing positions may be monitored during a halt, but no new order is submitted until the gate returns to `ENABLED`.

## Source links

- Non-functional requirements: [`../02_requirements/03-non-functional.md`](../02_requirements/03-non-functional.md)
- Compute guarantees: [`../02_requirements/02-functional/03-compute.md`](../02_requirements/02-functional/03-compute.md)
- Executor contract: [`../02_requirements/02-functional/07-executor.md`](../02_requirements/02-functional/07-executor.md)
