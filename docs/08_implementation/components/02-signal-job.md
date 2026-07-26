# Signal Flink Job Implementation Dossier

<!-- markdownlint-disable MD013 -->

## Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation-ready, connector/version evidence blocked |
| Owner | Compute and Strategy Teams |
| Requirements | `REQ-FC-*`, `REQ-BL-*`, `REQ-RNK-*` |
| Contracts | `docs/04_contracts/03-compute.md`, `04-business-logic.md`, `10-ranking.md` |
| Job topology | One Signal job containing Compute, Business Logic, and Ranking |
| Separate job | Babysitter only |

## Job graph

```text
Fluss raw_table_1 source
→ schema/validity filter
→ event-time assignment and watermark
→ keyBy(instrument_token)
→ bounded fingerprint dedup
→ forming-bar/window state
├─ final 15-second candle sink
└─ typed forming-bar update
   → business rules/candidate state
   → candidate audit sink
   → in-operator ranking/reservation state
   ├─ ranking audit sink
   └─ immutable Trade_Decisions sink
```

No feature-table read-back, candidate Fluss round trip, separate ranking job, or separate feature-compute job exists in MVP.

## Suggested operator boundaries

| Operator | Key/state | Responsibility |
| --- | --- | --- |
| `RawValidation` | Stateless | Schema version, validity, event classification |
| `FingerprintDedup` | Instrument + fingerprint scope | Bounded first-seen state and duplicate metrics |
| `CandleAndFormingBar` | Instrument | Event-time window and forming accumulator |
| `BusinessLogic` | Instrument/strategy | Active setup state and candidate emission |
| `RankingAndReservation` | Required portfolio scope | Deterministic scoring, capacity, winner selection |
| `AuditAndDecisionSinks` | Connector transaction boundary | Immutable event/decision outputs |

Actual chaining is performance-tested; logical boundaries remain explicit for metrics and state ownership.

## Configuration contract

| Key | Requirement |
| --- | --- |
| `FLINK_VERSION` / `FLUSS_CONNECTOR_VERSION` | Exact compatibility matrix IDs |
| `RAW_SCHEMA_VERSION` | Accepted input version/range |
| `WATERMARK_OUT_OF_ORDER_MS` | Default 5000; change only through tested profile |
| `ALLOWED_LATENESS_MS` | Default 5000; change only through tested profile |
| `SOURCE_IDLE_MS` | Default 15000 per source partition |
| `DEDUP_TTL_MS` | Must cover retry/replay horizon plus watermark delay |
| `WINDOW_SIZE_MS` | Fixed 15000 for `feature_candles_15s` |
| `CHECKPOINT_INTERVAL_TO_BE_PINNED` | Exact profile, not default |
| `CHECKPOINT_TIMEOUT_TO_BE_PINNED` | Exact profile |
| `STATE_BACKEND_TO_BE_PINNED` | Version-compatible managed state backend |
| `S3_CHECKPOINT_URI_TO_BE_DEFINED` | Production encrypted checkpoint/savepoint storage |
| `STRATEGY_VERSION` | Included in candidates/decisions |
| `RANKING_VERSION` | Included in ranking results/decisions |
| `RESERVATION_POLICY_VERSION` | Included in audit and restored state |

Deployment rejects unbounded or too-short dedup TTL and missing production checkpoint storage.

## Event-time contract

- Event time is the verified UTC broker timestamp.
- Events without verified event time do not advance watermarks.
- Watermark and idleness apply per source partition.
- One final candle emits after watermark passes `window_end + allowed_lateness`.
- Later records are discarded and measured in MVP.
- Open/close ties use the versioned deterministic fingerprint ordering.
- Empty windows emit no row.

## Dedup state

State key contains fingerprint version, scope, and fingerprint. State value contains first accepted event metadata and expiry. First event proceeds; later candidate within TTL increments duplicate metrics and does not affect candle/business state.

Identical legitimate events may be collapsed; this limitation must remain visible in metrics and documentation.

## Candle accumulator

Per instrument/window state:

```text
first_order_key, open_price
last_order_key, close_price
high_price
low_price
volume
accepted_tick_count
window_start, window_end
algorithm/config version
```

Order key is `(event_time, deterministic_fingerprint_order)`. Price and quantity validation occurs before aggregation. Overflow/invalid numeric behavior is explicit and tested.

## Forming-bar and candidate interface

Typed in-process update includes:

- Instrument and routing identity
- Window boundaries
- Current OHLCV/tick count
- Trigger event time/fingerprint
- Strategy configuration version
- Source schema and manifest version

Business Logic maintains active setup state and emits one immutable candidate audit record per detected setup. Same candidate identity cannot change content.

## Ranking and reservation protocol

Ranking consumes typed active candidate snapshots in-process. It records normalized inputs, weights, score, deterministic tie-break, rank, selected flag, rejection reason, ranking version, reservation ID/version, and evaluation ID.

Default MVP capacity:

- At most one reserved/open trade per instrument.
- At most three total reserved/open positions.
- At most one per strategy.

`RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` consume capacity. Reservation state uncertainty suppresses new decisions and signals Executor safety state.

A same winner with unchanged executable content produces audit only. Changed executable parameters require a new `instruction_id` and supersession relation.

## Output consistency

One Flink checkpoint covers job state and sinks, but cross-table atomic visibility is not assumed. Every output carries stable IDs and versions so consumers can tolerate partial visibility and reconcile.

`Trade_Decisions` is immutable. Executor never mutates it.

## Restore and degradation

Restore must recover source offsets, dedup state, windows, forming bars, active candidates, ranking, and reservation state consistently. If state compatibility or continuity cannot be proven:

1. Job reports not ready/degraded.
2. New decisions are suppressed.
3. Executor is signalled to halt.
4. Operator reconciliation/savepoint policy is invoked.

## Job submission contract

- Build one versioned job JAR.
- Upload once.
- Submit Signal job with explicit entry class/args.
- Submit Babysitter separately.
- Capture job IDs and artifact checksum.
- Treat repeated submission idempotently.
- READY requires both required jobs RUNNING and checkpointing.

## Required telemetry

Source throughput/lag, invalid events, dedup candidates/hits/state size, late events, watermark lag, window/candle rates, forming updates, candidate/ranking/decision rates, reservation states/conflicts, operator busy/idle/backpressure, sink latency, checkpoint duration/size/failures, restore count, and state compatibility failures.

## Required tests

- `SIG-UNIT-001` deterministic fingerprint tie ordering.
- `SIG-UNIT-002` candle aggregation and empty windows.
- `SIG-UNIT-003` dedup TTL validation.
- `SIG-UNIT-004` candidate identity/supersession.
- `SIG-UNIT-005` deterministic ranking/tie-break.
- `SIG-UNIT-006` reservation capacity transitions.
- `SIG-HARNESS-001` out-of-order/watermark/idleness.
- `SIG-HARNESS-002` late-before-final versus late-after-final.
- `SIG-HARNESS-003` checkpoint/restore deterministic replay.
- `SIG-INT-001` pinned Fluss source/sink boundary.
- `SIG-INT-002` partial output visibility/reconciliation.
- `SIG-FAIL-001` checkpoint/state-continuity safe halt.
- `SIG-PERF-001` full workload envelope and p99 decision latency.

## Definition of done

The implementation is complete when exactly one Signal job performs the full path, deterministic replay is proven, state/checkpoint compatibility passes, cross-table limitations are documented and tested, readiness suppresses decisions on uncertainty, and no code/documentation claims external broker exactly-once behavior.
