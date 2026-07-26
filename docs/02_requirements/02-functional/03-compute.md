# 02.3 — Compute

## Purpose

The Signal Flink job consumes `raw_table_1`, performs bounded best-effort deduplication, assigns event-time semantics, emits final MVP candles, and passes closed-candle plus forming-bar state to Business Logic within the same job. It does not read feature tables back from Fluss for signal generation.

## REQ-FC-001: MVP scope

MVP computes 15-second event-time OHLCV candles and the forming-bar state required by Business Logic. Advanced feature columns, market-context features, and current-price Babysitter inputs are deferred until a separate phase with its own schema and tests.

The `feature_candles_15s` table name fixes the deployed MVP granularity at 15 seconds. A different granularity requires a new versioned table and migration; it is not a runtime-only toggle.

## REQ-FC-002: Source and event classification

Compute reads only accepted rows from `raw_table_1`. It SHALL:

1. Filter to eligible trade events for OHLCV.
2. Permit quote/depth events to advance source/watermark state only when the source contract proves they carry valid event timestamps.
3. Exclude invalid trade rows from aggregation while preserving their raw audit and reason metrics.
4. Key state by `instrument_token` and preserve connection/fingerprint metadata for diagnostics.

## REQ-FC-003: Fingerprint deduplication

Compute SHALL use a bounded state keyed by the versioned `event_fingerprint` and its scope. It SHALL retain the first accepted event within the configured TTL and count later candidates as dedup hits.

The deduplication guarantee is best-effort. It may collapse identical legitimate events or fail to identify semantically duplicate packets. It SHALL NOT use `seq_no` as a required key or deterministic ordering field.

Dedup state TTL SHALL cover the configured ingestion retry/replay horizon plus watermark delay. The deployment SHALL reject a TTL configuration that is unbounded or shorter than the declared retry horizon.

## REQ-FC-004: Event-time and watermarks

- Event time is UTC epoch milliseconds from the verified broker timestamp.
- Default watermark strategy is bounded out-of-orderness of 5 seconds, configurable only with a tested deployment profile.
- Default allowed lateness is 5 seconds after the watermark, configurable with a tested profile.
- Source idleness is 15 seconds by default and must be configured per source partition.
- A source without a valid timestamp cannot advance the event-time watermark.

A late event within allowed lateness may affect an in-memory window before emission. A late event after emission is discarded in MVP and counted; it does not create a correction row or update a prior candle.

## REQ-FC-005: Candle aggregation

For each instrument and 15-second event-time window containing at least one eligible trade:

| Field                       | Rule                                                                                         |
| --------------------------- | -------------------------------------------------------------------------------------------- |
| `open`                      | Lowest event-time accepted trade in the window; ties use deterministic fingerprint ordering  |
| `high`                      | Maximum accepted trade price                                                                 |
| `low`                       | Minimum accepted trade price                                                                 |
| `close`                     | Highest event-time accepted trade in the window; ties use deterministic fingerprint ordering |
| `volume`                    | Sum of accepted non-negative trade quantities                                                |
| `tick_count`                | Count of accepted trade events after deduplication                                           |
| `window_start`/`window_end` | UTC epoch-millisecond window boundaries                                                      |
| `ingest_ts`                 | Timestamp of final output acknowledgement path                                               |

The implementation SHALL define the exact tie ordering in the fingerprint specification. It SHALL NOT use incomparable per-connection sequence values.

Empty windows and windows containing only invalid/late-discarded events produce no row.

## REQ-FC-006: Final-on-emission policy

A candle becomes final when the watermark passes `window_end + allowed_lateness` and the output is acknowledged by the tested Fluss sink boundary. Subsequent events for that window are discarded in MVP.

The discard metric SHALL include instrument, window, lateness, and reason. A future correction policy requires a new versioned table/contract and is outside MVP.

## REQ-FC-007: Forming-bar state handoff

Within the same Signal Flink job, Compute SHALL expose a typed in-job event to Business Logic whenever an eligible trade updates the current forming bar. The event includes instrument, window boundaries, current OHLCV accumulator, event timestamp, fingerprint, and source metadata.

Business Logic SHALL consume this state directly. It SHALL NOT wait for `feature_candles_15s` or perform a Fluss round trip for ranking.

## REQ-FC-008: Checkpoint boundary

The Signal job SHALL use RocksDB or the version-pinned equivalent managed state and durable S3 checkpoint storage in production. Checkpoint interval, timeout, concurrent checkpoint count, state backend, and restart strategy SHALL be exact-version configuration, not undocumented defaults.

Exactly-once may be claimed only for the state/sink boundary proven by the version-specific integration suite. The requirement does not make independent table visibility or broker REST calls exactly-once.

On restore, the job SHALL restore dedup, window, forming-bar, and ranking state consistently or enter a safe degraded state that prevents new instructions.

## REQ-FC-009: Backpressure

Compute SHALL use Flink-managed backpressure and expose source lag, watermark lag, operator busy/idle time, sink latency, checkpoint duration/size, pending records, and restart counts. It SHALL not add an unbounded custom queue.

Sustained backpressure that threatens the 100 ms decision SLO, checkpoint timeout, or safe state continuity SHALL alert and cause the order path to halt according to the Executor contract.

## REQ-FC-010: Metrics and acceptance

Required metrics include source throughput, dedup hits, dedup state size, invalid events by reason, late events, candle throughput, watermark lag, forming-bar update rate, source/sink latency, checkpoint duration/size, restore count, and state corruption/recovery events.

Acceptance tests SHALL prove:

1. Event-time window assignment with out-of-order fixtures.
2. Fingerprint duplicate and identical-legitimate-event behavior.
3. Invalid/empty-window handling.
4. Final-on-emission late-data discard.
5. Deterministic tie ordering from an identical input snapshot.
6. Checkpoint restore without state duplication within the tested boundary.
7. Backpressure and checkpoint stability at 75,000/112,500/150,000 ticks per second.
8. Safe-halt when state continuity or checkpoint health becomes uncertain. 
