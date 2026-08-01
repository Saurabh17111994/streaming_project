# Ingestion

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation active; Phases 2a-2g complete (149/155 tasks ✅, 96%); 78 tests (43 ingestion + 35 common), 0 failures, 4 env-gated skips; 3 ⚠️ evidence-gated; 3 ❌ blocked on Fluss multi-node cluster |
| Owner | Ingestion Team |
| Requirements | `REQ-ING-001`–`REQ-ING-016` |
| Build contract | `docs/04_contracts/01-ingestion.md` |
| Writes | `raw_table_1`, `suspected_discontinuities`, quarantine evidence |
| Must not own | Candle computation, strategy, ranking, broker orders, fill lifecycle |

### Process boundary

Two colocated processes in the same container form the ingestion boundary:

**Go arrow-bridge** — uses the official Arrow Go SDK (`go-arrow`) for: authentication (AutoLogin or static token), WebSocket connection (`wss://socket.arrow.trade`), binary frame decode (all four modes: LTP, LTPC, QUOTE, FULL including 5-level bid/ask depth), zstd decompression (HFT), subscription management, keepalive, and reconnection. Outputs one newline-delimited JSON tick per line to stdout.

**Java IngestionService** — reads NDJSON from stdin, validates, resolves instruments, computes a versioned canonical fingerprint, and appends each tick individually to `raw_table_1` through the Fluss Java client.

The pipe is the kernel's stdin/stdout — not a message queue, not a network hop. Both processes share one container lifecycle. Do not insert Python, Kafka, ZeroMQ, or another transport between the Go bridge and the Java Fluss append.

### Internal modules

| Module | Responsibility |
| --- | --- |
| `bridge` | Go arrow-bridge process lifecycle, stdin/stdout contract, NDJSON schema, reconnect backoff |
| `normalization` | Verified units, timestamps, instrument mapping, validity classification |
| `fingerprint` | Canonical versioned best-effort event fingerprint |
| `writer` | Bounded Fluss append, acknowledgements, retry classification |
| `discontinuity` | Connection/subscription/heartbeat/time-jump evidence |
| `quarantine` | Preserve unsupported/malformed packet evidence |
| `health` | Liveness, readiness, subscription completeness, clock and append health |
| `telemetry` | Structured logs and bounded-cardinality metrics |

### Configuration contract

| Key | Required | Semantics |
| --- | ---: | --- |
| `BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC` | Yes | `20` average for the synthetic baseline profile; not a live-feed interval or per-instrument requirement |
| `BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC` | Yes | `30` hard maximum enforced by the synthetic workload profile |
| `ARROW_APP_ID` | Yes | Arrow application ID for the Go bridge |
| `ARROW_APP_SECRET` | Yes | Arrow application secret for AutoLogin |
| `ARROW_TOKEN` | No | Pre-authenticated access token (24h TTL); if absent, AutoLogin creds are required |
| `ARROW_USER_ID` | No | User ID for AutoLogin (with password+TOTP) |
| `ARROW_PASSWORD` | No | Password for AutoLogin |
| `ARROW_TOTP_KEY` | No | TOTP secret for 2FA during AutoLogin |
| `ARROW_USE_STANDARD` | No | Set `true` for standard ds.arrow.trade; default `false` (HFT socket.arrow.trade) |
| `ARROW_HFT_LATENCY_MS` | No | HFT tick interval ms (default 50, range 50-60000) |
| `ARROW_INSTRUMENT_TOKENS` | No | Comma-separated instrument tokens; empty = synthetic 50-instrument dev set |
| `GO_ARROW_SDK_VERSION` | Yes | Pinned go-arrow SDK version tag (replaces DECODER_VERSION) |
| `FLUSS_BOOTSTRAP_SERVERS` | Yes | Pinned environment endpoint |
| `RAW_TABLE_NAME` | Yes | Must equal reconciled schema manifest |
| `INSTRUMENT_MANIFEST_VERSION` | Yes | Approved subscription snapshot |
| `INGESTION_MAX_BATCH_RECORDS` | Yes | Fixed at `1`; append each accepted tick immediately; startup fails for any other value |
| `INGESTION_MAX_BATCH_WAIT_MS` | Yes | Fixed at `0`; do not wait for a batch; startup fails for any other value |
| `MAX_PENDING_APPEND_RECORDS` | Yes | Fixed at `10000`; stop accepting new broker data and set readiness false at the limit |
| `MAX_PENDING_APPEND_BYTES` | Yes | `min(67108864, floor(container_memory_limit_bytes × 0.10))`; stop accepting new broker data and set readiness false at the limit |
| `PENDING_APPEND_WARNING_PERCENT` | Yes | Fixed at `80`; emit warning alert and set readiness false at 80% of either pending limit |
| `APPEND_TIMEOUT` | Yes | Pinned client classification |
| `FINGERPRINT_VERSION` | Yes | Canonical algorithm version |
| `CLOCK_OFFSET_LIMIT_MS` | Yes | 100 ms unless approved requirement changes |

Missing required configuration makes readiness false. Production never falls back to demo credentials or a guessed endpoint.

### Startup sequence

1. Validate configuration and exact versions.
2. Initialize telemetry without logging secrets.
3. Connect to Fluss and validate required table/schema version.
4. Load exactly one approved instrument manifest snapshot.
5. Validate every active row and routing field.
6. Validate Go arrow-bridge binary is present and executable.
7. Start arrow-bridge as subprocess with configured auth env vars.
8. Java reads NDJSON from bridge's stdout.
9. Enter READY only after recent successful Fluss append acknowledgement and acceptable clock offset.

### Packet processing algorithm

```text
receive NDJSON line from stdin
→ parse JSON; reject malformed lines
→ validate token, price, and timestamp semantics
→ resolve instrument manifest row; quarantine missing tokens
→ classify validity (VALID_TRADE / VALID_NON_TRADE / INVALID_VALUES / MISSING_INSTRUMENT)
→ hash raw JSON bytes for payload integrity
→ calculate versioned event fingerprint
→ create raw row containing original bytes + typed fields + provenance
→ submit single tick immediately through bounded writer (no batching; INGESTION_MAX_BATCH_RECORDS=1, INGESTION_MAX_BATCH_WAIT_MS=0)
→ record acknowledgement timestamp or uncertainty
```

Ingestion appends an accepted raw packet even if its fingerprint was seen before. Compute owns bounded logical deduplication. No time-based or record-count-based application batching is permitted: each accepted tick is submitted individually.

### Fingerprint contract

The versioned canonical specification must define:

- Hash algorithm and output encoding.
- Connection scope fields.
- Field order.
- Null representation.
- Integer/decimal/price canonicalization.
- Timestamp unit and timezone.
- Trade/quote/depth fields included.
- Raw-byte contribution, if approved.
- Collision and identical-legitimate-event limitation.

Fingerprint identity is best-effort, not broker-global identity.

### Backpressure and memory

- The pending append queue is bounded by `MAX_PENDING_APPEND_RECORDS` (10000 records) and `MAX_PENDING_APPEND_BYTES` (`min(67108864, floor(container_memory_limit_bytes × 0.10))`).
- Before accepting a tick, ingestion SHALL reject it when accepting would exceed either pending limit.
- At 80% of either pending limit: set readiness false, emit a warning event containing current records, current bytes, and both limits.
- At 100% of either pending limit: stop broker reads/subscriptions, keep readiness false, emit a critical event, and preserve an acknowledged-loss/uncertainty record. Silently discarding data is prohibited.
- Pending counters SHALL decrease only after the append completes, whether successful or failed.
- Record receive time, append-start time, append-acknowledgement time, append outcome, record size, and error class for every append outcome.
- Retry uses the pinned Fluss client classification.
- No unbounded custom queue is permitted.
- Arrow payloads SHALL NOT be compressed in the ingestion-to-Fluss path.
- TCP flow control is not described as lossless without a broker test.

### Slow-Fluss ingestion policy (`EVIDENCE-GATE-ING-BUFFER-001`) — RESOLVED BY CAPACITY

When Fluss latency, retry count, pending records, or pending bytes cross a configured threshold, the platform alerts immediately and automatic system checks determine readiness.

**Resolution:** Fluss ingests up to 1-2 million ticks/s, and the platform's maximum is 90,000 ticks/s (3,000 instruments × 30 ticks/s). The steady state and peak are within Fluss capacity with margin, so neither a durable local SSD buffer nor a controlled subscription pause is required.

**Remaining defensive bound:** Bounded pending-append limits (10,000 records / `min(64MiB, 10% container memory)` bytes) remain. Reaching a limit is a platform-capacity fault, not a normal operating condition; the existing readiness-halt behavior applies, and indefinite in-memory buffering or silent data loss remains prohibited.

### Failure matrix

| Failure | Required behavior |
| --- | --- |
| Go bridge crash | Java detects stdin EOF, records `BRIDGE_CRASH` discontinuity, exits with non-zero |
| Go bridge auth failure | Exit immediately with error to stderr; Java exits on pipe close |
| Missing instrument | Quarantine; do not append keyed raw row |
| Invalid trade values | Append evidence marked invalid; Compute excludes |
| Broker disconnect | Go bridge SDK handles reconnect internally; Java pauses processing during gaps |
| Subscription incomplete | Go bridge reports via stderr; Java detects startup subscription failure |
| Fluss unavailable | Bound pending data; retry only under tested policy; not ready |
| Append timeout/unknown | Record uncertainty; do not claim lossless delivery |
| Clock offset violation | Not ready; alert |
| Forced shutdown | Record abnormal loss/uncertainty evidence |

### Shutdown

Stop new reads/subscriptions, drain accepted pending writes for a configured deadline, persist uncertainty counters, close clients, and report drain result. Restart increments the assigned connection epoch and revalidates the manifest/subscriptions.

### Telemetry

Metrics include packet/byte rate, append acknowledgement latency, pending bytes, reconnects, active/subscribed connections, manifest version, decode/quarantine reasons, fingerprint count, discontinuities, clock offset, readiness, and acknowledged loss.

Logs include service, instance, connection scope, decoder/protocol version, manifest version, and event/correlation IDs. Raw packets and credentials are never logged.

### Required tests

- `ING-UNIT-001` golden packet decode.
- `ING-UNIT-002` unknown version quarantine.
- `ING-UNIT-003` byte/hash round trip.
- `ING-UNIT-004` normalization and validity classification.
- `ING-UNIT-005` fingerprint canonicalization and fixtures.
- `ING-INT-001` manifest load/subscription completeness.
- `ING-INT-002` Fluss append and acknowledgement timestamps.
- `ING-INT-003` no application batching: one append per accepted tick; no batch >1 record.
- `ING-FAIL-001` reconnect/resubscribe/epoch.
- `ING-FAIL-002` bounded append backpressure (80% warning, 100% critical halt, no unrecorded drop).
- `ING-FAIL-003` forced shutdown and uncertainty accounting.
- `ING-PERF-001` variable 60,000 ticks/s average-baseline full session; broker_receive_to_fluss_ack p99 <5 ms.
- `ING-PERF-002` 90,000 ticks/s peak with every instrument ≤30 ticks/s; bounded append backlog/memory and no acknowledged loss.

### Implementation checklist (from [`01_plan.md`](./01-foundation.md) Task 2)

Before code is accepted, verify each item:

1. Parse and validate every ingestion configuration key in the constants table above before connecting to Arrow or Fluss.
2. Decode and validate one broker tick, then submit that single tick immediately to the Fluss writer.
3. No time-based or record-count-based application batch exists.
4. Pending append counters exist in both records and bytes.
5. Before accepting a tick, reject it when accepting would exceed either pending limit.
6. At 80% of either pending limit: set readiness false, emit warning event containing current records, current bytes, and both limits.
7. At 100% of either pending limit: stop broker reads/subscriptions, keep readiness false, emit critical event, preserve acknowledged-loss/uncertainty record. Do not silently discard data.
8. Decrease pending counters only after append completes, whether successful or failed.
9. Record receive time, append-start time, append-acknowledgement time, append outcome, record size, and error class for every append outcome.
10. Do not compress Arrow payloads in the ingestion-to-Fluss path.

#### Acceptance checks

- At the variable 60,000 ticks/s average baseline and 90,000 ticks/s peak (3,000 instruments; every instrument ≤30 ticks/s), one append submission per accepted input tick; no application batch contains more than one record. The current testing phase uses the 1,024-instrument manifest on one connection; the 3,000-instrument / 3-connection envelope is the deferred target.
- `broker_receive_to_fluss_ack_p99_ms < 5` over a 30-minute 3,000-instrument production-manifest test.
- Simulated slow Fluss writer reaches 80% warning condition before exceeding a pending limit.
- Simulated unavailable Fluss writer reaches 100% condition without exceeding either limit and without an unrecorded drop.

### Definition of done

Implementation is complete only when the broker corpus and versions are pinned, the go-arrow SDK version is pinned in `go.mod`, all required fields match the validated DDL, all configuration constants in section 2 of [`01_plan.md`](./01-foundation.md) are validated at startup and reject incorrect values, tests pass, memory remains bounded with tiered 80%/100% response, no application batching exists, readiness reflects partial subscriptions/append uncertainty, the NDJSON schema is versioned and documented, the Go bridge binary builds from pinned go-arrow dependency, and no active documentation refers to Kite, `seq_no`, or exact missing sequence ranges without approved evidence.

## Verification mapping

The required behavior above is verified by the canonical [Ingestion test design](./11-testing-and-release.md#ingestion): `ING-UNIT-001` to `ING-UNIT-007`, `ING-INT-001` to `ING-INT-003`, `BROKER-MD-001`, `ING-FAIL-001` to `ING-FAIL-003`, and `ING-PERF-001` to `ING-PERF-002`.
