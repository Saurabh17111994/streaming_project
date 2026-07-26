# Ingestion Implementation Dossier

<!-- markdownlint-disable MD013 -->

## Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation-ready, broker-evidence blocked |
| Owner | Ingestion Team |
| Requirements | `REQ-ING-001`–`REQ-ING-016` |
| Build contract | `docs/04_contracts/01-ingestion.md` |
| Writes | `raw_table_1`, `suspected_discontinuities`, quarantine evidence |
| Must not own | Candle computation, strategy, ranking, broker orders, fill lifecycle |

## Process boundary

One Java service process owns the path from the approved broker stream through decode, validation, normalization, packet preservation, fingerprint creation, and Fluss append. Do not insert Python, Kafka, ZeroMQ, OpenAlgo, or another transport between decode and append.

## Internal modules

| Module | Responsibility |
| --- | --- |
| `config` | Validate endpoint, secret references, limits, versions, backoff, manifests |
| `connection` | Connection slots, epochs, authentication, reconnect, heartbeat |
| `protocol` | Versioned frame/packet decode; no guessed fields |
| `normalization` | Verified units, timestamps, instrument mapping, validity classification |
| `fingerprint` | Canonical versioned best-effort event fingerprint |
| `writer` | Bounded Fluss append, acknowledgements, retry classification |
| `discontinuity` | Connection/subscription/heartbeat/time-jump evidence |
| `quarantine` | Preserve unsupported/malformed packet evidence |
| `health` | Liveness, readiness, subscription completeness, clock and append health |
| `telemetry` | Structured logs and bounded-cardinality metrics |

## Configuration contract

| Key | Required | Semantics |
| --- | ---: | --- |
| `BROKER_MARKET_DATA_ENDPOINT_TO_BE_VERIFIED` | Yes | Approved endpoint; no default in production |
| `BROKER_MARKET_DATA_SECRET_REF` | Yes | Secret reference, not credential content |
| `BROKER_PROTOCOL_VERSION_TO_BE_PINNED` | Yes | Selects decoder |
| `INGESTION_DECODER_VERSION` | Yes | Included in every row/quarantine event |
| `FLUSS_BOOTSTRAP_SERVERS` | Yes | Pinned environment endpoint |
| `RAW_TABLE_NAME` | Yes | Must equal reconciled schema manifest |
| `INSTRUMENT_MANIFEST_VERSION` | Yes | Approved subscription snapshot |
| `CONNECTION_SLOT_COUNT_TO_BE_VERIFIED` | Yes | Derived from broker evidence/capacity test |
| `SUBSCRIPTION_LIMIT_TO_BE_VERIFIED` | Yes | Per-connection approved limit |
| `RECONNECT_BACKOFF_PROFILE` | Yes | Bounded min/max/jitter profile |
| `MAX_PENDING_APPEND_BYTES` | Yes | Hard process-memory protection |
| `APPEND_TIMEOUT` | Yes | Pinned client classification |
| `FINGERPRINT_VERSION` | Yes | Canonical algorithm version |
| `CLOCK_OFFSET_LIMIT_MS` | Yes | 100 ms unless approved requirement changes |

Missing required configuration makes readiness false. Production never falls back to demo credentials or a guessed endpoint.

## Startup sequence

1. Validate configuration and exact versions.
2. Initialize telemetry without logging secrets.
3. Connect to Fluss and validate required table/schema version.
4. Load exactly one approved instrument manifest snapshot.
5. Validate every active row and routing field.
6. Partition instruments deterministically across connection slots.
7. Establish broker connections and record `connection_id`, `connection_epoch`, and `instance_id`.
8. Subscribe and verify acknowledgement/completeness.
9. Enter READY only after recent successful Fluss append acknowledgement and acceptable clock offset.

## Packet processing algorithm

```text
receive original frame/packet bytes
→ identify protocol version
→ unknown version? preserve and quarantine; affected readiness false
→ decode verified fields
→ resolve instrument manifest row
→ validate event type and required fields
→ normalize verified units and UTC event time
→ classify validity without discarding raw evidence
→ hash original bytes
→ calculate versioned event fingerprint
→ create raw row containing original bytes + typed fields + provenance
→ submit through bounded writer
→ record acknowledgement timestamp or uncertainty
```

Ingestion appends an accepted raw packet even if its fingerprint was seen before. Compute owns bounded logical deduplication.

## Fingerprint contract

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

## Backpressure and memory

- The pending append queue is bounded by bytes and records.
- Retry uses the pinned Fluss client classification.
- No unbounded custom queue is permitted.
- Threshold breach makes readiness false before memory exhaustion.
- A packet may be dropped only under an explicit policy that records acknowledged-loss evidence and blocks readiness; silently dropping is prohibited.
- TCP flow control is not described as lossless without a broker test.

## Failure matrix

| Failure | Required behavior |
| --- | --- |
| Unknown protocol version | Preserve bytes, quarantine, critical alert, affected readiness false |
| Decode failure | Preserve bytes/reason, metric, bounded error policy |
| Missing instrument | Quarantine; do not append keyed raw row |
| Invalid trade values | Append evidence marked invalid; Compute excludes |
| Broker disconnect | Increment epoch, record discontinuity, bounded reconnect/resubscribe |
| Subscription incomplete | Not ready; identify missing instruments |
| Fluss unavailable | Bound pending data; retry only under tested policy; not ready |
| Append timeout/unknown | Record uncertainty; do not claim lossless delivery |
| Clock offset violation | Not ready; alert |
| Forced shutdown | Record abnormal loss/uncertainty evidence |

## Shutdown

Stop new reads/subscriptions, drain accepted pending writes for a configured deadline, persist uncertainty counters, close clients, and report drain result. Restart increments the assigned connection epoch and revalidates the manifest/subscriptions.

## Telemetry

Metrics include packet/byte rate, append acknowledgement latency, pending bytes, reconnects, active/subscribed connections, manifest version, decode/quarantine reasons, fingerprint count, discontinuities, clock offset, readiness, and acknowledged loss.

Logs include service, instance, connection scope, decoder/protocol version, manifest version, and event/correlation IDs. Raw packets and credentials are never logged.

## Required tests

- `ING-UNIT-001` golden packet decode.
- `ING-UNIT-002` unknown version quarantine.
- `ING-UNIT-003` byte/hash round trip.
- `ING-UNIT-004` normalization and validity classification.
- `ING-UNIT-005` fingerprint canonicalization and fixtures.
- `ING-INT-001` manifest load/subscription completeness.
- `ING-INT-002` Fluss append and acknowledgement timestamps.
- `ING-FAIL-001` reconnect/resubscribe/epoch.
- `ING-FAIL-002` bounded append backpressure.
- `ING-FAIL-003` forced shutdown and uncertainty accounting.
- `ING-PERF-001` 75k full session.
- `ING-PERF-002` 112.5k burst.
- `ING-PERF-003` 150k stress.

## Definition of done

Implementation is complete only when the broker corpus and versions are pinned, all required fields match the validated DDL, tests pass, memory remains bounded, readiness reflects partial subscriptions/append uncertainty, and no active documentation refers to Kite, `seq_no`, or exact missing sequence ranges without approved evidence.
