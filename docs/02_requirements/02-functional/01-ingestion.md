# 02.1 — Ingestion

## Purpose and readiness

Ingestion is the sole market-data entry point. It connects to the evidence-approved broker stream, preserves each original packet losslessly, maps verified fields into normalized typed columns, computes a versioned event fingerprint, and appends to Fluss.

Live-money readiness is blocked until Platform and Execution provide official protocol artifacts or captured packets and compatibility tests pass against the exact deployed decoder version.

## REQ-ING-001: Process boundary

Ingestion SHALL decode and write within one service process and SHALL use the supported Fluss Java client path. It SHALL NOT introduce Kafka, ZeroMQ, Python, or another transport between decode and Fluss.

The decoder MAY use a verified first-party SDK. If no supported SDK exists, it SHALL implement only the documented/captured protocol and pass the golden packet corpus. Unknown packet versions SHALL be quarantined rather than guessed.

**Acceptance:** A compatibility suite decodes all golden packets byte-for-byte and rejects unknown versions without process corruption.

## REQ-ING-002: Evidence-gated broker connection

Endpoint, authentication fields, compression, subscription limits, packet schemas, reconnect behavior, and token refresh semantics SHALL come from approved broker evidence. Current prose values are hypotheses until verified.

Configuration SHALL include endpoint, credential secret references, connection limits, subscription limits, reconnect backoff, protocol version, and decoder version. Secrets SHALL come from local `.env` only in development and Docker Swarm secrets in production.

**Failure behavior:** An unverified or unsupported protocol version sets readiness false, emits a critical alert, and prevents affected packets from entering the typed stream.

## REQ-ING-003: Connection identity

Each logical connection slot SHALL have:

- `connection_id`: stable identity derived from credential principal and slot.
- `connection_epoch`: monotonically increasing local epoch for each reconnect/restart assignment.
- `instance_id`: unique process instance.

No identity is assumed to be broker-global. Reassignment of instruments to another slot creates a new connection scope.

## REQ-ING-004: Instrument manifest

Ingestion SHALL load an explicit, versioned active-instrument manifest from the `instruments` table at startup. Readiness requires:

1. Table reachable.
2. Manifest version recorded.
3. Configured minimum instrument count met.
4. Every row validated for required routing fields.
5. Subscription acknowledgements received for all required instruments.

Runtime instrument changes are deferred; a controlled restart applies a new manifest.

## REQ-ING-005: Packet preservation and typed mapping

For every accepted packet ingestion SHALL persist:

| Field                 | Requirement                                                                               |
| --------------------- | ----------------------------------------------------------------------------------------- |
| `raw_payload`         | Original broker packet bytes, losslessly encoded as BYTES/VARBINARY by the reconciled DDL |
| `payload_hash`        | Cryptographic hash of original bytes for integrity checks; algorithm/version recorded     |
| Typed columns         | Verified 1:1 field mapping and approved unit/time normalization only                      |
| `decoder_version`     | Exact decoder/schema version                                                              |
| `event_fingerprint`   | Versioned bounded duplicate fingerprint                                                   |
| `fingerprint_version` | Algorithm/canonicalization version                                                        |

A canonical decoded JSON representation MAY be emitted as a separate diagnostic field or lake derivation, but SHALL NOT replace original packet bytes.

Unknown fields remain recoverable through `raw_payload`. Unknown packet versions are quarantined with original bytes and reason metadata.

## REQ-ING-006: Timestamp semantics

- `event_time` is the verified broker event timestamp converted to UTC epoch milliseconds.
- `ingest_ts` is the synchronized local time immediately before append submission.
- `append_ack_ts` is the synchronized local time when Fluss acknowledges the append, used for write latency.
- Original timestamp bytes remain in `raw_payload`.

If no verified event timestamp exists, the packet SHALL NOT silently use ingestion time as event time. It is quarantined or marked invalid according to the approved packet contract.

Production hosts SHALL maintain UTC clock offset within 100 ms and expose offset metrics.

## REQ-ING-007: Tick classification

Packet richness and market event type SHALL be modeled separately. A full-depth packet is not automatically a QUOTE event.

A tick contributes to candle aggregation only when the verified payload represents an eligible trade and has valid `event_time`, `instrument_token`, price, and quantity. Quote/depth data remains available for future context but does not contribute to MVP OHLCV.

## REQ-ING-008: Delivery semantics

The WebSocket-to-Fluss boundary is at-least-once. Ingestion SHALL not claim broker or Fluss retry duplicates are eliminated unless the pinned client integration test proves the specific case.

Ingestion SHALL append accepted packets even if their fingerprint has been seen; bounded logical deduplication is owned by Compute. This preserves raw audit fidelity.

## REQ-ING-009: Event fingerprint

Because no stable broker event/sequence ID is assumed, ingestion SHALL calculate a versioned fingerprint from stable normalized values and packet bytes approved by the fingerprint specification. At minimum the specification defines:

- Field order and encoding
- Numeric and timestamp canonicalization
- Hash algorithm
- Included connection scope
- Fingerprint version
- State TTL recommendation
- Collision/identical-legitimate-event limitation

The fingerprint is best-effort. Metrics SHALL distinguish duplicate candidates, dedup hits, and estimated collision risk. Documentation SHALL never call it exact identity.

## REQ-ING-010: Suspected discontinuities

Ingestion SHALL NOT calculate exact missing sequence ranges unless future broker evidence proves a suitable sequence. It MAY create a `suspected_discontinuities` record for:

- Connection interruption
- Heartbeat timeout
- Unsupported packet/version
- Exchange-time jump beyond configured policy
- Subscription loss or negative acknowledgement
- Decoder failure burst

Each record includes `discontinuity_id`, connection scope, affected manifest/segment, detection timestamps, reason code, evidence summary, and status. The live data path continues unless capacity, corruption, or authentication policy requires shutdown. Inline historical backfill is prohibited in MVP.

## REQ-ING-011: Invalid and unknown packets

| Condition                   | Required behavior                                                                 |
| --------------------------- | --------------------------------------------------------------------------------- |
| Unknown protocol version    | Preserve bytes in quarantine; readiness false for affected stream; critical alert |
| Decode failure              | Preserve bytes and reason; increment error counter; apply bounded error policy    |
| Missing instrument identity | Quarantine; do not append to keyed raw table                                      |
| Invalid trade values        | Append raw/typed audit row marked invalid; Compute excludes it                    |
| Authentication exhausted    | Stop affected connection; readiness false; critical alert                         |
| Fluss append uncertainty    | Retry only under pinned client policy; expose uncertainty/loss counters           |

No validly received packet may be silently dropped.

## REQ-ING-012: Backpressure and bounded memory

The exact Fluss client buffering behavior is evidence-gated. Regardless of implementation, ingestion SHALL:

- Bound process memory and pending bytes.
- Expose pending bytes, blocked append duration, timeout count, and dropped/quarantined packet count.
- Stop readiness and alert before uncontrolled memory exhaustion.
- Never describe TCP receive-buffer behavior as lossless flow control without a broker test.

Production acceptance SHALL sustain the defined workload envelope with bounded backlog and no acknowledged packet loss.

## REQ-ING-013: Readiness and health

Liveness proves the process and event loop are responsive. Readiness requires all mandatory broker connections/subscriptions, Fluss connectivity, decoder compatibility, valid instrument manifest, recent successful append acknowledgement, acceptable clock offset, and observability delivery.

Partial subscription is not READY unless an explicitly approved degraded mode identifies missing instruments and disables affected trading decisions.

## REQ-ING-014: Graceful shutdown

Shutdown SHALL stop new subscriptions/reads, drain pending accepted packets for a configured deadline, persist loss/uncertainty counts, close clients, and report whether the drain completed. Forced termination is an abnormal-loss event and SHALL create an operational alert.

## REQ-ING-015: Required metrics and logs

Metrics SHALL include packet/byte throughput, append acknowledgements, write latency percentiles, active/subscribed connections, manifest version, decode/quarantine counts by reason, fingerprint counts, suspected discontinuities, pending bytes, reconnects, clock offset, readiness, and acknowledged-loss count.

Logs SHALL be structured and include service, instance, connection scope, protocol/decoder version, manifest version, and correlation identifiers. Original packet bytes and credentials SHALL never be logged.

## REQ-ING-016: Acceptance gates

1. Golden packet compatibility and unknown-version quarantine tests pass.
2. Original bytes round-trip losslessly and payload hashes match.
3. Typed normalization matches approved fixtures.
4. Reconnect/re-subscription tests prove manifest completeness.
5. Duplicate/fingerprint tests document both false-positive and false-negative limits.
6. Backpressure tests stay within configured memory and backlog bounds.
7. Full-session 75,000 ticks/s, 30-minute 112,500 ticks/s burst, and 60-minute 150,000 ticks/s stress tests complete with defined loss accounting.
8. Credential rotation, exhaustion, shutdown, clock-skew, and observability failure tests pass.

## 
