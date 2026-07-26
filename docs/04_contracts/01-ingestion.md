# Segment Build Contract — Ingestion

## Boundary

One service process consumes the evidence-approved broker stream, preserves original packet bytes, maps approved fields into typed columns, calculates a versioned bounded fingerprint, and appends to `raw_table_1` through the supported Fluss Java client.

## Inputs

- Versioned instrument manifest
- Broker endpoint/auth/protocol artifacts approved by Platform and Execution
- Swarm secret references in production
- Exact decoder and Fluss client versions

## Outputs

- `raw_table_1`: original bytes, payload hash, decoder/protocol version, typed fields, timestamps, fingerprint/version, validity state
- `suspected_discontinuities`: connection/subscription/heartbeat/decoder evidence; never fabricated sequence ranges
- Quarantine for unknown versions, decode failures, and missing instrument identity

## Guarantees

- Broker-to-Fluss is at-least-once.
- Raw events are not deduplicated at ingestion.
- No broker sequence/event ID is assumed.
- Original payload bytes are never replaced by canonical JSON.
- Memory/backlog are bounded; exact client internals remain version-gated.

## Failure behavior

Unsupported protocol, incomplete subscription, auth exhaustion, append uncertainty beyond policy, or quarantine bursts set readiness false and alert. Live processing never performs inline history backfill.

## Acceptance

Golden packet decoding, byte round-trip/hash, typed normalization, reconnect/subscription completeness, fingerprint limitations, bounded backpressure, credential rotation, and the 75k/112.5k/150k workload tests must pass.

## Requirement traceability

- Functional: `REQ-ING-001` through `REQ-ING-016`
- Cross-cutting: `03-non-functional.md` §§3.1–3.3, 3.6–3.8; `04-data.md` §§4.1–4.6; `05-interfaces.md` §§5.1 and 5.11; `06-operational.md` §§6.2–6.3, 6.5, 6.8–6.10

See `../02_requirements/02-functional/01-ingestion.md`.
