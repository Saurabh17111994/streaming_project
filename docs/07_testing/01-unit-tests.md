# Unit Test Plan

<!-- markdownlint-disable MD013 -->

## Status

Implementation-ready unit-test plan. Executable tests are not yet implemented.

## Scope

Unit tests cover pure deterministic logic without requiring a live Fluss cluster, broker, OpenAlgo, or S3. Use fixed clocks, canonical fixtures, explicit versions, and deterministic IDs.

## Required suites

- **Ingestion:** packet decode, unknown version, normalization, validity, byte/hash preservation, fingerprint canonicalization, manifest validation, CSV field validation.
- **Signal:** event classification, dedup TTL, event-time ordering, candle aggregation, late policy, candidate identity, supersession, score normalization, ranking tie-break, reservation transitions.
- **Action Capture:** postback identity, fingerprint, correlation priority, quarantine reason, lifecycle precedence, terminal-state protection, position quantity/value transitions, projection status transitions.
- **Babysitter:** input validation, strict no-op, future action flag fail-closed, freshness classification.
- **Executor:** instruction hash/immutability, expiry/reservation, gate transitions, epoch checks, attempt phase transitions, result classification, client reference, fencing decision, approval identity/epoch/evidence validation, audit redaction.
- **Schema/config:** placeholder rejection, version compatibility classification, DDL manifest/checksum, non-null routing-key validation.

## Required edge cases

- Empty, malformed, unknown-version, missing, duplicate, identical-legitimate, delayed, out-of-order, stale, conflicting, terminal-regressive, maximum-size, repeated, and concurrent inputs.
- Clock skew and timestamp unit mismatch.
- Missing optional broker fields versus missing required fields.
- Same immutable ID with equal and changed content.
- Same gate epoch with matching and mismatched evidence hashes.

## Acceptance

Each suite has named test IDs linked to requirements and the implementation test catalog. A unit suite is not release evidence for external protocol, Fluss connector, checkpoint, crash-window, performance, or HA behavior.
