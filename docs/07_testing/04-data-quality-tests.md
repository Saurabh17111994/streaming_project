# Data Quality and Replay Test Plan

<!-- markdownlint-disable MD013 -->

## Status

Implementation-ready data-quality plan. Executable fixtures and reports are not yet present.

## Required quality checks

- Raw packet byte/hash preservation.
- Decoder/schema version and unknown-version quarantine.
- Instrument manifest completeness, types, active state, and checksum.
- Event timestamp validity, UTC conversion, clock offset, and missing-time quarantine.
- Fingerprint canonicalization, duplicate candidates, dedup hits, and identical-legitimate-event limitation.
- Trade/quote/depth classification and invalid-value handling.
- Candle OHLCV/tick-count correctness, deterministic ties, empty windows, and late discard.
- Candidate/ranking/decision identity, score provenance, reservation consistency, and immutable-content hashes.
- Postback correlation, lifecycle status/quantity/value, position derivation, quarantine, and conflict/UNKNOWN behavior.
- Attempt/request hash/client-reference/mapping consistency.
- Audit completeness, redaction, manifest/checksum, and reconstruction.

## Replay classes

| Replay | Expected result |
| --- | --- |
| Same raw packets, same versions/config | Identical accepted state/output hashes |
| Raw duplicate delivery | One compute effect; raw audit remains at-least-once |
| Same fingerprint but legitimate identical event | Bounded documented collapse possible; metric emitted |
| Changed decoder/schema version | Reject or explicit compatibility path; never silent reinterpretation |
| Reordered input within lateness | Deterministic final candle/ranking result |
| Replayed postback | Immutable duplicate evidence; idempotent projection |
| Replayed decision | No second active attempt for same request hash |
| Changed immutable content | Contract violation, quarantine, safety halt where relevant |

## Data-quality acceptance

Every fixture has a version/checksum and expected output. Any count/hash mismatch is investigated; “close enough” is not accepted for immutable/audit paths. Replay results include source range, schema/config versions, output hashes, duplicate/late counts, and unresolved evidence.
