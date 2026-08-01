# Action Capture

Build this phase, then implement the tests in the second section before moving on.

## What to build


<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation-ready, broker-postback evidence blocked |
| Owner | Action Capture Team; position projection ownership remains explicit |
| Requirements | `REQ-AC-001`–`REQ-AC-013` |
| Contract | `docs/04_contracts/06-action-capture.md` |
| Writes | `Fills`, `Order_Lifecycle`, `Positions`, `Postback_Quarantine` |
| Must not own | Strategy, ranking, order submission, gate approval |

### Processing topology

```text
broker postback delivery
→ protocol decode + original evidence preservation
→ platform postback identity/fingerprint
→ correlation lookup
├─ missing/ambiguous/invalid → immutable quarantine
└─ correlated
   → immutable Fills append
   → Postback_Projection_Ledger record
   → Order_Lifecycle projection
   → Positions projection for fill-bearing events
   → projection completion
```

Writes are independent unless a pinned connector test proves otherwise. Recovery is designed for partial completion.

### Internal modules

| Module | Responsibility |
| --- | --- |
| `postback-adapter` | Evidence-approved stream/webhook, auth, payload capture |
| `decoder` | Versioned payload/status/timestamp decode |
| `identity` | `postback_event_id`, fingerprint, payload hash |
| `correlation` | Mapping lookup and approved reconciliation evidence |
| `audit-writer` | Immutable delivery append |
| `lifecycle-projector` | Per-broker-order state/version transitions |
| `position-projector` | Fill-derived exposure state |
| `projection-ledger` | Pending/completed projection recovery |
| `quarantine` | Immutable ambiguity/invalid evidence |
| `health/telemetry` | Readiness, backlog, lag, transitions, recovery |

### Configuration contract

- `BROKER_POSTBACK_PROTOCOL_TO_BE_PINNED`
- `BROKER_POSTBACK_SECRET_REF`
- `BROKER_STATUS_MAPPING_VERSION_TO_BE_VERIFIED`
- `BROKER_CLIENT_REFERENCE_ECHO_TO_BE_VERIFIED`
- `POSTBACK_FINGERPRINT_VERSION`
- `CORRELATION_POLICY_VERSION`
- `LIFECYCLE_TRANSITION_VERSION`
- `POSITION_PROJECTION_VERSION`
- `MAX_PENDING_PROJECTION_RECORDS` to bound `Postback_Projection_Ledger`
- `MAX_PROJECTION_LAG_MS`
- Fluss table/schema/version identifiers

Missing protocol/status/correlation evidence keeps the service not ready for live money.

### Postback identity

Every received delivery receives a unique platform `postback_event_id`. The fingerprint is a versioned bounded logical duplicate hint; immutable audit may contain repeated deliveries. No `postback_seq` or broker-global event identity is assumed.

### Correlation algorithm

```text
broker_order_id present and uniquely mapped?
  → use verified Order_Correlation mapping
else verified echoed client_order_ref present and unique?
  → resolve to attempt/instruction mapping
else approved broker reconciliation query proves mapping?
  → persist evidence and mapping through Executor-owned protocol
else
  → quarantine + halt affected order flow + alert
```

Symbol, quantity, price, and timestamp proximity are never sufficient.

Action Capture reads Executor mappings but does not mutate Executor-owned mapping state except through an explicit reconciliation command/interface owned by Executor.

### Immutable audit append

One event per delivery contains original payload, hash, decoder/schema version, platform identity/fingerprint, available broker/platform IDs, broker status/quantities/prices, broker/receive/ingest timestamps, and correlation state/reason.

Same `postback_event_id` with different content is a contract violation.

### Projection ledger

Because writes may partially complete, maintain durable projection status keyed by `postback_event_id`:

```text
RECEIVED
→ AUDIT_WRITTEN
→ LIFECYCLE_APPLIED
→ POSITION_APPLIED_OR_NOT_REQUIRED
→ COMPLETE
```

Each step is idempotent. Restart scans non-complete records and resumes from persisted evidence. A duplicate immutable audit row does not authorize duplicate state effect.

The physical location of the ledger must be reconciled into the validated schema before implementation; it cannot be in-memory only.

### Order lifecycle transition protocol

Aggregate key: `broker_order_id`.

Each update includes source event ID, broker event timestamp if verified, receive timestamp, transition version, quantities, prices, status mapping version, and correlated IDs.

Required checks:

1. Exact duplicate source event → no duplicate effect.
2. Older source version → stale evidence metric/audit; no regression.
3. Equal version with conflicting content → `UNKNOWN`, quarantine, halt.
4. Terminal state regression → reject, quarantine, halt.
5. Quantity regression or impossible totals → `UNKNOWN`.
6. Unknown broker status → quarantine and `UNKNOWN`.

The final state vocabulary is derived from the verified broker mapping and normalized to `PENDING`, `PARTIAL`, `FILLED`, `CANCELLED`, `REJECTED`, or `UNKNOWN`.

### Position projection protocol

`position_id` is minted on the first uniquely correlated fill that creates exposure. `trade_context_id` groups related entry/trim/exit/re-entry orders.

Projection records side, opened/closed/current quantity, weighted entry/exit values, state, source event/version, and timestamps.

State vocabulary: `FLAT`, `OPEN`, `REDUCING`, `CLOSED`, `UNKNOWN`.

Order completion is not position closure. Conflicting fills, ambiguous side, quantity underflow, or missing correlation produces `UNKNOWN` and halts affected position actions.

### Reservation interaction

Rejected/cancelled orders release capacity only after unique correlation and terminal reconciliation. Unknown state continues consuming capacity. Action Capture reports lifecycle evidence; the reservation owner applies the release according to the versioned policy.

### Backpressure/readiness

Bound postback intake and projection backlog. Readiness is false for broker disconnect, schema mismatch, Fluss unavailability, backlog above policy, failed projection recovery, correlation invariant failure, or clock violation. Existing accepted evidence must remain recoverable.

### Required telemetry

Postback/byte rate, decode failures, fingerprint duplicates, correlation success/quarantine by reason, lifecycle transitions/rejections, stale/regressive/conflicting events, projection backlog/lag/retries, positions by state, independent-write failures, recovery duration, readiness, and clock offset.

### Required tests

- `AC-UNIT-001` postback decode/status mapping.
- `AC-UNIT-002` platform identity/fingerprint.
- `AC-UNIT-003` correlation priority and ambiguity.
- `AC-UNIT-004` lifecycle precedence/regression.
- `AC-UNIT-005` position quantity/value transitions.
- `AC-INT-001` immutable audit + projections.
- `AC-FAIL-001` crash after each projection step.
- `AC-FAIL-002` restart recovery from ledger.
- `AC-FAIL-003` missing/ambiguous mapping quarantine/halt.
- `AC-FAIL-004` duplicate/out-of-order/conflicting postbacks.
- `AC-REC-001` full projection rebuild from immutable events.

### Definition of done

Implementation is complete when every delivery is auditable, every event is either correlated or quarantined, partial writes recover after restart, stale evidence cannot regress state, position and order aggregates remain separate, unknown state does not release capacity, and no `postback_seq` assumption exists.


## Verification mapping

The required behavior above is verified by the canonical [Action Capture test design](./11-testing-and-release.md#action-capture): `AC-UNIT-001` to `AC-UNIT-005`, `AC-INT-001`, `BROKER-PB-001`, `AC-FAIL-001` to `AC-FAIL-004`, and `AC-REC-001`.
