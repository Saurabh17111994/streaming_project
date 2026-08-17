# 02.6 — Action Capture and Position Projection (capture path of the Execution Core)

> **RE-SCOPED 2026-08-18 (CHG-028, DEC-041):** the capture path is part of the integrated
> Execution Core. The go-arrow bridge consumes the broker postback stream (wrapping the pinned
> `go-arrow` SDK order-updates WebSocket) and re-publishes normalized events; the Nautilus OMS
> adopts them as external order events; the Nautilus position engine drives fill-derived
> positions; custom projection sinks materialize immutable audit, order/position state, and
> quarantine in Fluss. Postback evidence, identity, correlation, quarantine, and ledger
> requirements are unchanged.

## Purpose and readiness

The capture path independently consumes the evidence-approved broker postback stream through the
go-arrow bridge, preserves immutable postback evidence, correlates broker events to execution
mappings, updates order-lifecycle state without regression, quarantines ambiguity, and drives a
separate fill-derived position projection (Nautilus position engine).

Broker fields, ordering, replay, timestamps, and reference echo behavior are evidence-gated. No
`postback_seq` or broker event ID is assumed.

## REQ-AC-001: Evidence-gated postback intake

Platform and Execution teams SHALL provide official artifacts or sandbox captures proving
endpoint, authentication, payload schema, status vocabulary, timestamp semantics, replay
behavior, and available identities. Unknown schema/status versions are preserved and quarantined;
they are not guessed. The go-arrow SDK decode path and bridge normalization SHALL be
compatibility-tested against those artifacts.

Original postback bytes/text and a payload hash SHALL be retained in immutable audit storage with
decoder/schema version.

## REQ-AC-002: Postback identity

Each received postback SHALL receive a platform `postback_event_id` and versioned
`postback_fingerprint`. If the broker later proves a stable event/sequence ID, it may be stored
as evidence but does not replace platform identity without a new contract.

Duplicate handling is bounded and best-effort. Immutable audit may contain repeated deliveries;
logical projections SHALL be idempotent under the tested fingerprint/version and state-transition
rules (Nautilus fill dedup + custom projection idempotency).

## REQ-AC-003: Correlation

The capture path SHALL correlate in this order:

1. Verified `broker_order_id` mapping in `Order_Correlation`.
2. Verified echoed `client_order_ref` mapped to one `instruction_id`/attempt.
3. Evidence-approved reconciliation query.

It SHALL never infer a mapping from symbol, quantity, timestamp proximity, or overloaded
`order_id` alone. Missing/ambiguous correlation writes `Postback_Quarantine`, halts affected new
order flow, and alerts the Execution Core/operations.

## REQ-AC-004: Immutable postback/fill audit

`Fills` (or its reconciled replacement name) SHALL append one immutable platform event per
received delivery containing:

- `postback_event_id` and fingerprint/version
- `instruction_id`, `client_order_ref`, `broker_order_id`, and `execution_attempt_id` when
  correlated
- `trade_context_id` and `position_id` when available
- Broker status and verified quantities/prices
- Broker event timestamp when verified, receive/ingest timestamps
- Original payload and payload hash
- Schema/decoder version
- Correlation state and quarantine reason

No LOG-table PK uniqueness is claimed. Logical duplicates are explicit and auditable.

## REQ-AC-005: Order lifecycle projection

The capture path owns `Order_Lifecycle` KV keyed by `broker_order_id` and containing correlated
platform IDs. States include at minimum `PENDING`, `PARTIAL`, `FILLED`, `CANCELLED`, `REJECTED`,
and `UNKNOWN`, subject to broker status mapping evidence. The Nautilus OMS provides the order
state machine; the custom layer normalizes Nautilus order events to this vocabulary and owns the
Fluss projection.

Each projection update carries source event identity, source timestamp, source receive timestamp,
state version, cumulative quantity, average fill price, and pending quantity when verified.

An older or lower-precedence postback SHALL not regress a terminal or more advanced state. The
exact precedence/version rule is explicit and tested. Conflicting evidence moves the order to
`UNKNOWN`, quarantines the event, and halts affected order flow.

## REQ-AC-006: Independent-write consistency

Immutable audit append, lifecycle projection, position projection, and quarantine writes are
independent unless a pinned connector test proves a transaction boundary. Recovery SHALL
therefore use a durable projection/reconciliation protocol:

1. Persist immutable event or a durable pending record.
2. Apply lifecycle projection idempotently.
3. Apply position projection idempotently for fill-bearing events.
4. Mark projection completion with source event/version.
5. Scan and retry incomplete projections after restart.

Partial completion, duplicate LOG rows, and projection lag are observable. No cross-table
atomicity claim is permitted without evidence.

## REQ-AC-007: Position projection

A fill-derived projector (Nautilus position engine) SHALL maintain `Positions` KV keyed by
`position_id`. `trade_context_id` groups related orders. `position_id` is minted when the first
correlated fill opens exposure.

The projection records instrument, side, open/closed/cumulative quantities, average entry/exit
values, `FLAT|OPEN|REDUCING|CLOSED|UNKNOWN` state, source event/version, and timestamps.
Conflicting or uncorrelated fills produce `UNKNOWN` and halt affected actions.

Order lifecycle and position lifecycle remain separate aggregates.

## REQ-AC-008: Rejected/cancelled/unknown behavior

Rejected and cancelled broker orders are recorded in immutable audit and lifecycle state.
(Pre-2026-08-15 they also participated in reservation release — REMOVED CHG-005.) Unknown/
unrecognized states remain `UNKNOWN`; they do not silently release risk capacity.

## REQ-AC-009: Backpressure and readiness

Exact Fluss client internals are evidence-gated. The capture path SHALL bound memory and pending
writes, expose projection lag and incomplete work, and become not ready on broker disconnection,
bridge unavailability, schema mismatch, Fluss unavailability, unresolved projection backlog above
policy, or correlation invariant failure.

## REQ-AC-011: Durable projection ledger

The capture path SHALL persist a durable projection ledger keyed by `postback_event_id`, or an
explicitly approved equivalent inbox/outbox. The ledger SHALL record:

- `RECEIVED`
- `AUDIT_WRITTEN`
- `LIFECYCLE_APPLIED`
- `POSITION_APPLIED_OR_NOT_REQUIRED`
- `COMPLETE`
- `UNKNOWN`

Each transition includes source version, expected prior state, timestamps, retry count, next
retry time, error/disposition, and schema version. Restart SHALL scan non-complete records and
resume idempotently. A duplicate immutable audit row SHALL not create a duplicate lifecycle or
position effect.

## REQ-AC-012: Position and lifecycle arithmetic

The position projector SHALL define account/instrument/side uniqueness, position minting,
scale-in, scale-out, re-entry, closure, correction/bust, quantity underflow, price precision,
rounding, fee treatment, and impossible-fill behavior. A re-entry after a closed position SHALL
use a new `position_id` unless an approved versioned rule states otherwise. (Nautilus position
engine provides the arithmetic; the custom layer pins the `UNKNOWN`/quarantine policy.)

## REQ-AC-013: Evidence-based lifecycle precedence

When broker sequence/version evidence is unavailable, lifecycle projection SHALL use an explicit
tested combination of cumulative quantities, terminal-state precedence, verified broker event
time when available, platform receive time as non-authoritative evidence, and conflict handling.
Synthetic ordering SHALL not be described as broker ordering.

## REQ-AC-010: Observability and acceptance

Metrics include postbacks/bytes, decode failures, duplicates, correlation success/quarantine,
lifecycle transitions/rejections, stale/regressive events, projection lag/backlog, positions by
state, independent-write failures, ledger states, replay/recovery, readiness, bridge connection
state, and clock offset.

Tests SHALL cover duplicate and out-of-order postbacks, no-sequence behavior, missing/ambiguous
references, terminal-state regression, independent-write crash windows, restart projection
recovery, rejected/cancelled/unknown handling, first-fill position creation, partial fills,
multi-order trade contexts, position arithmetic/correction behavior, and long-term audit
reconstruction. Bridge order-updates decode and Nautilus external-order adoption require
dedicated adapter tests (canonical IDs in `11-testing-and-release.md` §Action Capture).
