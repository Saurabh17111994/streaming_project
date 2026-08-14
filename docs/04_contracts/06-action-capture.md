# Segment Build Contract — Action Capture

## Boundary

Action Capture consumes Arrow postbacks from the order-updates WebSocket (`wss://order-updates.arrow.trade`), preserves original payload evidence, assigns platform postback identity/fingerprint, correlates to Executor mappings, appends immutable audit, and materializes order and position state. The position projector runs in-process within Action Capture (not a separate deployment).

No broker `postback_seq` or overloaded `order_id` is assumed.

## Writes

1. `Fills` immutable postback/fill audit.
2. `Order_Lifecycle` KV keyed by `broker_order_id`.
3. `Positions` KV keyed by `position_id` and linked by `trade_context_id`.
4. `Postback_Quarantine` for unknown schema/status or missing/ambiguous correlation.
5. `Postback_Projection_Ledger` KV keyed by `postback_event_id` (durable projection status — DDL `17_postback_projection_ledger.sql`, live in dev 2026-08-13).

## Ordering and identity

State updates use source event/version/timestamp plus explicit status precedence. Older events cannot regress terminal state. Conflict moves state to `UNKNOWN`, halts affected order flow, and alerts.

Correlation accepts verified broker ID mapping, verified echoed client reference, or evidence-approved reconciliation query. Symbol/quantity/time proximity is never sufficient.

## Position arithmetic and precedence

The position projector SHALL define account/instrument/side uniqueness, position minting, scale-in, scale-out, re-entry (new `position_id` after closure), correction/bust handling, quantity underflow, price precision, rounding, and fee treatment. An impossible-fill event quarantines and halts affected flows.

When broker sequence/version evidence is unavailable, lifecycle projection SHALL use an explicit tested combination of cumulative quantities, terminal-state precedence, verified broker event time when available, platform receive time as non-authoritative evidence, and conflict handling. Synthetic ordering SHALL not be described as broker ordering. Conflicting evidence moves the order to `UNKNOWN`, quarantines the event, and halts affected order flow.

## Acceptance

Duplicate/out-of-order/no-sequence events, independent-write crash windows, missing mapping, quarantine, terminal-state regression, projection replay, first/partial/multiple fills, position creation/closure, rejected/cancelled/unknown status, and seven-year audit reconstruction tests pass.

## Requirement traceability

- Functional: `REQ-AC-001` through `REQ-AC-013`
- Cross-cutting: `03-non-functional.md` §§3.3–3.8; `04-data.md` §§4.2–4.4, 4.6–4.7; `05-interfaces.md` §§5.5, 5.10–5.11; `06-operational.md` §§6.3–6.8, 6.10

See `../02_requirements/02-functional/06-action-capture.md`.
