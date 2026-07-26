# Segment Build Contract — Action Capture

## Boundary

Action Capture consumes evidence-approved broker postbacks independently of OpenAlgo, preserves original payload evidence, assigns platform postback identity/fingerprint, correlates to Executor mappings, appends immutable audit, and materializes order and position state.

No broker `postback_seq` or overloaded `order_id` is assumed.

## Writes

1. `Fills_table` immutable postback/fill audit.
2. `Order_Lifecycle` KV keyed by `broker_order_id`.
3. `Positions` KV keyed by `position_id` and linked by `trade_context_id`.
4. `Postback_Quarantine` for unknown schema/status or missing/ambiguous correlation.

Writes are independent unless a pinned connector test proves otherwise. A durable projection status/reconciliation loop retries incomplete materializations after restart.

## Ordering and identity

State updates use source event/version/timestamp plus explicit status precedence. Older events cannot regress terminal state. Conflict moves state to `UNKNOWN`, halts affected order flow, and alerts.

Correlation accepts verified broker ID mapping, verified echoed client reference, or evidence-approved reconciliation query. Symbol/quantity/time proximity is never sufficient.

## Acceptance

Duplicate/out-of-order/no-sequence events, independent-write crash windows, missing mapping, quarantine, terminal-state regression, projection replay, first/partial/multiple fills, position creation/closure, rejected/cancelled/unknown status, and seven-year audit reconstruction tests pass.

## Requirement traceability

- Functional: `REQ-AC-001` through `REQ-AC-010`
- Cross-cutting: `03-non-functional.md` §§3.3–3.8; `04-data.md` §§4.2–4.4, 4.6–4.7; `05-interfaces.md` §§5.5, 5.10–5.11; `06-operational.md` §§6.3–6.8, 6.10

See `../02_requirements/02-functional/06-action-capture.md`.
