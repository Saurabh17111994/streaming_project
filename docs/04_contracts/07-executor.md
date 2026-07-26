# Segment Build Contract — Executor

## Boundary

Executor is the only component allowed to initiate OpenAlgo money-moving calls. OpenAlgo is a broker REST adapter and does not consume Fluss or own the safety decision.

## Inputs and owned state

Inputs: immutable `Trade_Decisions`; future immutable `Position_Actions`; lifecycle/position/changelog health for validation.

Owned Fluss state: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and immutable `Execution_Audit`. Executor never mutates strategy fields.

## Gate

Default/restart-uncertain state is `HALTED`. States are `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED`, with `ENABLED → HALTED` on uncertainty. Every call validates current gate epoch. Halt blocks calls within five seconds.

Resume requires broker/order, position/fill, offsets/continuity, Signal checkpoint, and unknown-attempt reconciliation, followed by two distinct authorized approvals of the same evidence hash/epoch.

## Attempt protocol

Persist `PREPARED` attempt, immutable request hash, client reference, and gate epoch before calling OpenAlgo. Timeout, disconnect, malformed response, crash window, or ambiguous response produces `UNKNOWN`, halts, and forbids automatic retry until broker non-acceptance or verified idempotency is proven.

## Concurrency

One fenced active Executor owns each account/order partition. Leadership or durable-state loss halts.

## Acceptance

Crash-window, duplicate, timeout, rejection, malformed response, missing mapping, changelog gap, restart/corrupt state, fencing, unauthorized/mismatched approval, two-person resume, and seven-year reconstruction tests pass. The current `NotImplementedError` scaffold is a live-money blocker.

## Requirement traceability

- Functional: `REQ-EXE-001` through `REQ-EXE-010`
- Cross-cutting: `03-non-functional.md` §§3.1–3.8; `04-data.md` §§4.2–4.4, 4.6–4.7; `05-interfaces.md` §§5.7–5.9, 5.11; `06-operational.md` §§6.2–6.10

See `../02_requirements/02-functional/07-executor.md`.
