# Segment Build Contract — Babysitter

## MVP boundary

Babysitter is the second Flink job and is a strict no-op in MVP. It consumes the versioned `Positions` changelog, validates continuity/schema, checkpoints observation state to S3, emits metrics, and produces zero actions for every input.

Order lifecycle is not position lifecycle. Babysitter does not read/write a generic trade-management row and never calls Arrow REST.

## Future boundary

After a separate Phase 4.3+ approval, and only after the position model, current-price source, strategy rules, structured schema, Executor integration, and failure tests are approved, Babysitter may append immutable structured `Position_Actions` with `action_id`, `position_id`, `trade_context_id`, typed action, quantity/price, source state/version, expiry, reason, and configuration version. Free-form command strings are prohibited.

Executor consumes future actions through the same gate, attempt, mapping, and reconciliation path as entry instructions.

## Failure behavior

Schema mismatch, stale/unknown position state, checkpoint/source discontinuity, or gate uncertainty makes Babysitter not ready and suppresses all future action output.

## Acceptance

Two-job deployment, input/schema handling, S3 checkpoint restore, changelog discontinuity, backpressure, stale-state behavior, and zero-output tests pass. Any MVP action is a release-blocking failure.

## Requirement traceability

- Functional: `REQ-BB-001` through `REQ-BB-008`
- Cross-cutting: `03-non-functional.md` §§3.2–3.5, 3.8; `04-data.md` §§4.2–4.4; `05-interfaces.md` §5.6 and §5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/05-babysitter.md`.
