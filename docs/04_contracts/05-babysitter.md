# Segment Build Contract — Babysitter

> **RE-SCOPED 2026-08-18 (CHG-028, DEC-041):** Babysitter remains the position-observation no-op
> of the integrated Execution Core. It consumes the Fluss `Positions` changelog (materialized by
> the Execution Core's projection sinks from the Nautilus position engine), validates
> continuity/schema, checkpoints observation state, emits metrics, and produces zero actions.
> The MVP no-op boundary and fail-closed rules are unchanged.

## MVP boundary

Babysitter is a strict no-op in MVP. It consumes the versioned `Positions` changelog (projected
into Fluss from Nautilus position events by the Execution Core), validates continuity/schema,
checkpoints observation state to S3, emits metrics, and produces zero actions for every input.

Order lifecycle is not position lifecycle. Babysitter does not read/write a generic
trade-management row and never calls Arrow — only the go-arrow bridge can reach Arrow, and
Babysitter is not a broker caller.

## Future boundary

After a separate Phase 4.3+ approval, and only after the position model, current-price source,
strategy rules, structured schema, Execution Core integration, and failure tests are approved,
Babysitter may append immutable structured `Position_Actions` with `action_id`, `position_id`,
`trade_context_id`, typed action, quantity/price, source state/version, expiry, reason, and
configuration version. Free-form command strings are prohibited.

Future actions pass through the same Execution Core gate, attempt, mapping, and reconciliation
path as entry instructions (Nautilus OMS commanded via the go-arrow bridge).

## Failure behavior

Schema mismatch, stale/unknown position state, checkpoint/source discontinuity, or gate
uncertainty makes Babysitter not ready and suppresses all future action output.
`POSITION_ACTIONS_ENABLED` SHALL fail closed at startup for any value other than `false`.

## Acceptance

Two-job deployment, input/schema handling, S3 checkpoint restore, changelog discontinuity,
backpressure, stale-state behavior, and zero-output tests pass. Any MVP action is a
release-blocking failure.

## Requirement traceability

- Functional: `REQ-BB-001` through `REQ-BB-008`
- Cross-cutting: `03-non-functional.md` §§3.2–3.5, 3.8; `04-data.md` §§4.2–4.4; `05-interfaces.md` §5.6 and §5.11; `06-operational.md` §§6.2–6.5, 6.10
- Implementation: `../08_implementation/05-execution-core.md` (integrated Execution Core dossier)

See `../02_requirements/02-functional/05-babysitter.md`.
