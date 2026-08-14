# Babysitter

Build this phase, then implement the tests in the second section before moving on.

## What to build


<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation-ready MVP no-op; future action contract deferred |
| Owner | Babysitter Team |
| Requirements | `REQ-BB-001`–`REQ-BB-008` → `AC-BB-001`–`AC-BB-009` |
| Contract | `docs/04_contracts/05-babysitter.md` |
| MVP output | No actions |
| Safety rule | Babysitter never calls broker/Arrow REST directly |

### MVP boundary

The Babysitter is a separate Flink job. It consumes versioned `Positions` state and any required lifecycle freshness evidence, checkpoints observation state, and emits zero actions in MVP.

```text
Positions changelog
→ schema/version validation
→ position freshness/state observation
→ checkpointed no-op decision
→ metrics only
```

A healthy Babysitter must not be interpreted as permission to trade.

### Configuration

- `BABYSITTER_JOB_VERSION`
- `POSITIONS_SCHEMA_VERSION`
- `POSITION_FRESHNESS_POLICY_VERSION`
- `CHECKPOINT_PROFILE_ID`
- `POSITION_ACTIONS_ENABLED=false` for MVP
- `POSITION_ACTIONS_SCHEMA_VERSION_TO_BE_DEFINED` for future phases

Any value enabling actions in the MVP build must fail startup.

### Observation state

Babysitter state SHALL contain only:

- Latest accepted position version per `position_id`.
- Last consumed source offset.
- Freshness timestamp.
- Schema version.
- No-op reason counters.

Babysitter state SHALL NOT contain:

- Historical position snapshots.
- Market ticks.
- Candles.
- Candidates.
- Strategy state.

No strategy or execution-owned state is mutated. A test with multiple updates for one position SHALL retain only the latest version in Babysitter state.

### Future action boundary

After explicit approval, `Position_Actions` will be an immutable versioned event containing:

- `action_id`
- `position_id`
- `trade_context_id`
- Action type and side
- Quantity and optional price
- Source state/version
- Reason and configuration version
- Created/expiry timestamps
- Supersession/correlation metadata

Future actions pass through the same Executor gate, attempt, correlation, fencing, and reconciliation protocol as entry instructions. Free-form strings are prohibited.

### Failure behavior

| Failure | MVP behavior |
| --- | --- |
| Missing Positions schema | Job not ready; no output |
| Changelog gap | Job degraded; no action; alert Executor/operations |
| Checkpoint failure | Job not ready; no action |
| Stale/conflicting position | Observe/audit; no action |
| Restart with unknown state | Restore or remain not ready; no action |
| `POSITION_ACTIONS_ENABLED=true` in MVP | Fail closed at startup |

### Implementation checklist (from [`01_plan.md`](./01-foundation.md) Task 6)

Before code is accepted, verify each item:

1. Store only: latest accepted position version, last consumed source offset, freshness timestamp, schema version, and no-op reason counters.
2. Do not store: historical position snapshots, market ticks, candles, candidates, or strategy state.
3. `POSITION_ACTIONS_ENABLED` is hard-coded `false` for MVP; startup fails if any environment variable tries to set it to `true`.
4. Emit zero `Position_Actions` records for every input.

#### Acceptance checks

- Test with multiple updates for one position retains only the latest version in Babysitter state.
- Every valid input produces zero position actions.
- Startup fails when `POSITION_ACTIONS_ENABLED=true`.

### Telemetry

Consumed position events, latest offset, state freshness, stale/conflicting positions, checkpoint health, restart count, no-op decisions by reason, readiness, and action-enabled guard status.

### Required tests

- `BAB-UNIT-001` no-op output for every valid position state.
- `BAB-UNIT-002` action feature flag fails closed in MVP.
- `BAB-INT-001` Positions changelog schema/offset handling.
- `BAB-FAIL-001` checkpoint restore and changelog gap.
- `BAB-FAIL-002` stale/conflicting position suppression.
- `BAB-OPS-001` readiness never implies trading readiness.

### Definition of done

The separate job starts, validates its input, checkpoints, emits no actions under all MVP inputs, fails closed when future action configuration is accidentally enabled, and exposes health distinct from Executor trading readiness.


## Verification mapping

The required behavior above is verified by the canonical [Babysitter test design](./11-testing-and-release.md#babysitter): `BAB-UNIT-001`, `BAB-UNIT-002`, `BAB-INT-001`, `BAB-HARNESS-001`, `BAB-FAIL-001`, `BAB-FAIL-002`, and `BAB-OPS-001`.
