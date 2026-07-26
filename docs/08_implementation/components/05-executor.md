# Executor Safety Boundary Implementation Dossier

<!-- markdownlint-disable MD013 -->

## Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation-ready, broker/API/fencing evidence blocked |
| Owner | Execution Team |
| Requirements | `REQ-EXE-001`–`REQ-EXE-010` |
| Contract | `docs/04_contracts/07-executor.md` |
| Default | `HALTED`; broker calls disabled |
| Sole side effect | Money-moving OpenAlgo call, only after all gates |

## Process boundary

The Executor consumes immutable `Trade_Decisions` and future structured `Position_Actions`. It owns and writes `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and `Execution_Audit`. It does not mutate strategy/candidate/ranking fields and does not capture authoritative fills.

OpenAlgo is a broker REST adapter. It does not consume Fluss or decide execution safety.

## Internal modules

| Module | Responsibility |
| --- | --- |
| `decision-consumer` | Changelog tail, schema/version, continuity, duplicate intake |
| `validator` | Immutability, expiry, reservation, action/config compatibility |
| `gate-store` | Epoch/state CAS, halt reasons, approvals |
| `attempt-store` | Prepare/submit/unknown/resolved phases |
| `reference` | Deterministic broker-facing reference after evidence |
| `fencing` | One active owner per account/order partition |
| `broker-adapter` | OpenAlgo request/response boundary; no guessed retry semantics |
| `correlator` | Mapping and verified reconciliation evidence |
| `reconciler` | Broker orders, fills, positions, offsets, unknown attempts |
| `audit` | Immutable execution/safety evidence |
| `readiness` | Durable state, continuity, broker, gate, telemetry health |
| `control` | Authenticated halt/reconcile/approval commands |

## Configuration contract

| Key | Rule |
| --- | --- |
| `OPENALGO_ENDPOINT_TO_BE_VERIFIED` | No unsafe production default |
| `OPENALGO_AUTH_SECRET_REF` | Secret reference only |
| `OPENALGO_REQUEST_SCHEMA_VERSION_TO_BE_VERIFIED` | Approved API contract |
| `OPENALGO_TIMEOUT_PROFILE_TO_BE_VERIFIED` | Timeout and classification |
| `OPENALGO_RETRY_POLICY_TO_BE_VERIFIED` | Unknown outcomes never blind-retried |
| `BROKER_CLIENT_REFERENCE_FORMAT_TO_BE_VERIFIED` | Length/charset/echo evidence |
| `EXECUTOR_ACCOUNT_SCOPE` | Fencing and gate scope |
| `FENCING_LEASE_PROFILE_TO_BE_DEFINED` | Durable single-owner protocol |
| `GATE_INITIAL_STATE` | Must be `HALTED` |
| `GATE_EPOCH_POLICY_VERSION` | Monotonic epoch transitions |
| `AUDIT_SCHEMA_VERSION` | Immutable evidence envelope |
| `DECISION_SCHEMA_VERSION` | Accepted immutable instruction version |
| `MAX_DECISION_LAG_MS` | Stale instruction policy |

## Gate state machine

```text
HALTED
  → RECONCILING
  → APPROVAL_PENDING
  → ENABLED
  → HALTED
```

Only explicit authenticated control operations cause transitions. Invalid transitions are rejected and audited.

Gate record includes scope, state, epoch, reason, detection time, evidence hash, approver identities, approval time, transition time, and schema version.

Every broker call performs an atomic or serialized current-state/epoch check immediately before submission.

## Startup and readiness

1. Load exact version/configuration matrix.
2. Connect to all Executor-owned Fluss state.
3. Verify schema versions and audit append capability.
4. Verify changelog continuity and consumer position.
5. Acquire/fence the account/order partition lease.
6. Start or restore gate as `HALTED` if any state is uncertain.
7. Validate OpenAlgo contract/reachability without placing a live order.
8. Reconcile unknown attempts, broker orders, fills, positions, and reservations.
9. Enter `APPROVAL_PENDING` only after reconciliation passes.
10. Require two distinct authenticated approvals for the same epoch/evidence hash.

Process health never implies trading readiness.

## Instruction intake

For each immutable decision/action:

```text
read event
→ validate schema/version
→ validate canonical identity/content hash
→ validate expiry and freshness
→ validate reservation/capacity
→ check supersession/cancellation
→ check no unresolved attempt/request-hash conflict
→ enqueue only if gate and fencing permit
```

Changed content under an existing immutable identity is a contract violation: quarantine, audit, alert, halt.

## Attempt protocol

```text
no active attempt
→ persist PREPARED attempt + request hash + client reference + gate epoch
→ persist ATTEMPT_PREPARED audit
→ verify gate epoch and fencing lease again
→ persist SUBMITTING
→ call OpenAlgo
→ classify result
```

Result classification:

| Result | State/action |
| --- | --- |
| Verified rejection/no acceptance | Terminal rejection; correlate/audit; release only after policy |
| Verified broker acknowledgement | Persist broker ID mapping and accepted state |
| Timeout/disconnect | `UNKNOWN`, halt, reconcile |
| Malformed/ambiguous response | `UNKNOWN`, halt, reconcile |
| Process crash around call | Recover as `UNKNOWN` unless durable evidence proves outcome |
| Duplicate request conflict | Halt/quarantine; never submit a second request |

Unknown is not rejection and not permission to retry.

## Correlation rules

Mapping is durable across:

```text
instruction_id ↔ execution_attempt_id ↔ client_order_ref ↔ broker_order_id
```

One attempt maps to at most one broker order. One client reference maps to one attempt. One broker order maps to one verified attempt. Violations are ambiguous and safety-relevant.

Postbacks without unique mapping are quarantined and halt affected new order flow.

## Fencing

The active owner lease is scoped to account/order partition. A new owner must fence the prior owner before calls. Lease loss, stale epoch, durable-state loss, or split-brain suspicion immediately prevents calls and halts the gate.

The exact mechanism is `FENCING_LEASE_PROFILE_TO_BE_DEFINED`; implementation must prove the interleaving, not merely use a process-local lock.

## Reconciliation and resume

Resume evidence must cover:

1. Broker open orders and submission history.
2. Open positions and fills.
3. Executor attempts and unknown outcomes.
4. Correlation mappings.
5. Changelog offsets/continuity.
6. Signal job/checkpoint health.
7. Reservations and stale instructions.
8. No unresolved safety invariant.

Two distinct authorized operators approve the same gate epoch/evidence hash. Automatic resume is prohibited.

## Audit envelope

Every money-moving or safety event contains:

- Audit ID/type/schema version.
- All relevant domain identities.
- Gate state/epoch before and after.
- Actor/service/operator identity.
- Executor instance/fencing identity.
- Request/response hashes and redacted evidence.
- Broker status/order ID when known.
- Changelog offset/continuity evidence.
- Configuration/version snapshot.
- Evidence hash and UTC/monotonic timestamps.

Secrets and raw credentials are never stored in logs/audit.

## Readiness and telemetry

Metrics: gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, duplicate suppressions, request conflicts, reconciliation duration/results, mapping/quarantine, approvals, fencing lease, consumer lag, OpenAlgo latency/status, readiness, clock offset, and audit append status.

Readiness is false for missing durable state, unknown gate, lost fencing, changelog gap, schema mismatch, unresolved attempt, broker contract failure, clock violation, or telemetry failure above policy.

## Required tests

- `EXE-UNIT-001` gate transition/epoch validation.
- `EXE-UNIT-002` immutable decision hash/expiry/reservation checks.
- `EXE-UNIT-003` client reference canonicalization.
- `EXE-UNIT-004` result classification.
- `EXE-UNIT-005` approval identity/epoch/evidence rules.
- `EXE-INT-001` Fluss changelog and owned-state writes.
- `EXE-FAIL-001` crash before/during/after broker acceptance.
- `EXE-FAIL-002` timeout/malformed/unknown response.
- `EXE-FAIL-003` restart with missing/corrupt state.
- `EXE-FAIL-004` changelog gap/checkpoint/durable-state loss.
- `EXE-FAIL-005` fencing/split-brain/concurrent Executor.
- `EXE-FAIL-006` mapping/quarantine/reconciliation.
- `EXE-OPS-001` two-person resume and unauthorized controls.
- `EXE-AUDIT-001` seven-year reconstruction simulation.

## Definition of done

The Executor is not complete until the `NotImplementedError` is removed, every state transition is durable/audited, unknown outcomes halt and reconcile, fencing prevents dual ownership, no crash-window test duplicates an order, two-person resume is enforced, and all broker calls remain disabled until the release evidence package approves enablement.
