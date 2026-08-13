# Executor

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation-ready, broker/API/fencing evidence blocked |
| Owner | Execution Team |
| Requirements | `REQ-EXE-001`–`REQ-EXE-013` |
| Contract | `docs/04_contracts/07-executor.md` |
| Default | `HALTED`; broker calls disabled |
| Sole side effect | Money-moving Arrow REST call, only after all gates |

### Process boundary

The Executor consumes immutable `Trade_Decisions`, durable `Safety_Halt_Requests`, and future structured `Position_Actions`. It owns and writes `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and `Execution_Audit`. It does not mutate strategy/candidate/ranking fields and does not capture authoritative fills. It calls Arrow's REST API directly — there is no intermediate broker adapter layer.

### Internal modules

| Module | Responsibility |
| --- | --- |
| `decision-consumer` | Changelog tail, schema/version, continuity, duplicate intake |
| `validator` | Immutability, expiry, reservation, action/config compatibility |
| `gate-store` | Epoch/state CAS, halt reasons, approvals |
| `attempt-store` | Prepare/submit/unknown/resolved phases |
| `reference` | Deterministic broker-facing reference after evidence |
| `safety-halt` | Consume idempotent `Safety_Halt_Requests`, validate scope/source, apply/reject with audit |
| `fencing` | One active owner per `execution_partition_id` with durable epoch/token |
| `broker-adapter` | Arrow REST request/response boundary; no guessed retry semantics |
| `correlator` | Mapping and verified reconciliation evidence |
| `reconciler` | Broker orders, fills, positions, offsets, unknown attempts |
| `audit` | Immutable execution/safety evidence |
| `readiness` | Durable state, continuity, broker, gate, telemetry health |
| `control` | Authenticated halt/reconcile/approval commands |

### Configuration contract

| Key | Rule |
| --- | --- |
| `ARROW_REST_URL_TO_BE_VERIFIED` | No unsafe production default |
| `ARROW_AUTH_SECRET_REF` | Secret reference only |
| `ARROW_REQUEST_SCHEMA_VERSION_TO_BE_VERIFIED` | Approved API contract |
| `ARROW_TIMEOUT_PROFILE_TO_BE_VERIFIED` | Timeout and classification |
| `ARROW_RETRY_POLICY_TO_BE_VERIFIED` | Unknown outcomes never blind-retried |
| `BROKER_CLIENT_REFERENCE_FORMAT_TO_BE_VERIFIED` | Length/charset/echo evidence |
| `EXECUTOR_ACCOUNT_SCOPE` | Fencing and gate scope |
| `FENCING_LEASE_PROFILE_TO_BE_DEFINED` | Durable single-owner protocol |
| `GATE_INITIAL_STATE` | Must be `HALTED` |
| `GATE_EPOCH_POLICY_VERSION` | Monotonic epoch transitions |
| `AUDIT_SCHEMA_VERSION` | Immutable evidence envelope |
| `DECISION_SCHEMA_VERSION` | Accepted immutable instruction version |
| `MAX_DECISION_LAG_MS` | Stale instruction policy |

### Gate state machine

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

### Startup and readiness

1. Load exact version/configuration matrix.
2. Connect to all Executor-owned Fluss state.
3. Verify schema versions and audit append capability.
4. Verify changelog continuity and consumer position.
5. Acquire/fence the `execution_partition_id` lease.
6. Start or restore gate as `HALTED` if any state is uncertain.
7. Validate Arrow REST contract/reachability without placing a live order.
8. Reconcile unknown attempts, broker orders, fills, positions, and reservations.
9. Enter `APPROVAL_PENDING` only after reconciliation passes.
10. Require two distinct authenticated approvals for the same epoch/evidence hash.

Process health never implies trading readiness.

### Instruction intake

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

### Attempt protocol

```text
no active attempt
→ persist PREPARED attempt + request hash + client reference + gate epoch
→ persist ATTEMPT_PREPARED audit
→ verify gate epoch and fencing lease again
→ persist SUBMITTING
→ call Arrow REST
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

### Correlation rules

Mapping is durable across:

```text
instruction_id ↔ execution_attempt_id ↔ client_order_ref ↔ broker_order_id
```

One attempt maps to at most one broker order. One client reference maps to one attempt. One broker order maps to one verified attempt. Violations are ambiguous and safety-relevant.

Postbacks without unique mapping are quarantined and halt affected new order flow.

### Fencing

The active owner lease is scoped to `execution_partition_id`. The owner holds a durable fencing epoch/token. Every attempt stores the gate epoch and fencing token. Immediately before an Arrow REST call, the Executor SHALL re-verify current gate state, gate epoch, fencing ownership/token, durable attempt phase, and required health evidence. Lease loss, token mismatch, storage uncertainty, network partition, or stale ownership prevents the call and moves the affected gate to `HALTED`.

The exact mechanism is `FENCING_LEASE_PROFILE_TO_BE_DEFINED`; implementation must prove the interleaving, not merely use a process-local lock.

### Reconciliation and resume

Live-money enablement requires evidence that Arrow REST/broker reconciliation can query or list recent orders, client references, broker order IDs, fills, and positions with defined consistency delay, pagination, rate limits, authentication, and history horizon. Missing reconciliation evidence blocks retry of unknown attempts and blocks live-money release.

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

### Audit envelope

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

### Readiness and telemetry

Metrics: gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, duplicate suppressions, request conflicts, reconciliation duration/results, mapping/quarantine, approvals, fencing lease, consumer lag, Arrow REST latency/status, readiness, clock offset, and audit append status.

Readiness is false for missing durable state, unknown gate, lost fencing, changelog gap, schema mismatch, unresolved attempt, broker contract failure, clock violation, or telemetry failure above policy.

### Required tests

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

### Definition of done

The Executor is not complete until the `NotImplementedError` is removed, every state transition is durable/audited, unknown outcomes halt and reconcile, fencing prevents dual ownership, no crash-window test duplicates an order, two-person resume is enforced, and all broker calls remain disabled until the release evidence package approves enablement.

## Verification mapping

The required behavior above is verified by the canonical [Executor test design](./11-testing-and-release.md#executor): `EXE-UNIT-001` to `EXE-UNIT-006`, `EXE-INT-001`, `ARROW-REST-001`, `ARROW-REST-002`, `EXE-FAIL-001` to `EXE-FAIL-006`, `EXE-OPS-001`, and `EXE-AUDIT-001`.

---

## Agent-2 independent executor safety core plan (2026-08-12)

### Agent 2 — Independent Executor Safety Core

## Overview

Build an offline, fail-closed Executor Safety Core under
`code/02_services/04_executor`.

This work package is intentionally smaller than the production Executor. It
implements deterministic execution-domain rules that can be developed and
tested without Flink, Fluss, Arrow, a broker account, network access,
credentials, shared Java classes, or Agent 1's Signal Job work.

The result is a reusable Java 17 library with in-memory state adapters and a
safe local Java entry point. It must never submit an order, consume a Fluss
changelog, or claim production readiness. Production adapters remain a later
integration phase owned by the Executor maintainer after the
`Trade_Decisions`, Fluss, fencing, reconciliation, and broker evidence gates
are complete.

### Acceptance criteria

1. `code/02_services/04_executor` is a standalone Java 17 Maven module with
   production sources under `src/main/java/com/trading/executor/core` and
   JUnit 5 tests under `src/test/java/com/trading/executor/core`.
2. The module uses only the Java 17 standard library at runtime. JUnit 5 is
   test scope only. It imports no Fluss client, HTTP client, Arrow SDK, Docker
   API, or network library.
3. A fresh core instance starts in `HALTED` and cannot submit an order in any
   state because no broker adapter is implemented in this work package.
4. Gate transitions, monotonic epochs, two-person approval, safety halts,
   immutable decision hashes, attempt phases, duplicate suppression, result
   classification, reference generation, fencing, and audit records are
   deterministic and covered by tests.
5. Unknown, ambiguous, timeout, malformed, crash-window, stale-epoch,
   duplicate, scope-mismatch, and fencing-loss cases fail closed.
6. The test suite runs offline with:

   ```text
   mvn -f code/02_services/04_executor/pom.xml test
   ```

7. No file outside `code/02_services/04_executor/**` is modified.
8. Live-money behavior remains disabled and the current production scaffold is
   not represented as complete.

## Context

### Existing repository evidence

- The current module is a Python scaffold: `main.py` raises
  `NotImplementedError`, the Dockerfile uses Python, and `requirements.txt`
  contains placeholder dependencies. Task 1 removes this scaffold and
  replaces it with a standalone Java 17 Maven module.
- The module README identifies the future flow as immutable decisions,
  validation, gate check, durable attempt, fencing, broker call, correlation,
  and reconciliation, but explicitly prohibits live money by default.
- The authoritative Executor requirements are in
  `docs/02_requirements/02-functional/07-executor.md`.
- The Executor contract is in `docs/04_contracts/07-executor.md`.
- The implementation dossier is in `docs/08_implementation/07-executor.md`.
- Canonical test IDs are listed in
  `docs/08_implementation/11-testing-and-release.md`, including
  `EXE-UNIT-001` through `EXE-UNIT-006`, `EXE-FAIL-001` through
  `EXE-FAIL-006`, `EXE-OPS-001`, and `EXE-AUDIT-001`.
- The physical Fluss DDLs are Executor-owned but are not touched by this
  package. They are reference-only for future integration:
  `code/01_platform/02_sql/ddl/11_execution_gate.sql`,
  `12_execution_attempts.sql`, `13_order_correlation.sql`,
  `14_execution_audit.sql`, and `18_safety_halt_requests.sql`.
- `code/common/src/main/java/com/trading/common/identity/IdentityModel.java`
  contains shared Java identities, but this isolated module must not import or
  modify them.

### Ownership boundary

Agent 2 owns only:

```text
code/02_services/04_executor/**
```

Agent 1 owns the ingestion and compute work. The following are hard
exclusions from this plan:

```text
code/01_platform/**
code/common/**
code/02_services/01_ingestion/**
code/02_services/02_compute/**
code/02_services/03_action_capture/**
docs/**
Makefile
code/pom.xml
docker-compose*.yml
deploy/**
```

Do not edit the plan, implementation trackers, DDLs, shared Java models,
deployment files, or Agent 1's files. Return the implementation report to the
coordinator instead.

## Review Handoff

- Original request: identify a genuinely independent project component that
  Agent 2 can implement in parallel, then provide a mechanical plan requiring
  no design decisions from Agent 2.
- Selected package: offline Executor Safety Core.
- Independence definition: no runtime connection to an earlier phase, no
  Flink/Fluss dependency, no broker/API call, no shared-source edit, and no
  dependency on a final upstream `Trade_Decisions` producer.
- The core accepts a local immutable `Decision` value object for tests. It does
  not consume the eventual Fluss `Trade_Decisions` table.
- Production integration is deliberately not part of this plan.
- Hidden context: none; this plan is self-contained for a fresh executor.

## Development Approach

- Use Java 17 and the Java standard library only in production sources.
- Use Java records for immutable values, enums for finite states,
  `MessageDigest` for SHA-256, `StandardCharsets.UTF_8`, and immutable
  collection copies (`List.copyOf`, `Map.copyOf`) where needed.
- Use integer Unix epoch milliseconds for every domain timestamp. Do not use
  `Instant`, `LocalDateTime`, or other clock-bearing values inside domain
  records.
- Use JUnit 5 from the parent dependency management. Do not add AssertJ or any
  other test dependency; use `org.junit.jupiter.api.Assertions`.
- Use one flat Java package, `com.trading.executor.core`; do not add
  repository/service/domain layers, dependency-injection frameworks,
  reflection registries, abstract base class hierarchies, or plugin systems.
  The in-memory stores are the concrete implementation for this work package.
- Implement one task at a time. Add tests in the same task as each module.
- Delete `requirements.txt`, `main.py`, the Python Dockerfile, `__pycache__`,
  and `.ruff_cache`; do not retain a second Python implementation.
- Do not use network sockets, subprocesses, file databases, environment
  credentials, Fluss, HTTP, or Arrow in tests.
- Do not make tests depend on wall-clock time, random values, process IDs, or
  map iteration order.
- Use Java records for immutable data. Defensive-copy collection components in
  compact constructors; never expose mutable internal collections.
- Raise typed domain exceptions or return explicit rejected results; never
  silently coerce invalid safety data.

## Technical Contract

### Fixed terminology

Use the following identities exactly. Never introduce a generic `order_id`:

| Identity | Meaning |
| --- | --- |
| `instruction_id` | Immutable upstream execution instruction identity |
| `execution_attempt_id` | One attempted broker submission identity |
| `client_order_ref` | Deterministic broker-facing attempt reference |
| `broker_order_id` | Broker-authoritative order identity; absent in this offline package |
| `account_scope_id` | Account isolation scope |
| `portfolio_id` | Portfolio capacity scope |
| `execution_partition_id` | Fencing and gate ownership scope |
| `reservation_id` | Upstream capacity reservation identity |
| `gate_epoch` | Monotonic execution safety epoch |
| `fencing_epoch` | Monotonic ownership epoch |

### Domain values

Implement these exact enums:

```text
GateState: HALTED, RECONCILING, APPROVAL_PENDING, ENABLED
AttemptPhase: PREPARED, SUBMITTING, ACCEPTED, REJECTED, CANCELLED, UNKNOWN
BrokerResult: VERIFIED_ACCEPTANCE, VERIFIED_REJECTION, TIMEOUT, DISCONNECT,
              MALFORMED, AMBIGUOUS, CRASH_WINDOW
ApprovalDecision: APPROVE, DENY
SafetyScopeResult: APPLIED, DUPLICATE, REJECTED
```

No transition may produce an implicit state outside these values.
Declare all records and enums as nested public types inside a non-instantiable
`DomainModel` class so Task 2 remains one source file and Java's one-public-
top-level-type rule is respected. Other core classes refer to them as
`DomainModel.Decision`, `DomainModel.GateState`, and so on; do not create one
file per record.

### Gate rules

Implement `GateStateMachine` with an immutable `GateRecord` and an append-only
in-memory audit sink.

Allowed transitions are exactly:

```text
HALTED -> RECONCILING
RECONCILING -> APPROVAL_PENDING
APPROVAL_PENDING -> ENABLED
ENABLED -> HALTED
RECONCILING -> HALTED
APPROVAL_PENDING -> HALTED
```

Rules:

- Initial state is `HALTED`, epoch `0`.
- Every accepted transition increments `gate_epoch` by exactly `1`.
- Invalid transitions are rejected without changing state or epoch and emit a
  rejected audit event.
- Every transition requires the expected current epoch. A stale epoch is
  rejected and cannot overwrite state.
- Any explicit safety halt from `ENABLED` increments the epoch and enters
  `HALTED`.
- A valid safety halt received while already `HALTED` is idempotently recorded
  without incrementing the epoch. A valid halt from `RECONCILING` or
  `APPROVAL_PENDING` increments the epoch and returns to `HALTED`.
- Unknown broker outcomes, ambiguous responses, stale health, duplicate
  instruction content, fencing loss, and storage uncertainty all request a
  halt. The core records the halt decision but does not call external systems.
- `ENABLED` is never the initial or automatic-restart state.

### Two-person approval rules

Implement `ApprovalService`:

- Approval is bound to one `execution_partition_id`, one `gate_epoch`, and one
  `evidence_hash`.
- The two approvers must have different nonblank operator IDs.
- Only authorized operator IDs supplied to the service constructor may approve.
- A denial leaves the gate halted and emits an audit event.
- A first valid approval moves `APPROVAL_PENDING` only if the gate is already
  in that state; it records approval 1 and does not change gate state or epoch.
- The second valid approval enables the gate only when operator, epoch, and
  evidence all match.
- Duplicate approval by the same operator is rejected.
- Mismatched epoch or evidence is rejected and cannot be combined with a valid
  approval.
- Approval order is irrelevant; the final state must be deterministic.

### Immutable decision rules

Implement a local frozen `Decision` record with exactly these fields:

```text
instruction_id: str
candidate_id: str
trade_context_id: str
instrument_token: int
exchange: str
symbol: str
side: str
quantity: int
order_type: str
product_type: str
limit_price_paise: int | None
portfolio_id: str
account_scope_id: str
strategy_id: str
strategy_version: str
configuration_version: str
evaluation_id: str
composite_score: float | None
reservation_id: str
reservation_version: str
created_ts: int
expiry_ts: int | None
supersedes_instruction_id: str | None
superseded_by_instruction_id: str | None
schema_version: str
```

Rules:

- Reject blank strings, non-positive `quantity`, non-positive
  `instrument_token`, unsupported `side`, unsupported `order_type`,
  unsupported `product_type`, unsupported `exchange == INDEX`, and invalid
  price combinations.
- Supported exchange values are the uppercase strings `NSE`, `BSE`, `NFO`,
  `BFO`, and `MCX`. Reject every other value, including `INDEX`; do not
  normalize case.
- Supported sides are `BUY` and `SELL`.
- Supported order types for this offline record are `MARKET` and `LIMIT`.
  Reject `SL` and `SLM` until a future schema provides explicit trigger-price
  fields; do not infer trigger behavior from `limit_price_paise`.
- Supported product types are `I`, `C`, and `M`.
- `LIMIT` requires a positive `limit_price_paise`; `MARKET` requires no limit
  price.
- `expiry_ts` is valid only when it is greater than `created_ts`.
- `schema_version` must equal the literal `"2"`. The configured client
  reference format version must equal the literal `"v1"`.
- A decision is executable only when `now_ts < expiry_ts` if expiry exists.
- A reservation must be present, nonblank, and marked `ACTIVE` by the supplied
  local reservation lookup. The lookup is an in-memory mapping, not Fluss.
- Define a frozen local `Reservation` with exactly:
  `reservation_id`, `portfolio_id`, `account_scope_id`, `instruction_id`,
  `reservation_version`, `state`, and `expiry_ts`. Validation requires exact
  ID/version/account/portfolio/instruction matches, `state == "ACTIVE"`, and
  `now_ts < expiry_ts`.
- `ExecutorSafetyCore` is constructed for exactly one nonblank
  `account_scope_id`, `portfolio_id`, and `execution_partition_id`. A decision
  outside the configured account or portfolio is rejected. The local caller
  supplies the partition; no partition is inferred from the instruction.
- Canonical content hashing must include every field in the declared order,
  including explicit nulls, and must serialize in that fixed field order with
  compact JSON separators, UTF-8 encoding, and SHA-256 lowercase hex output.
- Reusing an `instruction_id` with a different content hash is a contract
  violation: reject, request halt, and emit audit evidence.
- Replaying the same `instruction_id` with the same content hash is a duplicate
  and must not create another active attempt.

### Client reference rules

Implement deterministic `client_order_ref` generation:

- Input: `instruction_id`, `execution_attempt_id`, and a configured format
  version string.
- Build the canonical string `format_version|instruction_id|execution_attempt_id`.
- SHA-256 hash the UTF-8 bytes, uppercase the first 13 hexadecimal characters,
  and prefix with `E`.
- The result is exactly 14 ASCII characters and therefore fits the current
  16-character broker remarks constraint.
- Reject an empty format version or empty identity.
- The function must be pure and return the same value on every invocation.

### Attempt rules

Implement `AttemptStore` and immutable `AttemptRecord`:

- `prepare()` creates exactly one `PREPARED` attempt for a new instruction and
  request hash, with `phase_epoch = 0`, `retry_attempt = 0`, the current
  `gate_epoch`, the current fencing epoch/token, deterministic
  `client_order_ref`, and explicit created/updated timestamps supplied by the
  caller.
- A duplicate `(instruction_id, request_hash)` returns the existing attempt
  without creating a second record.
- A different request hash for an existing `instruction_id` raises a contract
  violation and requests a halt.
- Legal phase transitions are:

  ```text
  PREPARED -> SUBMITTING
  SUBMITTING -> ACCEPTED
  SUBMITTING -> REJECTED
  SUBMITTING -> CANCELLED
  SUBMITTING -> UNKNOWN
  UNKNOWN -> ACCEPTED
  UNKNOWN -> REJECTED
  UNKNOWN -> CANCELLED
  ```

- `UNKNOWN` cannot transition directly to `SUBMITTING` and cannot be retried
  automatically.
- Every accepted phase transition increments `phase_epoch` by exactly `1`.
- A stale `phase_epoch` update is rejected without mutation.
- Terminal phases are `ACCEPTED`, `REJECTED`, and `CANCELLED`; terminal records
  cannot transition again.
- `UNKNOWN` is non-terminal and blocks new submissions until an explicit
  reconciliation result resolves it.
- `request_hash` in this offline package is the canonical decision-content
  hash. Future Arrow adapter work may introduce a separate broker-request hash
  only after the final request schema is pinned; do not invent one here.
- No attempt method may call a broker or network function.

### Broker result classification

Implement a pure classifier that accepts a local fake response envelope:

```text
FakeBrokerResponse:
  transport_ok: bool
  http_status: int | None
  body: mapping | None
  timed_out: bool = False
  disconnected: bool = False
  process_crashed: bool = False
```

Classify exactly as follows:

- Apply precedence in this exact order: `process_crashed`, `timed_out`,
  `disconnected`, transport/status validation, then body validation.
  `process_crashed`, `timed_out`, or `disconnected` => corresponding unknown
  result; never rejection, even when the envelope also contains an HTTP body.
- When none of those flags is set, `transport_ok == false` => `AMBIGUOUS`.
- Missing body, non-mapping body, missing status, invalid status, or missing
  accepted `orderNo` => `MALFORMED` or `AMBIGUOUS`; both resolve to `UNKNOWN`.
- The only verified acceptance shape is HTTP `200`, `transport_ok == true`,
  `body["status"] == "success"`, and a nonblank string
  `body["data"]["orderNo"]` => `VERIFIED_ACCEPTANCE`.
- The only verified rejection shape in this offline fixture contract is
  `transport_ok == true`, HTTP `400`, `409`, or `422`,
  `body["status"] == "error"`, and a nonblank string `body["message"]` =>
  `VERIFIED_REJECTION`.
- HTTP `401`, `403`, `408`, `429`, or any `5xx` response is not proof of broker
  non-acceptance and therefore => `AMBIGUOUS`/`UNKNOWN`.
- Any unrecognized combination => `AMBIGUOUS`.
- The classifier never retries and never generates a broker request.

### Safety-halt rules

Implement a local frozen `SafetyHaltRequest` with:

```text
halt_request_id, account_scope_id, portfolio_id,
execution_partition_id, source_component, source_instance,
reason_code, reason_detail, detection_time, source_epoch,
evidence_hash, schema_version
```

Rules:

- `halt_request_id` must be deterministic SHA-256 of the canonical request
  tuple and must be nonblank. The tuple, in this exact order, is
  `account_scope_id|portfolio_id-or-empty|execution_partition_id-or-empty|source_component|source_instance|reason_code|detection_time|source_epoch|evidence_hash|schema_version`.
  A supplied ID that does not equal the lowercase SHA-256 hex digest of this
  UTF-8 tuple is malformed and rejected.
- Duplicate IDs are idempotent and return `DUPLICATE` without incrementing the
  gate epoch twice.
- A cross-scope request is rejected and audited.
- A stale `source_epoch` is rejected and audited.
- Maintain the greatest seen valid `source_epoch` independently for each
  `(source_component, source_instance, execution_partition_id)` tuple. A
  request is stale only when its epoch is less than that stored maximum; the
  same epoch is permitted when its deterministic request ID differs. Update
  this maximum after scope/shape/ID validation even when the gate is already
  halted, so later lower epochs cannot become acceptable.
- An accepted request enters `HALTED`, increments the gate epoch once, and
  records the source/evidence in audit.
- Malformed requests are rejected without state mutation.

### Fencing rules

Implement an in-memory `FenceStore` only to test the protocol:

- `acquire(partition, owner)` increments and returns a fencing epoch/token.
- A new owner invalidates the old token.
- `isCurrent(partition, owner, fencingEpoch, token)` is the only valid
  submission authorization check.
- Owner mismatch, stale epoch, token mismatch, and released ownership all
  return false.
- The core must require a current fence immediately before a simulated
  submission decision. A process-local boolean is not sufficient.
- This is a test protocol, not a production distributed lock. Do not claim
  split-brain protection beyond the deterministic stale-token behavior tested
  here.

### Audit rules

Implement an append-only in-memory `AuditSink` with frozen `AuditEvent` records:

- Each event has a caller-supplied `audit_event_id`, event type,
  partition/account scope, gate epoch, actor, evidence hash, event timestamp,
  schema version, and optional instruction/attempt IDs.
- The sink rejects mutation of existing events and duplicate IDs with different
  content.
- Production-strength audit-ID generation is out of scope. Tests and the core
  use an injected deterministic `Supplier<String> nextAuditId`; defaulting to a
  random UUID or wall-clock-derived ID is prohibited.
- Audit events must be emitted for accepted and rejected gate transitions,
  approvals, denials, duplicate decisions, content conflicts, phase changes,
  safety-halt application/rejection, fencing rejection, and result
  classification.
- Do not store credentials, tokens, raw payloads, or arbitrary response bodies.
- Provide `reconstructAttempt(attemptId)` that returns the ordered immutable
  audit timeline for that attempt.

### Core orchestration

Implement `ExecutorSafetyCore` as the only coordinating class. Its constructor
accepts explicit in-memory dependencies:

```text
gate_machine
approval_service
attempt_store
fence_store
audit_sink
reservation_lookup
next_audit_id
```

It exposes pure/local methods for:

```text
validateDecision(decision, nowTs) -> DecisionValidation
prepareAttempt(decision, executionAttemptId, gateEpoch, fence,
               actorId, nowTs) -> AttemptRecord
authorizeSubmission(attemptId, gateEpoch, fence,
                    actorId, nowTs) -> SubmissionAuthorization
classifyAndRecord(attemptId, fakeResponse,
                  actorId, nowTs) -> ResultDecision
applySafetyHalt(request, actorId, nowTs) -> SafetyScopeResult
approveResume(operatorId, gateEpoch, evidenceHash,
              nowTs) -> GateRecord
```

All coordinating methods accept explicit `actor_id`, `now_ts`, and any new
identity such as `execution_attempt_id`. The core must not call
`System.currentTimeMillis()`, `Instant.now()`, generate a UUID, or mint values
from process state. The signatures above show behavior, not permission to
hide nondeterministic generation.

For operations that emit multiple audit events, append them in this exact
order: validate input; append the domain transition event; append any halt
request/application event; append the final allow/deny or result event. All
events from one operation use the caller-supplied `now_ts`; insertion order is
the tie-breaker when reconstructing equal-timestamp events.

`authorize_submission` returns a value describing whether a simulated call is
allowed. It must not perform a call. It returns false unless all are true:

```text
gate.state == ENABLED
provided gate epoch == current gate epoch
attempt.phase == SUBMITTING
fence token is current
no unresolved UNKNOWN attempt blocks the partition
```

The core has no production adapter and no method named `submit_order` that
contacts anything external.

## Implementation Steps

### Task 1: Replace the Python scaffold with a standalone Java module

**Why:** Establish a physically separate Java 17 module with no dependency on
earlier project phases and no competing Python implementation.

**Files:**

- Delete: `code/02_services/04_executor/main.py`
- Delete: `code/02_services/04_executor/requirements.txt`
- Delete: `code/02_services/04_executor/Dockerfile`
- Delete generated directories if present:
  `code/02_services/04_executor/__pycache__` and
  `code/02_services/04_executor/.ruff_cache`
- Modify: `code/02_services/04_executor/README.md`
- Create: `code/02_services/04_executor/pom.xml`
- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/ExecutorSafetyCoreMain.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/ExecutorSafetyCoreMainTest.java`

- [ ] Before the first edit, capture `git status --short` outside the
  repository at `/tmp/agent2-executor-baseline.txt`. Do not clean or revert
  pre-existing changes.
- [ ] Create `pom.xml` with parent `com.trading:trading-platform:0.1.0`,
  `relativePath` `../../pom.xml`, artifact ID `executor`, packaging `jar`, and
  only `org.junit.jupiter:junit-jupiter` with `test` scope. Do not add this
  module to `code/pom.xml`; invoking it with `-f` builds it independently.
- [ ] Configure `maven-jar-plugin` `3.3.0` with main class
  `com.trading.executor.core.ExecutorSafetyCoreMain`. No shade plugin is
  needed because production code has no external dependency.
- [ ] Delete the complete Python scaffold and its generated caches.
- [ ] Update README status to `offline Java safety core only`; state that
  Fluss, Arrow REST, credentials, network, deployment, and live money are
  excluded.
- [ ] Implement `static int run(String[] args, PrintStream out,
  PrintStream err)` and a thin `main(String[] args)` that calls
  `System.exit(run(args, System.out, System.err))`. `run`: no arguments prints
  the local `HALTED` state and returns `0`; `--self-test` verifies that a newly
  constructed gate is `HALTED` and returns `0`; any other argument prints an
  error to `err` and returns `2`. It must read no environment variable and
  open no external resource. Unit tests call `run` directly and must never
  install a security manager or launch a subprocess.
- [ ] Run `mvn -f code/02_services/04_executor/pom.xml test`.

### Task 2: Add immutable domain records, audit storage, and canonical hashing

**Why:** All later safety decisions require deterministic values and immutable
identity/content hashes.

**Files:**

- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/DomainModel.java`
- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/CanonicalHashing.java`
- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/InMemoryAuditSink.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/DomainModelTest.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/CanonicalHashingTest.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/InMemoryAuditSinkTest.java`

- [ ] Implement the exact enums and frozen records from the Technical Contract.
- [ ] Implement nonblank string, positive integer, enum, and timestamp
  validation at record construction or explicit validator calls.
- [ ] Implement canonical JSON manually with fixed field order, explicit
  `null`, JSON string escaping, UTF-8, and lowercase SHA-256. Do not add a JSON
  library. The fixed record field order is the canonical order; never depend
  on reflection or `Map` iteration order.
- [ ] Implement `decisionContentHash(decision)` and
  `haltRequestId(request)` using fixed field order.
- [ ] Implement append-only audit events with duplicate-ID same-content
  idempotency, duplicate-ID different-content rejection, and ordered attempt
  timeline reconstruction. Later components must use this sink directly.
- [ ] Test equal values hash identically across separately constructed record
  instances and maps with different insertion orders where maps are accepted.
- [ ] Test changing every decision field changes the content hash.
- [ ] Test blank IDs, invalid quantities, invalid exchanges, invalid prices,
  and invalid expiry are rejected.
- [ ] Run the focused model/canonical tests and then the full suite.

### Task 3: Implement deterministic client references and broker result classification

**Why:** The broker-facing reference and unknown-result policy must be defined
before any future adapter can be connected.

**Files:**

- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/ClientOrderReference.java`
- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/BrokerResultClassifier.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/ClientOrderReferenceTest.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/BrokerResultClassifierTest.java`

- [ ] Implement the exact 14-character reference algorithm from the Technical
  Contract.
- [ ] Test determinism, ASCII-only output, exact length, empty-input rejection,
  and collision resistance across the supplied fixture set.
- [ ] Implement the local `FakeBrokerResponse` record and pure classifier.
- [ ] Test verified acceptance, verified rejection, timeout, disconnect,
  process crash, missing body, malformed body, missing order number, and
  ambiguous combinations.
- [ ] Assert unknown classifications are not rejection and cannot be retried.
- [ ] Run the focused tests and then the full suite.

### Task 4: Implement gate transitions and two-person resume

**Why:** The gate is the primary money-movement safety boundary and must be
fully testable without durable infrastructure.

**Files:**

- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/GateStateMachine.java`
- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/ApprovalService.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/GateStateMachineTest.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/ApprovalServiceTest.java`

- [ ] Implement `GateStateMachine` with initial `HALTED`, epoch `0`, exact
  allowed transitions, expected-epoch compare-and-swap behavior, and audit
  callbacks.
- [ ] Implement explicit `halt(reason, evidenceHash, actor)` behavior for
  all current states.
- [ ] Implement `ApprovalService` with authorized operator allowlist,
  partition/epoch/evidence binding, distinct-operator enforcement, duplicate
  approval rejection, denial, and second-approval enablement.
- [ ] Test every legal transition and every illegal transition.
- [ ] Test stale epoch, duplicate approval, same-operator approval, unknown
  operator, mismatched evidence, mismatched partition, denial, and approval
  order permutations.
- [ ] Test that restart construction is always `HALTED` regardless of prior
  in-memory objects.
- [ ] Run focused tests and then the full suite.

### Task 5: Implement immutable attempt lifecycle and duplicate guard

**Why:** The attempt protocol prevents duplicate submissions and converts
uncertainty into a blocked state before a future broker adapter exists.

**Files:**

- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/InMemoryAttemptStore.java`
- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/DecisionValidator.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/InMemoryAttemptStoreTest.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/DecisionValidatorTest.java`

- [ ] Implement the exact decision validation rules, including reservation
  lookup, expiry, schema version, scope, exchange, quantity, and price.
- [ ] Implement `AttemptStore.prepare()` with instruction/content-hash replay
  behavior and deterministic `execution_attempt_id` supplied by the caller.
- [ ] Implement exact legal phase transitions, monotonic `phase_epoch`, stale
  update rejection, terminal protection, and UNKNOWN resolution only through
  explicit reconciliation result.
- [ ] Ensure a modified decision under an existing instruction ID raises a
  contract violation and requests a halt through a callback.
- [ ] Test `EXE-UNIT-002`, `EXE-UNIT-006`, duplicate replay, modified replay,
  expired/unreserved input, all invalid input combinations, stale phase update,
  terminal regression, unknown no-auto-retry, and explicit unknown resolution.
- [ ] Run focused tests and then the full suite.

### Task 6: Implement safety-halt idempotency and in-memory fencing

**Why:** Independent control and ownership rules can be proven locally before
the project chooses Fluss and Swarm fencing mechanisms.

**Files:**

- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/SafetyHaltService.java`
- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/InMemoryFenceStore.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/SafetyHaltServiceTest.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/InMemoryFenceStoreTest.java`

- [ ] Implement canonical safety-halt ID generation, scope validation,
  duplicate handling, stale source epoch rejection, and one-epoch application.
- [ ] Implement `FenceStore.acquire`, `release`, and `is_current` with
  invalidation of previous owner tokens.
- [ ] Test same request twice, cross-scope request, stale source epoch,
  malformed request, new-owner invalidation, stale token, wrong owner, release,
  and current-token success.
- [ ] Add an interleaving test proving an old owner cannot authorize a
  simulated submission after a new owner acquires the partition.
- [ ] Record in module documentation that this is not distributed production
  fencing and does not satisfy `EXE-FAIL-005` release evidence by itself.
- [ ] Run focused tests and then the full suite.

### Task 7: Integrate immutable audit with the safety-core orchestrator

**Why:** The independent package must connect the tested rules into one local
execution decision surface while preserving an auditable timeline.

**Files:**

- Create: `code/02_services/04_executor/src/main/java/com/trading/executor/core/ExecutorSafetyCore.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/ExecutorSafetyCoreTest.java`

- [ ] Connect every gate, approval, attempt, halt, fencing, duplicate,
  conflict, and classification decision to the Task 2 audit sink.
- [ ] Implement `ExecutorSafetyCore` with only the local methods listed in the
  Technical Contract.
- [ ] Make `authorize_submission` return a structured allow/deny result and
  never perform I/O.
- [ ] Require gate `ENABLED`, matching gate epoch, `SUBMITTING` attempt,
  current fencing token, and no unresolved UNKNOWN attempt.
- [ ] On unknown or ambiguous result, transition the attempt to `UNKNOWN`,
  request a gate halt, and emit audit events in deterministic order.
- [ ] On verified acceptance/rejection, update the attempt and emit only
  redacted result evidence; never persist a raw response body.
- [ ] Test successful local orchestration, denied gate, stale epoch, stale
  fence, unknown result halt, duplicate decision, modified decision halt,
  safety-halt application, and complete audit reconstruction.
- [ ] Run focused tests and then the full suite.

### Task 8: Add adversarial offline regression tests and final safety checks

**Why:** The package is intended for a safety-sensitive boundary; ordinary
happy-path unit tests are insufficient.

**Files:**

- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/ExecutorSafetyCoreAdversarialTest.java`
- Create: `code/02_services/04_executor/src/test/java/com/trading/executor/core/ExecutorImportBoundaryTest.java`
- Modify: `code/02_services/04_executor/README.md`

- [ ] Test all permutations of two approvals and all combinations of stale
  epoch/evidence/operator identity.
- [ ] Test repeated decision replay, modified replay, repeated halt, stale
  halt, fence replacement during authorization, and unknown-result handling.
- [ ] In `ExecutorImportBoundaryTest`, walk `src/main/java` text files and fail
  if an import contains `org.apache.fluss`, `org.apache.flink`,
  `java.net`, `java.net.http`, Apache HTTP, Arrow, Docker, or any class outside
  `java.*` and `com.trading.executor.core.*`. Also fail if production source
  contains `Socket`, `HttpClient`, `ProcessBuilder`, or `Runtime.getRuntime`.
- [ ] Test `ExecutorSafetyCoreMain.run` with in-memory output streams and no
  secrets: no arguments and `--self-test` return `0`; any live execution
  argument returns `2`. The test must not invoke `main` because `main` is only
  the process-exit wrapper.
- [ ] Add a README table separating implemented offline-core behavior from
  blocked production integrations.
- [ ] Run the full suite with warnings visible:

  ```text
  mvn -f code/02_services/04_executor/pom.xml test
  ```

### Task 9: Final boundary and cleanliness verification

**Why:** Parallel work is safe only if the agent did not modify shared files or
leave generated artifacts in the owned module.

**Files:**

- No files should be created or modified by this task; verification only.

- [ ] Run:

  ```text
  git status --short
  git diff --name-only -- code/02_services/04_executor
  ```

- [ ] Compare final `git status --short` with the baseline captured in Task 1.
  Confirm every path newly added or changed by Agent 2 is under
  `code/02_services/04_executor/**`. Pre-existing dirty paths are not Agent 2's
  work and must not be reverted.
- [ ] Remove generated `target/`, IDE metadata, coverage, and test-output files
  if untracked. Confirm no Python file, `requirements.txt`, `__pycache__`, or
  `.ruff_cache` remains under the Executor module.
- [ ] Run:

  ```text
  mvn -f code/02_services/04_executor/pom.xml clean test
  java -cp code/02_services/04_executor/target/classes \
    com.trading.executor.core.ExecutorSafetyCoreMain --self-test
  ```

- [ ] Return a report containing changed files, test command/output summary,
  Java/Maven versions, dependency count (`0` external runtime dependencies),
  and explicit blocked production work.

## Explicitly Not Implemented

The following remain later integration work and must not be stubbed as if
complete in this plan:

- Fluss `Trade_Decisions` changelog consumer.
- Fluss writes to `Execution_Gate`, `Execution_Attempts`,
  `Order_Correlation`, `Execution_Audit`, or `Safety_Halt_Requests`.
- Arrow REST request construction, authentication, timeout configuration, or
  network calls.
- Broker response protocol evidence and live request/reconciliation calls.
- Distributed fencing using Swarm, ZooKeeper, Fluss, or another external
  lease service.
- Postback consumption, fill capture, order lifecycle, and positions. Those
  belong to Action Capture.
- Babysitter integration and `Position_Actions`.
- Signal Job ranking, reservation, and `Trade_Decisions` production.
- Docker image or Compose/Swarm deployment changes.
- Shared Java identity or schema changes.
- Live-money readiness, automatic resume, or production release evidence.

## Verification Matrix

| ID | Offline proof in this plan | Production proof still required |
| --- | --- | --- |
| `EXE-UNIT-001` | `GateStateMachineTest` | Durable Fluss CAS and audit append |
| `EXE-UNIT-002` | `DecisionValidatorTest` | Final upstream decision/schema compatibility |
| `EXE-UNIT-003` | `ClientOrderReferenceTest` | Broker length/charset/echo evidence |
| `EXE-UNIT-004` | `BrokerResultClassifierTest` | Arrow REST sandbox evidence |
| `EXE-UNIT-005` | `ApprovalServiceTest` | Authenticated operator integration |
| `EXE-UNIT-006` | `InMemoryAttemptStoreTest` | Durable replay behavior |
| `EXE-FAIL-002` | `BrokerResultClassifierTest`, `ExecutorSafetyCoreTest` | Live timeout and reconciliation |
| `EXE-FAIL-003` | restart-construction tests | Durable-state corruption handling |
| `EXE-FAIL-005` | `InMemoryFenceStoreTest` interleaving | Distributed split-brain proof |
| `EXE-OPS-001` | approval and safety tests | Authenticated control path |
| `EXE-AUDIT-001` | in-memory reconstruction test | Seven-year encrypted audit storage |

The offline tests must label themselves as core tests and must not claim the
production acceptance IDs are complete. The production integration owner will
re-run the remaining gates after upstream contracts and external evidence are
available.

## Post-Completion

### Required report from Agent 2

Return only:

1. Changed files, all under `code/02_services/04_executor/**`.
2. Exact Java and Maven versions and test commands.
3. Test count and pass/fail result.
4. Confirmation that imports and tests perform no network or subprocess I/O.
5. Confirmation that no external runtime dependency was added.
6. List of production integrations intentionally left blocked.
7. Any defect found inside the Executor module, without editing shared docs.

### Later integration order

After Agent 1 finalizes upstream schemas and the project owner approves the
external evidence, a separate integration change may add adapters in this
order:

```text
local Decision adapter
-> Fluss Trade_Decisions reader
-> Fluss-owned Executor state stores
-> authenticated control/safety-halt reader
-> distributed fencing provider
-> sandbox Arrow adapter
-> reconciliation provider
-> controlled live-money release evidence
```

Each integration must be a separate change with cross-boundary tests. Do not
extend this plan during Agent 2 execution to include those steps.
