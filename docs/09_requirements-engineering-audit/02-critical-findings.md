# Critical Findings

These findings block safe implementation of affected behavior. Each remains open until requirements, contracts, schemas, tests, and operations are reconciled.

## AUD-C01 — Global ranking and reservation topology is undefined

**Evidence:** `docs/02_requirements/02-functional/03-compute.md:15-20` keys compute by instrument; `10-ranking.md:42-46` requires portfolio-wide limits; `04-business-logic.md:7-17` names a reservation view without defining its source or consistency.

**Issue:** Per-instrument keyed subtasks cannot independently enforce global limits such as three total positions or one per strategy.

**Cause/risk:** Concurrent subtasks may observe the same free capacity and publish too many instructions. A restart or partial visibility can make the error nondeterministic.

**Recommended solution:** Keep detection keyed by instrument, then explicitly repartition eligible candidates to a deterministic reservation scope such as `portfolio_id`. Serialize ranking and reservation transitions for that scope using keyed managed state. Define candidate snapshot updates, reservation versions, hot-key behavior, parallelism, and recovery.

**Required artifacts:** topology diagram, in-job event contract, reservation schema/state contract, capacity model, Flink harness test, parallel concurrency test.

**Acceptance:** Across concurrent candidates, restart, delayed lifecycle state, and partial output visibility, the number of active reservations never exceeds each configured limit.

**Owner:** Compute/Strategy + Platform. **Status:** `OPEN`

## AUD-C02 — Reservation state has no authoritative owner or durable lifecycle

**Evidence:** `04-business-logic.md:45-57`, `10-ranking.md:42-46`, `06-action-capture.md:74-76`, `02-functional/02-storage.md:23-43`.

**Issue:** Reservation states exist, but no table, key, writer, transition source, or rebuild source is defined.

**Cause/risk:** Reservations can disappear after restart, be released twice, remain stuck forever, or be released while an unknown order still exists.

**Recommended solution:** Choose one owner. Define `reservation_id`, `portfolio_id`, aggregate key, transition version, expected prior state, legal transition matrix, stale/conflict handling, rebuild source, and reconciliation path. Use a durable reservation event/feed or KV projection only after connector semantics are tested.

**Acceptance:** Replay and crash tests show idempotent transitions, no capacity overbooking, and `UNKNOWN` reservations remain capacity-consuming.

**Owner:** Strategy/Execution/Platform. **Status:** `OPEN`

## AUD-C03 — Safety-halt propagation to Executor is unspecified

**Evidence:** `03-compute.md:79-84`, `06-action-capture.md:21-30`, `07-executor.md:20-31` require halting but `05-interfaces.md` defines no safety-event interface.

**Issue:** “Halt the order path” has no transport, identity, scope, authorization, acknowledgement, or stale-event rule.

**Cause/risk:** A detected checkpoint, correlation, or continuity failure may remain telemetry-only and fail the five-second safe-halt objective.

**Recommended solution:** Define a durable authenticated `Safety_Halt_Request` contract with request ID, scope, source, reason, detection time, evidence hash, source version, schema version, and acknowledgement. Executor consumes it idempotently and audits applied/rejected events. Executor must also independently detect critical stale health.

**Acceptance:** Inject each halt trigger and measure fault→detection, detection→gate-block, and fault→gate-block. Duplicate and stale halt events are harmless.

**Owner:** Execution + Platform. **Status:** `OPEN`

## AUD-C04 — Account, portfolio, and execution scope identities are missing

**Evidence:** `01-context-and-scope.md:60-73`, `04-data.md:9-23`, `07-executor.md:9-16,76-78`.

**Issue:** Gate and fencing are account-scoped and ranking is portfolio-scoped, but the identity model does not define those scopes.

**Cause/risk:** Future accounts or partitions may share gate, capacity, correlation, or client-reference state accidentally.

**Recommended solution:** Add `account_scope_id`, `portfolio_id`, and, if needed, `execution_partition_id`. Place them explicitly in instructions, attempts, mappings, lifecycle, positions, reservations, gate, and audit. Define redaction/cardinality rules for telemetry.

**Acceptance:** Isolation tests prove that a halt, reservation, mapping, or fence in one scope cannot affect another.

**Owner:** Platform + Execution. **Status:** `OPEN`

## AUD-C05 — Executor fencing is not an implementable protocol

**Evidence:** `07-executor.md:76-78` requires one active Executor but does not define lease/fence semantics.

**Issue:** “One active owner” is not sufficient without a durable fencing token and stale-owner rejection.

**Cause/risk:** A delayed or partitioned Executor may submit after another instance becomes owner.

**Recommended solution:** Define durable lease ownership, monotonically increasing fencing epoch/token, renewal/expiry, stale-token rejection, gate interaction, and call interleavings. Every attempt stores gate epoch and fence token; the final pre-call check validates both.

**Acceptance:** Test delayed stale processes, network partitions, lease expiry, replacement owners, storage loss, and calls around ownership transfer. No dual submission occurs.

**Owner:** Execution + Platform. **Status:** `EVIDENCE-BLOCKED`

## AUD-C06 — Action Capture projection ledger is required but absent

**Evidence:** `06-action-capture.md:54-64` requires durable pending/completion recovery; `02-functional/02-storage.md:23-43` and `04-data.md:25-44` define no ledger.

**Issue:** The crash-safe workflow has no logical storage location.

**Cause/risk:** A crash after immutable audit but before lifecycle/position projection may lose track of incomplete work.

**Recommended solution:** Add `Postback_Projection_Ledger` or an equivalent durable inbox/outbox with `postback_event_id`, state, source version, retry count, next retry, error/disposition, and completion evidence.

**Acceptance:** Crash after every projection step, restart scan, duplicate delivery, and rebuild-from-audit tests complete exactly one logical state effect.

**Owner:** Action Capture + Storage. **Status:** `OPEN`

## AUD-C07 — Instruction feed contract is ambiguous

**Evidence:** `02-functional/02-storage.md:23-43,74-78` says `Trade_Decisions` is `LOG or KV` and may receive Executor-assigned `client_order_ref`; `07-executor.md:17-18` prohibits mutation.

**Issue:** Immutable feed and Executor-owned reference assignment are incompatible if represented in one mutable row.

**Cause/risk:** Consumers cannot know whether updates are legal or whether a changed instruction is a mutation.

**Recommended solution:** Make `Trade_Decisions` an immutable feed containing only Signal-owned fields. Keep `client_order_ref` in `Execution_Attempts`; map all identities in `Order_Correlation`. Any current projection must be separate and non-authoritative.

**Acceptance:** Same instruction identity with changed content is rejected; Executor never writes the instruction feed; replay produces one intake effect.

**Owner:** Signal + Execution + Storage. **Status:** `RECONCILIATION-REQUIRED`

## AUD-C08 — Instruction visibility and checkpoint commit are conflated

**Evidence:** `03-non-functional.md:15-27`, `10-ranking.md:48-54`, `03-compute.md:71-77`.

**Issue:** “Instruction commit” is undefined: sink acknowledgement, Fluss visibility, checkpoint commit, or durable reader availability.

**Cause/risk:** The `<100 ms` target may be impossible if visibility waits for checkpoint completion, or may not mean durable exactly-once if it ends at an API acknowledgement.

**Recommended solution:** Define `decision_created`, `sink_ack`, `instruction_visible`, `checkpoint_commit`, and `executor_received` timestamps. Set the SLO boundary explicitly and measure checkpoint/recovery separately.

**Acceptance:** A pinned reader observes the instruction at the defined boundary under normal, burst, checkpoint, and partial-failure conditions.

**Owner:** Platform + Signal. **Status:** `EVIDENCE-BLOCKED`
