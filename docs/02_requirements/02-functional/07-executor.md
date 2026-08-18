# 02.7 — Executor (Execution Core — order path)

> **RE-SCOPED 2026-08-18 (CHG-028, DEC-041):** the Executor is the order path of the integrated
> Execution Core. Nautilus provides the OMS/position/risk/reconciliation machinery; the
> **go-arrow bridge** is the ONLY component permitted to call Arrow REST; the two-person gate,
> fencing, attempts, correlation, and immutable audit remain custom glue. Former
> standalone-service wording ("Executor calls Arrow REST directly") is superseded. The no-OpenAlgo
> policy (DEC-006) stands.

## Purpose

The Execution Core's order path is the only platform domain permitted to initiate money-moving
calls. Within it, the go-arrow bridge (localhost service wrapping the pinned `go-arrow` SDK) is
the only component that can physically reach Arrow — Nautilus commands the bridge and never holds
broker credentials. There is no OpenAlgo layer (DEC-006 — no third-party layer; the bridge is
first-party, pinned protocol code). It consumes immutable instructions and, after MVP, structured
position actions; enforces a durable order gate before every broker call; persists attempts and
identity mappings; reconciles uncertain broker outcomes; and writes only execution-owned state.

## Constraints

- The Execution Core SHALL be the only platform domain permitted to initiate money-moving Arrow
  REST calls, and the go-arrow bridge SHALL be the only component within it that calls Arrow. No
  other component may submit broker orders.
- The Execution Core SHALL NOT mutate strategy, candidate, ~~ranking,~~ or ~~instruction~~ fields
  in any table. Execution status goes in execution-owned state only. **(Ranking/instruction feed REMOVED 2026-08-15, CHG-005.)**
- Every money-moving call SHALL pass through the durable gate. A gate check that is skipped,
  stale, or bypassed is a release-blocking defect.
- An unknown broker outcome SHALL halt the gate within five seconds of detection. Automatic retry
  of an unknown attempt is prohibited.
- A repeated `instruction_id` with different content is a contract violation. The core SHALL
  halt, quarantine, and alert; it SHALL NOT submit the modified instruction.
- Only one fenced owner may submit for an `execution_partition_id` at a time. Dual submission is
  a release-blocking defect.
- A fresh installation and any restart with unverifiable durable state SHALL begin `HALTED`.
  Automatic resume from unknown state is prohibited.
- Resume SHALL require two distinct authenticated authorized operators approving the same gate
  epoch and evidence hash. A single approval or mismatched epoch/hash SHALL NOT enable the gate.
- The core SHALL NOT infer a broker-order mapping from proximity, timing, or overloaded IDs.
  Postbacks without a unique verified mapping are quarantined.
- Free-form command strings for position actions are prohibited. Future actions use the versioned
  structured `Position_Actions` schema.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-EXE-001 | The go-arrow bridge (wrapping the pinned `go-arrow` SDK) exposes deterministic order-submission responses including broker acceptance/rejection status and evidence to correlate broker order identity. ~~OpenAlgo~~ — removed per DEC-006. | ASM-007 |
| ASM-EXE-002 | `client_order_ref` fits within Arrow's 16-character `remarks` field and is echoed reliably in broker postbacks. | RISK-004 |
| ASM-EXE-003 | Arrow REST reconciliation endpoints (`GET /user/orders`, `/user/trades`, `/user/positions`, `/order/{id}`), reachable through the go-arrow bridge, can query recent orders, client references, broker order IDs, fills, and positions with defined consistency delay. | REQ-EXE-013 |
| ASM-EXE-004 | Arrow postbacks expose `broker_order_id`, lifecycle status, and the submitted `remarks` value for correlation. | ASM-002 |
| ASM-EXE-005 | The deployment provides a fencing/leadership mechanism sufficient for single-active-owner enforcement per `execution_partition_id`. | ASM-009, REQ-EXE-008 |
| ASM-EXE-006 | Two distinct authorized operators are available for the two-person resume protocol. Single-operator deployments are not production-ready. | REQ-EXE-003, RISK-012 |
| ASM-EXE-007 | The broker idempotency key or equivalent mechanism (if available) prevents duplicate order submission under crash-window scenarios. If unavailable, the durable attempt protocol plus reconciliation is the sole safeguard. | RISK-002 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions
register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the
affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Gate begins HALTED:** Every fresh installation and every restart that cannot verify durable
  state continuity begins with the gate halted. The platform does not assume clean state on
  startup.
- **Conditional auto-resume:** The core MAY auto-resume after a restart ONLY when durable
  evidence proves all seven conditions: known gate state/epoch, valid fencing owner, no UNKNOWN
  attempts, no unresolved correlations, healthy changelog continuity, healthy Signal-job/
  checkpoint evidence, and fresh mandatory health signals. If ANY proof is missing, the core
  remains HALTED and the two-person reconciliation/resume path applies. Every auto-resume SHALL
  produce an immutable audit event and OpenObserve notification.
- **Unknown outcomes halt the order path:** Timeout, disconnect, crash window, or ambiguous
  broker response does not retry automatically. The attempt is marked `UNKNOWN`, the gate halts,
  and reconciliation is mandatory before any retry or new order.
- **Two-person resume with matching evidence:** Gate resume requires two distinct operators
  approving the same gate epoch and evidence hash. This prevents a single compromised operator
  from enabling money-moving flow.
- **Reconciliation is mandatory, not optional:** Unknown attempts and unresolved correlations
  block new orders until explicitly reconciled. The platform will not silently resolve ambiguity.
- **Position actions follow the same safety protocol:** Post-MVP, Babysitter-emitted
  `Position_Actions` pass through the identical gate, attempt, correlation, and reconciliation
  pipeline as entry instructions.

## Out of Scope

The following capabilities are explicitly NOT owned by the Executor (order path):

- **Market data ingestion, broker connection, and packet decoding:** Owned by Ingestion.
- **Candle computation, deduplication, event-time watermarking:** Owned by Compute within the
  Signal Flink job.
- **Signal detection, candidate creation, strategy evaluation:** Owned by Business Logic within
  the Signal Flink job.
- ~~**Ranking, portfolio reservation, and winner selection:**~~ — **REMOVED 2026-08-15 (CHG-005).**
- **Postback capture, fill audit, order lifecycle projection, and position projection:** Owned by
  the capture path (go-arrow bridge intake + Nautilus OMS/position engine + projection sinks).
- **Position monitoring and position-action emission:** Owned by the Babysitter no-op.
- **EOD offload to Iceberg/S3:** Owned by the EOD controller.
- **Observability backend, dashboard, and alert configuration:** Owned by the observability layer
  and operations.
- **Strategy authoring, backtesting, or configuration UI:** Deferred; not in MVP scope.
- **Multi-broker or multi-account routing:** Deferred; not in MVP scope.
- **Automatic live-gap backfill or historical order replay:** Deferred; not in MVP scope.

## REQ-EXE-001: Dedicated execution state

The Execution Core SHALL own dedicated Fluss state with exact physical DDLs for:

- `Execution_Gate` KV: singleton/account-scoped gate state, epoch, reason, timestamps, approvals
- `Execution_Attempts` KV: one row per `execution_attempt_id`, immutable request hash, phase,
  outcome, retry eligibility
- `Order_Correlation` KV: `instruction_id` ↔ `client_order_ref` ↔ `broker_order_id` mapping and
  verification state
- `Execution_Audit` LOG: immutable gate transitions, attempt events, reconciliation evidence,
  approval events, and broker response summaries

The core SHALL not mutate strategy/candidate/~~ranking~~ fields. Execution state is tracked
exclusively in `Execution_Attempts`, `Order_Correlation`, and `Execution_Audit`.

## REQ-EXE-002: Gate state machine

```text
HALTED → RECONCILING → APPROVAL_PENDING → ENABLED
ENABLED → HALTED
```

A fresh installation and any restart with unverifiable durable state begin `HALTED`. Every
money-moving call SHALL atomically verify the current gate epoch/state before submission.

The gate enters `HALTED` for unknown submission outcome, duplicate risk, missing/ambiguous
correlation, changelog discontinuity, checkpoint failure affecting order correctness, missing
fill, failed reconciliation, unauthorized action, or security incident. **(Instruction/
reservation state clause REMOVED 2026-08-15, CHG-005.)**

Safe-halt SHALL block new calls within five seconds of detection.

## REQ-EXE-003: Two-person resume

Resume requires:

1. Broker order reconciliation.
2. Open position/fill reconciliation.
3. Consumer offset/changelog continuity verification.
4. Signal job/checkpoint health verification.
5. Resolution of every unknown attempt.
6. Two distinct authenticated authorized operators approving the same gate epoch and evidence
   hash.

The second approval transitions `APPROVAL_PENDING` to `ENABLED`. Automatic resume is prohibited.
Approvals and denied/unauthorized attempts are immutable audit events.

## REQ-EXE-004: Immutable execution-intent intake (reinstated by DEC-042)

The Nautilus Execution Service SHALL consume a durable, immutable execution-intent stream from
Fluss. This stream is distinct from the retired `Trade_Decisions` ranking feed: it carries only a
validated platform execution request after Signal-job strategy processing. The service SHALL
validate schema/version, scope, expiry, identity, request hash, supersession, and duplicate
semantics before creating a Nautilus order. A repeated `instruction_id` with different content is
a contract violation and SHALL be quarantined, audited, and halted.

The service SHALL NOT mutate the source intent or strategy fields. Execution state and outcomes
are written only to execution-owned control tables and Nautilus-derived Fluss projections.

## REQ-EXE-005: Attempt protocol

Before a broker call the core SHALL durably create `execution_attempt_id`, deterministic
evidence-gated `client_order_ref`, request hash, gate epoch, and `PREPARED` audit state. It then
transitions to `SUBMITTING` and commands the go-arrow bridge, which calls Arrow REST
(`POST /order/regular`).

Outcomes:

- Verified rejection/no acceptance: terminal failure; reconcile according to policy.
- Verified broker acknowledgement: persist `broker_order_id` mapping and terminal accepted state.
- **Timeout, disconnect, malformed response, process crash window, or ambiguous status:** mark
  `UNKNOWN`, halt, and reconcile before any retry.

A bridge command without a verified response after **15 seconds** SHALL become an `UNKNOWN`
outcome. The core SHALL immediately: persist the attempt as `UNKNOWN`, write an immutable
`Execution_Audit` event, halt all new orders globally, and initiate reconciliation before any
retry or resume.

An unknown attempt is never automatically retried as a new order. A retry requires proof the
broker did not accept the prior request or a verified broker idempotency contract; otherwise
manual disposition is required.

## REQ-EXE-006: Correlation

The broker-facing reference format, length, character set, uniqueness, and echo behavior are
evidence-gated. The mapping is durable before an attempt may be considered safely reconciled.

Postbacks without a unique mapping are quarantined; they halt affected new order flow and never
create an inferred instruction mapping silently.

## REQ-EXE-007: Structured position actions

MVP consumes no Babysitter actions. Future actions SHALL be immutable versioned records
containing `action_id`, `position_id`, `trade_context_id`, action type, side, quantity, optional
price, reason, source state/version, event/expiry timestamps, and correlation IDs.

They pass through the same gate, attempt, identity, and reconciliation protocol as entry
instructions.

## REQ-EXE-008: Availability and single-writer ownership

Only one active owner may command an account/order partition at a time, and only the go-arrow
bridge may physically call Arrow. Production deployment SHALL define leadership/fencing so two
instances cannot submit the same partition concurrently. Losing leadership or durable-state
connectivity halts submissions.

## REQ-EXE-009: Health and observability

Readiness requires durable state connectivity, changelog continuity, valid schema/version, bridge
and Arrow REST reachability, current gate state known, no unresolved invariant violation,
acceptable clock offset, and observability delivery. `HALTED` may be process-healthy but
trading-not-ready.

Metrics/logs include gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes,
duplicate suppressions, reconciliation duration/results, mapping/quarantine counts, approval
events, consumer lag, Arrow REST latency/status via the bridge, bridge connection state, and
security events. Secrets and raw credentials are never logged.

## REQ-EXE-011: Safety-halt control interface

The core SHALL consume durable, authenticated safety-halt requests from Signal, the capture path,
platform health, and authorized operators. Each request SHALL include `halt_request_id`,
`account_scope_id`, applicable `portfolio_id`/`execution_partition_id`, source component/instance,
reason code, detection time, source epoch/version, evidence hash, and schema version.

Requests SHALL be idempotent. The core SHALL apply or reject each request with an auditable
result, increment the gate epoch on an applied halt, and block new money-moving calls. Stale,
malformed, or cross-scope requests SHALL be rejected and audited. The core SHALL independently
detect stale mandatory health even if the request stream is unavailable.

## REQ-EXE-012: Fencing protocol

Only one fenced owner may submit for an `execution_partition_id`. The owner SHALL hold a durable
fencing epoch/token. Every attempt stores the gate epoch and fencing token. Immediately before a
bridge command leading to an Arrow REST call, the core SHALL verify current gate state, gate
epoch, fencing ownership/token, durable attempt phase, and required health evidence.

Lease loss, token mismatch, storage uncertainty, network partition, or stale ownership SHALL
prevent the call and move the affected gate to `HALTED`. The exact mechanism remains
evidence-gated, but stale-owner rejection and concurrent interleaving semantics SHALL be tested.

## REQ-EXE-013: Reconciliation capability

Live-money enablement requires evidence that Arrow REST/broker reconciliation, reachable through
the go-arrow bridge, can query or list recent orders, client references, broker order IDs, fills,
and positions with defined consistency delay, pagination, rate limits, authentication, and
history horizon. Missing reconciliation evidence blocks retry of unknown attempts and blocks
live-money release.

## REQ-EXE-010: Acceptance gates

1. Crash injection before call, during call, after broker acceptance, and before durable
   acknowledgement produces no duplicate broker order.
2. Unknown outcomes halt within five seconds and cannot retry automatically.
3. Restart with missing/corrupt state defaults to HALTED.
4. Two-person approval enforces distinct identities and matching evidence epoch/hash.
5. Concurrent-instance/fencing tests prevent dual submission.
6. Safety-halt requests are idempotent, scoped, audited, and applied within the defined
   fault-to-gate threshold.
7. Mapping, quarantine, broker rejection, timeout, malformed response, and changelog
   discontinuity tests pass.
8. Arrow REST reconciliation capability and consistency-delay tests pass (via the bridge).
9. Every money-moving call is reconstructable from immutable audit data retained for at least one year or longer under the approved retention policy.
10. Bridge place/modify/cancel endpoints pass Arrow-sandbox smoke tests (the go-arrow SDK order
    path is currently untested), the current `NotImplementedError` scaffold is replaced, and all
    gates pass before live-money release.
