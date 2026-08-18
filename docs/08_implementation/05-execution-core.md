# Execution Core — Action Capture, Babysitter & Executor (integrated)

> **ROLE — WORKING DOSSIER (2026-08-18):** this is the single integrated implementation dossier for
> the execution half of the platform. It replaces `05-action-capture.md`, `06-babysitter.md`, and
> `07-executor.md` (each SUPERSEDED and deleted 2026-08-18; full history in git at `74f3d89`). Architecture per the
> 2026-08-18 user decision: **Nautilus** (Rust-native trading engine) is the execution/position
> core; a **go-arrow bridge** (localhost) is the ONLY component that talks to Arrow; the Fluss
> trade-row reader and the two-person gate are custom glue. The upstream layer is reconciled to
> this architecture (2026-08-18, **CHG-028**): build contracts
> `docs/04_contracts/05-babysitter.md`/`06-action-capture.md`/`07-executor.md`, functional
> requirements `02-functional/05/06/07`, and DEC-006 are re-scoped — see their dated banners.
>
> Status banner: **Design — Draft · Implementation — Not-implemented · Evidence — Untested ·
> Live-money — Blocked.**

<!-- markdownlint-disable MD013 -->

## Status and sources

| Field | Value |
| --- | --- |
| Status | Design revision (Nautilus + go-arrow bridge); upstream contracts/requirements/DEC-006 re-scoped 2026-08-18 (CHG-028) |
| Owner | Execution Team (order path) · Action Capture Team (capture path) · Babysitter Team (position observation) |
| Requirements | `REQ-AC-001`–`REQ-AC-013` → `AC-AC-001`–`AC-AC-017`; `REQ-BB-001`–`REQ-BB-008` → `AC-BB-001`–`AC-BB-009`; `REQ-EXE-001`–`REQ-EXE-013` → `AC-EXE-001`–`AC-EXE-016` |
| Acceptance criteria | `AC-AC-001`–`AC-AC-017`, `AC-BB-001`–`AC-BB-009`, `AC-EXE-001`–`AC-EXE-016` |
| Contracts | `docs/04_contracts/06-action-capture.md` · `docs/04_contracts/05-babysitter.md` · `docs/04_contracts/07-executor.md` (re-scoped 2026-08-18, CHG-028) · `docs/04_contracts/arrow_broker.md` |
| Writes | `Fills`, `Order_Lifecycle`, `Positions`, `Postback_Quarantine`, `Postback_Projection_Ledger`, `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`; consumes immutable `Execution_Intent` and `Safety_Halt_Requests` (KV control table) |
| Must not own | Strategy, candidate scoring, gate approval, order submission to any component other than the go-arrow bridge (**ranking/reservations/decisions REMOVED 2026-08-15, CHG-005**) |

## Why one dossier

The three former services are one pipeline — **capture what the broker did → watch the position →
execute the next action** — and the adopted engine (Nautilus) provides shared machinery for all
three: the OMS (order lifecycle), the position engine, reconciliation, fill dedup, and the event
store. Three dossiers forced duplicated state machines, duplicated config, and three test
surfaces for one execution domain. This dossier is the single build contract.

## Recommended operating model

This is the normative explanation of how Nautilus complements the existing Flink/Fluss platform.

| Responsibility | System | Meaning |
| --- | --- | --- |
| Decide | Flink + Fluss | Process market data, compute candles/signals, and publish durable execution intent |
| Execute | Nautilus Execution Service | Manage orders, fills, positions, PnL, risk, reconciliation, and execution history |
| Control and integrate | Custom glue + go-arrow bridge | Enforce safety/fencing, translate protocols, and project execution state into Fluss |

> Flink produces trade intent. Nautilus manages execution reality.

Flink must not call Arrow, and Fluss must not become a second order or position engine. Nautilus
must run as a long-lived service so its OMS, cache, event processing, reconciliation context, and
position state survive individual instruction messages. Starting a new Nautilus process for every
order is prohibited because it loses state continuity and increases crash-window risk.

### Ownership matrix

| Domain | Single owner | Other components may |
| --- | --- | --- |
| Raw market-data ingestion | Ingestion | Read raw data and health evidence |
| Candle and signal computation | Signal Flink job | Consume signal outputs |
| Execution intent | Immutable Fluss intent stream | Read and validate |
| Live order lifecycle | Nautilus OMS | Consume projected state |
| Fill application and deduplication | Nautilus execution/portfolio path | Consume projected fills |
| Position quantity, side, average price, and PnL | Nautilus Portfolio/Position engine | Consume position events |
| Broker reconciliation | Nautilus reconciliation through the adapter | Request or observe reconciliation |
| Gate, approvals, and fencing | Custom execution control glue | Nautilus must obey the result |
| Arrow credentials and network access | go-arrow bridge | No other service may reach Arrow |
| Queryable execution projections | Nautilus projection boundary into Fluss | Read projections |
| Position observation | Babysitter | Emit no actions in MVP |

### What each system must not do

| Component | Must not implement |
| --- | --- |
| Signal Flink job | Broker calls, broker order lifecycle, broker-derived positions, or execution retries |
| Fluss | A competing OMS, position calculator, or broker command executor |
| Babysitter | Direct Arrow calls or a separate exit-order path |
| Custom projection code | A second position arithmetic engine or a second fill-deduplication engine |
| Nautilus adapter | Platform gate policy hidden inside broker-status mapping |
| go-arrow bridge | Strategy decisions, position arithmetic, or gate authorization |

## Service topology

```text
Signal Flink job
    │ immutable execution intent
    ▼
Fluss intent stream
    │
    ▼
Nautilus Execution Service
    ├─ Fluss intent consumer and schema validator
    ├─ custom gate, attempts, correlation, and fencing
    ├─ Nautilus OMS, risk, portfolio, position, and reconciliation engines
    ├─ Nautilus event store
    └─ Fluss projection writers
            │ local authenticated protocol
            ▼
    go-arrow bridge
    ├─ Arrow authentication and token refresh
    ├─ place/modify/cancel REST calls
    ├─ order-update WebSocket
    └─ orders/trades/positions/account reads
            │ TLS
            ▼
        Arrow broker
```

| Deployment option | Decision | Reason |
| --- | --- | --- |
| Embed Nautilus in a Flink task | Reject | Different lifecycle, failure model, language runtime, and checkpoint boundary |
| Start Nautilus once per order | Reject | Loses OMS/cache/reconciliation continuity |
| Run one shared process for all accounts immediately | Defer | Efficient, but increases scope isolation and fencing complexity |
| Run one long-lived Nautilus instance per execution partition | Recommend initially | Simple ownership, restart, fencing, and account isolation |
| Run Nautilus as a separate Docker service | Recommend | Isolates money-moving operations and credentials from Flink |
| Use Python only as the first control-plane prototype | Accept for discovery | Useful for adapter proof; production runtime should follow the native Rust service boundary |

## Boundary contracts

### Flink/Fluss to Nautilus

The input is an immutable, versioned execution-intent record. It is not a broker request body and
it is not the retired `Trade_Decisions` feed.

| Field group | Required content |
| --- | --- |
| Identity | `instruction_id`, `trade_context_id`, `account_scope_id`, `execution_partition_id` |
| Instrument | Canonical platform instrument identity and broker mapping version |
| Order | Side, quantity, order type, price if applicable, time-in-force, product/exchange |
| Safety | Signal/source version, creation time, expiry time, request hash |
| Replay | Schema version, strategy version, supersession/cancellation information |

The reader validates schema, scope, expiry, request hash, identity uniqueness, and source
continuity before creating a Nautilus order. A repeated `instruction_id` with different content is
a contract violation: quarantine, audit, halt, and no broker call.

### Nautilus to go-arrow bridge

The bridge protocol maps native Nautilus execution commands to the already pinned Go SDK. Nautilus
does not hold Arrow credentials or implement Arrow-specific authentication.

| Nautilus | Bridge |
| --- | --- |
| `ClientOrderId` | Deterministic `client_order_ref` carried in Arrow `remarks` |
| `InstrumentId` | Arrow exchange/symbol/token mapping |
| `OrderSide` | Arrow buy/sell value |
| `OrderType` | Arrow market/limit/trigger value |
| `Quantity`, `Price` | Arrow quantity and price fields with exact decimal conversion |
| Submit/modify/cancel command | Local bridge request |
| Order/fill/account/position report | Normalized bridge event consumed by Nautilus |

The bridge is loopback-only in local mode and service-network restricted in production. It is the
only process containing Arrow credentials and the only process with broker network access.

### Nautilus to Fluss

Nautilus emits execution events. Projection glue converts them into Fluss records:

| Nautilus event | Fluss projection |
| --- | --- |
| Fill event | `Fills` LOG |
| Order lifecycle event | `Order_Lifecycle` KV |
| Position event | `Positions` KV |
| Command/report/audit event | `Execution_Audit` LOG |
| Attempt transition | `Execution_Attempts` KV |
| Verified identity mapping | `Order_Correlation` KV |
| Projection progress | `Postback_Projection_Ledger` KV |
| Unknown or unmapped evidence | `Postback_Quarantine` LOG |

These are durable read models and integration surfaces. They do not authorize a second execution
effect and they do not replace Nautilus as the live execution authority.

## Identity mapping

The platform must preserve separate identities throughout the flow.

| Identity | Owner | Purpose |
| --- | --- | --- |
| `candidate_id` | Signal job | Detected setup/evidence record |
| `instruction_id` | Signal/intent writer | Immutable platform execution intent |
| `execution_attempt_id` | Custom execution control | One submission attempt for an instruction |
| `client_order_ref` | Custom execution control | Deterministic broker-facing reference |
| `broker_order_id` | Arrow | Broker-authoritative order identity |
| Nautilus `ClientOrderId` | Nautilus adapter | Native order identity used by the OMS |
| Nautilus `TradeId` | Nautilus | Native fill identity |
| `position_id` | Nautilus position projection | Fill-derived exposure aggregate |
| `trade_context_id` | Platform | Groups entry, trim, and exit orders |
| `postback_event_id` | Capture boundary | One received broker update |
| `halt_request_id` | Safety control | One durable safety-halt request |

Never use a generic `order_id` across these domains. A trim, exit, or re-entry is a new broker
order linked to the same trade context; it is not a mutation of the original order identity.

## End-to-end trade flows

### Normal accepted trade

| Step | Component | Action |
| ---: | --- | --- |
| 1 | Signal Flink job | Detects a valid setup |
| 2 | Fluss | Stores immutable execution intent |
| 3 | Execution reader | Validates intent, identity, expiry, and scope |
| 4 | Safety control | Checks gate state, epoch, and fencing token |
| 5 | Nautilus | Creates the native order and applies native risk checks |
| 6 | ExecutionClient | Converts the order to a bridge command |
| 7 | go-arrow bridge | Calls Arrow REST |
| 8 | Arrow | Returns a verified acceptance or rejection |
| 9 | Nautilus | Emits order lifecycle events and persists execution history |
| 10 | Arrow WebSocket | Publishes order updates and fills |
| 11 | Nautilus | Applies updates, deduplicates fills, updates the position and PnL |
| 12 | Projection glue | Writes Fluss execution projections |
| 13 | Babysitter | Observes the projected position; emits zero actions in MVP |

### Unknown broker outcome

| Step | Component | Action |
| ---: | --- | --- |
| 1 | Bridge | Times out, disconnects, or returns an unclassifiable response |
| 2 | Adapter/control | Classifies the attempt as `UNKNOWN`, never as rejection |
| 3 | Safety control | Moves the gate to `HALTED` and blocks new submissions |
| 4 | Nautilus | Preserves the unresolved order and event history |
| 5 | Reconciliation | Queries Arrow orders, trades, positions, and order detail |
| 6 | Nautilus | Applies only verified reconciliation reports |
| 7 | Projection glue | Records the outcome, evidence, and resolution |
| 8 | Operators | Review the evidence and perform the required approvals |

An unknown attempt is never blindly retried. A retry requires proof that the broker did not accept
the original request or an evidence-approved broker idempotency mechanism.

## Position-management model

Nautilus owns the position calculation path:

```text
Arrow fill report
    → Nautilus order event
    → Nautilus position engine
    → quantity, side, average price, realized/unrealized PnL
    → Fluss Positions projection
    → Babysitter observation
```

| Position behavior | Owner |
| --- | --- |
| Open a position | Nautilus position engine |
| Add to a position | Nautilus position engine |
| Partially reduce a position | Nautilus position engine |
| Close a position | Nautilus position engine |
| Long/short/net calculation | Nautilus position engine |
| Average entry/exit arithmetic | Nautilus position engine |
| Realized and unrealized PnL | Nautilus portfolio engine |
| Broker position reconciliation | Nautilus reconciliation |
| Decide whether an exit should be requested | Babysitter/strategy layer, future scope |
| Authorize the exit | Custom gate and fencing |
| Submit the exit order | Nautilus OMS through the same adapter path |

The current Java position projector is a migration and differential-test reference only. It must
not remain a second production authority after Nautilus parity is proven.

## Migration and proof roadmap

| Phase | Deliverable | Release gate |
| --- | --- | --- |
| 1. Boundary freeze | Intent schema, identities, ownership, gate contract, projection schemas | No duplicate authority in documents or code |
| 2. Offline adapter | Platform intent to Nautilus order; fake bridge; recorded updates/fills | Deterministic lifecycle and position parity |
| 3. Mock broker | Place, modify, cancel, reject, partial fill, disconnect, restart | No duplicate order across crash windows |
| 4. Fluss integration | Intent consumer, projections, ledger, gate, fencing | Restart-safe command and projection recovery |
| 5. Arrow sandbox | Real request/status/update/fill/reconciliation behavior | Arrow protocol evidence complete |
| 6. Shadow mode | Read-only broker reports and Nautilus state comparison | Broker and Nautilus positions converge |
| 7. Controlled enablement | Two-person approval, rollback, audit, operational runbook | Live-money approval package complete |

## Explicit non-goals

The following must not be built as part of the Nautilus integration:

| Non-goal | Reason |
| --- | --- |
| Java OMS | Duplicates Nautilus order lifecycle |
| Separate Java production position projector | Creates competing position truth |
| Direct Arrow calls from Flink | Couples strategy to money movement |
| Direct Arrow calls from Fluss clients | Bypasses the execution service and safety gate |
| Independent retry engine | Can duplicate an unknown broker order |
| Independent fill-deduplication engine | Conflicts with Nautilus reconciliation |
| Independent PnL calculator | Can disagree with Nautilus position arithmetic |
| Babysitter direct broker path | Bypasses OMS, risk, gate, and audit |

## Architecture

```text
Fluss (immutable execution intent · Safety_Halt_Requests · owned control/projection tables)
   │  Fluss reader (custom glue — tails execution intent, issues Nautilus order commands)
   ▼
Nautilus Engine            ← the execution/position core
   │  OMS · position engine · risk engine · event store · reconciliation · fill dedup
   │  ExecutionClient adapter (thin — maps bridge JSON ↔ Nautilus messages/events)
   ▼
go-arrow bridge (localhost)  ← the ONLY component that talks to Arrow
   │  go-arrow SDK (auth · PlaceOrder/Modify/Cancel · order-updates WS · orders/trades/positions)
   ▼
Arrow broker (edge.arrow.trade)
```

**Single-writer principle:** the go-arrow bridge is the only component with broker credentials and
the only component that can physically reach Arrow. Nautilus commands the bridge; nothing else can.
**Truth-authority (resolved by DEC-042):** Nautilus is authoritative for live order/fill/position
behavior and reconciliation. Its event processing is the execution state machine; Fluss stores
immutable execution intent, custom control state, and projections of Nautilus events for platform
consumers. The Fluss projections are durable read models and integration surfaces, not a second
production OMS or position engine. The Nautilus event store is an execution-history and replay
boundary, but its current early-alpha status requires dedicated durability, backup, verification,
and recovery evidence before live use. The current audit target is at least one year, or longer
under the approved retention policy.

## Component map

| Component | Responsibility | Source |
| --- | --- | --- |
| Fluss reader | Tail immutable execution-intent rows; issue validated Nautilus order commands; validate schema/version; fail closed on discontinuity | **Custom glue** |
| Nautilus OMS | Order lifecycle state machine, order types, contingency (OTO/OCO/OUO), duplicate suppression, multi-client routing | Nautilus (`execution`/`model` crates) |
| Nautilus position engine | Fill-derived position state, weighted entry/exit, PnL, FLAT/OPEN/REDUCING/CLOSED, `trade_context_id` grouping | Nautilus (`portfolio`/`model`) |
| Nautilus risk engine | Pre-trade checks: price/quantity/expiry validation, rate limits, max notional, trading-state kill switch | Nautilus (`risk`) |
| Nautilus reconciliation | Mass-status reconcile on connect, open-order checks, position consistency, inferred fills, **fill dedup** | Nautilus (`live`/`execution`) |
| Nautilus event store | Append-only audit of commands/events/reports/correlations; snapshot + tail-replay recovery; incident replay | Nautilus (`event_store`) |
| Nautilus adapter | Thin `ExecutionClient`: bridge HTTP ↔ submit/modify/cancel/reports; bridge WS events ↔ `ExecutionReport`s; symbol/status mapping | **Custom glue** (thin) |
| go-arrow bridge | Auth (TOTP/appID-token), `PlaceOrder`/modify/cancel, order-updates WS re-publish, orders/trades/positions/margin reads | **Custom glue** (wraps pinned go-arrow SDK) |
| Two-person gate + fencing | `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED`, epochs, two distinct approvals, per-partition fencing token | **Custom glue** |
| Projection sinks | Nautilus event store → Fluss `Fills`, `Order_Lifecycle`, `Positions`, `Execution_Gate/Attempts/Correlation/Audit`, ledger; quarantine path | **Custom glue** |
| Control API | Authenticated halt / reconcile / approval commands (two-person) | **Custom glue** |
| Telemetry | OTLP metrics/logs → OpenObserve; bridge + engine readiness | Custom + Nautilus-native |

## Capability mapping

| Requirement family | Behavior needed | Provided by |
| --- | --- | --- |
| AC — postback intake, correlation, quarantine, ledger | Decode Arrow order-updates WS; adopt orders the platform did not create here; immutable audit; quarantine unknowns; crash recovery | go-arrow bridge (`OrderStream`) + Nautilus external-order adoption + event store; **correlation mapping + quarantine path custom** |
| AC — lifecycle + position projection | Per-order state transitions, no regression on stale evidence; fill-derived positions with weighted values | Nautilus OMS + position engine; **UNKNOWN/regression policy custom** |
| BB — position observation | Consume `Positions` state; observe freshness; **MVP emits zero actions**; fail closed on `POSITION_ACTIONS_ENABLED=true` | Nautilus position events + a no-op observer strategy; **fail-closed guard custom** (Flink scaffold already implements it) |
| EXE — gate/attempts/audit | Durable gate, two-person resume, attempt phases, no blind retry on UNKNOWN, immutable audit, policy-controlled reconstruction for at least one year | **Gate + fencing + attempt/correlation glue custom** on Nautilus; audit via Nautilus event store + approved encrypted retention storage |
| EXE — broker side effects | Sole path to Arrow; verified acceptance/rejection only; timeout/disconnect → halt + reconcile | go-arrow bridge (single writer) + Nautilus reconciliation |

## State machines

### Gate (custom — the money gate)

```text
HALTED → RECONCILING → APPROVAL_PENDING → ENABLED → HALTED
```

- Initial state is always `HALTED`, epoch 0; every accepted transition increments the epoch by 1.
- Two distinct authorized operators approve the same gate epoch + evidence hash before `ENABLED`;
  automatic resume is prohibited (DEC-019).
- Any uncertainty (unknown outcome, fencing loss, storage uncertainty, changelog gap) moves the
  gate to `HALTED`; a safe halt received while already `HALTED` is recorded idempotently.
- Every broker-facing command re-verifies gate state + epoch + fencing token immediately before
  submission.

Startup/resume sequence: load the version/configuration matrix → connect owned Fluss state →
verify schema versions and audit append capability → verify changelog continuity and consumer
position → acquire/fence the `execution_partition_id` lease → start or restore `HALTED` if any
state is uncertain → validate bridge/Arrow REST contract and reachability **without placing a live
order** → reconcile unknown attempts, broker orders, fills, and positions → enter
`APPROVAL_PENDING` only after reconciliation passes → require two distinct authenticated approvals
of the same epoch/evidence hash. Process health never implies trading readiness.

Safety-halt rules: `halt_request_id` is a deterministic canonical hash of the request tuple
(account/partition scope, source component/instance, reason, detection time, source epoch,
evidence hash, schema version); a supplied ID that does not match the digest is malformed and
rejected. Duplicate IDs are idempotent (`DUPLICATE`, never a double epoch increment). Cross-scope
requests are rejected and audited. Stale `source_epoch` is rejected — the greatest seen valid
epoch is tracked per `(source_component, source_instance, execution_partition_id)`, and the
maximum is updated after validation even when the gate is already `HALTED`, so later lower epochs
cannot become acceptable. An accepted request enters `HALTED`, increments the gate epoch once, and
records source/evidence in audit. The order path independently detects stale mandatory health even
if the halt-request stream is unavailable.

### Attempt protocol (custom, on Nautilus order state)

```text
no active attempt → PREPARED (request hash + client ref + gate epoch + fence)
                 → SUBMITTING → ACCEPTED | REJECTED | CANCELLED | UNKNOWN
```

- A duplicate `(instruction_id, request_hash)` returns the existing attempt — never a second
  submission. Changed content under an existing instruction identity is a contract violation:
  quarantine, audit, halt.
- Intake validation (reader): schema/version, canonical identity/content hash, expiry/freshness,
  supersession/cancellation, and no unresolved attempt/request-hash conflict — enqueue only if
  gate and fencing permit (applies to future `Position_Actions` and control rows).
- `UNKNOWN` is non-terminal: it blocks new submissions and resolves only through explicit
  reconciliation — never auto-retry (DEC-011, DEC-030).

### Order lifecycle (Nautilus OMS → normalized vocabulary)

Nautilus's order events (`Initialized → Submitted → Accepted → PendingUpdate/Updated →
PendingCancel/Canceled → Rejected → Filled/PartiallyFilled → Expired`, plus `Triggered`,
`Released`, `Emulated`, `Denied`) normalize to the contract vocabulary `PENDING`, `PARTIAL`,
`FILLED`, `CANCELLED`, `REJECTED`, `UNKNOWN`. Exact-duplicate source events produce no duplicate
effect (Nautilus fill dedup); older versions cannot regress state; conflicting/regressive evidence
moves the record to `UNKNOWN`, halts affected flow, and alerts.

The per-update transition protocol is explicit (custom policy layered on the OMS):
1. Exact-duplicate source event → no duplicate effect.
2. Older source version → stale-evidence metric/audit; no regression.
3. Equal version with conflicting content → `UNKNOWN`, quarantine, halt.
4. Terminal-state regression → reject, quarantine, halt.
5. Quantity regression or impossible totals → `UNKNOWN`.
6. Unknown broker status → quarantine and `UNKNOWN`.

Every update carries source event ID, verified broker timestamp when available, receive
timestamp, transition version, quantities, prices, status-mapping version, and correlated IDs.

### Capture-path identity and correlation

Every received postback receives a platform `postback_event_id` and versioned
`postback_fingerprint` (a bounded logical duplicate hint; repeated deliveries may exist in
immutable audit). Same `postback_event_id` with different content is a contract violation:
quarantine, audit, alert. No `postback_seq` or broker-global event identity is assumed.

Correlation resolves in this order:
1. Verified `broker_order_id` mapping in `Order_Correlation`.
2. Verified echoed `client_order_ref` mapped to one attempt/instruction.
3. Evidence-approved reconciliation query.

Symbol, quantity, price, and timestamp proximity are never sufficient. Missing/ambiguous
correlation → `Postback_Quarantine` + halt affected order flow + alert. The mapping is bijective:
one attempt maps to at most one broker order; one client reference maps to one attempt; one
broker order maps to one verified attempt — violations are ambiguous and safety-relevant. The
capture path reads `Order_Correlation` but never mutates execution mapping state except through
the explicit reconciliation interface owned by the order path.

### Position states (Nautilus position engine)

`FLAT`, `OPEN`, `REDUCING`, `CLOSED`, `UNKNOWN`. `position_id` is minted on the first uniquely
correlated fill; `trade_context_id` groups related entry/trim/exit orders. Conflicting fills,
ambiguous side, quantity underflow, or missing correlation → `UNKNOWN` + halt of affected position
actions. Order completion is not position closure.

### Babysitter (MVP = no-op)

A separate observer consumes position events, checkpoints observation state (latest accepted
position version per `position_id`, last source offset, freshness timestamp, schema version,
no-op reason counters — never historical snapshots, ticks, candles, or strategy state), and emits
**zero actions**. `POSITION_ACTIONS_ENABLED` must fail closed at startup for any value other than
`false`. A healthy Babysitter never implies permission to trade. Future `Position_Actions` route
through the same gate/attempt/fencing protocol as entry instructions.

Failure behavior per mode: missing `Positions` schema → not ready, no output; changelog gap →
degraded, no action, alert; checkpoint failure → not ready, no action; stale/conflicting position
→ observe/audit, no action; restart with unknown state → restore or remain not ready, no action;
`POSITION_ACTIONS_ENABLED=true` → fail closed at startup. Multiple updates for one position retain
only the latest version in Babysitter state.

## Reconciliation and unknown outcomes

- On connect, mass-status reconciliation: order-status reports, fill reports, and position-status
  reports are generated through the adapter and applied to the OMS (Nautilus `reconcile_execution_mass_status`).
- Open-order checks re-query orders without a terminal event; in-flight tracking detects
  submissions that never got an ack; recent-fills dedup prevents double-counted fills.
- Failure classification is explicit: `not_sent`, `ambiguous`, `venue_rejected` — only a verified
  acceptance or verified rejection is terminal; anything else is `UNKNOWN` → halt + reconcile.
  Verified shapes (bridge `PlaceOrder` response envelope): HTTP 200 + `status:"success"` +
  nonblank `data.orderNo` → acceptance; HTTP 400/409/422 + `status:"error"` + nonblank `message`
  → rejection; HTTP 401/403/408/429/5xx, transport failure, missing body, or any other
  combination → `AMBIGUOUS`/`UNKNOWN` — never rejection, never retry.
- `client_order_ref` is deterministic and replay-safe: the same attempt always yields the same
  reference (canonical hash of `format_version|instruction_id|execution_attempt_id`, 14 ASCII
  chars, fits Arrow's 16-char `remarks`), so correlation and duplicate suppression never depend on
  wall-clock or process state.
- Missing reconciliation evidence blocks live-money enablement (test IDs `ARROW-REST-001`/
  `ARROW-REST-002`, DEC-023 endpoints).
- Projection-ledger recovery (capture path): durable `Postback_Projection_Ledger` KV keyed by
  `postback_event_id` tracks `RECEIVED → AUDIT_WRITTEN → LIFECYCLE_APPLIED →
  POSITION_APPLIED_OR_NOT_REQUIRED → COMPLETE` with `UNKNOWN` for conflict/regressive/ambiguous
  evidence; every step is idempotent; restart scans non-complete records and resumes from
  persisted evidence; a duplicate immutable audit row never authorizes a duplicate state effect.
  The ledger is Fluss-owned state (never in-memory only) and coexists with Nautilus event-store
  replay — the event store replays engine state; the ledger tracks projection completion into
  Fluss.

## Audit and event store

Every money-moving and safety event flows through the Nautilus event store (append-only,
replayable): commands, order events, venue reports, correlations, approvals, halts. Cache
snapshots + tail-replay provide stable restarts and deterministic incident replay. The audit target
is at least one year, or longer under the approved retention policy, using encrypted storage,
integrity verification, access governance, and an R2 bucket-lock or equivalent immutability
mechanism where deployed.

The audit envelope for every money-moving or safety event carries: audit ID/type/schema version;
all relevant domain identities; gate state/epoch before and after; actor/service/operator
identity; engine/fencing identity; request/response hashes with redacted evidence; broker
status/order ID when known; changelog offset/continuity evidence; configuration/version
snapshot; evidence hash and UTC/monotonic timestamps.

**Postback evidence is retained; credentials are not.** The capture path keeps the original
postback bytes/text and payload hash in immutable audit with decoder/schema version (REQ-AC-001,
REQ-AC-004) — that is evidence preservation, not credential storage. The execution-side rule is
narrower and unchanged: broker credentials, tokens, and raw request/response bodies are never
stored in audit (only redacted hashes and summaries).

## Configuration contract

| Key | Rule |
| --- | --- |
| `ARROW_APP_ID` / `ARROW_APP_SECRET` / `ARROW_TOKEN` / `ARROW_USER_ID` / `ARROW_PASSWORD` / `ARROW_TOTP_KEY` | Bridge credentials — secret refs only, never in Nautilus, never committed |
| `ARROW_REST_URL_TO_BE_VERIFIED` | Bridge → Arrow base URL; no unsafe production default |
| `ARROW_TIMEOUT_PROFILE_TO_BE_VERIFIED` / `ARROW_RETRY_POLICY_TO_BE_VERIFIED` | Timeout + classification; unknown outcomes never blind-retried |
| `BROKER_CLIENT_REFERENCE_FORMAT_TO_BE_VERIFIED` | Length/charset/echo evidence for the ≤16-char `client_order_ref` (carried in `remarks`) |
| `BRIDGE_LISTEN_ADDR` | Localhost bind for the go-arrow bridge (loopback only) |
| `EXECUTOR_ACCOUNT_SCOPE` / `EXECUTION_PARTITION_ID` | Fencing and gate scope |
| `FENCING_LEASE_PROFILE_TO_BE_DEFINED` | Durable single-owner protocol |
| `GATE_INITIAL_STATE` | Must be `HALTED`; `GATE_EPOCH_POLICY_VERSION` monotonic |
| `MAX_DECISION_LAG_MS` / `MAX_PROJECTION_LAG_MS` / `MAX_PENDING_PROJECTION_RECORDS` | Staleness and backlog bounds |
| `POSTBACK_FINGERPRINT_VERSION` / `CORRELATION_POLICY_VERSION` / `LIFECYCLE_TRANSITION_VERSION` / `POSITION_PROJECTION_VERSION` | Capture-path versioned policies (evidence-gated) |
| `BROKER_STATUS_MAPPING_VERSION_TO_BE_VERIFIED` / `BROKER_CLIENT_REFERENCE_ECHO_TO_BE_VERIFIED` | Status vocabulary + reference echo evidence |
| `POSITION_ACTIONS_ENABLED` | Must be `false` for MVP; any other value fails startup |
| `BABYSITTER_JOB_VERSION` / `POSITIONS_SCHEMA_VERSION` / `POSITION_FRESHNESS_POLICY_VERSION` | Versioned observation policy |
| `CHECKPOINT_PROFILE_ID` / `POSITION_ACTIONS_SCHEMA_VERSION_TO_BE_DEFINED` | Babysitter checkpoint profile; future action schema |
| `AUDIT_SCHEMA_VERSION` / `ARROW_REQUEST_SCHEMA_VERSION_TO_BE_VERIFIED` | Immutable audit envelope / approved API contract version |
| `NAUTILUS_RISK_MAX_ORDER_SUBMIT` / `NAUTILUS_RISK_MAX_NOTIONAL_PER_ORDER` | Pre-trade risk limits |
| Fluss table/schema/version identifiers | Reader + projection sinks |

Missing protocol/status/correlation/fencing evidence keeps the execution core not ready for live
money.

## Backpressure and readiness

Readiness is false for: broker disconnect, bridge unavailability, Fluss unavailability, changelog
gap, schema mismatch, gate in a non-`ENABLED` state, unknown outcome unresolved, projection
backlog above policy, failed recovery, correlation invariant failure, or clock violation. Existing
accepted evidence must remain recoverable.

## Required telemetry

Postback/byte rate, decode failures, fingerprint duplicates, correlation success/quarantine by
reason, lifecycle transitions/rejections, stale/regressive/conflicting events, projection
backlog/lag/retries, independent-write failures, recovery duration, positions by state,
checkpoint health, restart count, no-op decisions by reason, action-enabled guard status, gate
state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, duplicate suppressions,
request conflicts, approvals, fencing lease, consumer lag, Arrow REST latency/status, bridge
connected, clock offset, audit append status. Telemetry is native-first
(Nautilus + bridge OTLP) per CHG-023's direction.

## Required tests

Canonical IDs from `11-testing-and-release.md` (test design sections below):

- **Action Capture:** `AC-UNIT-001`–`AC-UNIT-005`, `AC-INT-001`, `BROKER-PB-001`, `AC-FAIL-001`–`AC-FAIL-004`, `AC-REC-001`.
- **Babysitter:** `BAB-UNIT-001`/`BAB-UNIT-002` (**IMPLEMENTED 2026-08-15** — `BabysitterJobTest`, no-op topology + fail-closed flag), `BAB-INT-001`, `BAB-HARNESS-001`, `BAB-FAIL-001`, `BAB-FAIL-002`, `BAB-OPS-001`.
- **Executor:** `EXE-UNIT-001`, `EXE-UNIT-003`–`EXE-UNIT-006`, `EXE-INT-001`, `EXE-FAIL-001`–`EXE-FAIL-006`, `EXE-OPS-001`, `EXE-AUDIT-001`, `ARROW-REST-001`, `ARROW-REST-002`.

Mapping note: behaviors native to Nautilus (order state machine, position projection, fill dedup,
reconciliation, event-store audit) are proven by Nautilus's own test suites plus adapter-level
integration tests; behaviors that remain custom (gate, fencing, two-person resume, correlation to
`Execution_Attempts`/`Order_Correlation`, quarantine, projection sinks, bridge endpoints) require
dedicated tests under the canonical IDs above. The order REST path in the go-arrow SDK is
currently **untested** — the bridge `PlaceOrder` endpoint requires Arrow-sandbox smoke tests
before it is trusted.

## Definition of done

The execution core is complete when: the go-arrow bridge is the only component that can reach
Arrow (proven by test); every trade row is either executed with a verified outcome or halted as
`UNKNOWN`; every postback is either correlated or quarantined; partial writes recover after
restart (event-store replay); stale evidence cannot regress state; positions and orders remain
separate aggregates; `UNKNOWN` never releases capacity and never auto-retries; the two-person gate
and fencing are enforced; no crash-window test duplicates an order; the policy-controlled audit
reconstruction (`EXE-AUDIT-001`) covers at least one year or the approved longer period; the
Babysitter emits zero actions and fails closed; and
all broker calls remain disabled until the release evidence package approves enablement.

## Verification mapping

- [Action Capture test design](./11-testing-and-release.md#action-capture)
- [Babysitter test design](./11-testing-and-release.md#babysitter)
- [Executor test design](./11-testing-and-release.md#executor)

## Open gates and upstream impact

1. **~~Upstream re-scope pending~~ — RESOLVED 2026-08-18 (CHG-028):** build contracts
   `05-babysitter.md`, `06-action-capture.md`, `07-executor.md`, requirements
   `02-functional/05/06/07`, and DEC-006 are re-scoped to the Nautilus + go-arrow bridge design
   (dated banners in each file).
2. **~~Truth authority~~ — RESOLVED 2026-08-19 (DEC-042/DEC-043):** Nautilus owns live execution behavior; Fluss owns immutable intent, control state, and queryable projections. The Nautilus event store's production durability, approved-policy retention, and recovery evidence remain open gates.
3. **Broker evidence:** Arrow sandbox order round-trip (place → update stream → fill), REST
   reconciliation endpoints, and client-reference echo (Level 3) remain the live-money gates.
4. **Existing code:** the Flink `BabysitterJob` scaffold and the `common` Agent-2 safety-core
   pieces (gate/attempt rules in `com.trading.common.schema.execution`) are inputs — the safety
   rules carry over to the custom gate layer; the no-op Babysitter behavior is already proven.
