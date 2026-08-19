# Final Implementation Plan — Nautilus Execution Service Integration

> **Status audited 2026-08-19:** This document is the implementation blueprint **and the live
> implementation ledger**. The repository-backed status audit below distinguishes implemented
> slices, partial scaffolding, and work that has not started. It does not authorize live-money
> behavior.
>
> **Primary dossier:** [`05-execution-core.md`](./05-execution-core.md). This plan expands that
> dossier into executable work packages and resolves the remaining implementation ambiguity around
> the Fluss client boundary.

## 1. Project Understanding

### Objective

Complete the missing execution path of the streaming trading platform without creating a second
OMS, position engine, reconciliation engine, or PnL calculator in Flink, Java, or Fluss.

The final system must:

```text
market data → Ingestion → raw_table_1 → Signal Flink job
  → immutable execution intent in Fluss
  → custom Fluss gateway
  → long-lived Rust Nautilus Execution Service
  → custom Rust ExecutionClient
  → private Go Arrow execution bridge
  → Arrow REST/order-update APIs
  → Nautilus order/fill/position state
  → projection gateway → Fluss execution projections
  → Babysitter observation
```

Live-money placement remains blocked until the evidence gates in this plan pass.

### Current implementation state

| Area | Current state | Evidence |
| --- | --- | --- |
| Market-data bridge | Implemented Go bridge using the pinned vendored `go-arrow` SDK; authenticates, consumes Arrow HFT data, emits NDJSON, handles reconnects and shutdown | `code/02_services/01_ingestion/go-bridge/main.go`, `go.mod`, bridge tests |
| Java ingestion | Substantially implemented; decodes bridge output, validates schema, fingerprints, and appends to Fluss | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` and tests |
| Signal Flink job | Implemented feature path and placeholder signal path; Flink keyed dedup state, event-time candles, forming bars, signal LOG/KV outputs, checkpoints and restart policy exist | `code/02_services/02_compute/.../SignalJob.java`, `SignalJobConfig.java`, `04-signal-job.md` |
| Execution intent | Dedicated `Execution_Intent` DDL, manifest entry, Java layout, pure builder/mapper, and duplicate/violation protocol exist; no Signal producer or feature flag is wired | `27_execution_intent.sql`, `ExecutionIntent*.java`, `ExecutionIntent*Test.java`, `SignalJob.java`, `SignalJobConfig.java` |
| Safety consumer | `SafetyHaltJob` consumes `Safety_Halt_Requests`, applies slot-scoped safety state, and has live-dev evidence; it is not the execution gate | `SafetyHaltJob.java`, `SafetyStateTracker`, `SuppressionGate` |
| Babysitter | Submittable MVP no-op shell, but currently uses `fromElements(0L)` rather than the `Positions` changelog | `BabysitterJob.java`, `BabysitterJobTest.java` |
| Execution service | Python scaffold only; `main.py` raises `NotImplementedError` and directly describes obsolete standalone Executor behavior | `code/02_services/04_executor/main.py`, `README.md` |
| Position model | Pure-JVM `PositionProjector`/`PositionProjectorDriver` and Fluss state-store helpers exist for parity and earlier design work; they are not the production authority | `code/common/src/main/java/com/trading/common/schema/position/` |
| Attempt model | `InMemoryAttemptStore`, `AttemptRecord`, `GateTransitionValidator`, and audit/hash-chain helpers exist and are unit-tested; no Fluss-backed execution service uses them | `code/common/src/main/java/com/trading/common/schema/execution/`, `model/` |
| Arrow execution bridge | Offline fake HTTP/WebSocket lifecycle and resolved Compose network policy now exist in a separate Go module; real sandbox authentication/re-authentication and full T8 service wiring remain blocked | `code/02_services/06_execution_bridge/`, `code/01_platform/04_scripts/execution_network_check.py`, `CHG-043.md` |
| Market-data mock broker | `MockArrowServer` remains a market-data TCP/NDJSON fixture only; order-path fake HTTP/WebSocket behavior is isolated in the Go execution-bridge tests and does not change market-data semantics | `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java`, `code/02_services/06_execution_bridge/go-bridge/fake_arrow_broker_test.go` |
| Fluss execution tables | DDLs exist for fills, lifecycle, positions, gate, attempts, correlation, audit, quarantine, ledger, halt requests, and the new intent LOG; integrated gateway writers/readers do not exist | `code/01_platform/02_sql/ddl/08–18`, `27_execution_intent.sql` |
| Local runtime | Flink/Fluss/ingestion infrastructure runs in Compose; compute and executor services are commented or only partially wired | `code/01_platform/01_docker/docker-compose.yml`, `08-local-compose.md` |
| Nautilus reference | Local checkout is clean on `develop` at `74d57e7e...`; Cargo crates compile and event-store tests pass, but the checkout is not a stable final v2 release | `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/nautilus_trader` |

### Existing safety and state invariants

These invariants are already required and must not be weakened:

1. Unknown external outcomes are `UNKNOWN`, never rejection, success, or permission to retry.
2. An `instruction_id` is immutable. Same ID plus a different content hash is a contract violation.
3. `execution_attempt_id`, `client_order_ref`, `broker_order_id`, `trade_id`, `position_id`, and
   `trade_context_id` remain separate identities.
4. `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED` is the only enablement path.
5. A fresh installation, uncertain restart, lost fence, missing state, or unresolved reconciliation
   starts or remains `HALTED`.
6. Babysitter emits zero money-moving actions in MVP.
7. Fluss projections are durable read models; they do not become a second OMS or position engine.
8. Audit remains immutable, encrypted, integrity-verifiable, access-controlled, offloaded, and
   retained for at least one year or longer under the approved policy (DEC-043).

## 2. Current Architecture

### Components and responsibilities

| Component | Current responsibility | Important implementation detail |
| --- | --- | --- |
| Arrow market-data bridge | Arrow HFT authentication/subscription/reconnect and NDJSON output | Go, vendored `go-arrow`, one approved HFT socket, credentials in the bridge process |
| Ingestion | Arrow bridge NDJSON → validation/fingerprint → Fluss raw LOG | Java 17; Fluss 0.9.1 connector/client; append acknowledgement and quarantine paths |
| Flink Signal job | Fluss raw LOG → keyed dedup → event-time candle/forming-bar/signal processing → Fluss sinks | Flink 2.2.1; exactly-once checkpoints; native Fluss sinks; no broker side effects |
| Fluss | Durable LOG/KV state and changelog integration plane | Fluss 0.9.1-incubating; DDL manifest and parity checks exist |
| SafetyHaltJob | Consumes halt-request changelog and applies slot safety state | Separate from the execution gate; currently a compute-side safety consumer |
| Babysitter | Intended position observer | MVP no-op; input wiring is incomplete |
| Executor scaffold | Intended execution path | Not implemented; must be replaced by the integrated Rust service plus custom glue |
| MockArrowServer | Synthetic market-data source | Not an order broker simulator; do not extend it into the order bridge without a separate contract |
| OpenObserve/OTel | Operational telemetry | Telemetry outage must not erase local durable execution evidence |

### Current data flow

```text
Arrow HFT WebSocket
  → Go market-data bridge
  → Java IngestionService
  → raw_table_1 LOG
  → SignalJob
      → Flink keyed fingerprint dedup state
      → feature_candles_15s KV
      → forming_bar KV
      → Signal_Candidates append LOG
      → Signal_Candidates_current KV projection
  → [execution-intent producer is currently absent/disabled]
  → [execution core is currently absent]
```

### Current state ownership

| State | Current owner | Target owner |
| --- | --- | --- |
| Fingerprint dedup working state | Signal Flink keyed state | Unchanged |
| Candle and forming-bar durable state | Fluss KV | Unchanged |
| Signal candidate audit/current state | Signal job + Fluss | Unchanged |
| Order lifecycle | No production owner | Nautilus OMS, projected to Fluss |
| Fills and fill deduplication | No production owner; Java helpers exist | Nautilus execution/portfolio path |
| Positions/PnL | Java parity projector only | Nautilus portfolio/position engine |
| Safety gate | Pure-JVM validators/specification only | Custom execution control backed by Fluss and enforced before every bridge command |
| Broker reconciliation | No production owner | Nautilus reconciliation through the custom bridge adapter |
| Queryable execution state | DDL exists, no complete writers | Projection gateway fed by Nautilus events |

### Current failure/restart behavior

- Signal data-path restart is governed by Flink checkpoint/savepoint configuration and native
  restart strategy.
- Fluss source/sink semantics are version-pinned but cross-table atomicity is not assumed.
- The execution scaffold does not consume instructions, persist attempts, call a bridge, reconcile,
  or recover order state.
- Babysitter currently cannot prove position-changelog recovery because it uses a marker source.
- No component currently proves the crash window between broker acceptance and durable acknowledgement.

## 3. Requirements

### Functional execution requirements

The implementation must satisfy `docs/02_requirements/02-functional/07-executor.md`:

- `REQ-EXE-001`: own durable gate, attempts, correlation, and audit state in Fluss.
- `REQ-EXE-002`: enforce the gate state machine and halt on uncertainty.
- `REQ-EXE-003`: require two distinct authenticated approvals for the same epoch/evidence hash.
- `REQ-EXE-004`: consume a durable immutable execution-intent stream distinct from the retired
  the old `Trade_Decisions` feed.
- `REQ-EXE-005`: persist `PREPARED` attempt identity before broker command; classify outcomes explicitly.
- `REQ-EXE-006`: correlate only by verified broker ID, echoed client reference, or evidence-approved
  reconciliation; never by proximity.
- `REQ-EXE-007`: route future structured `Position_Actions` through the same safety protocol.
- `REQ-EXE-008`: enforce one fenced active owner per `execution_partition_id`.
- `REQ-EXE-009`: expose readiness and execution safety telemetry.
- `REQ-EXE-010`: pass crash-window, duplicate, fencing, approval, reconciliation, and audit tests.
- `REQ-EXE-011`: consume authenticated idempotent `Safety_Halt_Requests`.
- `REQ-EXE-012`: reject stale fencing ownership immediately before every broker command.
- `REQ-EXE-013`: prove Arrow reconciliation capability and consistency behavior.

### Capture and position requirements

`REQ-AC-001` through `REQ-AC-013` require postback byte/hash preservation, platform postback
identity, verified correlation, immutable fills, lifecycle projection, a durable projection ledger,
position arithmetic, explicit precedence, quarantine, and recovery.

Nautilus owns the order and position state machine. The custom layer owns normalization, policy,
quarantine, projection idempotency, and Fluss writes.

### Babysitter requirements

`REQ-BB-001` through `REQ-BB-008` require a separate checkpointed Flink job consuming versioned
`Positions` state, emitting zero actions in MVP, failing closed on bad input/configuration, and
never calling Arrow.

### Data and retention requirements

- Event rows are immutable; corrections append superseding evidence.
- KV projections use source event/version checks and explicit ownership.
- Fluss source data remains live for at least three complete trading days and cannot expire before
  verified EOD offload.
- Money-moving audit is retained for at least one year or longer under approved policy; no fixed
  seven-year duration remains active.
- Audit deletion requires policy evidence, applicable legal-hold handling, and two-person authorization.

### Flink/Fluss requirements

- Preserve per-key ordering and explicit event-time/watermark behavior for the existing Signal job.
- Do not put broker calls in a Flink operator.
- Do not claim cross-table atomicity between Fluss LOG/KV projections.
- Use connector behavior verified against Flink 2.2.1 and Fluss 0.9.1-incubating.
- Treat checkpoint/savepoint compatibility and operator UIDs as compatibility surfaces.

### Deployment and observability requirements

- Local Compose is sandbox-only and starts execution `HALTED`.
- Production target is four-VM Docker Swarm: three workload/HA VMs and one observability VM.
- Arrow credentials and network access are restricted to Go bridge processes; Rust and Java never
  hold broker credentials.
- Process health, service readiness, job health, execution readiness, gate state, reconciliation,
  and audit durability are separate health dimensions.
- OpenObserve outage cannot erase local Nautilus/event and Fluss audit evidence.

## 4. Findings

| Finding | Root cause | Consequence | Disposition |
| --- | --- | --- | --- |
| Execution Core is absent | `04_executor/main.py` is a scaffold and raises `NotImplementedError` | No intent intake, gate enforcement, OMS, broker call, reconciliation, or projections | Replace the scaffold with the planned Rust service and custom gateways |
| Required intent stream is ambiguous | Existing `Trade_Decisions`/`trade_instruction_state` code is tied to removed ranking and is feature-gated off, while `REQ-EXE-004` requires a distinct intent stream | A coding agent could incorrectly revive ranking or consume the wrong feed | Add a dedicated `Execution_Intent` LOG; retain the existing hash-index table only as a re-scoped content index |
| No verified Rust Fluss client boundary exists | The current Fluss integration is Java; the Nautilus Rust crates compile independently but do not prove Fluss consumption | Direct Rust-to-Fluss implementation would be speculative and high-risk | Use a small Java Fluss gateway for intent/control/projection transport; keep execution authority in Rust Nautilus |
| Execution Arrow bridge is only an offline slice | The Go module now proves the fake HTTP/WebSocket lifecycle and resolved bridge-only egress policy; `live` mode still performs only initial token/AutoLogin setup | The order boundary is deterministic offline but not sandbox/prod verified, and automatic token refresh is absent | Keep real sandbox authentication blocked; retain T3 as offline partial until T8 adds cross-container runtime probes and T9 supplies external broker evidence |
| Intent contract has no producer | `ExecutionIntentBuilder.fromCandidate` rejects the current null `trade_context_id`; `SignalJob` and `SignalJobConfig` contain no `EXECUTION_INTENT_ENABLED` path | The physical contract cannot receive production rows and must not be enabled | Implement the upstream context/account/partition contract and a disabled-by-default Signal producer in T1 |
| Java position projector exists | `PositionProjectorDriver` implements arithmetic and Fluss store helpers | Leaving it as production authority would create competing truth | Use it only as a differential-test oracle, then remove it from the production execution path |
| Pure-JVM safety/attempt rules exist without durable wiring | `GateTransitionValidator` and `InMemoryAttemptStore` encode policy but have no Fluss-backed service integration | Tests can pass while runtime state is absent | Reuse the rules and implement durable gateway-backed writes |
| Babysitter input is a marker source | `BabysitterJob.buildTopology()` uses `fromElements(0L)` and has TODOs | Position observation/recovery is not proven | Replace only the marker source/observation state; retain no-op action boundary |
| Local execution services are not deployed | Compose comments out compute/executor/action services | Local integration cannot exercise the target topology | Add explicit sandbox-only service wiring and health checks |
| Nautilus event store is early alpha | `crates/event_store/README.md` explicitly warns API instability; it is single-node embedded `redb` | It cannot independently satisfy external durability/retention policy | Use it for engine history/replay only; pair with Fluss projections and separately verified policy-controlled audit storage |
| Nautilus ref is moving | Local checkout is `develop`, Cargo packages are `0.62.0`, metadata says `v2.0.0rc3`, Git describes a v1-derived tag | Builds can drift and APIs can change | Pin the exact audited commit in the service build before implementation |
| Cross-table atomicity is unavailable | Fluss LOG/KV writes are independent unless a pinned connector contract proves otherwise | Partial projection writes are possible | Use `Postback_Projection_Ledger`, idempotent event IDs, and recovery scans |
| Job launcher had legacy names/placeholders | `submit-jobs.sh` previously uploaded without running jobs; cleanup now submits Signal/Babysitter and checks `RUNNING` | Checkpoint-completion verification remains incomplete | Keep launcher validation as a prerequisite for local runtime; add checkpoint evidence in runtime task |

## 5. Target Architecture

### Target topology

```text
Signal Flink job
  └─ Execution_Intent LOG + trade_instruction_state KV (feature-gated)
       │ Fluss Java client
       ▼
Execution Gateway (Java custom glue)
  ├─ validates schema, hash, scope, expiry, and immutable identity
  ├─ reads/writes Execution_Gate, Execution_Attempts, Order_Correlation,
  │  Safety_Halt_Requests, and Postback_Projection_Ledger
  ├─ sends intents/control to Nautilus over private authenticated protocol
  └─ writes idempotent Nautilus event projections to Fluss
       │ private authenticated protocol
       ▼
Nautilus Execution Service (Rust)
  ├─ LiveNode/ExecutionEngine + custom ExecutionClient
  ├─ OMS and order event lifecycle
  ├─ risk engine
  ├─ portfolio/position engine and PnL
  ├─ reconciliation manager
  ├─ event-store history/replay (supplementary; early-alpha limitation recorded)
  ├─ custom gate/fencing/attempt orchestration calls to gateway
  └─ emits normalized execution events
       │ private authenticated protocol
       ▼
Execution Arrow Bridge (Go)
  ├─ sole order-path Arrow credential holder
  ├─ POST/modify/cancel through pinned go-arrow SDK
  ├─ order-updates WebSocket intake
  ├─ reconciliation reads: orders, trades, positions, order detail
  └─ normalized command/report envelopes
       │ TLS
       ▼
Arrow sandbox/broker

Nautilus events → Execution Gateway → Fluss:
  Fills LOG, Order_Lifecycle KV, Positions KV, Execution_Audit LOG,
  Execution_Attempts KV, Order_Correlation KV, Postback_Quarantine LOG,
  Postback_Projection_Ledger KV

Positions KV → Babysitter Flink job → zero Position_Actions in MVP
Safety_Halt_Requests → Execution Gateway → Nautilus gate → no broker call when HALTED
```

### Responsibility boundaries

| Boundary | Owner | Explicit non-responsibilities |
| --- | --- | --- |
| Market data | Existing Go market-data bridge + Java ingestion | No orders, fills, positions, or OMS |
| Signal decision | Signal Flink job | No broker calls or broker-derived position state |
| Intent contract | Signal/Execution_Intent writer | No broker retry or order lifecycle |
| Fluss transport/projection | Java Execution Gateway | No OMS, risk arithmetic, position arithmetic, or broker credentials |
| Order/fill/position truth | Nautilus Rust service | No direct Fluss client assumption, no Arrow credentials |
| Arrow order transport | Separate Go execution bridge | No strategy, gate authorization, or position arithmetic |
| Safety gate/fencing | Custom gateway/service control | Nautilus risk engine is necessary but not a replacement for the platform gate |
| Position observation | Babysitter | No action in MVP and no direct broker path |

### New execution-intent contract

Create a dedicated append-only `Execution_Intent` LOG. It is not `Trade_Decisions` and is not a
decision result. The canonical row contains:

| Field | Rule |
| --- | --- |
| `instruction_id` | Deterministic immutable identity; primary logical correlation key |
| `candidate_id` | Source candidate identity |
| `trade_context_id` | Entry/trim/exit grouping |
| `account_scope_id` | Account isolation |
| `execution_partition_id` | Fenced service ownership |
| `instrument_token`, `exchange`, `symbol` | Canonical instrument mapping |
| `side`, `quantity`, `order_type`, `limit_price_paise`, `product_type`, `time_in_force` | Execution request |
| `strategy_id`, `strategy_version`, `configuration_version` | Source/version evidence |
| `created_ts`, `expiry_ts` | Freshness and expiry |
| `request_hash` | Canonical hash over the complete executable request |
| `supersedes_instruction_id` | Explicit supersession only; no in-place mutation |
| `schema_version` | Versioned contract |

The existing `trade_instruction_state` KV may be re-scoped as the one-row-per-`instruction_id`
canonical hash/index table, but it must not be described as a decision output. No producer may revive
the removed ranking/reservation path.

### Protocol envelopes

All private protocols use versioned JSON envelopes with a request/event ID, schema version,
correlation ID, scope IDs, timestamp, payload hash, and redacted error details. They must support:

- command acknowledgement distinct from broker acceptance;
- idempotent duplicate delivery;
- explicit `UNKNOWN`/ambiguous result;
- bounded request timeouts and backpressure;
- health/readiness endpoints that do not imply `ENABLED` trading state.

The bridge must echo the deterministic `client_order_ref` through Arrow `remarks`. The canonical
reference is the 14-character deterministic hash described in `05-execution-core.md`, constrained
to Arrow's 16-character limit.

## 6. Architectural Decisions

### Decision A — Rust service, not embedded Flink or per-order process

Use one long-lived Rust-native Nautilus service per execution partition/account scope. Do not embed
Nautilus in a Flink task and do not start a new process for each instruction. This preserves OMS,
portfolio, reconciliation, event processing, and restart continuity. Cost: an additional service and
Rust build/runtime boundary.

### Decision B — Java Fluss gateway, Rust Nautilus authority

Use a Java gateway for the Fluss client/projection boundary because the repository's supported Fluss
client path is Java and no Rust Fluss client has been verified. The gateway only transports and
persists contracts; it must not calculate positions or own order state. This is safer than inventing
a Rust Fluss connector and safer than making Java an OMS.

### Decision C — Separate execution Go bridge

Keep the existing market-data Go bridge behavior isolated. Add a separate Go execution bridge using
the same pinned `go-arrow` SDK source/commit. Both Go bridge processes are the only Arrow-facing
components; Rust, Java, Flink, and Fluss never hold Arrow credentials. This avoids coupling broker
order failures to market-data ingestion restarts.

### Decision D — Dedicated `Execution_Intent` LOG

Create a dedicated execution-intent LOG instead of reviving `Trade_Decisions`. The decision path was
removed and must stay removed. A new table makes the source contract explicit and prevents a future
agent from treating a candidate/decision row as a broker instruction.

### Decision E — Nautilus is live execution authority; Fluss is command/control/projection authority

Nautilus owns live order/fill/position/PnL/reconciliation behavior. Fluss owns immutable intent,
control state, and queryable projections. The projection tables are rebuildable read models, not a
second execution state machine.

### Decision F — Event store is supplementary, not the only audit system

Use Nautilus event-store capture/replay for engine history and incident replay. Do not claim its
current early-alpha, single-node embedded implementation independently proves backup, off-host
durability, or the policy-controlled one-year audit requirement.

### Rejected alternatives

| Alternative | Rejection reason |
| --- | --- |
| Python implementation of the Executor scaffold | Contradicts the approved Rust-native Nautilus boundary and duplicates execution logic |
| Direct Arrow calls from Flink/Java gateway | Couples strategy/storage to money movement and bypasses the bridge boundary |
| Java `PositionProjector` as production authority | Creates competing position truth with Nautilus |
| Reuse `Trade_Decisions` as execution intent | Keeps removed ranking semantics and violates `REQ-EXE-004` distinction |
| Extend market-data `MockArrowServer` into a broker OMS | Mixes market-data test behavior with execution-side-effect testing |
| Direct Rust Fluss integration without a verified client | Unsupported dependency assumption and new recovery surface |
| Automatic retry after timeout | Can duplicate an accepted broker order; reconciliation must resolve `UNKNOWN` first |

## 7. Scope

### In scope

- Pin and consume the audited Nautilus Rust crates.
- Create the dedicated execution-intent contract and producer path.
- Replace the Python execution scaffold with a Rust-native Nautilus service.
- Build the Java Fluss intent/control/projection gateway.
- Build a separate Go Arrow execution bridge.
- Implement the custom gate, attempt, correlation, fencing, quarantine, and projection ledger flow.
- Replace the Babysitter marker source with the `Positions` changelog while retaining zero actions.
- Add deterministic fake-broker, replay, restart, reconciliation, and sandbox evidence.
- Wire local Compose in sandbox-only `HALTED` mode.
- Add observability, readiness, policy-controlled audit retention, and release evidence.

### Explicitly unchanged

- Existing market-data bridge protocol, HFT subscription policy, and ingestion hot path.
- Flink Signal candle/window/dedup semantics and stable operator UIDs/max parallelism.
- Removal of the ~~ranking~~/~~reservation~~ path and the old `Trade_Decisions` producer.
- Fluss table semantics for existing market/signal tables unless a schema parity test requires a
  documented clean-break change.
- Live-money enablement posture: remains blocked through every implementation phase.

### Out of scope

- Implementing a real trading strategy beyond the existing placeholder signal.
- Babysitter trim/exit/re-entry actions in MVP.
- Multi-broker support, BSE/currency derivatives, Kubernetes, or automatic live order-path resume.
- Treating OpenObserve as durable execution audit.
- Making the one-year policy a legal/compliance approval by code alone.
- Replacing Flink/Fluss with another streaming/storage platform.

## 8. Implementation Plan

### Task completion contract

This plan is executed as a gated TODO list. A task is **not complete because its code compiles**.
The implementer must mark every atomic checkbox for the task, produce the stated output guarantee,
and attach the stated validation evidence before starting the next task.

Each task has four required completion fields:

| Field | Required meaning |
| --- | --- |
| **Atomic TODO** | A checkbox for every independently verifiable implementation action. No unchecked item may be silently carried forward. |
| **Output guarantee** | The exact artifact and externally observable behavior that must exist when the task is complete. |
| **Evidence** | The command result, test report, schema checksum, fixture, or runtime observation that proves the output guarantee. |
| **Exit gate** | A deterministic pass/fail rule. If it fails, the task remains incomplete or is marked `BLOCKED:` with the reason. |

Use these status markers while executing the plan: `[ ]` not started, `[~]` in progress, `[x]`
complete, and `BLOCKED:` for a dependency or external prerequisite that prevents completion.
The implementation agent must not mark a parent task `[x]` while a child checkbox is unchecked.
Every output guarantee is a **necessary condition**, not a promise that live-money trading is
approved; live money remains blocked until Task 9 and the external approval gates pass.

For precise status reporting, the parent headings are phase IDs `T0` through `T9`. Within each
parent task, its checkboxes are atomic child items in display order: for example, the third
checkbox under Task 4 is `T4.3`. An implementation report must identify these IDs rather than
reporting only “Task 4 mostly done”. The following matrix shows which implementation task closes
each major workstream; the detailed checkboxes under that task are the actual TODO list.

| Workstream | Closing task | Completion proof |
| --- | --- | --- |
| Version, policy, and dependency freeze | `T0` | Reproducible pins, change record, and validation output |
| Immutable execution-intent contract | `T1` | DDL/manifest/hash tests and disabled-by-default graph proof |
| Fluss transport and projections | `T2` and `T6` | Gateway protocol plus idempotent projection/recovery evidence |
| Arrow order boundary | `T3` | Fake-broker lifecycle, `UNKNOWN` mapping, and network isolation proof |
| Nautilus runtime authority | `T4` | Rust service lifecycle, fake execution, position, and restart evidence |
| Gate, fencing, and unknown-outcome safety | `T5` | Crash-window, approval, fence, and zero-duplicate-command tests |
| Position projection and parity | `T6` | Rebuild and differential replay evidence |
| Babysitter observation boundary | `T7` | Checkpoint/restore and zero-action proof |
| Local deployability and readiness | `T8` | Compose acceptance, checkpoint, restart, and secret-isolation evidence |
| External sandbox, shadow, and release gates | `T9` | Evidence bundle and two-person review while still halted |

### Implementation progress — 2026-08-19

| Phase | Status | Verified output | Remaining boundary |
| --- | --- | --- | --- |
| `T0` dependency/policy freeze | **PARTIAL** | Nautilus commit/workspace pin, Rust 1.97.1 record, Go 1.24.5 image tag, platform/Arrow SDK pins, version matrix, `CHG-039`–`CHG-041`, cargo metadata/check, and saved phase-1 evidence exist | Immutable Go image digest, ignored credential-injection proof for real sandbox secrets, retention approval, and final service capability evidence remain absent |
| `T1` intent contract | **PARTIAL — producer slice** | `27_execution_intent.sql`, 25-entry manifest, Java layout/builder/protocol, deterministic context resolver, strict config, disabled-by-default Signal branch, stable UIDs, producer tests, and full compute suite | Enabled-graph scratch Fluss append proof and durable quarantine/audit writer remain T2 responsibilities |
| `T2` Java Fluss gateway | **PARTIAL — durable-replay leg done, offline+live green** | `code/02_services/06_execution_gateway/` contains the standalone Java 17 module, Fluss readers/writers, authenticated protocol, readiness, ledger, focused tests, and a durable source-event dedup index (`Execution_Intent_Processed`) that replaces the process-local duplicate guard. 18 tests PASS (13 unit incl. durable-dispatcher, 16 offline / 3 env-gated live: composite-key matrix, durable restart-replay, recoverable-ledger resume) | Production Compose wiring of the gateway remains; broker exactly-once across a forward/commit crash boundary is deferred to T5 (durable fence token) |
| `T3` Go Arrow execution bridge | **PARTIAL — offline fake/network slice** | Separate module, pinned vendored SDK replacement, private v1 command/report protocol, SDK REST adapter, fake HTTP lifecycle, fake WebSocket lifecycle/reconnect fixture, request-id cache, postback normalizer, Dockerfile, resolved Compose profile, network policy checker, race tests, and disabled-mode health probe | Real sandbox authentication/re-authentication remains intentionally blocked; T8 still must wire gateway/Rust services and perform cross-container runtime probes; T9 owns real broker evidence |
| `T4` Rust Nautilus service | **NOT IMPLEMENTED** | Nautilus reference crates compile at the pinned checkout; no service exists under `code/02_services/04_executor/` | Rust binary, `ExecutionClient`, runtime, health, fake bridge, event-store integration, and restart behavior remain |
| `T5` durable gate/attempt/fencing | **PARTIAL — prerequisite models only** | Pure-JVM `GateTransitionValidator`, `InMemoryAttemptStore`, audit/hash-chain, and identity tests exist | No durable gateway-backed state, Rust orchestration, pre-call persistence, fencing, approvals, or crash-window enforcement exists |
| `T6` postback/projection/position parity | **PARTIAL — test/reference helpers only** | Java `PositionProjector`/`PositionProjectorDriver`, position schemas, and broker postback unit models exist | No Nautilus event source, projection writer, ledger, quarantine runtime, Fluss rebuild, or differential replay exists |
| `T7` Babysitter Positions observation | **PARTIAL — MVP marker shell only** | Job has fail-closed action flag, explicit UIDs, and zero-action marker/discard tests | `BabysitterJob` still uses `fromElements(0L)`; no `Positions` source, observation state, or checkpoint/restore evidence exists |
| `T8` local Compose integration | **PARTIAL — T3 bridge profile only** | Compose now defines the T3 `execution-bridge` profile, internal `execution-net`, bridge-only `arrow-egress`, no host port, and a static resolved-config policy check | Gateway, Rust service, jobs, readiness ordering, checkpoint probe, and full end-to-end restart/recovery path remain |
| `T9` sandbox/shadow/release evidence | **NOT IMPLEMENTED** | No execution-specific Arrow sandbox, shadow comparison, or release evidence bundle exists | All external protocol, recovery, retention, shadow, and two-person review evidence remains blocked |

This progress table is evidence that the first slices are intentionally **not** reported as full
task completion. The system remains safe: the new bridge defaults to `disabled`, no broker
credentials are committed, no Rust execution service or Java gateway is connected, and no Signal
producer can submit an order.

### Repository-backed implementation audit — 2026-08-19

This audit was performed against the current files, not against task intent or documentation
claims. Status meanings are strict: **IMPLEMENTED** means the planned runtime behavior and its
tests exist; **PARTIAL** means only a contract, helper, scaffold, or offline slice exists;
**NOT IMPLEMENTED** means the planned component/path is absent. Existing prerequisite helpers do
not count as completion of the task that must integrate them.

| Task | Classification | Evidence inspected | Determining fact |
| --- | --- | --- | --- |
| `T0` | **PARTIAL** | `versions.pin:21-33`, `version_matrix.yaml:404-430`, `nautilus_trader` commit `74d57e7e...`, `CHG-039.md`–`CHG-041.md`, and `logs/nautilus-execution/phase1/` | Rust and Go target versions are recorded and Rust 1.97.1 is proven; the Go image digest, real credential-injection proof, policy approval, and final service capability evidence remain absent |
| `T1` | **PARTIAL** | `27_execution_intent.sql`, `ExecutionIntentBuilder.java`, `ExecutionIntentContextResolver.java`, `ExecutionIntentProducerFunction.java`, `SignalJob.java`, `SignalJobConfig.java`, and compute tests | Candidate rows still store null context, but the enabled producer resolves the approved deterministic entry context; only local enabled-graph Fluss evidence and T2 durable quarantine remain |
| `T2` | **PARTIAL** | `06_execution_gateway/` POM, `IntentReader`, `FlussControlStateStore`, `GatewayProtocol`, `FlussProjectionWriter`, `FlussProjectionLedgerStore`, `ProjectionApplier`, `GatewayHttpServer`, and 12 unit tests plus 1 env-gated integration test | Offline module and fail-closed runtime boundary exist; live Fluss scratch replay, real Nautilus endpoint evidence, and Compose wiring remain |
| `T3` | **PARTIAL** | `06_execution_bridge/go-bridge/main.go`, `server.go`, `broker.go`, `postback.go`, `fake_arrow_broker_test.go`, `go test -race ./...`, `execution_network_check.py`, resolved Compose `execution-t3` profile, Dockerfile | Offline fake place/modify/cancel/fill/partial-fill/reject/timeout/UNKNOWN/reconnect behavior and static network policy pass; live token refresh, cross-container route probes, and sandbox evidence remain |
| `T4` | **NOT IMPLEMENTED** | `code/02_services/04_executor/` contains only Python scaffold/Dockerfile/README/requirements; no `Cargo.toml` or Rust source | No Nautilus service or custom `ExecutionClient` exists |
| `T5` | **PARTIAL** | `code/common/.../GateTransitionValidator.java`, `InMemoryAttemptStore.java`, audit and identity tests | Rules and in-memory models exist, but no durable runtime invokes them before bridge calls |
| `T6` | **PARTIAL** | `code/common/.../PositionProjector.java`, `PositionProjectorDriver.java`, broker postback tests, projection DDLs | Reference arithmetic/schema helpers exist, but no Nautilus-to-Fluss projection path or rebuild/replay runtime exists |
| `T7` | **PARTIAL** | `BabysitterJob.java:65-97`, `BabysitterJobTest.java` | Strict no-op shell and fail-closed flag are tested, but source is still `fromElements(0L)` |
| `T8` | **PARTIAL** | T3 supplies `FakeBroker`; `docker-compose.yml:284-285,409-424` still leaves execution services commented | No end-to-end local path, private service network, checkpoint acceptance, or restart/recovery probe exists |
| `T9` | **NOT IMPLEMENTED** | no execution-specific sandbox/shadow evidence under `logs/`; Arrow VM rows remain `PINNED_AWAITING_EVIDENCE` | No real sandbox order, fill, reconciliation, shadow, or release review has occurred |

Verified commands for this audit:

```text
go vet ./... && go test -race ./...                         # execution bridge: PASS
mvn -o test                                                # compute: 319 run, 0 failures, 17 skipped
cargo metadata --locked --no-deps                           # pinned Nautilus workspace: PASS
cargo check --locked -p nautilus-execution -p nautilus-live -p nautilus-event-store  # PASS
```

These commands prove the implemented slices compile and test; they do not promote any partial
task to complete and do not authorize live orders.

### Task 0 — Freeze authority, versions, and change boundary — **PARTIAL**

**Objective:** Convert the design into pinned implementation inputs before writing runtime code.

**Files/components:**

- Modify `docs/08_implementation/05-execution-core.md` and this plan if evidence changes.
- Modify `code/01_platform/04_scripts/version_matrix.yaml` with the approved Nautilus commit,
  Rust toolchain, Go toolchain, and bridge SDK commit.
- Create the implementation change record using the next available `CHG-<N>` number when code/DDL
  work begins.

**Implementation details:**

1. Pin the exact Nautilus commit that passed `cargo check` and the event-store test suite; do not
   use `develop` or a floating branch.
2. Record the exact Cargo package versions, Rust version, Go version, go-arrow commit, Flink 2.2.1,
   Fluss 0.9.1-incubating, Java 17, and protocol evidence status.
3. Record the one-year minimum audit policy as requiring Compliance/Platform approval before live
   money.
4. Confirm Arrow sandbox credentials are supplied only through ignored secret injection.

**Failure behavior:** Missing pin, missing credentials, unsupported API, or unresolved protocol
field blocks the task; it must not be guessed or silently defaulted.

**Atomic TODO checklist:**

- [x] Select and record the exact Nautilus commit, Rust toolchain, Go toolchain, and bridge SDK
  commit that passed the audit; reject floating branches.
- [~] Record the service Rust toolchain (`1.97.1`) and the reproducible Go `1.24.x` image/toolchain
  digest separately from the host tool versions.
- [x] Record exact Cargo package versions, Flink/Fluss/Java versions, protocol evidence status,
  and the one-year minimum retention policy approval prerequisite.
- [x] Update `version_matrix.yaml` and create the implementation change record.
- [ ] Verify Arrow sandbox credentials are supplied only through ignored secret injection.
- [x] Create and hash a sanitized T0 evidence bundle; do not count terminal output that was not saved.
- [~] Run every Task 0 validation command and save the outputs as evidence.

**Output guarantee:** A fresh checkout can reproduce the exact dependency set and identify the
approved change boundary, while live money and live credentials remain disabled.

**Evidence:** Updated version matrix, change record, dependency metadata, successful Nautilus/event
store/bridge checks, and `make docs-audit` output.

**Exit gate:** Pass only when all five checkboxes are complete and no floating dependency,
unresolved API field, or unapproved credential path remains. Otherwise mark `BLOCKED:`.

**Tests/validation:**

- `cargo metadata --locked --no-deps`
- `cargo check -p nautilus-execution -p nautilus-live -p nautilus-event-store`
- `cargo test -p nautilus-event-store`
- `go test ./...` in the bridge module
- `make docs-audit`

**Completion criteria:** An exact reproducible dependency set exists, the change boundary is
recorded, and live money remains disabled.

### Task 1 — Define and produce the immutable execution intent — **PARTIAL: contract slice only**

**Objective:** Give the execution service a concrete input contract without reviving the decision path.

**Files/components:**

- Create `code/01_platform/02_sql/ddl/27_execution_intent.sql`.
- Modify `code/01_platform/02_sql/ddl/schema_manifest.json` through `ddl_apply.py`/the repository's
  manifest workflow.
- Re-scope `code/01_platform/02_sql/ddl/25_trade_instruction_state.sql` comments and ownership to
  the execution-intent hash index, if schema parity confirms no physical change is needed.
- Create Java column/schema classes under
  `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/`:
  `ExecutionIntentTableColumns`, `ExecutionIntent`, `ExecutionIntentBuilder`,
  `ExecutionIntentFeedProtocol`.
- Modify `SignalJob.java`, `SignalJobConfig.java`, and `TableContractValidator.java` only to add
  the explicitly disabled intent producer and preflight checks.
- Add tests beside the compute signal-job tests.

**Target behavior:**

- `Execution_Intent` is an append-only LOG keyed/routed by `instruction_id`.
- The row fields are exactly those specified in the Target Architecture section.
- The complete canonical request hash includes every executable field, scope field, version field,
  expiry, and supersession reference; field ordering and encoding are versioned.
- Same `instruction_id` plus same hash is a duplicate/no-op; same ID plus different hash is a
  violation that produces quarantine/audit intent and no execution event.
- `EXECUTION_INTENT_ENABLED` defaults to `false`; no production broker path is enabled by this flag.
- The old ~~ranking~~/~~reservation~~/`Trade_Decisions` producer remains disabled.

**Dependencies:** Task 0; existing `SignalCandidate` schema and `TradeDecisionBuilder` behavior may
be reused only after the new intent fields are explicitly mapped.

#### Repository audit and implementation handoff — T0/T1 boundary

The following facts are verified and must be used as implementation inputs:

| Fact | Current evidence | Required implementation consequence |
| --- | --- | --- |
| Nautilus commit is recorded | `code/01_platform/04_scripts/versions.pin` records `74d57e7e055015a8d974a0cf047ac2e5139610b3`; the sibling checkout is clean on `develop` at that commit | The service must use a Cargo lockfile and a git revision, never `develop`, a floating crate version, or an unpinned path outside the build context |
| Nautilus toolchain is known but not copied into this project | `nautilus_trader/rust-toolchain.toml` requires Rust `1.97.1`; this host currently has Rust `1.96.0` | T0 must record Rust `1.97.1` in the project pin/evidence and build the service with that toolchain; a host-only successful build is insufficient |
| Go SDK is pinned | `code/02_services/01_ingestion/go-bridge/go.mod` and T3 use vendored SDK base commit `7cce1630`; the source checkout resolves to `7cce1630ae2d45c59839f512f0f8c3fbb0be73cf` | T3/T8 must preserve the existing relative vendored replacement and record the SDK tree hash; no second SDK copy or unreviewed fork may be introduced |
| Current Go module/toolchain is not the host toolchain | T3 `go.mod` declares `go 1.24.5`, while this host reports Go `1.26.4`; its Dockerfile uses `golang:1.24-alpine` without a digest | T0/T8 must pin the image digest or an equivalent Go `1.24.x` toolchain and run evidence in that environment, not claim reproducibility from the host Go version |
| No execution evidence bundle exists | `logs/` contains market-data, schema, and ingestion evidence but no execution-specific T0/T4/T9 bundle | Create ignored, sanitized `logs/nautilus-execution/<run-id>/` evidence with commands, hashes, tool versions, and no credentials |

**T0 implementation sequence:**

1. Record `rust-version = 1.97.1`, the exact Cargo package set, Go `1.24.x`, Java 17,
   Flink 2.2.1, Fluss 0.9.1-incubating, and the SDK tree hash in `versions.pin` and
   `version_matrix.yaml`.
2. Build a minimal service dependency probe before any service code: `cargo metadata --locked`,
   locked checks for `nautilus-live`, `nautilus-execution`, and `nautilus-event-store`, and a
   bridge `go vet`/race test under the pinned toolchains.
3. Verify secret injection mechanically: `git check-ignore` must cover the local secret file;
   source and Compose scans must show `ARROW_*` values only on Go bridge processes. The existing
   market-data bridge is an intentional Arrow-facing exception; the order-path bridge must remain
   the only order-path credential holder.
4. Save sanitized output and SHA-256 hashes in the ignored evidence directory. A successful command
   without a saved output is not T0 evidence.

**T0 unresolved external input:** the one-year minimum is a documented baseline, not proof that
Compliance has approved the policy. The implementation agent must not invent an approver or
convert the baseline into a legal decision. Record the policy-change/approval identifier when
the owner supplies it; until then mark the release-policy child `BLOCKED:`.

#### T1 implementation handoff — exact current gap and safe producer design

| Item | Verified current state | Implementation rule |
| --- | --- | --- |
| Candidate source | `SignalDetectionFunction.toCandidate()` and `FormingBarDetectionFunction` emit the 22-column candidate layout; both set `trade_context_id` and `instruction_id` to `null` | Use the approved deterministic MVP context rule below; do not reuse a closed trade's context for a new entry |
| Existing builder | `ExecutionIntentBuilder.fromCandidate()` requires non-null `trade_context_id`, validates `ENTRY`, `VALID`, positive quantity/timestamp, and market/limit price rules | Reuse it as the only candidate-to-intent mapping; add a context/scope resolver before it, with explicit failure results and metrics |
| Missing configuration | `SignalJobConfig` has `TRADE_DECISIONS_ENABLED` but no `EXECUTION_INTENT_ENABLED`, account scope, or execution partition configuration | Add strict parsing for `EXECUTION_INTENT_ENABLED` default `false`; when true, require nonblank configured `ACCOUNT_SCOPE_ID`, `EXECUTION_PARTITION_ID`, product, validity, and configuration version; use one configured sandbox account and `partition-0` for the first path, without hard-coding the account value |
| Missing topology | `SignalJob.buildTopology()` currently ends at the `Signal_Candidates` append-only log and its current-state sinks; there is no intent transform or sink | Insert a separate intent branch after `allSignals`, preserve both candidate sinks and all existing UIDs, and add a pinned `execution-intent-sink` UID only when the feature is enabled |
| Duplicate protocol location | `ExecutionIntentFeedProtocol` is pure Java only; no quarantine writer exists in compute or gateway | Keep compute responsible for deterministic identity/hash; make T2 the authoritative durable duplicate/violation/quarantine enforcement point. T1 tests must not claim a durable quarantine write |

**T1 implementation sequence:**

1. Apply the approved identity decision: use one configured sandbox account and one execution
   worker/partition for the first path. Mint `trade_context_id` deterministically for an entry from
   the configured account plus instrument plus candidate identity; reuse that ID for reduce/exit
   actions belonging to that trade, and mint a new ID after the trade is closed and re-entered.
   The account value remains injected configuration, not a source-code default.
2. Add strict config parsing and a disabled-by-default graph branch. `false` must produce no
   `Execution_Intent` sink or broker-facing side effect; `true` with missing context/scope config
   must fail startup, not fall back to `QP3796` or a default partition.
3. Add a pure `ExecutionIntentProducerFunction`/resolver test fixture with valid and invalid
   candidates. The valid fixture must carry a non-null context and prove deterministic
   `instruction_id`, `request_hash`, DDL-order row mapping, and expiry/supersession behavior.
4. Append through the Fluss LOG sink using the same `RowDataSerializationSchema(true, true)` and
   bounded client timeout/retry settings already used by the signal LOG sink. Never put a bridge
   or broker call in this branch.
5. Add graph inspection for both feature states, contract preflight for the new table, and an
   explicit test that the retired `Trade_Decisions` sinks remain absent from the executable path.

**T1 required output:** an enabled offline graph can append a valid immutable intent to a scratch
Fluss table, while the default graph cannot append an intent and invalid context/scope/hash rows
produce no executable output. The actual sandbox account value must be supplied through deployment
configuration before the producer is enabled; no account value is guessed in code.

**Atomic TODO checklist:**

- [x] Define `Execution_Intent` columns, key/routing, schema version, and retention metadata.
- [x] Create the DDL and update the schema manifest/checksum.
- [x] Define canonical hash encoding, field order, null handling, and all executable inputs.
- [x] Implement `ExecutionIntentColumns`, `ExecutionIntentBuilder`, and
  `ExecutionIntentFeedProtocol` with explicit candidate-to-intent mapping.
- [x] Define the MVP mapping: one configured sandbox account, one `partition-0` worker, and one
  deterministic `trade_context_id` per entry trade; keep the actual account value in injected config.
- [x] Add `EXECUTION_INTENT_ENABLED=false` as the production-safe default.
- [x] Keep same-ID/same-hash and same-ID/different-hash verification pure here; implement the
  durable quarantine/audit action in T2 rather than claiming it is already a compute sink.
- [x] Add the disabled/enabled producer branch after `allSignals`, with stable UIDs and no broker
  side effect.
- [~] Add tests for missing fields, expiry, supersession, scope, schema version, hash sensitivity,
  duplicate content, changed content, and disabled graph behavior.
- [x] Run DDL, compute, documentation, and stale-table validation.

**Output guarantee:** A fresh implementation can reproduce one immutable, versioned
`Execution_Intent` record and its request hash, and a test proves the default Signal graph has no
active executable-intent sink and cannot call a broker.

**Evidence:** DDL and manifest checksum, Java producer/protocol tests, disabled-graph test output,
and validation command output.

**Exit gate:** Pass only when every checkbox is complete, the producer is disabled by default,
`Trade_Decisions` remains disabled, and all rejected/ambiguous inputs are quarantined without an
execution event.

**Edge cases/failure behavior:** Missing required fields, unsupported schema, expired intent,
invalid scope, duplicate content conflict, malformed hash, and supersession conflict are rejected,
quarantined/audited, and never emitted as executable intent.

**Tests:**

- DDL-to-column agreement and manifest checksum tests.
- Deterministic hash sensitivity tests for every executable field.
- Duplicate and modified-identity protocol tests.
- Expiry/supersession/scope/schema-version rejection tests.
- Feature-disabled Signal graph test proving no intent sink is active by default.
- Feature-enabled offline producer test using the placeholder candidate only; no broker call.

**Validation:**

```bash
python3 code/01_platform/04_scripts/ddl_apply.py
cd code/02_services/02_compute && mvn -o test
cd ../.. && make docs-audit && make stale-tables
```

**Completion criteria:** The new LOG schema and producer are pinned, disabled by default, and
covered by deterministic tests; `Trade_Decisions` remains retired/disabled.

### Task 2 — Build the Java Fluss Execution Gateway — **PARTIAL: offline/runtime slice implemented 2026-08-19**

#### T2 implementation evidence and remaining completion boundary

The repository now contains an implementation slice, but T2 is deliberately not marked complete
until it has been exercised against a live Fluss cluster and a real private Nautilus endpoint.
The slice is safe to run offline: it has no Arrow SDK, no `ARROW_*` configuration, no broker
credential, and it cannot claim execution readiness while the gate or durable Fluss path is
uncertain.

| T2 area | Implemented now | Still required for T2 completion |
| --- | --- | --- |
| Build/service boundary | Standalone Java 17 Maven module at `code/02_services/06_execution_gateway/`, root-reactor entry, shaded jar, Java 17 Dockerfile | Build the image from the `code/` context and record image/runtime evidence |
| Intent input | `IntentReader` validates the 22-column row, schema version, identity, request hash presence, scope, expiry, side/order/quantity; subscribes every bucket from offset 0; deferred handoffs are replayable | Live append one intent, restart the gateway, prove the same instruction/hash does not create a second handoff; durable attempt/index lookup must replace the current process-local duplicate guard |
| Control state | `FlussControlStateStore` performs bounded point lookups and full-bucket safety-halt replay; lookup status distinguishes found/not-found/unavailable | Run against real `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Postback_Projection_Ledger`, and `Safety_Halt_Requests`; add stale/version/fence assertions for the deployed schemas |
| Private protocol | HMAC-SHA256, versioned JSON envelope, request/scope/partition IDs, payload hash, gate epoch, fence token, deadline, malformed/tamper/expiry rejection | Interoperate with the Nautilus service and capture authenticated request/response fixtures; prove disconnect/backpressure keeps readiness false |
| Projections | Explicit Fluss serializers for `Execution_Audit`, `Fills`, `Order_Lifecycle`, `Positions`, and `Order_Correlation`; every future is awaited; no position arithmetic | Live normalized-event write/read proof, quarantine writer for malformed events, and independent-table recovery evidence |
| Ledger/recovery | `ProjectionLedger`, `FlussProjectionLedgerStore`, and `ProjectionApplier` persist each workflow step and resume incomplete states without repeating completed steps | Kill between each write, restart, scan incomplete ledger rows, resume, and prove terminal `COMPLETE` plus duplicate no-op |
| Readiness | Separate health, Fluss, protocol, durable-write, and execution readiness; HTTP `/healthz`, `/readyz`, `/v1/events` boundary | Verify readiness transitions under real Fluss loss, protocol loss, partial write, and gate `HALTED`/`RECONCILING` states |

The current output guarantee is therefore: **offline unit tests prove the state-machine and protocol
contracts; T2 remains incomplete until the live scratch run proves durable replay and recoverable
projection behavior.**

**Objective:** Provide the supported Fluss client boundary for intent/control/projection state
without making Java an execution authority.

**Files/components:**

- Replace the Python-only deployment role in `code/02_services/04_executor/` with a documented
  service layout, or create the separate Java gateway at
  `code/02_services/06_execution_gateway/` with `pom.xml`, Dockerfile, and
  `src/main/java/com/trading/execution/gateway/`.
- Use the same managed Fluss dependencies from `code/pom.xml` (`fluss-client`, Fluss Flink version
  compatibility, Java 17).
- Create gateway modules for `IntentReader`, `ControlStateStore`, `ProjectionWriter`,
  `ProjectionLedger`, `GatewayProtocol`, and `GatewayReadiness`.

**Target behavior:**

- Read `Execution_Intent` from Fluss using the supported Java client path.
- Validate schema, request hash, identity, expiry, scope, and current state before forwarding.
- Deliver intents, halt requests, gate reads, and reconciliation commands over a private,
  authenticated, versioned service protocol to Nautilus.
- Accept normalized Nautilus events and write Fluss projections with idempotent source event IDs.
- Maintain `Postback_Projection_Ledger` transitions:
  `RECEIVED → AUDIT_WRITTEN → LIFECYCLE_APPLIED → POSITION_APPLIED_OR_NOT_REQUIRED → COMPLETE`.
- Treat gateway/Fluss uncertainty as not-ready and never report successful durable application until
  the Fluss acknowledgement is verified.

**Dependencies:** Task 1; Fluss table manifest; private service identity/secret mechanism.

#### Repository audit and implementation handoff — T2

T2 is not merely a new Java module. It must first resolve the verified Fluss writer limitation:

| Surface | Verified fact | Required plan action |
| --- | --- | --- |
| Supported client | `code/pom.xml`, `SafetyHaltWriter`, `FlussPositionsStateStore`, `DdlApplyTool`, and `CompatFlussIntegrationTest` establish the Java raw-client API: `ConnectionFactory`, `Table`, `LogScanner`, `Lookuper`, `AppendWriter`, and `UpsertWriter` | Build the gateway on the pinned `fluss-client` 0.9.1-incubating API and reuse the existing connection/timeout/close patterns |
| LOG reads | `TokenCountReconcile` proves `LogScanner.subscribeFromBeginning(bucket)` and `ScanRecord.logOffset()`; a lake-enabled LOG must not use a bounded `BatchScanner` for continuous/replay reads | `IntentReader` and safety/control readers must use per-bucket `LogScanner`, retain the last processed offset for diagnostics, and be safe to replay from the beginning after restart |
| Single-key KV writes | `Positions`, `Execution_Gate`, `Execution_Attempts`, and `Postback_Projection_Ledger` have single-field primary keys and are raw-client writable | Use full-row `UpsertWriter` images and await every future; an unobserved future is not durable success |
| Composite-key KV writes | `logs/schema-compat/composite-pk-raw-client-20260815.md`, `CompositeKeyMatrixVerifier`, and `docs/08_implementation/02-schema-storage.md` prove raw-client upsert fails when the bucket key equals the composite PK; only `kv.format-version=2` plus a single-field subset bucket key passes | The owner approved the smallest fix: retain composite PK/account isolation, set `table.kv.format-version='2'`, and use `account_scope_id` as `Order_Lifecycle` bucket key and `instruction_id` as `Order_Correlation` bucket key. T2 must implement and test this DDL change before `ProjectionWriter` |
| Current gateway | `code/02_services/06_execution_gateway/` is a new Java 17 module in the root reactor; the Python executor scaffold remains separate | Keep the gateway as the Java Fluss/private-protocol boundary; do not move it into the Python executor scaffold; complete live integration and Compose wiring |

**T2 implementation sequence:**

1. Add the schema compatibility change and its manifest/checksum/change record first. Run the live
   `COMPAT-FLUSS-005` matrix and a scratch append/lookup for both affected tables. This is a
   prerequisite, not a later optimization.
2. Implement `GatewayConfig` with strict bootstrap/database/table/protocol/auth timeouts. The
   gateway receives only Fluss and private service credentials; it must have no `ARROW_*` values,
   Arrow host, SDK dependency, or Arrow route.
3. Implement `IntentReader` as a bounded single-writer loop: inspect table metadata, subscribe to
   all buckets, decode only current valid rows, validate schema/hash/expiry/scope, and replay from
   the beginning after restart. Replayed `instruction_id`/hash pairs must be idempotent through the
   durable attempt/index tables; a changed hash quarantines and halts.
4. Implement `ControlStateStore` with point lookups for `Execution_Gate`, `Execution_Attempts`,
   `Order_Correlation`, and `Postback_Projection_Ledger`, plus a replay reader for
   `Safety_Halt_Requests`. Every read must distinguish “not found”, stale, and unavailable.
5. Implement `GatewayProtocol` as a versioned authenticated request/response protocol. Require
   request ID, scope, payload hash, gate epoch, fence token, and bounded deadlines. Backpressure,
   disconnect, malformed envelope, or authentication failure keeps readiness false.
6. Implement `ProjectionWriter` with explicit per-table serializers. LOG writes use `AppendWriter`;
   KV writes use full images and awaited futures. Never claim cross-table atomicity. Write the
   `Postback_Projection_Ledger` step before/after each independent table write and recover all
   non-terminal ledger states on boot.
7. Expose separate health, Fluss-readiness, protocol-readiness, and durable-write-readiness. A
   healthy JVM with an unavailable Fluss table is not execution-ready.

**T2 output guarantee:** a scratch Fluss run can replay one intent, send one authenticated envelope,
replay it after gateway restart without a duplicate side effect, and apply a normalized event across
independent tables with a recoverable ledger. A direct client test proves no composite-key limitation
remains for the owner-approved DDL settings.

**Atomic TODO checklist:**

- [x] Resolve the Fluss 0.9.1 composite-key raw-client limitation and record the approved
  `kv.format-version=2`/single-subset bucket-key DDL change for `Order_Lifecycle` and
  `Order_Correlation`; DDL, manifest, agreement test, module, and unit proof are present, AND
  the live COMPAT-FLUSS-005 matrix + scratch append/lookup were run against the local cluster
  (2026-08-19): matrix PASS + full scratch apply `RESULT=PASS EXIT=0` for all 26 tables.
- [x] Create the gateway module, build descriptor, container image, configuration model, and
  private authenticated endpoint.
- [x] Implement `IntentReader` with offset/restart behavior and schema/hash/expiry/scope checks;
  the process-local duplicate guard is now reconciled from a durable attempt/index lookup
  (`FlussIntentDedupStore`/`DurableIntentDispatcher`, backed by the new `Execution_Intent_Processed`
  KV index), committed on each FORWARDED handoff and hydrated on boot; the live restart-replay
  proof (append one intent, hand off once, restart reader, no second handoff) PASSES (2026-08-19).
- [~] Implement control-state reads for gate epoch, halt state, approvals, and reconciliation
  requests without making Java an OMS.
- [x] Implement authenticated, versioned gateway-to-Rust envelopes with timeout/backpressure
  handling.
- [~] Implement `ProjectionWriter` and serializers for every required Fluss projection; local
  serializers and awaited futures exist, while live write/read proof and quarantine remain.
- [x] Implement `ProjectionLedger` transitions and idempotent replay from every incomplete state.
- [x] Implement readiness so Fluss or protocol uncertainty is never reported as durable success.
- [~] Add duplicate, malformed, gap, disconnect, partial-write, restart, and authentication tests;
  protocol/partial-write/readiness tests exist and the live Fluss restart legs now PASS
  (durable-replay no-second-handoff + recoverable-ledger resume to COMPLETE with a duplicate
  no-op); live gap/disconnect legs remain.
- [x] Run module unit tests and the environment-gated Fluss integration test; module tests pass
  (13 unit, 0 failures) and the env-gated `GatewayFlussIntegrationTest` + `CompatFlussCompositeKeyIntegrationTest`
  both PASS against the local cluster (`FLUSS_BOOTSTRAP=localhost:9123`, 2026-08-19).

**Current output guarantee:** Unit + live tests prove validated intent rules, authenticated
envelopes, readiness fail-closed behavior, forward-only ledger transitions, and resume without
repeating completed steps. The live durable-replay proof (append one Execution_Intent, hand it off
once, restart the reader, replay from offset 0 -> no second handoff via the durable
`Execution_Intent_Processed` index) and the live recoverable-ledger proof (crash mid-apply leaves an
incomplete ledger row; a fresh applier resumes to COMPLETE without repeating completed steps and a
re-apply is a duplicate no-op) both PASS against the local cluster (2026-08-19). The runtime awaits
Fluss futures and leaves an incomplete ledger state on a reported write failure. **The durable dedup
record is committed on a FORWARDED handoff; exactly-once across a crash between an outbound
forward and its durable-commit, and any real broker side effect, still require T5's durable fence
token / forwarding path** (a durable-commit failure today fails readiness closed rather than risk a
duplicate). The gateway has no order lifecycle arithmetic, position authority, broker credential, or
Arrow access.

**Evidence:** Gateway test report, protocol fixtures, incomplete-ledger restart report, Fluss
integration output, readiness transitions, and a static credential/Arrow-access check.

**Exit gate:** Pass only when `HALTED` produces no bridge command, duplicate events are idempotent,
incomplete writes resume safely, and Fluss uncertainty keeps readiness false.

**Edge cases/failure behavior:**

- Fluss unavailable: bounded retry, readiness false, no broker command.
- Changelog gap or malformed row: quarantine/audit and halt the affected partition.
- Duplicate intent/event: idempotent no-op.
- Projection partial write: ledger remains incomplete; restart scans and resumes.
- Gateway-to-Rust disconnect: no new command is acknowledged as accepted; Rust remains/returns
  `HALTED` or `RECONCILING`.

**Tests:**

- Fluss reader offset/restart tests with a test table.
- Schema/hash/expiry/scope validation tests.
- Duplicate and projection-ledger idempotency tests.
- Gateway protocol authentication, version, timeout, backpressure, and malformed-message tests.
- Restart with incomplete ledger and repeated normalized event tests.

**Validation:** `mvn -o test` in the gateway module (12 unit tests, 0 failures, plus 1 skipped
environment-gated integration test), env-gated Fluss integration with
`FLUSS_BOOTSTRAP=localhost:9123`, and a test proving no bridge command is emitted while `HALTED`.

**Completion criteria:** Gateway can read intent/control state and write projections durably, but
contains no order lifecycle, position arithmetic, broker credentials, or direct Arrow call.

### Task 3 — Build the isolated Go Arrow execution bridge — **PARTIAL: offline slice**

**Objective:** Implement the only order-path component allowed to call Arrow.

**Files/components:**

- Create `code/02_services/06_execution_bridge/go-bridge/` with `go.mod`, bridge server, Arrow
  client adapter, command/report models, auth provider, and tests.
- Reuse the exact pinned SDK source/commit used by
  `code/02_services/01_ingestion/go-bridge`; do not change the market-data bridge in this task.
- Create a Dockerfile and private-network runtime configuration.

**Target behavior:**

- Expose private authenticated commands for place, modify, cancel, query-order, reconcile-orders,
  reconcile-trades, reconcile-positions, and health.
- Consume Arrow order-update WebSocket events and normalize them into versioned event envelopes.
- Preserve `remarks` as `client_order_ref`, Arrow `orderNo` as `broker_order_id`, fill details,
  status/report type, timestamps, and redacted response evidence.
- Classify responses exactly:
  - verified success: HTTP 200 + `status=success` + nonblank `data.orderNo`;
  - verified rejection: documented 4xx rejection envelope with nonblank message;
  - all transport errors, timeouts, 401/403/408/429/5xx, malformed bodies, or ambiguous responses:
    `UNKNOWN`.
- Never retry an ambiguous place command automatically.
- Keep credentials, token refresh, broker endpoint, and TLS handling inside this process.

**Dependencies:** Task 0; Arrow sandbox protocol evidence; Go SDK pin.

**Atomic TODO checklist:**

- [x] Create the separate execution bridge module, build file, container image, configuration, and
  private authenticated command endpoint.
- [x] Pin and reuse the audited Arrow SDK source without modifying the market-data bridge.
- [x] Implement place, modify, cancel, query, order/trade/position reconciliation, and health.
- [x] Implement `remarks`/`client_order_ref`, broker ID, fill, status, timestamp, and fingerprint
  mappings.
- [x] Implement exact success/rejection/`UNKNOWN` classification and prohibit automatic retry of
  ambiguous place calls.
- [~] Keep Arrow authentication, token refresh, TLS, WebSocket updates, reconnect, and redaction
  inside this process only.
- [ ] Prove the SDK-supported refresh/re-authentication behavior in the sandbox; if unsupported,
  implement `EXPIRED`/not-ready fail-closed behavior instead of inventing a refresh endpoint.
- [x] Add fake-broker HTTP/WebSocket fixtures for place, modify, cancel, query, reconciliation,
  ack/open, partial fill, complete fill, cancel, reject, timeout, malformed text, unknown status,
  duplicate identity, out-of-order update, close, and reconnect cases.
- [x] Add a resolved-Compose static policy test proving only `execution-bridge` joins the
  bridge-only Arrow-egress network, `execution-net` is internal, the bridge has no host port,
  and non-market-data services cannot carry Arrow credentials.
- [x] Wire the T3 bridge-only Arrow-egress network separately from the internal execution network
  in the disabled-by-default `execution-t3` Compose profile.
- [x] Run `go test ./...` and the private health/contract probe without live credentials.

Current slice status: `T3.1`–`T3.5`, the offline portion of `T3.7`, `T3.8` static policy/network
work, and `T3.9` are complete. `T3.6` remains blocked because real sandbox authentication and
SDK refresh/re-authentication evidence are intentionally deferred. Cross-container network
reachability is also deferred to T8 because the Java gateway and Rust service are not deployed
yet. Therefore parent `T3` remains **PARTIAL — offline fake/network slice**, not live-ready.

#### Repository audit and implementation handoff — T3

| Surface | Verified current state | Remaining implementation contract |
| --- | --- | --- |
| Command boundary | `go-bridge/server.go` exposes authenticated `POST /v1/commands`; request-ID cache is process-local and deliberately lost on restart | Keep the cache as an optimization only. The Rust/gateway durable attempt record is the restart authority; add tests proving a restart does not turn a repeated request into an automatic place retry |
| Arrow REST adapter | `broker.go` maps `PlaceOrder`, `ModifyOrder`, `CancelOrder`, `GetOrder`, `GetOrderBook`, `GetTradeBook`, and `GetPositions`; `arrow/orders.go` confirms the exact SDK fields/endpoints | Preserve platform identities outside Arrow; map only `client_order_ref → remarks`, and retain sanitized response fingerprints. Do not add position arithmetic to Go |
| Authentication | `main.go` supports static `ARROW_TOKEN` or one startup `AutoLogin`; vendored `auth.go` stores `RefreshToken` but exposes no refresh operation; `AutoLogin` logs a success message | T3.6 must first prove the Arrow refresh/re-auth protocol in the sandbox and then implement bridge-owned re-authentication. If refresh is unsupported, expire readiness and require operator restart/reconciliation; never retry a money-moving call after 401 |
| Order stream | `postback.go` wraps `ConnectOrderStream`, normalizes known statuses, and reconnects with backoff; `fake_arrow_broker_test.go` now drives a local WebSocket fixture through the same `OrderUpdateSource` seam | Preserve ack/open/partial-fill/complete/cancel/reject, malformed/unknown, duplicate, out-of-order, close, and reconnect behavior. Reconnect restores observation only; it never retries place. The deterministic `PostbackEventID` is the downstream deduplication key |
| Reconciliation | SDK has `GetOrderBook`, `GetTradeBook`, `GetPositions`, and `GetOrder`; bridge currently returns opaque `Data` | Add fixtures and bounded pagination/rate-limit/consistency-delay measurements. Unknown or incomplete reconciliation remains `UNKNOWN` and keeps the gate halted |
| Network boundary | Dockerfile builds a single process and exposes port 8787; Compose now has a disabled-by-default `execution-t3` profile with `execution-net` (`internal: true`) and bridge-only `arrow-egress` | T3 static policy checks the resolved profile and rejects non-bridge egress, host port publication, non-internal execution networking, and order-path credentials outside the documented ingestion exception. T8 must add runtime route probes once Java/Rust containers exist |

**T3 implementation sequence:**

1. Define a bridge auth provider interface with explicit states `DISABLED`, `AUTHENTICATING`,
   `READY`, `EXPIRED`, and `AUTH_FAILURE`; only `READY` can make the `/readyz` broker dimension
   healthy. Store no token in logs, reports, or evidence.
2. Add the SDK-supported refresh path if sandbox evidence confirms it. Otherwise implement a
   fail-closed re-authentication state that stops commands and requires a fresh operator-approved
   restart; document that limitation in the version matrix rather than inventing an endpoint.
3. Use the in-process fake Arrow HTTP server to exercise the actual pinned SDK adapter and a local
   WebSocket fixture to exercise `OrderUpdateSource`/normalization. Keep exact JSON fields aligned
   with `Arrow_broker/go-arrow/arrow/orders.go` and `streams.go`; do not modify the market-data
   mock or add a second SDK copy.
4. Add command cancellation/deadline tests: a timeout after the request may have reached Arrow is
   `UNKNOWN`, with no internal retry. A verified documented 4xx rejection is `REJECTED`; all other
   transport/auth/malformed outcomes are `UNKNOWN`.
5. Add static and Compose network tests. The test must inspect service networks/env/dependencies and
   attempt direct Arrow access from Java/Rust/Flink containers; only the bridge may reach the Arrow
   endpoint. The market-data bridge remains a separate approved Arrow-facing exception.

**T3 output guarantee:** a no-credential fake run proves place/modify/cancel, fill/partial-fill,
rejection, timeout, malformed/unknown, duplicate/out-of-order, and reconnect behavior; the bridge
reports `UNKNOWN` for every ambiguous case; and the resolved Compose policy proves that only the
execution bridge can join the order-path Arrow-egress network. Sandbox readiness is not complete
until authentication/re-authentication and reconciliation evidence is saved.

**Output guarantee:** The execution bridge is the only **order-path** process with Arrow credentials
or an Arrow order route; the existing market-data bridge remains a separate, intentionally
Arrow-facing market-data process. A fake broker can drive the implemented offline command/report
slice, and every ambiguous outcome is observable as `UNKNOWN` rather than success.

**Evidence:** `go test -race ./...`, fake HTTP/WebSocket lifecycle tests, mapping fixtures,
redaction test, `python3 .../test_execution_network_check.py`, resolved Compose config, network
policy result, and private health probe.

**Exit gate:** The offline T3 gate passes when mappings round-trip, ambiguous place has no automatic
retry, duplicate updates have deterministic identities, all requested fake lifecycle cases pass,
and the resolved Compose policy has bridge-only order-path egress. The cross-container route probe
and real sandbox authentication remain T8/T9 gates.

**Edge cases/failure behavior:** token expiry, auth failure, WebSocket reconnect, duplicate
postback, unknown status/report type, malformed order response, timeout, bridge restart, and
backpressure all produce explicit health/events and cannot be silently converted to success.

**Tests:**

- Pure mapping tests for all order/status/fill/position fields.
- `client_order_ref` length/character/round-trip tests.
- Fake Arrow HTTP/WebSocket tests for place/modify/cancel and report delivery.
- Fake lifecycle matrix for ack/open, fill, partial fill, complete, reject, cancel, timeout,
  malformed/unknown, duplicate/out-of-order, close, and reconnect.
- Timeout/disconnect/malformed/5xx/duplicate-event tests.
- Credential redaction and no-secret-logging tests.
- Reconciliation pagination/rate-limit/consistency-delay tests using recorded sandbox fixtures.

**Validation:** `go test ./...`; private health/contract probe; no live order. Arrow sandbox is
required before any real place/update/fill evidence.

**Completion criteria:** The bridge can be tested end-to-end with a fake broker and the resolved
Compose profile statically proves bridge-only order-path Arrow egress. Runtime cross-container
reachability remains a T8 completion criterion because those peer services do not yet exist.

#### T3 phase-2 implementation evidence — 2026-08-19

| Evidence item | Result | What it proves | Remaining limitation |
| --- | --- | --- | --- |
| Pinned SDK HTTP fixture | **PASS** — `TestFakeArrowHTTPBroker` | The real SDK adapter sends authenticated place, modify, cancel, query, order-book, trade-book, and position requests to the expected paths; `remarks`/broker ID mapping remains intact | Local fixture is not Arrow sandbox evidence |
| Outcome matrix | **PASS** — rejection, timeout, malformed success, and ambiguous error tests | Documented rejection is `REJECTED`; timeout/malformed/ambiguous outcomes are never converted to success; place is not retried | Broker-side error semantics still need sandbox confirmation |
| Order-update WebSocket fixture | **PASS** — `TestFakeArrowWebSocketLifecycleAndReconnect` (re-verified 2026-08-19) | Ack/open, partial fill, complete fill, cancel, reject, unknown status, malformed keepalive text, duplicate identity, out-of-order delivery, close, and reconnect are exercised; reconnect emits observation only | Fixture uses the bridge seam rather than a live Arrow socket. A reconnect flakiness bug was root-caused and fixed in this pass: the loop drained buffered postbacks on disconnect and reset the reconnect interval to the caller-supplied backoff instead of a hard-coded 1s; the fixture is now deterministic at 30/30 runs |
| Race/lifecycle suite | **PASS** — 24 Go tests via `go test -race ./...`; `go vet ./...` (re-verified 2026-08-19, now genuinely race-clean after the reconnect fix above) | Request handling, reconnect backoff, event identity, and fake broker behavior are race-clean offline | No Rust/Java caller is connected yet |
| Bridge image | **PASS** — `docker compose --profile execution-t3 build execution-bridge` builds (re-verified 2026-08-19, exit 0; image `01_docker-execution-bridge` sha `804a96d4…`) | The code-root build context, pinned Go builder image, vendored SDK replacement, and non-root runtime image build successfully | Image digest is not yet recorded in T0 evidence |
| Resolved network policy | **PASS** — `execution_network_check.py` plus 5 unit tests | `execution-net` is internal; only `execution-bridge` joins `arrow-egress`; no bridge host port; order credentials are rejected outside the ingestion market-data exception | Static/resolved policy is not a cross-container reachability probe |
| Disabled runtime profile | **PASS** — profile health probe and network inspection (re-verified 2026-08-19: `/healthz`→200 `status: UP`, `/readyz`→503 `ready: false, reason: broker_disabled`, `credentials_in_process:false`/`arrow_route_in_process:false`) | Bridge joins exactly `execution-net` and `arrow-egress`; `/healthz` is UP while `/readyz` returns 503 in default `disabled` mode | Fake profile does not authorize execution and has no real Arrow endpoint |

The phase-2 output guarantee is therefore **offline fake/network evidence complete**. T3 remains
partial by design: real sandbox authentication/re-authentication is blocked, gateway/Rust runtime
route probes belong to T8, and real broker/reconciliation evidence belongs to T9. No test in this
phase places a real order or accepts production credentials.

**Re-verification note (2026-08-19):** all seven evidence rows above were re-run and confirmed in a
single pass — 24 Go tests race-clean with `go vet` clean; the WebSocket lifecycle fixture driven
30/30 without a flaky failure after the reconnect fix; the `execution-t3` image built with exit 0;
the five network-isolation unit tests and the live resolved-network-policy gate both passed; and a
`disabled`-mode probe returned `/healthz` 200 (`status: UP`) with `/readyz` 503
(`ready: false, reason: broker_disabled`). Nothing in this phase validates a live Arrow order or
accepts production credentials.

### Task 4 — Replace the executor scaffold with a Rust Nautilus service — **NOT IMPLEMENTED**

**Objective:** Establish the long-lived execution/position authority using the audited Nautilus
Rust APIs.

**Files/components:**

- Replace `code/02_services/04_executor/main.py`, `requirements.txt`, and Python Dockerfile with a
  Rust Cargo binary, or move the Python scaffold to an explicitly historical path and preserve its
  retirement note.
- Create `code/02_services/04_executor/Cargo.toml`, `Cargo.lock`, `src/main.rs`, and modules:
  `config`, `execution_client`, `engine`, `gate_client`, `intent_client`, `projection_client`,
  `health`, `telemetry`, and `shutdown`.
- Pin `nautilus-live`, `nautilus-execution`, `nautilus-portfolio`, `nautilus-risk`,
  `nautilus-event-store`, `nautilus-model`, and `nautilus-common` to the Task 0 commit.

**Target behavior:**

- Build a long-lived `LiveNodeBuilder`/Nautilus execution runtime with no market-data client and
  no strategy actor.
- Register the custom `ExecutionClient` through the stable audited API (`ExecutionClient` is
  `#[async_trait(?Send)]`; the service must respect its single-thread/non-`Send` constraint rather
  than inventing unsafe cross-thread sharing).
- Configure Nautilus OMS, risk, portfolio, reconciliation, and supplementary event-store capture.
- Start `HALTED`; process health never implies `ENABLED`.
- Consume intent/control envelopes from the Java gateway and send bridge commands only after the
  custom gate, attempt, scope, and fence checks pass.
- Apply bridge reports to Nautilus OMS/portfolio/reconciliation and emit normalized events to the
  gateway.
- Stop cleanly: stop new commands, mark in-flight state according to the contract, flush event
  evidence, release the fence, and expose shutdown readiness.

**Dependencies:** Tasks 0–3; approved Nautilus pin; private protocol schemas.

#### Repository audit and implementation handoff — T4

| Surface | Verified current state | Required implementation path |
| --- | --- | --- |
| Current service | `code/02_services/04_executor/main.py` raises `NotImplementedError`; no Cargo manifest or Rust source exists | Replace the scaffold with a Rust binary and retain a short retirement note; do not leave a Python fallback that can be mistaken for the executor |
| Nautilus client API | Pinned `nautilus-common/src/clients/execution.rs` defines `ExecutionClient` with `#[async_trait(?Send)]`, lifecycle methods, order commands, and async report/reconciliation methods | Implement a real `ExecutionClient` plus `ExecutionClientFactory`/`ClientConfig`; keep all client state on the LiveNode single-threaded runtime using `Rc<RefCell<...>>`-compatible patterns. Do not add `Send`/`Sync` wrappers or unsafe sharing |
| Node construction | `nautilus-live` exposes `LiveNodeBuilder::from_config`, `.add_exec_client`, `.with_reconciliation`, `.with_event_store`, and `LiveNode::run`; builder tests and `adapters/betfair/tests/node.rs` provide the construction pattern | Build the node through these public APIs, with no data client and no strategy. Use the runner/message-bus command path for gateway-intent ingress; do not call private Nautilus internals or directly mutate its cache |
| Event store | `nautilus-event-store/README.md` states early-alpha, single-node redb per run, immediate durability, verifier, and replay APIs; `nautilus-live` requires an event-store factory when configured | Treat event store as supplementary engine history. Boot failure, verifier failure, corruption, or replay mismatch keeps the service halted/reconciling and must be visible in readiness; it is not the sole policy audit store |
| Required credentials | The Rust service must talk only to the Java gateway and Go bridge; it must not receive `ARROW_*` or Fluss credentials | Give Rust only private protocol auth and endpoint configuration. Static scans and container env inspection must prove no Arrow SDK or Fluss client dependency is present |

**T4 implementation sequence:**

1. Create `Cargo.toml` with git dependencies pinned to `rev = 74d57e7e...`, the exact feature set
   needed by `nautilus-live`/event store, and a checked-in `Cargo.lock`. Build against Rust
   `1.97.1`; do not depend on the developer's installed toolchain.
2. Implement protocol clients first: strict JSON envelope decoding, version/hash/scope checks,
   deadline handling, bounded channels, and redacted errors. Separate gateway commands, bridge
   commands, normalized reports, and health state types.
3. Implement the custom `ExecutionClient` lifecycle. `start`/`stop`/`reset`/`dispose` must be
   idempotent; `connect` must establish the bridge event stream without enabling trading; bridge
   disconnect produces `UNKNOWN`/reconciliation state as appropriate.
4. Implement a `LiveNodeBuilder` factory using a neutral Arrow venue/instrument mapping approved
   by the protocol contract. The client maps Nautilus order types/side/quantity/price to the Go
   bridge's platform-neutral `OrderCommand`; it never constructs Arrow SDK requests.
5. Implement the service orchestrator around the node: receive a validated intent, run the custom
   gate/attempt authorization, submit a Nautilus `TradingCommand`, let Nautilus risk/OMS/portfolio
   process the lifecycle, and forward normalized events to the gateway. The orchestrator must not
   create a second arithmetic position model.
6. Configure startup as `HALTED` even when process health is good. Startup reconciliation and event
   store replay can move the service only to `RECONCILING`/`APPROVAL_PENDING`; only the custom gate
   can permit `ENABLED`.
7. On shutdown, stop ingress, reject new commands, drain bounded report queues, flush/seal event
   evidence, persist unresolved attempts, release the fence, and expose a non-ready shutdown state.

**T4 output guarantee:** a Rust process built from the locked commit starts with a fake bridge,
constructs the real Nautilus node, processes a fake order/fill/position lifecycle, emits normalized
events, and restarts into `HALTED`/`RECONCILING` without retrying an unresolved attempt. A container
inspection proves it has neither Arrow nor Fluss credentials.

**Atomic TODO checklist:**

- [ ] Replace or formally retire the Python scaffold and create the Rust binary, lockfile,
  configuration, health, telemetry, and shutdown paths.
- [ ] Build with Rust `1.97.1` and a Cargo git revision/lockfile; record the exact feature set and
  package metadata in the T0 evidence bundle.
- [ ] Pin every Nautilus crate to the audited commit and verify the audited `ExecutionClient` API.
- [ ] Verify the public `LiveNodeBuilder`/`ExecutionClientFactory` construction path with a compile
  probe before implementing the service orchestration.
- [ ] Implement the custom client lifecycle while respecting its non-`Send` constraint.
- [ ] Build the `LiveNodeBuilder` runtime with OMS, risk, portfolio, reconciliation, and
  supplementary event-store configuration, without a market-data client or strategy actor.
- [ ] Start in `HALTED` and separate process health, readiness, gate state, and trading readiness.
- [ ] Consume gateway envelopes and emit bridge commands only after gate, attempt, scope, and fence
  checks.
- [ ] Apply bridge reports to Nautilus and emit normalized events back to the gateway.
- [ ] Implement clean shutdown, event flush, fence release, and unresolved-attempt recovery.
- [ ] Add boot/config, lifecycle, fake-bridge, fill/position, event-store, invalid-report, and
  restart tests.
- [ ] Run formatting, clippy, locked build/test, and health/readiness validation.

**Output guarantee:** A long-lived Rust service starts halted, boots against a fake bridge, owns
live order/portfolio/reconciliation state, emits normalized events, and restarts without silently
retrying unresolved broker outcomes. It contains neither Arrow credentials nor Fluss credentials.

**Evidence:** Rust lockfile/build metadata, fake-bridge lifecycle transcript, position/fill tests,
event-store verification/replay report, crash-window report, and health output.

**Exit gate:** Pass only when a fake lifecycle works, invalid/uncertain startup and restart states
remain halted or reconciling, and all locked Rust checks pass.

**Edge cases/failure behavior:** missing config, incompatible Nautilus API, event-store open/verify
failure, gateway disconnect, bridge disconnect, invalid report, thread/lifecycle failure, and
restart with unresolved attempts all keep the service halted or reconciling.

**Tests:**

- Service boot with missing/invalid config fails closed.
- Custom `ExecutionClient` lifecycle (`start`, `connect`, `stop`, `disconnect`, `reset`, `dispose`)
  is idempotent.
- Fake bridge command/report tests for order lifecycle and fills.
- Nautilus position/PnL tests for open/add/reduce/close/re-entry/unknown cases.
- Event-store capture/replay smoke tests and corruption/verification handling.
- Process restart tests prove unresolved attempts are not silently retried.

**Validation:** `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
`cargo test --locked`, and service health/readiness tests.

**Completion criteria:** The Rust service boots and runs with a fake bridge, owns live execution
state, starts halted, and has no Arrow or Fluss credentials.

### Task 5 — Implement durable gate, attempt, correlation, and fencing control — **PARTIAL: prerequisite models only**

**Objective:** Move existing pure-JVM safety rules into the gateway/service runtime without
weakening them.

**Files/components:**

- Reuse and extend `GateTransitionValidator`, `GateState`, `AttemptPhase`, `AttemptStore`,
  `AttemptRecord`, `AuditHashChain`, `AuditDeletionControl`, and identity types in `code/common`.
- Add gateway-backed implementations for `Execution_Gate`, `Execution_Attempts`,
  `Order_Correlation`, `Execution_Audit`, and `Safety_Halt_Requests`.
- Add Rust orchestration tests around the gateway protocol.

#### Repository audit and implementation handoff — T5

| Surface | Verified current state | Required implementation consequence |
| --- | --- | --- |
| Pure attempt model | `InMemoryAttemptStore` and `AttemptRecord` implement deterministic prepare, same-ID/hash duplicate, changed-hash halt, monotone `phase_epoch`, and `UNKNOWN` resolution; they are not durable and are not thread-safe | Port the rules behind a single gateway-backed writer; never use the in-memory store as a production cache or claim it provides fencing |
| Gate validator conflict | `GateTransitionValidator` currently allows `ACCEPTED → REJECTED`, while `AttemptRecord.TERMINAL_PHASES` makes `ACCEPTED` terminal and `InMemoryAttemptStore` rejects terminal transitions | T5 must reconcile the transition matrix before runtime coding. The durable contract must make terminal outcomes terminal; update the pure validator/tests and record a change if the existing behavior is intentionally corrected |
| Gate schema | `11_execution_gate.sql` has state/epoch/approvals but no owner, lease, or fencing-token columns | Add the approved fencing representation to the DDL/layout/manifest before implementing writes. Every attempt must persist the exact gate epoch and fence token used for authorization |
| Fluss concurrency | `KvStateUpdateProtocol` documents no raw-client CAS and Fluss KV is last-write-wins; `CompatFlussIntegrationTest` confirms this limitation | A gateway must not infer single-writer safety from a KV upsert. Use the deployment leadership/fencing mechanism required by `ASM-EXE-005`/`REQ-EXE-012`, publish the acquired epoch/token into gate state, and fail closed on lease loss or storage uncertainty |
| Existing audit helpers | `AuditHashChain` and `AuditDeletionControl` are pure policy/integrity logic; `AuditLogger` does not create a durable execution writer | Persist immutable `Execution_Audit` events through the gateway and include the hash/evidence references needed to reconstruct each crash window |

**T5 implementation sequence:**

1. Freeze one canonical gate/attempt transition table and update `GateTransitionValidator` and
   `InMemoryAttemptStore` tests to agree. No runtime task starts while `ACCEPTED → REJECTED` has
   two different meanings.
2. Use the existing ZooKeeper 3.9.2 service for fencing: acquire an ephemeral per-partition lease
   with a monotonically increasing fencing sequence, mirror it into `Execution_Gate`, and reject
   stale owner/sequence values immediately before authorization. The first implementation uses one
   worker for `partition-0`; the lease still protects against an accidental second worker.
3. Extend the gate schema/layout and change record with explicit `owner_instance_id`,
   `fence_token`, `lease_expires_ts`, and acquisition/loss evidence. Keep the gate PK/routing
   account-safe; `epoch` remains the gate-generation value and is not a substitute for the fence
   token.
4. Implement a durable transaction-like command protocol even though Fluss has no cross-table
   transaction: write `PREPARED` plus hash/ref/epoch/fence, await acknowledgement, write
   `SUBMITTING`, perform exactly one bridge request, then write call evidence and terminal/unknown
   state. The ledger/audit rows document the order of independent writes.
5. Implement halt propagation from malformed/stale/cross-scope requests, `Safety_Halt_Requests`,
   lease loss, unknown response, missing health, and projection uncertainty. Applied halt requests
   increment the gate epoch once; duplicate IDs are no-ops; lower source epochs are rejected/audited.
6. Implement two-person approval over the same gate epoch and evidence hash. Approvals must be
   authenticated, distinct, authorized, immutable, and rejected when the gate epoch changes.
7. Inject crash points before durable prepare, after prepare, after `SUBMITTING`, during bridge
   call, after broker acceptance, and before evidence acknowledgement. Assert no automatic second
   place request and that restart begins halted with reconciliation required.

**T5 output guarantee:** every money-moving command has durable pre-call identity, gate epoch, fence
token, request hash, client reference, explicit outcome, and immutable audit evidence. A fake broker
test proves zero duplicate place calls for every injected crash window and stale-owner interleaving.

**Target behavior:**

1. Validate current gate epoch and fenced owner.
2. Persist `PREPARED` attempt with request hash and deterministic client reference.
3. Transition to `SUBMITTING` only after durable state acknowledgement.
4. Call the bridge exactly once for that attempt.
5. Classify verified acceptance/rejection; all ambiguity becomes `UNKNOWN`.
6. For `UNKNOWN`, persist evidence, halt, and require reconciliation before resolution.
7. Permit `UNKNOWN → ACCEPTED/REJECTED/CANCELLED` only through verified reconciliation.
8. Require two distinct approvals for the same gate epoch/evidence hash.
9. Reject stale lease/fence/epoch immediately before the bridge command.

**Atomic TODO checklist:**

- [ ] Reconcile the existing `GateTransitionValidator` matrix with `AttemptRecord` terminal
  semantics; add a single canonical transition test before durable wiring.
- [ ] Add the owner/lease/fence fields and layout/manifest/change record required by
  `Execution_Gate`; do not claim the existing epoch-only DDL provides a lease.
- [ ] Map the existing validator/state/attempt models to the durable gateway-backed tables.
- [ ] Implement gate epoch ownership, lease, fence, and two-person approval checks.
- [ ] Persist `PREPARED` with request hash and deterministic client reference before any bridge
  call.
- [ ] Persist `SUBMITTING` only after durable acknowledgement and issue one bridge call per
  attempt.
- [ ] Implement verified acceptance/rejection and `UNKNOWN` classification.
- [ ] Persist unknown evidence, halt the affected scope, and require reconciliation for resolution.
- [ ] Reject stale epoch/fence, cross-scope, malformed, concurrent-owner, and missing-approval
  requests without a broker call.
- [ ] Add crash-window tests before/during/after the bridge call and assert zero duplicate fake
  broker orders.
- [ ] Run common Maven, gateway Fluss, and Rust orchestration validation.

**Output guarantee:** Every broker command has a durable pre-call record, request hash, client
reference, gate epoch, valid fence, exactly one attempt, and an explicit outcome. An ambiguous
outcome becomes an auditable halted `UNKNOWN` state and cannot be auto-retried.

**Evidence:** Durable table rows around each crash window, approval/fence test report, unknown
reconciliation report, and zero-duplicate fake-broker assertion.

**Exit gate:** Pass only when all command paths are fenced and durable before invocation, every
uncertainty halts, and no crash-window test produces a duplicate command.

**Failure behavior:** any storage uncertainty, lease loss, concurrent owner, malformed halt request,
cross-scope request, stale source epoch, missing approval, or reconciliation conflict prevents the
broker command and records an immutable audit event.

**Tests:** use existing `InMemoryAttemptStoreTest`/validator tests as baseline and add durable
integration tests for duplicate instruction, changed hash, stale epoch, lease loss, crash before/
during/after bridge call, unknown outcome, two-person approval, and cross-scope isolation.

**Validation:** common Maven tests; gateway Fluss integration; Rust fake-bridge crash-window suite;
assert zero duplicate fake broker orders.

**Completion criteria:** Every broker command has durable pre-call state, a valid fence, a known
gate epoch, explicit outcome classification, and auditable recovery behavior.

### Task 6 — Implement postback capture, projections, and position parity — **PARTIAL: reference helpers only**

**Objective:** Make Nautilus-derived execution events durable and queryable in Fluss without a
second position authority.

**Files/components:**

- Gateway `ProjectionWriter`, `ProjectionLedger`, and table serializers.
- Fluss projection schemas/classes for `Fills`, `Order_Lifecycle`, `Positions`, `Postback_Quarantine`,
  `Execution_Audit`, `Execution_Attempts`, and `Order_Correlation`.
- Rust normalized event emitter and adapter report mapping.
- Existing Java `PositionProjector` tests retained as differential oracle only.

#### Repository audit and implementation handoff — T6

| Surface | Verified current state | Required implementation consequence |
| --- | --- | --- |
| Position arithmetic | `PositionProjector`/`PositionProjectorDriver` implement BUY/SELL quantity, weighted averages, lifecycle transitions, oversell rejection, duplicate/stale/conflict handling, deterministic position IDs, and re-entry IDs; tests are green | Keep these classes as a differential oracle only. Nautilus must calculate production position/PnL; projection code serializes Nautilus events and must not call `PositionProjector` in the production path |
| Fluss position store | `FlussPositionsStateStore` proves raw-client single-key `Positions` upsert/lookup; its live test only covers a scratch table and last-write-wins | Reuse the row layout/serializer patterns, but add source-version rejection in the gateway before upsert because raw KV itself has no CAS |
| Fill context gap | `Fills` DDL has no `side`; `FillEventMapper` requires the caller to resolve side/instrument through `FillContext` and currently uses `receive_time` as a non-authoritative source version | Nautilus normalized events must carry side, instrument, trade context, and a stable event-store/event sequence. Never infer side from broker proximity or from a missing field; do not use wall-clock receive time as the only version for live replay |
| Ownership documentation | `PositionsColumnOwnership` and `OrderLifecycleColumnOwnership` still name Action Capture as the writer, reflecting the earlier design | T6 must change the ownership matrix/contracts to the Nautilus projection boundary and keep Java projector classes explicitly parity-only. This is a documentation/contract change, not a second writer |
| Capture runtime | No `code/02_services/03_action_capture` runtime and no Nautilus event-to-Fluss projection writer exist | Implement normalized event intake, correlation, quarantine, ledger, and serializers in the gateway/Rust boundary; do not revive the old Java action-capture service as a second production engine |

**T6 implementation sequence:**

1. Define the normalized event envelope produced by Rust: event-store sequence/source version,
   postback event ID, fingerprint version, broker/client/platform identities, instrument/context,
   order status, cumulative/pending/fill quantities, prices, event/receive timestamps, and the
   mapping version. Preserve original Arrow bytes/hash separately when available.
2. Implement correlation precedence exactly as `broker_order_id` → echoed `client_order_ref` →
   approved reconciliation result. Missing, multiple, or conflicting matches go to
   `Postback_Quarantine`, emit an audit event, and halt the affected scope.
3. Implement lifecycle monotonicity before writing `Order_Lifecycle`: exact duplicate is a no-op;
   older source version is stale evidence; equal version with different content, terminal
   regression, impossible quantity, or unknown status is `UNKNOWN`/quarantine/halt.
4. Implement per-table serializers and ownership guards. Use the T2-approved Fluss-compatible
   bucket settings for composite projections, full KV images, awaited write futures, and ledger
   transitions. A partial write never becomes `COMPLETE` until every required table acknowledgement
   is observed.
5. Implement deterministic replay from captured normalized fixtures and a scratch Fluss rebuild.
   Compare valid fill/position sequences with `PositionProjectorDriver`; any arithmetic or lifecycle
   mismatch blocks Nautilus authority promotion. The oracle is not invoked by runtime projection.
6. Add recovery scans for every ledger state, duplicate/conflicting event tests, and a proof that
   deleting/rebuilding Fluss projections does not alter Nautilus state or issue a broker command.

**T6 output guarantee:** the same captured normalized event set produces identical Fluss rows,
source versions, fingerprints, and ledger completion after clean replay or restart; Nautilus is the
only production position/PnL calculator; no projection code can authorize a broker command.

**Target behavior:**

- Every postback gets `postback_event_id` and versioned fingerprint.
- Correlation order is broker ID → echoed client reference → approved reconciliation query.
- Unknown/malformed/ambiguous evidence is quarantined and halts the affected flow.
- Lifecycle state cannot regress; equal-version conflicting content becomes `UNKNOWN`.
- Nautilus computes positions/PnL; projection code serializes Nautilus position events and does not
  recompute arithmetic.
- `Postback_Projection_Ledger` makes independent Fluss writes restart-safe and idempotent.

**Atomic TODO checklist:**

- [ ] Define the normalized Nautilus event envelope with a stable event-store/source sequence;
  preserve side/context/instrument fields that the `Fills` DDL does not carry.
- [ ] Re-scope `PositionsColumnOwnership`/`OrderLifecycleColumnOwnership` and related contracts
  from the retired Action Capture writer to the Nautilus projection boundary, keeping Java classes
  parity-only.
- [ ] Implement normalized postback event IDs, fingerprints, source versions, and correlation
  precedence.
- [ ] Implement quarantine for unknown/malformed/ambiguous evidence and terminal-regression
  protection.
- [ ] Serialize Nautilus order/fill/position events into `Fills`, `Order_Lifecycle`, `Positions`,
  audit, correlation, attempt, and quarantine projections.
- [ ] Ensure projection code never recomputes position/PnL arithmetic or authorizes a broker action.
- [ ] Implement projection-ledger transitions, restart replay, duplicate no-op, and conflicting
  content handling.
- [ ] Replay recorded fixtures through Nautilus and compare valid results with the Java reference
  projector; retain the projector as a differential oracle only.
- [ ] Add fill overrun, missing instrument, stale event, terminal regression, and partial-write
  tests.
- [ ] Run projection DDL agreement, Rust replay, gateway Fluss integration, and rebuild checks.

**Output guarantee:** Deleting and rebuilding Fluss projections from captured normalized events
reproduces the same rows and source versions without changing Nautilus state or authorizing an
order. Position/PnL arithmetic exists in one production authority only: Nautilus.

**Evidence:** Fixture replay report, differential parity report, ledger restart report, scratch
Fluss rebuild row/source-version comparison, and quarantine test output.

**Exit gate:** Pass only when rebuild is deterministic, duplicate/conflicting events have the
  specified dispositions, and no projection path can issue a broker command.

**Edge cases/failure behavior:** duplicate postback, same event ID with different content, stale
postback, terminal regression, unknown status, fill overrun, missing instrument mapping, partial
projection write, and restart during each ledger state.

**Tests:**

- Recorded Arrow update/fill fixtures replay through Nautilus and projection gateway.
- Differential comparison against `PositionProjectorDriver` for valid cases.
- Negative cases prove Nautilus and the reference model agree on violation/unknown disposition.
- Ledger restart/replay/idempotency tests.
- DDL projection column agreement and source-version precedence tests.

**Validation:** common Maven audit/position tests, Rust replay tests, gateway Fluss integration,
and exact row-count/source-version checks in a scratch Fluss database.

**Completion criteria:** Fluss projections can be deleted/rebuilt from captured events and never
authorize a broker action or replace Nautilus arithmetic.

### Task 7 — Replace the Babysitter marker source with Positions changelog — **PARTIAL: marker shell only**

**Objective:** Complete the separate Flink position-observation boundary while preserving MVP no-op.

**Files/components:**

- Modify `code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java`.
- Add `Positions` row schema/deserializer, version gate, observation state, metrics, and source
  configuration.
- Extend `BabysitterJobTest` and add checkpoint/recovery integration tests.

#### Repository audit and implementation handoff — T7

| Surface | Verified current state | Required implementation consequence |
| --- | --- | --- |
| Existing source pattern | `SafetyHaltJob` already uses `FlussSource<RowData>`, `OffsetsInitializer.full()`, RowKind filtering, counters, and a pure RowData bridge | Mirror this pattern for `Positions`; do not invent a raw-client polling source inside the Flink job |
| Current Babysitter | `BabysitterJob.buildTopology()` uses `fromElements(0L)` → marker → discard, explicit UIDs, and no action sink; tests prove only the shell | Replace only the marker source/observation operator. Preserve the no-action output boundary and existing UID compatibility deliberately; document any UID migration before changing it |
| Position schema | `10_positions.sql`/`PositionsColumns` define a 17-column KV row keyed by `position_id`; KV changelog rows are full images per the Fluss compatibility tests | Deserialize the full image, validate schema/version/positive identity/quantity invariant, and key observation state by `position_id` |
| Failure posture | The current action flag is hard-coded false and `validateActionFlag` fails closed for any non-false value | Keep startup failure for `POSITION_ACTIONS_ENABLED=true`; malformed/missing source, stale/conflicting version, checkpoint failure, and unknown restore state must produce not-ready/no-action, not silent skip |

**T7 implementation sequence:**

1. Add a small Babysitter config reader for `FLUSS_BOOTSTRAP_SERVERS`, database/table,
   checkpoint interval/storage, freshness threshold, and `POSITION_ACTIONS_ENABLED`; all required
   production values must fail closed rather than default to a live path.
2. Build a `FlussSource` over `Positions` with full snapshot+changelog offsets, current-value
   RowKind filtering (`INSERT`/`UPDATE_AFTER`), and a pinned source UID. Use a keyed process
   function by `position_id` with checkpointed `ValueState` containing the latest source version,
   source event ID, last update timestamp, and schema version.
3. Apply `KvStateUpdateProtocol` semantics: same version/same event is a no-op; lower version is
   stale; equal version/different content or malformed state is conflict/not-ready. Do not emit a
   `Position_Actions` row for any outcome in MVP.
4. Add metrics for observed/applied/duplicate/stale/conflict/malformed rows, source lag/freshness,
   checkpoint restore, and action suppression. Health must distinguish a running no-op observer from
   a current/fresh observer.
5. Add a MiniCluster test that writes two full position changelog versions, checkpoints, restores,
   replays duplicates/stale/conflicting rows, and asserts exactly zero action records and stable
   observed state. Add write-path/static inspection proving no Arrow, lifecycle, position, or
   execution table sink exists.

**T7 output guarantee:** a restored MiniCluster Babysitter consumes the real `Positions` changelog,
retains the latest validated observation, and emits zero action records for valid, duplicate, stale,
malformed, and failure inputs. Any action-enable attempt fails startup before a job is submitted.

**Target behavior:**

- Read `Positions` KV changelog with versioned source schema.
- Store only latest accepted version/freshness metadata per `position_id`.
- Emit no `Position_Actions` in MVP and fail startup if `POSITION_ACTIONS_ENABLED` is not unset or
  false.
- Do not mutate lifecycle/position/execution tables.

**Atomic TODO checklist:**

- [ ] Add strict Babysitter source/checkpoint/freshness configuration with no live-action default.
- [ ] Replace `fromElements(0L)` with a versioned `Positions` KV changelog source.
- [ ] Add row deserialization, schema/version validation, and latest-version/freshness state by
  `position_id`.
- [ ] Add checkpoint/restore handling and metrics for source gaps, stale events, and conflicts.
- [ ] Keep `Position_Actions` empty and fail startup closed when `POSITION_ACTIONS_ENABLED` is not
  unset or false.
- [ ] Add duplicate/stale/conflicting/malformed position and checkpoint/recovery tests.
- [ ] Prove the job does not write lifecycle, position, execution, or Arrow-facing outputs.
- [ ] Run module tests and MiniCluster checkpoint/restore validation.

**Output guarantee:** Babysitter continuously observes the versioned `Positions` changelog,
recovers its observation state after checkpoint restore, and emits exactly zero action records in
MVP under every tested input and configuration failure.

**Evidence:** MiniCluster restore report, zero-action assertion, fail-closed flag tests, malformed
schema/gap test output, and write-path inspection.

**Exit gate:** Pass only when observation recovery is proven and all missing/stale/conflicting
inputs result in not-ready/no-action behavior.

**Failure behavior:** missing table/schema, source discontinuity, stale/conflicting position,
checkpoint failure, or unknown restored state means not-ready/no action; no Arrow path exists.

**Tests/validation:** module-local compute tests, MiniCluster checkpoint/restore, duplicate/stale
position events, malformed schema, and `POSITION_ACTIONS_ENABLED` fail-closed variants.

**Completion criteria:** Babysitter runs continuously from `Positions`, checkpoints observation
state, and proves zero action output.

### Task 8 — Add mock execution and local Compose integration — **PARTIAL: fake broker only**

**Objective:** Exercise the complete service boundary without live broker access.

**Files/components:**

- Add a fake order broker to the execution bridge test module; do not change market-data semantics
  of `MockArrowServer`.
- Modify `code/01_platform/01_docker/docker-compose.yml` to wire the gateway, Rust service,
  execution bridge, Signal/Babysitter jobs, health checks, private networks, and sandbox-only
  secrets. Keep production services and credentials excluded from the local profile.
- Modify Dockerfiles/build scripts for the new Rust/Java/Go services.
- Update `docs/08_implementation/08-local-compose.md` only after the runtime actually passes.

#### Repository audit and implementation handoff — T8

| Surface | Verified current state | Required implementation consequence |
| --- | --- | --- |
| Compose baseline | `code/01_platform/01_docker/docker-compose.yml` runs Fluss/Flink/ingestion and leaves compute/executor/action-capture commented | Add an explicit sandbox execution profile or uncomment only after images/protocols exist; default local startup must remain halted and credential-free |
| Compute image | `code/02_services/02_compute/Dockerfile` builds a standalone Maven project and `submit-jobs.sh` submits Signal and Babysitter, waits for `RUNNING`, but does not wait for a completed checkpoint | T8 must add checkpoint completion verification through Flink REST before local readiness; pass `EXECUTION_INTENT_ENABLED=false` and never pass Arrow variables to compute |
| Rust image | No Rust Dockerfile exists; Nautilus is a sibling git checkout with `Cargo.lock` and Rust `1.97.1` | Build from the pinned git revision with a reproducible Rust image or prebuilt digest. Do not copy a developer working tree or use a floating branch in Compose |
| Gateway image | No gateway module/image exists | Use the Java 17/Fluss dependency layout and expose only the private execution network; no host port and no Arrow SDK/credential |
| Bridge image | T3 Dockerfile exists, defaults to host `0.0.0.0:8787` in container, and the `execution-t3` Compose profile now provides a no-host-port service | Keep it on the private execution network plus bridge-only Arrow-egress network; use `fake` or `disabled` mode in local Compose, never `live`; T8 adds the gateway/Rust peers and runtime probes |
| Network policy | Current Compose has one shared `trading-net`, so it cannot prove direct Arrow isolation | Split execution traffic into an internal `execution-net`; attach only the Go bridge to an external `arrow-egress` network. Add a test that Java/Rust/Flink cannot route to Arrow |

**T8 implementation sequence:**

1. Build each image independently first and record immutable image digests. The Rust image must
   prove the Cargo revision and lockfile; the Java image must prove the Fluss client version; the
   Go image must prove the vendored SDK tree hash.
2. Add Compose networks and service environment with least privilege: Signal/Babysitter → Flink;
   gateway → Fluss and Rust; Rust → gateway and bridge; bridge → gateway-facing execution network
   and Arrow egress; no other service receives Arrow endpoint/credentials.
3. Add readiness dependencies in order: Fluss metadata/schema → Flink JM/TM → jobs `RUNNING` plus
   one completed checkpoint → gateway Fluss/protocol ready → Rust process ready but `HALTED` → fake
   bridge ready. Readiness must never imply `ENABLED`.
4. Wire a deterministic offline scenario: append one valid intent to scratch Fluss, confirm
   `HALTED` yields zero fake-broker calls, then run an explicitly approved test gate in fake mode to
   exercise place/modify/cancel/fill and projection recovery. No live credential is accepted by the
   local profile.
5. Add restart probes for each process and verify durable attempt/ledger replay, Flink checkpoint
   restore, bridge request-cache loss, and no duplicate fake place call. Add missing-config and
   secret-redaction probes before documenting the Compose command.
6. Update `submit-jobs.sh` to wait for `/jobs/<id>/checkpoints` evidence for both jobs and fail
   closed if either job is not `RUNNING` or has not completed a checkpoint.

**T8 output guarantee:** a clean local Compose run produces a machine-readable readiness transcript,
two running/checkpointed Flink jobs, a halted Rust/gateway path, a fake lifecycle and projections,
and zero live Arrow calls. Direct network/credential inspection must fail for every non-bridge
process.

**Target behavior:**

- Startup order is infrastructure → schema readiness → Flink jobs → gateway → Nautilus halted →
  execution bridge sandbox.
- Gateway/Rust/bridge private interfaces are not publicly exposed.
- Local health separates process, readiness, job health, gate state, and trading readiness.
- Job submitter verifies both jobs reach `RUNNING` and complete at least one checkpoint before the
  local acceptance probe reports healthy.
- No local profile can accept production credentials or place a live order.

**Atomic TODO checklist:**

- [ ] Add a fake order broker without changing the market-data `MockArrowServer` semantics.
- [ ] Add an internal execution network and bridge-only Arrow-egress network; prove direct Arrow
  access is unavailable from Java/Rust/Flink containers.
- [ ] Wire gateway, Rust service, execution bridge, Signal/Babysitter jobs, health checks,
  private networks, and sandbox-only secrets in Compose.
- [ ] Add Dockerfiles/build scripts and verify reproducible image builds.
- [ ] Enforce startup order and keep all execution services halted until readiness and approval.
- [ ] Keep execution interfaces private and separate process, readiness, job, gate, and trading
  health dimensions.
- [ ] Make the job submitter verify both jobs are `RUNNING` and have completed one checkpoint.
- [ ] Add clean-start, missing-config, halted-intent, fake lifecycle, service-restart, ledger
  recovery, allowlist, and redaction tests.
- [ ] Run Compose config/build/up and the canonical local acceptance probes.
- [ ] Update local-compose documentation only after the runtime evidence passes.

**Output guarantee:** A clean local Compose run executes the complete intent-to-projection path
against a fake broker, exposes no production credentials or public execution route, proves all
readiness boundaries, and produces zero live orders.

**Evidence:** `docker compose config`, image build logs, service/job/checkpoint probes, fake-broker
transcript, restart recovery report, network allowlist result, and secret-redaction result.

**Exit gate:** Pass only when the clean run and every restart/failure scenario pass, both Flink jobs
are running with a checkpoint, the gate remains halted, and no live Arrow route exists.

**Tests:**

- Clean startup with missing schema/config fails safely.
- HALTED intent produces no fake-broker command.
- Sandbox fake place/modify/cancel/fill/timeout/restart flows.
- Compose restart of each service and projection-ledger recovery.
- Network allowlist and secret-redaction tests.

**Validation:** `docker compose config`, build each image, `docker compose up`, Flink REST job/
checkpoint inspection, gateway/Rust/bridge health probes, and the canonical local acceptance tests.

**Completion criteria:** A reproducible local sandbox can run the full flow with no live order and
produce evidence for every service/readiness boundary.

### Task 9 — Arrow sandbox, shadow mode, and release evidence — **NOT IMPLEMENTED**

**Objective:** Prove the external protocol and operational behavior before any live-money review.

**Files/components:**

- Arrow sandbox credentials/configuration outside Git.
- Bridge sandbox tests and evidence files under `logs/` (ignored).
- `docs/08_implementation/11-testing-and-release.md`, `08-local-compose.md`,
  `09-production-swarm.md`, `10-observability.md`, and the applicable contracts.

#### Repository audit and implementation handoff — T9

T9 is entirely evidence work; no repository test can manufacture broker-side evidence.

| Evidence item | Current repository evidence | Required external action and artifact |
| --- | --- | --- |
| Arrow sandbox order path | `Arrow_broker/go-arrow/examples/example.go` contains optional place/modify/cancel code, but the execution bridge has no real sandbox run | Supply sandbox credentials outside Git; run exactly one bounded test order through the bridge; capture sanitized request/response, `remarks`, broker ID, and UTC timestamps |
| WebSocket/fills | SDK `ConnectOrderStream`/`ReadUpdates` exists; no real order-update transcript is present | Capture ack/open/partial-fill/complete/reject/cancel and reconnect behavior, including the raw-payload hash and normalized report; never store tokens/passwords |
| Reconciliation | SDK REST methods exist but no consistency-delay, pagination, or rate-limit measurement exists | Capture orders/trades/positions/order-detail snapshots before/after each sandbox action and record the observed delay/rate-limit behavior; mismatch blocks release |
| Full path | No `Execution_Intent → gateway → Rust → bridge → postback → Fluss` runtime evidence exists | Run the local/fake path first, then sandbox path with gate halted except for the explicitly controlled sandbox test; retain attempt IDs, broker IDs, ledger states, restart results, and projection rows |
| Shadow mode | No shadow-mode service/profile/evidence exists | Run with no new broker commands (gate halted and command counter asserted zero) while consuming permitted broker reports/reconciliation; compare broker, Nautilus, and Fluss positions |
| Retention/audit | `AuditHashChain`/`AuditDeletionControl` are pure code; no one-year offload/recovery proof exists | Record policy identifier/approval, encrypted offload destination class, integrity root, access-control review, restore verification, legal-hold/deletion evidence, and two-person review hash |

**T9 implementation sequence:**

1. Create an ignored evidence-run directory with a manifest containing repository commit, image
   digests, pins, UTC window, account/sandbox identifier (redacted), and test case IDs. Refuse to
   start if a secret appears in the evidence directory.
2. Run the ≤2-minute process/readiness smoke and prove that all services are halted/non-trading
   before any broker request.
3. Place one sandbox order using a dedicated harmless instrument/quantity approved by the owner;
   verify `remarks` round-trip, broker order ID, and no identity collapse. Modify and cancel only
   after the first order's state is verified.
4. Capture all update/fill/rejection/timeout/disconnect/restart/reconciliation scenarios. Any
   timeout remains `UNKNOWN` and requires explicit reconciliation; do not repeat the place request
   to “see if it worked”.
5. Run full projection recovery and shadow mode. Compare exact quantities, average prices,
   lifecycle state, IDs, source versions, and ledger states; a mismatch is a blocker.
6. Run audit offload/restore/integrity verification under the approved policy-controlled minimum
   (baseline one year, longer if policy requires). Record the evidence hash and obtain two-person
   review while the gate is still `HALTED`.

**T9 external inputs required:** sandbox endpoint/account and safe test instrument/quantity;
approved retention-policy identifier/approvers; approved shadow-mode definition; and authorized
operators for the two-person evidence review. Until supplied, mark only the affected T9 children
`BLOCKED:`—do not guess credentials, test size, or approval identity.

**T9 output guarantee:** a hashed, sanitized, independently reviewable evidence bundle proves the
external Arrow contract, recovery/unknown handling, projection convergence, shadow-mode zero-command
behavior, and policy-controlled audit retention. It does not authorize live money by itself.

### Confirmed owner decisions and remaining external inputs

The owner selected the recommended first-phase choices during the plan audit:

| ID | Confirmed decision | Affected tasks |
| --- | --- | --- |
| `INPUT-01` | Start with **one sandbox account and one execution worker**, using `partition-0`; inject the actual account value through deployment configuration | T1, T2, T5, T8 |
| `INPUT-02` | Use **one trade-group ID per trade**: entry/reduce/exit share it; a new entry after closure gets a new ID. The MVP may mint it deterministically from configured account + instrument + candidate identity | T1, T6 |
| `INPUT-03` | Keep the composite table keys and use the verified Fluss setting: `kv.format-version=2`, `Order_Lifecycle` bucketed by `account_scope_id`, `Order_Correlation` by `instruction_id` | T2, T6, T8 |
| `INPUT-04` | Use the existing **ZooKeeper 3.9.2** service for one active worker and stale-worker protection | T5, T8 |
| `INPUT-05` | Run the real Arrow sandbox **later**, after fake-broker and local Compose tests pass | T3, T9 |

The actual sandbox account value, safe test instrument/quantity, retention-policy approval ID, and
two reviewer identities are still external inputs and can be supplied when T9 starts. Until then,
the plan requires configuration placeholders and keeps all live/sandbox order paths disabled.

**Execution sequence:**

1. Run a ≤2-minute smoke of every long-running harness.
2. Place one sandbox order through the bridge; verify `remarks` echo and broker ID.
3. Verify modify and cancel mapping.
4. Capture order-update WebSocket lifecycle and fills.
5. Run partial fill, reject, timeout, disconnect, bridge restart, and reconciliation scenarios.
6. Run Fluss intent → Nautilus → bridge → postback → Fluss projection recovery.
7. Run shadow mode with no new broker commands and compare broker/Nautilus/Fluss positions.
8. Record one-year audit-policy retention/offload/recovery evidence and policy approval.
9. Keep gate `HALTED` until the two-person enablement review explicitly approves the evidence hash.

**Atomic TODO checklist:**

- [ ] Record sandbox endpoint/account, safe instrument/quantity, retention-policy approval ID,
  shadow-mode definition, and two-person reviewers before running external tests.
- [ ] Run the short smoke for every long-running harness and retain process/readiness output.
- [ ] Place exactly one sandbox order through the bridge and verify `remarks` and broker ID.
- [ ] Verify sandbox modify, cancel, WebSocket updates, fills, partial fills, and rejection.
- [ ] Run timeout, disconnect, bridge restart, reconciliation, duplicate, and unknown-outcome
  scenarios without automatic ambiguous-place retry.
- [ ] Run full Fluss intent → Nautilus → bridge → postback → projection recovery.
- [ ] Run shadow mode with no new broker commands and compare broker/Nautilus/Fluss positions.
- [ ] Record policy-controlled one-year minimum retention, encryption, integrity, access control,
  offload, recovery, deletion-governance, and policy-approval evidence.
- [ ] Hash/index the evidence bundle and obtain the two-person review while the gate is halted.
- [ ] Record every failed scenario as a blocker; do not proceed to live money.

**Output guarantee:** An evidence bundle proves the external Arrow contract, unknown-outcome
handling, projection recovery, position parity, retention controls, and shadow-mode no-command
behavior. It does not itself authorize live trading.

**Evidence:** Sandbox transcripts, WebSocket/fill fixtures, recovery reports, shadow comparison,
retention/offload/recovery records, evidence hash, and signed/two-person review decision.

**Exit gate:** Pass only when every required evidence artifact exists, all scenarios pass without
duplicate or ambiguous success, retention is approved as policy-controlled with a one-year minimum,
and the explicit live-money approval remains a separate decision.

**Failure behavior:** any unknown broker result, missing echo, projection mismatch, reconciliation
gap, duplicate order, audit loss, credential leak, or restart ambiguity blocks the next phase.

**Completion criteria:** All `AC-*`, `REQ-EXE-*`, `REQ-AC-*`, `REQ-BB-*`, Arrow sandbox, recovery,
fencing, projection, audit, and local runtime evidence is recorded; live money remains a separate
approval decision.

## 9. Execution Order

| Order | Task | Dependency reason |
| ---: | --- | --- |
| 0 | Version/policy/contract freeze | Prevents implementation against moving Nautilus APIs or ambiguous retention/intent semantics |
| 1 | `Execution_Intent` DDL and producer | The execution service cannot safely consume an undefined or retired feed |
| 2 | Java Fluss gateway | Establishes the supported Fluss transport/control/projection boundary |
| 3 | Go execution bridge | Provides a fakeable and credential-isolated broker protocol before Rust wiring |
| 4 | Rust Nautilus service and custom `ExecutionClient` | Requires the bridge and private protocol, but can be developed against fakes |
| 5 | Durable gate/attempt/fencing | Must wrap the Rust command path before any broker command is allowed |
| 6 | Postback/projection/position parity | Requires Nautilus events and gateway projection surfaces |
| 7 | Babysitter Positions source | Requires the stable `Positions` projection and schema |
| 8 | Compose/local integration | Requires all processes, schemas, health checks, and private protocols |
| 9 | Sandbox/shadow/release evidence | External evidence must run only after deterministic local/fake tests pass |

No task may enable live orders. Tasks 2–8 may run with fake bridge responses and sandbox-disabled
credentials.

## 10. Validation Strategy

### Static and unit validation

```bash
cd /home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project
make docs-audit
make stale-tables
python3 code/01_platform/04_scripts/change_control_check.py \
  --dir docs/05_deployment/change-records
git diff --check

cd code
mvn -o -pl common test

cd 02_services/02_compute
mvn -o test

cd ../01_ingestion/go-bridge
go test ./...
```

### Rust validation

Run from the pinned Nautilus checkout and the new service:

```bash
cargo fmt --check
cargo check --locked
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo check -p nautilus-execution -p nautilus-live -p nautilus-event-store
cargo test -p nautilus-event-store
```

The service build must verify the exact pinned commit, not the current branch tip.

### Flink/Fluss validation

- DDL manifest checksum/parity validation.
- Fluss 0.9.1 table metadata preflight for `Execution_Intent` and all execution tables.
- Intent LOG append and hash-index KV consistency tests.
- Fluss changelog restart and projection-ledger recovery tests.
- Signal/Babysitter checkpoint/restore tests with stable operator UIDs.
- No broker command while gate is `HALTED`, missing state, missing schema, or gateway unavailable.

### Failure and side-effect validation

The test matrix must include:

| Scenario | Expected result |
| --- | --- |
| Duplicate intent, same hash | Existing attempt/no second command |
| Duplicate intent, changed hash | Contract violation, quarantine, audit, halt |
| Timeout after request may have reached broker | `UNKNOWN`, halt, reconcile, no blind retry |
| Bridge 5xx/transport failure | `UNKNOWN`, no success/rejection assumption |
| Broker rejection with verified envelope | Terminal rejection, auditable |
| Duplicate postback | No duplicate lifecycle/fill/position effect |
| Out-of-order postback | Stale evidence rejected; terminal regression quarantined/halts |
| Fill oversell or impossible quantity | `UNKNOWN`, quarantine, affected flow halted |
| Gateway restart with incomplete projection ledger | Resume idempotently from durable ledger |
| Rust service restart with unresolved attempt | Starts halted/reconciling; no automatic order retry |
| Fence loss or concurrent owner | Bridge command rejected; gate halted |
| Fluss unavailable | Not-ready; no broker command |
| OpenObserve unavailable | Local durable audit continues; readiness policy decides live eligibility |
| Babysitter action flag true | Startup fails closed; zero broker calls |

### Runtime evidence

Every long run must record exact artifact commits, image digests, versions, UTC window, load shape,
checkpoint configuration, job IDs, gate state, attempt IDs, broker IDs where sandboxed, projection
ledger state, restart count, recovery duration, and evidence hashes. Evidence belongs under ignored
`logs/` and must not contain credentials or raw secrets.

## 11. Risks and Assumptions

| Risk/assumption | Status and mitigation |
| --- | --- |
| Nautilus API changes | Current `develop` is not a deployment pin; Task 0 requires exact commit and locked build |
| Event-store early alpha | Supplement with Fluss projections, verified external audit storage, backup/restore drills, and one-year policy evidence |
| No Rust Fluss client | Resolved by Java gateway; gateway is transport/projection glue only |
| Arrow wire fields/semantics | Sandbox evidence required; unknown fields map to `UNKNOWN`, never guessed |
| `client_order_ref` 16-character limit | Deterministic 14-character format test and Arrow `remarks` echo test |
| Fluss independent writes | Projection ledger and idempotent source event IDs; no cross-table atomicity assumption |
| Current Signal strategy is a placeholder | Execution tests use fixtures/placeholder only; strategy change is separate scope |
| Existing `PositionProjector` arithmetic differs from Nautilus | Differential replay must pass before production authority is switched; Java projector remains test-only |
| One-year audit target may be legally insufficient | Longer approved policy overrides baseline; Compliance approval is a live-money gate |
| Local Compose is single-host | It proves integration only, not HA, replication, or VM-loss tolerance |
| Credentials | Only Go bridge processes receive Arrow credentials; secrets remain ignored/injected |
| Live money | Permanently blocked until all acceptance/evidence gates and explicit approval pass |

## 12. Final Implementation Checklist

- [~] `T0` is complete only when dependency, policy, protocol, and change-record evidence is
  present.
- [~] `T1` is complete only when the disabled-by-default immutable intent contract and hash tests
  pass.
- [ ] `T2` is complete only when the Java gateway proves durable Fluss input/control/output and
  has no execution authority.
- [~] `T3` is complete only when the isolated Go bridge proves the fake Arrow lifecycle and
  `UNKNOWN` behavior.
- [ ] `T4` is complete only when the Rust Nautilus service owns fake live execution state and
  starts halted without Arrow/Fluss credentials.
- [~] `T5` is complete only when fencing, approvals, crash windows, unknown outcomes, and zero
  duplicate commands are proven.
- [~] `T6` is complete only when projections rebuild deterministically and position arithmetic is
  Nautilus-only.
- [~] `T7` is complete only when Babysitter recovers `Positions` observation and emits zero actions.
- [~] `T8` is complete only when local Compose proves the full sandbox topology and readiness
  boundaries with no live route.
- [ ] `T9` is complete only when external sandbox/shadow/retention evidence and the halted
  two-person review are recorded.
- [~] Exact Nautilus commit, Cargo lock, Rust toolchain, Go SDK commit, and protocol versions pinned.
- [~] One-year minimum audit policy approved; longer policy override and deletion/legal-hold evidence defined.
- [~] `Execution_Intent` LOG DDL, manifest, schema classes, hash index, and contract tests complete.
- [x] Removed ~~ranking-era~~ `Trade_Decisions` producer remains disabled and is not consumed by execution.
- [ ] Java Fluss gateway reads intent/control state and writes idempotent projections.
- [ ] Rust Nautilus service replaces the Python scaffold and starts `HALTED`.
- [ ] Custom Nautilus `ExecutionClient` lifecycle and bridge protocol tests pass.
- [~] Separate Go execution bridge owns order-path Arrow credentials and passes fake protocol tests.
- [~] No Flink, Java gateway, Rust service, or Fluss client calls Arrow directly.
- [~] Durable gate, attempts, request hashes, client references, fencing, approvals, and unknown handling pass crash-window tests.
- [ ] Nautilus is the only production order/fill/position/PnL authority.
- [ ] Fluss projections and `Postback_Projection_Ledger` recover idempotently.
- [~] Java position projector is parity-only and does not write production positions.
- [~] Babysitter consumes `Positions`, checkpoints observation state, emits zero actions, and fails closed.
- [ ] Local Compose runs the full sandbox topology with no production credentials/live order path.
- [~] Signal and Babysitter jobs are running and checkpointing before local readiness passes.
- [ ] Arrow sandbox place/modify/cancel/update/fill/reconciliation evidence is complete.
- [ ] Restart, unknown outcome, fencing, duplicate, quarantine, and projection recovery evidence is complete.
- [ ] Observability, readiness, audit, retention, rollback, and shutdown runbooks are complete.
- [ ] All required unit, integration, checkpoint/restore, failure, and runtime gates pass.
- [ ] Live-money status remains `BLOCKED` until explicit two-person evidence review approves enablement.

## Post-Completion / External Actions

These cannot be completed by repository code alone:

1. Compliance approval of the one-year minimum or a longer jurisdiction-specific retention policy.
2. Arrow sandbox credentials and broker-side protocol evidence.
3. Production secret provisioning, Swarm deployment, HA/VM-loss drills, and operator approval.
4. External backup/archive verification for the Nautilus event-store and policy-controlled audit store.
5. Final live-money enablement decision by the authorized operators.
