# Streaming Trading Data Platform — Implementation Plan

<!-- markdownlint-disable MD013 -->

> **Purpose:** single source of truth for implementing and validating the issues identified in the code audit.
>
> **Scope:** this plan covers every P0/P1/P2/P3 finding from the audit of `code/`. It is intentionally implementation-oriented, but this file itself does not implement production code.
>
> **Current status:** scaffold only. Live-money order placement is prohibited until the final release gates in this document pass.

---

## How to use this file

### Checklist status convention

Use exactly one status for every task:

- `[ ]` **Not started**
- `[-]` **In progress / blocked** — add a note directly below the task
- `[x]` **Implemented but not verified** — code exists, acceptance evidence is still missing
- `[X]` **Verified and complete** — only this state may be struck through
- `~~[X] ...~~` **Struck through after verification** — optional final cleanup after the task is complete

A task must **not** be marked `[X]` merely because code was written. It requires the evidence listed in its acceptance criteria.

### Required task record

For every completed task, record:

```text
Evidence: <test, command, artifact, report, or review link>
Date: YYYY-MM-DD
Owner: <person/team>
Notes: <important limitation or follow-up>
```

### Rules

1. Do not enable broker order placement while any P0 task, safety gate, or release blocker is incomplete.
2. Do not replace an unknown broker/protocol/version value with a plausible guess. Use an explicit placeholder ending in `_TO_BE_PINNED`, `_TO_BE_VERIFIED`, or `_TO_BE_DEFINED`.
3. A documentation-only task is complete only when stale guidance cannot lead an implementer toward the superseded architecture.
4. Every schema or protocol change must update its DDL, contract, requirements traceability, tests, and operational documentation together.
5. Every failure-path task must include restart, duplicate, timeout, partial-completion, and recovery behavior where applicable.
6. Keep local Compose and production Swarm artifacts separate.

---

## Requirement status

| Field | Current value |
| --- | --- |
| Classification | Architecture reconciliation, scaffold completion, reliability hardening, deployment, observability, and live-money readiness |
| Readiness | **Blocked** |
| Current implementation | Documentation/scaffold; service source trees are largely empty |
| Live-money status | **Disabled** |
| Evidence limitation | Maven was unavailable during the audit; runtime Fluss/Arrow/OpenAlgo behavior is not yet pinned or tested |
| Required runtime inputs | Exact Flink, Fluss, Java, Python, connector, Arrow protocol, OpenAlgo, image, and broker contract versions |
| Primary safety boundary | Executor durable gate before every money-moving call |
| Documentation milestone | Implementation dossiers complete under `docs/08_implementation/`; production code and runtime evidence remain open |

## Documentation-first milestone — 2026-07-26

The implementation-ready documentation layer is complete for the current design scope:

- [X] Documentation governance, evidence records, placeholder policy, and completion rules.
- [X] Version/compatibility matrix and evidence-gated capability checklist.
- [X] Schema lifecycle, immutability, stale-update, routing, retention, and offload contracts.
- [X] Ingestion, Signal job, Action Capture, Babysitter, and Executor implementation dossiers.
- [X] Local Compose, production Swarm, observability, operations, testing, and release dossiers.
- [X] Audit issue, plan phase, requirement, and test-family traceability.
- [X] Developer-facing service and SQL READMEs reconciled to the current architecture.
- [X] Placeholder testing documents replaced with implementation-ready test plans.

Evidence: `docs/08_implementation/00-index.md`, all linked dossiers, `docs/08_implementation/99-traceability.md`, and `docs/07_testing/`.
Date: 2026-07-26
Owner: Documentation-first design pass; Platform and Execution review still required before code implementation.
Notes: This milestone does not complete any production code, DDL validation, runtime deployment, executable test, or live-money release task below. Those checkboxes remain open.

---

## Audit issue register

This register preserves the audit findings. Each issue is addressed by one or more roadmap tasks below.

| ID | Severity | Issue | Main evidence |
| --- | ---: | --- | --- |
| P0-1 | P0 | Executor raises `NotImplementedError` and has no safety protocol | `code/02_services/04_executor/main.py` |
| P0-2 | P0 | Ingestion, compute, and action-capture implementation source is absent | Empty `src/` directories; no service implementation classes |
| P0-3 | P0 | Compute submission script uploads a JAR but never submits jobs; models three jobs instead of two | `code/02_services/02_compute/submit-jobs.sh` |
| P0-4 | P0 | Code targets Kite/sequence IDs while current design targets Arrow/fingerprints | Ingestion/action-capture READMEs, `.env.example`, Compose |
| P1-1 | P1 | Docker images assume executable applications and main classes that do not exist | Service Dockerfiles/POMs |
| P1-2 | P1 | Superseded `Trade_management_table` remains in active implementation guidance | SQL README, service READMEs, Executor |
| P1-3 | P1 | Makefile references deleted/stale DDL files and does not apply schema | `Makefile:22-37` |
| P1-4 | P1 | Execution/audit retention uses short TTL while claiming seven-year retention | Execution DDLs |
| P1-5 | P1 | No implementation extends retention while EOD offload is unverified | Raw/candle/fill DDLs and requirements |
| P1-6 | P1 | Compose has no health/readiness/trading-safety gates | `docker-compose.yml` |
| P1-7 | P1 | Coordinator and tablet share one local volume | `docker-compose.yml:37-57` |
| P1-8 | P1 | No production four-VM Swarm deployment exists | `code/01_platform/01_docker/` |
| P1-9 | P1 | Mutable `latest` image defaults violate version pinning | Compose and `.env.example` |
| P1-10 | P1 | `fluss.properties` is not mounted/applied by Compose | Fluss properties and Compose |
| P1-11 | P1 | S3 lake/checkpoint settings are declared but not wired | `.env.example`, properties, Compose |
| P1-12 | P1 | Nullable fields are used as bucket keys | Fills, audit, quarantine DDLs |
| P1-13 | P1 | DDL comments claim immutability but physical/application enforcement is absent | Signal/ranking/decision/audit DDLs |
| P1-14 | P1 | `partial_update` does not prevent stale/out-of-order state writes; projection code is absent | Lifecycle/state DDLs |
| P1-15 | P1 | Correlation mappings have no uniqueness protocol | `14_order_correlation.sql` |
| P1-16 | P1 | Attempts have no duplicate-prevention protocol | `13_execution_attempts.sql` |
| P1-17 | P1 | Execution audit lacks enough evidence for seven-year reconstruction | `15_execution_audit.sql` |
| P1-18 | P1 | `depends_on` is used as readiness | Compose |
| P1-19 | P1 | Compose is one coordinator/one tablet and cannot prove HA | Compose |
| P1-20 | P1 | Fluss, Flink, OpenAlgo, and OpenObserve management ports are exposed without boundaries | Compose |
| P1-21 | P1 | Secrets are normal environment variables; production Swarm secrets are absent | Compose |
| P1-22 | P1 | All services share one trust network | Compose |
| P2-1 | P2 | Base images and dependencies are not reproducible/pinned | Dockerfiles/POMs/Compose |
| P2-2 | P2 | Compose comment claims an OpenAlgo adapter mount that does not exist | Compose comments |
| P2-3 | P2 | Compute README claims exactly-once without implementation/evidence | Compute README |
| P2-4 | P2 | Ingestion README promises unsupported sequence gap detection | Ingestion README |
| P2-5 | P2 | Action Capture README promises unsupported `postback_seq` idempotency | Action Capture README |
| P2-6 | P2 | Instrument import script is not a real CSV parser | `import_instruments.sh` |
| P2-7 | P2 | Instrument import creates unescaped SQL | `import_instruments.sh:87-109` |
| P2-8 | P2 | Instrument import silently skips invalid rows | `import_instruments.sh:81-85` |
| P2-9 | P2 | “Latest instrument row wins” has no projector/tie-break implementation | Instruments DDL |
| P2-10 | P2 | No meaningful implementation tests exist | `code/` inventory |
| P2-11 | P2 | No schema migration/checksum/reset/replay safety mechanism exists | DDL/Makefile |
| P2-12 | P2 | DDL README calls blocked SQL the single source of truth | SQL README/blocker |
| P2-13 | P2 | Code models three Flink jobs instead of the required two | Compute README/scripts |
| P2-14 | P2 | Executor README says Executor is read-only, contrary to current ownership | Executor README |
| P2-15 | P2 | Order and position ownership remain conflated in scaffolding | Action Capture/Executor docs |
| P2-16 | P2 | No metrics, structured logs, traces, or health endpoints are implemented | All services |
| P2-17 | P2 | No clock/latency measurement implementation exists | All services |
| P3-1 | P3 | Service READMEs contain broken `design/` links | Service READMEs |
| P3-2 | P3 | READMEs use Gradle commands although services use Maven | Service READMEs |
| P3-3 | P3 | Docker image builds explicitly skip tests | Service Dockerfiles |
| P3-4 | P3 | Local Compose comments can be mistaken for production capability | Compose |

---

## Phase 0 — Governance and safety baseline

**Goal:** prevent stale documentation or partial implementation from being mistaken for a functioning trading system.

## 0.1 Establish implementation status

- [ ] Add a repository-level status block stating:
  - `IMPLEMENTATION_STATUS=PHASE_4_2_SCAFFOLD`
  - `LIVE_MONEY_ALLOWED=false`
  - `ORDER_PLACEMENT_ENABLED=false`
  - `PRODUCTION_DEPLOYMENT_STATUS=NOT_IMPLEMENTED`
- [ ] Add a startup/runtime guard that refuses broker calls unless an explicit non-default enablement configuration exists.
- [ ] Ensure the default Executor state is `HALTED` on first start, restart, missing state, unreadable state, or uncertain configuration.
- [ ] Add a release checklist command that fails if any P0 issue, critical risk, or mandatory gate remains open.

**Acceptance evidence:** fresh checkout starts with order placement disabled; missing/corrupt safety state cannot result in a broker call.

## 0.2 Define status and evidence storage

- [ ] Add a decision log or status section for every `_TO_BE_PINNED` and `_TO_BE_VERIFIED` value.
- [ ] Require each completed task in this file to record evidence, owner, and date.
- [ ] Define the canonical location for test reports, packet captures, connector evidence, load results, and deployment manifests.
- [ ] Define who can approve live-money enablement and how approval evidence is retained.

**Acceptance evidence:** an auditor can trace every release claim to a versioned artifact.

---

## Phase 1 — Reconcile code guidance with the current architecture

**Goal:** remove all stale implementation directions before writing service code.

**Addresses:** P0-4, P1-2, P2-2, P2-3, P2-4, P2-5, P2-13, P2-14, P2-15, P3-1, P3-2, P3-4.

## 1.1 Reconcile broker/protocol terminology

- [ ] Replace active Kite/Zerodha implementation guidance with the current evidence-gated broker boundary.
- [ ] Replace `seq_no` assumptions with versioned bounded `event_fingerprint` behavior.
- [ ] Replace `postback_seq` assumptions with `postback_event_id`, fingerprint, broker ID, and verified client-reference correlation.
- [ ] Replace direct broker assumptions with explicit placeholders:
  - `BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED`
  - `BROKER_POSTBACK_PROTOCOL_TO_BE_PINNED`
  - `BROKER_EVENT_ID_AVAILABILITY_TO_BE_VERIFIED`
  - `BROKER_CLIENT_REFERENCE_ECHO_TO_BE_VERIFIED`
  - `BROKER_IDEMPOTENCY_SUPPORT_TO_BE_VERIFIED`
- [ ] Record the selected broker only after protocol evidence and sandbox captures are approved.

**Acceptance evidence:** repository search finds no active implementation instruction that assumes `seq_no`, `postback_seq`, or a broker protocol not approved by the current decisions.

## 1.2 Remove superseded table architecture

- [ ] Remove or mark every `Trade_management_table` reference as `SUPERSEDED — DO NOT IMPLEMENT`.
- [ ] Remove active references to generic overloaded `order_id`.
- [ ] Update all service guidance to use:
  - `Trade_Decisions`
  - `Order_Lifecycle`
  - `Positions`
  - `Execution_Gate`
  - `Execution_Attempts`
  - `Order_Correlation`
  - `Execution_Audit`
- [ ] Update Executor guidance to state that it is an execution-state writer, not read-only.
- [ ] Update Action Capture guidance to distinguish order lifecycle from position lifecycle.

**Acceptance evidence:** repository search shows only historical/superseded references, each visibly marked; no build or runtime path refers to the old table.

## 1.3 Reconcile Flink job topology

- [ ] Update all compute documentation to define exactly two jobs:
  1. Signal job: dedup, candles, forming-bar state, business logic, ranking, decisions.
  2. Babysitter job: checkpointed no-op in MVP.
- [ ] Remove the separate `feature-compute` job from implementation instructions.
- [ ] Remove any implication that ranking is a separate deployment or Fluss round trip.
- [ ] Update job names, ownership, checkpoint expectations, and readiness checks consistently.

**Acceptance evidence:** code, READMEs, deployment files, and contracts all describe exactly two Flink jobs.

## 1.4 Fix documentation paths and build commands

- [ ] Replace broken `../../design/...` links with current `docs/...` and `specs/...` links.
- [ ] Remove references to the nonexistent OpenSpec tree or label them as historical.
- [ ] Replace `./gradlew build` instructions with the actual Maven workflow.
- [ ] Make README artifact names match actual Maven output.
- [ ] Mark Compose as local-only and not production proof.

**Acceptance evidence:** link/path scan finds no active broken implementation links; a new developer can follow the documented build/run path.

---

## Phase 2 — Pin versions and define compatibility evidence

**Goal:** make every external behavior version-specific and reproducible.

**Addresses:** P1-9, P1-10, P1-11, P2-1, P2-12, P3-3.

## 2.1 Pin runtime versions

- [ ] Record exact versions for:
  - Flink
  - Fluss server
  - Fluss Java client
  - Fluss Flink connector
  - Java runtime
  - Python runtime
  - Arrow/broker protocol or SDK
  - OpenAlgo
  - OpenObserve
  - Docker base images
- [ ] Replace all `latest` and floating version defaults.
- [ ] Prefer immutable image digests in production manifests.
- [ ] Define `*_VERSION_TO_BE_PINNED` placeholders where a selection is still pending.
- [ ] Record the compatibility matrix in a versioned file.

**Acceptance evidence:** no production manifest or default Compose path uses `latest`; the version matrix is checked into the repository.

## 2.2 Make builds reproducible

- [ ] Pin Maven plugin versions and base image digests.
- [ ] Pin all runtime dependencies.
- [ ] Define dependency repository/mirror policy.
- [ ] Add reproducible build metadata and artifact checksums.
- [ ] Add SBOM generation.
- [ ] Add vulnerability scan policy and failure thresholds.
- [ ] Split Docker build into test, package, and runtime stages.
- [ ] Remove unconditional `-DskipTests` from the final validation pipeline.

**Acceptance evidence:** two clean builds produce the same versioned artifact metadata and the test stage fails on a failing test.

## 2.3 Validate Fluss/Flink capabilities

- [ ] Prove `BYTES` support for raw payloads.
- [ ] Prove LOG/KV support for all required tables.
- [ ] Prove `partial_update` semantics for each owner column group.
- [ ] Prove `changelog.image=FULL` behavior.
- [ ] Prove connector checkpoint/restart semantics.
- [ ] Prove cross-table visibility limitations.
- [ ] Prove retention and lake-tier properties.
- [ ] Prove replication/quorum configuration.

**Acceptance evidence:** version-pinned integration test report exists for each capability.

---

## Phase 3 — Reconcile and validate the data model

**Goal:** make DDLs safe, internally consistent, and testable before application code depends on them.

**Addresses:** P1-4, P1-5, P1-12, P1-13, P1-14, P1-15, P1-16, P1-17, P2-9, P2-11, P2-12.

## 3.1 Correct DDL authority and application workflow

- [ ] Change DDL README wording from unconditional “single source of truth” to “reconciled DDL blocked pending pinned-version validation” until validation passes.
- [ ] Update `Makefile` to reference all current DDL files in correct order.
- [ ] Remove references to deleted `05_trade_management_table.sql` and `06_gaps.sql`.
- [ ] Decide whether `make ddl` will apply DDL or fail closed with a clear manual command; it must not merely echo stale paths.
- [ ] Add DDL checksums and schema-version metadata.
- [ ] Add clean-create, reset, replay, and destructive-change approval workflows.
- [ ] Add schema parity validation between requirements, contracts, DDLs, and code models.

**Acceptance evidence:** a clean environment can be initialized deterministically, or the command fails with an explicit version/evidence blocker; no stale DDL path remains.

## 3.2 Fix table routing identities

- [ ] Review every `bucket.key` for nullability.
- [ ] Replace nullable routing keys with non-null event identities where needed:
  - `Fills_table` → `postback_event_id` or another guaranteed identity
  - `Execution_Audit` → `audit_event_id`
  - `Postback_Quarantine` → `quarantine_id`
- [ ] Keep nullable business identifiers in payload columns.
- [ ] Test null, missing, and uncorrelated event routing.
- [ ] Test skew and bucket distribution with production-like workload.

**Acceptance evidence:** every accepted event has a non-null routing identity and distribution tests show no null-key hotspot.

## 3.3 Define immutable event enforcement

- [ ] Define duplicate behavior for `Signal_Candidates`, `Ranking_Results`, `Trade_Decisions`, `Fills_table`, and `Execution_Audit`.
- [ ] Define canonical content hashes for immutable records.
- [ ] Enforce:
  - same identity + same content → idempotent duplicate
  - same identity + changed content → contract violation, quarantine, and halt where safety-relevant
  - changed executable parameters → new identity
- [ ] Add replay and concurrent-writer tests.
- [ ] Ensure `NOT ENFORCED` primary keys are backed by application compare-and-set or serialized writer logic.

**Acceptance evidence:** duplicate/replay/mutation tests pass and mutation under an existing immutable identity cannot silently succeed.

## 3.4 Define stale-update and status-precedence rules

- [ ] Define source-version comparison for `Order_Lifecycle`.
- [ ] Define fill-to-position ordering and duplicate rules.
- [ ] Define gate transition compare-and-set behavior.
- [ ] Define attempt phase transitions and terminal-state protection.
- [ ] Define correlation verification transitions.
- [ ] Define conflict behavior: `UNKNOWN`, quarantine, halt, alert.
- [ ] Implement and test rules independently of `partial_update`.

**Acceptance evidence:** out-of-order, regressive, duplicate, conflicting, and concurrent state updates produce deterministic safe results.

## 3.5 Define correlation and attempt uniqueness invariants

- [ ] Define one-attempt-to-one-broker-order invariant.
- [ ] Define one-client-reference-to-one-attempt invariant.
- [ ] Define one-broker-order-to-one-verified-attempt invariant.
- [ ] Define instruction active-attempt uniqueness.
- [ ] Implement atomic or serialized checks before new attempt creation.
- [ ] Test concurrent Executor instances and crash/restart races.

**Acceptance evidence:** no test can create two active submissions for one immutable request or map one broker order to conflicting attempts.

## 3.6 Expand execution audit envelope

- [ ] Add or define versioned audit fields for:
  - request hash
  - response hash
  - redacted request/response evidence
  - gate state before/after
  - executor instance/fencing identity
  - changelog offsets
  - configuration/version snapshot
  - approval identity and authorization result
  - reconciliation query/result evidence
  - clock source and timestamp metadata
- [ ] Define secret redaction rules.
- [ ] Define seven-year reconstruction test.
- [ ] Define immutable audit storage, encryption, versioning, and deletion evidence.

**Acceptance evidence:** an execution incident can be reconstructed from retained audit artifacts without requiring unavailable logs or secrets.

## 3.7 Define instrument manifest projection

- [ ] Define one manifest version as the authoritative subscription input.
- [ ] Define ordering/tie-break rules for multiple rows per instrument.
- [ ] Implement a manifest projector or snapshot loader.
- [ ] Validate field count, types, active state, exchange, instrument type, expiry, tick size, and lot size.
- [ ] Add manifest checksum and import report.
- [ ] Ensure ingestion does not independently infer “latest” inconsistently.

**Acceptance evidence:** identical manifest input produces identical subscription state across restarts and consumers.

## 3.8 Define retention and offload safety

- [ ] Define source retention for each event table.
- [ ] Define EOD manifest contents: date, table/schema version, range, row/byte counts, hashes, commit ID, verification status, retries.
- [ ] Implement offload verification before expiry.
- [ ] Implement retention extension while offload is unverified, retryable, or under reconciliation.
- [ ] Define minimum three-complete-trading-day live retention.
- [ ] Define encrypted seven-year audit retention outside short-lived operational TTL.
- [ ] Define S3 versioning/object-lock/lifecycle policy consistent with legal requirements.
- [ ] Add failed-S3/offline/retry-before-expiry tests.

**Acceptance evidence:** source data cannot expire after an unverified offload; verified audit data remains reconstructable for the approved retention period.

---

## Phase 4 — Implement ingestion

**Goal:** implement the evidence-approved broker stream to Fluss raw LOG path.

**Addresses:** P0-2, P0-4, P2-4, P2-6, P2-7, P2-8, P2-9.

## 4.1 Implement protocol boundary

- [ ] Implement the selected broker protocol behind a versioned adapter boundary.
- [ ] Do not assume a stable broker sequence/event ID until evidence exists.
- [ ] Preserve original packet bytes.
- [ ] Record decoder/protocol version.
- [ ] Quarantine unknown protocol versions and malformed packets.
- [ ] Define bounded reconnect/backoff behavior.
- [ ] Define subscription completeness and readiness checks.

**Acceptance evidence:** golden packet corpus decodes into expected typed fields with byte/hash round-trip proof.

## 4.2 Implement normalized raw append

- [ ] Populate every required `raw_table_1` field.
- [ ] Calculate payload hash.
- [ ] Calculate versioned event fingerprint using the approved canonicalization.
- [ ] Record `connection_id` and `connection_epoch`.
- [ ] Keep raw append at-least-once; do not pretend ingestion performs exact logical deduplication.
- [ ] Bound memory and retry backlog.
- [ ] Record append uncertainty and readiness impact.

**Acceptance evidence:** append retry, duplicate packet, malformed packet, and backpressure tests pass.

## 4.3 Implement discontinuity evidence

- [ ] Emit `suspected_discontinuities` for reconnects, heartbeat gaps, timestamp jumps, and feed-health evidence.
- [ ] Do not emit fabricated exact missing sequence ranges.
- [ ] Include before/after fingerprints where available.
- [ ] Define investigation status ownership.

**Acceptance evidence:** connection failure and heartbeat scenarios produce evidence records without unsupported sequence claims.

## 4.4 Harden instrument import

- [ ] Replace shell splitting with a real CSV parser or explicitly constrained validated format.
- [ ] Validate exact field count and types.
- [ ] Escape or parameterize SQL values.
- [ ] Reject malformed rows by default; support explicit dry-run reporting.
- [ ] Report accepted/rejected counts and reasons.
- [ ] Write manifest version/checksum.
- [ ] Make partial import impossible unless explicitly requested.

**Acceptance evidence:** quoted commas, quotes, invalid rows, injection-like values, missing columns, and duplicate instruments are handled safely.

---

## Phase 5 — Implement the Signal Flink job

**Goal:** implement one checkpointed Signal job containing compute, business logic, and ranking.

**Addresses:** P0-2, P0-3, P2-3, P2-13, P2-17.

## 5.1 Implement source, watermark, and deduplication

- [ ] Consume `raw_table_1` through the pinned Fluss connector.
- [ ] Filter eligible trade events for candle computation.
- [ ] Use verified UTC event time.
- [ ] Implement configured bounded out-of-orderness.
- [ ] Implement allowed lateness and source idleness.
- [ ] Reject watermark advancement from events without verified event time.
- [ ] Implement bounded fingerprint dedup state.
- [ ] Ensure dedup TTL covers retry/replay horizon plus watermark delay.
- [ ] Define deterministic fingerprint tie ordering.

**Acceptance evidence:** duplicate, identical-legitimate-event, out-of-order, late, idle-source, invalid-time, and checkpoint-restore tests pass.

## 5.2 Implement final 15-second candles

- [ ] Produce one final row per non-empty accepted event-time window.
- [ ] Compute deterministic open/high/low/close/volume/count.
- [ ] Emit only after watermark passes window end plus allowed lateness.
- [ ] Discard and measure later events in MVP.
- [ ] Include candle/config/schema versions.
- [ ] Do not use correction rows in MVP.

**Acceptance evidence:** deterministic replay produces identical candle output and late records are measured without mutating emitted final rows.

## 5.3 Implement forming-bar business logic handoff

- [ ] Keep forming-bar state in the same Signal job.
- [ ] Publish immutable `Signal_Candidates` audit records.
- [ ] Include strategy/rule/configuration version and formation snapshot reference.
- [ ] Enforce candidate identity and mutation rules.
- [ ] Define reservation capacity and release behavior.

**Acceptance evidence:** candidate replay, same-winner reevaluation, changed-parameter supersession, and capacity-limit tests pass.

## 5.4 Implement in-operator ranking

- [ ] Evaluate active candidates in-process.
- [ ] Produce immutable `Ranking_Results` for winners and losers.
- [ ] Use deterministic normalization, weights, tie-breaks, and ranking version.
- [ ] Record reservation snapshot/version.
- [ ] Publish immutable `Trade_Decisions` only for selected winners.
- [ ] Never mutate strategy fields after publication.

**Acceptance evidence:** identical input/state produces identical rankings and decisions; no second ranking job or Fluss round trip is required.

## 5.5 Implement checkpointing and sink guarantees honestly

- [ ] Configure checkpoint interval, state backend, restart strategy, and savepoint path explicitly.
- [ ] Connect to encrypted S3 in production configuration.
- [ ] Test Fluss sink semantics for the pinned connector.
- [ ] Document exactly-once boundary and partial cross-table visibility.
- [ ] Add restart/rescale/replay tests.
- [ ] Remove unsupported exactly-once claims from READMEs.

**Acceptance evidence:** version-specific checkpoint/sink tests pass; claims in documentation match measured guarantees.

## 5.6 Implement job submission and lifecycle

- [ ] Upload the JAR once.
- [ ] Submit exactly Signal and Babysitter jobs with explicit main classes and arguments.
- [ ] Check HTTP status codes and response bodies.
- [ ] Capture job IDs.
- [ ] Verify both jobs reach `RUNNING`.
- [ ] Fail compute readiness if a job is missing, restarting, failed, or not checkpointing.
- [ ] Make submission idempotent across compute-container restarts.

**Acceptance evidence:** repeated compute-container restarts do not create uncontrolled duplicate jobs and readiness reflects actual job state.

---

## Phase 6 — Implement Action Capture and position projection

**Goal:** independently capture broker postbacks, preserve immutable evidence, and project lifecycle/position state safely.

**Addresses:** P0-2, P0-4, P1-14, P1-17, P2-5, P2-15, P2-16.

## 6.1 Implement postback protocol adapter

- [ ] Implement the evidence-approved postback stream or webhook boundary.
- [ ] Preserve original payload bytes and payload hash.
- [ ] Assign `postback_event_id`.
- [ ] Calculate versioned postback fingerprint.
- [ ] Quarantine unknown status/schema/protocol versions.
- [ ] Do not assume `postback_seq`.

**Acceptance evidence:** duplicate, malformed, unknown-version, missing-ID, and out-of-order postbacks are handled deterministically.

## 6.2 Implement correlation

- [ ] Correlate by verified broker order ID mapping.
- [ ] Use echoed client reference only after broker behavior is proven.
- [ ] Allow approved reconciliation evidence.
- [ ] Never infer correlation from symbol, quantity, or time proximity alone.
- [ ] Quarantine ambiguous/missing mappings.
- [ ] Notify Executor safety state for affected uncertainty.

**Acceptance evidence:** every postback is correlated or quarantined with a reason; no silent inferred mapping exists.

## 6.3 Implement independent projections

- [ ] Append every accepted delivery to immutable `Fills_table`.
- [ ] Project `Order_Lifecycle` keyed by `broker_order_id`.
- [ ] Project `Positions` keyed by `position_id` and linked by `trade_context_id`.
- [ ] Implement source-version and status-precedence rules.
- [ ] Protect terminal states from stale updates.
- [ ] Move conflicts to `UNKNOWN` and halt affected order flow.
- [ ] Implement durable pending/completion tracking for independent writes.
- [ ] Add restart/replay reconciliation worker.

**Acceptance evidence:** crash between each independent write is recoverable and idempotent.

## 6.4 Implement Babysitter MVP job

- [ ] Consume versioned `Positions` changelog.
- [ ] Checkpoint observation state.
- [ ] Emit no actions in MVP.
- [ ] Prove no-op behavior cannot submit orders.
- [ ] Define future structured `Position_Actions` contract separately.

**Acceptance evidence:** position changes are observed and checkpointed; no action is emitted in MVP.

---

## Phase 7 — Implement the Executor safety boundary

**Goal:** replace the scaffold with the only component allowed to initiate money-moving calls.

**Addresses:** P0-1, P1-15, P1-16, P1-17, P1-21, P2-14, P2-16, P2-17.

## 7.1 Implement immutable instruction intake

- [ ] Consume immutable `Trade_Decisions`.
- [ ] Validate schema and configuration versions.
- [ ] Validate expiry/freshness.
- [ ] Validate reservation and capacity state.
- [ ] Validate action, quantity, product, and price fields.
- [ ] Detect modified content under an existing `instruction_id`.
- [ ] Quarantine and halt on mutation.
- [ ] Reject superseded/cancelled instructions.

**Acceptance evidence:** malformed, expired, duplicate, mutated, superseded, and valid instructions produce the specified outcomes.

## 7.2 Implement durable gate

- [ ] Implement `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED`.
- [ ] Default fresh/restarted/uncertain state to `HALTED`.
- [ ] Persist epoch and evidence hash.
- [ ] Verify current gate epoch immediately before every broker call.
- [ ] Halt on unknown outcome, duplicate risk, stale state, missing correlation, changelog gap, checkpoint failure, security incident, or failed reconciliation.
- [ ] Ensure safe-halt blocks new calls within five seconds.

**Acceptance evidence:** every defined uncertainty trigger halts new submissions within five seconds and survives restart.

## 7.3 Implement attempt protocol

- [ ] Create durable attempt before the broker call.
- [ ] Persist immutable request hash.
- [ ] Generate deterministic length-limited `client_order_ref` only after broker constraints are verified.
- [ ] Persist gate epoch and audit event.
- [ ] Transition through attempt phases safely.
- [ ] Treat timeout, disconnect, malformed response, and crash window as `UNKNOWN`.
- [ ] Forbid blind retry.
- [ ] Retry only after proof of broker non-acceptance or verified idempotency.

**Acceptance evidence:** crash before, during, after acceptance, and before acknowledgement produces no duplicate broker order.

## 7.4 Implement correlation and reconciliation

- [ ] Persist instruction/attempt/client/broker mapping.
- [ ] Verify mapping before marking safe.
- [ ] Reconcile unknown attempts against broker evidence.
- [ ] Reconcile open orders, fills, and positions before resume.
- [ ] Record all reconciliation evidence in immutable audit.
- [ ] Quarantine ambiguous postbacks.

**Acceptance evidence:** unknown outcomes cannot resume or retry until all affected identities are resolved.

## 7.5 Implement fencing and multi-instance safety

- [ ] Define account/order partition ownership.
- [ ] Implement one active Executor owner per partition.
- [ ] Fence old owners before a new owner can submit.
- [ ] Halt on lost leadership or durable-state connectivity.
- [ ] Test split-brain, delayed lease, restart, and concurrent instance races.

**Acceptance evidence:** two Executor instances cannot submit the same partition concurrently.

## 7.6 Implement two-person resume

- [ ] Authenticate operators.
- [ ] Require distinct first and second approvers.
- [ ] Bind both approvals to the same gate epoch and evidence hash.
- [ ] Reject stale, duplicate, unauthorized, or same-identity approvals.
- [ ] Record approved and denied attempts in `Execution_Audit`.
- [ ] Ensure automatic resume is impossible.

**Acceptance evidence:** only a valid two-person approval can transition a reconciled gate to `ENABLED`.

## 7.7 Implement Executor readiness and observability

- [ ] Expose process health separately from trading readiness.
- [ ] Require durable execution-state connectivity.
- [ ] Require changelog continuity.
- [ ] Require valid schema/version.
- [ ] Require OpenAlgo reachability and contract validation.
- [ ] Require known gate state and no unresolved invariant violation.
- [ ] Emit structured logs, metrics, correlation IDs, and audit IDs.
- [ ] Never log secrets or raw credentials.

**Acceptance evidence:** health checks cannot report trading-ready when the gate is unknown, halted for unresolved reasons, or the execution state is unavailable.

---

## Phase 8 — Local Compose runtime

**Goal:** make local development honest, deterministic, and useful for integration tests without presenting it as HA production.

**Addresses:** P1-6, P1-7, P1-10, P1-11, P1-18, P1-19, P1-20, P1-21, P1-22, P2-2, P3-4.

## 8.1 Make local topology explicit

- [ ] Label Compose `LOCAL DEVELOPMENT ONLY`.
- [ ] State that it is single-host, non-HA, and not live-money capable.
- [ ] Use separate local volumes for coordinator metadata and tablet data unless the pinned Fluss distribution documents a shared volume requirement.
- [ ] Avoid presenting local volumes as production durability.
- [ ] Remove stale OpenAlgo adapter-mount comments.

**Acceptance evidence:** a reviewer cannot reasonably mistake Compose for the four-VM production topology.

## 8.2 Wire effective configuration

- [ ] Mount/apply `fluss.properties` explicitly or remove it and define the real mechanism.
- [ ] Wire local Iceberg/S3-compatible settings deliberately.
- [ ] Configure Flink checkpoint path explicitly for local tests.
- [ ] Log effective non-secret configuration and version matrix.
- [ ] Fail readiness when required configuration is absent.

**Acceptance evidence:** container logs or diagnostics prove which configuration is active.

## 8.3 Add health and readiness checks

- [ ] Fluss coordinator health check.
- [ ] Fluss tablet health check.
- [ ] Flink JobManager health check.
- [ ] Flink TaskManager/job health check.
- [ ] Schema existence check.
- [ ] Signal/Babysitter checkpoint health check.
- [ ] Ingestion subscription/readiness check.
- [ ] Action Capture protocol/readiness check.
- [ ] Executor durable-state and gate readiness check.
- [ ] OpenAlgo contract/reachability check.
- [ ] OpenObserve telemetry check.

**Acceptance evidence:** startup order failures are reported as readiness failures, not hidden as repeated container crashes.

## 8.4 Isolate local network boundaries

- [ ] Use separate logical networks where needed.
- [ ] Restrict execution-plane access.
- [ ] Bind operator-only ports to localhost by default.
- [ ] Document every exposed port and trust boundary.
- [ ] Ensure OpenAlgo is reachable only from Executor in the intended topology.

**Acceptance evidence:** network tests show only required service-to-service paths are reachable.

## 8.5 Keep local secrets explicit and safe

- [ ] Keep `.env.example` values empty or clearly dummy.
- [ ] Never log environment values containing secrets.
- [ ] Add `.env` to ignore rules and verify it is not tracked.
- [ ] Add a local secret scan.
- [ ] Document that Compose environment variables are not production secret handling.

**Acceptance evidence:** secret scan passes and local startup does not print credential values.

---

## Phase 9 — Production Swarm deployment

**Goal:** represent and validate the documented four-VM production topology separately from local Compose.

**Addresses:** P1-8, P1-9, P1-11, P1-19, P1-20, P1-21, P1-22, P2-1.

## 9.1 Create production stack definition

- [ ] Add a separate Swarm stack file.
- [ ] Define three workload/HA VMs and one observability VM.
- [ ] Define placement constraints and anti-co-location.
- [ ] Define replicas, resources, restart, update, rollback, and shutdown policy.
- [ ] Use immutable image digests.
- [ ] Separate internal and controlled ingress networks.
- [ ] Do not expose internal Fluss/tablet/checkpoint ports publicly.

**Acceptance evidence:** stack inspection shows explicit placement, resources, replicas, update policy, and network boundaries.

## 9.2 Configure Fluss HA and storage

- [ ] Configure three-node replication/quorum.
- [ ] Use durable per-node volumes.
- [ ] Test one workload VM loss.
- [ ] Verify recovery point and bounded backlog behavior.
- [ ] Verify tablet anti-co-location.
- [ ] Verify schema and retention behavior after failover.

**Acceptance evidence:** one workload VM can fail at normal baseline without violating the documented durability posture.

## 9.3 Configure S3 checkpoints, lake, and audit storage

- [ ] Configure encrypted S3 Flink checkpoints/savepoints.
- [ ] Configure encrypted Iceberg warehouse.
- [ ] Configure versioning/lifecycle/object-lock policy where required.
- [ ] Use Swarm secrets or workload identity, not normal environment secrets.
- [ ] Test credential rotation and revocation.
- [ ] Test S3 outage and recovery.

**Acceptance evidence:** Flink recovers from S3 after node loss; offload and audit retention tests pass.

## 9.4 Configure service identity and fencing

- [ ] Define least-privilege identities per service.
- [ ] Restrict Executor broker-call permission to Executor only.
- [ ] Restrict DDL/admin permissions to operators/platform services.
- [ ] Define Executor fencing identity and lease behavior.
- [ ] Test unauthorized gate/resume and broker-call attempts.

**Acceptance evidence:** unauthorized services cannot place orders or mutate execution-owned state.

---

## Phase 10 — Observability, operations, and security

**Goal:** make every safety and release gate measurable and diagnosable.

**Addresses:** P1-6, P1-17, P2-16, P2-17, P3-3.

## 10.1 Define common telemetry envelope

- [ ] Define service/instance identity.
- [ ] Define correlation ID and audit ID.
- [ ] Define schema/protocol version.
- [ ] Define event, ingest, processing, and completion timestamps.
- [ ] Define failure classification and retry attempt.
- [ ] Define secret/redaction policy.

## 10.2 Add required metrics

- [ ] Ingestion throughput, decode failures, append latency, append uncertainty, backlog, reconnects, and suspected discontinuities.
- [ ] Dedup counts, fingerprint collisions/limitations, late events, watermark lag, idle sources, and discarded records.
- [ ] Candle publication latency and counts.
- [ ] Candidate/ranking/decision counts and reservation conflicts.
- [ ] Flink checkpoint duration, failures, restart count, backpressure, and state size.
- [ ] Fluss replication/quorum, tablet health, bucket skew, and consumer lag.
- [ ] Action Capture correlation/quarantine/projection retry counts.
- [ ] Executor gate state/epoch, halt latency, attempts, unknown outcomes, duplicate suppression, reconciliation, fencing, approvals, and OpenAlgo latency/status.
- [ ] EOD manifest counts, verification, retry, expiry eligibility, and offload duration.

## 10.3 Add health dimensions

- [ ] Process health.
- [ ] Liveness.
- [ ] Readiness.
- [ ] Data-path readiness.
- [ ] Durability readiness.
- [ ] Trading readiness.
- [ ] Observability delivery health.

## 10.4 Add alerts and runbooks

- [ ] Alert for every P0/P1 safety condition.
- [ ] Alert for safe-halt latency above five seconds.
- [ ] Alert for unverified offload approaching expiry.
- [ ] Alert for unknown broker outcomes.
- [ ] Alert for changelog gaps and projection failures.
- [ ] Alert for checkpoint, replication, or S3 failures.
- [ ] Add runbooks for halt, reconciliation, restart, node loss, credential rotation, and offload failure.

## 10.5 Add security validation

- [ ] Network exposure test.
- [ ] TLS/encrypted transport test.
- [ ] Secret scan and log-redaction test.
- [ ] Least-privilege authorization test.
- [ ] Credential rotation/revocation test.
- [ ] Image/SBOM/vulnerability policy test.
- [ ] Encrypted storage and audit-access test.
- [ ] Unauthorized gate/resume test.

**Acceptance evidence:** dashboards and alerts can prove every live-money release gate and detect each defined safety failure.

---

## Phase 11 — Test and evidence program

**Goal:** produce binary evidence for all functional, safety, performance, and operational claims.

**Addresses:** P0-1 through P2-17 and all release gates.

## 11.1 Unit tests

- [ ] Packet decoding and normalization.
- [ ] Payload hashing and fingerprint canonicalization.
- [ ] Timestamp/validity classification.
- [ ] CSV/manifest validation and escaping.
- [ ] Candle aggregation.
- [ ] Watermark and lateness decisions.
- [ ] Candidate identity and supersession.
- [ ] Ranking normalization, weights, and deterministic tie-breaks.
- [ ] Status precedence and stale-update rejection.
- [ ] Correlation uniqueness.
- [ ] Gate state machine and approval validation.
- [ ] Client-reference encoding and length constraints.
- [ ] Audit envelope/redaction.

## 11.2 Integration tests

- [ ] Clean Fluss catalog/database/table creation.
- [ ] DDL schema parity.
- [ ] LOG/KV connector behavior.
- [ ] `BYTES` payload round trip.
- [ ] Partial update and FULL changelog behavior.
- [ ] Checkpoint and restore.
- [ ] Fluss output partial visibility.
- [ ] Ingestion append retry.
- [ ] Postback independent writes.
- [ ] Projection replay.
- [ ] Executor durable attempt protocol.
- [ ] OpenAlgo sandbox request/response evidence.
- [ ] Schema/protocol unknown-version handling.

## 11.3 Adversarial and failure tests

- [ ] Duplicate tick.
- [ ] Identical legitimate tick within fingerprint TTL.
- [ ] Out-of-order tick.
- [ ] Late tick after final candle.
- [ ] Connection drop/reconnect.
- [ ] Malformed packet.
- [ ] Duplicate postback.
- [ ] Missing broker ID.
- [ ] Ambiguous correlation.
- [ ] Regressive terminal lifecycle update.
- [ ] Crash before broker call.
- [ ] Crash during broker call.
- [ ] Broker accepts then Executor crashes.
- [ ] Timeout/ambiguous REST response.
- [ ] Changelog gap.
- [ ] Checkpoint failure affecting order correctness.
- [ ] Lost durable state.
- [ ] Lost Executor leadership.
- [ ] Unauthorized approval.
- [ ] Same-operator two-person approval attempt.
- [ ] Stale approval epoch/evidence hash.
- [ ] S3 offload failure and retry.
- [ ] One workload VM loss.

## 11.4 Performance and capacity tests

- [ ] 75,000 ticks/s for a full trading-session profile.
- [ ] 112,500 ticks/s for at least 30 minutes.
- [ ] 150,000 ticks/s for at least 60 minutes.
- [ ] Trigger tick → winner commit p99 below 100 ms at the normal baseline.
- [ ] Decode → raw append target measurement.
- [ ] Decision → Executor receipt baseline measurement.
- [ ] Broker REST latency measured separately.
- [ ] Data-path recovery under 30 seconds.
- [ ] Safe-halt under five seconds.
- [ ] EOD offload under 30 minutes.
- [ ] Report p50/p95/p99, workload, duration, UTC clock source/offset, versions, and restart/failure inclusion.

## 11.5 Test coverage gate

- [ ] Add test directories and CI commands for every implementation module.
- [ ] Fail CI when a required contract has no test suite.
- [ ] Fail CI when a production image is built with an absent entry point.
- [ ] Fail CI when stale identifiers or `latest` images reappear.
- [ ] Publish test reports as release evidence.

**Acceptance evidence:** every mandatory requirement maps to a passing test or an explicitly approved evidence-gated blocker.

---

## Phase 12 — Release and live-money gates

**Goal:** prevent premature live deployment.

## 12.1 Pre-production acceptance

- [ ] All P0 issues are verified complete.
- [ ] All P1 issues are verified complete or have explicit risk acceptance.
- [ ] All current DDLs pass pinned-version parse/apply/parity tests.
- [ ] All exact runtime versions and image digests are recorded.
- [ ] Arrow/OpenAlgo protocol evidence is approved.
- [ ] Signal and Babysitter jobs are running/checkpointing.
- [ ] Action Capture and Executor state are durable and observable.
- [ ] Executor starts `HALTED`.
- [ ] Reconciliation tooling is operational.
- [ ] Two-person approval is enforced.
- [ ] No duplicate broker order occurs in crash-window tests.
- [ ] Safe-halt occurs within five seconds.
- [ ] One workload VM loss test passes.
- [ ] Throughput/latency/recovery tests pass.
- [ ] EOD offload/retention tests pass.
- [ ] Security and authorization tests pass.
- [ ] Seven-year audit reconstruction test passes or has documented legal-policy blocker.

## 12.2 Paper/sandbox deployment

- [ ] Deploy with broker calls restricted to paper/sandbox mode.
- [ ] Run the complete vertical slice.
- [ ] Verify all IDs across decision, attempt, broker order, postback, fill, and position.
- [ ] Verify all audit and reconciliation paths.
- [ ] Keep live-money gate disabled.

## 12.3 Live-money approval

- [ ] Platform owner approval.
- [ ] Execution owner approval.
- [ ] Security/compliance approval.
- [ ] Evidence package reviewed.
- [ ] Rollback procedure tested.
- [ ] Gate starts halted after deployment.
- [ ] Reconciliation complete.
- [ ] Two distinct authenticated operators approve the same gate epoch/evidence hash.
- [ ] Enablement is recorded in immutable audit.

## 12.4 Rollback criteria

Immediately return to `HALTED` if any of the following occurs:

- Unknown broker outcome.
- Duplicate-order risk.
- Changelog discontinuity.
- Checkpoint/recovery failure affecting order correctness.
- Invalid schema/protocol/version.
- Lost fencing or durable execution state.
- Uncorrelated/conflicting postback.
- Offload/retention safety failure affecting required audit.
- Unauthorized control attempt.
- Observability failure that prevents safety verification.

---

## Final coverage matrix

Every audit finding must be linked to a verified task before this plan is complete.

| Audit issue | Covered by |
| --- | --- |
| P0-1 | 0.1, 7.1–7.7, 11.3, 12.1 |
| P0-2 | 4.1–4.4, 5.1–5.6, 6.1–6.4, 7.1–7.7 |
| P0-3 | 5.6 |
| P0-4 | 1.1, 4.1, 6.1 |
| P1-1 | 2.2, 4.1, 5.6, 6.1, 7.1 |
| P1-2 | 1.2 |
| P1-3 | 3.1 |
| P1-4 | 3.8, 10.4, 12.1 |
| P1-5 | 3.8 |
| P1-6 | 8.3, 10.3 |
| P1-7 | 8.1 |
| P1-8 | 9.1 |
| P1-9 | 2.1, 9.1 |
| P1-10 | 2.3, 8.2 |
| P1-11 | 2.3, 3.8, 8.2, 9.3 |
| P1-12 | 3.2 |
| P1-13 | 3.3 |
| P1-14 | 3.4, 6.3 |
| P1-15 | 3.5, 7.4 |
| P1-16 | 3.5, 7.3 |
| P1-17 | 3.6, 10.1, 12.1 |
| P1-18 | 8.3 |
| P1-19 | 8.1, 9.2 |
| P1-20 | 8.4, 9.1 |
| P1-21 | 8.5, 9.3, 9.4 |
| P1-22 | 8.4, 9.1 |
| P2-1 | 2.2, 9.1 |
| P2-2 | 1.4, 8.1 |
| P2-3 | 1.3, 5.5 |
| P2-4 | 1.1, 4.3 |
| P2-5 | 1.1, 6.1 |
| P2-6 | 4.4 |
| P2-7 | 4.4 |
| P2-8 | 4.4 |
| P2-9 | 3.7 |
| P2-10 | 11.5 |
| P2-11 | 3.1 |
| P2-12 | 3.1 |
| P2-13 | 1.3, 5.6 |
| P2-14 | 1.2, 7.1 |
| P2-15 | 1.2, 6.3 |
| P2-16 | 10.1–10.5 |
| P2-17 | 5.5, 10.2, 11.4 |
| P3-1 | 1.4 |
| P3-2 | 1.4 |
| P3-3 | 2.2, 11.5 |
| P3-4 | 1.4, 8.1 |

---

## Completion rule

This plan is complete only when:

- Every task is `[X]` with evidence, or explicitly marked `[-]` with a documented blocker and owner.
- Every P0/P1 issue is verified or formally risk-accepted before any live-money enablement.
- The final coverage matrix contains no unverified P0/P1 issue.
- The release gates in Phase 12 pass.
- The Executor remains disabled until the evidence package is approved.
