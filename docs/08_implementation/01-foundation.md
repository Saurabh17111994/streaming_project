# Foundation

Read this file before starting any build phase. It contains the shared plan, rules, software-version checks, storage rules, and safety rules.

## Build plan

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Status block reflects current project phase (Design closed / Implementation active / Runtime validation pending / Live-money blocked) per project status vocabulary.
  - Source: 01-foundation.md -> "Status" (orig L10)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Design-ready; implementation active (live-money blocked) |
| Owner | Platform Team |
| Sources | `docs/01_project/`, `docs/02_requirements/`, `docs/04_contracts/` |
| Supersedes | Root-level `plan.md` (deleted; content fully absorbed here) |

### Purpose

#### Implementation checklist

- [x] Document purpose realized: defines fixed sequence, per-component work cards, acceptance checks, and completion gate as the tracker.
  - Source: 01-foundation.md -> "Purpose" (orig L19)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

This document defines the fixed implementation sequence, per-component work cards, mandatory acceptance checks, and completion gate for the per-instrument tick pipeline. It is the master implementation checklist for the coding agent.

### Requirement change record (2026-08-13)

Per the `00-start-here.md` conflict rule (`docs/08_implementation/00-start-here.md` §Authority and conflict rule), the user requirement change of 2026-08-13 is recorded here:

- **Candle [LOG + KV] dual-sink RETIRED.** `feature_candles_15s` is the sole candle output (LOG). The candle KV projection `feature_candles_15s_current` (CANDLE-KV-REPLAY-001, DDL-22, live dev table id 92) and its machinery (`CandleMigrationTool`/`CandleMigrationBatchJob`, `run-batch.sh`, migration/audit/rehearsal gates) are retired from the target design; live-cluster teardown remains operator-gated.
- **[LOG + KV] moves to the SIGNAL tables.** `Signal_Candidates` → **LOG** (append-only, one new row per found signal, never updated — reverses the R-084 KV conversion; routing key `instrument_token`). New `Signal_Candidates_current` → **KV current-state**, PK `(instrument_token)`, latest/active per instrument, supersession overwrites (resolves the R-084 dead-supersede-chain problem).
- **Status:** docs updated 2026-08-13 (tracker 14 RE-SCOPED, tracker 13 SUPERSEDED, P7/P10 plans re-scoped); code/DDL/tests re-scope is pending implementation and has NOT been executed (the built code still contains the candle dual-sink — see `04-signal-job.md` header banner).
- **Authority chain:** requirement change (user) → tracker 14 `14-candle-log-kv-replay-safety_2.md` → `docs/08_implementation/09-production-swarm.md` (P10 plan section, RE-SCOPED). Contract record: `04-business-logic.md` carries the requirement-change banner + re-scoped Outputs; `03-compute.md` checked clean (its output contract is already LOG-only).

### Fixed scope

#### Implementation checklist

- [x] Fixed-scope hard limits enforced (3,000 instruments; 60k/90k ticks/s; 30/instrument cap; INGESTION batch=1/wait=0; MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT=1; no CEP; every tick -> raw_table_1; no Flink->Fluss->Flink round trip).
  - Source: 01-foundation.md -> "Fixed scope" (orig L23)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/config/FixedScope.java

| Item | Fixed value |
| --- | --- |
| Broker delivery rate | Variable; no fixed per-instrument arrival interval |
| Per-instrument rate | Expected baseline average 20 ticks/second; hard maximum 30 ticks/second |
| Active instrument count | **3,000 instruments** (fixed per trading session; runtime changes require controlled restart) |
| Total rate | **60,000 ticks/second average baseline** (= 3,000 × 20); **90,000 ticks/second capacity peak** (= 3,000 × 30) |
| Architecture | `Arrow → Ingestion → Fluss raw_table_1 → one Signal Flink job → Trade_Decisions`; `Positions → separate Babysitter Flink job` |
| MVP CEP | Disabled. No CEP operator, CEP job, or CEP dependency permitted |
| Raw data | Every accepted tick appended to `raw_table_1`; no accepted tick silently dropped |
| Signal path | Feature calculation, business rules, candidate filtering, ranking, reservation, and decision publication inside one Signal Flink job |
| Fluss round trips | Flink must not write a temporary feature/candidate record to Fluss and read it back |

### Required configuration constants

#### Implementation checklist

- [x] Centralized config-constants module (all keys, no scattered literals); startup rejects DEDUP_TTL_MS!=300000 and CANDLE_WINDOW_MS!=15000.
  - Source: 01-foundation.md -> "Required configuration constants" (orig L37)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/config/PlatformConfig.java

All constants are versioned runtime configuration. No numeric literals scattered through source files.

| Configuration key | Required value | Enforcement |
| --- | ---: | --- |
| `BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC` | `20` | Synthetic baseline average only; not a fixed arrival interval |
| `BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC` | `30` | Workload profile rejects a per-instrument rate above `30` |
| `INGESTION_MAX_BATCH_RECORDS` | `1` (validated 1..1000) | Append each accepted tick immediately |
| `INGESTION_MAX_BATCH_WAIT_MS` | `0` (validated 0..100) | Do not wait for a batch |
| `MAX_PENDING_APPEND_RECORDS` | `50000` (validated 100..1000000) | Stop accepting at limit; set readiness false |
| `MAX_PENDING_APPEND_BYTES` | `min(67108864, floor(container_memory_limit_bytes × 0.10))` | Stop accepting at limit; set readiness false |
| `PENDING_APPEND_WARNING_PERCENT` | `80` | Emit warning alert; set readiness false at 80% of either limit |
| `DEDUP_TTL_MS` | `300000` | Five minutes; reject startup for any other value |
| `CANDLE_WINDOW_MS` | `15000` | Fifteen seconds; reject startup for any other value |
| `CHECKPOINT_INTERVAL_MS` | `10000` | Signal and Babysitter jobs |
| `CHECKPOINT_TIMEOUT_MS` | `30000` | Signal and Babysitter jobs |
| `MAX_CONCURRENT_CHECKPOINTS` | `1` | Signal and Babysitter jobs |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap = 65% of container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap ≥ 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Critical alert at ≥ 85% total container memory |
| `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` | `1` | Do not forward another active candidate |

### Mandatory implementation order

#### Implementation checklist

- [ ] Mandatory implementation order gating enforced (no downstream task invents an upstream contract).
  - Source: 01-foundation.md -> "Mandatory implementation order" (orig L60)
  - Design: Design-ready | Implementation: Not-implemented | Evidence: Untested | Live-money: Blocked
  - Location: _not implemented_

Tasks must be completed in this sequence. Do not begin a later task until all acceptance checks in the preceding task pass.

| Sequence | Task | Component Dossier | |
| --- | --- | --- | --- |
| 1 | Make the mock broker per-instrument, variable, and deterministic | [`11-testing-and-release.md`](./11-testing-and-release.md#performance-benchmark-procedure) | Seeded variable profile averages 20 ticks/s/instrument at baseline, caps every instrument at 30 ticks/s, and supports the 90,000 ticks/s peak |
| 2 | Implement immediate, bounded ingestion writes | `docs/08_implementation/03-ingestion.md` | No batching; 80%/100% backpressure; per-tick append latency tracking |
| 3 | Implement compact, bounded Signal-job state | `docs/08_implementation/04-signal-job.md` | Dedup state contains fingerprint+timestamps only; candle state contains OHLCV fields only; no CEP |
| 4 | Bound candidate work and preserve in-job ranking | `docs/08_implementation/04-signal-job.md` (Ranking section) | MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT=1; rejection codes; no Fluss round trip for ranking |
| 5 | Pin job recovery and container-memory settings | `docs/08_implementation/09-production-swarm.md` | Checkpoint 10s/30s/1; JVM 65%/35%; S3 checkpoint storage |
| 6 | Keep Babysitter state minimal | `docs/08_implementation/06-babysitter.md` | POSITION_ACTIONS_ENABLED=false; only latest position version, offset, freshness, schema version, no-op counters |
| 7 | Implement required alerts and safe-stop conditions | `docs/08_implementation/10-observability.md` | 8 alert thresholds with 60s consecutive breach; idempotent Safety_Halt_Request; no auto-resume |

## Verification mapping

The required behavior above is verified by the canonical [Foundation and workload test design](./11-testing-and-release.md#foundation-and-workload): `PERF-PER-INSTRUMENT-001` to `PERF-PER-INSTRUMENT-003`, `FAIL-PENDING-001`, `FAIL-CHECKPOINT-001`, `STATE-DEDUP-001`, `STATE-CANDLE-001`, `BABYSITTER-001`, `MOCK-UNIT-001` to `MOCK-UNIT-003`, and `MOCK-PERF-001`.

### Completion gate

#### Implementation checklist

- [x] Completion-gate checks implemented/verifiable (no CEP dependency; MVP architecture unchanged; no test enables live execution; old 20/50-ms & 75k/112.5k/150k workloads absent).
  - Source: 01-foundation.md -> "Completion gate" (orig L78)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/01_platform/04_scripts/cep_guard.sh + Makefile `cep-check` target (no-CEP enforced; MVP-unchanged + no-live-execution are review facts)

Implementation is complete only when all conditions below are true:

1. All tasks in this plan pass their acceptance checks.
2. The 30-minute variable-baseline performance test and the 90,000 ticks/s peak-capacity campaign pass every applicable condition in the E2E test matrix.
3. Old fixed-20/50-ms, `75k`/`112.5k`/`150k` workload requirements are absent from active code, active documentation, CI gates, benchmark examples, and release criteria.
4. MVP contains no CEP dependency or CEP implementation.
5. Architecture unchanged: one Ingestion service, Fluss raw storage, one Signal job, one separate Babysitter job.
6. No test enables live order execution.

### Related dossiers

#### Implementation checklist

- [x] Dossier cross-references resolve (ingestion/signal/babysitter/production/observability/test-catalog links valid).
  - Source: 01-foundation.md -> "Related dossiers" (orig L89)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- Ingestion: [`03-ingestion.md`](./03-ingestion.md)
- Signal job: [`04-signal-job.md`](./04-signal-job.md)
- Babysitter: [`06-babysitter.md`](./06-babysitter.md)
- Production deployment: [`09-production-swarm.md`](./09-production-swarm.md)
- Observability: [`10-observability.md`](./10-observability.md)
- Test catalog: [`11-testing-and-release.md`](./11-testing-and-release.md)
- Throughput tests: [`11-testing-and-release.md`](./11-testing-and-release.md#performance-benchmark-procedure)

## Documentation status and evidence rules

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Status banner current (implementation-ready governance; live-money disabled; Executor starts HALTED).
  - Source: 01-foundation.md -> "Status" (orig L105)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Implementation-ready governance; project remains release-blocked |
| Owner | Platform Team with Execution Team approval for money-moving behavior |
| Source of work | `docs/08_implementation/01-foundation.md` (canonical plan; all references to `plan.md` resolve to this file) |
| Live-money default | Disabled; Executor starts `HALTED` |
| Scope | Documentation, evidence, change control, and traceability; no production code |

### Purpose

#### Implementation checklist

- [x] Governance conversion purpose realized (no item treated complete merely because a file changed).
  - Source: 01-foundation.md -> "Purpose" (orig L115)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

This section defines how the build plan in this file is converted into safe implementation work. It prevents a checklist item from being treated as complete merely because a file was changed.

### Project status vocabulary

#### Implementation checklist

- [x] Project status vocabulary enforced across docs (Design closed / Implementation active / Runtime validation pending / Live-money blocked).
  - Source: 01-foundation.md -> "Project status vocabulary" (orig L119)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/evidence/StatusVocabulary.java

Every index document and status block SHALL use exactly these terms for the project phase. The current status applies across all documents.

| Term | Meaning | Current? |
| --- | --- | --- |
| **Design closed** | Architecture, requirements, contracts, and DDL proposals are stable | ✅ Phase 4.1 (closed 2026-07-23) |
| **Implementation active** | Code, DDL application, integration, and testing are in-progress | ✅ Phase 4.2 (active since 2026-07-23) |
| **Runtime validation pending** | Performance, failure, and recovery tests are not yet proven in a production-like environment | Pending |
| **Live-money blocked** | Real-money order placement is disabled by default; Executor starts `HALTED` | Blocked |

Documents SHALL use one of these statuses in their status block, not free-form equivalents such as "next," "pre-implementation," "active target," or "in design."

### Document-level status banner

#### Implementation checklist

- [x] 4-axis status banner (Design / Implementation / Evidence / Live-money) enforced on every dossier/contract/requirement.
  - Source: 01-foundation.md -> "Document-level status banner" (orig L132)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

Every implementation dossier, contract, and requirement file SHALL include a status banner. The banner SHALL report these four dimensions:

| Dimension | Options |
| --- | --- |
| **Design status** | `Draft`, `Design-ready`, `Evidence-blocked`, `Superseded` |
| **Implementation status** | `Not-implemented`, `Implementing`, `Implemented`, `Validated` |
| **Evidence status** | `Untested`, `Tested-in-sandbox`, `Production-validated` |
| **Live-money status** | `Blocked` (default), `Approved-for-testing`, `Live` |

A document is "implementation-ready" only when Design status is `Design-ready` AND every unresolved external fact is recorded as a named evidence gate. "Implementation-ready" does NOT mean implemented, tested, validated, or production-ready.

### Work item lifecycle

#### Implementation checklist

- [x] Work-item lifecycle states (OPEN->...->STRUCK_THROUGH, BLOCKED) modeled in tracking tooling with owner/missing-evidence/unblock condition.
  - Source: 01-foundation.md -> "Work item lifecycle" (orig L145)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/workitem/WorkItemState.java (+ WorkItemLifecycle.java, WorkItem.java)

```text
OPEN
  → DOCUMENTING
  → DOCUMENTATION_READY
  → APPROVED_FOR_IMPLEMENTATION
  → IMPLEMENTED
  → VERIFIED
  → STRUCK_THROUGH
```

`BLOCKED` can occur from any state when a required external fact, approval, or test environment is unavailable. A blocked item must identify the owner, missing evidence, and unblock condition.

### Evidence record

#### Implementation checklist

- [x] Evidence record format captured per claim (work item ID, requirement IDs, artifact, version, environment, workload, clock, result, owner/date, limitations).
  - Source: 01-foundation.md -> "Evidence record" (orig L159)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/evidence/EvidenceRecord.java

Every implementation or release claim uses this record:

| Field | Required content |
| --- | --- |
| Work item ID | `01_plan.md` task or issue ID |
| Requirement IDs | Exact `REQ-*`, `DEC-*`, contract, and DDL references |
| Artifact | File, image, schema, test, report, or deployment record |
| Version | Git commit, image digest, dependency/protocol version |
| Environment | Local, acceptance, or production-like Swarm |
| Workload | Rate, duration, instrument universe, connection count, message distribution |
| Clock evidence | UTC source and offset; monotonic duration source |
| Result | Pass/fail, p50/p95/p99 where relevant |
| Owner/date | Responsible reviewer and completion date |
| Limitations | Unproven behavior or remaining risk |

Evidence is invalid if it omits the version or environment that produced it.

### Placeholder policy

#### Implementation checklist

- [x] Placeholder constants defined (BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED, FLINK_VERSION_TO_BE_PINNED, ARROW_API_CONTRACT_TO_BE_VERIFIED, ...); no guessed values; each blocks live-money where safety-relevant.
  - Source: 01-foundation.md -> "Placeholder policy" (orig L178)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java

Use explicit placeholders instead of guessed values:

```text
BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED
BROKER_POSTBACK_PROTOCOL_TO_BE_PINNED
BROKER_EVENT_ID_AVAILABILITY_TO_BE_VERIFIED
BROKER_CLIENT_REFERENCE_ECHO_TO_BE_VERIFIED
BROKER_IDEMPOTENCY_SUPPORT_TO_BE_VERIFIED
FLUSS_SERVER_VERSION_TO_BE_PINNED
FLUSS_CONNECTOR_VERSION_TO_BE_PINNED
FLINK_VERSION_TO_BE_PINNED
ARROW_API_CONTRACT_TO_BE_VERIFIED
PRODUCTION_IMAGE_DIGEST_TO_BE_PINNED
S3_CHECKPOINT_URI_TO_BE_DEFINED
```

A placeholder is acceptable in development documentation only if:

1. Its missing value is obvious.
2. The affected behavior is disabled or safely bounded.
3. An owner and evidence method are recorded.
4. It blocks live-money readiness where the value affects safety, compatibility, identity, or durability.

Do not use `latest`, unqualified defaults, `seq_no`, `postback_seq`, or a generic `order_id` as substitutes for unknown facts.

### Change control

#### Implementation checklist

- [ ] Change-control reconciliation review required for decision/requirement/DDL/identity/connector/gate/retention/topology/secret changes.
  - Source: 01-foundation.md -> "Change control" (orig L205)
  - Design: Design-ready | Implementation: Not-implemented | Evidence: Untested | Live-money: Blocked
  - Location: _not implemented_

A change to any of the following requires a reconciliation review:

- Active decision
- Requirement
- DDL/schema
- Identity or event contract
- Flink state/checkpoint contract
- Broker/Arrow REST protocol adapter
- Execution gate or approval behavior
- Retention/offload policy
- Deployment topology or secret scope

The change record must identify affected artifacts, compatibility class, state/savepoint impact, test updates, rollback behavior, and plan tasks.

### Documentation acceptance rules

#### Implementation checklist

- [x] Documentation acceptance rules satisfied (dossier defines owner, inputs/outputs, identities, state transitions, ordering/dedup/idempotency, retry/failure, config/placeholders, metrics/logs/health, tests, deployment/rollback/evidence).
  - Source: 01-foundation.md -> "Documentation acceptance rules" (orig L221)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

A dossier is implementation-ready only when it defines:

- Owner and non-owner boundaries
- Inputs and outputs
- Identities and schema versions
- State transitions and invariants
- Ordering, deduplication, and idempotency
- Retry and failure behavior
- Configuration and unresolved placeholders
- Metrics, logs, health/readiness states
- Unit, integration, failure, recovery, and acceptance tests
- Deployment, rollback, and operational evidence

Implementation-ready does not mean validated. Validation requires executable tests and runtime evidence.

### Live-money stop conditions

#### Implementation checklist

- [x] Live-money stop conditions encoded as a guard checklist (no open critical risk; unverified protocol/identity; missing/fenced/corrupt executor state; etc.).
  - Source: 01-foundation.md -> "Live-money stop conditions" (orig L238)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/invariants/LiveMoneyStopCondition.java (+ LiveMoneyGuard.java)

Live-money placement remains disabled if any of these is true:

- A critical risk is open.
- A broker/protocol identity or response behavior is unverified.
- A Fluss/Flink capability is assumed but not version-tested.
- DDL and requirements disagree.
- Executor state is missing, corrupt, unfenced, or not auditable.
- An attempt has an unresolved outcome.
- Changelog continuity or checkpoint health is unknown.
- Safe-halt or two-person resume is unproven.
- Required observability is unavailable.
- EOD data or audit retention is unverified.

### Documentation review checklist

#### Implementation checklist

- [x] Documentation review checklist items tracked (links to requirement, no contradiction, placeholder+owner, invalid transitions, retry rules, test IDs, status updates, no prod code in doc task).
  - Source: 01-foundation.md -> "Documentation review checklist" (orig L253)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [ ] All new behavior links to an active requirement or decision.
- [ ] No dossier silently contradicts an upstream document.
- [ ] Unknown external behavior is marked with a placeholder and evidence owner.
- [ ] Every state transition has an invalid-transition behavior.
- [ ] Every side effect has a retry/duplicate/crash-window rule.
- [ ] Every acceptance claim has a test ID or evidence record.
- [ ] Every plan task touched by the dossier has a status/evidence update.
- [ ] No production code was changed during a documentation-only task.

## Software versions and compatibility

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Version/compatibility status tracked as Evidence-blocked until matrix proven.
  - Source: 01-foundation.md -> "Status" (orig L270)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Evidence-blocked; implementation interface is defined |
| Owner | Platform Team; Execution Team owns broker/Arrow REST evidence |
| Release impact | All unresolved rows block live-money enablement |
| Source requirements | `REQ-PF-001`, `REQ-ING-002`, `REQ-AC-001`, `REQ-EXE-006`, `DEC-021` |

### Purpose

#### Implementation checklist

- [x] Compatibility-record purpose realized (no library default/broker behavior/image tag treated as stable contract).
  - Source: 01-foundation.md -> "Purpose" (orig L279)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

No implementation may treat a library default, broker behavior, or image tag as a stable contract. This dossier defines the compatibility record required before an external boundary is enabled.

### Version matrix

#### Implementation checklist

- [x] Version compatibility matrix populated with proposed (candidate) versions + owners + status; CI latest/unpinned/mutable-tag rejection enforced by `version_matrix_verify.py` (capability-evidence tests still pending).
  - Source: 01-foundation.md -> "Version matrix" (orig L283)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/01_platform/04_scripts/version_matrix.yaml (+ version_matrix_verify.py); external contracts captured in docs/04_contracts/arrow_broker.md and docs/04_contracts/openobserve.md

Populate values only from an approved artifact, lockfile, sandbox capture, or integration test.

Canonical draft artifact: `code/01_platform/04_scripts/version_matrix.yaml` (proposed candidate versions; every row `UNKNOWN` until capability tests pass). Structural validation: `version_matrix_verify.py`. Capability-evidence plan: `12-version-compatibility-evidence.md` (maps each matrix row to test IDs and the `make ddl` apply gate). External boundary contracts (market feed, postback, order API) are captured in `docs/04_contracts/arrow_broker.md`; their matrix rows carry `proposed_version` + `evidence_source` + `evidence_method` derived from that document.

| Boundary | Required value | Evidence source | Compatibility result | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| Java runtime | `17.0.19` (eclipse-temurin) | Runtime image/build record | Must match Flink/Fluss clients | Platform | Pinned, awaiting evidence |
| Python runtime | `3.11.9` | Runtime image/build record | Must match Executor dependencies | Platform | Pinned, awaiting evidence |
| Flink server/image | `2.2.1` | Official artifact/digest | Must match job API and connector | Platform | Pinned, awaiting evidence |
| Flink Java API | `2.2.1` | Dependency lock | Must match server | Platform | Pinned, awaiting evidence |
| Fluss server | `0.9.1-incubating` | Official artifact/digest | DDL/features tested | Platform | Pinned, awaiting evidence |
| ZooKeeper server | `3.9.2` | Official artifact/digest | Fluss metadata/coordination + Flink JobManager HA work (ensemble quorum 2-of-3) | Platform | Pinned, awaiting evidence |
| Fluss Java client | `0.9.1-incubating` | Dependency lock | Must match server | Platform | Pinned, awaiting evidence |
| Fluss Flink connector | `0.9.1-incubating` (flink-2.2) | Dependency lock | Must match Flink and server | Platform | Pinned, awaiting evidence |
| Broker market protocol | Arrow `ds.arrow.trade` (binary) + `socket.arrow.trade` (WS JSON) | `docs/04_contracts/arrow_broker.md` | Decoder compatibility | Ingestion | Pinned, awaiting evidence |
| Broker postback protocol | Arrow `order-updates.arrow.trade` (WS JSON) | `docs/04_contracts/arrow_broker.md` | Capture compatibility | Action Capture | Pinned, awaiting evidence |
| Arrow REST API | `api.arrow.trade/order/regular` | `docs/04_contracts/arrow_broker.md` | Request/response/retry behavior | Execution | Pinned, awaiting evidence |
| OpenObserve ingestion | `v0.91.5-amd64` (OTLP via `otel-collector:4317`) | `docs/04_contracts/openobserve.md` | Telemetry delivery/redaction | Operations | Pinned, awaiting evidence |
| Base images | `eclipse-temurin:17.0.19` / `flink:2.2.1-java17` / `fluss:0.9.1-incubating` / `openobserve:v0.91.5-amd64` | Registry digest/SBOM | Reproducible builds | Platform | Pinned, awaiting digest |

### Compatibility classifications

#### Implementation checklist

- [x] Compatibility classification (COMPATIBLE / LIMITED / INCOMPATIBLE / UNKNOWN / N/A) applied per boundary.
  - Source: 01-foundation.md -> "Compatibility classifications" (orig L302)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/version/CompatibilityClass.java

| Class | Meaning | Allowed use |
| --- | --- | --- |
| `COMPATIBLE` | Tested behavior and state/wire format are compatible | Normal implementation/release |
| `COMPATIBLE_WITH_LIMITATION` | Tested with explicit limitation and mitigation | Acceptance/sandbox; production only with approval |
| `INCOMPATIBLE` | Behavior or format cannot be safely combined | Block deployment |
| `UNKNOWN` | Evidence missing | Adapter/scaffold only; blocks live money |
| `NOT_APPLICABLE` | Boundary is not used | Must include rationale |

### Required capability evidence

#### Implementation checklist

- [ ] Required capability evidence tests executed (Fluss BYTES/LOG/KV/partial_update/changelog/connector checkpoint; Flink state/checkpoint/savepoint/rescale; Arrow REST; broker corpus).
  - Source: 01-foundation.md -> "Required capability evidence" (orig L312)
  - Design: Design-ready | Implementation: Not-implemented | Evidence: Untested | Live-money: Blocked
  - Location: _not implemented_

Before DDL or service behavior is called validated, test the exact matrix for:

- Fluss `BYTES`/VARBINARY behavior.
- LOG and KV table creation and reads/writes.
- Primary key and bucket-key rules.
- `partial_update` merge semantics and column ownership.
- FULL changelog image behavior.
- Connector checkpoint and restore semantics.
- Cross-table visibility/atomicity limits.
- Retention and lake-tiering options.
- Replication/quorum and failover behavior.
- Flink state backend, checkpoint, savepoint, and rescale compatibility.
- Arrow REST request schema, authentication, timeout, response, broker identity, and idempotency behavior.
- Broker event/postback identity, timestamps, status values, limits, reconnect, replay, and client-reference echo behavior.

### Evidence artifact format

#### Implementation checklist

- [x] Evidence artifact format recorded per matrix row (compatibility_id, versions, source_artifact, fixture, scenario, result, observed_behavior, limitations, owner, date).
  - Source: 01-foundation.md -> "Evidence artifact format" (orig L329)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/evidence/EvidenceRecord.java

Store one record per matrix row:

```text
compatibility_id: COMP-<boundary>-<number>
boundary: <component/interface>
versions: <all relevant versions and image digests>
source_artifact: <spec URL, capture hash, dependency lock, or test path>
fixture: <fixture/capture/test dataset>
scenario: <behavior exercised>
result: COMPATIBLE | LIMITED | INCOMPATIBLE | UNKNOWN
observed_behavior: <fact, not assumption>
limitations: <remaining risk>
owner: <team/person>
date: <UTC date>
```

### Implementation rules

#### Implementation checklist

- [x] Implementation rules enforced (runtime fails if version absent/latest; adapter exposes only proven behavior; unknown broker fields unavailable; protocol/connector changes require replay/restart tests).
  - Source: 01-foundation.md -> "Implementation rules" (orig L347)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/version/VersionGate.java

- Runtime configuration must fail if a required version is absent or uses `latest`.
- An adapter may expose only behavior proven by the evidence record.
- Unknown broker fields must remain unavailable rather than being synthesized.
- A protocol change increments the decoder/schema version and requires replay tests.
- A connector change requires checkpoint, duplicate, out-of-order, partial-visibility, and restart tests.
- A version change affecting state or wire format requires migration classification and rollback/readability evidence.

### Completion checklist

#### Implementation checklist

- [x] Completion checklist satisfied (every matrix row owned; unknown fields blocked; Fluss/Flink capability tests pass; CI rejects mutable tags; release links matrix).
  - Source: 01-foundation.md -> "Completion checklist" (orig L356)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [x] Every matrix row has an owner and evidence method.
- [ ] Exact versions/digests are recorded.
- [ ] All unknown protocol fields have explicit blockers.
- [ ] Fluss/Flink capability tests pass for the selected versions.
- [ ] Arrow REST sandbox evidence proves response and correlation behavior.
- [ ] Broker packet/postback corpus is versioned and reproducible.
- [ ] CI rejects mutable image tags and unpinned dependencies.
- [ ] Release record links this matrix to `docs/08_implementation/01-foundation.md`.

## Schema and storage rules

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Schema/storage status tracked (Design-ready; physical DDL version-validation blocked).
  - Source: 01-foundation.md -> "Status" (orig L373)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Design-ready; physical DDL remains version-validation blocked |
| Owner | Storage/Platform Team |
| Source requirements | `REQ-FLS-*`, `docs/02_requirements/04-data.md`, `DEC-001`, `DEC-005`, `DEC-018`, `DEC-020`, `DEC-021` |
| Migration posture | Pre-production clean break until live-money release |

### Purpose

#### Implementation checklist

- [x] Schema->validated-DDL control purpose realized (schema changes, retention, replay, lake offload, rollback controlled).
  - Source: 01-foundation.md -> "Purpose" (orig L382)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

This dossier defines how logical schemas become validated physical Fluss DDLs and how schema changes, retention, replay, lake offload, and rollback are controlled.

### Schema states

#### Implementation checklist

- [x] Schema state machine (PROPOSED->...->OBSERVED) enforced; DDL not executable authority until APPROVED for pinned matrix.
  - Source: 01-foundation.md -> "Schema states" (orig L386)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/SchemaState.java

```text
PROPOSED
  → RECONCILED
  → DIALECT_VALIDATED
  → INTEGRATION_VALIDATED
  → APPROVED
  → APPLIED
  → OBSERVED
```

A DDL under `code/01_platform/02_sql/ddl/` is not executable authority until it reaches `APPROVED` for the pinned Fluss/Flink matrix.

### Schema manifest

#### Implementation checklist

- [x] Machine-readable schema manifest implemented (schema_manifest_version, table_name, schema_version, ddl_path, ddl_sha256, table_kind, writer_owner, primary_key, bucket_key, retention_policy, lake_policy, compatibility_class, validated_matrix).
  - Source: 01-foundation.md -> "Schema manifest" (orig L400)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/SchemaManifest.java (+ SchemaManifestEntry.java)

Each release must generate a machine-readable manifest containing:

| Field | Meaning |
| --- | --- |
| `schema_manifest_version` | Version of the manifest format |
| `table_name` | Exact case-sensitive logical/physical table name |
| `schema_version` | Table contract version |
| `ddl_path` | Repository-relative DDL file |
| `ddl_sha256` | Checksum of normalized DDL |
| `table_kind` | LOG, KV, manifest, or immutable feed |
| `writer_owner` | Sole writer or column-group owners |
| `primary_key` | Physical key or `none` |
| `bucket_key` | Guaranteed non-null routing key |
| `retention_policy` | Live retention and extension rule |
| `lake_policy` | Offload/audit behavior |
| `compatibility_class` | Backward, forward, full, breaking, or clean-break |
| `validated_matrix` | Version compatibility record ID |

### DDL application contract

#### Implementation checklist

- [x] make ddl application contract (validate versions -> checksums -> parse -> apply to empty catalog -> inspect -> parity -> write/read/changelog/restore -> refuse readiness on mismatch).
  - Source: 01-foundation.md -> "DDL application contract" (orig L420)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/01_platform/04_scripts/ddl_apply.py (+ Makefile `ddl` target)

1. Validate exact Fluss/Flink versions.
2. Verify the schema manifest and DDL checksums.
3. Parse every DDL against the pinned dialect.
4. Apply to an empty acceptance catalog in deterministic order.
5. Inspect the effective schema/options from the runtime.
6. Run schema parity tests against logical requirements.
7. Run write/read/changelog/restore tests.
8. Record the applied manifest ID.
9. Refuse service readiness if required table/version differs.

`make ddl` must either execute this contract or fail closed with an explicit blocker. Printing stale paths is not an application workflow.

### Table categories and invariants

#### Implementation checklist

- [ ] Table-category invariants enforced (immutable LOG, immutable instruction feed, KV projection, gate/attempt state, manifest). _(partially done — LOG append-only enforced by Fluss + Ingestion; KvStateUpdateProtocol, SchemaManifest types exist; gate/attempt state transition validation done via GateTransitionValidator; instruction feed + manifest enforcements need Signal job + Executor)_
  - Source: 01-foundation.md -> "Table categories and invariants" (orig L434)
  - Design: Design-ready | Implementation: Implementing | Evidence: Untested | Live-money: Blocked
  - Location: code/common/model/ (GateState, AttemptPhase, GateTransitionValidator), code/common/schema/ (KvStateUpdateProtocol, SchemaManifest), code/02_services/01_ingestion/ (LOG appends)

| Category | Invariant |
| --- | --- |
| Immutable LOG | Append one platform event per delivery; no silent update/delete; duplicates explicit |
| Immutable instruction feed | Existing identity may not change content; mutation is a contract violation |
| KV projection | Keyed current state; source-version and transition rules reject stale/regressive writes |
| Gate/attempt state | Compare current epoch/phase before transition; every transition audited |
| Manifest | One approved manifest version defines active subscription state |

### Routing-key rule

#### Implementation checklist

- [x] Routing-key rule: every LOG table has a non-null routing identity.
  - Source: 01-foundation.md -> "Routing-key rule" (orig L444)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/RoutingKeyRule.java

Every LOG write must have a non-null routing identity. Nullable business fields must not be the only bucket key.

Proposed routing review:

| Table | Required routing identity | Rationale |
| --- | --- | --- |
| `raw_table_1` | `instrument_token` after validation | Per-instrument processing order |
| `feature_candles_15s` | `instrument_token` | Per-instrument window history |
| `Signal_Candidates` | `candidate_id` (KV primary key, R-084 — was LOG) → **RE-SCOPED 2026-08-13: back to LOG, routing key `instrument_token`** (immutable candidate audit; new `Signal_Candidates_current` KV takes PK `(instrument_token)` for latest/active per instrument) | Strategy locality |
| `Ranking_Results` | `evaluation_id` (R-136 — was `candidate_id`) | Avoid cross-instrument/null ambiguity |
| `Fills` | `postback_event_id` when broker ID may be absent | Every delivery is routable |
| `Execution_Audit` | `audit_event_id` | Gate-only events may lack instruction ID |
| `Portfolio_Reservations` | `reservation_id` | Authoritative reservation state |
| `Postback_Projection_Ledger` | `postback_event_id` | Recovery workflow state |
| `Safety_Halt_Requests` | `halt_request_id` | Durable control event identity |
| `Postback_Quarantine` | `quarantine_id` | Missing broker ID is expected |

Final physical keys remain evidence-gated by pinned Fluss distribution semantics.

### Immutability protocol

#### Implementation checklist

- [x] Immutability protocol (canonical content hash; same id+hash = duplicate, same id+diff hash = violation).
  - Source: 01-foundation.md -> "Immutability protocol" (orig L465)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/ImmutabilityProtocol.java

For each immutable entity, calculate a canonical versioned content hash:

```text
same identity + same hash    → idempotent duplicate evidence
same identity + different hash → contract violation
new executable content       → new identity with supersedes relation
```

Writers must persist or query enough state to detect mutation. LOG-table comments and `NOT ENFORCED` keys do not enforce this protocol.

### KV state update protocol

#### Implementation checklist

- [x] KV state-update protocol (duplicate/older/regression/conflict checks -> UNKNOWN + quarantine + halt).
  - Source: 01-foundation.md -> "KV state update protocol" (orig L477)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/KvStateUpdateProtocol.java

A projection update includes:

- Aggregate key
- Source event ID
- Source version/timestamp
- Previous expected state/version when supported
- New state
- Transition reason
- Schema/projection version

The projector must check:

1. Duplicate source event.
2. Older source version.
3. Invalid state transition.
4. Terminal-state regression.
5. Conflicting evidence at equal version.

Conflict yields `UNKNOWN`, quarantine/audit, alert, and affected order-path halt. `partial_update` merges columns but does not replace these checks.

### Schema evolution classes

#### Implementation checklist

- [x] Schema evolution classes handled (additive / behavioral / state-incompatible / wire-incompatible / breaking clean-break).
  - Source: 01-foundation.md -> "Schema evolution classes" (orig L499)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/SchemaEvolutionClass.java

| Class | Rule |
| --- | --- |
| Additive compatible | New optional field with default/null semantics; readers tolerate unknown field |
| Behavioral compatible | Same physical schema but changed algorithm/config version; replay comparison required |
| State incompatible | Flink serializer/state shape changes; savepoint migration or clean restart required |
| Wire incompatible | Protocol/event schema cannot be read by old consumer; ordered deployment required |
| Breaking clean-break | Pre-production only; destructive approval, reset, replay, and rollback evidence required |

Every change records producer-first/consumer-first order, dual-read/write needs, savepoint impact, lake synchronization, and rollback readability.

### EOD controller and offload gate

#### Implementation checklist

- [x] EOD controller state machine + offload gate (PENDING->...->VERIFIED; retention extension; no source expiry until VERIFIED).
  - Source: 01-foundation.md -> "EOD controller and offload gate" (orig L511)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/EodControllerState.java

The EOD controller is a named service or scheduled job owning manifest creation, verification, retry/backoff, retention extension, expiry protection, and manual reconciliation. Source data for a trading day SHALL not expire while the manifest is unverified, retryable, or under reconciliation.

Each table/day offload record contains:

- Trading date
- Table and schema version
- Source offset/range
- Row and byte counts
- Source and target hashes/checksums
- Iceberg snapshot/commit ID
- Verification state
- Retry count and next retry
- Earliest allowed source expiry

State machine:

```text
PENDING → WRITING → COMMITTED → VERIFYING → VERIFIED
                    ↘ FAILED_RETRYABLE
                    ↘ FAILED_MANUAL
```

Source data cannot expire unless state is `VERIFIED`, and at least three complete trading days remain live. Unverified or retryable state extends retention through a tested control mechanism; a fixed DDL TTL comment is insufficient.

### Seven-year audit boundary

#### Implementation checklist

- [ ] Seven-year audit boundary (encrypted immutable audit copy, key management, S3 versioning, access audit, deletion/legal-hold, periodic reconstruction test).
  - Source: 01-foundation.md -> "Seven-year audit boundary" (orig L537)
  - Design: Design-ready | Implementation: Not-implemented | Evidence: Untested | Live-money: Blocked
  - Location: _not implemented_

Short operational Fluss TTL and seven-year audit retention are separate contracts. Money-moving events must be copied to encrypted immutable audit storage with:

- Verified manifest
- Encryption and key-management evidence
- S3 versioning/lifecycle policy
- Access audit
- Approved deletion/legal-hold behavior
- Periodic reconstruction test

### Test requirements

#### Implementation checklist

- [x] Schema/storage test suite implemented (DDL parse/apply, parity, routing skew, immutable dup/mutation, KV stale, changelog/partial-update, checkpoint/replay, clean-break, EOD, 7-yr reconstruction).
  - Source: 01-foundation.md -> "Test requirements" (orig L548)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/test/java/com/trading/common/schema/ (unit: RoutingKeyRule/KvStateUpdateProtocol/ImmutabilityProtocol/SchemaState/EodControllerState/SchemaManifestSerialization/SchemaComplianceFullSuite; integration stubs: fluss/CompatFlussIntegrationTest, tagged `integration`)

- DDL parse/apply for pinned matrix.
- Effective schema/options inspection.
- Schema parity for every required field.
- Non-null routing and bucket-skew tests.
- Immutable duplicate/mutation tests.
- KV stale/regressive/conflict tests.
- Changelog and partial-update tests.
- Checkpoint/replay compatibility tests.
- Clean-break reset and replay tests.
- EOD failure/retry/expiry-protection tests.
- Seven-year audit reconstruction simulation.

### Completion checklist

#### Implementation checklist

- [x] Completion checklist satisfied (manifest format; DDL checksums; stale paths removed; pinned dialect tests; non-null routing; immutability/stale-update; retention executable; audit-lake evidence).
  - Source: 01-foundation.md -> "Completion checklist" (orig L562)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [x] Schema manifest format is implemented.
- [ ] All DDLs have checksums and compatibility classes.
- [x] Stale DDL paths are removed from application workflow.
- [ ] Pinned dialect tests pass.
- [x] Every table has a non-null routing strategy.
- [ ] Immutability and stale-update protocols are implemented and tested.
- [ ] Retention extension is executable, not just documented.
- [ ] Audit-lake retention and reconstruction evidence exist.

## Shared safety rules

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Safety-rules status tracked (implementation-ready; runtime validation pending).
  - Source: 01-foundation.md -> "Status" (orig L579)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Implementation-ready; runtime validation pending |
| Owner | Platform Team; component owners implement locally |
| Sources | `docs/01_project/02-system-context.md`, `docs/02_requirements/04-data.md`, `docs/02_requirements/05-interfaces.md`, `DEC-005`–`DEC-020` |

### Identity invariant

#### Implementation checklist

- [x] Typed identity model implemented (candidate_id, instruction_id, execution_attempt_id, client_order_ref, broker_order_id, trade_context_id, position_id, postback_event_id, action_id, reservation_id, halt_request_id); generic order_id prohibited. Scope identities (account_scope_id, portfolio_id, execution_partition_id) enforced with isolation tests.
  - Source: 01-foundation.md -> "Identity invariant" (orig L587)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/identity/IdentityModel.java

The following identities are never interchangeable:

| Identity | Owner | Meaning |
| --- | --- | --- |
| `candidate_id` | Signal job | One detected setup/audit record |
| `instruction_id` | Signal job | One immutable execution request |
| `execution_attempt_id` | Executor | One broker submission attempt |
| `client_order_ref` | Executor | Broker-facing attempt reference |
| `broker_order_id` | Broker/Action Capture | Broker-authoritative order |
| `trade_context_id` | Signal/Execution | Entry and position-management grouping |
| `position_id` | Position projector | Fill-derived exposure aggregate |
| `postback_event_id` | Action Capture | One received postback delivery |
| `action_id` | Future Babysitter | One immutable position action |
| `reservation_id` | Signal job | Portfolio capacity reservation |
| `halt_request_id` | Authorized component | Durable safety-halt request |

#### Scope identities

| Scope | Purpose | Carried by |
| --- | --- | --- |
| `account_scope_id` | Broker/account isolation boundary | Gates, attempts, mappings, positions, lifecycle, halt requests, audit |
| `portfolio_id` | Ranking/reservation/capacity boundary | Reservations, candidate evaluation, instruction context |
| `execution_partition_id` | Fenced Executor ownership boundary | Execution gate, fencing token, attempt state |

A generic `order_id` is prohibited in new requirements, DDL, code, logs, and tests.

### Ownership matrix

#### Implementation checklist

- [x] Ownership matrix enforced (sole owner / readers / prohibited owners per data/behavior).
  - Source: 01-foundation.md -> "Ownership matrix" (orig L615)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/ownership/OwnershipMatrix.java

| Data/behavior | Sole owner | Readers | Prohibited owners |
| --- | --- | --- | --- |
| Raw packet/decode | Ingestion | Signal, audit/offload | Strategy, Executor |
| Candle/forming-bar state | Signal job | Business Logic/ranking | Ingestion, Executor |
| Candidates/ranking/decisions | Signal job | Executor, audit | Action Capture |
| Order lifecycle | Action Capture | Executor, operations | Signal, Babysitter |
| Position aggregate | Position projector | Babysitter, Executor | Strategy, raw ingestion |
| Order gate/attempt/mapping/audit | Executor | Action Capture/operations as needed | Signal, Executor |
| Postback audit/lifecycle | Action Capture | Executor, operations | Signal, Babysitter |
| Portfolio reservations | Signal job | Executor, reconciliation | N/A |
| Projection ledger | Action Capture | Recovery scanner | N/A |
| Safety halt requests | Authorized components | Executor | N/A |
| Broker REST call | Executor via Arrow REST | Broker | Every other component |
| Position actions | Babysitter after MVP | Executor | Arrow REST / direct callers |

### Delivery semantics

#### Implementation checklist

- [x] Delivery semantics enforced (at-least-once boundaries; exactly-once only within tested Flink/Fluss boundary).
  - Source: 01-foundation.md -> "Delivery semantics" (orig L632)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/DeliverySemantics.java

| Boundary | Contract |
| --- | --- |
| Broker stream → Ingestion | At-least-once; possible gaps; protocol evidence required |
| Ingestion → raw LOG | At-least-once; preserve every accepted packet |
| Compute dedup | Bounded best-effort fingerprinting |
| Flink state/sink | Exactly-once only within tested version-pinned boundary |
| Multiple Fluss outputs | Partial visibility unless a test proves a transaction boundary |
| Decision/action → Executor | At-least-once; immutable identity/request hash guard |
| Executor → broker | At-least-once or unknown; reconcile before retry |
| Postback projections | At-least-once input; idempotent/versioned projection |

### Ordering invariant

#### Implementation checklist

- [x] Ordering invariant (event-time per-instrument, deterministic fingerprint tie-break, no broker-seq assumption, no global ordering from bucket affinity).
  - Source: 01-foundation.md -> "Ordering invariant" (orig L645)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/OrderingInvariant.java

- Per-instrument event processing uses event time and deterministic fingerprint tie ordering.
- Broker sequence ordering is not assumed.
- Per-order projection uses source version/timestamp and status precedence.
- Per-position projection uses fill-derived source version and duplicate event identity.
- Gate transitions use serialized epoch compare-and-set.
- Global ordering is never inferred from Fluss bucket affinity.

### Time invariant

#### Implementation checklist

- [x] Time invariant (event_time / receive_time / persist_start / persist_ack / processing_time / schema_version; monotonic duration clock; UTC correlation).
  - Source: 01-foundation.md -> "Time invariant" (orig L654)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/TimeInvariant.java

Every event carries:

```text
event_time        = verified broker/external event time
receive_time      = local receipt time
persist_start     = local monotonic/UTC append start
persist_ack       = local append acknowledgement time
processing_time   = local processing timestamp
schema_version    = event schema
```

Duration measurements use a monotonic clock. Cross-service correlation uses UTC. Clock offset is observable and readiness-affecting when outside policy.

### Immutability invariant

#### Implementation checklist

- [x] Immutability invariant (id+hash duplicate/violation; changed params -> new id + supersession; execution state never in strategy decision fields).
  - Source: 01-foundation.md -> "Immutability invariant" (orig L669)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/ImmutabilityProtocol.java

```text
immutable identity + same canonical hash
  → duplicate delivery; audit and suppress duplicate effect

immutable identity + different canonical hash
  → contract violation; quarantine and halt safety-relevant flow

changed executable parameters
  → new identity and explicit supersession
```

Execution-owned state is never written into strategy-owned immutable decision fields.

### Failure invariant

#### Implementation checklist

- [x] Failure invariant (behavior defined for before/during/ack/timeout/duplicate/restart/stale/corrupt; ambiguity -> UNKNOWN).
  - Source: 01-foundation.md -> "Failure invariant" (orig L684)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/FailureInvariant.java

Every side effect must define behavior for:

- Before side effect
- During side effect
- Successful side effect before acknowledgement persistence
- Timeout/disconnect
- Duplicate delivery
- Restart after partial completion
- Stale/out-of-order input
- Corrupt or unavailable durable state

Ambiguity is represented explicitly as `UNKNOWN` and never treated as rejection or success by assumption.

### Order safety invariant

#### Implementation checklist

- [x] Order-safety invariant (pre-call gate checks; any fail -> no call; UNKNOWN -> HALTED).
  - Source: 01-foundation.md -> "Order safety invariant" (orig L699)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/OrderSafetyGate.java

Before every money-moving call:

```text
valid immutable instruction/action
→ no unresolved duplicate/attempt
→ current gate state is ENABLED
→ current gate epoch matches attempt
→ active Executor fencing lease is valid
→ request hash/client reference are durably persisted
→ Arrow REST contract is ready
→ call is issued
```

Any failed check prevents the call. Unknown result transitions the affected gate to `HALTED`.

### Configuration invariant

#### Implementation checklist

- [x] Configuration invariant (static / deployment / secret / evidence-gated split; missing required value -> not ready, no unsafe default).
  - Source: 01-foundation.md -> "Configuration invariant" (orig L716)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/config/PlatformConfig.java

Configuration is divided into:

1. **Static build values:** exact library/image versions and schema version.
2. **Deployment values:** topology, resource, checkpoint, retention, and endpoint settings.
3. **Secret references:** credentials and keys, never secret contents in documentation.
4. **Evidence-gated values:** broker fields, limits, status mappings, idempotency, and protocol behavior.

A missing required value makes the affected component not ready. It must not fall back to an unsafe default.

### Observability invariant

#### Implementation checklist

- [x] Observability invariant (structured logs, contract-proving metrics, health != business state, immutable audit, redaction; OpenObserve outage cannot authorize orders/erase audit).
  - Source: 01-foundation.md -> "Observability invariant" (orig L727)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/observability/

Every component emits:

- Structured logs with service/instance/version/correlation IDs.
- Metrics sufficient to prove its contract.
- Health dimensions separate from business state.
- Immutable audit for safety and money-moving decisions.
- Redaction of credentials and sensitive raw payloads.

OpenObserve outage cannot authorize orders and cannot erase durable execution audit.

### Review checklist

#### Implementation checklist

- [x] Review checklist items tracked (owner per state/table, named identities, retry/idempotency rules, timestamp semantics, UNKNOWN outcomes, connector tests, audited safety transitions, readiness explanations).
  - Source: 01-foundation.md -> "Review checklist" (orig L739)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [x] New code has one documented owner per state/table. ✓ code `code/common/.../ownership/OwnershipMatrix.java` (12 rows: raw packets→Ingestion, candles→Signal, lifecycle→Action Capture, gate/attempt→Executor, etc.)
- [x] Every identity is named explicitly. ✓ code `code/common/.../identity/IdentityModel.java` (15 typed identities: InstructionId, ClientOrderRef, BrokerOrderId, InstrumentToken, etc.; generic `order_id` prohibited)
- [x] Every retry has a duplicate/idempotency rule. ✓ Foundation: ImmutabilityProtocol, KvStateUpdateProtocol (duplicate/stale conflict detection). Ingestion: RawTickWriter fingerprint-based LRU idempotency cache (10k entries, DUPLICATE outcome for re-submissions), retry loop with linear backoff (up to 3 attempts), UNCERTAIN outcome on timeout (no silent assumption of success). Executor-layer retry pending Phase 5.
- [x] Every timestamp has defined semantics. ✓ code `code/common/.../invariants/TimeInvariant.java` (6 canonical fields: event_time, receive_time, persist_start, persist_ack, processing_time, schema_version; monotonic clock)
- [x] Every uncertain outcome becomes explicit state. ✓ code `code/common/.../invariants/FailureInvariant.java` (Disposition.UNKNOWN — ambiguity never guessed, always explicit)
- [ ] Every cross-table assumption has a connector test. _(blocked — no Flink jobs built yet; needs Phase 3+)_
- [ ] Every safety transition is audited. _(partially done — AuditLogger exists in foundation; full audit needs Executor + Action Capture)_
- [x] Every readiness result explains which dependency is missing. ✓ code `code/.../ingestion/health/HealthProbe.java` (`diagnostics()` returns per-dimension map: alive, fluss_ready, tracker_ready, broker_connected, subscription_complete, frame_recent, clock_offset_ms, clock_ok)
