# Schema and Storage

Use this file to build the data tables and the tests that prove they are safe.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field               | Value                                                                                                 |
| ------------------- | ----------------------------------------------------------------------------------------------------- |
| Status              | Design-ready; Phase A (19 DDLs) complete; Phase B (static validation) complete; Phase C blocked on live Fluss cluster; Phase D partially complete; Phase E deferred                                         |
| Owner               | Storage/Platform Team                                                                                 |
| Source requirements | `REQ-FLS-*`, `docs/02_requirements/04-data.md`, `DEC-001`, `DEC-005`, `DEC-018`, `DEC-020`, `DEC-021` |
| Migration posture   | Pre-production clean break until live-money release                                                   |

### Purpose

This dossier defines how logical schemas become validated physical Fluss DDLs and how schema changes, retention, replay, lake offload, and rollback are controlled.

### Schema states

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

Each release must generate a machine-readable manifest containing:

| Field                     | Meaning                                           |
| ------------------------- | ------------------------------------------------- |
| `schema_manifest_version` | Version of the manifest format                    |
| `table_name`              | Exact case-sensitive logical/physical table name  |
| `schema_version`          | Table contract version                            |
| `ddl_path`                | Repository-relative DDL file                      |
| `ddl_sha256`              | Checksum of normalized DDL                        |
| `table_kind`              | LOG, KV, manifest, or immutable feed              |
| `writer_owner`            | Sole writer or column-group owners                |
| `primary_key`             | Physical key or `none`                            |
| `bucket_key`              | Guaranteed non-null routing key                   |
| `retention_policy`        | Live retention and extension rule                 |
| `lake_policy`             | Offload/audit behavior                            |
| `compatibility_class`     | Backward, forward, full, breaking, or clean-break |
| `validated_matrix`        | Version compatibility record ID                   |

### DDL application contract

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

| Category                   | Invariant                                                                               |
| -------------------------- | --------------------------------------------------------------------------------------- |
| Immutable LOG              | Append one platform event per delivery; no silent update/delete; duplicates explicit    |
| Immutable instruction feed | Existing identity may not change content; mutation is a contract violation              |
| KV projection              | Keyed current state; source-version and transition rules reject stale/regressive writes |
| Gate/attempt state         | Compare current epoch/phase before transition; every transition audited                 |
| Manifest                   | One approved manifest version defines active subscription state                         |

### Routing-key rule

Every LOG write must have a non-null routing identity. Nullable business fields must not be the only bucket key.

Proposed routing review:

| Table                        | Required routing identity                                 | Rationale                                |
| ---------------------------- | --------------------------------------------------------- | ---------------------------------------- |
| `raw_table_1`                | `instrument_token` after validation                       | Per-instrument processing order          |
| `feature_candles_15s`        | `instrument_token`                                        | Per-instrument window history            |
| `Signal_Candidates`          | `candidate_id` (KV primary key, R-084 — was LOG)                     | Strategy locality                        |
| `Ranking_Results`            | `evaluation_id` (R-136 — was `candidate_id`)                         | Avoid cross-instrument/null ambiguity    |
| `Fills`                      | `postback_event_id` when broker ID may be absent          | Every delivery is routable               |
| `Execution_Audit`            | `audit_event_id`                                          | Gate-only events may lack instruction ID |
| `Portfolio_Reservations`     | `reservation_id`                                          | Authoritative reservation state          |
| `Postback_Projection_Ledger` | `postback_event_id`                                       | Recovery workflow state                  |
| `Safety_Halt_Requests`       | `halt_request_id`                                         | Durable control event identity           |
| `Postback_Quarantine`        | `quarantine_id`                                           | Missing broker ID is expected            |

Final physical keys remain evidence-gated by pinned Fluss distribution semantics.

### Immutability protocol

For each immutable entity, calculate a canonical versioned content hash:

```text
same identity + same hash    → idempotent duplicate evidence
same identity + different hash → contract violation
new executable content       → new identity with supersedes relation
```

Writers must persist or query enough state to detect mutation. LOG-table comments and `NOT ENFORCED` keys do not enforce this protocol.

### KV state update protocol

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

| Class                 | Rule                                                                                     |
| --------------------- | ---------------------------------------------------------------------------------------- |
| Additive compatible   | New optional field with default/null semantics; readers tolerate unknown field           |
| Behavioral compatible | Same physical schema but changed algorithm/config version; replay comparison required    |
| State incompatible    | Flink serializer/state shape changes; savepoint migration or clean restart required      |
| Wire incompatible     | Protocol/event schema cannot be read by old consumer; ordered deployment required        |
| Breaking clean-break  | Pre-production only; destructive approval, reset, replay, and rollback evidence required |

Every change records producer-first/consumer-first order, dual-read/write needs, savepoint impact, lake synchronization, and rollback readability.

### EOD controller and offload gate

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

Short operational Fluss TTL and seven-year audit retention are separate contracts. Money-moving events must be copied to encrypted immutable audit storage with:

- Verified manifest
- Encryption and key-management evidence
- S3 versioning/lifecycle policy
- Access audit
- Approved deletion/legal-hold behavior
- Periodic reconstruction test

### Test requirements

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

### Implementation tasks

Each task is atomic — verifiable by a single test or code inspection.
Tasks are ordered by dependency; no downstream task may start before its
upstream is complete.

#### Phase A: DDL authoring (blocks all downstream tests)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-01 | Write DDL SQL for all 20 tables: `raw_table_1`, `feature_candles_15s`, `forming_bar`, `Signal_Candidates`, `Ranking_Results`, `Trade_Decisions`, `Fills`, `Order_Lifecycle`, `Positions`, `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`, `Portfolio_Reservations`, `Postback_Quarantine`, `Postback_Projection_Ledger`, `Safety_Halt_Requests`, `suspected_discontinuities`, `instruments`, `ingestion_quarantine` | [x] | `code/01_platform/02_sql/ddl/` — 20 SQL files (02_raw_table_1 through 21_ingestion_quarantine) with column definitions, bucket keys, retention policies from contracts |
| SCH-02 | Compute SHA-256 checksum for every DDL file; populate `schema_manifest.json` entries | [x] | `schema_manifest.json` (20 entries with table_name, ddl_sha256, table_kind LOG/KV, primary_key, bucket_key, schema_state=PROPOSED); `ddl_apply.py` computes + validates checksums |
| SCH-03 | Stale DDL paths removed from `make ddl` application workflow; manifest drift check runs clean | [x] | `ddl_apply.py` — manifest is current, no DDL drift detected (20 tables; checksum + `table_kind` + `primary_key`/`bucket_key` field-level validation since 2026-08-10); stale duplicates removed |
| SCH-04 | Version-gate: `make ddl` refuses to apply when any Fluss/Flink version is unpinned | [x] | `version_matrix_verify.py` + `VersionGate.requirePinned()` + `versions.pin` |

> **Note (2026-08-09):** `forming_bar` has a DDL and a manifest entry but no requirement, contract, or consumer defines its role — the compute contract (`04_contracts/03-compute.md`) explicitly keeps forming-bar state in-process with no Fluss round trip. It stays `PROPOSED`/unowned until a consumer requirement (e.g. per-instrument freshness, DEC-028) is written; do not treat it as owned.
>
> **Note (2026-08-10):** `Signal_Candidates` is now **owned and written** by the Signal job's Slice 2.1 (DEC-034) — closed-candle detection appends immutable candidate records via the KV upsert writer (`DdlBootstrap` carries the full 22-column KV descriptor; dev table created DDL-faithful). `Ranking_Results` / `Trade_Decisions` / `Portfolio_Reservations` remain unwritten (ranking postponed).

#### Phase B: Static schema validation (unit level, no cluster)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-05 | Schema manifest format: generate, serialize, deserialize round-trip | [x] | `SchemaManifest.java` + `SchemaManifestEntry.java` + `SchemaManifestSerializationTest` |
| SCH-06 | Schema state machine: `isExecutableAuthority()` is false until APPROVED | [x] | `SchemaState.java` + `SchemaStateTest` |
| SCH-07 | Routing-key rule: every LOG table must have a non-null routing identity | [x] | `RoutingKeyRule.java` + `RoutingKeyRuleTest` + `SchemaComplianceFullSuiteTest` |
| SCH-08 | Immutability protocol: same-id+same-hash=duplicate, same-id+diff-hash=violation | [x] | `ImmutabilityProtocol.java` + `ImmutabilityProtocolTest` + `SchemaComplianceFullSuiteTest` |
| SCH-09 | KV state update protocol: stale, regressive, conflict → UNKNOWN + halt signal | [x] | `KvStateUpdateProtocol.java` + `KvStateUpdateProtocolTest` + `SchemaComplianceFullSuiteTest` |
| SCH-10 | Schema evolution classes: all 5 classes (additive/behavioral/state-incompatible/wire-incompatible/breaking) defined and distinguishable | [x] | `SchemaEvolutionClass.java` |
| SCH-11 | EOD controller state machine: PENDING→WRITING→COMMITTED→VERIFYING→VERIFIED + FAILED_RETRYABLE/FAILED_MANUAL; `permitsSourceExpiry()` only true for VERIFIED; `requiresRetentionExtension()` for all non-VERIFIED; `isRetryable()` for FAILED_RETRYABLE | [x] | `EodControllerState.java` (7 states matching spec) + `EodControllerStateTest` (3 tests: expiry gating, retention extension, retryable discrimination) |

#### Phase C: Pinned-dialect validation (needs live Fluss cluster)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-12 | DDL parse + apply against pinned Fluss 0.9.1-incubating dialect | [ ] | **Blocked** — no live Fluss cluster available (19 DDLs ready, manifest current) |
| SCH-13 | Inspect effective schema/options from runtime → parity against logical requirements | [ ] | **Blocked** — depends on SCH-12 |
| SCH-14 | Changelog image behaviour: FULL changelog for KV tables with `partial_update` | [ ] | **Blocked** — depends on SCH-12 |
| SCH-15 | Partial-update merge semantics: column ownership, merge engine behaviour | [ ] | **Blocked** — depends on SCH-12 |
| SCH-16 | Cross-table visibility/atomicity limits documented with evidence | [ ] | **Blocked** — depends on SCH-12 |
| SCH-17 | Checkpoint + replay compatibility: connector checkpoint/restore + state savepoint/rescale | [ ] | **Blocked** — needs Flink jobs (Phase 3+) |

#### Phase D: Runtime enforcement (straddles Ingestion, Signal, Executor phases)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-18 | LOG append-only enforced: no silent update/delete; duplicates explicit | [x] | Fluss LOG table semantics + `RawTickWriter` per-tick append (Ingestion) |
| SCH-19 | Immutable instruction feed: existing identity may not change content | [ ] | Needs Signal job (Phase 3) — `TradeDecisions` writer |
| SCH-20 | KV projection version gating: stale/regressive writes rejected at projector | [ ] | Needs Action Capture (Phase 4) — `PositionState` projector |
| SCH-21 | Gate/attempt epoch+phase transition validation: stale epoch rejected, illegal transition blocked, UNKNOWN→HALTED | [x] | `GateTransitionValidator.java` — legal transition matrices for GateState + AttemptPhase |
| SCH-22 | Manifest enforcement: one approved manifest version defines active subscription state (version + count + fingerprint validation) | [x] | `InstrumentManifestLoader.java` — `loadDefault()` returns `ManifestResult(approved, version, count, fingerprint)`; `isManifestApproved()` validates version/count/fingerprint against expected; synthetic fallback only when `ALLOW_SYNTHETIC_MANIFEST=true`; `IngestionService.java` logs manifest metadata + exits on empty load |

#### Phase E: Retention, offload, audit (post-MVP / deferred)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-23 | Retention extension executable: EOD controller extends live retention for unverified days | [ ] | **Deferred** — EOD controller is a Phase 6 service |
| SCH-24 | Seven-year audit pipeline: encrypted immutable S3 copy, key management, periodic reconstruction test | [ ] | **Deferred** — post-MVP separate pipeline (DEC-021) |
| SCH-25 | Clean-break reset + replay: destructive approval, state reset, full replay from broker/log | [ ] | **Deferred** — pre-production drill, needs all services |

#### Test mapping

Each SCH task maps to a test ID from `11-testing-and-release.md`:

| SCH | Test IDs |
| --- | --- |
| SCH-05 | SCHEMA-UNIT-001 |
| SCH-07 | SCHEMA-UNIT-002 |
| SCH-08, SCH-09 | SCHEMA-UNIT-003 |
| SCH-12, SCH-13 | COMPAT-FLUSS-001 |
| SCH-14 | COMPAT-FLUSS-002 |
| SCH-15 | COMPAT-FLUSS-003 |
| SCH-17 | COMPAT-FLUSS-004 |
| SCH-18, SCH-21 | SCHEMA-REC-001 |
| SCH-23 | SCHEMA-EOD-001 |
| SCH-24, SCH-25 | SCHEMA-AUDIT-001 |

### Completion checklist

- [x] Schema manifest format is implemented. ✓ `SchemaManifest.java` + `SchemaManifestEntry.java` (SCH-05)
- [x] All DDLs have checksums and compatibility classes. ✓ `schema_manifest.json` (20 tables, SHA-256 per file), `ddl_apply.py` validates checksums (SCH-01, SCH-02)
- [x] Stale DDL paths are removed from application workflow. ✓ `ddl_apply.py` manifest drift check runs clean; stale duplicates removed (SCH-03)
- [ ] Pinned dialect tests pass. _(blocked — needs live Fluss cluster; SCH-12-17)_
- [x] Every table has a non-null routing strategy. ✓ `RoutingKeyRule.java` + all 20 DDLs have `bucket.key` or `PRIMARY KEY` (SCH-07)
- [x] Immutability and stale-update protocols are implemented and tested. ✓ `ImmutabilityProtocol.java` + `KvStateUpdateProtocol.java` + unit tests (SCH-08, SCH-09)
- [ ] Retention extension is executable, not just documented. _(deferred — needs EOD controller, SCH-23)_
- [ ] Audit-lake retention and reconstruction evidence exist. _(deferred — post-MVP, SCH-24, SCH-25)

## Verification mapping

The required behavior above is verified by the canonical [Schema and storage test design](./11-testing-and-release.md#schema-and-storage): `SCHEMA-UNIT-001` to `SCHEMA-UNIT-003`, `COMPAT-FLUSS-001` to `COMPAT-FLUSS-004`, `SCHEMA-REC-001`, `SCHEMA-EOD-001`, and `SCHEMA-AUDIT-001`.
