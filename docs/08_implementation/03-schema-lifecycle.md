# Schema Lifecycle and Storage Dossier

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Design-ready; physical DDL remains version-validation blocked |
| Owner | Storage/Platform Team |
| Source requirements | `REQ-ST-*`, `docs/02_requirements/04-data.md`, `DEC-001`, `DEC-005`, `DEC-018`, `DEC-020`, `DEC-021` |
| Migration posture | Pre-production clean break until live-money release |

## Purpose

This dossier defines how logical schemas become validated physical Fluss DDLs and how schema changes, retention, replay, lake offload, and rollback are controlled.

## Schema states

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

## Schema manifest

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

## DDL application contract

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

## Table categories and invariants

| Category | Invariant |
| --- | --- |
| Immutable LOG | Append one platform event per delivery; no silent update/delete; duplicates explicit |
| Immutable instruction feed | Existing identity may not change content; mutation is a contract violation |
| KV projection | Keyed current state; source-version and transition rules reject stale/regressive writes |
| Gate/attempt state | Compare current epoch/phase before transition; every transition audited |
| Manifest | One approved manifest version defines active subscription state |

## Routing-key rule

Every LOG write must have a non-null routing identity. Nullable business fields must not be the only bucket key.

Proposed routing review:

| Table | Required routing identity | Rationale |
| --- | --- | --- |
| `raw_table_1` | `instrument_token` after validation | Per-instrument processing order |
| `feature_candles_15s` | `instrument_token` | Per-instrument window history |
| `Signal_Candidates` | `instrument_token` or non-null candidate routing identity | Strategy locality |
| `Ranking_Results` | Non-null evaluation/candidate routing identity | Avoid cross-instrument/null ambiguity |
| `Fills_table` | `postback_event_id` when broker ID may be absent | Every delivery is routable |
| `Execution_Audit` | `audit_event_id` | Gate-only events may lack instruction ID |
| `Postback_Quarantine` | `quarantine_id` | Missing broker ID is expected |

Final physical keys remain evidence-gated by pinned Fluss distribution semantics.

## Immutability protocol

For each immutable entity, calculate a canonical versioned content hash:

```text
same identity + same hash    → idempotent duplicate evidence
same identity + different hash → contract violation
new executable content       → new identity with supersedes relation
```

Writers must persist or query enough state to detect mutation. LOG-table comments and `NOT ENFORCED` keys do not enforce this protocol.

## KV state update protocol

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

## Schema evolution classes

| Class | Rule |
| --- | --- |
| Additive compatible | New optional field with default/null semantics; readers tolerate unknown field |
| Behavioral compatible | Same physical schema but changed algorithm/config version; replay comparison required |
| State incompatible | Flink serializer/state shape changes; savepoint migration or clean restart required |
| Wire incompatible | Protocol/event schema cannot be read by old consumer; ordered deployment required |
| Breaking clean-break | Pre-production only; destructive approval, reset, replay, and rollback evidence required |

Every change records producer-first/consumer-first order, dual-read/write needs, savepoint impact, lake synchronization, and rollback readability.

## EOD offload and retention gate

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

## Seven-year audit boundary

Short operational Fluss TTL and seven-year audit retention are separate contracts. Money-moving events must be copied to encrypted immutable audit storage with:

- Verified manifest
- Encryption and key-management evidence
- S3 versioning/lifecycle policy
- Access audit
- Approved deletion/legal-hold behavior
- Periodic reconstruction test

## Test requirements

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

## Completion checklist

- [ ] Schema manifest format is implemented.
- [ ] All DDLs have checksums and compatibility classes.
- [ ] Stale DDL paths are removed from application workflow.
- [ ] Pinned dialect tests pass.
- [ ] Every table has a non-null routing strategy.
- [ ] Immutability and stale-update protocols are implemented and tested.
- [ ] Retention extension is executable, not just documented.
- [ ] Audit-lake retention and reconstruction evidence exist.
