# Implementation Documentation Governance

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready governance; project remains release-blocked |
| Owner | Platform Team with Execution Team approval for money-moving behavior |
| Source of work | `../../plan.md` |
| Live-money default | Disabled; Executor starts `HALTED` |
| Scope | Documentation, evidence, change control, and traceability; no production code |

## Purpose

This document defines how `plan.md` is converted into safe implementation work. It prevents a checklist item from being treated as complete merely because a file was changed.

## Work item lifecycle

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

## Evidence record

Every implementation or release claim uses this record:

| Field | Required content |
| --- | --- |
| Work item ID | `plan.md` task or issue ID |
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

## Placeholder policy

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
OPENALGO_API_CONTRACT_TO_BE_VERIFIED
PRODUCTION_IMAGE_DIGEST_TO_BE_PINNED
S3_CHECKPOINT_URI_TO_BE_DEFINED
```

A placeholder is acceptable in development documentation only if:

1. Its missing value is obvious.
2. The affected behavior is disabled or safely bounded.
3. An owner and evidence method are recorded.
4. It blocks live-money readiness where the value affects safety, compatibility, identity, or durability.

Do not use `latest`, unqualified defaults, `seq_no`, `postback_seq`, or a generic `order_id` as substitutes for unknown facts.

## Change control

A change to any of the following requires a reconciliation review:

- Active decision
- Requirement
- DDL/schema
- Identity or event contract
- Flink state/checkpoint contract
- Broker/OpenAlgo protocol adapter
- Execution gate or approval behavior
- Retention/offload policy
- Deployment topology or secret scope

The change record must identify affected artifacts, compatibility class, state/savepoint impact, test updates, rollback behavior, and plan tasks.

## Documentation acceptance rules

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

## Live-money stop conditions

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

## Documentation review checklist

- [ ] All new behavior links to an active requirement or decision.
- [ ] No dossier silently contradicts an upstream document.
- [ ] Unknown external behavior is marked with a placeholder and evidence owner.
- [ ] Every state transition has an invalid-transition behavior.
- [ ] Every side effect has a retry/duplicate/crash-window rule.
- [ ] Every acceptance claim has a test ID or evidence record.
- [ ] Every plan task touched by the dossier has a status/evidence update.
- [ ] No production code was changed during a documentation-only task.
