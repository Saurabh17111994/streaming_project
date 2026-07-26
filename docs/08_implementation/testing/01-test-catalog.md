# Implementation Test Catalog

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready test design; executable suites pending |
| Owner | Component owners; Platform owns integration/acceptance evidence |
| Scope | Unit, harness, integration, failure, recovery, performance, security, release |
| Rule | A skipped/flaky mandatory gate is a failure until dispositioned with evidence |

## Test record format

Every executable test or campaign records:

```text
test_id
requirement_ids
audit_issue_ids
component/boundary
preconditions and exact versions
fixture/workload and checksum
steps/fault injection
expected result
observed result
artifacts/log/audit IDs
clock/environment evidence
owner/date
```

## Unit and deterministic component tests

| Test family | Required behavior |
| --- | --- |
| `ING-UNIT-*` | Decode, bytes/hash, normalization, fingerprint, invalid/quarantine |
| `SIG-UNIT-*` | Dedup TTL, tie ordering, candles, candidates, ranking, reservation |
| `AC-UNIT-*` | Postback identity, correlation, status precedence, positions |
| `BAB-UNIT-*` | Strict no-op and action-enable fail-closed |
| `EXE-UNIT-*` | Gate, immutability, attempts, references, approvals, classification |
| `SCHEMA-UNIT-*` | Manifest/checksum/parity/compatibility classification |

Deterministic tests use fixed clocks, versioned fixtures, stable IDs/seeds, and canonical expected outputs.

## Flink harness and state tests

- `SIG-HARNESS-001`: out-of-order events, watermark and idleness.
- `SIG-HARNESS-002`: late-before-final versus discard-after-final.
- `SIG-HARNESS-003`: deterministic checkpoint/restore replay.
- `SIG-HARNESS-004`: identical legitimate event versus duplicate fingerprint limitation.
- `SIG-HARNESS-005`: reservation and ranking recovery.
- `BAB-HARNESS-001`: position changelog/offset restore with zero action output.
- `STATE-COMPAT-001`: serializer/savepoint compatibility for every version change.

## Integration tests

- `COMPAT-FLUSS-001`: DDL parse/apply/effective schema.
- `COMPAT-FLUSS-002`: BYTES round trip.
- `COMPAT-FLUSS-003`: LOG/KV/changelog semantics.
- `COMPAT-FLUSS-004`: partial update and stale-write application protocol.
- `COMPAT-FLINK-001`: source/sink checkpoint and restore.
- `COMPAT-FLINK-002`: cross-table partial visibility behavior.
- `BROKER-MD-001`: market packet corpus/protocol compatibility.
- `BROKER-PB-001`: postback schema/status/timestamp/identity behavior.
- `OPENALGO-001`: request/response/auth/timeout/rejection behavior.
- `OPENALGO-002`: client reference length/charset/echo and broker ID correlation.
- `E2E-001`: packet → decision → attempt → sandbox broker → postback → position.

## Crash-window and failure matrix

| Fault point | Expected invariant |
| --- | --- |
| Before raw append | Loss/uncertainty explicitly accounted; no silent success |
| After raw append before local ack | At-least-once duplicate tolerated downstream |
| During Flink checkpoint | Restore produces deterministic state within tested boundary |
| Between multiple Signal sinks | Partial visibility reconciled by IDs |
| After postback audit before lifecycle | Projection ledger resumes |
| After lifecycle before position | Position step resumes without lifecycle regression |
| Before attempt prepare | No broker call |
| After prepare before call | Recover pending attempt; no blind new attempt |
| During broker call | Outcome UNKNOWN; halt/reconcile |
| After broker acceptance before durable ack | UNKNOWN; no duplicate retry |
| During mapping persistence | Halt and reconcile mapping evidence |
| During gate approval | No partial/same-identity enablement |
| Fencing lease loss | Calls blocked; gate halted |
| Changelog gap | Trading readiness false; reconcile |
| S3/checkpoint unavailable | Job/durability not ready; gate halted if correctness affected |
| OpenObserve unavailable | Durable audit remains; telemetry readiness false |

## Schema and recovery tests

- Clean catalog/table creation.
- Effective schema and option parity.
- Missing/wrong schema version prevents readiness.
- Immutable identity same/different hash behavior.
- KV stale/regressive/conflicting transition behavior.
- Projection rebuild from immutable events.
- Clean-break reset/replay.
- Savepoint/schema migration and rollback readability.
- EOD offload retry and expiry protection.
- Audit retrieval/reconstruction.

## Performance campaigns

| Test | Workload | Duration | Required evidence |
| --- | ---: | ---: | --- |
| `PERF-BASELINE-001` | 75,000 ticks/s | Full trading session | SLOs, loss, backlog, checkpoints |
| `PERF-BURST-001` | 112,500 ticks/s | ≥30 min | p50/p95/p99 and bounded backlog |
| `PERF-STRESS-001` | 150,000 ticks/s | ≥60 min | Saturation, recovery, no ack loss |
| `PERF-NODELOSS-001` | 75,000 ticks/s + one VM loss | Recovery window | Quorum, restore, <30 s accepted recovery, <5 s halt |
| `PERF-EOD-001` | Full-volume day | EOD | Verified manifest <30 min target |

Use production instrument universe, connections, packet-size distribution, strategy state, and exact versions. Window waiting is reported separately from processing latency.

## Security tests

- Network exposure and deny-path tests.
- TLS/encrypted transport/storage.
- Secret scan, log redaction, and support-bundle redaction.
- Rotation/revocation for every credential class.
- Least privilege for table/state/broker calls.
- Unauthorized gate/approval/control operations.
- Image digest/SBOM/vulnerability policy.
- Audit access and deletion/legal-hold policy.

## CI gates

CI must fail for:

- Missing service entry point.
- Missing required test family.
- Stale prohibited identifiers or architecture terms.
- `latest` or unpinned production dependency.
- Requirement/contract/DDL/schema mismatch.
- Failing/skipped/flaky mandatory test.
- Missing evidence metadata.
- Secret/redaction failure.
- Unsupported state/schema compatibility.

## Definition of done

The test program is complete when every mandatory requirement and P0/P1 audit issue maps to executable evidence, exact versions and environments are recorded, failure tests exercise the actual crash windows, performance campaigns match the workload envelope, and release evidence can be independently reviewed.
