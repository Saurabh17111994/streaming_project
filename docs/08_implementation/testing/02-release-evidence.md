# Live-Money Release Evidence Dossier

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Design-ready release gate; evidence not yet produced |
| Owner | Platform and Execution leads; Security/Compliance approval required |
| Release posture | `LIVE_MONEY_ALLOWED=false` until every mandatory gate passes |
| Source | `docs/05_deployment/00-release-strategy.md`, `docs/02_requirements/00-index.md`, `plan.md` Phase 12 |

## Evidence package contents

1. Approved requirements/decision/contract/DDL revision set.
2. Version and compatibility matrix with artifact evidence.
3. DDL/schema manifest, checksums, effective schema inspection, and parity result.
4. Packet/postback corpus and broker/OpenAlgo sandbox evidence.
5. Component unit/integration/failure/recovery reports.
6. Flink checkpoint/savepoint/state compatibility reports.
7. EOD manifest/offload/retention verification.
8. Performance reports for baseline, burst, stress, and one-VM loss.
9. Security, secret rotation, least privilege, network, image/SBOM reports.
10. Dashboard/alert/runbook readiness evidence.
11. Rollback/readability test and deployment change record.
12. Executor crash-window, fencing, reconciliation, and two-person approval evidence.
13. Seven-year audit reconstruction simulation and policy approval.

## Binary release gates

| Gate | Pass condition | Evidence ID |
| --- | --- | --- |
| Requirements | No unresolved contradiction among requirements, decisions, contracts, DDLs, and code | `REL-REQ-*` |
| Versions | Exact versions/digests approved; no `latest` | `REL-VER-*` |
| Protocol | Broker/Arrow/OpenAlgo fields, identities, status, response, and limits proven | `REL-PROTO-*` |
| Schema | DDL parses/applies/parity/replay/retention tests pass | `REL-SCHEMA-*` |
| Ingestion | Golden packets, raw bytes, fingerprint limits, backpressure, subscription completeness pass | `REL-ING-*` |
| Signal job | Event time, dedup, candles, ranking, reservations, restore pass | `REL-SIG-*` |
| Action Capture | Correlation/quarantine/lifecycle/positions/partial writes/rebuild pass | `REL-AC-*` |
| Babysitter | Separate job checkpoints and emits zero MVP actions | `REL-BAB-*` |
| Executor | Durable gate/attempt/mapping/audit/fencing/reconciliation pass | `REL-EXE-*` |
| Crash window | No duplicate broker order in every tested ambiguity window | `REL-CRASH-*` |
| Safe halt | Calls block within five seconds for every defined uncertainty trigger | `REL-HALT-*` |
| Two-person resume | Distinct authenticated approvals match epoch/evidence hash | `REL-APPROVAL-*` |
| Capacity | 75k/112.5k/150k workload campaigns pass | `REL-PERF-*` |
| HA/recovery | One workload VM loss, checkpoint, replication, and recovery posture pass | `REL-HA-*` |
| EOD/audit | Offload verification and retention protection pass; audit reconstructable | `REL-RET-*` |
| Security | Network, secrets, rotation, authorization, encryption, image policy pass | `REL-SEC-*` |
| Operations | Dashboards, alerts, runbooks, rollback and owners are operational | `REL-OPS-*` |

## Approval sequence

1. Component owners sign their evidence.
2. Platform reconciles the version/schema/deployment package.
3. Execution signs gate/attempt/correlation/fencing/crash-window evidence.
4. Security signs secret/network/authorization/audit controls.
5. Operations signs dashboards/alerts/runbooks/rollback.
6. Compliance signs retention/deletion/legal policy.
7. Release owner confirms no unresolved critical risk.
8. Production deploys with gate `HALTED`.
9. Post-deployment reconciliation completes.
10. Two distinct authenticated operators approve the same gate epoch/evidence hash.
11. Enablement is recorded as an immutable audit event.

## Automatic blocking

The release process must fail closed for:

- Unknown version/protocol behavior.
- Missing or stale evidence.
- Failed/skipped mandatory test.
- Unresolved attempt or reservation.
- Unknown gate state.
- Lost fencing/durable state/changelog continuity.
- Unverified offload/retention.
- Missing telemetry or unowned critical alert.
- Rollback uncertainty.

## Rejection and rollback

A release is rejected if any mandatory gate fails. If uncertainty appears after deployment, the Executor returns to `HALTED`, evidence is preserved, affected orders/fills/positions are reconciled, and rollback follows the approved state-readable path. Automatic resume is prohibited.

## Final approval record

```text
release_id:
source_commit:
artifact_digests:
schema_manifest:
version_matrix:
compatibility_result:
all_gate_results:
open_risks:
rollback_artifact:
platform_approval:
execution_approval:
security_approval:
operations_approval:
compliance_approval:
first_operator:
second_operator:
gate_epoch:
evidence_hash:
enablement_timestamp_utc:
```

## Definition of done

This dossier is complete only when the evidence package can be independently reviewed and every gate is binary pass, with no P0/P1 issue unresolved or silently waived.
