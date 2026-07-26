# 06 — Operational Requirements

## 6.1 Operating modes

Local operations use Docker Compose. Production operations use Docker Swarm across three workload VMs and one observability VM. Every runbook identifies its environment; local commands are never presented as production procedures.

## 6.2 Startup

Production startup order is readiness-driven:

1. S3 checkpoint/lake and Swarm secret access validated.
2. Fluss quorum, tablets, replication, and schemas healthy.
3. Flink control/workers healthy.
4. Signal and Babysitter jobs deployed from pinned artifacts and checkpoint-compatible state.
5. Ingestion and Action Capture connect using verified protocol/schema versions.
6. Executor starts `HALTED`, validates durable state, mappings, continuity, OpenAlgo, and observability.
7. Reconciliation completes; two authorized operators approve the same gate epoch/evidence hash before `ENABLED`.

A process start or container health check never automatically enables order placement.

## 6.3 Health checks

Each service exposes liveness and readiness. Operations additionally monitor Flink job/checkpoint health, Fluss replication/quorum, Executor trading readiness/gate state, storage/offload readiness, broker subscription completeness, projection lag, and observability delivery.

Readiness failure removes traffic/work ownership where safe. Order-path uncertainty also halts the gate.

## 6.4 Shutdown and maintenance

Planned shutdown first halts new money-moving calls, records gate epoch/reason, drains/reconciles in-flight attempts, checkpoints jobs, drains accepted ingestion/postbacks within bounded deadlines, and verifies durable state before stopping storage.

Forced termination creates an audit/alert and requires reconciliation before resume.

DDL, version, checkpoint/savepoint, retention, secret, network, and capacity changes use approved change records, prechecks, rollback criteria, and post-deployment verification.

## 6.5 Backup, recovery, and disaster recovery

- Flink checkpoints/savepoints: encrypted versioned S3.
- Fluss: three-node replicated data across workload VMs.
- Immutable audit/lake: encrypted S3; execution/order/fill/gate audit retained seven years.
- Operational KV projections: rebuildable from immutable audit or tested backup.

Required exercises include process restart, tablet/worker loss, any one workload VM loss, checkpoint corruption/unavailability, S3 interruption, Fluss quorum degradation, observability loss, broker disconnect, and Executor crash windows.

Data processing resumes under 30 seconds for accepted failure scenarios. Order safe-halt completes under five seconds. RPO/RTO are reported per scenario; no broader claim is made than tested.

## 6.6 EOD offload operations

At EOD the operator/service verifies manifests, counts, source ranges, schema versions, hashes/checksums, and Iceberg commits. Failures retry with backoff and extend Fluss retention. Critical alerts fire on failed verification, insufficient expiry margin, S3 unavailability, or storage pressure.

No source day expires until its manifest is verified, and at least three complete trading days remain live.

## 6.7 Gate halt and resume runbook

Halt records reason, detection timestamp, scope, epoch, affected attempts/reservations, and evidence. Reconciliation checks broker orders, OpenAlgo responses, mappings, fills/positions, changelog continuity, Signal job/checkpoints, and every unknown attempt.

Resume requires two distinct authenticated authorized operators approving the same evidence hash and epoch. Unauthorized or mismatched approvals are rejected, audited, and alerted. Automatic resume is prohibited.

## 6.8 Secrets and security operations

Production credentials are Swarm secrets. Rotation/revocation is documented and tested for broker, OpenAlgo, S3, OpenObserve, TLS, and operator credentials. Logs and support bundles run redaction checks. Audit access is role-controlled and reviewed.

Security incidents halt affected order flow, rotate credentials, preserve evidence, and require reconciliation/two-person resume.

## 6.9 Observability and alert response

MVP runbooks cover all order-safety, streaming-health, storage-safety, and security alerts defined in `02-functional/08-observability.md`. Every critical alert has owner role, acknowledgement target, diagnostic evidence, halt behavior, escalation, remediation, and closure evidence. Due dates remain TBD until assigned; live-money release cannot proceed with unowned critical alerts.

## 6.10 Release gates

No live-money deployment until:

- Broker/OpenAlgo evidence and exact version matrix are approved.
- DDL/requirements/contracts/tests agree.
- Critical risks are closed.
- Throughput, latency, recovery, one-VM, checkpoint, replication, offload, security, and crash-window tests pass.
- Executor scaffold is replaced and durable gate/attempt/reconciliation behavior passes.
- Dashboards, alerts, runbooks, rollback, and audit retrieval are operational.

Paper/simulated trading may validate the system but does not waive live-money gates.

 
