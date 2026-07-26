# Maintenance and Change Operations

## Maintenance principles

Maintenance is performed against an explicit environment, release/configuration identity, state/schema compatibility classification, owner, rollback plan, and verification checklist. Production money-moving changes begin with the Executor gate `HALTED`.

Local Compose maintenance is not production procedure or HA evidence.

## Planned maintenance procedure

1. Create a change record with scope, affected components, requirements/contracts/DDL/interfaces, versions, state impact, risk, rollback, and acceptance checks.
2. Schedule outside critical workload windows where possible and verify EOD retention margin.
3. Confirm backup/checkpoint/savepoint and audit availability.
4. Halt new money-moving calls and reconcile attempts, orders, fills, positions, reservations, mappings, and projections.
5. Verify the target artifact/configuration is immutable and compatibility-tested.
6. Apply the change with controlled Swarm update, restart, DDL, secret, network, or capacity procedure.
7. Monitor liveness, readiness, job health, trading readiness, durability readiness, alerts, offsets, backlog, checkpoints, and storage.
8. Run targeted functional, recovery, security, and SLO checks.
9. Keep gate halted until reconciliation and post-change verification pass.
10. Require two distinct authenticated approvals for the same current epoch/evidence hash before enablement.
11. Record outcome, evidence, deviations, and follow-up work.

## Change classes

### Application and dependency update

Requires immutable image digest, exact dependency/version matrix, unit/integration/compatibility tests, state/savepoint analysis, and rollback artifact. Rolling/canary deployment is allowed only where compatibility is proven.

### Flink job update

Verify serializer/state compatibility, checkpoint/savepoint restore, source offsets, dedup/window/forming-bar/ranking state, sink behavior, and instruction duplication risk. Any uncertainty keeps the affected path not ready and the gate halted.

### Fluss schema or DDL change

Requires versioned DDL, ownership/identity review, additive/removed/renamed field analysis, replay/lake tests, consumer compatibility, deployment order, rollback/reset/replay plan, and migration evidence. Clean-break changes are pre-live only unless separately approved.

### Configuration/capacity change

Record workload basis, resource impact, bucket/partition/replication impact, checkpoint/backpressure implications, SLO test, and rollback. Do not use lower-than-production workload as readiness evidence.

### Retention or EOD policy change

Verify manifest fields, counts, ranges, hashes/checksums, commit state, retry behavior, expiry margin, three-day live buffer, seven-year audit policy, and legal/compliance approval where applicable.

### Secret/network/security change

Use the secrets runbook and security tests. Verify least privilege, rotation/revocation, TLS/transport, redaction, public exposure, audit access, and compromised-credential recovery.

## Routine checks

Operations reviews at an approved cadence:

- Fluss quorum, replicas, disk, and storage pressure
- Flink jobs, checkpoints, restores, watermarks, backpressure, and state freshness
- Ingestion subscriptions, append acknowledgements, clock offset, quarantine, and discontinuities
- Action Capture correlation, projection backlog, lifecycle/position freshness, and quarantine
- Executor gate, unknown attempts, reservations, mappings, fencing, changelog continuity, and approvals
- EOD manifest verification, retry state, expiry margin, retention days, and S3 health
- OpenObserve ingestion, alert ownership, dashboard health, and audit query access
- Credential age, rotation status, image digests, vulnerabilities, and access logs

## Emergency maintenance

For imminent safety or durability risk:

1. Halt the affected gate immediately.
2. Preserve evidence and announce scope/owner.
3. Apply the smallest reversible mitigation.
4. Do not delete volumes, checkpoints, audit, or source data without authorization.
5. Reconcile all affected state and execute rollback/DR procedures as needed.
6. Restore service readiness only through the two-person approval process.

## Closure criteria

A maintenance event is closed only when the change is verified, alerts are clear or dispositioned, state and offsets are consistent, storage/retention safety is intact, audit evidence is stored, rollback remains understood, and the gate epoch has either remained halted or been explicitly re-enabled by two authorized operators.

## References

- Deployment strategy: `../05_deployment/00-release-strategy.md`
- CI/CD: `../05_deployment/01-ci-cd.md`
- Rollback: `../05_deployment/03-rollback.md`
- Secrets rotation: `../05_deployment/04-secrets-rotation.md`
- Operational requirements: `../02_requirements/06-operational.md`
