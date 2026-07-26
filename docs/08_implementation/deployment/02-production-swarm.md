# Production Docker Swarm Implementation Dossier

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready, infrastructure/version evidence blocked |
| Owner | Platform Team |
| Topology | Four VMs: three workload/HA, one observability |
| Live-money | Disabled until Phase 12 evidence passes |

## Placement model

| Node class | Required workload |
| --- | --- |
| Workload VM 1 | Fluss replica/quorum, Flink capacity, assigned services |
| Workload VM 2 | Fluss replica/quorum, Flink capacity, assigned services |
| Workload VM 3 | Fluss replica/quorum, Flink capacity, assigned services |
| Observability VM | OpenObserve and telemetry storage/collection |

Fluss replicas cannot co-locate. OpenObserve loss must not authorize orders or erase local durable audit.

The final service-to-node placement, CPU/memory, disk, network, and replica values are `PRODUCTION_CAPACITY_PROFILE_TO_BE_VERIFIED` and require workload evidence.

## Stack requirements

The production stack must define:

- Immutable image digests for every image.
- Exact Java/Python/Flink/Fluss/connector/protocol versions.
- Placement constraints and anti-co-location.
- Resource reservations/limits.
- Health checks and readiness dependencies.
- Restart, update, rollback, and shutdown policies.
- Encrypted overlay/TLS-protected transport where supported.
- Internal-only Fluss/tablet/checkpoint ports.
- Swarm secrets and least-privilege identities.
- Durable per-node Fluss volumes.
- Encrypted/versioned S3 checkpoint, Iceberg, and audit storage.
- Executor fencing and one active owner per account/order partition.

## Readiness sequence

1. Verify image digests, Swarm secrets, encrypted networks, volumes, and S3.
2. Verify Fluss quorum, replication, tablets, placement, and schema manifest.
3. Verify Flink control/workers and checkpoint storage.
4. Deploy Signal and Babysitter artifacts; verify running/checkpointing.
5. Verify Ingestion manifest/subscriptions and Action Capture protocol readiness.
6. Start Executor `HALTED`; verify state, mappings, continuity, OpenAlgo contract, fencing, and telemetry.
7. Complete broker/order/fill/position/attempt/reservation reconciliation.
8. Require two distinct approvals for the same gate epoch/evidence hash.
9. Enable only the approved gate epoch.

A container or service becoming healthy never enables order placement.

## Storage and recovery

- Fluss data uses durable per-node volumes and tested replication.
- Flink checkpoints/savepoints use encrypted versioned S3.
- Iceberg/audit storage uses encryption, versioning, and approved retention/lifecycle policy.
- Operational projections are rebuildable from immutable events/audit or a tested backup.
- Loss of any one workload VM is tested at 75,000 ticks/s.
- RPO/RTO is recorded per failure scenario; no untested global claim is made.

## Security and networking

Define separate logical networks for:

```text
data ingress
storage/control
compute
execution
observability
operator control
```

Only required ingress endpoints are exposed. Service identities are least-privilege. Broker/OpenAlgo/S3/OpenObserve/TLS/operator credentials rotate through Swarm secrets or approved workload identity.

## Deployment and rollback

Every change record includes:

- Artifact digests and version matrix.
- DDL/schema/state compatibility.
- Savepoint/checkpoint impact.
- Gate halt and reconciliation requirement.
- Prechecks and acceptance evidence.
- Rollback artifact and state-readability path.
- Post-deployment verification.

Any uncertain rollback returns the Executor gate to `HALTED`. Schema-breaking clean break is permitted only before live-money release with reset/replay evidence.

## Capacity acceptance

The production-like topology must prove:

- 75,000 ticks/s for a full session with decision p99 under 100 ms.
- 112,500 ticks/s for at least 30 minutes.
- 150,000 ticks/s for at least 60 minutes.
- One workload VM loss at normal load.
- Data recovery under 30 seconds for accepted scenarios.
- Safe-halt under five seconds.
- Full-volume EOD verification under 30 minutes.
- No duplicate broker order in crash-window tests.

## Acceptance checklist

- [ ] Production stack is separate from Compose.
- [ ] No mutable image tags or unpinned dependencies remain.
- [ ] Placement prevents Fluss replica co-location.
- [ ] Secret/identity/network tests pass.
- [ ] S3 checkpoint/lake/audit recovery passes.
- [ ] One-VM loss passes with documented RPO/RTO.
- [ ] Job, service, durability, and trading readiness are separate.
- [ ] Rollback defaults to halted and preserves state readability.
