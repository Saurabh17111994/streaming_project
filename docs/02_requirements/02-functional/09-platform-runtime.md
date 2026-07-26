# 02.9 — Platform and Runtime

## Environments

| Environment       | Orchestrator   | Purpose                                                                 |
| ----------------- | -------------- | ----------------------------------------------------------------------- |
| Local/integration | Docker Compose | Single-host development, component integration, and deterministic tests |
| Production        | Docker Swarm   | Four VMs: three workload/HA nodes plus one dedicated observability node |

Compose behavior SHALL not be presented as proof of production HA.

## REQ-PF-001: Exact versions

Production manifests SHALL pin exact immutable image versions/digests for Fluss, Flink, OpenAlgo, OpenObserve, and all project services. `latest` and version ranges are prohibited. Exact Java, Python, protocol/SDK, connector, and plugin versions are release inputs.

## REQ-PF-002: Production Swarm placement

The three workload VMs SHALL host Fluss replicas/quorum and Flink workload capacity with constraints preventing replica co-location. The observability VM SHALL not be required for order-safety correctness. Placement, resources, update order, restart policy, health checks, and rollback are explicit in the stack definition.

Loss of any one workload VM SHALL be tested at 75,000 ticks/s. The test proves replica/quorum behavior, Flink recovery from S3 checkpoints, bounded backlog, data recovery under 30 seconds, and safe order halt under five seconds.

## REQ-PF-003: Networking

Local Compose uses an isolated bridge. Production uses encrypted Swarm overlay networks or equivalent TLS-protected cross-host transport. Only explicitly required operator/UI/API endpoints are exposed through controlled ingress/firewall rules. Fluss tablet, internal RPC, checkpoint, and service ports are not publicly exposed.

## REQ-PF-004: Secrets and identity

Local development may use ignored `.env` files. Production SHALL use Docker Swarm secrets with least-privilege service identities. Secrets are not embedded in images, stack files, logs, command lines, or environment dumps.

Rotation/revocation procedures cover broker credentials, OpenAlgo keys, S3 credentials, OpenObserve credentials, TLS material, and operator identities. Rotation tests include expired/revoked credentials and confirm alerting/readiness behavior.

## REQ-PF-005: Persistent storage

- Flink checkpoints/savepoints use encrypted, versioned S3 storage.
- Fluss data uses durable volumes per workload node with tested replication/recovery.
- Iceberg/audit uses encrypted S3 with versioning/lifecycle policy compatible with seven-year audit retention.
- OpenObserve uses dedicated observability storage and its failure cannot authorize orders or erase execution audit.

Local named volumes are not production durability.

## REQ-PF-006: Service topology

Required deployables are Fluss coordinator/quorum and tablets, Flink control/workers, Ingestion, Signal/Babysitter job submitter, Action Capture/position projector, Executor, OpenAlgo, and OpenObserve. Executor supports fencing/single active owner for each account/order partition.

## REQ-PF-007: Startup and readiness

Infrastructure may start concurrently, but services become ready only after dependency-specific readiness checks. New order placement remains disabled until:

- Required schemas/versions exist.
- Fluss quorum and replication are healthy.
- Signal and Babysitter jobs are running/checkpointing.
- Executor state is durable and gate is explicitly enabled through approved process.
- Broker/OpenAlgo contracts and credentials are valid.
- Changelog continuity and observability are healthy.

Startup dependency declarations alone do not satisfy readiness.

## REQ-PF-008: Deployment and rollback

Deployments use rolling/canary policy only where state/schema compatibility is proven. Money-moving changes begin with the gate halted, reconciliation complete, and controlled enablement. Rollback must preserve schema/state readability and default to halted when uncertain.

Schema-breaking clean-break migrations are allowed only before live-money release and require destructive-change approval and reset/replay plan.

## REQ-PF-009: Capacity acceptance

The production-like Swarm environment SHALL pass:

1. 75,000 ticks/s for a full session with decision p99 <100 ms.
2. 112,500 ticks/s for at least 30 minutes.
3. 150,000 ticks/s for at least 60 minutes with bounded saturation/recovery.
4. One workload VM loss at normal load.
5. EOD full-volume manifest verification under 30 minutes target.
6. No duplicate broker order in crash-window injection.

## REQ-PF-010: Security acceptance

Tests cover network exposure, TLS, secret scanning/redaction, rotation/revocation, least privilege, unauthorized gate/resume attempts, encrypted storage, audit access, image pinning/SBOM/vulnerability policy, and recovery from a compromised credential.



> 

 
