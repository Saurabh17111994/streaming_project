# Deployment Environments

## Environment matrix

| Environment | Orchestrator | Failure domain | State/storage | Purpose | Money-moving calls |
| --- | --- | --- | --- | --- | --- |
| Local development | Docker Compose, one host | Single host | Named volumes and local test storage | Fast development and debugging | Disabled by default; simulation only |
| Integration/acceptance | Compose or production-like harness | Controlled test failures | Test Fluss, encrypted test S3 | Contract, compatibility, workload, and recovery evidence | Sandbox/simulated only |
| Production | Docker Swarm, four VMs | Three workload VMs + observability VM | Replicated Fluss, encrypted S3, durable audit | Live platform | Blocked until release gates pass |

Environment definitions must remain separate. Local Compose settings must never be used as evidence for production HA, security, durability, or capacity.

## Production placement

### Workload VMs

The three workload VMs host:

- Fluss coordinator/tablet capacity and three-node replication/quorum
- Flink JobManager/TaskManager workload capacity according to the proven placement plan
- Ingestion, Action Capture, Executor, and job deployment control as assigned by the Swarm stack

Fluss replicas cannot co-locate on one workload VM. The placement plan must specify resources, update order, restart policy, shutdown grace, health checks, and persistent volume ownership.

### Observability VM

The dedicated observability VM hosts OpenObserve and related telemetry storage/collection. Its loss must not authorize orders, erase local durable execution audit, or make the order path appear healthy. Telemetry loss is itself observable and can block live-money readiness.

## Shared dependencies

Production readiness requires:

- Encrypted, versioned S3 for Flink checkpoints/savepoints and Iceberg/audit storage
- Docker Swarm secrets and least-privilege identities
- Pinned immutable image digests and exact dependency versions
- Version-reconciled Fluss schemas and connector configuration
- Broker/OpenAlgo protocol evidence and valid credentials
- Fluss quorum/replication health
- Signal and Babysitter jobs running and checkpointing
- Executor durable state, fencing, changelog continuity, and known gate state
- OpenObserve delivery or an approved durable telemetry degradation mode

## Readiness dimensions

A deployment reports separately:

- **Liveness:** process/event loop responds.
- **Readiness:** mandatory dependencies and data flow are available.
- **Job health:** required Flink jobs are running and checkpointing.
- **Trading readiness:** Executor gate is `ENABLED`, state is known, and reconciliation is clean.
- **Durability readiness:** replication, checkpoints, offload, audit retention, and recovery posture pass.

A healthy container is not sufficient for any higher readiness state.

## Local Compose expectations

Local Compose may include one Fluss coordinator/tablet, Flink control/workers, ingestion, Action Capture, Executor, OpenAlgo, OpenObserve, and job submission components. It is intended for deterministic development and sandbox testing.

Local configuration:

- Uses ignored `.env` only for development secrets.
- Uses test/sandbox broker credentials.
- Must not use production audit, checkpoint, or live-money credentials.
- May use local named volumes, which are not production durability evidence.
- Starts with the Executor gate `HALTED` unless a test explicitly controls a simulated gate.

## Production startup order

1. Validate Swarm secrets, network, S3, and durable storage access.
2. Validate Fluss quorum, replication, tablets, and required schemas.
3. Start Flink control/workers and verify checkpoint storage.
4. Deploy Signal and Babysitter jobs from pinned artifacts.
5. Verify ingestion manifest/subscriptions and Action Capture protocol readiness.
6. Start Executor with gate `HALTED`; verify durable state, mappings, continuity, OpenAlgo, and telemetry.
7. Complete reconciliation and verify all unknown attempts/reservations are resolved.
8. Obtain two distinct authenticated approvals for the same evidence hash/epoch.
9. Enable only the approved gate epoch.

Startup dependencies and health checks never automatically enable order placement.

## Failure and maintenance behavior

- Loss of any workload VM is tested at 75,000 ticks/s.
- Fluss quorum degradation, checkpoint failure, changelog discontinuity, or uncertain Executor state halts new money-moving calls.
- Broker/authentication failure makes affected services not ready and alerts operations.
- Planned maintenance begins with the gate halted, drains or reconciles attempts, checkpoints jobs, and verifies durable state.
- Forced termination creates an audit event and requires reconciliation before resumption.

## Environment acceptance

Acceptance must prove 75,000/112,500/150,000 workload profiles, one-VM loss, bounded backlog, checkpoint restore, data recovery under 30 seconds for accepted cases, safe-halt under five seconds, EOD manifest verification, security controls, and audit reconstruction.

## References

- Architecture: `../03_architecture/03-networking.md`, `../03_architecture/platform-architecture.md`
- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Operational requirements: `../02_requirements/06-operational.md`
