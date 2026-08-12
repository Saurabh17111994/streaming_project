# Production Swarm

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready, infrastructure/version evidence blocked |
| Owner | Platform Team |
| Topology | Four VMs: three workload/HA, one observability |
| EOD controller | Named service or scheduled job owning manifest lifecycle |
| Live-money | Disabled until Phase 12 evidence passes |

### Placement model

| Node class | Required workload | Disk |
| --- | --- | --- |
| Workload VM 1 | Fluss replica/quorum, ZooKeeper ensemble member (1 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Workload VM 2 | Fluss replica/quorum, ZooKeeper ensemble member (2 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Workload VM 3 | Fluss replica/quorum, ZooKeeper ensemble member (3 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Observability VM | OpenObserve and telemetry storage/collection | 500 GB SSD |

Fluss replicas cannot co-locate, and ZooKeeper ensemble members cannot co-locate: exactly one ZooKeeper node per workload VM. All three replicas of any critical Fluss/Flink role SHALL be placed across separate workload VMs via anti-co-location constraints. The 3-node ZooKeeper ensemble (quorum 2-of-3) and 3-node Fluss LOG-table replication survive loss of any single workload VM. OpenObserve loss must not authorize orders or erase local durable audit.

The final service-to-node placement, CPU, RAM, SSD IOPS/throughput, and network bandwidth are `EVIDENCE-BLOCKED` until `PERF-PROD-60000-001` and `FAIL-VM-LOSS-60000-001` pass. Current allocations (500 GB SSD per VM) are a starting point, not a proven sizing result.

### Stack requirements

The production stack must define:

- Immutable image digests for every image.
- Exact Java/Python/Flink/Fluss/connector/protocol versions.
- 3-node ZooKeeper ensemble (`zookeeper:3.9.2`), one member per workload VM, quorum 2-of-3, durable per-node data volume, internal-only ports (2181 client, 2888/3888 peer/leader).
- Placement constraints and anti-co-location.
- Resource reservations/limits.
- Health checks and readiness dependencies.
- Restart, update, rollback, and shutdown policies.
- Encrypted overlay/TLS-protected transport mandatory for all sensitive paths (broker, Arrow REST, S3, operator control, secret delivery, money-moving/state traffic).
- N+1 resource budget: per-VM CPU, memory, network, disk, Flink slots, Fluss capacity, checkpoint bandwidth, and catch-up rate documented; post-loss validation at 60,000 ticks/s variable average baseline (3,000 instruments; 20 ticks/s/instrument average).
- Internal-only Fluss/tablet/checkpoint ports.
- Swarm secrets and least-privilege identities.
- Durable per-node Fluss volumes.
- Encrypted/versioned S3 checkpoint, Iceberg, and audit storage.
- Executor fencing and one active owner per `execution_partition_id`.

### Readiness sequence

1. Verify image digests, Swarm secrets, encrypted networks, volumes, and S3.
2. Verify ZooKeeper ensemble quorum (2-of-3), then Fluss quorum, replication, tablets, placement, and schema manifest.
3. Verify Flink control/workers (JobManager HA leader elected via ZooKeeper), checkpoint storage, and HA metadata storage.
4. Deploy Signal and Babysitter artifacts; verify running/checkpointing.
5. Verify Ingestion manifest/subscriptions and Action Capture protocol readiness.
6. Start Executor `HALTED`; verify state, mappings, continuity, Arrow REST contract, fencing, and telemetry.
7. Complete broker/order/fill/position/attempt/reservation reconciliation.
8. Require two distinct approvals for the same gate epoch/evidence hash.
9. Enable only the approved gate epoch.

A container or service becoming healthy never enables order placement.

### Bootstrap and scaling (config-driven)

The production stack is config-driven: the same stack files, environment variables, secrets, and artifacts run at any node count. Bootstrap on one production-like VM first, then scale to three by configuration and node addition, not by rewriting the stack.

| Stage | Nodes | ZooKeeper | Fluss replication | Flink HA | Status |
| --- | --- | --- | --- | --- | --- |
| Bootstrap | 1 workload VM + observability VM | Single node (no quorum) | Replication factor 1 (LOG and KV) | JobManager HA disabled | Validates config/DDL/connectors/jobs only; NOT HA evidence |
| Target | 3 workload VMs + observability VM | 3-node ensemble, quorum 2-of-3 | LOG replication ≥2, anti-co-located | `high-availability.type: zookeeper`, standby JobManagers | Production HA topology |

Scale-out steps (1 → 3 VMs): add the two workload nodes and labels, convert ZooKeeper single-node to the 3-member ensemble (update `server.X` entries and quorum config), raise Fluss LOG-table replication factor, enable Flink ZK HA with the ensemble quorum, and re-verify the readiness sequence. The single-VM bootstrap stage SHALL NOT be cited as quorum, replication, or HA evidence.

### Storage and recovery

- Fluss data uses durable per-node volumes and tested replication (LOG tables; KV tables are single-replica in Fluss 0.9.1 — durability via Flink checkpoints + Fluss remote storage + rebuild from audit).
- ZooKeeper ensemble members use durable per-node volumes; loss of one member is tolerated while quorum (2-of-3) holds.
- Flink checkpoints/savepoints use encrypted versioned S3; Flink JobManager HA metadata (`high-availability.storageDir`) uses the same encrypted S3 store, with leadership in ZooKeeper.
- Iceberg/audit storage uses encryption, versioning, and approved retention/lifecycle policy.
- Operational projections are rebuildable from immutable events/audit or a tested backup.
- Loss of any one workload VM is tested at 60,000 ticks/s variable average baseline (3,000 instruments; 20 ticks/s/instrument average).
- RPO/RTO is recorded per failure scenario; no untested global claim is made.
- Flink JobManager HA: `high-availability.type: zookeeper` with `high-availability.zookeeper.quorum` = the 3-node ensemble, `high-availability.storageDir` = encrypted S3, `high-availability.zookeeper.path.root: /flink`, per-cluster `high-availability.cluster-id`. Multiple standby JobManagers run across workload VMs; ZooKeeper elects the leader, and a standby takes over JobManager failure without a full job re-submission.
- Checkpoint restart strategy: max 3 retries at 30s pause between attempts. After 3 consecutive checkpoint failures, the job fails. Swarm restarts it from the last successful checkpoint. If no valid checkpoint exists, the job stays down → critical alert → manual savepoint restore. Deployment SHALL reject unbounded retry. [Source: `REQ-FC-008`, estimated checkpoint size ~600 MB – 1 GB; 30s timeout provides 2-5× headroom over estimated write time.]

### Security and networking

Define separate logical networks for:

```text
data ingress
storage/control
compute
execution
observability
operator control
```

Only required ingress endpoints are exposed. Service identities are least-privilege. Broker/Arrow REST/S3/OpenObserve/TLS/operator credentials rotate through Swarm secrets or approved workload identity.

### Deployment and rollback

Every change record includes:

- Artifact digests and version matrix.
- DDL/schema/state compatibility.
- Savepoint/checkpoint impact.
- Gate halt and reconciliation requirement.
- Prechecks and acceptance evidence.
- Rollback artifact and state-readability path.
- Post-deployment verification.

Any uncertain rollback returns the Executor gate to `HALTED`. Schema-breaking clean break is permitted only before live-money release with reset/replay evidence.

### Capacity acceptance

The production-like topology must prove:

- variable 60,000 ticks/s average baseline (3,000 instruments; 20 ticks/s/instrument average) for a full session with decision p99 under 100 ms.
- One workload VM loss at the per-instrument production rate.
- Data recovery under 30 seconds for accepted scenarios.
- Safe-halt under five seconds.
- Full-volume EOD verification under 30 minutes.
- No duplicate broker order in crash-window tests.

### JVM and memory configuration

Every Java container (Ingestion, Flink TaskManager, Flink JobManager) SHALL enforce:

| Config key | Required value | Enforcement |
| --- | --- | --- |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap equals 65% of the container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap must be at least 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Emit critical alert at or above 85% total container memory |

- Set an explicit container memory limit for every Java container.
- Verify at startup that the container memory limit minus maximum heap is at least 35% of the container memory limit.
- Refuse production readiness when total container memory reaches or exceeds 85% for 60 consecutive seconds.

### Concrete sizing (48 GB VM)

Derived for a Flink TaskManager on a 48 GB VM. All numbers are starting points — superseded by `PERF-PROD-60000-001`.

| Resource | Value | Notes |
| --- | --- | --- |
| Container memory limit | 48 GB | Explicit Swarm limit |
| Java max heap (`-Xmx`) | **8 GB** | Modest — working state lives in RocksDB (direct memory) |
| Direct memory (`-XX:MaxDirectMemorySize`) | **30 GB** | RocksDB block cache + Flink network buffers |
| OS reserve | **~10 GB** | OS page cache, off-heap, Fluss client |
| GC | `-XX:+UseG1GC -XX:MaxGCPauseMillis=20` | Protect p99 <100 ms decision SLO |
| Container memory alert at 85% | ~40.8 GB | Critical alert when hit for 60 consecutive seconds |

For non-Flink containers (Ingestion, Action Capture, Executor), use the generic 65%/35% formula above. The Flink TaskManager split is different because RocksDB uses direct memory for its block cache and SST buffers. Source: dedup state budget ~1 GB, window + candidate + ranking state <10 MB, leaving substantial headroom for RocksDB block cache, write buffers, and network memory.

### Acceptance checklist

- [ ] Production stack is separate from Compose.
- [ ] No mutable image tags or unpinned dependencies remain.
- [ ] Placement prevents Fluss replica and ZooKeeper ensemble-member co-location.
- [ ] ZooKeeper ensemble quorum 2-of-3 verified; one ZK node loss tolerated.
- [ ] Secret/identity/network tests pass.
- [ ] S3 checkpoint/lake/audit recovery passes.
- [ ] One-VM loss passes with documented RPO/RTO.
- [ ] Job, service, durability, and trading readiness are separate.
- [ ] Rollback defaults to halted and preserves state readability.

## Verification mapping

The required behavior above is verified by the canonical [Production Swarm test design](./11-testing-and-release.md#production-swarm): `SWARM-INT-001`, `SWARM-INT-002`, `SWARM-FAIL-001`, `SWARM-FAIL-002`, `SWARM-REC-001`, `PERF-NODELOSS-001`, and `SEC-NET-001` to `SEC-AUDIT-001`.
