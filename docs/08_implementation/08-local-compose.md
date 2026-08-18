# Local Compose

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready local-only runtime |
| Owner | Platform Team |
| Runtime | Docker Compose, single host |
| Prohibited use | Production HA evidence or live-money enablement |
| Acceptance criteria | `AC-PF-016` (Compose network-isolation subset); the full `REQ-PF-001`–`REQ-PF-012` / `AC-PF-001`–`AC-PF-019` family is owned by [`09-production-swarm.md`](./09-production-swarm.md) |

### Local topology

Compose may run one development instance of:

```text
ZooKeeper (single node — dev simplification; production = 3-node ensemble)
Fluss coordinator/tablet
Flink JobManager/TaskManager
Signal and Babysitter job submitter
Ingestion
Nautilus Execution Service
go-arrow bridge / Arrow REST sandbox adapter
OpenObserve
```

Local volumes and one-node services are intentional development simplifications. They cannot prove replication, one-VM tolerance, encrypted S3 recovery, or production capacity.

### Runtime contracts

- All images and dependencies use explicit development versions; no `latest` default.
- All required tables are created from the validated schema manifest or the environment is clearly marked schema-unready.
- Nautilus Execution Service starts `HALTED`.
- The go-arrow bridge is the only service with Arrow credentials or Arrow network access.
- Nautilus owns the live OMS/position state; Fluss execution tables are written only by the projection boundary.
- Broker calls point only to sandbox/simulation unless an explicit non-default test profile is selected.
- Production credentials, audit buckets, checkpoints, and endpoints are rejected.
- Services expose liveness and readiness separately.
- Job submitter installs exactly Signal and Babysitter jobs and verifies both are running/checkpointing.

The full Nautilus operating model behind these runtime contracts — service topology, boundary
contracts, identity mapping, trade flows, and the migration roadmap — is documented in
[`05-execution-core.md`](./05-execution-core.md#recommended-operating-model).

### Configuration application

The runtime must explicitly apply or mount:

- Fluss server configuration.
- Flink job/checkpoint configuration.
- Schema manifest and DDL version.
- Local test object-store/lake configuration if used.
- Service protocol/configuration versions.
- Secret references from ignored local files only.

A repository file that is not mounted or passed to a process is not effective configuration.

### Health checks

| Service | Liveness | Readiness |
| --- | --- | --- |
| ZooKeeper | Client port responds | Quorum semantics available for local profile (single-node is acceptable locally) |
| Fluss coordinator | Process/RPC responds | Metadata and quorum semantics available for local profile |
| Fluss tablet | Process/RPC responds | Required test tables readable/writable |
| Flink JobManager | REST/RPC responds | Job submission accepted |
| Flink TaskManager | Task slot responds | Required jobs have resources |
| Ingestion | Event loop responds | Manifest/subscriptions/append/clock/telemetry pass |
| Nautilus Execution Service | Event loop responds | Native engine, Fluss intent/projection path, gate, and event-store readiness pass |
| go-arrow bridge | Process responds | Sandbox connectivity and order-update stream contract pass; no live order |
| Compute submitter | Process responds | Both jobs running/checkpointing |
| Babysitter job | Job running | Input schema/offset/checkpoint pass; no-op guard active |
| Execution control | Process responds | Durable state known, gate known, never implies ENABLED |
| Arrow REST | Sandbox API responds | Contract probe passes; no live order |
| OpenObserve | API responds | Telemetry delivery or approved local degradation |

### Network and ports

Document each exposed port as one of:

```text
LOCAL_OPERATOR_ONLY
LOCAL_SERVICE_ONLY
SANDBOX_CALLBACK
```

Bind operator interfaces to localhost by default. Do not expose Fluss internal RPC, ZooKeeper client/peer ports (2181/2888/3888 — `LOCAL_SERVICE_ONLY`), Flink administrative APIs, or Arrow REST order APIs beyond the local test boundary.

### Local secret rules

- `.env` is ignored and contains only sandbox/test credentials.
- No production credentials may be accepted by the local profile.
- Secrets are not printed in startup logs or diagnostics.
- Credential absence causes readiness failure for the dependent service.
- Local secret usage does not satisfy production Swarm-secret requirements.

### Startup procedure

1. Validate local profile and reject production markers.
2. Start infrastructure.
3. Wait for health checks, not only container start.
4. Validate schema manifest and required tables.
5. Start jobs and verify actual job IDs/status/checkpoints.
6. Start data services and validate subscriptions/projections.
7. Start Nautilus Execution Service in `HALTED` and start the go-arrow bridge with sandbox-only credentials.
8. Run simulation/sandbox reconciliation before any controlled test enablement.

### JVM and memory configuration

Every Java container SHALL enforce the standard formula from the Signal job spec unless overridden for Flink TaskManagers (RocksDB workload, direct-memory-heavy):

| Config key | Required value | Enforcement |
| --- | --- | --- |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap equals 65% of the container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap must be at least 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Emit warning at or above 85% total container memory |

Local Compose uses minimal resource limits (no production sizing). The concrete numbers below are dev defaults, not production guarantees.

### Shutdown procedure

Stop new simulated money-moving calls, record gate state, drain/reconcile test attempts, checkpoint jobs where applicable, stop services, and preserve diagnostic evidence. `make clean` is destructive and must never target production storage.

### Local acceptance

- [ ] Clean startup fails safely when schema/configuration is missing.
- [ ] Both required Flink jobs are visible and healthy.
- [ ] Executor cannot place a live order under the local profile.
- [ ] Health dimensions distinguish process health, readiness, job health, and trading readiness.
- [ ] Service-to-service network access matches the documented allowlist.
- [ ] Local secrets are ignored, redacted, and sandbox-only.
- [ ] Restart preserves or explicitly resets only documented test state.

## Verification mapping

The required behavior above is verified by the canonical [Local Compose test design](./11-testing-and-release.md#local-compose): `LOCAL-INT-001` to `LOCAL-INT-003`, `LOCAL-FAIL-001`, `LOCAL-FAIL-002`, and `LOCAL-OBS-001`.
