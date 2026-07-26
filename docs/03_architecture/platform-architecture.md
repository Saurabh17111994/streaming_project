# Docker Platform Architecture (MVP)

## Purpose and environment boundary

This file describes the Phase 4.2 runtime target. Docker Compose is for local development, integration, and deterministic tests. Production uses a separate four-VM Docker Swarm definition and requires HA, security, capacity, and recovery evidence. The Compose topology must never be presented as production proof.

## Local topology

```text
Arrow market-data stream
  → ingestion
  → Fluss raw_table_1 LOG
  → Signal Flink job
      ├─ candles, candidates, in-operator ranking
      └─ immutable Trade_Decisions
          → Executor durable gate
          → OpenAlgo
          → broker

Arrow postback stream
  → action-capture
      ├─ Fills_table LOG
      ├─ Order_Lifecycle KV
      ├─ Positions KV
      └─ Postback_Quarantine LOG
          → Babysitter Flink job (MVP zero actions)

Fluss immutable events → verified EOD Iceberg/S3
All services → OpenObserve
```

## Required deployables

| Service/deployable | Responsibility |
| --- | --- |
| Fluss coordinator/tablets | Metadata, storage, replication, changelogs, lake tiering |
| Flink control/workers | Run the Signal and Babysitter jobs |
| Ingestion | Evidence-approved market stream decode and raw append |
| Action Capture/position projector | Postback evidence, correlation, lifecycle, fill-derived positions |
| Executor | Durable gate, attempts, mappings, reconciliation, fencing, OpenAlgo calls |
| OpenAlgo | Broker REST adapter only |
| OpenObserve | Logs, metrics, traces, alerts |
| Operators/reconciliation control | Authenticated gate reconciliation and two-person approval |

The Signal job contains Compute, Business Logic, and Ranking. Ranking is not a service. Babysitter is a separate Flink job and a strict no-op in MVP.

## Required logical state

The deployment must provision or reconcile these logical tables before readiness:

- Market: `raw_table_1`, `feature_candles_15s`, `suspected_discontinuities`, `instruments`
- Strategy: `Signal_Candidates`, `Ranking_Results`, immutable `Trade_Decisions`
- Postback/position: `Fills_table`, `Order_Lifecycle`, `Positions`, `Postback_Quarantine`
- Execution: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`
- Future only: `Position_Actions`

Every table has an explicit schema version, owner, retention policy, writer/column ownership, and tested DDL. Startup does not enable order placement merely because a container is healthy.

## Production Swarm topology

Three workload VMs host Fluss replicas/quorum and Flink workload capacity with anti-co-location. A fourth dedicated observability VM hosts telemetry services and is not required for order-safety correctness.

Production requirements include:

- Immutable image digests and pinned Java/Python/Flink/Fluss/protocol versions
- Three-node Fluss replication/quorum across workload VMs
- Encrypted S3 checkpoints/savepoints
- Durable replicated Fluss volumes
- Encrypted Iceberg/audit storage
- Swarm secrets and least-privilege identities
- Encrypted overlay/TLS-protected cross-host transport where supported
- Executor single-owner fencing per account/order partition
- Explicit resource, placement, health, restart, update, rollback, and shutdown policies

## Readiness sequence

Services may start concurrently, but production readiness is dependency-driven:

1. S3 checkpoint/lake access and secrets validate.
2. Fluss quorum, replication, tablets, and required schemas are healthy.
3. Flink control/workers are healthy.
4. Signal and Babysitter jobs run and checkpoint.
5. Ingestion and Action Capture pass protocol/schema and subscription checks.
6. Executor validates durable state, mappings, changelog continuity, OpenAlgo reachability, and starts `HALTED`.
7. Reconciliation completes.
8. Two distinct authorized operators approve the same gate epoch/evidence hash before `ENABLED`.

Liveness, readiness, job health, trading readiness, and durability readiness are separate health dimensions.

## Capacity and failure gates

The production-like environment must test:

- 75,000 ticks/s for a full session with trigger-tick-to-instruction p99 below 100 ms
- 112,500 ticks/s for at least 30 minutes
- 150,000 ticks/s for at least 60 minutes
- One workload VM loss at the normal baseline
- Data-path recovery under 30 seconds for accepted scenarios
- Safe-halt under five seconds
- Full-volume EOD manifest verification under 30 minutes target
- No duplicate broker orders in crash-window injection

Evidence must report p50/p95/p99, workload, duration, UTC clock source/offset, exact versions, and restart/failure inclusion.

## Run and change controls

Local commands may create ignored `.env` secrets, start Compose, apply reconciled DDLs, and run focused tests. Production changes require a change record, prechecks, gate halted, reconciliation, compatibility proof, rollback criteria, and post-deployment verification.

Schema-breaking clean-break migrations are permitted only before live-money release and require destructive-change approval plus reset/replay evidence. Any uncertain rollback defaults to `HALTED`.

## References

- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Operational requirements: `../02_requirements/06-operational.md`
- Architecture overview: `./00-arch-overview.md`
- Build contracts: `../04_contracts/09-platform-runtime.md`, `../04_contracts/07-executor.md`
