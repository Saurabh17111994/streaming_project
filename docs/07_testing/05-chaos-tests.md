# Chaos and Disaster-Recovery Test Plan

<!-- markdownlint-disable MD013 -->

## Status

Implementation-ready chaos/DR plan. Production-like fault evidence is not yet present.

## Safety rules

Chaos tests use sandbox/simulated broker calls unless a separately approved controlled test exists. The Executor starts `HALTED`, all faults preserve evidence, and no test may bypass fencing or approval controls.

## Required fault matrix

- Ingestion process crash, connection drop, authentication expiry, partial subscription, Fluss append timeout, bounded-buffer saturation.
- Signal TaskManager/JobManager failure, checkpoint timeout/corruption, S3 unavailable, state incompatibility, sink unavailable, sustained backpressure.
- Action Capture crash after each independent write, projection backlog, Fluss unavailable, ambiguous correlation, postback storm.
- Babysitter restart, changelog gap, stale position, accidental action-enable configuration.
- Executor crash before call, during call, after broker acceptance, before durable acknowledgement, mapping write failure, changelog gap, state corruption, fencing/lease loss, split brain, audit append failure.
- Fluss coordinator/tablet/volume loss, quorum degradation, leader instability.
- One workload VM loss under 75,000 ticks/s.
- OpenAlgo timeout/malformed/ambiguous response.
- OpenObserve outage and alert-delivery failure.
- S3 checkpoint/lake outage, failed EOD manifest, retry exhaustion, expiry pressure.
- Credential revocation/rotation, TLS failure, unauthorized gate/approval/control request.

## Expected invariants

- Data-path recovery is measured and under thirty seconds only for accepted scenarios.
- Order-path uncertainty blocks calls within five seconds.
- Unknown broker outcomes never automatically retry.
- One active Executor owner per partition.
- Restart with unverifiable state is `HALTED`.
- Partial projections recover idempotently.
- Source data does not expire before verified offload.
- Durable execution audit remains reconstructable.
- Observability loss never authorizes orders.

## Evidence

Each exercise records exact versions/topology/workload, fault injection point/time, detected signals, gate state/epoch, RPO/RTO, backlog, checkpoints/offsets, reconciliation actions, recovery verification, alerts, and operator approvals.

## Pass criteria

Every critical fault produces the documented readiness/gate response, preserves evidence, recovers or remains safely halted, and passes post-recovery reconciliation. Unexplained recovery, missing telemetry, or automatic resume is failure.
