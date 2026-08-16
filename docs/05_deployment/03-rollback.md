# Rollback and Recovery

## Safety rule

Rollback is a controlled operational procedure, not an automatic image downgrade. Any uncertainty about broker outcome, state readability, schema compatibility, changelog continuity, ownership, or deployment fencing defaults the Executor gate to `HALTED`.

No rollback procedure may resume money-moving calls automatically.

## Preconditions

Before changing a production deployment:

1. Record the release, image digests, schema versions, job artifacts, checkpoint/savepoint IDs, and active configuration hashes.
2. Halt new money-moving calls.
3. Reconcile broker orders, fills, positions, execution attempts, and identity mappings. (**Reservations REMOVED 2026-08-15, CHG-005.**)
4. Capture gate epoch, evidence hash, consumer offsets, Flink job/checkpoint status, Fluss quorum, and EOD manifest status.
5. Confirm the target artifact can read the existing state, or approve a pre-production clean-break reset/replay migration.
6. Confirm operators have the rollback artifact, runbook, access, and telemetry.

## Rollback classes

### Application-only rollback

Allowed only when schemas, state serializers, connector behavior, interfaces, and external protocol versions remain compatible.

- Stop or suspend the affected job/service according to its drain policy.
- Preserve checkpoints/savepoints and immutable audit.
- Deploy the previously approved immutable artifact.
- Restore or resume from the compatible checkpoint/savepoint.
- Verify changelog continuity, identity mappings, projections, and observability.
- Keep the gate halted until reconciliation is complete.

### Schema or state rollback

A schema-breaking rollback is not a normal rolling deployment. It requires:

- Compatibility classification
- DDL and serializer analysis
- Replay and lake-schema test
- Deployment order
- Destructive-change approval where applicable
- Reset/replay plan
- Verification that no stale consumer can mutate new state

This clean-break path is permitted only before live-money release unless an explicit migration plan has been approved. If state cannot be proven readable, start from `HALTED` and rebuild projections from immutable events/audit.

### Infrastructure rollback

For Swarm or storage changes:

- Preserve persistent volumes, S3 checkpoints, manifests, and audit.
- Restore the last approved stack/configuration by immutable digest.
- Verify replica/quorum placement, network policies, secrets, health checks, and storage access.
- Do not delete volumes or checkpoints as part of an emergency rollback.
- Run the one-workload-VM and recovery checks relevant to the changed failure domain.

## Recovery procedures

### Executor uncertainty

If a call times out, disconnects, returns malformed/ambiguous data, or the Executor crashes around broker acceptance:

1. Mark the attempt `UNKNOWN`.
2. Halt new calls within five seconds.
3. Query/reconcile broker orders using approved identity evidence.
4. Reconcile fills and positions.
5. Resolve the attempt (**and, pre-2026-08-15, reservation — REMOVED CHG-005**); never submit a blind new retry.
6. Record immutable reconciliation evidence.
7. Require two distinct authenticated approvals for the same gate epoch/evidence hash.

### Normal Flink restart vs exceptional Fluss-state rebuild (DEC-038)

Normal Flink recovery is **not** a restore of the full Signal business state from the Flink checkpoint — the checkpoint never holds a second complete copy of Fluss-owned durable state:

```text
normal restart:
  compact Flink checkpoint (source offsets, watermarks, timers, in-flight windows,
                            working-cache metadata)
    → verify Fluss authoritative-state availability and compatibility
    → rehydrate only the required working state from Fluss
    → continue from the recovered source position
```

No full `raw_table_1` replay is performed on a normal restart.

**Exceptional Fluss-state rebuild** is a separate controlled procedure for when the authoritative Fluss state is missing, corrupt, incompatible, or otherwise untrustworthy:

1. Fail closed / stay degraded — never assume the Fluss state is empty.
2. Obtain approval for the controlled bounded replay plan (within the dedup TTL horizon).
3. Reconstruct the Fluss authoritative state from `raw_table_1`.
4. Verify state/schema compatibility before any resume.

This path is exceptional recovery, not automatic rollback, and never resumes money-moving calls without reconciliation and two-person approval (safety rule above).

### Flink checkpoint or job failure

- Keep the order gate halted if order correctness or changelog continuity is uncertain.
- Restore the **compact Flink checkpoint** (source offsets, watermarks, timers, in-flight windows, working-cache metadata), then **verify Fluss authoritative-state availability and compatibility** and **rehydrate the required working state from Fluss** (dedup working cache from the dedup state table; current signals/candles already Fluss-owned) — DEC-038 (2026-08-14). Flink checkpoints are not a second complete copy of the durable Signal business state.
- Verify no duplicate instruction or incomplete sink effect was introduced.
- If restore or Fluss-state verification is not proven, keep the job/deployment not ready and fail closed; rebuild/reconcile from immutable data according to the approved replay plan.

### Fluss quorum or workload VM loss

- Verify replica/quorum state and anti-co-location.
- Preserve acknowledged-loss and backlog accounting.
- Restore Flink processing from encrypted S3 checkpoints.
- Measure recovery duration, backlog, and data correctness.
- Keep order placement halted until Executor, changelog, and broker reconciliation are clean.

### EOD offload failure

- Keep source data retained.
- Retry with bounded backoff.
- Verify manifest counts, ranges, hashes/checksums, schema version, and Iceberg commit.
- Alert before expiry margin is threatened.
- Do not expire the source day while verification is incomplete.
- Maintain at least three complete trading days live.

## Verification after rollback/recovery

The deployment is not recovered until all applicable checks pass:

- Services and jobs are live and ready at the correct dimensions.
- Fluss schemas, quorum, replication, and changelog continuity are healthy.
- Checkpoint/savepoint restore is verified.
- Projections are complete or have durable retry state.
- No unknown attempts or ambiguous mappings remain without approved disposition. (**Unresolved reservations REMOVED 2026-08-15, CHG-005.**)
- Metrics, alerts, audit, and operator access work.
- EOD retention safety is intact.
- Gate remains `HALTED` until two-person approval enables the verified epoch.

## Recovery evidence

Record release/configuration digests, timeline, failure trigger, gate transitions, checkpoint IDs, offsets, broker responses, reconciliation evidence, data-loss/lateness accounting, RPO/RTO, operator approvals, and post-recovery verification. Do not claim broader HA or exactly-once behavior than the tested scenario proves.

## References

- Operational requirements: `../02_requirements/06-operational.md` §§6.4–6.7
- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md` §§REQ-PF-007–REQ-PF-009
- Executor contract: `../04_contracts/07-executor.md`
- Storage contract: `../04_contracts/02-storage.md`
