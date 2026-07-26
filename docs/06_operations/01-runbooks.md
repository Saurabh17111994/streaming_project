# Operational Runbooks

## Runbook standard

Every critical runbook records:

- Trigger, severity, and affected scope
- Owner role and acknowledgement target
- Evidence to collect before intervention
- Whether the Executor gate must halt
- Bounded mitigation steps
- Recovery and reconciliation procedure
- Verification and closure evidence
- Escalation path

Commands and API paths are version-specific implementation details. Validate them against the deployed release before execution.

## Gate halt

### Triggers

Halt for unknown broker outcome, duplicate-order risk, missing/ambiguous identity, stale instruction/reservation state, changelog discontinuity, checkpoint failure affecting order correctness, missing fill, failed reconciliation, unauthorized action, security incident, or unverifiable Executor state.

### Procedure

1. Transition the affected gate scope to `HALTED` and increment/record the gate epoch.
2. Record reason, detection timestamp, affected attempts/reservations, actor/service, and evidence reference.
3. Confirm new money-moving calls stop within five seconds.
4. Preserve attempts, request hashes, mappings, responses, offsets, fills, and logs.
5. Alert the owner and begin the matching reconciliation runbook.

Existing positions may remain monitored, but no new money-moving call is permitted while halted.

## Gate reconciliation and resume

1. Reconcile broker orders and OpenAlgo responses.
2. Reconcile fills, order lifecycle, and positions.
3. Verify `instruction_id` ↔ `execution_attempt_id` ↔ `client_order_ref` ↔ `broker_order_id` mappings.
4. Resolve every `UNKNOWN` attempt and reservation through evidence-backed disposition.
5. Verify Executor fencing and durable state.
6. Verify Fluss changelog offsets/continuity.
7. Verify Signal job and checkpoint health.
8. Verify projection completion and quarantine disposition.
9. Produce an evidence hash bound to the current gate epoch.
10. Require two distinct authenticated authorized operators to approve that same hash/epoch.
11. Transition to `ENABLED` only after the second approval.

Automatic resume and approval reuse across epochs are prohibited.

## Unknown execution outcome

1. Mark the attempt `UNKNOWN`; do not create a replacement attempt.
2. Halt the affected gate scope.
3. Query the broker through the evidence-approved reconciliation method using the durable client/broker reference.
4. If acceptance is proven, persist the `broker_order_id` mapping and reconcile fills/positions.
5. If non-acceptance is proven, record terminal evidence before retry/release according to policy.
6. If neither is proven, require manual disposition and retain reservation capacity.
7. Record immutable execution audit and follow the resume runbook.

## Broker market-data disconnect

1. Capture connection identity/epoch, affected manifest, last verified event/receive time, subscription acknowledgements, clock offset, and reconnect attempts.
2. Mark affected Ingestion readiness false.
3. Reconnect using the approved backoff/authentication policy and revalidate all required subscriptions.
4. Create suspected-discontinuity evidence; do not fabricate exact sequence gaps.
5. Do not perform inline historical backfill in MVP.
6. Suppress affected trading decisions unless an explicitly approved degraded mode proves safe.
7. Verify append acknowledgements, bounded backlog, and telemetry before ready.

## Flink job or checkpoint failure

1. Identify the affected job, checkpoint, source offsets, watermark, state size, failure, and sink status.
2. Halt the order path if state continuity or instruction correctness is uncertain.
3. Restore from the tested encrypted S3 checkpoint/savepoint.
4. Verify dedup, window, forming-bar, ranking, and source state consistency.
5. Verify no duplicate immutable instruction and no unaccounted partial sink visibility.
6. If restoration cannot be proven, remain not ready and execute the approved reset/replay procedure.
7. Reconcile before any gate resume.

## Fluss quorum, tablet, or workload VM loss

1. Record failed VM/tablet, leader/replica state, quorum, disk, backlog, and acknowledged-loss counters.
2. Halt the order path if changelog, durable state, or mappings are uncertain.
3. Verify replica placement and remaining quorum before restart/reassignment.
4. Restore Flink from S3 checkpoints as required.
5. Measure backlog recovery and data correctness at the current workload.
6. Verify recovery under the accepted RTO and run reconciliation before resume.

## Postback correlation or projection failure

1. Preserve the immutable postback and payload hash.
2. Write or verify `Postback_Quarantine` for missing/ambiguous identity or invalid schema/status.
3. Halt affected new order/action flow.
4. Inspect durable projection pending/completion state.
5. Retry lifecycle and position projections idempotently using source event/version.
6. Reject regressive terminal-state updates; conflicting evidence becomes `UNKNOWN`.
7. Verify `Order_Lifecycle`, `Positions`, mappings, and reservation impact before closure.

## EOD offload failure

1. Mark the manifest unverified/retryable and emit a critical storage alert.
2. Extend or preserve Fluss retention for affected source data.
3. Capture table/schema version, date, source range, rows/bytes, checksums, commit state, S3 errors, and expiry margin.
4. Retry with approved bounded backoff.
5. Verify the Iceberg commit and manifest before marking success.
6. Do not expire the source day; maintain at least three complete trading days live.

## Observability outage

1. Confirm whether telemetry ingestion, storage, query, or alert delivery failed.
2. Preserve local structured logs and immutable execution audit.
3. Verify order safety using durable gate/attempt state, not a green dashboard.
4. If required safety evidence or alerting is unavailable, mark trading readiness false and halt according to policy.
5. Recover telemetry, replay buffered data where supported, and verify alert delivery before closure.

## Security or credential incident

1. Halt affected order flow.
2. Preserve access, gate, network, and execution evidence.
3. Revoke/rotate compromised credentials using `../05_deployment/04-secrets-rotation.md`.
4. Validate least privilege, transport, redaction, and unauthorized-control attempts.
5. Reconcile and require two-person resume.

## References

- Operational requirements: `../02_requirements/06-operational.md`
- Executor contract: `../04_contracts/07-executor.md`
- Rollback and recovery: `../05_deployment/03-rollback.md`
- Alert configuration: `./03-alert-configuration.md`
