# Alert Configuration

## Alert policy

Alerts are versioned configuration, not prose-only thresholds. Every critical alert has an owner role, acknowledgement target, affected scope, gate impact, evidence link, severity, escalation path, remediation, and closure evidence.

A missing metric, alert, or audit trail makes the corresponding safety/release gate unproven. Alert delivery failure is itself observable and cannot silently authorize orders.

## Order-safety alerts — critical

Alert and evaluate the gate for:

- Unknown broker outcome
- Duplicate-order risk or request-hash conflict
- Missing/ambiguous identity mapping
- Postback quarantine affecting an active order
- Reconciliation failure or unresolved attempt/reservation
- Changelog discontinuity
- Executor fencing/leadership loss
- Unauthorized, mismatched, or failed two-person approval
- Safe-halt latency above five seconds
- Unverifiable Executor state or security incident

Expected response: gate `HALTED`, preserve evidence, notify Execution and Operations, and start the matching runbook.

## Streaming-health alerts

Alert for:

- Signal or Babysitter job failure
- Checkpoint failure, timeout, corruption, or restore failure
- Watermark stall or excessive lag
- Sustained backpressure or bounded-memory threshold breach
- Ingestion append uncertainty or acknowledged-loss count
- Broker disconnect, authentication exhaustion, partial subscription, or protocol mismatch
- Projection backlog or state freshness breach
- Data-path recovery target breach

The Signal job or affected path becomes not ready when correctness state is uncertain. Order placement remains blocked until reconciliation.

## Storage and durability alerts

Alert for:

- Fluss replica/quorum loss or leader instability
- Disk, volume, or object-store pressure
- S3 checkpoint unavailability
- EOD manifest failure, verification failure, retry exhaustion, or insufficient expiry margin
- Source retention approaching expiry while verification is incomplete
- Failed checksum/count/range validation
- One-workload-VM recovery posture breach

Failed offload extends retention and emits a critical alert. No source day expires before verified manifest completion.

## Security alerts

Alert for:

- Credential expiry, revocation, or authentication exhaustion
- TLS/certificate failure
- Secret exposure or redaction failure
- Unauthorized gate/control operation
- Compromised service/operator identity
- Unexpected public network exposure
- Image/SBOM/vulnerability policy failure
- Audit access anomaly

Affected money-moving paths halt until credentials, access, evidence, and reconciliation are verified.

## Alert record

Each alert contains:

- `alert_id`, schema/configuration version, UTC detection timestamp
- Service/instance and bounded affected scope
- Condition and measured value
- Gate impact and current gate epoch
- Correlation, audit, checkpoint, offset, manifest, or evidence IDs
- Severity, owner, acknowledgement, escalation, and closure state

Raw packets, credentials, tokens, and unnecessary account identifiers are not included.

## Alert testing

Acceptance tests prove metric emission, alert delivery and acknowledgement, backend outage buffering, redaction, cardinality controls, safe-halt alerts, EOD expiry alerts, unauthorized approval alerts, checkpoint/replication alerts, and reconstruction of every live-money acceptance gate.

## References

- Observability requirements: `../02_requirements/02-functional/08-observability.md`
- Security requirements: `../02_requirements/03-non-functional.md` §3.6
- Runbooks: `./01-runbooks.md`
