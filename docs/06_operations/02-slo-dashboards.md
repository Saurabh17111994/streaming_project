# SLO Dashboards and Evidence

## Dashboard principles

OpenObserve is the target backend. Dashboard values are not release evidence unless they include the measurement boundary, workload, duration, UTC clock source/offset, exact software versions, sample count, and whether failures/restarts are included.

Dashboards separate liveness, readiness, job health, trading readiness, and durability readiness. High-cardinality instrument/order details use bounded aggregation or structured logs rather than unbounded metric labels.

## Data and ingestion dashboard

Track:

- Packet/tick and byte throughput
- Append acknowledgements and p50/p95/p99 write latency
- Active connections, subscription completeness, reconnects, and connection epochs
- Decoder/protocol versions and manifest versions
- Decode/quarantine counts by reason
- Fingerprint candidates, dedup hits, and bounded dedup state size
- Pending bytes, blocked append duration, timeouts, and acknowledged-loss count
- Clock offset and readiness
- Suspected discontinuities by reason and status

## Compute and decision dashboard

Track:

- Source throughput and lag
- Watermark and allowed-lateness behavior
- Invalid, late, and discarded-after-emission events
- Candle and forming-bar rates
- Candidate, ranking, reservation, and instruction rates
- Score validation failures, selection/rejection/churn reasons
- Trigger-tick-to-instruction latency p50/p95/p99
- Operator busy/idle time, backpressure, pending records
- Checkpoint duration, size, failure, restore count, and state corruption/recovery

Window waiting is reported separately from processing latency.

## Order safety dashboard

Track:

- Gate state and epoch
- Halt detection-to-block latency
- Attempts by phase/outcome, request hashes, and unknown outcomes
- Unresolved reservations and duplicate suppression
- Identity mappings and postback quarantines
- Reconciliation duration/result
- Changelog lag and continuity
- Executor fencing/leadership
- OpenAlgo request latency/status and broker response classification
- Two-person approvals, denials, mismatches, and unauthorized attempts

Never calculate trading readiness from process liveness alone.

## Storage, EOD, and durability dashboard

Track:

- Fluss replica/quorum/leader health
- Disk and volume pressure
- Checkpoint S3 availability and restore status
- Projection backlog and source-version freshness
- EOD rows/bytes, manifest status, retries, verification age, and expiry margin
- Live retention days and retention-extension state
- Iceberg commit/checksum verification
- One-workload-VM recovery state and backlog

## Security and operations dashboard

Track:

- Credential age, expiry, rotation, and revocation
- Authentication failures and token refresh failures
- TLS/certificate status
- Secret scanning/redaction failures
- Unauthorized gate/control attempts
- Audit access and support-bundle generation
- Container, VM, Flink job, Swarm, and OpenObserve health
- Alert acknowledgement age and escalation state

## SLO definitions

| Boundary | Target/evidence |
| --- | --- |
| Broker packet receive → raw append acknowledgement | p99 <5 ms target, evidence-gated |
| Trigger tick → immutable instruction commit | p99 <100 ms at 75,000 ticks/s |
| Instruction commit → Executor receipt | p50/p95/p99 baseline required before release threshold |
| OpenAlgo call → verified broker response | Report separately; no unverified fixed SLA |
| Failure detection → data processing resumed | <30 s target for accepted scenarios |
| Uncertainty detection → gate blocks new calls | <5 s target |
| Market close → verified EOD manifest | <30 min target at full-volume baseline |

Workload acceptance includes 75,000 ticks/s full session, 112,500 ticks/s for at least 30 minutes, and 150,000 ticks/s for at least 60 minutes.

## Evidence and retention

Each acceptance report stores dashboard/query version, metric definitions, sample count, workload profile, UTC clock evidence, software/configuration versions, failure/restart inclusion, and linked logs/audit IDs. Money-moving audit and gate/reconciliation evidence is encrypted and retained seven years; other telemetry follows approved operational retention.

## References

- Observability requirements: `../02_requirements/02-functional/08-observability.md`
- Quality targets: `../01_project/03-quality-targets.md`
- Observability contract: `../04_contracts/08-observability.md`
