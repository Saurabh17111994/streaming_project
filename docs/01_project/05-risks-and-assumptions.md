# Risks and Assumptions Register

Risks and assumptions are actionable records, not passive prose. Every open item needs an owner, validation evidence, and a decision or mitigation before the affected production capability is accepted.

## Open risks

| ID | Severity | Risk | Owner | Trigger / evidence required | Mitigation or acceptance condition | Status |
| --- | --- | --- | --- | --- | --- | --- |
| RISK-001 | Critical | Arrow exposes no stable tick sequence/event ID; fingerprint dedup can collapse identical legitimate ticks or miss semantically duplicate packets. | Ingestion | Captured packet corpus and broker protocol confirmation | Quantify false-positive/false-negative rate; version the fingerprint and TTL; accept bounded best-effort semantics | Open |
| RISK-002 | Critical | Broker accepts an order but Executor crashes before recording acknowledgement. | Execution | Fault injection between REST response and durable state update | Durable attempt ledger, broker reconciliation before retry, broker idempotency key if available | Open |
| RISK-003 | Critical | The order path does not halt consistently during uncertain state. | Platform + Execution | Safe-halt chaos test | Durable gate enforced before every broker call; halt within 5 seconds; explicit resume checks | Open |
| RISK-004 | High | `client_order_ref` may not fit or be echoed reliably in Arrow's 16-character `remarks`. | Execution + Action Capture | Broker sandbox round-trip test | Collision-tested encoding plus durable `client_order_ref ↔ instruction_id ↔ broker_order_id` mapping | Open |
| RISK-005 | High | EOD offload failure races short Fluss retention. | Storage | Failed/offline S3 test spanning EOD | Multi-day raw retention buffer, verified commit manifest, retries and pre-expiry alert | Open |
| RISK-006 | High | Shared checkpoint storage is a production single point of failure. | Platform | Loss of checkpoint host/share | Redundant durable checkpoint storage or explicit reduced-HA acceptance | Open |
| RISK-007 | High | The normal production workload is 75,000 ticks/s, and lower test rates could under-size the cluster or falsely indicate readiness. | Performance | Captured full-session and market-open workload | Size for 75,000/s sustained; pass 112,500/s burst and 150,000/s stress/headroom tests | Open |
| RISK-008 | High | Fluss connector semantics may not provide atomic visibility across all LOG and KV outputs. | Compute + Storage | Version-specific connector test | Document per-table visibility and design consumers for partial visibility if cross-table atomicity is unavailable | Open |
| RISK-009 | Medium | Swarm overlay latency may violate the decision-path p99 target. | Platform | Production-like multi-VM benchmark | Placement optimization and measured p99 under load | Open |
| RISK-010 | Medium | Three Fluss/Flink nodes may not tolerate one VM loss as configured. | Platform | Per-node chaos matrix | Set replication/quorum/placement explicitly and prove one-node tolerance | Open |
| RISK-011 | Critical | Current physical DDLs encode superseded identities, sequence assumptions, ownership, and retention and cannot be safely applied. | Platform + Storage | Pinned Fluss/Flink version and schema-parity suite | Generate replacement clean-break DDLs from reconciled requirements; keep existing SQL blocked until all DDL tests pass | Open |
| RISK-012 | High | Two-person gate resume and Executor fencing may be bypassed by incomplete identity/access-control implementation. | Platform + Execution | Authorization, concurrency, and adversarial control tests | Distinct authenticated approvals bound to epoch/evidence hash; immutable audit; one fenced active Executor per account/order partition | Open |
| RISK-013 | High | Seven-year money-moving audit retention may not satisfy final legal/regulatory policy or deletion obligations. | Platform + Compliance | Legal/policy review | Encrypt, restrict and audit access; confirm retention/deletion policy before live-money release | Open |

## Assumptions requiring validation

| ID | Assumption | Owner | Validation method | Status |
| --- | --- | --- | --- | --- |
| ASM-001 | TCP preserves order within each Arrow WebSocket connection, but the feed has no usable sequence number. | Ingestion | Protocol review and capture analysis | Partially confirmed |
| ASM-002 | Arrow postbacks expose `broker_order_id`, lifecycle status, and the submitted `remarks` value. | Action Capture | Sandbox order round trip | To verify end-to-end |
| ASM-003 | The custom Arrow decoder can remain compatible with protocol evolution. | Ingestion | Golden packet corpus and schema-version tests | To verify |
| ASM-004 | Fluss `partial_update` and FULL changelog behavior match the pinned server/client version. | Storage | Integration test against pinned Fluss version | To verify |
| ASM-005 | Four VMs can sustain the normal production baseline of 75,000 ticks/s, plus 112,500/s burst load, while one HA VM is unavailable. | Platform | Production-like load plus node-loss test | To verify |
| ASM-006 | S3 `ap-south-1` can complete verified EOD offload within 30 minutes. | Storage | Full-volume offload test | To verify |
| ASM-007 | OpenAlgo exposes deterministic order-submission responses needed for reconciliation. | Execution | API and failure-mode test | To verify |
| ASM-008 | The selected Fluss version supports the required binary payload type, KV state tables, changelog behavior, replication, retention extension, and lake properties. | Storage | Pinned-version DDL and integration suite | To verify |
| ASM-009 | Docker Swarm secrets, encrypted overlay/TLS, S3 checkpoints, and three-node Fluss placement can be operated within the four-VM target. | Platform | Production-like deployment and node-loss/security tests | To verify |
| ASM-010 | Seven-year audit retention is acceptable for the applicable live-money jurisdiction and account model. | Platform + Compliance | Legal/policy review | To verify |

## Review cadence

- Critical open risks block live-money production acceptance.
- High risks require mitigation evidence or explicit risk acceptance before production.
- Update this register when evidence changes; do not duplicate stale assumptions in the charter.
- Link test evidence or issue IDs in the status field when available.
