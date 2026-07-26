# 03 — Non-functional Requirements

## 3.1 Performance

### NFR-PERF-001: Workload envelope

| Scenario          | Rate            | Duration                                   | Required evidence                                   |
| ----------------- | ---------------:| ------------------------------------------:| --------------------------------------------------- |
| Normal production | 75,000 ticks/s  | Full production-equivalent trading session | All latency, checkpoint, loss, and recovery SLOs    |
| Burst             | 112,500 ticks/s | ≥30 minutes                                | Bounded backlog/backpressure, no acknowledged loss  |
| Stress/headroom   | 150,000 ticks/s | ≥60 minutes                                | Saturation behavior, recovery, checkpoint stability |

Tests use the production instrument manifest, connection count, subscription mode, packet-size/type distribution, and exact software versions.

### NFR-PERF-002: Latency SLOs

| Boundary                                                           | Target                                                                    |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------:|
| Broker packet receive → raw append acknowledgement                 | p99 <5 ms target, evidence-gated against actual protocol/client           |
| Trigger tick consumed by Signal job → immutable instruction commit | p99 <100 ms at 75,000 ticks/s                                             |
| Instruction commit → Executor receipt                              | Report p50/p95/p99; release threshold set after pinned connector baseline |
| OpenAlgo call start → verified broker response                     | Report separately; no unverified fixed SLA                                |
| Failure detection → data processing resumed                        | <30 s                                                                     |
| Uncertainty detection → gate blocks new calls                      | <5 s                                                                      |
| Market close → verified EOD manifest                               | <30 min target at full-volume baseline                                    |

Every report includes p50/p95/p99, UTC clock source/offset, sample duration, failures/restarts included, workload, and exact versions. Candle window waiting is reported separately.

## 3.2 Availability and recovery

- Production SHALL tolerate loss of any one workload VM at the normal workload without violating the tested durability posture.
- Fluss uses three-node replication/quorum and anti-co-location across workload VMs.
- Flink checkpoints/savepoints use durable encrypted S3 storage.
- Data path recovers automatically when correctness state is verifiable.
- Order path never resumes automatically after uncertainty.
- Executor restart with unverifiable state defaults to `HALTED`.

RPO/RTO evidence is produced per failure scenario; broad “HA” claims without tests are prohibited.

## 3.3 Delivery and consistency

| Boundary                            | Contract                                                                        |
| ----------------------------------- | ------------------------------------------------------------------------------- |
| Broker stream → Ingestion           | Evidence-gated delivery; treated as at-least-once with possible gaps            |
| Ingestion → raw LOG                 | At-least-once; no exact raw dedup claim                                         |
| Compute dedup                       | Bounded best-effort fingerprint; false positives/negatives documented           |
| Flink managed state/sinks           | Exactly-once only where pinned integration tests prove the boundary             |
| Multiple Fluss outputs              | Partial visibility allowed unless tested transaction semantics prove otherwise  |
| Fluss instruction/action → Executor | At-least-once delivery; durable identity/request-hash guard                     |
| Executor → OpenAlgo/broker          | At-least-once or unknown; durable attempt + reconciliation prevents blind retry |
| Postback projections                | At-least-once input; idempotent/versioned materialization and reconciliation    |

## 3.4 Durability and retention

- Eligible source/audit tables retain at least three complete trading days in Fluss.
- Retention automatically extends while the relevant EOD manifest is unverified, retryable, or under reconciliation.
- Execution, gate, order, fill, correlation, approval, and position-action audit records are encrypted in lake storage and retained seven years.
- EOD commits include counts, ranges, schema versions, hashes/checksums, and verification state.
- Operational state is rebuildable from immutable audit or has a tested backup/restore contract.

## 3.5 State and safety invariants

1. One immutable `instruction_id` maps to one immutable request definition.
2. No money-moving call occurs without an `ENABLED` gate and matching current epoch.
3. Unknown broker outcome remains reserved, halts, and is reconciled before retry/release.
4. `instruction_id`, `client_order_ref`, and `broker_order_id` remain distinct and durably mapped.
5. Order lifecycle and position lifecycle are separate aggregates.
6. Older/out-of-order postbacks cannot regress authoritative lifecycle/position state.
7. No source data expires before verified EOD offload plus the minimum live buffer.
8. Two distinct authenticated operators approve every post-reconciliation resume.

## 3.6 Security and privacy

- TLS is required for broker, S3, observability, and cross-host production traffic where supported; unencrypted cross-host secrets or money-moving data are prohibited.
- Data at rest is encrypted for Fluss volumes, S3 checkpoints, lake/audit, secrets, and Executor state.
- Local development uses ignored `.env`; production uses Docker Swarm secrets.
- Least-privilege identities separate ingestion, Flink, Action Capture, Executor, observability, and operators.
- Logs/traces redact credentials, tokens, raw payloads, and unnecessary account identifiers.
- Audit access is role-restricted and itself audited.
- Credential rotation/revocation, unauthorized resume, secret exposure, and expired-token scenarios are tested and alerted.

## 3.7 Maintainability and compatibility

Exact Flink, Fluss, Java, Python, broker protocol/SDK, and OpenAlgo versions are mandatory release inputs. Upgrade requires compatibility tests, schema/serializer analysis, savepoint/restore proof, rollback plan, and updated evidence.

Every requirement, DDL, contract, test, metric, and runbook SHALL use the same identity/state vocabulary. Documentation link/schema consistency is validated in CI.

## 3.8 Observability SLO evidence

The platform SHALL expose enough metrics, logs, correlation IDs, immutable audit, alerts, and health states to prove every acceptance gate. Missing observability for an invariant makes that invariant unproven and blocks live-money release.
