# 02.2 — Storage

## Purpose and authority

Fluss is the live streaming bus and operational storage layer. This requirement owns the logical schema, table ownership, distribution, retention, replication, changelog behavior, and EOD lake policy. Physical DDLs under `code/01_platform/02_sql/ddl/` SHALL be reconciled to this document before implementation.

No table definition may use an overloaded `order_id`, assume broker sequence IDs, or claim an atomic cross-table transaction without a pinned connector test.

## REQ-FLS-001: Production storage posture

Production Fluss SHALL use three-node replication/quorum across the three workload VMs, with replica placement preventing co-location of a table's replicas on one VM. Loss of any one workload VM SHALL be tested at the normal workload.

Fluss metadata, tablet data, and replication configuration SHALL be version-pinned. A topology that cannot prove one-VM tolerance is not production-ready.

## REQ-FLS-002: Logical table classes

| Class               | Role                                   | Guarantee                                                                                            |
| ------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| LOG                 | Immutable event/audit append           | At-least-once unless a specific producer/dedup test proves stronger behavior                         |
| KV                  | Current materialized operational state | Idempotent projection under versioned source events; partial-update only with tested merge semantics |
| Execution audit LOG | Money-moving evidence                  | Immutable, encrypted lake retention for seven years                                                  |

## REQ-FLS-003: Required logical tables

| Table                       | Type                                  | Owner                       | Purpose                                      |
| --------------------------- | ------------------------------------- | --------------------------- | -------------------------------------------- |
| `raw_table_1`               | LOG                                   | Ingestion                   | Original bytes plus normalized market ticks  |
| `feature_candles_15s`       | LOG                                   | Signal job                  | Final MVP candles                            |
| `Signal_Candidates`         | LOG                                   | Business Logic              | Immutable candidate audit                    |
| `Ranking_Results`           | LOG                                   | Signal job ranking operator | Immutable score/selection audit              |
| `Trade_Decisions`           | LOG or KV as proven by implementation | Signal job                  | Immutable instructions; no Executor mutation |
| `Order_Lifecycle`           | KV                                    | Action Capture              | Broker-order lifecycle projection            |
| `Positions`                 | KV                                    | Fill-derived projector      | Position lifecycle aggregate                 |
| `Fills_table`               | LOG                                   | Action Capture              | Immutable postback/fill audit                |
| `Order_Correlation`         | KV                                    | Executor                    | Three-ID and attempt mappings                |
| `Execution_Gate`            | KV                                    | Executor                    | Gate state and approvals                     |
| `Execution_Attempts`        | KV                                    | Executor                    | Attempt state and request hash               |
| `Execution_Audit`           | LOG                                   | Executor                    | Immutable execution and gate audit           |
| `Position_Actions`          | LOG                                   | Babysitter after MVP        | Structured future position actions           |
| `Postback_Quarantine`       | LOG                                   | Action Capture              | Uncorrelated/invalid postbacks               |
| `suspected_discontinuities` | LOG                                   | Ingestion                   | Non-sequence discontinuity evidence          |
| `instruments`               | LOG/KV per tested DDL                 | Operators                   | Versioned instrument manifest                |

A pre-production clean break permits replacing stale table definitions. Every table SHALL have an explicit schema-version and owner matrix.

## REQ-FLS-004: Identity and event fields

Event/audit tables SHALL use the identities applicable to their domain:

- Market events: `event_fingerprint`, `fingerprint_version`, `connection_id`, `connection_epoch`
- Strategy: `candidate_id`, `instruction_id`, `trade_context_id`
- Broker/execution: `broker_order_id`, `client_order_ref`, `execution_attempt_id`
- Position: `position_id`, `trade_context_id`
- Postbacks: `postback_event_id`, `postback_fingerprint`

`order_id` SHALL NOT be used as a generic cross-domain identity.

## REQ-FLS-005: Raw market log

`raw_table_1` SHALL contain original packet bytes, payload hash, decoder/protocol version, fingerprint/version, UTC event/ingest/ack timestamps, instrument/routing fields, and verified normalized trade/depth fields. `seq_no` is not a required column. If retained for future evidence, it is nullable observational data and cannot support guarantees.

Distribution SHALL preserve per-instrument affinity using tested Fluss bucketing. Retention is at least three complete trading days and is extended automatically while the relevant EOD manifest is unverified or retryable.

## REQ-FLS-006: Candle log

`feature_candles_15s` is append-only final MVP candle output. It contains instrument, UTC window boundaries, OHLCV, tick count, source/algorithm version, and output timestamp. Late corrections are not written in MVP. Retention is at least three complete trading days plus offload safety extension.

## REQ-FLS-007: Strategy and ranking audit

`Signal_Candidates` and `Ranking_Results` are immutable LOG tables. They include candidate/evaluation/instruction identity, strategy/rule/configuration versions, score inputs, normalized components, ranking model version, selection/rank/rejection reason, and timestamps.

Ranking is written by the Signal job operator, not a separate Ranking job. Both tables are EOD-tiered and retained in encrypted lake storage for the approved analytics/audit period.

## REQ-FLS-008: Immutable instruction feed

`Trade_Decisions` SHALL contain immutable instruction records keyed by `instruction_id` or a tested equivalent. It SHALL contain the complete execution request, `client_order_ref` only after Executor assignment if the architecture chooses a two-stage feed, reservation state/version, expiry, and correlation fields needed for intake.

Executor SHALL NOT mutate strategy fields. If execution status is published, it is a separate execution-owned projection. The requirement does not assume KV partial-update on the instruction feed unless the connector test proves immutable event ordering and replay behavior.

## REQ-FLS-009: Lifecycle, positions, and execution state

`Order_Lifecycle`, `Positions`, `Order_Correlation`, `Execution_Gate`, and `Execution_Attempts` are separate state aggregates. Partial update may be used only where columns have explicit owners and stale-update/version tests pass.

`Execution_Audit` and money-moving postback/fill audit events SHALL be immutable and lake-tiered/encrypted for seven years. Operational projections may have shorter live retention only if their source audit allows complete rebuild.

## REQ-FLS-010: Quarantine and discontinuities

Quarantine tables preserve original bytes, reason codes, schema/version, source identity, timestamps, and disposition status. They are not silently discarded and do not become executable state.

`speculated` or exact gap-range columns are prohibited unless protocol evidence proves the required sequence semantics.

## REQ-FLS-011: EOD offload and retention gate

Eligible immutable tables SHALL be offloaded to encrypted Iceberg/S3 at EOD. The offload process SHALL produce a manifest containing table/schema version, trading date, source range, row/byte counts, content hashes/checksums, and commit status.

Source retention SHALL NOT expire a trading day's data while its manifest is unverified, retryable, or under reconciliation. The minimum live buffer is three complete trading days. A failed offload extends retention and emits a critical storage alert.

## REQ-FLS-012: Schema evolution

Every schema change requires:

1. Versioned DDL and compatibility classification.
2. Additive/removed/renamed field analysis.
3. Replay and lake schema tests.
4. Deployment order and rollback plan.
5. Consumer compatibility validation.
6. Migration evidence before live-money enablement.

Pre-production clean break allows replacing stale schemas without preserving incompatible old consumers.

## REQ-FLS-013: Acceptance

Storage tests SHALL prove schema/DDL parity, three-node replication and one-VM loss, partial-update ownership, stale update rejection, immutable event behavior, checkpoint/sink recovery, quarantine rebuild, EOD manifest validation/retry, three-day retention safety, seven-year encrypted audit retrieval, and schema migration/rollback.
