# 04 — Data Requirements

## 4.1 Authority and migration posture

Physical schemas SHALL be generated/reconciled from these logical contracts into `code/01_platform/02_sql/ddl/`. The platform is pre-production; stale schemas may be replaced as a clean break. Until DDLs and connector tests match this document, schema readiness is **blocked**.

Every table records a schema version. Event tables are immutable; corrections append superseding records. State tables use source event/version checks and explicit ownership.

## 4.2 Identity model

| Identity               | Scope                                       | Required relationship                                      |
| ---------------------- | ------------------------------------------- | ---------------------------------------------------------- |
| `candidate_id`         | One detected setup/audit record             | May produce zero or one `instruction_id`                   |
| `instruction_id`       | One immutable execution request             | May have multiple attempts; executable fields never mutate |
| `execution_attempt_id` | One submission attempt                      | Belongs to one instruction or position action              |
| `client_order_ref`     | Broker-facing attempt reference             | Maps durably to attempt/instruction                        |
| `broker_order_id`      | Broker-authoritative order                  | Maps to attempt/instruction when correlated                |
| `trade_context_id`     | Entry and related position-management chain | Groups one or more broker orders/positions                 |
| `position_id`          | One exposure aggregate                      | Minted on first correlated fill                            |
| `postback_event_id`    | One platform-captured delivery              | May be logically duplicate by fingerprint                  |
| `action_id`            | One immutable structured position action    | Future phase; maps to attempts like an instruction         |

An overloaded `order_id` is prohibited.

## 4.3 Table ownership and retention

| Table                       | Type           | Writer               | Live retention                                            | Lake/audit                             |
| --------------------------- | -------------- | -------------------- | --------------------------------------------------------- | -------------------------------------- |
| `raw_table_1`               | LOG            | Ingestion            | ≥3 complete trading days; extend while offload unverified | EOD Iceberg                            |
| `feature_candles_15s`       | LOG            | Signal job           | ≥3 complete trading days; extend while offload unverified | EOD Iceberg                            |
| `Signal_Candidates`         | LOG            | Signal job           | ≥3 complete trading days                                  | EOD Iceberg                            |
| `Ranking_Results`           | LOG            | Signal job           | ≥3 complete trading days                                  | EOD Iceberg                            |
| `Trade_Decisions`           | immutable feed | Signal job           | Until consumed plus replay/reconciliation buffer          | Execution audit links retained 7 years |
| `Fills_table`               | LOG            | Action Capture       | ≥3 complete trading days                                  | Encrypted 7-year audit                 |
| `Order_Lifecycle`           | KV             | Action Capture       | Current state plus rebuild window                         | Rebuilt from audit                     |
| `Positions`                 | KV             | Position projector   | Current state plus rebuild window                         | Rebuilt from audit                     |
| `Execution_Gate`            | KV             | Executor             | Current plus history in audit                             | Encrypted 7-year audit                 |
| `Execution_Attempts`        | KV             | Executor             | Active/reconciliation window                              | Encrypted 7-year audit                 |
| `Order_Correlation`         | KV             | Executor             | Active/reconciliation window                              | Encrypted 7-year audit                 |
| `Execution_Audit`           | LOG            | Executor             | ≥3 complete trading days                                  | Encrypted 7-year audit                 |
| `Position_Actions`          | LOG            | Babysitter after MVP | Replay/reconciliation buffer                              | Encrypted 7-year audit                 |
| `Postback_Quarantine`       | LOG            | Action Capture       | Until disposition plus buffer                             | Encrypted evidence per policy          |
| `suspected_discontinuities` | LOG            | Ingestion            | Operational investigation window                          | Optional operational lake retention    |
| `instruments`               | manifest       | Operators            | Current and prior manifest versions                       | Configuration audit                    |

## 4.4 Core logical schemas

### `raw_table_1`

Required fields: event/ingest/ack timestamps, instrument/routing data, verified typed trade/depth data, `connection_id`, `connection_epoch`, `event_fingerprint`, `fingerprint_version`, original packet bytes, payload hash, decoder/protocol version, validity state/reason, and schema version. Broker sequence is not required.

### `feature_candles_15s`

Required fields: instrument, window start/end, OHLCV, tick count, algorithm/configuration version, output timestamp, and schema version. One final row per non-empty accepted window; no MVP correction rows.

### `Signal_Candidates`

Required fields: `candidate_id`, nullable `instruction_id`, nullable `trade_context_id`, instrument, strategy/rule/version, event/evaluation timestamps, action/request fields, score inputs, formation snapshot reference, validity/detection reason, supersession relation, and schema version.

### `Ranking_Results`

Required fields: evaluation ID, candidate/instruction identity, ranking model/configuration version, normalized score inputs/weights/result, rank, selected flag, rejection reason, reservation snapshot/version, timestamp, and schema version.

### `Trade_Decisions`

Required fields: immutable `instruction_id`, `candidate_id`, `trade_context_id`, instrument/symbol/exchange, side, quantity, order/product type, optional limit price, strategy/rule/configuration version, score/evaluation identity, reservation ID/version, creation/expiry timestamps, supersedes relation, and schema version. Execution-owned status does not mutate this record.

### `Fills_table`

Required fields: `postback_event_id`, fingerprint/version, correlated IDs when available, broker status, verified cumulative/pending/fill quantities and prices, broker/receive/ingest timestamps, original payload and hash, correlation state/reason, decoder/schema version. Duplicate deliveries may exist as separate immutable events.

### `Order_Lifecycle`

Key: `broker_order_id`. Fields: correlated IDs, normalized state, cumulative/pending quantities, average fill price, source event/version/timestamps, correlation state, and schema version. Stale/regressive updates are rejected or move state to `UNKNOWN`.

### `Positions`

Key: `position_id`. Fields: `trade_context_id`, instrument, side, open/closed/current quantities, average entry/exit values, state (`FLAT`, `OPEN`, `REDUCING`, `CLOSED`, `UNKNOWN`), source event/version, timestamps, and schema version.

### Execution state

- `Execution_Gate`: scope key, state, epoch, reason, detection time, evidence hash, two distinct approvals, transition timestamp, schema version.
- `Execution_Attempts`: attempt/instruction/action IDs, immutable request hash, client reference, gate epoch, phase/outcome, timestamps, retry eligibility, broker response summary, schema version.
- `Order_Correlation`: instruction/attempt/client/broker/trade/position IDs, verification state/evidence, timestamps, schema version.
- `Execution_Audit`: immutable event ID/type, all relevant IDs, gate epoch, actor/service identity, evidence hash/summary, timestamp, schema version.

### Future `Position_Actions`

Required fields are the structured action contract in `02-functional/05-babysitter.md`. The table exists only when post-MVP behavior is approved; free-form command strings are prohibited.

## 4.5 Distribution and ordering

LOG tables SHOULD use a tested `bucket.key` aligned with their dominant identity (`instrument_token`, `broker_order_id`, `instruction_id`, or `position_id`). KV tables distribute by primary key. Bucket counts are workload-tested configuration, not copied assumptions.

Bucket affinity does not establish global order or exact event identity. State projections enforce source-version/precedence rules.

## 4.6 EOD manifest and expiry

Each EOD commit produces a manifest with trading date, table/schema version, source range, object/row/byte counts, hashes/checksums, commit identifier, verification state, and retries. Source data for that date cannot expire until verification succeeds. Minimum live retention remains three complete trading days even after success.

## 4.7 Evolution and recovery

All state tables must be rebuildable from immutable events/audit or have a documented backup/restore contract. Schema evolution requires compatibility classification, replay test, lake synchronization, deployment order, and rollback. Seven-year audit deletion requires approved retention policy and immutable deletion evidence.

 
