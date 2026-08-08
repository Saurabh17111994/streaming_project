-- Order_Correlation: KV lookup — maps platform IDs to broker IDs
-- Owner: Executor
-- Type: KV (primary key on instruction_id, execution_attempt_id)
-- Retention: active + reconciliation window (30 calendar days via table.log.ttl)
-- Lake: encrypted 7-year audit
-- Scope: account_scope_id (R-145: column materialized — the header declared
--   account scoping but the schema had no such column)
-- Schema version: 2
--
-- v2 (2026-08-03, review R-086): composite PK. A single instruction can be
-- retried as multiple execution attempts, each producing a distinct
-- broker_order_id; keying the correlation row only on instruction_id meant a
-- retry silently overwrote the previous attempt's correlation. The composite
-- key (instruction_id, execution_attempt_id) keeps one correlation row per
-- attempt.

CREATE TABLE Order_Correlation (
    instruction_id          STRING      NOT NULL,
    execution_attempt_id    STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    client_order_ref        STRING      NOT NULL,
    broker_order_id         STRING,
    trade_context_id        STRING,
    position_id             STRING,
    verification_state      STRING      NOT NULL,
    verification_evidence   STRING,
    correlated_ts           BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instruction_id, execution_attempt_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id,execution_attempt_id',
    'table.log.ttl' = '30d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
