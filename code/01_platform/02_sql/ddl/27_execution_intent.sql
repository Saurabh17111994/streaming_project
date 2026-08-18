-- Execution_Intent: immutable execution request produced from an approved signal
-- Owner: Signal job (sole writer); Nautilus/Executor never mutates this table
-- Type: LOG (append-only, no primary key)
-- Bucket key: instruction_id
-- Retention: table.log.ttl = 2d for operational replay; durable audit evidence
--   is offloaded and retained under the approved policy-controlled minimum.
-- Lake: enabled for durable replay/offload; this table is intent evidence, not
--   an order-lifecycle or position state machine.
-- Scope: account_scope_id, execution_partition_id
-- Schema version: 1

CREATE TABLE Execution_Intent (
    instruction_id             STRING      NOT NULL,
    candidate_id               STRING      NOT NULL,
    trade_context_id           STRING      NOT NULL,
    account_scope_id           STRING      NOT NULL,
    execution_partition_id     STRING      NOT NULL,
    instrument_token           BIGINT      NOT NULL,
    exchange                   STRING      NOT NULL,
    symbol                     STRING      NOT NULL,
    side                       STRING      NOT NULL,
    quantity                   BIGINT      NOT NULL,
    order_type                 STRING      NOT NULL,
    limit_price_paise          BIGINT,
    product_type               STRING      NOT NULL,
    time_in_force              STRING      NOT NULL,
    strategy_id                STRING      NOT NULL,
    strategy_version           STRING      NOT NULL,
    configuration_version      STRING      NOT NULL,
    created_ts                 BIGINT      NOT NULL,
    expiry_ts                  BIGINT,
    request_hash               STRING      NOT NULL,
    supersedes_instruction_id  STRING,
    schema_version             STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.log.ttl' = '2d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
