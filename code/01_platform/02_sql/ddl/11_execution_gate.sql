-- Execution_Gate: KV state — gate management owned by Executor
-- Owner: Executor
-- Type: KV (primary key on execution_partition_id)
-- Retention: current + history in audit
-- Lake: encrypted 7-year audit
-- Scope: execution_partition_id, account_scope_id
-- Schema version: 2

CREATE TABLE Execution_Gate (
    execution_partition_id  STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    state                   STRING      NOT NULL,
    epoch                   BIGINT      NOT NULL,
    reason                  STRING,
    detection_time          BIGINT,
    evidence_hash           STRING,
    approval_1              STRING,
    approval_2              STRING,
    transition_ts           BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (execution_partition_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'execution_partition_id',
    'table.log.ttl' = '30d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
