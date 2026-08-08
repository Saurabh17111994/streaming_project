-- Execution_Audit: Immutable LOG — all execution lifecycle events
-- Owner: Executor
-- Type: LOG (no primary key)
-- Bucket key: audit_event_id
-- Retention: ≥3 trading days — 5 calendar days via table.log.ttl (R-087:
--   3 calendar days could contain only 1-2 trading days over a weekend/
--   holiday; 5 calendar days always covers 3 trading days)
-- Lake: encrypted 7-year audit
-- Scope: account_scope_id, execution_partition_id
-- Schema version: 2

CREATE TABLE Execution_Audit (
    audit_event_id          STRING      NOT NULL,
    event_type              STRING      NOT NULL,
    instruction_id          STRING,
    execution_attempt_id    STRING,
    execution_partition_id  STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    gate_epoch              BIGINT      NOT NULL,
    actor_id                STRING      NOT NULL,
    evidence_hash           STRING,
    evidence_summary        STRING,
    event_ts                BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'audit_event_id',
    'table.log.ttl' = '5d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
