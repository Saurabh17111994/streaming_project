-- Execution_Audit: Immutable LOG — all execution lifecycle events
-- Owner: Executor
-- Type: LOG (no primary key)
-- Bucket key: audit_event_id
-- Retention: ≥3 complete trading days
-- Lake: encrypted 7-year audit
-- Scope: account_scope_id, execution_partition_id
-- Schema version: 1

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
    'table.retention.days' = '3'
);
