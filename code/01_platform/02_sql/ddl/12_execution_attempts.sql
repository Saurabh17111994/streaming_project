-- Execution_Attempts: KV state — attempt lifecycle keyed by execution_attempt_id
-- Owner: Executor
-- Type: KV (primary key on execution_attempt_id)
-- Retention: active + reconciliation window
-- Lake: encrypted 7-year audit
-- Scope: execution_partition_id, account_scope_id
-- Schema version: 1

CREATE TABLE Execution_Attempts (
    execution_attempt_id    STRING      NOT NULL,
    instruction_id          STRING      NOT NULL,
    action_id               STRING,
    execution_partition_id  STRING      NOT NULL,
    request_hash            STRING      NOT NULL,
    client_order_ref        STRING      NOT NULL,
    broker_order_id         STRING,
    gate_epoch              BIGINT      NOT NULL,
    phase                   STRING      NOT NULL,
    outcome                 STRING,
    outcome_detail          STRING,
    prepared_ts             BIGINT      NOT NULL,
    submitted_ts            BIGINT,
    broker_response_summary STRING,
    retry_attempt           INT         NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (execution_attempt_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'execution_attempt_id',
    'table.retention.days' = '30'
);
