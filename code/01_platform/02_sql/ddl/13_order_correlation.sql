-- Order_Correlation: KV lookup — maps platform IDs to broker IDs
-- Owner: Executor
-- Type: KV (primary key on instruction_id)
-- Retention: active + reconciliation window
-- Lake: encrypted 7-year audit
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE Order_Correlation (
    instruction_id          STRING      NOT NULL,
    execution_attempt_id    STRING      NOT NULL,
    client_order_ref        STRING      NOT NULL,
    broker_order_id         STRING,
    trade_context_id        STRING,
    position_id             STRING,
    verification_state      STRING      NOT NULL,
    verification_evidence   STRING,
    correlated_ts           BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instruction_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.retention.days' = '30'
);
