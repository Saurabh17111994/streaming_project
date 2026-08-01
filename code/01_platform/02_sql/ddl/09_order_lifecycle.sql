-- Order_Lifecycle: KV projection — current order state keyed by broker_order_id
-- Owner: Action Capture
-- Type: KV (primary key on broker_order_id)
-- Retention: current state + rebuild window; rebuildable from Fills audit
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE Order_Lifecycle (
    broker_order_id         STRING      NOT NULL,
    instruction_id          STRING,
    execution_attempt_id    STRING,
    trade_context_id        STRING,
    normalized_state        STRING      NOT NULL,
    cumulative_qty          BIGINT      NOT NULL,
    pending_qty             BIGINT      NOT NULL,
    average_fill_price_paise BIGINT,
    source_event_id         STRING      NOT NULL,
    source_version          BIGINT      NOT NULL,
    source_event_time       BIGINT,
    last_receive_time       BIGINT      NOT NULL,
    correlation_state       STRING      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (broker_order_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'broker_order_id',
    'table.retention.days' = '7'
);
