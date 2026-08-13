-- Order_Lifecycle: KV projection — current order state keyed by account+broker order
-- Owner: Action Capture
-- Type: KV (primary key on account_scope_id, broker_order_id)
-- Bucket key: account_scope_id, broker_order_id
-- Retention: current state + short rebuild buffer (2 calendar days);
--   rebuildable from Fills audit (Fills retains 7d; see 08_fills.sql)
-- Scope: account_scope_id (R-013: column materialized + composite PK)
-- Schema version: 2
--
-- v2 (2026-08-03, review R-013): the header declared "Scope: account_scope_id"
-- but the table had no such column and was keyed only on broker_order_id.
-- Broker-assigned order IDs are typically unique only within one brokerage
-- account; two accounts can produce the same broker_order_id and the KV
-- projection would silently overwrite one account's order state. The composite
-- key (account_scope_id, broker_order_id) makes the projection account-safe.

CREATE TABLE Order_Lifecycle (
    account_scope_id        STRING      NOT NULL,
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
    PRIMARY KEY (account_scope_id, broker_order_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'account_scope_id,broker_order_id',
    'table.log.ttl' = '2d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
