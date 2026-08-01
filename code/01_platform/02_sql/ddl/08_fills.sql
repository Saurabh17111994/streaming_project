-- Fills: Immutable LOG — postback/fill audit from Action Capture
-- Owner: Action Capture
-- Type: LOG (no primary key)
-- Bucket key: postback_event_id
-- Retention: ≥3 complete trading days
-- Lake: encrypted 7-year audit
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE Fills (
    postback_event_id       STRING      NOT NULL,
    postback_fingerprint    STRING      NOT NULL,
    fingerprint_version     STRING      NOT NULL,
    broker_order_id         STRING,
    instruction_id          STRING,
    execution_attempt_id    STRING,
    trade_context_id        STRING,
    order_status            STRING      NOT NULL,
    cumulative_qty          BIGINT      NOT NULL,
    pending_qty             BIGINT      NOT NULL,
    fill_qty                BIGINT,
    fill_price_paise        BIGINT,
    fill_id                 STRING,
    broker_event_time       BIGINT,
    receive_time            BIGINT      NOT NULL,
    ingest_ts               BIGINT      NOT NULL,
    original_payload        BYTES       NOT NULL,
    payload_hash            STRING      NOT NULL,
    correlation_state       STRING      NOT NULL,
    correlation_reason      STRING,
    decoder_version         STRING      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'postback_event_id',
    'table.retention.days' = '3'
);
