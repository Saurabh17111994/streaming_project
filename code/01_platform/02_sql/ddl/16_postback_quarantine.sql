-- Postback_Quarantine: Immutable LOG — unknown/malformed/ambiguous postback events
-- Owner: Action Capture
-- Type: LOG (no primary key)
-- Bucket key: quarantine_id
-- Retention: until disposition + buffer
-- Lake: encrypted evidence per policy
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE Postback_Quarantine (
    quarantine_id           STRING      NOT NULL,
    postback_event_id       STRING,
    reason                  STRING      NOT NULL,
    original_payload        BYTES       NOT NULL,
    payload_hash            STRING      NOT NULL,
    broker_order_id         STRING,
    instruction_id          STRING,
    correlation_attempt     STRING,
    disposition             STRING      NOT NULL,
    disposition_reason      STRING,
    quarantined_ts          BIGINT      NOT NULL,
    disposition_ts          BIGINT,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'quarantine_id',
    'table.retention.days' = '7'
);
