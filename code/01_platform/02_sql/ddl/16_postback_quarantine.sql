-- Postback_Quarantine: Immutable LOG — unknown/malformed/ambiguous postback events
-- Owner: Action Capture
-- Type: LOG (no primary key)
-- Bucket key: quarantine_id
-- Retention: until disposition + buffer (7 calendar days via table.log.ttl —
--   R-088: table.retention.days is not a Fluss option; table.log.ttl is)
-- Lake: encrypted evidence per policy (R-146: datalake options restored — they
--   were dropped in a rewrite while the header still claimed lake storage)
-- Scope: account_scope_id
-- Schema version: 2
--
-- reason vocabulary (R-219 — restored from the pre-rewrite DDL):
--   MISSING_BROKER_ID | AMBIGUOUS_CORRELATION | NO_MATCHING_INSTRUCTION
--   | UNPARSEABLE_PAYLOAD | UNKNOWN_POSTBACK_TYPE | DUP_FINGERPRINT
-- disposition vocabulary:
--   OPEN | INVESTIGATING | RESOLVED | DISMISSED | ESCALATED

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
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
