-- suspected_discontinuities: Immutable LOG — connection/subscription/heartbeat/time evidence
-- Owner: Ingestion
-- Type: LOG (no primary key)
-- Bucket key: discontinuity_id (synthetic)
-- Retention: operational investigation window
-- Lake: optional operational lake retention
-- Scope: account_scope_id
-- Schema version: 2

CREATE TABLE suspected_discontinuities (
    discontinuity_id        STRING      NOT NULL,
    source                  STRING      NOT NULL,
    reason                  STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    last_tick_ts            BIGINT,
    last_tick_fingerprint   STRING,
    last_tick_token         BIGINT,
    last_tick_exchange      STRING,
    last_tick_symbol        STRING,
    detected_ts             BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'discontinuity_id',
    'table.log.ttl' = '2d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
