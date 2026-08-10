-- feature_candles_15s_current: KV current-state companion to the immutable LOG
-- Owner: Signal job
-- Type: KV (primary key on instrument_token, window_start)
-- Bucket key: instrument_token (colocates with feature_candles_15s; the Fluss
--   connector requires bucket.key to be a SUBSET of the primary key, and this
--   PK superset keeps every candle of a ticker in the same bucket as its LOG
--   twin — R-012 / CANDLE-KV-REPLAY-001)
-- Retention: ≤7 calendar days via table.log.ttl
-- Lake: EOD Iceberg offload
-- Scope: account_scope_id
-- Schema version: 2
--
-- Why KV (2026-08-10, CANDLE-KV-REPLAY-001): feature_candles_15s is an
-- immutable LOG — a restart without state restore replays the whole raw_table_1
-- backlog and appends a duplicate row per re-emitted candle (observed 2026-08-10:
-- ~550 k duplicate candle rows). Consumers of "the current candle" cannot tell
-- which row is canonical. As a KV table the storage layer enforces one row per
-- (instrument_token, window_start) and a replay upserts the same key — the
-- LOG stays as immutable evidence, this table is the idempotent projection.

CREATE TABLE feature_candles_15s_current (
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    window_start            BIGINT      NOT NULL,
    window_end              BIGINT      NOT NULL,
    open_paise              BIGINT      NOT NULL,
    high_paise              BIGINT      NOT NULL,
    low_paise               BIGINT      NOT NULL,
    close_paise             BIGINT      NOT NULL,
    volume                  BIGINT      NOT NULL,
    tick_count              INT         NOT NULL,
    algorithm_version       STRING      NOT NULL,
    configuration_version   STRING      NOT NULL,
    output_ts               BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token, window_start) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
