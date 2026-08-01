-- feature_candles_15s: Immutable LOG — one final row per non-empty 15s window
-- Owner: Signal job
-- Type: LOG (no primary key)
-- Bucket key: instrument_token
-- Retention: ≤7 trading days (ceiling); extend while EOD offload unverified
-- Lake: EOD Iceberg offload
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE feature_candles_15s (
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
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.retention.days' = '7'
);
