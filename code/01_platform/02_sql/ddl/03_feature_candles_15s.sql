-- feature_candles_15s: KV current-state — one row per non-empty 15s window
--   per instrument (PK instrument_token, window_start)
-- Owner: Signal job
-- Type: KV (primary key on instrument_token, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker
--   colocation, and the Fluss connector requires bucket.key ⊆ primary key).
--   Bucket key ≠ PK + kv.format-version=2 lets the raw Fluss client encode the
--   composite PK with Fluss's default encoder (COMPAT evidence 2026-08-15) —
--   without v2 the iceberg datalake encoder requires exactly one key field and
--   raw-client upserts fail (Flink connector unaffected).
-- Retention: 7 calendar days via table.log.ttl (R-055; T8 G1/G4 7d hardening
-- 2026-08-22 — was 2d; supersedes 2026-08-16 candle-table exception which kept
-- 2d as derived/rebuildable — now 7d + block-delete-unverified guard: Fluss
-- delete blocked until iceberg manifest VERIFIED, else EOD controller extends;
-- critical alert. Fluss TTL is calendar-based; "trading days" unverifiable,
-- table.retention.days is not a Fluss option).
-- Lake: EOD Iceberg offload (R-168: datalake options restored — dropped in a
-- rewrite while the header still claimed offload)
-- Scope: account_scope_id
-- Schema version: 2 (columns unchanged; LOG -> KV conversion 2026-08-13 —
--   user requirement: candle tables are KV-only, no LOG+KV twin)

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
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token, window_start) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true',
    'table.kv.format-version' = '2'
);
