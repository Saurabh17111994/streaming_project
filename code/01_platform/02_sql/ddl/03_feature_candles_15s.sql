-- feature_candles_15s — append-only log of closed 15-second candles
-- Owner: Signal Flink job (writes). Schema authority: storage.
--
-- TRADE ticks only. One row per non-empty accepted window.
-- No backfill/correction of already-emitted candles in MVP.
-- =============================================================================

CREATE TABLE feature_candles_15s (
  instrument_token   BIGINT,     -- Arrow instrument token (bucket.key)
  window_start       BIGINT,     -- epoch ms UTC; start of tumbling window
  window_end         BIGINT,     -- epoch ms UTC; end of tumbling window
  open               DOUBLE,     -- first price in window
  high               DOUBLE,     -- maximum price in window
  low                DOUBLE,     -- minimum price in window
  close              DOUBLE,     -- last price in window
  volume             BIGINT,     -- sum of TRADE volumes
  tick_count         BIGINT,     -- count of TRADE ticks aggregated
  candle_version     STRING,     -- algorithm/configuration version that produced this candle
  ingest_ts          BIGINT,     -- epoch ms UTC when row was written
  schema_version     STRING      -- version of this DDL
) WITH (
  'bucket.num' = '8',
  'bucket.key' = 'instrument_token',
  'table.log.ttl' = '3d',                    -- ≥3 trading days; extend while EOD offload unverified
  'table.datalake.enabled' = 'true',
  'table.datalake.format' = 'iceberg',
  'table.datalake.freshness' = 'EOD',
  'table.datalake.auto-compaction' = 'true'
);
