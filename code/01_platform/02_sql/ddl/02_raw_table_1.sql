-- raw_table_1: Immutable LOG — every accepted market tick
-- Owner: Ingestion
-- Type: LOG (no primary key)
-- Bucket key: instrument_token
-- Retention: ≤7 calendar days via table.log.ttl (R-011); the "trading days"
-- ceiling in earlier headers was unverifiable — Fluss TTL is calendar-based,
-- so the honest claim is 7 calendar days. Extend once EOD offload is verified.
-- Lake: EOD Iceberg offload (R-011: datalake options restored — they were
-- dropped in a rewrite while the header still claimed offload)
-- Scope: account_scope_id
-- Schema version: 2
--
-- v2 (2026-08-03, review R-054/R-231): removed the 8 columns the ingestion
-- path never populates — quote fields (bid_price_paise/bid_qty/ask_price_paise/
-- ask_qty, hardcoded 0 by RealFlussRowConverter) and option metadata
-- (instrument_type/strike_paise/expiry/option_type, always empty/null).
-- The DDL must tell the truth: these columns return in v3 when the bridge
-- carries quote/derivative data. Column count drops 28 -> 20.

CREATE TABLE raw_table_1 (
    event_fingerprint       STRING      NOT NULL,
    fingerprint_version     STRING      NOT NULL,
    connection_id           STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    event_time              BIGINT      NOT NULL,
    ingest_ts               BIGINT      NOT NULL,
    ack_ts                  BIGINT      NULL, -- 0 = unknown (R-010)
    tick_type               STRING      NOT NULL,
    last_price_paise        BIGINT      NOT NULL,
    last_qty                BIGINT      NOT NULL,
    raw_payload             BYTES       NOT NULL,
    payload_hash            STRING      NOT NULL,
    decoder_version         STRING      NOT NULL,
    protocol_version        STRING      NOT NULL,
    validity_state          STRING      NOT NULL,
    validity_reason         STRING,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
