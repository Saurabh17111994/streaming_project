-- raw_table_1: Immutable LOG — every accepted market tick
-- Owner: Ingestion
-- Type: LOG (no primary key)
-- Bucket key: instrument_token
-- Retention: ≤7 trading days (ceiling); extend while EOD offload unverified
-- Lake: EOD Iceberg offload
-- Scope: account_scope_id
-- Schema version: 1
-- ack_ts: nullable (R-010). Fluss LOG rows are immutable and the broker ack
-- timestamp is not known at row-build time, so a stored row can never be
-- updated with its ack time. 0 / NULL means "unknown" — consumers must not
-- use ack_ts for latency/ordering analysis.

CREATE TABLE raw_table_1 (
    event_fingerprint       STRING      NOT NULL,
    fingerprint_version     STRING      NOT NULL,
    connection_id           STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    instrument_type         STRING,
    strike_paise            BIGINT,
    expiry                  BIGINT,
    option_type             STRING,
    event_time              BIGINT      NOT NULL,
    ingest_ts               BIGINT      NOT NULL,
    ack_ts                  BIGINT      NULL, -- 0 = unknown (R-010)
    tick_type               STRING      NOT NULL,
    last_price_paise        BIGINT      NOT NULL,
    last_qty                BIGINT      NOT NULL,
    bid_price_paise         BIGINT,
    bid_qty                 BIGINT,
    ask_price_paise         BIGINT,
    ask_qty                 BIGINT,
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
    'table.retention.days' = '7'
);
