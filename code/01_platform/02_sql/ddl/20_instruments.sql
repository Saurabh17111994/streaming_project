-- instruments: Manifest table — current and prior instrument manifest versions
-- Owner: Operators
-- Type: KV (primary key on instrument_token, manifest_version)
-- Schema version: 2
--
-- v2 (2026-08-03, review R-090): composite PK. The header claimed "current AND
-- prior manifest versions" but the single-column key made it a one-row-per-
-- instrument upsert — a new manifest silently overwrote the prior version.
-- (instrument_token, manifest_version) retains every loaded manifest version.
-- Retention: current and prior manifest versions
-- Scope: account_scope_id

CREATE TABLE instruments (
    instrument_token        BIGINT      NOT NULL,
    trading_symbol          STRING      NOT NULL,
    exchange                STRING      NOT NULL,
    segment                 STRING      NOT NULL,
    instrument_type         STRING,
    lot_size                INT         NOT NULL,
    tick_size_paise         BIGINT,
    strike_paise            BIGINT,
    expiry                  BIGINT,
    option_type             STRING,
    manifest_version        INT         NOT NULL,
    is_active               BOOLEAN     NOT NULL,
    loaded_ts               BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token, manifest_version) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'instrument_token,manifest_version',
    'table.log.ttl' = '90d'
);
