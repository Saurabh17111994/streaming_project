-- forming_bar: KV projection — current-state forming bar per instrument
-- Owner: Signal job
-- Type: KV (primary key on instrument_token)
-- Retention: current state only; rebuildable from raw_table_1 replay
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE forming_bar (
    instrument_token        BIGINT      NOT NULL,
    window_start            BIGINT      NOT NULL,
    open_paise              BIGINT      NOT NULL,
    high_paise              BIGINT      NOT NULL,
    low_paise               BIGINT      NOT NULL,
    close_paise             BIGINT      NOT NULL,
    volume                  BIGINT      NOT NULL,
    tick_count              INT         NOT NULL,
    last_event_time         BIGINT      NOT NULL,
    last_event_fingerprint  STRING,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token'
);
