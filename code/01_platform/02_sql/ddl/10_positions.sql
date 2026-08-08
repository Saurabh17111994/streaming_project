-- Positions: KV projection — current position aggregate keyed by position_id
-- Owner: Position projector (Action Capture, in-process)
-- Type: KV (primary key on position_id)
-- Retention: current state + rebuild window (90 calendar days via table.log.ttl
--   — R-280: 7 days expired an OPEN position that simply had no fill/postback
--   for a week, deleting it from the current-state view)
-- Scope: account_scope_id
-- Schema version: 2
--
-- v2 (2026-08-03, review R-232): removed the derived `current_quantity`
-- column. It equals open_quantity - closed_quantity; persisting it as a
-- separate NOT NULL column lets any write path that updates only one of the
-- three quantity fields silently corrupt position state. Consumers derive it.

CREATE TABLE Positions (
    position_id             STRING      NOT NULL,
    trade_context_id        STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    side                    STRING      NOT NULL,
    state                   STRING      NOT NULL,
    open_quantity           BIGINT      NOT NULL,
    closed_quantity         BIGINT      NOT NULL,
    average_entry_paise     BIGINT,
    average_exit_paise      BIGINT,
    source_event_id         STRING      NOT NULL,
    source_version          BIGINT      NOT NULL,
    created_ts              BIGINT      NOT NULL,
    last_update_ts          BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (position_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'position_id',
    'table.log.ttl' = '90d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
