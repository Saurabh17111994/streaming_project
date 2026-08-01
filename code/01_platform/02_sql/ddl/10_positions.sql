-- Positions: KV projection — current position aggregate keyed by position_id
-- Owner: Position projector (Action Capture, in-process)
-- Type: KV (primary key on position_id)
-- Retention: current state + rebuild window; rebuildable from Fills audit
-- Scope: account_scope_id
-- Schema version: 1

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
    current_quantity        BIGINT      NOT NULL,
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
    'table.retention.days' = '7'
);
