-- Trade_Decisions: Immutable instruction feed — immutable Signal-owned LOG
-- Owner: Signal job (sole writer). Executor never mutates this table.
-- Type: LOG (no primary key)
-- Bucket key: instruction_id
-- Retention: until consumed + replay/reconciliation buffer
-- Lake: execution audit links retained 7 years
-- Scope: portfolio_id, account_scope_id
-- Schema version: 1

CREATE TABLE Trade_Decisions (
    instruction_id          STRING      NOT NULL,
    candidate_id            STRING      NOT NULL,
    trade_context_id        STRING      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    side                    STRING      NOT NULL,
    quantity                BIGINT      NOT NULL,
    order_type              STRING      NOT NULL,
    product_type            STRING      NOT NULL,
    limit_price_paise       BIGINT,
    portfolio_id            STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    strategy_id             STRING      NOT NULL,
    strategy_version        STRING      NOT NULL,
    configuration_version   STRING      NOT NULL,
    evaluation_id           STRING      NOT NULL,
    composite_score         DOUBLE,
    reservation_id          STRING      NOT NULL,
    reservation_version     STRING      NOT NULL,
    created_ts              BIGINT      NOT NULL,
    expiry_ts               BIGINT,
    supersedes_instruction_id STRING,
    superseded_by_instruction_id STRING,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.retention.days' = '7'
);
