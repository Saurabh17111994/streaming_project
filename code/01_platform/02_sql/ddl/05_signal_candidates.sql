-- Signal_Candidates: Immutable LOG — candidate detection audit
-- Owner: Signal job
-- Type: LOG (no primary key)
-- Bucket key: instrument_token or candidate routing identity
-- Retention: ≤7 trading days
-- Lake: EOD Iceberg offload
-- Scope: portfolio_id
-- Schema version: 1

CREATE TABLE Signal_Candidates (
    candidate_id            STRING      NOT NULL,
    instruction_id          STRING,
    trade_context_id        STRING,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    strategy_id             STRING      NOT NULL,
    strategy_version        STRING      NOT NULL,
    rule_id                 STRING      NOT NULL,
    detection_ts            BIGINT      NOT NULL,
    evaluation_ts           BIGINT      NOT NULL,
    action                  STRING      NOT NULL,
    side                    STRING      NOT NULL,
    quantity                BIGINT      NOT NULL,
    order_type              STRING,
    limit_price_paise       BIGINT,
    score_inputs            STRING,
    formation_snapshot_ref  STRING,
    validity_reason         STRING,
    supersedes_candidate_id STRING,
    superseded_by_candidate_id STRING,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.retention.days' = '7'
);
