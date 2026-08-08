-- Signal_Candidates: KV projection — candidate detection state keyed by candidate_id
-- Owner: Signal job
-- Type: KV (primary key on candidate_id)
-- Bucket key: candidate_id
-- Retention: ≤7 calendar days via table.log.ttl
-- Lake: EOD Iceberg offload
-- Scope: portfolio_id
-- Schema version: 2
--
-- v2 (2026-08-03, review R-084): was LOG. `superseded_by_candidate_id` can
-- only be populated by UPDATING the already-appended candidate row, which an
-- immutable LOG cannot do — so the supersede chain was dead. As a KV table the
-- storage layer enforces one row per candidate_id and supersede updates land.

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
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (candidate_id) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'candidate_id',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
