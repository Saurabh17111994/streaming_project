-- Signal_Candidates_current: KV current-state companion to the immutable LOG
-- Owner: Signal job
-- Type: KV (primary key on instrument_token)
-- Bucket key: instrument_token (colocates with Signal_Candidates; the Fluss
--   connector requires bucket.key to be a SUBSET of the primary key, and this
--   single-column PK keeps every signal of a ticker in the same bucket as its
--   LOG twin — DEC-035)
-- Retention: ≤7 calendar days via table.log.ttl
-- Lake: EOD Iceberg offload
-- Scope: portfolio_id
-- Schema version: 1
--
-- Why KV (2026-08-13, DEC-035): Signal_Candidates is an immutable LOG (v3) —
-- one row per fired signal, never updated — so consumers of "the current
-- signal per instrument" cannot tell which row is current. As a KV table the
-- storage layer enforces one row per instrument_token and a supersession
-- upserts the same key — the LOG stays as immutable audit, this table is the
-- idempotent current-state projection.

CREATE TABLE Signal_Candidates_current (
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
    PRIMARY KEY (instrument_token) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
