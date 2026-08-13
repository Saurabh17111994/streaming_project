-- Ranking_Results: Immutable LOG — per-evaluation ranking audit
-- Owner: Signal job
-- Type: LOG (no primary key)
-- Bucket key: evaluation_id (R-136 — was candidate_id, which scattered one
-- evaluation's rows across all buckets; a consumer reading per evaluation now
-- reads a single bucket)
-- Retention: ≤2 calendar days via table.log.ttl
-- Lake: EOD Iceberg offload
-- Scope: portfolio_id
-- Schema version: 2

CREATE TABLE Ranking_Results (
    evaluation_id           STRING      NOT NULL,
    candidate_id            STRING      NOT NULL,
    instruction_id          STRING,
    portfolio_id            STRING      NOT NULL,
    model_id                STRING      NOT NULL,
    configuration_version   STRING      NOT NULL,
    normalized_scores       STRING      NOT NULL,
    weight_id               STRING      NOT NULL,
    composite_score         DOUBLE      NOT NULL,
    rank                    INT         NOT NULL,
    selected                BOOLEAN     NOT NULL,
    rejection_reason        STRING,
    reservation_snapshot    STRING,
    reservation_version     STRING,
    capacity_config_hash    STRING,
    evaluation_trigger      STRING      NOT NULL,
    tie_break_data          STRING,
    evaluation_ts           BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'evaluation_id',
    'table.log.ttl' = '2d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
