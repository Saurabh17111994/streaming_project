-- Position_State: KV current-state — open/closed lifecycle per instrument
-- Owner: Execution Gateway (Nautilus) — sole writer; Signal job is reader
-- Type: KV (primary key on instrument_token)
-- Bucket key: instrument_token (PK prefix — per-instrument colocation with
--   Signal tables; Fluss connector requires bucket.key ⊆ primary key)
-- Retention: 7d via table.log.ttl (audit + recovery)
-- Lake: EOD Iceberg offload (like Signal_Candidates_current)
-- Scope: portfolio_id / account_scope_id
-- Schema version: 1
--
-- Why KV (2026-08-18, Option B): Signal_Candidates is immutable LOG,
-- Signal_Candidates_current is Flink's current-signal view, but the
-- lifecycle (OPEN vs CLOSED) is owned by the execution layer (Nautilus
-- confirms broker fill/exit). Fluss Position_State is the handshake:
-- Flink writes signal → Nautilus reads → broker → Nautilus UPSERTS
-- CLOSED. Flink's ActiveSignalFilter watches this table's LOG changelog
-- (via a second Fluss source) and clears its per-instrument ACTIVE
-- block only on CLOSED. No TTL — block is indefinite until CLOSED,
-- survives restarts (checkpointed + durable KV). TTL would risk duplicate
-- while broker position still open.

CREATE TABLE Position_State (
    instrument_token        BIGINT      NOT NULL,
    status                  STRING      NOT NULL,
    position_id             STRING,
    updated_ts              BIGINT      NOT NULL,
    closed_ts               BIGINT,
    closed_reason           STRING,
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
