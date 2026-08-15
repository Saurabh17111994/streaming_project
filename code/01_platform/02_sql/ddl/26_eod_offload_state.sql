-- eod_offload_state: durable per-day per-table offload records for the EOD
--   controller (SCH-23; docs/08_implementation/01-foundation.md "EOD controller
--   and offload gate", docs/08_implementation/02-schema-storage.md "EOD
--   controller and offload gate")
-- Owner: EOD controller (sole writer)
-- Type: KV state table (PK record_id) — durable with restart/resume; every
--   transition goes through the validated PENDING → WRITING → COMMITTED →
--   VERIFYING → VERIFIED machine (FAILED_RETRYABLE / FAILED_MANUAL exits)
-- Bucket key: record_id. SINGLE-FIELD PK BY DESIGN: the EOD controller is a
--   plain-JVM runner driven by the raw client, which cannot upsert
--   composite-PK KV tables in Fluss 0.9.1 (iceberg key encoder — COMPAT-FLUSS-005
--   matrix). trading_date|table_name is the deterministic record_id;
--   trading_date/table_name stay queryable columns. The single-writer lease
--   row uses the reserved identity 'lease|controller' (token in source_hash,
--   lease expiry in source_offset_start, acquired time in updated_at_ms).
-- Retention: table.log.ttl = 2d bounds the changelog; the durable contract is
--   the KV current state (VERIFIED days release source expiry via
--   permitsSourceExpiry; the record is recreated per trading day)
-- Lake: none — transient operational state; the lake target holds the
--   offloaded data, not the controller's own ledger
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE eod_offload_state (
    record_id                      STRING      NOT NULL,
    trading_date                   STRING      NOT NULL,
    table_name                     STRING      NOT NULL,
    schema_version                 STRING      NOT NULL,
    source_offset_start            BIGINT      NOT NULL,
    source_offset_end              BIGINT      NOT NULL,
    row_count                      BIGINT      NOT NULL,
    byte_count                     BIGINT      NOT NULL,
    source_hash                    STRING      NOT NULL,
    target_hash                    STRING      NOT NULL,
    iceberg_snapshot_id            STRING      NOT NULL,
    state                          STRING      NOT NULL,
    retry_count                    INT         NOT NULL,
    next_retry_at_ms               BIGINT      NOT NULL,
    earliest_allowed_source_expiry_ms BIGINT   NOT NULL,
    updated_at_ms                  BIGINT      NOT NULL,
    state_schema_version           STRING      NOT NULL,
    PRIMARY KEY (record_id) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'record_id',
    'table.log.ttl' = '2d'
);
