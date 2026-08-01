-- Safety_Halt_Requests: Immutable LOG/control — durable halt requests from authorized components
-- Owner: Authorized components (Signal job, Action Capture, platform health, operators)
-- Type: LOG (no primary key)
-- Bucket key: halt_request_id
-- Retention: safety/reconciliation window
-- Lake: encrypted 7-year audit
-- Scope: account_scope_id, portfolio_id, execution_partition_id
-- Schema version: 2
--
-- v2 (offline migration, plan B2): adds ingestion slot-scoped safety fields so
-- the Ingestion service can emit per-slot unsafe/recovered evidence consumed by
-- the Signal job. halt_request_id is the SHA-256 hex of the pipe-separated
-- tuple: manifest_fingerprint|slot_id|connection_epoch|state|reason_code.
-- DDL is applied only through the offline make ddl/reconciliation gate — never
-- at runtime.

CREATE TABLE Safety_Halt_Requests (
    halt_request_id         STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    portfolio_id            STRING,
    execution_partition_id  STRING,
    source_component        STRING      NOT NULL,
    source_instance         STRING      NOT NULL,
    reason_code             STRING      NOT NULL,
    reason_detail           STRING,
    detection_time          BIGINT      NOT NULL,
    source_epoch            BIGINT      NOT NULL,
    evidence_hash           STRING      NOT NULL,
    application_result      STRING      NOT NULL,
    applied_ts              BIGINT,
    schema_version          STRING      NOT NULL,
    -- v2 additions (plan B2 slot-scoped safety):
    slot_id                 STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    manifest_fingerprint    STRING      NOT NULL,
    assigned_token_set_hash STRING      NOT NULL,
    state                   STRING      NOT NULL,
    evidence_reference      STRING,
    contract_version        INT         NOT NULL
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'halt_request_id',
    'table.retention.days' = '30'
);
