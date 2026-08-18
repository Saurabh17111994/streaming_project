-- Execution_Attempts: KV state — attempt lifecycle keyed by execution_attempt_id
-- Owner: Executor
-- Type: KV (primary key on execution_attempt_id)
-- Retention: active + reconciliation window (30 calendar days via table.log.ttl)
-- Lake: encrypted immutable audit under approved policy (one-year minimum target)
-- Scope: execution_partition_id, account_scope_id (R-233: account_scope_id
--   column materialized — the header declared account scoping but the schema
--   had no such column, so account-scoped reconciliation was impossible)
-- Schema version: 2
--
-- v2 (2026-08-03, review R-234): added `terminal_ts` (set when the attempt
-- reaches a terminal phase — ACCEPTED/REJECTED/CANCELLED/UNKNOWN; the old
-- schema only recorded prepared_ts/submitted_ts) and `phase_epoch` (monotonic
-- phase version so stale phase writes from a crashed executor are detectable).

CREATE TABLE Execution_Attempts (
    execution_attempt_id    STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    instruction_id          STRING      NOT NULL,
    action_id               STRING,
    execution_partition_id  STRING      NOT NULL,
    request_hash            STRING      NOT NULL,
    client_order_ref        STRING      NOT NULL,
    broker_order_id         STRING,
    gate_epoch              BIGINT      NOT NULL,
    phase                   STRING      NOT NULL,
    phase_epoch             BIGINT      NOT NULL,
    outcome                 STRING,
    outcome_detail          STRING,
    prepared_ts             BIGINT      NOT NULL,
    submitted_ts            BIGINT,
    terminal_ts             BIGINT,
    broker_response_summary STRING,
    retry_attempt           INT         NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (execution_attempt_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'execution_attempt_id',
    'table.log.ttl' = '30d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
