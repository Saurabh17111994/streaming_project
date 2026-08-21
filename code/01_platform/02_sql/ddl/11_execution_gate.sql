-- Execution_Gate: KV state — gate management owned by Executor
-- Owner: Executor
-- Type: KV (primary key on execution_partition_id)
-- Retention: current + history in audit
-- Lake: encrypted immutable audit under approved policy (one-year minimum target)
-- Scope: execution_partition_id, account_scope_id
-- Schema version: 3
--
-- v2 history: state/epoch/approvals plus evidence hash.
--
-- v3 (2026-08-20, CHG-044, T5): added the fencing representation the epoch-only
-- DDL previously lacked — explicit `owner_instance_id` (the fenced executor
-- instance that holds the partition lease), `fence_token` (monotonically
-- increasing per partition, never reused), `fence_acquired_ts` /
-- `fence_lost_ts` (acquisition and loss evidence), `lease_expires_ts`, and
-- `approved_evidence_hash` (the exact evidence hash the single-operator approval
-- covered, so an epoch change or a new evidence package invalidates the
-- approval; DEC-044 keeps `approval_1` (authenticated authorized principal
-- `saurabh`) and retains `approval_2` as optional — a second approval is not
-- required and not checked).
-- `epoch` remains the gate-generation value and is NOT a substitute for the
-- fence token: the fence token is the per-partition owner sequence that must
-- still be valid immediately before every authorized bridge command.
-- Writers: gate-transition (state/epoch/reason/detection_time/evidence_hash/
-- transition_ts), gate-fence (owner/fence/lease columns), gate-approvals
-- (approval_1/approval_2/approved_evidence_hash). See
-- ExecutionGateColumnOwnership (SCH-15).

CREATE TABLE Execution_Gate (
    execution_partition_id  STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    state                   STRING      NOT NULL,
    epoch                   BIGINT      NOT NULL,
    reason                  STRING,
    detection_time          BIGINT,
    evidence_hash           STRING,
    approval_1              STRING,
    approval_2              STRING,
    transition_ts           BIGINT      NOT NULL,
    owner_instance_id       STRING,
    fence_token             BIGINT,
    fence_acquired_ts       BIGINT,
    lease_expires_ts        BIGINT,
    fence_lost_ts           BIGINT,
    approved_evidence_hash  STRING,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (execution_partition_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'execution_partition_id',
    'table.log.ttl' = '30d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
