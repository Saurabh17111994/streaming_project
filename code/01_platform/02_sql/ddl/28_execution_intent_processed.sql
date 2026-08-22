-- Execution_Intent_Processed: gateway-owned durable source-event reprocessing
--   index — one row per Execution_Intent (instruction_id) the gateway has
--   durably handed off. This replaces the process-local duplicate guard (which
--   started empty on every restart) with a durable attempt/index lookup, so a
--   replayed intent is idempotent across a gateway restart: same instruction_id
--   + same request_hash = already handed off (skip, no duplicate side effect);
--   same instruction_id + different request_hash = contract violation
--   (quarantine + halt). This is the IntentReader dedup authority referenced by
--   docs/08_implementation/19-nautilus-execution-service-implementation-plan.md
--   T2 ("durable attempt/index lookup must replace the current process-local
--   duplicate guard").
-- Owner: Action Capture
-- Type: KV (primary key on instruction_id)
-- Bucket key: instruction_id (single-field PK -> raw-client writable per the
--   COMPAT-FLUSS-005 matrix; same single-field-PK shape as Execution_Attempts /
--   trade_instruction_state)
-- Retention: table.log.ttl = 7d bounds the changelog (T8 G1/G4 7d hardening
--   2026-08-22 — was 2d; now 7d + block-delete-unverified guard: Fluss delete
--   blocked until iceberg manifest VERIFIED, else EOD controller extends;
--   critical alert); the index is rebuildable from the Execution_Intent LOG
--   replay (same replay model as the source)
-- Lake: none — transient reprocessing index, rebuildable from the source LOG;
--   the LOG twin (Execution_Intent) is the audit record
-- Scope: account_scope_id, execution_partition_id
-- Schema version: 1

CREATE TABLE Execution_Intent_Processed (
    instruction_id     STRING      NOT NULL,
    request_hash       STRING      NOT NULL,
    handed_off_ts      BIGINT      NOT NULL,
    source_log_offset  BIGINT,
    schema_version     STRING      NOT NULL,
    PRIMARY KEY (instruction_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.log.ttl' = '7d'
);
