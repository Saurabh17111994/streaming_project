-- Execution_Audit — immutable log of every execution, gate, and safety decision
-- Owner: Executor (writes). Schema authority: storage.
--
-- Encrypted 7-year audit trail (DEC-020). Every money-moving action, gate
-- transition, approval, reconciliation, and correlation event.
-- =============================================================================

CREATE TABLE Execution_Audit (
  audit_event_id      STRING,     -- PK-equivalent; stable audit event identity
  event_type          STRING,     -- GATE_TRANSITION | ATTEMPT_PREPARED | ORDER_SUBMITTED | ORDER_ACKNOWLEDGED | ORDER_REJECTED | OUTCOME_UNKNOWN | CORRELATION_ESTABLISHED | RECONCILIATION_STARTED | RECONCILIATION_COMPLETED | HALT_DETECTED | APPROVAL_SUBMITTED | APPROVAL_VERIFIED | POSITION_ACTION

  -- correlated identities
  instruction_id      STRING,     -- null for gate-only events
  attempt_id          STRING,     -- null for gate-only events
  broker_order_id     STRING,     -- null until correlated
  trade_context_id    STRING,
  position_id         STRING,

  gate_epoch          BIGINT,     -- gate epoch at event time
  actor_identity      STRING,     -- service or operator that produced this event
  evidence_hash       STRING,     -- hash of evidence supporting this event
  evidence_summary    STRING,     -- human-readable summary

  timestamp_utc       BIGINT,     -- epoch ms UTC
  schema_version      STRING
) WITH (
  'bucket.num' = '8',
  'bucket.key' = 'instruction_id',
  'table.log.ttl' = '7d',
  'table.datalake.enabled' = 'true',
  'table.datalake.format' = 'iceberg',
  'table.datalake.freshness' = 'EOD',
  'table.datalake.auto-compaction' = 'true'
  -- Encrypted 7-year retention in lake per DEC-020.
);
