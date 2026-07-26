-- Execution_Gate — durable order-gate state for the Executor
-- Owner: Executor (writes). Schema authority: storage.
--
-- Enforces safe-halt before every money-moving broker call (DEC-011/DEC-019).
-- Gate scope: one per trading account/order partition.
-- State machine: HALTED → RECONCILING → APPROVAL_PENDING → ENABLED → HALTED
-- =============================================================================

CREATE TABLE Execution_Gate (
  gate_scope          STRING,     -- PK; trading account or partition identity
  state               STRING,     -- HALTED | RECONCILING | APPROVAL_PENDING | ENABLED
  epoch               BIGINT,     -- increments on each halt; attempts use current epoch
  halt_reason         STRING,     -- why gate was halted (DUPLICATE_RISK | UNKNOWN_OUTCOME | CHECKPOINT_FAILURE | STALE_SIGNAL | MISSING_FILL | CHANGELOG_GAP | MANUAL)
  detected_ts         BIGINT,     -- epoch ms UTC when uncertainty was detected
  evidence_hash       STRING,     -- hash of reconciliation evidence for this halt
  first_approver      STRING,     -- identity of first authorized approver
  second_approver     STRING,     -- identity of second distinct authorized approver
  approval_ts         BIGINT,     -- epoch ms UTC when second approval was granted
  transition_ts       BIGINT,     -- epoch ms UTC of last state transition
  schema_version      STRING,

  PRIMARY KEY (gate_scope) NOT ENFORCED
) WITH (
  'bucket.num' = '4',
  'table.merge-engine' = 'partial_update',
  'table.changelog.image' = 'FULL',
  'table.log.ttl' = '7d'
  -- Encrypted 7-year audit retention in Execution_Audit.
);
