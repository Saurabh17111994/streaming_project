-- Execution_Attempts — durable record of every broker submission attempt
-- Owner: Executor (writes). Schema authority: storage.
--
-- One row per submission attempt. Prepared BEFORE calling OpenAlgo/broker.
-- Unknown outcomes halt the gate and prevent blind retry (DEC-011/DEC-016).
-- =============================================================================

CREATE TABLE Execution_Attempts (
  attempt_id          STRING,     -- PK; stable attempt identity
  instruction_id      STRING,     -- instruction being executed
  action_id           STRING,     -- action being executed (future; null for instructions)
  gate_epoch          BIGINT,     -- gate epoch at attempt time; verified before call

  -- immutable request
  request_hash        STRING,     -- hash of the full order request payload
  client_order_ref    STRING,     -- broker-facing reference (≤16 chars for Arrow remarks)

  -- lifecycle
  phase               STRING,     -- SUBMITTING | SUBMITTED | UNKNOWN | ACKNOWLEDGED | REJECTED | CANCELLED
  outcome             STRING,     -- null while in-flight; filled after correlated
  retry_eligible       BOOLEAN,   -- can this attempt be retired? (false for UNKNOWN outcomes)

  -- timestamps
  prepared_ts         BIGINT,     -- epoch ms UTC when attempt was durably recorded
  call_start_ts       BIGINT,     -- epoch ms UTC when REST call began
  call_end_ts         BIGINT,     -- epoch ms UTC when REST call completed (null if crash)
  resolved_ts         BIGINT,     -- epoch ms UTC when outcome was reconciled

  -- broker response
  broker_order_id     STRING,     -- null until correlated
  broker_status       STRING,     -- null until correlated
  broker_response_summary STRING, -- human-readable summary of broker response

  -- reconciliation
  correlation_state   STRING,     -- CORRELATED | UNCORRELATED | AMBIGUOUS
  schema_version      STRING,

  PRIMARY KEY (attempt_id) NOT ENFORCED
) WITH (
  'bucket.num' = '8',
  'table.merge-engine' = 'partial_update',
  'table.changelog.image' = 'FULL',
  'table.log.ttl' = '7d'
  -- Encrypted 7-year audit retention in Execution_Audit.
);
