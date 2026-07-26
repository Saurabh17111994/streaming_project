-- Order_Correlation — durable mapping between instruction, attempt, and broker identities
-- Owner: Executor (writes). Schema authority: storage.
--
-- Persists the mapping before treating an order as safely reconciled (DEC-007).
-- One instruction_id can map to multiple broker_order_ids (e.g., trim/exit/re-entry).
-- =============================================================================

CREATE TABLE Order_Correlation (
  correlation_id      STRING,     -- PK; stable correlation identity

  -- three-ID model (DEC-007)
  instruction_id      STRING,     -- platform instruction
  attempt_id          STRING,     -- execution attempt
  client_order_ref    STRING,     -- broker-facing reference
  broker_order_id     STRING,     -- broker-authoritative order identity

  -- trade grouping
  trade_context_id    STRING,     -- entry + position chain
  position_id         STRING,     -- resulting position (null until first correlated fill)

  -- verification
  verification_state  STRING,     -- PENDING | VERIFIED | FAILED
  verification_evidence STRING,   -- how correlation was confirmed

  -- timestamps
  created_ts          BIGINT,     -- epoch ms UTC when mapping was created
  verified_ts         BIGINT,     -- epoch ms UTC when mapping was verified
  schema_version      STRING,

  PRIMARY KEY (correlation_id) NOT ENFORCED
) WITH (
  'bucket.num' = '8',
  'table.merge-engine' = 'partial_update',
  'table.changelog.image' = 'FULL',
  'table.log.ttl' = '7d'
  -- Encrypted 7-year audit retention in Execution_Audit.
);
