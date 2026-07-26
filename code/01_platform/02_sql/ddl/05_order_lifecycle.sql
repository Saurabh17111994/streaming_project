-- Order_Lifecycle — Fluss KV, broker-order lifecycle projection
-- Owner: Action Capture (writes). Schema authority: storage.
--
-- Keyed by broker_order_id. State transitions use source event version checks
-- to reject stale/out-of-order updates (DEC-005/DEC-013).
-- Replaces the old Trade_management_table.
-- =============================================================================

CREATE TABLE Order_Lifecycle (
  broker_order_id     STRING,     -- PK; broker-authoritative order identity

  -- correlated identities
  instruction_id      STRING,     -- platform instruction (null until correlated)
  client_order_ref    STRING,     -- broker-facing reference from order submission
  trade_context_id    STRING,     -- groups entry + position chain

  -- lifecycle state (Action Capture column group)
  status              STRING,     -- SUBMITTING | PENDING | PARTIAL | FILLED | CANCELLED | REJECTED | UNKNOWN
  filled_qty          BIGINT,     -- cumulative filled quantity
  pending_qty         BIGINT,     -- pending quantity
  avg_fill_price      DOUBLE,     -- volume-weighted average fill price

  instrument_token    BIGINT,
  exchange            STRING,
  symbol              STRING,

  -- provenance
  source_event_id     STRING,     -- Fills_table postback_event_id of latest update
  source_version      BIGINT,     -- broker_timestamp for state precedence
  correlation_state   STRING,     -- CORRELATED | UNCORRELATED | QUARANTINED
  lifecycle_ts        BIGINT,     -- epoch ms UTC of last lifecycle update
  schema_version      STRING,

  PRIMARY KEY (broker_order_id) NOT ENFORCED
) WITH (
  'bucket.num' = '8',
  'table.merge-engine' = 'partial_update',
  'table.changelog.image' = 'FULL',
  'table.log.ttl' = '7d'
  -- Rebuildable from Fills_table audit; not tiered to lake.
);
