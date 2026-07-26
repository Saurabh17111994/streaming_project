-- Positions — Fluss KV, position aggregate derived from fills
-- Owner: position projector (from Action Capture). Schema authority: storage.
--
-- Keyed by position_id. Position lifecycle is a separate aggregate from order
-- lifecycle (DEC-013/DEC-017). State: FLAT → OPEN → REDUCING → CLOSED.
-- =============================================================================

CREATE TABLE Positions (
  position_id         STRING,     -- PK; minted on first correlated fill
  trade_context_id    STRING,     -- groups entry + position chain

  instrument_token    BIGINT,
  exchange            STRING,
  symbol              STRING,
  side                STRING,     -- BUY | SELL

  -- quantities
  open_qty            BIGINT,     -- total opened quantity
  closed_qty          BIGINT,     -- total closed quantity
  current_qty         BIGINT,     -- open_qty - closed_qty

  -- price
  avg_entry_price     DOUBLE,     -- volume-weighted average entry
  avg_exit_price      DOUBLE,     -- volume-weighted average exit

  -- state
  status              STRING,     -- FLAT | OPEN | REDUCING | CLOSED | UNKNOWN

  -- provenance
  source_event_id     STRING,     -- last fill event that updated this position
  source_version      BIGINT,     -- broker_timestamp for state precedence
  position_ts         BIGINT,     -- epoch ms UTC of last update
  schema_version      STRING,

  PRIMARY KEY (position_id) NOT ENFORCED
) WITH (
  'bucket.num' = '8',
  'table.merge-engine' = 'partial_update',
  'table.changelog.image' = 'FULL',
  'table.log.ttl' = '7d'
  -- Rebuildable from Fills_table audit; not tiered to lake.
);
