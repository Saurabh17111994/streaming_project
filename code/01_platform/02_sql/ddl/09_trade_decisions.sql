-- Trade_Decisions — immutable execution feed for winning instructions
-- Owner: Signal Flink job (in-operator ranking writes; Executor reads).
-- Schema authority: storage.
--
-- IMMUTABLE after write (DEC-016). Strategy fields are never mutated by
-- the Executor or any other downstream component. Execution state lives
-- in Execution_Gate / Execution_Attempts / Order_Correlation / Execution_Audit.
-- =============================================================================

CREATE TABLE Trade_Decisions (
  -- immutable instruction identity
  instruction_id      STRING,     -- PK; stable platform instruction identity
  candidate_id        STRING,     -- joins to Signal_Candidates
  trade_context_id    STRING,     -- groups entry + position chain

  -- instrument
  instrument_token    BIGINT,
  exchange            STRING,
  symbol              STRING,

  -- strategy provenance
  strategy_id         STRING,
  strategy_version    STRING,
  rule_id             STRING,

  -- instruction definition (immutable after write)
  action              STRING,     -- BUY | SELL
  entry_price         DOUBLE,     -- 0 or null = MARKET
  qty                 BIGINT,
  pricetype           STRING,     -- MARKET | LIMIT
  product             STRING,     -- MIS | NRML | CNC

  -- ranking provenance
  evaluation_id       STRING,     -- joins to Ranking_Results
  ranking_version     STRING,
  composite_score     DOUBLE,

  -- reservation
  reservation_id      STRING,
  reservation_version STRING,

  -- lifecycle
  created_ts          BIGINT,     -- epoch ms UTC when this instruction was written
  expiry_ts           BIGINT,     -- epoch ms UTC when this instruction expires (null = no expiry)
  supersedes_id       STRING,     -- previous instruction_id this supersedes (null if original)
  schema_version      STRING,

  PRIMARY KEY (instruction_id) NOT ENFORCED
) WITH (
  'bucket.num' = '8',
  'table.log.ttl' = '7d'
  -- IMMUTABLE feed. Execution audit links retained 7 years in lake.
  -- NOT tiered to lake directly; audit trail lives in Signal_Candidates + Ranking_Results.
);
