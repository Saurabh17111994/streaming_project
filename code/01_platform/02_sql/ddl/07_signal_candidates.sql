-- Signal_Candidates — immutable append-only log of every detected signal
-- Owner: Signal Flink job (sole writer). Schema authority: storage.
--
-- Every detected setup from every strategy. Winners AND losers.
-- candidate_id is the primary stable identity. instruction_id is set when
-- the candidate is selected as a winner (DEC-007). Trade context groups
-- entry and position-management chain.
-- =============================================================================

CREATE TABLE Signal_Candidates (
  -- identity
  candidate_id        STRING,     -- PK-equivalent; stable per-detection identity
  instruction_id      STRING,     -- set when selected as winner; immutable after set
  trade_context_id    STRING,     -- groups entry + position chain (set on winner creation)

  -- instrument
  instrument_token    BIGINT,     -- Arrow token (bucket.key)
  exchange            STRING,     -- NSE | NFO | MCX
  symbol              STRING,     -- trading symbol from master contract

  -- strategy provenance
  strategy_id         STRING,     -- which strategy generated this signal
  strategy_version    STRING,     -- strategy configuration version at detection time
  rule_id             STRING,     -- which rule within the strategy triggered

  -- timing
  signal_timestamp    BIGINT,     -- epoch ms UTC when signal was generated (breakout tick event_time)
  eval_timestamp      BIGINT,     -- epoch ms UTC of the evaluation boundary (forming candle window_end)
  written_ts          BIGINT,     -- epoch ms UTC when this row was written to Fluss

  -- instruction fields (strategy-level — set by Business Logic)
  action              STRING,     -- BUY | SELL
  entry_price         DOUBLE,     -- trigger price (0 or null = MARKET)
  qty                 BIGINT,     -- position size
  pricetype           STRING,     -- MARKET | LIMIT
  product             STRING,     -- MIS | NRML | CNC

  -- strategy-level scores
  confidence_score    DOUBLE,     -- strategy's own confidence
  risk_reward_ratio   DOUBLE,     -- calculated R:R at signal time
  expected_move_pct   DOUBLE,     -- expected price move %
  signal_reason       STRING,     -- human-readable description

  -- formation snapshot
  formation_snapshot  STRING,     -- reference to the forming-bar state at detection (candle ring buffer version)

  -- validity
  validity_state      STRING,     -- VALID | SUPERSEDED
  supersedes_id       STRING,     -- candidate_id this supersedes (null if original)
  schema_version      STRING
) WITH (
  'bucket.num' = '8',
  'bucket.key' = 'instrument_token',
  'table.log.ttl' = '7d',
  'table.datalake.enabled' = 'true',
  'table.datalake.format' = 'iceberg',
  'table.datalake.freshness' = 'EOD',
  'table.datalake.auto-compaction' = 'true'
);
