-- Ranking_Results — immutable append-only log of every scored candidate
-- Owner: Signal Flink job (in-operator ranking). Schema authority: storage.
--
-- Ranking runs in-operator within the signal job (DEC-004). Every candidate
-- scored per evaluation tick: winners AND losers, with full score breakdown,
-- selection decision, and rejection reason.
-- =============================================================================

CREATE TABLE Ranking_Results (
  -- identity
  evaluation_id       STRING,     -- PK-equivalent; stable ranking evaluation identity
  candidate_id        STRING,     -- joins to Signal_Candidates
  instruction_id      STRING,     -- instruction if selected; null if rejected

  -- instrument
  instrument_token    BIGINT,     -- Arrow token (bucket.key)
  exchange            STRING,
  symbol              STRING,
  strategy_id         STRING,
  strategy_version    STRING,

  -- timing
  eval_timestamp      BIGINT,     -- epoch ms UTC of evaluation boundary
  signal_timestamp    BIGINT,     -- epoch ms UTC of signal generation
  ranking_ts          BIGINT,     -- epoch ms UTC when ranking wrote this row

  -- ranking model provenance
  ranking_model_id    STRING,     -- which ranking model/configuration was used
  ranking_version     STRING,     -- ranking configuration version

  -- normalized score inputs
  volume_context_zscore DOUBLE,   -- volume surge z-score (null in MVP)
  spread_pct          DOUBLE,     -- bid-ask spread as % of price (null in MVP)
  sector              STRING,     -- sector classification (null in MVP)

  -- composite scoring
  confidence_weight   DOUBLE,     -- weight applied to confidence_score
  rr_weight           DOUBLE,     -- weight applied to risk_reward_ratio
  move_weight         DOUBLE,     -- weight applied to expected_move_pct
  composite_score     DOUBLE,     -- final weighted composite score

  -- selection
  selection_rank      BIGINT,     -- rank within evaluation (1 = best; null if not selected)
  selected            BOOLEAN,    -- promoted to Trade_Decisions?
  rejection_reason    STRING,     -- null if selected; reason if rejected

  -- portfolio state at ranking time
  reservation_id      STRING,     -- active portfolio reservation identity
  reservation_version STRING,     -- reservation version for reconciliation
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
