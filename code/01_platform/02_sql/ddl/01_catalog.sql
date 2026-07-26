-- Fluss catalog + database bootstrap
-- Run once against the Fluss cluster (via Flink SQL client / Fluss CLI).
-- Server address from FLUSS_BOOTSTRAP env var in platform/docker/.env.
--
-- Apply order:
--   01_catalog.sql (this file)
--   02_raw_table_1.sql
--   03_feature_candles_15s.sql
--   04_fills_table.sql
--   05_order_lifecycle.sql
--   06_positions.sql
--   07_signal_candidates.sql
--   08_ranking_results.sql
--   09_trade_decisions.sql
--   10_suspected_discontinuities.sql
--   11_instruments.sql
--   12_execution_gate.sql
--   13_execution_attempts.sql
--   14_order_correlation.sql
--   15_execution_audit.sql
--   16_postback_quarantine.sql
-- =============================================================================

CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = '${FLUSS_BOOTSTRAP:-fluss-coordinator:9123}'
);

USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS trading;
USE trading;
