-- Postback_Quarantine — immutable log of postbacks that cannot be correlated
-- Owner: Action Capture (writes). Schema authority: storage.
--
-- Postbacks with missing/ambiguous/invalid broker_order_id, no matching
-- instruction, or conflicting data land here for operator investigation.
-- Quarantined postbacks do NOT enter Order_Lifecycle or Positions projections.
-- =============================================================================

CREATE TABLE Postback_Quarantine (
  quarantine_id       STRING,     -- PK-equivalent; stable quarantine event identity
  postback_event_id   STRING,     -- the Fills_table row being quarantined (null if postback wasn't fully written)
  reason              STRING,     -- MISSING_BROKER_ID | AMBIGUOUS_CORRELATION | NO_MATCHING_INSTRUCTION | CONFLICTING_STATE | MALFORMED | UNKNOWN_STATUS

  -- available broker fields
  broker_order_id     STRING,     -- null if missing
  client_order_ref    STRING,     -- null if missing or not echoed
  broker_status       STRING,
  broker_timestamp    BIGINT,

  instrument_token    BIGINT,
  exchange            STRING,
  symbol              STRING,

  -- raw evidence
  raw_payload         BYTES,      -- original broker postback bytes
  payload_hash        STRING,

  -- lifecycle
  detected_ts         BIGINT,     -- epoch ms UTC when quarantined
  status              STRING,     -- OPEN | INVESTIGATING | RESOLVED | DISMISSED
  resolution_ts       BIGINT,     -- epoch ms UTC when resolved (null while open)
  resolution_note     STRING,     -- operator notes
  operator_identity   STRING,     -- who resolved it
  schema_version      STRING
) WITH (
  'bucket.num' = '8',
  'bucket.key' = 'broker_order_id',
  'table.log.ttl' = '7d',
  'table.datalake.enabled' = 'true',
  'table.datalake.format' = 'iceberg',
  'table.datalake.freshness' = 'EOD',
  'table.datalake.auto-compaction' = 'true'
  -- Encrypted 7-year retention per policy.
);
