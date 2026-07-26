-- suspected_discontinuities — operational record of potential data gaps
-- Owner: ingestion (writes). Schema authority: storage.
--
-- Replaces the old gaps table. Since broker sequence numbers are not verified
-- (DEC-012/DEC-015), the platform cannot report exact missing ranges. It reports
-- suspected discontinuities from connection drops, heartbeat gaps, exchange-time
-- jumps, or feed-health signals.
-- =============================================================================

CREATE TABLE suspected_discontinuities (
  discontinuity_id   STRING,     -- stable platform id
  connection_id      STRING,     -- connection where discontinuity was suspected
  connection_epoch   BIGINT,     -- connection epoch at detection time
  instrument_token   BIGINT,     -- Arrow token (bucket.key)
  exchange           STRING,
  symbol             STRING,

  -- evidence (no exact missing sequence range; see DEC-015)
  detection_reason   STRING,     -- DROP | HEARTBEAT_GAP | TIME_JUMP | FEED_HEALTH | RECONNECT
  last_event_ts      BIGINT,     -- epoch ms UTC of last event before suspicion
  first_event_ts     BIGINT,     -- epoch ms UTC of first event after suspicion
  event_fingerprint_before  STRING,  -- fingerprint of last event before suspicion
  event_fingerprint_after   STRING,  -- fingerprint of first event after suspicion

  -- status
  detected_ts        BIGINT,     -- epoch ms UTC when suspicion was reported
  status             STRING,     -- OPEN | INVESTIGATING | RECONCILED | IGNORED
  note               STRING,     -- free text / investigation notes
  schema_version     STRING
) WITH (
  'bucket.num' = '8',
  'bucket.key' = 'instrument_token',
  'table.log.ttl' = '7d'                    -- operational investigation window
);
