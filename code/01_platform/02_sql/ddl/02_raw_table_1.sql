-- raw_table_1 — unified append-only log of TRADE + L1 BBO QUOTE ticks
-- Owner: ingestion (writes). Schema authority: storage.
--
-- Distribution: bucket.key = 'instrument_token' preserves per-instrument order.
-- Fingerprint-based bounded best-effort dedup (DEC-012/DEC-015). No broker
-- sequence number is assumed or required.
-- =============================================================================

CREATE TABLE raw_table_1 (
  -- identity and routing
  event_fingerprint  STRING,     -- versioned hash of (connection_id, instrument_token, event_time, tick_type, price/qty/depth fields); DEC-015
  fingerprint_version STRING,    -- algorithm version for the fingerprint above
  connection_id      STRING,     -- stable per-connection identity (survives reconnects)
  connection_epoch   BIGINT,     -- increments on each reconnect; scopes fingerprint window
  instrument_token   BIGINT,     -- Arrow instrument token (bucket.key / ordering)
  exchange           STRING,     -- NSE | NFO | MCX
  symbol             STRING,
  instrument_type    STRING,     -- EQ | IDX | OPT | FUT | COMFUT
  strike             DOUBLE,
  expiry             BIGINT,
  option_type        STRING,     -- CE | PE | null

  -- timestamps
  event_time         BIGINT,     -- epoch ms UTC from broker (exchange_ts)
  ingest_ts          BIGINT,     -- epoch ms UTC when write was handed to Fluss client
  ack_ts             BIGINT,     -- epoch ms UTC when Fluss acknowledged the append

  -- trade fields (populated for TRADE and QUOTE ticks)
  tick_type          STRING,     -- 'TRADE' | 'QUOTE'
  last_price         DOUBLE,
  last_qty           BIGINT,

  -- quote (L1 BBO) fields (populated for QUOTE ticks; NULL for TRADE-only)
  bid_price          DOUBLE,
  bid_qty            BIGINT,
  ask_price          DOUBLE,
  ask_qty            BIGINT,

  -- payload preservation (DEC-014)
  raw_payload        BYTES,      -- original broker packet bytes; NEVER replaced by decoded JSON
  payload_hash       STRING,     -- hash of raw_payload for integrity verification

  -- provenance
  decoder_version    STRING,     -- ingestion decoder protocol version
  protocol_version   STRING,     -- broker protocol version tag from connection handshake
  validity_state     STRING,     -- VALID | SEMANTIC_ANOMALY (price≤0, negative volume, etc.)
  validity_reason    STRING,     -- null if VALID; human-readable explanation if anomaly
  schema_version     STRING      -- version of this DDL
) WITH (
  'bucket.num' = '16',
  'bucket.key' = 'instrument_token',
  'table.log.ttl' = '3d',                    -- ≥3 trading days; extend while EOD offload unverified (DEC-018)
  'table.datalake.enabled' = 'true',
  'table.datalake.format' = 'iceberg',
  'table.datalake.freshness' = 'EOD',
  'table.datalake.auto-compaction' = 'true'
);
