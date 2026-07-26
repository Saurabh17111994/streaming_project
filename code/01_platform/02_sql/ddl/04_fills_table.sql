-- Fills_table — immutable append-only log of every broker postback event
-- Owner: Action Capture (writes). Schema authority: storage.
--
-- Every postback delivery is one immutable row. Duplicate deliveries create
-- separate rows with different postback_event_ids; the Order_Lifecycle
-- projection enforces state precedence.
-- =============================================================================

CREATE TABLE Fills_table (
  -- identity
  postback_event_id   STRING,     -- platform-assigned unique event identity
  event_fingerprint   STRING,     -- versioned hash of broker fields for duplicate detection
  fingerprint_version STRING,     -- fingerprint algorithm version

  -- correlated identities (available when mapped)
  broker_order_id     STRING,     -- broker-authoritative order identity
  instruction_id      STRING,     -- platform instruction that produced this order (null if unmapped)
  trade_context_id    STRING,     -- trade context grouping entry + position chain
  position_id         STRING,     -- position aggregate (null until first correlated fill)
  client_order_ref    STRING,     -- broker-facing reference echoed from order submission

  -- broker data
  status              STRING,     -- PENDING | PARTIAL | FILLED | CANCELLED | REJECTED | UNKNOWN
  filled_qty          BIGINT,     -- cumulative filled quantity per broker
  pending_qty         BIGINT,     -- pending quantity per broker
  avg_fill_price      DOUBLE,     -- volume-weighted average fill price per broker

  instrument_token    BIGINT,
  exchange            STRING,
  symbol              STRING,

  -- timestamps
  broker_timestamp    BIGINT,     -- epoch ms UTC from broker postback
  event_time          BIGINT,     -- copied from broker_timestamp
  ingest_ts           BIGINT,     -- epoch ms UTC when postback was handed to Fluss client

  -- provenance
  raw_payload         BYTES,      -- original broker postback bytes
  payload_hash        STRING,     -- hash for integrity
  correlation_state   STRING,     -- CORRELATED | UNCORRELATED | QUARANTINED | AMBIGUOUS
  correlation_reason  STRING,     -- null if CORRELATED; explanation otherwise
  decoder_version     STRING,
  schema_version      STRING
) WITH (
  'bucket.num' = '8',
  'bucket.key' = 'broker_order_id',
  'table.log.ttl' = '3d',                    -- ≥3 trading days; extend while EOD offload unverified
  'table.datalake.enabled' = 'true',
  'table.datalake.format' = 'iceberg',
  'table.datalake.freshness' = 'EOD',
  'table.datalake.auto-compaction' = 'true'
);
