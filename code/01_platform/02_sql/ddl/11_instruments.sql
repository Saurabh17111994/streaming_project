-- instruments — instrument metadata manifest
-- Owner: operators (one-time CSV import + ad-hoc INSERTs). Schema authority: storage.
--
-- Ingestion reads this at startup to build the subscription list. LOG table:
-- metadata changes append a new row; the latest row by instrument_token wins.
-- =============================================================================

CREATE TABLE instruments (
  instrument_token    BIGINT,     -- Arrow instrument token (PK-equivalent, bucket.key)
  symbol              STRING,     -- trading symbol, e.g. RELIANCE
  exchange            STRING,     -- NSE | NFO | MCX
  instrument_type     STRING,     -- EQ | IDX | OPT | FUT | COMFUT
  strike              DOUBLE,     -- null for non-options
  expiry              BIGINT,     -- epoch ms UTC; null for non-derivatives
  option_type         STRING,     -- CE | PE | null
  lot_size            BIGINT,     -- minimum trade quantity
  tick_size           DOUBLE,     -- minimum price increment
  segment             STRING,     -- Arrow connection routing segment
  is_active           BOOLEAN,    -- currently subscribed? (soft-delete)
  manifest_version    STRING,     -- version of this instrument manifest
  updated_ts          BIGINT,     -- epoch ms UTC when this row was written
  schema_version      STRING
) WITH (
  'bucket.num' = '8',
  'bucket.key' = 'instrument_token',
  'table.log.ttl' = '30d'
  -- Operational metadata. NOT tiered to lake.
);
