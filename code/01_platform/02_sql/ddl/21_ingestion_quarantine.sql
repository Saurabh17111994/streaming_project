-- ingestion_quarantine: immutable evidence for malformed or unsafe broker packets
-- Owner: Ingestion; separate from Action Capture Postback_Quarantine.
-- Type: LOG (no primary key)
-- Bucket key: quarantine_id
-- Retention: operational investigation window (2 calendar days via table.log.ttl)
-- Scope: account_scope_id
-- Schema version: 1
--
-- v1 (2026-08-15): header completed — the DDL previously carried no
-- Type/Retention/Schema-version header, leaving the manifest's
-- schema_version/retention_policy fields unemittable for this table.
CREATE TABLE ingestion_quarantine (
    quarantine_id       STRING NOT NULL,
    reason              STRING NOT NULL,
    instrument_token    BIGINT,
    exchange            STRING,
    symbol              STRING,
    raw_payload         BYTES NOT NULL,
    payload_hash        STRING NOT NULL,
    detected_ts         BIGINT NOT NULL,
    detail              STRING,
    schema_version      STRING NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'quarantine_id',
    'table.log.ttl' = '2d'
);
