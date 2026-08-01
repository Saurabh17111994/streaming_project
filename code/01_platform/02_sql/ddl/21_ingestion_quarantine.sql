-- ingestion_quarantine: immutable evidence for malformed or unsafe broker packets
-- Owner: Ingestion; separate from Action Capture Postback_Quarantine.
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
    'table.retention.days' = '7'
);
