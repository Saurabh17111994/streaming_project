-- trade_instruction_state: authoritative instruction-hash index — one row per
--   published immutable instruction (SCH-19, REQ-FLS-008/015)
-- Owner: Signal job (sole writer)
-- Type: KV state table (PK instruction_id) — the LOG twin (Trade_Decisions) is the
--   immutable instruction feed; this index holds the canonical content hash the
--   instruction-feed protocol checks before every append (same id + same hash =
--   duplicate evidence; same id + different hash = contract violation)
-- Bucket key: instruction_id (routing identity — every instruction is routable;
--   single-field PK, so raw-client writable per the COMPAT-FLUSS-005 matrix)
-- Retention: table.log.ttl = 2d bounds the changelog; the index is rebuildable from
--   the Trade_Decisions LOG replay (REQ-FLS-015 pinned Fluss source replay)
-- Lake: none — transient enforcement index, rebuildable; no EOD/audit value
--   (the LOG twin is the audit record)
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE trade_instruction_state (
    instruction_id     STRING      NOT NULL,
    canonical_hash     STRING      NOT NULL,
    first_written_ts   BIGINT      NOT NULL,
    schema_version     STRING      NOT NULL,
    PRIMARY KEY (instruction_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.log.ttl' = '2d'
);
