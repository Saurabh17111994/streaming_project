-- Postback_Projection_Ledger: KV state — projection recovery workflow
-- Owner: Action Capture
-- Type: KV (primary key on postback_event_id)
-- Retention: incomplete + recovery/disposition window
-- Note: MVP (2026-07-23) skipped — re-process recent postbacks on restart
-- R-235 (2026-08-03): column renamed completeted_ts -> completed_ts (typo baked
--   into the schema would have propagated into downstream recovery code).
-- Scope: account_scope_id
-- Schema version: 2

CREATE TABLE Postback_Projection_Ledger (
    postback_event_id       STRING      NOT NULL,
    projection_state        STRING      NOT NULL,
    expected_prior_state    STRING,
    retry_count             INT         NOT NULL,
    last_error              STRING,
    disposition             STRING,
    step_ts                 BIGINT      NOT NULL,
    completed_ts            BIGINT,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (postback_event_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'postback_event_id',
    'table.log.ttl' = '2d'
);
