-- Postback_Projection_Ledger: KV state — projection recovery workflow
-- Owner: Action Capture
-- Type: KV (primary key on postback_event_id)
-- Retention: incomplete + recovery/disposition window
-- Note: MVP (2026-07-23) skipped — re-process recent postbacks on restart
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE Postback_Projection_Ledger (
    postback_event_id       STRING      NOT NULL,
    projection_state        STRING      NOT NULL,
    expected_prior_state    STRING,
    retry_count             INT         NOT NULL,
    last_error              STRING,
    disposition             STRING,
    step_ts                 BIGINT      NOT NULL,
    completeted_ts          BIGINT,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (postback_event_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'postback_event_id',
    'table.retention.days' = '7'
);
