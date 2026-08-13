-- Portfolio_Reservations: KV state — portfolio capacity reservations
-- Owner: Signal job
-- Type: KV (primary key on reservation_id)
-- Retention: active + rebuild/reconciliation window
-- Scope: portfolio_id
-- Schema version: 2

CREATE TABLE Portfolio_Reservations (
    reservation_id          STRING      NOT NULL,
    portfolio_id            STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    instruction_id          STRING,
    candidate_id            STRING      NOT NULL,
    capacity_class          STRING      NOT NULL,
    state                   STRING      NOT NULL,
    transition_version      BIGINT      NOT NULL,
    source_evidence         STRING,
    expiry_ts               BIGINT      NOT NULL,
    created_ts              BIGINT      NOT NULL,
    updated_ts              BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (reservation_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'reservation_id',
    'table.log.ttl' = '2d'
);
