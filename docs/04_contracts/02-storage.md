# Segment Build Contract — Storage

## Authority and status

Physical DDLs must implement the logical model in `../02_requirements/04-data.md`. Existing DDLs are stale if they use `seq_no` as required identity, ordering, deduplication, or gap evidence; use overloaded `order_id`; assign ownership to a separate Ranking deployment; allow Executor/Arrow REST to mutate instructions; or retain raw data for only one day. A nullable broker sequence field may remain only as observational evidence and cannot support a guarantee.

Exact Fluss DDL properties are version-gated; unsupported syntax is not invented in this contract.

## Production topology

Three Fluss replicas/quorum are placed across the three workload VMs with anti-co-location. One-VM-loss tests at the variable 50,000 ticks/s average baseline are mandatory. (The 90,000 ticks/s peak is retired, DEC-036.)

## Required schemas

Market: `raw_table_1`, `feature_candles_15s`, `feature_candles_15s_current` (KV projection of the candle stream — RETIRED 2026-08-13 re-scope: candle output is LOG-only, KV dropped, live-table teardown pending), `suspected_discontinuities`, `instruments`.

Strategy: `Signal_Candidates` (immutable append-only LOG — RE-SCOPED 2026-08-13, pending implementation; one row per fired signal), `Signal_Candidates_current` (candidate current-state KV, PK `(instrument_token)`, supersession overwrites — NEW 2026-08-13, pending implementation), `Ranking_Results`, immutable `Trade_Decisions`. `Trade_Decisions` SHALL be an immutable Signal-owned LOG feed with no Executor-assigned fields, execution status, or KV partial-update behavior.

Order/position: `Fills`, `Order_Lifecycle`, `Positions`, `Postback_Projection_Ledger`, `Postback_Quarantine`.

Execution: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`.

Control: `Safety_Halt_Requests`, `Portfolio_Reservations`.

Future: `Position_Actions`.

Every table requires an explicit owner, schema version, retention policy, writer/column ownership matrix, and applicable scope identity (`account_scope_id`, `portfolio_id`, or `execution_partition_id`) before DDL generation.

## Retention and lake

Eligible live event tables keep at least three complete trading days and extend retention while an EOD manifest is unverified, retryable, or under reconciliation. Execution, order, fill, gate, correlation, approval, reconciliation, and future position-action audit is immutable, encrypted, and retained seven years in Iceberg/S3.

Each manifest includes schema/table version, date/source range, rows/bytes, hashes/checksums, commit ID, verification state, and retries.

## Consistency

LOG append is at-least-once unless a pinned producer test proves a narrower retry case. KV projections use source event/version and state precedence. Independent writes are reconciled; cross-table atomicity is not assumed. Projections must persist or recover a durable pending/completion marker so audit, lifecycle, position, quarantine, and execution state can be replayed idempotently after partial failure.

## EOD controller

The EOD controller is a named service or scheduled job owning manifest creation, verification, retry/backoff, retention extension, expiry protection, storage-pressure alerts, and manual reconciliation. Its state is durable with restart/resume behavior. Source data for a trading day SHALL not expire while the manifest is unverified, retryable, or under reconciliation.

## DDL migration gate

Because the project is pre-production, replace stale tables through a clean-break migration. Before applying DDLs: pin Fluss version, validate types/properties, review destructive changes, define reset/replay, and pass schema parity tests.

## Requirement traceability

- Functional: `REQ-FLS-001` through `REQ-FLS-016`
- Cross-cutting: `03-non-functional.md` §§3.2–3.4, 3.6–3.8; `04-data.md` §§4.1–4.7; `05-interfaces.md` §§5.1, 5.3, 5.5, 5.11; `06-operational.md` §§6.5–6.6, 6.10

See `../02_requirements/02-functional/02-storage.md` and `../02_requirements/04-data.md`.
