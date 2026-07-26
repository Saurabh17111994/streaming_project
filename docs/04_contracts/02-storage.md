# Segment Build Contract — Storage

## Authority and status

Physical DDLs must implement the logical model in `../02_requirements/04-data.md`. Existing DDLs are stale if they use `seq_no` as required identity, ordering, deduplication, or gap evidence; use overloaded `order_id`; assign ownership to a separate Ranking deployment; allow Executor/OpenAlgo to mutate instructions; or retain raw data for only one day. A nullable broker sequence field may remain only as observational evidence and cannot support a guarantee.

Exact Fluss DDL properties are version-gated; unsupported syntax is not invented in this contract.

## Production topology

Three Fluss replicas/quorum are placed across the three workload VMs with anti-co-location. One-VM-loss tests at 75,000 ticks/s are mandatory.

## Required schemas

Market: `raw_table_1`, `feature_candles_15s`, `suspected_discontinuities`, `instruments`.

Strategy: `Signal_Candidates`, `Ranking_Results`, immutable `Trade_Decisions`. `Trade_Decisions` may be LOG or KV only when the implementation and pinned connector tests prove immutable ordering/replay semantics; it is never an Executor-owned mutable status row.

Order/position: `Fills_table`, `Order_Lifecycle`, `Positions`, `Postback_Quarantine`.

Execution: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`; future `Position_Actions`.

Every table requires an explicit owner, schema version, retention policy, and writer/column ownership matrix before DDL generation.

## Retention and lake

Eligible live event tables keep at least three complete trading days and extend retention while an EOD manifest is unverified, retryable, or under reconciliation. Execution, order, fill, gate, correlation, approval, reconciliation, and future position-action audit is immutable, encrypted, and retained seven years in Iceberg/S3.

Each manifest includes schema/table version, date/source range, rows/bytes, hashes/checksums, commit ID, verification state, and retries.

## Consistency

LOG append is at-least-once unless a pinned producer test proves a narrower retry case. KV projections use source event/version and state precedence. Independent writes are reconciled; cross-table atomicity is not assumed. Projections must persist or recover a durable pending/completion marker so audit, lifecycle, position, quarantine, and execution state can be replayed idempotently after partial failure.

## DDL migration gate

Because the project is pre-production, replace stale tables through a clean-break migration. Before applying DDLs: pin Fluss version, validate types/properties, review destructive changes, define reset/replay, and pass schema parity tests.

## Requirement traceability

- Functional: `REQ-FLS-001` through `REQ-FLS-013`
- Cross-cutting: `03-non-functional.md` §§3.2–3.4, 3.6–3.8; `04-data.md` §§4.1–4.7; `05-interfaces.md` §§5.1, 5.3, 5.5, 5.11; `06-operational.md` §§6.5–6.6, 6.10

See `../02_requirements/02-functional/02-storage.md` and `../02_requirements/04-data.md`.
