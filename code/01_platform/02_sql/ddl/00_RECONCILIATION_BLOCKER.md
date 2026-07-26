# DDL Reconciliation Blocker

## Status: REQUIREMENTS RECONCILED — SQL GENERATED — VERSION PINNING PENDING

The requirements documents have been reconciled to the active decisions (DEC-001 through DEC-021). Replacement DDL SQL files have been generated using the reconciled schemas. **However, the SQL cannot be applied to a production or live-money environment until version-specific integration tests pass.**

## What changed

| Old | New |
| --- | --- |
| `order_id` overloaded across all tables | Three-ID model: `instruction_id`, `client_order_ref`, `broker_order_id` (DEC-007) |
| `seq_no` as dedup/gap identity | `event_fingerprint` + `fingerprint_version` (DEC-012/DEC-015) |
| `gaps` table with exact sequence ranges | `suspected_discontinuities` with detection reason (DEC-015) |
| `Trade_management_table` (combined lifecycle + babysitting) | `Order_Lifecycle` KV + `Positions` KV (DEC-013/DEC-017) |
| `Trade_Decisions` with mutable `status` field | Immutable `Trade_Decisions` (DEC-016) |
| Executor "read-only" / no Fluss writes | `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit` (DEC-006/DEC-016/DEC-019) |
| No postback quarantine | `Postback_Quarantine` LOG table |
| 1-day raw retention | ≥3 trading days; extends while EOD unverified (DEC-018) |
| `raw_payload` as STRING/JSON | `raw_payload` as BYTES (original broker bytes) (DEC-014) |

## Generated DDL files

1. `01_catalog.sql` — catalog + database bootstrap
2. `02_raw_table_1.sql` — unified tick log
3. `03_feature_candles_15s.sql` — candle log
4. `04_fills_table.sql` — immutable fill event log
5. `05_order_lifecycle.sql` — broker-order lifecycle KV
6. `06_positions.sql` — position aggregate KV
7. `07_signal_candidates.sql` — signal audit log
8. `08_ranking_results.sql` — ranking analytics log
9. `09_trade_decisions.sql` — immutable instruction feed
10. `10_suspected_discontinuities.sql` — operational discontinuity log
11. `11_instruments.sql` — instrument metadata manifest
12. `12_execution_gate.sql` — durable order-gate KV
13. `13_execution_attempts.sql` — submission attempt KV
14. `14_order_correlation.sql` — three-ID correlation KV
15. `15_execution_audit.sql` — immutable execution audit LOG
16. `16_postback_quarantine.sql` — postback quarantine LOG

## Why application is still blocked

Exact Fluss server/client/connector versions are not yet pinned. The generated SQL requires version-specific proof for:

- `BYTES` column type for raw payloads
- LOG/KV table capabilities across all 16 tables
- `partial_update` merge engine and `changelog.image = 'FULL'`
- Retention extension while EOD unverified
- Lake-tier properties for Iceberg/S3
- Replication/quorum/placement configuration
- Connector checkpoint and visibility semantics
- Encrypted 7-year lake retention for execution audit

Inventing SQL before those facts are known would create another false authority layer.

## Exit criteria

The blocker is removed only when:

- Exact Fluss/Flink versions are recorded.
- Every logical field and owner matches the reconciled data requirements (`docs/02_requirements/04-data.md`).
- SQL parses and applies to the pinned Fluss version.
- Schema parity, clean creation, replay, migration/reset, retention, lake, replication, partial-update, and changelog tests pass.
- No superseded table DDLs remain in the directory.
- `Position_Actions` placeholder is created when post-MVP babysitter behavior is approved (DEC-017).
