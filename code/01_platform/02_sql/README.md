# Fluss DDL — reconciled proposals pending version validation

> **Status:** logical schemas are reconciled to current decisions, but the SQL is not approved for runtime application until the pinned Fluss/Flink capability suite passes.
>
> **Implementation contract:** [`../../../docs/08_implementation/01-foundation.md`](../../../docs/08_implementation/01-foundation.md)

## Current DDL set

| File | Logical object |
| --- | --- |
| `01_catalog.sql` | Fluss catalog and `trading` database |
| `02_raw_table_1.sql` | Immutable raw packet/tick LOG |
| `03_feature_candles_15s.sql` | Final 15-second candle LOG |
| `04_fills.sql` | Immutable broker postback/fill LOG |
| `05_order_lifecycle.sql` | Broker-order lifecycle KV |
| `06_positions.sql` | Fill-derived position KV |
| `07_signal_candidates.sql` | Immutable candidate audit LOG |
| `08_ranking_results.sql` | Immutable ranking audit LOG |
| `09_trade_decisions.sql` | Immutable instruction feed |
| `10_suspected_discontinuities.sql` | Feed discontinuity evidence LOG |
| `11_instruments.sql` | Versioned instrument manifest |
| `12_execution_gate.sql` | Executor gate KV |
| `13_execution_attempts.sql` | Executor attempt KV |
| `14_order_correlation.sql` | Identity mapping KV |
| `15_execution_audit.sql` | Immutable execution/safety audit LOG |
| `16_postback_quarantine.sql` | Immutable ambiguous/invalid postback LOG |
| `17_portfolio_reservations.sql` | Portfolio-capacity reservation KV |
| `18_postback_projection_ledger.sql` | Projection progress and recovery KV |
| `19_safety_halt_requests.sql` | Immutable safety-halt request LOG |

## Validation required before application

- Exact Fluss server/client and Flink connector versions.
- DDL dialect parse and clean apply.
- Effective schema/options inspection.
- `BYTES`, LOG, KV, bucket-key, and primary-key behavior.
- `partial_update` and FULL changelog behavior.
- Checkpoint, restore, and partial-visibility behavior.
- Non-null routing keys for all accepted events.
- Retention extension and EOD offload verification.
- Replication/quorum/placement.
- Encrypted long-term audit retention.

## Important rules

- The old `Trade_management_table`, overloaded `order_id`, `seq_no`, and exact `gaps` model are superseded.
- LOG-table comments do not enforce immutability; writer protocols and tests must.
- `partial_update` does not enforce stale-write or state-transition rules.
- A fixed TTL does not implement retention extension while offload is unverified.
- `make ddl` must not be treated as valid until it follows the schema lifecycle dossier and references the full current set.

## References

- Logical data contract: `../../../docs/02_requirements/04-data.md`
- Storage requirements: `../../../docs/02_requirements/02-functional/02-storage.md`
- Storage contract: `../../../docs/04_contracts/02-storage.md`
- Schema lifecycle: `../../../docs/08_implementation/01-foundation.md`
- Compatibility matrix: `../../../docs/08_implementation/01-foundation.md`
