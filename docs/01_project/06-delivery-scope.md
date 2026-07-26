# Delivery Scope and Evolution

## MVP Phase 4.2

The MVP proves the complete path with conservative safety boundaries.

### Must deliver

- Local Docker Compose environment
- Production Docker Swarm deployment definition
- Arrow market-data ingestion and normalized raw payload preservation
- Best-effort bounded fingerprint deduplication
- Event-time 15-second candle computation
- Forming-bar signal detection
- In-operator ranking and portfolio constraints
- Immutable candidate and ranking audit records
- `Trade_Decisions` winner feed
- Durable Executor order gate and attempt ledger
- OpenAlgo order submission adapter
- Three-ID order correlation and broker reconciliation
- Independent postback/fill capture
- Babysitter job wired as a checkpointed no-op
- Structured logs, metrics, traces, health checks, and operational alerts
- Verified EOD Iceberg offload with a retention safety buffer

### MVP acceptance gates

1. The normal production baseline of **75,000 ticks/second** is sustained for the full trading-session test window without acknowledged data loss.
2. Burst capacity of at least **112,500 ticks/second** is sustained for the market-open test window.
3. Stress/headroom capacity of at least **150,000 ticks/second** is tested for saturation, bounded backpressure, checkpoint stability, and recovery behavior.
4. Trigger-tick-to-winner p99 meets the agreed target at the 75,000/s normal baseline.
5. No duplicate broker order in crash-window fault-injection tests.
6. Order gate halts within five seconds of every defined uncertainty trigger.
7. One production workload VM can fail without violating the documented durability posture at the normal baseline.
8. Failed EOD offload retries successfully before any source data expires.
9. Every broker postback can be correlated to an instruction and broker order, or is quarantined for reconciliation.

## Phase 4.3

- Advanced feature columns and pattern features
- Real babysitter trim, exit, trailing-stop, and re-entry logic
- Market-context ranking inputs
- Sector and exposure constraints
- Current-price input for position management

## Phase 4.4

- Out-of-band suspected-discontinuity investigation and historical reconciliation
- Configuration hot reload where operationally justified
- Ranking experiment support
- Mature order/position reconciliation tooling

## Phase 4.5

- Final alert thresholds and routing
- SLO dashboards
- Operator runbooks
- Expanded chaos and disaster-recovery exercises

## Phase 4.6

- Backfill existing Parquet history into Iceberg
- Validate counts/checksums and retire the old history source

## Phase 4.7

- Multi-broker adapters
- BSE and currency derivatives
- Charting and end-user alerts
- Backtesting
- P&L, win-rate, slippage, and strategy analytics
- Lake pruning and compaction policy
- ML-based ranking

## Change-control rule

A deferred capability moves into an earlier phase only when its owner, contract, acceptance test, operational cost, and effect on the live-money safety boundary are documented. Updating this roadmap alone does not change authoritative schemas or interfaces.

## Related plans

- Requirements index: [`../02_requirements/00-index.md`](../02_requirements/00-index.md)
- Test strategy: [`../07_testing/00-test-strategy.md`](../07_testing/00-test-strategy.md)
- Deployment strategy: [`../05_deployment/00-release-strategy.md`](../05_deployment/00-release-strategy.md)
