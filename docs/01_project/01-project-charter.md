# Project Charter

## Metadata

| Field | Value |
| --- | --- |
| Document ID | DOC-PROJ-CHARTER-001 |
| Status | Active project summary; MVP Phase 4.2 |
| Owner | Platform Team |
| Updated | 2026-07-23 |

## Purpose

The Streaming Trading Data Platform ingests live Arrow Trade market data, computes features and signals with Apache Flink, stores live events in Apache Fluss, executes approved instructions through OpenAlgo, and preserves eligible history in Apache Iceberg on S3.

The platform separates the **data path** from the **order path**:

- The data path may auto-recover and tolerate bounded data gaps.
- The order path must stop placing new orders when state is uncertain and require reconciliation before resuming.

## Goals

1. Ingest the supported NSE and MCX market-data scope.
2. Build event-time OHLCV candles in Flink.
3. Detect signals on forming bars and rank active setups in the same Flink job.
4. Deliver only selected winners to a push-driven executor.
5. Capture broker order lifecycle independently from execution submission.
6. Provide a separately deployable babysitter job; MVP behavior is a no-op stub.
7. Keep immutable event/audit history separate from mutable operational state.
8. Provide measurable latency, throughput, recovery, and safe-halt behavior.

## In scope for MVP

- Arrow market-data ingestion
- Fluss raw and feature LOG tables
- Flink deduplication, event-time windows, candle computation
- Stateful signal detection and in-operator ranking
- `Signal_Candidates`, `Ranking_Results`, and `Trade_Decisions` outputs
- Independent postback capture into `Fills_table`
- Executor-to-OpenAlgo order handoff
- Babysitter wiring with no emitted actions
- Operational logs, metrics, health checks, and alerts
- EOD offload of eligible history to Iceberg/S3

## Non-goals for MVP

- Multi-broker support
- BSE and currency derivatives
- Charting and end-user notification features
- 250+ feature columns and pattern-feature libraries
- Real-time gap reconciliation
- Strategy authoring or backtesting
- ML ranking
- Business analytics such as P&L and win rate
- Kubernetes deployment

Operational alerts remain in scope; **end-user trading alerts** are not.

## Source links

- Detailed context: [`../02_requirements/01-context-and-scope.md`](../02_requirements/01-context-and-scope.md)
- Functional requirements: [`../02_requirements/02-functional/`](../02_requirements/02-functional/)
- Data requirements: [`../02_requirements/04-data.md`](../02_requirements/04-data.md)
- Interfaces: [`../02_requirements/05-interfaces.md`](../02_requirements/05-interfaces.md)
