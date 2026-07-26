# 01 — System Context and Scope

## 1.1 Purpose

The Streaming Trading Data Platform ingests live market data, computes event-time candles and forming-bar signals with Apache Flink, stores streaming events and operational state in Apache Fluss, submits approved immutable instructions through an Executor/OpenAlgo boundary, captures broker postbacks independently, and preserves eligible history in encrypted Apache Iceberg/S3 storage.

The system has two distinct safety postures:

- **Data path:** may recover automatically and tolerate bounded, explicitly measured data loss or lateness.
- **Order path:** must halt new money-moving calls whenever state, correlation, or broker outcome is uncertain. Resumption requires reconciliation and two-person authorization.

## 1.2 Actors and ownership

| Actor/component      | Owns                                                                                                                                                         | Does not own                                                 |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------ |
| Ingestion            | Broker connection, protocol decoding, normalized typed tick placement, original packet preservation, bounded fingerprinting, suspected discontinuity records | Candle computation, strategy, order placement                |
| Fluss storage        | Tables, DDL, distribution, retention, changelog, replication, lake tiering                                                                                   | Strategy or broker calls                                     |
| Signal Flink job     | Dedup state, event-time candles, forming-bar detection, candidates, scoring, ranking, portfolio reservations, immutable instructions                         | Broker REST calls, authoritative fill capture                |
| Action Capture       | Broker postback intake, immutable postback audit, order-lifecycle projection, identity correlation quarantine                                                | Strategy, ranking, broker submission                         |
| Babysitter Flink job | Position-management evaluation; no-op in MVP; future structured actions                                                                                      | New entry strategy, lifecycle authority, direct broker calls |
| Executor             | Changelog intake, durable order gate, attempt ledger, ID mapping, reconciliation, controlled execution state, OpenAlgo calls                                 | Strategy scoring, authoritative fill capture                 |
| OpenAlgo             | Broker REST adapter                                                                                                                                          | Fluss consumption, strategy, fill capture, gate decisions    |
| Position state       | Correlated fill-derived position aggregate                                                                                                                   | Raw order lifecycle authority                                |
| OpenObserve          | Operational logs, metrics, traces, alert delivery                                                                                                            | Trading decisions                                            |

## 1.3 Topology

```text
Arrow market-data stream
  → Ingestion
  → raw_table_1 LOG
  → Signal Flink job
      ├─ bounded fingerprint deduplication
      ├─ event-time candle and forming-bar state
      ├─ candidate audit
      ├─ in-operator scoring and ranking
      ├─ portfolio reservation gate
      └─ immutable Trade_Decisions instruction
          → Executor durable order gate
          → OpenAlgo REST adapter
          → broker

Arrow broker postback stream
  → Action Capture
      ├─ immutable postback audit
      ├─ order-lifecycle projection
      └─ correlation quarantine when identity is missing/ambiguous
          → Position projection from correlated fills
              ↕
          Babysitter Flink job (MVP no-op)
              → structured position action
              → Executor gate and OpenAlgo

Eligible immutable events → EOD Iceberg/S3
All components → OpenObserve
```

There are exactly two Flink jobs in MVP: the Signal job and the Babysitter job. Ranking is not a deployment or Fluss round trip.

## 1.4 Identity model

The following identities SHALL remain distinct:

| Identity               | Created by                              | Purpose                                                                            |
| ---------------------- | --------------------------------------- | ---------------------------------------------------------------------------------- |
| `instruction_id`       | Signal job                              | Immutable platform decision; changed parameters create a new instruction           |
| `client_order_ref`     | Executor                                | Deterministic broker-facing reference; length and echo behavior are evidence-gated |
| `broker_order_id`      | Broker/OpenAlgo response                | Broker-authoritative order identity                                                |
| `trade_context_id`     | Signal/position projection              | Groups an entry and its related child orders                                       |
| `position_id`          | Position projection when exposure opens | Stable position aggregate for fills, trim, exit, and re-entry relationships        |
| `execution_attempt_id` | Executor                                | One durable attempt to submit one instruction or structured position action        |

A missing or ambiguous mapping is quarantined and cannot be retried as a new order automatically.

## 1.5 Scope

### MVP in scope

- One broker integration, subject to evidence-gated protocol validation
- Supported NSE and MCX instrument manifest
- Original broker packet preservation and normalized raw tick log
- Bounded best-effort tick fingerprint deduplication
- Event-time 15-second candles with configurable watermark and allowed lateness
- Forming-bar signal detection and in-operator ranking
- Immutable candidate, ranking, and instruction records
- Durable Executor gate, attempt ledger, identity mapping, reconciliation, and OpenAlgo handoff
- Independent postback capture and order-lifecycle projection
- Separate position projection
- Checkpointed Babysitter no-op
- Operational logs, metrics, traces, health checks, and all MVP safety alerts
- EOD Iceberg/S3 offload with verified manifest and retention safety buffer
- Local Docker Compose and production four-VM Docker Swarm definitions

### Explicit non-goals for MVP

- Multi-broker support
- BSE and currency derivatives
- Strategy authoring, backtesting, ML ranking, P&L, win-rate, or business analytics
- Advanced feature libraries and market-context ranking
- Real Babysitter actions; they are Phase 4.3+ after the structured action contract is proven
- Kubernetes
- Automatic gap backfill during live ingestion
- Automatic resumption of money-moving calls after uncertain state

## 1.6 Evidence-gated dependencies

Platform and Execution teams SHALL provide and test:

- Arrow market-data packet corpus and protocol documentation
- Arrow postback fields, ordering, replay, and identity behavior
- OpenAlgo request/response/error contract
- Broker-facing `client_order_ref` length and echo behavior
- Exact Flink, Fluss, Java, Python, SDK, and OpenAlgo versions
- Version-specific connector semantics for checkpoints, sinks, changelogs, partial updates, replication, and recovery

Until evidence passes, the relevant capability remains blocked for live-money use.

 
