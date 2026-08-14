# System Context and Ownership

## Current topology

```text
Arrow HFT market-data WebSocket (wss://socket.arrow.trade, binary; Standard feed removed 2026-08-14)
  → Ingestion (binary decoder, epoch-sec → epoch-ms, paise → rupees)
  → Fluss raw_table_1 LOG
  → Signal Flink job
      ├─ bounded fingerprint deduplication
      ├─ event-time candle state
      ├─ forming-bar signal detection
      ├─ in-operator ranking and portfolio gates
      ├─ Signal_Candidates LOG
      ├─ Ranking_Results LOG
      └─ Trade_Decisions immutable feed
          → Executor / durable order gate
          → Arrow REST (POST /order/regular)
          → Arrow broker

Arrow order-updates WebSocket (wss://order-updates.arrow.trade, JSON)
  → Action Capture
      ├─ Fills LOG
      ├─ Order_Lifecycle KV
      ├─ Positions KV (fill-derived)
      └─ Postback_Quarantine LOG
          ↕
      Babysitter Flink job
          → position-management action KV (future)
          → Executor

Eligible LOG tables → EOD Iceberg/S3
All services → OpenObserve
```

## Deployable components

| Component | Owns | Does not own |
| --- | --- | --- |
| Ingestion | Decode, normalize, fingerprint, append raw ticks, report suspected discontinuities | Business enrichment, candle aggregation, order placement |
| Fluss storage | Table schemas, distribution, retention, changelog and lake tiering | Strategy rules or broker calls |
| Signal Flink job | Dedup, candles, forming-bar detection, candidate audit, ranking, winner output | Broker REST side effects, fill lifecycle |
| Action Capture | Broker postback ingestion, immutable fill log, order lifecycle updates | Strategy, position decisions, order submission |
| Babysitter Flink job | Post-entry position-management decisions | New entry signals, authoritative order lifecycle |
| Executor | Changelog consumption, durable order gate, idempotency, reconciliation, Arrow REST calls (`POST /order/regular`) | Strategy ranking, fill-state authority |
| Arrow REST | Broker order entry and management (`https://edge.arrow.trade`) | Fluss consumption, strategy, fill capture, gate decisions |
| OpenObserve | Logs, metrics, traces, operational alerting | Trading decisions |

## Identity model

The old overloaded `order_id` term is replaced by three identities:

| Identity | Created by | Lifetime and purpose |
| --- | --- | --- |
| `instruction_id` | Business Logic | Stable platform decision; one winning instruction can produce multiple broker orders |
| `client_order_ref` | Executor | Short, deterministic reference sent through Arrow `remarks` (maximum 16 characters) |
| `broker_order_id` | Arrow broker | Broker's authoritative order identity returned by order submission and postbacks |

Persist the mapping between these identities before treating an order as safely reconciled. A trim, exit, or re-entry is a new broker order linked to the same position/trade context, not a mutation of the original broker order.

## State ownership

Order lifecycle and position lifecycle are different aggregates:

- **Order lifecycle:** `SUBMITTING`, `PENDING`, `PARTIAL`, `FILLED`, `CANCELLED`, `REJECTED`, `UNKNOWN`; keyed by `broker_order_id`.
- **Position lifecycle:** `FLAT`, `OPEN`, `REDUCING`, `CLOSED`; keyed by `position_id` or trade context and derived from related fills.
- **Order gate:** `ENABLED`, `HALTED`, `RECONCILING`; owned and enforced by the Executor.

`partial_update` protects independent column groups from clobbering. It does not by itself solve stale writes, ordering within one column group, broker-side idempotency, or external REST atomicity.

## Runtime environments

| Environment | Orchestrator | Purpose |
| --- | --- | --- |
| Local/development | Docker Compose | Fast single-host development and integration testing; singleton platform services are acceptable |
| Production | Docker Swarm | Four-VM deployment: three HA workload VMs and one dedicated observability VM; placement, replicas, persistent storage, and quorum rules are mandatory |

The production topology is not proven merely by running three processes. Failure tolerance and recovery must be demonstrated by the test plan.

## Source links

- Data flow and interfaces: [`../02_requirements/05-interfaces.md`](../02_requirements/05-interfaces.md)
- Data ownership: [`../02_requirements/04-data.md`](../02_requirements/04-data.md)
- Runtime details: [`../02_requirements/02-functional/09-platform-runtime.md`](../02_requirements/02-functional/09-platform-runtime.md)
