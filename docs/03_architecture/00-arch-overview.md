# Architecture Overview — Streaming Trading Data Platform

## Status and authority

This document describes the target Phase 4.2 architecture. It is derived from:

1. Executable implementation and tests
2. Active decisions in `../01_project/04-decisions.md`
3. Reconciled DDLs under `../../code/01_platform/02_sql/ddl/`
4. Build contracts under `../04_contracts/`
5. Requirements under `../02_requirements/`

The architecture is **blocked for live-money use** until the evidence-gated Arrow/OpenAlgo protocols, exact software versions, Flink/Fluss connector semantics, DDLs, and acceptance tests pass. Unknown external behavior is not represented as an implementation fact.

## System purpose

The platform ingests supported NSE and MCX market data, computes event-time 15-second candles and forming-bar signals in Apache Flink, ranks active setups in the same Signal job, publishes immutable instructions through Apache Fluss, and submits approved instructions through a durable Executor/OpenAlgo boundary.

It independently captures broker postbacks, builds order-lifecycle and fill-derived position state, runs a checkpointed Babysitter no-op in MVP, and offloads eligible immutable history to encrypted Iceberg/S3 storage.

The platform has two safety postures:

- **Data path:** may recover automatically when correctness state is verifiable and may tolerate only bounded, measured data gaps or lateness.
- **Order path:** halts new money-moving calls whenever state, identity, changelog continuity, or broker outcome is uncertain. Resume requires reconciliation and two distinct authenticated approvals.

## Scope and non-goals

MVP includes one evidence-approved broker integration, the supported NSE/MCX instrument manifest, raw packet preservation, bounded best-effort fingerprinting, event-time candles, forming-bar detection, in-operator ranking, immutable candidates/ranking/instructions, durable execution state, independent postback capture, fill-derived positions, Babysitter no-op wiring, operational observability, and verified EOD offload.

MVP does not include multi-broker support, BSE/currency derivatives, strategy authoring, backtesting, ML ranking, business analytics, Kubernetes, automatic live gap backfill, or automatic order-path resume. Real Babysitter actions are deferred until the structured action contract and safety tests are approved.

## System at a glance

```text
Arrow market-data WebSocket
  → Ingestion
  → raw_table_1 LOG
  → Signal Flink job
      ├─ bounded fingerprint deduplication
      ├─ event-time 15-second candle state
      ├─ forming-bar detection
      ├─ candidate audit
      ├─ in-operator ranking and portfolio gates
      ├─ Signal_Candidates LOG
      ├─ Ranking_Results LOG
      └─ immutable Trade_Decisions
          → Executor durable order gate
          → OpenAlgo REST adapter
          → broker

Arrow broker postback stream
  → Action Capture
      ├─ immutable Fills_table LOG
      ├─ Order_Lifecycle KV
      ├─ correlation quarantine
      └─ fill-derived Positions KV
          ↕
      Babysitter Flink job (MVP strict no-op)
          → future structured Position_Actions
          → Executor gate

Eligible immutable events → verified EOD Iceberg/S3 manifest
All components → OpenObserve plus local durable execution audit
```

There are exactly two Flink jobs in MVP: the Signal job and the Babysitter job. Ranking is an in-process function/operator boundary, not a deployment or Fluss round trip.

## Component ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Ingestion | Broker connection, decode, normalization, packet preservation, fingerprinting, discontinuity evidence | Candles, strategy, broker orders |
| Fluss | Tables, DDL, distribution, replication, retention, changelog, lake tiering | Strategy rules and broker calls |
| Signal Flink job | Dedup, candles, forming bars, candidates, ranking, reservations, immutable instructions | OpenAlgo calls and authoritative fills |
| Action Capture | Postback intake, immutable fill audit, order lifecycle, correlation quarantine | Strategy and order submission |
| Position projector | Fill-derived `Positions` aggregate | Raw order lifecycle authority |
| Babysitter | Position observation; no-op in MVP; future structured actions | New entry signals, lifecycle authority, direct broker calls |
| Executor | Durable gate, attempts, identity mappings, reconciliation, fencing, OpenAlgo calls | Strategy, ranking, authoritative fill capture |
| OpenAlgo | Broker REST adapter | Fluss consumption and safety decisions |
| OpenObserve | Operational telemetry and alert delivery | Trading decisions or durable execution evidence |

## Identity and state boundaries

The following identities remain distinct:

- `candidate_id`: detected setup audit record
- `instruction_id`: immutable platform execution request
- `execution_attempt_id`: one submission attempt
- `client_order_ref`: deterministic broker-facing attempt reference
- `broker_order_id`: broker-authoritative order identity
- `trade_context_id`: entry and related position-management chain
- `position_id`: fill-derived exposure aggregate
- `postback_event_id`: platform-captured postback delivery
- `action_id`: future immutable structured position action

`Order_Lifecycle` is keyed by `broker_order_id`. `Positions` is keyed by `position_id`. `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and `Execution_Audit` are separate Executor-owned state. A generic `order_id` is prohibited across domains.

## Deployment environments

| Environment | Topology | Purpose |
| --- | --- | --- |
| Local/integration | Docker Compose, single host | Development, deterministic tests, component integration |
| Production | Docker Swarm, four VMs | Three workload/HA VMs plus one dedicated observability VM |

Compose is not evidence of production HA. Production must prove three-node Fluss replication/quorum, anti-co-location, encrypted S3 checkpoints, one-workload-VM loss tolerance, safe halt under five seconds, and data recovery under thirty seconds at the normal workload.

## Architecture references

- Project context and ownership: `../01_project/02-system-context.md`
- Quality targets and guarantees: `../01_project/03-quality-targets.md`
- Active decisions: `../01_project/04-decisions.md`
- Functional requirements: `../02_requirements/02-functional/`
- Data and interfaces: `../02_requirements/04-data.md`, `../02_requirements/05-interfaces.md`
- Operational requirements: `../02_requirements/06-operational.md`
- Build contracts: `../04_contracts/00-index.md`
