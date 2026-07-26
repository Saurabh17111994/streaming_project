# Technology Choices

## Status

Technology choices are architectural targets, not evidence that a specific version or protocol has already been validated. Exact Flink, Fluss, Java, Python, broker protocol/SDK, OpenAlgo, OpenObserve, container image, and connector versions are release inputs and must be pinned before live-money enablement.

## Apache Fluss

Fluss is the live streaming bus and operational storage layer.

- LOG tables hold immutable events and audit records.
- KV tables hold current materialized operational state.
- Table ownership, distribution, retention, changelog behavior, replication, and lake tiering are explicit contracts.
- `partial_update` is permitted only with tested column ownership, stale-update rejection, and merge semantics.
- No cross-table atomicity is assumed without a version-specific connector test.

Required logical tables include `raw_table_1`, `feature_candles_15s`, `Signal_Candidates`, `Ranking_Results`, `Trade_Decisions`, `Order_Lifecycle`, `Positions`, `Fills_table`, `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`, `Postback_Quarantine`, `suspected_discontinuities`, and `instruments`. `Position_Actions` is future phase only.

Fluss uses three-node replication/quorum across production workload VMs. The exact server/client release and DDL properties remain evidence-gated.

## Apache Flink

MVP has exactly two jobs:

1. **Signal job:** deduplication, event-time candle computation, forming-bar detection, Business Logic, in-operator ranking, portfolio reservations, and immutable instruction publication.
2. **Babysitter job:** checkpointed position observation and strict no-op action behavior in MVP.

Flink managed state and Fluss sink guarantees are exactly-once only at the tested, version-pinned boundary. They do not make broker REST calls or independent projections exactly-once.

Production checkpoints/savepoints use encrypted, versioned S3. The state backend, checkpoint interval, restart strategy, and connector versions are exact-version configuration rather than assumed defaults.

## Ingestion implementation boundary

Ingestion is one service process from broker decode through the supported Fluss Java client append path. The implementation language and SDK are evidence-gated: no unsupported Arrow SDK, packet format, compression mode, connection limit, or endpoint behavior may be treated as fact.

The service preserves original packet bytes, payload hash, decoder/protocol version, normalized typed fields, timestamps, and versioned event fingerprint. It appends accepted packets even when a fingerprint has been seen; Compute owns bounded logical deduplication.

Unknown protocol versions and decode failures are quarantined with original evidence. No exact broker sequence or gap range is assumed.

## Executor and OpenAlgo

The Executor is the only component permitted to initiate money-moving OpenAlgo calls. OpenAlgo is a broker REST adapter, not a Fluss consumer, strategy engine, fill authority, or order-safety owner.

The Executor durably persists an attempt and deterministic client reference before a call. Timeout, disconnect, malformed response, crash window, or ambiguous response produces `UNKNOWN`, halts the gate, and requires reconciliation before retry. The request/response schema, timeout, retry classification, client-reference behavior, and broker identity fields are evidence-gated.

## OpenObserve

OpenObserve is the target backend for structured operational logs, metrics, traces where supported, health signals, and alerts. Correlation IDs and immutable local execution audit remain mandatory if telemetry delivery is unavailable.

Observability must prove throughput, latency, deduplication, checkpoint health, replication, EOD manifests, gate state, unknown outcomes, reconciliation, approvals, security events, and every live-money acceptance gate.

## Iceberg and S3

Eligible immutable events are offloaded at EOD to encrypted Iceberg/S3 storage. The offload produces a verification manifest containing table/schema version, trading date, source range, row/byte counts, hashes/checksums, commit status, and retry state.

Source retention is at least three complete trading days and extends while the relevant manifest is unverified, retryable, or under reconciliation. Execution, order, fill, gate, correlation, approval, reconciliation, and future action audit is encrypted and retained for seven years according to approved policy.

## Container runtime

- Local development and integration use Docker Compose.
- Production uses a four-VM Docker Swarm: three workload/HA VMs and one observability VM.
- Production images use immutable digests; `latest` and version ranges are prohibited.
- Production secrets use Docker Swarm secrets and least-privilege service identities.
- Production network traffic uses encrypted overlay/TLS-protected transport where supported.

Compose is deliberately simpler, but it cannot prove production HA or live-money safety.

## Technology decision references

- Active decisions: `../01_project/04-decisions.md`
- Non-functional requirements: `../02_requirements/03-non-functional.md`
- Platform requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Build contracts: `../04_contracts/01-ingestion.md`, `../04_contracts/02-storage.md`, `../04_contracts/03-compute.md`, `../04_contracts/07-executor.md`
