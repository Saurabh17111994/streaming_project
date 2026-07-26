# Data Pipeline

## Market-data and decision path

```text
Arrow market-data stream
  → Ingestion
      ├─ evidence-approved decode
      ├─ original packet preservation
      ├─ normalized typed fields
      ├─ versioned event fingerprint
      └─ suspected-discontinuity evidence
  → raw_table_1 LOG (at-least-once raw append)
  → Signal Flink job
      ├─ eligible-trade filtering
      ├─ bounded best-effort fingerprint deduplication
      ├─ 15-second event-time candle state
      ├─ final feature_candles_15s LOG
      ├─ forming-bar typed in-process handoff
      ├─ Business Logic candidate detection
      ├─ Signal_Candidates LOG
      ├─ in-operator Ranking
      ├─ Ranking_Results LOG
      └─ immutable Trade_Decisions
  → Executor
      ├─ verify immutable instruction and reservation
      ├─ verify durable gate state/epoch
      ├─ persist execution_attempt_id, request hash, client_order_ref
      ├─ fence one active owner per account/order partition
      └─ call OpenAlgo only while ENABLED
  → OpenAlgo REST adapter
  → broker
```

Ranking consumes typed in-process candidate/state snapshots. It does not read `Signal_Candidates` from Fluss, create a second evaluation window, or run as a separate job.

## Event-time and deduplication

Ingestion preserves every accepted raw packet and does not claim exact deduplication. Compute deduplicates within bounded state using the versioned `event_fingerprint` and scope. The TTL covers the declared ingestion retry/replay horizon plus watermark delay and cannot be unbounded or shorter than that horizon.

The default tested profile is five seconds bounded out-of-orderness, five seconds allowed lateness, and fifteen seconds source idleness. These are configuration values, not broker guarantees. A source without a verified event timestamp cannot advance the watermark.

For each non-empty 15-second event-time window, the Signal job emits one final candle after the watermark passes `window_end + allowed_lateness`. Later events are discarded and measured in MVP. Open/close event-time ties use the versioned fingerprint ordering specification, never an assumed broker `seq_no`.

## Candidate, ranking, and instruction flow

Business Logic appends an immutable `Signal_Candidates` event for every detected candidate. Ranking writes an immutable result for every evaluated active candidate, including score inputs, normalization, weights, rank, selection, rejection reason, reservation snapshot, and configuration version.

MVP reservation defaults are:

- At most one reserved/open trade per instrument
- At most three total reserved/open positions
- At most one per strategy

`RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, and `UNKNOWN` consume capacity. `RELEASED` does not.

A same winner with unchanged executable parameters creates audit only. Changed parameters create a new immutable `instruction_id` after prior disposition. Uncertain lifecycle or reservation state suppresses publication.

`Trade_Decisions` is an immutable instruction feed. Its physical LOG-or-KV representation remains implementation- and connector-proven; the Executor never mutates strategy fields.

## Broker postback and position path

```text
Arrow broker postback stream
  → Action Capture
      ├─ preserve original payload and hash
      ├─ assign postback_event_id and bounded fingerprint
      ├─ correlate by broker_order_id mapping,
      │  verified echoed client_order_ref, or approved reconciliation
      ├─ append immutable Fills_table event
      ├─ update Order_Lifecycle KV
      ├─ update fill-derived Positions KV
      └─ quarantine missing, ambiguous, or invalid correlation
  → Babysitter Flink job
      ├─ consume versioned Positions changelog
      ├─ checkpoint observation state
      └─ emit zero actions in MVP
```

No broker `postback_seq` or stable postback event ID is assumed. Symbol, quantity, and timestamp proximity are insufficient for correlation.

Audit append, lifecycle projection, position projection, and quarantine writes are independent unless a pinned test proves a transaction boundary. A durable pending/completion protocol retries incomplete projections idempotently after restart.

Order lifecycle and position lifecycle are distinct:

- `Order_Lifecycle` is keyed by `broker_order_id` and owned by Action Capture.
- `Positions` is keyed by `position_id`, linked by `trade_context_id`, and derived from uniquely correlated fills.
- Conflicting or regressive evidence moves affected state to `UNKNOWN` and halts affected order/action flow.

Future `Position_Actions` are immutable structured records and pass through the same Executor gate, attempt, correlation, and reconciliation path. Free-form action strings are prohibited.

## Table ownership and consumers

| Table | Type | Writer | Primary consumers | Retention/lake role |
| --- | --- | --- | --- | --- |
| `raw_table_1` | LOG | Ingestion | Signal job | ≥3 trading days; EOD Iceberg |
| `feature_candles_15s` | LOG | Signal job | Downstream/lake | ≥3 trading days; EOD Iceberg |
| `Signal_Candidates` | LOG | Signal job | Audit/lake | Immutable; EOD Iceberg |
| `Ranking_Results` | LOG | Signal job | Audit/lake | Immutable; EOD Iceberg |
| `Trade_Decisions` | Immutable feed | Signal job | Executor | Replay/reconciliation buffer; audit-linked |
| `Fills_table` | LOG | Action Capture | Projection/audit | Immutable; encrypted seven-year audit |
| `Order_Lifecycle` | KV | Action Capture | Executor/operations | Current state; rebuildable |
| `Positions` | KV | Position projector | Babysitter/Executor | Current state; rebuildable |
| `Execution_Gate` | KV | Executor | Executor/control plane | Current state plus immutable audit |
| `Execution_Attempts` | KV | Executor | Executor/reconciliation | Active/reconciliation window |
| `Order_Correlation` | KV | Executor | Executor/Action Capture | Active/reconciliation window |
| `Execution_Audit` | LOG | Executor | Operations/audit | Encrypted seven-year audit |
| `Postback_Quarantine` | LOG | Action Capture | Reconciliation | Until disposition plus evidence retention |
| `suspected_discontinuities` | LOG | Ingestion | Operations | Investigation window |
| `instruments` | Manifest | Operators | Ingestion | Current and prior versions |
| `Position_Actions` | Future LOG | Babysitter | Executor | Disabled in MVP |

## Delivery and consistency boundaries

| Boundary | Guarantee |
| --- | --- |
| Broker stream → Ingestion | Evidence-gated; treated as at-least-once with possible gaps |
| Ingestion → raw LOG | At-least-once; no exact raw deduplication |
| Compute deduplication | Bounded best-effort fingerprinting |
| Flink managed state/sinks | Exactly-once only where pinned tests prove it |
| Multiple Fluss outputs | Partial visibility unless tested otherwise |
| Instruction/action → Executor | At-least-once with durable identity/request-hash guard |
| Executor → OpenAlgo/broker | At-least-once or unknown; reconcile before retry |
| Postback projections | At-least-once input and idempotent/versioned projection |

## Live-to-lake flow

Eligible immutable event tables offload at EOD to encrypted Iceberg/S3. Every offload produces a manifest with source range, counts, bytes, schema versions, hashes/checksums, commit identifier, verification status, and retries.

A source day cannot expire while its manifest is unverified, retryable, or under reconciliation. At least three complete trading days remain live even after successful offload. Money-moving audit categories are encrypted and retained seven years.

## Safe-halt coupling

The Signal job, Action Capture, projections, and observability do not place orders directly. They expose uncertainty to the Executor. Unknown broker outcome, duplicate risk, stale reservation, missing correlation, changelog discontinuity, checkpoint failure affecting order correctness, unresolved fill, failed reconciliation, unauthorized action, or security incident transitions the gate to `HALTED` and blocks new calls within five seconds.

## References

- Compute, ranking, and execution requirements: `../02_requirements/02-functional/03-compute.md`, `../02_requirements/02-functional/10-ranking.md`, `../02_requirements/02-functional/07-executor.md`
- Data model: `../02_requirements/04-data.md`
- Interface semantics: `../02_requirements/05-interfaces.md`
- Segment contracts: `../04_contracts/`
