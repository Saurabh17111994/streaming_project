# Cross-Cutting Invariants and Internal Contracts

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready; runtime validation pending |
| Owner | Platform Team; component owners implement locally |
| Sources | `docs/01_project/02-system-context.md`, `docs/02_requirements/04-data.md`, `docs/02_requirements/05-interfaces.md`, `DEC-005`–`DEC-020` |

## Identity invariant

The following identities are never interchangeable:

| Identity | Owner | Meaning |
| --- | --- | --- |
| `candidate_id` | Signal job | One detected setup/audit record |
| `instruction_id` | Signal job | One immutable execution request |
| `execution_attempt_id` | Executor | One broker submission attempt |
| `client_order_ref` | Executor | Broker-facing attempt reference |
| `broker_order_id` | Broker/Action Capture | Broker-authoritative order |
| `trade_context_id` | Signal/Execution | Entry and position-management grouping |
| `position_id` | Position projector | Fill-derived exposure aggregate |
| `postback_event_id` | Action Capture | One received postback delivery |
| `action_id` | Future Babysitter | One immutable position action |

A generic `order_id` is prohibited in new requirements, DDL, code, logs, and tests.

## Ownership matrix

| Data/behavior | Sole owner | Readers | Prohibited owners |
| --- | --- | --- | --- |
| Raw packet/decode | Ingestion | Signal, audit/offload | Strategy, Executor |
| Candle/forming-bar state | Signal job | Business Logic/ranking | Ingestion, Executor |
| Candidates/ranking/decisions | Signal job | Executor, audit | Action Capture |
| Order lifecycle | Action Capture | Executor, operations | Signal, Babysitter |
| Position aggregate | Position projector | Babysitter, Executor | Strategy, raw ingestion |
| Order gate/attempt/mapping/audit | Executor | Action Capture/operations as needed | Signal, OpenAlgo |
| Broker REST call | Executor via OpenAlgo | Broker | Every other component |
| Position actions | Babysitter after MVP | Executor | OpenAlgo/direct callers |

## Delivery semantics

| Boundary | Contract |
| --- | --- |
| Broker stream → Ingestion | At-least-once; possible gaps; protocol evidence required |
| Ingestion → raw LOG | At-least-once; preserve every accepted packet |
| Compute dedup | Bounded best-effort fingerprinting |
| Flink state/sink | Exactly-once only within tested version-pinned boundary |
| Multiple Fluss outputs | Partial visibility unless a test proves a transaction boundary |
| Decision/action → Executor | At-least-once; immutable identity/request hash guard |
| Executor → broker | At-least-once or unknown; reconcile before retry |
| Postback projections | At-least-once input; idempotent/versioned projection |

## Ordering invariant

- Per-instrument event processing uses event time and deterministic fingerprint tie ordering.
- Broker sequence ordering is not assumed.
- Per-order projection uses source version/timestamp and status precedence.
- Per-position projection uses fill-derived source version and duplicate event identity.
- Gate transitions use serialized epoch compare-and-set.
- Global ordering is never inferred from Fluss bucket affinity.

## Time invariant

Every event carries:

```text
event_time        = verified broker/external event time
receive_time      = local receipt time
persist_start     = local monotonic/UTC append start
persist_ack       = local append acknowledgement time
processing_time   = local processing timestamp
schema_version    = event schema
```

Duration measurements use a monotonic clock. Cross-service correlation uses UTC. Clock offset is observable and readiness-affecting when outside policy.

## Immutability invariant

```text
immutable identity + same canonical hash
  → duplicate delivery; audit and suppress duplicate effect

immutable identity + different canonical hash
  → contract violation; quarantine and halt safety-relevant flow

changed executable parameters
  → new identity and explicit supersession
```

Execution-owned state is never written into strategy-owned immutable decision fields.

## Failure invariant

Every side effect must define behavior for:

- Before side effect
- During side effect
- Successful side effect before acknowledgement persistence
- Timeout/disconnect
- Duplicate delivery
- Restart after partial completion
- Stale/out-of-order input
- Corrupt or unavailable durable state

Ambiguity is represented explicitly as `UNKNOWN` and never treated as rejection or success by assumption.

## Order safety invariant

Before every money-moving call:

```text
valid immutable instruction/action
→ no unresolved duplicate/attempt
→ current gate state is ENABLED
→ current gate epoch matches attempt
→ active Executor fencing lease is valid
→ request hash/client reference are durably persisted
→ OpenAlgo contract is ready
→ call is issued
```

Any failed check prevents the call. Unknown result transitions the affected gate to `HALTED`.

## Configuration invariant

Configuration is divided into:

1. **Static build values:** exact library/image versions and schema version.
2. **Deployment values:** topology, resource, checkpoint, retention, and endpoint settings.
3. **Secret references:** credentials and keys, never secret contents in documentation.
4. **Evidence-gated values:** broker fields, limits, status mappings, idempotency, and protocol behavior.

A missing required value makes the affected component not ready. It must not fall back to an unsafe default.

## Observability invariant

Every component emits:

- Structured logs with service/instance/version/correlation IDs.
- Metrics sufficient to prove its contract.
- Health dimensions separate from business state.
- Immutable audit for safety and money-moving decisions.
- Redaction of credentials and sensitive raw payloads.

OpenObserve outage cannot authorize orders and cannot erase durable execution audit.

## Review checklist

- [ ] New code has one documented owner per state/table.
- [ ] Every identity is named explicitly.
- [ ] Every retry has a duplicate/idempotency rule.
- [ ] Every timestamp has defined semantics.
- [ ] Every uncertain outcome becomes explicit state.
- [ ] Every cross-table assumption has a connector test.
- [ ] Every safety transition is audited.
- [ ] Every readiness result explains which dependency is missing.
