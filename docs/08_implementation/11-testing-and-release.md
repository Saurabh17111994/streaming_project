# Testing and Release

Use this file after each phase to track all tests, map requirements to proof, and prepare the final release evidence.

## Master test catalog


<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Implementation-ready test design; executable suites pending |
| Owner | Component owners; Platform owns integration/acceptance evidence |
| Scope | Unit, harness, integration, failure, recovery, performance, security, release |
| Rule | A skipped/flaky mandatory gate is a failure until dispositioned with evidence |

### Progress snapshot

| Work | Current state |
| --- | --- |
| Test design | Complete: every required test type is documented in this file or its owning phase document. |
| Executable tests | Not started: no Java, integration, or benchmark test suite exists yet. |
| Runtime evidence | Not started: no local, sandbox, or production-like test report exists yet. |
| Live-money approval | Blocked until executable tests and all release evidence pass. |

### Detailed test designs

This catalog is the single authoritative home for detailed test inputs, actions, pass results, and evidence. Component dossiers define the system to build and link here for verification; they do not duplicate test procedures.

| Area | Test-scope link |
| --- | --- |
| Mock broker and workload generator | [Foundation and workload](#foundation-and-workload) |
| Schema and storage | [Schema and storage](#schema-and-storage) |
| Ingestion | [Ingestion](#ingestion) |
| Signal job | [Signal job](#signal-job) |
| Action Capture | [Action Capture](#action-capture) |
| Babysitter | [Babysitter](#babysitter) |
| Executor | [Executor](#executor) |
| Local Compose | [Local Compose](#local-compose) |
| Production Swarm | [Production Swarm](#production-swarm) |
| Observability and operations | [Observability and operations](#observability-and-operations) |

The following mappings identify the detailed sections in this catalog.

| Test IDs | Detailed section |
| --- | --- |
| `MOCK-*`, `PERF-PER-INSTRUMENT-*`, `FAIL-PENDING-001`, `FAIL-CHECKPOINT-001`, `STATE-*`, `BABYSITTER-001` | [Foundation and workload](#foundation-and-workload) |
| `SCHEMA-*`, `COMPAT-FLUSS-*` | [Schema and storage](#schema-and-storage) |
| `ING-*`, `BROKER-MD-001` | [Ingestion](#ingestion) |
| `SIG-*`, `STATE-COMPAT-001`, `COMPAT-FLINK-001` | [Signal job](#signal-job) |
| `AC-*`, `BROKER-PB-001` | [Action Capture](#action-capture) |
| `BAB-*` | [Babysitter](#babysitter) |
| `EXE-*`, `ARROW-REST-*` | [Executor](#executor) |
| `LOCAL-*` | [Local Compose](#local-compose) |
| `SWARM-*`, `PERF-NODELOSS-001`, `SEC-*` | [Production Swarm](#production-swarm) |
| `OPS-*` | [Observability and operations](#observability-and-operations) |

## Component test designs

### Foundation and workload

| Test ID | Duration | Input | Pass conditions |
| --- | ---: | --- | --- |
| `PERF-PER-INSTRUMENT-001` | 30 min | 3,000 instruments; variable 60,000 ticks/s average baseline | Raw append p99 <50 ms; decision p99 <100 ms; no acknowledged loss; total memory <85%; checkpoint p99 <5 s |
| `PERF-PER-INSTRUMENT-002` | 10 min | 3,000 instruments; variable baseline; restart Signal job once | Processing resumes <30 s; state restores; no duplicate final candle or decision within proven boundary |
| `PERF-PER-INSTRUMENT-003` | Declared campaign | 3,000 instruments; 90,000 ticks/s peak; every instrument <=30 ticks/s | No acknowledged loss; bounded backlog/memory; checkpoints and recovery remain stable; no per-instrument cap violation |
| `FAIL-PENDING-001` | Until queue limit | Fluss append artificially stalled | Warning at 80%; readiness false; critical at 100%; no unrecorded loss |
| `FAIL-CHECKPOINT-001` | 5 min | Force checkpoint failure | Signal job suppresses decisions; one idempotent safety halt published; no Arrow REST call from Flink |
| `STATE-DEDUP-001` | 15 min | Variable baseline plus duplicates | Duplicate state contains compact identity/timestamps only; expired entries removed; no raw payload retained |
| `STATE-CANDLE-001` | 15 min | Variable baseline input | One final candle per non-empty 15-second window; no tick collection exists in active state |
| `BABYSITTER-001` | 5 min | Repeated position updates | Latest state only; zero actions; startup rejects action enablement |

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `MOCK-UNIT-001` | Same manifest, seed, profile, and clock | Repeated runs produce the identical tick sequence and timestamps. |
| `MOCK-UNIT-002` | Variable baseline and peak profiles | The baseline averages 20 ticks/s/instrument across its measurement window; the peak reaches 90,000 ticks/s across 3,000 instruments; no instrument exceeds 30 ticks/s in the defined enforcement window. |
| `MOCK-UNIT-003` | Missing seed, unknown profile, missing instrument manifest, or a cap above 30 ticks/s | Startup rejects the invalid configuration with a clear error. |
| `MOCK-PERF-001` | Recorded per-instrument and aggregate rate distribution | The evidence shows variable arrivals rather than a universal 50 ms cycle, plus the configured seed, profile, average, cap, and observed distribution. |

### Schema and storage

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `SCHEMA-UNIT-001` | Manifest generation, DDL checksum, and version fields | A changed DDL or checksum prevents readiness. |
| `COMPAT-FLUSS-001` | Every approved DDL parses, applies, and is inspected on the pinned matrix | Effective schema/options equal the approved manifest. |
| `SCHEMA-UNIT-002` | Required-field parity and non-null routing identity | Missing field or invalid routing key fails validation. |
| `SCHEMA-UNIT-003` | Missing, unknown, or placeholder schema/configuration version | Readiness is blocked; no guessed default or silent compatibility path is used. |
| `COMPAT-FLUSS-002` | Raw `BYTES` write/read round trip | Original bytes and hash are unchanged. |
| `COMPAT-FLUSS-003` | LOG, KV, and changelog behavior | Observed behavior matches the table contract. |
| `COMPAT-FLUSS-004` | Stale, regressive, and conflicting KV updates | Invalid transition is rejected, quarantined, and audited. |
| `SCHEMA-REC-001` | Clean-break reset, replay, and rollback readability | Rebuilt state matches expected state without silent data loss. |
| `SCHEMA-EOD-001` | Offload retry and retention-expiry protection | Source data cannot expire before verified offload. |
| `SCHEMA-AUDIT-001` | Seven-year audit reconstruction simulation | A selected order path can be reconstructed from immutable evidence. |

Evidence: record the exact Fluss/Flink versions, DDL manifest ID, checksums, effective-schema output, fixture checksum, and test report for every run.

### Ingestion

| Test ID | Input/action | Pass result |
| --- | --- | --- |
| `ING-UNIT-001` | Decode each approved golden market packet | Typed fields match the approved fixture. |
| `ING-UNIT-002` | Send an unknown protocol version | Original bytes are preserved, quarantined, and readiness becomes false. |
| `ING-UNIT-003` | Write and read original packet bytes | Bytes and hash round-trip unchanged. |
| `ING-UNIT-004` | Send valid and invalid values | Validity is classified; invalid evidence is not silently discarded. |
| `ING-UNIT-005` | Recalculate fingerprints from fixed fixtures | Canonical fingerprint is stable and versioned. |
| `ING-UNIT-006` | Valid, missing, malformed, and wrongly scaled event timestamps with a controlled clock offset | Accepted timestamps have the approved UTC/unit interpretation; unsafe timestamps are quarantined with the reason recorded. |
| `ING-UNIT-007` | Trade, quote, depth, optional-field, and missing-required-field packets | Each packet is correctly classified; optional omissions remain valid and required omissions are quarantined. |
| `ING-INT-001` | Load manifest and subscribe | Every required instrument is subscribed or readiness is false. |
| `ING-INT-002` | Append accepted packets to Fluss | Every outcome has receive, start, acknowledgement/uncertainty timestamps. |
| `ING-INT-003` | Send multiple accepted ticks | Exactly one append submission is made per tick; no application batch exceeds one record. |
| `BROKER-MD-001` | Sandbox market-data packet corpus, endpoint behavior, protocol fields, limits, and unknown-version handling | Observed behavior is recorded as protocol evidence; unsupported or unknown behavior blocks readiness rather than being guessed. |
| `ING-FAIL-001` | Disconnect and reconnect broker | Connection epoch increases and subscription completeness is rechecked. |
| `ING-FAIL-002` | Slow/unavailable Fluss writer | 80% warning and 100% stop behavior occur within both bounds; no unrecorded drop. |
| `ING-FAIL-003` | Force shutdown with pending writes | Uncertainty/loss evidence is persisted. |
| `ING-PERF-001` | Variable 60,000 ticks/s average baseline, 3,000 instruments | Append p99 is under 50 ms and memory/backlog remain bounded. |
| `ING-PERF-002` | 90,000 ticks/s peak; every instrument at or below 30 ticks/s | Bounded backlog/memory and no acknowledged loss. |

Evidence: approved packet corpus, manifest snapshot, deterministic clock, workload seed, append-outcome log, metrics report, and quarantine records. Real broker credentials are never used in unit tests.

### Signal job

| Test IDs | What is tested | Pass result |
| --- | --- | --- |
| `SIG-UNIT-001` to `SIG-UNIT-006` | Tie ordering, candles, 300000 ms dedup TTL, candidate identity, ranking, and reservations | Output is deterministic for fixed input and clock. |
| `SIG-UNIT-007` | Dependency scan | No `flink-cep` dependency or CEP import exists. |
| `SIG-UNIT-008` to `SIG-UNIT-009` | Dedup and candle state contents | State stays compact; no raw packet/event collection or tick list is stored. |
| `SIG-HARNESS-001` | Out-of-order events, watermark, and idleness | Correct event-time outcome is emitted. |
| `SIG-HARNESS-002` | Late before-final versus after-final event | Only the permitted update/discard behavior occurs. |
| `SIG-HARNESS-003` | Checkpoint then restore and replay | Recovered output equals the expected deterministic output. |
| `SIG-HARNESS-004` | Two identical-looking events: one broker duplicate and one legitimate identical event | The documented fingerprint limitation is applied consistently and its metric/audit evidence is emitted. |
| `SIG-HARNESS-005` | Checkpoint/restore while a reservation and ranking result are active | Recovery preserves the correct reservation and ranking outcome without a duplicate decision. |
| `STATE-COMPAT-001` | Approved serializer and savepoint version change | State/savepoint restore succeeds through the approved compatibility path, or startup blocks before unsafe use. |
| `SIG-INT-001` | Pinned Fluss source/sink boundary | Source/sink semantics work with approved versions. |
| `COMPAT-FLINK-001` | Source/sink checkpoint, restore, and rescale on the pinned Flink/connector versions | Restored processing and state remain within the approved consistency boundary. |
| `SIG-INT-002` | Partial visibility across outputs | Reconciliation identifies and handles partial visibility. |
| `SIG-FAIL-001` | Checkpoint or continuity failure | New decisions are suppressed and a safe halt is requested. |
| `SIG-PERF-001` | Variable baseline and peak workload | Decision p99, state, checkpoint, and memory stay within the defined limits. |

Evidence: fixture seed, event-time sequence, expected output, checkpoint/savepoint reference, state-size report, and performance report.

### Action Capture

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `AC-UNIT-001` | Postback decode and status mapping | Known packet maps to the approved internal status. |
| `AC-UNIT-002` | Platform identity and fingerprint | Identity is stable for duplicate delivery evidence. |
| `AC-UNIT-003` | Correlation priority and ambiguity | Exactly one valid match is correlated; ambiguity is quarantined. |
| `AC-UNIT-004` | Lifecycle precedence and regression | Stale/regressive state cannot overwrite newer state. |
| `AC-UNIT-005` | Position quantity/value transitions | Position rules are correct and independent of lifecycle state. |
| `AC-INT-001` | Immutable audit plus projections | Audit persists before/with recoverable projection work. |
| `BROKER-PB-001` | Sandbox postback corpus: status values, identities, timestamps, optional fields, unknown fields, and delivery behavior | Observed protocol behavior is versioned as evidence; unsupported behavior is quarantined and blocks readiness. |
| `AC-FAIL-001` | Crash after each projection step | Restart resumes unfinished work without regression. |
| `AC-FAIL-002` | Ledger-based restart recovery | Ledger produces the correct resumed projection. |
| `AC-FAIL-003` | Missing/ambiguous mapping | Event is quarantined and affected order path halts. |
| `AC-FAIL-004` | Duplicate, out-of-order, conflicting postbacks | No incorrect state transition occurs. |
| `AC-REC-001` | Full rebuild from immutable events | Rebuilt projections match approved expected output. |

Evidence: versioned postback fixtures, projection-ledger snapshots, crash-point logs, quarantine records, and before/after projection comparisons.

### Babysitter

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `BAB-UNIT-001` | Every valid position state | Job emits zero action records. |
| `BAB-UNIT-002` | Future action flag enabled in MVP | Startup fails closed; no action is emitted. |
| `BAB-INT-001` | Positions changelog schema and offsets | Input is consumed using approved schema and offset behavior. |
| `BAB-HARNESS-001` | Position changelog checkpoint, restore, and offset recovery | The latest safe state resumes correctly and the job still emits zero actions. |
| `BAB-FAIL-001` | Checkpoint restore and changelog gap | Restore is correct; a gap makes readiness false. |
| `BAB-FAIL-002` | Stale/conflicting position input | Unsafe input is suppressed and reported. |
| `BAB-OPS-001` | Job readiness state | Babysitter health never claims Executor trading readiness. |

Evidence: input fixture, output capture proving zero actions, checkpoint/restore report, changelog-gap record, and readiness metrics.

### Executor

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `EXE-UNIT-001` | Gate transition and epoch | Invalid transition/epoch is refused and audited. |
| `EXE-UNIT-002` | Decision hash, expiry, reservation | Mutated, expired, or unreserved decision is refused. |
| `EXE-UNIT-003` | Client reference canonicalization | Reference is stable and conforms to broker constraints. |
| `EXE-UNIT-004` | Broker result classification | Success, failure, and unknown are classified safely. |
| `EXE-UNIT-005` | Two-person approval identity/epoch/evidence | One person or mismatched approval cannot resume trading. |
| `EXE-UNIT-006` | Replay the same immutable decision and request hash | At most one active attempt exists; replay cannot create a second broker call. |
| `EXE-INT-001` | Fluss changelog and owned state writes | State ownership and durable transitions are preserved. |
| `ARROW-REST-001` | Sandbox Arrow REST authentication, request fields, response fields, rejection, and timeout behavior | Actual behavior is captured as protocol evidence; timeout or ambiguity becomes `UNKNOWN` and blocks a blind retry. |
| `ARROW-REST-002` | Client-reference length, character set, echo behavior, and broker-ID correlation | The approved reference format correlates one attempt to one broker order, or live-money readiness remains blocked. |
| `EXE-FAIL-001` | Crash before, during, after broker acceptance | No duplicate order; uncertain outcome halts and reconciles. |
| `EXE-FAIL-002` | Timeout, malformed, or unknown response | Outcome becomes UNKNOWN; no blind retry. |
| `EXE-FAIL-003` | Missing/corrupt state at restart | Calls remain blocked and reconciliation is required. |
| `EXE-FAIL-004` | Changelog, checkpoint, durable-state loss | Trading readiness is false and gate halts. |
| `EXE-FAIL-005` | Fencing, split-brain, concurrent Executor | Only current fenced owner can call broker. |
| `EXE-FAIL-006` | Mapping quarantine/reconciliation | Ambiguous mapping blocks further unsafe action. |
| `EXE-OPS-001` | Unauthorized controls and two-person resume | Unsafe control attempts are rejected and audited. |
| `EXE-AUDIT-001` | Audit reconstruction | Order path can be reconstructed from retained audit evidence. |

Evidence: sandbox broker or deterministic stub only; preserve attempt timeline, gate epoch, audit IDs, reconciliation output, and proof that no duplicate request was sent.

### Local Compose

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `LOCAL-INT-001` | Fresh start with documented command | All required local services become healthy in dependency order. |
| `LOCAL-INT-002` | Effective configuration and schema manifest | Running services use pinned versions and approved local configuration. |
| `LOCAL-INT-003` | End-to-end synthetic path | Packet reaches decision, simulated postback, and projection without real order placement. |
| `LOCAL-FAIL-001` | Restart Flink/service with checkpoint state | State restores according to component recovery rules. |
| `LOCAL-FAIL-002` | Missing secret, dependency, or schema | Affected service is not ready; no unsafe fallback occurs. |
| `LOCAL-OBS-001` | Local logs, metrics, and audit | Health and correlation evidence can be inspected locally. |

Evidence: compose file digest, image digests, effective config with secrets removed, startup log, test fixture seed, and E2E report.

### Production Swarm

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `SWARM-INT-001` | Pinned images, placement, network, secrets, and identities | No mutable image, unsafe network exposure, or replica co-location remains. |
| `SWARM-INT-002` | Separate service, durability, job, and trading readiness | Each readiness state reports independently. |
| `SWARM-FAIL-001` | One workload VM loss | Quorum/restore passes; processing recovery is within accepted target and gate halts within 5 seconds when required. |
| `PERF-NODELOSS-001` | 90,000 ticks/s peak plus one VM loss | Records quorum degradation, leader re-election, checkpoint restore, safe-halt latency, processing recovery, backlog drain, replica catch-up, and zero acknowledged loss against the catalog limits. |
| `SWARM-FAIL-002` | S3/checkpoint/lake/audit dependency failure | Affected readiness is false; unsafe trading is blocked. |
| `SWARM-REC-001` | Halted rollback and state readability | Rollback preserves readable state and never auto-enables trading. |
| `SEC-NET-001` | Public and internal deny-path network probes | Only approved ingress and service paths are reachable; every prohibited path is blocked. |
| `SEC-TRANSPORT-001` | TLS and storage-encryption verification for broker, Arrow REST, S3, and internal sensitive paths | Unencrypted or unverified transport/storage is rejected and readiness is false. |
| `SEC-CRED-001` | Secret rotation, revocation, expiry, and invalid-secret startup | Access is removed or restored safely; no secret is exposed in logs or evidence. |
| `SEC-AUTHZ-001` | Least-privilege table, state, broker, and control permissions | Authorized operations work; every excessive or unauthorized operation is denied and audited. |
| `SEC-IMAGE-001` | Pinned image digest, SBOM, and vulnerability-policy validation | Mutable, unapproved, or policy-failing images block deployment. |
| `SEC-AUDIT-001` | Audit access, deletion, retention, and legal-hold policy | Unauthorized access/deletion is denied and retention/legal-hold behavior is evidenced. |

### Observability and operations

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `OPS-UNIT-001` | Telemetry envelope and redaction | Required fields exist; credentials, tokens, and raw packets do not appear. |
| `OPS-UNIT-002` | Metric labels | Labels remain bounded; IDs are not high-cardinality labels. |
| `OPS-INT-001` | Health dimensions | Liveness, readiness, job, trading, durability, and telemetry health are independently queryable. |
| `OPS-INT-002` | Critical alert delivery, acknowledgement, escalation, runbook link | Each alert reaches its owner and records evidence. |
| `OPS-FAIL-001` | OpenObserve outage | Immutable audit remains available; outage cannot authorize orders. |
| `OPS-FAIL-002` | Checkpoint failure, backlog, or unknown broker result | Correct safe-halt/suppression action occurs within defined limits. |
| `OPS-RUNBOOK-001` | Runbook exercise for a selected critical incident | Operator follows the runbook to containment, reconciliation, recovery, and closure evidence. |
| `OPS-REL-001` | Dashboard/query version in release evidence | Release package identifies the exact dashboard/query version used. |

Evidence: alert configuration version, dashboard/query version, injected-fault record, notification and acknowledgement timestamps, runbook exercise notes, and closure evidence.

### Test record format

Every executable test or campaign records:

```text
test_id
requirement_ids
audit_issue_ids
component/boundary
preconditions and exact versions
fixture/workload and checksum
steps/fault injection
expected result
observed result
artifacts/log/audit IDs
clock/environment evidence
owner/date
```

## Shared testing rules

### What the testing program must prove

- Versioned inputs and state behave deterministically.
- Delivery and consistency limits are stated honestly.
- Memory, state, backlog, and recovery remain bounded.
- No crash window creates a duplicate broker order.
- Every defined uncertainty condition safely halts order calls.
- Schema, protocol, checkpoint, and deployment compatibility are proven.
- Production workload, node loss, offload, security, and audit guarantees are proven.

### Test levels and environments

| Level | Purpose | Environment |
| --- | --- | --- |
| Unit | Pure decode, canonicalization, transition, ranking, and validation logic | Build runner |
| Flink harness/state | Event time, operators, timers/state, checkpoint/replay | Test JVM |
| Component integration | Fluss client, schema, persistence, service boundary | Pinned local stack |
| End-to-end | Tick → decision → sandbox order → postback → position | Acceptance environment |
| Failure/recovery | Crash windows, partial writes, restart, corruption, gaps | Fault-enabled acceptance |
| Performance | Baseline, burst, stress, latency/backpressure | Production-like Swarm |
| Chaos/DR | VM/node/store/network/credential loss | Production-like Swarm |
| Security | Network, identity, secret, authorization, audit controls | Acceptance/production-like |

### Unit-test rules

Unit tests use fixed clocks, canonical fixtures, explicit versions, and deterministic IDs. They do not need a live Fluss cluster, broker, Arrow REST, or S3.

Every component test set must cover empty, malformed, unknown-version, missing, duplicate, legitimate-identical, delayed, out-of-order, stale, conflicting, terminal-regressive, maximum-size, repeated, and concurrent inputs where the case applies. It must also cover clock skew, timestamp-unit mismatch, optional versus required broker fields, immutable IDs with equal and changed content, and matching versus mismatched gate approval evidence.

Passing unit tests never replaces connector, crash-window, capacity, or recovery proof.

### Integration-test rules

Use these environments in order:

1. A clean local Fluss/Flink stack for schema and connector behavior.
2. A sandbox broker and Arrow REST environment for protocol and response evidence.
3. A production-like four-VM Swarm for workload, HA, security, and recovery evidence.

Integration coverage includes catalog/table creation, effective DDL/options, `BYTES` round trips, LOG/KV/changelog behavior, Flink checkpoint/restore/rescale, all service paths, and S3 checkpoint/lake/offload/retention behavior.

Unknown endpoint paths, fields, limits, status values, timestamps, identity behavior, timeouts, or idempotency are blockers. The test records what was observed; it never invents a contract. Every integration run records a fixed fixture, exact versions, failure classification, expected consistency boundary, and evidence artifact.

### Unit and deterministic component tests

| Test family | Required behavior |
| --- | --- |
| `ING-UNIT-*` | Decode, bytes/hash, normalization, fingerprint, invalid/quarantine |
| `SIG-UNIT-*` | Dedup TTL, tie ordering, candles, candidates, ranking, reservation |
| `AC-UNIT-*` | Postback identity, correlation, status precedence, positions |
| `BAB-UNIT-*` | Strict no-op and action-enable fail-closed |
| `EXE-UNIT-*` | Gate, immutability, attempts, references, approvals, classification |
| `SCHEMA-UNIT-*` | Manifest/checksum/parity/compatibility classification |

Deterministic tests use fixed clocks, versioned fixtures, stable IDs/seeds, and canonical expected outputs.

### Flink harness and state tests

- `SIG-HARNESS-001`: out-of-order events, watermark and idleness.
- `SIG-HARNESS-002`: late-before-final versus discard-after-final.
- `SIG-HARNESS-003`: deterministic checkpoint/restore replay.
- `SIG-HARNESS-004`: identical legitimate event versus duplicate fingerprint limitation.
- `SIG-HARNESS-005`: reservation and ranking recovery.
- `BAB-HARNESS-001`: position changelog/offset restore with zero action output.
- `STATE-COMPAT-001`: serializer/savepoint compatibility for every version change.

### Integration tests

- `COMPAT-FLUSS-001`: DDL parse/apply/effective schema.
- `COMPAT-FLUSS-002`: BYTES round trip.
- `COMPAT-FLUSS-003`: LOG/KV/changelog semantics.
- `COMPAT-FLUSS-004`: partial update and stale-write application protocol.
- `COMPAT-FLINK-001`: source/sink checkpoint and restore.
- `COMPAT-FLINK-002`: cross-table partial visibility behavior.
- `BROKER-MD-001`: market packet corpus/protocol compatibility.
- `BROKER-PB-001`: postback schema/status/timestamp/identity behavior.
- `ARROW-REST-001`: request/response/auth/timeout/rejection behavior.
- `ARROW-REST-002`: client reference length/charset/echo and broker ID correlation.
- `E2E-001`: packet → decision → attempt → sandbox broker → postback → position.

### Crash-window and failure matrix

| Fault point | Expected invariant |
| --- | --- |
| Before raw append | Loss/uncertainty explicitly accounted; no silent success |
| After raw append before local ack | At-least-once duplicate tolerated downstream |
| During Flink checkpoint | Restore produces deterministic state within tested boundary |
| Between multiple Signal sinks | Partial visibility reconciled by IDs |
| After postback audit before lifecycle | Projection ledger resumes |
| After lifecycle before position | Position step resumes without lifecycle regression |
| Before attempt prepare | No broker call |
| After prepare before call | Recover pending attempt; no blind new attempt |
| During broker call | Outcome UNKNOWN; halt/reconcile |
| After broker acceptance before durable ack | UNKNOWN; no duplicate retry |
| During mapping persistence | Halt and reconcile mapping evidence |
| During gate approval | No partial/same-identity enablement |
| Fencing lease loss | Calls blocked; gate halted |
| Safety-halt request stale/unauthorized/cross-scope | Rejected; audited |
| Changelog gap | Trading readiness false; reconcile |
| S3/checkpoint unavailable | Job/durability not ready; gate halted if correctness affected |
| OpenObserve unavailable | Durable audit remains; telemetry readiness false |

### Chaos and disaster-recovery rules

All chaos tests use a sandbox or simulated broker unless a separately approved controlled test exists. The Executor starts `HALTED`; every fault preserves evidence; no test bypasses fencing or two-person approval controls.

The required fault coverage includes: Ingestion crash, disconnect, authentication expiry, partial subscription, append timeout, and bounded-buffer saturation; Signal JobManager/TaskManager failure, checkpoint timeout/corruption, S3/state/sink failure, and backpressure; Action Capture crashes after each independent write, projection backlog, Fluss outage, ambiguity, and postback storm; Babysitter restart, changelog gap, stale input, and accidental action enablement; Executor crash windows, mapping/state/audit failure, fencing loss, and split brain; Fluss coordinator/tablet/volume/quorum/leader failures; one-VM loss at baseline and peak; Arrow REST timeout/malformed/ambiguous response; OpenObserve alert failure; EOD/offload/retry/expiry failures; and credential/TLS/authorization failures.

Every exercise records exact versions, topology, workload, fault point/time, detected signals, gate state/epoch, RPO/RTO, backlog, checkpoints/offsets, reconciliation actions, recovery proof, alerts, and operator approvals.

The expected result is always: data-path recovery is measured and only claimed below thirty seconds for accepted cases; order uncertainty blocks calls inside five seconds; unknown outcomes never retry automatically; one fenced Executor owner exists per partition; unverifiable state remains `HALTED`; partial projections recover idempotently; source data never expires before verified offload; execution audit stays reconstructable; and observability loss never authorizes orders. Missing telemetry, unexplained recovery, or automatic resume is failure.

### Schema and recovery tests

- Clean catalog/table creation.
- Effective schema and option parity.
- Missing/wrong schema version prevents readiness.
- Immutable identity same/different hash behavior.
- KV stale/regressive/conflicting transition behavior.
- Projection rebuild from immutable events.
- Clean-break reset/replay.
- Savepoint/schema migration and rollback readability.
- EOD offload retry and expiry protection.
- Audit retrieval/reconstruction.

### Data quality and replay rules

Test raw byte/hash preservation; decoder/schema version and unknown-version quarantine; manifest completeness/types/active state/checksum; timestamp UTC conversion/clock offset/missing time; fingerprint and duplicate behavior; trade/quote/depth classification; candle correctness and late-event handling; candidate/ranking/decision identity and score provenance; postback correlation/lifecycle/position behavior; attempt/request hash/client-reference/mapping consistency; and audit redaction/reconstruction.

| Replay | Expected result |
| --- | --- |
| Same raw packets, same versions/config | Identical accepted state and output hashes. |
| Raw duplicate delivery | One compute effect; raw audit remains at-least-once. |
| Legitimate identical event | Bounded documented collapse may occur; metric is emitted. |
| Changed decoder/schema version | Reject or use an explicit compatibility path; never silently reinterpret. |
| Reordered input within lateness | Deterministic final candle/ranking result. |
| Replayed postback | Immutable duplicate evidence and idempotent projection. |
| Replayed decision | No second active attempt for the same request hash. |
| Changed immutable content | Contract violation, quarantine, and safety halt where relevant. |

Every fixture has a version/checksum and expected output. Any count or hash mismatch is investigated; immutable/audit data never accepts “close enough.” Replay evidence records source range, schema/config versions, output hashes, duplicate/late counts, and unresolved evidence.

### Performance campaigns

| Test | Workload | Duration | Required evidence |
| --- | ---: | ---: | --- |
| `PERF-PER-INSTRUMENT-001` | variable 60,000 ticks/s average baseline (3,000 instruments; 20 ticks/s/instrument average) | Full trading session | SLOs, loss, backlog, checkpoints |
| `PERF-PER-INSTRUMENT-002` | Same manifest; restart Signal job once | Recovery window | Restore, no duplicate final candle or decision |
| `PERF-NODELOSS-001` | 90,000 ticks/s peak profile + one VM loss | Recovery window | Quorum, restore, <30 s accepted recovery, <5 s halt |
| `PERF-EOD-001` | Full-volume day | EOD | Verified manifest <30 min target |

Use production instrument universe, connections, packet-size distribution, strategy state, and exact versions. Window waiting is reported separately from processing latency.

### Performance benchmark procedure

The active instrument manifest is fixed at 3,000 instruments during a trading session. The baseline is 60,000 ticks/s on average across the declared measurement window (20 ticks/s/instrument on average). No instrument may exceed 30 ticks/s in the profile enforcement window. The capacity peak is 90,000 ticks/s. A universal fixed 50 ms schedule is prohibited. The current testing phase uses the 1,024-instrument manifest `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv` on one HFT connection (basic tier); the 3,000-instrument / 3-connection envelope is the deferred production target.

Record decode-to-append, tick-to-decision, decision-to-Executor receipt, and Arrow REST response percentiles separately. Also record throughput, backlog, source/sink and watermark lag, backpressure, state/checkpoint size and duration, restart count, acknowledged loss, recovery time, safe-halt time, EOD offload duration, and expiry margin. Every result includes versions/digests, configuration hash, duration, UTC and monotonic clock source, sample count, and whether a restart/failure occurred.

The per-instrument mock Arrow broker is the normal benchmark source. It uses a recorded seed, variable arrivals, the production instrument manifest, a 20 ticks/s/instrument baseline average, and a hard 30 ticks/s/instrument cap. It rejects a missing seed, unknown profile, or a cap above 30. A live Arrow Trade WebSocket capture is optional for protocol evidence only; it records actual packet sizes, tick frequency, total rate, endpoint/session/reconnect data, and still uses the full Ingestion → Fluss → Flink path.

| Tool | Purpose |
| --- | --- |
| `hyperfine` | Warm-up and repeated-run orchestration |
| Flink REST API | Operator throughput, backpressure, checkpoints, watermark |
| Fluss metrics | Append latency, fetch/replication lag, bytes in/out |
| OpenObserve / Prometheus | Long-duration metrics, percentiles, alert proof |
| Docker stats / `htop` / `vmstat` | CPU, memory, disk IOPS, network bandwidth |

Each benchmark produces a versioned JSON record in `code/benchmarks/results/` and links it here. Its profile records the seed, profile name, 3,000-instrument manifest, baseline average, hard cap, observed distribution, expected 60,000 average rate, expected 90,000 peak rate, and duration. It must not record a fixed `tick_interval_ms`.

| Step | Action |
| --- | --- |
| Warm-up | Run for two minutes for JIT and cache stabilization. |
| Variable baseline | Run 60,000 ticks/s average across the 3,000-instrument manifest for 30 minutes. |
| Capacity peak | Run 90,000 ticks/s with every instrument capped at 30 ticks/s for the declared campaign. |
| Cool-down | Drain backlog, wait for checkpoints, and verify no acknowledged loss. |

The baseline must meet the documented decision p99 target. Backlog must stay bounded, checkpoints must restore, safe halt must remain below five seconds, accepted data recovery below thirty seconds, and EOD verification below thirty minutes at full volume. Performance alone never proves protocol correctness, duplicate safety, or live-money readiness.

### One-VM-loss procedure

Run this at the variable baseline or peak profile; use the 90,000 ticks/s peak unless lower fault evidence is approved. All three Fluss workload VMs and encrypted-S3 checkpoints must be healthy first. The Executor may be enabled only against a sandbox broker.

1. Record two minutes of healthy baseline metrics.
2. Hard-stop one workload VM and record `T0`.
3. Record Fluss quorum degradation and leader re-election.
4. Record Flink TaskManager loss and restart/rescale trigger.
5. Record the gate transition to `HALTED`.
6. Record Ingestion reconnect and Flink checkpoint restore.
7. Run for ten minutes at reduced capacity and record throughput, backlog, and checkpoints.
8. Restore the VM and record quorum, replica catch-up, and backlog drain.

Pass requires zero acknowledged loss, safe halt below five seconds, data-path recovery below thirty seconds, successful checkpoint restore, and quorum re-formation without manual intervention. The evidence JSON records timestamps, topology, workload, versions, configuration hash, ISR shrink, leader election, checkpoint restore, backlog drain, and replica catch-up.

### Required end-to-end test matrix ([`01_plan.md`](./01-foundation.md) Section 4)

| Test ID | Duration | Input | Pass conditions |
| --- | ---: | --- | --- |
| `PERF-PER-INSTRUMENT-001` | 30 min | Production instrument manifest; variable 60,000 ticks/s average baseline | Raw append p99 <50 ms; decision p99 <100 ms; no acknowledged loss; total memory <85%; checkpoint p99 <5 s |
| `PERF-PER-INSTRUMENT-002` | 10 min | Same manifest; restart Signal job once | Processing resumes <30 s; state restores; no duplicate final candle or decision within the proven boundary |
| `FAIL-PENDING-001` | Until queue limit | Fluss append artificially stalled | Warning at 80%; readiness false; critical at 100%; no unrecorded loss |
| `FAIL-CHECKPOINT-001` | 5 min | Force checkpoint failure | Signal job suppresses decisions; one idempotent safety halt published; no Arrow REST call from Flink |
| `PERF-PER-INSTRUMENT-003` | Declared campaign | Production manifest; variable 90,000 ticks/s peak; each instrument ≤30 ticks/s | No acknowledged loss; bounded memory/backlog; checkpoint and recovery evidence; no cap violation |
| `STATE-DEDUP-001` | 15 min | Variable baseline plus duplicates | Duplicate state contains compact identity/timestamps only; expired entries removed; no raw payload retained |
| `STATE-CANDLE-001` | 15 min | Variable baseline input | One final candle per non-empty 15-second window; no tick collection exists in active state |
| `BABYSITTER-001` | 5 min | Repeated position updates | Latest state only; zero actions; startup rejects action enablement |

### Security tests

- `SEC-NET-001`: network exposure and deny-path tests.
- `SEC-TRANSPORT-001`: TLS/encrypted transport/storage.
- `SEC-CRED-001`: secret scan, log redaction, support-bundle redaction, and rotation/revocation.
- `SEC-AUTHZ-001`: least privilege for table/state/broker calls and unauthorized controls.
- `SEC-IMAGE-001`: image digest/SBOM/vulnerability policy.
- `SEC-AUDIT-001`: audit access and deletion/legal-hold policy.

### CI gates

CI must fail for:

- Missing service entry point.
- Missing required test family.
- Stale prohibited identifiers or architecture terms.
- `latest` or unpinned production dependency.
- Requirement/contract/DDL/schema mismatch.
- Failing/skipped/flaky mandatory test.
- Missing evidence metadata.

`CI-PERF-001`: the variable baseline and peak benchmark profiles use a recorded seed, the production instrument count, no fixed 50 ms schedule, no per-instrument rate above 30 ticks/s, and zero acknowledged loss.
- Secret/redaction failure.
- Unsupported state/schema compatibility.

### Definition of done

The test program is complete when every mandatory requirement and P0/P1 audit issue maps to executable evidence, exact versions and environments are recorded, failure tests exercise the actual crash windows, performance campaigns match the workload envelope, and release evidence can be independently reviewed.


## Traceability


<!-- markdownlint-disable MD013 -->

### Purpose

This matrix maps audit findings and `01_plan.md` task sequence to the implementation dossiers and executable evidence families. It prevents an issue from disappearing during documentation or code work.

### Audit issue traceability

| Audit issues | Primary dossier | Test/evidence families |
| --- | --- | --- |
| P0-1 | `components/05-executor.md` | `EXE-*`, `REL-EXE-*`, `REL-CRASH-*`, `REL-HALT-*` |
| P0-2 | All component dossiers | `ING-*`, `SIG-*`, `AC-*`, `BAB-*`, `EXE-*` |
| P0-3 | `components/02-signal-job.md` | Job submission/readiness integration tests |
| P0-003 | `17_portfolio_reservations.sql`, `18_postback_projection_ledger.sql`, `19_safety_halt_requests.sql`, `03-schema-lifecycle.md` | DDL-INV-*, DDL-SCHEMA-*, DDL-APPLY-*, DDL-META-*, DDL-REPLAY-* |
| P0-4 | `01-documentation-governance.md`, ingestion/action dossiers | `BROKER-MD-*`, `BROKER-PB-*`, stale-term CI gate |
| P1-1 | Component dossiers, `02-version-compatibility.md` | Build entry-point and artifact tests |
| P1-2 | Governance and cross-cutting invariants | Stale-term CI gate |
| P1-3 | `03-schema-lifecycle.md` | `COMPAT-FLUSS-*`, schema workflow tests |
| P1-4, P1-5 | `03-schema-lifecycle.md`, release evidence | `PERF-EOD-*`, `REL-RET-*` |
| P1-6, P1-18 | Local/production deployment dossiers | Health/readiness/startup tests |
| P1-7, P1-19 | Local and production deployment dossiers | Volume/replication/one-VM tests |
| P1-8 | `deployment/02-production-swarm.md` | `REL-HA-*` |
| P1-9, P2-1 | Version dossier, production deployment | Image/digest/SBOM CI gates |
| P1-10, P1-11 | Version/schema/local/production dossiers | Effective-config/S3/checkpoint tests |
| P1-12 | `03-schema-lifecycle.md` | Routing/null/skew tests |
| P1-13 | Schema and cross-cutting invariants | Immutable duplicate/mutation tests |
| P1-14 | Schema and Action Capture dossiers | State precedence/stale/conflict tests |
| P1-15, P1-16 | Executor and cross-cutting invariants | Correlation/attempt/concurrency tests |
| P1-17 | Schema, Executor, release evidence | `EXE-AUDIT-*`, `REL-RET-*` |
| P1-20, P1-22 | Local/production deployment dossiers | Network exposure/deny-path tests |
| P1-21 | Governance/local/production/security dossiers | Secret scan/rotation/least privilege |
| P2-2 | Local dossier | Documentation and effective-mount checks |
| P2-3 | Signal dossier/version compatibility | Pinned connector checkpoint tests |
| P2-4 | Ingestion dossier | Discontinuity/no-sequence tests |
| P2-5 | Action Capture dossier | Duplicate/no-sequence postback tests |
| P2-6, P2-7, P2-8, P2-9 | Ingestion and schema dossiers | Manifest parser/validation/injection/projection tests |
| P2-10 | Test catalog | CI test-family coverage gate |
| P2-11, P2-12 | Schema lifecycle | Manifest/checksum/parity/reset/replay tests |
| P2-13 | Signal dossier | Two-job topology/job lifecycle tests |
| P2-14 | Executor dossier | Owned-state write tests |
| P2-15 | Action Capture/Babysitter/Executor dossiers | Aggregate ownership tests |
| P2-16, P2-17 | Observability dossier | Telemetry, clock, SLO and alert tests |
| P3-1, P3-2, P3-3, P3-4 | Governance/local/version/test dossiers | Docs links/build commands/test-stage/local-only CI checks |

### Plan phase traceability

| Plan phase | Dossiers |
| --- | --- |
| 0 Governance | `01-documentation-governance.md`, `testing/02-release-evidence.md` |
| 1 Reconciliation | Governance, cross-cutting invariants, component dossiers |
| 2 Versions | `02-version-compatibility.md` |
| 3 Data model | `03-schema-lifecycle.md`, `04-cross-cutting-invariants.md` |
| 4 Ingestion | `components/01-ingestion.md` |
| 5 Signal job | `components/02-signal-job.md` |
| 6 Action/Babysitter | `components/03-action-capture.md`, `components/04-babysitter.md` |
| 7 Executor | `components/05-executor.md` |
| 8 Local runtime | `deployment/01-local-compose.md` |
| 9 Production runtime | `deployment/02-production-swarm.md` |
| 10 Observability | `deployment/03-observability-operations.md` |
| 11 Testing | `testing/01-test-catalog.md` |
| 12 Release | `testing/02-release-evidence.md` |

### Requirements traceability

| Requirement family | Owning dossier |
| --- | --- |
| `REQ-ING-*` | Ingestion |
| `REQ-FLS-*` / data requirements | Schema lifecycle |
| `REQ-FC-*` | Signal job |
| `REQ-SS-*` | Signal job |
| `REQ-RNK-*` | Signal job |
| `REQ-AC-*` | Action Capture |
| `REQ-BB-*` | Babysitter |
| `REQ-EXE-*` | Executor |
| `REQ-OBS-*` | Observability/operations |
| `REQ-PF-*` | Local/production deployment and version compatibility |

### Documentation completion statement

The dossiers specify implementation behavior but do not prove that code, DDL, deployments, or tests exist. Corresponding `01_plan.md` implementation checkboxes remain unchecked until executable evidence is produced. Documentation tasks may record these dossier paths as evidence and move to documentation-complete status.


## Release evidence


<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Design-ready release gate; evidence not yet produced |
| Owner | Platform and Execution leads; Security/Compliance approval required |
| Release posture | `LIVE_MONEY_ALLOWED=false` until every mandatory gate passes |
| Source | `docs/05_deployment/00-release-strategy.md`, `docs/02_requirements/00-index.md`, `docs/08_implementation/01-foundation.md` |

### Evidence package contents

1. Approved requirements/decision/contract/DDL revision set.
2. Version and compatibility matrix with artifact evidence.
3. DDL/schema manifest, checksums, effective schema inspection, and parity result.
4. Packet/postback corpus and broker/Arrow sandbox evidence.
5. Component unit/integration/failure/recovery reports.
6. Flink checkpoint/savepoint/state compatibility reports.
7. EOD manifest/offload/retention verification.
8. Performance reports for baseline, burst, stress, and one-VM loss.
9. Security, secret rotation, least privilege, network, image/SBOM reports.
10. Dashboard/alert/runbook readiness evidence.
11. Rollback/readability test and deployment change record.
12. Executor crash-window, fencing, reconciliation, and two-person approval evidence.
13. Seven-year audit reconstruction simulation and policy approval.

### Binary release gates

| Gate | Current status | Evidence ID | Owner | Last reviewed | Blocker | Blocking DATA-GAP | Pass condition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Data gaps | NOT_PASSED | `REL-DG-*` | User + Platform | 2026-07-23 | Live-money | DATA-GAP-001, DATA-GAP-002 | No open Critical-priority gap; all required external inputs provided and validated |
| Requirements | NOT_PASSED | `REL-REQ-*` | Platform | 2026-07-23 | Live-money | — | No unresolved contradiction among requirements, decisions, contracts, DDLs, and code |
| Versions | EVIDENCE-GATED | `REL-VER-*` | Platform | 2026-07-23 | Live-money | DATA-GAP-001, DATA-GAP-002 | Exact versions/digests approved; no `latest` |
| Protocol | EVIDENCE-GATED | `REL-PROTO-*` | Ingestion + Action Capture + Executor | 2026-07-23 | Live-money | DATA-GAP-005 | Broker/Arrow REST fields, identities, status, response, and limits proven at Level 3+ |
| Schema | NOT_PASSED | `REL-SCHEMA-*` | Storage | 2026-07-23 | Implementation | — | DDL parses/applies/parity/replay/retention tests pass |
| Ingestion | NOT_PASSED | `REL-ING-*` | Ingestion | 2026-07-23 | Implementation | DATA-GAP-001 | Golden packets, raw bytes, fingerprint limits, backpressure, subscription completeness pass |
| Signal job | NOT_PASSED | `REL-SIG-*` | Compute | 2026-07-23 | Implementation | — | Event time, dedup, candles, ranking, reservations, restore pass |
| Action Capture | NOT_PASSED | `REL-AC-*` | Action Capture | 2026-07-23 | Implementation | DATA-GAP-005 | Correlation/quarantine/lifecycle/positions/partial writes/rebuild pass |
| Babysitter | NOT_PASSED | `REL-BAB-*` | Compute | 2026-07-23 | Implementation | — | Separate job checkpoints and emits zero MVP actions |
| Executor | NOT_PASSED | `REL-EXE-*` | Executor | 2026-07-23 | Implementation | DATA-GAP-005 | Durable gate/attempt/mapping/audit/fencing/reconciliation pass; safety-halt control evidenced |
| Crash window | NOT_PASSED | `REL-CRASH-*` | Executor | 2026-07-23 | Live-money | DATA-GAP-005 | No duplicate broker order in every tested ambiguity window |
| Safe halt | NOT_PASSED | `REL-HALT-*` | Platform + Executor | 2026-07-23 | Live-money | — | Calls block within five seconds for every defined uncertainty trigger |
| Two-person resume | NOT_PASSED | `REL-APPROVAL-*` | Platform + Executor | 2026-07-23 | Live-money | — | Distinct authenticated approvals match epoch/evidence hash |
| Capacity | NOT_PASSED | `REL-PERF-*` | Platform | 2026-07-23 | Live-money | DATA-GAP-001 | 60,000 ticks/s workload campaign passes (3,000 instruments, 20 ticks/s/instrument average) |
| HA/recovery | NOT_PASSED | `REL-HA-*` | Platform | 2026-07-23 | Live-money | — | One workload VM loss, checkpoint, replication, and recovery posture pass; N+1 budgets documented and validated |
| EOD/audit | NOT_PASSED | `REL-RET-*` | Storage + Compliance | 2026-07-23 | Live-money | DATA-GAP-002, DATA-GAP-004 | Offload verification and retention protection pass; audit reconstructable |
| Security | NOT_PASSED | `REL-SEC-*` | Platform + Security | 2026-07-23 | Live-money | DATA-GAP-002 | Network, secrets, rotation, authorization, encryption, image policy pass |
| Operations | NOT_PASSED | `REL-OPS-*` | Platform + Operations | 2026-07-23 | Live-money | DATA-GAP-003 | Dashboards, alerts, runbooks, rollback and owners are operational |

A gate with `Blocker: Live-money` prevents production order placement. A gate with `Blocker: Implementation` blocks implementation progress for the affected component. Each gate SHALL be re-evaluated when its evidence changes.

### Approval sequence

1. Component owners sign their evidence.
2. Platform reconciles the version/schema/deployment package.
3. Execution signs gate/attempt/correlation/fencing/crash-window evidence.
4. Security signs secret/network/authorization/audit controls.
5. Operations signs dashboards/alerts/runbooks/rollback.
6. Compliance signs retention/deletion/legal policy.
7. Release owner confirms no unresolved critical risk.
8. Production deploys with gate `HALTED`.
9. Post-deployment reconciliation completes.
10. Two distinct authenticated operators approve the same gate epoch/evidence hash.
11. Enablement is recorded as an immutable audit event.

### Automatic blocking

The release process must fail closed for:

- Unknown version/protocol behavior.
- Missing or stale evidence.
- Failed/skipped mandatory test.
- Unresolved attempt or reservation.
- Unknown gate state.
- Lost fencing/durable state/changelog continuity.
- Unverified offload/retention.
- Missing telemetry or unowned critical alert.
- Rollback uncertainty.

### Rejection and rollback

A release is rejected if any mandatory gate fails. If uncertainty appears after deployment, the Executor returns to `HALTED`, evidence is preserved, affected orders/fills/positions are reconciled, and rollback follows the approved state-readable path. Automatic resume is prohibited.

### Final approval record

```text
release_id:
source_commit:
artifact_digests:
schema_manifest:
version_matrix:
compatibility_result:
all_gate_results:
open_risks:
rollback_artifact:
platform_approval:
execution_approval:
security_approval:
operations_approval:
compliance_approval:
first_operator:
second_operator:
gate_epoch:
evidence_hash:
enablement_timestamp_utc:
```

### Definition of done

This dossier is complete only when the evidence package can be independently reviewed and every gate is binary pass, with no P0/P1 issue unresolved or silently waived.
