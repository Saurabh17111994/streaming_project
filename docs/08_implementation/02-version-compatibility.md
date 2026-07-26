# Version and Compatibility Dossier

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | Evidence-blocked; implementation interface is defined |
| Owner | Platform Team; Execution Team owns broker/OpenAlgo evidence |
| Release impact | All unresolved rows block live-money enablement |
| Source requirements | `REQ-PF-001`, `REQ-ING-002`, `REQ-AC-001`, `REQ-EXE-006`, `DEC-021` |

## Purpose

No implementation may treat a library default, broker behavior, or image tag as a stable contract. This dossier defines the compatibility record required before an external boundary is enabled.

## Version matrix

Populate values only from an approved artifact, lockfile, sandbox capture, or integration test.

| Boundary | Required value | Evidence source | Compatibility result | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| Java runtime | `JAVA_VERSION_TO_BE_PINNED` | Runtime image/build record | Must match Flink/Fluss clients | Platform | Open |
| Python runtime | `PYTHON_VERSION_TO_BE_PINNED` | Runtime image/build record | Must match Executor dependencies | Platform | Open |
| Flink server/image | `FLINK_VERSION_TO_BE_PINNED` | Official artifact/digest | Must match job API and connector | Platform | Open |
| Flink Java API | `FLINK_JAVA_API_VERSION_TO_BE_PINNED` | Dependency lock | Must match server | Platform | Open |
| Fluss server | `FLUSS_SERVER_VERSION_TO_BE_PINNED` | Official artifact/digest | DDL/features tested | Platform | Open |
| Fluss Java client | `FLUSS_CLIENT_VERSION_TO_BE_PINNED` | Dependency lock | Must match server | Platform | Open |
| Fluss Flink connector | `FLUSS_CONNECTOR_VERSION_TO_BE_PINNED` | Dependency lock | Must match Flink and server | Platform | Open |
| Broker market protocol | `BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED` | Official spec/capture corpus | Decoder compatibility | Ingestion | Open |
| Broker postback protocol | `BROKER_POSTBACK_PROTOCOL_TO_BE_PINNED` | Official spec/sandbox capture | Capture compatibility | Action Capture | Open |
| OpenAlgo API | `OPENALGO_API_CONTRACT_TO_BE_VERIFIED` | API spec/sandbox | Request/response/retry behavior | Execution | Open |
| OpenObserve ingestion | `OPENOBSERVE_INGESTION_CONTRACT_TO_BE_VERIFIED` | API/runtime test | Telemetry delivery/redaction | Operations | Open |
| Base images | `*_IMAGE_DIGEST_TO_BE_PINNED` | Registry digest/SBOM | Reproducible builds | Platform | Open |

## Compatibility classifications

| Class | Meaning | Allowed use |
| --- | --- | --- |
| `COMPATIBLE` | Tested behavior and state/wire format are compatible | Normal implementation/release |
| `COMPATIBLE_WITH_LIMITATION` | Tested with explicit limitation and mitigation | Acceptance/sandbox; production only with approval |
| `INCOMPATIBLE` | Behavior or format cannot be safely combined | Block deployment |
| `UNKNOWN` | Evidence missing | Adapter/scaffold only; blocks live money |
| `NOT_APPLICABLE` | Boundary is not used | Must include rationale |

## Required capability evidence

Before DDL or service behavior is called validated, test the exact matrix for:

- Fluss `BYTES`/VARBINARY behavior.
- LOG and KV table creation and reads/writes.
- Primary key and bucket-key rules.
- `partial_update` merge semantics and column ownership.
- FULL changelog image behavior.
- Connector checkpoint and restore semantics.
- Cross-table visibility/atomicity limits.
- Retention and lake-tiering options.
- Replication/quorum and failover behavior.
- Flink state backend, checkpoint, savepoint, and rescale compatibility.
- OpenAlgo request schema, authentication, timeout, response, broker identity, and idempotency behavior.
- Broker event/postback identity, timestamps, status values, limits, reconnect, replay, and client-reference echo behavior.

## Evidence artifact format

Store one record per matrix row:

```text
compatibility_id: COMP-<boundary>-<number>
boundary: <component/interface>
versions: <all relevant versions and image digests>
source_artifact: <spec URL, capture hash, dependency lock, or test path>
fixture: <fixture/capture/test dataset>
scenario: <behavior exercised>
result: COMPATIBLE | LIMITED | INCOMPATIBLE | UNKNOWN
observed_behavior: <fact, not assumption>
limitations: <remaining risk>
owner: <team/person>
date: <UTC date>
```

## Implementation rules

- Runtime configuration must fail if a required version is absent or uses `latest`.
- An adapter may expose only behavior proven by the evidence record.
- Unknown broker fields must remain unavailable rather than being synthesized.
- A protocol change increments the decoder/schema version and requires replay tests.
- A connector change requires checkpoint, duplicate, out-of-order, partial-visibility, and restart tests.
- A version change affecting state or wire format requires migration classification and rollback/readability evidence.

## Completion checklist

- [ ] Every matrix row has an owner and evidence method.
- [ ] Exact versions/digests are recorded.
- [ ] All unknown protocol fields have explicit blockers.
- [ ] Fluss/Flink capability tests pass for the selected versions.
- [ ] OpenAlgo sandbox evidence proves response and correlation behavior.
- [ ] Broker packet/postback corpus is versioned and reproducible.
- [ ] CI rejects mutable image tags and unpinned dependencies.
- [ ] Release record links this matrix to `plan.md` Phase 2 and Phase 12.
