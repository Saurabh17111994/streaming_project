# Test Strategy

<!-- markdownlint-disable MD013 -->

## Status and authority

This strategy is implementation-ready, but executable tests and runtime evidence are not yet present. Detailed test IDs, records, fault points, and campaigns are defined in [`../08_implementation/testing/01-test-catalog.md`](../08_implementation/testing/01-test-catalog.md).

Mandatory release evidence is defined in [`../08_implementation/testing/02-release-evidence.md`](../08_implementation/testing/02-release-evidence.md).

## Goals

The test program must prove:

- Deterministic behavior for versioned inputs and state.
- Honest delivery/consistency boundaries.
- Bounded memory, state, backlog, and recovery.
- No duplicate broker order across crash windows.
- Safe halt under every defined uncertainty trigger.
- Schema, protocol, checkpoint, and deployment compatibility.
- Production workload, node-loss, offload, security, and audit guarantees.

## Test levels

| Level | Purpose | Environment |
| --- | --- | --- |
| Unit | Pure decode, canonicalization, transition, ranking, validation logic | Build runner |
| Flink harness/state | Event time, operators, timers/state, checkpoint/replay | Test JVM |
| Component integration | Fluss client, schema, persistence, service boundary | Pinned local stack |
| End-to-end | Tick → decision → sandbox order → postback → position | Acceptance environment |
| Failure/recovery | Crash windows, partial writes, restart, corruption, gaps | Fault-enabled acceptance |
| Performance | Baseline, burst, stress, latency/backpressure | Production-like Swarm |
| Chaos/DR | VM/node/store/network/credential loss | Production-like Swarm |
| Security | Network, identity, secret, authorization, audit controls | Acceptance/production-like |

## Evidence rules

Every test records its ID, requirement and audit issue IDs, exact versions/digests, fixture/workload checksum, environment, UTC/monotonic clock evidence, expected/observed result, artifacts, owner, and date.

A skipped, flaky, or missing mandatory test fails the gate until explicitly dispositioned with evidence. Passing unit tests cannot substitute for connector, crash-window, capacity, or recovery evidence.

## Release-blocking suites

- Broker packet/postback and OpenAlgo compatibility.
- DDL schema and Fluss/Flink connector compatibility.
- Checkpoint/savepoint restore and state evolution.
- Immutable identity, stale-update, and projection recovery.
- Executor attempt, unknown outcome, fencing, reconciliation, approval, and audit.
- Full workload envelope and one-VM loss.
- EOD offload/retention and audit reconstruction.
- Network, secret, authorization, encryption, image, and observability tests.

## Traceability

Every `REQ-*` and P0/P1 audit issue must map to at least one executable test/evidence record in [`../08_implementation/99-traceability.md`](../08_implementation/99-traceability.md). Documentation-only completion does not satisfy this strategy.
