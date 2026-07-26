# Integration Test Plan

<!-- markdownlint-disable MD013 -->

## Status

Implementation-ready integration-test plan. Executable tests and pinned runtime evidence are not yet implemented.

## Required environments

1. Clean local Fluss/Flink test stack for schema and connector behavior.
2. Sandbox broker/OpenAlgo environment for protocol and response evidence.
3. Production-like four-VM Swarm for workload, HA, security, and recovery evidence.

## Required scenarios

- Catalog/database/table clean creation and effective DDL/options inspection.
- `BYTES` payload round trip and hash validation.
- LOG/KV reads/writes, primary keys, bucket routing, partial update, FULL changelog.
- Flink source/sink checkpoint, restart, restore, rescale, and cross-table visibility.
- Ingestion manifest/subscription/append acknowledgement.
- Signal raw source, event-time, dedup, candle, candidate, ranking, reservation, decision path.
- Action Capture immutable audit, correlation, lifecycle, position, quarantine, and independent-write recovery.
- Babysitter Positions changelog and strict no-op.
- Executor decision intake, owned-state writes, gate/attempt/mapping/audit, fencing, reconciliation, and two-person controls.
- S3 checkpoints/lake/offload and retention extension.

## Protocol evidence

Unknown endpoint paths, fields, limits, status values, timestamps, identity behavior, timeouts, and idempotency are blockers. The test harness must record actual observed behavior rather than encode a guessed contract.

## Acceptance

Each integration scenario has a fixed fixture, exact versions, failure classification, expected consistency boundary, and evidence artifact. Passing integration tests are required before production-like performance or live-money gate review.
