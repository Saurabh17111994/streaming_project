# CI/CD and Artifact Promotion

## Purpose

The delivery pipeline must prove that code, DDLs, contracts, images, state compatibility, and deployment definitions describe the same release. A successful image build alone is not release evidence.

The exact CI provider is implementation-owned. The required stages and gates apply regardless of provider.

## Artifact policy

Every releasable artifact has an immutable identity:

- Project source commit
- Dependency lock state
- Java/Python/toolchain versions
- Flink and Fluss versions
- Connector/plugin versions
- Broker protocol/decoder version
- Arrow REST/OpenObserve versions
- Container image digest
- DDL/schema version
- Strategy/ranking/configuration hash
- State/savepoint compatibility classification
- Swarm stack and Compose definition version

Production prohibits `latest`, floating image tags, and version ranges.

## Pipeline stages

### 1. Documentation and contract checks

Validate:

- Local Markdown links and required document structure
- Consistent identity/state vocabulary
- Requirement-to-contract traceability
- DDL/table names against data requirements
- No prohibited legacy assumptions such as overloaded `order_id`, broker sequence guarantees, separate Ranking deployment, mutable instructions, or external exactly-once claims

### 2. Static and supply-chain checks

Run applicable:

- Formatting, lint, compilation, and type checks
- Unit tests
- Secret scanning and redaction checks
- Dependency and image vulnerability policy
- SBOM generation
- License/policy checks
- Reproducible artifact metadata capture

A release artifact is signed or otherwise provenance-verifiable according to the approved platform policy.

### 3. Component tests

Test each owner boundary:

- Ingestion golden packets, raw-byte hash, normalization, quarantine, reconnect, and bounded buffering
- Signal job event time, deduplication, final candles, forming bars, ranking, reservation, and deterministic restore
- Action Capture duplicates, ordering, correlation, projection recovery, and position derivation
- Babysitter schema/continuity/checkpoint behavior with zero MVP actions
- Executor gate, attempts, unknown outcomes, mapping, reconciliation, approval, and fencing
- EOD manifest validation and retention extension

### 4. Integration and compatibility tests

Run against the exact pinned versions:

- Fluss DDL/type/property compatibility
- Flink source/sink/changelog/checkpoint behavior
- Partial-update and stale-write behavior where used
- Independent-write crash windows
- Schema and protocol version rejection
- Savepoint/checkpoint restore
- Upgrade and rollback compatibility
- Arrow REST/broker sandbox response and correlation behavior

Failure of an evidence-gated integration keeps live-money readiness blocked.

### 5. Production-like acceptance

Deploy immutable candidate artifacts to the production-like four-VM Swarm environment and run:

- Full-session variable 60,000 ticks/s average baseline and 90,000 ticks/s peak baseline
- One workload VM loss
- Checkpoint and object-store failure exercises
- Executor crash-window and duplicate-order tests
- Safe-halt under five seconds
- Data recovery under 30 seconds for accepted scenarios
- Full-volume EOD offload and manifest verification
- Security, secret rotation, alert, and audit reconstruction tests

## Promotion model

Artifacts move forward by immutable digest:

```text
build
  → local/component validation
  → integration/sandbox validation
  → production-like Swarm acceptance
  → approved production candidate
  → controlled production deployment while HALTED
  → reconciliation and two-person enablement
```

Rebuilding between environments creates a new artifact and restarts required promotion evidence.

## Deployment procedure

1. Confirm release record, digests, schema/state classification, and evidence links.
2. Confirm rollback artifact and state-readability path.
3. Halt the order gate for money-moving changes.
4. Reconcile outstanding attempts, orders, fills, positions, and offsets.
5. Deploy only where compatibility is proven; otherwise use a clean-break pre-production reset/replay plan.
6. Verify service liveness/readiness, Fluss quorum, Flink jobs/checkpoints, projections, changelog continuity, telemetry, and storage.
7. Keep the gate halted until post-deployment reconciliation completes.
8. Require two distinct authenticated approvals for the current evidence hash/epoch before enablement.

## CI evidence retention

Retain release evidence sufficient to reconstruct:

- Source and artifact provenance
- Version matrix
- Test results and workload profiles
- Schema/state compatibility decision
- Security and vulnerability disposition
- Deployment and rollback approval
- Gate transitions and operators
- Post-deployment verification

Money-moving execution evidence follows the seven-year encrypted audit policy. General CI logs follow the approved operational retention policy and must redact secrets and raw payloads.

## Failure policy

- A failed mandatory stage blocks promotion.
- A flaky or skipped release gate is a failure until dispositioned with evidence.
- CI cannot automatically enable the order gate.
- Observability failure cannot silently pass acceptance.
- Rollback uncertainty defaults the order path to `HALTED`.

## References

- Release strategy: `./00-release-strategy.md`
- Runtime contract: `../04_contracts/09-platform-runtime.md`
- Test strategy: `../08_implementation/11-testing-and-release.md`
- Operational requirements: `../02_requirements/06-operational.md`
