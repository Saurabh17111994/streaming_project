# Project Design — Compatibility Entry Point

> This file previously contained the complete project design. The project layer was split into focused documents to reduce contradictory updates and make ownership easier to navigate.

Start with [Project Layer Index](./00-index.md).

## Current documents

| Document | Purpose |
| --- | --- |
| [Project charter](./01-project-charter.md) | Problem, goals, MVP scope, and non-goals |
| [System context](./02-system-context.md) | Current topology, component ownership, identities, and state boundaries |
| [Quality targets](./03-quality-targets.md) | SLOs, workload envelope, delivery guarantees, and safe-halt protocol |
| [Architectural decisions](./04-decisions.md) | Active decisions and superseded statements |
| [Risks and assumptions](./05-risks-and-assumptions.md) | Owned validation and mitigation register |
| [Delivery scope](./06-delivery-scope.md) | MVP acceptance gates and later phases |

## Key changes from version 1.3

- Ranking is in the signal Flink operator; it is not a separate deployment.
- The Executor consumes Fluss and owns the durable order gate; Arrow REST is the direct broker integration — there is no intermediate adapter layer.
- `instruction_id`, `client_order_ref`, and `broker_order_id` are distinct identities.
- Tick deduplication is bounded best-effort fingerprinting because no broker sequence/event ID is assumed.
- Suspected feed discontinuities replace unsupported exact missing-sequence ranges.
- Docker Compose is for local development; four-VM Docker Swarm is the production target.
- Exactly-once claims stop at the Flink/Fluss transaction boundary and do not include broker REST side effects.
- Order lifecycle, position lifecycle, and order-gate state are separate models.
- EOD offload requires a verification manifest and retention safety buffer.

## Revision history

| Version | Date | Summary |
| --- | --- | --- |
| 1.0 | 2026-07-21 | Initial structured project design |
| 1.1 | 2026-07-22 | Four-VM Swarm and HA target |
| 1.2 | 2026-07-22 | EOD Iceberg offload and analytics scope changes |
| 1.3 | 2026-07-22 | Arrow API assumptions revised |
| 2.0 | 2026-07-23 | Split project layer; reconciled ranking, runtime, identity, deduplication, safety, and delivery semantics |
