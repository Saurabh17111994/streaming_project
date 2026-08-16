# Project Layer Index

This directory is the project-level architecture and governance layer for the Streaming Trading Data Platform. It explains **why the platform exists, what is currently decided, and which quality bars matter**. It does not replace detailed requirements, contracts, DDLs, deployment manifests, or code.

## Reading order

1. [Project charter](./01-project-charter.md) — purpose, scope, goals, and non-goals
2. [System context](./02-system-context.md) — current topology, ownership, identities, and data flow
3. [Quality targets](./03-quality-targets.md) — measurable SLOs and delivery guarantees
4. [Decisions](./04-decisions.md) — active architectural decisions and superseded choices
5. [Risks and assumptions](./05-risks-and-assumptions.md) — owned validation register
6. [Delivery scope](./06-delivery-scope.md) — MVP and later phases

## Authority hierarchy

When documents disagree, use this order:

1. **Executable implementation and tests** — actual behavior
2. **Explicit active architectural decisions in [Decisions](./04-decisions.md)** — accepted direction for resolving contradictions
3. **Authoritative DDLs** — `../../code/01_platform/02_sql/ddl/*.sql`
4. **Build contracts** — `../04_contracts/`
5. **Detailed requirements** — `../02_requirements/`
6. **Other project summaries and historical prose**

An active project decision is not implementation proof. After a decision changes a schema, protocol, or runtime contract, the corresponding DDL, contract, requirement, and code must be reconciled before implementation is considered complete.

A project-layer statement must link to the lower-level source that makes it implementable. A summary must not silently redefine a schema or protocol.

## Project status

| Phase | Status | Date |
| --- | --- | --- |
| Phase 4.1 — Design and architecture | Design closed | 2026-07-23 |
| Phase 4.2 — MVP implementation | Implementation active | 2026-07-23 |
| Runtime validation | Runtime validation pending | — |
| Live-money placement | Live-money blocked | — |

Core unresolved risks are recorded in [Risks and assumptions](./05-risks-and-assumptions.md).
The previous monolithic design is retained as a compatibility redirect: [project-design.md](./project-design.md).

## Scope of this revision

This project layer standardizes the following decisions:

- Docker Compose for local development and Docker Swarm for production
- ~~In-operator ranking in the signal Flink job~~ — **REMOVED 2026-08-15 (CHG-005); no separate Ranking deployment**
- Three order identities: `instruction_id`, `client_order_ref`, and `broker_order_id`
- Best-effort bounded tick deduplication using an event fingerprint when no broker sequence is available
- Explicit order-gate safe-halt and broker reconciliation
- Honest distinction between exactly-once stream state and external broker side effects
