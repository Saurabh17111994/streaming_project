# B7 — In-service durable write path (four clients, flag-gated) (2026-08-21)

Master-plan Task B7 — DoD: four clients implemented + tested behind flags; enabling is a recorded human decision.

**What was done**

- `B7.1` — read §durable-clients design + CHG-052: no `DURABLE_*` flags existed; executor used `InMemory*` stores; Fluss-backed stores exist on the Java gateway side (CHG-051/052) but were not consumed durably on the Rust side.
- `B7.2` — new `src/durable.rs`: 4 clients (gate/attempts/journal/audit) behind `DURABLE_GATE/ATTEMPTS/JOURNAL/AUDIT_ENABLED` (all default OFF). Gate/attempts reuse `GateStateStore`/`AttemptStore` traits + `InMemory*` stores; journal/audit are new append-only traits with in-memory offline impls. `DurableClients` bundles them as `Rc`-shared handles for restart sharing; live Fluss/file/OTel impls swap at Workstream D.
- `B7.3` — 8 durable tests (gate/attempt/journal/audit write→restart→recovered; flag OFF vs ON identical — no regression) + 1 config test (defaults + selective enable).
- `B7.4` — suite green 153 passed / 0 failed (was 144).
- `B7.5` — documented: default deployment keeps all OFF until B6 sign-off (DEC-044); enabling any flag in compose requires explicit user approval recorded in the enabling CHG.

**Disposition**

Implemented + tested behind fail-closed flags; runtime enablement deliberately gated on the human DEC-044 release review (B6).

**Evidence**

- Change record: CHG-065.
- Files: `code/02_services/04_executor/src/durable.rs`.
