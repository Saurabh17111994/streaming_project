# Streaming Trading Data Platform

<!-- markdownlint-disable MD013 MD060 -->

Real-time market-data → feature → trade-execution pipeline:
broker ticks → **Fluss** (streaming bus + storage) → **Flink** (features + strategy) →
trade instructions → **OpenAlgo** (dumb executor) → broker → exchange.
Fills are captured independently by a separate service.

> Repository layout follows a **Spec-Driven Development (SDD)** structure.
> This README is a discoverability map — see each directory's docs for detail.

---

## Repository map

```text
streaming_project/
├── docs/        long-lived documentation — what the system IS
├── specs/       active implementation planning — what we're BUILDING
├── code/        production implementation — what is BUILT
├── Makefile     common dev commands (make up / down / ddl / build)
└── CLAUDE.md    agent instructions (symlink)
```

### `docs/` — reference documentation

Stable, long-lived. Read these to understand the system.

**Read in numbered order, 01 → 08:** project → requirements → architecture → contracts → deployment → operations → testing → implementation dossiers.

| Directory | Contents | Read when |
| ----------- | ---------- | ----------- |
| `01_project/` | `project-design.md` — macro design, boundaries, closed decisions | First read |
| `02_requirements/` | 6-layer requirements (context, functional per-segment, NFRs, data, interfaces, operational) | Designing/implementing any component |
| `03_architecture/` | Architecture overview, technology choices, data pipeline, networking, security, platform topology | Understanding the system shape |
| `04_contracts/` | One build-ready contract per atomic segment (ingestion, storage, compute, business-logic, babysitter, action-capture, executor, observability, platform-runtime) | Implementing a segment |
| `05_deployment/` | Release strategy, CI/CD, environments, rollback, secrets rotation | Deploying |
| `06_operations/` | Operational strategy, runbooks, SLO dashboards, alert config, DR plan, maintenance | Running in production |
| `07_testing/` | Test strategy, unit, integration, throughput, data-quality, chaos tests | Writing/running tests |
| `08_implementation/` | Implementation dossiers, state machines, config placeholders, test IDs, release evidence | Before changing production code |

### `specs/` — active planning

The *work in progress*. Separate from stable reference docs.

| File | Purpose |
|------|---------|
| `roadmap.md` | Build phases 4.1 → 4.7 (process) |
| `status.md` | Current focus & open decisions (working status only) |

> **Active implementation planning:** `plan.md` is the master checklist. Use `docs/08_implementation/` for implementation-ready dossiers before changing `code/`. This documentation-first workflow keeps requirements, schemas, interfaces, tests, operations, and release evidence aligned before production code is written.

### `docs/08_implementation/` — implementation-ready dossiers

This is the bridge between stable requirements/contracts and production code. It defines internal module boundaries, state machines, schemas, configuration placeholders, failure behavior, observability, test IDs, and release evidence. These documents do not claim that implementation or runtime validation already exists.

Read `docs/08_implementation/00-index.md` first.

### `code/` — production implementation

Code is not yet implemented. The service directories currently contain scaffolding and entry-point placeholders. Use the corresponding `docs/08_implementation/components/` dossier before writing code.

```text
code/
├── 01_platform/
│   ├── 01_docker/               local Compose scaffolding
│   ├── 02_sql/ddl/              reconciled DDL proposals; version validation pending
│   └── 03_fluss/                Fluss configuration scaffolding
└── 02_services/
    ├── 01_ingestion/            ingestion scaffolding → raw_table_1
    ├── 02_compute/              Signal + Babysitter job scaffolding
    ├── 03_action_capture/       postback/lifecycle/position scaffolding
    └── 04_executor/             Executor scaffold; starts disabled and halted
```

**Schema status:** `code/01_platform/02_sql/ddl/*.sql` contains reconciled proposals, but they are blocked from runtime application until the pinned Fluss/Flink compatibility and schema lifecycle tests pass. See `docs/08_implementation/03-schema-lifecycle.md`.

---

## Quick start

```bash
make env      # copy .env.example -> .env, then edit secrets
make up       # docker compose up -d (full stack)
make ddl      # display the currently blocked DDL workflow; do not treat as applied until schema validation is implemented
make logs     # tail compose logs
make down     # stop
```

## Status

Phase 4.1 (Design & architecture) — **closed**.
Phase 4.2 (MVP) — **next**. See `specs/status.md`.
