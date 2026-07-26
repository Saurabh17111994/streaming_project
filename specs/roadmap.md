# Roadmap — Build phases & process

> Companion to `project_design.md`. This file covers *how we build* (phases, process) — not *what we build* (that lives in the design doc).
> Last updated: 2026-07-20

---

## How we work

1. Pick one phase.
2. Discuss it until its open questions are answered.
3. Enrich the design (`project_design.md`) with the decisions.
4. Repeat until all phases are enriched.
5. The enriched design becomes the macro guide that directs all later implementation.

Phases are organized on a lifecycle / maturity axis — each raises the bar on the same system; they are not tools or a strict build order.

## The phases (4.1 → 4.7)

### 4.1 Design & architecture — CLOSED

Decide what the system is, what it computes, where data lives, who reads it.
Deliverable: architecture diagram, table designs, job designs, agreed NFRs.
*All macro sub-topics closed; see `project_design.md` §4.*

### 4.2 MVP

Smallest production-shaped vertical slice — real schemas, real storage, correct happy path, one reader.
Open questions: which reader first; minimum correct happy path; what is out of scope.
Deliverable: a working vertical slice that is minimal but right, not a toy.

### 4.3 Feature completeness

Reach functional parity with the target system — all compute, all storage tiers, all readers wired.
Open questions: where the ~240 feature columns are computed/stored; all storage tiers (log, KV, lake); all readers wired to one place.
Deliverable: a functionally complete system, unhardened.

### 4.4 Hardening

Correctness and reliability at scale, under failure.
Open questions: exactly-once verified; checkpoint/recovery drills; backpressure; schema evolution & replay; idempotency of every write.
Deliverable: a system that survives real conditions, with evidence.

### 4.5 Observability & operations

Know when the system is healthy, run it without surprises.
Open questions: infra health only, or also business/SLA metrics; dashboards, alerting thresholds, runbooks.
Deliverable: dashboards, alerts, runbooks.

### 4.6 Production cutover

Move real use off the old path onto the new system, safely.
Open questions: keep Parquet / migrate / parallel-run; parity bar; rollback plan.
Deliverable: a verified cutover with rollback, and a retired or relegated old path.

### 4.7 Evolution

Ongoing change after go-live — new instruments, features, readers, tuning, migrations.
Open questions: how new features are added without breaking readers; how the lake tier is pruned/compacted.
Deliverable: a maintainable system with a change process.
