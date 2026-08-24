# B2 — Crash-exactly-once (T5 fence proof) (2026-08-21)

Master-plan Task B2 — DoD: duplicate-order impossible by construction; fence test green.

**What was proven**

- `B2.1` — `tests/crash_exactly_once.rs`: mid-flight `kill -9` → restart on shared durable memory → replay halts with ZERO duplicate broker calls; single durable attempt record survives.
- Protocol finding: epoch bump CANNOT fence an unresolved attempt (quarantine holds); reconciliation-by-evidence then re-enable resumes flow — recorded as the design constraint.
- `B2.2` — runs against the existing durable protocol traits (`AttemptStore`/`GateStateStore`/`BridgeCaller` + `InMemory*` stores mirroring the Java Fluss-backed stores). Fluss-backed store swap is Workstream D; traits unchanged by D.
- `B2.3` — full suite green: 136 passed / 0 failed. Compose drill not needed (pure in-process proof; DR-005 covers gateway crash/restart at compose level).

**Disposition**

Duplicate order impossible by construction on the replayed attempt; the unresolved-attempt quarantine is the honest cross-crash escalation path.

**Evidence**

- Change record: CHG-063.
- Files: `code/02_services/04_executor/tests/crash_exactly_once.rs`.
