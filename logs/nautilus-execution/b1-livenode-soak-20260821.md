# B1 — LiveNodeRuntime long-run soak (FakeBridge) (2026-08-21)

Master-plan Task B1 — DoD: soak green; leaks = 0; duplicate-run guard proven.

**What was proven**

- `B1.1` — `tests/live_node_soak.rs` (`live_node_runtime_sustained_soak`, env-tunable `SOAK_SECS`, default 30 s dev leg / 1800 s evidence leg): gate-boots-HALTED, liveness sampling, clean stop via handle, duplicate-run guard, fd/RSS leak bounds.
- `B1.2` — evidence leg: `SOAK_SECS=1800 cargo test --offline --test live_node_soak` — exit 0 in 1810.01 s (boot 12:23:40Z → stop signal at exactly 1800 s → graceful shutdown). Dev leg + full suite green (136/0).
- `B1.3` — no leak growth across 30 min; duplicate-run guard rejected a second concurrent run.

**Disposition**

Run loop stable for 30 minutes with zero leaks; HALTED boot contract observed.

**Evidence**

- Change record: CHG-062.
- Files: `code/02_services/04_executor/tests/live_node_soak.rs`.
