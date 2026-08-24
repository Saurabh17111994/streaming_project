# B5 — Babysitter live observation drill (offline half) (2026-08-21)

Master-plan Task B5 — DoD: babysitter observes silently; restart-safe; fails closed if enabled.

**What was verified**

- `B5.1` — `BabysitterConfigTest` 4: fail-closed (`true` → `IllegalStateException`), disabled-by-default; `BabysitterJobTest` 4: observation-only graph, RowKind filter, keyed ValueState, no-op discard for non-Position rows.
- `B5.2` — `RETAIN_ON_CANCELLATION` + `BABYSITTER_STATE_RECOVERY_PATH → SAVEPOINT_PATH` wiring verified; `BabysitterPositionsRestoreIntegrationTest` exists (env-gated `COMPUTE_INT_TEST_T7`, correctly 1 skipped without a cluster).

**Disposition**

Offline half verified; live position-write storm + zero `Position_Actions` rows + live restore deferred to D-era (recorded).

**Evidence**

- Change record: CHG-072.
- Files: `code/02_services/02_compute/.../BabysitterJobTest.java`, `BabysitterConfigTest.java` (test module).
