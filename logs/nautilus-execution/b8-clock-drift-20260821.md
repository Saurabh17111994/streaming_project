# B8 — Clock-drift safety enforcement (2026-08-21)

Master-plan Task B8 — DoD: declared limit actually enforced end-to-end; halt-on-drift proven by test; no silent tolerance of skew.

**What was done**

- `B8.1` — found: `CLOCK_OFFSET_LIMIT_MS` (default 200) existed ONLY in compose's ingestion service block (enforced there by Java `NtpClockChecker`); executor + gateway configs had zero drift/skew/NTP keys.
- `B8.2` — new `src/clockwatch.rs`: `DriftMonitor.enforce()` — `Beyond`/`Unmeasurable` → `Gate::safety_halt()` + `tracing::error!`; recovery ONLY via sanctioned path (proven by test). Config reads `CLOCK_OFFSET_LIMIT_MS` (default 200); bootstrap exposes `Runtime::enforce_clock_drift()`. Production NTP source = Workstream D behind an `OffsetSource` trait.
- `B8.3` — 7 monitor tests: ±200/±201 boundary symmetric, fail-closed on probe error, halt clears approvals, idempotent breaches, within-limit no-op, sanctioned-path-only recovery (incl. rejected direct transition + zero auto-recovery) + config default/override test.
- `B8.4` — suite green 144 passed / 0 failed (was 136). Java side not needed (gap was execution-side only).

**Disposition**

Enforced end-to-end in the executor; the declared limit is no longer decorative.

**Evidence**

- Change record: CHG-064.
- Files: `code/02_services/04_executor/src/clockwatch.rs`.
