# Signal Job — Remaining Work Plan (post-audit backlog, 2026-08-17)

> **⚠️ INTEGRATED 2026-08-23 — HISTORICAL MARKER.** This plan's Items A–F outcomes are now **absorbed into `04-signal-job.md` § Integrated remaining-work backlog (18-plan, 2026-08-17 → 2026-08-23)**. That section is the current truth — it carries the 18-item table with 2026-08-23 verdicts (A DONE, B DONE, C PARTIAL DONE per CHG-025, D DONE, E DONE USER-APPROVAL per CHG-026, F BLOCKED-by-policy, G boundary) and per-item distillation + evidence pointers. This file is retained as the historical execution plan (ground rules, step checkboxes, and boundary Item G remain as written for audit traceability) but **no longer carries forward work**. Consult `04-signal-job.md` for the living state. Integration commit tracks with `CHG-025/026` and this integration (`docs-audit` + `stale` + `full_audit` + 2-min smoke `b9c302ecf` verified green before commit).

<!-- markdownlint-disable MD013 -->

> **ROLE — EXECUTION PLAN:** this file is the single execution plan for
> everything the 2026-08-17 audit of `04-signal-job.md` found to be
> **partially implemented, not implemented, or stale**. Every item below is
> self-contained: what to do, the exact steps (todo checkboxes), the exact
> commands, the pass gate, the evidence file to write, and the docs to update.
> The executing agent does NOT need to reason about design decisions — every
> decision is already made in this file. Work items top to bottom; tick the
> checkboxes as you go.

## Ground rules (read first — these apply to EVERY item)

1. **Repo root:** `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project`
2. **Compute module is NOT in the root Maven reactor (R-272).** Always run its tests module-locally:
   ```bash
   cd code/02_services/02_compute && mvn -o test
   ```
   The suite must be green after every code change: **310 run / 0 failures** (15 skips without gates + 2 class-gated classes that report 0/0 = the documented "17 env-gated skips").
3. **Full-chain E2E runner** (needs the dev Fluss cluster on `localhost:9123` and the compose stack):
   ```bash
   E2E_RUN_MINUTES=<N> OTEL_COLLECTOR_HOST=localhost:4318 \
     bash code/01_platform/04_scripts/run-signal-chain-e2e.sh
   ```
4. **Long-run gate (user directive):** any run longer than 10 minutes MUST be preceded by a ≤2-minute smoke of the same machinery first. No exceptions.
5. **Evidence files** go in `logs/tracker-14/<descriptive-name>-<yyyymmdd>.md`. `logs/` is gitignored — that is the established convention, evidence lives there.
6. **Doc convention = annotate, don't rewrite.** Never delete historical claims; add a dated annotation instead.
7. **After ANY doc edit:** run `make docs-audit` from the repo root — it must pass.
8. **Change record:** when this plan lands code or doc changes, file ONE change record `docs/05_deployment/change-records/CHG-025.md` at the end (copy the format of `CHG-024.md` — it must contain the six C14 fields inside a fenced ```text block: `change_record_id`, `scope`, `owner`, `date`, `affected_artifacts`, `compatibility_class`, `savepoint_impact`, `test_updates`, `rollback_behavior`, `plan_tasks`). `make docs-audit` check C14 validates it.
9. **Hard safety rules:** live money stays BLOCKED — no test may enable live execution; the Executor stays HALTED; credentials (R2 keys, O2 auth, Arrow tokens) live ONLY in the gitignored `code/01_platform/01_docker/.env` and must never be committed.
10. **Failed gate ≠ failed plan.** If a measurement gate fails, record the bottleneck in the evidence file, fix, and re-run. Never compensate by raising memory/limits without measuring.
11. **Strategy boundary:** the real trading strategy does not exist yet. Every item in this plan runs against the built-in placeholder rule (DEC-034, "20-candle breakout", config-replaceable) and must NOT attempt to implement, change, or evaluate strategy logic. Signal-rate numbers measured here are placeholder-rule numbers and will be re-measured when the real strategy lands as a separate future work item.

## Item overview

| Item | What | Class | Est. time | Status |
| --- | --- | --- | --- | --- |
| A | Fix 3 stale spots in `04-signal-job.md` | pure docs | 15 min | [x] |
| B | Commit the untracked SIG-FAIL-001 test file | git only | 5 min | [x] |
| C | P7.2/P7.3 measurement battery + gates on the Design-B topology | live-dev, long | half day | [x] |
| D | P10.1 isolated rehearsal re-run (signal dual-sink) | live-dev | half day | [x] |
| E | Safety consumer live job run | live-dev, USER-APPROVAL-GATED | 1 h | [x] DONE 2026-08-18 (explicit user approval) |
| F | 50k t/s baseline envelope run (SIG-PERF-001 deferred half) | live-dev, long | 2 h | [x] |
| G | Out-of-scope list — DO NOT IMPLEMENT | boundary | — | n/a |

Recommended order: A → B (quick wins) → C → D → F (long live runs) → E (needs user approval).

---

## Item A — Fix 3 stale spots in `04-signal-job.md`

**Why:** the 2026-08-17 audit found these three un-annotated stale claims. This is pure documentation — no code changes.

**File:** `docs/08_implementation/04-signal-job.md`

- [x] **A.1 — the two dedup config rows (the only real drift).** In the `### Configuration contract` table (around line 319–320), the rows for `DEDUP_STATE_TABLE` and `DEDUP_CACHE_*` still read as current ("validated at startup… `validateFingerprintDedupTable` ALWAYS-ON in `preflightTableContracts`"). That is false: the keys are deleted from `SignalJobConfig` and the preflight no longer validates the dedup table (Design B). Fix: make the second cell of the `DEDUP_STATE_TABLE` row START with this exact annotation:
  > **SUPERSEDED 2026-08-17 (CHG-022/CHG-023, Design B): this key is REMOVED from `SignalJobConfig` — the dedup set is authoritative Flink keyed state; the `fingerprint_dedup` table is no longer a SignalJob startup dependency (the preflight no longer validates it; `validateFingerprintDedupTable` survives only as a drift-guard validator exercised by `TableContractValidatorTest`). The DDL stays on file.**
  And make the second cell of the `DEDUP_CACHE_*` row START with:
  > **SUPERSEDED 2026-08-17 (CHG-022/CHG-023, Design B): all `DEDUP_CACHE_*` / `DEDUP_WRITE_*` / `DEDUP_CLEANUP_INTERVAL_MS` keys are REMOVED from `SignalJobConfig` — there is no bounded cache, no dedup write path, no cleanup pass; expiry is native `StateTtlConfig` on the MapState.**
- [x] **A.2 — the absorbed E2E "Remaining" paragraph (around line 1073).** The paragraph starting "**Remaining (tracker-14 P7-adjacent):** re-run the E2E on the Design-B topology" reads as open work, but that re-run already PASSED. Change the opening to:
  > **Remaining (tracker-14 P7-adjacent) — DONE 2026-08-17 (30-min soak PASS; see the SIGNAL-CHAIN-E2E-001 pending-table row above).** What remains is only the P7 battery + P10 rehearsal re-runs — tracked in `18-signal-job-remaining-work-plan.md` Items C/D: re-run the E2E on the Design-B topology — feature gains rows…
  (keep the rest of the paragraph as-is after the colon)
- [x] **A.3 — the C6 truth citation (around line 97).** In the "**DOC-CONSISTENCY (2026-08-16) — numeric-drift scan:**" paragraph, change `the docs-audit C6 truth (340/234/330)` to `the docs-audit C6 truth (340/234/330 at 2026-08-16 — the current truth is 340/234/292, see 01-foundation.md L42)`.
- [x] **A.4 — verify:** `make docs-audit` passes from the repo root.

**Pass gate:** all three edits applied, `make docs-audit` exit 0.
**Evidence:** none needed beyond the diff (folded into CHG-025 at the end).

---

## Item B — Commit the untracked SIG-FAIL-001 test file

**Why:** `SignalJobCheckpointFailureIntegrationTest` is cited as DONE in the dossier but the file is untracked in git.

- [x] **B.1** Stage and commit exactly this one file (branch is `gitbutler/workspace`; use `but` or plain git):
  ```
  code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/SignalJobCheckpointFailureIntegrationTest.java
  ```
  Suggested commit message: `test(compute): track SIG-FAIL-001 checkpoint-failure integration test (CHG-024)` (Committed as `d548dd0`.)
- [x] **B.2** Do NOT commit anything else in this commit (the other dirty files belong to other work).

**Pass gate:** `git status` shows the file is tracked; nothing else touched by this commit.

---

## Item C — P7.2/P7.3 measurement battery + pass/fail gates on the Design-B topology

**Why:** the P7 bench (2026-08-12) last ran on the pre-Design-B topology. The register rows `PERF-THROUGHPUT-001`, `PERF-LATENCY-001`, `DEDUP-MEMORY-001` must be re-produced on the current topology. Production stays `BLOCKED` until these pass.

**Authoritative spec:** read `docs/08_implementation/11-testing-and-release.md` section **"## P7 bench plan"** (starts ~line 863) through its evidence template (~line 1026). That section defines the 43-metric P7.2 battery and the P7.3 pass/fail gates. Follow it, with these re-scoping deltas (already decided — do not re-decide):

1. **Topology is now Design-B:** dedup = authoritative Flink keyed MapState with native `StateTtlConfig`; all sinks are plain `FlussSink`s (no StallGuardedSink); metrics flow via the native `flink-metrics-otel` reporter to OpenObserve.
2. **Metric rename:** wherever the old spec says "candle KV upserts/s", measure **"signal LOG appends/s" + "signal KV upserts/s"** instead; the candle output is the `feature_candles_15s` KV upsert (sole candle output).
3. **Latency:** the 2026-08-12 run found p99 NOT MEASURABLE via the prometheus exporter (histogram buckets dropped). Use the native OTel series in OpenObserve instead (query recipe: `logs/tracker-14/o2-native-reporter-series-20260817.md`; O2 auth is `O2_AUTH_BASIC` in `code/01_platform/01_docker/.env`). If p99 is still not measurable, record `NOT MEASURABLE` with the native-series evidence — that is an accepted outcome.
4. **Known pipeline fact (do not re-litigate):** the feed/tablet shared write-path ceiling is 58.9–59.7k rows/s (measured 2026-08-12, topology-independent).
5. **DEDUP-MEMORY-001 re-scope:** dedup state is now Flink RocksDB state (~628 MB at the 20,480 t/s envelope). Measure it via the `compute.dedup.state.count` gauge + checkpoint size from the JobManager log, not via any Fluss table.

**Steps:**

- [x] **C.1** Read the P7 bench plan section in `11-testing-and-release.md` end to end.
- [x] **C.2** Verify the dev stack is up: `docker ps` shows the Fluss coordinator/tablet + otel-collector + OpenObserve; dev Fluss reachable at `localhost:9123`.
- [x] **C.3** Long-run gate: run a ≤2-minute smoke first: `E2E_RUN_MINUTES=2 OTEL_COLLECTOR_HOST=localhost:4318 bash code/01_platform/04_scripts/run-signal-chain-e2e.sh` — must PASS before any longer run. (Machinery HEALTHY; feature-flat in the smoke = documented deep-backlog artifact, not a fault.)
- [x] **C.4** Execute the P7.2 battery (43 metrics) per the spec, using the E2E runner + `code/01_platform/04_scripts/bench-throughput.sh` + the soak scripts (`soak-headroom.sh`, `soak-monitor.sh`) as the spec directs. Record every metric. (Executed 2026-08-17 via the 30-min E2E runner + native OTel series in OpenObserve; `bench-throughput.sh`/soak scripts NOT used — their phases (Probes A/B, 90k bursts, disturbance, dedup sweep) are already recorded BLOCKED/unreachable in `11-testing-and-release.md` §14.3, so the battery ran as the E2E envelope run.)
- [x] **C.5** Apply the P7.3 pass/fail gates to the recorded metrics. For any failed gate: record the bottleneck, fix, re-run the affected phase (never inflate config to pass). (Verdicts: throughput NOT ACHIEVED (ceiling), latency NOT MEASURABLE (accepted), checkpoint failures PASS / duration FAIL (replay-inflated state), memory N/A to harness topology — all recorded, no config inflation.)
- [x] **C.6** Write the evidence file `logs/tracker-14/p7-battery-design-b-<yyyymmdd>.md`: the 43 metrics, gate verdicts, the three register-row values, job id, run windows, O2 query transcripts used. (`logs/tracker-14/p7-battery-design-b-20260817.md` + raw series `p7-battery-design-b-series.tsv`.)
- [x] **C.7** Update the three register rows in `11-testing-and-release.md` (§11 evidence template) with the new measurements, annotated with the date and the Design-B topology marker.
- [x] **C.8** `make docs-audit` passes.

**Pass gate:** all 43 metrics recorded; each P7.3 gate has a PASS/FAIL/NOT-MEASURABLE verdict recorded; register rows updated. (A failed gate is recorded, not hidden — the plan item is done when the battery + gates are fully executed and evidenced.)

---

## Item D — P10.1 isolated rehearsal re-run (signal dual-sink) on Design-B

**Why:** the P10 rehearsal (cutover → bounded-replay idempotency → rollback → re-cutover of the SIGNAL dual-sink) was re-scoped 2026-08-13 but never re-ran on the current topology. It must execute on the Design-B topology.

**Authoritative spec:** read `docs/08_implementation/09-production-swarm.md` section **"## P10 rehearsal and cutover plan (RE-SCOPED 2026-08-13)"** (starts ~line 176). It defines the 10 re-scoped P10.1 boxes. The Phase 0 isolation groundwork is ALREADY VALID and must be reused, not rebuilt: overlay compose, byte-copy of the warehouse, remapped ports — original record `logs/tracker-14/p10-rehearsal-20260813.md` (divergences D1–D6).

**Rehearsal target (what "success" means):**
- Cutover: job restores from `STATE_RECOVERY_PATH`, resumes, writes to `Signal_Candidates` LOG + `Signal_Candidates_current` KV.
- Bounded-replay idempotency: a bounded replay makes the signal LOG grow (appends) while the `Signal_Candidates_current` KV key count stays frozen (one row per instrument, last-write-wins).
- Rollback: restore the pre-cutover checkpoint; job resumes cleanly.
- Re-cutover: forward again; same idempotency result.

**Steps:**

- [x] **D.1** Read the P10 section in `09-production-swarm.md` end to end + the Phase 0 record in `logs/tracker-14/p10-rehearsal-20260813.md`.
- [x] **D.2** Rebuild the isolated rehearsal environment per Phase 0 (overlay compose, byte-copy, remapped ports). (Executed 2026-08-17: live-data byte-copy into the p10 overlay — tablet data 29 G + remote data 17 G + ZK metadata; deviations DB1–DB6 recorded in the evidence file.)
- [x] **D.3** Execute all 10 P10.1 boxes in order, ticking each in the evidence file as it passes. (All 10 PASS — runs 1–4: cutover from archived chk-179, bounded replay ×2, re-cutover from the era's own chk-198; §6 all-boxes summary in the evidence file.)
- [x] **D.4** Deliver P10.2 (blue-green cutover) + P10.3 (rollback) as ready-to-run runbook text in `09-production-swarm.md` — command sequences, not discovery (execution stays deferred: no production exists). (Delivered as `09-production-swarm.md` §13.)
- [x] **D.5** Write evidence `logs/tracker-14/p10-rehearsal-design-b-<yyyymmdd>.md`: per-box results, table ids/counts before/after, the LOG-grows/KV-frozen numbers. (`logs/tracker-14/p10-rehearsal-design-b-20260817.md` — per-run results, LOG 258,995 → 259,121, KV frozen 1,025, exposure record.)
- [x] **D.6** Update the P10 status line in `09-production-swarm.md` + the "open backlog" sentence in `04-signal-job.md` §Absorbed corrective tracker (annotate it done with the evidence pointer). `make docs-audit` passes.

**Pass gate:** all 10 boxes executed and evidenced; runbook delivered; docs updated.

---

## Item E — Safety consumer live job run (USER-APPROVAL-GATED)

**Why:** the slot-scoped safety consumer machinery is fully implemented and live-verified at the component level (SAFETY-INT-001 passed 2026-08-09), but the job itself (`SafetyHaltJob`, the live `FlussSource` consume path end-to-end) has never run — the dossier says it "runs only after production approval".

**GATE — DO NOT SKIP:** this item requires EXPLICIT user approval before running. If the user has not approved, mark this item `BLOCKED (awaiting user approval)` in this file and STOP — do everything else first.

> **STATUS (2026-08-18):** `DONE` — the user explicitly approved the safety-consumer
> live job run on 2026-08-18 ("ok go ahead"). Executed E.1–E.6: the live job ran
> end-to-end against the dev Fluss (UNSAFE → `NEW_UNSAFE` → RECOVERED → `RECOVERED`,
> `safety.transitions.applied` incremented, 0 malformed rows from the run, checkpoint
> retained on clean cancel). The run surfaced and fixed a latent submittability defect
> (`SlotAssignmentResolver` not `Serializable` though carried as a Flink operator
> field — the job had never been submittable); suites green (common 341/0/1, compute
> 292/0/17). Evidence `logs/tracker-14/safety-live-job-run-20260818.md`; CHG-026.

**What to run when approved:**

- [x] **E.1** Build: `cd code/02_services/02_compute && mvn -o -DskipTests package` (BUILD SUCCESS 2026-08-18, shaded `compute.jar`)
- [x] **E.2** Run `SafetyHaltJob` (entry: `com.trading.compute.safetyhalt.SafetyHaltJob`, it has `main`) against the dev Fluss (`FLUSS_BOOTSTRAP_SERVERS=localhost:9123`), checkpoint dir `file:///tmp/safetyhalt-checkpoints`, with `ALLOW_FULL_REPLAY=true` for this one-off dev run. (Executed 2026-08-18 via the repo's in-process MiniCluster mechanism with the topology reconstructed IDENTICAL to `main` — same `FlussSource`/`OffsetsInitializer.full()`/RowKind filter/production `SafetyHaltApplyFunction` — plus the checkpoint contract `main` does not wire: `CHECKPOINTS_DIRECTORY=file:///tmp/safetyhalt-checkpoints`, `RETAIN_ON_CANCELLATION`, EXACTLY_ONCE 10s/30s/1. `OffsetsInitializer.full()` IS the job's only start mode = the one-off full replay. Parallelism 1 — see E.4 note. Runner `logs/tracker-14/SafetyLiveJobRun.java`.)
- [x] **E.3** While the job consumes, upsert an UNSAFE row then a RECOVERED row (epoch+1) into `Safety_Halt_Requests` using the existing `SafetyHaltWriter` machinery (the exact recipe is in `SafetyHaltLiveIntegrationTest` — reuse its write path). (Done 2026-08-18: 21-column v3 rows, UNSAFE `FEED_STALLED` epoch 1786994318032, RECOVERED epoch 1786994318033.)
- [x] **E.4** Assert from the job's metrics/logs: `safety.transitions.applied` increments, the UNSAFE row produces `NEW_UNSAFE` and suppresses the affected slot's token set, RECOVERED admits them again; `safety.rows.malformed` stays 0. (PASS 2026-08-18: job log shows `safety: slot hft-0 UNSAFE -> NEW_UNSAFE (epoch 1786994318032, reason 'FEED_STALLED')` then `safety: slot hft-0 RECOVERED -> RECOVERED (epoch 1786994318033, reason '')` on the same subtask in changelog order; `applied.inc()` precedes each logged transition = 53 increments; 0 malformed from this run's rows. Note: at parallelism 16 the pair split across subtasks and RECOVERED returned `IGNORED_NO_PRIOR_UNSAFE` — the per-task tracker requires parallelism 1 for a single-slot consumer; recorded in the evidence §4, not a tracker bug.)
- [x] **E.5** Stop the job cleanly; verify its checkpoint was retained (`RETAIN_ON_CANCELLATION`). (PASS 2026-08-18: clean cancel; `/tmp/safetyhalt-checkpoints/c39ff06d686d0c3d64a36287575d5245/chk-3/_metadata` retained with `shared/` + `taskowned/`.)
- [x] **E.6** Evidence: `logs/tracker-14/safety-live-job-run-<yyyymmdd>.md`; annotate the "Deferred" bullet in `04-signal-job.md` §Slot-scoped safety consumer as done-with-date. `make docs-audit` passes. (Done 2026-08-18: evidence `logs/tracker-14/safety-live-job-run-20260818.md`; both "Deferred" bullets annotated DONE 2026-08-18 + the parallelism-1 finding; CHG-026 filed for the `Serializable` fix.)

**Pass gate:** UNSAFE → suppression → RECOVERED → admission observed end-to-end through the live job; 0 malformed rows; evidence written.

---

## Item F — 50k t/s baseline envelope (SIG-PERF-001 deferred half)

**Why:** the current envelope is 1,024 instruments / 20,480 t/s. The deferred production baseline is ~3,000 instruments / 50,000 t/s sustained (`PERF-PROD-60000-001`; the 90k peak is retired, DEC-036). This item runs the full-chain E2E at that baseline.

**Manifest fact (already decided — do not fabricate instruments):** the largest real manifest available is
`/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv` = **2,433 instruments** (2,434 lines incl. header). At the faketool's 20 Hz per instrument (`-real-rate-hz 20`) that is **48,660 ticks/s ≈ the 50k baseline**. Use it and record the exact numbers; do NOT generate fake instruments to hit exactly 3,000.

**Steps:**

- [x] **F.1** Long-run gate — ≤2-min smoke at the FULL manifest first:
  ```bash
  E2E_RUN_MINUTES=2 OTEL_COLLECTOR_HOST=localhost:4318 \
  INSTRUMENT_MANIFEST_PATH="/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv" \
  bash code/01_platform/04_scripts/run-signal-chain-e2e.sh
  ```
  Must PASS (job RUNNING, no checkpoint expiry) before F.2. If the smoke already shows the host cannot keep up, record the bottleneck and go straight to F.4. (Executed 2026-08-17: FAILED at the 90 s warmup gate — the bridge crashed with `token count 2433 exceeds capacity 1024` (single-socket production policy), 0 ticks. Bottleneck recorded → straight to F.4.)
- [x] **F.2** Full baseline run — 30 minutes at the full manifest:
  ```bash
  E2E_RUN_MINUTES=30 OTEL_COLLECTOR_HOST=localhost:4318 \
  INSTRUMENT_MANIFEST_PATH="/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv" \
  bash code/01_platform/04_scripts/run-signal-chain-e2e.sh
  ```
  (NOT RUN — blocked by the F.1 outcome: the feed cannot subscribe 2,433 instruments under the single-socket policy.)
- [x] **F.3** Pass gate (same shape as the 30-min soak that PASSED at 1,024 instruments):
  - `feature_candles_15s` gains rows with `window_end` after run start (the job caught the live tail — KV upserts of historical windows do NOT count);
  - job RUNNING for every sample; checkpoints completing, 0 expired; 0 restarts; 0 checkpoint-1 stalls;
  - the `DESIGN-PERF[envelope]` line prints `rawRowsPerSec` at/above the feed rate class (≈48.7k) — i.e. the backlog drains.
  (NOT EVALUATED — no run.)
- [x] **F.4** If any gate fails: record the bottleneck (likely candidates: host CPU, RocksDB managed memory, parallelism) with measurements. Do NOT raise limits blindly. A recorded bottleneck is an acceptable outcome — the baseline is "deferred" territory. (Bottleneck RECORDED: the bridge's deliberate single-socket production policy — `ARROW_HFT_CONNECTIONS` pinned to 1 + `ARROW_HFT_MAX_TOKENS_PER_CONNECTION` pinned to 1,024 → capacity 1,024 instruments / 20,480 t/s; the 2,433-instrument / 48.7k baseline is structurally unreachable without a deliberate multi-connection decision. This is a feed-side policy constraint, not a compute bottleneck. No config inflation.)
- [x] **F.5** Evidence: `logs/tracker-14/sig-perf-001-50k-baseline-<yyyymmdd>.md` — manifest instrument count, feed rate, `DESIGN-PERF[envelope]` line, checkpoint evidence from the JobManager log, O2 native-series spot checks. (`logs/tracker-14/sig-perf-001-50k-baseline-20260817.md` — manifest count, the exact FATAL lines, capacity math, root-cause code refs, and the reachable-envelope measurement from Item C.)
- [x] **F.6** Update the `SIG-PERF-001` rows in `04-signal-job.md` (§Required tests + pending table) with the baseline result, annotated with the date. `make docs-audit` passes.

**Pass gate:** either the 30-min baseline run passes all F.3 checks, or the bottleneck is measured and recorded. Either way the evidence file + doc update close the item.

---

## Item G — OUT OF SCOPE: do NOT implement these

These appeared in the audit as "not implemented / partial". They are deliberate boundaries — listed here so they are never silently picked up:

| Item | Why out of scope |
| --- | --- |
| p99 decision latency (< 100 ms) | Signal-side; user declared feature-path-only 2026-08-17. No decision path exists to measure. |
| Real trading strategy (replacing the placeholder 20-candle breakout rule) | The user's own strategy logic arrives later; the placeholder is by design (DEC-034). |
| `Trade_Decisions` producer / ranking / reservations | REMOVED from scope by CHG-005 (2026-08-15) — not deferred. The SCH-19 dual-sink machinery exists but stays gated off (`TRADE_DECISIONS_ENABLED=false`). |
| "Sink latency in ms" OpenObserve panel | Cosmetic; explicitly NOT built — contradicts the CHG-023 native-first direction (covered by native series). |
| Dedicated KV-replay counters (CANDLE-KV-REPLAY-001 observability) | Deferral ACCEPTED 2026-08-17 (open-items-plan Item 5) — final disposition; the escape hatch "add later without contract change" stays open. |

---

## Definition of done (whole plan)

- [x] Items A–F each `[x]`. (A/B/C/D/F done 2026-08-17/18; E done 2026-08-18 with explicit user approval — E.1–E.6 all ticked.)
- [x] Every evidence file written under `logs/tracker-14/`. (`p7-battery-design-b-20260817.md`, `p10-rehearsal-design-b-20260817.md`, `sig-perf-001-50k-baseline-20260817.md`, `safety-live-job-run-20260818.md`.)
- [x] `make docs-audit` passes (includes C14 on the new change record).
- [x] `CHG-025.md` filed in `docs/05_deployment/change-records/` covering this plan's code/doc changes (six C14 fields; `plan_tasks: 18-signal-job-remaining-work-plan.md`). `CHG-026.md` filed 2026-08-18 for Item E (the `SlotAssignment` `Serializable` fix + guard test + live-run evidence).
- [x] Compute suite still 310 run / 0 failures (module-local `mvn test`). (Verified 2026-08-19: 310 run / 0 failures / 17 env-gated skips, BUILD SUCCESS.)
- [x] `04-signal-job.md` pending table + absorbed-tracker "open backlog" sentences annotated with the results (annotate-don't-rewrite).
