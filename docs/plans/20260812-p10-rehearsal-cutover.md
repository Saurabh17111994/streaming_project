# P10 — Candle KV Migration Rehearsal & Cutover Runbook (tracker 14)

**Status:** `PLANNED — not yet executed` (2026-08-12)
**File:** `docs/plans/20260812-p10-rehearsal-cutover.md`
**Tracker:** `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` — `## P10 — Operator-only migration and cutover` (L1144), `## P10.1 Isolated rehearsal` (L1150-1165), `## P10.2 Production blue-green migration` (L1167-1183), `## P10.3 Rollback` (L1185-1193).
**Sequencing gate:** the tracker says "Do not execute until P1–P9 code/evidence gates are complete" — this plan starts strictly AFTER the P7 bench (`docs/plans/20260812-p7-bench.md`) completes.
**Recipe source:** `skill://candle-kv-rollback-rehearsal` (B8.7 rollback procedure) + `CandleMigrationBatchJob`/`run-batch.sh` (batch audit/load) + `docs/plans/20260812-fix-r2-iceberg-lake-read-stall.md` (R2 containment).

## 1. Objective

Execute the full P10.1 isolated rehearsal on the dev host (all 14 boxes), deliver P10.2/P10.3 as a ready-to-run production runbook (execution deferred — no production exists), and leave the register/evidence trail so production cutover is a command sequence, not a discovery exercise. Dev cluster is the qualification target (user decision); "production objects" in P10.1 wording = the live dev stack's objects.

## 2. Locked spec (user decisions, 2026-08-12)

### 2.1 Scope decisions

| Dimension | Decision |
| --- | --- |
| Production definition | Dev cluster = qualification target; P10.2/10.3 delivered as a READY runbook, executed only when a real production deployment exists |
| Isolated env | Second compose project on this host: separate project name → separate network, container names, ports; live stack untouched |
| Rehearsal data | Full dev data + checkpoints (Fluss tablet segments + ZK metadata + archived checkpoint); R2 lake objects shared read-only (see §4.2) |
| Audit | Full lake+log union audit on the copied data (CANDLE-MIGRATION-002 recipe; 1,638,400-row contract, 16/16 buckets) |
| Load target | Destination row count == audit-derived approved distinct key count (the audit's distinct-key result, recorded in evidence) |
| Bounded replay | Migration load run TWICE against the same bounded source offsets — idempotency proof (KV identical after 2nd load) |
| Checkpoint source | Archived known-good checkpoint (47/47-complete 2026-08-11-era R2 checkpoint; archived before dev runs rotate it) |
| Rollback rehearsal | B8.7 FULL: rollback (dual-sink → single-LOG artifact, pre-cutover checkpoint, KV frozen / LOG grows) AND re-cutover (dual-sink from its own checkpoint) |
| Operator | Autonomous execution by the agent; user reviews evidence + approves register rows |
| Sequencing | Strictly after P7 bench completes (no parallel override) |
| Consumers | P10.2 "point current-state consumers to KV" marked VACUOUS today (no consumers exist) — delivered as a runbook step for when the downstream pipeline lands |
| DDL path | Identical gated path: `ddl_apply.py` + version-matrix gate, against the isolated env's coordinator |
| Exposure record | KV + LOG envelope: timestamp range + row counts + sampled keys written during the dual-sink window |

### 2.2 Isolation mechanism (design, verified against Fluss lake layout at execution)

- Fluss lake objects live at `s3://<bucket>/lake/<database>/<table>/...` (verified path layout: `lake/default/candle_scale_log/metadata/`). The isolated env creates its tables in a dedicated database (e.g. `rehearsal`) → its lake writes land at `lake/rehearsal/<table>/` — no R2 object collision with live `lake/default/...`.
- The union AUDIT reads the ORIGINAL `lake/default/...` objects read-only (no mutation), plus the copied log segments.
- The dual-sink rehearsal job writes new candles → they go to the rehearsal env's own tables (db `rehearsal`, distinct lake path + separate tablet data dirs).
- Checkpoint restore reads the ARCHIVED COPY (`s3a://.../p10-rehearsal/<run>/chk-N` + `shared/`), never the live checkpoint dir.

## 3. Prerequisites (checked at Phase 0 entry)

- [ ] P7 bench complete with evidence registered (`PERF-*` + `DEDUP-MEMORY-001` rows).
- [ ] Archived known-good checkpoint copied to a stable archive prefix (`s3a://…/p10-rehearsal/archive/`) BEFORE further live runs rotate it.
- [ ] Live stack healthy; no other rehearsal/bench in flight.
- [ ] R2 endpoint reachable; S3A timeout pins + outer-deadline containment available for every audit step.
- [ ] Fast smoke gate probes available on the rehearsal network (§4.1): `probe-r2.sh` + bounded-read probe + `KvCountProbe`.

## 4. Phase 0 — prepare isolated environment

1. **Archive the checkpoint:** copy the known-good R2 checkpoint tree (chk-N + `shared/` incremental SSTs) to the archive prefix; verify with `_metadata` read + a restore probe on a throwaway MiniCluster (CHECKPOINT-DURABILITY-001 recipe).
2. **Stand up the second compose project:** `docker compose -p rehearsal -f <copy of compose>` — project-name override isolates network/container names; remap published ports (9123/9124/9249/9250 → alternate host ports); same image digests; dev secrets.
3. **Copy Fluss data:** copy the live tablet data dirs + ZK metadata into the rehearsal volumes while the live writer is stopped (brief ingestion stop; restart after copy — record start/stop timestamps). Verify segment integrity (torn-tail truncation procedure if needed — `fluss-tablet-crash-loop-repair` skill pattern).
4. **Database/table setup in the rehearsal env:** create database `rehearsal`; provision `raw_table_1`/`feature_candles_15s`/`feature_candles_15s_current` via `ddl_apply.py` + version-matrix gate (identical path, offline/evidence-gated — no bootstrap at service startup; DdlBootstrap owns only registry tables).
5. **Load the copied history:** copy log segments into the rehearsal `raw_table_1`; lake tier attaches to the existing R2 objects (shared read for the audit, §2.2).
6. **Environment health gate:** coordinator/tablet healthy in the rehearsal network; O2/collector reachable (or a rehearsal-scoped metric sink); rehearsal `raw_table_1` visible with the full copied history.
7. **Fast smoke gate (≤ 2 min, §4.1):** `probe-r2.sh` against the rehearsal R2 path (`lake/rehearsal/...`) PASS + 30 s bounded log read on rehearsal `raw_table_1` returning the copied count + 30 s Table-API batch read smoke (the audit's exact read path, bounded to one bucket). Smoke fails → fix + re-smoke before Phase 1.

### 4.1 Long-run gate rule (user directive, 2026-08-12)

Any phase estimated > 10 min MUST be preceded by a ≤ 2-min smoke exercise of the SAME machinery that phase depends on: R2 lake read (`probe-r2.sh`) for audit/checkpoint steps, bounded log/batch reads for scan steps, checkpoint-restore probe for state steps. Smoke passes → run the long phase; smoke fails → fix + re-smoke. No blind long waits: the P3.5 R2 saga proved a 55-90 min audit can wedge with NO error while a 1-min probe catches the same failure in seconds. The smoke result (probe log path + exit code) is recorded in the evidence file as part of the run's proof.

## 5. Phase 1 — complete lake+log union audit (P10.1 boxes 4-5)

0. **Smoke (§4.1), immediately before the audit starts:** `probe-r2.sh` PASS + 30 s bounded log read — the R2 edge blackholes intermittently; the smoke costs 30 s, the audit costs up to 90 min.

1. Run the union audit (CANDLE-MIGRATION-002 recipe: `Iceberg HadoopCatalog` lake scan + `LogScanner` tail + full `LogScanner`) against the rehearsal copy, with the outer deadline (90 min) and R2 pins. Engine: current proven batch job (parallelism 1); the P3.6 efficiency follow-up is NOT required for this rehearsal.
2. Pass contract: `RESULT=OK`, `UNION_TOTAL==FULL_TOTAL` (dev contract 1,638,400 = 1,536,000 + 102,400 delta; rehearsal total recorded from the copied data), 16/16 buckets.
3. Resolve or exclude all conflicts: dev history already carries 25 hash-approved resolutions → expectation is ZERO new conflicts; any new conflict is recorded with field hashes and excluded (never silently dropped).
4. Record: audit evidence file (`logs/tracker-14/p10-rehearsal-<date>.md`), the derived **approved distinct key count** (= migration load target).

## 6. Phase 2 — migration load (P10.1 box 6)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + rehearsal `feature_candles_15s_current` readable (`KvCountProbe`) before the load.
1. Run `run-batch.sh` load against the rehearsal env (`FLUSS_BOOTSTRAP` → rehearsal coordinator; `CANDLE_TABLE` → rehearsal `feature_candles_15s_current`), bounded by the audit's source offsets.
2. Pass: destination row count == approved distinct key count; sampled values match the audit's canonical rows (spot-check per register requirement).

## 7. Phase 3 — dual-sink from copied checkpoint (P10.1 boxes 7-10)

0. **Smoke (§4.1):** the archived-checkpoint restore probe (Phase 0 step 1) re-run immediately before this phase — the checkpoint-restore machinery is what this phase exercises; `probe-r2.sh` PASS alongside (checkpoint lives on R2).
1. Submit the SignalJob (application mode, rehearsal env, PARALLELISM from P7) in RESTORE mode from the ARCHIVED checkpoint copy; `allowNonRestoredState=false` (never set — STARTUP-GATE-001 contract).
2. Verify: table preflight passes; startup mode = RESTORE (no FULL_REPLAY); source/dedup/window/detection/LOG state restored (CHECKPOINT-RESTORE-002 recipe: offsets, dedup map, window state, LOG sink offsets); KV sink starts cleanly (no state, first upserts from restored LOG tail).
3. Verify first checkpoint meets target (30 s interval; duration recorded; R2 pins active).

## 8. Phase 4 — bounded replay twice (P10.1 boxes 11-12)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + rehearsal env health before the replay runs.
1. Re-run the migration load against the same bounded source offsets — 2nd load must leave KV byte-identical to the 1st (idempotency proof; per-key checksum comparison, sampled + count).
2. Verify LOG may grow and KV keys do not: run the dual-sink job forward; confirm LOG row count grows while `_current` key count stays == approved distinct count.

## 9. Phase 5 — rollback + re-cutover rehearsal (P10.1 box 14, B8.7 full)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + restore probe against BOTH checkpoints (pre-cutover single-LOG and dual-sink era) before the rollback direction starts.
1. **Rollback direction:** stop the dual-sink job (approved operator procedure); preserve LOG and KV tables; reconstruct the single-LOG artifact (KV sink stripped, restore wiring kept); restore from the pre-cutover checkpoint; verify KV frozen (key count unchanged) and LOG grows; checkpoints complete <= 30 s.
2. **Re-cutover direction:** stop the single-LOG job; restore the dual-sink job from the dual-sink era's own checkpoint; verify full graph restore + KV sink resumes.
3. **Exposure record (P10.3 box):** capture the KV + LOG envelope of the dual-sink window — timestamp range, LOG rows written, KV upserts, sampled keys (the data that a production rollback would expose as duplicates).
4. No `allowNonRestoredState=true` shortcut; no automatic full replay at any point (STARTUP-GATE-001 / P10.3 contract).

## 10. Phase 6 — evidence + runbook delivery

1. Check all 14 P10.1 boxes in the tracker with evidence annotations (date + artifact path, register-format fields: commit, commands, topology, volume, output, pass/fail, operator/approver line).
2. Deliver `docs/08_implementation/15-candle-kv-production-cutover.md` (or appendix in the tracker): the P10.2 (16 boxes) + P10.3 (9 boxes) ready-runbook — exact commands, stop/freeze/load/RESTORE sequence, consumer-repoint step (marked for when the downstream pipeline exists), rollback triggers, exposure-record format. Execution deferred until a real production deployment exists.
3. Register rows updated where rehearsal evidence contributes (e.g. `CHECKPOINT-RESTORE-002` gains a rehearsal-env variant annotation).

## 11. Evidence template

Per tracker §4 fields: date; commit/image IDs; exact command or test; environment topology (isolated project, ports, db name); input volume/rate (copied row counts, offsets); output location (archive checkpoint path, rehearsal O2 queries, evidence file `logs/tracker-14/p10-rehearsal-<date>.md`); pass/fail per box; operator/approver line (user review).

## 12. Pass/fail handling

Any box failing mid-rehearsal: record the failure + root cause in the evidence file, fix (env/config/artifact), re-run the affected phase. The rehearsal env is throwaway — a failed phase never touches live data (isolation is the safety net). Production stays `BLOCKED` (tracker §6).

## 13. Cross-references

- Tracker: `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` P10 (L1144-1193), §4 register (L1198+), §6 acceptance.
- Recipe skills: `candle-kv-rollback-rehearsal` (B8.7), `candle-migration-batch-run` (audit/load), `candle-failure-injection-tests` (state-restore verification patterns), `fluss-tablet-crash-loop-repair` (segment integrity).
- P7 bench: `docs/plans/20260812-p7-bench.md` (sequencing gate).
- R2 fix + containment: `docs/plans/20260812-fix-r2-iceberg-lake-read-stall.md`; tracker P3.5.
- Audit efficiency follow-up: tracker P3.6 — the engine may gain parallelism/metadata-only counting BEFORE P10.2's production dry audit; not required for this rehearsal (current proven engine used).
- Prior union-audit evidence: `logs/tracker-14/p3-2-*` (CANDLE-MIGRATION-002 dev proof).
- Production target (future): `docs/08_implementation/09-production-swarm.md`, `docs/05_deployment/06-swarm-secrets.md` (P9 open review).

## 14. Execution results

*(to be appended after each phase run — date, commands, raw numbers, pass/fail, bottleneck notes)*
