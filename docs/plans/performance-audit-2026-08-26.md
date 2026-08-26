# Flink/Fluss Performance Diagnosis — 2026-08-26 (Audit Only, No Changes)

**Date:** 2026-08-26
**Type:** Diagnosis / audit record — no code or config changed.
**Scope:** Single-VM dev stack performance & reliability state; primary finding = chaos-03 tablet-kill harness race.

---

## 1. Environment (facts, verified)

| Item | Value | Source |
|---|---|---|
| Repo | `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project_New` (branch `sj-branch-1`, clean) | `git status` |
| Deployed Flink | `flink:2.2.1-scala_2.12-java17` | `docker images` + `.env` |
| Deployed Fluss | `apache/fluss:0.9.1-incubating` | `docker images` + `.env` |
| Flink source checkout | `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/flink` — **2.4-SNAPSHOT** | `pom.xml` |
| Fluss source checkout | `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/fluss` — **0.9-SNAPSHOT** | `pom.xml` |
| Stack now | coordinator/tablet/execution-gateway up 3h; flink-*, ingestion, otel-collector, webhook-receiver exited 127 ~6h ago; nautilus/execution-bridge exited 0 13h ago; minio/O2/cadvisor/ZK up 6h | `docker compose ps -a` |
| TM memory (compose) | `taskmanager.memory.process.size: 6g`, managed fraction `0.4` (→ ~2.4 GB RocksDB), network 256m, task off-heap `512m`, slots 8, parallelism 8, RocksDB incremental, checkpoint 10 s | `docker-compose.yml` `x-flink-common` |

**Deployed-vs-checkout caveat:** source citations in this record are to the 0.9-SNAPSHOT / 2.4-SNAPSHOT checkouts. The running images are 0.9.1-incubating / 2.2.1. Config-key semantics are verified-for-0.9-SNAPSHOT and **assumed for 0.9.1** unless the image jar says otherwise.

## 2. Historical performance evidence on disk (facts)

| Evidence | Result |
|---|---|
| `logs/tracker-14/sig-perf-20260824.md` | Job `aaccc1cc…` RUNNING 120/120, 463/463 checkpoints, `feature_candles_15s` 371,133 → 401,720, `DEDUP_TTL_MS=300000` live |
| `logs/tracker-14/e2e-30min-soak-20260817.md` | raw 65,748,709 rows (+29.9M / 1822 s ≈ **16,410 rows/s**), candles +123,904 (~68 rows/s), cp 179 = 775.6 MB / 2,478 ms, 0 restarts |
| `logs/tracker-14/sig-perf-001-50k-baseline-20260817.md` | 50k t/s baseline deferred — single-socket policy ceiling (1024 tokens/conn); `F.4` bottleneck recorded, no config inflation |
| Plan `docs/plans/2026-08-25-live-readiness-unified-plan.md` | U0–U3.x DONE; `make gate` 13/13 (2026-08-25); U4.1–U4.4 MARKET-OPEN; U5 BLOCKED: needs prod VMs |
| C4 synthetic | 48,660 tps (Go bridge synthetic envelope, plan AC-U3) |
| `logs/chaos/chaos-20260824-155057/SUMMARY.txt` | 01 PASS, 02 PASS, **03 FAIL**, 04 SKIP |

## 3. Primary finding — chaos-03 (tablet-kill) FAIL is a harness/measurement race, not a perf or data-integrity defect

**Symptom (fact):** `raw_table_1 not readable within 180s of tablet restart` — `TabletKillChaosIntegrationTest.java:394` assertion failed.

**Evidence chain (facts, from `logs/chaos/chaos-20260824-155057/03-tablet-kill.log`):**

1. Kill → `docker start` → tablet container running.
2. **The table WAS readable** — the client returned data. Every "not readable yet" came from:
   - `scan returned 0 rows after quiescence — table still loading after tablet restart` (line 17839), or
   - `post-kill log scan did not finish within 60s (leader-less client or slow tablet replay)` (lines 27262, 46224, 63674).
3. The 60 s attempt bound (`withAttemptBound`, test lines 120–134) kept **aborting scans that were making progress** — the test's own comment (2026-08-25 reproduction) admits: *"3 consecutive 'did not finish within 60s' while the probe then completed the scan in 3m49s"*.
4. Live ingestion was writing the table continuously during the test. The quiescence detector (`QUIESCE_POLLS=12` = 2 rounds × ~6 s of empty polls) **can never form while a writer keeps the log non-empty**, so the full scan either hangs to `SCAN_TIMEOUT_MS` or returns 0 (table still loading) → the 180 s recovery deadline governs → FAIL.
5. In the RF=1 dev case the test's own invariant permits tail loss (newest acked rows lost to unclean SIGKILL, un-fsynced tail). **This run never got far enough to evaluate the data invariants at all.**

**Why it is not performance and not data loss:**
- Pre-kill the table scanned fine (7,829,826 rows).
- The failure is entirely in the post-restart scan quiescence/attempt-bound interaction.
- No OOM, no network-credit saturation, no KV/tablet stall, no heap error in the log.
- `probeReadable` (bucket-0 fast path, test lines 167–184) is deliberately separate from the full scan and would have passed quickly — the harness just never used it as the recovery gate.

**Verdict:** the fix belongs in the harness (quiescence logic + attempt bounds vs continuous writers — e.g. use `probeReadable` as the recovery gate, bound the full scan by progress not quiescence, or pause writers during the invariant scan). Tablet recovery and LOG immutability are **not disproven** by this run. This is a test-design defect.

## 4. Secondary observations (lower confidence, no action taken)

| # | Finding | Evidence | Confidence |
|---|---|---|---|
| 1 | TM envelope mismatch: managed `0.4 × 6g = 2.4g` vs `TASK_MANAGER_MEMORY_MANAGED_SIZE=2g` — one of them wins; RocksDB also shares the host with a 16-container stack on a 15 GB box | `docker-compose.yml` lines 66–67, 108–114; prior heap-OOM evidence (P7) at lower budgets | Medium |
| 2 | Single-replica / RF=1 on laptop — prod contract is RF≥3 (D23) | test replication logic + `CHAOS_REPLICATION_REQUIRED`; D23 | High |
| 3 | Deployed vs source drift (0.9.1 image vs 0.9-SNAPSHOT checkout) — affects any config-key citation | images vs checkouts | High (correctness) |
| 4 | `docker kill` bypasses `restart: unless-stopped` — prod swarm restart policy won't auto-recover a SIGKILLed tablet unless explicitly configured (daemon-crash only) | compose + test comments | High |

## 5. Not investigated (no budget spent)

- Per-subtask busy/backPressured/idle, checkpoint phase durations, Fluss writer/server buffer gauges — the Flink/Fluss processes are **not running now**, so no live metrics exist to collect.
- Any throughput/latency work — the only open performance item on the plan is `U5` (needs prod VMs).

## 6. Facts / inference / assumption split

- **Facts:** stack state; evidence counts (16,410 rows/s, 463/463 ckpts, 7,829,826 pre-kill rows); failure messages; test source lines; RF=1 on dev.
- **Inference:** the 60 s attempt bound aborted progressing scans (supported by the test's own 2026-08-25 comment); continuous writers defeat quiescence (log shows writer active post-kill).
- **Assumptions:** config-key semantics verified-for-0.9-SNAPSHOT apply to 0.9.1 image; the tablet was actually serving (only the scan harness failed) — supported by probe logic but not directly proven in this run.

## 7. Suggested next steps (pick any)

1. **Chaos-03 harness fix** (most actionable — single-file test change): make the recovery gate `probeReadable` + pause/isolate writers during the invariant scan; re-run `chaos-03` live.
2. **Config audit**: reconcile TM managed-memory (0.4×6g vs explicit 2g) + pin the `docker kill` restart-policy gap in compose/dossier.
3. **Live re-measure** (stack is down now; needs `make up` + a market session): capture actual `busy/backPressured/idle`, checkpoint history, and Fluss buffer metrics per the flink-performance skill workflow.
