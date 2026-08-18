# Open Items — Implementation Plan (Signal job, post-CHG-023)

> **ROLE — WORKING PLAN (open-item backlog, 2026-08-17):** this file is the
> single execution plan for every item the 04-signal-job audit (2026-08-17)
> found still open, partially implemented, or unverified. It follows the
> dossier convention: each item carries an execution class, solving method,
> prerequisite, and pass gate. Items land in code/evidence as they are done and
> this plan is ticked [x]; the authoritative living record for the Signal job
> stays `04-signal-job.md`.
>
> **Scope boundary (user, 2026-08-17):** feature-path-only — `raw_table_1` →
> validation → dedup → candles → `feature_candles_15s`. Signal-side items
> (real detection strategy, p99 decision latency, Trade_Decisions) are
> OUT OF SCOPE and listed only so the boundary is explicit.

<!-- markdownlint-disable MD013 -->

## Status

| Field | Value |
| --- | --- |
| Status | **ALL 6 ITEMS RESOLVED 2026-08-17 (Item 6 2026-08-17; Item 1 2026-08-17; Item 2 2026-08-17 — Cloudflare R2 decided, URI wired, P4.2 live proof PASSED against the real bucket; Item 3 2026-08-17 — 30-min soak PASSED; Item 4 2026-08-17 — CLOSED-AS-COVERED, native metrics verified live in O2; Item 5 2026-08-17 — CLOSED-AS-DEFERRED, deferral accepted; CHG-024 filed per Definition of Done).** Audit basis: `04-signal-job.md` §Pending work items / §Required tests / §Required telemetry (2026-08-17); verified against the code tree the same day (`S3_CHECKPOINT_URI_TO_BE_DEFINED` never defined; the audit initially missed that `CandleFailureInjectionIntegrationTest` P6.2 already covers the checkpoint-failure path live — the host-runnable variant below is still needed and landed) |
| Owner | Compute and Strategy Teams |
| Affected | `04-signal-job.md`, `05-deployment/change-records/CHG-024.md` (new), `11-testing-and-release.md`, `o2-provision.py` (alert threshold re-base only if needed) |
| Baseline | Compute suite 291 run / 0 failures / 16 env-gated skips (verified 2026-08-17) |

## Execution classes

- **pure-JVM now** — needs only compute-pom test code; no cluster, no live env.
- **live-dev now** — env-gated against the dev Fluss cluster (pattern: `SignalChainLiveE2ETest` / `FormingBarRehydrationIntegrationTest`).
- **decision-gated** — needs a user/deployment decision before code can run in production (infra/credentials).
- **out-of-scope** — signal-side per user instruction; documented so the boundary is explicit, never silently dropped.

---

## Item 1 — `SIG-FAIL-001`: checkpoint/continuity failure → safe halt

| Field | Value |
| --- | --- |
| Status | **DONE 2026-08-17** — `SignalJobCheckpointFailureIntegrationTest` (env-gated `COMPUTE_INT_TEST_SIG_FAIL=true`, host-runnable: embedded MiniCluster + `file://` checkpoints, no dev cluster): healthy checkpoint first (control), then the job-id checkpoint dir stripped of write → next checkpoint fails → the production declarative restart route (`RestartStrategyOptions` FIXED_DELAY, dev overrides 3 × 1000 ms) drove **exactly 3 RESTARTING episodes then FAILED**, cause contains "checkpoint". PASS: `SIG-FAIL-001[checkpoint-failure] attempts=3 restartEpisodes=3 terminal=FAILED cause~checkpoint`. Suite re-verified 294/0/17. (The audit initially said no checkpoint-failure test existed — the live P6.2 leg in `CandleFailureInjectionIntegrationTest` already covered the same failure path against the real Fluss topology; this host-runnable variant closes the shell-level half + asserts the full ×3 restart count.) |
| Execution class | **pure-JVM now** (MiniCluster, host-runnable — same pattern as `SignalJobCompactCheckpointRestoreIntegrationTest`) |
| Scope | **Feature-path half only.** The decision-suppression + safe-halt half is REMOVED (CHG-005, 2026-08-15 — decision operators out of scope). What remains: **shell-level checkpoint-failure test** — force a checkpoint failure, assert the job fails closed under the fixed-delay restart policy (×3 at 30 s) then fails the job, never continuing unsafely |
| How to solve | New test class `SignalJobCheckpointFailureIntegrationTest` (env-gated, host-runnable, embedded MiniCluster + `file://` checkpoints): (1) run the real `buildTopology` sub-graph; (2) inject a checkpoint failure (failing `CheckpointStorage`/state snapshot or a checkpoint-timeout injection) on the first checkpoint; (3) assert the restart counter hits the pinned `RESTART_MAX_ATTEMPTS=3` × `RESTART_DELAY_MS=30000` then the job FAILS (not silently degraded); (4) a control leg without injection checkpoints cleanly. Optionally assert the `SIGNAL-error-job-restarting` alert path is reachable (log-line level; the O2 alert itself is already live) |
| Prerequisite | Harness infra landed (compute pom test scope, 2026-08-10); MiniCluster pattern landed (`SignalJobCompactCheckpointRestoreIntegrationTest`). **None new** |
| Pass gate | Job fails closed with the fixed-delay restart policy (3 × 30 s, `PlatformConfig.RESTART_MAX_ATTEMPTS`/`RESTART_DELAY_MS`); no unsafe continuation; control leg clean. Suite count +N (compute), 0 failures |
| Notes | Doc currently says "waits on Phase 6" — this item does NOT need Phase 6; it is implementable now. On landing, update the pending-table row in `04-signal-job.md` to DONE and remove the "waits on Phase 6" qualifier |

## Item 2 — Production checkpoint storage (`S3_CHECKPOINT_URI_TO_BE_DEFINED`)

| Field | Value |
| --- | --- |
| Status | **DONE 2026-08-17** — user decision: **Cloudflare R2** (S3-compatible; the S3 API plugin `flink-s3-fs-hadoop` is already on the compute classpath). URI wired: `CHECKPOINT_DIR=s3://tradingticks-aug-2026/flink-checkpoints` in `code/01_platform/01_docker/.env` (gitignored; same R2 bucket as the Iceberg lake `s3://tradingticks-aug-2026/lake/`, separate prefix). Credentials `R2_ENDPOINT`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION=auto` already present in the same file. `SignalJobConfig` production fail-closed validation accepts the URI (verified: `s3://` prefix, object-store path); `SignalJob.applyRuntimeOptions` wires `fs.s3a.*` (endpoint/access/secret/region/path-style) + `SimpleAWSCredentialsProvider`; `submit-jobs.sh` launcher gate already enforces S3 URI + credentials in `DEPLOYMENT_ENV=production`. **Live proof 2026-08-17: `SignalJobObjectStoreCheckpointIntegrationTest` (gate `COMPUTE_INT_TEST_P42=true`, run `run-1786975842757`) PASSED against the real bucket — 1 test / 0 failures / 43.2 s; phase 1 wrote RocksDB checkpoints to `s3a://tradingticks-aug-2026/flink-checkpoints/run-1786975842757/b5b167981101249c49b952c452f88da0/chk-2` (artifacts on the object store), phase 2 restored keyed state on a brand-new worker (`firstRun=phase-1` tag, resumed counts 900 = 600 checkpointed + 300 new), strict restore, then the run prefix was removed from the bucket** |
| Execution class | **decision-gated** (infra choice + credentials) — decision made by the user 2026-08-17 |
| Scope | Define and wire the production checkpoint/savepoint URI |
| How to solve | (1) **User decision:** S3 bucket (+ IAM/keys + the Flink S3 filesystem plugin in the dist) or MinIO (self-hosted, S3-compatible); (2) set `CHECKPOINT_URI`/savepoint URI to the chosen store; (3) verify `SignalJobConfig`'s production fail-closed validation accepts it (`s3://` or `s3a://` prefix, object-store URI without local path — the existing guard at `SignalJobConfig` ~L430-461); (4) one env-gated object-store restore test against the real store (the existing `SignalJobObjectStoreCheckpointIntegrationTest` skips without its gate — run it with the store configured) |
| Prerequisite | Bucket + credentials (user); S3 plugin on the Flink classpath |
| Pass gate | Production launch with `DEPLOYMENT_ENV=production` + the defined URI passes the config gate; checkpoint/savepoint round-trip (write → cancel → restore) verified against the real store |
| Notes | User chose Cloudflare R2 (not MinIO / not AWS S3) — no Gravity-Index comparison needed. Dev continues on `file://` overrides; the production URI is only enforced when `DEPLOYMENT_ENV=production`. The bucket is not secret; the credentials never leave the gitignored `.env` / secret injection |

## Item 3 — 30-min E2E soak (long-run acceptance evidence)

| Field | Value |
| --- | --- |
| Status | **DONE 2026-08-17** — full 30-min soak PASSED on the final native profile (run by the second coding agent, launched detached per the operational note; log `/tmp/e2e-soak.log`, 19:43–20:14 IST). `chain-e2e PASS: raw 35 849 389 → 65 748 709 rows, feature 186 884 → 310 788 rows (+123 904), 1024 instruments, 117 samples`; `Tests run: 1, Failures: 0, Errors: 0, Time elapsed: 1822 s — BUILD SUCCESS (30:24 min)`, `E2E_SOAK_EXIT=0`. Pass gate fully met: feature +123 904 rows with `window_end` after run start; job RUNNING across all 117 samples; **179 consecutive EXACTLY_ONCE checkpoints** (checkpoint 1 completed in 2 912 ms — the investigation's zero-ack stall absent; last checkpoint 179 = 775.6 MB, 2 478 ms); **0 restarts, 0 expired/declined checkpoints, 0 checkpoint-1 stalls** across the full 30 min. Sustained ~16 556 raw rows/s + ~69 candle rows/s. Evidence: `logs/tracker-14/e2e-30min-soak-20260817.md` |
| Execution class | **live-dev now** (no code — a run) |
| Scope | Re-run `run-signal-chain-e2e.sh` with `E2E_RUN_MINUTES=30` (script default is already 30) on the final native profile |
| How to solve | Detached run (`setsid --fork`, per the 2026-08-17 operational note); poll with ≤30 s checks; capture `DESIGN-PERF[envelope]` + final acceptance lines |
| Prerequisite | Dev Fluss cluster up; faketool/ingestion built |
| Pass gate | Feature gains rows with `window_end` after run start, job stays RUNNING, ≥1 EXACTLY_ONCE checkpoint, 0 restarts, 0 checkpoint-1 stalls across the full 30 min |
| Notes | Optional per the dossier — run it when a 30-min window is convenient; no code changes expected |

## Item 4 — Sink-latency / restore-count / state-compat-failure telemetry

| Field | Value |
| --- | --- |
| Status | **CLOSED-AS-COVERED 2026-08-17** — the dossier's "Pending (Slice 2+)" row is annotated in `04-signal-job.md` §Required telemetry with the native series + first-hand live verification (direct OpenObserve `_search` queries during the 30-min soak, job `cfd15e7603c6d1eaac70abce8edf7665`): `flink_jobmanager_job_numrestarts` = 178 rows all 0.0 (restore count); `flink_jobmanager_job_numberoffailedcheckpoints` = 178 rows all 0.0; `flink_jobmanager_job_numberofcompletedcheckpoints` rising to 178; `flink_jobmanager_job_lastcheckpointrestoretimestamp` constant at job start (one initial restore, none mid-run); sink keep-up via `flink_taskmanager_job_task_numrecordsoutpersecond_rate` + `numrecordsout` counter (one nuance found during verification: the bare meter-count stream `numrecordsoutpersecond` exports empty — the `_rate` substream carries the data). State-compat failure is loud by nature (restore fails at startup → job failure → `SIGNAL-error-job-restarting` alert; STATE-COMPAT-001 proves the block). No custom counters built — native-first per CHG-023; the only residual gap is a cosmetic one-click "sink latency in ms" panel, explicitly not built. No dead "pending" claim remains that native metrics answer |
| Execution class | **close-as-covered** (recommended) — no code |
| Scope | Decision: close the dossier's "Pending (Slice 2+)" telemetry row as **covered by native metrics** (the O2-native-series verification proved the data lands), or add custom counters only if a one-click "sink latency in ms" O2 panel is wanted |
| How to solve | If closed: annotate the `04-signal-job.md` telemetry row (sink latency / restore count / state-compat failures) with the native series + the 2026-08-17 live verification, mirroring the DEC-038-telemetry annotation style. If custom counters are wanted (not recommended — contradicts the native-first direction): add a sink-latency gauge + restore-count counter via MetricGroup and wire through the native reporter |
| Prerequisite | None |
| Pass gate | Dossier row annotated; no dead "pending" claim remains that native metrics already answer |
| Notes | Recommendation (2026-08-17, prior audit): **close as covered** — the failure case (state-compat restore failure) is loud by nature (job fails → `SIGNAL-error-job-restarting` alert), it cannot happen silently |

## Item 5 — CANDLE-KV-REPLAY-001 observability (dedicated per-sink counters)

| Field | Value |
| --- | --- |
| Status | **CLOSED-AS-DEFERRED 2026-08-17** — deferral accepted as the final disposition (no code, no counters added). The candle table is KV-only since 2026-08-13, so the LOG-vs-KV convergence surface these counters were designed to observe no longer exists; signal LOG/KV convergence observability is covered by existing native telemetry (see Item 4's CLOSED-AS-COVERED annotation), and dedicated counters would contradict the CHG-023 native-first direction. Dossier annotated: `04-signal-job.md` §Required telemetry CANDLE-KV-REPLAY-001 row now carries the DEFERRAL ACCEPTED marker; the "can be added later without contract change" escape hatch stays open |
| Execution class | **close-as-deferred** (recommended) — no code |
| Scope | The candle table is KV-only since 2026-08-13, so the LOG-vs-KV surface is gone; convergence observability now belongs to the signal LOG/KV pair. Decision: keep the deferral (existing telemetry: `compute.startup.mode`, `compute.candles.emitted`, checkpoint metrics, signal-sink `numRecordsIn` + LOG:KV ratio) or add signal LOG-vs-KV convergence counters |
| How to solve | If kept deferred: tick the dossier's deferred note as accepted (no action). If wanted: add `Signal_Candidates` LOG appends vs `Signal_Candidates_current` KV unique-key counters via MetricGroup (native reporter path) |
| Prerequisite | None |
| Pass gate | N/A if kept deferred; counters + one assertion test if added |
| Notes | Recommend **keep deferred** — matches the native-first direction and the "no hand-built telemetry" principle |

## Item 6 — Doc-drift fixes (verification rows)

| Field | Value |
| --- | --- |
| Status | **DONE 2026-08-17** — all three doc errors fixed and verified: (a) gate name corrected to `COMPUTE_INT_TEST_SIGNAL_KV`; (b) deleted classes annotated as DELETED; (c) skip count reconciled to 16 env-gated skips across 13 classes |
| Execution class | **pure-JVM now** (docs + one test re-run) |
| Scope | Three verified doc errors in `04-signal-job.md`: (a) L625 says `SignalCurrentKvIdempotencyTest` is gated on `COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT` — **code says `COMPUTE_INT_TEST_SIGNAL_KV`** (+ `FLUSS_BOOTSTRAP`); (b) L625's env-gated list still names `SigState002RehydrationRestoreIntegrationTest` + `SigState003FailClosedPreflightIntegrationTest` — **both deleted in the Design-B merge `34af190`**; (c) L625 skip count says "17 skipped tests across 13 classes" while the banner says 16 env-gated skips (the 17 is a pre-Design-B 2026-08-15 measurement) |
| How to solve | Fix the gate name; annotate the deleted classes as deleted (or remove from the list); reconcile 17 → 16 with the dated pre-merge marker; re-run the compute suite to re-verify 291/0/16 and confirm the list against live surefire reports |
| Prerequisite | None |
| Pass gate | `make full-audit` exit 0; suite count 291/0/16 re-verified; no un-annotated stale gate/class/skip claims |
| Notes | Do this FIRST — it is the cheapest item and keeps the dossier honest while the rest land |

---

## Out-of-scope items (explicit boundary, per user 2026-08-17)

| Item | Why out of scope | Status if ever re-scoped |
| --- | --- | --- |
| p99 decision-latency half of `SIG-PERF-001` | Signal-side; feature-path-only instruction | Measure once real detection lands |
| Real detection strategy (Slice 2.2 Q2 — mirrored breakout is a documented placeholder) | Your trading logic; never in build scope | Replace the placeholder rule; re-run forming-bar tests |
| Trade_Decisions / SCH-19 decision dual-sink | Gated off `TRADE_DECISIONS_ENABLED=false`; no producer in scope | CHG-005 decision required |

---

## Execution order (why)

**One line: cheap-and-safe first, valuable-and-self-contained next, decision-gated whenever you're ready, evidence-gathering opportunistically, judgment calls last.**

| Order | Item | Class | Why here | When |
| --- | --- | --- | --- | --- |
| 1 | **Item 6 — Doc-drift fixes** | pure-JVM now | Clean baseline: removes the doc-vs-code drift that confused the audit itself; every later item's evidence lands against a clean dossier. Zero risk, immediate payoff | Start now |
| 2 | **Item 1 — `SIG-FAIL-001` checkpoint-failure test** | pure-JVM now | Closes the last open *required-test* ID in the feature path; MiniCluster pattern already landed; also exercises the restart pins (`RESTART_MAX_ATTEMPTS=3` × 30 s) that today have only config-level tests | After 6 |
| 3 | **Item 2 — Production checkpoint storage** | decision-gated | The only item needing your input (S3 vs MinIO + credentials); everything else works on `file://` until then — movable, nothing blocks on it | Whenever you decide |
| 4 | **Item 3 — 30-min E2E soak** | live-dev now | Pure evidence for the long-run acceptance gate; no code, no dependency on 1–3 — can run in parallel with step 1 since they touch nothing in common | Any free 30-min window |
| 5 | **Items 4 + 5 — Telemetry decisions** | close-as-covered / close-as-deferred | Annotations only, not implementation: native series already verified live in O2; building custom counters would contradict the native-first direction. Done = a judgment call, not a test | After 1–4 (or whenever) |

1. **Item 6 first** — 15 minutes, keeps the dossier honest, removes the doc-vs-code drift that confused the audit itself.
2. **Item 1 next** — the only genuinely-open *required-test* ID (`SIG-FAIL-001`) in the feature path; pure-JVM, host-runnable, no blockers.
3. **Item 2 when you decide** — the only decision-gated item; everything else works on `file://` until then.
4. **Item 3 whenever a 30-min window is free** — optional long-run evidence, no code; can run in parallel with step 1.
5. **Items 4 + 5 — decisions, not code**: recommend closing as covered/deferred; no work unless you want custom O2 panels.

**Parallelism note:** steps 1 (doc fixes) and 4 (30-min soak) are fully independent — the soak can be launched detached while the doc fixes land.

## Definition of done

All execution-class items landed with evidence (tests green, suite count re-verified, `make full-audit` exit 0, CHG-024 change record filed); decision-gated items resolved (either implemented or explicitly annotated decision); the out-of-scope boundary unchanged.
