# 21 — Savepoint rollout (G5 Ops T12)

**Status:** implemented (script + Makefile target), not executed against a live
cluster (build-only change; the 1k acceptance run validates the dedup hit rate
externally).

## Problem

A SignalJob update must not fall back to an offset-0 full replay: that
re-emits the whole backlog, balloons the fingerprint-dedup MapState past the
pinned checkpoint contract, and appends duplicate candle rows
(CANDLE-KV-REPLAY-001 A3.3). The fail-closed gate in `SignalJobConfig`
enforces this — a restart must set `STATE_RECOVERY_PATH` (restore) or
explicitly accept `ALLOW_FULL_REPLAY=true` (replay, break-glass only) — but
the operator needed a single rollout command that captures state, replaces
the artifact, and restores from the fresh savepoint.

## What the rollout does

`make rollout-savepoint` (backed by
`code/01_platform/04_scripts/rollout-savepoint.sh`) executes, in order:

1. **Preflight** — Flink REST (`JM_URL`, default `http://localhost:8081`)
   reachable; resolves the job by name (`JOB_NAME`, default
   `signal-job-compute` — matches `env.execute("signal-job-compute")` in
   SignalJob.java) and requires state `RUNNING`; `JOB_ID=<jid>` bypasses the
   lookup. In `DEPLOYMENT_ENV=production` the savepoint target must be
   `s3://*`/`s3a://*` (G4 T9 parity with submit-jobs.sh); dev defaults to
   `file:///checkpoints/savepoints` (the `flink-checkpoints` volume mounted
   in both JM and TM).
2. **Savepoint** — `POST /jobs/{jobid}/savepoints` (async request-id, polled
   until `COMPLETED`, `SAVEPOINT_TIMEOUT_S` default 600 s). The completed
   savepoint path becomes the restore path.
3. **Stop** — `PATCH /jobs/{jobid}?mode=cancel` (fallback
   `POST /jobs/{jobid}/cancel`), waits for `CANCELED`.
4. **Deploy** — `docker compose cp` the new jar
   (`JAR`, default `code/02_services/02_compute/target/compute.jar`) into
   `flink-jobmanager:$JAR_IN_CONTAINER`, then submits inside the JM container:

   ```bash
   docker compose exec -T \
     -e STATE_RECOVERY_PATH=<savepoint> -e ALLOW_FULL_REPLAY=false \
     [-e <job env forwarded from the caller>] \
     flink-jobmanager flink run -d \
     -c com.trading.compute.signaljob.SignalJob /opt/flink/jobs/compute.jar
   ```

   Why the `flink run` client and not the REST `/jars/run` path: the job
   config comes from `System.getenv()` in `main()` (SignalJobConfig via
   `fromEnv()`), and the JVM that runs `main()` is the submitting client.
   `submit-jobs.sh`'s `/jars/run` executes in the JobManager JVM, whose
   composed env carries `ALLOW_FULL_REPLAY=true` and cannot take a
   per-release restore path. The `docker compose exec -e` pattern is the one
   already verified in `docs/06_operations/01-runbooks.md` (start/restart).
   The script forces `ALLOW_FULL_REPLAY=false`, so the F005 combination
   cannot occur, and forwards every job config var it inherited (see the
   `JOB_ENV_NAMES` list in the script). The pinned required vars —
   `DEDUP_TTL_MS`, `CANDLE_WINDOW_MS`, `CHECKPOINT_INTERVAL_MS`,
   `CHECKPOINT_TIMEOUT_MS`, `MAX_CONCURRENT_CHECKPOINTS` — have **no
   defaults** in SignalJobConfig and must be exported by the caller.
5. **Verify start** — the new job reaches `RUNNING`, its client log shows
   `startup mode = RESTORE (restore=true, fullReplay=false)` (a
   `FULL_REPLAY` or `F005` line fails the rollout), and it completes ≥ 1
   durable checkpoint.
6. **Dedup continuity (degradable)** — samples the TaskManager Prometheus
   metrics `compute_dedup_state_count` / `compute_dedup_first` /
   `compute_dedup_duplicates` (`PROMETHEUS_URL`, default
   `http://localhost:9250/metrics` — the host-published TM port in dev)
   before the savepoint and after restore; fails if the restored state count
   drops below 50 % of the pre-rollout value (state was NOT preserved),
   warns and continues if the endpoint is unreachable, and reports the
   session dedup hit share as evidence. `VERIFY_DEDUP_STATE=0` disables.

The whole transcript (plus the savepoint path and job ids) lands in
`logs/rollout/rollout-<job>-<timestamp>.log` (gitignored evidence, repo
convention).

## Usage

```bash
# preview without touching anything
make rollout-savepoint ARGS="DRY_RUN=1"

# full rollout (env-driven; export the required pinned job config first)
export DEDUP_TTL_MS=300000 CANDLE_WINDOW_MS=15000 \
       CHECKPOINT_INTERVAL_MS=10000 CHECKPOINT_TIMEOUT_MS=30000 \
       MAX_CONCURRENT_CHECKPOINTS=1
make rollout-savepoint ARGS="JAR=/tmp/compute.jar"

# re-run an interrupted rollout from an already-captured path (deploy+verify only)
make rollout-savepoint ARGS="RECOVERY_PATH=file:/checkpoints/savepoints/savepoint-abc-123"

# direct invocation
code/01_platform/04_scripts/rollout-savepoint.sh --job-id <jid> --jar code/02_services/02_compute/target/compute.jar
```

Overlays (p10 rehearsal, ...): `COMPOSE_FILE` accepts the base file and
`COMPOSE_PROJECT` the project name (`-p p10`); `JM_URL` must point at the
overlay's published REST port and `PROMETHEUS_URL` at the overlay's TM port.
Production swarm: same script, with `JM_URL` = the stack's JobManager REST
endpoint, `SAVEPOINT_DIR=s3://...`, `DEPLOYMENT_ENV=production`, and the
deploy step targeting the JM service task container (the swarm equivalent of
`docker compose exec`).

## Scope and ownership

- Script: `code/01_platform/04_scripts/rollout-savepoint.sh` (T12, G5).
- Makefile target: `rollout-savepoint` (T12).
- No changes to `docker-compose.yml` / `docker-stack.yml` (T0/T9 own them) or
  `code/02_services/02_compute` (T3 owns it) — the script reads the job's
  env contract and overrides only `STATE_RECOVERY_PATH`/`ALLOW_FULL_REPLAY`
  at submit time; it does not modify the compose defaults.

## Rollback

If the restored job misbehaves, redeploy the previous jar through the same
script with `RECOVERY_PATH` set to the same savepoint (or the previous
checkpoint dir per the runbook) — the savepoint is retained (the stop is a
cancel, not a delete) and remains the restore anchor for both directions.

## Validation (this change)

- `bash -n` and `shellcheck -S warning` (repo static-check level) pass on the
  script.
- `make rollout-savepoint ARGS="DRY_RUN=1"` prints the full command plan
  without executing anything.
- Live acceptance (plan T12 at 1k): rolling update retains the dedup hit
  rate — measured externally by the existing E2E/bench gates; the script's
  step 6 supplies the state-continuity evidence.
