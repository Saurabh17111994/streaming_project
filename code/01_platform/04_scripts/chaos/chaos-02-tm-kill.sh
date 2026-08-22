#!/usr/bin/env bash
# chaos-02-tm-kill.sh — T13 failure chaos test 2: kill the Flink TaskManager.
#
# Two legs:
#   A) Deterministic MiniCluster IT (always runs, offline, no cluster needed):
#      SignalJobTaskManagerKillIntegrationTest (SIG-TMKILL-001) — a 2-TM
#      MiniCluster job whose dedup markers are checkpointed, one TM is
#      hard-terminated, the job must restore from the last checkpoint and the
#      replayed feed must be suppressed by the restored first-write-wins
#      markers: no duplicate fingerprint ever leaves the dedup boundary
#      (the plan's "no dup candle / no dup signal" invariant).
#   B) Live-cluster leg (only when a Flink stack is up): SIGKILL the
#      taskmanager container; assert the job returns RUNNING from the last
#      checkpoint (fresh completed checkpoint after the kill) and that the
#      job's own compute.candles.duplicate_window counter does not advance
#      across the failover replay.
#
# Invariant under test (plan section 6, T3/T5 row): TM death → restore from
# last checkpoint → no duplicate candle, no duplicate signal.
#
# Usage: ./chaos-02-tm-kill.sh
# Env:   FLINK_REST_URL   (default http://localhost:8081)  live leg
#        TM_METRICS_URL   (default http://localhost:9250/metrics)  live leg
#        MAVEN_TIMEOUT_SEC (default 1200)
#        CHAOS_COMPUTE_POM (default $PROJECT_ROOT/code/02_services/02_compute/pom.xml)
# Exit:  0 PASS (A passed and B passed or skipped)
#        1 FAIL (A failed, or B failed when it ran)
#        3 SKIP (only when the whole stack is absent AND A could not run —
#                A is always runnable, so 3 is effectively never emitted)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../../.." && pwd)}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/chaos/chaos-$(date +%Y%m%d-%H%M%S)}"
COMPUTE_POM="${CHAOS_COMPUTE_POM:-$PROJECT_ROOT/code/02_services/02_compute/pom.xml}"
MAVEN_TIMEOUT_SEC="${MAVEN_TIMEOUT_SEC:-1200}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
TM_METRICS_URL="${TM_METRICS_URL:-http://localhost:9250/metrics}"

for tool in mvn java; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		echo "TM-KILL-CHAOS-02: FAIL — required tool '$tool' not found on PATH" >&2
		exit 1
	fi
done
if [ ! -f "$COMPUTE_POM" ]; then
	echo "TM-KILL-CHAOS-02: FAIL — compute pom missing: $COMPUTE_POM" >&2
	exit 1
fi
mkdir -p "$OUT_DIR"

# ---------- leg A: deterministic MiniCluster IT -----------------------------
A_LOG="$OUT_DIR/chaos-02-leg-a-minicluster.log"
echo "TM-KILL-CHAOS-02: [leg A] MiniCluster TM-kill IT (offline)"
set +e
if command -v timeout >/dev/null 2>&1; then
	timeout --signal=KILL "$MAVEN_TIMEOUT_SEC" env COMPUTE_INT_TEST_TM_KILL=true \
		mvn -o -f "$COMPUTE_POM" test -Dtest=SignalJobTaskManagerKillIntegrationTest 2>&1 |
		tee "$A_LOG"
else
	COMPUTE_INT_TEST_TM_KILL=true mvn -o -f "$COMPUTE_POM" test \
		-Dtest=SignalJobTaskManagerKillIntegrationTest 2>&1 | tee "$A_LOG"
fi
RC=${PIPESTATUS[0]}
set -e
if [ "$RC" -ne 0 ]; then
	echo "TM-KILL-CHAOS-02: FAIL — leg A (MiniCluster IT) rc=$RC (see $A_LOG)" >&2
	echo "TM-KILL-CHAOS-02: RESULT=FAIL EXIT=1"
	exit 1
fi
echo "TM-KILL-CHAOS-02: [leg A] PASS — restore from checkpoint, no duplicate fingerprint"

# ---------- leg B: live-cluster leg (optional) ------------------------------
if ! command -v docker >/dev/null 2>&1; then
	echo "TM-KILL-CHAOS-02: [leg B] SKIP — docker CLI absent (no live-cluster leg)"
	echo "TM-KILL-CHAOS-02: RESULT=PASS EXIT=0 (leg A passed, leg B skipped)"
	exit 0
fi
TM_CONTAINER="$(docker ps --filter "name=taskmanager" --format '{{.Names}}' | head -1 || true)"
if [ -z "$TM_CONTAINER" ]; then
	echo "TM-KILL-CHAOS-02: [leg B] SKIP — no flink-taskmanager container (stack not up)"
	echo "TM-KILL-CHAOS-02: RESULT=PASS EXIT=0 (leg A passed, leg B skipped)"
	exit 0
fi
if ! command -v curl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
	echo "TM-KILL-CHAOS-02: [leg B] SKIP — curl+python3 required for the Flink REST leg"
	echo "TM-KILL-CHAOS-02: RESULT=PASS EXIT=0 (leg A passed, leg B skipped)"
	exit 0
fi

B_LOG="$OUT_DIR/chaos-02-leg-b-live.log"
echo "TM-KILL-CHAOS-02: [leg B] live leg — container=$TM_CONTAINER rest=$FLINK_REST_URL"

if [ "${CHAOS_DRY_RUN:-0}" = "1" ]; then
	echo "TM-KILL-CHAOS-02: [leg B] DRY-RUN — would SIGKILL $TM_CONTAINER and verify REST/checkpoint/metrics"
	echo "TM-KILL-CHAOS-02: RESULT=PASS EXIT=0 (dry-run)"
	exit 0
fi

# Job id of the RUNNING signal job (JSON via python3 — no jq dependency).
# A missing job is a PREREQUISITE gap, not an assertion failure: the job is
# deployed serially later by the main agent (submit-jobs.sh), so SKIP the
# live leg in that state instead of failing the whole test.
OVERVIEW="$OUT_DIR/chaos-02-jobs-overview.json"
if ! curl -fsS "$FLINK_REST_URL/jobs/overview" >"$OVERVIEW" 2>&1; then
	echo "TM-KILL-CHAOS-02: [leg B] SKIP — JobManager REST not reachable at $FLINK_REST_URL"
	echo "TM-KILL-CHAOS-02: RESULT=PASS EXIT=0 (leg A passed, leg B skipped)"
	exit 0
fi
JOB_ID="$(python3 -c '
import json, sys
overview = json.load(sys.stdin)
for j in overview.get("jobs", []):
	if j.get("state") == "RUNNING":
		print(j["id"])
		break
')" <"$OVERVIEW" 2>/dev/null || true
if [ -z "$JOB_ID" ]; then
	echo "TM-KILL-CHAOS-02: [leg B] SKIP — no RUNNING job on $FLINK_REST_URL (deploy with submit-jobs.sh first)"
	echo "TM-KILL-CHAOS-02: RESULT=PASS EXIT=0 (leg A passed, leg B skipped)"
	exit 0
fi

cp_before() {
	curl -fsS "$FLINK_REST_URL/jobs/$JOB_ID/checkpoints" | python3 -c '
import json, sys
data = json.load(sys.stdin)
latest = data.get("latest", {}).get("completed") or {}
print(latest.get("id", -1))
'
}
dup_before() {
	# compute.candles.duplicate_window counter on the TM reporter
	curl -fsS "$TM_METRICS_URL" 2>/dev/null | grep "duplicate_window" |
		awk '{sum += $NF} END {print sum + 0}' || echo 0
}

CP0="$(cp_before)"
DUP0="$(dup_before)"
echo "TM-KILL-CHAOS-02: [leg B] baseline checkpoint=$CP0 duplicate_window=$DUP0"
if [ -z "$CP0" ] || [ "$CP0" = "-1" ]; then
	echo "TM-KILL-CHAOS-02: [leg B] FAIL — no completed checkpoint before the kill" >&2
	echo "TM-KILL-CHAOS-02: RESULT=FAIL EXIT=1"
	exit 1
fi

KILL_TS="$(date +%s)"
echo "TM-KILL-CHAOS-02: [leg B] SIGKILL $TM_CONTAINER at t=$KILL_TS"
docker kill -s KILL "$TM_CONTAINER" >>"$B_LOG" 2>&1 || {
	echo "TM-KILL-CHAOS-02: [leg B] FAIL — docker kill $TM_CONTAINER" >&2
	echo "TM-KILL-CHAOS-02: RESULT=FAIL EXIT=1"
	exit 1
}

# 1) container restarts (compose/swarm restart policy), 2) job RUNNING again,
# 3) a FRESH completed checkpoint after the kill (restore + live pipeline).
ok_container=0
for _ in $(seq 1 120); do
	if [ "$(docker inspect -f '{{.State.Running}}' "$TM_CONTAINER" 2>/dev/null || true)" = "true" ]; then
		ok_container=1
		break
	fi
	sleep 1
done
ok_job=0
ok_checkpoint=0
for _ in $(seq 1 240); do
	STATE="$(curl -fsS "$FLINK_REST_URL/jobs/$JOB_ID" | python3 -c '
import json, sys
print(json.load(sys.stdin).get("state", ""))
' 2>/dev/null || true)"
	if [ "$STATE" = "RUNNING" ]; then
		ok_job=1
		CP1="$(cp_before)" || true
		if [ -n "$CP1" ] && [ "$CP1" -gt "$CP0" ]; then
			ok_checkpoint=1
			break
		fi
	fi
	sleep 1
done

echo "TM-KILL-CHAOS-02: [leg B] container_up=$ok_container job_running=$ok_job fresh_checkpoint=$ok_checkpoint"
if [ "$ok_container" -ne 1 ] || [ "$ok_job" -ne 1 ] || [ "$ok_checkpoint" -ne 1 ]; then
	echo "TM-KILL-CHAOS-02: [leg B] FAIL — recovery incomplete (see $B_LOG)" >&2
	echo "TM-KILL-CHAOS-02: RESULT=FAIL EXIT=1"
	exit 1
fi

# No-duplicate live check: the job's own duplicate_window counter must not
# advance across the failover replay (T5 first-write-wins restores).
sleep 15 # let replay + a metrics scrape pass
DUP1="$(dup_before)"
echo "TM-KILL-CHAOS-02: [leg B] duplicate_window before=$DUP0 after=$DUP1"
if [ -n "$DUP1" ] && [ "$DUP1" -gt "$DUP0" ]; then
	echo "TM-KILL-CHAOS-02: [leg B] FAIL — duplicate_window advanced $DUP0 -> $DUP1 after TM kill" >&2
	echo "TM-KILL-CHAOS-02: RESULT=FAIL EXIT=1"
	exit 1
fi

echo "TM-KILL-CHAOS-02: PASS — TM kill: restore from last checkpoint, no duplicate candle/signal"
echo "TM-KILL-CHAOS-02: RESULT=PASS EXIT=0"
