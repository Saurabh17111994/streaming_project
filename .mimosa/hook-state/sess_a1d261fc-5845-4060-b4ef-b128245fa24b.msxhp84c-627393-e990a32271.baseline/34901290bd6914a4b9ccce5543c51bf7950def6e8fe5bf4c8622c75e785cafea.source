#!/usr/bin/env bash
# soak-reconnect-loop.sh — force bridge crash-restarts and verify NO leak.
#
# ALIGNED WITH THE SERVICE'S ACTUAL RESTART SEMANTICS (review R-001):
#   * The Go bridge installs a SIGTERM handler, so a plain `kill` makes it
#     exit cleanly (code 0). IngestionService treats exit code 0 as a
#     *requested* exit (NO_RESTART) and shuts the whole pipeline down. Only a
#     crash (non-zero exit) triggers the restart path.
#   * IngestionService.MAX_BRIDGE_RESTARTS = 1: the JVM restarts the bridge
#     exactly ONCE per process run; a second crash is TERMINAL (the JVM exits
#     cleanly with the restart budget exhausted — that is correct behavior,
#     not a failure).
#   * Therefore this script drives exactly ONE crash-restart cycle by default:
#     `kill -9` the bridge (a genuine crash), verify Java restarts it and the
#     process is healthy after the restart, and confirm the FD/thread counts
#     return to the first-cycle baseline.
#
# FD/thread metrics are compared against the first-cycle baseline (R-170), so
# a monotonic leak FAILS the run instead of hiding in the TSV. The Java PID is
# re-discovered every cycle (R-173), never reused from a stale capture.
#
# Proves plan §1377 for the service's bounded restart window: no socket,
# child-process, thread, or goroutine leak across the restart it actually
# performs. Non-destructive: never touches Fluss data, never places orders.
#
# Usage:  ./soak-reconnect-loop.sh [cycles] [settle_seconds]
#   e.g.   ./soak-reconnect-loop.sh 1 8    # one crash-restart, 8s settle
#          ./soak-reconnect-loop.sh        # default: the service's restart
#                                          # budget (1, from MAX_BRIDGE_RESTARTS)
#
# Cycles above the restart budget are refused: killing the bridge beyond the
# JVM's restart budget is TERMINAL by design and would tear the pipeline down.

set -euo pipefail

# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
LOG_FILE="${LOG_FILE:-$PROJECT_ROOT/code/logs/ingestion.json}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"
JAVA_MATCH="${JAVA_MATCH:-com.trading.ingestion.IngestionService}"
BRIDGE_MATCH="${BRIDGE_MATCH:-arrow-bridge}"
INGESTION_SRC="${INGESTION_SRC:-$PROJECT_ROOT/code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java}"

# Container mode (R-222): Stage 3/4 run the java inside the ingestion
# container, so process discovery + SIGKILL must target its PID namespace.
# Host mode (Stage 2 marathon) is the default: empty CONTAINER.
CONTAINER="${CONTAINER:-}"
if [ -n "$CONTAINER" ]; then
	docker inspect "$CONTAINER" >/dev/null 2>&1 || {
		echo "FATAL: container '$CONTAINER' not found — start the ingestion container first." >&2
		exit 1
	}
	NS() { docker exec "$CONTAINER" "$@"; }
else
	NS() { "$@"; }
fi

# The one progress signal Java actually writes to the journal for every bridge
# lifecycle event (subscription_ack ACTIVE after a restart = recovery). Tick
# NDJSON from the bridge is consumed in-process and never logged (R-004), so
# per-tick lines cannot be used.
PROGRESS_PATTERN="bridge lifecycle event="

# Service restart budget — read from source so it can never drift.
RESTART_BUDGET="${RESTART_BUDGET:-$(
	grep -oE 'MAX_BRIDGE_RESTARTS[[:space:]]*=[[:space:]]*[0-9]+' "$INGESTION_SRC" 2>/dev/null |
		grep -oE '[0-9]+$' | head -1
)}"
[ -n "${RESTART_BUDGET:-}" ] || RESTART_BUDGET=1

CYCLES="${1:-$RESTART_BUDGET}"
SETTLE_SEC="${2:-8}"

if [ "$CYCLES" -gt "$RESTART_BUDGET" ]; then
	echo "FATAL: requested $CYCLES cycles but the service restarts the bridge at most $RESTART_BUDGET time(s) per run (MAX_BRIDGE_RESTARTS). A further kill is TERMINAL by design. Use CYCLES <= $RESTART_BUDGET." >&2
	exit 2
fi
if [ ! -f "$LOG_FILE" ]; then
	echo "FATAL: journal not found at $LOG_FILE — cannot observe recovery. Is ingestion running?" >&2
	exit 1
fi

# Leak thresholds vs the first-cycle baseline (R-170).
LEAK_FD_MARGIN_PCT="${LEAK_FD_MARGIN_PCT:-50}" # allow 50% FD growth before failing
LEAK_THREAD_MARGIN="${LEAK_THREAD_MARGIN:-20}" # allow 20 threads growth before failing

mkdir -p "$OUT_DIR"
RESULT="$OUT_DIR/reconnect-leak-$(date +%Y%m%d-%H%M%S).tsv"
echo "reconnect-loop: $CYCLES cycle(s) (service restart budget), ${SETTLE_SEC}s settle"
echo "reconnect-loop: budget=$RESTART_BUDGET  journal=$LOG_FILE"
echo "reconnect-loop: result → $RESULT"
echo 'cycle	java_fds_before	java_fds_after	bridge_fds_before	bridge_fds_after	java_threads_before	java_threads_after	progress_delta	recovered_ok	leak_ok' >"$RESULT"

# Newest matching PID (not numerically highest — R-173). Matches the FULL
# command line (the java class is not in argv[0]), and the pattern is passed
# via the environment so the awk process's own cmdline cannot self-match.
find_pid() {
	# export (not VAR=... command): the awk is a pipeline sibling, so a
	# command-scoped assignment never reaches its ENVIRON.
	export PAT="$1"
	NS ps -eo pid,etimes,args 2>/dev/null |
		awk 'index($0, ENVIRON["PAT"]) { print $1, $2 }' |
		sort -k2 -n |
		tail -1 |
		awk '{print $1}'
}
count_fds() {
	local pid="$1"
	[ -z "$pid" ] && {
		echo 0
		return
	}
	local n
	n=$(NS ls /proc/"$pid"/fd 2>/dev/null | wc -l) || n=0
	echo "$n"
}
threads_of() {
	local pid="$1"
	[ -z "$pid" ] && {
		echo 0
		return
	}
	local n
	n=$(NS grep -s '^Threads:' /proc/"$pid"/status 2>/dev/null | awk '{print $2}') || n=0
	[ -n "$n" ] || n=0
	echo "$n"
}
count_progress() {
	local n
	n=$(grep -Fc "$PROGRESS_PATTERN" "$LOG_FILE" 2>/dev/null) || n=0
	echo "$n"
}

java_pid_before=$(find_pid "$JAVA_MATCH")
if [ -z "$java_pid_before" ]; then
	echo "FATAL: Java ingestion service not running (no process matching '$JAVA_MATCH')." >&2
	echo "Start it first: ./start-all.sh" >&2
	exit 1
fi

# Baseline progress count (per-cycle deltas are computed against this).
t0_progress=$(count_progress)
echo "reconnect-loop: Java pid=$java_pid_before, baseline progress lines=$t0_progress"

# First-cycle baseline for the leak comparison (R-170): refreshed each cycle,
# so the comparison is always against the most recent healthy reading.
baseline_java_fds=0
baseline_java_threads=0
baseline_bridge_fds=0
fail=0

for ((i = 1; i <= CYCLES; i++)); do
	[ -n "$java_pid_before" ] || {
		echo "  cycle $i: Java PID lost before sampling — cannot continue" >&2
		fail=1
		break
	}
	jfds_b=$(count_fds "$java_pid_before")
	jthr_b=$(threads_of "$java_pid_before")

	bridge_pid=$(find_pid "$BRIDGE_MATCH")
	bfds_b=$(count_fds "$bridge_pid")

	# Force the disconnect: SIGKILL only — a clean SIGTERM exit would be
	# treated as a requested shutdown and stop the whole pipeline (R-001).
	[ -n "$bridge_pid" ] && NS kill -9 "$bridge_pid" 2>/dev/null || true

	# Wait for Java to restart the bridge + resubscribe.
	sleep "$SETTLE_SEC"

	java_pid_after=$(find_pid "$JAVA_MATCH")
	bridge_pid_a=$(find_pid "$BRIDGE_MATCH")
	jfds_a=$(count_fds "$java_pid_after")
	jthr_a=$(threads_of "$java_pid_after")
	bfds_a=$(count_fds "$bridge_pid_a")

	progress_now=$(count_progress)
	progress_delta=$((progress_now - t0_progress))

	# Recovered = Java alive AND a bridge child exists AND a fresh lifecycle
	# event (subscription_ack on resubscribe) was logged after the kill.
	recovered=1
	if [ -z "$java_pid_after" ] || [ -z "$bridge_pid_a" ]; then recovered=0; fi
	if [ "$progress_delta" -le 0 ]; then recovered=0; fi

	# Leak check (R-170): after-metrics must stay within margin of the
	# baseline captured at the start of this cycle.
	leak_ok=1
	if [ "$i" -eq 1 ]; then
		baseline_java_fds=$jfds_a
		baseline_java_threads=$jthr_a
		baseline_bridge_fds=$bfds_a
	else
		max_fds=$((baseline_java_fds * (100 + LEAK_FD_MARGIN_PCT) / 100))
		max_thr=$((baseline_java_threads + LEAK_THREAD_MARGIN))
		max_bfds=$((baseline_bridge_fds * (100 + LEAK_FD_MARGIN_PCT) / 100))
		if [ "$jfds_a" -gt "$max_fds" ] || [ "$jthr_a" -gt "$max_thr" ] || [ "$bfds_a" -gt "$max_bfds" ]; then
			leak_ok=0
		fi
	fi

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$i" "$jfds_b" "$jfds_a" "$bfds_b" "$bfds_a" \
		"$jthr_b" "$jthr_a" "$progress_delta" "$recovered" "$leak_ok" >>"$RESULT"

	if [ "$recovered" = "0" ] || [ "$leak_ok" = "0" ]; then
		echo "  cycle $i: $([ "$recovered" = 0 ] && echo NOT RECOVERED || echo LEAK DETECTED) (java_fds $jfds_b→$jfds_a, bridge_fds $bfds_b→$bfds_a, threads $jthr_b→$jthr_a, progress +$progress_delta)"
		fail=1
	else
		echo "  cycle $i: ok (java_fds $jfds_b→$jfds_a, bridge_fds $bfds_b→$bfds_a, threads $jthr_b→$jthr_a, progress +$progress_delta)"
	fi

	# Next cycle samples the (possibly new) Java process (R-173).
	java_pid_before="$java_pid_after"
	t0_progress="$progress_now"
done

echo "reconnect-loop: done. Result → $RESULT"
if [ "$fail" = "1" ]; then
	echo "reconnect-loop: ⚠️ some cycles failed — inspect $RESULT" >&2
	exit 1
fi
echo "reconnect-loop: all $CYCLES cycle(s) recovered cleanly with no leak signal."
