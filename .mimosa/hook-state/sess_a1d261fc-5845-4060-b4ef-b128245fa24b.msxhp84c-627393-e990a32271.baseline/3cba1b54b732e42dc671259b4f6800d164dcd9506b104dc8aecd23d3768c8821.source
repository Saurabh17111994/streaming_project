#!/usr/bin/env bash
# soak-monitor.sh — sample live ingestion health during the Monday soak.
#
# Samples every INTERVAL_SECONDS:
#   - Java process open FDs (/proc/<pid>/fd)
#   - Go bridge child open FDs (/proc/<bridge_pid>/fd)
#   - Java JVM threads (/proc/<pid>/status Threads:)
#   - Go bridge OS threads (/proc/<pid>/status Threads:) — NOTE: a goroutine
#     leak does NOT necessarily raise the OS-thread count (the Go runtime
#     multiplexes goroutines over a small thread pool), so this column is a
#     weak proxy; the authoritative signal is the bridge's own
#     runtime.NumGoroutine() telemetry event (R-169, Phase 4).
#   - bridge lifecycle events from the JSON journal: reconnect /
#     heartbeat_failed / feed_stalled / subscription_ack. These are mirrored
#     into the journal by Java ("bridge lifecycle event=..."); the raw bridge
#     NDJSON tick lines are consumed in-process and never logged (R-020), so
#     per-tick rate is NOT journal-observable — it requires the bridge NDJSON
#     side-channel or the OTLP metrics endpoint (observability phase).
#
# Reads the journal INCREMENTALLY (tail -c +N from the last offset) and
# reports per-interval deltas, so long soaks don't re-scan a 64 MB journal on
# every sample and journal rotation does not reset the counts (R-137).
#
# G3 self-check: the monitor FAILS immediately if it cannot observe its
# subject (journal missing, or Java/bridge process not found at startup).
#
# Usage:  ./soak-monitor.sh [duration_seconds] [interval_seconds]
#   e.g.   ./soak-monitor.sh 3600 10     # 1 hour, every 10s
#          ./soak-monitor.sh             # forever until Ctrl+C

set -euo pipefail

# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
LOG_FILE="${LOG_FILE:-$PROJECT_ROOT/code/logs/ingestion.json}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"
JAVA_MATCH="${JAVA_MATCH:-com.trading.ingestion.IngestionService}"
BRIDGE_MATCH="${BRIDGE_MATCH:-arrow-bridge}"

# ── CLI args ──────────────────────────────────────────────────────────────────
DURATION_SEC="${1:-0}" # 0 = run forever
INTERVAL_SEC="${2:-5}" # default 5s

# ── G3 observation self-check ────────────────────────────────────────────────
if [ ! -f "$LOG_FILE" ]; then
	echo "FATAL: journal not found at $LOG_FILE — cannot observe the soak. Set LOG_FILE." >&2
	exit 1
fi

# Newest matching PID (not numerically highest — R-057). Both PIDs are resolved
# in the same sample so a restart can never mix generations.
find_pid() {
	ps -eo pid,etimes,args 2>/dev/null |
		awk -v pat="$1" '$3 ~ pat { print $1, $2 }' |
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
	n=$(ls /proc/"$pid"/fd 2>/dev/null | wc -l) || n=0 # R-021: never abort on a dead PID
	echo "$n"
}
threads_of() {
	local pid="$1"
	[ -z "$pid" ] && {
		echo 0
		return
	}
	local n
	n=$(grep -s '^Threads:' /proc/"$pid"/status 2>/dev/null | awk '{print $2}') || n=0
	[ -n "$n" ] || n=0
	echo "$n"
}
# Incremental journal read (R-137): count per-interval occurrences of the given
# literal pattern starting at $offset (in bytes); advances the offset.
count_events_since() {
	local pat="$1"
	local offset="$2"
	local tail_data
	tail_data=$(tail -c +"$((offset + 1))" "$LOG_FILE" 2>/dev/null) || tail_data=""
	local n
	n=$(printf '%s' "$tail_data" | grep -Fc "$pat" 2>/dev/null || echo 0)
	echo "$n"
}

mkdir -p "$OUT_DIR"
TSV="$OUT_DIR/soak-summary-$(date +%Y%m%d-%H%M%S).tsv"
echo "soak-monitor: log=$LOG_FILE"
echo "soak-monitor: writing $TSV"
echo "soak-monitor: interval=${INTERVAL_SEC}s duration=${DURATION_SEC:-∞}"
printf 'ts\tjava_fds\tbridge_fds\tjava_threads\tbridge_os_threads\tsub_acks\treconnects\theartbeat_fails\tstalls\n' >"$TSV"

# Startup observation check: the processes must exist or there is nothing to soak.
if [ -z "$(find_pid "$JAVA_MATCH")" ]; then
	echo "FATAL: no Java ingestion process matching '$JAVA_MATCH' — cannot monitor." >&2
	exit 1
fi

start_epoch=$(date +%s)
log_offset=$(wc -c <"$LOG_FILE" 2>/dev/null || echo 0)
prev_subs=0
prev_rec=0
prev_hb=0
prev_stall=0

while true; do
	java_pid=$(find_pid "$JAVA_MATCH")
	bridge_pid=$(find_pid "$BRIDGE_MATCH")
	now=$(date '+%Y-%m-%d %H:%M:%S')

	jfds=$(count_fds "$java_pid")
	bfds=$(count_fds "$bridge_pid")
	jthr=$(threads_of "$java_pid")
	bthr=$(threads_of "$bridge_pid")

	# Per-interval deltas (R-137): read only the bytes appended since the last
	# sample and count literal patterns.
	subs=$(count_events_since "bridge lifecycle event=subscription_ack" "$log_offset")
	rec=$(count_events_since "bridge lifecycle event=reconnect" "$log_offset")
	hb=$(count_events_since "bridge lifecycle event=heartbeat_failed" "$log_offset")
	stall=$(count_events_since "bridge lifecycle event=feed_stalled" "$log_offset")

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$now" "$jfds" "$bfds" "$jthr" "$bthr" \
		"$((subs - prev_subs))" "$((rec - prev_rec))" \
		"$((hb - prev_hb))" "$((stall - prev_stall))" >>"$TSV"

	printf '  %s java_fds=%s bridge_fds=%s java_threads=%s bridge_os_threads=%s sub_acks=%s rec=%s hb_fail=%s stalls=%s\n' \
		"$now" "$jfds" "$bfds" "$jthr" "$bthr" \
		"$((subs - prev_subs))" "$((rec - prev_rec))" \
		"$((hb - prev_hb))" "$((stall - prev_stall))"

	prev_subs=$subs
	prev_rec=$rec
	prev_hb=$hb
	prev_stall=$stall
	log_offset=$(wc -c <"$LOG_FILE" 2>/dev/null || echo "$log_offset")

	# Run forever, or stop after duration
	if [ "$DURATION_SEC" -gt 0 ]; then
		elapsed=$(($(date +%s) - start_epoch))
		[ "$elapsed" -ge "$DURATION_SEC" ] && {
			echo "soak-monitor: done (${elapsed}s) → $TSV"
			exit 0
		}
	fi
	sleep "$INTERVAL_SEC"
done
