#!/usr/bin/env bash
# soak-monitor.sh — sample live ingestion health during the Monday soak.
#
# Samples every INTERVAL_SECONDS:
#   - Java process open FDs (/proc/<pid>/fd)
#   - Go bridge child open FDs (/proc/<bridge_pid>/fd)
#   - Go bridge goroutines (reads /proc/<pid>/stat via a tiny `go`-free trick:
#     bridge runtime.NumGoroutine is NOT exposed on proc; we approximate via
#     thread count from /proc/<pid>/status Threads: line — the Go runtime maps
#     goroutines onto threads, so a stable thread count implies no goroutine
#     explosion at the process level).
#   - Java JVM threads (/proc/<pid>/status Threads:)
#   - reconnect / heartbeat_failed / feed_stalled events from the live log
#   - raw tick count (grep '"feed":"hft"')
#
# Writes one TSV row per sample to $OUT_DIR/soak-summary-<date>.tsv and a
# human-readable tail to the same file every sample.
#
# Usage:  ./soak-monitor.sh [duration_seconds] [interval_seconds]
#   e.g.   ./soak-monitor.sh 3600 10     # 1 hour, every 10s
#          ./soak-monitor.sh             # forever until Ctrl+C

set -euo pipefail

# ── Config (edit paths here, or override via env) ─────────────────────────────
PROJECT_ROOT="${PROJECT_ROOT:-/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project}"
LOG_FILE="${LOG_FILE:-$PROJECT_ROOT/logs/ingestion.log}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"
JAVA_MATCH="${JAVA_MATCH:-com.trading.ingestion.IngestionService}"
BRIDGE_MATCH="${BRIDGE_MATCH:-arrow-bridge}"

# ── CLI args ──────────────────────────────────────────────────────────────────
DURATION_SEC="${1:-0}"      # 0 = run forever
INTERVAL_SEC="${2:-5}"      # default 5s

mkdir -p "$OUT_DIR"
TSV="$OUT_DIR/soak-summary-$(date +%Y%m%d-%H%M%S).tsv"
echo "soak-monitor: log=$LOG_FILE"
echo "soak-monitor: writing $TSV"
echo "soak-monitor: interval=${INTERVAL_SEC}s duration=${DURATION_SEC:-∞}"
printf 'ts\tjava_fds\tbridge_fds\tjava_threads\tbridge_threads\tticks\treconnects\theartbeat_fails\tstalls\n' > "$TSV"

# PID discovery — most recent matching process.
find_pid() {
	pgrep -f "$1" | tail -1 || true
}
count_fds() {
	local pid="$1"; [ -z "$pid" ] && { echo 0; return; }
	ls /proc/"$pid"/fd 2>/dev/null | wc -l
}
threads_of() {
	local pid="$1"; [ -z "$pid" ] && { echo 0; return; }
	grep -s '^Threads:' /proc/"$pid"/status | awk '{print $2}' || echo 0
}
count_events() {
	# count occurrences of a pattern in the live log (single line)
	local pat="$1"
	local n
	n=$(grep -c "$pat" "$LOG_FILE" 2>/dev/null) || n=0
	echo "$n"
}

start_epoch=$(date +%s)
while true; do
	java_pid=$(find_pid "$JAVA_MATCH")
	bridge_pid=$(find_pid "$BRIDGE_MATCH")
	now=$(date '+%Y-%m-%d %H:%M:%S')

	jfds=$(count_fds "$java_pid")
	bfds=$(count_fds "$bridge_pid")
	jthr=$(threads_of "$java_pid")
	bthr=$(threads_of "$bridge_pid")
	ticks=$(count_events '"feed":"hft"')
	reconnects=$(count_events '"event":"reconnect"')
	hbfail=$(count_events 'heartbeat_failed')
	stalls=$(count_events 'feed_stalled')

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$now" "$jfds" "$bfds" "$jthr" "$bthr" \
		"$ticks" "$reconnects" "$hbfail" "$stalls" >> "$TSV"

	# Human-readable tail line
	printf '  %s java_fds=%s bridge_fds=%s java_threads=%s bridge_threads=%s ticks=%s rec=%s hb_fail=%s stalls=%s\n' \
		"$now" "$jfds" "$bfds" "$jthr" "$bthr" \
		"$ticks" "$reconnects" "$hbfail" "$stalls"

	# Run forever, or stop after duration
	if [ "$DURATION_SEC" -gt 0 ]; then
		elapsed=$(( $(date +%s) - start_epoch ))
		[ "$elapsed" -ge "$DURATION_SEC" ] && { echo "soak-monitor: done (${elapsed}s) → $TSV"; exit 0; }
	fi
	sleep "$INTERVAL_SEC"
done
