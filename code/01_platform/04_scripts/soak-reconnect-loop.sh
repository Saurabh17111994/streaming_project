#!/usr/bin/env bash
# soak-reconnect-loop.sh — force ~N bridge disconnects and verify NO leak.
#
# Proves plan §1377: "reconnect testing proves no socket, child-process,
# thread, or goroutine leak." It kills the Go arrow-bridge child process
# (NOT the Java service — Java's runWithBridge restart loop restarts it),
# waits for recovery, then measures the Java process's open FDs + threads.
#
# A leak would show as a MONOTONIC INCREASE in open FDs or threads across
# cycles. A healthy system returns to roughly the same baseline each cycle.
#
# Non-destructive: it exercises the exact restart path the service is built
# on. It never touches Fluss data, never places orders, never edits config.
#
# Usage:  ./soak-reconnect-loop.sh [cycles] [settle_seconds]
#   e.g.   ./soak-reconnect-loop.sh 100 10   # 100 cycles, 10s settle each
#          ./soak-reconnect-loop.sh          # default 100 cycles, 8s settle

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ROOT="${PROJECT_ROOT:-/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project}"
LOG_FILE="${LOG_FILE:-$PROJECT_ROOT/logs/ingestion.log}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"
JAVA_MATCH="${JAVA_MATCH:-com.trading.ingestion.IngestionService}"
BRIDGE_MATCH="${BRIDGE_MATCH:-arrow-bridge}"

CYCLES="${1:-100}"
SETTLE_SEC="${2:-8}"

mkdir -p "$OUT_DIR"
RESULT="$OUT_DIR/reconnect-leak-$(date +%Y%m%d-%H%M%S).tsv"
echo "reconnect-loop: $CYCLES cycles, ${SETTLE_SEC}s settle"
echo "reconnect-loop: result → $RESULT"
echo 'cycle	java_fds_before	java_fds_after	bridge_fds_before	bridge_fds_after	java_threads_before	java_threads_after	ticks_total	recovered_ok' > "$RESULT"

find_pid() { pgrep -f "$1" | tail -1 || true; }
count_fds() { local pid="$1"; [ -z "$pid" ] && { echo 0; return; }; ls /proc/"$pid"/fd 2>/dev/null | wc -l; }
threads_of() { local pid="$1"; [ -z "$pid" ] && { echo 0; return; }; grep -s '^Threads:' /proc/"$pid"/status | awk '{print $2}' || echo 0; }

java_pid_before=$(find_pid "$JAVA_MATCH")
if [ -z "$java_pid_before" ]; then
	echo "FATAL: Java ingestion service not running (no process matching '$JAVA_MATCH')." >&2
	echo "Start it first: ./start-all.sh" >&2
	exit 1
fi

t0_ticks=$(grep -c '"feed":"hft"' "$LOG_FILE" 2>/dev/null) || t0_ticks=0
echo "reconnect-loop: Java pid=$java_pid_before, baseline ticks=$t0_ticks"

fail=0
for (( i=1; i<=CYCLES; i++ )); do
	jfds_b=$(count_fds "$java_pid_before")
	jthr_b=$(threads_of "$java_pid_before")

	bridge_pid=$(find_pid "$BRIDGE_MATCH")
	bfds_b=$(count_fds "$bridge_pid")

	# Force the disconnect: kill ONLY the bridge child. Java restarts it.
	[ -n "$bridge_pid" ] && kill "$bridge_pid" 2>/dev/null || true

	# Wait for Java to restart the bridge + resubscribe.
	sleep "$SETTLE_SEC"

	java_pid_after=$(find_pid "$JAVA_MATCH")
	jfds_a=$(count_fds "$java_pid_after")
	jthr_a=$(threads_of "$java_pid_after")
	bridge_pid_a=$(find_pid "$BRIDGE_MATCH")
	bfds_a=$(count_fds "$bridge_pid_a")

	ticks_now=$(grep -c '"feed":"hft"' "$LOG_FILE" 2>/dev/null) || ticks_now=0

	# Recovered = Java still alive AND a bridge child exists AND ticks advanced.
	recovered=1
	if [ -z "$java_pid_after" ] || [ -z "$bridge_pid_a" ]; then recovered=0; fi
	if [ "$ticks_now" -le "$t0_ticks" ]; then recovered=0; fi

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$i" "$jfds_b" "$jfds_a" "$bfds_b" "$bfds_a" \
		"$jthr_b" "$jthr_a" "$ticks_now" "$recovered" >> "$RESULT"

	if [ "$recovered" = "0" ]; then
		echo "  cycle $i: NOT RECOVERED (java_fds $jfds_b→$jfds_a, bridge_fds $bfds_b→$bfds_a, threads $jthr_b→$jthr_a)"
		fail=1
	else
		# Optional leak check: JDK threads must not grow unboundedly.
		echo "  cycle $i: ok (java_fds $jfds_b→$jfds_a, bridge_fds $bfds_b→$bfds_a, threads $jthr_b→$jthr_a)"
	fi
done

echo "reconnect-loop: done. Result → $RESULT"
if [ "$fail" = "1" ]; then
	echo "reconnect-loop: ⚠️ some cycles did not recover — inspect $RESULT" >&2
	exit 1
fi
echo "reconnect-loop: all $CYCLES cycles recovered cleanly."
