#!/usr/bin/env bash
# chaos-run.sh — T13 failure chaos suite runner (plan section 8; docs/
# 08_implementation/22-failure-chaos-suite.md). Runs the four failure tests
# IN ORDER with per-test pass/fail/skip output and a terminal status marker.
#
#   01 slot kill    (Go bridge, offline, always runs)
#   02 TM kill      (MiniCluster IT, offline, always runs + optional live leg)
#   03 tablet kill  (live Fluss stack required — SKIP 3 when absent)
#   04 VM loss      (multi-node Swarm required — SKIP 3 when absent)
#
# Exit codes: 0 = no test failed (SKIPs allowed), 1 = at least one FAIL.
# Per-test codes: 0 PASS, 1 FAIL, 3 SKIP (prerequisite absent — the live
# deployment runs are executed later, serially, by the main agent).
#
# Usage:  ./chaos-run.sh            (or: make chaos-suite)
# Env:    CHAOS_OUT_DIR   evidence root (default $PROJECT_ROOT/logs/chaos/<ts>)
#         CHAOS_TIMEOUT_SEC per-test wall-clock cap when `timeout` exists
#         (default 3600); pass-through env (FLUSS_BOOTSTRAP, TABLET_CONTAINER,
#         CHAOS_ORDER_PROBE_TCP, CHAOS_WORKLOAD_NODE, ...) is inherited.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../../.." && pwd)}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/chaos/chaos-$(date +%Y%m%d-%H%M%S)}"
CHAOS_TIMEOUT_SEC="${CHAOS_TIMEOUT_SEC:-3600}"

mkdir -p "$OUT_DIR"
SUMMARY="$OUT_DIR/SUMMARY.txt"
: >"$SUMMARY"

# ── static self-check: every suite script must be syntactically valid ──────
STATIC_FAIL=0
for s in "$SCRIPT_DIR"/chaos-*.sh "$SCRIPT_DIR"/chaos-run.sh; do
	if ! bash -n "$s" >>"$OUT_DIR/static.log" 2>&1; then
		echo "chaos-run: FAIL — bash -n $s" | tee -a "$SUMMARY"
		STATIC_FAIL=1
	fi
	if command -v shellcheck >/dev/null 2>&1; then
		if ! shellcheck -S warning "$s" >>"$OUT_DIR/static.log" 2>&1; then
			echo "chaos-run: FAIL — shellcheck $s" | tee -a "$SUMMARY"
			STATIC_FAIL=1
		fi
	fi
done
if [ "$STATIC_FAIL" -ne 0 ]; then
	echo "CHAOS-SUITE: RESULT=FAIL EXIT=1" | tee -a "$SUMMARY"
	exit 1
fi

# ── the four failure tests, in order ───────────────────────────────────────
TESTS=(
	"chaos-01-slot-kill.sh|01 slot kill — Go bridge, 1-of-3 slots"
	"chaos-02-tm-kill.sh|02 TM kill — restore from checkpoint, no dup candle/signal"
	"chaos-03-tablet-kill.sh|03 tablet kill — LOG x3, no acked-row loss"
	"chaos-04-vm-loss.sh|04 VM loss — halt <5s, recovery <30s, overlay re-route"
)

overall=0
idx=0
for entry in "${TESTS[@]}"; do
	idx=$((idx + 1))
	script="${entry%%|*}"
	title="${entry#*|}"
	echo ""
	echo "=== [$idx/${#TESTS[@]}] $title ===" | tee -a "$SUMMARY"
	TEST_LOG="$OUT_DIR/${script%.sh}.log"
	set +e
	if command -v timeout >/dev/null 2>&1; then
		timeout --signal=KILL "$CHAOS_TIMEOUT_SEC" bash "$SCRIPT_DIR/$script" 2>&1 |
			tee "$TEST_LOG"
	else
		bash "$SCRIPT_DIR/$script" 2>&1 | tee "$TEST_LOG"
	fi
	rc=${PIPESTATUS[0]}
	set -e
	case "$rc" in
	0)
		echo "RESULT [$idx]: PASS" | tee -a "$SUMMARY"
		;;
	3)
		echo "RESULT [$idx]: SKIP (prerequisite absent — see $TEST_LOG)" | tee -a "$SUMMARY"
		;;
	*)
		echo "RESULT [$idx]: FAIL (rc=$rc — see $TEST_LOG)" | tee -a "$SUMMARY"
		overall=1
		;;
	esac
done

echo ""
echo "chaos-run: evidence → $OUT_DIR"
if [ "$overall" -eq 0 ]; then
	echo "CHAOS-SUITE: RESULT=PASS EXIT=0" | tee -a "$SUMMARY"
	exit 0
fi
echo "CHAOS-SUITE: RESULT=FAIL EXIT=1" | tee -a "$SUMMARY"
exit 1
