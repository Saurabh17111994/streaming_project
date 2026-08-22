#!/usr/bin/env bash
# chaos-01-slot-kill.sh — T13 failure chaos test 1: kill 1 of 3 bridge slots.
#
# Deterministic offline leg (no cluster, no broker): runs the Go bridge's
# named supervisor/resilience tests that IMPLEMENT the slot-kill chaos:
#   * TestSubscriptionPlanShards3000Tokens          — 3000-token plan is
#     1024+1024+952 (T1 baseline; the assertions below only mean something
#     when the sharding itself is deterministic).
#   * TestSupervisorAuthTerminalIsolatedPerSlot     — the kill itself: slot-0
#     is driven TERMINAL (3 failed auth refreshes) while slots 1+2 stay
#     ACTIVE with ticks flowing; asserts exactly one terminal outcome, the
#     shared context NOT cancelled, peer sockets open, peer ticks delivered.
#   * TestINGRES001HealthySlotNotInterruptedByPeerReconnect — the reconnect
#     leg: slot-0 drop-cycles through the real SDK while slot-1 stays ACTIVE
#     with zero disconnect events (no cross-slot drop regression).
#   * TestReconnectLoopRecoversAfterFailures / TestReconnectLoopEpochAndBackoffAfterForcedDisconnect
#     — per-slot reconnect + epoch advance after a forced disconnect.
#   * TestINGRES001OneHundredForcedDisconnectReconnectCycles — 100 forced
#     disconnect/reconnect cycles: every cycle recovers to ACTIVE, no
#     goroutine/FD/socket leak (drop-missed resource regression).
#
# Invariant under test (plan section 6 row 1): killing one slot must not
# kill or degrade the peers — no cross-slot kill, no peer drop, exactly one
# terminal outcome, and the killed slot's retryable path reconnects.
#
# Usage: ./chaos-01-slot-kill.sh   (GO_TIMEOUT_SEC, GO test env as usual)
# Exit: 0 PASS, 1 FAIL (3 is never emitted by this test — it is always
# runnable; preflight failures are FAILs, not skips).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../../.." && pwd)}"
BRIDGE_DIR="${BRIDGE_DIR:-$PROJECT_ROOT/code/02_services/01_ingestion/go-bridge}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/chaos/chaos-$(date +%Y%m%d-%H%M%S)}"
GO_TIMEOUT_SEC="${GO_TIMEOUT_SEC:-900}"

if ! command -v go >/dev/null 2>&1; then
	echo "SLOT-KILL-CHAOS-01: FAIL — 'go' not found on PATH (required for the offline leg)"
	exit 1
fi
if [ ! -d "$BRIDGE_DIR" ]; then
	echo "SLOT-KILL-CHAOS-01: FAIL — go-bridge dir missing: $BRIDGE_DIR" >&2
	exit 1
fi
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/chaos-01-slot-kill.log"

# The named tests that implement the chaos assertions (kept as one -run regex
# so the whole leg is a single go test invocation).
TESTS='TestSubscriptionPlanShards3000Tokens|TestSupervisorAuthTerminalIsolatedPerSlot|TestINGRES001HealthySlotNotInterruptedByPeerReconnect|TestReconnectLoopRecoversAfterFailures|TestReconnectLoopEpochAndBackoffAfterForcedDisconnect|TestINGRES001OneHundredForcedDisconnectReconnectCycles$'

echo "SLOT-KILL-CHAOS-01: offline leg — go test (timeout ${GO_TIMEOUT_SEC}s)"
echo "SLOT-KILL-CHAOS-01: tests: $TESTS"
echo "SLOT-KILL-CHAOS-01: log → $LOG"

cd "$BRIDGE_DIR"
set +e
if command -v timeout >/dev/null 2>&1; then
	timeout --signal=KILL "$GO_TIMEOUT_SEC" go test -count=1 -v -run "$TESTS" . 2>&1 | tee "$LOG"
else
	go test -count=1 -v -run "$TESTS" . 2>&1 | tee "$LOG"
fi
RC=${PIPESTATUS[0]}
set -e

if [ "$RC" -ne 0 ]; then
	echo "SLOT-KILL-CHAOS-01: FAIL — go test rc=$RC (see $LOG)" >&2
	echo "SLOT-KILL-CHAOS-01: RESULT=FAIL EXIT=1"
	exit 1
fi

# Named-assertion pass markers (each assertion lives in the Go test, so the
# gate re-states them as a checkable summary instead of re-implementing).
for marker in \
	"PASS: TestSubscriptionPlanShards3000Tokens" \
	"PASS: TestSupervisorAuthTerminalIsolatedPerSlot" \
	"PASS: TestINGRES001HealthySlotNotInterruptedByPeerReconnect" \
	"PASS: TestReconnectLoopRecoversAfterFailures" \
	"PASS: TestReconnectLoopEpochAndBackoffAfterForcedDisconnect" \
	"PASS: TestINGRES001OneHundredForcedDisconnectReconnectCycles"; do
	name="${marker#PASS: }"
	if grep -q "^--- PASS: $name" "$LOG"; then
		echo "$marker"
	else
		echo "SLOT-KILL-CHAOS-01: FAIL — $name did not pass in the run" >&2
		echo "SLOT-KILL-CHAOS-01: RESULT=FAIL EXIT=1"
		exit 1
	fi
done

echo "SLOT-KILL-CHAOS-01: PASS — 1-of-3 slot kill: terminal isolated, peers healthy, reconnect clean"
echo "SLOT-KILL-CHAOS-01: RESULT=PASS EXIT=0"
