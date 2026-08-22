#!/usr/bin/env bash
# chaos-03-tablet-kill.sh — T13 failure chaos test 3: kill the Fluss tablet.
#
# Live-leg IT (TabletKillChaosIntegrationTest, DUR-TABLETKILL-001):
#   * writes rows to raw_table_1 with the same fluss-client append path the
#     bridge uses and ACKS them BEFORE the kill,
#   * SIGKILLs the tablet container (auto-discovered, TABLET_CONTAINER
#     overrides),
#   * waits for the restart policy to bring it back and the table to become
#     readable again,
#   * asserts every acked fingerprint is still readable and the immutable LOG
#     count never shrank (no data loss for already-acked rows),
#   * reports the coordinator-stamped table.replication.factor (prod contract
#     x3) and asserts it when CHAOS_REPLICATION_REQUIRED=true.
#
# The gate is env-gated like every *IT in this repo: no live Fluss stack →
# SKIP (exit 3), not FAIL.
#
# Invariant under test (plan section 6, T12/T8 storage rows): tablet death →
# LOG replication x3 → no data loss for acked rows; KV rebuildable; no
# silent truncation (a repair-tablet.sh-grade tail corruption surfaces as a
# missing acked row / failed scan and FAILs this test).
#
# Usage: ./chaos-03-tablet-kill.sh
# Env:   FLUSS_BOOTSTRAP        (default localhost:9123)
#        TABLET_CONTAINER       (default: auto-detect name=fluss-tablet)
#        CHAOS_REPLICATION_REQUIRED (default false; true on prod-like Swarm)
#        CHAOS_REPLICATION_MIN   (default 3)
#        TABLET_KILL_ROWS        (default 25)
#        CHAOS_SCAN_LIMIT_ROWS   (default 200000; -1 = full-LOG scan for the
#                                 prod gate — heap-limited dev boxes bound the
#                                 scan to the last N rows while the row count
#                                 still covers the whole log, one streaming
#                                 pass, O(1) extra heap)
#        CHAOS_SCAN_TIMEOUT_SEC  (default 600; scan fails with guidance when
#                                 the log is written continuously and never
#                                 reaches quiescence)
#        MAVEN_TIMEOUT_SEC       (default 1200)
# Exit:  0 PASS, 1 FAIL, 3 SKIP (no live stack / no docker)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../../.." && pwd)}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/chaos/chaos-$(date +%Y%m%d-%H%M%S)}"
COMPUTE_POM="${CHAOS_COMPUTE_POM:-$PROJECT_ROOT/code/02_services/02_compute/pom.xml}"
MAVEN_TIMEOUT_SEC="${MAVEN_TIMEOUT_SEC:-1200}"
FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
CHAOS_SCAN_LIMIT_ROWS="${CHAOS_SCAN_LIMIT_ROWS:-200000}"
CHAOS_SCAN_TIMEOUT_SEC="${CHAOS_SCAN_TIMEOUT_SEC:-600}"

for tool in docker mvn java; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		echo "TABLET-KILL-CHAOS-03: SKIP — required tool '$tool' not found on PATH"
		exit 3
	fi
done
if [ ! -f "$COMPUTE_POM" ]; then
	echo "TABLET-KILL-CHAOS-03: SKIP — compute pom missing: $COMPUTE_POM"
	exit 3
fi

TABLET_CONTAINER="${TABLET_CONTAINER:-$(docker ps --filter "name=fluss-tablet" --format '{{.Names}}' | head -1 || true)}"
if [ -z "$TABLET_CONTAINER" ]; then
	echo "TABLET-KILL-CHAOS-03: SKIP — no fluss-tablet container (start the stack; deployment runs are executed serially by the main agent)"
	exit 3
fi
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/chaos-03-tablet-kill.log"

echo "TABLET-KILL-CHAOS-03: live leg — bootstrap=$FLUSS_BOOTSTRAP tablet=$TABLET_CONTAINER"
echo "TABLET-KILL-CHAOS-03: replication contract: required=${CHAOS_REPLICATION_REQUIRED:-false} min=${CHAOS_REPLICATION_MIN:-3}"
echo "TABLET-KILL-CHAOS-03: log → $LOG"

if [ "${CHAOS_DRY_RUN:-0}" = "1" ]; then
	echo "TABLET-KILL-CHAOS-03: DRY-RUN — would SIGKILL $TABLET_CONTAINER and assert acked-row survival via the IT (no cluster contact, nothing killed)"
	echo "TABLET-KILL-CHAOS-03: RESULT=PASS EXIT=0 (dry-run)"
	exit 0
fi

set +e
if command -v timeout >/dev/null 2>&1; then
	timeout --signal=KILL "$MAVEN_TIMEOUT_SEC" env \
		COMPUTE_INT_TEST_TABLET_KILL=true TABLET_CONTAINER="$TABLET_CONTAINER" \
		FLUSS_BOOTSTRAP="$FLUSS_BOOTSTRAP" \
		CHAOS_SCAN_LIMIT_ROWS="$CHAOS_SCAN_LIMIT_ROWS" \
		CHAOS_SCAN_TIMEOUT_SEC="$CHAOS_SCAN_TIMEOUT_SEC" \
		mvn -o -f "$COMPUTE_POM" test -Dtest=TabletKillChaosIntegrationTest 2>&1 |
		tee "$LOG"
else
	COMPUTE_INT_TEST_TABLET_KILL=true TABLET_CONTAINER="$TABLET_CONTAINER" \
		FLUSS_BOOTSTRAP="$FLUSS_BOOTSTRAP" \
		CHAOS_SCAN_LIMIT_ROWS="$CHAOS_SCAN_LIMIT_ROWS" \
		CHAOS_SCAN_TIMEOUT_SEC="$CHAOS_SCAN_TIMEOUT_SEC" \
		mvn -o -f "$COMPUTE_POM" test -Dtest=TabletKillChaosIntegrationTest 2>&1 |
		tee "$LOG"
fi
RC=${PIPESTATUS[0]}
set -e

if [ "$RC" -ne 0 ]; then
	echo "TABLET-KILL-CHAOS-03: FAIL — IT rc=$RC (see $LOG)" >&2
	echo "TABLET-KILL-CHAOS-03: RESULT=FAIL EXIT=1"
	exit 1
fi
echo "TABLET-KILL-CHAOS-03: PASS — tablet killed+recovered, all acked rows readable, LOG count non-decreasing"
echo "TABLET-KILL-CHAOS-03: RESULT=PASS EXIT=0"
