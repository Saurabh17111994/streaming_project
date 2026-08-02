#!/usr/bin/env bash
# run-monday-gates.sh — ONE command to run the full Monday verification gate.
#
# Orchestrates Workstream A from the Monday soak plan:
#   1. Go bridge suite (no deps)
#   2. Pre-build the E2E test binaries the Java suite execs (R-016): the
#      FullStackE2ETest launches `go-bridge/faketool/faketool` (behind the
#      `faketool` build tag) and `go-bridge/arrow-bridge`; `go test` builds
#      neither, so a clean checkout would fail the E2E gate.
#   3. Full Java suite with ALL integration flags (needs local Fluss up)
#   4. Report PASS/FAIL with evidence file paths and a terminal status marker
#
# Usage:  ./run-monday-gates.sh
#   - The script FAILS (exit != 0) if any suite fails, so you can wire it
#     into CI or a cron.
#
# Prereqs: Fluss up (docker compose up -d), local ~/.m2 warm, Go 1.24+, JDK 17.

set -euo pipefail

# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
CODE_DIR="${CODE_DIR:-$PROJECT_ROOT/code}"
BRIDGE_DIR="${BRIDGE_DIR:-$CODE_DIR/02_services/01_ingestion/go-bridge}"
INGESTION_DIR="${INGESTION_DIR:-$CODE_DIR/02_services/01_ingestion}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak/monday-gates-$(date +%Y%m%d-%H%M%S)}"

# Suite timeouts (R-281) — a stuck JVM/Fluss must not block the gate forever.
GO_TIMEOUT_SEC="${GO_TIMEOUT_SEC:-1800}"
JAVA_TIMEOUT_SEC="${JAVA_TIMEOUT_SEC:-3600}"

# ── Preflight: required tooling + paths (R-150) ──────────────────────────────
for tool in go mvn java; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		echo "FATAL: required tool '$tool' not found on PATH" >&2
		exit 1
	fi
done
for dir in "$CODE_DIR" "$BRIDGE_DIR" "$INGESTION_DIR"; do
	if [ ! -d "$dir" ]; then
		echo "FATAL: expected directory missing: $dir" >&2
		exit 1
	fi
done

mkdir -p "$OUT_DIR"
GO_LOG="$OUT_DIR/go-suite.log"
JAVA_LOG="$OUT_DIR/java-suite.log"
SUMMARY="$OUT_DIR/SUMMARY.txt"

echo "run-monday-gates: output → $OUT_DIR"
echo "run-monday-gates: go timeout=${GO_TIMEOUT_SEC}s, java timeout=${JAVA_TIMEOUT_SEC}s"

gate_fail() {
	echo "GATE RESULT: FAIL" | tee -a "$SUMMARY"
	exit 1
}

# ── 1. Go suite ───────────────────────────────────────────────────────────────
echo "=== [1/3] Go bridge suite ===" | tee -a "$SUMMARY"
if ! timeout "$GO_TIMEOUT_SEC" bash -c "cd '$BRIDGE_DIR' && go test -count=1 ./..."; then
	echo "FAIL: Go suite failed or timed out — see $GO_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: Go suite" | tee -a "$SUMMARY"

# ── 2. Build E2E test binaries (R-016) ────────────────────────────────────────
echo "=== [2/3] Building E2E test binaries (faketool + arrow-bridge) ===" | tee -a "$SUMMARY"
if ! (cd "$BRIDGE_DIR" &&
	go build -tags faketool -o faketool/faketool ./faketool &&
	go build -o arrow-bridge .); then
	echo "FAIL: could not build E2E test binaries — see $GO_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: E2E binaries built (faketool/faketool, arrow-bridge)" | tee -a "$SUMMARY"

# ── 3. Full Java gate with ALL integration flags ──────────────────────────────
echo "=== [3/3] Java full gate (FLUSS+MANIFEST+PERF+E2E) ===" | tee -a "$SUMMARY"
if ! timeout "$JAVA_TIMEOUT_SEC" bash -c "cd '$CODE_DIR' && \
	INGESTION_INT_TEST_E2E=true INGESTION_INT_TEST_FLUSS=true \
	INGESTION_INT_TEST_MANIFEST=true INGESTION_INT_TEST_PERF=true \
	mvn -o test -pl 02_services/01_ingestion -am"; then
	echo "FAIL: Java suite failed or timed out — see $JAVA_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: Java suite" | tee -a "$SUMMARY"

echo "=== ALL GATES PASSED ===" | tee -a "$SUMMARY"
echo "GATE RESULT: PASS" | tee -a "$SUMMARY"
echo "Evidence:" | tee -a "$SUMMARY"
echo "  Go:   $GO_LOG" | tee -a "$SUMMARY"
echo "  Java: $JAVA_LOG" | tee -a "$SUMMARY"
