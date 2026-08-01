#!/usr/bin/env bash
# run-monday-gates.sh — ONE command to run the full Monday verification gate.
#
# Orchestrates Workstream A from the Monday soak plan:
#   1. Go bridge suite (no deps)
#   2. Full Java suite with ALL integration flags (needs local Fluss up)
#   3. Report PASS/FAIL with evidence file paths
#
# Usage:  ./run-monday-gates.sh
#   - The script FAILS (exit != 0) if any suite fails, so you can wire it
#     into CI or a cron.
#
# Prereqs: Fluss up (docker compose up -d), local ~/.m2 warm, Go 1.24+, JDK 17.

set -euo pipefail

# ── Config (override via env) ─────────────────────────────────────────────────
PROJECT_ROOT="${PROJECT_ROOT:-/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project}"
CODE_DIR="${CODE_DIR:-$PROJECT_ROOT/code}"
BRIDGE_DIR="${BRIDGE_DIR:-$CODE_DIR/02_services/01_ingestion/go-bridge}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak/monday-gates-$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$OUT_DIR"
GO_LOG="$OUT_DIR/go-suite.log"
JAVA_LOG="$OUT_DIR/java-suite.log"
SUMMARY="$OUT_DIR/SUMMARY.txt"

echo "run-monday-gates: output → $OUT_DIR"

# ── 1. Go suite ───────────────────────────────────────────────────────────────
echo "=== [1/2] Go bridge suite ===" | tee -a "$SUMMARY"
if ! ( cd "$BRIDGE_DIR" && go test -count=1 ./... ) >"$GO_LOG" 2>&1; then
	echo "FAIL: Go suite failed — see $GO_LOG" | tee -a "$SUMMARY"
	exit 1
fi
echo "PASS: Go suite" | tee -a "$SUMMARY"

# ── 2. Full Java gate with ALL integration flags ──────────────────────────────
echo "=== [2/2] Java full gate (FLUSS+MANIFEST+PERF+E2E) ===" | tee -a "$SUMMARY"
if ! ( cd "$CODE_DIR" && \
	INGESTION_INT_TEST_E2E=true INGESTION_INT_TEST_FLUSS=true \
	INGESTION_INT_TEST_MANIFEST=true INGESTION_INT_TEST_PERF=true \
	mvn -o test -pl 02_services/01_ingestion -am ) >"$JAVA_LOG" 2>&1; then
	echo "FAIL: Java suite failed — see $JAVA_LOG" | tee -a "$SUMMARY"
	exit 1
fi
echo "PASS: Java suite" | tee -a "$SUMMARY"

echo "=== ALL GATES PASSED ===" | tee -a "$SUMMARY"
echo "Evidence:" | tee -a "$SUMMARY"
echo "  Go:   $GO_LOG" | tee -a "$SUMMARY"
echo "  Java: $JAVA_LOG" | tee -a "$SUMMARY"
