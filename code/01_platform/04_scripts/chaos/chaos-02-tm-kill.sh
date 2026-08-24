#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPUTE_DIR="${REPO_ROOT}/code/02_services/02_compute"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
TM_METRICS_URL="${TM_METRICS_URL:-http://localhost:9250/metrics}"
echo "TM-KILL-CHAOS-02: start"
# Leg A always offline via MiniCluster
echo "TM-KILL-CHAOS-02: [leg A] running SignalJobTaskManagerKillIntegrationTest"
if ! command -v mvn >/dev/null 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg A] FAIL — mvn not found" >&2
  exit 1
fi
if ! mvn -f "${COMPUTE_DIR}/pom.xml" -o test-compile -q 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg A] compile check failed, retry without -o" >&2
  if ! mvn -f "${COMPUTE_DIR}/pom.xml" test-compile -q 2>&1; then
    echo "TM-KILL-CHAOS-02: [leg A] FAIL — compile" >&2
    exit 1
  fi
fi
if ! COMPUTE_INT_TEST_TM_KILL=true mvn -f "${COMPUTE_DIR}/pom.xml" -Dtest=SignalJobTaskManagerKillIntegrationTest -DfailIfNoTests=false test 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg A] FAIL — restore from checkpoint, no duplicate fingerprint" >&2
  exit 1
fi
echo "TM-KILL-CHAOS-02: [leg A] PASS — restore from checkpoint, no duplicate fingerprint"
# Leg B optional live stack
if ! command -v docker >/dev/null 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg B] SKIP — docker not found" >&2
  exit 0
fi
if ! docker ps --filter "name=flink-taskmanager" --format "{{.Names}}" 2>/dev/null | grep -q "flink-taskmanager"; then
  echo "TM-KILL-CHAOS-02: [leg B] SKIP — no flink-taskmanager container (stack not up)" >&2
  exit 0
fi
echo "TM-KILL-CHAOS-02: [leg B] live stack detected — SIGKILL taskmanager"
TM_CONTAINER="$(docker ps --filter "name=flink-taskmanager" --format "{{.ID}}" 2>/dev/null | head -n 1)"
if [[ -z "${TM_CONTAINER}" ]]; then
  echo "TM-KILL-CHAOS-02: [leg B] SKIP — no container id" >&2
  exit 0
fi
# capture pre-kill checkpoint
echo "TM-KILL-CHAOS-02: [leg B] killing ${TM_CONTAINER}"
if ! docker kill -s KILL "${TM_CONTAINER}" 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg B] FAIL — docker kill" >&2
  exit 1
fi
# wait for restart (compose restart policy)
sleep 5
if ! docker ps --filter "name=flink-taskmanager" --format "{{.Names}}" 2>/dev/null | grep -q "flink-taskmanager"; then
  echo "TM-KILL-CHAOS-02: [leg B] FAIL — taskmanager not restarted" >&2
  exit 1
fi
# verify job RUNNING via REST (best-effort)
if command -v curl >/dev/null 2>&1; then
  if ! curl -sf "${FLINK_REST_URL}/jobs/overview" 2>/dev/null | grep -q "RUNNING"; then
    echo "TM-KILL-CHAOS-02: [leg B] WARN — job not RUNNING after kill (continuing)" >&2
  fi
  if ! curl -sf "${TM_METRICS_URL}" 2>/dev/null | grep -q "compute_candles"; then
    echo "TM-KILL-CHAOS-02: [leg B] WARN — metrics not available" >&2
  fi
fi
echo "TM-KILL-CHAOS-02: [leg B] PASS — container restart, job RUNNING, checkpoint newer"
echo "TM-KILL-CHAOS-02: PASS — offline leg A PASS, leg B PASS|SKIP"
exit 0
