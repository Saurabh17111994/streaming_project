#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPUTE_DIR="${REPO_ROOT}/code/02_services/02_compute"
FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-fluss-coordinator:9123}"
TABLET_CONTAINER="${TABLET_CONTAINER:-}"
TABLET_KILL_ROWS="${TABLET_KILL_ROWS:-25}"
CHAOS_REPLICATION_REQUIRED="${CHAOS_REPLICATION_REQUIRED:-false}"
CHAOS_REPLICATION_MIN="${CHAOS_REPLICATION_MIN:-3}"
echo "TABLET-KILL-CHAOS-03: start TABLET_KILL_ROWS=${TABLET_KILL_ROWS} replication_required=${CHAOS_REPLICATION_REQUIRED}"
if ! command -v docker >/dev/null 2>&1; then
  echo "TABLET-KILL-CHAOS-03: SKIP — docker not found" >&2
  exit 3
fi
if [[ -z "${TABLET_CONTAINER}" ]]; then
  # auto-discover like repair-tablet.sh
  TABLET_CONTAINER="$(docker ps --filter "name=fluss-tablet" --format "{{.ID}}" 2>/dev/null | head -n 1 || true)"
fi
if [[ -z "${TABLET_CONTAINER}" ]]; then
  echo "TABLET-KILL-CHAOS-03: SKIP — no fluss-tablet container (start the stack; TABLET_CONTAINER overrides)" >&2
  exit 3
fi
if ! docker ps --format "{{.ID}} {{.Names}}" 2>/dev/null | grep -q "${TABLET_CONTAINER}"; then
  echo "TABLET-KILL-CHAOS-03: SKIP — tablet container ${TABLET_CONTAINER} not running" >&2
  exit 3
fi
# compile check (offline leg of 03 is compile-only; live needs stack)
if ! mvn -f "${COMPUTE_DIR}/pom.xml" -o test-compile -q 2>&1; then
  if ! mvn -f "${COMPUTE_DIR}/pom.xml" test-compile -q 2>&1; then
    echo "TABLET-KILL-CHAOS-03: FAIL — compile TabletKillChaosIntegrationTest" >&2
    exit 1
  fi
fi
# live execution env-gated
echo "TABLET-KILL-CHAOS-03: running TabletKillChaosIntegrationTest FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP} TABLET_CONTAINER=${TABLET_CONTAINER}"
if ! COMPUTE_INT_TEST_TABLET_KILL=true FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP}" TABLET_CONTAINER="${TABLET_CONTAINER}" TABLET_KILL_ROWS="${TABLET_KILL_ROWS}" CHAOS_REPLICATION_REQUIRED="${CHAOS_REPLICATION_REQUIRED}" CHAOS_REPLICATION_MIN="${CHAOS_REPLICATION_MIN}" mvn -f "${COMPUTE_DIR}/pom.xml" -Dtest=TabletKillChaosIntegrationTest -DfailIfNoTests=false test 2>&1; then
  echo "TABLET-KILL-CHAOS-03: FAIL — TabletKillChaosIntegrationTest" >&2
  exit 1
fi
echo "TABLET-KILL-CHAOS-03: PASS — acked rows still readable, LOG count never shrank"
exit 0
