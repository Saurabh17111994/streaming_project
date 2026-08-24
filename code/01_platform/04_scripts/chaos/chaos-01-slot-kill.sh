#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
BRIDGE_DIR="${REPO_ROOT}/code/02_services/01_ingestion/go-bridge"
echo "SLOT-KILL-CHAOS-01: start offline deterministic"
if ! command -v go >/dev/null 2>&1; then
  echo "SLOT-KILL-CHAOS-01: SKIP — go not found" >&2
  exit 3
fi
# T1 sharding 3000→1024+1024+952
echo "SLOT-KILL-CHAOS-01: running TestSubscriptionPlanShards3000Tokens"
if ! go -C "${BRIDGE_DIR}" test -run TestSubscriptionPlanShards3000Tokens -count 1 -v 2>&1; then
  echo "SLOT-KILL-CHAOS-01: FAIL — sharding" >&2
  exit 1
fi
# terminal isolated per slot
echo "SLOT-KILL-CHAOS-01: running TestSupervisorAuthTerminalIsolatedPerSlot"
if ! go -C "${BRIDGE_DIR}" test -run TestSupervisorAuthTerminalIsolatedPerSlot -count 1 -v 2>&1; then
  echo "SLOT-KILL-CHAOS-01: FAIL — terminal isolated" >&2
  exit 1
fi
echo "SLOT-KILL-CHAOS-01: running TestINGRES001HealthySlotNotInterruptedByPeerReconnect"
if ! go -C "${BRIDGE_DIR}" test -run TestINGRES001HealthySlotNotInterruptedByPeerReconnect -count 1 -v 2>&1; then
  echo "SLOT-KILL-CHAOS-01: FAIL — healthy slot not interrupted" >&2
  exit 1
fi
echo "SLOT-KILL-CHAOS-01: running TestReconnectLoopRecoversAfterFailures"
if ! go -C "${BRIDGE_DIR}" test -run TestReconnectLoopRecoversAfterFailures -count 1 -v 2>&1; then
  echo "SLOT-KILL-CHAOS-01: FAIL — reconnect recovers" >&2
  exit 1
fi
echo "SLOT-KILL-CHAOS-01: running TestReconnectLoopEpochAndBackoffAfterForcedDisconnect"
if ! go -C "${BRIDGE_DIR}" test -run TestReconnectLoopEpochAndBackoffAfterForcedDisconnect -count 1 -v 2>&1; then
  echo "SLOT-KILL-CHAOS-01: FAIL — epoch and backoff" >&2
  exit 1
fi
echo "SLOT-KILL-CHAOS-01: running TestINGRES001OneHundredForcedDisconnectReconnectCycles"
if ! go -C "${BRIDGE_DIR}" test -run TestINGRES001OneHundredForcedDisconnectReconnectCycles -count 1 -v 2>&1; then
  echo "SLOT-KILL-CHAOS-01: FAIL — 100 cycles" >&2
  exit 1
fi
# go vet as gate the doc relies on
if ! go -C "${BRIDGE_DIR}" vet ./... 2>&1; then
  echo "SLOT-KILL-CHAOS-01: FAIL — go vet" >&2
  exit 1
fi
echo "SLOT-KILL-CHAOS-01: PASS — 1-of-3 slot kill: terminal isolated, peers healthy, reconnect clean"
exit 0
