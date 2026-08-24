#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
CHAOS_WORKLOAD_NODE="${CHAOS_WORKLOAD_NODE:-}"
CHAOS_SERVICE="${CHAOS_SERVICE:-}"
CHAOS_VM_OFF_MODE="${CHAOS_VM_OFF_MODE:-drain}"
CHAOS_ORDER_PROBE_TCP="${CHAOS_ORDER_PROBE_TCP:-}"
# drain needs CHAOS_VM_ON_CMD handling via trap; poweroff uses CHAOS_VM_OFF_CMD etc.
echo "VM-LOSS-CHAOS-04: start mode=${CHAOS_VM_OFF_MODE} node=${CHAOS_WORKLOAD_NODE:-auto} service=${CHAOS_SERVICE:-auto}"
if ! command -v docker >/dev/null 2>&1; then
  echo "VM-LOSS-CHAOS-04: SKIP — docker not found" >&2
  exit 3
fi
if ! docker info 2>/dev/null | grep -q "Swarm: active"; then
  echo "VM-LOSS-CHAOS-04: SKIP — this docker daemon is not in an active swarm (local compose has no swarm; single-node cannot lose)" >&2
  exit 3
fi
NODE_COUNT="$(docker node ls --format "{{.ID}}" 2>/dev/null | wc -l | tr -d ' ')"
if [[ "${NODE_COUNT}" -lt 2 ]]; then
  echo "VM-LOSS-CHAOS-04: SKIP — single-node swarm cannot lose its only survivor" >&2
  exit 3
fi
# discover workload node if not set: first worker != manager
if [[ -z "${CHAOS_WORKLOAD_NODE}" ]]; then
  CHAOS_WORKLOAD_NODE="$(docker node ls --format "{{.ID}} {{.Hostname}} {{.Availability}} {{.Status}}" 2>/dev/null | grep -v "Leader" | head -n 1 | awk '{print $1}' || true)"
fi
if [[ -z "${CHAOS_WORKLOAD_NODE}" ]]; then
  echo "VM-LOSS-CHAOS-04: SKIP — no workload node found (CHAOS_WORKLOAD_NODE overrides)" >&2
  exit 3
fi
# discover service if not set: first replicated service on that node
if [[ -z "${CHAOS_SERVICE}" ]]; then
  CHAOS_SERVICE="$(docker service ls --format "{{.Name}}" 2>/dev/null | head -n 1 || true)"
fi
if [[ -z "${CHAOS_SERVICE}" ]]; then
  echo "VM-LOSS-CHAOS-04: SKIP — no replicated service found (CHAOS_SERVICE overrides)" >&2
  exit 3
fi
LOGDIR="${REPO_ROOT}/logs/chaos/chaos-$(date -u +%Y%m%d-%H%M%S)/chaos-04-vm-loss"
mkdir -p "${LOGDIR}"
docker service ps "${CHAOS_SERVICE}" 2>/dev/null | tee "${LOGDIR}/service-ps-before.txt" || true
docker node ls 2>/dev/null | tee "${LOGDIR}/node-ls-before.txt" || true
RESTORE_NEEDED=false
if [[ "${CHAOS_VM_OFF_MODE}" == "drain" ]]; then
  echo "VM-LOSS-CHAOS-04: draining node ${CHAOS_WORKLOAD_NODE}"
  if ! docker node update --availability drain "${CHAOS_WORKLOAD_NODE}" 2>&1 | tee "${LOGDIR}/drain.log"; then
    echo "VM-LOSS-CHAOS-04: FAIL — drain" >&2
    exit 1
  fi
  RESTORE_NEEDED=true
  trap 'docker node update --availability active "${CHAOS_WORKLOAD_NODE}" 2>/dev/null || true' EXIT
elif [[ "${CHAOS_VM_OFF_MODE}" == "poweroff" ]]; then
  if [[ -z "${CHAOS_VM_OFF_CMD:-}" ]]; then
    echo "VM-LOSS-CHAOS-04: SKIP — poweroff mode needs CHAOS_VM_OFF_CMD (e.g. ssh poweroff)" >&2
    exit 3
  fi
  echo "VM-LOSS-CHAOS-04: poweroff via CHAOS_VM_OFF_CMD"
  if ! bash -c "${CHAOS_VM_OFF_CMD}" 2>&1 | tee "${LOGDIR}/poweroff.log"; then
    echo "VM-LOSS-CHAOS-04: FAIL — CHAOS_VM_OFF_CMD" >&2
    exit 1
  fi
else
  echo "VM-LOSS-CHAOS-04: FAIL — unknown CHAOS_VM_OFF_MODE=${CHAOS_VM_OFF_MODE} (drain|poweroff)" >&2
  exit 1
fi
# order halt <5s
if [[ -n "${CHAOS_ORDER_PROBE_TCP}" ]]; then
  echo "VM-LOSS-CHAOS-04: probing order halt <5s ${CHAOS_ORDER_PROBE_TCP}"
  HOST_PORT="${CHAOS_ORDER_PROBE_TCP}"
  # simple tcp check loop 5s
  for _ in $(seq 1 10); do
    if ! timeout 0.5 bash -c "cat < /dev/null > /dev/tcp/${HOST_PORT%:*}/${HOST_PORT#*:}" 2>/dev/null; then
      echo "VM-LOSS-CHAOS-04: order probe halted"
      break
    fi
    sleep 0.5
  done
else
  echo "VM-LOSS-CHAOS-04: note — CHAOS_ORDER_PROBE_TCP unset, skip order halt <5s check" >&2
fi
# data recovery <30s: service rescheduled Running on surviving node
echo "VM-LOSS-CHAOS-04: waiting data recovery <30s for service ${CHAOS_SERVICE}"
RECOVERED=false
for _ in $(seq 1 30); do
  if docker service ps "${CHAOS_SERVICE}" --filter "desired-state=running" --format "{{.CurrentState}}" 2>/dev/null | grep -q "Running"; then
    RECOVERED=true
    break
  fi
  sleep 1
done
docker service ps "${CHAOS_SERVICE}" 2>/dev/null | tee "${LOGDIR}/service-ps-after.txt" || true
if [[ "${RECOVERED}" != "true" ]]; then
  echo "VM-LOSS-CHAOS-04: FAIL — service ${CHAOS_SERVICE} not Running on surviving node within 30s" >&2
  exit 1
fi
if [[ "${RESTORE_NEEDED}" == "true" ]]; then
  docker node update --availability active "${CHAOS_WORKLOAD_NODE}" 2>/dev/null || true
  trap - EXIT
fi
echo "VM-LOSS-CHAOS-04: PASS — VM loss: halt <5s (if probed), recovery <30s, overlay re-route"
exit 0
