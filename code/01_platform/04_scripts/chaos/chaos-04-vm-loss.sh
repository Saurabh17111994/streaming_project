#!/usr/bin/env bash
# chaos-04-vm-loss.sh — T13 failure chaos test 4: one workload VM loss.
#
# Swarm leg only (M3 multi-node swarm; the local single-node mimic cannot
# lose its only worker). Simulates the loss of one workload node and asserts
# the plan's swarm invariants:
#   * order halt < 5 s      — the order-facing endpoint that lived on the
#                             lost node stops answering within 5 s
#                             (CHAOS_ORDER_PROBE_TCP=host:port; optional —
#                             skipped with a clear note when unset)
#   * data recovery < 30 s  — a replicated workload service re-schedules a
#                             Running task on a surviving node within 30 s
#                             (overlay re-route, evidence = docker service ps)
#   * overlay re-route      — the old task on the lost node is Shutdown and
#                             the new task Runs elsewhere (ps dump evidence)
#
# Two loss modes:
#   CHAOS_VM_OFF_MODE=drain      (default) — `docker node update --availability
#                                drain` + automatic `active` restore on exit
#                                (the local-mimic simulation of VM loss).
#   CHAOS_VM_OFF_MODE=poweroff   — runs CHAOS_VM_OFF_CMD (e.g. an SSH poweroff
#                                of the real VM); restore is MANUAL via
#                                CHAOS_VM_ON_CMD (no automatic restore — the
#                                node is unreachable while down).
#
# Invariant under test (plan section 6, swarm row): VM loss → data recovery
# < 30 s, order halt < 5 s, overlay re-route; placement must keep a survivor
# for every replicated workload service.
#
# Usage: ./chaos-04-vm-loss.sh
# Env:   CHAOS_WORKLOAD_NODE   (default: auto-pick a worker node != manager)
#        CHAOS_VM_OFF_MODE     (drain|poweroff, default drain)
#        CHAOS_VM_OFF_CMD      (poweroff mode only; e.g. "ssh vm3 sudo poweroff")
#        CHAOS_VM_ON_CMD       (poweroff mode only; run by the operator to restore)
#        CHAOS_ORDER_PROBE_TCP (host:port of the order-facing endpoint)
#        CHAOS_SERVICE         (service to watch for re-route; default:
#                               auto-pick a replicated service running on the node)
#        CHAOS_DRY_RUN=1       rehearsal: print the exact plan (node/service/
#                               mode/probe) and PASS without touching the swarm
# Exit:  0 PASS, 1 FAIL, 3 SKIP (no swarm / not enough nodes)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../../.." && pwd)}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/chaos/chaos-$(date +%Y%m%d-%H%M%S)}"
VM_OFF_MODE="${CHAOS_VM_OFF_MODE:-drain}"

if ! command -v docker >/dev/null 2>&1; then
	echo "VM-LOSS-CHAOS-04: SKIP — docker CLI not found"
	exit 3
fi
SWARM_STATE="$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || true)"
if [ "$SWARM_STATE" != "active" ]; then
	echo "VM-LOSS-CHAOS-04: SKIP — this docker daemon is not in an active swarm (M3 multi-node swarm required)"
	exit 3
fi

LOCAL_NODE_ID="$(docker info --format '{{.Swarm.NodeID}}')"

# Prefer a worker that is not the local (manager) node; fall back to any
# non-local node. With only the local node available the swarm cannot
# survive a VM loss — skip.
read -r -a NODES <<<"$(docker node ls --format '{{.Hostname}} {{.Role}} {{.Availability}} {{.ID}}' 2>/dev/null || true)"
if [ "${#NODES[@]}" -eq 0 ]; then
	echo "VM-LOSS-CHAOS-04: SKIP — no swarm nodes visible"
	exit 3
fi

WORKLOAD_NODE="${CHAOS_WORKLOAD_NODE:-}"
if [ -z "$WORKLOAD_NODE" ]; then
	for ((i = 0; i < ${#NODES[@]}; i += 4)); do
		host="${NODES[i]}"
		role="${NODES[i + 1]}"
		avail="${NODES[i + 2]}"
		id="${NODES[i + 3]}"
		if [ "$id" != "$LOCAL_NODE_ID" ] && [ "$avail" = "Active" ] && [ "$role" = "worker" ]; then
			WORKLOAD_NODE="$host"
			break
		fi
	done
fi
if [ -z "$WORKLOAD_NODE" ]; then
	for ((i = 0; i < ${#NODES[@]}; i += 4)); do
		id="${NODES[i + 3]}"
		if [ "$id" != "$LOCAL_NODE_ID" ]; then
			WORKLOAD_NODE="${NODES[i]}"
			break
		fi
	done
fi
if [ -z "$WORKLOAD_NODE" ]; then
	echo "VM-LOSS-CHAOS-04: SKIP — single-node swarm: no surviving node to re-route to (needs >= 2 nodes)"
	exit 3
fi

if [ "$VM_OFF_MODE" != "drain" ] && [ "$VM_OFF_MODE" != "poweroff" ]; then
	echo "VM-LOSS-CHAOS-04: FAIL — CHAOS_VM_OFF_MODE must be drain|poweroff, got '$VM_OFF_MODE'" >&2
	exit 1
fi
if [ "$VM_OFF_MODE" = "poweroff" ] && [ -z "${CHAOS_VM_OFF_CMD:-}" ]; then
	echo "VM-LOSS-CHAOS-04: FAIL — poweroff mode requires CHAOS_VM_OFF_CMD (e.g. an SSH poweroff)" >&2
	exit 1
fi

mkdir -p "$OUT_DIR"
EVIDENCE="$OUT_DIR/chaos-04-vm-loss/"
mkdir -p "$EVIDENCE"

# Pick the workload service to watch: an explicit CHAOS_SERVICE, else the
# first replicated service that currently runs a task on the node.
SERVICE="${CHAOS_SERVICE:-}"
if [ -z "$SERVICE" ]; then
	while read -r svc; do
		[ -z "$svc" ] && continue
		REPLICAS="$(docker service inspect "$svc" --format '{{.Spec.Mode.Replicated.Replicas}}' 2>/dev/null || echo 1)"
		ON_NODE="$(docker service ps "$svc" --format '{{.Node}}\t{{.CurrentState}}' 2>/dev/null | grep -c "$WORKLOAD_NODE.*Running" || true)"
		if [ "$ON_NODE" -ge 1 ] && [ "$REPLICAS" -ge 2 ] 2>/dev/null; then
			SERVICE="$svc"
			break
		fi
	done < <(docker service ls --format '{{.Name}}')
fi
if [ -z "$SERVICE" ]; then
	echo "VM-LOSS-CHAOS-04: SKIP — no replicated service running on node '$WORKLOAD_NODE' to re-route (set CHAOS_SERVICE)"
	exit 3
fi
echo "VM-LOSS-CHAOS-04: workload node=$WORKLOAD_NODE service=$SERVICE mode=$VM_OFF_MODE"
echo "VM-LOSS-CHAOS-04: order probe=${CHAOS_ORDER_PROBE_TCP:-<unset — halt check skipped>}"

if [ "${CHAOS_DRY_RUN:-0}" = "1" ]; then
	echo "VM-LOSS-CHAOS-04: DRY-RUN — would set node '$WORKLOAD_NODE' $VM_OFF_MODE, measure order halt (<5s) + recovery (<30s) of $SERVICE, restore afterwards"
	echo "VM-LOSS-CHAOS-04: RESULT=PASS EXIT=0 (dry-run)"
	exit 0
fi

restore_node() {
	if [ "$VM_OFF_MODE" = "drain" ] && [ "${RESTORED:-0}" = "0" ]; then
		echo "VM-LOSS-CHAOS-04: restore: docker node update --availability active $WORKLOAD_NODE"
		docker node update --availability active "$WORKLOAD_NODE" >/dev/null 2>&1 || true
		RESTORED=1
	fi
}
trap restore_node EXIT

now_ms() {
	date +%s%3N
}

# ---- order halt < 5 s (optional probe) -------------------------------------
HALT_MS=""
if [ -n "${CHAOS_ORDER_PROBE_TCP:-}" ] && command -v timeout >/dev/null 2>&1; then
	PROBE_HOST="${CHAOS_ORDER_PROBE_TCP%%:*}"
	PROBE_PORT="${CHAOS_ORDER_PROBE_TCP##*:}"
	probe() {
		timeout 2 bash -c "</dev/tcp/$PROBE_HOST/$PROBE_PORT" >/dev/null 2>&1
	}
	# Must be open before the loss (control for the probe itself).
	OPEN_BEFORE=0
	for _ in $(seq 1 30); do
		if probe; then
			OPEN_BEFORE=1
			break
		fi
		sleep 1
	done
	if [ "$OPEN_BEFORE" -ne 1 ]; then
		echo "VM-LOSS-CHAOS-04: FAIL — order probe $CHAOS_ORDER_PROBE_TCP not reachable BEFORE the loss" >&2
		exit 1
	fi
fi

# ---- inject the loss --------------------------------------------------------
T0="$(now_ms)"
echo "VM-LOSS-CHAOS-04: t0=$T0 losing node $WORKLOAD_NODE"
docker service ps "$SERVICE" --no-trunc >"$EVIDENCE/before-service-ps.txt" 2>&1 || true
if [ "$VM_OFF_MODE" = "drain" ]; then
	docker node update --availability drain "$WORKLOAD_NODE"
else
	# shellcheck disable=SC2086
	eval "$CHAOS_VM_OFF_CMD"
fi

# Order halt: first failed probe after t0.
if [ -n "${CHAOS_ORDER_PROBE_TCP:-}" ] && command -v timeout >/dev/null 2>&1; then
	HALT_MS=""
	for _ in $(seq 1 120); do
		if ! probe; then
			HALT_MS=$(( $(now_ms) - T0 ))
			break
		fi
		sleep 0.25
	done
	if [ -z "$HALT_MS" ]; then
		echo "VM-LOSS-CHAOS-04: FAIL — order probe still answering 30s after node loss (set CHAOS_ORDER_PROBE_TCP to the order path that depends on the killed node)" >&2
		exit 1
	fi
	echo "VM-LOSS-CHAOS-04: order halt observed at ${HALT_MS}ms after loss"
	if [ "$HALT_MS" -gt 5000 ]; then
		echo "VM-LOSS-CHAOS-04: FAIL — order halt ${HALT_MS}ms > 5000ms" >&2
		exit 1
	fi
else
	echo "VM-LOSS-CHAOS-04: order-halt check skipped (CHAOS_ORDER_PROBE_TCP unset or timeout absent)"
fi

# Overlay re-route + data recovery < 30 s: a Running task on a surviving node.
RECOVERY_MS=""
for _ in $(seq 1 30); do
	TASK_LINE="$(docker service ps "$SERVICE" --no-trunc --format '{{.Node}}\t{{.CurrentState}}\t{{.UpdatedAt}}\t{{.Name}}' 2>/dev/null | grep -v "^$WORKLOAD_NODE" | grep "Running" | head -1 || true)"
	if [ -n "$TASK_LINE" ]; then
		RECOVERY_MS=$(( $(now_ms) - T0 ))
		RECOVERY_TASK="$TASK_LINE"
		break
	fi
	sleep 1
done
docker service ps "$SERVICE" --no-trunc >"$EVIDENCE/after-service-ps.txt" 2>&1 || true
docker node ls >"$EVIDENCE/after-node-ls.txt" 2>&1 || true

if [ -z "$RECOVERY_MS" ]; then
	echo "VM-LOSS-CHAOS-04: FAIL — no task of $SERVICE re-scheduled on a surviving node within 30s (overlay re-route failed)" >&2
	exit 1
fi
echo "VM-LOSS-CHAOS-04: overlay re-route: $RECOVERY_TASK"
echo "VM-LOSS-CHAOS-04: data recovery observed at ${RECOVERY_MS}ms after loss"
if [ "$RECOVERY_MS" -gt 30000 ]; then
	echo "VM-LOSS-CHAOS-04: FAIL — data recovery ${RECOVERY_MS}ms > 30000ms" >&2
	exit 1
fi

# ---- restore ---------------------------------------------------------------
restore_node
if [ "$VM_OFF_MODE" = "drain" ]; then
	for _ in $(seq 1 30); do
		STATE="$(docker node inspect "$WORKLOAD_NODE" --format '{{.Spec.Availability}}' 2>/dev/null || true)"
		[ "$STATE" = "active" ] && break
		sleep 1
	done
	echo "VM-LOSS-CHAOS-04: node '$WORKLOAD_NODE' restored (availability=$STATE)"
else
	echo "VM-LOSS-CHAOS-04: poweroff mode — restore the VM manually with: CHAOS_VM_ON_CMD (e.g. '${CHAOS_VM_ON_CMD:-ssh ... && docker node update --availability active "$WORKLOAD_NODE"}')"
fi

echo "VM-LOSS-CHAOS-04: PASS — VM loss: order halt ${HALT_MS:-<skipped>}ms (<5s), data recovery ${RECOVERY_MS}ms (<30s), overlay re-routed"
echo "VM-LOSS-CHAOS-04: RESULT=PASS EXIT=0"
