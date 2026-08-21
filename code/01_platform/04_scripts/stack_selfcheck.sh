#!/usr/bin/env bash
# stack_selfcheck.sh — one-host Swarm self-check for docker-stack.yml (M2 mimic).
#
# Purpose: exercise the PRODUCTION stack deploy path on a SINGLE host (no 4/7
# VMs). This is the offline-prep bridge that proves `docker stack deploy`
# succeeds against a real (local) Swarm, without needing the real rig. It is
# NOT multi-VM HA evidence (SWARM-MGR-001..006 need a 3-manager quorum) — that
# is M3.
#
# What it does:
#   1. Requires the docker CLI + a running daemon (SKIP otherwise).
#   2. If not already a manager, `docker swarm init` on this single node.
#   3. Labels this node `role=worker` AND `role=observability` so the same
#      constraint set the real cluster uses actually schedules here.
#   4. `docker stack config -c <stack>` — compile the manifest (no services
#      started). Exit non-zero on any stack error.
#   5. Optional real deploy: DEPLOY=1 -> docker stack deploy -c <stack> prod.
#   6. Teardown: DOWN=1 -> docker stack rm prod (single-node swarm left active).
#
# Usage:
#   ./stack_selfcheck.sh                 # validate (swarm init + stack config)
#   ./stack_selfcheck.sh DEPLOY=1        # also docker stack deploy prod
#   ./stack_selfcheck.sh DOWN=0          # leave the deployed stack up
set -euo pipefail

cd "$(dirname "$0")/../../.."            # repo root
STACK="${STACK:-code/01_platform/01_docker/docker-stack.yml}"
STACK_NAME="${STACK_NAME:-prod}"

echo "== stack_selfcheck: one-host Swarm deploy path =="

if ! command -v docker >/dev/null 2>&1; then
  echo "SKIP: docker CLI not installed — run offline static check via make stack-selfcheck."
  exit 0
fi
if ! docker info >/dev/null 2>&1; then
  echo "SKIP: docker daemon not running/usable."
  exit 0
fi

# 2. Ensure we are a swarm manager on this single node.
if ! docker node ls >/dev/null 2>&1; then
  echo ">> swarm not active — docker swarm init (single-node mimic)"
  docker swarm init --advertise-addr 127.0.0.1
else
  echo ">> swarm already active"
fi

node_id="$(docker node ls -q | head -1)"
echo ">> current node: $node_id"

# 3. Label the single node as both worker + observability, so every role-based
#    constraint in the stack schedules on this one host (mirror of v1 M1-3).
docker node update --label-add role=worker "$node_id" >/dev/null
docker node update --label-add observability=true "$node_id" >/dev/null
echo ">> labelled $node_id role=worker + observability=true"

# 4b. Export compile-time placeholders so `docker stack config` can interpolate
#     the required image/infra vars (real digests + endpoints come from
#     .env / runtime.lock in production). Credentials are external Swarm
#     secrets, so they deliberately have NO env fallback here (fail closed).
: "${FLUSS_IMAGE:=fluss:unset}"
: "${FLINK_IMAGE:=flink:unset}"
: "${INGESTION_IMAGE:=ingestion:unset}"
: "${EXECUTION_BRIDGE_IMAGE:=exec-bridge:unset}"
: "${EXECUTION_GATEWAY_IMAGE:=exec-gateway:unset}"
: "${NAUTILUS_IMAGE:=nautilus:unset}"
: "${OPENOBSERVE_IMAGE:=openobserve:unset}"
: "${ZOOKEEPER_IMAGE:=zookeeper:3.9.2}"
: "${S3_WAREHOUSE_PATH:=s3://placeholder/warehouse}"
: "${R2_ENDPOINT:=https://placeholder.example}"
: "${ARROW_APP_ID:=000000}"
: "${ARROW_USER_ID:=00000000}"
export FLUSS_IMAGE FLINK_IMAGE INGESTION_IMAGE EXECUTION_BRIDGE_IMAGE \
       EXECUTION_GATEWAY_IMAGE NAUTILUS_IMAGE OPENOBSERVE_IMAGE \
       ZOOKEEPER_IMAGE S3_WAREHOUSE_PATH R2_ENDPOINT ARROW_APP_ID ARROW_USER_ID

# 4. Compile the stack (catches YAML/deploy-schema errors without starting).
echo ">> docker stack config"
docker stack config -c "$STACK" >/dev/null
echo "   stack config: OK"

# 5. Optional real deploy.
if [[ "${DEPLOY:-0}" == "1" ]]; then
  echo ">> docker stack deploy -c $STACK $STACK_NAME"
  docker stack deploy -c "$STACK" "$STACK_NAME"
  echo "   deploy initiated — inspect with: docker stack services $STACK_NAME"
  echo "   node labels: $(docker node inspect "$node_id" --format '{{.Spec.Labels}}')"
fi

# 6. Optional teardown.
if [[ "${DOWN:-1}" == "1" ]]; then
  docker stack rm "$STACK_NAME" >/dev/null 2>&1 || true
  echo ">> removed stack $STACK_NAME (single-node swarm left active)"
fi

echo "== stack_selfcheck: DONE =="
