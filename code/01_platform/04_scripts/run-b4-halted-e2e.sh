#!/usr/bin/env bash
#
# run-b4-halted-e2e.sh — B4.2 HALTED-path signal→intent E2E (non-market half).
#
# Runs the two live Java E2E tests that prove the B4.2 chain up to the
# fail-closed wall, WITHOUT a broker or market hours:
#
#   1. compute   B4SignalIntentE2ETest        signal→candidates→intents
#        - EXECUTION_INTENT_ENABLED=true: real SignalJob topology turns a
#          crafted rising raw series into Signal_Candidates + immutable
#          Execution_Intent LOG rows (rule-v1 breakout, lookback=2).
#        - flag absent (default): the same signal flow writes ZERO intents.
#   2. gateway   B4HaltedIntentConsumeDeferE2ETest   intent→gateway→DEFERRED
#        - a canonical Execution_Intent row reaches the real IntentReader +
#          DurableIntentDispatcher + NautilusIntentClient; with no ENABLED
#          Execution_Gate row and no durable fence token the handoff is
#          DEFERRED: zero Execution_Attempts / Order_Lifecycle rows, intent
#          stays replayable, readiness is fail-closed.
#
# Prereqs (D-era, no market):
#   * docker compose Fluss cluster up with the platform DDL applied to the
#     `default` catalog (26 tables): make ddl validates; the sanctioned apply
#     runs in the ddl-apply container (see Dockerfile README), or the cluster
#     may already carry the tables (the apply then REFUSES on the
#     empty-catalog precondition — that refusal itself proves the tables are
#     present). The tests fail with explicit messages if tables are missing.
#   * Maven offline deps in ~/.m2 (the repo gates already build these modules).
#
# Usage:
#   ./run-b4-halted-e2e.sh                 # runs both E2E tests (default)
#   FLUSS_BOOTSTRAP=host:9123 ./run-b4-halted-e2e.sh
#   ./run-b4-halted-e2e.sh --gateway-only | --compute-only
#
set -euo pipefail

CODE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJ_ROOT="$(cd "$CODE_ROOT/.." && pwd)"
export CODE_ROOT PROJ_ROOT
: "${FLUSS_BOOTSTRAP:=localhost:9123}"
export FLUSS_BOOTSTRAP

mode="${1:-both}"
case "$mode" in
  --gateway-only) run_gw=1; run_cp=0 ;;
  --compute-only) run_gw=0; run_cp=1 ;;
  both)           run_gw=1; run_cp=1 ;;
  *) echo "usage: $0 [--gateway-only|--compute-only]" >&2; exit 2 ;;
esac

echo "== b4-halted-e2e: FLUSS_BOOTSTRAP=$FLUSS_BOOTSTRAP =="

if [ "$run_gw" = "1" ]; then
  echo "== [1/2] gateway: halted intent consume+defer =="
  (cd "$CODE_ROOT/02_services/06_execution_gateway" && \
   mvn -o test -Dtest=B4HaltedIntentConsumeDeferE2ETest -q)
  echo "   gateway HALTED E2E: PASS"
fi

if [ "$run_cp" = "1" ]; then
  echo "== [2/2] compute: signal->candidate->intent (enabled + disabled) =="
  (cd "$CODE_ROOT/02_services/02_compute" && \
   mvn -o test -Dtest=B4SignalIntentE2ETest -q)
  echo "   signal->intent E2E: PASS"
fi

echo "== b4-halted-e2e: ALL PASS (non-market half of B4.2; full fill leg "
echo "   remains gated on A2.3/A2.4 sandbox config + T4 bridge wiring) =="
