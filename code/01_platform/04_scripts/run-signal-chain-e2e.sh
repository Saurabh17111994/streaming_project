#!/usr/bin/env bash
#
# run-signal-chain-e2e.sh (DRAFT)
#
# Full-chain live E2E: broker -> raw_table_1 -> SignalJob -> feature_candles_15s
# for E2E_RUN_MINUTES (default 30). See the SignalChainLiveE2ETest javadoc for
# the full env contract. This script builds the bridge/faketool binaries and
# the ingestion classpath, then runs the env-gated test module-locally
# (R-272: compute is excluded from the root reactor).
#
# Modes:
#   E2E_BROKER=faketool    (default)  Go fake broker, runs any time
#   E2E_BROKER=arrow-hft   REAL wss://socket.arrow.trade — market hours only
#   (the arrow-std / Standard-feed mode was removed 2026-08-14)
#
# Arrow mode additionally requires (device-flow tokens, never committed):
#   ARROW_APP_ID ARROW_APP_SECRET ARROW_TOKEN
#
# Host mode expects Fluss published on localhost:9123 (dev compose). For the
# in-container variant (trading-net, fluss-coordinator:9123) run this script
# from a maven:3.9-eclipse-temurin-17 container with the .m2 rewrite — see
# logs/tracker-14 probes for the classpath recipe.
#
set -euo pipefail

CODE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJ_ROOT="$(cd "$CODE_ROOT/.." && pwd)"

: "${E2E_BROKER:=faketool}"
: "${E2E_RUN_MINUTES:=30}"
: "${FLUSS_BOOTSTRAP:=localhost:9123}"
: "${E2E_CHECKPOINT_DIR:=file:///tmp/signal-chain-e2e-checkpoints}"
# RocksDB managed-memory budget for the embedded job (E2E root cause
# 2026-08-17): local execution defaults taskmanager.memory.managed.size to
# 128 MB TOTAL, which starves the RocksDB block cache and throttles the job
# to ≈ the feed rate — the backlog never drains and the tail is never
# reached. 2048m gives RocksDB a real cache (probe-verified: 128m → ~30k/s,
# 2048m → ~49k/s throughput on the same topology).
: "${TASK_MANAGER_MEMORY_MANAGED_SIZE:=2048m}"
# The E2E must exercise the production state backend (RocksDB), not the dev
# hashmap default — the job's RocksDB state is what the managed-memory budget
# above feeds. STATE_BACKEND_LOCAL_DIRS stays caller-optional (default tmp).
: "${STATE_BACKEND:=rocksdb}"
: "${INGESTION_JAR_DIR:=$CODE_ROOT/02_services/01_ingestion/target}"
MANIFEST_DEFAULT="$PROJ_ROOT/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"
: "${INSTRUMENT_MANIFEST_PATH:=$MANIFEST_DEFAULT}"

case "$E2E_BROKER" in
faketool | arrow-hft) ;;
*)
	echo "E2E_BROKER must be faketool | arrow-hft (got '$E2E_BROKER')" >&2
	exit 2
	;;
esac

if [ "$E2E_BROKER" != "faketool" ]; then
	echo "==> arrow mode: REAL broker. Requires market hours (09:15-15:30 IST) — post-close"
	echo "    data is STALE and quarantined, so raw_table_1 never grows and the test SKIPS."
	: "${ARROW_APP_ID:?arrow mode needs ARROW_APP_ID (device-flow token)}"
	: "${ARROW_APP_SECRET:?arrow mode needs ARROW_APP_SECRET}"
	: "${ARROW_TOKEN:?arrow mode needs ARROW_TOKEN}"
fi

echo "=== chain-e2e: builds (bridge + faketool + ingestion jar + classpaths)"

BRIDGE_DIR="$CODE_ROOT/02_services/01_ingestion/go-bridge"
(cd "$BRIDGE_DIR" && go build -o arrow-bridge .)
ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge"
FAKETOOL_BIN="$BRIDGE_DIR/faketool/faketool"
if [ "$E2E_BROKER" = "faketool" ]; then
	(cd "$BRIDGE_DIR" && go build -tags faketool -o faketool ./faketool)
fi

(cd "$CODE_ROOT" && mvn -o -q -DskipTests package -pl 02_services/01_ingestion -am) ||
	{
		echo "ingestion package failed" >&2
		exit 1
	}

# The E2E spawns the REAL IngestionService as a subprocess with
# INGESTION_CLASSPATH; the compute surefire classpath does NOT contain the
# ingestion module, so the classpath must be computed here (previously the
# script exported INGESTION_CLASSPATH unset — an empty classpath silently
# killed the ingestion subprocess, failing the warmup await).
# mvn -q suppresses the classpath stdout (dependency:build-classpath prints it
# at INFO level), so the cp must be captured via -Dmdep.outputFile — a stdout
# capture yields an empty deps list and the ingestion subprocess dies instantly
# on a missing fluss-client class (observed 2026-08-17: warmup timeout).
INGESTION_CP_FILE="$CODE_ROOT/02_services/01_ingestion/target/e2e-ingestion-cp.txt"
(cd "$CODE_ROOT" && mvn -q -o dependency:build-classpath \
	-pl 02_services/01_ingestion -Dmdep.outputAbsoluteArtifactFilename=true \
	-Dmdep.outputFile="$INGESTION_CP_FILE") ||
	{
		echo "ingestion classpath failed" >&2
		exit 1
	}
INGESTION_CP="$CODE_ROOT/02_services/01_ingestion/target/classes:$(cat "$INGESTION_CP_FILE")"
rm -f "$INGESTION_CP_FILE"

export INGESTION_CLASSPATH="$INGESTION_CP"

echo "=== chain-e2e: run (E2E_BROKER=$E2E_BROKER, ${E2E_RUN_MINUTES} min, Fluss $FLUSS_BOOTSTRAP)"
export SIGNAL_CHAIN_E2E=true
export E2E_BROKER E2E_RUN_MINUTES FLUSS_BOOTSTRAP E2E_CHECKPOINT_DIR
export TASK_MANAGER_MEMORY_MANAGED_SIZE STATE_BACKEND
export INSTRUMENT_MANIFEST_PATH
export ARROW_BRIDGE_BIN FAKETOOL_BIN
# arrow modes: pass the credentials through untouched
[ "$E2E_BROKER" != "faketool" ] && export ARROW_APP_ID ARROW_APP_SECRET ARROW_TOKEN

cd "$CODE_ROOT/02_services/02_compute" &&
	mvn -o test -Dtest=SignalChainLiveE2ETest
