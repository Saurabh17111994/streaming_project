#!/usr/bin/env bash
#
# run-signal-chain-e2e.sh (DRAFT)
#
# Full-chain live E2E: broker -> raw_table_1 -> SignalJob -> feature_candles_15s
# for E2E_RUN_MINUTES (default 5). See the SignalChainLiveE2ETest javadoc for
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
: "${E2E_RUN_MINUTES:=5}"
: "${FLUSS_BOOTSTRAP:=localhost:9123}"
: "${E2E_CHECKPOINT_DIR:=file:///tmp/signal-chain-e2e-checkpoints}"
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

echo "=== chain-e2e: run (E2E_BROKER=$E2E_BROKER, ${E2E_RUN_MINUTES} min, Fluss $FLUSS_BOOTSTRAP)"
export SIGNAL_CHAIN_E2E=true
export E2E_BROKER E2E_RUN_MINUTES FLUSS_BOOTSTRAP E2E_CHECKPOINT_DIR
export INSTRUMENT_MANIFEST_PATH
export INGESTION_CLASSPATH ARROW_BRIDGE_BIN FAKETOOL_BIN
# arrow modes: pass the credentials through untouched
[ "$E2E_BROKER" != "faketool" ] && export ARROW_APP_ID ARROW_APP_SECRET ARROW_TOKEN

cd "$CODE_ROOT/02_services/02_compute" &&
	mvn -o test -Dtest=SignalChainLiveE2ETest
