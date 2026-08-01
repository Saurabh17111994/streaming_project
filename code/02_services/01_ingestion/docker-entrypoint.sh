#!/usr/bin/env bash
# Ingestion entrypoint — Java IngestionService manages Go arrow-bridge lifecycle.
# Java launches bridge as subprocess (ProcessBuilder), reads NDJSON from stdout,
# pipes bridge stderr into SLF4J logs. Bridge crash → discontinuity → shutdown.
set -euo pipefail

echo "ingestion: starting (FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP:-fluss-coordinator:9123})"

if [[ -z "${FLUSS_BOOTSTRAP:-}" ]]; then
	echo "ingestion: FATAL — FLUSS_BOOTSTRAP is required" >&2
	exit 2
fi

MANIFEST_PATH="${ARROW_INSTRUMENT_MANIFEST:-${INSTRUMENT_MANIFEST_PATH:-}}"
if [[ -z "$MANIFEST_PATH" || ! -r "$MANIFEST_PATH" ]]; then
	echo "ingestion: FATAL — readable instrument manifest is required: ${MANIFEST_PATH:-<unset>}" >&2
	exit 2
fi
export ARROW_INSTRUMENT_MANIFEST="$MANIFEST_PATH"

# Validate Go bridge binary (D6) — still checked for early failure
BRIDGE_BIN="${ARROW_BRIDGE_BIN:-/app/arrow-bridge}"
if [[ ! -x "$BRIDGE_BIN" ]]; then
	echo "ingestion: FATAL — arrow-bridge binary not found or not executable: $BRIDGE_BIN" >&2
	exit 1
fi
echo "ingestion: arrow-bridge binary OK ($BRIDGE_BIN)"

if [[ ! -r /app/ingestion.jar ]]; then
	echo "ingestion: FATAL — Java artifact not found: /app/ingestion.jar" >&2
	exit 2
fi

# Export bridge path so Java ProcessBuilder can find it
export ARROW_BRIDGE_BIN="$BRIDGE_BIN"

MAIN_CLASS="${INGESTION_MAIN_CLASS:-com.trading.ingestion.IngestionService}"
exec java -cp /app/ingestion.jar "${MAIN_CLASS}"
