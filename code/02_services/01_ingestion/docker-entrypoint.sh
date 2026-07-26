#!/usr/bin/env bash
# Ingestion entrypoint: connect to Kite WS and stream into raw_table_1.
set -euo pipefail
echo "ingestion: starting (FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP:-fluss-coordinator:9123})"
exec java -jar /app/ingestion.jar
