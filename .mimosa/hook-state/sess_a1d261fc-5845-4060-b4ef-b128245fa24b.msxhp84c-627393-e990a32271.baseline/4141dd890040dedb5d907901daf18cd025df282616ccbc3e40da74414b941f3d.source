#!/bin/bash
# Ingestion smoke test — runs DDL bootstrap + 10 synthetic ticks against local Fluss.
set -euo pipefail

# R-166: derive the code dir from this script's location (portable).
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export FLUSS_BOOTSTRAP=localhost:9123
export ARROW_APP_ID=smoke-test
export ARROW_APP_SECRET=smoke-secret
export ARROW_TOKEN=fake-token-for-test
export RAW_TABLE_NAME=raw_table_1
# R-050: IngestionConfig treats these as required with no code default —
# without them SmokeTest throws at the first config-validation step.
export ARROW_MAX_EVENT_AGE_MS=5000
export ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000

cd "$DIR"
java --add-opens=java.base/java.nio=ALL-UNNAMED \
	-cp "02_services/01_ingestion/target/ingestion.jar:02_services/01_ingestion/target/test-classes" \
	com.trading.ingestion.SmokeTest
