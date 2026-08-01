#!/bin/bash
# Ingestion smoke test — runs DDL bootstrap + 10 synthetic ticks against local Fluss.
set -euo pipefail

DIR="/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project/code"

export FLUSS_BOOTSTRAP=localhost:9123
export ARROW_APP_ID=smoke-test
export ARROW_APP_SECRET=smoke-secret
export ARROW_TOKEN=fake-token-for-test
export RAW_TABLE_NAME=raw_table_1

cd "$DIR"
java --add-opens=java.base/java.nio=ALL-UNNAMED \
	-cp "02_services/01_ingestion/target/ingestion.jar:02_services/01_ingestion/target/test-classes" \
	com.trading.ingestion.SmokeTest
