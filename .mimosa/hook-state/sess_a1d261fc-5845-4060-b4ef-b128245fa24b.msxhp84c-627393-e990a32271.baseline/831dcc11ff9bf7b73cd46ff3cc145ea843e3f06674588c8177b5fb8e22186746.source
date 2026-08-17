#!/usr/bin/env bash
# =============================================================================
# import_instruments.sh — One-time CSV → Fluss instruments table import
#
# Usage:
#   ./import_instruments.sh <csv_file> [--dry-run]
#
# CSV columns (order must match):
#   instrument_token,symbol,exchange,instrument_type,strike,expiry,option_type,lot_size,tick_size,segment,is_active
#
# Example CSV row:
#   256265,RELIANCE,NSE,EQ,,,,1,0.05,nse_equity,true
#   260105,NIFTY 24 APR FUT,NFO,FUT,,1712851200000,,50,0.05,nse_derivatives,true
#
# Prerequisites:
#   - Fluss catalog + database created (01_catalog.sql applied)
#   - instruments table created (11_instruments.sql applied)
#   - FLUSS_BOOTSTRAP env var set (e.g. fluss-coordinator:9123)
# =============================================================================

set -euo pipefail

CSV_FILE="${1:-}"
DRY_RUN=false
if [[ "${2:-}" == "--dry-run" ]]; then
	DRY_RUN=true
fi

if [[ -z "$CSV_FILE" ]]; then
	echo "Usage: $0 <csv_file> [--dry-run]" >&2
	echo "  csv_file: path to instruments CSV" >&2
	echo "  --dry-run: print SQL to stdout without executing" >&2
	exit 1
fi

if [[ ! -f "$CSV_FILE" ]]; then
	echo "ERROR: file not found: $CSV_FILE" >&2
	exit 1
fi

FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-fluss-coordinator:9123}"
SQL_FILE="$(mktemp /tmp/instruments_import_XXXXXX.sql)"
NOW_EPOCH_MS=$(date +%s%3N)

echo "-- instruments import generated $(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$SQL_FILE"
echo "USE CATALOG fluss_catalog;" >>"$SQL_FILE"
echo "USE trading;" >>"$SQL_FILE"
echo "" >>"$SQL_FILE"

LINE_NUM=0
INSERT_COUNT=0

# Read CSV, skip header if it looks like one
while IFS= read -r line; do
	LINE_NUM=$((LINE_NUM + 1))

	# Skip empty lines
	[[ -z "$line" ]] && continue

	# Skip header row
	if [[ $LINE_NUM -eq 1 ]] && [[ "$line" =~ ^instrument_token ]]; then
		echo "-- Skipping header row (line $LINE_NUM)" >>"$SQL_FILE"
		continue
	fi

	# Parse CSV — handle empty fields
	IFS=',' read -ra FIELDS <<<"$line"

	TOKEN="${FIELDS[0]:-}"
	SYMBOL="${FIELDS[1]:-}"
	EXCHANGE="${FIELDS[2]:-}"
	INSTYPE="${FIELDS[3]:-}"
	STRIKE="${FIELDS[4]:-}"
	EXPIRY="${FIELDS[5]:-}"
	OPTYPE="${FIELDS[6]:-}"
	LOT="${FIELDS[7]:-}"
	TICK="${FIELDS[8]:-}"
	SEGMENT="${FIELDS[9]:-}"
	ACTIVE="${FIELDS[10]:-}"

	# Validate required fields
	if [[ -z "$TOKEN" ]] || [[ -z "$SYMBOL" ]] || [[ -z "$EXCHANGE" ]] || [[ -z "$INSTYPE" ]]; then
		echo "-- WARNING: skipping line $LINE_NUM (missing required field): $line" >>"$SQL_FILE"
		continue
	fi

	# Build NULL-safe INSERT
	STRIKE_SQL="NULL"
	[[ -n "$STRIKE" ]] && STRIKE_SQL="$STRIKE"
	EXPIRY_SQL="NULL"
	[[ -n "$EXPIRY" ]] && EXPIRY_SQL="$EXPIRY"
	OPTYPE_SQL="NULL"
	[[ -n "$OPTYPE" ]] && OPTYPE_SQL="'$OPTYPE'"
	LOT_SQL="NULL"
	[[ -n "$LOT" ]] && LOT_SQL="$LOT"
	TICK_SQL="NULL"
	[[ -n "$TICK" ]] && TICK_SQL="$TICK"
	SEGMENT_SQL="NULL"
	[[ -n "$SEGMENT" ]] && SEGMENT_SQL="'$SEGMENT'"
	ACTIVE_SQL="true"
	[[ -n "$ACTIVE" ]] && ACTIVE_SQL="$ACTIVE"

	cat >>"$SQL_FILE" <<EOF
INSERT INTO instruments VALUES (
  $TOKEN, '$SYMBOL', '$EXCHANGE', '$INSTYPE',
  $STRIKE_SQL, $EXPIRY_SQL, $OPTYPE_SQL,
  $LOT_SQL, $TICK_SQL, $SEGMENT_SQL,
  $ACTIVE_SQL, $NOW_EPOCH_MS
);
EOF
	INSERT_COUNT=$((INSERT_COUNT + 1))

done <"$CSV_FILE"

echo "" >>"$SQL_FILE"
echo "-- Total: $INSERT_COUNT rows" >>"$SQL_FILE"

if $DRY_RUN; then
	echo "=== DRY RUN — SQL written to $SQL_FILE ==="
	cat "$SQL_FILE"
	rm "$SQL_FILE"
	exit 0
fi

echo "Importing $INSERT_COUNT instruments into Fluss..."
echo "Fluss bootstrap: $FLUSS_BOOTSTRAP"

# Execute via Flink SQL Client (default — adjust if using Fluss CLI directly)
if command -v sql-client.sh &>/dev/null; then
	sql-client.sh -f "$SQL_FILE"
elif command -v fluss-cli.sh &>/dev/null; then
	fluss-cli.sh --bootstrap "$FLUSS_BOOTSTRAP" -f "$SQL_FILE"
else
	echo "WARNING: Neither sql-client.sh nor fluss-cli.sh found on PATH." >&2
	echo "SQL written to $SQL_FILE — execute it manually." >&2
	echo "Example:" >&2
	echo "  sql-client.sh -f $SQL_FILE" >&2
fi

rm -f "$SQL_FILE"
echo "Done."
