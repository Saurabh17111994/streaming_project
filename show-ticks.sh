#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
LIMIT="${1:-20}"

if [[ ! -f "$JAR" ]]; then
  printf 'Missing ingestion JAR. Build it first with:\n  cd %s/code && mvn -pl 02_services/01_ingestion -am package\n' "$ROOT" >&2
  exit 1
fi

if [[ ! "$LIMIT" =~ ^[1-9][0-9]*$ ]]; then
  printf 'Usage: %s [positive-row-limit]\n' "$0" >&2
  exit 2
fi

# R-273: honor FLUSS_BOOTSTRAP (falling back to localhost:9123) so the
# pre-flight probe checks the same endpoint the viewer will actually use.
FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
FLUSS_HOST="${FLUSS_BOOTSTRAP%%:*}"
FLUSS_PORT="${FLUSS_BOOTSTRAP##*:}"
if ! (exec 3<>/dev/tcp/"$FLUSS_HOST"/"$FLUSS_PORT") 2>/dev/null; then
  cat >&2 <<EOF
Local Fluss is not running on $FLUSS_BOOTSTRAP.
Start the local cluster first, then run this command again.
EOF
  exit 1
fi

exec java \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp "$JAR" \
  com.trading.ingestion.TickTableViewer "$LIMIT"
