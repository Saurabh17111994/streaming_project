#!/usr/bin/env bash
# Start ingestion pipeline: Arrow broker → Go bridge → Java → Fluss
#
# SECURITY: credentials are NOT committed. This script sources Arrow secrets
# from ~/.env.arrow (git-ignored, outside the repo). Create it by copying the
# template block below:
#
#   cp run-ingestion.sh ~/arrow-creds.template && chmod 600 ~/.env.arrow
#
# Template (~/.env.arrow):
#   ARROW_APP_ID=your_app_id
#   ARROW_APP_SECRET=your_app_secret
#   ARROW_USER_ID=your_user_id
#   ARROW_PASSWORD=your_password
#   ARROW_TOTP_KEY=your_totp_key
#   # optional pre-authenticated token (alternative to AutoLogin):
#   # ARROW_TOKEN=your_token
#
set -euo pipefail

SECRETS_FILE="${SECRETS_FILE:-$HOME/.env.arrow}"

if [ ! -f "$SECRETS_FILE" ]; then
	echo "ingestion: FATAL — no secrets file at $SECRETS_FILE." >&2
	echo "Create it from the template in the header of run-ingestion.sh (chmod 600)." >&2
	exit 1
fi
# R-135: owner-only check — permissive umasks leak Arrow credentials.
if [ "$(stat -c '%a' "$SECRETS_FILE" 2>/dev/null)" != "600" ]; then
	echo "ingestion: FATAL — $SECRETS_FILE is not owner-only (mode $(stat -c '%a' "$SECRETS_FILE" 2>/dev/null || echo '?')). Run: chmod 600 \"$SECRETS_FILE\"" >&2
	exit 1
fi

# shellcheck disable=SC1090
source "$SECRETS_FILE"

# Required creds (fail fast, never print values)
: "${ARROW_APP_ID:?ARROW_APP_ID must be set in $SECRETS_FILE}"
: "${ARROW_APP_SECRET:?ARROW_APP_SECRET must be set in $SECRETS_FILE}"
if [ -z "${ARROW_TOKEN:-}" ]; then
	: "${ARROW_USER_ID:?ARROW_USER_ID must be set (or set ARROW_TOKEN)}"
	: "${ARROW_PASSWORD:?ARROW_PASSWORD must be set (or set ARROW_TOKEN)}"
	: "${ARROW_TOTP_KEY:?ARROW_TOTP_KEY must be set (or set ARROW_TOKEN)}"
fi

export ARROW_HFT_LATENCY_MS=50
export FLUSS_BOOTSTRAP=localhost:9123
export FLUSS_BOOTSTRAP_SERVERS=localhost:9123

# R-053: derive the chained launcher from this script's own location so the
# script is portable (any checkout, any user).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/code/run-ingestion-full.sh"
