#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  start-all.sh — ONE COMMAND to start the whole ingestion pipeline:
#
#    Arrow broker  →  Go bridge  →  Java IngestionService  →  local Fluss
#
#  What this script does, in order:
#    1. Checks your Arrow credentials file exists (~/.env.arrow) — creates a
#       template you fill in if it's missing (this is the ONLY manual step).
#    2. Starts Fluss (zookeeper + coordinator + tablet) via docker compose
#       if it isn't already running.
#    3. Builds the Go bridge binary and the Java jar if they're out of date.
#    4. Creates the Fluss tables (DDL) if they don't exist yet (local dev only).
#    5. Runs the pipeline.  Press Ctrl+C to stop everything cleanly.
#
#  SECURITY: this file never contains or prints secrets. Credentials stay in
#  ~/.env.arrow (chmod 600, git-ignored).
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Config (edit these, or override via env when you run the script) ──────────
PROJECT_ROOT="${PROJECT_ROOT:-/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project}"
CODE_DIR="${CODE_DIR:-$PROJECT_ROOT/code}"
COMPOSE_DIR="${COMPOSE_DIR:-$CODE_DIR/01_platform/01_docker}"
COMPOSE_FILE="${COMPOSE_FILE:-$COMPOSE_DIR/docker-compose.yml}"
SECRETS_FILE="${SECRETS_FILE:-$HOME/.env.arrow}"
MANIFEST="${ARROW_INSTRUMENT_MANIFEST:-/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv}"
BRIDGE_DIR="${BRIDGE_DIR:-$CODE_DIR/02_services/01_ingestion/go-bridge}"
JAVA_DIR="${JAVA_DIR:-$CODE_DIR/02_services/01_ingestion}"
DDL_DIR="${DDL_DIR:-$CODE_DIR/01_platform/02_sql/ddl}"
FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
ALLOW_RUNTIME_DDL="${ALLOW_RUNTIME_DDL:-true}"   # local dev only; production must be false
GO_FLAGS="${GO_FLAGS:-}"                          # e.g. GO_FLAGS=-tags=netgo
MVN_FLAGS="${MVN_FLAGS:--o}"                      # -o = offline (use cached deps)
LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"

log()  { printf '\033[1;36m[start-all]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[start-all] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

# ── 1. Arrow credentials ───────────────────────────────────────────────────────
# Preferred source: ~/.env.arrow (git-ignored, outside the repo).
# Fallback: the docker-compose .env (code/01_platform/01_docker/.env), which
# already holds the real Arrow creds for this machine. Only the ARROW_* keys
# are pulled — the .env is NOT sourced wholesale (it contains non-shell-safe
# values like O2_PASSWORD=<choose a password>). Values are never printed.
if [ -f "$SECRETS_FILE" ]; then
	# shellcheck disable=SC1090
	source "$SECRETS_FILE"
	log "credentials from $SECRETS_FILE"
elif [ -f "$COMPOSE_DIR/.env" ]; then
	# Extract only ARROW_* assignments, eval each with proper quoting.
	while IFS= read -r line; do
		eval "export ${line?}"
	done < <(awk -F= '/^ARROW_[A-Z_]+=/ {print $1 "=" substr($0, index($0,"=")+1)}' "$COMPOSE_DIR/.env")
	log "no $SECRETS_FILE — using ARROW_* credentials from $COMPOSE_DIR/.env"
else
	cat > "$SECRETS_FILE" <<EOF
# Arrow broker credentials — fill these in, then re-run start-all.sh
ARROW_APP_ID=
ARROW_APP_SECRET=
ARROW_USER_ID=
ARROW_PASSWORD=
ARROW_TOTP_KEY=
# optional pre-authenticated token (alternative to AutoLogin):
# ARROW_TOKEN=
EOF
	chmod 600 "$SECRETS_FILE"
	die "created a template at $SECRETS_FILE — open it, fill in your Arrow credentials, then re-run."
fi
: "${ARROW_APP_ID:?ARROW_APP_ID must be set (credentials file / compose .env)}"
: "${ARROW_APP_SECRET:?ARROW_APP_SECRET must be set (credentials file / compose .env)}"
if [ -z "${ARROW_TOKEN:-}" ]; then
	: "${ARROW_USER_ID:?ARROW_USER_ID must be set (or set ARROW_TOKEN)}"
	: "${ARROW_PASSWORD:?ARROW_PASSWORD must be set (or set ARROW_TOKEN)}"
	: "${ARROW_TOTP_KEY:?ARROW_TOTP_KEY must be set (or set ARROW_TOKEN)}"
fi
export ARROW_APP_ID ARROW_APP_SECRET ARROW_TOKEN ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY
log "credentials OK (from $SECRETS_FILE)"

# ── 2. Start Fluss core if not running ────────────────────────────────────────
if ! nc -z "${FLUSS_BOOTSTRAP%:*}" "${FLUSS_BOOTSTRAP##*:}" 2>/dev/null; then
	log "Fluss not running — starting zookeeper + coordinator + tablet..."
	# The .env supplies FLUSS_IMAGE etc.; fail loudly if it's missing.
	[ -f "$COMPOSE_DIR/.env" ] || die "missing $COMPOSE_DIR/.env (copy from Arrow_broker/.env and fill in FLUSS_IMAGE)"
	( cd "$COMPOSE_DIR" && docker compose up -d zookeeper fluss-coordinator fluss-tablet )
	log "waiting for Fluss coordinator on $FLUSS_BOOTSTRAP..."
	for i in $(seq 1 30); do
		if nc -z "${FLUSS_BOOTSTRAP%:*}" "${FLUSS_BOOTSTRAP##*:}" 2>/dev/null; then break; fi
		sleep 2
		[ "$i" = 30 ] && die "Fluss did not come up on $FLUSS_BOOTSTRAP after 60s — check docker compose logs"
	done
else
	log "Fluss already running on $FLUSS_BOOTSTRAP — skipping docker compose"
fi

# ── 3. Build bridge + jar if stale ────────────────────────────────────────────
mkdir -p "$LOG_DIR"
log "building Go bridge..."
( cd "$BRIDGE_DIR" && go build $GO_FLAGS -o arrow-bridge . )
log "building Java jar (offline)..."
( cd "$CODE_DIR" && mvn $MVN_FLAGS -q -pl 02_services/01_ingestion -am package -DskipTests )

# ── 4. Create tables if missing (local dev only) ──────────────────────────────
if [ "$ALLOW_RUNTIME_DDL" = "true" ]; then
	log "ensuring Fluss tables exist (ALLOW_RUNTIME_DDL=true, local dev)..."
	# The service calls DdlBootstrap.ensureTables when ALLOW_RUNTIME_DDL=true,
	# so tables are created on startup — nothing extra to do here.
else
	log "ALLOW_RUNTIME_DDL=false — tables must already exist; service verifies read-only"
fi

# ── 5. Run the pipeline ───────────────────────────────────────────────────────
log "starting ingestion pipeline (Ctrl+C to stop, logs → $LOG_DIR/ingestion.log)..."
export FLUSS_BOOTSTRAP
export FLUSS_BOOTSTRAP_SERVERS="$FLUSS_BOOTSTRAP"
export RAW_TABLE_NAME="${RAW_TABLE_NAME:-raw_table_1}"
export ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge"
export ARROW_INSTRUMENT_MANIFEST="$MANIFEST"
export ARROW_USE_STANDARD="${ARROW_USE_STANDARD:-false}"
export ARROW_HFT_LATENCY_MS="${ARROW_HFT_LATENCY_MS:-50}"
export NTP_SERVER="${NTP_SERVER:-ntp.ubuntu.com,time.google.com,in.pool.ntp.org}"
export ALLOW_RUNTIME_DDL
mkdir -p "$LOG_DIR"
java -jar "$JAVA_DIR/target/ingestion.jar" 2>&1 | tee "$LOG_DIR/ingestion.log"
