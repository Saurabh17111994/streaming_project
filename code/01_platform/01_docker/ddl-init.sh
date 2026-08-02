#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Fluss init container
# ─────────────────────────────────────────────────────────────────────────────
# Runs once at startup. Waits for the Fluss coordinator to accept a TCP
# connection, then exits 0.
#
# It does NOT create databases or tables (review R-083): database + table
# bootstrap happens inside the ingestion service (DdlBootstrap.ensureTables,
# local dev with ALLOW_RUNTIME_DDL=true) or through the offline DDL gate
# (ddl_apply.py + schema_manifest.json, production). Claiming otherwise here
# would give false confidence about cluster state.
#
# A TCP connect is a liveness signal, not a readiness check — the coordinator
# can accept connections before it can serve admin requests — so downstream
# work is still gated by the ingestion service's own verifyTables().
#
# Retries for up to MAX_WAIT seconds while Fluss starts up.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

COORDINATOR="${1:-fluss-coordinator:9123}"
MAX_WAIT="${MAX_WAIT:-60}"
ELAPSED=0

# ── Defensive host:port parsing (review R-183) ──────────────────────────────
# bash /dev/tcp does not support IPv6, and a missing port would make the
# probe path `/dev/tcp/host/host` (never succeeds). Fail fast instead.
case "$COORDINATOR" in
*:*) HOST="${COORDINATOR%:*}" PORT="${COORDINATOR##*:}" ;;
*) HOST="" PORT="" ;;
esac
if [ -z "$HOST" ] || ! [[ "$PORT" =~ ^[0-9]+$ ]]; then
	echo "[ddl-init] FATAL: invalid coordinator address '$COORDINATOR' (expected <host>:<numeric-port>; IPv6 not supported by /dev/tcp)." >&2
	exit 1
fi

echo "[ddl-init] waiting for Fluss coordinator at $COORDINATOR ..."

# Wait until the coordinator port is reachable (TCP connect)
until echo >/dev/tcp/"$HOST/$PORT" 2>/dev/null; do
	sleep 2
	ELAPSED=$((ELAPSED + 2))
	if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
		echo "[ddl-init] ERROR: coordinator $COORDINATOR not reachable after ${MAX_WAIT}s"
		exit 1
	fi
	echo "[ddl-init] still waiting (${ELAPSED}s/${MAX_WAIT}s) ..."
done

echo "[ddl-init] coordinator reachable at $COORDINATOR"
echo "[ddl-init] note: database/table bootstrap is done by the ingestion service"
echo "[ddl-init] (DdlBootstrap, dev) or the offline DDL gate (ddl_apply.py, production)."
exit 0
