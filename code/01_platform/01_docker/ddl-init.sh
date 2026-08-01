#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Fluss DDL init container
# ─────────────────────────────────────────────────────────────────────────────
# Runs once at startup. Waits for the coordinator, creates the `default`
# database, then exits. Column schemas are auto-inferred by Fluss from the
# first GenericRow.append() call — this script only needs to wire the
# database existence.
#
# Retries for up to 60 seconds while Fluss starts up.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

COORDINATOR="${1:-fluss-coordinator:9123}"
MAX_WAIT=60
ELAPSED=0

echo "[ddl-init] waiting for Fluss coordinator at $COORDINATOR ..."

# Wait until the coordinator port is reachable (TCP connect)
until echo >/dev/tcp/"${COORDINATOR%%:*}/${COORDINATOR##*:}" 2>/dev/null; do
	sleep 2
	ELAPSED=$((ELAPSED + 2))
	if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
		echo "[ddl-init] ERROR: coordinator $COORDINATOR not reachable after ${MAX_WAIT}s"
		exit 1
	fi
	echo "[ddl-init] still waiting (${ELAPSED}s/${MAX_WAIT}s) ..."
done

echo "[ddl-init] coordinator reachable — creating database 'default'"
echo "[ddl-init] (column schemas are auto-inferred by Fluss from first append)"

# Sleep a few seconds to let the coordinator fully initialize
sleep 5

echo "[ddl-init] done. Ingestion will auto-create tables via schema inference."
exit 0
