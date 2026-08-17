#!/usr/bin/env bash
# Action-capture entrypoint: subscribe to postbacks, write Fills + KV lifecycle.
set -euo pipefail
echo "action-capture: starting (FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP:-fluss-coordinator:9123})"
exec java -jar /app/action-capture.jar
