#!/usr/bin/env bash
# Submit the three Flink jobs (feature-compute, signal-strategy, babysitter) to
# the JobManager, waited on via its REST API. Runs inside the compute container.
set -euo pipefail

JM="${FLINK_JOBMANAGER:-flink-jobmanager}:8081"
JAR=/opt/flink-jobs/compute.jar

echo "compute: waiting for JobManager at ${JM}"
# SC2034: loop counter is unused — the loop is a bounded retry-wait.
for _ in $(seq 1 30); do
	if curl -s "http://${JM}/v1/config" >/dev/null 2>&1; then break; fi
	sleep 2
done

for entry in feature-compute signal-strategy babysitter; do
	echo "compute: submitting ${entry}"
	curl -s -X POST "http://${JM}/jars/upload" -F "jarfile=@${JAR}" >/dev/null
	# Real submission passes the job's main class / args; placeholder until jobs exist.
	echo "  (placeholder: wire ${entry} main class + program args)"
done
