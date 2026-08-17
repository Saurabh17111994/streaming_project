#!/usr/bin/env bash
# Submit the three Flink jobs (feature-compute, signal-strategy, babysitter) to
# the JobManager, waited on via its REST API. Runs inside the compute container.
set -euo pipefail

# Tracker 14 P4.1/P4.2 — deployment validation BEFORE anything is submitted:
# production must use the pinned RocksDB backend and durable S3 checkpoints;
# heap/HashMap state or local /tmp paths are never silently substituted.
case "${DEPLOYMENT_ENV:-dev}" in
	dev) ;;
	production)
		if [ "${STATE_BACKEND:-rocksdb}" = "hashmap" ]; then
			echo "compute: FATAL — STATE_BACKEND=hashmap is forbidden in DEPLOYMENT_ENV=production (tracker 14 P4.1); use rocksdb" >&2
			exit 1
		fi
		case "${CHECKPOINT_DIR:-}" in
			s3://* | s3a://*) ;;
			*)
				echo "compute: FATAL — CHECKPOINT_DIR must be an S3 object-store URI in DEPLOYMENT_ENV=production (tracker 14 P4.2)" >&2
				exit 1
				;;
		esac
		# Tracker 14 P4.2 — object-store checkpoint credentials come from secret
		# injection (env), never committed files; the job's own config fails the
		# same way (SignalJobConfig.s3Endpoint), this is the launcher-side gate
		# that runs before anything is submitted.
		if [ -z "${S3_ENDPOINT:-}" ] && [ -z "${R2_ENDPOINT:-}" ]; then
			echo "compute: FATAL — S3_ENDPOINT (or R2_ENDPOINT) is required for S3 checkpoints in DEPLOYMENT_ENV=production (tracker 14 P4.2)" >&2
			exit 1
		fi
		if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
			echo "compute: FATAL — AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required for S3 checkpoints in DEPLOYMENT_ENV=production (tracker 14 P4.2); inject via secrets, never commit files" >&2
			exit 1
		fi
		;;
	*)
		echo "compute: FATAL — DEPLOYMENT_ENV must be 'dev' or 'production', got '${DEPLOYMENT_ENV}' (tracker 14 P4.1)" >&2
		exit 1
		;;
esac

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
