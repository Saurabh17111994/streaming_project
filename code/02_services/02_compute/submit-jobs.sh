#!/usr/bin/env bash
# Submit the two Flink jobs (signal and babysitter) to the JobManager through its
# REST API. Feature computation is part of SignalJob. Runs in the compute container.
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
ready=0
# SC2034: loop counter is unused — the loop is a bounded retry-wait.
for _ in $(seq 1 30); do
	if curl -fsS "http://${JM}/v1/config" >/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 2
done

if [ "${ready}" -ne 1 ]; then
	echo "compute: FATAL — JobManager ${JM} did not become ready" >&2
	exit 1
fi

if [ ! -r "${JAR}" ]; then
	echo "compute: FATAL — compute jar is not readable: ${JAR}" >&2
	exit 1
fi

echo "compute: uploading ${JAR}"
upload_response=$(curl -fsS -X POST "http://${JM}/jars/upload" \
	-F "jarfile=@${JAR}")
uploaded_filename=$(printf '%s' "${upload_response}" \
	| sed -n 's/.*"filename":"\([^"]*\)".*/\1/p')
jar_id=${uploaded_filename##*/}

if [ -z "${jar_id}" ] || [ "${jar_id}" = "${uploaded_filename}" ]; then
	echo "compute: FATAL — JobManager upload response did not contain a jar filename: ${upload_response}" >&2
	exit 1
fi

wait_for_running() {
	local job_id="$1"
	local job_name="$2"
	local state=""

	for _ in $(seq 1 30); do
		state=$(curl -fsS "http://${JM}/jobs/${job_id}" \
			| sed -n 's/.*"state":"\([^"]*\)".*/\1/p')
		case "${state}" in
			RUNNING)
				echo "compute: ${job_name} is RUNNING (${job_id})"
				return 0
				;;
			FAILED|CANCELED|CANCELING|SUSPENDED|RECONCILING)
				echo "compute: FATAL — ${job_name} entered terminal/failed state ${state} (${job_id})" >&2
				return 1
				;;
		esac
		sleep 2
	done

	echo "compute: FATAL — ${job_name} did not reach RUNNING (last state: ${state:-unknown})" >&2
	return 1
}

# T8 (Phase 5): local readiness must reflect a completed checkpoint, not just a
# RUNNING job. Poll the Flink REST checkpoints endpoint for the durable
# checkpoint COUNTER (counts.completed) and fail closed if neither job reaches
# one completed checkpoint within the timeout — a job that runs but never
# checkpoints is not locally ready (checkpoint/restore evidence is a T8 exit
# gate). The compute container never receives Arrow credentials/env (see the
# compose service), so execution intent stays disabled for both jobs.
wait_for_checkpoint() {
	local job_id="$1"
	local job_name="$2"
	local completed=""

	for _ in $(seq 1 30); do
		completed=$(curl -fsS "http://${JM}/jobs/${job_id}/checkpoints" 2>/dev/null \
			| sed -n 's/.*"counts":{[^}]*"completed":\([0-9][0-9]*\).*/\1/p')
		if [ -n "${completed}" ] && [ "${completed}" -gt 0 ]; then
			echo "compute: ${job_name} completed ${completed} checkpoint(s) (${job_id})"
			return 0
		fi
		sleep 2
	done

	echo "compute: FATAL — ${job_name} did not complete a checkpoint within timeout (last completed=${completed:-0})" >&2
	return 1
}

submit_job() {
	local job_name="$1"
	local entry_class="$2"
	local run_response
	local job_id

	echo "compute: submitting ${job_name} (${entry_class})"
	run_response=$(curl -fsS -X POST "http://${JM}/jars/${jar_id}/run" \
		-H "Content-Type: application/json" \
		-d "{\"entryClass\":\"${entry_class}\"}")
	job_id=$(printf '%s' "${run_response}" \
		| sed -n 's/.*"jobid":"\([^"]*\)".*/\1/p')

	if [ -z "${job_id}" ]; then
		echo "compute: FATAL — ${job_name} submission returned no job id: ${run_response}" >&2
		return 1
	fi

	wait_for_running "${job_id}" "${job_name}"
	wait_for_checkpoint "${job_id}" "${job_name}"
}

submit_job "signal-job-compute" "com.trading.compute.signaljob.SignalJob"
submit_job "Babysitter MVP no-op job" "com.trading.compute.babysitter.BabysitterJob"

echo "compute: Signal and Babysitter jobs submitted and RUNNING"
echo "compute: both jobs reach RUNNING and complete at least one durable checkpoint (T8 local readiness gate)"
