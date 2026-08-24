#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
TS="$(date -u +%Y%m%d-%H%M%S)"
LOGDIR="${REPO_ROOT}/logs/chaos/chaos-${TS}"
mkdir -p "${LOGDIR}"
SUMMARY="${LOGDIR}/SUMMARY.txt"
echo "CHAOS-SUITE: start ${TS} repo ${REPO_ROOT}" | tee "${SUMMARY}"
SELF_FAIL=0
for f in "${SCRIPT_DIR}/chaos-01-slot-kill.sh" "${SCRIPT_DIR}/chaos-02-tm-kill.sh" "${SCRIPT_DIR}/chaos-03-tablet-kill.sh" "${SCRIPT_DIR}/chaos-04-vm-loss.sh"; do
  if ! bash -n "${f}" 2>>"${SUMMARY}"; then
    echo "SELF-CHECK FAIL: ${f} bash -n" | tee -a "${SUMMARY}"
    SELF_FAIL=1
  fi
  if command -v shellcheck >/dev/null 2>&1; then
    if ! shellcheck -S warning "${f}" 2>>"${SUMMARY}"; then
      echo "SELF-CHECK FAIL: ${f} shellcheck" | tee -a "${SUMMARY}"
      SELF_FAIL=1
    fi
  fi
done
if [[ "${SELF_FAIL}" -ne 0 ]]; then
  echo "CHAOS-SUITE: RESULT=FAIL EXIT=1 (self-check)" | tee -a "${SUMMARY}"
  exit 1
fi
RESULTS=()
FAIL_COUNT=0
run_one() {
  local idx="$1"
  local name="$2"
  local script="$3"
  local log="${LOGDIR}/${name}.log"
  echo "=== [${idx}/4] ${name} ===" | tee -a "${SUMMARY}"
  set +e
  bash "${script}" 2>&1 | tee "${log}"
  local rc=${PIPESTATUS[0]}
  set -e
  local label="PASS"
  if [[ "${rc}" -eq 0 ]]; then
    label="PASS"
  elif [[ "${rc}" -eq 3 ]]; then
    label="SKIP"
  else
    label="FAIL"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  echo "RESULT [${idx}]: ${label} (exit ${rc})" | tee -a "${SUMMARY}"
  RESULTS+=("${label}")
  # also echo sentinel per test for grep
  if [[ "${label}" == "PASS" ]]; then
    echo "${name}: PASS" >>"${SUMMARY}"
  elif [[ "${label}" == "SKIP" ]]; then
    echo "${name}: SKIP" >>"${SUMMARY}"
  else
    echo "${name}: FAIL" >>"${SUMMARY}"
  fi
}
run_one "1" "01-slot-kill" "${SCRIPT_DIR}/chaos-01-slot-kill.sh"
run_one "2" "02-tm-kill" "${SCRIPT_DIR}/chaos-02-tm-kill.sh"
run_one "3" "03-tablet-kill" "${SCRIPT_DIR}/chaos-03-tablet-kill.sh"
run_one "4" "04-vm-loss" "${SCRIPT_DIR}/chaos-04-vm-loss.sh"
if [[ "${FAIL_COUNT}" -eq 0 ]]; then
  echo "CHAOS-SUITE: RESULT=PASS EXIT=0" | tee -a "${SUMMARY}"
  exit 0
else
  echo "CHAOS-SUITE: RESULT=FAIL EXIT=1" | tee -a "${SUMMARY}"
  exit 1
fi
