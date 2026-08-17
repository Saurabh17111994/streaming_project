#!/usr/bin/env bash
# CI-style module check for the no-CEP project rule (01-foundation.md): run the
# shell guard scoped to the compute module and confirm it agrees with the
# SIG-UNIT-007 JUnit dependency-scan test.
#
#   cep_guard.sh          -> shell gate on the compute module (same scope the
#                            SIG-UNIT-007 test scans)
#   CepDependencyGuardTest -> the in-JVM scan; its shell-guard-agreement and
#                            scan-scope-parity legs fail if the two ever
#                            disagree about the verdict or the file set
#
# Exit 0 only when both pass. Run from the repo root: `make cep-check-module`
# (honors MVN_FLAGS, e.g. MVN_FLAGS=-o for a warm local cache — R-143).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
GUARD="$ROOT/code/01_platform/04_scripts/cep_guard.sh"
MODULE="$ROOT/code/02_services/02_compute"
POM="$MODULE/pom.xml"

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

[ -f "$GUARD" ] || fail "cep_guard.sh not found at $GUARD"
[ -f "$POM" ] || fail "compute module pom not found at $POM"

echo "=== [1/2] shell guard on the compute module ==="
if ! bash "$GUARD" "$MODULE"; then
	fail "cep_guard.sh found CEP references in the compute module"
fi

echo "=== [2/2] SIG-UNIT-007 JUnit test (CepDependencyGuardTest) ==="
# shellcheck disable=SC2086  # MVN_FLAGS is a word-split flags variable by design
if ! mvn ${MVN_FLAGS:-} -q -f "$POM" test -Dtest=CepDependencyGuardTest; then
	fail "CepDependencyGuardTest failed (in-JVM scan / shell-guard agreement / scope parity)"
fi

echo "PASS: cep_guard.sh (compute module) + CepDependencyGuardTest agree — no CEP in the module"
