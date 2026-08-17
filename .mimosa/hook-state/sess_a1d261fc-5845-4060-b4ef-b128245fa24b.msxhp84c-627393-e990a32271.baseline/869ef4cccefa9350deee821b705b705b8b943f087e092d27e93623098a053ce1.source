#!/usr/bin/env bash
# ddl-apply-run — DDL application contract runner (engine dispatcher, run as
# the non-root ddlapply user via the entrypoint wrapper).
#
# Subcommands (extra args pass through to the underlying script):
#   validate    the plain `make ddl` check: version gate, manifest staleness,
#               checksum verification (exit 0 when the corpus is current)
#   apply       the FULL 9-step contract: empty-catalog precondition, in-band
#               COMPAT-FLUSS-005 matrix gate, deterministic apply, parity,
#               write/read smoke, evidence record. Exit 0 full PASS; 6
#               acknowledged PASS_WITH_LIMITATION; 1 failure/refused limitation.
#               Honours DDL_APPLY_TABLE_PREFIX, DDL_APPLY_ACK_LIMITATIONS
#               (=auto), DDL_APPLY_SKIP_SMOKE via the environment.
#   smoke       the exit-code contract battery (0/6/1 + sentinels + evidence
#               matrix) against scratch-prefixed catalogs; when docker + the
#               ddl-apply image are available on the HOST it also runs the
#               containerized bad-ownership drill (a pre-seeded engine-uid
#               644 record must flip the apply to exit 1 with EVIDENCE
#               OWNERSHIP CHECK FAILED)
#   evidence-check  the non-root ownership gate: every container-written
#               evidence record must be group-writable and none root-owned
#               (evidence_ownership_check.py); also auto-runs at the end of
#               every `apply` so the container validates its own corpus before
#               exiting
#   self-test   the engine's python unit tests (test_ddl_apply.py +
#               test_evidence_ownership_check.py), no cluster needed — sibling
#               host tooling tests are deliberately NOT in this image
#
# FLUSS_BOOTSTRAP defaults to fluss-coordinator:9123 — compose DNS inside
# trading-net, so no host /etc/hosts aliases are involved.
set -euo pipefail

SCRIPTS=/app/code/01_platform/04_scripts
MATRIX_EVIDENCE="${DDL_APPLY_MATRIX_EVIDENCE:-/app/logs/schema-compat/composite-pk-raw-client-20260815.md}"
# In-container marker: the entrypoint wrapper already emitted the APPLIED
# ownership contract, so the engine's host-style echo (ddl_apply.py
# echo_ownership_contract) is suppressed — one contract line per run, never two.
export DDL_APPLY_IN_CONTAINER=1

cmd="${1:-validate}"
if [ "$#" -gt 0 ]; then shift; fi

case "$cmd" in
  validate)
    exec python3 "$SCRIPTS/ddl_apply.py" "$@"
    ;;
  apply)
    # Run the full 9-step contract, then validate the corpus we just wrote
    # BEFORE exiting: a broken non-root ownership contract is a hard failure
    # even when the apply itself passed (the RESULT= sentinel from the engine
    # still documents the apply's own status).
    set +e
    python3 "$SCRIPTS/ddl_apply.py" \
      --apply-verified --matrix-evidence "$MATRIX_EVIDENCE" "$@"
    rc=$?
    set -e
    if ! python3 "$SCRIPTS/evidence_ownership_check.py"; then
      echo "ddl-apply: apply exit=$rc but EVIDENCE OWNERSHIP CHECK FAILED — " \
        "the non-root ownership contract is broken" >&2
      exit 1
    fi
    exit "$rc"
    ;;
  smoke)
    exec python3 "$SCRIPTS/ddl_apply_smoke.py" \
      --matrix-evidence "$MATRIX_EVIDENCE" "$@"
    ;;
  evidence-check)
    exec python3 "$SCRIPTS/evidence_ownership_check.py" "$@"
    ;;
  self-test)
    # The image ships only the engine's tests (test_ddl_apply.py +
    # test_evidence_ownership_check.py) — the whole tests dir is ours, so a
    # plain discover runs exactly the engine's suite.
    exec python3 -m unittest discover -s "$SCRIPTS/tests"
    ;;
  *)
    echo "ddl-apply: unknown subcommand '$cmd'" >&2
    echo "usage: ddl-apply {validate|apply|smoke|evidence-check|self-test} [args...]" >&2
    echo "  env: FLUSS_BOOTSTRAP, DDL_APPLY_TABLE_PREFIX, DDL_APPLY_ACK_LIMITATIONS," >&2
    echo "       DDL_APPLY_SKIP_SMOKE, DDL_APPLY_MATRIX_EVIDENCE, DDL_APPLY_EVIDENCE_DIR," >&2
    echo "       DDL_APPLY_UID/GID" >&2
    exit 2
    ;;
esac
