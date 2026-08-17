#!/usr/bin/env bash
# ddl-apply — container entrypoint (root wrapper).
#
# The image runs the ENGINE as a non-root user (ddlapply, uid/gid 10001 by
# default): python orchestrator + Java DdlApplyTool never execute as root. This
# wrapper exists ONLY to repair the bind-mounted evidence dir, which on the
# host is owned by the host user and therefore unwritable by the engine user:
#
#   1. claim ONLY the evidence root dir: mkdir -p + chown + setgid (2775). This
#      is a TOP-LEVEL, NON-RECURSIVE contract — pre-existing files/subdirs
#      under a redirected path keep their ownership, so pointing
#      DDL_APPLY_EVIDENCE_DIR at a shared mount never flips unrelated content
#      to 10001. Everything the engine creates underneath is owned by the
#      engine user and inherits the group through the setgid bit (umask 002 is
#      set before the drop) -> records land group-writable (664) by
#      construction, no recursion needed;
#   2. PERMANENTLY drop privileges via setpriv — the engine work happens after
#      the drop, never as root;
#   3. emit the applied contract (evidence root owner/mode, umask -> 664) into
#      the engine output BEFORE the drop, so operators see exactly what the
#      ownership gate (evidence_ownership_check.py, C15 + in-band apply check)
#      will enforce for this run.
#
# Host-side consequence: the evidence root dir becomes owned 10001:10001 mode
# 2775, records 664. Host automation reads freely; for delete/rotate add the
# host user to a group with GID 10001 (one-time, then re-login):
#   sudo groupadd -g 10001 ddlapply && sudo usermod -aG ddlapply $USER
# (Or pin the ids to the host user with DDL_APPLY_UID/DDL_APPLY_GID.) Because
# only the top dir is claimed, point DDL_APPLY_EVIDENCE_DIR at a DEDICATED
# subdirectory on any mount — never the mount root itself.
#
# A strict non-root deployment (docker run --user ddlapply) skips the repair —
# the host must pre-chown the evidence dir — and runs the runner directly.
set -euo pipefail

DDL_APPLY_UID="${DDL_APPLY_UID:-10001}"
DDL_APPLY_GID="${DDL_APPLY_GID:-10001}"
# The evidence root the engine writes (ddl_apply.py's DDL_APPLY_EVIDENCE_DIR
# override). An operator redirects evidence to any mounted dir with the same
# env var — the wrapper repairs ownership on that exact path, no image edit.
DDL_APPLY_EVIDENCE_DIR="${DDL_APPLY_EVIDENCE_DIR:-/app/logs/ddl-apply}"

if [ "$(id -u)" -eq 0 ]; then
  mkdir -p "$DDL_APPLY_EVIDENCE_DIR"
  # Top-level only (non-recursive by contract): claim the root dir itself;
  # descendants are created by the engine (owned by it, group inherited via
  # setgid). Pre-existing content under a redirected path is never flipped.
  chown "${DDL_APPLY_UID}:${DDL_APPLY_GID}" "$DDL_APPLY_EVIDENCE_DIR"
  chmod 2775 "$DDL_APPLY_EVIDENCE_DIR"
  umask 002
  # Emit the applied contract into the engine output (same stream, printed
  # before the drop/exec) so operators see what the ownership gate will
  # enforce for this run. Mode is read back via stat to show the ACTUAL state.
  printf 'ddl-apply: evidence root %s repaired → owner %s:%s, mode %s (setgid + group-write), umask 002 → records land 664; gate enforces uid %s gid %s + group-writable\n' \
    "$DDL_APPLY_EVIDENCE_DIR" "$DDL_APPLY_UID" "$DDL_APPLY_GID" \
    "$(stat -c '%a' "$DDL_APPLY_EVIDENCE_DIR")" "$DDL_APPLY_UID" "$DDL_APPLY_GID"
  exec setpriv --reuid "${DDL_APPLY_UID}" --regid "${DDL_APPLY_GID}" \
    --clear-groups /usr/local/bin/ddl-apply-run "$@"
fi

# Already non-root: no ownership repair possible, run the engine directly.
# Still emit the contract the gate will enforce (strict deployments must
# pre-chown the evidence dir on the host before running).
printf 'ddl-apply: non-root entrypoint (uid %s) — ownership repair skipped; gate still enforces uid %s gid %s, evidence root setgid 2775, records group-writable 664\n' \
  "$(id -u)" "$DDL_APPLY_UID" "$DDL_APPLY_GID"
exec /usr/local/bin/ddl-apply-run "$@"
