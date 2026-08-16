#!/usr/bin/env bash
# Full documentation audit — one command, three layers (2026-08-16 campaign
# consolidation; wired as `make full-audit`):
#
#   Layer 1 — the machine gates:
#     a. stale-claim scanner --upstream (table kinds, phase status, numeric
#        drift, test counts, C6 triples) — the `make stale-tables` gate
#     b. docs-audit (manifest, ownership matrix, schema-state diagram,
#        compat vocabulary, stale phrases, test counts, version pins)
#     c. --ddl mode (DDL files + schema_manifest.json table-kind parity)
#
#   Layer 2 — beyond-scanner sweeps (claims the scanner's pattern set cannot
#     see):
#     a. live Ranking/Reservations/Decisions claims (post-CHG-005 removal) —
#        the only allowed hits are the intentional whitelist: C8-required
#        matrix/REQ rows, struck-through lines, and the dated changelog entry
#     b. stale 'pending implementation / still pending' prose in the upstream
#        layers (requirements, architecture, contracts, project)
#
#   Layer 3 — dossier-trio coherence: docs/08_implementation/{04-signal-job,
#     13-candle-log-kv-replay-safety, 14-candle-log-kv-replay-safety_2} must
#     agree on the 2026-08-13 re-scope, the DEC-038 externalization landing,
#     and the P11 status.
#
# Exit 0 only when all three layers pass. Run from the repo root.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPTS="$ROOT/code/01_platform/04_scripts"
DOCS="$ROOT/docs"
TRIO_DIR="$DOCS/08_implementation"

FAIL=0

step() { echo; echo "===== $1 ====="; }
pass() { echo "  PASS  $1"; }
fail() { echo "  FAIL  $1"; FAIL=1; }

cd "$ROOT"

# ---------------------------------------------------------------------------
# Layer 1 — machine gates
# ---------------------------------------------------------------------------
step "Layer 1a: stale-claim scanner (--upstream)"

if python3 "$SCRIPTS/stale_table_kind_scan.py" --upstream; then
	pass "stale_table_kind_scan --upstream (table kinds / phase status / numeric drift / test counts / C6)"
else
	fail "stale_table_kind_scan --upstream found un-annotated claims"
fi

step "Layer 1b: docs-audit"

if python3 "$SCRIPTS/docs_audit.py"; then
	pass "docs_audit.py (manifest / ownership / schema-state / compat / stale phrases / counts / pins)"
else
	fail "docs_audit.py failed"
fi

step "Layer 1c: DDL + manifest parity (--ddl)"

if python3 "$SCRIPTS/stale_table_kind_scan.py" --ddl; then
	pass "stale_table_kind_scan --ddl (DDL files + schema_manifest.json kinds)"
else
	fail "stale_table_kind_scan --ddl found DDL/manifest drift"
fi

# ---------------------------------------------------------------------------
# Layer 2 — beyond-scanner sweeps
# ---------------------------------------------------------------------------
step "Layer 2a: live Ranking/Reservations/Decisions claims (post-CHG-005)"

RANKING_HITS="$(grep -rn -i 'ranking\|reservation' "$DOCS" --include='*.md' \
	| grep -v 'REMOVED\|out of scope\|OUT OF SCOPE\|removed\|REMOVAL\|preservation\|Resource reservations' \
	| grep -vE 'change-records|04-decisions|10-ranking\.md|05-risks|historical|Historical|HISTORICAL|postponed|deferred|superseded|SUPERSEDED|AC-RNK|REQ-RNK|ASM-RNK|stub retained|cross-reference' \
	| grep -vE '### Ranking|\| Ranking \||REQ-FLS-007|REQ-FLS-014|REQ-FLS-016|2026-07-23|~~' || true)"

if [ -z "$RANKING_HITS" ]; then
	pass "no live Ranking/Reservations/Decisions claims beyond the intentional whitelist"
else
	fail "unexpected live ranking/reservation claims:"
	echo "$RANKING_HITS"
fi

step "Layer 2b: stale 'pending implementation' prose in upstream layers"

PENDING_HITS="$(grep -rn -i 'still pending\|pending implementation\|remains pending\|not yet implemented' \
	"$DOCS/02_requirements" "$DOCS/03_architecture" "$DOCS/04_contracts" "$DOCS/01_project" 2>/dev/null \
	| grep -vi 'removed\|out of scope\|RESOLVED\|LANDED\|superseded\|historical' || true)"

if [ -z "$PENDING_HITS" ]; then
	pass "no un-annotated 'pending implementation' claims in upstream layers"
else
	fail "un-annotated pending-implementation claims:"
	echo "$PENDING_HITS"
fi

# ---------------------------------------------------------------------------
# Layer 3 — dossier-trio coherence
# ---------------------------------------------------------------------------
step "Layer 3: dossier-trio coherence (04-signal-job / 13-candle-log-kv-replay-safety / 14-candle-log-kv-replay-safety_2)"

TRIO_FAIL=0
trio_check() { # desc, file, literal substring
	if grep -qF "$3" "$2"; then
		echo "  PASS  $1"
	else
		echo "  FAIL  $1 — missing '$3' in $2"
		TRIO_FAIL=1
	fi
}

# Doc 04 (04-signal-job.md): DEC-038 banner + SIG-PERF-001 halves.
trio_check "doc04: DEC-038 banner records the externalization landing" \
	"$TRIO_DIR/04-signal-job.md" \
	"SUPERSEDED SAME-DAY (2026-08-15): the live-cluster externalization measurement LANDED"
trio_check "doc04: DEC-038 status line records the live writer wiring landing" \
	"$TRIO_DIR/04-signal-job.md" \
	"SUPERSEDED SAME-DAY (2026-08-15): the live writer wiring LANDED"
trio_check "doc04: SIG-PERF-001 halves disambiguated (benchmark landed / decision-p99 removed)" \
	"$TRIO_DIR/04-signal-job.md" \
	"externalization-benchmark half LANDED"

# Doc 13 (13-candle-log-kv-replay-safety.md): candle [LOG+KV] retired, signal tables carry the facility.
trio_check "doc13: candle [LOG+KV] RETIRED banner" \
	"$TRIO_DIR/13-candle-log-kv-replay-safety.md" \
	"CANDLE [LOG + KV] RETIRED"
trio_check "doc13: feature_candles_15s_current retired" \
	"$TRIO_DIR/13-candle-log-kv-replay-safety.md" \
	"feature_candles_15s_current"

# Doc 14 (14-candle-log-kv-replay-safety_2.md): P11 + re-scope markers.
trio_check "doc14: P11 section present" \
	"$TRIO_DIR/14-candle-log-kv-replay-safety_2.md" \
	"## P11 — DEC-038 state ownership: dedup externalization"
trio_check "doc14: re-scope LANDED marker (CANDLE-CANONICAL-001 cell)" \
	"$TRIO_DIR/14-candle-log-kv-replay-safety_2.md" \
	"re-scope LANDED 2026-08-13"
trio_check "doc14: P11 landed re-target note" \
	"$TRIO_DIR/14-candle-log-kv-replay-safety_2.md" \
	"P11 landed 2026-08-15"

if [ "$TRIO_FAIL" -ne 0 ]; then
	fail "dossier-trio coherence broken — the three dossiers disagree on a reconciled truth"
fi

# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
echo
if [ "$FAIL" -eq 0 ]; then
	echo "FULL-AUDIT: all layers green (gates + beyond-scanner sweeps + trio coherence) — exit 0"
	exit 0
fi
echo "FULL-AUDIT: FAILURES above — exit 1"
exit 1
