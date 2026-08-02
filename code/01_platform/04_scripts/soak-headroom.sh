#!/usr/bin/env bash
# soak-headroom.sh — subscription headroom evidence for the whole session.
#
# Proves plan §1379: "subscription headroom is observable and alerted." It
# reads the live ingestion JSON journal and derives headroom from the slot
# lifecycle events mirrored there by Java ("bridge lifecycle event=..."):
#   capacity_used = acknowledged_tokens / assigned_tokens  (per subscription_ack)
#   headroom      = 1 - capacity_used  (0 = fully subscribed, no room)
#
# On the 1-slot / 1024-instrument envelope, a full ack is 1024/1024 = 100% of
# the assignment — and the LIMIT for one connection is MaxHFTTokensPerConnection
# (see subscription_plan.go). Headroom is reported BOTH against the assignment
# (acked/assigned) and against the per-connection cap (spare tokens).
#
# The script also reports any subscription PARTIAL/TERMINAL/REJECTED events
# (real evidence of tightness).
#
# If you later run OTel/Prometheus, this can read bridge.slot.capacity_used_percent
# directly instead — but the log-derived version works today with zero deps.
#
# Usage:  ./soak-headroom.sh [log_file]
#   e.g.   ./soak-headroom.sh                 # default code/logs/ingestion.json
#          ./soak-headroom.sh logs/ingestion-2026-08-03.log

set -euo pipefail

# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
LOG_FILE="${1:-$PROJECT_ROOT/code/logs/ingestion.json}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"

# The journal message format (IngestionService.java:807):
#   bridge lifecycle event=subscription_ack slot=... state=ACTIVE epoch=1
#   assigned=1024 acknowledged=1024 rejected=0 ...
# The regex tolerates the epoch= token between state= and assigned= (R-018).
ACK_LINE_PATTERN='bridge lifecycle event=subscription_ack'
ACK_TOKENS_PATTERN='state=[A-Z]+ (epoch=[0-9]+ )?assigned=[0-9]+ acknowledged=[0-9]+ rejected=[0-9]+'

[ -f "$LOG_FILE" ] || { echo "FATAL: no journal at $LOG_FILE" >&2; exit 1; }

mkdir -p "$OUT_DIR"
SUMMARY="$OUT_DIR/headroom-summary-$(date +%Y%m%d-%H%M%S).txt"
{
echo "headroom: scanning $LOG_FILE"
echo "---"
echo "Subscription acks (ACTIVE/PARTIAL/TERMINAL):"
grep -E "$ACK_LINE_PATTERN" "$LOG_FILE" \
	| grep -oE "$ACK_TOKENS_PATTERN" \
	| sort | uniq -c || true

echo "---"
echo "Any partial/terminal/rejected events (tightness evidence):"
grep -E "$ACK_LINE_PATTERN" "$LOG_FILE" \
	| grep -vE 'state=ACTIVE (epoch=[0-9]+ )?rejected=0' || echo "  (none — all acks full & clean)"

echo "---"
echo "Headroom (capacity_used = acknowledged/assigned; also vs the per-connection cap MaxHFTTokensPerConnection=1024, see subscription_plan.go):"
CAP="${CAP_TOKENS:-1024}"
awk -v cap="$CAP" -v ackpat="$ACK_LINE_PATTERN" '
$0 ~ ackpat {
	for (i=1; i<=NF; i++) {
		if ($i ~ /^assigned=/) { split($i,a,"="); assigned=a[2] }
		if ($i ~ /^acknowledged=/) { split($i,a,"="); acked=a[2] }
	}
	if (assigned > 0) {
		used = (acked*100)/assigned;   # R-236: capacity used of the assignment
		max = (max<used)?used:max;
		if (used < min_used || min_used=="") min_used=used;
		n++;
		sum += used;
		vals[n]=used;
		# Cap-side view for the spare-token line.
		used_cap = (acked*100)/cap;
		max_cap = (max_cap<used_cap)?used_cap:max_cap;
	}
}
END {
	if (n==0) { print "  (no subscription_ack rows found)"; exit }
	for (i=1; i<=n; i++) for (j=i+1; j<=n; j++) if (vals[j]<vals[i]) { t=vals[i]; vals[i]=vals[j]; vals[j]=t }
	# R-174: nearest-rank p99 (not the max).
	idx = int(n*0.99 + 0.5); if (idx < 1) idx = 1; if (idx > n) idx = n;
	printf "  samples=%d  min=%.1f%%  max=%.1f%%  p99=%.1f%%  avg=%.1f%%  (of assignment)\n", n, min_used, max, vals[idx], sum/n
	if (max_cap > 0) printf "  vs per-connection cap: max used=%.1f%%  (%.0f spare tokens of %d)\n", max_cap, cap - (cap*max_cap)/100, cap
	headroom = 100-max
	if (headroom <= 0) printf "  ⚠️ AT CAPACITY (%.1f%%) — 0 headroom on this connection; adding instruments requires a 2nd slot/connection (multi-connection approval, plan 816)\n", max
	else printf "  headroom=%.1f%%\n", headroom
}' <(grep -E "$ACK_LINE_PATTERN" "$LOG_FILE")

echo "---"
echo "Run this periodically during the day; pass = capacity stays below 100% of the assignment and the 1024-token per-connection cap."
} | tee "$SUMMARY"
echo "headroom: summary → $SUMMARY"
