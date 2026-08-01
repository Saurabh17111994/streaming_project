#!/usr/bin/env bash
# soak-headroom.sh — subscription headroom evidence for the whole session.
#
# Proves plan §1379: "subscription headroom is observable and alerted." It
# reads the live ingestion log and derives headroom from the slot lifecycle:
#   capacity_used = acknowledged_tokens / assigned_tokens (per subscription_ack)
#   headroom      = 1 - capacity_used  (0 = fully subscribed, no room)
#
# On the 1-slot / 1024-instrument envelope, a full ack is 1024/1024 = 100%
# used — but the LIMIT for one connection is MaxHFTTokensPerConnection
# (see subscription_plan.go). Headroom here means: how close are we to the
# broker's per-connection cap. The script also reports any subscription
# PARTIAL/TERMINAL/REJECTED events (real evidence of tightness).
#
# If you later run OTel/Prometheus, this can read bridge.slot.capacity_used_percent
# directly instead — but the log-derived version works today with zero deps.
#
# Usage:  ./soak-headroom.sh [log_file]
#   e.g.   ./soak-headroom.sh                 # default logs/ingestion.log
#          ./soak-headroom.sh logs/ingestion-2026-08-03.log

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ROOT="${PROJECT_ROOT:-/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project}"
LOG_FILE="${1:-$PROJECT_ROOT/logs/ingestion.log}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"

[ -f "$LOG_FILE" ] || { echo "FATAL: no log at $LOG_FILE" >&2; exit 1; }

echo "headroom: scanning $LOG_FILE"
echo "---"
echo "Subscription acks (ACTIVE/PARTIAL/TERMINAL):"
grep -E 'subscription_ack' "$LOG_FILE" | grep -oE 'state=[A-Z]+ assigned=[0-9]+ acknowledged=[0-9]+ rejected=[0-9]+' | sort | uniq -c || true

echo "---"
echo "Any partial/terminal/rejected events (tightness evidence):"
grep -E 'subscription_ack' "$LOG_FILE" | grep -vE 'state=ACTIVE.*rejected=0' || echo "  (none — all acks full & clean)"

echo "---"
echo "Headroom vs the per-connection cap (MaxHFTTokensPerConnection=1024, see subscription_plan.go):"
CAP="${CAP_TOKENS:-1024}"
awk -v cap="$CAP" '/subscription_ack/ {
	for (i=1; i<=NF; i++) {
		if ($i ~ /^assigned=/) { split($i,a,"="); assigned=a[2] }
		if ($i ~ /^acknowledged=/) { split($i,a,"="); acked=a[2] }
	}
	if (assigned > 0) {
		used = (acked*100)/cap;
		max = (max<used)?used:max;
		if (used < min_used || min_used=="") min_used=used;
		n++;
		sum += used;
		vals[n]=used;
	}
}
END {
	if (n==0) { print "  (no subscription_ack rows found)"; exit }
	for (i=1; i<=n; i++) for (j=i+1; j<=n; j++) if (vals[j]<vals[i]) { t=vals[i]; vals[i]=vals[j]; vals[j]=t }
	idx = int(n*0.99)+1; if (idx>n) idx=n;
	printf "  samples=%d  min=%.1f%%  max=%.1f%%  p99=%.1f%%  avg=%.1f%%  (cap=%d tokens)\n", n, min_used, max, vals[idx], sum/n, cap
	headroom = 100-max
	if (headroom <= 0) printf "  ⚠️ AT CAPACITY (%.1f%%) — 0 headroom on this connection; adding instruments requires a 2nd slot/connection (multi-connection approval, plan 816)\n", max
	else printf "  headroom=%.1f%%  (%.0f spare tokens on this connection)\n", headroom, (cap*max)/100
}' <(grep -E 'subscription_ack' "$LOG_FILE")

echo "---"
echo "Run this periodically during the day; pass = capacity stays below 100% of the 1024-token per-connection cap."
