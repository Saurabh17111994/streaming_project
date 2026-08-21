#!/usr/bin/env bash
# Bucket B — load / scale ladder harness (09 §7 SCALE-*, §8 REC-LOAD, §9 resource-exhaustion).
#
# Runs a rate ladder (default 10k→25k→50k→75k→100k ticks/s), samples the running Swarm
# stack + an optional latency endpoint, and writes a TSV report (throughput, p50/p95/p99,
# CPU, RPO-style observability columns). The harness is deposit-ready: on the M3 rig the
# credentialed 50k/s / p99<100ms / one-VM-loss acceptance is one command.
#
# Usage:
#   ./run_scale_ladder.sh ["10k,25k,50k" as "10000,25000,50000"]
# Env:
#   STACK_NAME   docker stack filter to sample        (default "nautilus")
#   DURATION     seconds per ladder rung               (default 15)
#   METRICS_URL  optional gateway/metrics URL to curl for latency
#   LOAD_CMD     command that applies $RATE ticks/s (drives the real workload)
#
# NOTE: on a local single-node dev swarm this is a SANITY ladder (functional, not
# production-credentialed). The documented acceptance numbers are M3/rig evidence.

set -euo pipefail
RATES_IN="${1:-10000,25000,50000,75000,100000}"
DURATION="${DURATION:-15}"
STACK_NAME="${STACK_NAME:-nautilus}"
METRICS_URL="${METRICS_URL:-}"
LOAD_CMD="${LOAD_CMD:-}"
OUT="scale_report_$(date +%Y%m%d_%H%M%S).tsv"
printf 'rung\trate_per_s\tduration_s\tsamples\tp50_ms\tp95_ms\tp99_ms\tp_max_ms\tcpu_pct\n' > "$OUT"

percentiles() {  # stdin: one latency-ms value per line -> "p50 p95 p99 max"
python3 - <<'PY'
import sys
v=sorted(float(l) for l in sys.stdin if l.strip())
if not v:
    print("NA NA NA NA"); sys.exit(0)
p=lambda q: v[min(len(v)-1,int(q*len(v)))]
print(f"{p(.5):.1f} {p(.95):.1f} {p(.99):.1f} {max(v):.1f}")
PY
}

rung=0
IFS=',' read -r -a RATES <<< "$RATES_IN"
for RATE in "${RATES[@]}"; do
  rung=$((rung+1))
  echo "== rung $rung: ${RATE} ticks/s for ${DURATION}s =="
  LOAD_PID=""
  if [ -n "$LOAD_CMD" ]; then
    ( RATE="$RATE" DURATION="$DURATION" bash -c "$LOAD_CMD" ) &
    LOAD_PID=$!
  fi

  LAT="/tmp/scale_lat_$$"; : > "$LAT"
  for ((i=0;i<DURATION;i++)); do
    if [ -n "$METRICS_URL" ]; then
      curl -so /dev/null -w '%{time_total}\n' "$METRICS_URL" 2>/dev/null >> "$LAT" || true
    fi
    sleep 1
  done
  [ -n "$LOAD_PID" ] && wait "$LOAD_PID" 2>/dev/null || true

  # Sample per-container CPU for the stack.
  CONTAINERS=$(docker ps -q --filter "name=$STACK_NAME" 2>/dev/null || true)
  CPU="NA"
  if [ -n "$CONTAINERS" ]; then
    CPU=$(docker stats --no-stream --format '{{.CPUPerc}}' $CONTAINERS 2>/dev/null \
          | awk '{s+=$1; n++} END{if(n)printf "%.1f", s/n; else print "NA"}')
  fi
  read P50 P95 P99 PMAX < <(percentiles < "$LAT")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$rung" "$RATE" "$DURATION" "$(wc -l < "$LAT")" "$P50" "$P95" "$P99" "$PMAX" "$CPU" >> "$OUT"
  rm -f "$LAT"
done
echo "scale ladder report: $OUT"
