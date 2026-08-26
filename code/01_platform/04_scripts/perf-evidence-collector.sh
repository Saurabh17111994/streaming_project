#!/usr/bin/env bash
# =============================================================================
# perf-evidence-collector.sh — evidence capture for PERF-AUDIT-001 (the
# controlled measurement run). Runs ALONGSIDE bench-throughput.sh (load gen)
# and snapshots the audit's 7 evidence categories every INTERVAL seconds:
#   1. Flink per-subtask busy/backpressure/idle + records in/out (aggregated)
#   2. Checkpoint metrics (size, durations, counts)
#   3. Memory split (docker stats per container)
#   4. (GAP) Fluss client internals — documented gap, no clean source
#   5. (GAP) Fluss server internals — documented gap
#   6. Data distribution (per-vertex records-in)
#   7. Latency (O2 append_latency_ms count/sum; p99 fallback = mean)
#
# Usage: ./perf-evidence-collector.sh <out-dir> [interval-seconds]
#   out-dir: where snapshots land (logs/tracker-14/perf-audit-001-<ts>/)
#   interval: default 30
#
# Evidence: <out-dir>/snapshots/snap-<n>.json + <out-dir>/summary.tsv
# =============================================================================
set -euo pipefail

OUT_DIR="${1:?usage: perf-evidence-collector.sh <out-dir> [interval]}"
INTERVAL="${2:-30}"
SNAP_DIR="$OUT_DIR/snapshots"
mkdir -p "$SNAP_DIR"

JEXEC() { docker exec 01_docker-flink-jobmanager-1 curl -s "$@"; }

job_id() {
	JEXEC http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for j in d['jobs']:
    if j['state']=='RUNNING' and 'signal' in j['name'].lower():
        print(j['jid']); break
" 2>/dev/null
}

snapshot() {
	local n="$1"
	local jid="$2"
	local out="$SNAP_DIR/snap-$n.json"
	local vertices_json="[]"
	local metrics_json="{}"
	local ckpt_json="{}"
	local stats_json="[]"

	if [ -n "$jid" ]; then
		# Vertices (names + parallelism)
		vertices_json=$(JEXEC "http://localhost:8081/jobs/$jid" 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    out=[{'name': v['name'], 'par': v.get('parallelism'), 'id': v['id']} for v in d.get('vertices',[])]
    print(json.dumps(out))
except: print('[]')
" 2>/dev/null)
		# Aggregated per-vertex metrics (busy/backpressure/records) — query each vertex
		metrics_json=$(python3 - "$jid" << 'PYEOF'
import json, sys, subprocess
jid = sys.argv[1]
def jexec(path):
    r = subprocess.run(['docker','exec','01_docker-flink-jobmanager-1','curl','-s',f'http://localhost:8081{path}'],
                       capture_output=True, text=True)
    try: return json.loads(r.stdout)
    except: return None
job = jexec(f'/jobs/{jid}')
if not job: print('{}'); sys.exit()
out = {}
for v in job.get('vertices', []):
    m = jexec(f"/jobs/{jid}/vertices/{v['id']}/subtasks/metrics?get=busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond")
    if m:
        # m is a list of per-subtask entries: [{"id": metric, "min":..,"max":..,"avg":..}, ...]
        # aggregate: average of 'avg' across subtasks, plus the sum
        agg = {}
        for x in m:
            mid = x.get('id')
            if mid and x.get('avg') is not None:
                agg.setdefault(mid, []).append(x['avg'])
        out[v['name']] = {k: {'avg_of_subtask_avgs': round(sum(vals)/len(vals),1), 'n_subtasks': len(vals)} for k, vals in agg.items()}
print(json.dumps(out))
PYEOF
)
		# Checkpoints
		ckpt_json=$(JEXEC "http://localhost:8081/jobs/$jid/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    print(json.dumps({'counts': d.get('counts',{}), 'latest_completed': d.get('latest',{}).get('completed',{})}))
except: print('{}')
" 2>/dev/null)
	fi

	# Docker stats
	stats_json=$(docker stats --no-stream --format '{{.Name}}|{{.MemUsage}}|{{.CPUPerc}}' 2>/dev/null | grep -E "flink|fluss|ingestion" | python3 -c "
import json,sys
rows=[]
for line in sys.stdin:
    p=line.strip().split('|')
    rows.append({'name': p[0], 'mem': p[1], 'cpu': p[2] if len(p)>2 else ''})
print(json.dumps(rows))
" 2>/dev/null)

	# Write valid JSON
	python3 - "$out" "$n" "$vertices_json" "$metrics_json" "$ckpt_json" "$stats_json" << 'PYEOF'
import json, sys
out, n = sys.argv[1], int(sys.argv[2])
try: verts = json.loads(sys.argv[3])
except: verts = []
try: metrics = json.loads(sys.argv[4])
except: metrics = {}
try: ckpt = json.loads(sys.argv[5])
except: ckpt = {}
try: stats = json.loads(sys.argv[6])
except: stats = []
doc = {
  "snapshot": n,
  "ts": __import__('datetime').datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
  "vertices": verts,
  "per_vertex_metrics": metrics,
  "checkpoints": ckpt,
  "docker_stats": stats
}
json.dump(doc, open(out, 'w'), indent=1)
PYEOF
}

# ── Main loop ────────────────────────────────────────────────────────────────
echo "perf-evidence: capturing every ${INTERVAL}s → $SNAP_DIR"
{
	echo -e "snap\tts\tckpt_completed\tckpt_latest_size\tckpt_duration_ms\tflink_mem\tflink_cpu\tingestion_mem"
} > "$OUT_DIR/summary.tsv"

n=0
while true; do
	n=$((n+1))
	JID=$(job_id)
	snapshot "$n" "$JID"
	# Compact summary row
	python3 - "$n" "$OUT_DIR" << 'PYEOF'
import json, sys, os
n = sys.argv[1]; out_dir = sys.argv[2]
try:
    d = json.load(open(f"{out_dir}/snapshots/snap-{n}.json"))
    ck = d.get('checkpoints', {})
    comp = ck.get('counts', {}).get('completed', '')
    lc = ck.get('latest_completed', {})
    size = lc.get('checkpointed_size', '')
    dur = lc.get('end_to_end_duration', '')
    flink_mem = flink_cpu = ing_mem = ''
    for s in d.get('docker_stats', []):
        if 'taskmanager' in s['name']: flink_mem = s['mem']; flink_cpu = s['cpu']
        if 'ingestion' in s['name']: ing_mem = s['mem']
    with open(f"{out_dir}/summary.tsv", 'a') as f:
        f.write(f"{n}\t{d.get('ts','')}\t{comp}\t{size}\t{dur}\t{flink_mem}\t{flink_cpu}\t{ing_mem}\n")
except Exception as e:
    with open(f"{out_dir}/summary.tsv", 'a') as f:
        f.write(f"{n}\tERROR {e}\n")
PYEOF
	sleep "$INTERVAL"
done
