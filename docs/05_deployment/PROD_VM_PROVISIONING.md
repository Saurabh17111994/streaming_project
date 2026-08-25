# PROD VM Provisioning — Swarm target topology (v1 4 → v2 7 VMs)

- **Plan task:** `D1.1` — VM provisioning + agent-verifiable checklist
  (`docs/plans/2026-08-25-live-readiness-unified-plan.md`, Phase U5)
- **Authoritative references this doc must stay consistent with:**
  `docs/08_implementation/09-production-swarm.md`, `docs/05_deployment/02-environments.md`,
  `code/01_platform/01_docker/docker-stack.yml`, `docs/06_operations/04-dr-plan.md`,
  `docs/05_deployment/06-swarm-secrets.md`.
- **Status at issue (2026-08-21):** D1.1/D1.2 authored; **D1.3 (human provisions the VMs)
  is OPEN** — this is the checklist the operator follows, and `prod_node_check.py` (D1.2)
  is the verification gate that must pass before D2.

## 1. Target topology (v2 — 7 VMs, the production target)

| VM | Role | Swarm role | Node labels required | Notes |
|---|---|---|---|---|
| `M1` | Swarm manager #1 | `manager` (Active) | `role=manager` | Raft quorum 2/3; v2 → `drain` |
| `M2` | Swarm manager #2 | `manager` (Active) | `role=manager` | v2 → `drain` |
| `M3` | Swarm manager #3 | `manager` (Active) | `role=manager` | v2 → `drain` |
| `W1` | Worker | `worker` | `role=worker` | Workloads run here |
| `W2` | Worker | `worker` | `role=worker` | |
| `W3` | Worker | `worker` | `role=worker` | |
| `W4+` | Worker (scale, optional) | `worker` | `role=worker` | joins with NO stack redesign |
| `O1` | Observability | outside Swarm | `observability=true` | OpenObserve + telemetry; must NOT be inside the manager quorum |

**Disk:** 500 GB SSD per VM (workload VMs and the observability VM). Managers in v2 are
small-footprint (≈10 GB disk, 2 CPU / 2 GB RAM per `09-production-swarm.md` §v2) — treat
500 GB as the workload/observability floor, not a manager requirement.

**v1 baseline (ship-now, 4 VMs)** — same stack, different labels: `M1 M2 M3` are
Manager+Worker (labels `role=worker` AND `role=manager`, `docker node update
--availability drain` never applied) and `O1` outside Swarm. Per the DECISION 2026-08-20
in `docker-stack.yml`, adopt v2 only on a trigger: `N>6` workers, sustained CPU >80%, or
Raft election flaps.

## 2. Hard rules (fail = provisioning defect)

1. **No hostname pinning anywhere.** The stack places by `node.labels.role == worker` and
   `node.labels.observability == true` only. Never edit `docker-stack.yml` to name a host;
   W4+ joins by labeling, not by stack rewrite (`test_09_stack.py` enforces this).
2. **Manager quorum is 3** (tolerant of 1 loss). O1 (observability) is outside the swarm;
   its loss must never authorize orders or erase the durable audit.
3. **Anti-co-location of critical replicas:** Fluss replicas, ZooKeeper ensemble members
   (3-node, quorum 2-of-3) and Flink HA (JobManager leader) must land across SEPARATE
   workload VMs (`02-environments.md` §Workload VMs).
4. **Encrypted overlays** for `trading-net`/`execution-net`; Swarm secrets `external: true`
   (`06-swarm-secrets.md`); per-node durable volumes declared, never hostname-bound.
5. **No stray host ports** for the execution/gateway/bridge trio (private topology, T8).

## 3. Provisioning steps (operator, D1.3)

1. Create the VMs (cloud provider of choice) with 500 GB SSD on workload/observability
   nodes; record their addresses in the inventory file (see §4).
2. Install Docker Engine + enable Swarm:
   - `M1`: `docker swarm init --advertise-addr <M1-ip>`
   - `M2`, `M3`: `docker swarm join --token <manager-token> <M1-ip>:2377`
   - `W1..W3`: `docker swarm join --token <worker-token> <M1-ip>:2377`
3. Apply the labels (never hostnames downstream):
   ```sh
   for n in m1 m2 m3; do docker node update --label-add role=manager "$n"; done
   for n in w1 w2 w3;  do docker node update --label-add role=worker "$n"; done
   docker node update --label-add observability=true o1   # if O1 joins (else leave out)
   ```
4. **v2 managers drain:** `docker node update --availability drain m1 m2 m3`.
5. `docker stack deploy -c docker-stack.yml trading` (after D1.2 gate passes — see below).

## 4. Verification gate (D1.2 — `prod_node_check.py`)

`code/01_platform/04_scripts/prod_node_check.py` verifies per-VM **disk / label / role**
from an inventory file (SSH or a cloud-API access profile), and **exits non-zero on drift**.

- Inventory: `--inventory <JSON>` (schema documented in the script header and in a bundled
  example `prod_vms.example.json`).
- Run now offline: `--self-check` proves the checker logic without VMs (no provisioning
  needed — the only runnable mode until D1.3).
- GA preflight (after D1.3): `python3 prod_node_check.py --inventory prod_vms.json --out
  logs/nautilus-execution/` — **must exit 0 before D2** (swarm bootstrap).

## 5. Honest sizing note

The final service-to-node placement, CPU/RAM, SSD IOPS/throughput and network bandwidth
are **`EVIDENCE-BLOCKED`** until `PERF-PROD-60000-001` and `FAIL-VM-LOSS-60000-001` pass
on the real stack (D5/D4). 500 GB SSD per VM is a starting allocation, not a proven sizing
result (`09-production-swarm.md`).
