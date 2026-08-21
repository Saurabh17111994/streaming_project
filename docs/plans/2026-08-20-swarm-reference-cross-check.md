# Swarm Reference Architecture — Cross-Check

**Date:** 2026-08-20
**Mode:** Read-only (no project changes — as requested)
**Reference:** `Reference Architecture Context — Production Docker Swarm HA` (4 VMs: VM1-3 = Manager+Worker, VM4 = Observability; Raft quorum 2-of-3)
**Project docs checked:** `docs/08_implementation/09-production-swarm.md`, `08-local-compose.md`, `11-testing-and-release.md`, `00-start-here.md`, `code/01_platform/01_docker/docker-compose.yml`, `docker-stack.yml` (missing — Option B decided 2026-08-20)

## Instruction

Compare existing project specification/documentation against the reference. Do NOT modify. Classify every finding as MATCH / PARTIAL MATCH / CONFLICT / MISSING / UNCLEAR. For CONFLICT/PARTIAL MATCH quote the project document and explain the difference.

No analogies used (per user style).

---

## 1) What topology does the existing project currently specify?

**Verdict: PARTIAL MATCH**

- Project: `09-production-swarm.md § Placement model` — 4 VMs: W1/W2/W3 = Fluss replica/quorum + ZK ensemble member 1/3 + Flink capacity (JM/TM) + assigned services, 500GB SSD each; Observability VM = OpenObserve + telemetry storage. `§ Status` Topology = Four VMs: three workload/HA, one observability.
- Reference: VM1-3 = Manager+Worker + workload containers, VM4 = Observability.
- **Match:** 4 VMs, 3+1 split, roles by VM, disk size.
- **Partial:** Project frames topology via Fluss/ZK/Flink roles; Reference frames via Swarm Manager+Worker roles. Same VM count, different vocabulary. No conflict, but Swarm role framing not yet written in 09.

---

## 2) How many Swarm managers does it specify?

**Verdict: UNCLEAR / MISSING**

- Project defines **3-node ZK ensemble (quorum 2-of-3)** and **Flink HA via ZooKeeper** (`09 § Placement model`, `§ Storage and recovery`, `§ Readiness sequence` steps 2–3, `11-testing § SWARM-FAIL-001`). No explicit section `Swarm Manager Design` stating `3 Swarm managers (Raft)`.
- `code/01_platform/01_docker/docker-compose.yml` header: `Production topology lives in 09` — Compose only.
- `docker-stack.yml` (Option B path `code/01_platform/01_docker/docker-stack.yml`) was decided 2026-08-20 but file does not exist (`ls` → no such file).
- Reference requires: 3 Swarm managers (Raft).
- **Finding:** Project proves HA via ZK/Fluss/Flink, not via Swarm Raft. Count is implied (3 HA nodes) but not pinned as Swarm managers.

---

## 3) How many Swarm workers does it specify?

**Verdict: PARTIAL MATCH**

- Project: `09 § Placement model` `assigned services` + `Flink capacity` on W1/W2/W3; `§ Stack requirements` `All three replicas ... SHALL be placed across separate workload VMs via anti-co-location constraints`.
- Reference: 3 workers (VM1-3).
- **Partial:** Intent is 3 executors; not expressed as Swarm Worker count. No `deploy.placement` with 3 replicas yet implemented.

---

## 4) Are managers also workers?

**Verdict: UNCLEAR**

- Project never states `Manager+Worker` co-location. It says each workload VM hosts both state (ZK, Fluss) and compute (Flink JM/TM, assigned services), which implies co-location if mapped to Swarm, but not declared as Swarm managers participating as workers.
- `09 § Option B` (2026-08-20) says local mimic = `docker swarm init` + `docker stack deploy prod on 1 host` — implies manager+worker on same host, but not committed as `VM1-3 = Manager+Worker`.
- Reference requires: VM1=Manager+Worker ×3.
- **Finding:** Architecture compatible, but not explicitly documented.

---

## 5) Is there a dedicated manager VM?

**Verdict: MATCH** (desired direction)

- Project has **no** single dedicated manager. Placement allocates per workload VM (3×), not `VM4 = single Manager`.
- `docker-compose.yml` avoids anti-pattern; no doc proposes `VM4 = Manager`.
- Reference forbids: `VM1=Worker, VM2=Worker, VM3=Worker, VM4=single Manager`.
- **Finding:** No conflict — project avoids the SPOF pattern.

---

## 6) Is there a dedicated observability VM?

**Verdict: MATCH**

- `09 § Placement model` row: `Observability VM | OpenObserve and telemetry storage/collection | 500 GB SSD`.
- Quote: `OpenObserve loss must not authorize orders or erase local durable audit.`
- Reference: VM4 dedicated to OpenObserve.
- **Finding:** Identical.

---

## 7) Does the existing design provide Swarm control-plane HA?

**Verdict: PARTIAL MATCH**

- Project quorum math: `3-node ZK ensemble (quorum 2-of-3) ... survive loss of any single workload VM` (`09 § Placement model`); `Readiness sequence` step 2 `Verify ZooKeeper ensemble quorum (2-of-3), then Fluss quorum`; `11-testing § SWARM-FAIL-001` tests `ZooKeeper quorum 2-of-3 maintained with leader re-election`.
- Reference quorum math: `3 managers, quorum 2; 2 remain after 1 loss` (Raft).
- **Match:** Same 3→2→quorum reasoning, same failure bound.
- **Partial:** HA proved via ZK/Fluss/Flink HA, not via Swarm Raft `docker node ls` quorum. Swarm HA is assumed equivalent, not evidenced.

---

## 8) Does it provide workload HA?

**Verdict: MATCH**

- `09 § Stack requirements` `Placement constraints and anti-co-location`, `Resource reservations/limits, Health checks, Restart/update/rollback`, `All three replicas ... SHALL be placed across separate workload VMs`.
- `09 § Capacity acceptance` `One workload VM loss at 50,000 ticks/s variable average baseline`.
- `11-testing § SWARM-FAIL-001` (one VM loss) + `PERF-NODELOSS-001` + `OPS-FAIL-002` (checkpoint/backlog).
- Reference requires: replicas across VM1-3, health checks, restart policies.
- **Finding:** Matches.

---

## 9) What are the current single points of failure?

**Verdict: PARTIAL MATCH**

- **Local (08):** Explicit SPOFs flagged as intentional and non-HA: `08 § Local topology` `ZooKeeper (single node — dev simplification; production = 3-node ensemble)`, `Local volumes ... cannot prove replication`, `09 § Bootstrap` `Single node (no quorum) ... NOT HA evidence` — all gated as `Prohibited use: Production HA evidence`.
- **Production intent (09):** No claimed SPOF — 3× replication, N+1 budget, S3 durable, fencing `one active owner per execution_partition_id`.
- **Partial:** Swarm control-plane SPOF not yet proven absent. If `docker-stack.yml` were deployed as `VM4 = single Manager`, ZK checks would still pass while Swarm would be SPOF. `SWARM-INT-001` checks `no replica co-location` but not `manager count =3`.

---

## 10) Does the local development/testing topology accurately reproduce production?

**Verdict: PARTIAL MATCH**

- Reference wants: `PC → VM1-3 Manager+Worker + VM4 O2` to validate topology, quorum, placement, failures.
- Project local: `08 § Local topology` `may run one dev instance of: ZK single, Fluss, Flink JM/TM, ingestion, Nautilus, O2`; plus `09 § Option B` `Local mimic = docker swarm init + docker stack deploy prod on 1 host`.
- Quote: `Local volumes and one-node services are intentional development simplifications. They cannot prove replication, one-VM tolerance ...` (`08 § Local topology`); `The single-VM bootstrap SHALL NOT be cited as quorum evidence` (`09 § Bootstrap`).
- **Finding:** Local reproduces contracts and readiness sequence, not 3-manager quorum or VM loss / partition. Matches reference note `local VMs need NOT be prod-sized but must validate topology` — currently at 1-host level, 4-VM local is planned but not built (stack file missing).

---

## 11) What failure scenarios are already covered?

**Verdict: PARTIAL MATCH**

- Reference 12 scenarios.
- **Covered (5/12):** 1–2 `One/Any manager VM fails` → `SWARM-FAIL-001` + `PERF-NODELOSS-001`; 3–4 `container/service unhealthy` → `OPS-FAIL-002`, `SWARM-INT-002`; 10 `O2 fails` → `OPS-FAIL-001` (`OpenObserve outage cannot authorize orders`); 12 `recover after return` implicit in `SWARM-FAIL-001` `Fluss quorum/restore`.
- **Missing as explicit Swarm-level tests:** 5 `failed VM returns`, 6 `network partition to one VM`, 7 `rolling deploy while one VM unavailable`, 9 `replicas concentrated on one node` (lint `SWARM-INT-001 no co-location` exists, no fault-injection test), 11 `manager quorum lost (2 fail → 1 remains)` not named as separate case (ZK quorum loss implied).

---

## 12) Which HA requirements are missing?

**Verdict: MISSING**

- Explicit Swarm Raft spec `VM1-3 = Manager+Worker, Raft quorum 2` — not written as Swarm terms (only as ZK quorum).
- `docker-stack.yml` with `deploy: placement constraints` + `replicas:3` + manager quorum — file missing.
- Network partition / split-brain test (scenario 6), rolling deploy under degraded quorum (7), concentration detection (9), 2-VM loss → quorum lost (11) as separate cases.
- VM4 failure impact on control plane as Swarm membership test (OPS-FAIL-001 proves `O2 loss ≠ authorize`, not `Swarm stays up if O2 down`).

---

## 13) Which existing requirements conflict?

**Verdict: NO CONFLICT**

- No direct conflict. Project rejects `VM4 = single Manager` anti-pattern by omission, mandates `Fluss/ZK cannot co-locate, exactly one ZK per workload VM`, `encrypted overlay/TLS mandatory`, `S3 encrypted/versioned`, `fencing one active owner per execution_partition_id`. These strengthen reference.
- Tension is only terminology: 09 emphasizes `ZK 3-ensemble` where reference emphasizes `Swarm 3-manager`. Both need 3 nodes quorum 2 — complementary layers (orchestrator vs state store), correctly treated as separate concerns (`11-testing` lists `SWARM-*, SEC-*, PERF-NODELOSS` separately).

---

## 14) Which parts are equivalent even if expressed differently?

**Verdict: MATCH**

- `4 VMs (3+1 O2)` + `500GB` + `W1-3 = Fluss/ZK/Flink` ≡ `VM1-3 = Manager+Worker, VM4 = Observability`.
- `3-node ZK quorum 2-of-3 survives 1 loss` ≡ `3-manager Raft quorum 2 survives 1 loss` (same fault math, different layer).
- `anti-co-location, replicas across W1-3` ≡ Swarm `placement constraints, spread replicas`.
- `PERF-NODELOSS-001: 50k ticks/s + 1 VM loss` ≡ `tolerate any single VM failure` + capacity intent.
- `OPS-FAIL-001: O2 loss cannot authorize` ≡ `Observability should not be dependency`.
- `08 Bootstrap 1 VM = NOT HA evidence / Target 3 VM = HA` ≡ `local VMs need not be prod-sized but must validate topology; prod-sized VMs still needed for perf`.
- Single branch Option B (`docker-compose.yml` for 08, `docker-stack.yml` for 09) ≡ reference `local VMs simulate production topology on PC`.

---

## Overall verdict

**PARTIAL MATCH — no conflicts, conceptually aligned, Swarm control-plane still informal.**

- **Right:** VM count, 3+1 split, 3 replicas, quorum 2-of-3, N+1, fencing, S3, O2 not-a-dependency, local ≠ prod evidence, most failure scenarios covered at Flink/Fluss/O2 layer.
- **To close before shipping docker-stack.yml:** Add explicit `Swarm Manager Design` section in `09` stating `VM1,2,3 = Swarm Manager+Worker (Raft), VM4 NOT a manager`, and add Swarm-level tests for manager quorum (`docker node ls`), network partition, rolling deploy under degraded quorum, replica concentration, and 2-manager loss. Today those guarantees live as ZK HA — correct for data plane, but Swarm HA is the orchestrator layer the reference isolates as (1) Swarm HA vs (2) Workload HA vs (3) Data durability.
- **Risk if not fixed:** Deploy could pass `ZK quorum` checks while using `VM4 = single Manager` — ZK would still survive 1 loss, but Swarm control plane would be SPOF (the anti-pattern the reference forbids). Adding the 3-manager line and 4-VM local mimic (`PC → VM1-3 Manager+Worker + VM4 O2`, e.g. multipass or `docker swarm` single-host mimic per Option B) closes gap without code change.

---

## Files referenced

- `docs/08_implementation/09-production-swarm.md` §§ Placement model, Stack requirements, Readiness sequence, Bootstrap and scaling, Storage and recovery, § Repository and branching model — Option B (2026-08-20)
- `docs/08_implementation/08-local-compose.md` §§ Local topology, Runtime contracts
- `docs/08_implementation/11-testing-and-release.md` §§ Production Swarm, Observability and operations
- `code/01_platform/01_docker/docker-compose.yml` (header: Production topology lives in 09)
- Missing: `code/01_platform/01_docker/docker-stack.yml`
