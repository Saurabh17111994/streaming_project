# D2 — Swarm bootstrap + stack self-check (first half) (2026-08-21)

Master-plan Task D2.1 first half — DoD (half): stack-selfcheck passes; real deploy waits D1.3 (human VMs).

**What was proven**

- `make stack-selfcheck` (single-node mimic): real daemon rc=0; node labelled `role=worker` + `observability=true`; `docker stack config` compiles `docker-stack.yml`; `make test-09` **25/25**.
- No hostname-pinned placement; role-label placement verified statically.

**Disposition**

Single-node mimic PASS. Second half (`make stack-config DEPLOY=1` on the real swarm) waits D1.3 human VM provisioning. D2.2/D2.3 remain pending (verified on deploy).

**Evidence**

- Change record: CHG-085.
- Files: `code/01_platform/04_scripts/prod_node_check.py`, `docker-stack.yml`.
