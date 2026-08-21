#!/usr/bin/env python3
"""prod_node_check.py — per-VM provisioning gate for the production Swarm topology (D1.2).

Verifies every VM in the target topology (docs/05_deployment/PROD_VM_PROVISIONING.md,
D1.1 of the 2026-08-21 live-readiness plan) against its provisioned reality, using
SSH access constants from an inventory file. **Exits non-zero on any drift** — that is
the gate D1.3/D2 depends on.

Per-Node checks:
  * reachability   — SSH connect (BatchMode; no password prompt ever)
  * disk           — root filesystem size >= disk_min_gb (default 500, imported from
                     PROD_VM_PROVISIONING.md; manager nodes may set a smaller floor)
  * label/role     — Swarm node role + labels match the inventory expectation
                     (role=manager / role=worker / observability=true), and (optional)
                     availability is `drained` for v2 manager-only nodes. Swarm checks
                     run only when the inventory marks the node `swarm: true`.
  * placement rule — the stack must NEVER pin a hostname; this checker only confirms
                     labels because test_09_stack.py enforces no hostname in the stack.

No hostname pinning is introduced here: the inventory is the operator's record of
*which physical host carries which labels*, and the Swarm still places by label only.

Usage:
  python3 prod_node_check.py --inventory prod_vms.json [--out DIR] [--self-check]

  --self-check  run the checker logic against a bundled FAKE inventory with an
                in-process runner (no SSH, no VMs) — proves classification + exit
                codes offline. This is the only runnable mode until D1.3 provisions
                the real VMs.
  --out DIR     write an EvidenceRecord-shape JSON (foundation docs/08_implementation/
                01-foundation.md L159; mirror of audit_r2.py) under DIR.

Stdlib only (subprocess ssh; no paramiko/boto3). Secrets are never printed.
================================================================================="""

import argparse
import datetime as _dt
import json
import os
import socket
import subprocess
import sys
import tempfile

REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
)
EVIDENCE_DIR_DEFAULT = os.path.join(REPO_ROOT, "logs", "nautilus-execution")

DEFAULT_DISK_MIN_GB = 500  # PROD_VM_PROVISIONING.md §1 (workload/observability floor)

# Inventory shape (JSON):
# {
#   "access": {"ssh_user": "ubuntu", "ssh_port": 22, "ssh_key": "/abs/or/omit",
#              "connect_timeout_s": 8},
#   "disk_min_gb": 500,            # optional global default
#   "nodes": [
#     {"name": "M1", "host": "10.0.0.11", "role": "manager", "swarm": true,
#      "labels": {"role": "manager"}, "expect_availability": "drained",  # optional
#      "disk_min_gb": 10},          # optional per-node override
#     {"name": "W1", "host": "10.0.0.21", "role": "worker", "swarm": true,
#      "labels": {"role": "worker"}},
#     {"name": "O1", "host": "10.0.0.40", "role": "observability", "swarm": false,
#      "labels": {"observability": "true"}}
#   ]
# }


class RemoteRunner:
    """SSH-backed command runner. Swap for a fake in tests / --self-check."""

    def __init__(self, access):
        self.access = access

    def run(self, host, command, timeout=20):
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]
        if self.access.get("ssh_port"):
            ssh += ["-p", str(self.access["ssh_port"])]
        if self.access.get("ssh_key"):
            ssh += ["-i", self.access["ssh_key"]]
        timeout_s = self.access.get("connect_timeout_s", 8)
        ssh += ["-o", f"ConnectTimeout={timeout_s}"]
        user = self.access.get("ssh_user", "root")
        try:
            proc = subprocess.run(
                ssh + [f"{user}@{host}", command],
                capture_output=True, text=True, timeout=timeout,
            )
            return proc.returncode, proc.stdout.strip()
        except (subprocess.TimeoutExpired, OSError) as exc:
            return 1, f"runner error: {exc}"


def load_inventory(path):
    with open(path, encoding="utf-8") as fh:
        inv = json.load(fh)
    access = inv.setdefault("access", {})
    default_disk = inv.get("disk_min_gb", DEFAULT_DISK_MIN_GB)
    nodes = inv.get("nodes") or []
    if not nodes:
        raise ValueError("inventory must define at least one node")
    for n in nodes:
        n.setdefault("disk_min_gb", default_disk)
        n.setdefault("labels", {})
        n.setdefault("swarm", True)
        if not n.get("name") or not n.get("host"):
            raise ValueError(f"node missing name/host: {n}")
    return {"access": access, "nodes": nodes}


def _parse_df_gb(stdout):
    """Turn `df -BG --output=size /` output ('500G' or 'Filesystem 500G') into int GB."""
    for token in stdout.replace(",", "").split():
        token = token.strip()
        if token.endswith("G") and token[:-1].isdigit():
            return int(token[:-1])
    return None


def _node_swarm_info(runner, node):
    """Role + availability + labels for node via `docker node ls`/`inspect` on itself."""
    rc, out = runner.run(node["host"], "docker node ls --format '{{.Hostname}}|{{.Role}}|{{.Availability}}'")
    if rc != 0:
        return None, f"docker node ls failed (rc={rc}): {out}"
    row = {}
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) == 3:
            row[parts[0].strip()] = (parts[1].strip(), parts[2].strip())
    if node["host"] not in row and node["name"] not in row:
        return None, f"node {node['name']} absent from swarm membership"
    host = node["host"] if node["host"] in row else node["name"]
    role, availability = row[host]
    rc2, labels_out = runner.run(
        node["host"],
        f"docker node inspect {host} --format '{{{{json .Spec.Labels}}}}'",
    )
    labels = {}
    if rc2 == 0 and labels_out:
        try:
            labels = json.loads(labels_out)
        except json.JSONDecodeError:
            labels = {}
    return {"role": role, "availability": availability, "labels": labels}, None


def check_node(node, runner):
    """Returns (checks, ok) for one node; checks is {name: (PASS|FAIL, detail)}."""
    checks = {}
    # 1. reachability
    rc, out = runner.run(node["host"], "true")
    if rc != 0:
        checks["reachability"] = ("FAIL", f"ssh rc={rc}: {out}")
        return checks, False
    checks["reachability"] = ("PASS", "ssh ok")

    # 2. disk floor
    rc, out = runner.run(node["host"], "df -BG --output=size / | tail -1")
    size_gb = _parse_df_gb(out) if rc == 0 else None
    floor = int(node["disk_min_gb"])
    if size_gb is None:
        checks["disk"] = ("FAIL", f"could not parse df output (rc={rc}): {out!r}")
    elif size_gb < floor:
        checks["disk"] = ("FAIL", f"{size_gb}G < floor {floor}G")
    else:
        checks["disk"] = ("PASS", f"{size_gb}G >= floor {floor}G")

    # 3. swarm role/labels (only when the node is intended to be in the swarm)
    if node.get("swarm"):
        info, err = _node_swarm_info(runner, node)
        if err:
            checks["swarm"] = ("FAIL", err)
        else:
            problems = []
            expect_role = node.get("role")
            if expect_role and info["role"].lower() != expect_role:
                problems.append(f"role {info['role']} != expected {expect_role}")
            for k, v in (node.get("labels") or {}).items():
                if info["labels"].get(k) != v:
                    problems.append(f"label {k}={info['labels'].get(k)!r} != expected {v!r}")
            want_avail = node.get("expect_availability")
            if want_avail and info["availability"].lower() != want_avail:
                problems.append(
                    f"availability {info['availability']} != expected {want_avail}"
                )
            checks["swarm"] = (
                ("PASS", f"role={info['role']} labels={info['labels']}") if not problems
                else ("FAIL", "; ".join(problems))
            )
    else:
        checks["swarm"] = ("PASS", "outside swarm (observability) — label check n/a; "
                                    "stack places by observability=true only if joined")
    ok = all(verdict == "PASS" for verdict, _ in checks.values())
    return checks, ok


def build_evidence(inventory, per_node, run_id, utc_now):
    all_pass = all(ok for _, (_, ok) in per_node)
    checks = {}
    for node, (node_checks, _) in per_node:
        for name, (verdict, detail) in node_checks.items():
            checks[f"{node['name']}:{name}"] = verdict
            checks[f"{node['name']}:{name}_note"] = detail
    return {
        "work_item_id": f"PROD-NODE-CHECK-{run_id}",
        "requirement_ids": ["D1", "09-production-swarm", "02-environments"],
        "artifact": f"logs/nautilus-execution/{run_id}-prod-node-check-evidence.json",
        "version": "prod_node_check.py (stdlib ssh)",
        "environment": "production Swarm target topology (v1 4 -> v2 7 VMs)",
        "workload": "read-only SSH probes: reachability/disk/labels/role; no writes",
        "clock": "UTC",
        "result": "PASS" if all_pass else "FAIL",
        "owner": "Saurabh (DEC-044)",
        "date": utc_now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": checks,
        "limitations": [
            "swarm checks run only on nodes marked swarm:true (O1 observability is "
            "verified for reachability+disk unless it joins the swarm)",
            "label/role verified against docker node inspect on the node itself; "
            "hostname-free placement itself is enforced by test_09_stack.py",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Per-VM production provisioning gate (D1.2)")
    parser.add_argument("--inventory", default="prod_vms.json", help="JSON inventory")
    parser.add_argument("--out", default=EVIDENCE_DIR_DEFAULT,
                        help="evidence output directory (EvidenceRecord JSON)")
    parser.add_argument("--self-check", action="store_true",
                        help="run against the bundled fake inventory + in-process runner")
    args = parser.parse_args(argv)

    utc_now = _dt.datetime.now(_dt.timezone.utc)
    run_id = utc_now.strftime("%Y%m%d-%H%M%S")

    if args.self_check:
        return _self_check(args, utc_now, run_id)

    if not os.path.exists(args.inventory):
        print(f"error: inventory file not found: {args.inventory}", file=sys.stderr)
        print("  (D1.3 provisions the VMs; until then run --self-check)", file=sys.stderr)
        return 2
    inventory = load_inventory(args.inventory)
    runner = RemoteRunner(inventory["access"])
    per_node = []
    for node in inventory["nodes"]:
        node_checks, ok = check_node(node, runner)
        per_node.append((node, (node_checks, ok)))
        for name, (verdict, detail) in node_checks.items():
            print(f"{node['name']:<16} {name:<12} {verdict:<4} {detail}")
    evidence = build_evidence(inventory, per_node, run_id, utc_now)
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, f"{run_id}-prod-node-check-evidence.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"\nevidence: {path}")
    print(f"result: {evidence['result']} — {'all nodes healthy' if evidence['result'] == 'PASS' else 'drift detected, D2 GATE FAILED'}")
    return 0 if evidence["result"] == "PASS" else 1


def _self_check(args, utc_now, run_id):
    """Offline proof of the checker: fake inventory + in-process runner, no SSH/VMs."""

    class FakeRunner:
        def __init__(self, access):
            self.access = access
            self.reachable = {"10.0.0.11", "10.0.0.21", "10.0.0.40"}
            self.disks = {"10.0.0.11": 600, "10.0.0.21": 480, "10.0.0.40": 512}
            self.swarm = {
                "10.0.0.11": ("manager", "drained", {"role": "manager"}),
                "10.0.0.21": ("worker", "active", {"role": "employee"}),  # label drift!
            }

        def run(self, host, command, timeout=20):
            if host not in self.reachable:
                return 1, "Connection refused"
            if command == "true":
                return 0, ""
            if command.startswith("df -BG"):
                return 0, f"{self.disks[host]}G"
            if "docker node ls" in command:
                lines = []
                for h, (role, avail, _) in self.swarm.items():
                    lines.append(f"{h}|{role}|{avail}")
                return 0, "\n".join(lines)
            if "docker node inspect" in command:
                host = command.split("inspect ")[1].split()[0]
                return 0, json.dumps(self.swarm[host][2])
            return 0, ""

    fake_inv = {
        "access": {"ssh_user": "ubuntu", "ssh_port": 22, "connect_timeout_s": 3},
        "nodes": [
            {"name": "M1", "host": "10.0.0.11", "role": "manager", "swarm": True,
             "labels": {"role": "manager"}, "expect_availability": "drained",
             "disk_min_gb": 10},
            {"name": "W1", "host": "10.0.0.21", "role": "worker", "swarm": True,
             "labels": {"role": "worker"}, "disk_min_gb": 500},
            {"name": "O1", "host": "10.0.0.40", "role": "observability", "swarm": False,
             "labels": {"observability": "true"}, "disk_min_gb": 500},
        ],
    }
    runner = FakeRunner(fake_inv["access"])
    per_node = []
    expect_ok = {"M1": True, "W1": False, "O1": True}  # W1 disk 480<500 AND label drift
    for node in fake_inv["nodes"]:
        node_checks, ok = check_node(node, runner)
        per_node.append((node, (node_checks, ok)))
        for name, (verdict, detail) in node_checks.items():
            print(f"[self-check] {node['name']:<6} {name:<12} {verdict:<4} {detail}")
        assert ok == expect_ok[node["name"]], (
            f"self-check classification drift on {node['name']}: expected "
            f"{expect_ok[node['name']]}, got {ok}"
        )
    evidence = build_evidence(fake_inv, per_node, run_id, utc_now)
    assert evidence["result"] == "FAIL"  # W1 is intentionally drifting
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, f"self-check-{run_id}-prod-node-check-evidence.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"\n[self-check] PASS — checker classifies reachable/disk/label/availability "
          f"correctly; evidence at {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
