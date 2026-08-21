"""D1.2 — offline tests for prod_node_check.py (no SSH, no VMs needed).

These exercise the checker's classification logic against a fake inventory +
in-process runner, proving reachability/disk/label/availability classification and
the non-zero-drift exit contract before any real VM exists (D1.3 is human-gated).
"""

import json
import os
import subprocess
import sys
import tempfile

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS)
import prod_node_check as pnc  # noqa: E402


class FakeRunner(pnc.RemoteRunner):
    """Deterministic stand-in; reachable hosts + canned df/docker outputs."""

    def __init__(self, access):
        super().__init__(access)
        self.reachable = {"10.0.0.11", "10.0.0.21", "10.0.0.40", "10.0.0.41"}
        self.disks = {"10.0.0.11": 600, "10.0.0.21": 480, "10.0.0.40": 512, "10.0.0.41": 520}
        self.swarm = {
            "10.0.0.11": ("manager", "drained", {"role": "manager"}),
            "10.0.0.21": ("worker", "active", {"role": "employee"}),  # label drift
            "10.0.0.41": ("worker", "active", {"role": "worker"}),
        }

    def run(self, host, command, timeout=20):
        if host not in self.reachable:
            return 1, "Connection refused"
        if command == "true":
            return 0, ""
        if command.startswith("df -BG"):
            return 0, f"{self.disks[host]}G"
        if "docker node ls" in command:
            return 0, "\n".join(f"{h}|{r}|{a}" for h, (r, a, _) in self.swarm.items())
        if "docker node inspect" in command:
            host = command.split("inspect ")[1].split()[0]
            return 0, json.dumps(self.swarm[host][2])
        return 0, ""


def _inv(access=None):
    return {
        "access": access or {"ssh_user": "ubuntu", "ssh_port": 22, "connect_timeout_s": 3},
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


def test_healthy_manager_passes():
    node = _inv()["nodes"][0]
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is True
    assert checks["disk"][0] == "PASS"
    assert checks["swarm"][0] == "PASS"
    assert "10.0.0.11" not in [c[1] for c in checks.values()]


def test_worker_small_disk_fails_with_reason():
    node = _inv()["nodes"][1]
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is False
    assert checks["disk"] == ("FAIL", "480G < floor 500G")


def test_worker_label_drift_fails():
    # W1's host is in the swarm with role=employee instead of role=worker.
    node = _inv()["nodes"][1]
    node["disk_min_gb"] = 400  # neutralize the disk failure, keep label drift
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is False
    assert checks["swarm"][0] == "FAIL"
    assert "label role" in checks["swarm"][1]


def test_unreachable_node_fails_and_short_circuits():
    node = {"name": "WX", "host": "10.0.0.99", "role": "worker", "swarm": True,
            "labels": {"role": "worker"}, "disk_min_gb": 500}
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is False
    assert checks["reachability"][0] == "FAIL"
    assert "disk" not in checks  # short-circuit after reachability failure


def test_observability_skips_swarm_checks():
    node = _inv()["nodes"][2]
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is True
    assert checks["swarm"][0] == "PASS"
    assert "outside swarm" in checks["swarm"][1]


def test_cli_self_check_exits_zero():
    with tempfile.TemporaryDirectory() as out:
        rc = pnc.main(["--self-check", "--out", out])
        assert rc == 0
        files = os.listdir(out)
        assert any(f.endswith("-prod-node-check-evidence.json") for f in files)


def test_cli_drift_exits_nonzero():
    """Real-mode exit contract: drift -> non-zero (the D2 gate)."""
    with tempfile.TemporaryDirectory() as tmp:
        inv_path = os.path.join(tmp, "inv.json")
        out = os.path.join(tmp, "out")
        with open(inv_path, "w", encoding="utf-8") as fh:
            # Test the main() argv handling by pointing at a missing file first.
            pass
        # missing inventory file -> exit 2 (drift/blocked)
        rc = pnc.main(["--inventory", os.path.join(tmp, "missing.json"), "--out", out])
        assert rc == 2


def test_cli_real_run_against_fake_runner_is_fail_on_drift():
    """End-to-end main() with a fake runner swapped in: W1 drift -> exit 1."""
    import prod_node_check as pnc_mod

    real_runner = pnc_mod.RemoteRunner
    try:
        pnc_mod.RemoteRunner = lambda access: FakeRunner(access)
        with tempfile.TemporaryDirectory() as tmp:
            inv_path = os.path.join(tmp, "inv.json")
            out = os.path.join(tmp, "out")
            with open(inv_path, "w", encoding="utf-8") as fh:
                json.dump(_inv(), fh)
            rc = pnc.main(["--inventory", inv_path, "--out", out])
            assert rc == 1  # W1 is drifting -> D2 gate fails
            ev = os.listdir(out)[0]
            with open(os.path.join(out, ev), encoding="utf-8") as fh:
                assert json.load(fh)["result"] == "FAIL"
    finally:
        pnc_mod.RemoteRunner = real_runner


def test_load_inventory_validates_shape():
    with tempfile.TemporaryDirectory() as tmp:
        inv_path = os.path.join(tmp, "bad.json")
        with open(inv_path, "w", encoding="utf-8") as fh:
            json.dump({"access": {}, "nodes": []}, fh)
        import pytest
        with pytest.raises(ValueError):
            pnc.load_inventory(inv_path)
