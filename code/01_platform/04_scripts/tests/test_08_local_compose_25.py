"""25-instrument extended prod-scale smoke via Nautilus trader (fake broker).

Extends LOCAL-INT-004 (canonical 10) to 25 random instruments — same
lifecycle: signal→instruction→gateway→Nautilus→FakeBridge→ReportEnvelope→
Fluss Order_Lifecycle/Positions/Order_Correlation→Babysitter zero-actions,
no ARROW_* egress. Offline PASS without containers; live probes when
execution-t3=fake stack is up.
"""
import subprocess, unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
HARNESS = ROOT / "code/01_platform/04_scripts/local_int_004_smoke.py"

class Nautilus25SmokeTest(unittest.TestCase):
    def test_25_instruments_offline_contract(self):
        """25-instrument offline: 25 random instruments, fake lifecycle via Nautilus — must PASS without containers."""
        out = subprocess.check_output(["python3", str(HARNESS), "--offline", "--instruments", "25"], text=True)
        self.assertIn("PASS LOCAL-INT-004 [offline-25]", out, f"25 offline failed: {out}")
        self.assertIn("25 instruments", out)
        self.assertIn("Nautilus trader", out)
        self.assertNotIn("FAIL", out)

    def test_25_instruments_live_gated(self):
        """25-instrument live: execution-t3=fake stack → real 25-smoke; otherwise contract-only PASS."""
        out = subprocess.check_output(["python3", str(HARNESS), "--live", "--instruments", "25"], text=True)
        self.assertIn("PASS LOCAL-INT-004 [live-25]", out, f"25 live failed: {out}")
        self.assertNotIn("FAIL", out)

    def test_10_still_passes_after_extension(self):
        """Canonical 10-instrument must still pass (regression guard)."""
        out = subprocess.check_output(["python3", str(HARNESS), "--offline", "--instruments", "10"], text=True)
        self.assertIn("PASS LOCAL-INT-004 [offline-10]", out)

    def test_pool_can_supply_25(self):
        """Instrument pool must be >=25."""
        # harness now uses range(1001,1031) = 30
        import importlib.util
        spec = importlib.util.spec_from_file_location("smoke", HARNESS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.assertGreaterEqual(len(mod.INSTRUMENTS), 25, "pool too small for 25-instrument smoke")

if __name__ == "__main__":
    unittest.main()
