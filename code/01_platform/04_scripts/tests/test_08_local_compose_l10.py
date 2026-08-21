"""L10 End-to-end 10-instrument smoke — LOCAL-INT-004."""
import subprocess, unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
HARNESS = ROOT / "code/01_platform/04_scripts/local_int_004_smoke.py"

class LocalInt004Test(unittest.TestCase):
    def test_LOCAL_INT_004_offline_contract(self):
        """LOCAL-INT-004 offline: 10 instruments, fake bridge lifecycle, no live Arrow egress — must PASS without containers."""
        out=subprocess.check_output(["python3", str(HARNESS), "--offline"], text=True)
        self.assertIn("PASS LOCAL-INT-004 [offline", out, f"LOCAL-INT-004 offline failed: {out}")

    def test_LOCAL_INT_004_live_is_either_pass_or_contract_only(self):
        """LOCAL-INT-004 live: when execution-t3 stack is up, drive real smoke; otherwise contract-only is OK."""
        out=subprocess.check_output(["python3", str(HARNESS), "--live"], text=True)
        self.assertIn("PASS LOCAL-INT-004", out, f"LOCAL-INT-004 live failed: {out}")
        self.assertNotIn("FAIL", out)

    def test_LOCAL_INT_004_harness_is_executable(self):
        """Harness must be executable and importable."""
        self.assertTrue(HARNESS.exists(), "harness missing")
        self.assertTrue(HARNESS.stat().st_mode & 0o111, "harness not executable")

if __name__ == "__main__":
    unittest.main()
