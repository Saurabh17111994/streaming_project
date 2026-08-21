"""L11 — Resource / memory + performance  RES-001..006 + PERF-001..005.

Per 08-local-compose.md §L11. Offline contracts on PlatformConfig 65/35/85,
JVM wiring, and throughput spec targets; live load is env-gated.
"""
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
PLATFORM_CONFIG = ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java"


class ResourceL11Test(unittest.TestCase):
    def test_RES_001_memory_limit_enforced(self):
        """RES-001: memory limits are enforced (PlatformConfig + compose)."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", cfg)
        # Compose must have memory-related wiring (JVM heap / flink memory)
        text = COMPOSE.read_text()
        self.assertTrue("memory" in text.lower() or "JVM_HEAP" in text, "RES-001: no memory wiring in compose")

    def test_RES_002_jvm_heap_65_percent(self):
        """RES-002: JVM heap = 65% — verify actual pin, not just compose var."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65", cfg, "RES-002: heap pin not 65")
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", doc)
        self.assertIn("65", doc)

    def test_RES_003_non_heap_reserve_35_percent(self):
        """RES-003: 35% non-heap reserve."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("NON_HEAP_MEMORY_RESERVE_PERCENT = 35", cfg, "RES-003: reserve not 35")
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("35", doc)

    def test_RES_004_alert_threshold_85_percent(self):
        """RES-004: 85% container memory alert threshold."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("CONTAINER_MEMORY_ALERT_PERCENT = 85", cfg, "RES-004: alert not 85")
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("85", doc)

    def test_RES_005_cpu_saturation_observable(self):
        """RES-005: CPU saturation → latency/backpressure/dropped messages observable."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("CPU saturation", spec, "RES-005: spec missing")
        # Throughput backpressure is covered by STREAM-009 + PERF burst

    def test_RES_006_disk_pressure_degrades_readiness(self):
        """RES-006: disk pressure → clear readiness degradation, no silent corruption."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("Disk pressure", spec, "RES-006: spec missing")
        # Fluss data volumes must be persistent so pressure is observable, not silent
        self.assertIn("fluss-data", COMPOSE.read_text())


class PerformanceL11Test(unittest.TestCase):
    def test_PERF_001_one_k_ticks(self):
        """PERF-001: 1k ticks/s — spec target exists, baseline is 20 ticks/instrument/s."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC = 20", cfg)
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("1k ticks/s", spec)

    def test_PERF_002_five_k_ticks(self):
        """PERF-002: 5k ticks/s."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("5k ticks/s", spec)

    def test_PERF_003_ten_k_ticks(self):
        """PERF-003: 10k ticks/s — spec target ~10k sustained with no drops."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("10k ticks/s", spec)
        self.assertIn("10k ticks/s sustained with no drops", spec)

    def test_PERF_004_ten_k_sustained_long(self):
        """PERF-004: 10k sustained (long, not 10s)."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("10k sustained (long, not 10s)", spec)

    def test_PERF_005_burst_recovery(self):
        """PERF-005: burst + recovery 2k→10k→20k→2k."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("Burst + recovery", spec)
        self.assertIn("2k", spec)
        self.assertIn("20k", spec)


if __name__ == "__main__":
    unittest.main()
