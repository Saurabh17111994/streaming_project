"""Validate the OpenObserve dashboard corpus + seed script contracts."""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
DASH_DIR = ROOT / "code/01_platform/01_docker/openobserve/dashboards"
SEED = ROOT / "code/01_platform/04_scripts/seed_dashboards.py"

REQUIRED_MANIFEST_FIELDS = [
    "title", "file", "folder", "dashboard_version", "query_version",
    "measurement_boundary", "workload", "duration", "utc_clock",
    "sample_count", "failures_or_restarts_included", "streams", "software_versions",
]
PANEL_TYPES = {"bar", "line", "table", "area", "gauge"}


def _load_all():
    manifest = json.loads((DASH_DIR / "manifest.json").read_text())
    docs = {}
    for rec in manifest["dashboards"]:
        docs[rec["title"]] = (rec, json.loads((DASH_DIR / rec["file"]).read_text()))
    return manifest, docs


def test_manifest_records_carry_evidence_fields():
    manifest, _ = _load_all()
    assert manifest["schema_version"] == 1
    for rec in manifest["dashboards"]:
        for field in REQUIRED_MANIFEST_FIELDS:
            assert rec.get(field), f"{rec['title']} missing manifest field {field}"
        assert set(rec["streams"]) <= {"metrics", "logs", "traces"}


def test_dashboard_files_are_valid_v8_corpus():
    manifest, docs = _load_all()
    assert len(docs) >= 3, "at least the core dashboards must exist"
    for title, (rec, doc) in docs.items():
        assert doc["version"] == 8, f"{title}: v8 required"
        assert doc["title"] == rec["title"]
        assert doc.get("folder_id") == rec["folder"]
        assert doc.get("tabs"), f"{title}: at least one tab"
        seen = set()
        for tab in doc["tabs"]:
            assert tab.get("tabId") and tab.get("name")
            for panel in tab.get("panels", []):
                assert panel.get("id") not in seen, f"{title}: duplicate panel id"
                seen.add(panel.get("id"))
                assert panel["type"] in PANEL_TYPES, f"{title}: bad type {panel['type']}"
                assert panel["queryType"] == "sql"
                assert panel.get("layout"), f"{title}: panel {panel['id']} missing layout"
                lay = panel["layout"]
                assert lay["w"] > 0 and lay["x"] + lay["w"] <= 192, f"{title}: panel off 192-grid"
                assert panel.get("queries"), f"{title}: panel {panel['id']} has no queries"
                for q in panel["queries"]:
                    assert q.get("fields", {}).get("stream"), f"{title}: query stream required"
                    assert q.get("query"), f"{title}: query sql required"


def test_no_secrets_in_corpus():
    for f in ["manifest.json", "safe-to-trade.json", "order-execution.json",
              "data-ingestion.json", "storage-eod.json"]:
        doc = json.loads((DASH_DIR / f).read_text())

        def walk(node):
            if isinstance(node, dict):
                for k, v in node.items():
                    if any(s in k.lower() for s in ("password", "secret", "token")):
                        raise AssertionError(f"{f}: suspicious key {k}")
                    walk(v)
            elif isinstance(node, list):
                for it in node:
                    walk(it)

        walk(doc)


def test_seed_refuses_without_password():
    env = {k: v for k, v in os.environ.items() if k != "O2_PASSWORD"}
    proc = subprocess.run([sys.executable, str(SEED), "--dry-run"], env=env,
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 2, "blank O2_PASSWORD must exit 2"
    assert "O2_PASSWORD" in proc.stderr


def test_seed_dry_run_plans_known_titles():
    env = dict(os.environ)
    env["O2_PASSWORD"] = "Dry-Run!2026"
    proc = subprocess.run([sys.executable, str(SEED), "--dry-run"], env=env,
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0
    assert "RESULT:" in proc.stdout
    assert "Safe to Trade" in proc.stdout
