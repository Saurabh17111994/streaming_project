"""Shared helpers for 08 Local Compose Phase A checks.

Covers compose JSON loading, image tag lint, secret/prod-marker scans,
DDL catalog, and health/dependency contracts.
"""
from __future__ import annotations
import json
import subprocess
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parents[3]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
DOCKER_DIR = ROOT / "code/01_platform/01_docker"
DDL_DIR = ROOT / "code/01_platform/02_sql/ddl"
ENV_EXAMPLE = DOCKER_DIR / ".env.example"

# ——— compose loading ———
def load_compose_json(compose: Path = COMPOSE, profile: str | None = "execution-t3") -> dict[str, Any]:
    cmd = ["docker", "compose", "-f", str(compose)]
    if profile:
        cmd += ["--profile", profile]
    cmd += ["config", "--format", "json"]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)

def load_compose_yaml_text(compose: Path = COMPOSE) -> str:
    return compose.read_text(encoding="utf-8", errors="replace")

# ——— image tag checks ———
def image_uses_latest(image: str) -> bool:
    # :latest explicit OR no tag and no @sha256 digest
    if ":latest" in image:
        return True
    # image without colon and without @ => implicit latest
    # but allow `image:tag@sha256:...` — not latest
    if "@sha256:" in image:
        return False
    # contains / and no colon after last /
    last = image.rsplit("/", 1)[-1]
    return ":" not in last

def collect_images_from_yaml(text: str) -> list[str]:
    # naive: image: <value>
    vals = []
    for m in re.finditer(r'image:\s*["\']?([^"\'\s#]+)', text):
        vals.append(m.group(1).strip())
    return vals

# ——— secret / prod marker scans ———
PROD_MARKERS = ["ENVIRONMENT=production", "production", "prod-"]
SECRET_KEYS = ["O2_PASSWORD",  "OPENALGO_API_KEY", "ARROW_APP_SECRET", "AWS_SECRET_ACCESS_KEY"]
ARROW_CREDS = {"ARROW_APP_ID","ARROW_APP_SECRET","ARROW_USER_ID","ARROW_PASSWORD","ARROW_TOTP_KEY","ARROW_REST_URL","ARROW_ORDER_UPDATES_URL"}

# env leakage: values that must never appear in logs/inspect/config
def contains_secret_value(text: str, secrets: list[str]) -> list[str]:
    hits=[]
    for s in secrets:
        if s and len(s) >= 4 and s in text:
            hits.append(s)
    return hits

# ——— DDL catalog ———
def ddl_files() -> list[Path]:
    return sorted(DDL_DIR.glob("*.sql"))

def ddl_table_names() -> list[str]:
    names=[]
    for p in ddl_files():
        # file name without number prefix, e.g., 09_order_lifecycle.sql -> order_lifecycle
        stem = p.stem
        # strip leading digits+_
        m=re.match(r'^\d+_(.+)$', stem)
        names.append(m.group(1) if m else stem)
    return names

# ——— health contract ———
REQUIRED_HEALTH_SERVICES = ["zookeeper","fluss-coordinator","fluss-tablet","flink-jobmanager","flink-taskmanager","ingestion","openobserve","otel-collector"]
# execution-* are profile-gated, checked separately
