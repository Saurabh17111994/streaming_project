#!/usr/bin/env python3
"""seed_dashboards — idempotent OpenObserve dashboard provisioning.

Ensures every dashboard in ``code/01_platform/01_docker/openobserve/dashboards/``
exists in the configured OpenObserve org. Idempotent: a dashboard whose title
already exists is left untouched (created count 0) unless ``--force`` is given,
in which case the stored v8 doc is PUT with the file's tabs/panels.

Credentials come from the environment only — the script never defaults a
password and refuses to run without one:
    O2_API_URL   (default http://localhost:5080)
    O2_ORG       (default default)
    O2_USER      (default admin@example.com)
    O2_PASSWORD  (REQUIRED)

Exit codes: 0 = all ensured (or dry-run); 1 = API/apply error; 2 = credential
gate refused; 3 = invalid dashboard files or manifest.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DASH_DIR = ROOT / "code/01_platform/01_docker/openobserve/dashboards"
MANIFEST = DASH_DIR / "manifest.json"


def _load() -> tuple[list[dict], list[dict]]:
    if not MANIFEST.exists():
        raise SystemExit(f"exit 3: manifest missing: {MANIFEST}")
    manifest = json.loads(MANIFEST.read_text())
    records = manifest["dashboards"]
    dashboards = []
    for rec in records:
        path = DASH_DIR / rec["file"]
        if not path.exists():
            raise SystemExit(f"exit 3: manifest references missing file: {rec['file']}")
        doc = json.loads(path.read_text())
        if doc.get("title") != rec["title"]:
            raise SystemExit(
                f"exit 3: {rec['file']} title {doc.get('title')!r} != manifest {rec['title']!r}"
            )
        dashboards.append((rec, doc))
    return manifest, dashboards


def _api(base: str, org: str, user: str, password: str, path: str, method="GET", body=None):
    url = f"{base.rstrip('/')}/api/{org}{path}"
    headers = {
        "Authorization": "Basic " + _b64(f"{user}:{password}"),
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except urllib.error.URLError as e:
        raise SystemExit(f"exit 1: cannot reach {url}: {e.reason}")


def _b64(s: str) -> str:
    import base64

    return base64.b64encode(s.encode()).decode()


EMPTY_FILTER = {
    "type": "condition",
    "values": [],
    "logicalOperator": "AND",
    "filterType": "condition",
}


def _normalize(doc: dict) -> None:
    """Complete a dashboard file to the full v8 schema O2 v0.91.5 requires.

    Fill-in defaults only — existing values are never overwritten. Without
    this the POST 422s (`missing field show_legends`) because the repo files
    carry a reduced panel shape. Field requirements verified against the live
    API (see o2-provision.py make_panel_v8 / managed skill
    o2-v8-dashboard-provisioning): PanelConfig.show_legends and
    QueryConfig.promql_legend have no serde default.
    """
    for tab in doc.get("tabs", []):
        for index, panel in enumerate(tab.get("panels", [])):
            config = panel.setdefault("config", {})
            config.setdefault("show_legends", True)
            is_promql = panel.get("queryType") == "promql"
            for query in panel.get("queries", []):
                query.setdefault("customQuery", True)
                fields = query.setdefault("fields", {})
                fields.setdefault("stream_type", "metrics")
                fields.setdefault("x", [])
                fields.setdefault("y", [])
                fields.setdefault("z", [])
                fields.setdefault("filter", dict(EMPTY_FILTER))
                if not is_promql and not fields["x"] and not fields["y"]:
                    fields["x"] = [
                        {"label": "_timestamp", "alias": "_timestamp",
                         "column": "_timestamp"}
                    ]
                    fields["y"] = [
                        {"label": "value", "alias": "value", "column": "value"}
                    ]
                qconfig = query.setdefault("config", {})
                qconfig.setdefault(
                    "promql_legend", "{{task_name}}" if is_promql else ""
                )
            layout = panel.setdefault("layout", {"x": 0, "y": 0, "w": 96, "h": 4})
            layout.setdefault("i", index)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="print the plan, change nothing")
    parser.add_argument("--force", action="store_true", help="PUT tabs/panels onto existing titles")
    args = parser.parse_args()

    password = os.environ.get("O2_PASSWORD", "")
    if not password:
        print(
            "ddl-apply gate: O2_PASSWORD is required (dashboard seeding refuses to guess "
            "credentials). Set O2_API_URL/O2_ORG/O2_USER/O2_PASSWORD.",
            file=sys.stderr,
        )
        return 2
    base = os.environ.get("O2_API_URL", "http://localhost:5080")
    org = os.environ.get("O2_ORG", "default")
    user = os.environ.get("O2_USER", "admin@example.com")

    manifest, dashboards = _load()
    for _, doc in dashboards:
        _normalize(doc)
    existing = {}
    if not args.dry_run:
        status, body = _api(base, org, user, password, "/dashboards")
        if status != 200:
            print(f"exit 1: list dashboards failed ({status}): {body[:200]}", file=sys.stderr)
            return 1
        existing = {d["title"]: d for d in json.loads(body).get("dashboards", [])}

    created = updated = untouched = 0
    for rec, doc in dashboards:
        title = doc["title"]
        dash_id = existing.get(title, {}).get("dashboard_id")
        if args.dry_run:
            action = "update" if (dash_id and args.force) else ("create" if not dash_id else "keep")
            print(f"  [{action:6s}] {title}")
            if action == "create":
                created += 1
            elif action == "update":
                updated += 1
            else:
                untouched += 1
            continue
        if not dash_id:
            status, body = _api(base, org, user, password, "/dashboards", "POST", doc)
            if status not in (200, 201):
                print(f"exit 1: create {title} failed ({status}): {body[:200]}", file=sys.stderr)
                return 1
            created += 1
            continue
        if args.force:
            stored = existing[title].get("v8") or {}
            if stored.get("tabs") == doc.get("tabs"):
                untouched += 1
                continue
            payload = {**stored, "title": title, "description": doc.get("description", ""),
                       "tabs": doc.get("tabs", []), "version": 8}
            status, body = _api(base, org, user, password, f"/dashboards/{dash_id}", "PUT", payload)
            if status == 200:
                updated += 1
            else:
                print(f"exit 1: update {title} failed ({status}): {body[:200]}", file=sys.stderr)
                return 1
        else:
            untouched += 1

    print(f"RESULT: created={created} updated={updated} untouched={untouched} "
          f"({len(dashboards)} dashboards, manifest {manifest.get('schema_version')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
