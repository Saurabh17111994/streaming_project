#!/usr/bin/env python3
"""
docs_audit.py — doc-vs-code truth gate (docs/08_implementation/01-foundation.md L388
"no dossier silently contradicts an upstream document").

Checks the concrete, machine-verifiable invariants that the 2026-08-13 ground-truth
audit established. Failures are exit-code 1 with a named message; run as
`make docs-audit` from the repo root. Intentionally NOT exhaustive — it pins the
checks that have already caught real drift, so the same class of mistake fails fast.

Checks:
  C1  schema_manifest.json: 21 tables; every entry has non-null ddl_sha256,
      compatibility_class; LOG entries have non-null bucket_key.
  C2  OwnershipMatrix.java encodes exactly the 12 doc rows; test pins it.
  C3  Foundation schema-state diagram is the 5-state enum (no RECONCILED/...).
  C4  CompatibilityClass enum names == version_matrix.yaml header comment.
  C5  No known-stale phrases anywhere in docs/.
  C6  Test counts in 01-foundation.md L42 match current surefire totals.
  C7  versions.pin holds FLINK/FLUSS pins; no 'latest' / placeholder.
"""

import glob
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
DDL_DIR = os.path.join(ROOT, "code", "01_platform", "02_sql", "ddl")
SCRIPTS_DIR = os.path.join(ROOT, "code", "01_platform", "04_scripts")
DOCS_DIR = os.path.join(ROOT, "docs")
COMMON_DIR = os.path.join(ROOT, "code", "common")
INGEST_DIR = os.path.join(ROOT, "code", "02_services", "01_ingestion")

failures = []


def check(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}{(' — ' + detail) if detail and not ok else ''}")
    if not ok:
        failures.append(name)


def safe_read(path):
    """Return file text, or None when unreadable (missing file is a check failure)."""
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except (OSError, ValueError):
        return None


def safe_read_json(path):
    try:
        return json.loads(safe_read(path) or "null")
    except (ValueError, TypeError):
        return None


def c1_manifest():
    p = os.path.join(DDL_DIR, "schema_manifest.json")
    m = safe_read_json(p)
    if m is None:
        return check("C1 manifest readable", False, p)
    tables = m.get("tables", [])
    check("C1 manifest has 21 tables", len(tables) == 21, f"got {len(tables)}")
    bad_sha = [t["table_name"] for t in tables if not t.get("ddl_sha256")]
    bad_compat = [t["table_name"] for t in tables if not t.get("compatibility_class")]
    bad_routing = [
        t["table_name"]
        for t in tables
        if str(t.get("table_kind", "")).upper() == "LOG" and not t.get("bucket_key")
    ]
    check("C1 all entries have ddl_sha256", not bad_sha, f"{bad_sha}")
    check("C1 all entries have compatibility_class", not bad_compat, f"{bad_compat}")
    check("C1 LOG entries have bucket_key", not bad_routing, f"{bad_routing}")


def c2_ownership_matrix():
    p = os.path.join(
        COMMON_DIR,
        "src",
        "main",
        "java",
        "com",
        "trading",
        "common",
        "ownership",
        "OwnershipMatrix.java",
    )
    src = safe_read(p)
    if src is None:
        return check("C2 OwnershipMatrix readable", False, p)
    # "new Rule(" also matches the logRule() helper's dynamic instantiation —
    # the 12 encoded rows are exactly the string-literal-target rules.
    n = len(re.findall(r'new Rule\("', src))
    check("C2 ownership matrix = 12 rows", n == 12, f"found {n} Rule( instantiations")
    test_p = os.path.join(
        COMMON_DIR,
        "src",
        "test",
        "java",
        "com",
        "trading",
        "common",
        "ownership",
        "OwnershipMatrixTest.java",
    )
    check("C2 OwnershipMatrixTest pins matrix", os.path.exists(test_p))


def c3_schema_state_diagram():
    p = os.path.join(DOCS_DIR, "08_implementation", "01-foundation.md")
    txt = safe_read(p)
    if txt is None:
        return check("C3 foundation doc readable", False, p)
    five = all(
        s in txt for s in ["PROPOSED", "APPROVED", "APPLYING", "OBSERVED", "REJECTED"]
    )
    stale = any(
        s in txt for s in ["RECONCILED", "DIALECT_VALIDATED", "INTEGRATION_VALIDATED"]
    )
    check("C3 schema-state diagram = 5-state enum", five and not stale)


def c4_compat_vocabulary():
    p = os.path.join(
        COMMON_DIR,
        "src",
        "main",
        "java",
        "com",
        "trading",
        "common",
        "version",
        "CompatibilityClass.java",
    )
    enum_src = safe_read(p)
    yaml = safe_read(os.path.join(SCRIPTS_DIR, "version_matrix.yaml"))
    if enum_src is None or yaml is None:
        return check("C4 enum/yaml readable", False)
    header = "COMPATIBLE_WITH_LIMITATION" in yaml and "NOT_APPLICABLE" in yaml
    enum_has = "COMPATIBLE_WITH_LIMITATION" in enum_src and "NOT_APPLICABLE" in enum_src
    no_old = "LIMITED," not in enum_src and "\n    NA" not in enum_src
    check("C4 enum vocabulary matches yaml header", header and enum_has and no_old)


STALE_PHRASES = [
    "fingerprint-based LRU idempotency cache",
    "fingerprint idempotency cache",
    "RECONCILED",
    "no Flink jobs built yet",
    "MAX_PENDING_APPEND_RECORDS = 10000",
]


def c5_stale_phrases():
    hits = []
    for root, _, files in os.walk(DOCS_DIR):
        for f in files:
            if not f.endswith(".md"):
                continue
            path = os.path.join(root, f)
            txt = safe_read(path)
            if txt is None:
                continue
            for phrase in STALE_PHRASES:
                if phrase in txt:
                    hits.append(f"{os.path.relpath(path, ROOT)}: '{phrase}'")
    check("C5 no stale phrases in docs", not hits, "; ".join(hits[:5]))


def _is_gated_class(test_src_dir, report_path):
    """True when the report is stale — its class no longer exists in the
    current test source tree (renamed/deleted, e.g. the candle->signal KV
    conversion). Gated classes DO count: surefire's module headline
    "Tests run: N, Skipped: M" includes skipped tests, and the doc count
    follows that headline convention. Reports for gated classes are written
    by the normal suite (as skips) exactly like any other class."""
    if not test_src_dir:
        return False
    name = os.path.basename(report_path)  # TEST-com.trading.compute.X.xml
    cls = name[len("TEST-") : -len(".xml")].rsplit(".", 1)[-1]
    return all(cls + ".java" not in files for _, _, files in os.walk(test_src_dir))


def surefire_total(module_dir, test_src_dir=None):
    total = 0
    for p in glob.glob(
        os.path.join(module_dir, "target", "surefire-reports", "TEST-*.xml")
    ):
        if _is_gated_class(test_src_dir, p):
            continue
        try:
            root = ET.parse(p).getroot()
            total += int(root.get("tests", 0))
        except (ET.ParseError, OSError, ValueError):
            continue  # stale/corrupt report dirs are ignored — totals come from mtime-clean runs
    return total


def c6_test_counts():
    p = os.path.join(DOCS_DIR, "08_implementation", "01-foundation.md")
    txt = safe_read(p)
    if txt is None:
        return check("C6 foundation doc readable", False, p)
    m = re.search(r"unit suites green (\d+)/(\d+)/(\d+)", txt)
    if not m:
        return check("C6 doc test-count line found", False)
    doc_c, doc_i, doc_comp = map(int, m.groups())
    c = surefire_total(COMMON_DIR, os.path.join(COMMON_DIR, "src", "test", "java"))
    i = surefire_total(INGEST_DIR, os.path.join(INGEST_DIR, "src", "test", "java"))
    # compute module target/ is often absent or root-owned; only compare when present.
    comp_dir = os.path.join(ROOT, "code", "02_services", "02_compute")
    comp = (
        surefire_total(comp_dir, os.path.join(comp_dir, "src", "test", "java"))
        if os.path.isdir(comp_dir)
        else None
    )
    check("C6 common count matches doc", c == doc_c, f"surefire={c} doc={doc_c}")
    check("C6 ingestion count matches doc", i == doc_i, f"surefire={i} doc={doc_i}")
    if comp is not None:
        check(
            "C6 compute count matches doc",
            comp == doc_comp,
            f"surefire={comp} doc={doc_comp}",
        )


def c7_version_pins():
    p = os.path.join(SCRIPTS_DIR, "versions.pin")
    txt = safe_read(p)
    if txt is None:
        return check("C7 versions.pin readable", False, p)
    flink = re.search(r"FLINK_VERSION=(\S+)", txt)
    fluss = re.search(r"FLUSS_VERSION=(\S+)", txt)
    ok = bool(flink and fluss)
    bad = any(
        v in (flink.group(1) if flink else "", fluss.group(1) if fluss else "")
        for v in ("latest", "TO_BE_PINNED")
    )
    check(
        "C7 FLINK/FLUSS pinned",
        ok and not bad,
        f"flink={flink.group(1) if flink else '?'} fluss={fluss.group(1) if fluss else '?'}",
    )


def main():
    c1_manifest()
    c2_ownership_matrix()
    c3_schema_state_diagram()
    c4_compat_vocabulary()
    c5_stale_phrases()
    c6_test_counts()
    c7_version_pins()
    if failures:
        print(f"\ndocs-audit: {len(failures)} check(s) FAILED — fix before proceeding")
        return 1
    print("\ndocs-audit: all checks pass — docs agree with code")
    return 0


if __name__ == "__main__":
    sys.exit(main())
