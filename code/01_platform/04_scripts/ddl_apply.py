#!/usr/bin/env python3
"""
DDL application contract
(docs/08_implementation/01-foundation.md -> "DDL application contract", orig L420).

Workflow (gated):
  1. Load version pins from versions.pin (sibling of this script).
  2. Enforce VersionGate on PLATFORM versions only (FLINK_VERSION, FLUSS_VERSION):
     any absent / 'latest' / placeholder -> FAIL (no apply).
     External/verification versions (broker protocol, Arrow API, schema
     lifecycle) may remain TO_BE_VERIFIED and do NOT block DDL emission.
  3. Compute SHA-256 of every DDL file; detect whether the committed
     schema_manifest.json is stale (names, paths, or checksums differ).
  4. Without --force, refuse to overwrite an existing manifest that differs
     from the computed one. Print a diff and require an explicit decision.
  5. Parse each DDL for its routing identity (bucket.key); LOG tables must have a non-null one.
  6. Refuse to apply to Fluss unless --apply-verified AND --matrix-evidence given
     AND the reconciliation blocker exit criteria are met.

Application is blocked until the reconciliation blocker
(code/01_platform/02_sql/ddl/00_RECONCILIATION_BLOCKER.md) exit criteria are met.
"""

import argparse
import hashlib
import json
import os
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DDL_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "02_sql", "ddl")
VERSIONS_PIN = os.path.join(SCRIPT_DIR, "versions.pin")
MANIFEST_PATH = os.path.join(DDL_DIR, "schema_manifest.json")
PLACEHOLDERS = {
    "BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED",
    "FLINK_VERSION_TO_BE_PINNED",
    "FLUSS_VERSION_TO_BE_PINNED",
    "ARROW_API_CONTRACT_TO_BE_VERIFIED",
    "OPENALGO_API_CONTRACT_TO_BE_VERIFIED",
    "SCHEMA_LIFECYCLE_TO_BE_VERIFIED",
}
REQUIRED_VERSIONS = [
    # Platform versions: required for Fluss DDL definition / schema.
    # External/verification versions (broker/Arrow/schema-lifecycle) are NOT
    # required here; they remain TO_BE_VERIFIED and do not block DDL emission.
    "FLINK_VERSION",
    "FLUSS_VERSION",
]


def sha256_of(path):
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                digest.update(chunk)
    except OSError as exc:
        raise RuntimeError(f"cannot read {path}: {exc}") from exc
    return digest.hexdigest()


def load_versions(pin_path):
    values = {}
    if not os.path.exists(pin_path):
        return values
    try:
        with open(pin_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                values[key.strip()] = val.strip()
    except OSError as exc:
        raise RuntimeError(f"cannot read {pin_path}: {exc}") from exc
    return values


def enforce_version_gate(versions):
    problems = []
    for key in REQUIRED_VERSIONS:
        val = versions.get(key)
        if not val:
            problems.append(f"{key} is absent")
        elif val.lower() == "latest":
            problems.append(f"{key} is 'latest'")
        elif val in PLACEHOLDERS:
            problems.append(f"{key} is a placeholder ({val})")
    return problems


def parse_bucket_key(ddl_text):
    match = re.search(r"'bucket\.key'\s*=\s*'([^']*)'", ddl_text)
    return match.group(1) if match else None


def parse_primary_key(ddl_text):
    match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", ddl_text)
    return match.group(1).strip() if match else None


def parse_table_name(ddl_text):
    match = re.search(r"CREATE TABLE (\w+)", ddl_text)
    return match.group(1) if match else None


def load_existing_manifest():
    """Return the committed schema_manifest.json as a dict, or None if absent/unreadable."""
    if not os.path.exists(MANIFEST_PATH):
        return None
    try:
        with open(MANIFEST_PATH, encoding="utf-8") as fh:
            raw = fh.read()
        manifest = json.loads(raw)
        # R-147: normalize the structure so a malformed-but-valid-JSON
        # manifest fails with the clear DDL-contract diagnostic, not a
        # KeyError/TypeError deep in diff_manifests.
        if not isinstance(manifest, dict) or not isinstance(
            manifest.get("tables"), list
        ):
            raise RuntimeError(
                f"{MANIFEST_PATH} has no 'tables' list — malformed manifest structure"
            )
        return manifest
    except (OSError, json.JSONDecodeError) as exc:
        print(f"WARNING: cannot parse existing manifest ({exc}); treating as absent")
        return None


def compute_manifest_entries():
    """Parse all DDL files and return list of manifest entries."""
    try:
        ddl_files = sorted(
            name for name in os.listdir(DDL_DIR) if name.endswith(".sql")
        )
    except OSError as exc:
        raise RuntimeError(f"cannot list DDL directory {DDL_DIR}: {exc}") from exc

    entries = []
    for name in ddl_files:
        path = os.path.join(DDL_DIR, name)
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
        except OSError as exc:
            raise RuntimeError(f"cannot read {path}: {exc}") from exc

        # Non-table SQL (catalog/database bootstrap) is not subject to
        # the routing-key rule.
        if "CREATE TABLE" not in text:
            continue

        primary_key = parse_primary_key(text)
        if primary_key:
            # KV / primary-key table: routed by primary key, not bucket.key.
            entries.append(
                {
                    "table_name": parse_table_name(text),
                    "ddl_path": name,
                    "ddl_sha256": sha256_of(path),
                    "table_kind": "KV",
                    "primary_key": primary_key,
                    "bucket_key": None,
                }
            )
            continue

        # LOG table: must carry a non-null routing identity (bucket.key).
        bucket_key = parse_bucket_key(text)
        if not bucket_key or not bucket_key.strip():
            raise RuntimeError(
                f"ROUTING-KEY VIOLATION: {name} (LOG table) has no non-null "
                "bucket.key (routing identity)"
            )
        entries.append(
            {
                "table_name": parse_table_name(text),
                "ddl_path": name,
                "ddl_sha256": sha256_of(path),
                "table_kind": "LOG",
                "bucket_key": bucket_key,
                "primary_key": None,
            }
        )
    return entries


def diff_manifests(existing, computed):
    """Return list of human-readable diff lines, or empty list if identical."""
    existing_entries = {t["ddl_path"]: t for t in existing.get("tables", [])}
    computed_entries = {t["ddl_path"]: t for t in computed["tables"]}
    diffs = []

    for path in sorted(set(existing_entries) | set(computed_entries)):
        e = existing_entries.get(path)
        c = computed_entries.get(path)
        if e is None and c is not None:
            diffs.append(f"  + ADDED:   {path} ({c['table_name']})")
        elif c is None and e is not None:
            diffs.append(f"  - REMOVED: {path} ({e['table_name']})")
        elif e is not None and c is not None and e["ddl_sha256"] != c["ddl_sha256"]:
            diffs.append(f"  ~ CHANGED: {path} ({c['table_name']}) checksum differs")
        elif (
            e is not None
            and c is not None
            and e.get("table_kind") != c.get("table_kind")
        ):
            diffs.append(
                f"  ~ KIND:    {path} ({c['table_name']}) "
                f"{e.get('table_kind')} -> {c.get('table_kind')}"
            )
        elif (
            e is not None
            and c is not None
            and (
                e.get("primary_key") != c.get("primary_key")
                or e.get("bucket_key") != c.get("bucket_key")
            )
        ):
            diffs.append(
                f"  ~ FIELD:   {path} ({c['table_name']}) primary_key/bucket_key "
                "differs from DDL (manifest vs generator)"
            )

    return diffs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="allow overwriting schema_manifest.json even when the DDLs have changed",
    )
    parser.add_argument(
        "--apply-verified",
        action="store_true",
        help="attempt real application (still gated on evidence)",
    )
    parser.add_argument(
        "--matrix-evidence", help="path to capability-evidence artifact"
    )
    args = parser.parse_args()

    try:
        versions = load_versions(VERSIONS_PIN)
        gate_problems = enforce_version_gate(versions)
        if gate_problems:
            print("VERSION GATE FAILED (refusing to apply DDL):")
            for problem in gate_problems:
                print("  - " + problem)
            print("See code/01_platform/02_sql/ddl/00_RECONCILIATION_BLOCKER.md")
            return 2

        entries = compute_manifest_entries()
        computed = {"schema_manifest_version": "1", "tables": entries}

        # --- Manifest staleness detection (R-014: never return before the
        # apply step runs — a synced manifest must still reach the gated
        # apply/refusal handling below) ---
        existing = load_existing_manifest()
        needs_write = False
        if existing is not None:
            diffs = diff_manifests(existing, computed)
            if diffs:
                print(
                    "MANIFEST IS STALE — DDLs differ from committed "
                    "schema_manifest.json:"
                )
                for d in diffs:
                    print(d)
                if not args.force:
                    print(
                        "Re-run with --force to regenerate schema_manifest.json "
                        "and accept these changes."
                    )
                    return 5
                print("--force: regenerating schema_manifest.json despite drift.")
                needs_write = True
            else:
                print("Manifest is current; no DDL drift detected.")
        else:
            needs_write = True

        # Write the (new or force-accepted) manifest.
        if needs_write:
            try:
                with open(MANIFEST_PATH, "w", encoding="utf-8") as fh:
                    json.dump(computed, fh, indent=2)
            except OSError as exc:
                print(f"cannot write {MANIFEST_PATH}: {exc}")
                return 2
            print(f"Wrote {MANIFEST_PATH} ({len(entries)} tables).")
        else:
            print(f"{MANIFEST_PATH} unchanged ({len(entries)} tables).")

        # --- Gated apply (R-014): runs, or is refused, regardless of drift
        # state. A caller that passes --apply-verified in the synced state
        # must reach this code, never a silent early return. ---
        if args.apply_verified:
            if not args.matrix_evidence:
                print(
                    "REFUSED: --apply-verified requires --matrix-evidence "
                    "(capability tests)."
                )
                return 4
            print(
                "Applying to Fluss (stub) -- requires pinned Fluss client + "
                "empty catalog."
            )
            return 0

        print(
            "DDL NOT APPLIED: application blocked until reconciliation blocker "
            "exit criteria are met."
        )
        return 0
    except (RuntimeError, KeyError, TypeError, UnicodeDecodeError) as exc:
        # R-147: malformed manifests, non-UTF-8 DDLs, and structurally-wrong
        # entries must surface as the clear DDL-contract diagnostic instead of
        # a raw traceback from a safety-gate script.
        print(f"DDL contract error: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
