#!/usr/bin/env python3
"""pom-snapshot-scan.py — CI gate (foundation L554: "CI rejects mutable image
tags and unpinned dependencies").

Fails when any pom.xml pins an EXTERNAL dependency to a SNAPSHOT version
(mutable). The workspace's own com.trading SNAPSHOT is the module-build
contract (common -> ingestion/compute) and is explicitly allowed.

Usage: python3 pom-snapshot-scan.py   (exit 0 = clean, 1 = violations found)
"""

import glob
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEP_RE = re.compile(
    r"<groupId>([^<]+)</groupId>\s*<artifactId>[^<]+</artifactId>\s*"
    r"<version>([^<]+)</version>",
    re.DOTALL,
)


def main():
    violations = []
    for pom in sorted(
        glob.glob(os.path.join(ROOT, "code", "**", "pom.xml"), recursive=True)
    ):
        try:
            with open(pom, encoding="utf-8") as fh:
                text = fh.read()
        except OSError:
            continue
        for match in DEP_RE.finditer(text):
            group, version = match.group(1), match.group(2)
            if "SNAPSHOT" in version and not group.startswith("com.trading"):
                violations.append(f"{os.path.relpath(pom, ROOT)}: {group}:{version}")
    if violations:
        for v in violations:
            print(f"FAIL: external SNAPSHOT dependency: {v}")
        print(
            "pom-snapshot-scan: external SNAPSHOT dependencies are mutable — pin a release"
        )
        return 1
    print("pom-snapshot-scan: no external SNAPSHOT dependencies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
