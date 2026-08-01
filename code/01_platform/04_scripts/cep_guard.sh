#!/usr/bin/env bash
# Fail the build if Apache Flink CEP (Complex Event Processing) is introduced
# into dependency or source files. Project rule (01-foundation.md): no CEP
# dependency/usage in the MVP order path.
#
# Scans only dependency declarations (pom.xml) and source (java/scala).
# Documentation that *prohibits* CEP is intentionally NOT scanned, so the
# guard does not trip on the rule text itself.
set -euo pipefail

ROOT="${1:-.}"

HITS="$(grep -rEn \
	--exclude-dir=.git --exclude-dir=target --exclude-dir=node_modules \
	--include=pom.xml --include='*.java' --include='*.scala' \
	'flink-cep|org\.apache\.flink\.cep' "$ROOT" 2>/dev/null || true)"

if [ -n "$HITS" ]; then
	echo "ERROR: Flink CEP usage is forbidden by project policy (no CEP in MVP order path)."
	echo "$HITS"
	exit 1
fi

echo "OK: no Flink CEP references found in dependency/source files."
