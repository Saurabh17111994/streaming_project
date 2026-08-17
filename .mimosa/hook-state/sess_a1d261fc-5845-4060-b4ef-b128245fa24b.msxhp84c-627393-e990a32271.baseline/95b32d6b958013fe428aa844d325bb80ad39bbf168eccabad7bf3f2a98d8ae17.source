#!/usr/bin/env bash
# corpus-pin.sh — pin/verify the versioned broker packet corpus (foundation L553:
# "Broker packet/postback corpus is versioned and reproducible").
#
# The corpus is the golden Arrow frame set captured from the broker protocol:
#   code/02_services/01_ingestion/go-bridge/testdata/golden/
#   (full-tick, ltp-tick, response, unknown-packet + their .golden decoded forms)
# Every frame is sha256-pinned in code/01_platform/04_scripts/corpus.sha256 so a
# decoder change that alters a golden byte fails `make pin-check` instead of
# silently diverging from the versioned corpus.
#
# Usage:
#   corpus-pin.sh --verify       verify committed corpus against corpus.sha256 (CI)
#   corpus-pin.sh --regenerate   re-pin corpus.sha256 after an intentional corpus change

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CORPUS_DIR="$REPO_ROOT/code/02_services/01_ingestion/go-bridge/testdata/golden"
MANIFEST="$SCRIPT_DIR/corpus.sha256"

if [ ! -d "$CORPUS_DIR" ]; then
	echo "ERROR: corpus dir missing: $CORPUS_DIR" >&2
	exit 2
fi

case "${1:-}" in
--verify)
	if [ ! -f "$MANIFEST" ]; then
		echo "ERROR: $MANIFEST missing — run 'corpus-pin.sh --regenerate' to pin the corpus" >&2
		exit 2
	fi
	# sha256sum -c expects paths relative to the invocation directory.
	(cd "$REPO_ROOT" && sha256sum -c "$MANIFEST")
	;;
--regenerate)
	(cd "$REPO_ROOT" &&
		sha256sum "$CORPUS_DIR"/* |
		sed "s|  $CORPUS_DIR/|  code/02_services/01_ingestion/go-bridge/testdata/golden/|") >"$MANIFEST"
	echo "Wrote $MANIFEST ($(wc -l <"$MANIFEST") files)."
	;;
*)
	echo "Usage: $0 [--verify|--regenerate]" >&2
	exit 2
	;;
esac
