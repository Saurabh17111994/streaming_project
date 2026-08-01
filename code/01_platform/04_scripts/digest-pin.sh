#!/usr/bin/env bash
# digest-pin.sh — resolve mutable image tags to immutable digests.
#
# Usage:
#   ./digest-pin.sh apache/fluss:0.9.1-incubating
#   → apache/fluss:0.9.1-incubating@sha256:...
#
# Use this to fill the digest fields in runtime.lock before any production
# deployment. CI SHALL reject any runtime.lock that still contains a bare
# semantic tag without a digest.
#
# Requires: docker or skopeo or crane (tried in that order).

set -euo pipefail

if [ $# -lt 1 ]; then
	echo "Usage: $0 <image:tag> [<image:tag> ...]" >&2
	echo "  Resolves each image:tag to image:tag@sha256:digest" >&2
	exit 2
fi

resolve_one() {
	local img="$1"
	local digest

	if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
		digest=$(docker manifest inspect "$img" 2>/dev/null |
			grep -o '"sha256:[a-f0-9]\{64\}"' | head -1 | tr -d '"')
	elif command -v skopeo &>/dev/null; then
		digest=$(skopeo inspect "docker://${img}" 2>/dev/null |
			grep -o '"sha256:[a-f0-9]\{64\}"' | head -1 | tr -d '"')
	elif command -v crane &>/dev/null; then
		digest=$(crane digest "$img" 2>/dev/null)
	else
		echo "ERROR: no resolver available (docker/skopeo/crane). Install one." >&2
		return 1
	fi

	if [ -z "$digest" ]; then
		echo "ERROR: could not resolve digest for $img" >&2
		return 1
	fi

	echo "${img}@${digest}"
}

rc=0
for img in "$@"; do
	if ! resolve_one "$img"; then
		rc=1
	fi
done
exit $rc
