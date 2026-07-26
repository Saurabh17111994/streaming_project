#!/usr/bin/env python3
"""OpenAlgo executor (MVP scaffold).

Tail-subscribes to the Trade_management_table changelog and places orders via the
OpenAlgo REST API when a NEW/CHANGED instruction row appears. READ-ONLY on Fluss.

This is a scaffold: the changelog consumer and order-placing logic are implemented
in the Phase 4.2 build (../../design/roadmap/4-2-mvp/tasks.md §4.3).
"""
import os

FLUSS_BOOTSTRAP = os.environ.get("FLUSS_BOOTSTRAP", "fluss-coordinator:9123")
OPENALGO_BASE_URL = os.environ.get("OPENALGO_BASE_URL", "http://openalgo:5000")
OPENALGO_API_KEY = os.environ.get("OPENALGO_API_KEY", "")


def main() -> None:
    print(f"executor: scaffold start (fluss={FLUSS_BOOTSTRAP}, oa={OPENALGO_BASE_URL})")
    # TODO(4.2 §4.3): tail Trade_management_table changelog; on instruction row ->
    #   POST order to OpenAlgo; never write back to Fluss.
    raise NotImplementedError("executor not yet implemented (scaffold)")


if __name__ == "__main__":
    main()
