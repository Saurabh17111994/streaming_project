#!/usr/bin/env python3
"""Executor (MVP scaffold).

Tail-subscribes to the Trade_Decisions changelog and places orders via the
Arrow REST API when a new immutable instruction appears. Executor owns and
writes: Execution_Gate, Execution_Attempts, Order_Correlation, Execution_Audit.
It never mutates strategy/candidate/ranking fields.

This is a scaffold: the changelog consumer and order-placing logic are not
yet implemented. See docs/08_implementation/components/05-executor.md for
the full implementation contract.
"""

import os

FLUSS_BOOTSTRAP = os.environ.get("FLUSS_BOOTSTRAP", "fluss-coordinator:9123")
ARROW_REST_URL = os.environ.get("ARROW_REST_URL", "")
ARROW_APP_ID = os.environ.get("ARROW_APP_ID", "")
ARROW_TOKEN = os.environ.get("ARROW_TOKEN", "")


def _env_bool(name: str, default: str = "false") -> bool:
    """R-210: case-insensitive boolean env parsing.

    The old `== "true"` check silently treated TRUE / True / 1 as disabled
    — and this flag gates real order execution, so a wrong read must be
    impossible. Accepted true values: true/1/yes/on (any case).
    """
    return os.environ.get(name, default).strip().lower() in {"true", "1", "yes", "on"}


EXECUTION_ENABLED = _env_bool("EXECUTION_ENABLED")


def main() -> None:
    enabled = "ENABLED" if EXECUTION_ENABLED else "DISABLED"
    print(
        f"executor: scaffold start (fluss={FLUSS_BOOTSTRAP}, "
        f"arrow={ARROW_REST_URL}, execution={enabled})"
    )
    # TODO: tail Trade_Decisions changelog; consume immutable instructions;
    #   enforce durable gate before every Arrow REST call; persist attempts
    #   and identity mappings; halt on unknown outcome; reconcile before retry.
    raise NotImplementedError("executor not yet implemented (scaffold)")


if __name__ == "__main__":
    main()
