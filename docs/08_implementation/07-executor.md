# Executor — SUPERSEDED

> **SUPERSEDED 2026-08-18 — integrated into [`05-execution-core.md`](./05-execution-core.md).**
> The standalone Executor design (decision-consumer → gate-store → attempt-store → broker-adapter
> calling Arrow REST directly) is replaced by the integrated Execution Core architecture:
> **Nautilus** is the execution engine (OMS, risk, reconciliation, event-store audit) and the
> **go-arrow bridge** is the ONLY component that talks to Arrow (orders + order-updates stream).
> The Executor requirements (`REQ-EXE-001`–`REQ-EXE-013`, `AC-EXE-001`–`AC-EXE-016`), the
> two-person gate / fencing / unknown-outcome-halt safety model, and the canonical test IDs
> (`EXE-*`, `ARROW-REST-*`) are unchanged and are owned by `05-execution-core.md` (order path).
> The Agent-2 offline safety-core plan (this file's appended section) is absorbed there — the
> `common` safety-core pieces already landed remain inputs to the custom gate layer. DEC-006
> (direct Arrow REST from the Executor) and the executor build contracts are flagged for
> re-scope in `05-execution-core.md` §Open gates.
>
> Full historical content is preserved in git at commit `74f3d89` (this file as of 2026-08-18).
