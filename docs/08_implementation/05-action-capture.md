# Action Capture — SUPERSEDED

> **SUPERSEDED 2026-08-18 — integrated into [`05-execution-core.md`](./05-execution-core.md).**
> The standalone Action Capture service design (postback-adapter → decoder → correlation →
> projections) is replaced by the integrated Execution Core architecture: **Nautilus** provides
> the order-lifecycle and position-projection machinery, and the **go-arrow bridge** consumes the
> Arrow order-updates WebSocket as the postback source. The Action Capture requirements
> (`REQ-AC-001`–`REQ-AC-013`, `AC-AC-001`–`AC-AC-017`) and canonical test IDs are unchanged and
> are owned by `05-execution-core.md` (capture path).
>
> Full historical content is preserved in git at commit `74f3d89` (this file as of 2026-08-18).
