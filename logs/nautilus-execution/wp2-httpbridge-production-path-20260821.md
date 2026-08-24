# WP-2 / B9 — HTTP bridge transport into production path (2026-08-21)

Master-plan Task B9 (WP-2 remainder) — DoD: production bridge transport is what runs when `BRIDGE_ENDPOINT` is configured; offline default unchanged; gate boots HALTED; selection unit-tested + live-interop-proven.

**What was done**

- `B9.1` — `BridgeSelection { Fake | Http{base_url, auth_token} }`, `from_config(&ServiceConfig)`, `mode()` (never logs token); `BridgeExecutionClientFactory` holds a selection (default `Fake`); `create()` picks `FakeBridge` or `HttpBridgeClient` (lazy connect = constructible offline). `LiveNodeRuntime::build_with_bridge(selection)`; `build()` keeps Fake default.
- `B9.2` — config: `BRIDGE_AUTH_TOKEN` → `bridge_auth_token` (default empty). `main.rs` derives + logs bridge mode. Compose passes `BRIDGE_AUTH_TOKEN` from the bridge's own `${EXECUTION_BRIDGE_AUTH_TOKEN:-local-only}` so both sides agree (`BRIDGE_ENDPOINT` already set).
- `B9.3` — tests: +5 executor (token read; selection fake-without-endpoint; selection http-with-token; factory http-create offline; runtime http-build halts). Suite green `cargo test --offline --lib` 153 PASS (was 148). **Live interop:** `HttpBridgeClient` round-trips the real Go fake bridge — `cargo test --offline --test live_go_bridge` 1 PASS; bridge healthz `{"mode":"fake","status":"UP"}`.
- `B9.4` — stale "take_reports returns None" doc note closed (§2, 20-...-plan).

**Disposition**

Production HTTP transport selectable in the real service path; gate still boots HALTED (no orders possible); offline default unchanged.

**Evidence**

- Change record: CHG-079.
- Files: `code/02_services/04_executor/src/bridge/transport.rs`, `bridge/selection.rs` (+ factory/runtime wiring), `tests/live_go_bridge.rs`.
