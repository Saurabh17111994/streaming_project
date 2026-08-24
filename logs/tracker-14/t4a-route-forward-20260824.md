# T4a evidence — nautilus route → bridge sync forward (2026-08-24)

## Context

- Market was closed (post-close ticks still flowing — see note below). This is pure
  offline-codable work: route forward with the gate HALTED is behavior-neutral.
- The "T4 wall" from A2/CHG-084: `http.rs` acked 202 without executing when ENABLED.
- T4a = route forward + deterministic identity minting; the full order-lifecycle wiring
  (postback → projection → gateway `/v1/events`) remains open (A2.4 leg).

## Scope (what landed)

- `src/intent.rs` — gateway payload → bridge place `CommandEnvelope` mapper (fail-closed
  enum mapping; fresh UUID v4 attempt id; deterministic 14-hex client_order_ref).
- `src/http.rs` — `ServerState.forwarder` + async route: ENABLED forward with
  SUCCESS→202 / REJECTED→409 / UNKNOWN|transport-err→503; auth 401 + HALTED 503 unchanged;
  no forwarder → paper 202 (offline/test construction unchanged).
- `src/main.rs` — forwarder from `BridgeSelection` (Fake scripted Accept / HttpBridgeClient).
- `src/execution/client.rs` — `deterministic_client_order_ref` → `pub(crate)`.

## Verification

| Check | Result |
| --- | --- |
| `cargo test --offline --lib` | **179 pass / 0 fail** (was 168 baseline; +11 — 7 intent + 5 route) — see CHG-090 |
| `cargo test --offline` (all targets) | 193 pass / 0 fail (incl. executor_offline_contract 4/4, live_go_bridge 1/1) |
| `cargo clippy --all-targets -- -D warnings` | clean |
| `cargo fmt --check` | clean |
| HALTED + forwarder (route test) | 503 `gate HALTED`, broker id never leaked → **no forward while HALTED** |
| ENABLED + Accept forwarder (route test) | 202, `broker_order_id="BRK-0001"`, `client_order_ref` 14-hex echoed, `gate_state=ENABLED` |
| ENABLED + Unknown forwarder | 503 `outcome:UNKNOWN`, no broker id (never an ack) |
| ENABLED + Reject forwarder | 409 `outcome:REJECTED` |
| Missing `symbol` payload | 422 `symbol required` (mapping fail-closed, never forwarded) |
| Priced MARKET payload | maps to UNPRICED MKT (price dropped; protocol rejects priced MKT) |

## Notes

- **Post-close ticks** (user question): ticks still flowing at ~10:55 UTC — counter file
  `total=574,213` (fresh 10:55:10Z; per-token 20–140/min = EOD/closing prints). Full flow
  resumes 09:15+ IST.
- Identity rules: `execution_attempt_id` = UUID v4 (same rule as `build_order_envelope`);
  `client_order_ref` = `sha256("v1|instruction_id|execution_attempt_id")`[0..14] hex.
- Resilience: single attempt, no auto-retry (duplicate risk); unknown → 503 (reconcile by
  query is the separate `reconcile` module).
- Still open: (1) A2.4 lifecycle leg (WS postback → normalized event → gateway `/v1/events`);
  (2) durable attempt-store flags (B7.5) — both needed for the harness exit-0 A2.6 run;
  (3) enable decision (`EXECUTION_ENABLED`/control-plane approve) — operator-only, deferred
  to market hours with explicit go.
