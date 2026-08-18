# Execution Arrow Bridge

This is a separate, order-path-only Go process. It is the only component in
the execution topology that may hold Arrow credentials or call Arrow REST and
the Arrow order-update WebSocket.

## Safety posture

- Default mode is `disabled`; it is healthy but not ready for broker commands.
- `fake` mode is offline-only and is used by tests.
- `live` mode requires explicit `EXECUTION_BRIDGE_MODE=live` and Arrow
  credentials in this process. It is not a trading enablement decision.
- Ambiguous place outcomes are returned as `UNKNOWN` and are never retried by
  this process.
- This module must not be merged into or invoked by the market-data bridge.

## Private protocol

Commands are authenticated with `Authorization: Bearer <token>`:

- `POST /v1/commands` — place, modify, cancel, query-order, and reconciliation
  commands.
- `GET /v1/events` — authenticated WebSocket for normalized postbacks.
- `GET /healthz` and `GET /readyz` — process/readiness probes.

The wire contract is version `1`. `client_order_ref` is validated to the Arrow
`remarks` limit of 16 safe ASCII characters. `instruction_id`,
`execution_attempt_id`, `client_order_ref`, and `broker_order_id` remain
separate identities.

## Local run

```bash
EXECUTION_BRIDGE_AUTH_TOKEN=local-only \
EXECUTION_BRIDGE_MODE=disabled \
go run .
```

No live credentials are needed for the disabled mode. Run tests with:

The container sets `EXECUTION_BRIDGE_LISTEN_ADDR=0.0.0.0:8787`; a host run
defaults to `127.0.0.1:8787`.

```bash
go test -race ./...
```

The module uses the exact vendored SDK tree from
`../../01_ingestion/go-bridge/third_party/go-arrow`; the market-data bridge is
not modified by this module.
