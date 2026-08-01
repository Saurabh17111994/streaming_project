# Segment Build Contract — Executor

## Boundary

Executor is the only component allowed to initiate money-moving calls. It calls Arrow's REST API directly (`https://edge.arrow.trade`). There is no intermediate OpenAlgo layer (DEC-006 revised).

## Inputs and owned state

Inputs: immutable `Trade_Decisions`; durable `Safety_Halt_Requests`; future immutable `Position_Actions`; lifecycle/position/changelog health for validation.

Owned Fluss state: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and immutable `Execution_Audit`. Executor never mutates strategy fields.

## Gate

Default/restart-uncertain state is `HALTED`. States are `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED`, with `ENABLED → HALTED` on uncertainty. Every call validates current gate epoch. Halt blocks calls within five seconds.

Resume requires broker/order, position/fill, offsets/continuity, Signal checkpoint, and unknown-attempt reconciliation, followed by two distinct authorized approvals of the same evidence hash/epoch.

## Attempt protocol

Persist `PREPARED` attempt, immutable request hash, `client_order_ref` (deterministic, ≤16 chars for Arrow `remarks` field), and gate epoch before calling Arrow REST. Timeout, disconnect, malformed response, crash window, or ambiguous response produces `UNKNOWN`, halts, and forbids automatic retry until broker non-acceptance or verified idempotency is proven.

## Order API (Arrow REST, confirmed)

- Place: `POST /order/regular` — `{exchange, symbol, quantity, transactionType: "B"/"S", order: "LMT"/"MKT", product: "I"/"C"/"M", price, validity: "DAY"/"IOC", remarks (max 16 chars), mpp (bool)}`. Response: `{status:"success", data:{orderNo, requestTime}}`
- Modify: `PATCH /order/regular/{id}`
- Cancel: `DELETE /order/regular/{id}`
- Detail: `GET /order/{id}` — full lifecycle history with `orderStatus`, `reportType`, `exchangeOrderID`, `fillShares`, `averagePrice`
- Order book: `GET /user/orders`
- Trade book: `GET /user/trades` — all fills with `fillPrice`, `fillQuantity`, `fillTime`, `fillID`
- Positions: `GET /user/positions`
- Auth: `appID` + `token` headers; token from `/auth/app/authenticate-token`, 24hr TTL, refreshable
- Rate limit: 10 req/sec per endpoint
- MKT orders disabled by default; use `mpp:true` for upper-limit routing
- Order lifecycle: PENDING → OPEN → COMPLETE (filled) / CANCELLED / REJECTED. TRIGGER_PENDING for stop orders
- Product codes: `I`=MIS (intraday, auto-squared 3:15 PM), `C`=CNC (delivery, T+1), `M`=NRML (F&O)
- Exchanges in scope: NSE, NFO, MCX. INDEX is market data only — Executor must reject INDEX instructions

## Concurrency and fencing

One fenced active Executor owns each `execution_partition_id`. The owner holds a durable fencing epoch/token. Every attempt stores the gate epoch and fencing token. Immediately before an Arrow REST call, Executor SHALL verify current gate state, gate epoch, fencing ownership/token, durable attempt phase, and required health evidence. Lease loss, token mismatch, storage uncertainty, network partition, or stale ownership prevents the call and moves the affected gate to `HALTED`.

## Safety-halt control

Executor SHALL consume durable, authenticated `Safety_Halt_Requests` from Signal, Action Capture, platform health, and authorized operators. Each request SHALL include `halt_request_id`, account/portfolio/execution scope, source component/instance, reason code, detection time, source epoch/version, evidence hash, and schema version. Requests SHALL be idempotent. Executor SHALL apply or reject each request with immutable audit evidence, incrementing the gate epoch on an applied halt. Stale, malformed, or cross-scope requests are rejected and audited. Executor SHALL independently detect stale mandatory health even if the halt-request stream is unavailable.

## Reconciliation capability

Reconciliation uses Arrow REST endpoints (DEC-023): `GET /user/orders`, `GET /user/trades`, `GET /user/positions`, and `GET /order/{id}`. These provide near-real-time data for the two-person resume protocol. Consistency delay and rate limits (10 req/sec) must be measured.

## Acceptance

Crash-window, duplicate, timeout, rejection, malformed response, missing mapping, changelog gap, restart/corrupt state, fencing, safety-halt idempotency/scope, unauthorized/mismatched approval, two-person resume, reconciliation capability, and seven-year reconstruction tests pass. The current `NotImplementedError` scaffold is a live-money blocker.

## Requirement traceability

- Functional: `REQ-EXE-001` through `REQ-EXE-013`
- Cross-cutting: `03-non-functional.md` §§3.1–3.8; `04-data.md` §§4.2–4.4, 4.6–4.7; `05-interfaces.md` §§5.7–5.9, 5.11; `06-operational.md` §§6.2–6.10

See `../02_requirements/02-functional/07-executor.md`.
