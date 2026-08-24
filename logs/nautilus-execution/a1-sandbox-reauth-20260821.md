# A1 — Sandbox auth + auto re-auth harness (2026-08-21)

Master-plan Task A1 — DoD: re-auth unit-tested; `/healthz` reflects disabled on auth failure; no test regression.

**What was done**

- `A1.1` — read `broker.go` + `fake_arrow_broker_test.go`: no re-auth stub existed; `classifySDKError` treated 401 as UNKNOWN but never retried; `brokerFromEnvironment` did AutoLogin once at startup only.
- `A1.2` — new `reauth.go` (`ReauthBroker`: one `reauth` + one retry, then `broker_disabled` + `IsDisabled()`); `server.go` health now returns `UP disabled` + 503 when the ReauthBroker is disabled; `main.go` live mode wraps `ArrowBroker` with a fresh-TOTP `AutoLogin` closure.
- `A1.3` — new `broker_reauth_test.go` (`TestSandboxAutoReauth`, 4 legs: one_reauth_then_success, reauth_failure_then_disabled, second_401_after_reauth_also_disabled, non_auth_error_no_reauth; `countingBroker` + injected `reauth`, no real Arrow).
- `A1.4` — `go test -race -run TestSandboxAutoReauth` PASS (1.01s, 4/4); full bridge suite PASS (0.11s).

**Disposition**

Live `AutoLogin` round-trip against the sandbox auth endpoint (needs `ARROW_*` credentials, off-hours) deferred — recorded honestly; the re-auth state machine is unit-proven.

**Evidence**

- Change record: CHG-074.
- Files: `code/02_services/01_ingestion/go-bridge/reauth.go`, `broker_reauth_test.go`, `server.go`, `main.go`.
