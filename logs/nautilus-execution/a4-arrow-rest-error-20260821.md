# A4 — Arrow REST capability matrix (error half) (2026-08-21)

Master-plan Task A4 — DoD: matrix row 8 → VERIFIED; tests green (error half; success half needs live BI-EQ ×1).

**What was proven**

- `A4.1` — error half captured: auth 401 → ReauthBroker one retry → `broker_disabled`; 15 s UNKNOWN timeout (no retry); one-attempt-to-one-order (RequestID + fingerprint coalesce vs `reuse_violation`). New `arrow_capability_test.go` 4 legs.
- `A4.2` — `TestArrowRestCapability` (Go, fake broker) — `go test -race` PASS 1.12s.

**Disposition**

Success half (`BI-EQ ×1` live order) deferred to market-hours A2/A3 — `TO_BE_VERIFIED` remains for the success half. Error half fully VERIFIED on the fake bridge.

**Evidence**

- Change record: CHG-075.
- Files: `code/02_services/01_ingestion/go-bridge/arrow_capability_test.go`.
