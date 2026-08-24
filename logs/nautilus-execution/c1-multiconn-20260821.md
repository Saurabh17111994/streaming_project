# C1 — Bridge fan-out implementation (2026-08-21)

Master-plan Task C1 — DoD: `N>1` streams N×1,024 instruments; single-socket path unchanged; tests green.

**What was done**

- `C1.1` — fan-out was already implemented (`subscription_plan.go` `MaxHFTConnections=3` round-robin sharding, `hft_slot.go` per-slot Backoff, `supervisor.go` independent reconnect). Plug-and-play lift: `hftPolicyTable` 1,1,1,1 → -1,1,1,3 and `main.go` hftPin → hftRange(1..`MaxHFTConnections`). Default 1 preserves single-socket behavior.
- `C1.2` — N=1 bit-identical proven (deterministic sharding test + full Go suite PASS with default 1).
- `C1.3` — new `c1_plug_and_play_test.go`: `TestTokenShardingDeterministic` (2,433 → 3 slots deterministic, N=1 stable, input-order independent), `TestPerSocketReconnectIsolation` (3 slots disjoint, per-slot Backoff), `TestAggregateCounts` (N=1 → 1,024; N=2 → 2,048; N=3 → 2,433/3,072 with caps).
- `C1.4` — `go test -count=1` PASS (18.6 s) in ingestion bridge; 06_execution_bridge is the order bridge (no HFT), no change needed.

**Disposition**

Envelope lift ready: default 1 = Normal behavior; set 3 on a Premium day and restart (human decision, C5).

**Evidence**

- Change record: CHG-066.
- Files: `code/02_services/01_ingestion/go-bridge/c1_plug_and_play_test.go`, `subscription_plan.go`, `main.go`.
