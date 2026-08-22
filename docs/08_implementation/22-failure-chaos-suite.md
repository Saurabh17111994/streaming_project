# 22 — Failure chaos suite (T13, G5 Ops)

**Status:** implemented (built against the T1/T3/T5/T6/T7/T9-state code; the
live-cluster and Swarm legs are *not* executed here — deployment runs are
executed later, serially, by the main agent).

The four planned failure tests (plan `streaming-3000-flink-fluss-hardening.md`
§5 T13 "Add slot kill, TM kill, tablet kill, VM loss as L11 gates") live as
env-gated integration tests + gate scripts, driven by ONE runner. They are
deliberately layered the same way the repo's L0–L11 suite is: the
deterministic core runs anywhere (Go unit tests, embedded MiniCluster), the
cluster-shaped assertions run against the live stack / Swarm and SKIP
cleanly when the prerequisite is absent — because the deployment tests run
later, serially, by the main agent.

## Files

| Path | Role |
|---|---|
| `code/01_platform/04_scripts/chaos/chaos-run.sh` | Runner: static self-check, then tests 1→4 in order, PASS/FAIL/SKIP + exit codes |
| `code/01_platform/04_scripts/chaos/chaos-01-slot-kill.sh` | Test 1 — slot kill (Go bridge) |
| `code/01_platform/04_scripts/chaos/chaos-02-tm-kill.sh` | Test 2 — TM kill (Flink checkpoint) |
| `code/01_platform/04_scripts/chaos/chaos-03-tablet-kill.sh` | Test 3 — tablet kill (Fluss LOG) |
| `code/01_platform/04_scripts/chaos/chaos-04-vm-loss.sh` | Test 4 — VM loss (Swarm overlay) |
| `code/02_services/02_compute/src/test/.../SignalJobTaskManagerKillIntegrationTest.java` | SIG-TMKILL-001 MiniCluster IT (test 2, offline leg) |
| `code/02_services/02_compute/src/test/.../TabletKillChaosIntegrationTest.java` | DUR-TABLETKILL-001 live IT (test 3) |
| `Makefile` target `chaos-suite` | `make chaos-suite` == runner |

No production code (Java/Go) and no compose/stack/otel/dashboard files were
touched — tests reference them read-only. `docs/08_implementation/08-local-compose.md`
L8 `FAIL-*`/`DUR-*` lanes stay as-is; this suite is the T13 "L11 gate" row on
top of them.

## Invocation

```bash
# everything, in order (01→04)
make chaos-suite                        # or: code/01_platform/04_scripts/chaos/chaos-run.sh

# individual tests
code/01_platform/04_scripts/chaos/chaos-01-slot-kill.sh     # offline, always runs
code/01_platform/04_scripts/chaos/chaos-02-tm-kill.sh       # offline IT + optional live leg
code/01_platform/04_scripts/chaos/chaos-03-tablet-kill.sh   # live stack required (SKIP 3)
code/01_platform/04_scripts/chaos/chaos-04-vm-loss.sh       # multi-node swarm required (SKIP 3)
```

Exit codes: **0 PASS, 1 FAIL, 3 SKIP** (prerequisite absent). The runner
exits 0 when nothing FAILed, 1 with a `CHAOS-SUITE: RESULT=FAIL EXIT=1`
sentinel when any test failed. Evidence lands under
`logs/chaos/chaos-<ts>/` (per-test logs + `SUMMARY.txt`).

## Expected output shape

```text
=== [1/4] 01 slot kill — Go bridge, 1-of-3 slots ===
SLOT-KILL-CHAOS-01: PASS — 1-of-3 slot kill: terminal isolated, peers healthy, reconnect clean
RESULT [1]: PASS
=== [2/4] 02 TM kill ... ===
TM-KILL-CHAOS-02: [leg A] PASS — restore from checkpoint, no duplicate fingerprint
TM-KILL-CHAOS-02: [leg B] SKIP — no flink-taskmanager container (stack not up)
RESULT [2]: PASS
=== [3/4] 03 tablet kill ===
TABLET-KILL-CHAOS-03: SKIP — no fluss-tablet container (start the stack; ...)
RESULT [3]: SKIP
=== [4/4] 04 VM loss ===
VM-LOSS-CHAOS-04: SKIP — this docker daemon is not in an active swarm (...)
RESULT [4]: SKIP
CHAOS-SUITE: RESULT=PASS EXIT=0
```

On a machine with the full stack + M3 swarm, tests 03/04 run their live
assertions instead of skipping.

## Test 1 — slot kill (Go bridge)

Script: `chaos-01-slot-kill.sh`. Offline, deterministic, no cluster/broker.
Runs the named Go tests that implement the chaos:

| Go test | Assertion the gate relies on |
|---|---|
| `TestSubscriptionPlanShards3000Tokens` | 3000 tokens → deterministic 1024+1024+952 (T1 baseline) |
| `TestSupervisorAuthTerminalIsolatedPerSlot` | slot-0 driven TERMINAL → exactly 1 terminal outcome, shared context NOT cancelled, peer sockets open, peer ticks flow (no cross-slot kill) |
| `TestINGRES001HealthySlotNotInterruptedByPeerReconnect` | slot-0 drop-cycles via the real SDK while slot-1 stays ACTIVE with zero disconnect events (no peer drop regression) |
| `TestReconnectLoopRecoversAfterFailures`, `TestReconnectLoopEpochAndBackoffAfterForcedDisconnect` | killed slot reconnects with a fresh epoch |
| `TestINGRES001OneHundredForcedDisconnectReconnectCycles` | 100 forced disconnect/reconnect cycles: every cycle recovers to ACTIVE, no goroutine/FD/socket leak (drop-missed resource regression) |

Invariant (plan §6 row 1): kill 1 of 3 slots → reconnect, no drop-missed
regression, peers stay healthy.

## Test 2 — TM kill (Flink)

Script: `chaos-02-tm-kill.sh`. Two legs:

- **Leg A (always, offline):** `SignalJobTaskManagerKillIntegrationTest`
  (SIG-TMKILL-001) — MiniCluster with 2 TMs, the real
  `FingerprintDedupFunction` (the gate in front of candle emission and
  signal detection), one TM hard-terminated (`terminateTaskManager`, the
  same failover path as `docker kill` on the TM). Asserts: job leaves
  RUNNING and comes back RUNNING restored from the last checkpoint; a fresh
  checkpoint completes after restore; the legacy source re-emits its whole
  feed after restore and the restored first-write-wins markers suppress
  every replayed row — **no duplicate fingerprint** ever leaves the dedup
  boundary (the plan's "no dup candle / no dup signal" invariant; the
  KV-side `CandleKvFirstWriteWinsFunction` guard adds a second layer
  verified live in leg B).
- **Leg B (optional, live stack):** SIGKILL the taskmanager container,
  assert container restart (compose/swarm restart policy), job RUNNING
  again, a completed checkpoint strictly newer than the pre-kill one
  (restore from last checkpoint, then live), and the job's own
  `compute.candles.duplicate_window` counter does not advance across the
  failover replay.

Env: `FLINK_REST_URL` (default `http://localhost:8081`),
`TM_METRICS_URL` (default `http://localhost:9250/metrics`).

## Test 3 — tablet kill (Fluss)

Script: `chaos-03-tablet-kill.sh` → `TabletKillChaosIntegrationTest`
(DUR-TABLETKILL-001), env-gated `COMPUTE_INT_TEST_TABLET_KILL=true`, live
stack required (SKIP 3 otherwise). Flow: write N acked rows to `raw_table_1`
with the same fluss-client append path the bridge uses (append futures
complete BEFORE the kill); `docker kill -s KILL` the tablet container
(auto-discovered like `repair-tablet.sh`, `TABLET_CONTAINER` overrides);
wait for the restart policy + table readability; assert every acked
fingerprint is still readable and the immutable LOG count never shrank.
Reports the coordinator-stamped `table.replication.factor`; the prod x3
contract is asserted when `CHAOS_REPLICATION_REQUIRED=true`
(`CHAOS_REPLICATION_MIN`, default 3) — set on prod-like Swarm, keep false on
the single-tablet dev compose. A truncated tail (repair-tablet.sh symptom)
surfaces as a missing acked row / failed scan → FAIL.

Env: `FLUSS_BOOTSTRAP`, `TABLET_CONTAINER`, `TABLET_KILL_ROWS` (default 25),
`CHAOS_REPLICATION_REQUIRED`, `CHAOS_REPLICATION_MIN`.

## Test 4 — VM loss (Swarm)

Script: `chaos-04-vm-loss.sh`. Multi-node Swarm required (SKIP 3: single-node
swarms cannot lose their only survivor; local compose has no swarm at all).
Selects a workload node (`CHAOS_WORKLOAD_NODE` overrides; default: worker ≠
manager) and a replicated workload service running on it (`CHAOS_SERVICE`
overrides). Two modes:

- `CHAOS_VM_OFF_MODE=drain` (default) — `docker node update --availability
  drain`, automatic `active` restore on exit (trap).
- `CHAOS_VM_OFF_MODE=poweroff` — runs `CHAOS_VM_OFF_CMD` (e.g. an SSH
  poweroff of the real VM); restore is manual via `CHAOS_VM_ON_CMD`.

Asserts (T0 = loss injection): **order halt < 5 s** — `CHAOS_ORDER_PROBE_TCP`
(host:port of the order-facing endpoint that depends on the killed node)
stops answering within 5000 ms; skipped with a note when unset. **Data
recovery < 30 s** — the watched service re-schedules a Running task on a
surviving node within 30000 ms (overlay re-route). Evidence: `docker service
ps` dumps before/after + node list under `logs/chaos/.../chaos-04-vm-loss/`.

## Mapping to the plan and existing lanes

| Task | Docs | Test ID | Plan §6 invariant |
|---|---|---|---|
| T13 test 1 slot kill | this doc | L11 gate (Go) | No cross-slot kill |
| T13 test 2 TM kill | this doc | SIG-TMKILL-001 (extends L8 FAIL-002) | TM death → restore → no dup window/signal |
| T13 test 3 tablet kill | this doc, 02-schema-storage.md | DUR-TABLETKILL-001 (extends L8 FAIL-004, DUR-002) | Tablet death → LOG x3 → no acked-row loss |
| T13 test 4 VM loss | this doc, 09-production-swarm.md | SWARM-FAIL/DUR-003 style (M3) | VM loss → <30 s recovery, <5 s halt, overlay re-route |
| T13 acceptance | plan §5 row | 4 failures: no dup candle/signal, halt <5 s | Swarm 3k ×30 (main agent, serial, later) |

## Validation performed

- `bash -n` clean on all five scripts.
- `shellcheck -S warning` clean where shellcheck is available on the host.
- `mvn -o test-compile` on `02_compute` (both new ITs compile; no main
  sources touched).
- Go sources untouched — the Go leg only *selects* existing named tests.
