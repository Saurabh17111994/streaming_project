# Ingestion Test Hardening Plan

<!-- markdownlint-disable MD013 -->

> **Status:** **M1 (Safety core) landed 2026-08-15** — ING-FAIL-004 (concurrency), ING-FAIL-005 (halt-latch pinned), ING-FAIL-007 (TIME_JUMP wired), ING-DQ-010 (no-silent-drop) implemented; **ING-DQ-011 (fuzz corpus) landed early 2026-08-15** (G4 RESOLVED); **M2 (Losslessness + config) landed 2026-08-15** — ING-TCP-002 (comparator suite, 20 tests), ING-UNIT-018 (Java↔Go parity table, 11 keys), ING-UNIT-019 (C16 env-key drift check) (G3/G7 RESOLVED); **M3 (Failure-path E2E) landed 2026-08-15** — ING-FAIL-008 (crash-loop), ING-FAIL-009 (auth-failure), ING-FAIL-010 (shutdown-deadlock), ING-INT-005 (READY gating matrix, G8 RESOLVED) implemented; ingestion suite 224/0/8-skips (M3: +13). M4–M5 remain as backlog below.
>
> **Source dossier:** [`03-ingestion.md`](./03-ingestion.md) — every test below maps to one of its aspects (packet processing, fingerprint, backpressure, failure matrix, config contract, losslessness, Go bridge, telemetry, startup/readiness).
>
> **Owner:** Ingestion Team · **Author:** audit session 2026-08-15 · **Related:** [`11-testing-and-release.md`](./11-testing-and-release.md) (master test catalog), [`04_contracts/01-ingestion.md`](../04_contracts/01-ingestion.md) (build contract).

## 1. Why this plan exists

The 2026-08-15 audit verified that every requirement of `03-ingestion.md` is implemented and the suites are green (ingestion 193/0/8-skips, common 340/0/1-skip, Go bridge PASS). The audit also found **coverage gaps** — behaviors that are implemented but not pinned by a test, or code paths that exist but are never exercised:

| # | Gap | Severity | Detail |
| --- | --- | --- | --- |
| G1 | `TIME_JUMP` discontinuity is never emitted | High (code gap) | **RESOLVED in M1 (2026-08-15)** — `TimeJumpMonitor` + a periodic clock re-measurement in `IngestionService` emit one TIME_JUMP row per violation episode (ING-FAIL-007). |
| G2 | AppendTracker halt latch never auto-resets | Medium (semantic ambiguity) | **RESOLVED in M1 (2026-08-15)** — pinned as "halted until restart" (fail-closed; a capacity fault needs operator restart, not silent resume) and asserted by ING-FAIL-005. |
| G3 | `reconcile-compare.py` has zero tests | High (untested evidence tool) | **RESOLVED in M2 (2026-08-15)** — `tests/test_reconcile_compare.py` (ING-TCP-002, 20 unittest cases): every mutation class (lost/extra/vanished), missing/truncated files **fail closed** (the comparator now exits 1 on an empty bridge or post-probe file instead of vacuous PASS), chunked reports, `-1` sentinel. |
| G4 | No fuzz / property-based tests on the parse path | Medium | `processLine` is only exercised with hand-written fixtures; random/malformed input could throw uncaught or drop silently. **RESOLVED (2026-08-15)** — `FuzzIngestionTest` (ING-DQ-011): seeded generator (3 pinned seeds), no external property library; asserts no uncaught exception, no silent drop, error-counter ↔ INTERNAL_ERROR-evidence consistency, and seed-reproducibility. |
| G5 | Process-level Go bridge tests are minimal | Medium | Only `hft_policy_test.go` drives the real binary; FATAL-exit codes and pin enforcement are otherwise untested at the process level. |
| G6 | Metrics-side secret scrubbing is untested | Medium | Log scrubbing (ING-SEC-RED-001) is tested; the OTLP export body is never scanned for leaked credentials/raw payloads. |
| G7 | Config parity Java↔Go is by convention only | Medium | **RESOLVED in M2 (2026-08-15)** — a single 11-key table mirrored in `ConfigParityTest` (Java) and `hft_policy_test.go` (Go): same defaults, accepted values, rejected values (Java throws, Go exits FATAL 2). The parity test drove out a real divergence: the Go bridge read `ARROW_HFT_LATENCY_MS`/`ARROW_HFT_CONNECTIONS` inline with fail-open semantics (non-integers silently ignored) — both now flow through `hftRange`/`hftPin`. |
| G8 | READY gating has no exhaustive no-false-positive test | Low | **RESOLVED in M3 (2026-08-15)** — `ReadinessGatingMatrixTest` (ING-INT-005) walks every `HealthProbe.isReady()` dimension (liveness, Fluss, tracker, broker, subscription, data, frame recency, clock): ready only when all eight are true, false when any single one is false. |

## 2. Test inventory

Priority: **P1** = safety-critical gap or untested safety tool · **P2** = meaningful hardening · **P3** = polish/defense-in-depth.
Conventions: Java = JUnit 5 under `code/02_services/01_ingestion/src/test/java/`; Go = stdlib `testing` under `go-bridge/`; Python = `unittest` under `code/01_platform/04_scripts/tests/` (mirrors `test_audit_r2.py`). **No new third-party dependencies** — pure JDK/Go stdlib/unittest only.

### 2.1 Packet processing and data quality (dossier §Packet processing algorithm, §Golden corpus)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-DQ-010` | P1 | G4-adjacent — no global no-silent-drop invariant | Drive the full pipeline (fake bridge → Java → fake Fluss) with a mixed stream: valid ticks, malformed JSON, unknown feed, missing instrument, stale/future timestamps, 1 MB lines, NUL bytes | `accepted + quarantined + rejected == lines consumed` exactly; every line maps to exactly one evidence row (append or quarantine); zero uncaught exceptions. **Landed 2026-08-15** as `IngestionNoSilentDropTest` — **default-run**: the service is constructed with no-op evidence sinks (`QuarantineSink`/`DiscontinuitySink`/`SafetySink` test seam in the `IngestionService` constructor), so no reachable Fluss is required and the ledger is asserted from in-process append + decode-error counters, independent of any cluster | `IngestionNoSilentDropTest` |
| `ING-DQ-011` | P1 | G4 — no fuzz coverage | Seeded property-based generator produces random NDJSON lines (mutated ticks, schema violations, malformed JSON, garbage text, oversized lines, control chars) | No line throws out of `processLine`; `appends + quarantines == lines fed` (no silent drop); `INTERNAL_ERROR` evidence == error counter; corpus regenerates identically from the fixed seed; guaranteed-valid ticks always append. **Landed 2026-08-15** as `FuzzIngestionTest` (3 pinned seeds × ~810 lines, JDK `java.util.Random`, no external property library; default-run via the no-op-sink seam) | `FuzzIngestionTest` |
| `ING-UNIT-014` | P2 | Freshness-gate boundaries unpinned | `ts_ms` exactly at `maxEventAge`, exactly at `maxFutureSkew`, ±1 ms over/under, receive==broker time | FRESH/STALE/FUTURE classification exact at every boundary | extend `StaleDataTradeGuardTest` |
| `ING-UNIT-015` | P3 | Payload-hash edge cases | Empty payload, URL-safe base64, padded/unpadded variants, multi-frame large payloads | `PayloadHashValidator` accepts the valid forms and rejects the invalid ones with the typed result (R-186: SHA-256 of "" is a real digest, never dropped) | extend `PayloadHashValidatorTest` |

### 2.2 Fingerprint contract (dossier §Fingerprint contract)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-UNIT-016` | P2 | Canonical form not frozen | Golden-hash snapshot for the v1 fixture inputs | The test fails on *any* silent change to the canonical form (field order, encoding, null representation, version) — a frozen cross-commit pin | extend `FingerprintBuilderTest` |
| `ING-UNIT-017` | P3 | Dedup semantics not pinned in code | Same tick, same epoch → identical fingerprint; same tick, next epoch → different fingerprint | Documents DEC-012 best-effort identity: duplicates within an epoch are expected (Compute dedups); reconnects reset identity | extend `FingerprintBuilderTest` |

### 2.3 Backpressure and memory (dossier §Backpressure and memory, §Slow-Fluss policy)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-FAIL-004` | P1 | Tracker only tested single-threaded | 16 threads × 100k `tryAccept`/`onAppendSuccess`/`onAppendFailure` with small limits | Pending counters never negative and never exceed either limit; `totalAccepted + totalRejected == totalAppended + totalFailed + totalRejected`; `pendingBytes` equals the sum of accepted sizes | extend `AppendTrackerTest` (multithreaded invariant test) |
| `ING-FAIL-005` | P2 | G2 — halt-latch lifecycle unpinned | **Decision required first:** (a) pin "halted until restart" with a test asserting `isHalted()` stays true after draining, or (b) implement a resume path (halt clears when pending drops below the warning threshold) and test it | The documented lifecycle is asserted either way — no ambiguity | `AppendTrackerTest` + `ReadinessRecoveryTest` |
| `ING-FAIL-006` | P3 | 80% throttle unpinned | Reach ≥80% on records, then on bytes alone; re-trigger within 30 s | Warning fires once per 30 s window; readiness false at ≥80% on *either* axis | extend `AppendTrackerTest` |

### 2.4 Failure matrix (dossier §Failure matrix, §Shutdown)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-FAIL-007` | P1 | G1 — TIME_JUMP never emitted | **Code change:** on a clock-offset violation crossing `CLOCK_OFFSET_LIMIT_MS` (continuous offset monitor or bridge-restart hook), write a TIME_JUMP discontinuity row | One TIME_JUMP row per violation with the epoch + last-tick snapshot; reason mapping added to `DiscontinuityReasonMappingTest`; if the enum is removed instead, the test asserts the removal | `IngestionService`/`NtpClockChecker` + new `TimeJumpDiscontinuityTest` |
| `ING-FAIL-008` | P2 | Crash-loop only unit-tested | Scripted fake bridge binary that exits non-zero repeatedly | Java logs `BRIDGE_CRASH`, writes a DROP discontinuity, restarts exactly once, then exits 0; readiness marker cleared | ✅ landed 2026-08-15: `BridgeCrashLoopE2ETest` (scripted bridge pattern from `FullStackE2ETest`, default-run via the ING-DQ-010 no-op-sink seam; 2× BRIDGE_CRASH + 2× DROP rows, one restart, terminal shutdown, readiness marker cleared, single journal entry) |
| `ING-FAIL-009` | P2 | Auth-failure path untested E2E | Bad `ARROW_TOKEN`/revoked creds | Bridge exits 2, Java exits on pipe close, zero partial appends, `auth_failure` evidence row written | ✅ landed 2026-08-15: `BridgeAuthFailureE2ETest` (scripted bridge mirrors the real auth drain: `auth_failure` → `bridge_shutdown` → exit 2; DROP evidence with the auth reason, zero accepted ticks, no restart, clean single shutdown) |
| `ING-FAIL-010` | P3 | Shutdown deadlock not tested | Fluss ack never completes; call `shutdown()` twice | Exits within the drain deadline (no hang); journal records the exact remaining pending bytes (R-260); second `shutdown()` is a no-op (single journal entry, single drain) | ✅ landed 2026-08-15: `ShutdownDeadlockTest` — un-cancelable stuck ack, `DRAIN_DEADLINE_SECONDS` (new config key, default 30) makes the deadline provable; journal entry pins `pending_records`/`pending_bytes` (new `UncertaintyJournal.Entry` fields, R-260); duplicate shutdown writes nothing |

### 2.5 Configuration contract (dossier §Configuration contract, §Startup sequence)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-UNIT-018` | P1 | G7 — Java↔Go parity by convention only | Table-driven: for every `ARROW_HFT_*` key, feed the same accepted/rejected values to Java `exactInt`/`intRange` and Go `hftPin`/`hftRange` | Both sides agree on every accepted value, rejected value, and FATAL behavior | ✅ landed 2026-08-15: `ConfigParityTest` (Java, 4 tests) + mirror table in `hft_policy_test.go` (Go, incl. 13 exec-self FATAL cases); `ARROW_HFT_LATENCY_MS`/`ARROW_HFT_CONNECTIONS` converted to the mirrored helpers |
| `ING-UNIT-019` | P2 | Doc drift for env keys unguarded | Extend `docs_audit.py` with a new C-check: parse the dossier's config table and assert every documented key is read by `IngestionConfig` or the Go bridge | New C-check PASS (like C6 for test counts) | ✅ landed 2026-08-15: `docs_audit.py` **C16** (`c16_env_key_drift`), PASS on the current table |
| `ING-UNIT-020` | P3 | G5 — process-level FATAL exits | Build the bridge once; run with bad env (wrong pin, `ARROW_HFT_CONNECTIONS=2`, missing `ARROW_APP_ID`) | Exit code 2 + the documented FATAL message for each case | extend `hft_policy_test.go` |

### 2.6 Losslessness tooling (dossier §ING-TCP-001)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-TCP-002` | P1 | G3 — comparator untested | Synthetic tick-count fixtures with known lost / extra / vanished mutations; missing or truncated counter file | Each mutation class is detected (exact-equality mode fails on any delta); a missing/truncated file **fails closed**, never passes | ✅ landed 2026-08-15: `code/01_platform/04_scripts/tests/test_reconcile_compare.py` (20 unittest cases) + `reconcile-compare.py` fail-closed hardening |
| `ING-TCP-003` | P3 | Counter-report persistence un-pinned | 1,024-token report emitted as bounded chunks (20/line); file + stderr mirror | Both are byte-identical; the file persists even when stderr dies (SIGPIPE path); `ARROW_TICK_COUNTS_FILE` honored | extend `go-bridge/ndjson_test.go` |

### 2.7 Go bridge (dossier §Process boundary, §ING-RES-001)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-RES-002` | P2 | Plan-boundary N untested | N ∈ {1, 511, 512, 513, 1023, 1024, 1025}: build the subscription plan | Every token appears exactly once; each request ≤ `MaxHFTTokensPerRequest`; slot count ≤ configured connections; `validateRequestUnion` rejects duplicates/missing tokens | extend `subscription_plan_test.go` |
| `ING-RES-003` | P3 | Backoff determinism unpinned | `Backoff(attempt)` golden sequence | Exactly 1,2,4,8,16,30,30… s (no jitter — deterministic for soak accounting) | extend `reconnect_test.go` |
| `ING-RES-004` | P3 | NDJSON contract version un-pinned | Every emitted record (`tick`, `bridge_event`, `bridge_metrics`) carries `contract_version=2` | Schema conformance asserted across all emit paths | extend `ndjson_test.go` |

### 2.8 Telemetry (dossier §Telemetry)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-UNIT-021` | P1 | G6 — metrics scrubbing untested | Serialize a full OTLP export body after exercising every metric path | Regex scan finds no `ARROW_*` values, Bearer tokens, `raw_payload`, or credentials (mirrors ING-SEC-RED-001 for logs) | extend `OtlpMetricsEmitterTest` |
| `ING-UNIT-022` | P3 | Bounded-cardinality unpinned | Enumerate emitted metric labels | Label keys come from a fixed set; no token/symbol/high-cardinality labels | extend `OtlpMetricsEmitterTest` |

### 2.9 Startup and readiness (dossier §Startup sequence, §Health)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-INT-005` | P2 | G8 — READY gating not exhaustive | Walk every readiness dimension (fluss, tracker, broker, subscription, data, frame recency, clock) | Readiness is false when any one dimension is false and true only when all are — no false positive | ✅ landed 2026-08-15: `ReadinessGatingMatrixTest` (10 cases — all-true baseline + one flip per dimension incl. liveness, data/partial-ack, stale frame, and the fail-closed clock) |
| `ING-INT-006` | P3 | Entrypoint exit codes untested | `docker-entrypoint.sh` FATAL paths: missing `FLUSS_BOOTSTRAP` (2), missing manifest (2), missing bridge binary (1) | Exit codes and messages match the documented contract | `test_docker_entrypoint.sh` (bash harness) |

## 3. Implementation order (milestones)

Each milestone ends with its full suite green and the catalog updated — nothing ships half-done.

| Milestone | Tests | Scope |
| --- | --- | --- |
| **M1 — Safety core** ✅ landed 2026-08-15 | `ING-FAIL-007`, `ING-FAIL-005`, `ING-FAIL-004`, `ING-DQ-010` | Closed the two code gaps (TIME_JUMP wired; halt-latch pinned "until restart"), pinned the tracker under concurrency, proved the no-silent-drop invariant. Suite 204/0/8-skips. |
| **M2 — Losslessness + config** ✅ landed 2026-08-15 | `ING-TCP-002`, `ING-UNIT-018`, `ING-UNIT-019` | Comparator suite (20 tests, fail-closed hardening), 11-key Java↔Go parity table (drove out the LATENCY_MS/CONNECTIONS fail-open reads — both now hftRange/hftPin), docs-audit C16 env-key drift check; all wired into `run-monday-gates.sh` + `run-full-suite.sh` so comparator/env-key regressions fail CI. Suite 211/0/8-skips. |
| **M3 — Failure-path E2E** ✅ landed 2026-08-15 | `ING-FAIL-008`, `ING-FAIL-009`, `ING-FAIL-010`, `ING-INT-005` | Crash-loop, auth-failure, shutdown-deadlock, and the READY gating matrix. Two small code changes shipped with the tests: `DRAIN_DEADLINE_SECONDS` config key (deadline was hardcoded 30 s — the deadlock test proves the deadline is honored) and `pending_records`/`pending_bytes` on the uncertainty journal entry (R-260 exact-bytes pin). Suite 224/0/8-skips. |
| **M4 — Go bridge + data quality** | ~~`ING-DQ-011`~~ ✅ landed 2026-08-15, `ING-UNIT-014`, `ING-UNIT-015`, `ING-UNIT-016`, `ING-UNIT-017`, `ING-RES-002`, `ING-RES-003`, `ING-RES-004`, `ING-TCP-003` | Fuzz corpus (**ING-DQ-011 done — `FuzzIngestionTest`**), golden pins, plan boundaries, backoff, contract version. |
| **M5 — Telemetry + ops** | `ING-UNIT-021`, `ING-UNIT-022`, `ING-INT-006` | Metrics scrubbing + cardinality, entrypoint exit codes. |

## 4. Definition of done

1. Every test in §2 passes with **0 failures** on the default run; env-gated tests skip cleanly (repo pattern: `FLUSS_BOOTSTRAP`, `INGESTION_INT_TEST_*`).
2. **No new third-party dependencies** — pure JDK / Go stdlib / Python `unittest` only.
3. The two code gaps are closed **or** explicitly pinned: G1 (TIME_JUMP wired and tested, or the enum removed) and G2 (halt-latch lifecycle decided and asserted).
4. Catalog + counts updated: each new `ING-*` ID added to [`11-testing-and-release.md`](./11-testing-and-release.md) §Ingestion; the "Executable tests" totals in `11-testing-and-release.md`, `00-start-here.md`, and `03-ingestion.md` updated; `docs_audit.py` C6 line re-measured.
5. A change record is filed under `docs/05_deployment/change-records/` following the CHG-0xx convention (test_updates + rollback_behavior), referencing this plan.
6. Evidence pattern honored: any live/evidence-backed claim cites its log path; unit tests never use real broker credentials.

## 5. Out of scope (explicitly deferred)

- The accepted deferrals of `03-ingestion.md` (daily `GET /nse`/`GET /all` manifest refresh; 3,000-instrument / 3-connection envelope) — unchanged by this plan.
- Perf-gate changes beyond the existing `ING-PERF-001` certification (no new envelope runs; DEC-036/037 stand).
