# Ingestion Test Hardening Plan

<!-- markdownlint-disable MD013 -->

> **Status:** **M1 (Safety core) landed 2026-08-15** — ING-FAIL-004 (concurrency), ING-FAIL-005 (halt-latch pinned), ING-FAIL-007 (TIME_JUMP wired), ING-DQ-010 (no-silent-drop) implemented; **ING-DQ-011 (fuzz corpus) landed early 2026-08-15** (G4 RESOLVED); **M2 (Losslessness + config) landed 2026-08-15** — ING-TCP-002 (comparator suite, 20 tests), ING-UNIT-018 (Java↔Go parity table, 11 keys), ING-UNIT-019 (C16 env-key drift check) (G3/G7 RESOLVED); **M3 (Failure-path E2E) landed 2026-08-15** — ING-FAIL-008 (crash-loop), ING-FAIL-009 (auth-failure), ING-FAIL-010 (shutdown-deadlock), ING-INT-005 (READY gating matrix, G8 RESOLVED) implemented; **M4 (Go bridge + data quality) landed 2026-08-15** — ING-UNIT-014 (freshness boundary matrix), ING-UNIT-015 (payload-hash edge cases), ING-UNIT-016/017 (canonical golden pin + DEC-012 dedup semantics), ING-RES-002 (plan boundaries), ING-RES-003 (backoff golden sequence), ING-RES-004 (contract v2 on all emit paths), ING-TCP-003 (chunked counter-report persistence) implemented; ingestion suite 231/0/8-skips (M3: +13, M4 Java: +7); **M5 (Telemetry + ops + remaining polish) landed 2026-08-15** — ING-UNIT-021 (OTLP payload scrubbing, G6 RESOLVED; drove out a real leak: decode-error reason labels carried raw `ARROW_TOKEN=`/Bearer strings into the export body — now scrubbed + fail-closed collapse), ING-UNIT-022 (bounded label-key set pinned exactly), ING-INT-006 (entrypoint FATAL exit codes + messages), ING-FAIL-006 (30 s warning-window throttle) and ING-UNIT-020 (missing-`ARROW_APP_ID` + FATAL message text) implemented; ingestion suite 234/0/8-skips (M5 Java: +3, plus the Go exec-self cases and the entrypoint bash harness). **All five milestones of this plan are now landed.**
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
| G5 | Process-level Go bridge tests are minimal | Medium | **RESOLVED (2026-08-15)** — ING-UNIT-018 (M2) added 13 exec-self subprocess FATAL cases to `hft_policy_test.go` (wrong pins incl. `ARROW_HFT_CONNECTIONS=2`, non-integers, out-of-range tunables → exit 2); ING-UNIT-020 (M5) closed the remainder — missing-`ARROW_APP_ID` startup FATAL (exit 2 + `missing required env: ARROW_APP_ID`) and the documented FATAL message text asserted on the exec-self cases (helper env stripped of policy keys so a polluted test environment cannot mask the FATAL). |
| G6 | Metrics-side secret scrubbing is untested | Medium | Log scrubbing (ING-SEC-RED-001) is tested; the OTLP export body is never scanned for leaked credentials/raw payloads. **RESOLVED in M5 (2026-08-15)** — `OtlpMetricsEmitterTest.exportBodyScrubsSecrets` (ING-UNIT-021) serialized a full export body and found the decode-error reason labels carrying raw `ARROW_TOKEN=`/Bearer strings to the collector; the emitter now scrubs reason labels (ING-SEC-RED-001 pattern family) and fails closed to `REDACTED` on any surviving marker. |
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
| `ING-UNIT-014` | P2 | Freshness-gate boundaries unpinned | `ts_ms` exactly at `maxEventAge`, exactly at `maxFutureSkew`, ±1 ms over/under, receive==broker time | FRESH/STALE/FUTURE classification exact at every boundary | ✅ landed 2026-08-15: `StaleDataTradeGuardTest.exactBoundaryMatrix` — 7-case walk (receive==broker time, ±1 ms under/over both limits; both exact boundaries inclusive-FRESH) |
| `ING-UNIT-015` | P3 | Payload-hash edge cases | Empty payload, URL-safe base64, padded/unpadded variants, multi-frame large payloads | `PayloadHashValidator` accepts the valid forms and rejects the invalid ones with the typed result (R-186: SHA-256 of "" is a real digest, never dropped) | ✅ landed 2026-08-15: `PayloadHashValidatorTest` +4 — unpadded base64 accepted (basic decoder is padding-lenient), URL-safe base64 rejected as MALFORMED_PAYLOAD (never silently dropped), 64 KB multi-frame round-trips byte-exactly, sha256("") pinned to its real digest with the empty payload still a typed rejection (R-186) |

### 2.2 Fingerprint contract (dossier §Fingerprint contract)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-UNIT-016` | P2 | Canonical form not frozen | Golden-hash snapshot for the v1 fixture inputs | The test fails on *any* silent change to the canonical form (field order, encoding, null representation, version) — a frozen cross-commit pin | ✅ landed 2026-08-15: `FingerprintBuilderTest.goldenHashFrozen` — v1 fixture (epoch 0 / token 3045 / 1719000000000 / TRADE / 234500 / 50 / 234400 / 234600) pinned to `aec03d3d…bc5e31`; any canonical-form change fails the suite |
| `ING-UNIT-017` | P3 | Dedup semantics not pinned in code | Same tick, same epoch → identical fingerprint; same tick, next epoch → different fingerprint | Documents DEC-012 best-effort identity: duplicates within an epoch are expected (Compute dedups); reconnects reset identity | ✅ landed 2026-08-15: `FingerprintBuilderTest.dedupSemanticsPinned` — same epoch duplicates are identical (Compute dedups), epoch+1 reconnect resets identity |

### 2.3 Backpressure and memory (dossier §Backpressure and memory, §Slow-Fluss policy)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-FAIL-004` | P1 | Tracker only tested single-threaded | 16 threads × 100k `tryAccept`/`onAppendSuccess`/`onAppendFailure` with small limits | Pending counters never negative and never exceed either limit; `totalAccepted + totalRejected == totalAppended + totalFailed + totalRejected`; `pendingBytes` equals the sum of accepted sizes | ✅ landed 2026-08-15 (M1): `AppendTrackerTest.concurrentAcceptReleaseKeepsInvariants` — 16 threads, small limits; pending reconciles to zero, counters never negative |
| `ING-FAIL-005` | P2 | G2 — halt-latch lifecycle unpinned | **Decision (M1): (a) pin "halted until restart"** — fail-closed; a capacity fault needs operator restart, not silent resume | `isHalted()` stays true after pending drains to zero; accepts stay rejected; readiness stays false | ✅ landed 2026-08-15 (M1): `AppendTrackerTest.haltLatchPersistsAfterDrain` |
| `ING-FAIL-006` | P3 | 30 s warning-window throttle unpinned | Re-trigger the ≥80% warning within 30 s (records then bytes) | The warning listener fires at most once per 30 s window — the ≥80% readiness-false half is already covered by `warningAt80PercentRecords`/`warningAt80PercentBytes` | ✅ landed 2026-08-15 (M5): `AppendTrackerTest.warningThrottledToOncePer30sWindow` — first crossing fires once; re-crossings within the window never re-fire; a 10 s-old marker still throttles; a 31 s-old marker unlocks exactly one re-fire and opens a fresh window (clock advanced by reflection — no 30 s sleep) |

### 2.4 Failure matrix (dossier §Failure matrix, §Shutdown)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-FAIL-007` | P1 | G1 — TIME_JUMP never emitted | **Code change (landed M1):** a periodic clock re-measurement in `IngestionService` crossing `CLOCK_OFFSET_LIMIT_MS` writes a TIME_JUMP discontinuity row | Exactly one TIME_JUMP row per violation episode with the epoch + last-tick snapshot; reason mapping pinned | ✅ landed 2026-08-15 (M1): `IngestionService` clock monitor + `TimeJumpMonitor` (main) verified by `TimeJumpMonitorTest` (8) + `DiscontinuityReasonMappingTest` (TIME_JUMP mapping) |
| `ING-FAIL-008` | P2 | Crash-loop only unit-tested | Scripted fake bridge binary that exits non-zero repeatedly | Java logs `BRIDGE_CRASH`, writes a DROP discontinuity, restarts exactly once, then exits 0; readiness marker cleared | ✅ landed 2026-08-15: `BridgeCrashLoopE2ETest` (scripted bridge pattern from `FullStackE2ETest`, default-run via the ING-DQ-010 no-op-sink seam; 2× BRIDGE_CRASH + 2× DROP rows, one restart, terminal shutdown, readiness marker cleared, single journal entry) |
| `ING-FAIL-009` | P2 | Auth-failure path untested E2E | Bad `ARROW_TOKEN`/revoked creds | Bridge exits 2, Java exits on pipe close, zero partial appends, `auth_failure` evidence row written | ✅ landed 2026-08-15: `BridgeAuthFailureE2ETest` (scripted bridge mirrors the real auth drain: `auth_failure` → `bridge_shutdown` → exit 2; DROP evidence with the auth reason, zero accepted ticks, no restart, clean single shutdown) |
| `ING-FAIL-010` | P3 | Shutdown deadlock not tested | Fluss ack never completes; call `shutdown()` twice | Exits within the drain deadline (no hang); journal records the exact remaining pending bytes (R-260); second `shutdown()` is a no-op (single journal entry, single drain) | ✅ landed 2026-08-15: `ShutdownDeadlockTest` — un-cancelable stuck ack, `DRAIN_DEADLINE_SECONDS` (new config key, default 30) makes the deadline provable; journal entry pins `pending_records`/`pending_bytes` (new `UncertaintyJournal.Entry` fields, R-260); duplicate shutdown writes nothing |

### 2.5 Configuration contract (dossier §Configuration contract, §Startup sequence)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-UNIT-018` | P1 | G7 — Java↔Go parity by convention only | Table-driven: for every `ARROW_HFT_*` key, feed the same accepted/rejected values to Java `exactInt`/`intRange` and Go `hftPin`/`hftRange` | Both sides agree on every accepted value, rejected value, and FATAL behavior | ✅ landed 2026-08-15: `ConfigParityTest` (Java, 4 tests) + mirror table in `hft_policy_test.go` (Go, incl. 13 exec-self FATAL cases); `ARROW_HFT_LATENCY_MS`/`ARROW_HFT_CONNECTIONS` converted to the mirrored helpers |
| `ING-UNIT-019` | P2 | Doc drift for env keys unguarded | Extend `docs_audit.py` with a new C-check: parse the dossier's config table and assert every documented key is read by `IngestionConfig` or the Go bridge | New C-check PASS (like C6 for test counts) | ✅ landed 2026-08-15: `docs_audit.py` **C16** (`c16_env_key_drift`), PASS on the current table |
| `ING-UNIT-020` | P3 | G5 — process-level FATAL exits (remainder) | Run the real binary with missing `ARROW_APP_ID`; assert the documented FATAL message text on the exec-self cases | Exit code 2 + the documented FATAL message for each case | ✅ landed 2026-08-15 (M5): `hft_policy_test.go` — `TestIngUnit020MissingAppIdAndFatalMessages` (exec-self subprocess): missing `ARROW_APP_ID` → exit 2 + `missing required env: ARROW_APP_ID`; `ARROW_HFT_CONNECTIONS=2` → exit 2 + `FATAL: … pinned to 1`; `ARROW_HFT_LATENCY_MS=49` → exit 2 + `FATAL: … must be in range 50..60000`; helper env stripped of policy keys so a polluted test environment cannot mask the FATAL (G5 fully RESOLVED) |

### 2.6 Losslessness tooling (dossier §ING-TCP-001)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-TCP-002` | P1 | G3 — comparator untested | Synthetic tick-count fixtures with known lost / extra / vanished mutations; missing or truncated counter file | Each mutation class is detected (exact-equality mode fails on any delta); a missing/truncated file **fails closed**, never passes | ✅ landed 2026-08-15: `code/01_platform/04_scripts/tests/test_reconcile_compare.py` (20 unittest cases) + `reconcile-compare.py` fail-closed hardening |
| `ING-TCP-003` | P3 | Counter-report persistence un-pinned | 1,024-token report emitted as bounded chunks (20/line); file + stderr mirror | Both are byte-identical; the file persists even when stderr dies (SIGPIPE path); `ARROW_TICK_COUNTS_FILE` honored | ✅ landed 2026-08-15: `ndjson_test.go` — `TestIngTcp003TickCountReportChunkedFileAndStderrMirror` (1,024 tokens → 52 bounded chunks, total=524800, file+stderr byte-identical, each line < 603 B) + `TestIngTcp003ReportFilePersistsWhenStderrDies` (dead stderr via /dev/full — file written first, report survives) |

### 2.7 Go bridge (dossier §Process boundary, §ING-RES-001)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-RES-002` | P2 | Plan-boundary N untested | N ∈ {1, 511, 512, 513, 1023, 1024, 1025}: build the subscription plan | Every token appears exactly once; each request ≤ `MaxHFTTokensPerRequest`; slot count ≤ configured connections; `validateRequestUnion` rejects duplicates/missing tokens | ✅ landed 2026-08-15: `subscription_plan_test.go` — `TestIngRes002PlanBoundaries` (6 valid Ns: exact-once union, ≤512 requests, ceil(n/512) chunk count; 1025 over-capacity rejected) + `TestIngRes002ValidateRequestUnionRejects` (6 sub-cases: cross/within-request dup, outside token, missing token, empty request, over-size request) |
| `ING-RES-003` | P3 | Backoff determinism unpinned | `Backoff(attempt)` golden sequence | Exactly 1,2,4,8,16,30,30… s (no jitter — deterministic for soak accounting) | ✅ landed 2026-08-15: `reconnect_test.go` — `TestBackoffGoldenSequence` (1,2,4,8,16,30×4 s, no jitter; negative attempts clamp to 1 s) |
| `ING-RES-004` | P3 | NDJSON contract version un-pinned | Every emitted record (`tick`, `bridge_event`, `bridge_metrics`) carries `contract_version=2` | Schema conformance asserted across all emit paths | ✅ landed 2026-08-15: `ndjson_test.go` — `TestIngRes004AllEmitPathsCarryContractVersion2` (all three emit paths in one buffer, each decoded line carries `contract_version=2`) |

### 2.8 Telemetry (dossier §Telemetry)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-UNIT-021` | P1 | G6 — metrics scrubbing untested | Serialize a full OTLP export body after exercising every metric path | Regex scan finds no `ARROW_*` values, Bearer tokens, `raw_payload`, or credentials (mirrors ING-SEC-RED-001 for logs) | ✅ landed 2026-08-15 (M5): `OtlpMetricsEmitterTest.exportBodyScrubsSecrets` — drove out a REAL leak: `appendReasonSum` emitted decode-error reason labels verbatim, so a reason like `ARROW_TOKEN=eyJ…` reached the collector. `OtlpMetricsEmitter` now scrubs reason labels with the ING-SEC-RED-001 pattern family (Bearer-then-name=value) and fails CLOSED (whole label collapses to `REDACTED`) if any ARROW_*/Bearer/raw_payload marker survives. Test exercises every recording path with secret-shaped reasons and asserts none appear in the serialized body (G6 RESOLVED) |
| `ING-UNIT-022` | P3 | Bounded-cardinality unpinned | Enumerate emitted metric labels | Label keys come from a fixed set; no token/symbol/high-cardinality labels | ✅ landed 2026-08-15 (M5): `OtlpMetricsEmitterTest.labelCardinalityBounded` — recursive attribute-key collection across the whole payload pins the exact fixed set {`service.name`, `service.instance.id`, `slot`, `reason`, `p50`, `p90`, `p99`}; slot label VALUES asserted bounded (`hft-\d+`) |

### 2.9 Startup and readiness (dossier §Startup sequence, §Health)

| Test ID | Priority | Gap | Input/action | Pass result | Implementing class |
| --- | --- | --- | --- | --- | --- |
| `ING-INT-005` | P2 | G8 — READY gating not exhaustive | Walk every readiness dimension (fluss, tracker, broker, subscription, data, frame recency, clock) | Readiness is false when any one dimension is false and true only when all are — no false positive | ✅ landed 2026-08-15: `ReadinessGatingMatrixTest` (10 cases — all-true baseline + one flip per dimension incl. liveness, data/partial-ack, stale frame, and the fail-closed clock) |
| `ING-INT-006` | P3 | Entrypoint exit codes untested | `docker-entrypoint.sh` FATAL paths: missing `FLUSS_BOOTSTRAP` (2), missing manifest (2), missing bridge binary (1) | Exit codes and messages match the documented contract | ✅ landed 2026-08-15 (M5): `code/01_platform/04_scripts/tests/test_docker_entrypoint.sh` (bash harness, shellcheck-clean) — all three paths assert exit code AND message; wired into `run-monday-gates.sh` so a regression fails the gate |

## 3. Implementation order (milestones)

Each milestone ends with its full suite green and the catalog updated — nothing ships half-done.

| Milestone | Tests | Scope |
| --- | --- | --- |
| **M1 — Safety core** ✅ landed 2026-08-15 | `ING-FAIL-007`, `ING-FAIL-005`, `ING-FAIL-004`, `ING-DQ-010` | Closed the two code gaps (TIME_JUMP wired; halt-latch pinned "until restart"), pinned the tracker under concurrency, proved the no-silent-drop invariant. Suite 204/0/8-skips. |
| **M2 — Losslessness + config** ✅ landed 2026-08-15 | `ING-TCP-002`, `ING-UNIT-018`, `ING-UNIT-019` | Comparator suite (20 tests, fail-closed hardening), 11-key Java↔Go parity table (drove out the LATENCY_MS/CONNECTIONS fail-open reads — both now hftRange/hftPin), docs-audit C16 env-key drift check; all wired into `run-monday-gates.sh` + `run-full-suite.sh` so comparator/env-key regressions fail CI. Suite 211/0/8-skips. |
| **M3 — Failure-path E2E** ✅ landed 2026-08-15 | `ING-FAIL-008`, `ING-FAIL-009`, `ING-FAIL-010`, `ING-INT-005` | Crash-loop, auth-failure, shutdown-deadlock, and the READY gating matrix. Two small code changes shipped with the tests: `DRAIN_DEADLINE_SECONDS` config key (deadline was hardcoded 30 s — the deadlock test proves the deadline is honored) and `pending_records`/`pending_bytes` on the uncertainty journal entry (R-260 exact-bytes pin). Suite 224/0/8-skips. |
| **M4 — Go bridge + data quality** ✅ landed 2026-08-15 | ~~`ING-DQ-011`~~ ✅, `ING-UNIT-014`, `ING-UNIT-015`, `ING-UNIT-016`, `ING-UNIT-017`, `ING-RES-002`, `ING-RES-003`, `ING-RES-004`, `ING-TCP-003` | Fuzz corpus (**ING-DQ-011**), golden pins (**ING-UNIT-016**), boundary matrix (**ING-UNIT-014**), payload-hash edge cases (**ING-UNIT-015**), DEC-012 dedup semantics (**ING-UNIT-017**), plan boundaries (**ING-RES-002**), backoff sequence (**ING-RES-003**), contract version (**ING-RES-004**), chunked counter persistence (**ING-TCP-003**). Suite 231/0/8-skips. |
| **M5 — Telemetry + ops + remaining polish** ✅ landed 2026-08-15 | `ING-UNIT-021`, `ING-UNIT-022`, `ING-INT-006`, `ING-FAIL-006`, `ING-UNIT-020` | Metrics scrubbing + cardinality (ING-UNIT-021/022), docker-entrypoint exit codes (ING-INT-006), the 30 s warning-window throttle (ING-FAIL-006), and the G5 remainder — missing-`ARROW_APP_ID` + FATAL-message assertions (ING-UNIT-020). One production change shipped with the tests: `OtlpMetricsEmitter` now scrubs decode-error reason labels (G6 — a real raw-credential leak was found and fixed). The last two were folded into M5 at the 2026-08-15 audit (previously in the inventory but assigned to no milestone). Suite 234/0/8-skips. **All five milestones of this plan are now landed.** |

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
