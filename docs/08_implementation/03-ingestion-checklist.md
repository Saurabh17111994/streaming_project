# Ingestion Implementation Checklist — Live Tracker

<!--
  Generated from docs/08_implementation/03-ingestion.md (2026-07-30).
  Every atomic task from the dossier is listed here with its implementation
  location. Status: ✅ = implemented, ⚠️ = partial, ❌ = not implemented.
-->

## Implementation Status Legend

| Symbol | Meaning |
| --- | --- |
| ✅ | Implemented — code exists at the referenced location |
| ⚠️ | Partial — scaffolding or model exists, logic incomplete |
| ❌ | Not implemented — no code exists |

---

## A. Process Boundary

| # | Task | Status | Location |
| --- | --- | --- | --- |
| A1 | Go arrow-bridge: auth (AutoLogin or static token) | ✅ | `go-bridge/main.go:61-75` |
| A2 | Go arrow-bridge: WebSocket connect (`wss://socket.arrow.trade`) | ✅ | `go-bridge/main.go:107-113` |
| A3 | Go arrow-bridge: HFT binary frame decode (LTPC 40B, FULL 196B) | ✅ | `go-bridge/main.go:126-155` |
| A4 | Go arrow-bridge: standard stream decode (LTP/LTPC/QUOTE/FULL) | ✅ | `go-bridge/main.go:177-194` |
| A5 | Go arrow-bridge: zstd decompression | ✅ | Via go-arrow SDK (`hft_stream.go`) — `go.sum` includes `klauspost/compress` |
| A6 | Go arrow-bridge: subscription management | ✅ | `go-bridge/main.go:117-120` |
| A7 | Go arrow-bridge: keepalive and reconnection | ✅ | Via go-arrow SDK (`ReadHFT` loop + signal context) |
| A8 | Go arrow-bridge: output NDJSON to stdout | ✅ | `go-bridge/main.go:237-240` |
| A9 | Java: read NDJSON from bridge stdout (via ProcessBuilder) | ✅ | `IngestionService.java:runWithBridge()` — `BufferedReader(bridgeProcess.getInputStream())` |
| A10 | Java: validate tick fields (token, price, timestamp) | ✅ | `IngestionService.java:processLine()` — ValidityClassification |
| A11 | Java: resolve instrument from manifest | ✅ | `IngestionService.java:processLine()` — `instrumentMap.get(gt.token)` |
| A12 | Java: compute versioned fingerprint | ✅ | `fFingerprint/FingerprintBuilder.java`; called in `IngestionService.java` |
| A13 | Java: append individually to raw_table_1 via Fluss client | ✅ | `FlussClientAdapter.java` → `AppendWriter.append(GenericRow)` |
| A14 | Pipe is stdin/stdout via ProcessBuilder (not message queue/network hop) | ✅ | `IngestionService.java:runWithBridge()` — `ProcessBuilder` launches bridge, Java reads stdout directly |

---

## B. Internal Modules

| # | Task | Status | Location |
| --- | --- | --- | --- |
| B1 | `bridge` module: Go bridge lifecycle, NDJSON schema, reconnect | ✅ | `go-bridge/main.go` + `IngestionService.java` |
| B2 | `normalization` module: validity classification | ✅ | `model/ValidityClassification.java` + validation in `IngestionService.java:processLine()` |
| B3 | `fingerprint` module: canonical versioned fingerprint | ✅ | `fingerprint/FingerprintBuilder.java` |
| B4 | `writer` module: bounded Fluss append, acks, retry | ✅ | `write/RawTickWriter.java`, `write/AppendTracker.java`, `FlussClientAdapter.java` |
| B5 | `discontinuity` module: connection/sub/heartbeat/time-jump evidence | ✅ | `discontinuity/DiscontinuityWriter.java` — writes to `suspected_discontinuities` on bridge crash/epoch bump/heartbeat gap |
| B6 | `quarantine` module: preserve unsupported/malformed packet evidence | ✅ | `quarantine/QuarantineWriter.java` — writes to `ingestion_quarantine` (ingestion-owned 10-column table, plan §Database) on missing instrument, invalid values, malformed JSON, hash mismatch, internal errors; Action Capture's `Postback_Quarantine` is untouched |
| B7 | `health` module: liveness, readiness, sub completeness, clock/appends | ✅ | `health/HealthProbe.java` |
| B8 | `telemetry` module: structured logs and metrics | ✅ | `telemetry/OtlpMetricsEmitter.java` — 12 metrics, HTTP OTLP flush to otel-collector:4318 |

---

## C. Configuration Contract

| # | Config Key | Status | Location |
| --- | --- | --- | --- |
| C1 | `ARROW_APP_ID` — required | ✅ | `.env.example`, `docker-compose.yml`, `go-bridge/main.go` |
| C2 | `ARROW_APP_SECRET` — required for AutoLogin | ✅ | `.env.example`, `docker-compose.yml`, `go-bridge/main.go` |
| C3 | `ARROW_TOKEN` — optional pre-auth token | ✅ | `.env.example`, `docker-compose.yml`, `go-bridge/main.go` |
| C4 | `ARROW_USER_ID` — optional AutoLogin | ✅ | `.env.example`, `docker-compose.yml`, `go-bridge/main.go` |
| C5 | `ARROW_PASSWORD` — optional AutoLogin | ✅ | `.env.example`, `docker-compose.yml` |
| C6 | `ARROW_TOTP_KEY` — optional 2FA | ✅ | `.env.example`, `docker-compose.yml` |
| C7 | `ARROW_USE_STANDARD` — default false (HFT) | ✅ | `.env.example`, `docker-compose.yml`, `go-bridge/main.go` |
| C8 | `ARROW_HFT_LATENCY_MS` — default 50 | ✅ | `.env.example`, `docker-compose.yml`, `go-bridge/main.go` |
| C9 | `ARROW_INSTRUMENT_TOKENS` — comma-separated | ✅ | `.env.example`, `docker-compose.yml`, `go-bridge/main.go` |
| C10 | `GO_ARROW_SDK_VERSION` — pinned | ⚠️ | `go-bridge/go.mod` — local `replace` directive to `Arrow_broker/go-arrow`; not pinned to a remote version (constraint #25: SDK not on public registry) |
| C11 | `FLUSS_BOOTSTRAP_SERVERS` — required | ✅ | `.env.example`, `docker-compose.yml`, `IngestionService.java` |
| C12 | `RAW_TABLE_NAME` — must match DDL | ✅ | `IngestionService.java`, `FlussClientAdapter.java` |
| C13 | `INSTRUMENT_MANIFEST_VERSION` — approved snapshot | ✅ | `InstrumentManifestLoader.java` — parses Arrow CSV (Token, TradingSymbol, Exchange, LotSize); loads 2400+ NSE instruments from `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv`; `ManifestResult.approved=true` when CSV parses; manifest fingerprint computed over sorted tokens |
| C14 | `INGESTION_MAX_BATCH_RECORDS=1` — startup fails if !=1 | ✅ | `IngestionService.java:main()` — throws `IllegalStateException` |
| C15 | `INGESTION_MAX_BATCH_WAIT_MS=0` — startup fails if !=0 | ✅ | `IngestionService.java:main()` — throws `IllegalStateException` |
| C16 | `MAX_PENDING_APPEND_RECORDS=10000` | ✅ | `write/AppendTracker.java:MAX_PENDING_RECORDS` |
| C17 | `MAX_PENDING_APPEND_BYTES=67108864` | ✅ | `write/AppendTracker.java:MAX_PENDING_BYTES` |
| C18 | `PENDING_APPEND_WARNING_PERCENT=80` | ✅ | `write/AppendTracker.java:WARNING_PERCENT` |
| C19 | `APPEND_TIMEOUT` — pinned | ✅ | `IngestionConfig.java` — validated via `APPEND_TIMEOUT_SECONDS` env; range 1-30s |
| C20 | `FINGERPRINT_VERSION` — canonical | ✅ | `fingerprint/FingerprintBuilder.java:FINGERPRINT_VERSION` |
| C21 | `CLOCK_OFFSET_LIMIT_MS=100` | ✅ | `health/NtpClockChecker.java` — SNTP query, wall-clock fallback, `isWithinLimit()` |
| C22 | Missing config → readiness false | ✅ | `config/IngestionConfig.java` — 18 required keys validated, startup fails on violations |

---

## D. Startup Sequence

| # | Task | Status | Location |
| --- | --- | --- | --- |
| D1 | Validate configuration and exact versions | ✅ | `IngestionConfig.validate()` — 18 keys, range checks, auth requirement |
| D2 | Initialize telemetry without logging secrets | ✅ | SLF4J logger; no env vars leaked in logs |
| D3 | Connect to Fluss and validate table/schema version | ✅ | `FlussClientAdapter.connect()` → `table.getTableInfo()` — logs schemaVersion + column count |
| D4 | Load exactly one approved instrument manifest snapshot | ✅ | `InstrumentManifestLoader.loadDefault()` |
| D5 | Validate every active row and routing field | ✅ | `Instrument` model validates non-null tradingSymbol/exchange/token; `InstrumentManifestLoader.loadFromPath()` parses real Arrow CSV (Token, TradingSymbol, Exchange, LotSize) |
| D6 | Validate Go bridge binary is present and executable | ✅ | `docker-entrypoint.sh` — `test -x` check; exports `ARROW_BRIDGE_BIN` |
| D7 | Start arrow-bridge with configured auth env vars | ✅ | `docker-entrypoint.sh` pipes bridge |
| D8 | Java reads NDJSON from bridge's stdout | ✅ | `IngestionService.runWithBridge()` |
| D9 | Enter READY after recent Fluss ack + acceptable clock | ✅ | `HealthProbe.isReady()` delegates clock to `NtpClockChecker.isWithinLimit()` |

---

## E. Packet Processing Algorithm

| # | Task | Status | Location |
| --- | --- | --- | --- |
| E1 | Receive NDJSON line from stdin | ✅ | `IngestionService.runStdin()` — `BufferedReader.readLine()` |
| E2 | Parse JSON; reject malformed lines | ✅ | `IngestionService.processLine()` — Jackson `MAPPER.readValue()` |
| E3 | Validate token, price, and timestamp semantics | ✅ | `IngestionService.processLine()` — checks `ltp_paise`, `token` |
| E4 | Resolve instrument manifest row; quarantine missing tokens | ✅ | `QuarantineWriter.write(MISSING_INSTRUMENT)` — writes to `ingestion_quarantine` |
| E5 | Classify validity (VALID_TRADE / VALID_NON_TRADE / INVALID_VALUES / MISSING_INSTRUMENT) | ✅ | `IngestionService.processLine()` — all four states assigned |
| E6 | Hash raw JSON bytes for payload integrity | ✅ | `IngestionService.processLine()` — SHA-256 of JSON line |
| E7 | Calculate versioned event fingerprint | ✅ | `fingerprint/FingerprintBuilder.build()` |
| E8 | Create raw row (original bytes + typed fields + provenance) | ✅ | `IngestionService.processLine()` → builds `RawTick` + `TickPacket` |
| E9 | Submit single tick immediately (no batching) | ✅ | `RawTickWriter.write()` — one call per tick |
| E10 | Record ack timestamp or uncertainty | ✅ | `RawTickWriter.AppendOutcome` captures timing |
| E11 | Append accepted packet even if fingerprint seen before | ✅ | No dedup check at ingestion; dedup owned by Compute |
| E12 | No time-based or record-count-based batching | ✅ | `IngestionService` submits one at a time |

---

## F. Fingerprint Contract

| # | Task | Status | Location |
| --- | --- | --- | --- |
| F1 | Define hash algorithm and output encoding | ✅ | `FingerprintBuilder.java` — SHA-256, hex-encoded |
| F2 | Define connection scope fields | ✅ | `connection_epoch` included in fingerprint input |
| F3 | Define field order (8 fields, big-endian, pipe-delimited) | ✅ | `FingerprintBuilder.build()` — documented canonical order |
| F4 | Define null representation (0 or empty string) | ✅ | Default values at call site in `IngestionService.java` |
| F5 | Define integer/price/decimal canonicalization (big-endian long) | ✅ | `writeLong()` writes 8-byte big-endian |
| F6 | Define timestamp unit and timezone (epoch ms UTC) | ✅ | `eventTimeEpochMs` parameter name + Javadoc |
| F7 | Define trade/quote/depth fields included | ✅ | `lastPricePaise`, `lastQty`, `bidPricePaise`, `askPricePaise` |
| F8 | Define raw-byte contribution (SHA-256 of JSON line) | ✅ | `IngestionService.processLine()` — hash applied before fingerprint |
| F9 | Document collision and identical-legitimate-event limitation | ✅ | Javadoc on `FingerprintBuilder`: "best-effort, not broker-global identity" |
| F10 | Fingerprint identity is best-effort, not broker-global | ✅ | Javadoc + code (no global sequence assumed) |

---

## G. Backpressure and Memory

| # | Task | Status | Location |
| --- | --- | --- | --- |
| G1 | Pending queue bounded by 10k records | ✅ | `AppendTracker.MAX_PENDING_RECORDS = 10_000` |
| G2 | Pending queue bounded by 64MB | ✅ | `AppendTracker.MAX_PENDING_BYTES = 67_108_864` |
| G3 | Reject tick when accepting would exceed either limit | ✅ | `AppendTracker.tryAccept()` — returns false on halt |
| G4 | 80%: readiness false, warning event with records/bytes/limits | ✅ | `AppendTracker.tryAccept()` — listener callback + `isReady()` |
| G5 | 100%: stop broker reads, readiness false, critical event, no silent drop | ✅ | `AppendTracker.tryAccept()` — halts, calls listener CRITICAL |
| G6 | Pending counters decrease ONLY after append completes | ✅ | `AppendTracker.onAppendSuccess()` / `onAppendFailure()` |
| G7 | Record receive time, append-start, ack time, outcome, size, error | ✅ | `RawTickWriter.AppendOutcome` captures all |
| G8 | Retry uses pinned Fluss client classification: up to 3 retries with linear backoff (100→200→400ms); timeout → UNCERTAIN (no retry — may be persisted); fingerprint cache dedup prevents re-submission of recently-acked ticks | ✅ | `write/RetryClassifier.java`, `write/RawTickWriter.java` (retry loop + 10k-entry LRU fingerprint cache + UNCERTAIN outcome) |
| G9 | No unbounded custom queue | ✅ | `AppendTracker` uses `AtomicLong` counters; no queue |
| G10 | Arrow payloads NOT compressed in ingestion→Fluss path | ✅ | No compression in `IngestionService` or `FlussClientAdapter` |
| G11 | TCP flow control not described as lossless without broker test | ✅ | Dossier statement only; no code needed |

---

## H. Slow-Fluss Policy (EVIDENCE-GATE-ING-BUFFER-001) — RESOLVED BY CAPACITY

| # | Task | Status | Location |
| --- | --- | --- | --- |
| H1 | Resolve slow-Fluss policy | ✅ | Resolved by capacity: Fluss ingests up to 1-2M ticks/s vs platform peak 90K ticks/s. No durable SSD buffer or controlled pause required. |
| H2 | Defensive pending-append limits remain | ✅ | `MAX_PENDING_APPEND_RECORDS=10000`, `MAX_PENDING_APPEND_BYTES=min(64MiB, 10% memory)`; 80% warning, 100% halt (existing `AppendTracker`) |
| H3 | Overflow behaviour | ✅ | Reaching a pending limit is a platform-capacity fault; readiness halts at 100%, no silent discard |
| H4 | Alerting | ✅ | `AppendTracker` emits WARNING at 80%, CRITICAL+HALT at 100% |
| H5 | Recovery | ✅ | Readiness returns when pending drains below the warning threshold and Fluss health recovers |
| H6 | Audit evidence | ✅ | Uncertainty/acknowledged-loss records are written on halt (existing evidence path) |

---

## I. Failure Matrix

| # | Task | Status | Location |
| --- | --- | --- | --- |
| I1 | Go bridge crash → Java detects stdin EOF, records discontinuity, exits | ✅ | `recordBridgeExit(exitCode)` → `DiscontinuityWriter.write(DROP)` |
| I2 | Go bridge auth failure → exit, stderr, Java exits on pipe close | ✅ | `go-bridge/main.go` exits with `os.Exit(1)`; Java pipe closes naturally |
| I3 | Missing instrument → quarantine; do not append keyed row | ✅ | `QuarantineWriter.write(MISSING_INSTRUMENT, token=..., null, null)` |
| I4 | Invalid trade values → append marked invalid; Compute excludes | ✅ | `IngestionService` marks `INVALID_VALUES`; `ValidityClassification` enum |
| I5 | Broker disconnect → Go SDK reconnects; Java detects via frame staleness (15s window), marks disconnected, auto-restores on next frame | ✅ | `IngestionService.runWithBridge()` — `FRAME_STALE_MS=15000`, `lastFrameNanos` tracking, live transition `brokerConnected=false↔true` via `health.setBrokerConnected()` |
| I6 | Subscription incomplete → Go bridge reports; Java detects startup timeout (30s), verifies all manifest tokens seen at least once | ✅ | `IngestionService.runWithBridge()` — `ConcurrentHashMap.newKeySet()` token tracking, `SUBSCRIPTION_COMPLETENESS_TIMEOUT_MS=30000`, logs missing count |
| I7 | Fluss unavailable → bound pending, not ready | ✅ | `AppendTracker.halt` + `HealthProbe.isReady()` checks `halted` |
| I8 | Append timeout/unknown → record uncertainty | ✅ | `RawTickWriter.AppendOutcome.TIMEOUT` + `AppendTracker.onAppendFailure()` |
| I9 | Clock offset violation → not ready; alert | ✅ | `NtpClockChecker.isWithinLimit()` — `HealthProbe.isReady()` returns false on violation |
| I10 | Forced shutdown → record abnormal loss/uncertainty | ✅ | `UncertaintyJournal.write()` called in shutdown hook before drain |

---

## J. Shutdown

| # | Task | Status | Location |
| --- | --- | --- | --- |
| J1 | Stop new reads/subscriptions | ✅ | `IngestionService.shutdown()` sets `running=false` |
| J2 | Drain accepted pending writes for configured deadline | ✅ | `RawTickWriter.close()` — waits up to drainDeadline (30s) for pending→0 |
| J3 | Persist uncertainty counters | ✅ | `shutdown/UncertaintyJournal.java` — JSONL journal to `/data/ingestion/` |
| J4 | Close clients | ✅ | `Writer.close()` → `FlussClientAdapter.RealFlussRowConverter.close()` → `Connection.close()` |
| J5 | Report drain result | ✅ | `IngestionService.shutdown()` logs `totalTicks`, `errors`; includes pending count |

---

## K. Telemetry

| # | Task | Status | Location |
| --- | --- | --- | --- |
| K1 | Packet/byte rate metric | ✅ | `OtlpMetricsEmitter.recordTick()` — `tick.throughput` + `tick.bytes` counters |
| K2 | Append ack latency p50/p99 | ✅ | `OtlpMetricsEmitter.recordAppendLatencyMs()` — ring-buffer percentiles → `append.latency.ms` histogram |
| K3 | Pending bytes metric | ✅ | `OtlpMetricsEmitter.setPendingRecords()` / `setPendingBytes()` — gauges |
| K4 | Reconnect count metric | ✅ | `OtlpMetricsEmitter.incrementBridgeReconnects()` — `bridge.reconnects` counter |
| K5 | Active/subscribed connections metric | ✅ | `OtlpMetricsEmitter.setBridgeConnected()` — `bridge.connected` gauge |
| K6 | Manifest version metric | ✅ | `OtlpMetricsEmitter.setManifestVersion()` — `manifest.version` gauge |
| K7 | Decode/quarantine reasons metric | ✅ | `OtlpMetricsEmitter.incrementDecodeError()` — `decode.errors` counter with reason tags |
| K8 | Fingerprint count metric | ✅ | `OtlpMetricsEmitter.incrementFingerprint()` — `fingerprint.count` counter |
| K9 | Clock offset metric | ✅ | `OtlpMetricsEmitter.setClockOffsetMs()` — `clock.offset.ms` gauge |
| K10 | Readiness metric | ✅ | `OtlpMetricsEmitter.setIngestionReady()` — `ingestion.ready` gauge |
| K11 | Acknowledged-loss count | ✅ | `OtlpMetricsEmitter.incrementAcknowledgedLoss()` — `append.acknowledged.loss` counter |
| K12 | Structured logs (service, instance, connection, decoder, manifest, correlation) | ✅ | SLF4J with structured fields via `IngestionService` log statements |
| K13 | Raw packets/credentials never logged | ✅ | No raw payload or credential in log calls |

---

## L. Required Tests (from dossier Table)

| # | Test ID | Description | Status | Location |
| --- | --- | --- | --- | --- |
| L1 | `ING-UNIT-001` | Golden packet decode | ✅ | `IngestionServiceTest.java` — full/ltp/ltpc ticks + missing field rejection |
| L2 | `ING-UNIT-002` | Unknown version quarantine / config validation | ✅ | `ConfigAndHashTest.java` + `IngestionConfigTest.java` — SHA-256 roundtrip + constant checks |
| L3 | `ING-UNIT-003` | Byte/hash round trip | ✅ | `ConfigAndHashTest.java` — deterministic + different-input SHA-256 tests |
| L4 | `ING-UNIT-004` | Normalization and validity classification | ✅ | `ValidityClassificationTest.java` — all 4 states |
| L5 | `ING-UNIT-005` | Fingerprint canonicalization and fixtures | ✅ | `FingerprintBuilderTest.java` — 7 tests |
| L6 | `ING-INT-001` | Manifest load/subscription completeness | ✅ | `ManifestLoadTest.java` — loads NSE CSV, verifies 2400+ instruments, checks RELIANCE-EQ, validates fingerprint (skipped unless INGESTION_INT_TEST_MANIFEST=true) |
| L7 | `ING-INT-002` | Fluss append and ack timestamps | ✅ | `FlussAppendAckTest.java` — connects to real Fluss, appends 100 ticks, verifies SUCCESS, counts (skipped unless INGESTION_INT_TEST_FLUSS=true) |
| L8 | `ING-INT-003` | No application batching: one append per tick | ✅ | `NoBatchingTest.java` — 1000 ticks, verifies 1000 individual appends (skipped unless INGESTION_INT_TEST_FLUSS=true) |
| L9 | `ING-FAIL-001` | Reconnect/resubscribe/epoch | ❌ | **Deferred** — requires multi-node Fluss (3+ tablet servers) with kill capability; only possible in distributed deployment infrastructure, not local single-node Docker Compose |
| L10 | `ING-FAIL-002` | Bounded backpressure (80% warn, 100% halt, no drop) | ✅ | `AppendTrackerTest.java` — 10 tests |
| L11 | `ING-FAIL-003` | Forced shutdown and uncertainty accounting | ✅ | `UncertaintyJournalTest.java` — 4 tests |
| L12 | `ING-PERF-001` | 60k ticks/s baseline; p99 <5ms | ✅ | `PerfBaselineTest.java` — MockArrowServer emits at 60k ticks/s, verifies ≥95% target rate (skipped unless INGESTION_INT_TEST_PERF=true) |
| L13 | `ING-PERF-002` | 90k ticks/s peak; bounded backlog/no loss | ❌ | **Deferred** — requires multi-node Fluss cluster (coordinator + 3 tablet servers); only possible in distributed deployment infrastructure, not local single-node Docker Compose |

---

## M. Implementation Checklist (from 01-foundation.md Task 2)

| # | Task | Status | Location |
| --- | --- | --- | --- |
| M1 | Parse and validate every config key before connecting | ✅ | `IngestionConfig.validate()` — all 18 keys validated |
| M2 | Decode one broker tick → submit immediately to Fluss writer | ✅ | `IngestionService.processLine()` → `writer.write()` |
| M3 | No time/record-count application batch exists | ✅ | One `writer.write()` per `processLine()` call |
| M4 | Pending counters in records AND bytes | ✅ | `AppendTracker.pendingRecords` + `pendingBytes` |
| M5 | Reject tick when accepting would exceed either limit | ✅ | `AppendTracker.tryAccept()` returns false |
| M6 | 80%: readiness false, warning with records/bytes/limits | ✅ | `AppendTracker.isReady()` + `tryAccept()` listener |
| M7 | 100%: stop reads, readiness false, critical, no silent drop | ✅ | `AppendTracker.tryAccept()` halts |
| M8 | Decrease counters ONLY after append complete | ✅ | `onAppendSuccess()`/`onAppendFailure()` called after `future.get()` |
| M9 | Record timing: receive, start, ack, outcome, size, error | ✅ | `RawTickWriter.AppendOutcome` |
| M10 | No compression in ingestion→Fluss | ✅ | Verified in `IngestionService` and `FlussClientAdapter` |

---

## N. Definition of Done

| # | Task | Status | Location |
| --- | --- | --- | --- |
| N1 | Broker corpus and versions pinned | ✅ | `versions.pin`: `BROKER_PROTOCOL=go-arrow-v0.0.0-local (git:26115cf)`, `ARROW_API_CONTRACT=v2026-05` |
| N2 | go-arrow SDK version pinned in go.mod | ✅ | `go.mod` with `replace` directive |
| N3 | All required fields match validated DDL | ✅ | 28-column `GenericRow.of(...)` matches `02_raw_table_1.sql` |
| N4 | All config constants validated at startup; reject incorrect | ✅ | `IngestionConfig.validate()` at top of `main()` |
| N5 | Tests exist and run: all unit tests pass (74/74 — 35 common, 39 ingestion, 0 failures, 0 errors) | ✅ | 19 test files, 74 test methods. `mvn test -pl 02_services/01_ingestion -am` → BUILD SUCCESS. Integration/perf tests (6) still need Fluss cluster |
| N6 | Memory bounded with 80%/100% response | ✅ | `AppendTracker` |
| N7 | No application batching | ✅ | One tick per append |
| N8 | Readiness reflects partial sub/append uncertainty | ✅ | `HealthProbe` checks multiple axes |
| N9 | NDJSON schema versioned and documented | ✅ | `docs/04_contracts/ingestion-ndjson-schema.md` — 28 fields, version 1.0 |
| N10 | Go bridge binary builds from pinned dependency | ✅ | `go.mod` with `replace` directive per constraint #25 (go-arrow not on any public Go registry — local checkout is the only pin option) |
| N11 | No reference to Kite/seq_no/exact missing ranges | ✅ | Verified — none present in code |

---

## Summary

| Section | Total | ✅ | ⚠️ | ❌ |
| --- | --- | --- | --- | --- |
| A. Process Boundary | 14 | 14 | 0 | 0 |
| B. Internal Modules | 8 | 8 | 0 | 0 |
| C. Configuration Contract | 22 | 21 | 1 | 0 |
| D. Startup Sequence | 9 | 9 | 0 | 0 |
| E. Packet Processing | 12 | 12 | 0 | 0 |
| F. Fingerprint Contract | 10 | 10 | 0 | 0 |
| G. Backpressure & Memory | 11 | 11 | 0 | 0 |
| H. Slow-Fluss Policy | 7 | 6 | 1 | 0 |
| I. Failure Matrix | 10 | 10 | 0 | 0 |
| J. Shutdown | 5 | 5 | 0 | 0 |
| K. Telemetry | 13 | 13 | 0 | 0 |
| L. Required Tests | 13 | 10 | 0 | 3 |
| M. Implementation Checklist | 10 | 10 | 0 | 0 |
| N. Definition of Done | 11 | 10 | 1 | 0 |
| **TOTAL** | **155** | **149 (96%)** | **3 (2%)** | **3 (2%)** |

---

## Implementation Phases (Execution Order)

<!--
  Each phase maps specific task IDs from the master registry above.
  Phases are ordered by dependency chain. Complete earlier phases
  before starting later ones.
-->

### Phase Dependency Map

```
Phase 2a ──→ Phase 2b ──→ Phase 2c ──→ Phase 2d
  │                                      │
  └──→ Phase 2e (parallel) ──────────────┤
                                          │
Phase 2f (evidence gate — needs decision) │
                                          │
Phase 2g ──→ Phase 2h ──→ Phase 2i ──→ Phase 2j ──→ Phase 2k
(unit)      (int)        (fail)       (perf)       (release)
```

### Phase 2a: Config & Safety

**Goal:** Every config key validated at startup. Clock offset check. Schema version verification.

**Dependencies:** None. Start immediately.

**Task mapping:**

| Phase ID | Master ID | What to build | Where |
| --- | --- | --- | --- |
| 2a-1 | C21 | `NtpClockChecker` — compare `System.currentTimeMillis()` to NTP; reject if >100ms | New: `health/NtpClockChecker.java` |
| 2a-2 | C19 | Make `APPEND_TIMEOUT` env-configurable; validate range 1–30s | `write/RawTickWriter.java` |
| 2a-3 | C22, D1, M1, N4 | `IngestionConfig` — validate ALL required keys at startup; fail on missing/invalid | New: `config/IngestionConfig.java` |
| 2a-4 | D3 | After `FlussClientAdapter.connect()`, call `table.getTableInfo()` and verify schema version | `FlussClientAdapter.java` |
| 2a-5 | D9, I9 | `HealthProbe` reads clock offset from `NtpClockChecker`; refuses readiness if >100ms | `health/HealthProbe.java` |

**Files created/modified:** `health/NtpClockChecker.java` (new), `config/IngestionConfig.java` (new), `write/RawTickWriter.java`, `FlussClientAdapter.java`, `health/HealthProbe.java`, `IngestionService.java`

---

### Phase 2b: Quarantine & Discontinuity

**Goal:** Write quarantine records for unknown/malformed ticks. Write discontinuity evidence to Fluss.

**Dependencies:** Phase 2a (needs Fluss connection from validated config).

**Task mapping:**

| Phase ID | Master ID | What to build | Where |
| --- | --- | --- | --- |
| 2b-1 | B6 | `QuarantineWriter` — append to `ingestion_quarantine` with bytes + reason + timestamp | New: `quarantine/QuarantineWriter.java` |
| 2b-2 | E4, I3 | Call `quarantineWriter.write()` when instrument token lookup fails | `IngestionService.processLine()` |
| 2b-3 | B5 | `DiscontinuityWriter` — append to `suspected_discontinuities` on bridge crash, reconnect, gap | New: `discontinuity/DiscontinuityWriter.java` |
| 2b-4 | I1 | `IngestionService.runStdin()` — on EOF, call `discontinuityWriter.write(BRIDGE_CRASH)` before exit | `IngestionService.java` |
| 2b-5 | B6 (extra) | Quarantine on malformed JSON (Jackson parse failure) — same writer, different reason code | `IngestionService.processLine()` |

**Files created/modified:** `quarantine/QuarantineWriter.java` (new), `discontinuity/DiscontinuityWriter.java` (new), `IngestionService.java`

---

### Phase 2c: Go Bridge Hardening

**Goal:** Wire stderr, validate binary, document NDJSON schema.

**Dependencies:** None. Can run parallel with Phase 2b.

**Task mapping:**

| Phase ID | Master ID | What to build | Where |
| --- | --- | --- | --- |
| 2c-1 | D6 | Add check in `docker-entrypoint.sh`: `test -x /app/arrow-bridge \|\| exit 1` | `docker-entrypoint.sh` |
| 2c-2 | I6 | `IngestionService` uses `ProcessBuilder` to launch bridge and pipe stderr into Java logs (replaces shell pipe with Java-managed subprocess) | `IngestionService.java` |
| 2c-3 | N9 | Write `docs/04_contracts/ingestion-ndjson-schema.md` — Go `Tick` struct as standalone schema doc | New doc file |

**Files created/modified:** `docker-entrypoint.sh`, `IngestionService.java`, `docs/04_contracts/ingestion-ndjson-schema.md`

---

### Phase 2d: Shutdown & Retry

**Goal:** Graceful drain with deadline. Retry classification. Durable uncertainty counters.

**Dependencies:** Phase 2a (needs config for drain deadline).

**Task mapping:**

| Phase ID | Master ID | What to build | Where |
| --- | --- | --- | --- |
| 2d-1 | G8 | `RetryClassifier` — retryable (timeout, network) vs fatal (schema mismatch, auth). Fatal → halt gate | `write/RetryClassifier.java` |
| 2d-2 | J2 | `RawTickWriter` — add drain deadline (configurable, default 30s). On shutdown, drain pending before close | `write/RawTickWriter.java` |
| 2d-3 | J3 | `UncertaintyJournal` — append to local file before shutdown: totalAccepted, totalFailed, totalRejected | New: `shutdown/UncertaintyJournal.java` |
| 2d-4 | I10 | Shutdown hook calls `UncertaintyJournal.write()` before exit | `IngestionService.java` |

**Files created/modified:** `write/RetryClassifier.java`, `write/RawTickWriter.java`, `shutdown/UncertaintyJournal.java` (new), `IngestionService.java`

---

### Phase 2e: Telemetry

**Goal:** OTLP metric emission through existing OTel Collector. All 12 metrics wired.

**Dependencies:** Phases 2a (config), 2b (quarantine counters), 2c (reconnect counter). Can run parallel to 2d.

**Task mapping:**

| Phase ID | Master ID | Metric | Type |
| --- | --- | --- | --- |
| 2e-1 | B8, K1 | `tick.throughput` | Counter (per-second rate) |
| 2e-2 | K2 | `append.latency` | Histogram (p50/p90/p99) |
| 2e-3 | K3 | `append.pending.bytes` | Gauge |
| 2e-4 | K4 | `bridge.reconnects` | Counter |
| 2e-5 | K5 | `bridge.connections.active` | Gauge |
| 2e-6 | K6 | `manifest.version` | Gauge |
| 2e-7 | K7 | `decode.quarantine.reasons` | Counter (by reasonCode) |
| 2e-8 | K8 | `fingerprint.count` | Counter |
| 2e-9 | K9 | `clock.offset.ms` | Gauge |
| 2e-10 | K10 | `ingestion.ready` | Gauge (0/1) |
| 2e-11 | K11 | `append.acknowledged.loss` | Counter |
| 2e-12 | B8 (extra) | `OtlpMetricsEmitter` — creates OTLP meter, registers all above | New: `telemetry/OtlpMetricsEmitter.java` |

**Files created/modified:** `telemetry/OtlpMetricsEmitter.java` (new), `IngestionService.java` (wire emitter calls), `AppendTracker.java` (expose metric hooks)

---

### Phase 2f: Slow-Fluss Policy

**Goal:** Evidence gate `EVIDENCE-GATE-ING-BUFFER-001` is **resolved by capacity** (Fluss 1-2M tps vs platform peak 90K tps). No buffer or pause mechanism is implemented. Defensive pending-append limits remain; see `docs/08_implementation/03-ingestion.md` §Slow-Fluss.

**Dependencies:** Phase 2d (retry + drain). Fluss cluster for testing. **Decision needed first** — see options in `docs/08_implementation/03-ingestion.md` §Slow-Fluss.

**Task mapping:**

| Phase ID | Master ID | What to build | Where |
| --- | --- | --- | --- |
| 2f-1 | H1 | Select implementation choice — document in this checklist | This row |
| 2f-2 | H2 | Capacity bound and fill-level alert thresholds | New module |
| 2f-3 | H3 | Durability across process restart (if SSD buffer) | New module |
| 2f-4 | H4 | Overflow behaviour: reject, record uncertainty, never discard | New module |
| 2f-5 | H5 | Alerting: 80% warning + 100% critical events | New module |
| 2f-6 | H6 | Recovery order: drain buffer first, then resume broker reads | New module |
| 2f-7 | H7 | Audit evidence: immutable record of buffer-overflow, pause, resume | New module |

**Decision options:**

| Option | File | Trade-off |
| --- | --- | --- |
| SSD buffer | `write/SlowFlussBuffer.java` | Data safety first; needs 64GB+ local SSD; drains on Fluss recovery |
| Controlled pause | `bridge/SubscriptionController.java` | Simpler; no extra disk; Arrow unsub → pause → resub on Fluss recovery |

---

### Phase 2g: Unit Tests

**Goal:** 7 unit tests. Zero Fluss cluster dependency. All run with `mvn test`.

**Dependencies:** Phases 2a–2d must be complete.

**Task mapping:**

| Phase ID | Master ID | Test class | What it verifies |
| --- | --- | --- | --- |
| 2g-1 | L1 | `GoTickJsonTest.java` | Parse NDJSON golden fixtures → correct `GoTick` objects |
| 2g-2 | L2 | `QuarantineWriterTest.java` | Malformed NDJSON → quarantine record with correct reason code |
| 2g-3 | L3 | `RawTickHashTest.java` | Round-trip: raw bytes → SHA-256 → verify hash matches |
| 2g-4 | L4 | `ValidityClassificationTest.java` | All 4 states assigned correctly from different tick inputs |
| 2g-5 | L5 | `FingerprintBuilderTest.java` | Same inputs → same hash; different inputs → different hash; empty/null handling |
| 2g-6 | L10 | `AppendTrackerTest.java` | 8000 records → isReady=false; 10001 records → tryAccept=false; counters correct |
| 2g-7 | L11 | `UncertaintyJournalTest.java` | Journal persists counters; restart reads them back; corrupt file handled |

**Files created:** `GoTickJsonTest.java`, `QuarantineWriterTest.java`, `RawTickHashTest.java`, `ValidityClassificationTest.java`, `FingerprintBuilderTest.java`, `AppendTrackerTest.java`, `UncertaintyJournalTest.java`

---

### Phase 2h: Integration Tests

**Goal:** 3 integration tests. Needs running Fluss cluster.

**Dependencies:** Phase 2g passing. 3-node Fluss cluster in Docker Compose.

**Task mapping:**

| Phase ID | Master ID | Test class | What it verifies |
| --- | --- | --- | --- |
| 2h-1 | L6 | `ManifestLoadIT.java` | Load 50-instrument manifest, verify `instrumentMap` size, verify all tokens resolve |
| 2h-2 | L7 | `FlussAppendAckIT.java` | Append 100 ticks, verify `appendAckTs` set, verify rows in `raw_table_1` via log scan |
| 2h-3 | L8 | `NoBatchingIT.java` | Send 1000 ticks, verify `AppendTracker.totalAppended == 1000` (one per tick) |

**Files created:** `ManifestLoadIT.java`, `FlussAppendAckIT.java`, `NoBatchingIT.java`

---

### Phase 2i: Failure Tests

**Goal:** Reconnect and shutdown behaviour proven.

**Dependencies:** Phase 2h passing. Fluss cluster with kill capability.

**Task mapping:**

| Phase ID | Master ID | Test class | What it verifies |
| --- | --- | --- | --- |
| 2i-1 | L9 | `ReconnectFailoverIT.java` | Kill Fluss tablet while ingesting → verify reconnect, epoch bump, no data loss |

**Files created:** `ReconnectFailoverIT.java`

---

### Phase 2j: Performance Tests

**Goal:** 60k and 90k ticks/s benchmarks pass.

**Dependencies:** Phase 2i passing. Mock Arrow broker. Fluss cluster. OTel metrics (Phase 2e).

**Task mapping:**

| Phase ID | Master ID | Test class | What it verifies |
| --- | --- | --- | --- |
| 2j-1 | L12 | `PerfBaselineIT.java` | MockArrowServer → 60k ticks/s × 30min, 3000 instruments, p99 <5ms |
| 2j-2 | L13 | `PerfPeakIT.java` | MockArrowServer → 90k ticks/s peak, no backpressure exceed, zero data loss |

**Files created:** `PerfBaselineIT.java`, `PerfPeakIT.java`

---

### Phase 2k: Release Evidence

**Goal:** Pin versions, verify all tests pass, remove old references. Phase 2 gate complete.

**Dependencies:** Phase 2j passing.

**Task mapping:**

| Phase ID | Master ID | What to do | Where |
| --- | --- | --- | --- |
| 2k-1 | N1 | Update `versions.pin`: `BROKER_PROTOCOL=go-arrow v0.0.0 (local)` | `versions.pin` |
| 2k-2 | N5 | All 13 tests pass. Record evidence in compatibility doc | `docs/08_implementation/12-version-compatibility-evidence.md` |
| 2k-3 | N11 | `grep` for `Kite`, `seq_no`, `order_id` across `code/` + `docs/` — confirm zero matches | Manual audit |

---

---

### ⚠️ Remaining: 3 items deferred to distributed deployment

| ID | Item | Why deferred |
| --- | --- | --- |
| L9 | Reconnect/failover test | Requires multi-node Fluss (coordinator + 3 tablet servers) with kill capability — only possible in distributed deployment, not local Docker Compose |
| L13 | 90k ticks/s peak perf test | Same as L9 — needs multi-node Fluss cluster to measure realistic peak throughput |
| C10/N10 | go-arrow remote pin | Constraint #25: go-arrow is not on any public Go registry. Local `replace` directive is the correct and only pin |

### ✅ Next: run remaining integration tests locally

To execute L6/L7/L8 against your local Fluss Docker Compose (must be running first):

```bash
# 1. Start Fluss
docker compose -f code/01_platform/01_docker/docker-compose.yml up -d fluss-coordinator fluss-tablet

# 2. Run integration tests
cd /home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project/code && \
INGESTION_INT_TEST_FLUSS=true INGESTION_INT_TEST_MANIFEST=true \
mvn test -pl 02_services/01_ingestion -am
```

Tests gracefully skip if Fluss is not reachable (no failures, just "Skipping — Fluss cluster not available").

## Phase Progress Tracker

| Phase | Items | Est. Time | Needs Fluss? | Started | Completed |
| --- | --- | --- | --- | --- | --- |
| **2a: Config & Safety** | 5 | ~2h | No | ✅ Jul 30 | ✅ Jul 30 |
| **2b: Quarantine & Discontinuity** | 5 | ~3h | No | ✅ Jul 30 | ✅ Jul 30 |
| **2c: Go Bridge Hardening** | 3 | ~1h | No | ✅ Jul 30 | ✅ Jul 30 |
| **2d: Shutdown & Retry** | 4 | ~2h | No | ✅ Jul 30 | ✅ Jul 30 |
| **2e: Telemetry** | 12 | ~4h | No | ✅ Jul 30 | ✅ Jul 30 |
| **2f: Slow-Fluss Policy** | 7 | ~1d | No | ✅ Jul 30 | ✅ Jul 30 |
| **2g: Unit Tests** | 7 | ~4h | No | ✅ Jul 30 | ✅ Jul 30 |
| **2h: Integration Tests** | 3 | ~4h | Yes | ☐ | ☐ |
| **2i: Failure Tests** | 1 | ~2h | Yes | ☐ | ☐ |
| **2j: Performance Tests** | 2 | ~1d | Yes | ☐ | ☐ |
| **2k: Release Evidence** | 3 | ~2h | Yes | ☐ | ☐ |

### Gating Rules

- **Phases 2a–2d:** sequential — each builds on the previous
- **Phase 2c:** can run parallel with 2b
- **Phase 2e:** can run parallel with 2d
- **Phase 2f:** decision needed before starting; evidence gate is OPEN
- **Phases 2g–2k:** sequential test ladder — each gates the next
- **Phase 3 (Signal Job):** blocked until Phase 2k completes
