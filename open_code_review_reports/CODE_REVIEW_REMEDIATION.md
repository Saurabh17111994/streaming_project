# Code Review Remediation Tracker

Living single source of truth for resolving every finding in the Open Code Review reports.

- **Repository root (report paths are relative to this):** `streaming_project/`
- **Reports:** `01_bug.md` (167), `02_security.md` (7), `03_performance.md` (13), `04_maintainability.md` (82), `05_style_and_docs.md` (6), `06_other.md` (12)
- **Verdict:** every finding was reviewed; the single exact textual duplicate in `04_maintainability.md` (`margin.go` low) was merged. Nothing else was dropped, merged, or invented.

> **Execution companion:** this file defines **WHAT** each task is (R-001…R-286). **WHEN / HOW / ORDER — the phase sequence, file batching, current position, and live evidence log — live in [`EXECUTION_PLAN.md`](./EXECUTION_PLAN.md).** Update both files together: the acceptance checkboxes here and the verdict/status/evidence there. A task is never marked done in one without the other.

## Summary

| Metric | Count |
| --- | --- |
| Total review findings | 287 |
| Total atomic tasks | 286 |
| Critical | 3 |
| High | 50 |
| Medium | 119 |
| Low | 114 |
| Duplicate findings merged | 1 |
| Files affected | 108 |

### Conventions

- **Task IDs are stable** — `R-001` … `R-286`.
- **Path convention:** all paths are relative to the repository root above.
- **Completion:** implementation finished + acceptance criteria satisfied + related tests pass + no new review issues.
- **Dependency ordering:** do not start a task whose `Dependencies` are unresolved.
- **Atomicity:** if a task still feels too large, split it and mint new R-xxx IDs at the end — never reuse an ID.
- **Status lifecycle:** `Not Started` → `In Progress` → `Done`; mark `Blocked` when a dependency is unresolved.

---

## Priority Queue

### Critical

| ID | Finding |
| --- | --- |
| R-001 | code/01_platform/04_scripts/soak-reconnect-loop.sh — The core premise of this soak test is broken by the service's own design |
| R-002 | code/02_services/01_ingestion/Dockerfile — This Dockerfile now assumes the build context is the Maven reactor root (`code/`) — it does `COPY pom.xml .`, |
| R-003 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java — This drop-and-recreate path is destructive for DDL-provisioned clusters |

### High

| ID | Finding |
| --- | --- |
| R-004 | code/01_platform/04_scripts/soak-reconnect-loop.sh — grep -c '"feed":"hft"' on $LOG_FILE (Java stdout/stderr tee'd into logs/ingestion.log) can never match: the Go |
| R-005 | code/02_services/01_ingestion/Dockerfile — The runtime image launches Java via the copied docker-entrypoint.sh using `java -cp /app/ingestion.jar` withou |
| R-006 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java — verifyTables() compares the DDL-created tables' column counts against the in-code schemas, but these schemas d |
| R-007 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java — ALL_TABLES omits `ingestion_quarantine` (DDL 21_ingestion_quarantine.sql, 10 columns) |
| R-008 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java — POSTBACK_QUARANTINE_SCHEMA is stale: it carries the old 18-column shape (broker_order_id, client_order_ref, br |
| R-009 | code/01_platform/01_docker/docker-compose.yml — The ingestion `environment:` block omits required Java config keys that `IngestionConfig.validate()` treats as |
| R-010 | code/01_platform/02_sql/ddl/02_raw_table_1.sql — `ack_ts BIGINT NOT NULL` cannot be satisfied truthfully on an immutable append-only LOG table |
| R-011 | code/01_platform/02_sql/ddl/02_raw_table_1.sql — The WITH clause drops all previously configured datalake/Iceberg options (`table.datalake.enabled/format/fresh |
| R-012 | code/01_platform/02_sql/ddl/03_feature_candles_15s.sql — This DDL rewrite changes the table contract (adds `exchange`/`symbol`, replaces `candle_version` with `algorit |
| R-013 | code/01_platform/02_sql/ddl/09_order_lifecycle.sql — The header declares "Scope: account_scope_id", but the table has no `account_scope_id` column and the primary |
| R-014 | code/01_platform/04_scripts/ddl_apply.py — Control-flow defect: this branch returns 0 whenever the committed schema_manifest.json already matches the com |
| R-015 | code/01_platform/04_scripts/digest-pin.sh — `docker manifest inspect` (without `--verbose`) prints the raw manifest JSON, which contains the `config` and |
| R-016 | code/01_platform/04_scripts/run-monday-gates.sh — The Java gate enables the E2E test (INGESTION_INT_TEST_E2E=true), but the preceding `go test -count=1 ./...` s |
| R-017 | code/01_platform/04_scripts/soak-headroom.sh — Default LOG_FILE points to `$PROJECT_ROOT/logs/ingestion.log`, but the pipeline never writes that file: log4j2 |
| R-018 | code/01_platform/04_scripts/soak-headroom.sh — This regex expects the fields `state=ACTIVE assigned=1024 acknowledged=1024 rejected=0` to be adjacent tokens, |
| R-019 | code/01_platform/04_scripts/soak-monitor.sh — The default LOG_FILE (`$PROJECT_ROOT/logs/ingestion.log`) is never created by the ingestion stack |
| R-020 | code/01_platform/04_scripts/soak-monitor.sh — Even with a corrected log path, the `ticks`/`reconnects`/`hbfail`/`stalls` columns will always be 0: bridge ti |
| R-021 | code/01_platform/04_scripts/soak-monitor.sh — Under `set -euo pipefail`, `ls /proc/$pid/fd 2>/dev/null | wc -l` will abort the entire monitor if the monitor |
| R-022 | code/02_services/01_ingestion/docker-entrypoint.sh — Environment variable mismatch between the entrypoint and the Java consumer: `InstrumentManifestLoader.loadDefa |
| R-023 | code/02_services/01_ingestion/go-bridge/main.go — `classifyAuthRefresh` returns `authResumed` as soon as `refreshErr == nil`, before checking `hasRefresh`/budge |
| R-024 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go — Use-after-release of the pooled fasthttp body buffer |
| R-025 | code/02_services/01_ingestion/pom.xml — log4j-slf4j-impl:2.25.4 is the SLF4J 1.7.x binding, but this module (and the parent-managed slf4j-api 2.0.9 us |
| R-026 | code/02_services/01_ingestion/pom.xml — This parent + sibling `com.trading:common` dependency makes the ingestion module non-self-contained: any build |
| R-027 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java — syntheticSet() generates tokens as 100_000 + i*100 + (i%10), i.e |
| R-028 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java — This throws for any record_type other than `tick`/`bridge_event` — including `broker_quarantine` |
| R-029 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java — The event vocabulary here does not match the actual BridgeEvent contract |
| R-030 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java — The CompletableFuture returned by writer.append(row) is discarded at both append sites (write and writeWithEpo |
| R-031 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java — Per-slot frame recency check will go stale during healthy steady-state operation, making isReady() permanently |
| R-032 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java — When all NTP servers are unreachable and `required=false` — which is the default, since `CLOCK_CHECK_REQUIRED` |
| R-033 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java — The CompletableFuture returned by writer.append(row) is assigned to an unused local and never observed (neithe |
| R-034 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java — The `CompletableFuture<AppendResult>` returned by `writer.append(row)` is discarded (note the `@SuppressWarnin |
| R-035 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — close() sets `closed = true` before calling flush(), but flush() begins with `if (closed) return;` |
| R-036 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — OTLP/HTTP JSON payload is not spec-compliant and will be rejected by strict collectors: (1) `asDouble` is emit |
| R-037 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java — On timeout, the append future returned by rowConverter.append() may still be in-flight, but tracker.onAppendFa |
| R-038 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java — The retryable checks return RETRYABLE immediately, short-circuiting the cause-chain walk before deeper fatal c |
| R-039 | code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java — The class Javadoc advertises this as a "plain WebSocket server" (port 8888, ws:// scheme), but the implementat |
| R-040 | code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java — The documented message contract is "newline-delimited JSON" with one tick object per line, but `generateTicks( |
| R-041 | code/common/invariants/LiveMoneyGuard.java — This file lives at `code/common/invariants/LiveMoneyGuard.java`, but the `common` Maven module compiles only t |
| R-042 | code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java — Ambiguous time unit: the field comment says the standard feed provides epoch seconds while HFT provides epoch |
| R-043 | code/common/src/main/java/com/trading/common/identity/IdentityModel.java — Inconsistent value semantics: only InstructionId, ClientOrderRef, BrokerOrderId, InstrumentToken, and Exchange |
| R-044 | code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java — Logic error: when `to == PREPARED` (the only enum value falling into this `default` branch), `legalSources` be |
| R-045 | code/common/src/main/java/com/trading/common/observability/Json.java — The shared mutable `first` flag is reset to `true` at the start of every `obj(...)`/`arr(...)` block and is ne |
| R-046 | code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java — `event.level` and `event.message` are concatenated into the JSON document without going through `escapeJson()` |
| R-047 | code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java — Same JSON-escaping gap in `emitAlert()`: `service`, `host`, `vmId`, `environment`, `correlationId`, `category` |
| R-048 | code/common/src/main/java/com/trading/common/version/VersionGate.java — Placeholder versions bypass the gate |
| R-049 | code/run-ingestion-full.sh — In this background pipeline, `$!` is the PID of the *last* pipeline element (`tee -a`), not the JVM |
| R-050 | code/smoke-test.sh — SmokeTest calls IngestionConfig.validate(), which treats ARROW_MAX_EVENT_AGE_MS and ARROW_MAX_FUTURE_EVENT_SKE |
| R-051 | code/logs/ingestion.json — Sensitive operational data is committed to version control in this runtime log: absolute host paths exposing t |
| R-052 | start-all.sh — The fallback credential branch pipes raw `.env` content through `eval` without quoting the value, so any ARROW |
| R-053 | run-ingestion.sh — The `exec` target is a hardcoded absolute path tied to a single developer's machine (`/home/saurabh/...`) |

### Medium

| ID | Finding |
| --- | --- |
| R-054 | code/01_platform/02_sql/ddl/02_raw_table_1.sql — The schema declares `bid_price_paise`/`bid_qty`/`ask_price_paise`/`ask_qty` and marks `last_price_paise`/`last |
| R-055 | code/01_platform/02_sql/ddl/03_feature_candles_15s.sql — The header documents "Retention: ≤7 trading days (ceiling); extend while EOD offload unverified", but `table.r |
| R-056 | code/01_platform/04_scripts/digest-pin.sh — The script does not validate the input reference |
| R-057 | code/01_platform/04_scripts/soak-monitor.sh — `pgrep -f "$1" | tail -1` selects the numerically highest PID, not the newest or intended process |
| R-058 | code/02_services/01_ingestion/go-bridge/main.go — Standard mode writes ticks through the global `encoder` (json.NewEncoder(os.Stdout)) while all events — includ |
| R-059 | code/02_services/01_ingestion/go-bridge/main.go — The heartbeat and watchdog goroutines only exit on `ctx.Done()` or their own failure condition; they are not s |
| R-060 | code/02_services/01_ingestion/pom.xml — The shaded fat jar only merges the manifest |
| R-061 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java — loadFromPath() returns approved=true unconditionally after parsing, even when zero data rows were loaded (head |
| R-062 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java — The Connection created in the constructor is a local variable and is never stored or exposed, yet close() stat |
| R-063 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java — For connection-wide events (before == null and no instrument context), this row writes last_tick_token = 0L in |
| R-064 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java — queryNtp() accepts any datagram without validating that it is a real NTP server response: it never checks `res |
| R-065 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — Thread-safety race on the latency ring buffer: `latencyRingPos` is read-check-then-written and `latencyRing` i |
| R-066 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — esc() escapes only backslash and double-quote, not JSON control characters |
| R-067 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — Two robustness defects in the flush path: (1) reportHealth(true) is invoked even when the collector returns HT |
| R-068 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java — close() never invokes rowConverter.close() |
| R-069 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java — write() performs a check-then-act on the volatile closed flag: the entry check can pass, then a concurrent clo |
| R-070 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java — The substring check `name.contains("Retriable")` has two problems: (1) it uses the uncommon spelling "Retriabl |
| R-071 | code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java — `tickRatePerSec` is misleading: with the fixed 10 ms scheduler interval (100 batches/sec) and `batchSize = cei |
| R-072 | code/common/invariants/LiveMoneyGuard.java — All ten fact booleans default to `false` in the builder, so an omitted setter is indistinguishable from an exp |
| R-073 | code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java — Mode declares LTPC, QUOTE and FULL, but the fields only capture last-trade data (lastTradedPrice, lastTradedQt |
| R-074 | code/common/src/main/java/com/trading/common/identity/IdentityModel.java — Constructors accept null/blank values without validation |
| R-075 | code/common/src/main/java/com/trading/common/identity/IdentityModel.java — InstrumentToken performs no range/sign validation even though it is documented as the join key across market/p |
| R-076 | code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java — The stale-epoch rejection records `GateState.HALTED` as the `from` state instead of the actual `currentState` |
| R-077 | code/common/src/main/java/com/trading/common/observability/Json.java — `escape(null)` returns `""`, so `kv(k, null)` silently serializes a null value as an empty JSON string (`"k":" |
| R-078 | code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java — `escapeJson()` only escapes `"`, `\`, `\n` and `\r` |
| R-079 | code/common/src/main/java/com/trading/common/version/VersionGate.java — requirePinned trims the value only for the 'latest' comparison but returns the original, untrimmed string |
| R-080 | code/run-ingestion-full.sh — `cleanup_stale_bridges` only matches `arrow-bridge` binaries |
| R-081 | code/run-ingestion-full.sh — The token list is derived with `cut -f4 | tail -n +2 | paste` without validating the expected count (1,024), t |
| R-082 | Makefile — The `-o` (offline) flag makes `make build`/`make test` fail on any machine whose local Maven repository (~/.m2 |
| R-083 | code/01_platform/01_docker/ddl-init.sh — This script claims to create the `default` database (header comment, this log line, and the 'done' message), b |
| R-084 | code/01_platform/02_sql/ddl/05_signal_candidates.sql — This table is declared as an immutable LOG (no primary key), and the schema manifest registers it as `table_ki |
| R-085 | code/01_platform/02_sql/ddl/08_fills.sql — The header declares "Scope: account_scope_id" and this table feeds the encrypted 7-year audit lake, but the sc |
| R-086 | code/01_platform/02_sql/ddl/13_order_correlation.sql — The primary key is `instruction_id` while `execution_attempt_id` is NOT NULL |
| R-087 | code/01_platform/02_sql/ddl/14_execution_audit.sql — Retention configuration contradicts the stated requirement |
| R-088 | code/01_platform/02_sql/ddl/16_postback_quarantine.sql — `table.retention.days` is not a standard Fluss log-table option — the previous version of this DDL used `table |
| R-089 | code/01_platform/02_sql/ddl/18_safety_halt_requests.sql — halt_request_id is a deterministic SHA-256 of the transition tuple, but this is an append-only LOG table with |
| R-090 | code/01_platform/02_sql/ddl/20_instruments.sql — The header says this table retains "current and prior instrument manifest versions", but the primary key is on |
| R-091 | code/01_platform/04_scripts/cep_guard.sh — Silent false-negative: if `$ROOT` is a typo or the script is invoked from an unexpected working directory, `gr |
| R-092 | code/01_platform/04_scripts/version_matrix_verify.py — `yaml.safe_load(fh)` returns `None` for an empty or comment-only file, and raises `yaml.YAMLError` for malform |
| R-093 | code/01_platform/04_scripts/version_matrix_verify.py — The loop assumes every row is a dict and that `proposed_version` / `evidence_owner` / `evidence_method` / `com |
| R-094 | code/02_services/01_ingestion/go-bridge/faketool/main.go — Data race on the `connections` counter |
| R-095 | code/02_services/01_ingestion/go-bridge/hft_slot.go — Stall detection has a blind window that defeats its purpose: `BeginConnect()` resets `lastFrame` to the zero t |
| R-096 | code/02_services/01_ingestion/go-bridge/hft_slot.go — `BeginConnect()` does not honor the `closed` flag, unlike `SetState()`/`Close()` |
| R-097 | code/02_services/01_ingestion/go-bridge/ndjson.go — validateBridgeEvent is defined here but never invoked by production code: EmitEvent writes the event directly |
| R-098 | code/02_services/01_ingestion/go-bridge/subscription_plan.go — The SHA-256 fingerprint is computed only from SlotID/ConnectionID/Tokens; the per-slot `Requests` partitioning |
| R-099 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go — Request bodies are assembled with `fmt.Sprintf` raw string interpolation (`userID`/`password` here, and `reque |
| R-100 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go — `Authenticate` reports success whenever `Status == "success"` without checking that `Data.Token` is non-empty, |
| R-101 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go — `Authenticate`/`AutoLogin` call `c.request`/`c.rawRequest`, which execute on `&fasthttp.Client{}` (no `ReadTim |
| R-102 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go — Data race on the zstd decoder between Close() and the read loop |
| R-103 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go — No read/write deadlines are set anywhere in this client, so a wedged TCP connection blocks indefinitely |
| R-104 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go — The historical data endpoint hardcodes the production host `https://historical-api.arrow.trade` and goes throu |
| R-105 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/orders.go — The broker's `errorCode` and `message` are parsed and logged but dropped from the returned error — callers rec |
| R-106 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/user.go — GetUserDetails performs an unbounded blocking HTTP request: it calls c.request(), which executes c.HTTPClient. |
| R-107 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java — The GenericRow is fully materialized before `writer.append(row)` is invoked, so the comment 'set after append' |
| R-108 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — The ING-1 broker-staleness detection is effectively a no-op |
| R-109 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — The ING-3 Slow-Fluss pause percentage is computed against the static `AppendTracker.MAX_PENDING_RECORDS` (10,0 |
| R-110 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — On a bridge restart, the subscription-completeness state is not reset |
| R-111 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — `lastTickSnapshot` is updated unconditionally even when `writer.write()` returned REJECTED, TIMEOUT, UNCERTAIN |
| R-112 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java — This stub unconditionally acknowledges every append as successfully persisted (with a locally incremented offs |
| R-113 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java — The volatile `closed` field is written by `close()` but never read, and `append()` does not check it — so `clo |
| R-114 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java — requiredLong() accepts 0 for ARROW_MAX_EVENT_AGE_MS / ARROW_MAX_FUTURE_EVENT_SKEW_MS (only negatives are rejec |
| R-115 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java — The Builder does not validate `instrumentToken` (it defaults to 0) before `build()`, yet `equals()`/`hashCode( |
| R-116 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java — `builder.lotSize > 0 ? builder.lotSize : 1` silently coerces any non-positive lot size (0, negative, or omitte |
| R-117 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java — NPE risk in the shutdown path: `journalPath.getParent()` can return `null` when the configured path is a bare |
| R-118 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java — `totalBytesAccepted` is declared with a getter and consumed at shutdown by `IngestionService.shutdown()` (unce |
| R-119 | code/02_services/01_ingestion/src/main/resources/log4j2.xml — This CONSOLE_PATTERN renders a non-empty `correlation_id` three times with no separator |
| R-120 | code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java — The Flink job graph is empty — no source, transformation, or sink is added before `env.execute()` |
| R-121 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java — `clientOrderRef.value()` is dereferenced before any null check on `clientOrderRef` itself |
| R-122 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java — `price` is accepted as an arbitrary String with no format or cross-field validation |
| R-123 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java — Mandatory Arrow fields — `exchange`, `instrumentToken`, `transactionType`, `order`, `product`, and `validity` |
| R-124 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java — When `requestTime` is absent from the response or is not a JSON number (e.g., the broker returns it as a quote |
| R-125 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java — OrderStatus.from uses a strict valueOf() after trim/uppercase, which throws an unchecked IllegalArgumentExcept |
| R-126 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java — ReportType.from throws IllegalArgumentException for any unrecognized value, and the wire vocabulary declared h |
| R-127 | code/common/src/main/java/com/trading/common/config/PlatformConfig.java — This validation can never trigger: `DEDUP_TTL_MS` and `CANDLE_WINDOW_MS` are `static final` compile-time const |
| R-128 | code/common/src/main/java/com/trading/common/model/MarketTick.java — `isValid()` compares `validityState` against the exact literal `"VALID"`, but the Ingestion pipeline (`RealFlu |
| R-129 | code/common/src/main/java/com/trading/common/observability/SafetyHaltRequest.java — `isIdempotentDuplicate` returns true whenever `prior == incoming`, which treats repeated `FAILED` or `PENDING` |
| R-130 | code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java — The Javadoc states 12 required fields (timestamp, level, service, component, subsystem, host, vm_id, environme |
| R-131 | code/pom.xml — This JVM flag is only wired into the surefire (test) argLine, but the comment states it is required by Arrow M |
| R-132 | start-all.sh — The script depends on `nc` (netcat) for the Fluss reachability check without verifying it is installed |
| R-133 | code/02_services/04_executor/Dockerfile — Pinning to the exact patch `3.11.9` is quite stale (released April 2024), so the resulting image misses subseq |
| R-134 | code/common/src/main/java/com/trading/common/observability/AuditLogger.java — The redaction check is case-sensitive and exact-match only |
| R-135 | run-ingestion.sh — The secrets file is sourced after only an existence check; nothing verifies it is not group/world readable |
| R-136 | code/01_platform/02_sql/ddl/06_ranking_results.sql — The table is declared as a "per-evaluation ranking audit" and `rank`/`selected` only have meaning within one e |
| R-137 | code/01_platform/04_scripts/soak-monitor.sh — Every sample re-greps the entire journal for each pattern — the journal rolls at 64 MB, so over a multi-hour s |
| R-138 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go — The HTTP client is created with no timeout and the three request helpers use plain `HTTPClient.Do` (no context |
| R-139 | code/02_services/01_ingestion/go-bridge/token_provider.go — Refresh holds p.mu while invoking refreshFn, which for a token provider typically performs network I/O |
| R-140 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — `refreshResourceMetrics()` is invoked once per processed frame and performs a directory listing of `/proc/self |
| R-141 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java — The Fluss `Connection` opened in the constructor is a local variable — it is never stored, and `close()` only |
| R-142 | code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java — `generateTicks()` runs on the single scheduled executor thread and synchronously `write`/`flush`es to every co |
| R-143 | Makefile — The new targets `cep-check`, `test`, and `test-ingestion` are not declared in `.PHONY` |
| R-144 | code/01_platform/02_sql/ddl/09_order_lifecycle.sql — The header states this table is "rebuildable from Fills audit", but the source `Fills` table has `table.retent |
| R-145 | code/01_platform/02_sql/ddl/13_order_correlation.sql — The header comment declares the table scope as `account_scope_id`, but no `account_scope_id` column exists in |
| R-146 | code/01_platform/02_sql/ddl/16_postback_quarantine.sql — The header claims "Retention: until disposition + buffer" and "Lake: encrypted evidence per policy", but the W |
| R-147 | code/01_platform/04_scripts/ddl_apply.py — Exception-hardening gap: main() only catches RuntimeError, but several non-RuntimeError failures can occur ups |
| R-148 | code/01_platform/04_scripts/digest-pin.sh — All three resolver branches redirect stderr to /dev/null, so registry authentication errors, unreachable regis |
| R-149 | code/01_platform/04_scripts/run-monday-gates.sh — PROJECT_ROOT defaults to a hard-coded personal absolute path |
| R-150 | code/01_platform/04_scripts/run-monday-gates.sh — No preflight validation: with `set -euo pipefail`, a missing `go`/`mvn`/`java` or a nonexistent project direct |
| R-151 | code/01_platform/04_scripts/soak-monitor.sh — The hard-coded `PROJECT_ROOT=/home/saurabh/...` default makes the script non-portable |
| R-152 | code/02_services/01_ingestion/.dockerignore — This `.dockerignore` is placed in a subdirectory of the build context and will never be honored by `docker bui |
| R-153 | code/02_services/01_ingestion/go-bridge/hft_slot.go — `Run()` is a no-op session driver: it only blocks on `ctx.Done()` and returns `nil` — it never calls `BeginCon |
| R-154 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java — Detecting an existing table by scanning the exception message for the substring 'already exist' is fragile: th |
| R-155 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java — The viewer reads columns via hardcoded positional indexes (4, 5, 6, 11, 14, 15, 16, 25) that are coupled to th |
| R-156 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java — MAX_PENDING_APPEND_BYTES is validated twice with conflicting behavior |
| R-157 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java — This model class is dead code: it is never referenced anywhere in the codebase |
| R-158 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java — The `status` field is an unvalidated free-form String with magic values (OPEN/ACKNOWLEDGED/CLOSED) |
| R-159 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java — The builder silently substitutes defaults for potentially required fields: `connectionEpoch` is a primitive `l |
| R-160 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java — appendAckTs is a final field hardcoded to Instant.EPOCH in the constructor, and the Builder exposes no field o |
| R-161 | code/02_services/01_ingestion/src/main/resources/log4j2.xml — `HOST`, `ENV`, and `VM_ID` are declared as Log4j2 `<Property>` lookups but are never referenced by any appende |
| R-162 | code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java — This is an immutable value object intended for pipeline data exchange but it does not implement equals/hashCod |
| R-163 | code/common/src/main/java/com/trading/common/model/MarketTick.java — `byte[] rawPayload` is a mutable array component in a record that is documented as the normalized immutable ma |
| R-164 | code/logs/ingestion.json — This file is a generated runtime artifact, not source: log4j2.xml defines a `RollingFile` appender writing JSO |
| R-165 | code/run-ingestion-full.sh — `REPO_ROOT` and `ARROW_INSTRUMENT_MANIFEST` are hard-coded to a specific user's absolute paths |
| R-166 | code/smoke-test.sh — Hardcoded absolute path tied to this developer's machine makes the script non-portable (breaks for any other c |
| R-167 | start-all.sh — PROJECT_ROOT and MANIFEST default to a hardcoded developer path (`/home/saurabh/Jupyter_notebook/...`, and the |
| R-168 | code/01_platform/02_sql/ddl/03_feature_candles_15s.sql — The previous WITH clause carried the datalake/iceberg settings (`table.datalake.enabled`, `table.datalake.form |
| R-169 | code/01_platform/04_scripts/soak-monitor.sh — The `bridge_threads` column approximates goroutine count with the OS `Threads:` count from `/proc/<pid>/status |
| R-170 | code/01_platform/04_scripts/soak-reconnect-loop.sh — The script's headline claim is to 'verify NO leak', and it dutifully records java_fds / bridge_fds / java_thre |
| R-171 | code/02_services/01_ingestion/src/main/resources/log4j2.xml — `LOG_DIR` defaults to the relative path `logs` |
| R-172 | code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java — Unit inconsistency in the same broker module: `ArrowOrderResponse.requestTime` is documented as epoch **ms**, |

### Low

| ID | Finding |
| --- | --- |
| R-173 | code/01_platform/04_scripts/soak-reconnect-loop.sh — java_pid_before is captured once at startup and never refreshed, so every cycle's 'before' FD/thread readings |
| R-174 | code/01_platform/04_scripts/soak-headroom.sh — `int(n*0.99)+1` overestimates p99 whenever `n*0.99` is an integer: for n=100 it yields idx=100 (the maximum), |
| R-175 | code/02_services/01_ingestion/go-bridge/main.go — `envOrFatal` exits with status 1 for a missing required environment variable, but the file's own exit-status c |
| R-176 | code/02_services/01_ingestion/go-bridge/main.go — Instrument tokens are parsed with `strconv.Atoi` and narrowed to `int32` without range or sign validation |
| R-177 | code/02_services/01_ingestion/go-bridge/main.go — The single-socket policy violation path emits `Event: "auth_failure"`, which the NDJSON contract reserves for |
| R-178 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java — `isFrameRecent()` treats the default value 0 of `lastFrameReceivedNanos` as a valid timestamp |
| R-179 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — The latency buffer never wraps: once 1024 samples are collected between flushes, every further recordAppendLat |
| R-180 | code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java — If `BufferedWriter`/`OutputStreamWriter` construction throws IOException, the accepted client `Socket` is neve |
| R-181 | code/common/invariants/LiveMoneyGuard.java — `evaluate` dereferences `facts` without a null check (`facts.triggered()` would throw an NPE at the call site) |
| R-182 | code/common/src/main/java/com/trading/common/version/VersionGate.java — requireAllPinned dereferences entries without a null check; a null list would throw a raw NullPointerException |
| R-183 | code/01_platform/01_docker/ddl-init.sh — The `${COORDINATOR%%:*}` / `${COORDINATOR##*:}` split assumes exactly `host:port` |
| R-184 | code/01_platform/02_sql/ddl/08_fills.sql — The header requires "Retention: ≥3 complete trading days", but `table.retention.days = '3'` is a calendar-day |
| R-185 | code/02_services/01_ingestion/go-bridge/ndjson.go — The doc comment states the per-slot counter "resets when the process restarts (a new connection epoch begins)" |
| R-186 | code/02_services/01_ingestion/go-bridge/ndjson.go — sha256Hex documents that it returns the SHA-256 hex digest of b but special-cases empty input to return "" |
| R-187 | code/02_services/01_ingestion/go-bridge/ndjson.go — s = s[:512] truncates on a byte boundary and can split a multi-byte UTF-8 rune mid-sequence when a broker-supp |
| R-188 | code/02_services/01_ingestion/go-bridge/subscription_plan.go — `BuildSubscriptionPlan` validates that the token list is non-empty and duplicate-free but never checks the tok |
| R-189 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go — `token` and `interval` are interpolated into the URL path with `fmt.Sprintf` without any path escaping (same a |
| R-190 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java — In `exceptionally`, `ex.getCause()` can be null when the future completes with a plain (non-CompletionExceptio |
| R-191 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java — If `connection.getTable(path)` or `table.newAppend().createWriter()` throws during startup, the already-create |
| R-192 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — `readFdLimit()` reads `/proc/sys/fs/file-max`, which is the *system-wide* FD limit, not the process's per-proc |
| R-193 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java — The constructor rejects only `null` for `tradingSymbol`/`exchange`; empty or whitespace-only values pass valid |
| R-194 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java — `escape()` only escapes backslash and double-quote |
| R-195 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java — The `halted` check at the top of `tryAccept` is a non-atomic check-then-act: a thread can pass `if (halted)` a |
| R-196 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java — In the warning branch the pluggable `BackpressureListener` is invoked synchronously while the pending counters |
| R-197 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java — `disclosedQty` is not validated and can be negative |
| R-198 | code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java — `data.get("orderNo")` will throw a bare NPE if the caller passes a null map (e.g., when a JSON body of `null` |
| R-199 | code/common/src/main/java/com/trading/common/config/PlatformConfig.java — When `containerMemoryLimitBytes <= 0` (e.g., an unreadable cgroup limit surfaced as 0, or a negative value fro |
| R-200 | code/02_services/01_ingestion/go-bridge/supervisor.go — Each slot runs in a bare goroutine with no `recover` |
| R-201 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go — When the API returns `data: null` (or `[]`) with status `success`, `json.Unmarshal` into `[]map[string]any` su |
| R-202 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go — When the server replies `{"data": null, "status": "success"}`, `result.Data` is a nil map and is returned with |
| R-203 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go — parseQuote reuses parseLTPC, which computes NetChange from bytes 13:17 as if they were the close price |
| R-204 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go — ReadTicks (and the analogous ReadUpdates) check ctx.Done() only at the top of the loop; the blocking conn.Read |
| R-205 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java — Unlike the string fields (which are guarded by `text()` via `isNullAt`), the numeric fields `event_time` (11), |
| R-206 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEvent.java — The Go-side bridge contract (`validateBridgeEvent` in ndjson.go) requires `received_ts_ms > 0`, but this const |
| R-207 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BrokerQuarantine.java — The record is documented as "Immutable" and its constructor validates `payloadHash` against `rawPayload`, but |
| R-208 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/ReadinessFile.java — `Files.move` with `ATOMIC_MOVE` throws `AtomicMoveNotSupportedException` on filesystems that do not support at |
| R-209 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java — The Builder defaults eventTime to Instant.EPOCH and the constructor never assigns null, so the `eventTime != n |
| R-210 | code/02_services/04_executor/main.py — The boolean env-var check is case-sensitive: values like `TRUE`, `True`, or `1` will silently evaluate to disa |
| R-211 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/.gitignore — Ignoring `go.sum` in a Go module is generally discouraged: it holds cryptographic checksums that verify depend |
| R-212 | code/run-ingestion-full.sh — `$SECRETS_FILE` holds `ARROW_APP_SECRET` and the TOTP key, but the script never verifies its permissions (it o |
| R-213 | code/02_services/01_ingestion/go-bridge/faketool/main.go — A new zstd encoder (including its internal state/table initialization) is constructed and closed for every out |
| R-214 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java — Every NDJSON line is fully JSON-parsed here (`mapper.readTree`) and again in `parse()`, and then a third time |
| R-215 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java — `String.matches("[0-9a-f]{64}")` compiles a fresh regex Pattern on every invocation |
| R-216 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java — The defensive clone in rawPayload() is taken multiple times per tick on the ingestion hot path: RawTickWriter. |
| R-217 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java — `Files.lines(journalPath).count()` opens an I/O-backed stream that is never closed — each call leaks a file de |
| R-218 | code/common/src/main/java/com/trading/common/observability/Json.java — `String.format("\\u%04x", (int) c)` is invoked inside the per-character loop for every escaped control charact |
| R-219 | code/01_platform/02_sql/ddl/16_postback_quarantine.sql — `reason` and `disposition` are NOT NULL string columns but their allowed values are no longer documented or co |
| R-220 | code/01_platform/04_scripts/ddl_apply.py — Dead assignment: `_existing_raw` is destructured from load_existing_manifest() but never referenced afterwards |
| R-221 | code/01_platform/04_scripts/digest-pin.sh — Parsing the resolver's JSON with `grep -o | head -1` is fragile: it depends on field ordering and exact quotin |
| R-222 | code/01_platform/04_scripts/digest-pin.sh — Gating the docker branch on `docker info &>/dev/null` requires a running daemon and can block on a daemon conn |
| R-223 | code/01_platform/04_scripts/run-monday-gates.sh — On failure the script exits immediately after writing the per-suite FAIL line, so SUMMARY.txt lacks a terminal |
| R-224 | code/02_services/01_ingestion/go-bridge/hft_slot.go — The package-level helper `min(a, b int)` is never referenced anywhere in the go-bridge package (including the |
| R-225 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java — Contract mismatch: the class javadoc promises 'missing required keys or out-of-range values throw IllegalState |
| R-226 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java — The errors parameter of this required-with-fallback overload is never used — it always logs a warning and retu |
| R-227 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java — The class Javadoc states "All typed fields are verified and normalized before construction", but build() perfo |
| R-228 | start-all.sh — The header claims builds happen only "if they're out of date", but the script always rebuilds both the Go brid |
| R-229 | code/.dockerignore — Since the build context is the reactor root `code/` (per the ingestion Dockerfile header: "build context MUST |
| R-230 | code/01_platform/01_docker/docker-compose.yml — `FLUSS_BOOTSTRAP_SERVERS` is never consumed: the Go bridge does not read it, and the Java client reads `FLUSS_ |
| R-231 | code/01_platform/02_sql/ddl/02_raw_table_1.sql — Option/derivative metadata columns `instrument_type`, `strike_paise`, `expiry`, and `option_type` are declared |
| R-232 | code/01_platform/02_sql/ddl/10_positions.sql — `current_quantity` is a derived value (open_quantity - closed_quantity) |
| R-233 | code/01_platform/02_sql/ddl/12_execution_attempts.sql — The header declares `Scope: execution_partition_id, account_scope_id`, but the schema has no `account_scope_id |
| R-234 | code/01_platform/02_sql/ddl/12_execution_attempts.sql — This is a KV state table whose row is upserted as the attempt advances through phases (PREPARED → SUBMITTING → |
| R-235 | code/01_platform/02_sql/ddl/17_postback_projection_ledger.sql — Typo in the column name: `completeted_ts` should be `completed_ts` |
| R-236 | code/01_platform/04_scripts/soak-headroom.sh — The header documents `capacity_used = acknowledged_tokens / assigned_tokens` and `headroom = 1 - capacity_used |
| R-237 | code/01_platform/04_scripts/soak-headroom.sh — The default PROJECT_ROOT is a developer-specific absolute path (`/home/saurabh/...`), so on any other machine |
| R-238 | code/01_platform/04_scripts/soak-headroom.sh — OUT_DIR is defined but never used — no directory is created and no output file is written |
| R-239 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go — `Login()` has no return value and swallows authentication errors (only logs them), so callers cannot determine |
| R-240 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/constants.go — The order-type constants define two different wire encodings for the same order type: `OrderTypeSL`/`OrderType |
| R-241 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/limits.go — The non-success error discards the actual API status, unlike the sibling SDK methods GetMargin/GetUserDetails |
| R-242 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/margin.go — On a non-success response, the server-provided error details are lost: `MarginResponse` has no `Message`/`Erro |
| R-243 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go — `mode` (type `InfoQuoteMode`) is interpolated into the URL path without being validated against the three allo |
| R-244 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java — `packet.validity().name().contains("NON_TRADE")` is a fragile substring match on an enum name |
| R-245 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — `metrics.setManifestVersion(1)` hardcodes the schema/manifest version, while `main()` already loads `manifestR |
| R-246 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java — `updateReadinessFile()` is only invoked from `processBridgeEvent()` |
| R-247 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java — manifestVersion is hardcoded to 1 in both the production CSV path and the synthetic fallback, so the version c |
| R-248 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java — `Result[] out` is a fragile, undocumented out-parameter: the method dereferences `out[0]` in every branch with |
| R-249 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java — The `after` parameter is documented as "first tick after gap (null if none yet)" and `note` as an operator not |
| R-250 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java — `sha256()` hardcodes the literal `"SHA-256"`, while the class-level `ALGORITHM` constant is used only for `Res |
| R-251 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java — `diagnostics()` — intended as 'a human-readable readiness breakdown' — omits the telemetry readiness dimension |
| R-252 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java — The Builder never validates required inputs |
| R-253 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java — The Connection and Table created in the constructor are local variables that are never retained or closed; clo |
| R-254 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java — BUCKET_COUNT = 8 is declared but never referenced anywhere in this class |
| R-255 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java — Dead code: `Instant now = Instant.now();` is declared but never used anywhere in this method (the row uses `de |
| R-256 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java — Unused import `java.util.UUID` — it is not referenced anywhere in this class |
| R-257 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java — `java.io.BufferedWriter` is imported but never used anywhere in this class — dead import that should be remove |
| R-258 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — decodeReasonCounters is write-only: it is populated in incrementDecodeError() but never read, serialized, or e |
| R-259 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java — AppendOutcome.timeout(...) and Status.TIMEOUT are defined but never produced by write() — a timeout is deliber |
| R-260 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java — close() releases remaining pending bytes using an arbitrary average of 512 bytes/record ((int) (remaining * 51 |
| R-261 | code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java — The class documents itself as a "Deterministic" workload, and the tests assert that two instances with the sam |
| R-262 | code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java — The 11-parameter positional constructor has three adjacent `long` fields (`fillQuantity`, `fillPrice`, `fillTi |
| R-263 | code/common/src/main/java/com/trading/common/config/PlatformConfig.java — `BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` duplicates `FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` a |
| R-264 | code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java — Attribute keys from `event.toAttributes()` are written to the JSON document without `escapeJson()` while only |
| R-265 | code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java — The public constant `TRADING_ALERTS_STREAM` is never referenced in this class — `emitAlert()` hardcodes the li |
| R-266 | code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java — `equals()`/`hashCode()` only consider timestampMs, level, service, message, and correlationId, while ignoring |
| R-267 | code/common/src/main/java/com/trading/common/schema/SchemaManifestEntry.java — `schemaState` is declared as a raw `String` even though the same package already defines a dedicated `SchemaSt |
| R-268 | code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java — `isPlaceholder` compares the input against the constant *values*, but the class-level javadoc requires each of |
| R-269 | code/common/workitem/WorkItem.java — The spec states a blocked item "must record the owner, missing evidence, and unblock condition" (see WorkItemS |
| R-270 | code/pom.xml — The comment says "Flink (provided scope — runtime provides these)", but these managed dependencies declare no |
| R-271 | code/pom.xml — `scala.binary.version` is declared but never referenced by any dependency or plugin in this POM or its childre |
| R-272 | code/pom.xml — This aggregator only lists `common` and `02_services/01_ingestion`, but `02_services/02_compute`, `02_services |
| R-273 | show-ticks.sh — The pre-flight check hardcodes 127.0.0.1:9123, but the launched Java program honors the FLUSS_BOOTSTRAP env va |
| R-274 | code/02_services/01_ingestion/go-bridge/faketool/main.go — The documented invocation does not match the program: `FAKE_HFT_PORT` / `FAKE_HFT_DISCONNECT_AFTER` environmen |
| R-275 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java — The class javadoc claims 'Fluss infers the column schema from the first AppendWriter.append(GenericRow) call, |
| R-276 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java — The `java.util.Arrays` import is never referenced anywhere in this class |
| R-277 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java — The javadoc's column mapping references "16_postback_quarantine.sql" and documents an 18-column schema (postba |
| R-278 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java — `DoubleAdder` is imported and the class javadoc states gauges use DoubleAdder, but no DoubleAdder is ever used |
| R-279 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java — The class and write() Javadoc say 'Retry with linear backoff', but the implementation doubles the delay each a |
| R-280 | code/01_platform/02_sql/ddl/10_positions.sql — `table.retention.days = '7'` on a 'current state' projection means a position that remains open without any so |
| R-281 | code/01_platform/04_scripts/run-monday-gates.sh — Neither the Go suite nor the Java suite is wrapped with a timeout |
| R-282 | code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go — When the API returns a 200 with `status != "success"` (an application-level error), the server's `errorMessage |
| R-283 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java — Files.newBufferedReader(Path.of(path)) on Java 17 (the project targets release 17 and the launcher pins Java 1 |
| R-284 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java — `Integer.parseInt(args[0])` is unguarded: a non-numeric first argument (e.g |
| R-285 | code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java — Unclassified exceptions default to RETRYABLE |
| R-286 | code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java — The fail-closed guard compares the raw env var without trimming |

---

## Atomic Tasks

### R-001 — code/01_platform/04_scripts/soak-reconnect-loop.sh (lines 59-60)

**Status**

- [x] Done

**Priority**
Critical

**Affected Files**

- code/01_platform/04_scripts/soak-reconnect-loop.sh

**Issue**

The core premise of this soak test is broken by the service's own design. The Go bridge installs a SIGTERM handler (signal.NotifyContext with syscall.SIGTERM in go-bridge/main.go:155), so the default kill makes the bridge exit cleanly with code 0. In IngestionService.recordBridgeExit, requested = exitCode == 0 || !running is true, so bridgeRestartDecision returns NO_RESTART and the JVM calls shutdown() — the whole ingestion service terminates on the very first kill, not 'Java's runWithBridge restart loop restarts it'. Even with kill -9 (non-zero exit), MAX_BRIDGE_RESTARTS = 1 (IngestionService.java:77) means Java restarts the bridge exactly once; the second kill is TERMINAL and shuts Java down. With the default CYCLES=100, this script tears the pipeline down instead of soaking it. The kill/settle strategy must be aligned with the service's actual restart semantics (or the service's restart policy changed) before this script can fulfil its purpose.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 59-60). The reviewer's guidance: The kill/settle strategy must be aligned with the service's actual restart semantics (or the service's restart policy changed) before this script can fulfil its purpose. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-002 — code/02_services/01_ingestion/Dockerfile (lines 18-18)

**Status**

- [x] Done

**Priority**
Critical

**Affected Files**

- code/02_services/01_ingestion/Dockerfile

**Issue**

This Dockerfile now assumes the build context is the Maven reactor root (`code/`) — it does `COPY pom.xml .`, `COPY common ./common`, and `COPY 02_services/01_ingestion/...`. However, the `ingestion` service in `code/01_platform/01_docker/docker-compose.yml` (updated in the same change set) still sets `build.context: ../../02_services/01_ingestion`, i.e. only the ingestion directory. Building via `docker compose build ingestion` will therefore fail with `COPY failed: file not found` because the parent POM and the `common` module are outside that context. Either update the compose build to `context: ../..` + `dockerfile: 02_services/01_ingestion/Dockerfile`, or make the Dockerfile self-contained so it can build from the ingestion directory.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-18). The reviewer's guidance: This Dockerfile now assumes the build context is the Maven reactor root (`code/`) — it does `COPY pom.xml .`, `COPY common ./common`, and `COPY 02_services/01_ingestion/...`. However, the `ingestion` service in `code/01_platform/01_docker/docker-compose.yml` (updated in the same change set) still sets `build.context: ../../02_services/01_ingestion`, i.e. Building via `docker compose build ingestion` will therefore fail with `COPY failed: file not found` because the parent POM and the `common` module are outside that context. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-009, R-230

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-003 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java (lines 130-131)

**Status**

- [x] Done

**Priority**
Critical

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

**Issue**

This drop-and-recreate path is destructive for DDL-provisioned clusters. For every existing table whose column count differs from the in-code schema — which is the case for all 15 MINIMAL_SCHEMA tables (7 cols vs DDL 11-24) and the stale Postback_Quarantine/suspected_discontinuities schemas — ensureTables() will unconditionally drop the table and recreate it with a reduced/incorrect schema, wiping all data. A bootstrap utility should never drop tables based on a column-count heuristic; restrict mutation to creating missing tables and leave schema reconciliation to the offline DDL gate.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 130-131). The reviewer's guidance: For every existing table whose column count differs from the in-code schema — which is the case for all 15 MINIMAL_SCHEMA tables (7 cols vs DDL 11-24) and the stale Postback_Quarantine/suspected_discontinuities schemas — ensureTables() will unconditionally drop the table and recreate it with a reduced/incorrect schema, wiping all data. A bootstrap utility should never drop tables based on a column-count heuristic; restrict mutation to creating missing tables and leave schema reconciliation to the offline DDL gate. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-004 — code/01_platform/04_scripts/soak-reconnect-loop.sh (lines 48-48)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/soak-reconnect-loop.sh

**Issue**

grep -c '"feed":"hft"' on $LOG_FILE (Java stdout/stderr tee'd into logs/ingestion.log) can never match: the Go bridge emits tick NDJSON to its own stdout, which Java consumes in processLine() and never re-logs (see 'Tick stdout is never logged' in IngestionService.drainStderr). So t0_ticks and ticks_now are always 0, the recovery check [ "$ticks_now" -le "$t0_ticks" ] is always true (vacuous), and the ticks_total column in the TSV is always 0. Additionally, even if the pattern did match, a fixed cumulative baseline means the criterion passes forever once the first tick arrives after startup, so it can never detect a per-cycle stall. Count a progress signal that is actually written to the log (e.g. Java's 'ingestion: bridge exited ...' / 'subscription complete' lines) or query Fluss/readiness, and compare per-cycle deltas instead of a cumulative total.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 48-48). The reviewer's guidance: Additionally, even if the pattern did match, a fixed cumulative baseline means the criterion passes forever once the first tick arrives after startup, so it can never detect a per-cycle stall. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-005 — code/02_services/01_ingestion/Dockerfile (lines 31-31)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/Dockerfile

**Issue**

The runtime image launches Java via the copied docker-entrypoint.sh using `java -cp /app/ingestion.jar` without `--add-opens=java.base/java.nio=ALL-UNNAMED`. The project explicitly documents this flag as required for the Fluss client's shaded Arrow (MemoryUtil touches java.nio internals on JDK 17+) — it is used in the parent POM's surefire config and in both host launchers (`run-ingestion-full.sh`, `start-all.sh`). Without it, the containerized IngestionService will likely fail at startup with `InaccessibleObjectException` when the Fluss client initializes. Add the flag to the entrypoint's java command (or set `JAVA_TOOL_OPTIONS` in this stage) so the container behaves like the verified host run path.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 31-31). The reviewer's guidance: The runtime image launches Java via the copied docker-entrypoint.sh using `java -cp /app/ingestion.jar` without `--add-opens=java.base/java.nio=ALL-UNNAMED`. Add the flag to the entrypoint's java command (or set `JAVA_TOOL_OPTIONS` in this stage) so the container behaves like the verified host run path. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-009, R-230

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-006 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java (lines 59-60)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

**Issue**

verifyTables() compares the DDL-created tables' column counts against the in-code schemas, but these schemas do not match the authoritative SQL DDL for most tables: 15 of the 19 entries use MINIMAL_SCHEMA (7 columns) while the DDL creates those tables with 11-24 columns (e.g. feature_candles_15s=15, Signal_Candidates=21, Trade_Decisions=24, Fills=22, instruments=14). Postback_Quarantine (in-code 18 vs DDL 13) and suspected_discontinuities (in-code 15 vs DDL 11) are also stale. Since IngestionService.main() treats a false return as FATAL (System.exit(1)), the default production start path will always fail with 'schema-mismatch' even when the DDL has been applied correctly. The in-code schemas must match the DDL column counts, or verification should only cover the tables ingestion actually owns.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 59-60). The reviewer's guidance: verifyTables() compares the DDL-created tables' column counts against the in-code schemas, but these schemas do not match the authoritative SQL DDL for most tables: 15 of the 19 entries use MINIMAL_SCHEMA (7 columns) while the DDL creates those tables with 11-24 columns (e.g. The in-code schemas must match the DDL column counts, or verification should only cover the tables ingestion actually owns. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-007 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java (lines 297-298)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

**Issue**

ALL_TABLES omits `ingestion_quarantine` (DDL 21_ingestion_quarantine.sql, 10 columns). This table is written by the ingestion service's own QuarantineWriter (TABLE_NAME = "ingestion_quarantine"), which connects to it during service construction. As a result: ensureTables() will never create it, so the ALLOW_RUNTIME_DDL=true local path fails when QuarantineWriter connects; and verifyTables() never checks it, so the read-only verification gives false confidence that the pipeline's tables exist. The class javadoc also claims 'all 19 platform tables' while the DDL directory contains 21 files. Add ingestion_quarantine with the 10-column schema matching the DDL and the writer's GenericRow.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 297-298). The reviewer's guidance: As a result: ensureTables() will never create it, so the ALLOW_RUNTIME_DDL=true local path fails when QuarantineWriter connects; and verifyTables() never checks it, so the read-only verification gives false confidence that the pipeline's tables exist. Add ingestion_quarantine with the 10-column schema matching the DDL and the writer's GenericRow. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-008 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java (lines 206-206)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

**Issue**

POSTBACK_QUARANTINE_SCHEMA is stale: it carries the old 18-column shape (broker_order_id, client_order_ref, broker_status, resolution_ts, operator_identity, ...) while the migrated DDL 16_postback_quarantine.sql now defines a 13-column schema (quarantine_id, postback_event_id, reason, original_payload, payload_hash, broker_order_id, instruction_id, correlation_attempt, disposition, disposition_reason, quarantined_ts, disposition_ts, schema_version). DISCONTINUITY_SCHEMA has the same problem — 15 columns vs the 11 columns in 19_suspected_discontinuities.sql and the 11 values DiscontinuityWriter actually appends. These stale schemas drive the verifyTables()/ensureTables() failures above. Keep the in-code schemas in sync with the DDL and the writer rows.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 206-206). The reviewer's guidance: These stale schemas drive the verifyTables()/ensureTables() failures above. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-009 — code/01_platform/01_docker/docker-compose.yml (lines 125-126)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/01_docker/docker-compose.yml

**Issue**

The ingestion `environment:` block omits required Java config keys that `IngestionConfig.validate()` treats as mandatory: `RAW_TABLE_NAME`, `ARROW_MAX_EVENT_AGE_MS`, and `ARROW_MAX_FUTURE_EVENT_SKEW_MS` (the latter two are defined in `.env.example`, and none are in this map). Docker Compose only injects keys listed here into the container, so `IngestionConfig.validate()` will throw `IllegalStateException` ("RAW_TABLE_NAME is required but not set", etc.) and the Java service exits before the bridge ever launches — the Phase-1 pipeline cannot start as configured. Add pass-through entries for these keys (e.g. `RAW_TABLE_NAME: ${RAW_TABLE_NAME:-raw_table_1}`).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 125-126). The reviewer's guidance: The ingestion `environment:` block omits required Java config keys that `IngestionConfig.validate()` treats as mandatory: `RAW_TABLE_NAME`, `ARROW_MAX_EVENT_AGE_MS`, and `ARROW_MAX_FUTURE_EVENT_SKEW_MS` (the latter two are defined in `.env.example`, and none are in this map). Docker Compose only injects keys listed here into the container, so `IngestionConfig.validate()` will throw `IllegalStateException` ("RAW_TABLE_NAME is required but not set", etc.) and the Java service exits before the bridge ever launches — the Phase-1 pipeline cannot start as configured. Add pass-through entries for these keys (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-010 — code/01_platform/02_sql/ddl/02_raw_table_1.sql (lines 24-24)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/01_platform/02_sql/ddl/02_raw_table_1.sql

**Issue**

`ack_ts BIGINT NOT NULL` cannot be satisfied truthfully on an immutable append-only LOG table. The ack timestamp is not known at row-build time: the ingestion writer (`RealFlussRowConverter.append`) writes a placeholder `0L` (comment: 'set after append') and there is no post-append code path that updates the row — Fluss LOG rows are immutable and the append result (offset) is discarded. As a result every stored record has `ack_ts = 0`, permanently defeating the DDL's acknowledgment-timestamp contract and misleading any consumer using `ack_ts` for latency/ordering analysis. Remove the column, make it nullable with 0 meaning 'unknown', or introduce an explicit ack-time recording design before declaring it NOT NULL.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 24-24). The reviewer's guidance: The ack timestamp is not known at row-build time: the ingestion writer (`RealFlussRowConverter.append`) writes a placeholder `0L` (comment: 'set after append') and there is no post-append code path that updates the row — Fluss LOG rows are immutable and the append result (offset) is discarded. Remove the column, make it nullable with 0 meaning 'unknown', or introduce an explicit ack-time recording design before declaring it NOT NULL. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-011 — code/01_platform/02_sql/ddl/02_raw_table_1.sql (lines 42-42)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
High

**Affected Files**

- code/01_platform/02_sql/ddl/02_raw_table_1.sql

**Issue**

The WITH clause drops all previously configured datalake/Iceberg options (`table.datalake.enabled/format/freshness/auto-compaction`) and caps retention at 7 days, while the header still claims 'Lake: EOD Iceberg offload' and 'extend while EOD offload unverified'. No lake-tiering config exists elsewhere in this repo (docker-compose only has a placeholder comment about S3 lake tiering). If the EOD Iceberg offload is not enabled by some other mechanism, every accepted tick will be hard-deleted after 7 days without ever being offloaded — permanent loss of the only market-data record. Restore the datalake options or an EOD offload job, or update the header claims to match reality. Also verify `table.retention.days` is a valid Fluss 0.9.1 table property (the previous DDL used `table.log.ttl`); if unsupported, Fluss may reject the DDL or silently ignore retention.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 42-42). The reviewer's guidance: The WITH clause drops all previously configured datalake/Iceberg options (`table.datalake.enabled/format/freshness/auto-compaction`) and caps retention at 7 days, while the header still claims 'Lake: EOD Iceberg offload' and 'extend while EOD offload unverified'. Also verify `table.retention.days` is a valid Fluss 0.9.1 table property (the previous DDL used `table.log.ttl`); if unsupported, Fluss may reject the DDL or silently ignore retention. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-012 — code/01_platform/02_sql/ddl/03_feature_candles_15s.sql (lines 22-24)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
High

**Affected Files**

- code/01_platform/02_sql/ddl/03_feature_candles_15s.sql

**Issue**

This DDL rewrite changes the table contract (adds `exchange`/`symbol`, replaces `candle_version` with `algorithm_version`+`configuration_version`, renames `ingest_ts` to `output_ts`, OHLC → paise BIGINT), but the shared producer model `com.trading.common.model.Candle15s` added in this same change set still declares `candleVersion`/`ingestTs` and has no `exchange`/`symbol`/`algorithmVersion`/`configurationVersion` fields. Since Fluss auto-infers the table column schema from the first append (see ddl-init.sh / DdlBootstrap.verifyTables), a writer built on `Candle15s` would create a 12-column table that does not match this 15-column DDL, causing append/column-resolution or column-count verification failures. Align the model and this DDL (and any consumer) before landing.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 22-24). The reviewer's guidance: This DDL rewrite changes the table contract (adds `exchange`/`symbol`, replaces `candle_version` with `algorithm_version`+`configuration_version`, renames `ingest_ts` to `output_ts`, OHLC → paise BIGINT), but the shared producer model `com.trading.common.model.Candle15s` added in this same change set still declares `candleVersion`/`ingestTs` and has no `exchange`/`symbol`/`algorithmVersion`/`configurationVersion` fields. Align the model and this DDL (and any consumer) before landing. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
R-010, R-011, R-054, R-231

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-013 — code/01_platform/02_sql/ddl/09_order_lifecycle.sql (lines 21-23)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
High

**Affected Files**

- code/01_platform/02_sql/ddl/09_order_lifecycle.sql

**Issue**

The header declares "Scope: account_scope_id", but the table has no `account_scope_id` column and the primary key is only `broker_order_id`. Broker-assigned order IDs are typically unique only within a single brokerage account; in a multi-account deployment two accounts can legitimately produce the same `broker_order_id`, and this KV projection would silently overwrite one account's order state with another's. This also diverges from the sibling schema convention (e.g. `Trade_Decisions` stores `account_scope_id STRING NOT NULL` to match its scope header). Suggest adding `account_scope_id` as a column and including it in the primary key (and bucket.key) so order state is scoped correctly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 21-23). The reviewer's guidance: The header declares "Scope: account_scope_id", but the table has no `account_scope_id` column and the primary key is only `broker_order_id`. Broker-assigned order IDs are typically unique only within a single brokerage account; in a multi-account deployment two accounts can legitimately produce the same `broker_order_id`, and this KV projection would silently overwrite one account's order state with another's. This also diverges from the sibling schema convention (e.g. `Trade_Decisions` stores `account_scope_id STRING NOT NULL` to match its scope header). Suggest adding `account_scope_id` as a column and including it in the primary key (and bucket.key) so order state is scoped correctly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-014 — code/01_platform/04_scripts/ddl_apply.py (lines 252-254)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/ddl_apply.py

**Issue**

Control-flow defect: this branch returns 0 whenever the committed schema_manifest.json already matches the computed manifest — the steady-state case for a re-run — so the `--apply-verified` / `--matrix-evidence` handling below is never reached. A caller that passes both flags in the synced state gets exit code 0 and a "Manifest is current" message, silently skipping the documented step 6 (gated apply). Today the apply branch is a stub, but the structure guarantees this becomes a silent success-with-no-apply the moment real Fluss DDL application is implemented. Restructure so the apply step runs (or is refused) regardless of drift state, e.g. only return early when not applying.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 252-254). The reviewer's guidance: Restructure so the apply step runs (or is refused) regardless of drift state, e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-015 — code/01_platform/04_scripts/digest-pin.sh (lines 27-28)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/digest-pin.sh

**Issue**

`docker manifest inspect` (without `--verbose`) prints the raw manifest JSON, which contains the `config` and `layers` blob digests but NOT the manifest's own digest. The first `sha256:` match is therefore typically the config blob digest, so the script emits `img@sha256:<config-digest>` — an invalid manifest reference. When this output is written to runtime.lock, image pulls will fail (or, for a manifest list, pin an arbitrary platform manifest instead of the index). Use `docker manifest inspect --verbose` and extract the top-level `Descriptor.digest`, or `docker buildx imagetools inspect "$img" --format '{{.Manifest.Digest}}'`. Note `skopeo inspect` and `crane digest` do return the real manifest digest, so this is the highest-priority branch to fix.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-28). The reviewer's guidance: Use `docker manifest inspect --verbose` and extract the top-level `Descriptor.digest`, or `docker buildx imagetools inspect "$img" --format '{{.Manifest.Digest}}'`. Note `skopeo inspect` and `crane digest` do return the real manifest digest, so this is the highest-priority branch to fix. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-016 — code/01_platform/04_scripts/run-monday-gates.sh (lines 41-42)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/run-monday-gates.sh

**Issue**

The Java gate enables the E2E test (INGESTION_INT_TEST_E2E=true), but the preceding `go test -count=1 ./...` step does not build the binaries FullStackE2ETest execs: it defaults to `go-bridge/faketool/faketool` and `go-bridge/arrow-bridge` (relative to the surefire working dir). `faketool` is behind a `//go:build faketool` tag, so `go test ./...` doesn't even compile it, and `go test` never emits binaries into the module tree. On a clean checkout the E2E test will fail with IOException, so the gate cannot pass no matter how correct the code is. Build these binaries before running the Java suite.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 41-42). The reviewer's guidance: The Java gate enables the E2E test (INGESTION_INT_TEST_E2E=true), but the preceding `go test -count=1 ./...` step does not build the binaries FullStackE2ETest execs: it defaults to `go-bridge/faketool/faketool` and `go-bridge/arrow-bridge` (relative to the surefire working dir). `faketool` is behind a `//go:build faketool` tag, so `go test ./...` doesn't even compile it, and `go test` never emits binaries into the module tree. On a clean checkout the E2E test will fail with IOException, so the gate cannot pass no matter how correct the code is. Build these binaries before running the Java suite. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-094, R-213, R-274

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-017 — code/01_platform/04_scripts/soak-headroom.sh (lines 26-26)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/soak-headroom.sh

**Issue**

Default LOG_FILE points to `$PROJECT_ROOT/logs/ingestion.log`, but the pipeline never writes that file: log4j2.xml writes `logs/ingestion.json` (a JSON log, e.g. `code/logs/ingestion.json`), and the launcher (`run-ingestion-full.sh`) tees the console stream to `$HOME/.local/state/trading-platform/ingestion/ingestion-<timestamp>.log`. `logs/ingestion.log` does not exist in the repo, so running the script with no arguments immediately fails at the `[ -f "$LOG_FILE" ]` FATAL check. Point the default at the actual log path (e.g. `$PROJECT_ROOT/code/logs/ingestion.json`) or auto-discover the latest dated log.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 26-26). The reviewer's guidance: Default LOG_FILE points to `$PROJECT_ROOT/logs/ingestion.log`, but the pipeline never writes that file: log4j2.xml writes `logs/ingestion.json` (a JSON log, e.g. Point the default at the actual log path (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-019, R-020, R-021, R-057, R-137, R-151, R-169

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-018 — code/01_platform/04_scripts/soak-headroom.sh (lines 34-34)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/soak-headroom.sh

**Issue**

This regex expects the fields `state=ACTIVE assigned=1024 acknowledged=1024 rejected=0` to be adjacent tokens, but neither actual log source produces that. The Java service logs (IngestionService.handleBridgeEvent, line 807) the message as `bridge lifecycle event=subscription_ack slot=... state=ACTIVE epoch=1 assigned=1024 acknowledged=1024 rejected=0 ...` — `epoch=` sits between `state=` and `assigned=`, so the pattern never matches and the "Subscription acks" summary is always empty. The raw Go bridge NDJSON is also JSON (`"state":"ACTIVE","assigned_tokens":1024,...`), which this flat pattern cannot match either. The headroom evidence this script is meant to prove (plan §1379) is therefore silently absent. Align the pattern with the actual message format (tolerate `epoch=` between `state=` and `assigned=`) or parse the JSON fields.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 34-34). The reviewer's guidance: This regex expects the fields `state=ACTIVE assigned=1024 acknowledged=1024 rejected=0` to be adjacent tokens, but neither actual log source produces that. state=ACTIVE epoch=1 assigned=1024 acknowledged=1024 rejected=0 ...` — `epoch=` sits between `state=` and `assigned=`, so the pattern never matches and the "Subscription acks" summary is always empty. Align the pattern with the actual message format (tolerate`epoch=` between `state=` and `assigned=`) or parse the JSON fields. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java,`go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-019, R-020, R-021, R-057, R-137, R-151, R-169

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-019 — code/01_platform/04_scripts/soak-monitor.sh (lines 27-27)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/soak-monitor.sh

**Issue**

The default LOG_FILE (`$PROJECT_ROOT/logs/ingestion.log`) is never created by the ingestion stack. The Log4j2 file appender writes JSON-structured records to `logs/ingestion.json` relative to `code/` (i.e. `code/logs/ingestion.json`), and the launcher (`run-ingestion-full.sh`) additionally tees a copy under `~/.local/state/trading-platform/ingestion/ingestion-*.log`. With this default, `count_events` always greps a non-existent file and every event column silently reads 0 — giving false confidence in a healthy soak. Point the default at the real journal (or derive it from the script location) and, since the journal is one-JSON-object-per-line, parse it accordingly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-27). The reviewer's guidance: `code/logs/ingestion.json`), and the launcher (`run-ingestion-full.sh`) additionally tees a copy under `~/.local/state/trading-platform/ingestion/ingestion-*.log`. Point the default at the real journal (or derive it from the script location) and, since the journal is one-JSON-object-per-line, parse it accordingly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-017, R-018, R-174, R-236, R-237, R-238

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-020 — code/01_platform/04_scripts/soak-monitor.sh (lines 73-73)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/soak-monitor.sh

**Issue**

Even with a corrected log path, the `ticks`/`reconnects`/`hbfail`/`stalls` columns will always be 0: bridge tick NDJSON (`"feed":"hft"`) and lifecycle events (`"event":"reconnect"`, `heartbeat_failed`, `feed_stalled`) are emitted by the Go bridge on stdout via `BridgeEmitter` and consumed in-process by `IngestionService` — `IngestionService.java` explicitly notes "Tick stdout is never logged". None of these are written to the Log4j2 journal, so this monitor can never observe the pipeline's actual tick/health behavior. Sample the bridge stdout/NDJSON directly (or the OTel metrics endpoint) instead of grepping the journal.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 73-73). The reviewer's guidance: Sample the bridge stdout/NDJSON directly (or the OTel metrics endpoint) instead of grepping the journal. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-017, R-018, R-174, R-236, R-237, R-238

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-021 — code/01_platform/04_scripts/soak-monitor.sh (lines 49-49)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/01_platform/04_scripts/soak-monitor.sh

**Issue**

Under `set -euo pipefail`, `ls /proc/$pid/fd 2>/dev/null | wc -l` will abort the entire monitor if the monitored process exits (or is restarted) between `find_pid` and this read: `ls` returns non-zero, pipefail propagates it, and `set -e` kills the script. That is precisely the failure scenario this soak monitor is meant to observe, so the summary would be truncated exactly when the pipeline goes down. Guard the read so a missing process yields 0 without aborting.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 49-49). The reviewer's guidance: Under `set -euo pipefail`, `ls /proc/$pid/fd 2>/dev/null | wc -l` will abort the entire monitor if the monitored process exits (or is restarted) between `find_pid` and this read: `ls` returns non-zero, pipefail propagates it, and `set -e` kills the script. Guard the read so a missing process yields 0 without aborting. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-017, R-018, R-174, R-236, R-237, R-238

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-022 — code/02_services/01_ingestion/docker-entrypoint.sh (lines 19-19)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/docker-entrypoint.sh

**Issue**

Environment variable mismatch between the entrypoint and the Java consumer: `InstrumentManifestLoader.loadDefault()` reads `INSTRUMENT_MANIFEST_PATH`, never `ARROW_INSTRUMENT_MANIFEST` (the latter is consumed only by the Go bridge's `main.go`). Here the resolved path is exported solely as `ARROW_INSTRUMENT_MANIFEST`, so when an operator supplies only `ARROW_INSTRUMENT_MANIFEST` (a supported input per the resolution above), Java sees no manifest path and fails startup with an empty instrument set. And if both variables are set to different values, Java and the Go bridge would load different manifests. Suggest normalizing and exporting the resolved path under both names, e.g.: ```bash export ARROW_INSTRUMENT_MANIFEST="$MANIFEST_PATH" export INSTRUMENT_MANIFEST_PATH="$MANIFEST_PATH"```

**Implementation Plan**

Implement the corrective action described in the finding (report lines 19-19). The reviewer's guidance: Environment variable mismatch between the entrypoint and the Java consumer: `InstrumentManifestLoader.loadDefault()` reads `INSTRUMENT_MANIFEST_PATH`, never `ARROW_INSTRUMENT_MANIFEST` (the latter is consumed only by the Go bridge's `main.go`). Here the resolved path is exported solely as `ARROW_INSTRUMENT_MANIFEST`, so when an operator supplies only `ARROW_INSTRUMENT_MANIFEST` (a supported input per the resolution above), Java sees no manifest path and fails startup with an empty instrument set. And if both variables are set to different values, Java and the Go bridge would load different manifests. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-023 — code/02_services/01_ingestion/go-bridge/main.go (lines 489-492)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/go-bridge/main.go

**Issue**

`classifyAuthRefresh` returns `authResumed` as soon as `refreshErr == nil`, before checking `hasRefresh`/budget. Callers pass `refreshErr == nil` in two terminal cases: (1) token-only deployments (`refreshAuth == nil`, so the `if refreshAuth != nil && authRefreshes < 3` guard never runs) and (2) after the refresh budget guard skips an attempt. In both cases an auth failure is misclassified as a successful refresh: the error callback emits `authentication_refreshed`, the epoch returns retryable, and the slot reconnects forever instead of emitting `auth_failure`/terminating — permanently invalid credentials are silently masked. Note also that `authRefreshes` is a per-epoch local in `runHFTEpoch` and each epoch ends after the first auth error, so the 3-attempt budget never accumulates across reconnects even when `refreshAuth` exists, making the terminal branch unreachable. Reorder the checks to evaluate `!hasRefresh || authRefreshes >= 3` before the `refreshErr == nil` short-circuit, and carry the auth-refresh budget across epochs (e.g., in the slot/loop state) so the documented terminal policy actually takes effect.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 489-492). The reviewer's guidance: Callers pass `refreshErr == nil` in two terminal cases: (1) token-only deployments (`refreshAuth == nil`, so the `if refreshAuth != nil && authRefreshes < 3` guard never runs) and (2) after the refresh budget guard skips an attempt. Reorder the checks to evaluate `!hasRefresh || authRefreshes >= 3` before the `refreshErr == nil` short-circuit, and carry the auth-refresh budget across epochs (e.g., in the slot/loop state) so the documented terminal policy actually takes effect. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-097, R-185, R-186, R-187

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-024 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go (lines 90-94)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go

**Issue**

Use-after-release of the pooled fasthttp body buffer. `resp.Body()` returns a slice that aliases the response's internal buffer, and `defer fasthttp.ReleaseResponse(resp)` returns that buffer to the pool as soon as this function returns. The `[]byte` handed to the caller therefore points at recycled memory that the next request executed on this client can overwrite. In this same package, `GetCandleData` returns `json.RawMessage(bytes.TrimSpace(resp))` — a sub-slice of the released buffer retained beyond the call — and any concurrent use of the shared client will silently corrupt previously returned bodies. Copy the body before returning (e.g. `append([]byte(nil), resp.Body()...)` or `fasthttp.CopyBody`). This applies to all three helpers (`request`, `rawRequest`, `rawRequestAuth`).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 90-94). The reviewer's guidance: The `[]byte` handed to the caller therefore points at recycled memory that the next request executed on this client can overwrite. In this same package, `GetCandleData` returns `json.RawMessage(bytes.TrimSpace(resp))` — a sub-slice of the released buffer retained beyond the call — and any concurrent use of the shared client will silently corrupt previously returned bodies. Copy the body before returning (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-025 — code/02_services/01_ingestion/pom.xml (lines 37-41)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/pom.xml

**Issue**

log4j-slf4j-impl:2.25.4 is the SLF4J 1.7.x binding, but this module (and the parent-managed slf4j-api 2.0.9 used by common and the Fluss client) is on SLF4J 2.0. SLF4J 2.0 discovers providers via ServiceLoader for org.slf4j.spi.SLF4JServiceProvider, which the 1.7 binding does not implement — so at runtime logging silently falls back to NOPLogger (or fails with NoSuchMethodError). Use the SLF4J 2.0 binding artifact log4j-slf4j2-impl instead.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 37-41). The reviewer's guidance: Use the SLF4J 2.0 binding artifact log4j-slf4j2-impl instead. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-009, R-230

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-026 — code/02_services/01_ingestion/pom.xml (lines 7-12)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/pom.xml

**Issue**

This parent + sibling `com.trading:common` dependency makes the ingestion module non-self-contained: any build must run from the Maven reactor root (`code/`). The updated Dockerfile already requires that (copies `pom.xml`, `common/`, `02_services/01_ingestion/...`), but `docker-compose.yml` still builds ingestion with `context: ../../02_services/01_ingestion`, which contains neither the parent POM nor the `common/` module — so `docker compose build ingestion` fails at the first `COPY` in the Dockerfile. Update the compose build context to the reactor root (e.g. `context: ../..` from `01_docker/`) so the parent and sibling module are available.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 7-12). The reviewer's guidance: This parent + sibling `com.trading:common` dependency makes the ingestion module non-self-contained: any build must run from the Maven reactor root (`code/`). The updated Dockerfile already requires that (copies `pom.xml`, `common/`, `02_services/01_ingestion/...`), but `docker-compose.yml` still builds ingestion with `context: ../../02_services/01_ingestion`, which contains neither the parent POM nor the `common/` module — so `docker compose build ingestion` fails at the first `COPY` in the Dockerfile. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-009, R-230

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-027 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java (lines 264-264)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java

**Issue**

syntheticSet() generates tokens as 100_000 + i*100 + (i%10), i.e. {100000, 100101, 100202, ..., 104949}, but MockArrowServer.main() builds its default 50-instrument set as 100_000 + i*100, i.e. {100000, 100100, ..., 104900}. The two sets share only 5 tokens, so in the ALLOW_SYNTHETIC_MANIFEST dev path the ticks emitted by MockArrowServer will mostly be absent from the loaded manifest — IngestionService will quarantine them as MISSING_INSTRUMENT and the subscription-completeness check (seenTokens >= instrumentMap.size()) will never pass. Fix the formula to match MockArrowServer: 100_000L + i * 100L.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 264-264). The reviewer's guidance: syntheticSet() generates tokens as 100_000 + i*100 + (i%10), i.e. {100000, 100101, 100202, ..., 104949}, but MockArrowServer.main() builds its default 50-instrument set as 100_000 + i*100, i.e. The two sets share only 5 tokens, so in the ALLOW_SYNTHETIC_MANIFEST dev path the ticks emitted by MockArrowServer will mostly be absent from the loaded manifest — IngestionService will quarantine them as MISSING_INSTRUMENT and the subscription-completeness check (seenTokens >= instrumentMap.size()) will never pass. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-028 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java (lines 17-17)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java

**Issue**

This throws for any record_type other than `tick`/`bridge_event` — including `broker_quarantine`. In `IngestionService.processLine`, `parse()` is called before `parseQuarantine()` on every line, and the caller never falls through after an exception (it's caught by the outer `catch (Exception e)` and logged as an INTERNAL_ERROR quarantine). As a result, a `broker_quarantine` line will never reach `parseQuarantine()`, and the broker-quarantine handling path is effectively dead. Since the caller relies on `parse()` returning `Optional.empty()` to fall through to the next parser, non-`bridge_event` records should be skipped, not rejected.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 17-17). The reviewer's guidance: In `IngestionService.processLine`, `parse()` is called before `parseQuarantine()` on every line, and the caller never falls through after an exception (it's caught by the outer `catch (Exception e)` and logged as an INTERNAL_ERROR quarantine). As a result, a `broker_quarantine` line will never reach `parseQuarantine()`, and the broker-quarantine handling path is effectively dead. Since the caller relies on `parse()` returning `Optional.empty()` to fall through to the next parser, non-`bridge_event` records should be skipped, not rejected. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-029 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java (lines 161-164)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java

**Issue**

The event vocabulary here does not match the actual BridgeEvent contract. BridgeEvent validates `event` against exactly {slot_state, subscription_ack, heartbeat_failed, feed_stalled, disconnect, reconnect, auth_failure, bridge_shutdown}. Consequently: (1) `case "bridge_exit"` is dead code — no such event can ever reach this switch; the real bridge-exit event is `bridge_shutdown`, which is unmapped, so a bridge shutdown produces no DROP evidence; (2) the Javadoc promises `SUBSCRIPTION_PARTIAL -> FEED_HEALTH`, and IngestionService explicitly calls writeBridgeEvent for `subscription_ack` with rejectedTokens>0, but "subscription_ack" is unmapped — mapEventToReason returns null and that feed-health evidence is silently dropped. Align the switch with the validated event names (handle `bridge_shutdown` and the partial-subscription case, e.g. in writeBridgeEvent where rejectedTokens is available).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 161-164). The reviewer's guidance: BridgeEvent validates `event` against exactly {slot_state, subscription_ack, heartbeat_failed, feed_stalled, disconnect, reconnect, auth_failure, bridge_shutdown}. Consequently: (1) `case "bridge_exit"` is dead code — no such event can ever reach this switch; the real bridge-exit event is `bridge_shutdown`, which is unmapped, so a bridge shutdown produces no DROP evidence; (2) the Javadoc promises `SUBSCRIPTION_PARTIAL -> FEED_HEALTH`, and IngestionService explicitly calls writeBridgeEvent for `subscription_ack` with rejectedTokens>0, but "subscription_ack" is unmapped — mapEventToReason returns null and that feed-health evidence is silently dropped. Align the switch with the validated event names (handle `bridge_shutdown` and the partial-subscription case, e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-030 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java (lines 237-245)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java

**Issue**

The CompletableFuture returned by writer.append(row) is discarded at both append sites (write and writeWithEpoch). Fluss AppendWriter.append is asynchronous — broker-side or serialization failures are surfaced by completing the future exceptionally, not by throwing from append(), so the surrounding try/catch never sees them and the discontinuity row is silently lost with no retry, metric, or alarm. This is a money-safety evidence path; contrast RawTickWriter, which awaits the future. Attach a whenComplete/exceptionally handler (or await with timeout) and log/record a metric on failure.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 237-245). The reviewer's guidance: This is a money-safety evidence path; contrast RawTickWriter, which awaits the future. Attach a whenComplete/exceptionally handler (or await with timeout) and log/record a metric on failure. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-031 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java (lines 115-121)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java

**Issue**

Per-slot frame recency check will go stale during healthy steady-state operation, making isReady() permanently false ~15s after the last ACTIVE bridge event. `slot.lastFrameNanos` is only written by `updateSlot(...)`, and the sole caller (`IngestionService.processBridgeEvent`) passes a fresh timestamp only when a bridge lifecycle event arrives with state ACTIVE (line 758: `active ? System.nanoTime() : 0L`). The Go bridge emits no periodic lifecycle events while a slot is healthy — ticks flow as NDJSON lines that update only the *global* `setLastFrameReceived(...)` timestamp, never the per-slot one. So in steady state no new bridge event arrives, `System.nanoTime() - slot.lastFrameNanos` exceeds the 15s timeout, and `isDataReady()` (hence `isReady()`) flips to false even while frames keep flowing. Consider refreshing `lastFrameNanos` on all ACTIVE slots inside `setLastFrameReceived(...)`, or feeding actual per-tick arrival timestamps into `updateSlot(...)` so the per-slot recency reflects real frame flow.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 115-121). The reviewer's guidance: The Go bridge emits no periodic lifecycle events while a slot is healthy — ticks flow as NDJSON lines that update only the *global* `setLastFrameReceived(...)` timestamp, never the per-slot one. Consider refreshing `lastFrameNanos` on all ACTIVE slots inside `setLastFrameReceived(...)`, or feeding actual per-tick arrival timestamps into `updateSlot(...)` so the per-slot recency reflects real frame flow. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-032 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java (lines 118-121)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java

**Issue**

When all NTP servers are unreachable and `required=false` — which is the default, since `CLOCK_CHECK_REQUIRED` defaults to `false` in `IngestionConfig` — this fallback unconditionally marks the check as passed (`lastCheckPassed=true`, offset reported as 0) as long as the local wall clock is merely after 2024-01-01. This bypasses the documented `CLOCK_OFFSET_LIMIT_MS` (100 ms) readiness contract entirely: a clock skewed by hours or days still passes `HealthProbe.isClockOk()`, so ingestion starts writing ticks with wrong timestamps into raw_table while diagnostics report `clock_ok=true, clock_offset_ms=0`. Since NTP UDP/123 is often blocked in containers, this degraded path is the likely default in practice. Consider fail-closed readiness (report clock as 'unverified' instead of passing), a much tighter fallback sanity bound, and/or surfacing the degraded state so operators can distinguish a verified offset from a guessed one.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 118-121). The reviewer's guidance: When all NTP servers are unreachable and `required=false` — which is the default, since `CLOCK_CHECK_REQUIRED` defaults to `false` in `IngestionConfig` — this fallback unconditionally marks the check as passed (`lastCheckPassed=true`, offset reported as 0) as long as the local wall clock is merely after 2024-01-01. This bypasses the documented `CLOCK_OFFSET_LIMIT_MS` (100 ms) readiness contract entirely: a clock skewed by hours or days still passes `HealthProbe.isClockOk()`, so ingestion starts writing ticks with wrong timestamps into raw_table while diagnostics report `clock_ok=true, clock_offset_ms=0`. Consider fail-closed readiness (report clock as 'unverified' instead of passing), a much tighter fallback sanity bound, and/or surfacing the degraded state so operators can distinguish a verified offset from a guessed one. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-031, R-178, R-251

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-033 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java (lines 151-152)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java

**Issue**

The CompletableFuture returned by writer.append(row) is assigned to an unused local and never observed (neither awaited nor given a failure handler). Fluss appends are asynchronous, so if the append completes exceptionally, the error is silently swallowed — the quarantine evidence is lost without the ERROR log promised in the class contract ("failures are logged at ERROR and must not block the ingestion pipeline"). Note the sibling RawTickWriter awaits the future with future.get(...), which is the correct pattern in this codebase. Since this writer's entire purpose is preserving rejection evidence, attach a whenComplete/exceptionally handler (or await with a timeout) so async failures are logged and not silently dropped.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 151-152). The reviewer's guidance: The CompletableFuture returned by writer.append(row) is assigned to an unused local and never observed (neither awaited nor given a failure handler). Fluss appends are asynchronous, so if the append completes exceptionally, the error is silently swallowed — the quarantine evidence is lost without the ERROR log promised in the class contract ("failures are logged at ERROR and must not block the ingestion pipeline"). Note the sibling RawTickWriter awaits the future with future.get(...), which is the correct pattern in this codebase. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-034 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java (lines 133-136)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java

**Issue**

The `CompletableFuture<AppendResult>` returned by `writer.append(row)` is discarded (note the `@SuppressWarnings("unused")`). Fluss appends are asynchronous — this try-catch only catches synchronous exceptions, so any async write failure is silently lost, while `LOG.info("wrote ...")` is emitted before persistence is confirmed. This is the safety-halt path consumed by the Signal job: a missed/undelivered halt request means an unsafe state can go un-halted with no alert. Additionally, the caller dedups on the returned `halt_request_id`, so a failed append is never retried. Await the future (e.g. `future.get(timeout, TimeUnit.MILLISECONDS)` as RawTickWriter does) or attach an `exceptionally`/`whenComplete` handler that at minimum logs at ERROR, and only log success after the future completes.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 133-136). The reviewer's guidance: Await the future (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-035 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 141-143)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

close() sets `closed = true` before calling flush(), but flush() begins with `if (closed) return;`. The documented "final flush before shutdown" therefore never executes — every shutdown silently discards up to 10s of buffered metrics, defeating the class's lifecycle contract and the caller's expectation (IngestionService.shutdown() relies on metrics.close() to trigger a final flush). Reorder so the final flush runs before the closed flag is set.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 141-143). The reviewer's guidance: close() sets `closed = true` before calling flush(), but flush() begins with `if (closed) return;`. The documented "final flush before shutdown" therefore never executes — every shutdown silently discards up to 10s of buffered metrics, defeating the class's lifecycle contract and the caller's expectation (IngestionService.shutdown() relies on metrics.close() to trigger a final flush). Reorder so the final flush runs before the closed flag is set. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-036 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 399-399)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

OTLP/HTTP JSON payload is not spec-compliant and will be rejected by strict collectors: (1) `asDouble` is emitted as a JSON string ("asDouble":"...") but protobuf JSON requires double fields to be JSON numbers — since process.fd_usage_percent and every slot capacity gauge use this, each flush contains an invalid value; (2) sum data points omit the required `aggregationTemporality` and `isMonotonic` fields; (3) the histogram always sends `bucketCounts:[0,0,0,0]` with `explicitBounds:[0,0,0,0]` (explicitBounds must be one element shorter than bucketCounts, and bucket totals must reconcile with `count`), so the histogram is internally inconsistent even when count/sum are non-zero. Emit `asDouble` as a number and populate temporality/monotonic and the histogram fields correctly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 399-399). The reviewer's guidance: OTLP/HTTP JSON payload is not spec-compliant and will be rejected by strict collectors: (1) `asDouble` is emitted as a JSON string ("asDouble":"...") but protobuf JSON requires double fields to be JSON numbers — since process.fd_usage_percent and every slot capacity gauge use this, each flush contains an invalid value; (2) sum data points omit the required `aggregationTemporality` and `isMonotonic` fields; (3) the histogram always sends `bucketCounts:[0,0,0,0]` with `explicitBounds:[0,0,0,0]` (explicitBounds must be one element shorter than bucketCounts, and bucket totals must reconcile with `count`), so the histogram is internally inconsistent even when count/sum are non-zero. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-037 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java (lines 130-134)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

**Issue**

On timeout, the append future returned by rowConverter.append() may still be in-flight, but tracker.onAppendFailure(rowBytes) is called immediately, decrementing the pending counters before the append has actually completed. This directly violates the AppendTracker contract ('Pending counters decrease only after append completes') and abandons the future without cancellation. Under sustained Fluss latency, timed-out appends keep running in the Fluss client's background while the tracker under-counts in-flight work, so the backpressure accounting no longer reflects reality and client-side memory/connection usage can grow. Consider canceling the future (future.cancel(true)) and deferring the counter release until the future actually completes (e.g., future.whenComplete) so accounting stays accurate.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 130-134). The reviewer's guidance: Consider canceling the future (future.cancel(true)) and deferring the counter release until the future actually completes (e.g., future.whenComplete) so accounting stays accurate. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-038, R-070, R-118, R-195, R-196, R-285

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-038 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java (lines 72-79)

**Status**

- [x] Done (Phase 2, 2026-08-02)

**Priority**
High

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java

**Issue**

The retryable checks return RETRYABLE immediately, short-circuiting the cause-chain walk before deeper fatal causes can be inspected. If any wrapper exception in the chain (e.g., an ExecutionException/RuntimeException whose message contains "connection", "timeout", "leader", or "coordinator") matches a retryable pattern, the loop never reaches an underlying fatal cause such as AuthenticationException, AccessControlException, or TableNotExistException. In RawTickWriter.write() this results in a permanent failure being retried MAX_RETRY_ATTEMPTS times and ending as FAILED instead of FATAL, so the safety halt gate is never opened — masking permanent failures in the ingestion pipeline. Fatal patterns should take precedence across the *entire* cause chain before deciding RETRYABLE; e.g., collect a `retryable` flag during the walk and only return RETRYABLE after the full chain has been checked.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 72-79). The reviewer's guidance: If any wrapper exception in the chain (e.g., an ExecutionException/RuntimeException whose message contains "connection", "timeout", "leader", or "coordinator") matches a retryable pattern, the loop never reaches an underlying fatal cause such as AuthenticationException, AccessControlException, or TableNotExistException. Fatal patterns should take precedence across the *entire* cause chain before deciding RETRYABLE; e.g., collect a `retryable` flag during the walk and only return RETRYABLE after the full chain has been checked. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 161 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-037, R-068, R-069, R-259, R-260, R-279

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-039 — code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java (lines 64-64)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
High

**Affected Files**

- code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java

**Issue**

The class Javadoc advertises this as a "plain WebSocket server" (port 8888, ws:// scheme), but the implementation only opens a raw TCP `ServerSocket` and never performs the HTTP Upgrade/WebSocket handshake or frame encoding. Any real WebSocket client (e.g. gorilla/websocket used by the Go Arrow bridge, or a browser) will fail the handshake because the server never responds with `101 Switching Protocols` — it just starts writing raw bytes. Either implement an actual WebSocket endpoint (handshake + frame codec) or correct the documentation and log message to describe a plain TCP newline-delimited server.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 64-64). The reviewer's guidance: gorilla/websocket used by the Go Arrow bridge, or a browser) will fail the handshake because the server never responds with `101 Switching Protocols` — it just starts writing raw bytes. Either implement an actual WebSocket endpoint (handshake + frame codec) or correct the documentation and log message to describe a plain TCP newline-delimited server. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-040 — code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java (lines 137-137)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
High

**Affected Files**

- code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java

**Issue**

The documented message contract is "newline-delimited JSON" with one tick object per line, but `generateTicks()` serializes the whole `List<Map<String,Object>>` batch via `mapper.writeValueAsString(batch)`, i.e. one JSON **array** per line. Any downstream parser that reads line-by-line and expects a single tick object per line (as the Javadoc and typical NDJSON consumers do) will fail to deserialize the array. Serialize each tick individually per line, or update the documented contract to reflect array-per-line.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 137-137). The reviewer's guidance: Any downstream parser that reads line-by-line and expects a single tick object per line (as the Javadoc and typical NDJSON consumers do) will fail to deserialize the array. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-041 — code/common/invariants/LiveMoneyGuard.java (lines 15-15)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/invariants/LiveMoneyGuard.java

**Issue**

This file lives at `code/common/invariants/LiveMoneyGuard.java`, but the `common` Maven module compiles only the default source root `code/common/src/main/java` (confirmed: `code/common/pom.xml` has no `<sourceDirectory>` override or build-helper-maven-plugin, and all other module classes sit under `src/main/java/com/trading/common/...`). As a result `LiveMoneyGuard` and `LiveMoneyStopCondition` will not be compiled or packaged by `mvn package` (`make build`), so the guard silently disappears at runtime. Move both files under `code/common/src/main/java/com/trading/common/invariants/` and align the package with `com.trading.common.invariants`, or register the directory as an additional Maven source root.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 15-15). The reviewer's guidance: This file lives at `code/common/invariants/LiveMoneyGuard.java`, but the `common` Maven module compiles only the default source root `code/common/src/main/java` (confirmed: `code/common/pom.xml` has no `<sourceDirectory>` override or build-helper-maven-plugin, and all other module classes sit under `src/main/java/com/trading/common/...`). As a result `LiveMoneyGuard` and `LiveMoneyStopCondition` will not be compiled or packaged by `mvn package` (`make build`), so the guard silently disappears at runtime. Move both files under `code/common/src/main/java/com/trading/common/invariants/` and align the package with `com.trading.common.invariants`, or register the directory as an additional Maven source root. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-042 — code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java (lines 20-20)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java

**Issue**

Ambiguous time unit: the field comment says the standard feed provides epoch seconds while HFT provides epoch nanoseconds, but the model exposes no feed source or time-unit discriminator — Mode is insufficient since LTP/LTPC can appear on either feed. A downstream consumer (15s candle bucketing, discontinuity detection, dedup) cannot reliably tell seconds from nanoseconds and would silently misinterpret the value (a 10^9/10^3 error). Note the Go bridge already normalizes every timestamp to epoch ms (`ts_ms`). Recommend storing a single unambiguous unit (e.g., epoch ms) or adding an explicit unit/feed field so the value is self-describing.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 20-20). The reviewer's guidance: Recommend storing a single unambiguous unit (e.g., epoch ms) or adding an explicit unit/feed field so the value is self-describing. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-043 — code/common/src/main/java/com/trading/common/identity/IdentityModel.java (lines 74-80)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/src/main/java/com/trading/common/identity/IdentityModel.java

**Issue**

Inconsistent value semantics: only InstructionId, ClientOrderRef, BrokerOrderId, InstrumentToken, and ExchangeId override equals()/hashCode(). The remaining 11 identity classes (CandidateId, ExecutionAttemptId, TradeContextId, PositionId, PostbackEventId, AccountScopeId, PortfolioId, ExecutionPartitionId, ReservationId, HaltRequestId, ActionId) fall back to reference equality. For a canonical identity model whose entire purpose is unambiguous value-based identity, two instances wrapping the same logical ID will not be equal — breaking HashSet/HashMap lookups, deduplication, state comparison, and cross-service matching, which could silently produce duplicate or missed operations. Since Java 17 is configured in the parent POM (maven.compiler.release=17), consider converting these classes to records to guarantee uniform equals/hashCode/toString, or at least add equals()/hashCode() to every identity type.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 74-80). The reviewer's guidance: For a canonical identity model whose entire purpose is unambiguous value-based identity, two instances wrapping the same logical ID will not be equal — breaking HashSet/HashMap lookups, deduplication, state comparison, and cross-service matching, which could silently produce duplicate or missed operations. Since Java 17 is configured in the parent POM (maven.compiler.release=17), consider converting these classes to records to guarantee uniform equals/hashCode/toString, or at least add equals()/hashCode() to every identity type. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-044 — code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java (lines 149-149)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java

**Issue**

Logic error: when `to == PREPARED` (the only enum value falling into this `default` branch), `legalSources` becomes `Set.of(from)`, so `legalSources.contains(from)` is always true. Combined with the earlier same-phase idempotent check (`from != to`), this makes *every* transition back into `PREPARED` legal — from `ACCEPTED`, `REJECTED`, `CANCELLED`, or `UNKNOWN` — directly contradicting the comment "PREPARED has no incoming transitions defined". This can lead to re-processing, duplicate submissions, or a corrupted audit trail. The `default` branch should return an empty set.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 149-149). The reviewer's guidance: Logic error: when `to == PREPARED` (the only enum value falling into this `default` branch), `legalSources` becomes `Set.of(from)`, so `legalSources.contains(from)` is always true. Combined with the earlier same-phase idempotent check (`from != to`), this makes *every* transition back into `PREPARED` legal — from `ACCEPTED`, `REJECTED`, `CANCELLED`, or `UNKNOWN` — directly contradicting the comment "PREPARED has no incoming transitions defined". The `default` branch should return an empty set. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-045 — code/common/src/main/java/com/trading/common/observability/Json.java (lines 18-24)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/Json.java

**Issue**

The shared mutable `first` flag is reset to `true` at the start of every `obj(...)`/`arr(...)` block and is never restored when the block returns, so the enclosing container's separator state is corrupted by any nested structure. For example, `arr(a -> { a.obj(o -> o.kv("a","b")); a.obj(o -> o.kv("c","d")); })` produces `[{"a":"b"}{"c":"d"}]` (missing comma), and `obj(w -> { w.kv("x",1); w.arr(a -> a.kv("y",2)); })` produces `{"x":1["y":2]}`. Any non-flat JSON built with this class is malformed. Fix by tracking per-container separator state (e.g. a stack of booleans) and having `obj`/`arr` emit a separator from the parent container before opening their bracket.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-24). The reviewer's guidance: The shared mutable `first` flag is reset to `true` at the start of every `obj(...)`/`arr(...)` block and is never restored when the block returns, so the enclosing container's separator state is corrupted by any nested structure. Fix by tracking per-container separator state (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-046 — code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java (lines 30-31)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java

**Issue**

`event.level` and `event.message` are concatenated into the JSON document without going through `escapeJson()`, and the same applies to `event.service` in the `service.name` attribute above. `message` is free-text log content that can easily contain double quotes, backslashes, or control characters (e.g. a message like `Order "AAPL" rejected`); in that case the emitted OTLP/JSON document is syntactically invalid and the OpenTelemetry Collector may reject the whole log record. Wrap all interpolated dynamic values with `escapeJson()`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 30-31). The reviewer's guidance: `event.level` and `event.message` are concatenated into the JSON document without going through `escapeJson()`, and the same applies to `event.service` in the `service.name` attribute above. a message like `Order "AAPL" rejected`); in that case the emitted OTLP/JSON document is syntactically invalid and the OpenTelemetry Collector may reject the whole log record. Wrap all interpolated dynamic values with `escapeJson()`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-047 — code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java (lines 53-54)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java

**Issue**

Same JSON-escaping gap in `emitAlert()`: `service`, `host`, `vmId`, `environment`, `correlationId`, `category`, `message` and `alert.name()` are interpolated into the document raw, while only `alert.condition` goes through `escapeJson()`. Since `message` is free text and the identity/correlation fields can contain arbitrary characters, a quote or backslash will corrupt the `trading_alerts` record and cause alert loss in OpenObserve. Wrap every interpolated dynamic value with `escapeJson()`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 53-54). The reviewer's guidance: Same JSON-escaping gap in `emitAlert()`: `service`, `host`, `vmId`, `environment`, `correlationId`, `category`, `message` and `alert.name()` are interpolated into the document raw, while only `alert.condition` goes through `escapeJson()`. Since `message` is free text and the identity/correlation fields can contain arbitrary characters, a quote or backslash will corrupt the `trading_alerts` record and cause alert loss in OpenObserve. Wrap every interpolated dynamic value with `escapeJson()`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-048 — code/common/src/main/java/com/trading/common/version/VersionGate.java (lines 23-26)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
High

**Affected Files**

- code/common/src/main/java/com/trading/common/version/VersionGate.java

**Issue**

Placeholder versions bypass the gate. The class contract states a placeholder is not accepted for a live-money path, and PlaceholderVersions declares placeholders as "intentionally NOT real versions". However, requirePinned only rejects null/blank/"latest", so a placeholder like FLINK_VERSION_TO_BE_PINNED passes through. Because requireAllPinned (the CI gate) delegates to requirePinned, CI can proceed with unresolved placeholder versions — inconsistent with version_matrix_verify.py which treats TO_BE_PINNED as a pin blocker. Add a PlaceholderVersions.isPlaceholder check inside requirePinned so both the runtime and CI gates reject placeholders.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 23-26). The reviewer's guidance: However, requirePinned only rejects null/blank/"latest", so a placeholder like FLINK_VERSION_TO_BE_PINNED passes through. Because requireAllPinned (the CI gate) delegates to requirePinned, CI can proceed with unresolved placeholder versions — inconsistent with version_matrix_verify.py which treats TO_BE_PINNED as a pin blocker. Add a PlaceholderVersions.isPlaceholder check inside requirePinned so both the runtime and CI gates reject placeholders. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
R-268

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-049 — code/run-ingestion-full.sh (lines 146-150)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/run-ingestion-full.sh

**Issue**

In this background pipeline, `$!` is the PID of the *last* pipeline element (`tee -a`), not the JVM. Consequently `wait "$PID"` returns `tee`'s exit status — a JVM crash (non-zero exit) is reported as a successful run because `tee` exits 0 after EOF — and the TERM/INT trap plus the EXIT cleanup send SIGTERM to `tee` instead of the JVM, orphaning the ingestion JVM and its child arrow-bridge after `kill` or Ctrl+C. Capture the JVM's PID directly via process substitution so `$!`, `wait`, and signal forwarding all target the actual JVM, e.g.: `"$JAVA_BIN" ... > >(tee -a "$RUN_LOG") 2>&1 &`. (Note: `start-all.sh` runs the same pipeline in the foreground and is not affected.)

**Implementation Plan**

Implement the corrective action described in the finding (report lines 146-150). The reviewer's guidance: Consequently `wait "$PID"` returns `tee`'s exit status — a JVM crash (non-zero exit) is reported as a successful run because `tee` exits 0 after EOF — and the TERM/INT trap plus the EXIT cleanup send SIGTERM to `tee` instead of the JVM, orphaning the ingestion JVM and its child arrow-bridge after `kill` or Ctrl+C. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-050 — code/smoke-test.sh (lines 7-11)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/smoke-test.sh

**Issue**

SmokeTest calls IngestionConfig.validate(), which treats ARROW_MAX_EVENT_AGE_MS and ARROW_MAX_FUTURE_EVENT_SKEW_MS as required keys with no default (IngestionConfig.validateFrom lines 170-171). These are not set here, so SmokeTest will throw IllegalStateException at the first config-validation step ("ARROW_MAX_EVENT_AGE_MS is required but not set") and the smoke test can never run. run-ingestion-full.sh already documents these as required with no code default. Add both exports with the approved values (5000/2000).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 7-11). The reviewer's guidance: SmokeTest calls IngestionConfig.validate(), which treats ARROW_MAX_EVENT_AGE_MS and ARROW_MAX_FUTURE_EVENT_SKEW_MS as required keys with no default (IngestionConfig.validateFrom lines 170-171). These are not set here, so SmokeTest will throw IllegalStateException at the first config-validation step ("ARROW_MAX_EVENT_AGE_MS is required but not set") and the smoke test can never run. Add both exports with the approved values (5000/2000). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-051 — code/logs/ingestion.json (lines 37-37)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- code/logs/ingestion.json

**Issue**

Sensitive operational data is committed to version control in this runtime log: absolute host paths exposing the developer account (`/home/saurabh/Jupyter_notebook/...`), the live broker account identifier (`user QP3796`), internal endpoints (`localhost:9123`, `otel-collector:4318`), and the arrow-bridge binary path. Anyone with repository access can infer infrastructure layout and user identity. The `.gitignore` change in this update does not add any log patterns, so this file and future runs will keep being committed. Recommend removing this file from the changeset and adding ignore rules (e.g. `code/logs/`, `logs/`, `*.log`, `ingestion*.json`) to `.gitignore`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 37-37). The reviewer's guidance: Sensitive operational data is committed to version control in this runtime log: absolute host paths exposing the developer account (`/home/saurabh/Jupyter_notebook/...`), the live broker account identifier (`user QP3796`), internal endpoints (`localhost:9123`, `otel-collector:4318`), and the arrow-bridge binary path. The `.gitignore` change in this update does not add any log patterns, so this file and future runs will keep being committed. Recommend removing this file from the changeset and adding ignore rules (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-052 — start-all.sh (lines 52-54)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- start-all.sh

**Issue**

The fallback credential branch pipes raw `.env` content through `eval` without quoting the value, so any ARROW_* value containing shell metacharacters is interpreted as shell code. E.g. `ARROW_PASSWORD=abc$def` silently expands `$def` (wrong secret), `ARROW_PASSWORD=a b` tries to execute `b` as a command, and `ARROW_PASSWORD=x;cmd` runs `cmd`. This contradicts the script's own SECURITY comment (values handled safely) and can break the export or enable arbitrary command execution. Parse key/value with `IFS='=' read` and use a quoted `export "$key=$value"` (a single assignment argument is never re-evaluated) instead of `eval`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 52-54). The reviewer's guidance: Parse key/value with `IFS='=' read` and use a quoted `export "$key=$value"` (a single assignment argument is never re-evaluated) instead of `eval`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-053 — run-ingestion.sh (lines 46-46)

**Status**

- [x] Done

**Priority**
High

**Affected Files**

- run-ingestion.sh

**Issue**

The `exec` target is a hardcoded absolute path tied to a single developer's machine (`/home/saurabh/...`). Any clone, move, or another user's checkout will fail with a generic `exec: No such file or directory` at the very end of the script. Since `code/run-ingestion-full.sh` ships inside this same repo, derive the path from the script's own location so it remains portable.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 46-46). The reviewer's guidance: Since `code/run-ingestion-full.sh` ships inside this same repo, derive the path from the script's own location so it remains portable. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-054 — code/01_platform/02_sql/ddl/02_raw_table_1.sql (lines 28-28)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/02_raw_table_1.sql

**Issue**

The schema declares `bid_price_paise`/`bid_qty`/`ask_price_paise`/`ask_qty` and marks `last_price_paise`/`last_qty` NOT NULL, but the ingestion path does not carry bid/ask data: `RealFlussRowConverter` hardcodes 0 for all four quote columns (TickPacket has no bid/ask fields), and QUOTE (`VALID_NON_TRADE`) ticks still write `last_price_paise`/`last_qty` from the quote frame's ltp/volume (frequently 0). Every QUOTE row therefore stores fabricated zero bid/ask and ambiguous trade fields — downstream consumers reading bid/ask or aggregating price/volume will be silently corrupted. Align the implementation with the schema (propagate Arrow bid/ask for QUOTE ticks and leave trade fields null/absent for QUOTE rows) or drop the unimplemented columns.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 28-28). The reviewer's guidance: The schema declares `bid_price_paise`/`bid_qty`/`ask_price_paise`/`ask_qty` and marks `last_price_paise`/`last_qty` NOT NULL, but the ingestion path does not carry bid/ask data: `RealFlussRowConverter` hardcodes 0 for all four quote columns (TickPacket has no bid/ask fields), and QUOTE (`VALID_NON_TRADE`) ticks still write `last_price_paise`/`last_qty` from the quote frame's ltp/volume (frequently 0). Align the implementation with the schema (propagate Arrow bid/ask for QUOTE ticks and leave trade fields null/absent for QUOTE rows) or drop the unimplemented columns. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-055 — code/01_platform/02_sql/ddl/03_feature_candles_15s.sql (lines 29-29)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/03_feature_candles_15s.sql

**Issue**

The header documents "Retention: ≤7 trading days (ceiling); extend while EOD offload unverified", but `table.retention.days='7'` is 7 calendar days (≈5 trading days after weekends/holidays), so rows would be evicted earlier than the stated business requirement. In addition, there is no mechanism here to extend retention while the EOD offload is unverified, so candle data could be purged before it is safely offloaded. Either express the value in trading days (≈10 calendar days) or add an explicit extension control.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 29-29). The reviewer's guidance: Either express the value in trading days (≈10 calendar days) or add an explicit extension control. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
R-010, R-011, R-054, R-231

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-056 — code/01_platform/04_scripts/digest-pin.sh (lines 44-44)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/digest-pin.sh

**Issue**

The script does not validate the input reference. If a caller passes a reference that is already pinned (`image:tag@sha256:abc`), the script blindly appends another digest and emits a malformed reference like `image:tag@sha256:abc@sha256:def`, silently corrupting runtime.lock. References without a tag are likewise accepted. Consider rejecting any input that already contains `@sha256:` (or is not in `name:tag` form) with a clear error before resolving.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 44-44). The reviewer's guidance: The script does not validate the input reference. Consider rejecting any input that already contains `@sha256:` (or is not in `name:tag` form) with a clear error before resolving. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-057 — code/01_platform/04_scripts/soak-monitor.sh (lines 45-45)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/soak-monitor.sh

**Issue**

`pgrep -f "$1" | tail -1` selects the numerically highest PID, not the newest or intended process. If multiple instances match (e.g. leftover dev runs or the stale-bridge cleanup window), the monitor samples an unrelated process and reports misleading FDs/threads. Additionally, the Java and bridge PIDs are resolved in two separate `find_pid` calls per sample, so after a restart one sample can mix a new generation's bridge PID with an old generation's Java PID. Prefer selecting the newest process (e.g. filter `ps -eo pid,etimes,cmd` and sort by start time) and resolve both PIDs consistently within the same sample.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 45-45). The reviewer's guidance: Prefer selecting the newest process (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-017, R-018, R-174, R-236, R-237, R-238

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-058 — code/02_services/01_ingestion/go-bridge/main.go (lines 594-595)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/main.go

**Issue**

Standard mode writes ticks through the global `encoder` (json.NewEncoder(os.Stdout)) while all events — including `bridge_shutdown` — are written through `bridgeEmitter`, whose mutex only guards its own writes. These are two independent writers to the same stdout fd. `runStandard` returns as soon as the context is cancelled without joining the `ds.ReadTicks` goroutine, so a final tick can still be mid-`emit()` when `main()` calls `emitShutdownEvent()`. The two writes can interleave (corrupting an NDJSON line) or a tick can land after the drain marker, breaking the documented guarantee that `bridge_shutdown` is the last line. Route standard ticks through `bridgeEmitter` (shared mutex) and/or join/stop the reader goroutine before emitting the shutdown event.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 594-595). The reviewer's guidance: Standard mode writes ticks through the global `encoder` (json.NewEncoder(os.Stdout)) while all events — including `bridge_shutdown` — are written through `bridgeEmitter`, whose mutex only guards its own writes. Route standard ticks through `bridgeEmitter` (shared mutex) and/or join/stop the reader goroutine before emitting the shutdown event. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-097, R-185, R-186, R-187

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-059 — code/02_services/01_ingestion/go-bridge/main.go (lines 383-385)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/main.go

**Issue**

The heartbeat and watchdog goroutines only exit on `ctx.Done()` or their own failure condition; they are not stopped when the epoch ends via `epochStop`. After a disconnect/decode-burst ends an epoch (and `defer stream.Close()` runs), the old goroutines keep running against the closed stream: the heartbeat can emit a stale `heartbeat_failed` for an obsolete epoch at the next 3s tick, and the watchdog can emit `feed_stalled` up to 15s after the frozen `lastFrameNanos`. Across repeated reconnect cycles goroutines accumulate until their timers fire, and can leak indefinitely if `WriteText` blocks on the dead connection. Select on a per-epoch context (e.g., the `readCtx` cancelled by the deferred `stopRead()`) so these goroutines terminate together with the epoch.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 383-385). The reviewer's guidance: After a disconnect/decode-burst ends an epoch (and `defer stream.Close()` runs), the old goroutines keep running against the closed stream: the heartbeat can emit a stale `heartbeat_failed` for an obsolete epoch at the next 3s tick, and the watchdog can emit `feed_stalled` up to 15s after the frozen `lastFrameNanos`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-097, R-185, R-186, R-187

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-060 — code/02_services/01_ingestion/pom.xml (lines 75-79)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/pom.xml

**Issue**

The shaded fat jar only merges the manifest. Dependencies such as the Fluss client / Arrow bring their own `META-INF/services` provider files and Log4j2 plugin metadata; without a ServicesResourceTransformer these files are overwritten (last one wins), so ServiceLoader-based providers can be silently missing at runtime. Add `ServicesResourceTransformer` to the transformer list.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 75-79). The reviewer's guidance: Add `ServicesResourceTransformer` to the transformer list. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-009, R-230

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-061 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java (lines 142-142)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java

**Issue**

loadFromPath() returns approved=true unconditionally after parsing, even when zero data rows were loaded (header-only file, or every row malformed) or when some rows were skipped. This contradicts the class javadoc's SCH-22 semantics ('one approved manifest version defines the active subscription set… if validation fails, readiness remains false') — a truncated/corrupt production CSV with a valid header would still be reported as approved. The count/fingerprint validation in isManifestApproved() is never invoked from this loader or its caller. Suggest returning approved=false when rows were skipped or the parsed set is empty.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 142-142). The reviewer's guidance: This contradicts the class javadoc's SCH-22 semantics ('one approved manifest version defines the active subscription set… if validation fails, readiness remains false') — a truncated/corrupt production CSV with a valid header would still be reported as approved. Suggest returning approved=false when rows were skipped or the parsed set is empty. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-062 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java (lines 253-263)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java

**Issue**

The Connection created in the constructor is a local variable and is never stored or exposed, yet close() states it "is closed by the creator" — no caller holds a reference, so it can never be closed. Every DiscontinuityWriter leaks its Fluss coordinator session/TCP connection until JVM exit. Since IngestionService creates this writer once per process the leak is currently bounded, but to make the class genuinely AutoCloseable, store the Connection as a field and close it in close().

**Implementation Plan**

Implement the corrective action described in the finding (report lines 253-263). The reviewer's guidance: The Connection created in the constructor is a local variable and is never stored or exposed, yet close() states it "is closed by the creator" — no caller holds a reference, so it can never be closed. Since IngestionService creates this writer once per process the leak is currently bounded, but to make the class genuinely AutoCloseable, store the Connection as a field and close it in close(). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-063 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java (lines 216-221)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java

**Issue**

For connection-wide events (before == null and no instrument context), this row writes last_tick_token = 0L instead of SQL NULL, and last_tick_exchange/last_tick_symbol become BinaryString.EMPTY_UTF8 empty strings because bs(null) maps to EMPTY_UTF8. The DDL defines these columns as nullable and the class Javadoc explicitly states they should be "null for connection-wide events". Downstream IS NULL checks (or treating token 0 as an unknown instrument) will misclassify connection-wide discontinuities as instrument-specific. Return null from bs() for null input (as QuarantineWriter/SafetyHaltWriter do) and pass null for instToken when no instrument context is available.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 216-221). The reviewer's guidance: For connection-wide events (before == null and no instrument context), this row writes last_tick_token = 0L instead of SQL NULL, and last_tick_exchange/last_tick_symbol become BinaryString.EMPTY_UTF8 empty strings because bs(null) maps to EMPTY_UTF8. The DDL defines these columns as nullable and the class Javadoc explicitly states they should be "null for connection-wide events". Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-064 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java (lines 172-175)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java

**Issue**

queryNtp() accepts any datagram without validating that it is a real NTP server response: it never checks `response.getLength() == NTP_PACKET_SIZE`, the response LI/VN/Mode byte, or that the origin timestamp (bytes 24-31) echoes the client's transmit timestamp. Because the request never sets bytes 40-47 (transmit timestamp stays zero), origin-timestamp matching is impossible, and a short/truncated datagram leaves bytes 40-47 at zero — producing `ntpMillis ≈ -2.2e12` and a bogus offset; a spoofed or corrupted packet can likewise force a false pass/fail of the ingestion readiness gate. Set a real (current time/random) transmit timestamp in the request and validate packet length + mode + origin timestamp before computing the offset.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 172-175). The reviewer's guidance: Because the request never sets bytes 40-47 (transmit timestamp stays zero), origin-timestamp matching is impossible, and a short/truncated datagram leaves bytes 40-47 at zero — producing `ntpMillis ≈ -2.2e12` and a bogus offset; a spoofed or corrupted packet can likewise force a false pass/fail of the ingestion readiness gate. Set a real (current time/random) transmit timestamp in the request and validate packet length + mode + origin timestamp before computing the offset. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-031, R-178, R-251

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-065 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 158-162)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

Thread-safety race on the latency ring buffer: `latencyRingPos` is read-check-then-written and `latencyRing` is a plain long[] shared with the background scheduler thread, which concurrently reads and resets the same state in flush()/computeLatencyPercentiles(). Concurrent recorders can write the same slot (losing a sample), and a flush can copy a half-written snapshot or reset the position while a producer writes, corrupting the p50/p90/p99 approximations. recordAppendLatencyMs() runs on the ingestion reader thread while flush() runs on the scheduler thread, so this is a genuine data race. Synchronize access (lock both the record and percentile-computation paths) or use an atomic/reset-free approach.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 158-162). The reviewer's guidance: Thread-safety race on the latency ring buffer: `latencyRingPos` is read-check-then-written and `latencyRing` is a plain long[] shared with the background scheduler thread, which concurrently reads and resets the same state in flush()/computeLatencyPercentiles(). Concurrent recorders can write the same slot (losing a sample), and a flush can copy a half-written snapshot or reset the position while a producer writes, corrupting the p50/p90/p99 approximations. Synchronize access (lock both the record and percentile-computation paths) or use an atomic/reset-free approach. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-066 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 440-440)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

esc() escapes only backslash and double-quote, not JSON control characters. Reason strings passed to incrementDecodeError (e.g., broker quarantine reasons or exception messages) can contain \n, \t, \r or other control characters, which will produce invalid JSON and make the entire 10-second POST fail. Escape all characters below 0x20 (\n, \r, \t, \u00XX) as well.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 440-440). The reviewer's guidance: esc() escapes only backslash and double-quote, not JSON control characters. Escape all characters below 0x20 (\n, \r, \t, \u00XX) as well. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-067 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 241-246)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

Two robustness defects in the flush path: (1) reportHealth(true) is invoked even when the collector returns HTTP >= 400, so rejected/malformed payloads are still reported as healthy and otel.collector.healthy stays 1, masking real failures (only the exception path reports false) — health should reflect the actual HTTP status; (2) conn.disconnect() is not in a finally block, so when getOutputStream()/getResponseCode() throw (collector down) the connection is never explicitly released and the 10s retry loop can accumulate unclosed sockets/fds over a long outage. Move disconnect into a finally block.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 241-246). The reviewer's guidance: Two robustness defects in the flush path: (1) reportHealth(true) is invoked even when the collector returns HTTP >= 400, so rejected/malformed payloads are still reported as healthy and otel.collector.healthy stays 1, masking real failures (only the exception path reports false) — health should reflect the actual HTTP status; (2) conn.disconnect() is not in a finally block, so when getOutputStream()/getResponseCode() throw (collector down) the connection is never explicitly released and the 10s retry loop can accumulate unclosed sockets/fds over a long outage. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-068 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java (lines 214-215)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

**Issue**

close() never invokes rowConverter.close(). FlussRowConverter extends AutoCloseable, and RealFlussRowConverter.close() is the only place that releases the underlying Fluss Connection (and its sockets). The Javadoc here even claims it 'force-closes the connection', but the underlying connection is leaked on every close, which can exhaust connections/sockets across reopen cycles. Close the converter after the drain completes.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 214-215). The reviewer's guidance: close() never invokes rowConverter.close(). FlussRowConverter extends AutoCloseable, and RealFlussRowConverter.close() is the only place that releases the underlying Fluss Connection (and its sockets). The Javadoc here even claims it 'force-closes the connection', but the underlying connection is leaked on every close, which can exhaust connections/sockets across reopen cycles. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-038, R-070, R-118, R-195, R-196, R-285

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-069 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java (lines 102-107)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

**Issue**

write() performs a check-then-act on the volatile closed flag: the entry check can pass, then a concurrent close() (e.g., from the shutdown-hook thread while the main read loop is still calling write()) sets closed=true and drains pending counters, and only afterwards does this thread reach tracker.tryAccept/append. A late append can be issued after close() has returned, making shutdown non-hermetic and leaving that append unaccounted. Re-check closed after reserving capacity (and release the slot if closed), or synchronize write()/close() on the same lock.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 102-107). The reviewer's guidance: write() performs a check-then-act on the volatile closed flag: the entry check can pass, then a concurrent close() (e.g., from the shutdown-hook thread while the main read loop is still calling write()) sets closed=true and drains pending counters, and only afterwards does this thread reach tracker.tryAccept/append. A late append can be issued after close() has returned, making shutdown non-hermetic and leaving that append unaccounted. Re-check closed after reserving capacity (and release the slot if closed), or synchronize write()/close() on the same lock. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-038, R-070, R-118, R-195, R-196, R-285

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-070 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java (lines 73-73)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java

**Issue**

The substring check `name.contains("Retriable")` has two problems: (1) it uses the uncommon spelling "Retriable" while the javadoc and the rest of the codebase use "Retryable", so a `RetryableException` would not match; (2) `contains` also matches negative names — a class named `NonRetriableException` (or `NonRetryableException`) contains/relates to the same token and would be classified RETRYABLE, which is exactly backwards for a permanent failure. Consider matching the standard spelling and explicitly treating `NonRetryable`/`NonRetriable` prefixes as FATAL.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 73-73). The reviewer's guidance: The substring check `name.contains("Retriable")` has two problems: (1) it uses the uncommon spelling "Retriable" while the javadoc and the rest of the codebase use "Retryable", so a `RetryableException` would not match; (2) `contains` also matches negative names — a class named `NonRetriableException` (or `NonRetryableException`) contains/relates to the same token and would be classified RETRYABLE, which is exactly backwards for a permanent failure. Consider matching the standard spelling and explicitly treating `NonRetryable`/`NonRetriable` prefixes as FATAL. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-037, R-068, R-069, R-259, R-260, R-279

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-071 — code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java (lines 109-109)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java

**Issue**

`tickRatePerSec` is misleading: with the fixed 10 ms scheduler interval (100 batches/sec) and `batchSize = ceil(instruments.size() * tickRatePerSec / 100.0)`, the server actually emits ~`instruments.size() * tickRatePerSec` ticks/sec (e.g. 50×20 = 1000/s), not `tickRatePerSec`. The startup log `({} instruments, {} ticks/s)` therefore understates the true total rate by a factor of `instruments.size()`. Additionally, `Math.max(1, ...)` forces at least 100 ticks/s even for very low configured rates. Clarify whether the parameter is per-instrument or total and adjust the formula/log accordingly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 109-109). The reviewer's guidance: `tickRatePerSec` is misleading: with the fixed 10 ms scheduler interval (100 batches/sec) and `batchSize = ceil(instruments.size() * tickRatePerSec / 100.0)`, the server actually emits ~`instruments.size() * tickRatePerSec` ticks/sec (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-072 — code/common/invariants/LiveMoneyGuard.java (lines 161-162)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/invariants/LiveMoneyGuard.java

**Issue**

All ten fact booleans default to `false` in the builder, so an omitted setter is indistinguishable from an explicit `false`. Because `evaluate()` approves live money whenever the triggered set is empty, a caller who forgets to supply one fact (e.g., `criticalRiskOpen`) will silently receive approval instead of a halt. For a safety-critical guard this should fail closed: validate in `build()` that every field was explicitly supplied (track a set flag per field), or require all ten values via a constructor.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 161-162). The reviewer's guidance: All ten fact booleans default to `false` in the builder, so an omitted setter is indistinguishable from an explicit `false`. Because `evaluate()` approves live money whenever the triggered set is empty, a caller who forgets to supply one fact (e.g., `criticalRiskOpen`) will silently receive approval instead of a halt. For a safety-critical guard this should fail closed: validate in `build()` that every field was explicitly supplied (track a set flag per field), or require all ten values via a constructor. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-073 — code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java (lines 15-15)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java

**Issue**

Mode declares LTPC, QUOTE and FULL, but the fields only capture last-trade data (lastTradedPrice, lastTradedQty, volume, averagePrice, openInterest) — there is no previous-close field for LTPC, no best bid/ask for QUOTE, and no order-book depth for FULL. The Go bridge emits close_paise, bid_px[5], ask_px[5], bid_qty/ask_qty, and raw_table_1 has bid_price_paise/ask_price_paise columns, so decoding a QUOTE/FULL packet into this model would silently drop that data at the model boundary. Either restrict Mode to the representable values or add the missing fields (close, bid/ask price+qty, depth).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 15-15). The reviewer's guidance: Mode declares LTPC, QUOTE and FULL, but the fields only capture last-trade data (lastTradedPrice, lastTradedQty, volume, averagePrice, openInterest) — there is no previous-close field for LTPC, no best bid/ask for QUOTE, and no order-book depth for FULL. The Go bridge emits close_paise, bid_px[5], ask_px[5], bid_qty/ask_qty, and raw_table_1 has bid_price_paise/ask_price_paise columns, so decoding a QUOTE/FULL packet into this model would silently drop that data at the model boundary. Either restrict Mode to the representable values or add the missing fields (close, bid/ask price+qty, depth). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-074 — code/common/src/main/java/com/trading/common/identity/IdentityModel.java (lines 16-16)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/identity/IdentityModel.java

**Issue**

Constructors accept null/blank values without validation. For the String-based classes, equals()/hashCode() call value.equals(...)/value.hashCode(), so a null value throws NPE at the first comparison or collection use, while toString() silently renders "null". For ClientOrderRef, the javadoc documents a hard limit of "max 16 chars for Arrow remarks", yet no length check exists — an oversized value would only fail at the broker boundary during submission after significant pipeline work. Consider validating non-null/non-blank (and enforcing the 16-char limit for ClientOrderRef) in the constructors, e.g. throw IllegalArgumentException on invalid input, applied consistently to all identity types.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 16-16). The reviewer's guidance: Consider validating non-null/non-blank (and enforcing the 16-char limit for ClientOrderRef) in the constructors, e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-075 — code/common/src/main/java/com/trading/common/identity/IdentityModel.java (lines 51-53)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/identity/IdentityModel.java

**Issue**

InstrumentToken performs no range/sign validation even though it is documented as the join key across market/postback/order books (equals Arrow Token, an int32). A zero or negative token is accepted without complaint and would silently produce wrong joins in raw_table, candle generation, or position tracking, making schema-level integrity issues hard to diagnose. Consider rejecting invalid tokens (e.g. <= 0) at construction time.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 51-53). The reviewer's guidance: Consider rejecting invalid tokens (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-076 — code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java (lines 67-68)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java

**Issue**

The stale-epoch rejection records `GateState.HALTED` as the `from` state instead of the actual `currentState`. This misrepresents the audit trail (an operator would see the gate as already HALTED even if it was ENABLED) and, more importantly, breaks `requiresHalt()`: for a gate actually in `ENABLED` receiving a stale request, `from == GateState.ENABLED` would be false, so the process would not halt even though the epoch mismatch indicates a lost lease / delayed message. Use `currentState` as the source state.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 67-68). The reviewer's guidance: The stale-epoch rejection records `GateState.HALTED` as the `from` state instead of the actual `currentState`. Use `currentState` as the source state. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-077 — code/common/src/main/java/com/trading/common/observability/Json.java (lines 55-58)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/Json.java

**Issue**

`escape(null)` returns `""`, so `kv(k, null)` silently serializes a null value as an empty JSON string (`"k":""`) instead of `null` or an omitted field. In telemetry/OTLP records an absent attribute and an empty string are semantically different; this silently loses null information. Either have `kv` write `null` for null values or skip the pair entirely.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 55-58). The reviewer's guidance: `escape(null)` returns `""`, so `kv(k, null)` silently serializes a null value as an empty JSON string (`"k":""`) instead of `null` or an omitted field. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-078 — code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java (lines 74-78)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java

**Issue**

`escapeJson()` only escapes `"`, `\`, `\n` and `\r`. Per RFC 8259 all control characters in the range U+0000–U+001F (including tab, backspace, form feed) must be escaped in JSON strings. A log message or alert condition containing a tab or other control char will still produce invalid OTLP/JSON. Add handling for `\t`, `\b`, `\f` and a `c < 0x20` → `\uXXXX` fallback.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 74-78). The reviewer's guidance: `escapeJson()` only escapes `"`, `\`, `\n` and `\r`. Per RFC 8259 all control characters in the range U+0000–U+001F (including tab, backspace, form feed) must be escaped in JSON strings. Add handling for `\t`, `\b`, `\f` and a `c < 0x20` → `\uXXXX` fallback. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-079 — code/common/src/main/java/com/trading/common/version/VersionGate.java (lines 27-27)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/version/VersionGate.java

**Issue**

requirePinned trims the value only for the 'latest' comparison but returns the original, untrimmed string. A version such as " 1.2.3 " passes the gate and is returned with leading/trailing whitespace, which can silently break downstream exact-match comparisons, version matrix lookups, or path construction. Return the trimmed value instead.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-27). The reviewer's guidance: requirePinned trims the value only for the 'latest' comparison but returns the original, untrimmed string. A version such as " 1.2.3 " passes the gate and is returned with leading/trailing whitespace, which can silently break downstream exact-match comparisons, version matrix lookups, or path construction. Return the trimmed value instead. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
R-268

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-080 — code/run-ingestion-full.sh (lines 114-117)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/run-ingestion-full.sh

**Issue**

`cleanup_stale_bridges` only matches `arrow-bridge` binaries. If a previous run left the `IngestionService` JVM alive (e.g. after the wrong-PID/tee issue above, or a `kill` that only hit the pipeline), this launcher starts a second JVM against the same Fluss/Arrow resources, producing duplicate ingestion and double writes. `IngestionService` has no PID-file/lock protection against concurrent instances, so the launcher should also detect a running `com.trading.ingestion.IngestionService` process (e.g. include it in the `pgrep` pattern or maintain a PID file) before starting.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 114-117). The reviewer's guidance: `IngestionService` has no PID-file/lock protection against concurrent instances, so the launcher should also detect a running `com.trading.ingestion.IngestionService` process (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-081 — code/run-ingestion-full.sh (lines 57-57)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/run-ingestion-full.sh

**Issue**

The token list is derived with `cut -f4 | tail -n +2 | paste` without validating the expected count (1,024), the header row, or CRLF/BOM/quoted fields. The Go bridge (`loadTokensFromCSV`) trusts `ARROW_INSTRUMENT_TOKENS` first and only falls back to its own robust CSV parsing when the env list is empty, so a partially-wrong list silently produces missing/incorrect subscriptions (Java only notices later via the 30s subscription-completeness check). Validate the extracted token count equals the expected 1,024 and strip CR/BOM before exporting.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 57-57). The reviewer's guidance: The token list is derived with `cut -f4 | tail -n +2 | paste` without validating the expected count (1,024), the header row, or CRLF/BOM/quoted fields. Validate the extracted token count equals the expected 1,024 and strip CR/BOM before exporting. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-082 — Makefile (lines 38-38)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- Makefile

**Issue**

The `-o` (offline) flag makes `make build`/`make test` fail on any machine whose local Maven repository (~/.m2) is not already populated (fresh checkout, CI, or clean container), because Maven will not download dependencies. Note the ingestion Dockerfile explicitly runs `mvn dependency:go-offline` first to seed the repo; the Makefile path has no such step. Either drop `-o` or document the prerequisite.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 38-38). The reviewer's guidance: The `-o` (offline) flag makes `make build`/`make test` fail on any machine whose local Maven repository (~/.m2) is not already populated (fresh checkout, CI, or clean container), because Maven will not download dependencies. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-083 — code/01_platform/01_docker/ddl-init.sh (lines 31-35)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/01_docker/ddl-init.sh

**Issue**

This script claims to create the `default` database (header comment, this log line, and the 'done' message), but it never issues any Fluss/Database command — it only waits for a TCP connection to the coordinator port, sleeps 5s, and exits 0. As written it is a no-op init container that gives false confidence that the database exists. Note that the real creation is done by DdlBootstrap.ensureTables() in the ingestion service (or by ddl_apply.py), and this script is not wired into docker-compose at all. Either perform the actual creation here (call the Fluss Admin API / DdlBootstrap and verify `listDatabases` contains `default`), or remove the misleading 'creating database' output and document that database bootstrap happens inside the ingestion service. Additionally, a TCP connect is not a readiness check — the coordinator can accept connections before it can serve admin/DDL requests, so the fixed `sleep 5` is a fragile heuristic for gating downstream work.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 31-35). The reviewer's guidance: Note that the real creation is done by DdlBootstrap.ensureTables() in the ingestion service (or by ddl_apply.py), and this script is not wired into docker-compose at all. Either perform the actual creation here (call the Fluss Admin API / DdlBootstrap and verify `listDatabases` contains `default`), or remove the misleading 'creating database' output and document that database bootstrap happens inside the ingestion service. Additionally, a TCP connect is not a readiness check — the coordinator can accept connections before it can serve admin/DDL requests, so the fixed `sleep 5` is a fragile heuristic for gating downstream work. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-084 — code/01_platform/02_sql/ddl/05_signal_candidates.sql (lines 30-31)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/05_signal_candidates.sql

**Issue**

This table is declared as an immutable LOG (no primary key), and the schema manifest registers it as `table_kind: LOG`. However, `superseded_by_candidate_id` can only be populated by updating an already-appended row (candidate A is written first, and its `superseded_by_candidate_id` can only be known when a later candidate B supersedes it). Append-only LOG tables do not support in-place updates, so this column will always be NULL — the supersession chain is only discoverable in the reverse direction via `supersedes_candidate_id` on the newer row. Either drop `superseded_by_candidate_id` and reconstruct chains from `supersedes_candidate_id`, or if the field is truly required, change this to a KV table (e.g., `PRIMARY KEY (candidate_id)`) so the row can be updated when supersession occurs.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 30-31). The reviewer's guidance: This table is declared as an immutable LOG (no primary key), and the schema manifest registers it as `table_kind: LOG`. However, `superseded_by_candidate_id` can only be populated by updating an already-appended row (candidate A is written first, and its `superseded_by_candidate_id` can only be known when a later candidate B supersedes it). Append-only LOG tables do not support in-place updates, so this column will always be NULL — the supersession chain is only discoverable in the reverse direction via `supersedes_candidate_id` on the newer row. Either drop `superseded_by_candidate_id` and reconstruct chains from `supersedes_candidate_id`, or if the field is truly required, change this to a KV table (e.g., `PRIMARY KEY (candidate_id)`) so the row can be updated when supersession occurs. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-085 — code/01_platform/02_sql/ddl/08_fills.sql (lines 16-17)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/08_fills.sql

**Issue**

The header declares "Scope: account_scope_id" and this table feeds the encrypted 7-year audit lake, but the schema has no `account_scope_id` column. The analogous LOG audit table `Execution_Audit` includes `account_scope_id STRING NOT NULL` (as do Trade_Decisions and Positions). Without this column, fills cannot be attributed to or filtered by account, breaking per-account data isolation and weakening the compliance/audit trail the table is meant to provide. Add an `account_scope_id STRING NOT NULL` column (or drop the scope claim from the design).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 16-17). The reviewer's guidance: Add an `account_scope_id STRING NOT NULL` column (or drop the scope claim from the design). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-086 — code/01_platform/02_sql/ddl/13_order_correlation.sql (lines 11-11)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/13_order_correlation.sql

**Issue**

The primary key is `instruction_id` while `execution_attempt_id` is NOT NULL. Since a single instruction can be retried as multiple execution attempts (each attempt can produce a distinct `broker_order_id`, per the `Execution_Attempts` table keyed by attempt), this single-row KV can only hold the correlation of the latest attempt — earlier attempts' broker mapping is silently overwritten. If per-attempt correlation must be preserved for reconciliation, key the table on `(instruction_id, execution_attempt_id)`; otherwise confirm and document that this intentionally stores only the current/latest mapping.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 11-11). The reviewer's guidance: If per-attempt correlation must be preserved for reconciliation, key the table on `(instruction_id, execution_attempt_id)`; otherwise confirm and document that this intentionally stores only the current/latest mapping. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-087 — code/01_platform/02_sql/ddl/14_execution_audit.sql (lines 26-26)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/14_execution_audit.sql

**Issue**

Retention configuration contradicts the stated requirement. The header declares "Retention: ≥3 complete trading days", but `table.retention.days = '3'` enforces 3 calendar days. Since trading days exclude weekends/holidays, this cannot guarantee 3 complete trading sessions are retained (e.g., on a Monday, only Friday's data remains and Thursday's is already purged; after a multi-day holiday, even more is lost). For an immutable audit log that must be offloaded to the 7-year lake, this risks silently dropping evidence before the EOD offload runs. Either raise the retention (e.g., ≥5 calendar days to cover a weekend) or correct the header to state the actual calendar-day retention.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 26-26). The reviewer's guidance: For an immutable audit log that must be offloaded to the 7-year lake, this risks silently dropping evidence before the EOD offload runs. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-088 — code/01_platform/02_sql/ddl/16_postback_quarantine.sql (lines 27-27)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/16_postback_quarantine.sql

**Issue**

`table.retention.days` is not a standard Fluss log-table option — the previous version of this DDL used `table.log.ttl = '7d'`, which is the actual Fluss option (default TTL is also 7 days). If the pinned Fluss 0.9.1-incubating server does not recognize `table.retention.days`, the CREATE TABLE may be rejected or, more likely, the option is silently ignored so the intended retention policy is not explicitly enforced. Since every DDL in this update shares this option, verify the option name against the pinned Fluss version before the DDL apply gate rather than assuming it takes effect.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-27). The reviewer's guidance: If the pinned Fluss 0.9.1-incubating server does not recognize `table.retention.days`, the CREATE TABLE may be rejected or, more likely, the option is silently ignored so the intended retention policy is not explicitly enforced. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-089 — code/01_platform/02_sql/ddl/18_safety_halt_requests.sql (lines 18-18)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/18_safety_halt_requests.sql

**Issue**

halt_request_id is a deterministic SHA-256 of the transition tuple, but this is an append-only LOG table with no primary key, so the storage layer does not enforce uniqueness. In the caller path (IngestionService.emitSafetyTransition), SafetyHaltWriter.write() is invoked unconditionally and the in-memory safetyEmitted dedup set only gates the log message — it is also lost on process restart. So if the same transition tuple is re-emitted (duplicate bridge lifecycle event, restart, or ack-loss retry), a second row with the same halt_request_id is appended. Since downstream consumers treat this table as a control signal, consider declaring halt_request_id as the PRIMARY KEY (KV table, e.g. like Execution_Gate) so re-emission becomes an idempotent upsert, or ensure dedup happens before the append.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-18). The reviewer's guidance: In the caller path (IngestionService.emitSafetyTransition), SafetyHaltWriter.write() is invoked unconditionally and the in-memory safetyEmitted dedup set only gates the log message — it is also lost on process restart. Since downstream consumers treat this table as a control signal, consider declaring halt_request_id as the PRIMARY KEY (KV table, e.g. like Execution_Gate) so re-emission becomes an idempotent upsert, or ensure dedup happens before the append. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-090 — code/01_platform/02_sql/ddl/20_instruments.sql (lines 19-23)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/20_instruments.sql

**Issue**

The header says this table retains "current and prior instrument manifest versions", but the primary key is only `instrument_token`. This is a one-row-per-instrument KV table: a Fluss upsert on the same key overwrites the previous row, so prior manifest versions can never be retained — `manifest_version` would always reflect only the latest version, and the header's retention claim is unmet. If history is actually required, use a composite key `PRIMARY KEY (instrument_token, manifest_version) NOT ENFORCED`. If only the current version is needed (matching SCH-22's "one approved manifest version" model and `InstrumentManifestLoader` always writing version 1), update the header comment to "current state only" to avoid a misleading contract.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 19-23). The reviewer's guidance: If history is actually required, use a composite key `PRIMARY KEY (instrument_token, manifest_version) NOT ENFORCED`. If only the current version is needed (matching SCH-22's "one approved manifest version" model and `InstrumentManifestLoader` always writing version 1), update the header comment to "current state only" to avoid a misleading contract. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-091 — code/01_platform/04_scripts/cep_guard.sh (lines 11-11)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/cep_guard.sh

**Issue**

Silent false-negative: if `$ROOT` is a typo or the script is invoked from an unexpected working directory, `grep -r` on a nonexistent path emits nothing to stdout (stderr is suppressed by `2>/dev/null`) and exits non-zero, which is swallowed by `|| true`. The script then prints "OK: no Flink CEP references found" and exits 0, so the guard would silently pass without scanning anything — defeating its purpose as a CI enforcement gate. Validate that the scan root actually exists (and is a directory) before running grep, failing the build otherwise.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 11-11). The reviewer's guidance: The script then prints "OK: no Flink CEP references found" and exits 0, so the guard would silently pass without scanning anything — defeating its purpose as a CI enforcement gate. Validate that the scan root actually exists (and is a directory) before running grep, failing the build otherwise. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-092 — code/01_platform/04_scripts/version_matrix_verify.py (lines 46-49)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/version_matrix_verify.py

**Issue**

`yaml.safe_load(fh)` returns `None` for an empty or comment-only file, and raises `yaml.YAMLError` for malformed YAML. Neither case is handled here: an empty file crashes with `AttributeError: 'NoneType' object has no attribute 'get'`, and a malformed file propagates a raw traceback. For a CI structural gate, this produces an opaque error instead of the intended clear failure. Guard the document type and catch the parse error.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 46-49). The reviewer's guidance: Guard the document type and catch the parse error. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-093 — code/01_platform/04_scripts/version_matrix_verify.py (lines 55-56)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/version_matrix_verify.py

**Issue**

The loop assumes every row is a dict and that `proposed_version` / `evidence_owner` / `evidence_method` / `compatibility_class` are strings before calling `.strip()` / `.upper()`. YAML parses unquoted scalars like `2.2`, `true`, or `2024-01-01` into float/bool/date, so `.strip()` raises `AttributeError`, and a non-dict row crashes at `row.get`. Since this validator exists precisely to flag bad entries, a crash obscures the offending row. Coerce defensively or validate types explicitly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 55-56). The reviewer's guidance: YAML parses unquoted scalars like `2.2`, `true`, or `2024-01-01` into float/bool/date, so `.strip()` raises `AttributeError`, and a non-dict row crashes at `row.get`. Coerce defensively or validate types explicitly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-094 — code/02_services/01_ingestion/go-bridge/faketool/main.go (lines 44-45)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/faketool/main.go

**Issue**

Data race on the `connections` counter. Each WebSocket handler runs in its own goroutine under net/http, so `connections++` and `idx := connections` are unsynchronized concurrent accesses. Under concurrent or rapidly reconnecting clients (reconnect loops, multiple slots), two handlers can observe the same index or skip indices, making `-disconnect-after=N` close the wrong connection (or none) and turning the E2E disconnect scenario nondeterministic. Use `sync/atomic.Int32` (declare `var connections atomic.Int32` and read via `idx := connections.Add(1)`).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 44-45). The reviewer's guidance: Under concurrent or rapidly reconnecting clients (reconnect loops, multiple slots), two handlers can observe the same index or skip indices, making `-disconnect-after=N` close the wrong connection (or none) and turning the E2E disconnect scenario nondeterministic. Use `sync/atomic.Int32` (declare `var connections atomic.Int32` and read via `idx := connections.Add(1)`). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-095 — code/02_services/01_ingestion/go-bridge/hft_slot.go (lines 152-152)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/hft_slot.go

**Issue**

Stall detection has a blind window that defeats its purpose: `BeginConnect()` resets `lastFrame` to the zero time, and this guard requires `!s.lastFrame.IsZero()`, so a slot that reaches `SlotActive` but never receives a single frame (a completely silent/dead feed — exactly the failure stall detection exists for) will never be flagged as stalled. Slots stuck in CONNECTING/SUBSCRIBING are likewise never considered (`state == SlotActive` required). The production supervisor (runHFTEpoch) avoids this by seeding `lastFrameNanos` at subscription time, but this method as written would silently mask total feed loss if `HFTSlot` is ever wired into the supervisor. Consider seeding `lastFrame` when entering ACTIVE and treating a zero `lastFrame` after ACTIVE as stalled.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 152-152). The reviewer's guidance: Stall detection has a blind window that defeats its purpose: `BeginConnect()` resets `lastFrame` to the zero time, and this guard requires `!s.lastFrame.IsZero()`, so a slot that reaches `SlotActive` but never receives a single frame (a completely silent/dead feed — exactly the failure stall detection exists for) will never be flagged as stalled. Slots stuck in CONNECTING/SUBSCRIBING are likewise never considered (`state == SlotActive` required). Consider seeding `lastFrame` when entering ACTIVE and treating a zero `lastFrame` after ACTIVE as stalled. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-096 — code/02_services/01_ingestion/go-bridge/hft_slot.go (lines 131-135)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/hft_slot.go

**Issue**

`BeginConnect()` does not honor the `closed` flag, unlike `SetState()`/`Close()`. After `Close()` sets `closed=true` and `SlotTerminal`, a late/racing `BeginConnect()` silently resurrects the slot to `SlotAuthenticating` and bumps the epoch, so a reconnect overlapping shutdown can attempt connections on a closed slot (and performs the illegal TERMINAL→AUTHENTICATING move). Additionally, `NewHFTSlot` initializes state to `SlotTerminal`, making a never-started slot indistinguishable from a closed one for readers of `State()`. Guard against `s.closed` (and consider an explicit idle state for a fresh slot).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 131-135). The reviewer's guidance: `BeginConnect()` does not honor the `closed` flag, unlike `SetState()`/`Close()`. After `Close()` sets `closed=true` and `SlotTerminal`, a late/racing `BeginConnect()` silently resurrects the slot to `SlotAuthenticating` and bumps the epoch, so a reconnect overlapping shutdown can attempt connections on a closed slot (and performs the illegal TERMINAL→AUTHENTICATING move). Additionally, `NewHFTSlot` initializes state to `SlotTerminal`, making a never-started slot indistinguishable from a closed one for readers of `State()`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-097 — code/02_services/01_ingestion/go-bridge/ndjson.go (lines 103-108)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/ndjson.go

**Issue**

validateBridgeEvent is defined here but never invoked by production code: EmitEvent writes the event directly without validating it, and the only callers of validateBridgeEvent are the unit tests. As a result, invalid bridge events are not rejected at the source. A concrete reachable case exists: the single-socket policy path in main.go emits an `auth_failure` event with `ConnectionEpoch: 0`, which this validator (and the Java BridgeEvent constructor) would reject as `connection_epoch must be positive` — so the invalid line is written to stdout and only fails later on the Java side, where the root cause is obscured. Wire the validator into EmitEvent so invalid events fail fast with a clear Go-side error.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 103-108). The reviewer's guidance: validateBridgeEvent is defined here but never invoked by production code: EmitEvent writes the event directly without validating it, and the only callers of validateBridgeEvent are the unit tests. As a result, invalid bridge events are not rejected at the source. A concrete reachable case exists: the single-socket policy path in main.go emits an `auth_failure` event with `ConnectionEpoch: 0`, which this validator (and the Java BridgeEvent constructor) would reject as `connection_epoch must be positive` — so the invalid line is written to stdout and only fails later on the Java side, where the root cause is obscured. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-023, R-058, R-059, R-175, R-176, R-177

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-098 — code/02_services/01_ingestion/go-bridge/subscription_plan.go (lines 71-78)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/subscription_plan.go

**Issue**

The SHA-256 fingerprint is computed only from SlotID/ConnectionID/Tokens; the per-slot `Requests` partitioning and the `requestLimit` parameter are excluded. Because the request chunks are what actually get sent to the broker in `runHFTEpoch` (one `SubscribeHFTTokens` call per chunk), two plans built from the same sorted tokens/slots/connectionLimit but with a different `requestLimit` (e.g., 512 vs 256) produce identical fingerprints while issuing different subscription requests. This defeats the digest's purpose as a plan identifier for drift/change detection (the fingerprint is exposed as `SubscriptionPlan.Fingerprint` and logged at startup). Since the requests are already computed at this point, include them (and/or `requestLimit`) in the hash.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 71-78). The reviewer's guidance: Because the request chunks are what actually get sent to the broker in `runHFTEpoch` (one `SubscribeHFTTokens` call per chunk), two plans built from the same sorted tokens/slots/connectionLimit but with a different `requestLimit` (e.g., 512 vs 256) produce identical fingerprints while issuing different subscription requests. Since the requests are already computed at this point, include them (and/or `requestLimit`) in the hash. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-099 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go (lines 135-137)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go

**Issue**

Request bodies are assembled with `fmt.Sprintf` raw string interpolation (`userID`/`password` here, and `requestToken`/`appID` in `Authenticate`). Any credential containing `"`, `\`, or control characters produces invalid JSON, so authentication fails for otherwise-valid credentials (e.g., passwords containing quotes). Build these payloads with `encoding/json.Marshal` (a `struct` or `map`) instead of string interpolation.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 135-137). The reviewer's guidance: Request bodies are assembled with `fmt.Sprintf` raw string interpolation (`userID`/`password` here, and `requestToken`/`appID` in `Authenticate`). Any credential containing `"`, `\`, or control characters produces invalid JSON, so authentication fails for otherwise-valid credentials (e.g., passwords containing quotes). Build these payloads with `encoding/json.Marshal` (a `struct` or `map`) instead of string interpolation. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-100 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go (lines 83-87)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go

**Issue**

`Authenticate` reports success whenever `Status == "success"` without checking that `Data.Token` is non-empty, and `AutoLogin` never verifies that `requestToken` was actually extracted from the redirect URL (or that `loginResp.Data.RequestID` is non-empty). A partial/malformed response or a redirect URL without `request-token` therefore returns a nil error with an empty token; main.go only logs `token_len` and proceeds, so all subsequent authenticated calls fail with opaque auth errors. Validate `requestToken != ""` and `authResponse.Data.Token != ""` and return an explicit error otherwise.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 83-87). The reviewer's guidance: Validate `requestToken != ""` and `authResponse.Data.Token != ""` and return an explicit error otherwise. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-101 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go (lines 67-67)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go

**Issue**

`Authenticate`/`AutoLogin` call `c.request`/`c.rawRequest`, which execute on `&fasthttp.Client{}` (no `ReadTimeout`/`WriteTimeout` configured in `NewClient`) and take no context or deadline. A stalled network connection blocks indefinitely: at startup `client.AutoLogin` hangs before the pipeline starts, and during reconnects `refreshAuth(ctx)` hangs the HFT read goroutine so the epoch and reconnect loop never complete (the caller's context is checked only before the call). Add a per-request timeout/context (e.g., a `fasthttp.Client` Timeout or `fasthttp.DoTimeout`) and honor the caller's context.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 67-67). The reviewer's guidance: Add a per-request timeout/context (e.g., a `fasthttp.Client` Timeout or `fasthttp.DoTimeout`) and honor the caller's context. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-102 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go (lines 81-85)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go

**Issue**

Data race on the zstd decoder between Close() and the read loop. In main.go's runHFTEpoch the read loop runs in a fire-and-forget goroutine (`go stream.ReadHFTWithFrame(...)`) and `stream.Close()` is invoked from the epoch goroutine via defer without joining the read goroutine first. decodeHFTPayload does an unsynchronized check-then-use of `s.zdec` (`if s.zdec == nil { return payload }` then `s.zdec.DecodeAll(...)`), so it can observe a decoder that is being closed/concurrently niled. This is race-detector visible and calling DecodeAll on a just-closed decoder is undefined (can panic, crashing the whole bridge). Guard zdec with the existing mutex (and have decodeHFTPayload take the same lock around the check + DecodeAll), or ensure the read loop is stopped/joined before closing the decoder.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 81-85). The reviewer's guidance: Data race on the zstd decoder between Close() and the read loop. In main.go's runHFTEpoch the read loop runs in a fire-and-forget goroutine (`go stream.ReadHFTWithFrame(...)`) and `stream.Close()` is invoked from the epoch goroutine via defer without joining the read goroutine first. decodeHFTPayload does an unsynchronized check-then-use of `s.zdec` (`if s.zdec == nil { return payload }` then `s.zdec.DecodeAll(...)`), so it can observe a decoder that is being closed/concurrently niled. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-103 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go (lines 287-289)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go

**Issue**

No read/write deadlines are set anywhere in this client, so a wedged TCP connection blocks indefinitely. writeJSON/WriteText hold `s.mu` across a blocking WriteMessage — the 3s heartbeat goroutine in runHFTEpoch and all subscription/unsubscription writes share this mutex, so one stalled write starves all of them. On the read side, ctx cancellation cannot interrupt `s.conn.ReadMessage()` (ctx is only checked at the top of the loop), so the documented "until ctx is done" contract is not honored and only Close() unblocks it. Worse, Close() itself writes a close frame with no deadline, so a full send buffer can hang the entire shutdown path. Recommend setting a write deadline (e.g. SetWriteDeadline(time.Now().Add(N*time.Second))) before each write and a read deadline/pong-based deadline per loop so a stalled socket surfaces an error and the reconnect loop can act.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 287-289). The reviewer's guidance: No read/write deadlines are set anywhere in this client, so a wedged TCP connection blocks indefinitely. On the read side, ctx cancellation cannot interrupt `s.conn.ReadMessage()` (ctx is only checked at the top of the loop), so the documented "until ctx is done" contract is not honored and only Close() unblocks it. Worse, Close() itself writes a close frame with no deadline, so a full send buffer can hang the entire shutdown path. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-104 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go (lines 188-189)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go

**Issue**

The historical data endpoint hardcodes the production host `https://historical-api.arrow.trade` and goes through `rawRequestAuth` with an absolute URL, bypassing the configurable `Config.BaseURL` that every other method in this file honors (via `c.request`). I verified `rawRequestAuth` does accept an absolute URL, so the request is not malformed — but the host cannot be redirected for a non-production environment (sandbox/soak/test), where these calls will still silently hit the live production historical API. Consider deriving the base from a config field or an override (e.g. `Config.HistoricalBaseURL` defaulting to the production host).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 188-189). The reviewer's guidance: The historical data endpoint hardcodes the production host `https://historical-api.arrow.trade` and goes through `rawRequestAuth` with an absolute URL, bypassing the configurable `Config.BaseURL` that every other method in this file honors (via `c.request`). Consider deriving the base from a config field or an override (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-105 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/orders.go (lines 178-181)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/orders.go

**Issue**

The broker's `errorCode` and `message` are parsed and logged but dropped from the returned error — callers receive `fmt.Errorf("order placement failed")` with no rejection taxonomy. This makes it impossible to distinguish a broker order rejection (insufficient margin, invalid symbol, rate limit, etc.) from other failures, which is critical for retry/abort decisions in order workflows. The same pattern appears in `ModifyOrder`, `CancelOrder`, and `GetOrder`. Suggest including the parsed details in the returned error, e.g. `fmt.Errorf("order placement failed: code=%s message=%s", result.ErrorCode, result.Message)`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 178-181). The reviewer's guidance: The broker's `errorCode` and `message` are parsed and logged but dropped from the returned error — callers receive `fmt.Errorf("order placement failed")` with no rejection taxonomy. This makes it impossible to distinguish a broker order rejection (insufficient margin, invalid symbol, rate limit, etc.) from other failures, which is critical for retry/abort decisions in order workflows. Suggest including the parsed details in the returned error, e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-106 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/user.go (lines 76-76)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/user.go

**Issue**

GetUserDetails performs an unbounded blocking HTTP request: it calls c.request(), which executes c.HTTPClient.Do(req, resp). The fasthttp.Client created in NewClient sets no ReadTimeout/WriteTimeout, and request() applies no deadline, so if the Arrow /user/details endpoint stalls after the connection is established (e.g. never sends a response), this call blocks the invoking goroutine indefinitely with no recovery. For an ingestion pipeline that must start or fail fast, this can hang startup/health paths. Suggest configuring ReadTimeout/WriteTimeout on the fasthttp client, or using a deadline-bounded call (DoTimeout/DoDeadline) so a hung endpoint returns an error instead of hanging the caller.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 76-76). The reviewer's guidance: The fasthttp.Client created in NewClient sets no ReadTimeout/WriteTimeout, and request() applies no deadline, so if the Arrow /user/details endpoint stalls after the connection is established (e.g. For an ingestion pipeline that must start or fail fast, this can hang startup/health paths. Suggest configuring ReadTimeout/WriteTimeout on the fasthttp client, or using a deadline-bounded call (DoTimeout/DoDeadline) so a hung endpoint returns an error instead of hanging the caller. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-107 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java (lines 174-179)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java

**Issue**

The GenericRow is fully materialized before `writer.append(row)` is invoked, so the comment 'set after append' is misleading — `ack_ts` will always be persisted as 0 in every stored row. Additionally, `thenApply` ignores the real Fluss `AppendResult` (the `result` parameter is unused) and fabricates `RawTickWriter.AppendResult(0, tablePath)`, so RawTickWriter success tracking/outcomes will always report `offset=0, partition=<tablePath>` regardless of the actual Fluss acknowledgement. Either preserve the real ack metadata (offset/partition) and remove the placeholder value, or drop the misleading ack_ts field/comment.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 174-179). The reviewer's guidance: The GenericRow is fully materialized before `writer.append(row)` is invoked, so the comment 'set after append' is misleading — `ack_ts` will always be persisted as 0 in every stored row. Additionally, `thenApply` ignores the real Fluss `AppendResult` (the `result` parameter is unused) and fabricates `RawTickWriter.AppendResult(0, tablePath)`, so RawTickWriter success tracking/outcomes will always report `offset=0, partition=<tablePath>` regardless of the actual Fluss acknowledgement. Either preserve the real ack metadata (offset/partition) and remove the placeholder value, or drop the misleading ack_ts field/comment. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-108 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 314-319)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

The ING-1 broker-staleness detection is effectively a no-op. This check only executes inside the read loop after a new line has already arrived — during a genuine feed outage `readLine()` blocks and the check never runs. When the first post-stall frame finally arrives, this block marks the broker disconnected, but the very next lines (`lastFrameNanos = nowNanos; if (!health.isBrokerConnected()) ... setBrokerConnected(true)`) immediately flip it back to connected within the same iteration, so `brokerConnected`/`metrics.setBridgeConnected` never observably stay false. If ING-1 is meant to detect disconnects independently of the Go bridge's own 15s `feed_stalled` watchdog, it needs a background watchdog thread that sets `brokerConnected=false` based on `lastFrameNanos` without requiring a frame arrival; otherwise this block is misleading dead code.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 314-319). The reviewer's guidance: setBrokerConnected(true)`) immediately flip it back to connected within the same iteration, so`brokerConnected`/`metrics.setBridgeConnected` never observably stay false. If ING-1 is meant to detect disconnects independently of the Go bridge's own 15s `feed_stalled` watchdog, it needs a background watchdog thread that sets `brokerConnected=false` based on `lastFrameNanos` without requiring a frame arrival; otherwise this block is misleading dead code. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-109 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 372-374)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

The ING-3 Slow-Fluss pause percentage is computed against the static `AppendTracker.MAX_PENDING_RECORDS` (10,000), but the tracker is constructed with `config.maxPendingRecords`, which is configurable via `MAX_PENDING_APPEND_RECORDS` (valid range 100..1,000,000). If the configured limit is below 10,000, the 90% pause threshold (9,000) is never reached before the tracker halts at 100%, so the read-pause protection never engages; if configured higher, the pause fires far too early. Compute the percentage against the tracker's actual configured capacity.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 372-374). The reviewer's guidance: The ING-3 Slow-Fluss pause percentage is computed against the static `AppendTracker.MAX_PENDING_RECORDS` (10,000), but the tracker is constructed with `config.maxPendingRecords`, which is configurable via `MAX_PENDING_APPEND_RECORDS` (valid range 100..1,000,000). If the configured limit is below 10,000, the 90% pause threshold (9,000) is never reached before the tracker halts at 100%, so the read-pause protection never engages; if configured higher, the pause fires far too early. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-110 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 418-420)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

On a bridge restart, the subscription-completeness state is not reset. `seenTokens` is declared outside the restart loop, so tokens seen by the previous process still count toward completeness, and `health.setSubscriptionComplete(false)` is only asserted once before the loop — `resetSlotsToAuthenticating()` does not touch the `subscriptionComplete` flag. If completeness had been reached in the first process, the ING-2 check is skipped entirely after restart, masking a partial subscription in the fresh process. Reset both before relaunching.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 418-420). The reviewer's guidance: On a bridge restart, the subscription-completeness state is not reset. `seenTokens` is declared outside the restart loop, so tokens seen by the previous process still count toward completeness, and `health.setSubscriptionComplete(false)` is only asserted once before the loop — `resetSlotsToAuthenticating()` does not touch the `subscriptionComplete` flag. Reset both before relaunching. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-111 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 717-718)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

`lastTickSnapshot` is updated unconditionally even when `writer.write()` returned REJECTED, TIMEOUT, UNCERTAIN, FAILED, or FATAL — outcomes where the tick was not persisted (or, for UNCERTAIN, may not have been). This contradicts the comment "Track last accepted tick for discontinuity evidence": after a later bridge crash/reconnect, `DiscontinuityWriter` will use a snapshot referencing a tick Fluss may never have stored, corrupting gap-boundary evidence. Only update the snapshot when the append was accepted.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 717-718). The reviewer's guidance: `lastTickSnapshot` is updated unconditionally even when `writer.write()` returned REJECTED, TIMEOUT, UNCERTAIN, FAILED, or FATAL — outcomes where the tick was not persisted (or, for UNCERTAIN, may not have been). This contradicts the comment "Track last accepted tick for discontinuity evidence": after a later bridge crash/reconnect, `DiscontinuityWriter` will use a snapshot referencing a tick Fluss may never have stored, corrupting gap-boundary evidence. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-112 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java (lines 48-49)

**Status**

- [x] REJECTED (2026-08-03): BY DESIGN: StubFlussRowConverter is the evidence-gated development stub — it acknowledges without persisting (class javadoc: 'NO REAL DATA IS PERSISTED') until Fluss capability evidence passes. The unconditional ack is the stub's documented contract, not a defect; RealFlussRowConverter is the production path.

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java

**Issue**

This stub unconditionally acknowledges every append as successfully persisted (with a locally incremented offset) while discarding the packet entirely. Since this class lives in the production source tree (src/main/java) and the production entry point (IngestionService.main → FlussClientAdapter.connect) already returns the real converter, this is currently unreachable — but if it is ever wired into the ingestion path it will silently count every tick as durably stored without writing anywhere, causing unrecoverable data loss while misleading the uncertainty journal and backpressure accounting. Consider relocating the stub to test scope or adding a hard guard (e.g., fail-fast when a stub is requested under a production profile) so it can never be selected accidentally.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 48-49). The reviewer's guidance: This stub unconditionally acknowledges every append as successfully persisted (with a locally incremented offset) while discarding the packet entirely. Since this class lives in the production source tree (src/main/java) and the production entry point (IngestionService.main → FlussClientAdapter.connect) already returns the real converter, this is currently unreachable — but if it is ever wired into the ingestion path it will silently count every tick as durably stored without writing anywhere, causing unrecoverable data loss while misleading the uncertainty journal and backpressure accounting. Consider relocating the stub to test scope or adding a hard guard (e.g., fail-fast when a stub is requested under a production profile) so it can never be selected accidentally. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] REJECTED — not a defect; see reason above
- [ ] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [ ] No new review issue introduced by the change
- [ ] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-113 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java (lines 41-43)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java

**Issue**

The volatile `closed` field is written by `close()` but never read, and `append()` does not check it — so `close()` has no observable effect and appends submitted afterwards still succeed. This breaks the AutoCloseable lifecycle contract: RealFlussRowConverter.append() returns `CompletableFuture.failedFuture(new IllegalStateException(...))` once closed, so the stub should mirror that to prevent writes-after-close from being acknowledged as persisted (which could mask shutdown-ordering bugs in the writer).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 41-43). The reviewer's guidance: The volatile `closed` field is written by `close()` but never read, and `append()` does not check it — so `close()` has no observable effect and appends submitted afterwards still succeed. This breaks the AutoCloseable lifecycle contract: RealFlussRowConverter.append() returns `CompletableFuture.failedFuture(new IllegalStateException(...))` once closed, so the stub should mirror that to prevent writes-after-close from being acknowledged as persisted (which could mask shutdown-ordering bugs in the writer). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-114 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java (lines 170-171)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java

**Issue**

requiredLong() accepts 0 for ARROW_MAX_EVENT_AGE_MS / ARROW_MAX_FUTURE_EVENT_SKEW_MS (only negatives are rejected). In IngestionService.classifyFreshness() the check is `receiveTsMs - tsMs > maxEventAgeMs`, so maxEventAgeMs=0 would classify any tick whose receive time is even 1ms after the broker timestamp as STALE and quarantine it — a missing digit or bad value would silently kill the entire market-data feed, which is exactly what this fail-fast startup validator exists to prevent. Enforce a positive minimum (e.g. >= 1) and/or an upper sanity bound, and fix the message to say 'long' instead of 'integer'.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 170-171). The reviewer's guidance: requiredLong() accepts 0 for ARROW_MAX_EVENT_AGE_MS / ARROW_MAX_FUTURE_EVENT_SKEW_MS (only negatives are rejected). >= 1) and/or an upper sanity bound, and fix the message to say 'long' instead of 'integer'. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-115 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java (lines 18-18)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java

**Issue**

The Builder does not validate `instrumentToken` (it defaults to 0) before `build()`, yet `equals()`/`hashCode()` are based solely on `instrumentToken`. Any instrument built without an explicit token — or parsed from a manifest row containing token `0` (which `Long.parseLong` in the loader accepts) — collapses to the same identity and silently overwrites/collides with other entries in hash-based collections. Since the class contract states every field must be validated, `build()`/the constructor should reject `instrumentToken <= 0` (and a default `manifestVersion` of 0) with an explicit exception rather than silently accepting them.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-18). The reviewer's guidance: The Builder does not validate `instrumentToken` (it defaults to 0) before `build()`, yet `equals()`/`hashCode()` are based solely on `instrumentToken`. Any instrument built without an explicit token — or parsed from a manifest row containing token `0` (which `Long.parseLong` in the loader accepts) — collapses to the same identity and silently overwrites/collides with other entries in hash-based collections. Since the class contract states every field must be validated, `build()`/the constructor should reject `instrumentToken <= 0` (and a default `manifestVersion` of 0) with an explicit exception rather than silently accepting them. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-116 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java (lines 22-22)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java

**Issue**

`builder.lotSize > 0 ? builder.lotSize : 1` silently coerces any non-positive lot size (0, negative, or omitted) to 1. This masks invalid manifest data and contradicts the class contract that every field must be validated — a bad lot size is silently turned into a plausible-looking value that will be used in downstream quantity calculations with no signal to monitoring. Consider rejecting invalid values (e.g., throw IllegalArgumentException) or explicitly documenting the normalization as intended behavior.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 22-22). The reviewer's guidance: This masks invalid manifest data and contradicts the class contract that every field must be validated — a bad lot size is silently turned into a plausible-looking value that will be used in downstream quantity calculations with no signal to monitoring. Consider rejecting invalid values (e.g., throw IllegalArgumentException) or explicitly documenting the normalization as intended behavior. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-117 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java (lines 88-88)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java

**Issue**

NPE risk in the shutdown path: `journalPath.getParent()` can return `null` when the configured path is a bare filename (e.g. `UNCERTAINTY_JOURNAL_PATH=journal.jsonl`). `Files.createDirectories(null)` throws `NullPointerException`, which is a `RuntimeException` and is NOT caught by the `catch (IOException e)` block, so it escapes `write()` and aborts the rest of `shutdown()` (metrics flush, drain). Note `ensureWritable()` already null-checks the parent gracefully — this method should do the same for consistency and robustness.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 88-88). The reviewer's guidance: `Files.createDirectories(null)` throws `NullPointerException`, which is a `RuntimeException` and is NOT caught by the `catch (IOException e)` block, so it escapes `write()` and aborts the rest of `shutdown()` (metrics flush, drain). Note `ensureWritable()` already null-checks the parent gracefully — this method should do the same for consistency and robustness. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-118 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java (lines 116-117)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java

**Issue**

`totalBytesAccepted` is declared with a getter and consumed at shutdown by `IngestionService.shutdown()` (uncertainty journal), but it is never incremented anywhere in this class. As a result `totalBytesAccepted()` always returns 0, so the journal and any byte-volume monitoring record zero accepted bytes. Increment it by `recordBytes` on the successful accept path (alongside `totalAccepted`).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 116-117). The reviewer's guidance: `totalBytesAccepted` is declared with a getter and consumed at shutdown by `IngestionService.shutdown()` (uncertainty journal), but it is never incremented anywhere in this class. As a result `totalBytesAccepted()` always returns 0, so the journal and any byte-volume monitoring record zero accepted bytes. Increment it by `recordBytes` on the successful accept path (alongside `totalAccepted`). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-119 — code/02_services/01_ingestion/src/main/resources/log4j2.xml (lines 21-21)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/resources/log4j2.xml

**Issue**

This CONSOLE_PATTERN renders a non-empty `correlation_id` three times with no separator. `%equals{%mdc{correlation_id}}{}{}` outputs the MDC value whenever it is non-empty, `%equals{%mdc{correlation_id}}{ - cid=}{}` always outputs the value again (a real correlation ID never equals the literal `- cid=`), and the trailing `%mdc{correlation_id}` prints it a third time. The result for a correlation id `abc` is `abcabcabc`, making console logs unreadable and preventing the intended `- cid=` prefix from ever appearing. Replace the three expressions with a single conditional rendering.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 21-21). The reviewer's guidance: The result for a correlation id `abc` is `abcabcabc`, making console logs unreadable and preventing the intended `- cid=` prefix from ever appearing. Replace the three expressions with a single conditional rendering. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-120 — code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java (lines 58-58)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java

**Issue**

The Flink job graph is empty — no source, transformation, or sink is added before `env.execute()`. Flink's `StreamGraphGenerator` rejects an empty topology at submission with `IllegalStateException: No operators defined in streaming topology. Cannot execute.`, so this job will fail on startup instead of running as a safe no-op (and `submit-jobs.sh` submits `babysitter` as one of the expected jobs). If the MVP intent is a deployable no-op job, add at least a placeholder source + sink (e.g., `env.fromElements(0).print()`) or defer `env.execute()` until the Positions changelog source is wired per the TODO.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 58-58). The reviewer's guidance: Flink's `StreamGraphGenerator` rejects an empty topology at submission with `IllegalStateException: No operators defined in streaming topology. If the MVP intent is a deployable no-op job, add at least a placeholder source + sink (e.g.,`env.fromElements(0).print()`) or defer`env.execute()` until the Positions changelog source is wired per the TODO. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-121 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java (lines 46-47)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java

**Issue**

`clientOrderRef.value()` is dereferenced before any null check on `clientOrderRef` itself. If a caller passes a null `ClientOrderRef`, the constructor throws a `NullPointerException` instead of the intended `IllegalArgumentException`, which is harder to diagnose. Also note `ClientOrderRef`'s own constructor performs no validation, so `new ClientOrderRef(null)` is possible — add a null guard for the wrapper.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 46-47). The reviewer's guidance: If a caller passes a null `ClientOrderRef`, the constructor throws a `NullPointerException` instead of the intended `IllegalArgumentException`, which is harder to diagnose. Also note `ClientOrderRef`'s own constructor performs no validation, so `new ClientOrderRef(null)` is possible — add a null guard for the wrapper. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-122 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java (lines 58-58)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java

**Issue**

`price` is accepted as an arbitrary String with no format or cross-field validation. For LMT/SL_LMT orders the broker requires a valid numeric price, while MKT/SL_MKT should use "0". Without validating that price is numeric and consistent with `order`, an invalid payload can be constructed here and only be rejected later by the broker, or worse, sent with unintended order semantics. Consider validating the numeric format and requiring non-empty price for limit-type orders / "0" for market-type orders.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 58-58). The reviewer's guidance: For LMT/SL_LMT orders the broker requires a valid numeric price, while MKT/SL_MKT should use "0". Without validating that price is numeric and consistent with `order`, an invalid payload can be constructed here and only be rejected later by the broker, or worse, sent with unintended order semantics. Consider validating the numeric format and requiring non-empty price for limit-type orders / "0" for market-type orders. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-123 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java (lines 43-45)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java

**Issue**

Mandatory Arrow fields — `exchange`, `instrumentToken`, `transactionType`, `order`, `product`, and `validity` — are not null-checked in the constructor. A null in any of these will not be caught at construction time and will surface later as an NPE while building/serializing the request payload, making the failure harder to attribute. Add explicit null checks (or use Objects.requireNonNull) alongside the existing symbol/quantity/ref validation to fail fast.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 43-45). The reviewer's guidance: Add explicit null checks (or use Objects.requireNonNull) alongside the existing symbol/quantity/ref validation to fail fast. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-124 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java (lines 32-32)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java

**Issue**

When `requestTime` is absent from the response or is not a JSON number (e.g., the broker returns it as a quoted string), the method silently defaults to 0L. Since 0 is an invalid epoch-ms timestamp (1970-01-01), this can silently corrupt downstream ordering/audit logic, and it is inconsistent with the `orderNo` handling above, which throws on missing values. Consider throwing `IllegalArgumentException` when `requestTime` is missing or non-numeric, or at least validate `time > 0` before constructing the response.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 32-32). The reviewer's guidance: Consider throwing `IllegalArgumentException` when `requestTime` is missing or non-numeric, or at least validate `time > 0` before constructing the response. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-125 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java (lines 24-24)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java

**Issue**

OrderStatus.from uses a strict valueOf() after trim/uppercase, which throws an unchecked IllegalArgumentException for any value not exactly matching one of the enum constants. Note the internal spelling inconsistency in this same class: OrderStatus.CANCELLED uses a double "L", while ReportType.CANCELED (wire "Canceled") uses a single "L". Since both claim to model Arrow's cancellation vocabulary, if the broker's orderStatus uses the single-L spelling (as its own reportType wire does), OrderStatus.from("CANCELED") will fail to match and throw. Any new/renamed broker status will also abort the current record decode. Consider aligning the spellings and mapping unrecognized statuses to an UNKNOWN sentinel (or returning Optional) so unexpected broker data is quarantined rather than crashing the pipeline.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 24-24). The reviewer's guidance: Consider aligning the spellings and mapping unrecognized statuses to an UNKNOWN sentinel (or returning Optional) so unexpected broker data is quarantined rather than crashing the pipeline. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-126 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java (lines 51-51)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java

**Issue**

ReportType.from throws IllegalArgumentException for any unrecognized value, and the wire vocabulary declared here ("NewAck", "PendingNew", "Canceled") may not cover what the broker actually emits. The vendored Arrow Go SDK in this repo documents reportType as "Type of order report (e.g., NEW, FILL, CANCEL)" (go-bridge/third_party/go-arrow/arrow/orders.go), i.e. a real reportType such as "NEW" would not match "NewAck"/"NEW_ACK" or "CANCEL"/"CANCELED" and would throw, aborting the event decode path. Since this consumes external broker data, consider returning an UNKNOWN sentinel (or Optional) and quarantining the raw event instead of propagating an unhandled exception.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 51-51). The reviewer's guidance: Since this consumes external broker data, consider returning an UNKNOWN sentinel (or Optional) and quarantining the raw event instead of propagating an unhandled exception. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-127 — code/common/src/main/java/com/trading/common/config/PlatformConfig.java (lines 62-62)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/config/PlatformConfig.java

**Issue**

This validation can never trigger: `DEDUP_TTL_MS` and `CANDLE_WINDOW_MS` are `static final` compile-time constants initialized to exactly the compared literals (300_000L / 15_000L), so both `!=` conditions are compile-time-constant expressions that are always false. The javadoc claims startup rejects wrong values, but this method is both unreachable dead code and never invoked anywhere — the actual ingestion entry point (`IngestionService.main`) validates via `IngestionConfig.validate()` instead, so the promised safety net provides only false assurance. If these values truly must be validated at startup, they should be loaded from configuration rather than hard-coded finals; otherwise the method should be removed. Also note the literal `300_000L`/`15_000L` duplicates violate the class's own 'no scattered numeric literals' rule and will go stale if the constants change.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 62-62). The reviewer's guidance: The javadoc claims startup rejects wrong values, but this method is both unreachable dead code and never invoked anywhere — the actual ingestion entry point (`IngestionService.main`) validates via `IngestionConfig.validate()` instead, so the promised safety net provides only false assurance. If these values truly must be validated at startup, they should be loaded from configuration rather than hard-coded finals; otherwise the method should be removed. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-128 — code/common/src/main/java/com/trading/common/model/MarketTick.java (lines 39-39)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/model/MarketTick.java

**Issue**

`isValid()` compares `validityState` against the exact literal `"VALID"`, but the Ingestion pipeline (`RealFlussRowConverter.append`) writes the `ValidityClassification` enum name into the `validity_state` column of raw_table_1 — i.e. `"VALID_TRADE"`, `"VALID_NON_TRADE"`, `"INVALID_VALUES"`, etc. Since none of the values actually persisted equals `"VALID"`, `isValid()` (and therefore `isValidTrade()`) will always return `false` for the rows this record claims to represent. Align the check with the stored values (e.g. `validityState != null && validityState.startsWith("VALID")`), or change the writer to persist a canonical `"VALID"`/`"INVALID"` string.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 39-39). The reviewer's guidance: Align the check with the stored values (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-129 — code/common/src/main/java/com/trading/common/observability/SafetyHaltRequest.java (lines 21-23)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/SafetyHaltRequest.java

**Issue**

`isIdempotentDuplicate` returns true whenever `prior == incoming`, which treats repeated `FAILED` or `PENDING` results as idempotent no-ops as well. A `FAILED` result means the halt was never applied, and `PENDING` is non-terminal — treating a repeat of those as a duplicate would suppress a retry and could leave the system halted-intended-but-not-actually-halted. Idempotency should only hold for terminal success outcomes (`APPLIED`, `ALREADY_HALTED`), so `FAILED`/`PENDING` repeats should be retried rather than deduped.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 21-23). The reviewer's guidance: Idempotency should only hold for terminal success outcomes (`APPLIED`, `ALREADY_HALTED`), so `FAILED`/`PENDING` repeats should be retried rather than deduped. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-130 — code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java (lines 165-167)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java

**Issue**

The Javadoc states 12 required fields (timestamp, level, service, component, subsystem, host, vm_id, environment, correlation_id, trace_id, span_id, message) and this class is the only supported log shape, but `build()` performs no validation. If any required field is passed as null, `toAttributes()` will emit null/empty values for mandated attributes, and `OtlpEmitter.emitLog` will append a literal `null` string for service/level/message (it appends these directly without null handling), producing a malformed record that violates the OpenObserve contract (docs/04_contracts/openobserve.md §F). Suggest fail-fast validation with `Objects.requireNonNull` on all required fields in the constructor/build().

**Implementation Plan**

Implement the corrective action described in the finding (report lines 165-167). The reviewer's guidance: The Javadoc states 12 required fields (timestamp, level, service, component, subsystem, host, vm_id, environment, correlation_id, trace_id, span_id, message) and this class is the only supported log shape, but `build()` performs no validation. If any required field is passed as null, `toAttributes()` will emit null/empty values for mandated attributes, and `OtlpEmitter.emitLog` will append a literal `null` string for service/level/message (it appends these directly without null handling), producing a malformed record that violates the OpenObserve contract (docs/04_contracts/openobserve.md §F). Suggest fail-fast validation with `Objects.requireNonNull` on all required fields in the constructor/build(). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-131 — code/pom.xml (lines 143-143)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/pom.xml

**Issue**

This JVM flag is only wired into the surefire (test) argLine, but the comment states it is required by Arrow MemoryUtil (Fluss client) at runtime on JDK 17+. The production launch paths are inconsistent: `run-ingestion-full.sh` does pass `--add-opens=java.base/java.nio=ALL-UNNAMED`, while the Docker deployment (`docker-entrypoint.sh`) launches `java -cp /app/ingestion.jar` with no JVM options. If the Fluss/Arrow native-memory path is exercised in the container (as the comment implies), the service can fail with `InaccessibleObjectException`/`IllegalAccessError`. Add the flag to the runtime JVM launch (e.g. via `JAVA_TOOL_OPTIONS` in the Dockerfile/entrypoint) so test and production environments match.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 143-143). The reviewer's guidance: The production launch paths are inconsistent: `run-ingestion-full.sh` does pass `--add-opens=java.base/java.nio=ALL-UNNAMED`, while the Docker deployment (`docker-entrypoint.sh`) launches `java -cp /app/ingestion.jar` with no JVM options. Add the flag to the runtime JVM launch (e.g. via `JAVA_TOOL_OPTIONS` in the Dockerfile/entrypoint) so test and production environments match. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-132 — start-all.sh (lines 81-81)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- start-all.sh

**Issue**

The script depends on `nc` (netcat) for the Fluss reachability check without verifying it is installed. On a host without `nc`, `! nc -z ...` evaluates to true (command-not-found 127 negated), so the script starts docker compose unnecessarily, and the wait loop below can never detect readiness — it always dies with "Fluss did not come up" after 60s even when Fluss is already healthy. Use bash's built-in `/dev/tcp` (as run-ingestion-full.sh does) or add a `command -v nc` preflight check first.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 81-81). The reviewer's guidance: Use bash's built-in `/dev/tcp` (as run-ingestion-full.sh does) or add a `command -v nc` preflight check first. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-133 — code/02_services/04_executor/Dockerfile (lines 1-1)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/02_services/04_executor/Dockerfile

**Issue**

Pinning to the exact patch `3.11.9` is quite stale (released April 2024), so the resulting image misses subsequent Python 3.11 security and bug-fix patches as well as updated OS packages. If the intent is to stay on Python 3.11, prefer a floating tag like `python:3.11-slim` to track the latest 3.11.x patch; if a fully reproducible build is required, pin to the current 3.11 patch and add a comment/automation so the pin is regularly refreshed.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 1-1). The reviewer's guidance: Pinning to the exact patch `3.11.9` is quite stale (released April 2024), so the resulting image misses subsequent Python 3.11 security and bug-fix patches as well as updated OS packages. If the intent is to stay on Python 3.11, prefer a floating tag like `python:3.11-slim` to track the latest 3.11.x patch; if a fully reproducible build is required, pin to the current 3.11 patch and add a comment/automation so the pin is regularly refreshed. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-134 — code/common/src/main/java/com/trading/common/observability/AuditLogger.java (lines 21-21)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/AuditLogger.java

**Issue**

The redaction check is case-sensitive and exact-match only. For a "mandatory redaction" invariant, this silently leaks sensitive data whenever the caller passes a field name that differs in case (e.g., `apiKey`, `authToken`, `API_KEY`) or contains a sensitive keyword (e.g., `client_secret`, `access_token`) — none of which match the literal entries in REDACTED_FIELDS. Consider normalizing the field (lowercase/trim) before the lookup, and optionally match on containment so variants of known sensitive keys are still caught.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 21-21). The reviewer's guidance: Consider normalizing the field (lowercase/trim) before the lookup, and optionally match on containment so variants of known sensitive keys are still caught. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-135 — run-ingestion.sh (lines 29-30)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- run-ingestion.sh

**Issue**

The secrets file is sourced after only an existence check; nothing verifies it is not group/world readable. If the file was created with a default umask (e.g. 644), the Arrow credentials are exposed to other local users. Enforce restrictive permissions before sourcing, matching the `chmod 600` guidance in the header.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 29-30). The reviewer's guidance: The secrets file is sourced after only an existence check; nothing verifies it is not group/world readable. If the file was created with a default umask (e.g. 644), the Arrow credentials are exposed to other local users. Enforce restrictive permissions before sourcing, matching the `chmod 600` guidance in the header. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-136 — code/01_platform/02_sql/ddl/06_ranking_results.sql (lines 32-32)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/06_ranking_results.sql

**Issue**

The table is declared as a "per-evaluation ranking audit" and `rank`/`selected` only have meaning within one evaluation, yet it is bucketed by `candidate_id`. A single evaluation's rows (one per candidate) will therefore be scattered across all 8 buckets, so a consumer reading one evaluation's full ranking must scan and merge every bucket and can no longer rely on append order (rank order) within a bucket. If the primary consumer reads per evaluation (e.g., Signal job reconstructing the ranking to derive Trade_Decisions), `evaluation_id` would be a better bucket key to co-locate an evaluation's rows and preserve insertion order. Note the header comment also says "Bucket key: evaluation/candidate routing identity", which is ambiguous and doesn't match the actual `candidate_id`. Please confirm the actual read pattern and align the bucket key (or the header comment) accordingly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 32-32). The reviewer's guidance: A single evaluation's rows (one per candidate) will therefore be scattered across all 8 buckets, so a consumer reading one evaluation's full ranking must scan and merge every bucket and can no longer rely on append order (rank order) within a bucket. If the primary consumer reads per evaluation (e.g., Signal job reconstructing the ranking to derive Trade_Decisions), `evaluation_id` would be a better bucket key to co-locate an evaluation's rows and preserve insertion order. Please confirm the actual read pattern and align the bucket key (or the header comment) accordingly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-137 — code/01_platform/04_scripts/soak-monitor.sh (lines 59-59)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/soak-monitor.sh

**Issue**

Every sample re-greps the entire journal for each pattern — the journal rolls at 64 MB, so over a multi-hour soak each 5 s sample performs up to 4 full-file O(n) scans, adding continuous CPU/disk I/O to the ingestion host. Also `grep -c` counts matching *lines* (not occurrences), and the totals are cumulative since file start, so when the daily/64 MB rolling policy rotates the file the counts silently reset and trend analysis is misleading. Prefer incremental reads (e.g. `tail -c +N` from the last offset) and per-interval deltas; use `grep -Fc` for these literal patterns since they are fixed strings, not regexes.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 59-59). The reviewer's guidance: Also `grep -c` counts matching *lines* (not occurrences), and the totals are cumulative since file start, so when the daily/64 MB rolling policy rotates the file the counts silently reset and trend analysis is misleading. Prefer incremental reads (e.g. `tail -c +N` from the last offset) and per-interval deltas; use `grep -Fc` for these literal patterns since they are fixed strings, not regexes. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-017, R-018, R-174, R-236, R-237, R-238

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-138 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go (lines 47-47)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go

**Issue**

The HTTP client is created with no timeout and the three request helpers use plain `HTTPClient.Do` (no context, deadline, or `DoTimeout`/`DoDeadline`). A stalled Arrow endpoint blocks the calling goroutine indefinitely: at startup this hangs `main` inside `AutoLogin` before the signal handler is installed, and during HFT auth refresh it stalls the read goroutine (tick decoding and the raw_table write path stop with no way to cancel) and leaks a goroutine per stuck refresh. Set `ReadTimeout`/`WriteTimeout` on the fasthttp client and/or bound each call with `DoTimeout`/`DoDeadline`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 47-47). The reviewer's guidance: The HTTP client is created with no timeout and the three request helpers use plain `HTTPClient.Do` (no context, deadline, or `DoTimeout`/`DoDeadline`). A stalled Arrow endpoint blocks the calling goroutine indefinitely: at startup this hangs `main` inside `AutoLogin` before the signal handler is installed, and during HFT auth refresh it stalls the read goroutine (tick decoding and the raw_table write path stop with no way to cancel) and leaks a goroutine per stuck refresh. Set `ReadTimeout`/`WriteTimeout` on the fasthttp client and/or bound each call with `DoTimeout`/`DoDeadline`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-139 — code/02_services/01_ingestion/go-bridge/token_provider.go (lines 42-42)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/token_provider.go

**Issue**

Refresh holds p.mu while invoking refreshFn, which for a token provider typically performs network I/O. During the refresh window, every Current() call and every queued Refresh() caller blocks on the exclusive Lock(); because sync.Mutex.Lock is not context-aware, waiters queued behind a slow/hung refresh cannot honor their context cancellation or deadline while blocked. This can stall the auth path that depends on Current once the provider is wired into the bridge. Consider running refreshFn outside the exclusive lock (snapshot `current`, release the lock, call refreshFn, then re-acquire and update only on success) and serialize in-flight refreshes with a dedicated mechanism (e.g., a singleflight/atomic flag or a shared result channel) so readers are not blocked for the whole refresh. At minimum, use RWMutex so Current takes the read lock.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 42-42). The reviewer's guidance: During the refresh window, every Current() call and every queued Refresh() caller blocks on the exclusive Lock(); because sync.Mutex.Lock is not context-aware, waiters queued behind a slow/hung refresh cannot honor their context cancellation or deadline while blocked. Consider running refreshFn outside the exclusive lock (snapshot `current`, release the lock, call refreshFn, then re-acquire and update only on success) and serialize in-flight refreshes with a dedicated mechanism (e.g., a singleflight/atomic flag or a shared result channel) so readers are not blocked for the whole refresh. At minimum, use RWMutex so Current takes the read lock. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-140 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 714-715)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

`refreshResourceMetrics()` is invoked once per processed frame and performs a directory listing of `/proc/self/fd`, a read of `/proc/sys/fs/file-max`, a full read of `/proc/self/status`, and a `ThreadMXBean.getThreadCount()` call. At HFT tick rates (thousands of frames/sec) these syscalls add measurable per-frame overhead on the hot ingestion path. Throttle the refresh to a fixed interval (e.g., 1s) instead of every frame.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 714-715). The reviewer's guidance: At HFT tick rates (thousands of frames/sec) these syscalls add measurable per-frame overhead on the hot ingestion path. Throttle the refresh to a fixed interval (e.g., 1s) instead of every frame. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-141 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java (lines 187-194)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java

**Issue**

The Fluss `Connection` opened in the constructor is a local variable — it is never stored, and `close()` only calls `writer.flush()`, so the connection (and the underlying Sender/MetadataUpdater threads) is never released. If `getTable()` or `createWriter()` throws after the connection is created, it also leaks on the exception path. In a long-running service this leaks client resources. Store the connection in a field and close it in `close()` (and on the constructor failure path).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 187-194). The reviewer's guidance: The Fluss `Connection` opened in the constructor is a local variable — it is never stored, and `close()` only calls `writer.flush()`, so the connection (and the underlying Sender/MetadataUpdater threads) is never released. Store the connection in a field and close it in `close()` (and on the constructor failure path). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-142 — code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java (lines 148-150)

**Status**

- [x] Done (Phase 7, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java

**Issue**

`generateTicks()` runs on the single scheduled executor thread and synchronously `write`/`flush`es to every connected client in a loop. A slow or stalled client can block the scheduler, stalling tick generation for all clients and making the mock unresponsive. There is no per-client send queue, write timeout, or backpressure. Consider decoupling per-client delivery (e.g. bounded per-client queues with a dedicated sender per client, or socket write timeouts).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 148-150). The reviewer's guidance: Consider decoupling per-client delivery (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green; PerfBaselineTest re-certified)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test green)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-143 — Makefile (lines 46-47)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- Makefile

**Issue**

The new targets `cep-check`, `test`, and `test-ingestion` are not declared in `.PHONY`. Since these targets do not correspond to real files, Make will skip them if a file/directory with the same name exists (a `test` directory at the repo root is a common occurrence), silently doing nothing. Add them to the existing `.PHONY` line.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 46-47). The reviewer's guidance: Add them to the existing `.PHONY` line. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-144 — code/01_platform/02_sql/ddl/09_order_lifecycle.sql (lines 26-27)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/09_order_lifecycle.sql

**Issue**

The header states this table is "rebuildable from Fills audit", but the source `Fills` table has `table.retention.days = '3'` while this table retains 7 days. Any rebuild beyond the 3-day Fills window would be incomplete, contradicting the 7-day retention claim. Either align retention with the rebuild source (keep Order_Lifecycle ≤ Fills retention) or document that full rebuild is limited to the Fills retention window.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 26-27). The reviewer's guidance: Either align retention with the rebuild source (keep Order_Lifecycle ≤ Fills retention) or document that full rebuild is limited to the Fills retention window. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-145 — code/01_platform/02_sql/ddl/13_order_correlation.sql (lines 6-6)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/13_order_correlation.sql

**Issue**

The header comment declares the table scope as `account_scope_id`, but no `account_scope_id` column exists in the schema (unlike `Positions`, which materializes the scope column). If `instruction_id` is not guaranteed to be globally unique across accounts, KV upserts keyed only on `instruction_id` will collide and silently overwrite another account's correlation record. Either add an `account_scope_id` column (and include it in the PK/bucket key if scoping is required) or correct the header comment to reflect that the key is globally unique.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 6-6). The reviewer's guidance: Either add an `account_scope_id` column (and include it in the PK/bucket key if scoping is required) or correct the header comment to reflect that the key is globally unique. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-146 — code/01_platform/02_sql/ddl/16_postback_quarantine.sql (lines 25-27)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/16_postback_quarantine.sql

**Issue**

The header claims "Retention: until disposition + buffer" and "Lake: encrypted evidence per policy", but the WITH clause only sets a fixed 7-day retention and drops the datalake options that the previous DDL had (`table.datalake.enabled`/`format`/`freshness`/`auto-compaction`). A quarantined postback awaiting operator disposition past 7 days will expire, and evidence will never be offloaded to the encrypted lake — contradicting the stated evidence policy and the original 7-year encrypted retention intent. Either restore the datalake options and use a disposition-aware retention (e.g. extend while OPEN), or correct the header to match the actual (7-day, no-lake) behavior.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 25-27). The reviewer's guidance: The header claims "Retention: until disposition + buffer" and "Lake: encrypted evidence per policy", but the WITH clause only sets a fixed 7-day retention and drops the datalake options that the previous DDL had (`table.datalake.enabled`/`format`/`freshness`/`auto-compaction`). A quarantined postback awaiting operator disposition past 7 days will expire, and evidence will never be offloaded to the encrypted lake — contradicting the stated evidence policy and the original 7-year encrypted retention intent. Either restore the datalake options and use a disposition-aware retention (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-147 — code/01_platform/04_scripts/ddl_apply.py (lines 284-286)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/ddl_apply.py

**Issue**

Exception-hardening gap: main() only catches RuntimeError, but several non-RuntimeError failures can occur upstream: a structurally-wrong yet valid-JSON manifest makes diff_manifests() raise KeyError/TypeError (e.g., a table entry missing 'ddl_path'), and compute_manifest_entries() opens DDL files with encoding='utf-8' while only catching OSError, so a non-UTF-8 file raises an unhandled UnicodeDecodeError. All of these escape as raw tracebacks instead of the intended clean 'DDL contract error' diagnostic, which is especially counterproductive for a safety-gate script. Normalize/validate the manifest structure on load and broaden the catch (e.g., include KeyError/TypeError/UnicodeDecodeError, or catch Exception) in main().

**Implementation Plan**

Implement the corrective action described in the finding (report lines 284-286). The reviewer's guidance: All of these escape as raw tracebacks instead of the intended clean 'DDL contract error' diagnostic, which is especially counterproductive for a safety-gate script. Normalize/validate the manifest structure on load and broaden the catch (e.g., include KeyError/TypeError/UnicodeDecodeError, or catch Exception) in main(). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-148 — code/01_platform/04_scripts/digest-pin.sh (lines 33-33)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/digest-pin.sh

**Issue**

All three resolver branches redirect stderr to /dev/null, so registry authentication errors, unreachable registries, and nonexistent tags are hidden. The user only ever sees the generic "could not resolve digest" message, which in a CI pipeline obscures the root cause and slows debugging. Consider capturing the underlying error and printing it on failure (e.g., keep stderr in a variable and echo it when the digest is empty).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 33-33). The reviewer's guidance: The user only ever sees the generic "could not resolve digest" message, which in a CI pipeline obscures the root cause and slows debugging. Consider capturing the underlying error and printing it on failure (e.g., keep stderr in a variable and echo it when the digest is empty). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-149 — code/01_platform/04_scripts/run-monday-gates.sh (lines 18-18)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/run-monday-gates.sh

**Issue**

PROJECT_ROOT defaults to a hard-coded personal absolute path. The script's stated purpose is "wire it into CI or a cron", where this path will not exist, so `mkdir -p $OUT_DIR` and the `cd $BRIDGE_DIR`/`cd $CODE_DIR` steps will fail with confusing errors. Derive the root from the script location instead (overridable via env).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-18). The reviewer's guidance: Derive the root from the script location instead (overridable via env). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-094, R-213, R-274

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-150 — code/01_platform/04_scripts/run-monday-gates.sh (lines 23-23)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/run-monday-gates.sh

**Issue**

No preflight validation: with `set -euo pipefail`, a missing `go`/`mvn`/`java` or a nonexistent project directory produces an opaque "command not found"/`cd` failure buried in a log file, making environment breakdowns hard to diagnose in CI/cron. Add explicit prerequisite checks up front so failures are actionable.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 23-23). The reviewer's guidance: No preflight validation: with `set -euo pipefail`, a missing `go`/`mvn`/`java` or a nonexistent project directory produces an opaque "command not found"/`cd` failure buried in a log file, making environment breakdowns hard to diagnose in CI/cron. Add explicit prerequisite checks up front so failures are actionable. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-094, R-213, R-274

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-151 — code/01_platform/04_scripts/soak-monitor.sh (lines 26-26)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/soak-monitor.sh

**Issue**

The hard-coded `PROJECT_ROOT=/home/saurabh/...` default makes the script non-portable. On any other checkout/host, `LOG_FILE` and `OUT_DIR` resolve to non-existent paths and every event count silently becomes 0 — the monitor would report a healthy soak even though it is sampling nothing. Derive the root from the script's own location (e.g. `SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"` then `$SCRIPT_DIR/..`, or `git rev-parse --show-toplevel`) and keep the absolute path only as an env override.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 26-26). The reviewer's guidance: Derive the root from the script's own location (e.g. `SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"` then `$SCRIPT_DIR/..`, or `git rev-parse --show-toplevel`) and keep the absolute path only as an env override. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-017, R-018, R-174, R-236, R-237, R-238

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-152 — code/02_services/01_ingestion/.dockerignore (lines 1-12)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/.dockerignore

**Issue**

This `.dockerignore` is placed in a subdirectory of the build context and will never be honored by `docker build`. Per the Dockerfile header, the build context MUST be the reactor root `code/` (`docker build -f 02_services/01_ingestion/Dockerfile .` from `code/`), and Docker only reads `.dockerignore` at the context root. The same exclusions are already effectively applied by `code/.dockerignore` via `**/`-prefixed patterns (`**/.env`, `**/logs/`, `**/target/`, `**/*.log`, etc.). As written, this file is dead config that may mislead developers into thinking the context is sanitized when building from this directory (which would actually fail, since the Dockerfile's COPY paths are relative to `code/`). Recommend removing this file, or if per-module ignore rules are desired, documenting that they must live in `code/.dockerignore` to take effect.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 1-12). The reviewer's guidance: Per the Dockerfile header, the build context MUST be the reactor root `code/` (`docker build -f 02_services/01_ingestion/Dockerfile .` from `code/`), and Docker only reads `.dockerignore` at the context root. The same exclusions are already effectively applied by `code/.dockerignore` via `**/`-prefixed patterns (`**/.env`, `**/logs/`, `**/target/`, `**/*.log`, etc.). As written, this file is dead config that may mislead developers into thinking the context is sanitized when building from this directory (which would actually fail, since the Dockerfile's COPY paths are relative to `code/`). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-153 — code/02_services/01_ingestion/go-bridge/hft_slot.go (lines 163-163)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/go-bridge/hft_slot.go

**Issue**

`Run()` is a no-op session driver: it only blocks on `ctx.Done()` and returns `nil` — it never calls `BeginConnect()`, `SubscribeHFTTokens()`, or the frame-read loop, and never propagates errors. If this method were ever used as the per-slot main loop (its clear intent), slots would silently "succeed" without ingesting a single tick. Broader context: the whole `HFTSlot`/`SlotConfig`/`validateRequestUnion` surface is referenced only by `hft_slot_test.go`; the production supervisor (`runHFTSlotWithFactory`/`runHFTEpoch` in supervisor.go/main.go) implements a separate functional slot machine and never touches this type. This file is effectively a duplicate, test-only stub of the supervisor logic — either wire it into the real path or remove it to avoid drift between two implementations.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 163-163). The reviewer's guidance: Broader context: the whole `HFTSlot`/`SlotConfig`/`validateRequestUnion` surface is referenced only by `hft_slot_test.go`; the production supervisor (`runHFTSlotWithFactory`/`runHFTEpoch` in supervisor.go/main.go) implements a separate functional slot machine and never touches this type. This file is effectively a duplicate, test-only stub of the supervisor logic — either wire it into the real path or remove it to avoid drift between two implementations. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-154 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java (lines 121-121)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

**Issue**

Detecting an existing table by scanning the exception message for the substring 'already exist' is fragile: the text is version/locale dependent, and any future change in Fluss's error wording will misclassify the error as a real failure (or, worse, mask one). Additionally, in the inner catch that logs 'already exists, skip schema check', ok++ still counts the table as verified even though the schema check was skipped. Prefer the ignoreIfExists flag semantics or a dedicated exception type, and surface schema-check failures explicitly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 121-121). The reviewer's guidance: Prefer the ignoreIfExists flag semantics or a dedicated exception type, and surface schema-check failures explicitly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-155 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java (lines 94-97)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java

**Issue**

The viewer reads columns via hardcoded positional indexes (4, 5, 6, 11, 14, 15, 16, 25) that are coupled to the exact column order of `raw_table_1`. While these currently match the DDL (`instrument_token`, `exchange`, `symbol`, `event_time`, `tick_type`, `last_price_paise`, `last_qty`, `validity_state`), both the DDL (02_raw_table_1.sql) and the row converters (RealFlussRowConverter/StubFlussRowConverter) are changed in this same update. Any future column reorder, insertion, or count change will silently misalign the displayed data or throw IndexOutOfBounds and crash the viewer. Since `table.getTableInfo().getRowType()` is already available, consider resolving indexes by field name from the schema (or at least defining named constants mirroring the DDL order) and validating the field count before reading.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 94-97). The reviewer's guidance: Any future column reorder, insertion, or count change will silently misalign the displayed data or throw IndexOutOfBounds and crash the viewer. Since `table.getTableInfo().getRowType()` is already available, consider resolving indexes by field name from the schema (or at least defining named constants mirroring the DDL order) and validating the field count before reading. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-156 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java (lines 0-0)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java

**Issue**

MAX_PENDING_APPEND_BYTES is validated twice with conflicting behavior. The earlier longRange() call already enforces the 1 MiB floor and records range/format errors, but this block re-reads the same env key and unconditionally overwrites b.maxPendingBytes with Long.parseLong(), bypassing the min/max check (and on parse failure it appends a duplicate error for the same key). The 'container memory' comment doesn't match the source — it is the exact same variable already validated above, so the first validation result is effectively dead. Recommend removing this redundant block so the longRange() validation is authoritative; if a dynamic container-memory override is genuinely intended, read a distinct key and derive the byte value from it.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 0-0). The reviewer's guidance: MAX_PENDING_APPEND_BYTES is validated twice with conflicting behavior. The earlier longRange() call already enforces the 1 MiB floor and records range/format errors, but this block re-reads the same env key and unconditionally overwrites b.maxPendingBytes with Long.parseLong(), bypassing the min/max check (and on parse failure it appends a duplicate error for the same key). The 'container memory' comment doesn't match the source — it is the exact same variable already validated above, so the first validation result is effectively dead. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-157 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java (lines 11-12)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java

**Issue**

This model class is dead code: it is never referenced anywhere in the codebase. `DiscontinuityWriter` persists suspected-discontinuity evidence by building Fluss `GenericRow` rows directly, and `IngestionService`/`DiscontinuityReasonMappingTest` never construct `DiscontinuityEvent`. An unused model that duplicates the writer's own field mapping will silently drift from the real persistence path (it already does: the DDL has no `status`/`affected_scope` columns). Either wire this model into `DiscontinuityWriter` so the record shape stays consistent, or remove it.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 11-12). The reviewer's guidance: Either wire this model into `DiscontinuityWriter` so the record shape stays consistent, or remove it. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-158 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java (lines 20-20)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java

**Issue**

The `status` field is an unvalidated free-form String with magic values (OPEN/ACKNOWLEDGED/CLOSED). A typo such as "OPEn" would be silently accepted and produce inconsistent evidence, and the `suspected_discontinuities` DDL contains no `status` column, so this field does not map to the persisted schema. Use an enum or shared constants (with validation) and keep the model aligned with the actual DDL columns if this class is intended for persistence.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 20-20). The reviewer's guidance: The `status` field is an unvalidated free-form String with magic values (OPEN/ACKNOWLEDGED/CLOSED). Use an enum or shared constants (with validation) and keep the model aligned with the actual DDL columns if this class is intended for persistence. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-159 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java (lines 25-26)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java

**Issue**

The builder silently substitutes defaults for potentially required fields: `connectionEpoch` is a primitive `long`, so omitting it yields 0 (indistinguishable from a real epoch and easily mistaken for 1970-01-01), `reasonCode` becomes "UNKNOWN", and `status` becomes "OPEN". This masks construction bugs and can produce misleading evidence records. Consider requiring these fields (e.g., boxed `Long` with a null check) or making the fallback behavior explicit and documented.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 25-26). The reviewer's guidance: Consider requiring these fields (e.g., boxed `Long` with a null check) or making the fallback behavior explicit and documented. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-160 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java (lines 58-58)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java

**Issue**

appendAckTs is a final field hardcoded to Instant.EPOCH in the constructor, and the Builder exposes no field or setter for it. Despite the comment "set post-append", there is no code path that can ever update it, so appendAckTs() always returns the epoch sentinel (1970-01-01). Ack timing is actually tracked outside the packet via RawTickWriter.AppendOutcome.ackTime(); any future consumer of appendAckTs() would silently get a wrong timestamp. Either remove this field/accessor or make it settable (e.g., via the Builder).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 58-58). The reviewer's guidance: appendAckTs is a final field hardcoded to Instant.EPOCH in the constructor, and the Builder exposes no field or setter for it. Despite the comment "set post-append", there is no code path that can ever update it, so appendAckTs() always returns the epoch sentinel (1970-01-01). Either remove this field/accessor or make it settable (e.g., via the Builder). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-161 — code/02_services/01_ingestion/src/main/resources/log4j2.xml (lines 40-41)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/resources/log4j2.xml

**Issue**

`HOST`, `ENV`, and `VM_ID` are declared as Log4j2 `<Property>` lookups but are never referenced by any appender/layout. `JsonLayout properties="true"` serializes only the ThreadContext (MDC) map — arbitrary configuration `<Property>` values are not emitted. Consequently the JSON file output does not contain `host`, `vm_id`, `environment` (nor `service`, `component`, `subsystem`, `correlation_id`, `trace_id`, `span_id`), even though the header comment states this file "Matches the OpenObserve contract", which requires those fields (docs/04_contracts/openobserve.md §F). Wire the values into the layout via `<KeyValuePair>` children and populate the correlation/trace/span keys into the MDC so emitted records satisfy the contract.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 40-41). The reviewer's guidance: `HOST`, `ENV`, and `VM_ID` are declared as Log4j2 `<Property>` lookups but are never referenced by any appender/layout. `JsonLayout properties="true"` serializes only the ThreadContext (MDC) map — arbitrary configuration `<Property>` values are not emitted. Consequently the JSON file output does not contain `host`, `vm_id`, `environment` (nor `service`, `component`, `subsystem`, `correlation_id`, `trace_id`, `span_id`), even though the header comment states this file "Matches the OpenObserve contract", which requires those fields (docs/04_contracts/openobserve.md §F). Wire the values into the layout via `<KeyValuePair>` children and populate the correlation/trace/span keys into the MDC so emitted records satisfy the contract. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-162 — code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java (lines 27-27)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java

**Issue**

This is an immutable value object intended for pipeline data exchange but it does not implement equals/hashCode (nor toString). If instances are ever placed in sets/maps or compared for equality (e.g., dedup by tick), default identity semantics will silently produce wrong results. Sibling value types in this module all provide value equality (ExchangeId, InstrumentToken implement equals/hashCode; MarketTick is a record). Consider implementing equals/hashCode based on exchange, instrumentToken, mode, exchangeTimestamp and lastTradedPrice.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-27). The reviewer's guidance: If instances are ever placed in sets/maps or compared for equality (e.g., dedup by tick), default identity semantics will silently produce wrong results. Consider implementing equals/hashCode based on exchange, instrumentToken, mode, exchangeTimestamp and lastTradedPrice. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-163 — code/common/src/main/java/com/trading/common/model/MarketTick.java (lines 30-30)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/model/MarketTick.java

**Issue**

`byte[] rawPayload` is a mutable array component in a record that is documented as the normalized immutable market tick. Records generate `equals()`/`hashCode()`/`toString()` using reference identity for array components, so two content-identical ticks will compare unequal, and the `rawPayload()` accessor returns the internal array directly, allowing callers to mutate the supposedly immutable payload. This diverges from the project convention in `RawTick`, which defensively clones on construction and access. Consider a compact constructor that clones the input and an accessor that returns a clone.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 30-30). The reviewer's guidance: Consider a compact constructor that clones the input and an accessor that returns a clone. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-164 — code/logs/ingestion.json (lines 1-1)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/logs/ingestion.json

**Issue**

This file is a generated runtime artifact, not source: log4j2.xml defines a `RollingFile` appender writing JSON lines to `${LOG_DIR}/ingestion.json` (LOG_DIR defaults to `logs/`), so this file is rewritten on every service run and grows unbounded. Committing it produces noisy diffs on each execution, repository bloat, and eventual merge conflicts. It should be excluded via `.gitignore` and removed from the changeset rather than tracked.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 1-1). The reviewer's guidance: It should be excluded via `.gitignore` and removed from the changeset rather than tracked. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-165 — code/run-ingestion-full.sh (lines 6-7)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/run-ingestion-full.sh

**Issue**

`REPO_ROOT` and `ARROW_INSTRUMENT_MANIFEST` are hard-coded to a specific user's absolute paths. `ARROW_INSTRUMENT_MANIFEST` in particular is exported unconditionally, overwriting any pre-set env value, so it cannot be overridden from outside — contradicting the script's otherwise env-overridable design. Derive `REPO_ROOT` from the script location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/..`) and allow env overrides (`${ARROW_INSTRUMENT_MANIFEST:-...}`), matching the pattern already used in `start-all.sh`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 6-7). The reviewer's guidance: `ARROW_INSTRUMENT_MANIFEST` in particular is exported unconditionally, overwriting any pre-set env value, so it cannot be overridden from outside — contradicting the script's otherwise env-overridable design. Derive `REPO_ROOT` from the script location (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-166 — code/smoke-test.sh (lines 5-5)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/smoke-test.sh

**Issue**

Hardcoded absolute path tied to this developer's machine makes the script non-portable (breaks for any other checkout location). Derive the directory from the script location instead, which also avoids the `cd "$DIR"` silently succeeding even when the script itself is run from elsewhere.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 5-5). The reviewer's guidance: Derive the directory from the script location instead, which also avoids the `cd "$DIR"` silently succeeding even when the script itself is run from elsewhere. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-167 — start-all.sh (lines 22-22)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- start-all.sh

**Issue**

PROJECT_ROOT and MANIFEST default to a hardcoded developer path (`/home/saurabh/Jupyter_notebook/...`, and the manifest filename even contains a space). For any other checkout location the script fails immediately before the pipeline starts, undermining the "ONE COMMAND to start" goal. Derive PROJECT_ROOT from the script's own location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`) and validate MANIFEST explicitly with a clear error instead of relying on an absolute user-specific default.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 22-22). The reviewer's guidance: Derive PROJECT_ROOT from the script's own location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`) and validate MANIFEST explicitly with a clear error instead of relying on an absolute user-specific default. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-168 — code/01_platform/02_sql/ddl/03_feature_candles_15s.sql (lines 27-29)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/01_platform/02_sql/ddl/03_feature_candles_15s.sql

**Issue**

The previous WITH clause carried the datalake/iceberg settings (`table.datalake.enabled`, `table.datalake.format`, `table.datalake.freshness`, `table.datalake.auto-compaction`) that backed the header's "Lake: EOD Iceberg offload" claim, but the new clause contains only bucket and retention settings. If these properties are not configured as cluster-wide defaults, the EOD Iceberg offload will not run for this table, and the 7-day retention will permanently delete candle data before it is offloaded. Please confirm cluster-level defaults exist or restore the per-table datalake settings.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-29). The reviewer's guidance: The previous WITH clause carried the datalake/iceberg settings (`table.datalake.enabled`, `table.datalake.format`, `table.datalake.freshness`, `table.datalake.auto-compaction`) that backed the header's "Lake: EOD Iceberg offload" claim, but the new clause contains only bucket and retention settings. Please confirm cluster-level defaults exist or restore the per-table datalake settings. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
R-010, R-011, R-054, R-231

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-169 — code/01_platform/04_scripts/soak-monitor.sh (lines 53-53)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/soak-monitor.sh

**Issue**

The `bridge_threads` column approximates goroutine count with the OS `Threads:` count from `/proc/<pid>/status`, but the Go runtime multiplexes goroutines over a small pool of OS threads — a goroutine leak usually does not increase the thread count, so a stable `bridge_threads` can hide a real goroutine explosion. This directly weakens the script's stated goal (the header says thread stability "implies no goroutine explosion"). Prefer sampling an explicit `runtime.NumGoroutine()` metric exposed by the bridge (e.g. a periodic health/telemetry event on stdout) rather than this proxy.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 53-53). The reviewer's guidance: Prefer sampling an explicit `runtime.NumGoroutine()` metric exposed by the bridge (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-017, R-018, R-174, R-236, R-237, R-238

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-170 — code/01_platform/04_scripts/soak-reconnect-loop.sh (lines 74-76)

**Status**

- [x] Done

**Priority**
Medium

**Affected Files**

- code/01_platform/04_scripts/soak-reconnect-loop.sh

**Issue**

The script's headline claim is to 'verify NO leak', and it dutifully records java_fds / bridge_fds / java_threads for every cycle, but it never analyzes them: there is no monotonic-increase detection, threshold, or trend check anywhere. recovered only checks 'Java alive + bridge present + ticks advanced'. A genuine FD/thread/socket leak would therefore pass the script unnoticed — the TSV would require post-hoc manual analysis. Add a per-cycle comparison (e.g., fail when the after-metrics exceed the first-cycle baseline by a margin, or a simple running regression) so the run itself detects the leak it is supposed to prove absent.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 74-76). The reviewer's guidance: Add a per-cycle comparison (e.g., fail when the after-metrics exceed the first-cycle baseline by a margin, or a simple running regression) so the run itself detects the leak it is supposed to prove absent. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-171 — code/02_services/01_ingestion/src/main/resources/log4j2.xml (lines 15-15)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Medium

**Affected Files**

- code/02_services/01_ingestion/src/main/resources/log4j2.xml

**Issue**

`LOG_DIR` defaults to the relative path `logs`. No launcher passes `-Dlog.dir`: the Docker entrypoint runs `java -cp /app/ingestion.jar ...` with no volume mount for logs, so in the container the JSON file lands in `/app/logs` (ephemeral writable layer, lost on container recreate); local runs resolve it relative to the working directory (`code/logs` — a `code/logs/ingestion.json` has even been committed to the repo). Note also that `start-all.sh` exports a `LOG_DIR` env var, but this config reads `sys:log.dir`, so that env var is silently ignored. Read the environment variable (and/or mount/point to an absolute path) so the JSON log is written to a durable, well-known location.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 15-15). The reviewer's guidance: No launcher passes `-Dlog.dir`: the Docker entrypoint runs `java -cp /app/ingestion.jar ...` with no volume mount for logs, so in the container the JSON file lands in `/app/logs` (ephemeral writable layer, lost on container recreate); local runs resolve it relative to the working directory (`code/logs` — a `code/logs/ingestion.json` has even been committed to the repo). Read the environment variable (and/or mount/point to an absolute path) so the JSON log is written to a durable, well-known location. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-172 — code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java (lines 25-25)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Medium

**Affected Files**

- code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java

**Issue**

Unit inconsistency in the same broker module: `ArrowOrderResponse.requestTime` is documented as epoch **ms**, while `fillTime` here is epoch **s**, and the Fills DDL column `broker_event_time BIGINT` carries no unit comment. When Action Capture persists `fillTime` and later compares/joins it against market-data timestamps (which the pipeline stores in epoch ms), any code that forgets the *1000 conversion will silently produce time-window errors (wrong fill ordering, misaligned candles/ranking). Either align the field to the platform's epoch-ms convention or make the seconds unit explicit at every consumption point and in the DDL comment.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 25-25). The reviewer's guidance: When Action Capture persists `fillTime` and later compares/joins it against market-data timestamps (which the pipeline stores in epoch ms), any code that forgets the *1000 conversion will silently produce time-window errors (wrong fill ordering, misaligned candles/ranking). Either align the field to the platform's epoch-ms convention or make the seconds unit explicit at every consumption point and in the DDL comment. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-173 — code/01_platform/04_scripts/soak-reconnect-loop.sh (lines 65-65)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/soak-reconnect-loop.sh

**Issue**

java_pid_before is captured once at startup and never refreshed, so every cycle's 'before' FD/thread readings use the same (potentially stale) PID. If the Java process is ever restarted mid-run (crash + supervisor, or the shutdown path triggered by the kill issue above), count_fds/threads_of on the dead PID return 0 — or worse, the PID could be recycled by an unrelated process — producing meaningless TSV rows. Also find_pid uses pgrep -f "$1" | tail -1, which can select the wrong process when more than one command line matches. Recompute the baseline PID each cycle (from java_pid_after) and verify it is non-empty before trusting the numbers.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 65-65). The reviewer's guidance: java_pid_before is captured once at startup and never refreshed, so every cycle's 'before' FD/thread readings use the same (potentially stale) PID. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-174 — code/01_platform/04_scripts/soak-headroom.sh (lines 60-60)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/soak-headroom.sh

**Issue**

`int(n*0.99)+1` overestimates p99 whenever `n*0.99` is an integer: for n=100 it yields idx=100 (the maximum), so "p99" degenerates to the max and the reported distribution is biased toward the cap (understating headroom). Use the nearest-rank definition, e.g. `idx = int(n*0.99 + 0.5)` with a lower clamp of 1.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 60-60). The reviewer's guidance: Use the nearest-rank definition, e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-019, R-020, R-021, R-057, R-137, R-151, R-169

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-175 — code/02_services/01_ingestion/go-bridge/main.go (lines 601-606)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/main.go

**Issue**

`envOrFatal` exits with status 1 for a missing required environment variable, but the file's own exit-status contract (`exitRequested=0, exitSupervisor=1, exitFatalStart=2`) assigns status 2 to fatal config/startup failures. A missing `ARROW_APP_ID`/`ARROW_APP_SECRET` is exactly such a startup config failure, so supervisors that distinguish config failures from runtime stream failures will misclassify it. Use `os.Exit(exitFatalStart)`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 601-606). The reviewer's guidance: Use `os.Exit(exitFatalStart)`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-097, R-185, R-186, R-187

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-176 — code/02_services/01_ingestion/go-bridge/main.go (lines 628-633)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/main.go

**Issue**

Instrument tokens are parsed with `strconv.Atoi` and narrowed to `int32` without range or sign validation. A token outside the int32 range silently wraps (e.g., `99999999999` → a different instrument) and negative values are accepted verbatim, so the bridge can subscribe to the wrong instruments or get an opaque rejection mid-subscription. Validate `0 <= n <= math.MaxInt32` (or use `strconv.ParseInt(..., 10, 32)` with a range check) and skip/reject invalid entries at the ingestion boundary.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 628-633). The reviewer's guidance: Instrument tokens are parsed with `strconv.Atoi` and narrowed to `int32` without range or sign validation. A token outside the int32 range silently wraps (e.g., `99999999999` → a different instrument) and negative values are accepted verbatim, so the bridge can subscribe to the wrong instruments or get an opaque rejection mid-subscription. Validate `0 <= n <= math.MaxInt32` (or use `strconv.ParseInt(..., 10, 32)` with a range check) and skip/reject invalid entries at the ingestion boundary. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-097, R-185, R-186, R-187

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-177 — code/02_services/01_ingestion/go-bridge/main.go (lines 202-202)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/main.go

**Issue**

The single-socket policy violation path emits `Event: "auth_failure"`, which the NDJSON contract reserves for credential failures. Alerting and telemetry keyed on `auth_failure` will fire (and the Java side may treat it as an auth terminal state) for a deployment/config error, masking the real cause. Use a distinct event type (e.g., `slot_state` with `SlotTerminal` and a `single_socket_policy_violation` reason) instead of `auth_failure`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 202-202). The reviewer's guidance: Use a distinct event type (e.g., `slot_state` with `SlotTerminal` and a `single_socket_policy_violation` reason) instead of `auth_failure`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-097, R-185, R-186, R-187

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-178 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java (lines 136-139)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java

**Issue**

`isFrameRecent()` treats the default value 0 of `lastFrameReceivedNanos` as a valid timestamp. `System.nanoTime()` has an arbitrary (boot-time) origin; if it is still within 15s of its origin (e.g. a freshly booted host or container), `System.nanoTime() - 0` is `< FRAME_STALE_TIMEOUT`, so the probe reports a 'recent frame' before any frame has ever arrived. The check should treat 0 as an explicit 'never received' sentinel and return false.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 136-139). The reviewer's guidance: The check should treat 0 as an explicit 'never received' sentinel and return false. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-179 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 75-76)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

The latency buffer never wraps: once 1024 samples are collected between flushes, every further recordAppendLatencyMs() call is silently dropped until the next flush resets latencyRingPos. Under high append rates the p50/p90/p99 window covers only the first 1024 samples of each 10s interval, biasing percentiles and hiding tail-latency spikes that occur later in the window. Size the buffer to cover the expected window or make it a true overwriting ring.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 75-76). The reviewer's guidance: The latency buffer never wraps: once 1024 samples are collected between flushes, every further recordAppendLatencyMs() call is silently dropped until the next flush resets latencyRingPos. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-180 — code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java (lines 96-104)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java

**Issue**

If `BufferedWriter`/`OutputStreamWriter` construction throws IOException, the accepted client `Socket` is never closed, leaking the open connection (no close in the catch block and the session was never added to `clients`). Also, the initial `ClientSession(client, null, ...)` with a null writer is a throwaway object only used to carry `connectedAt`. Close the socket on failure and simplify by constructing the writer directly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 96-104). The reviewer's guidance: If `BufferedWriter`/`OutputStreamWriter` construction throws IOException, the accepted client `Socket` is never closed, leaking the open connection (no close in the catch block and the session was never added to `clients`). Also, the initial `ClientSession(client, null, ...)` with a null writer is a throwaway object only used to carry `connectedAt`. Close the socket on failure and simplify by constructing the writer directly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-181 — code/common/invariants/LiveMoneyGuard.java (lines 181-182)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/invariants/LiveMoneyGuard.java

**Issue**

`evaluate` dereferences `facts` without a null check (`facts.triggered()` would throw an NPE at the call site). For a safety gate, failing fast with an explicit message via `Objects.requireNonNull(facts, "facts")` makes the cause of the failure clear.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 181-182). The reviewer's guidance: For a safety gate, failing fast with an explicit message via `Objects.requireNonNull(facts, "facts")` makes the cause of the failure clear. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-182 — code/common/src/main/java/com/trading/common/version/VersionGate.java (lines 48-48)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/version/VersionGate.java

**Issue**

requireAllPinned dereferences entries without a null check; a null list would throw a raw NullPointerException instead of a descriptive error in a safety-critical CI gate. Add a defensive null check before iterating.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 48-48). The reviewer's guidance: requireAllPinned dereferences entries without a null check; a null list would throw a raw NullPointerException instead of a descriptive error in a safety-critical CI gate. Add a defensive null check before iterating. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
R-268

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-183 — code/01_platform/01_docker/ddl-init.sh (lines 14-14)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/01_docker/ddl-init.sh

**Issue**

The `${COORDINATOR%%:*}` / `${COORDINATOR##*:}` split assumes exactly `host:port`. If the argument is passed without a port (e.g., `fluss-coordinator`), both expansions return the whole string and the probe path becomes `/dev/tcp/fluss-coordinator/fluss-coordinator`, so the wait can never succeed and the loop ends with a misleading timeout error; IPv6 addresses break the same way. Parse defensively (split on last `:`, then verify both host and a numeric port are non-empty) so a misconfigured argument fails fast with a clear message instead of hanging for MAX_WAIT.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 14-14). The reviewer's guidance: Parse defensively (split on last `:`, then verify both host and a numeric port are non-empty) so a misconfigured argument fails fast with a clear message instead of hanging for MAX_WAIT. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-184 — code/01_platform/02_sql/ddl/08_fills.sql (lines 34-36)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/08_fills.sql

**Issue**

The header requires "Retention: ≥3 complete trading days", but `table.retention.days = '3'` is a calendar-day setting in Fluss. Over a weekend or holiday, 3 calendar days can contain only 1–2 trading days, so audit records could be expired before the stated compliance floor is met. Use a calendar-day value that guarantees at least 3 complete trading days (e.g., 5), or document that retention is calendar-based and adjust the requirement accordingly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 34-36). The reviewer's guidance: The header requires "Retention: ≥3 complete trading days", but `table.retention.days = '3'` is a calendar-day setting in Fluss. Use a calendar-day value that guarantees at least 3 complete trading days (e.g., 5), or document that retention is calendar-based and adjust the requirement accordingly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-185 — code/02_services/01_ingestion/go-bridge/ndjson.go (lines 61-64)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/ndjson.go

**Issue**

The doc comment states the per-slot counter "resets when the process restarts (a new connection epoch begins)", but seqBySlot is keyed only by slotID and lives for the process lifetime. The reconnect loop (runReconnectLoop) advances the epoch on every reconnect without ever resetting this map, so after a reconnect `feed_sequence_local` continues from the previous epoch instead of restarting at 1. The emitted epoch on the line changes while the sequence does not — a consumer treating (connection_epoch, feed_sequence_local) as per-epoch counters would mis-detect gaps or mis-order ticks after a reconnect. Either key the counter by slotID+epoch (e.g. reset on first tick of a new epoch) or correct the comment to state that the sequence is process-lifetime and continuous across reconnects.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 61-64). The reviewer's guidance: The doc comment states the per-slot counter "resets when the process restarts (a new connection epoch begins)", but seqBySlot is keyed only by slotID and lives for the process lifetime. The reconnect loop (runReconnectLoop) advances the epoch on every reconnect without ever resetting this map, so after a reconnect `feed_sequence_local` continues from the previous epoch instead of restarting at 1. reset on first tick of a new epoch) or correct the comment to state that the sequence is process-lifetime and continuous across reconnects. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-023, R-058, R-059, R-175, R-176, R-177

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-186 — code/02_services/01_ingestion/go-bridge/ndjson.go (lines 94-99)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/ndjson.go

**Issue**

sha256Hex documents that it returns the SHA-256 hex digest of b but special-cases empty input to return "". Combined with `json:"payload_hash,omitempty"` (and the same on raw_payload), a tick whose raw payload is empty is emitted with neither field present. The Java PayloadHashValidator then classifies it as MALFORMED_HASH (payload_hash missing) and quarantines every such tick — so the integrity contract is silently skipped for empty payloads instead of being satisfied with the (well-defined) digest of empty bytes. If an empty payload genuinely indicates an upstream problem, that's the right moment to fail loudly; either always compute the digest (remove the special case) or make an empty payload an explicit emit error rather than silently dropping both integrity fields.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 94-99). The reviewer's guidance: If an empty payload genuinely indicates an upstream problem, that's the right moment to fail loudly; either always compute the digest (remove the special case) or make an empty payload an explicit emit error rather than silently dropping both integrity fields. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-023, R-058, R-059, R-175, R-176, R-177

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-187 — code/02_services/01_ingestion/go-bridge/ndjson.go (lines 135-137)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/ndjson.go

**Issue**

s = s[:512] truncates on a byte boundary and can split a multi-byte UTF-8 rune mid-sequence when a broker-supplied Reason exceeds 512 bytes. Go strings tolerate the invalid UTF-8, but json.Marshal will then replace the broken sequence with U+FFFD, corrupting the tail of the diagnostic message (e.g. CJK or emoji payloads). Impact is limited to cosmetic degradation of audit data, but the truncation should be rune-safe (e.g. loop back to a rune boundary or truncate on []rune) if multi-byte diagnostics are possible.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 135-137). The reviewer's guidance: Go strings tolerate the invalid UTF-8, but json.Marshal will then replace the broken sequence with U+FFFD, corrupting the tail of the diagnostic message (e.g. Impact is limited to cosmetic degradation of audit data, but the truncation should be rune-safe (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
R-023, R-058, R-059, R-175, R-176, R-177

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-188 — code/02_services/01_ingestion/go-bridge/subscription_plan.go (lines 44-45)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/subscription_plan.go

**Issue**

`BuildSubscriptionPlan` validates that the token list is non-empty and duplicate-free but never checks the token value domain. A zero or negative int32 token passes plan construction and is distributed into real `SubscribeHFTTokens` requests, failing only at subscription time (the broker rejects it and the slot enters a terminal state). Tokens originate from `ARROW_INSTRUMENT_TOKENS` (parsed via `strconv.Atoi` and narrowed with `int32(n)`, which can also silently wrap out-of-range values) or the instrument CSV, so the failure should be surfaced here at plan-build time in this fail-fast pipeline.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 44-45). The reviewer's guidance: `BuildSubscriptionPlan` validates that the token list is non-empty and duplicate-free but never checks the token value domain. A zero or negative int32 token passes plan construction and is distributed into real `SubscribeHFTTokens` requests, failing only at subscription time (the broker rejects it and the slot enters a terminal state). Tokens originate from `ARROW_INSTRUMENT_TOKENS` (parsed via `strconv.Atoi` and narrowed with `int32(n)`, which can also silently wrap out-of-range values) or the instrument CSV, so the failure should be surfaced here at plan-build time in this fail-fast pipeline. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-189 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go (lines 196-196)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go

**Issue**

`token` and `interval` are interpolated into the URL path with `fmt.Sprintf` without any path escaping (same applies to `GetInstrumentsCSV` with `segment`). The current pipeline feeds these from typed constants/manifests, but as a public SDK method the caller can pass values containing reserved characters (`/`, `?`, `#`, `%`), which would corrupt the endpoint or, if ever derived from untrusted input, enable path injection. Use `url.PathEscape` on each path segment.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 196-196). The reviewer's guidance: The current pipeline feeds these from typed constants/manifests, but as a public SDK method the caller can pass values containing reserved characters (`/`, `?`, `#`, `%`), which would corrupt the endpoint or, if ever derived from untrusted input, enable path injection. Use `url.PathEscape` on each path segment. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-190 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java (lines 180-184)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java

**Issue**

In `exceptionally`, `ex.getCause()` can be null when the future completes with a plain (non-CompletionException) exception, in which case the original exception is dropped entirely (`new RuntimeException("Fluss append failed", null)`). Since `RetryClassifier` walks the cause chain to distinguish FATAL (e.g., schema mismatch) from RETRYABLE failures, losing the root cause would silently downgrade fatal errors to retryable attempts. Wrap the received exception directly to preserve the chain: `throw new RuntimeException("Fluss append failed", ex)`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 180-184). The reviewer's guidance: Since `RetryClassifier` walks the cause chain to distinguish FATAL (e.g., schema mismatch) from RETRYABLE failures, losing the root cause would silently downgrade fatal errors to retryable attempts. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-191 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java (lines 57-63)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java

**Issue**

If `connection.getTable(path)` or `table.newAppend().createWriter()` throws during startup, the already-created `Connection` is never closed, leaking client sockets and background threads in a long-running ingestion service. Wrap the setup sequence in try/catch and close the connection on any failure before rethrowing.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 57-63). The reviewer's guidance: If `connection.getTable(path)` or `table.newAppend().createWriter()` throws during startup, the already-created `Connection` is never closed, leaking client sockets and background threads in a long-running ingestion service. Wrap the setup sequence in try/catch and close the connection on any failure before rethrowing. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-192 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 1063-1066)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

`readFdLimit()` reads `/proc/sys/fs/file-max`, which is the *system-wide* FD limit, not the process's per-process `RLIMIT_NOFILE` (available under `/proc/self/limits`). The FD-usage-percent gauge is therefore computed against the wrong denominator and will appear near 0% even as the process approaches its real FD cap, hiding genuine FD exhaustion. Parse `RLIMIT_NOFILE` from `/proc/self/limits` instead.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 1063-1066). The reviewer's guidance: Parse `RLIMIT_NOFILE` from `/proc/self/limits` instead. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-193 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java (lines 19-19)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java

**Issue**

The constructor rejects only `null` for `tradingSymbol`/`exchange`; empty or whitespace-only values pass validation even though these are documented as required routing fields. The manifest loader can produce an empty `tradingSymbol` when the CSV column is missing, so a blank routing symbol can reach lookups/routing logic. Consider rejecting blank strings (e.g., `isBlank()` check) or trimming plus validating non-empty in `build()`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 19-19). The reviewer's guidance: The constructor rejects only `null` for `tradingSymbol`/`exchange`; empty or whitespace-only values pass validation even though these are documented as required routing fields. Consider rejecting blank strings (e.g., `isBlank()` check) or trimming plus validating non-empty in `build()`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-194 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java (lines 150-150)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java

**Issue**

`escape()` only escapes backslash and double-quote. Control characters such as newline (`\n`), carriage return (`\r`), and tab (`\t`) are emitted verbatim, which can break the JSONL invariant (an embedded newline creates an extra physical line and malformed JSON). `instanceId` and `shutdownReason` are public API fields; if they ever carry multi-line text (e.g. an exception message) the journal becomes unparseable for resume-after-restart. Consider escaping control chars or using a proper JSON writer.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 150-150). The reviewer's guidance: `escape()` only escapes backslash and double-quote. `instanceId` and `shutdownReason` are public API fields; if they ever carry multi-line text (e.g. an exception message) the journal becomes unparseable for resume-after-restart. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-195 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java (lines 83-86)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java

**Issue**

The `halted` check at the top of `tryAccept` is a non-atomic check-then-act: a thread can pass `if (halted)` and then, before it increments the counters, another thread hits the 100% limit and sets `halted = true`. The first thread will still accept and return `true` for a record after the tracker is halted, violating the documented '100% → stop accepting broker data' contract and allowing the backlog to exceed the safety bound. The class documents itself as 'Thread-safe'; if concurrent producers are ever used (the current main-loop mutation is single-threaded, but the class contract claims otherwise), the halt check should be performed atomically with the capacity reservation (e.g., a synchronized block or CAS loop on the halted flag).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 83-86). The reviewer's guidance: The `halted` check at the top of `tryAccept` is a non-atomic check-then-act: a thread can pass `if (halted)` and then, before it increments the counters, another thread hits the 100% limit and sets `halted = true`. The class documents itself as 'Thread-safe'; if concurrent producers are ever used (the current main-loop mutation is single-threaded, but the class contract claims otherwise), the halt check should be performed atomically with the capacity reservation (e.g., a synchronized block or CAS loop on the halted flag). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-196 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java (lines 109-113)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java

**Issue**

In the warning branch the pluggable `BackpressureListener` is invoked synchronously while the pending counters are already incremented but before `totalAccepted.incrementAndGet()`. If a listener implementation throws a runtime exception (e.g., a logging/telemetry failure in a future listener), the exception propagates to the caller: `RawTickWriter.write()` will not submit the record, yet `pendingRecords`/`pendingBytes` remain incremented — a phantom pending entry that inflates backpressure and can trigger a spurious halt. Guard the listener invocation (catch-and-log) or document that listeners must not throw.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 109-113). The reviewer's guidance: Guard the listener invocation (catch-and-log) or document that listeners must not throw. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-197 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java (lines 60-60)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java

**Issue**

`disclosedQty` is not validated and can be negative. Arrow expects a non-negative disclosed quantity, so a negative value would only be rejected by the broker after construction. Add a `disclosedQty >= 0` check for fail-fast behavior.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 60-60). The reviewer's guidance: `disclosedQty` is not validated and can be negative. Arrow expects a non-negative disclosed quantity, so a negative value would only be rejected by the broker after construction. Add a `disclosedQty >= 0` check for fail-fast behavior. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-198 — code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java (lines 26-27)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java

**Issue**

`data.get("orderNo")` will throw a bare NPE if the caller passes a null map (e.g., when a JSON body of `null` is decoded). Add a null guard on `data` for a clearer failure mode, consistent with the explicit validation used for `orderNo`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 26-27). The reviewer's guidance: Add a null guard on `data` for a clearer failure mode, consistent with the explicit validation used for `orderNo`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-199 — code/common/src/main/java/com/trading/common/config/PlatformConfig.java (lines 53-53)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/config/PlatformConfig.java

**Issue**

When `containerMemoryLimitBytes <= 0` (e.g., an unreadable cgroup limit surfaced as 0, or a negative value from a misconfigured/test harness), `Math.floor(limit * 0.10)` yields <= 0 and `Math.min(67_108_864L, derived)` returns 0 or a negative value. If the result is used as a buffer/queue capacity or byte-array size, this can silently disable buffering or fail at allocation. Consider clamping to a safe floor (e.g., `Math.max(1L, derived)` or the original lower bound) and/or rejecting non-positive input explicitly.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 53-53). The reviewer's guidance: When `containerMemoryLimitBytes <= 0` (e.g., an unreadable cgroup limit surfaced as 0, or a negative value from a misconfigured/test harness), `Math.floor(limit * 0.10)` yields <= 0 and `Math.min(67_108_864L, derived)` returns 0 or a negative value. Consider clamping to a safe floor (e.g., `Math.max(1L, derived)` or the original lower bound) and/or rejecting non-positive input explicitly. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-200 — code/02_services/01_ingestion/go-bridge/supervisor.go (lines 117-120)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/supervisor.go

**Issue**

Each slot runs in a bare goroutine with no `recover`. A panic in one slot's main flow — e.g. a nil stream returned by `makeFactory(client, i)` with a nil error (the production factory's SDK dial can't return nil,nil today, but an injectable factory in the supervisor's contract can), an SDK panic during `SubscribeHFTTokens`, or an unexpected panic in `runHFTEpoch` — propagates out of this goroutine and crashes the whole bridge process, taking down healthy slots and the downstream Java consumer. Since the supervisor's stated purpose is to isolate slots and keep healthy peers alive during a peer's retry, adding a per-slot recover that converts a panic into a terminal outcome would preserve that isolation. Note this only covers the slot's main flow; the read/heartbeat/watchdog sub-goroutines spawned inside `runHFTEpoch` still cannot be recovered from here, but the slot's own panic path would no longer be fatal to peers.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 117-120). The reviewer's guidance: Each slot runs in a bare goroutine with no `recover`. A panic in one slot's main flow — e.g. a nil stream returned by `makeFactory(client, i)` with a nil error (the production factory's SDK dial can't return nil,nil today, but an injectable factory in the supervisor's contract can), an SDK panic during `SubscribeHFTTokens`, or an unexpected panic in `runHFTEpoch` — propagates out of this goroutine and crashes the whole bridge process, taking down healthy slots and the downstream Java consumer. Since the supervisor's stated purpose is to isolate slots and keep healthy peers alive during a peer's retry, adding a per-slot recover that converts a panic into a terminal outcome would preserve that isolation. Note this only covers the slot's main flow; the read/heartbeat/watchdog sub-goroutines spawned inside `runHFTEpoch` still cannot be recovered from here, but the slot's own panic path would no longer be fatal to peers. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-201 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go (lines 76-80)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go

**Issue**

When the API returns `data: null` (or `[]`) with status `success`, `json.Unmarshal` into `[]map[string]any` succeeds and the function returns a nil/empty slice with a nil error. A batch quote call that returned no quotes is therefore indistinguishable from a genuine server anomaly, and a caller that indexes `quotes[0]` without checking length would panic. Consider returning an explicit error for null data (or documenting the empty-result contract), since nothing in the current code lets callers detect this condition.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 76-80). The reviewer's guidance: Consider returning an explicit error for null data (or documenting the empty-result contract), since nothing in the current code lets callers detect this condition. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-202 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go (lines 108-111)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go

**Issue**

When the server replies `{"data": null, "status": "success"}`, `result.Data` is a nil map and is returned with a nil error. This diverges from the nil-handling convention used elsewhere in this same package (e.g., `GetBasketMargin` and `GetAllOptionChainSymbols` normalize nil `data` to empty containers); a caller that writes into the returned map (per that convention) would panic on a nil map. Normalize nil data to an empty map or return an error.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 108-111). The reviewer's guidance: When the server replies `{"data": null, "status": "success"}`, `result.Data` is a nil map and is returned with a nil error. This diverges from the nil-handling convention used elsewhere in this same package (e.g., `GetBasketMargin` and `GetAllOptionChainSymbols` normalize nil `data` to empty containers); a caller that writes into the returned map (per that convention) would panic on a nil map. Normalize nil data to an empty map or return an error. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-203 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go (lines 174-176)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go

**Issue**

parseQuote reuses parseLTPC, which computes NetChange from bytes 13:17 as if they were the close price. parseQuote then overwrites LTQ with those same bytes (13:17) and Close with bytes 45:49, but the NetChange recompute only runs when the new Close != 0. For quote/full ticks where the real close price (45:49) is 0 (e.g., newly listed or illiquid instruments), NetChange is left computed from LTQ, yielding a meaningless percentage. Compute NetChange only from the final Close and explicitly clear it when Close is 0.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 174-176). The reviewer's guidance: parseQuote reuses parseLTPC, which computes NetChange from bytes 13:17 as if they were the close price. parseQuote then overwrites LTQ with those same bytes (13:17) and Close with bytes 45:49, but the NetChange recompute only runs when the new Close != 0. For quote/full ticks where the real close price (45:49) is 0 (e.g., newly listed or illiquid instruments), NetChange is left computed from LTQ, yielding a meaningless percentage. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-204 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go (lines 106-112)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go

**Issue**

ReadTicks (and the analogous ReadUpdates) check ctx.Done() only at the top of the loop; the blocking conn.ReadMessage() has no read deadline, so canceling ctx while the socket is idle does not unblock the goroutine — it can only stop via socket close or incoming data. The standard-path caller currently closes the socket on shutdown (defer ds.Close()), so the bridge doesn't hang today, but any caller relying on ctx alone to stop the reader will leak the goroutine indefinitely. Consider a goroutine watching ctx.Done() that closes the connection, or set/refresh a read deadline so cancellation can interrupt the read.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 106-112). The reviewer's guidance: ReadTicks (and the analogous ReadUpdates) check ctx.Done() only at the top of the loop; the blocking conn.ReadMessage() has no read deadline, so canceling ctx while the socket is idle does not unblock the goroutine — it can only stop via socket close or incoming data. The standard-path caller currently closes the socket on shutdown (defer ds.Close()), so the bridge doesn't hang today, but any caller relying on ctx alone to stop the reader will leak the goroutine indefinitely. Consider a goroutine watching ctx.Done() that closes the connection, or set/refresh a read deadline so cancellation can interrupt the read. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-205 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java (lines 98-99)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java

**Issue**

Unlike the string fields (which are guarded by `text()` via `isNullAt`), the numeric fields `event_time` (11), `instrument_token` (4), `last_price_paise` (15), and `last_qty` (16) are read with `row.getLong(...)` directly. Although these columns are declared NOT NULL in the DDL, Fluss LOG tables do not guarantee runtime enforcement of nullability, and this tool is precisely for inspecting data quality — including malformed rows. A NULL in any of these columns would throw an NPE and terminate the polling loop, making the viewer unavailable exactly when it is needed most. Consider guarding numeric reads with `isNullAt` (similar to `text()`) and printing a placeholder.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 98-99). The reviewer's guidance: Unlike the string fields (which are guarded by `text()` via `isNullAt`), the numeric fields `event_time` (11), `instrument_token` (4), `last_price_paise` (15), and `last_qty` (16) are read with `row.getLong(...)` directly. A NULL in any of these columns would throw an NPE and terminate the polling loop, making the viewer unavailable exactly when it is needed most. Consider guarding numeric reads with `isNullAt` (similar to `text()`) and printing a placeholder. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-206 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEvent.java (lines 30-30)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEvent.java

**Issue**

The Go-side bridge contract (`validateBridgeEvent` in ndjson.go) requires `received_ts_ms > 0`, but this constructor validates every other field except `receivedTsMs`. Since `BridgeEventParser` defaults a missing `received_ts_ms` to 0 (and `asLong` can yield 0/negative values on malformed input), an event with a missing or non-positive receive timestamp would pass this validation gate and flow downstream as evidence. Mirror the bridge contract for defense in depth: add a `receivedTsMs <= 0` check alongside the `connectionEpoch` check.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 30-30). The reviewer's guidance: The Go-side bridge contract (`validateBridgeEvent` in ndjson.go) requires `received_ts_ms > 0`, but this constructor validates every other field except `receivedTsMs`. Since `BridgeEventParser` defaults a missing `received_ts_ms` to 0 (and `asLong` can yield 0/negative values on malformed input), an event with a missing or non-positive receive timestamp would pass this validation gate and flow downstream as evidence. Mirror the bridge contract for defense in depth: add a `receivedTsMs <= 0` check alongside the `connectionEpoch` check. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-207 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BrokerQuarantine.java (lines 31-34)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BrokerQuarantine.java

**Issue**

The record is documented as "Immutable" and its constructor validates `payloadHash` against `rawPayload`, but the `byte[]` is stored without a defensive copy. Because arrays are mutable, a caller can mutate `rawPayload` after construction (the accessor returns the same reference), which silently breaks the record's core invariant — the stored `payloadHash` would no longer match the payload bytes, and the mutated bytes could be persisted downstream (e.g., into `ingestion_quarantine`) without ever being re-validated. Take a defensive copy in the compact constructor so the hash invariant and documented immutability actually hold.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 31-34). The reviewer's guidance: The record is documented as "Immutable" and its constructor validates `payloadHash` against `rawPayload`, but the `byte[]` is stored without a defensive copy. Because arrays are mutable, a caller can mutate `rawPayload` after construction (the accessor returns the same reference), which silently breaks the record's core invariant — the stored `payloadHash` would no longer match the payload bytes, and the mutated bytes could be persisted downstream (e.g., into `ingestion_quarantine`) without ever being re-validated. Take a defensive copy in the compact constructor so the hash invariant and documented immutability actually hold. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-208 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/ReadinessFile.java (lines 20-20)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/ReadinessFile.java

**Issue**

`Files.move` with `ATOMIC_MOVE` throws `AtomicMoveNotSupportedException` on filesystems that do not support atomic moves (e.g., some NFS- or bind-mounted volumes in container deployments). Since this marker drives the container readiness probe, a single unsupported-move failure would leave the marker unwritten and the container permanently not-ready, while the caller in IngestionService only logs a warning. Consider falling back to a non-atomic move when the filesystem does not support it, keeping the tmp-then-rename pattern.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 20-20). The reviewer's guidance: Consider falling back to a non-atomic move when the filesystem does not support it, keeping the tmp-then-rename pattern. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-209 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java (lines 103-103)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java

**Issue**

The Builder defaults eventTime to Instant.EPOCH and the constructor never assigns null, so the `eventTime != null` guard here can never detect a missing/unset event time — a packet built without an explicit eventTime still evaluates as trade-eligible. Check for a real timestamp (e.g., eventTime.isAfter(Instant.EPOCH)) or document that EPOCH is a valid sentinel.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 103-103). The reviewer's guidance: The Builder defaults eventTime to Instant.EPOCH and the constructor never assigns null, so the `eventTime != null` guard here can never detect a missing/unset event time — a packet built without an explicit eventTime still evaluates as trade-eligible. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-210 — code/02_services/04_executor/main.py (lines 20-20)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/04_executor/main.py

**Issue**

The boolean env-var check is case-sensitive: values like `TRUE`, `True`, or `1` will silently evaluate to disabled. Since this flag gates real order execution, a typo like `EXECUTION_ENABLED=TRUE` would silently leave the executor disabled (or worse, a future implementation could silently disable safeguards). Normalize the value before comparing.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 20-20). The reviewer's guidance: Since this flag gates real order execution, a typo like `EXECUTION_ENABLED=TRUE` would silently leave the executor disabled (or worse, a future implementation could silently disable safeguards). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-211 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/.gitignore (lines 1-1)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/.gitignore

**Issue**

Ignoring `go.sum` in a Go module is generally discouraged: it holds cryptographic checksums that verify dependency integrity and enable reproducible builds (Go docs recommend committing it). `third_party/go-arrow` is a real Go module with its own dependencies (websocket, fasthttp, zerolog, etc.), and its `go.mod` is already committed. While the parent module `go-bridge/go.sum` captures the checksums for normal builds via the `replace` directive, this module's own checksums are not version-controlled — so if the SDK is ever built/tidied/tested on its own (e.g., running the SDK's own tests, or if the `replace` is later removed), `go mod` will regenerate and silently accept whatever the proxy returns, weakening supply-chain verification. Suggest removing the `go.sum` line (or documenting why it is intentionally ignored).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 1-1). The reviewer's guidance: Ignoring `go.sum` in a Go module is generally discouraged: it holds cryptographic checksums that verify dependency integrity and enable reproducible builds (Go docs recommend committing it). While the parent module `go-bridge/go.sum` captures the checksums for normal builds via the `replace` directive, this module's own checksums are not version-controlled — so if the SDK is ever built/tidied/tested on its own (e.g., running the SDK's own tests, or if the `replace` is later removed), `go mod` will regenerate and silently accept whatever the proxy returns, weakening supply-chain verification. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-212 — code/run-ingestion-full.sh (lines 27-28)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/run-ingestion-full.sh

**Issue**

`$SECRETS_FILE` holds `ARROW_APP_SECRET` and the TOTP key, but the script never verifies its permissions (it only mentions `chmod 600` in a comment). If the file was created under a permissive umask, other local users can read the credentials. Add a check that fails or warns unless the file is owner-only (e.g. `[ "$(stat -c '%a' "$SECRETS_FILE")" = "600" ]`).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-28). The reviewer's guidance: Add a check that fails or warns unless the file is owner-only (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-213 — code/02_services/01_ingestion/go-bridge/faketool/main.go (lines 100-101)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/faketool/main.go

**Issue**

A new zstd encoder (including its internal state/table initialization) is constructed and closed for every outbound frame inside the per-connection loop. Since `Encoder.EncodeAll` is safe for concurrent use, create one encoder per connection (or in `main`) and reuse it across frames; this removes repeated per-message allocation/init overhead that can make the fake broker's throughput/latency unrepresentative of the real one during the E2E test.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 100-101). The reviewer's guidance: A new zstd encoder (including its internal state/table initialization) is constructed and closed for every outbound frame inside the per-connection loop. Since `Encoder.EncodeAll` is safe for concurrent use, create one encoder per connection (or in `main`) and reuse it across frames; this removes repeated per-message allocation/init overhead that can make the fake broker's throughput/latency unrepresentative of the real one during the E2E test. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-214 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java (lines 28-29)

**Status**

- [x] Done (Phase 7, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java

**Issue**

Every NDJSON line is fully JSON-parsed here (`mapper.readTree`) and again in `parse()`, and then a third time by `MAPPER.readValue(line, GoTick.class)` in the caller — so each high-frequency tick is parsed 3 times, even though this parser only needs the `record_type` field to decide it is not a lifecycle/quarantine record. On the HFT ingestion hot path (ticks with large base64 `raw_payload`) this is significant avoidable overhead. Consider doing a cheap `record_type` pre-check (e.g. substring/lookup) before the full tree parse, or having `parse()`/`parseQuarantine()` share one parsed node/dispatch result so the caller can avoid re-parsing.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 28-29). The reviewer's guidance: Every NDJSON line is fully JSON-parsed here (`mapper.readTree`) and again in `parse()`, and then a third time by `MAPPER.readValue(line, GoTick.class)` in the caller — so each high-frequency tick is parsed 3 times, even though this parser only needs the `record_type` field to decide it is not a lifecycle/quarantine record. Consider doing a cheap `record_type` pre-check (e.g. substring/lookup) before the full tree parse, or having `parse()`/`parseQuarantine()` share one parsed node/dispatch result so the caller can avoid re-parsing. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green; PerfBaselineTest re-certified)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test green)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-215 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java (lines 38-39)

**Status**

- [x] Done (Phase 7, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java

**Issue**

`String.matches("[0-9a-f]{64}")` compiles a fresh regex Pattern on every invocation. This validator runs for every tick on the ingestion hot path (HFT feed can be thousands of records/sec), so the per-call compile adds avoidable allocation/GC pressure. Precompile the pattern once as a static field and use `Pattern.matcher(...).matches()` instead.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 38-39). The reviewer's guidance: Precompile the pattern once as a static field and use `Pattern.matcher(...).matches()` instead. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green; PerfBaselineTest re-certified)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test green)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-216 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java (lines 29-29)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java

**Issue**

The defensive clone in rawPayload() is taken multiple times per tick on the ingestion hot path: RawTickWriter.write() → rowConverter.estimatedRowSize(packet) calls raw.rawPayload().length (cloning the entire payload just to read its length), and RealFlussRowConverter.append() calls raw.rawPayload() again to build the Fluss row. At market-data rates each tick therefore allocates at least two full payload copies, adding avoidable GC pressure. Consider exposing a length-only accessor (e.g., rawPayloadLength()) for the size estimate, or document the copy cost so callers can reuse a single cached array.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 29-29). The reviewer's guidance: Consider exposing a length-only accessor (e.g., rawPayloadLength()) for the size estimate, or document the copy cost so callers can reuse a single cached array. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-217 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java (lines 96-97)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java

**Issue**

`Files.lines(journalPath).count()` opens an I/O-backed stream that is never closed — each call leaks a file descriptor. It also re-scans the entire journal file just to report the entry count, making repeated writes O(n^2). Use try-with-resources, or track the count in memory (e.g. an AtomicLong) instead of re-reading the file.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 96-97). The reviewer's guidance: `Files.lines(journalPath).count()` opens an I/O-backed stream that is never closed — each call leaks a file descriptor. Use try-with-resources, or track the count in memory (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-218 — code/common/src/main/java/com/trading/common/observability/Json.java (lines 70-70)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/Json.java

**Issue**

`String.format("\\u%04x", (int) c)` is invoked inside the per-character loop for every escaped control character, allocating a formatted String each time. For a low-level JSON builder likely used on a telemetry hot path, prefer writing the four hex digits directly (e.g. via a `"0123456789abcdef"` lookup) or appending to the `StringBuilder` with a small helper.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 70-70). The reviewer's guidance: `String.format("\\u%04x", (int) c)` is invoked inside the per-character loop for every escaped control character, allocating a formatted String each time. For a low-level JSON builder likely used on a telemetry hot path, prefer writing the four hex digits directly (e.g. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-219 — code/01_platform/02_sql/ddl/16_postback_quarantine.sql (lines 19-19)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/16_postback_quarantine.sql

**Issue**

`reason` and `disposition` are NOT NULL string columns but their allowed values are no longer documented or constrained. The previous DDL explicitly listed the vocabularies (e.g. MISSING_BROKER_ID/AMBIGUOUS_CORRELATION/NO_MATCHING_INSTRUCTION, OPEN/INVESTIGATING/RESOLVED/DISMISSED). Without this contract, the future Action Capture writer can emit inconsistent values that operators and downstream tooling cannot reliably interpret. Add a comment documenting the allowed `reason` and `disposition` enumerations (matching whatever the writer will emit).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 19-19). The reviewer's guidance: Add a comment documenting the allowed `reason` and `disposition` enumerations (matching whatever the writer will emit). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-220 — code/01_platform/04_scripts/ddl_apply.py (lines 234-234)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/ddl_apply.py

**Issue**

Dead assignment: `_existing_raw` is destructured from load_existing_manifest() but never referenced afterwards; the raw JSON string is only computed (read into memory) to be thrown away. Drop the unused second return value from load_existing_manifest() (or actually use it, e.g., to print the raw manifest on drift), to avoid confusing future maintainers.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 234-234). The reviewer's guidance: Drop the unused second return value from load_existing_manifest() (or actually use it, e.g., to print the raw manifest on drift), to avoid confusing future maintainers. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-221 — code/01_platform/04_scripts/digest-pin.sh (lines 30-31)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/digest-pin.sh

**Issue**

Parsing the resolver's JSON with `grep -o | head -1` is fragile: it depends on field ordering and exact quoting, and will silently pick the wrong digest if the tool changes its output layout or a non-sha256 digest appears first. For skopeo, prefer the structured `skopeo inspect --format '{{.Digest}}' docker://...`; for docker use `--verbose` and parse the `Descriptor.digest`. This makes extraction deterministic and self-documenting.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 30-31). The reviewer's guidance: For skopeo, prefer the structured `skopeo inspect --format '{{.Digest}}' docker://...`; for docker use `--verbose` and parse the `Descriptor.digest`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-222 — code/01_platform/04_scripts/digest-pin.sh (lines 26-26)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/digest-pin.sh

**Issue**

Gating the docker branch on `docker info &>/dev/null` requires a running daemon and can block on a daemon connection timeout. However, `docker manifest inspect` queries the registry directly and does not need the daemon, so this gate may skip a perfectly usable docker-based resolution or add latency. Consider checking only that the docker CLI is present (and applying a timeout if a daemon probe is truly needed).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 26-26). The reviewer's guidance: However, `docker manifest inspect` queries the registry directly and does not need the daemon, so this gate may skip a perfectly usable docker-based resolution or add latency. Consider checking only that the docker CLI is present (and applying a timeout if a daemon probe is truly needed). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-223 — code/01_platform/04_scripts/run-monday-gates.sh (lines 33-33)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/run-monday-gates.sh

**Issue**

On failure the script exits immediately after writing the per-suite FAIL line, so SUMMARY.txt lacks a terminal status marker equivalent to "ALL GATES PASSED". For automated parsers expecting a single decisive terminal line, append a `GATE RESULT: FAIL` marker before each `exit 1`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 33-33). The reviewer's guidance: For automated parsers expecting a single decisive terminal line, append a `GATE RESULT: FAIL` marker before each `exit 1`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-094, R-213, R-274

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-224 — code/02_services/01_ingestion/go-bridge/hft_slot.go (lines 174-179)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/hft_slot.go

**Issue**

The package-level helper `min(a, b int)` is never referenced anywhere in the go-bridge package (including the test file), so it is dead code. On this module's Go version (go 1.24.x per go.mod) it also shadows the built-in `min` and can produce type/behavior confusion. Remove it (or rename) rather than leaving an unused shadow of a standard builtin.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 174-179). The reviewer's guidance: Remove it (or rename) rather than leaving an unused shadow of a standard builtin. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-225 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java (lines 320-325)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java

**Issue**

Contract mismatch: the class javadoc promises 'missing required keys or out-of-range values throw IllegalStateException' and that production 'never falls back to ... guessed values', but exactInt() (and intRange/longRange/doubleRange) silently return plan defaults when a key is missing or blank, with no warning logged. A typo'd or renamed variable in the deployment (e.g. ARROW_HFT_MIN_ACTIVE_SLOTS, INGESTION_MAX_BATCH_RECORDS) would deploy with plan defaults and no observable signal, masking misconfiguration. Consider logging a warning on fallback (consistent with the required(env,key,fallback,errors) overload), and note DEPLOY_ENV also silently defaults to 'dev', which disables the production guards if a production deploy forgets to set it.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 320-325). The reviewer's guidance: Consider logging a warning on fallback (consistent with the required(env,key,fallback,errors) overload), and note DEPLOY_ENV also silently defaults to 'dev', which disables the production guards if a production deploy forgets to set it. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-226 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java (lines 306-313)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java

**Issue**

The errors parameter of this required-with-fallback overload is never used — it always logs a warning and returns the fallback instead of recording an error. Combined with the misleading 'required' name, this can mislead maintainers into thinking GO_ARROW_SDK_VERSION is mandatory and that validation failures are recorded through errors here. Either drop the unused parameter and rename the method to reflect fallback semantics, or record the missing-key case as an error to honor the 'required' contract.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 306-313). The reviewer's guidance: The errors parameter of this required-with-fallback overload is never used — it always logs a warning and returns the fallback instead of recording an error. Combined with the misleading 'required' name, this can mislead maintainers into thinking GO_ARROW_SDK_VERSION is mandatory and that validation failures are recorded through errors here. Either drop the unused parameter and rename the method to reflect fallback semantics, or record the missing-key case as an error to honor the 'required' contract. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-227 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java (lines 160-160)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java

**Issue**

The class Javadoc states "All typed fields are verified and normalized before construction", but build() performs no validation at all. A caller can construct a packet with null validity/raw, inconsistent OHLC (high < low), or negative prices, which contradicts the class contract and can cause NPEs (e.g., validity().name() in the Fluss row conversion) or malformed data downstream. Enforce the invariants in build() (or clearly document that verification is the caller's responsibility).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 160-160). The reviewer's guidance: A caller can construct a packet with null validity/raw, inconsistent OHLC (high < low), or negative prices, which contradicts the class contract and can cause NPEs (e.g., validity().name() in the Fluss row conversion) or malformed data downstream. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-228 — start-all.sh (lines 100-101)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- start-all.sh

**Issue**

The header claims builds happen only "if they're out of date", but the script always rebuilds both the Go bridge and the Java jar on every run. Also the default `MVN_FLAGS=-o` (offline) will fail on a fresh machine with an empty Maven local repository, and there are no `command -v` checks for java/go/mvn/docker/nc/awk. Consider a source-vs-artifact staleness check and a tool preflight so a first run fails with actionable messages instead of obscure build errors.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 100-101). The reviewer's guidance: Consider a source-vs-artifact staleness check and a tool preflight so a first run fails with actionable messages instead of obscure build errors. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-229 — code/.dockerignore (lines 9-9)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/.dockerignore

**Issue**

Since the build context is the reactor root `code/` (per the ingestion Dockerfile header: "build context MUST be the Maven reactor root code/"), the repository's `.git` directory sits above the context and this `.git/` pattern matches nothing — it is effectively dead config. The file's stated purpose is keeping credentials/history out of the build context, so consider `**/.git/` to also cover any nested `.git` directories (e.g., if a future `third_party` submodule is vendored with its own `.git`), or remove the line to avoid dead configuration.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 9-9). The reviewer's guidance: Since the build context is the reactor root `code/` (per the ingestion Dockerfile header: "build context MUST be the Maven reactor root code/"), the repository's `.git` directory sits above the context and this `.git/` pattern matches nothing — it is effectively dead config. The file's stated purpose is keeping credentials/history out of the build context, so consider `**/.git/` to also cover any nested `.git` directories (e.g., if a future `third_party` submodule is vendored with its own `.git`), or remove the line to avoid dead configuration. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-230 — code/01_platform/01_docker/docker-compose.yml (lines 123-124)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/01_docker/docker-compose.yml

**Issue**

`FLUSS_BOOTSTRAP_SERVERS` is never consumed: the Go bridge does not read it, and the Java client reads `FLUSS_BOOTSTRAP` (via `IngestionConfig`), with `FlussClientAdapter` deriving the Fluss `bootstrap.servers` from that same value. This duplicate key is dead configuration that can silently drift from the real bootstrap endpoint. Remove it (or actually use it in the client).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 123-124). The reviewer's guidance: This duplicate key is dead configuration that can silently drift from the real bootstrap endpoint. Remove it (or actually use it in the client). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-231 — code/01_platform/02_sql/ddl/02_raw_table_1.sql (lines 18-18)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/02_raw_table_1.sql

**Issue**

Option/derivative metadata columns `instrument_type`, `strike_paise`, `expiry`, and `option_type` are declared nullable but are never populated by the ingestion pipeline (`RealFlussRowConverter` writes empty string/null for every row, and neither TickPacket nor the Go bridge supplies these fields). The raw_table_1 LOG thus implies option metadata is captured when it is not. If no downstream producer will ever fill these columns, consider removing them to keep the schema honest, or document them as reserved for a later phase.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-18). The reviewer's guidance: If no downstream producer will ever fill these columns, consider removing them to keep the schema honest, or document them as reserved for a later phase. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-232 — code/01_platform/02_sql/ddl/10_positions.sql (lines 19-19)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/10_positions.sql

**Issue**

`current_quantity` is a derived value (open_quantity - closed_quantity). Persisting it as a separate NOT NULL column in the KV projection creates a consistency risk: any write path that updates only one of the three quantity fields will silently corrupt position state, and nothing in the schema enforces the invariant. Consider computing it at read time, or at minimum documenting/enforcing the invariant (e.g., a projector-level test that current_quantity == open_quantity - closed_quantity after every update).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 19-19). The reviewer's guidance: `current_quantity` is a derived value (open_quantity - closed_quantity). Consider computing it at read time, or at minimum documenting/enforcing the invariant (e.g., a projector-level test that current_quantity == open_quantity - closed_quantity after every update). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-233 — code/01_platform/02_sql/ddl/12_execution_attempts.sql (lines 6-6)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/12_execution_attempts.sql

**Issue**

The header declares `Scope: execution_partition_id, account_scope_id`, but the schema has no `account_scope_id` column. Sibling KV tables that claim account scoping (Execution_Gate, Portfolio_Reservations) materialize `account_scope_id` as a NOT NULL column. Without it, account-scoped queries, access control, and audit of attempts require a join through Execution_Gate, and the table cannot be filtered/partitioned by account on its own. Either add the `account_scope_id` column (consistent with Execution_Gate) or correct the scope comment to match the schema.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 6-6). The reviewer's guidance: Either add the `account_scope_id` column (consistent with Execution_Gate) or correct the scope comment to match the schema. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-234 — code/01_platform/02_sql/ddl/12_execution_attempts.sql (lines 22-22)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/12_execution_attempts.sql

**Issue**

This is a KV state table whose row is upserted as the attempt advances through phases (PREPARED → SUBMITTING → ACCEPTED/REJECTED/CANCELLED/UNKNOWN), yet only `prepared_ts` and `submitted_ts` exist — later terminal transitions have no recorded timestamp here, and there is no monotonic ordering column (e.g. `updated_ts`/`transition_version` as used in Portfolio_Reservations) to guard against stale-write overwrites if updates ever race. Consider adding a state-transition timestamp (and/or a version column) so the row's lifecycle is fully ordered and auditable.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 22-22). The reviewer's guidance: This is a KV state table whose row is upserted as the attempt advances through phases (PREPARED → SUBMITTING → ACCEPTED/REJECTED/CANCELLED/UNKNOWN), yet only `prepared_ts` and `submitted_ts` exist — later terminal transitions have no recorded timestamp here, and there is no monotonic ordering column (e.g. `updated_ts`/`transition_version` as used in Portfolio_Reservations) to guard against stale-write overwrites if updates ever race. Consider adding a state-transition timestamp (and/or a version column) so the row's lifecycle is fully ordered and auditable. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-235 — code/01_platform/02_sql/ddl/17_postback_projection_ledger.sql (lines 17-17)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/17_postback_projection_ledger.sql

**Issue**

Typo in the column name: `completeted_ts` should be `completed_ts`. Since this is a schema definition, the misspelled identifier will be baked into the Fluss table and can propagate into downstream projection/recovery code, making it awkward to fix later. Please rename it for consistency with the `*_ts` naming convention used in the other DDL files (e.g., `disposition_ts`, `quarantined_ts`).

**Implementation Plan**

Implement the corrective action described in the finding (report lines 17-17). The reviewer's guidance: Typo in the column name: `completeted_ts` should be `completed_ts`. Since this is a schema definition, the misspelled identifier will be baked into the Fluss table and can propagate into downstream projection/recovery code, making it awkward to fix later. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-236 — code/01_platform/04_scripts/soak-headroom.sh (lines 6-7)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/soak-headroom.sh

**Issue**

The header documents `capacity_used = acknowledged_tokens / assigned_tokens` and `headroom = 1 - capacity_used`, but the implementation never uses `assigned`: it computes `used = (acked*100)/cap` against the per-connection cap (1024) and only conditionally parses `assigned`. These two definitions only coincide when `assigned == cap`. Align the comment with the implementation (acked vs cap) or switch the calc to acked/assigned so the comment and the metric agree.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 6-7). The reviewer's guidance: The header documents `capacity_used = acknowledged_tokens / assigned_tokens` and `headroom = 1 - capacity_used`, but the implementation never uses `assigned`: it computes `used = (acked*100)/cap` against the per-connection cap (1024) and only conditionally parses `assigned`. Align the comment with the implementation (acked vs cap) or switch the calc to acked/assigned so the comment and the metric agree. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-019, R-020, R-021, R-057, R-137, R-151, R-169

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-237 — code/01_platform/04_scripts/soak-headroom.sh (lines 25-25)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/soak-headroom.sh

**Issue**

The default PROJECT_ROOT is a developer-specific absolute path (`/home/saurabh/...`), so on any other machine the script fails unless PROJECT_ROOT is exported manually. Derive it from the script location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`) or leave it unset and require the env var.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 25-25). The reviewer's guidance: Derive it from the script location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`) or leave it unset and require the env var. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-019, R-020, R-021, R-057, R-137, R-151, R-169

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-238 — code/01_platform/04_scripts/soak-headroom.sh (lines 27-27)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/soak-headroom.sh

**Issue**

OUT_DIR is defined but never used — no directory is created and no output file is written. Either implement saving the summary to `$OUT_DIR` or remove this dead configuration so it doesn't suggest the script persists artifacts.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-27). The reviewer's guidance: Either implement saving the summary to `$OUT_DIR` or remove this dead configuration so it doesn't suggest the script persists artifacts. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-019, R-020, R-021, R-057, R-137, R-151, R-169

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-239 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go (lines 99-100)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go

**Issue**

`Login()` has no return value and swallows authentication errors (only logs them), so callers cannot determine whether interactive login actually succeeded. Return an `error` (or bool) so automated flows can branch on failure instead of assuming success.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 99-100). The reviewer's guidance: `Login()` has no return value and swallows authentication errors (only logs them), so callers cannot determine whether interactive login actually succeeded. Return an `error` (or bool) so automated flows can branch on failure instead of assuming success. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-240 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/constants.go (lines 43-46)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/constants.go

**Issue**

The order-type constants define two different wire encodings for the same order type: `OrderTypeSL`/`OrderTypeSLM` ("SL"/"SL-M", labeled legacy) versus `OrderTypeSLLMT`/`OrderTypeSLMKT` ("SL-LMT"/"SL-MKT", labeled REST docs). These raw strings are sent to the broker verbatim, and the SDK's own OrderRequest/OrderDetails docs (orders.go) describe yet another vocabulary ("MARKET, LIMIT, SL, SL-M"). A future caller has no canonical value to use for stop-loss orders, so picking the wrong alias results in a rejected order or an unintended order mode. Consider keeping exactly one constant per supported order type (documenting the canonical wire value the REST API actually accepts) and deprecating/removing the rest.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 43-46). The reviewer's guidance: A future caller has no canonical value to use for stop-loss orders, so picking the wrong alias results in a rejected order or an unintended order mode. Consider keeping exactly one constant per supported order type (documenting the canonical wire value the REST API actually accepts) and deprecating/removing the rest. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-241 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/limits.go (lines 36-38)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/limits.go

**Issue**

The non-success error discards the actual API status, unlike the sibling SDK methods GetMargin/GetUserDetails which include `result.Status` in the error. When the broker returns e.g. `status: "error"` with a reason code, callers/operators only see a generic message with no diagnostic detail. Include the status value (and, where available, a message field) in the returned error.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 36-38). The reviewer's guidance: The non-success error discards the actual API status, unlike the sibling SDK methods GetMargin/GetUserDetails which include `result.Status` in the error. When the broker returns e.g. `status: "error"` with a reason code, callers/operators only see a generic message with no diagnostic detail. Include the status value (and, where available, a message field) in the returned error. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-242 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/margin.go (lines 69-71)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/margin.go

**Issue**

On a non-success response, the server-provided error details are lost: `MarginResponse` has no `Message`/`ErrorCode` field (unlike `OrderResponse` in orders.go), so the returned error only contains the bare status string. Consider adding a `Message`/`ErrorCode` field to `MarginResponse` and including it in the error to make production failures debuggable.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 69-71). The reviewer's guidance: Consider adding a `Message`/`ErrorCode` field to `MarginResponse` and including it in the error to make production failures debuggable. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-243 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go (lines 50-50)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go

**Issue**

`mode` (type `InfoQuoteMode`) is interpolated into the URL path without being validated against the three allowed constants (`ltp`, `full`, `ohlcv`). Since Go does not enforce a string-const set at compile time, any caller passing an unexpected value (e.g. `"../foo"`) silently builds a different request path. Add a switch that rejects unknown modes before constructing the endpoint so the exported API contract is enforced.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 50-50). The reviewer's guidance: `mode` (type `InfoQuoteMode`) is interpolated into the URL path without being validated against the three allowed constants (`ltp`, `full`, `ohlcv`). Since Go does not enforce a string-const set at compile time, any caller passing an unexpected value (e.g. Add a switch that rejects unknown modes before constructing the endpoint so the exported API contract is enforced. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-244 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java (lines 155-155)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java

**Issue**

`packet.validity().name().contains("NON_TRADE")` is a fragile substring match on an enum name. Prefer an explicit enum comparison (`packet.validity() == ValidityClassification.VALID_NON_TRADE`) so renaming/adding validity states cannot silently change tick classification. Note also that this classifies every non-quote state (INVALID_VALUES, INVALID_TIMESTAMP, DECODE_FAILURE, etc.) as "TRADE", which may mislabel the stored data.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 155-155). The reviewer's guidance: Prefer an explicit enum comparison (`packet.validity() == ValidityClassification.VALID_NON_TRADE`) so renaming/adding validity states cannot silently change tick classification. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-245 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 137-138)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

`metrics.setManifestVersion(1)` hardcodes the schema/manifest version, while `main()` already loads `manifestResult.version()` and never passes it into the service. When the instrument manifest version evolves, telemetry will keep reporting the stale literal `1`. Pass the actual manifest version into the service (constructor or setter) instead of hardcoding it.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 137-138). The reviewer's guidance: `metrics.setManifestVersion(1)` hardcodes the schema/manifest version, while `main()` already loads `manifestResult.version()` and never passes it into the service. Pass the actual manifest version into the service (constructor or setter) instead of hardcoding it. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-246 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java (lines 895-898)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

**Issue**

`updateReadinessFile()` is only invoked from `processBridgeEvent()`. The tick-processing path and the backpressure listener never refresh the readiness marker, so during a long healthy feed — or during silent degradation (append failures/backpressure) with no bridge lifecycle event — the marker stays at its last event-driven value and can report ready while the service is degraded. Refresh the marker periodically or from the tick/backpressure path as well.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 895-898). The reviewer's guidance: `updateReadinessFile()` is only invoked from `processBridgeEvent()`. The tick-processing path and the backpressure listener never refresh the readiness marker, so during a long healthy feed — or during silent degradation (append failures/backpressure) with no bridge lifecycle event — the marker stays at its last event-driven value and can report ready while the service is degraded. Refresh the marker periodically or from the tick/backpressure path as well. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-247 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java (lines 121-124)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java

**Issue**

manifestVersion is hardcoded to 1 in both the production CSV path and the synthetic fallback, so the version check inside isManifestApproved() is effectively vacuous — it can only ever match an expected version of 1, and any approved manifest version bump (e.g., from versions.pin / schema_manifest) would require this hardcoded value to change in lockstep. Consider deriving the version from the manifest source or configuration instead.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 121-124). The reviewer's guidance: Consider deriving the version from the manifest source or configuration instead. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-248 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java (lines 37-41)

**Status**

- [x] Done (Phase 7, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java

**Issue**

`Result[] out` is a fragile, undocumented out-parameter: the method dereferences `out[0]` in every branch without any precondition, so a future caller passing a null or zero-length array gets an NPE / ArrayIndexOutOfBoundsException instead of a classified result. The current callers always pass `new Result[1]`, but the contract should be enforced or, better, replaced with a small immutable result type (result + packet bytes) which also removes the per-tick array allocation on the ingestion hot path. At minimum, add a null/length guard and document the precondition.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 37-41). The reviewer's guidance: The current callers always pass `new Result[1]`, but the contract should be enforced or, better, replaced with a small immutable result type (result + packet bytes) which also removes the per-tick array allocation on the ingestion hot path. At minimum, add a null/length guard and document the precondition. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green; PerfBaselineTest re-certified)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test green)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-249 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java (lines 129-132)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java

**Issue**

The `after` parameter is documented as "first tick after gap (null if none yet)" and `note` as an operator note, but neither is ever persisted — `after` is dropped entirely and `note` only appears in an INFO log (the DDL has no after-gap or note columns). This makes the API contract misleading: callers passing after-gap snapshots or notes cannot tell the data is discarded. Either remove the unused parameters or add matching columns and persist them.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 129-132). The reviewer's guidance: Either remove the unused parameters or add matching columns and persist them. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-250 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java (lines 112-112)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java

**Issue**

`sha256()` hardcodes the literal `"SHA-256"`, while the class-level `ALGORITHM` constant is used only for `Result.algorithm` metadata. If the constant is ever changed (e.g., to SHA-512), the actual digest produced here and the algorithm reported in the result would silently diverge, corrupting the fingerprint metadata that downstream dedup depends on. Use the constant here so the two can never drift apart.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 112-112). The reviewer's guidance: Use the constant here so the two can never drift apart. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-251 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java (lines 149-152)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java

**Issue**

`diagnostics()` — intended as 'a human-readable readiness breakdown' — omits the telemetry readiness dimension (`otlpHealthy` / `isTelemetryReady()`), even though the field and setter exist and telemetry is a tracked readiness input for live-money release. Operators debugging why a readiness gate fails can't see the telemetry health in the same map. Add a `telemetry_ready` entry next to the other dimensions.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 149-152). The reviewer's guidance: `diagnostics()` — intended as 'a human-readable readiness breakdown' — omits the telemetry readiness dimension (`otlpHealthy` / `isTelemetryReady()`), even though the field and setter exist and telemetry is a tracked readiness input for live-money release. Add a `telemetry_ready` entry next to the other dimensions. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-252 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java (lines 60-60)

**Status**

- [x] Done (Phase 7, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java

**Issue**

The Builder never validates required inputs. If a caller passes null for rawPayload, the constructor's `builder.rawPayload.clone()` throws NPE at build() time; a null payloadHash makes toString() NPE via `payloadHash.substring(...)`. Additionally, because receiveTime defaults to Instant.EPOCH, a caller that forgets to set it silently produces a 1970-01-01 timestamp that downstream storage/ordering would treat as a real receive time. Since this is a public value class, consider failing fast in build() (e.g., requireNonNull for rawPayload/payloadHash and explicit receiveTime) so misconfiguration surfaces at the construction site rather than in unrelated paths such as logging.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 60-60). The reviewer's guidance: The Builder never validates required inputs. Additionally, because receiveTime defaults to Instant.EPOCH, a caller that forgets to set it silently produces a 1970-01-01 timestamp that downstream storage/ordering would treat as a real receive time. Since this is a public value class, consider failing fast in build() (e.g., requireNonNull for rawPayload/payloadHash and explicit receiveTime) so misconfiguration surfaces at the construction site rather than in unrelated paths such as logging. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green; PerfBaselineTest re-certified)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test green)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-253 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java (lines 167-171)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java

**Issue**

The Connection and Table created in the constructor are local variables that are never retained or closed; close() only calls writer.flush() and never releases the underlying Fluss connection. Since the connection object is dropped, the client resources can only be freed by process exit. This is fine for the single per-process instance used today, but if QuarantineWriter is ever recreated (reconnect/retry lifecycle), each instance leaks a coordinator connection. Retain the Connection/Table (or the writer's own close) and close it in close() to make the resource lifecycle explicit.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 167-171). The reviewer's guidance: The Connection and Table created in the constructor are local variables that are never retained or closed; close() only calls writer.flush() and never releases the underlying Fluss connection. Retain the Connection/Table (or the writer's own close) and close it in close() to make the resource lifecycle explicit. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-254 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java (lines 63-63)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java

**Issue**

BUCKET_COUNT = 8 is declared but never referenced anywhere in this class. It appears to be leftover from a table-creation path that was never implemented (the DDL in 21_ingestion_quarantine.sql already sets 'bucket.num' = '8'). This is dead code that can mislead maintainers into thinking the writer creates the table. Remove it unless it is actually used.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 63-63). The reviewer's guidance: It appears to be leftover from a table-creation path that was never implemented (the DDL in 21_ingestion_quarantine.sql already sets 'bucket.num' = '8'). Remove it unless it is actually used. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-255 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java (lines 105-107)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java

**Issue**

Dead code: `Instant now = Instant.now();` is declared but never used anywhere in this method (the row uses `detectedTsMs`). Remove it.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 105-107). The reviewer's guidance: Remove it. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-256 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java (lines 5-7)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java

**Issue**

Unused import `java.util.UUID` — it is not referenced anywhere in this class. Remove it.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 5-7). The reviewer's guidance: Remove it. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-257 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java (lines 3-3)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java

**Issue**

`java.io.BufferedWriter` is imported but never used anywhere in this class — dead import that should be removed.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 3-3). The reviewer's guidance: `java.io.BufferedWriter` is imported but never used anywhere in this class — dead import that should be removed. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-258 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 172-172)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

decodeReasonCounters is write-only: it is populated in incrementDecodeError() but never read, serialized, or emitted anywhere, so the documented "decode.errors by reason" breakdown never reaches the collector — dead state. It is also an unbounded ConcurrentHashMap keyed by arbitrary reason strings, so a long-running process can accumulate entries without limit. Either emit the per-reason counts in buildMetricsJson() or remove the map.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 172-172). The reviewer's guidance: Either emit the per-reason counts in buildMetricsJson() or remove the map. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-259 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java (lines 257-257)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

**Issue**

AppendOutcome.timeout(...) and Status.TIMEOUT are defined but never produced by write() — a timeout is deliberately reported as UNCERTAIN (the TimeoutException branch returns AppendOutcome.uncertain). The timeout() factory has no callers, and the caller's TIMEOUT branch is therefore unreachable. Remove the unused factory (and either drop TIMEOUT or use it consistently) to avoid semantic confusion between timeout and uncertainty.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 257-257). The reviewer's guidance: Remove the unused factory (and either drop TIMEOUT or use it consistently) to avoid semantic confusion between timeout and uncertainty. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-038, R-070, R-118, R-195, R-196, R-285

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-260 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java (lines 219-219)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

**Issue**

close() releases remaining pending bytes using an arbitrary average of 512 bytes/record ((int) (remaining * 512)). This magic number can over- or under-release the tracker's pendingBytes counter, causing byte accounting to drift from the actual pending capacity. Track per-record sizes for accurate release (e.g., keep a queue of outstanding row sizes), or document the approximation.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 219-219). The reviewer's guidance: close() releases remaining pending bytes using an arbitrary average of 512 bytes/record ((int) (remaining * 512)). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-038, R-070, R-118, R-195, R-196, R-285

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-261 — code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java (lines 30-30)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java

**Issue**

The class documents itself as a "Deterministic" workload, and the tests assert that two instances with the same seed produce identical tick sequences. However, the priority queue is ordered only by `Due.timeMs`. With 33/34 ms (PEAK) or 40–60 ms (BASELINE) intervals and modulo-1000 staggered offsets across many instruments, equal due timestamps are common, and `PriorityQueue` breaks ties arbitrarily (its ordering is not stable). The extraction order — and therefore the assigned `sequence` numbers — is thus not guaranteed reproducible across JVM implementations, which undermines the determinism guarantee for benchmarks. Add the instrument index as a secondary sort key to make ordering fully deterministic.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 30-30). The reviewer's guidance: With 33/34 ms (PEAK) or 40–60 ms (BASELINE) intervals and modulo-1000 staggered offsets across many instruments, equal due timestamps are common, and `PriorityQueue` breaks ties arbitrarily (its ordering is not stable). Add the instrument index as a secondary sort key to make ordering fully deterministic. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-262 — code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java (lines 29-29)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java

**Issue**

The 11-parameter positional constructor has three adjacent `long` fields (`fillQuantity`, `fillPrice`, `fillTime`); swapping any two at a call site compiles silently and corrupts data (e.g., quantity written as price). The rest of the codebase uses the Builder pattern for multi-field DTOs (`RawTick.Builder`, `TickPacket.Builder`). Consider introducing a builder (or at least named static factories) so arguments are bound to field names, making accidental reordering impossible.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 29-29). The reviewer's guidance: Consider introducing a builder (or at least named static factories) so arguments are bound to field names, making accidental reordering impossible. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-263 — code/common/src/main/java/com/trading/common/config/PlatformConfig.java (lines 25-25)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/config/PlatformConfig.java

**Issue**

`BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` duplicates `FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` added in the same change. These are load-bearing capacity values (also referenced by `FixedScope.maxSustainedTicksPerSec()`); keeping two independent copies of the same bound in separate classes creates a silent drift risk where one module tunes a value and another keeps the old bound. Single-source the value (e.g., have one class reference the other) or consolidate the constants.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 25-25). The reviewer's guidance: `BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` duplicates `FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` added in the same change. These are load-bearing capacity values (also referenced by `FixedScope.maxSustainedTicksPerSec()`); keeping two independent copies of the same bound in separate classes creates a silent drift risk where one module tunes a value and another keeps the old bound. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-264 — code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java (lines 37-38)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java

**Issue**

Attribute keys from `event.toAttributes()` are written to the JSON document without `escapeJson()` while only the values are escaped. The current keys are compile-time constants from `StructuredLogEvent.toAttributes()`, so the risk is low today, but escaping both keys and values uniformly would make the serializer robust against future dynamic keys.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 37-38). The reviewer's guidance: Attribute keys from `event.toAttributes()` are written to the JSON document without `escapeJson()` while only the values are escaped. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-265 — code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java (lines 18-18)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java

**Issue**

The public constant `TRADING_ALERTS_STREAM` is never referenced in this class — `emitAlert()` hardcodes the literal `"trading_alerts"` for the `stream` attribute instead. Using the constant here would keep the stream name in sync and avoid silent drift if it is ever renamed.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-18). The reviewer's guidance: The public constant `TRADING_ALERTS_STREAM` is never referenced in this class — `emitAlert()` hardcodes the literal `"trading_alerts"` for the `stream` attribute instead. Using the constant here would keep the stream name in sync and avoid silent drift if it is ever renamed. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-266 — code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java (lines 108-110)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java

**Issue**

`equals()`/`hashCode()` only consider timestampMs, level, service, message, and correlationId, while ignoring identity/correlation fields such as host, vm_id, trace_id, and span_id (plus all optional fields). Two distinct log records that differ only in host/trace/span would therefore compare as equal. If instances are ever stored in a Set or used as Map keys, log records could be silently deduplicated, breaking per-host/per-trace observability. If subset equality is intentional (e.g., correlation-based dedup), document it; otherwise include all fields in `equals`/`hashCode`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 108-110). The reviewer's guidance: `equals()`/`hashCode()` only consider timestampMs, level, service, message, and correlationId, while ignoring identity/correlation fields such as host, vm_id, trace_id, and span_id (plus all optional fields). If instances are ever stored in a Set or used as Map keys, log records could be silently deduplicated, breaking per-host/per-trace observability. If subset equality is intentional (e.g., correlation-based dedup), document it; otherwise include all fields in `equals`/`hashCode`. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-267 — code/common/src/main/java/com/trading/common/schema/SchemaManifestEntry.java (lines 23-23)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/schema/SchemaManifestEntry.java

**Issue**

`schemaState` is declared as a raw `String` even though the same package already defines a dedicated `SchemaState` enum (PROPOSED/APPROVED/APPLYING/OBSERVED/REJECTED) whose values exactly match the inline comment. The same applies to `compatibilityClass`, which duplicates the `CompatibilityClass` enum in `com.trading.common.version`. Using String re-implements vocabulary that already exists as strongly-typed enums, allowing invalid values to slip through without compile-time checks and risking drift between the DTO and the enum state machine (e.g., `SchemaState.isExecutableAuthority()` semantics). Jackson can bind JSON string values to enum types by name directly, so the fields can be typed with the existing enums without changing the wire format. Consider changing these fields to the enum types.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 23-23). The reviewer's guidance: `schemaState` is declared as a raw `String` even though the same package already defines a dedicated `SchemaState` enum (PROPOSED/APPROVED/APPLYING/OBSERVED/REJECTED) whose values exactly match the inline comment. Consider changing these fields to the enum types. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-268 — code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java (lines 27-32)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java

**Issue**

`isPlaceholder` compares the input against the constant *values*, but the class-level javadoc requires each of these constants to be replaced by a pinned, evidence-verified value before the live-money path is enabled. The moment any constant is replaced with a real version (e.g. `FLINK_VERSION_TO_BE_PINNED = "1.17.2"`), this method will start returning `true` for that correctly pinned value, and `VersionGate.isPinnedAndVerified` will reject a valid version — silently defeating the safety check it is meant to provide. The javadoc on this method says "equals its own sentinel name", but the implementation does not compare against the sentinel name/string literal; it is coupled to the (soon-to-be-mutated) constant values. Suggest checking against fixed sentinel literals (e.g. the name strings or a dedicated immutable sentinel set) that are independent of the pinning substitution.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-32). The reviewer's guidance: `isPlaceholder` compares the input against the constant *values*, but the class-level javadoc requires each of these constants to be replaced by a pinned, evidence-verified value before the live-money path is enabled. The moment any constant is replaced with a real version (e.g. `FLINK_VERSION_TO_BE_PINNED = "1.17.2"`), this method will start returning `true` for that correctly pinned value, and `VersionGate.isPinnedAndVerified` will reject a valid version — silently defeating the safety check it is meant to provide. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-269 — code/common/workitem/WorkItem.java (lines 33-34)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/common/workitem/WorkItem.java

**Issue**

The spec states a blocked item "must record the owner, missing evidence, and unblock condition" (see WorkItemState javadoc). `transitionTo` doesn't enforce this: it permits entering BLOCKED with all three fields null, and also allows BLOCKED -> BLOCKED re-blocking while silently dropping the prior context. Consider validating that owner/missingEvidence/unblockCondition are non-blank when `next == BLOCKED`, and guarding against re-entering BLOCKED from an already-BLOCKED item if that isn't intended.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 33-34). The reviewer's guidance: The spec states a blocked item "must record the owner, missing evidence, and unblock condition" (see WorkItemState javadoc). Consider validating that owner/missingEvidence/unblockCondition are non-blank when `next == BLOCKED`, and guarding against re-entering BLOCKED from an already-BLOCKED item if that isn't intended. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-270 — code/pom.xml (lines 38-38)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/pom.xml

**Issue**

The comment says "Flink (provided scope — runtime provides these)", but these managed dependencies declare no `<scope>provided</scope>`. In `dependencyManagement`, omitting scope means children inherit compile scope, which would bundle Flink into fat jars and cause classloader conflicts at runtime. Today there is no functional break because neither reactor module (common/ingestion) depends on Flink and the out-of-reactor compute module redeclares `provided` explicitly — but the comment/declaration mismatch is misleading and a future child relying on the managed version will silently get compile scope. Either add `<scope>provided</scope>` to the three Flink entries (matching the comment) or fix the comment.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 38-38). The reviewer's guidance: In `dependencyManagement`, omitting scope means children inherit compile scope, which would bundle Flink into fat jars and cause classloader conflicts at runtime. Today there is no functional break because neither reactor module (common/ingestion) depends on Flink and the out-of-reactor compute module redeclares `provided` explicitly — but the comment/declaration mismatch is misleading and a future child relying on the managed version will silently get compile scope. Either add `<scope>provided</scope>` to the three Flink entries (matching the comment) or fix the comment. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-271 — code/pom.xml (lines 27-27)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/pom.xml

**Issue**

`scala.binary.version` is declared but never referenced by any dependency or plugin in this POM or its children (all managed Flink artifacts are non-Scala Java artifacts). This dead property misleads maintainers into thinking Scala-specific artifacts are managed here; remove it or use it consistently.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 27-27). The reviewer's guidance: This dead property misleads maintainers into thinking Scala-specific artifacts are managed here; remove it or use it consistently. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-272 — code/pom.xml (lines 15-18)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/pom.xml

**Issue**

This aggregator only lists `common` and `02_services/01_ingestion`, but `02_services/02_compute`, `02_services/03_action_capture`, and `02_services/05_mock_arrow` also declare `com.trading:trading-platform` as their parent. They are silently excluded from a root `mvn package` reactor build (and building them standalone requires the parent to be installed first). If the exclusion is intentional (ingestion-only scope), document it in the module comment; otherwise add them to `<modules>` so the root build covers the whole platform.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 15-18). The reviewer's guidance: If the exclusion is intentional (ingestion-only scope), document it in the module comment; otherwise add them to `<modules>` so the root build covers the whole platform. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-273 — show-ticks.sh (lines 18-18)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- show-ticks.sh

**Issue**

The pre-flight check hardcodes 127.0.0.1:9123, but the launched Java program honors the FLUSS_BOOTSTRAP env var (defaults to localhost:9123, see TickTableViewer). If a user overrides FLUSS_BOOTSTRAP to a different host/port, this check will either falsely reject a healthy remote cluster, or pass against a local server while the viewer actually connects elsewhere. Consider deriving the probe target from FLUSS_BOOTSTRAP (falling back to 127.0.0.1:9123), or documenting that this wrapper only supports the local cluster.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 18-18). The reviewer's guidance: If a user overrides FLUSS_BOOTSTRAP to a different host/port, this check will either falsely reject a healthy remote cluster, or pass against a local server while the viewer actually connects elsewhere. Consider deriving the probe target from FLUSS_BOOTSTRAP (falling back to 127.0.0.1:9123), or documenting that this wrapper only supports the local cluster. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
None.

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-274 — code/02_services/01_ingestion/go-bridge/faketool/main.go (lines 7-7)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/faketool/main.go

**Issue**

The documented invocation does not match the program: `FAKE_HFT_PORT` / `FAKE_HFT_DISCONNECT_AFTER` environment variables are never read (the tool only defines `-port` and `-disconnect-after` flags), and the file is `main.go`, not `faketool.go`. A developer following this usage line will get `go: no such file 'faketool.go'` or, if they rename/point at the file, silently won't get the intended forced-disconnect behavior. Update the usage comment to e.g. `go run -tags faketool main.go -port 8899 -disconnect-after 1`.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 7-7). The reviewer's guidance: A developer following this usage line will get `go: no such file 'faketool.go'` or, if they rename/point at the file, silently won't get the intended forced-disconnect behavior. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-275 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java (lines 17-18)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

**Issue**

The class javadoc claims 'Fluss infers the column schema from the first AppendWriter.append(GenericRow) call, so we only need table existence + bucket distribution', but the code actually creates explicit in-code schemas and verifyTables() strictly compares column counts. If Fluss truly inferred the schema, this column-count check would be meaningless; if it doesn't, the 7-column MINIMAL_SCHEMA is insufficient for the DDL-defined tables. The comment contradicts the implementation — clarify the actual Fluss behavior and make the schemas consistent across the DDL, writers, and this bootstrap.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 17-18). The reviewer's guidance: The class javadoc claims 'Fluss infers the column schema from the first AppendWriter.append(GenericRow) call, so we only need table existence + bucket distribution', but the code actually creates explicit in-code schemas and verifyTables() strictly compares column counts. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-107, R-190, R-191, R-244

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-276 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java (lines 4-4)

**Status**

- [x] Done (Phase 7, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java

**Issue**

The `java.util.Arrays` import is never referenced anywhere in this class. Remove it to avoid dead code and potential lint/checkstyle failures.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 4-4). The reviewer's guidance: Remove it to avoid dead code and potential lint/checkstyle failures. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green; PerfBaselineTest re-certified)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test green)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-277 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java (lines 35-35)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java

**Issue**

The javadoc's column mapping references "16_postback_quarantine.sql" and documents an 18-column schema (postback_event_id, broker_order_id, client_order_ref, broker_status, broker_timestamp, status, resolution_ts, ...). The table this class actually writes is ingestion_quarantine, created by 21_ingestion_quarantine.sql, which has only 10 columns (and the GenericRow construction correctly matches it). The stale javadoc will mislead maintainers verifying the row layout against the real DDL. Update it to reference 21_ingestion_quarantine.sql with the actual 10-column mapping, and remove the duplicated bs() javadoc line below.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 35-35). The reviewer's guidance: Update it to reference 21_ingestion_quarantine.sql with the actual 10-column mapping, and remove the duplicated bs() javadoc line below. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-003, R-006, R-007, R-008, R-154, R-275

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-278 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java (lines 15-15)

**Status**

- [x] Done (Phase 3, 2026-08-02)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

**Issue**

`DoubleAdder` is imported and the class javadoc states gauges use DoubleAdder, but no DoubleAdder is ever used — all gauges are volatile primitives and counters are AtomicLong. Remove the unused import and align the javadoc so readers aren't misled about the metric semantics.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 15-15). The reviewer's guidance: `DoubleAdder` is imported and the class javadoc states gauges use DoubleAdder, but no DoubleAdder is ever used — all gauges are volatile primitives and counters are AtomicLong. Remove the unused import and align the javadoc so readers aren't misled about the metric semantics. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 177 run, 0 fail, 5 env-gated skips)

**Dependencies**
R-108, R-109, R-110, R-111, R-140, R-192, R-245, R-246

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-279 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java (lines 157-157)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

**Issue**

The class and write() Javadoc say 'Retry with linear backoff', but the implementation doubles the delay each attempt (100ms, 200ms, 400ms — BASE_RETRY_BACKOFF_MS * (1L << (attempt - 1))), i.e., exponential backoff. Align the documentation with the actual behavior (or the code with the documentation) so operators tuning retry parameters aren't misled.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 157-157). The reviewer's guidance: Align the documentation with the actual behavior (or the code with the documentation) so operators tuning retry parameters aren't misled. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-038, R-070, R-118, R-195, R-196, R-285

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-280 — code/01_platform/02_sql/ddl/10_positions.sql (lines 31-31)

**Status**

- [x] Done (Phase 6, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/01_platform/02_sql/ddl/10_positions.sql

**Issue**

`table.retention.days = '7'` on a 'current state' projection means a position that remains open without any source event (fill/postback) for more than 7 days may be expired from the KV store and disappear from the current-state view. Other current-state KV tables in this project use longer retention (instruments: 90, execution_gate: 30). If positions can span more than a few days, or if the Fills-audit rebuild depends on this row still existing, confirm this retention is intentional; otherwise extend it to match the intended position lifecycle.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 31-31). The reviewer's guidance: Other current-state KV tables in this project use longer retention (instruments: 90, execution_gate: 30). Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (SchemaAgreementTest guard + DDL sweep; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 220 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

DDL: verify any option against the pinned Fluss 0.9.1-incubating property set before applying; coordinate with the offline DDL gate (`ddl_apply.py`). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-281 — code/01_platform/04_scripts/run-monday-gates.sh (lines 32-32)

**Status**

- [x] Done

**Priority**
Low

**Affected Files**

- code/01_platform/04_scripts/run-monday-gates.sh

**Issue**

Neither the Go suite nor the Java suite is wrapped with a timeout. The E2E/FLUSS integration tests can hang indefinitely (e.g., Fluss down, stuck JVM, or offline-mode dependency resolution), which would block a CI/cron run forever without a decisive result. Wrap each suite with `timeout` to bound the run.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 32-32). The reviewer's guidance: Neither the Go suite nor the Java suite is wrapped with a timeout. The E2E/FLUSS integration tests can hang indefinitely (e.g., Fluss down, stuck JVM, or offline-mode dependency resolution), which would block a CI/cron run forever without a decisive result. Wrap each suite with `timeout` to bound the run. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit or integration test, or manual verification where no test harness exists)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass

**Dependencies**
R-094, R-213, R-274

**Agent Notes**

Shell: validate with `bash -n <script>`; keep `set -euo pipefail` semantics and env-overridable config convention. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-282 — code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go (lines 59-61)

**Status**

- [x] Done (Phase 4, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go

**Issue**

When the API returns a 200 with `status != "success"` (an application-level error), the server's `errorMessage`/`errorCode` fields in the response body are discarded and only the status string is surfaced (this repeats across all six methods in this file). `c.request`/`rawRequestAuth` only return the body for HTTP >= 400, so these application-level failures are hard to diagnose (e.g. invalid token, unavailable Greeks, bad segment). Include the error body or an `errorMessage` field in the returned error for diagnosability.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 59-61). The reviewer's guidance: When the API returns a 200 with `status != "success"` (an application-level error), the server's `errorMessage`/`errorCode` fields in the response body are discarded and only the status string is surfaced (this repeats across all six methods in this file). `c.request`/`rawRequestAuth` only return the body for HTTP >= 400, so these application-level failures are hard to diagnose (e.g. invalid token, unavailable Greeks, bad segment). Include the error body or an `errorMessage` field in the returned error for diagnosability. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; `go test -race` suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (go test -race ./... ok)

**Dependencies**
None.

**Agent Notes**

Go module: run `go test ./...` from `code/02_services/01_ingestion/go-bridge`; run `go vet ./...` if available. Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-283 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java (lines 80-80)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java

**Issue**

Files.newBufferedReader(Path.of(path)) on Java 17 (the project targets release 17 and the launcher pins Java 17) decodes using the platform default charset and does not strip a UTF-8 BOM. If the Arrow CSV (e.g., an Excel-exported NSE_CM_EQUITY.csv) is saved with a UTF-8 BOM, or the host locale is non-UTF-8, header/field parsing can be corrupted and a valid manifest rejected. Suggest explicitly using StandardCharsets.UTF_8 and stripping a leading BOM from the first line.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 80-80). The reviewer's guidance: Files.newBufferedReader(Path.of(path)) on Java 17 (the project targets release 17 and the launcher pins Java 17) decodes using the platform default charset and does not strip a UTF-8 BOM. If the Arrow CSV (e.g., an Excel-exported NSE_CM_EQUITY.csv) is saved with a UTF-8 BOM, or the host locale is non-UTF-8, header/field parsing can be corrupted and a valid manifest rejected. Suggest explicitly using StandardCharsets.UTF_8 and stripping a leading BOM from the first line. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-284 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java (lines 35-35)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java

**Issue**

`Integer.parseInt(args[0])` is unguarded: a non-numeric first argument (e.g. a typo) throws an uncaught `NumberFormatException` with no usage guidance, and `limit <= 0` only catches the negative/zero case, not invalid input. For a developer-facing CLI utility, wrapping the parse and printing a short usage message would make the failure mode much clearer.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 35-35). The reviewer's guidance: `Integer.parseInt(args[0])` is unguarded: a non-numeric first argument (e.g. For a developer-facing CLI utility, wrapping the parse and printing a short usage message would make the failure mode much clearer. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-285 — code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java (lines 96-97)

**Status**

- [x] Done (Phase 8 final gate, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java

**Issue**

Unclassified exceptions default to RETRYABLE. The class javadoc states that fatal failures should "halt the append path, open the safety gate", and by the time RawTickWriter invokes this classifier the Fluss client's built-in retries have already been exhausted. For an unknown/unexpected exception, a fail-open default of RETRYABLE means the pipeline will retry and then drop the record as FAILED without ever opening the halt gate, silently masking systemic issues. Consider a fail-safe default of FATAL (or make the default configurable) so unknowns surface through the safety gate.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 96-97). The reviewer's guidance: The class javadoc states that fatal failures should "halt the append path, open the safety gate", and by the time RawTickWriter invokes this classifier the Fluss client's built-in retries have already been exhausted. Consider a fail-safe default of FATAL (or make the default configurable) so unknowns surface through the safety gate. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test BUILD SUCCESS)

**Dependencies**
R-037, R-068, R-069, R-259, R-260, R-279

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

### R-286 — code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java (lines 40-40)

**Status**

- [x] Done (Phase 5, 2026-08-03)

**Priority**
Low

**Affected Files**

- code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java

**Issue**

The fail-closed guard compares the raw env var without trimming. Values such as `' false '` or `'false\n'` (common when the variable is exported from config files with trailing whitespace) would trigger the `IllegalStateException` and crash startup even though the effective value is `false`. Trim the value before the comparison to keep the guard robust.

**Implementation Plan**

Implement the corrective action described in the finding (report lines 40-40). The reviewer's guidance: The fail-closed guard compares the raw env var without trimming. Trim the value before the comparison to keep the guard robust. Concretely: read the referenced source, apply the minimal change that resolves the defect, and add or update a focused unit/integration test where one exists for the affected code path. Preserve the surrounding API and behavior unless the finding explicitly requires a contract change. Verify with the relevant build/test command for the module (Maven reactor for Java, `go test ./...` for the Go bridge, `bash -n` for shell scripts).

**Acceptance Criteria**

- [x] The defect described in the finding is resolved
- [x] Affected behavior/path verified (unit/integration test added; full reactor suite green)
- [x] No new review issue introduced by the change
- [x] Existing related tests pass (mvn -o test: 213 run, 0 fail)

**Dependencies**
None.

**Agent Notes**

Java: build/test via `mvn -o test -pl 02_services/01_ingestion -am` from `code/` (or `make test`); keep the module's existing style (records, builders, fail-fast validation). Follow the project's existing conventions; do not change public API signatures unless the finding requires it. Update related tests.

---

## File Index

- `Makefile`
  - R-082
  - R-143
- `code/.dockerignore`
  - R-229
- `code/01_platform/01_docker/ddl-init.sh`
  - R-083
  - R-183
- `code/01_platform/01_docker/docker-compose.yml`
  - R-009
  - R-230
- `code/01_platform/02_sql/ddl/02_raw_table_1.sql`
  - R-010
  - R-011
  - R-054
  - R-231
- `code/01_platform/02_sql/ddl/03_feature_candles_15s.sql`
  - R-012
  - R-055
  - R-168
- `code/01_platform/02_sql/ddl/05_signal_candidates.sql`
  - R-084
- `code/01_platform/02_sql/ddl/06_ranking_results.sql`
  - R-136
- `code/01_platform/02_sql/ddl/08_fills.sql`
  - R-085
  - R-184
- `code/01_platform/02_sql/ddl/09_order_lifecycle.sql`
  - R-013
  - R-144
- `code/01_platform/02_sql/ddl/10_positions.sql`
  - R-232
  - R-280
- `code/01_platform/02_sql/ddl/12_execution_attempts.sql`
  - R-233
  - R-234
- `code/01_platform/02_sql/ddl/13_order_correlation.sql`
  - R-086
  - R-145
- `code/01_platform/02_sql/ddl/14_execution_audit.sql`
  - R-087
- `code/01_platform/02_sql/ddl/16_postback_quarantine.sql`
  - R-088
  - R-146
  - R-219
- `code/01_platform/02_sql/ddl/17_postback_projection_ledger.sql`
  - R-235
- `code/01_platform/02_sql/ddl/18_safety_halt_requests.sql`
  - R-089
- `code/01_platform/02_sql/ddl/20_instruments.sql`
  - R-090
- `code/01_platform/04_scripts/cep_guard.sh`
  - R-091
- `code/01_platform/04_scripts/ddl_apply.py`
  - R-014
  - R-147
  - R-220
- `code/01_platform/04_scripts/digest-pin.sh`
  - R-015
  - R-056
  - R-148
  - R-221
  - R-222
- `code/01_platform/04_scripts/run-monday-gates.sh`
  - R-016
  - R-149
  - R-150
  - R-223
  - R-281
- `code/01_platform/04_scripts/soak-headroom.sh`
  - R-017
  - R-018
  - R-174
  - R-236
  - R-237
  - R-238
- `code/01_platform/04_scripts/soak-monitor.sh`
  - R-019
  - R-020
  - R-021
  - R-057
  - R-137
  - R-151
  - R-169
- `code/01_platform/04_scripts/soak-reconnect-loop.sh`
  - R-001
  - R-004
  - R-173
  - R-170
- `code/01_platform/04_scripts/version_matrix_verify.py`
  - R-092
  - R-093
- `code/02_services/01_ingestion/.dockerignore`
  - R-152
- `code/02_services/01_ingestion/Dockerfile`
  - R-002
  - R-005
- `code/02_services/01_ingestion/docker-entrypoint.sh`
  - R-022
- `code/02_services/01_ingestion/go-bridge/faketool/main.go`
  - R-094
  - R-213
  - R-274
- `code/02_services/01_ingestion/go-bridge/hft_slot.go`
  - R-095
  - R-096
  - R-153
  - R-224
- `code/02_services/01_ingestion/go-bridge/main.go`
  - R-023
  - R-058
  - R-059
  - R-175
  - R-176
  - R-177
- `code/02_services/01_ingestion/go-bridge/ndjson.go`
  - R-097
  - R-185
  - R-186
  - R-187
- `code/02_services/01_ingestion/go-bridge/subscription_plan.go`
  - R-098
  - R-188
- `code/02_services/01_ingestion/go-bridge/supervisor.go`
  - R-200
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/.gitignore`
  - R-211
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go`
  - R-099
  - R-100
  - R-101
  - R-239
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go`
  - R-024
  - R-138
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/constants.go`
  - R-240
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go`
  - R-102
  - R-103
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/limits.go`
  - R-241
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/margin.go`
  - R-242
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go`
  - R-104
  - R-189
  - R-282
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/orders.go`
  - R-105
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go`
  - R-201
  - R-202
  - R-243
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go`
  - R-203
  - R-204
- `code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/user.go`
  - R-106
- `code/02_services/01_ingestion/go-bridge/token_provider.go`
  - R-139
- `code/02_services/01_ingestion/pom.xml`
  - R-025
  - R-026
  - R-060
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java`
  - R-003
  - R-006
  - R-007
  - R-008
  - R-154
  - R-275
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java`
  - R-107
  - R-190
  - R-191
  - R-244
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java`
  - R-108
  - R-109
  - R-110
  - R-111
  - R-192
  - R-140
  - R-245
  - R-246
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java`
  - R-027
  - R-061
  - R-247
  - R-283
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java`
  - R-112
  - R-113
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java`
  - R-205
  - R-155
  - R-284
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEvent.java`
  - R-206
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java`
  - R-028
  - R-214
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BrokerQuarantine.java`
  - R-207
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java`
  - R-215
  - R-248
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java`
  - R-114
  - R-156
  - R-225
  - R-226
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java`
  - R-029
  - R-030
  - R-062
  - R-063
  - R-249
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java`
  - R-250
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java`
  - R-031
  - R-178
  - R-251
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java`
  - R-032
  - R-064
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/ReadinessFile.java`
  - R-208
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java`
  - R-157
  - R-158
  - R-159
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java`
  - R-115
  - R-116
  - R-193
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java`
  - R-216
  - R-252
  - R-276
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java`
  - R-209
  - R-160
  - R-227
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java`
  - R-033
  - R-253
  - R-254
  - R-277
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java`
  - R-034
  - R-141
  - R-255
  - R-256
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java`
  - R-117
  - R-194
  - R-217
  - R-257
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java`
  - R-035
  - R-036
  - R-065
  - R-066
  - R-067
  - R-179
  - R-258
  - R-278
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java`
  - R-118
  - R-195
  - R-196
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java`
  - R-037
  - R-068
  - R-069
  - R-259
  - R-260
  - R-279
- `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java`
  - R-038
  - R-070
  - R-285
- `code/02_services/01_ingestion/src/main/resources/log4j2.xml`
  - R-119
  - R-161
  - R-171
- `code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java`
  - R-120
  - R-286
- `code/02_services/04_executor/Dockerfile`
  - R-133
- `code/02_services/04_executor/main.py`
  - R-210
- `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java`
  - R-039
  - R-040
  - R-071
  - R-180
  - R-142
- `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java`
  - R-261
- `code/common/invariants/LiveMoneyGuard.java`
  - R-041
  - R-072
  - R-181
- `code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java`
  - R-121
  - R-122
  - R-123
  - R-197
- `code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java`
  - R-124
  - R-198
- `code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java`
  - R-125
  - R-126
- `code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java`
  - R-042
  - R-073
  - R-162
- `code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java`
  - R-262
  - R-172
- `code/common/src/main/java/com/trading/common/config/PlatformConfig.java`
  - R-127
  - R-199
  - R-263
- `code/common/src/main/java/com/trading/common/identity/IdentityModel.java`
  - R-043
  - R-074
  - R-075
- `code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java`
  - R-044
  - R-076
- `code/common/src/main/java/com/trading/common/model/MarketTick.java`
  - R-128
  - R-163
- `code/common/src/main/java/com/trading/common/observability/AuditLogger.java`
  - R-134
- `code/common/src/main/java/com/trading/common/observability/Json.java`
  - R-045
  - R-077
  - R-218
- `code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java`
  - R-046
  - R-047
  - R-078
  - R-264
  - R-265
- `code/common/src/main/java/com/trading/common/observability/SafetyHaltRequest.java`
  - R-129
- `code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java`
  - R-130
  - R-266
- `code/common/src/main/java/com/trading/common/schema/SchemaManifestEntry.java`
  - R-267
- `code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java`
  - R-268
- `code/common/src/main/java/com/trading/common/version/VersionGate.java`
  - R-048
  - R-079
  - R-182
- `code/common/workitem/WorkItem.java`
  - R-269
- `code/logs/ingestion.json`
  - R-051
  - R-164
- `code/pom.xml`
  - R-131
  - R-270
  - R-271
  - R-272
- `code/run-ingestion-full.sh`
  - R-049
  - R-080
  - R-081
  - R-212
  - R-165
- `code/smoke-test.sh`
  - R-050
  - R-166
- `run-ingestion.sh`
  - R-135
  - R-053
- `show-ticks.sh`
  - R-273
- `start-all.sh`
  - R-132
  - R-052
  - R-167
  - R-228

## Needs Investigation

These findings require a product/spec decision before they can be implemented as-is. Each is already an atomic task above; resolve the open question during implementation and record the decision in the task's Agent Notes.

| Task | Open question |
| --- | --- |
| R-010 | Is there a writer/consumer of `Candle15s` yet? Which canonical field set is the contract? |
| R-011 | Is `broker_order_id` intended to be globally unique across accounts? Is there a writer for Order_Lifecycle? |
| R-085 | Is per-account fill attribution a current requirement? |
| R-086 | Is per-attempt correlation required for reconciliation, or latest-attempt-only intended? |
| R-084 | Is supersession a real requirement? LOG vs KV decision. |
| R-089 | Does the consumer expect idempotent upsert (KV) or accept duplicate rows? |
| R-090 | Is manifest history required (composite PK) or current-state-only (header fix)? |
| R-009/R-206 | Are there cluster-wide datalake defaults? Is EOD offload in scope? Is `table.retention.days` valid in pinned Fluss 0.9.1? |
| R-008/R-107 | Does any consumer rely on `ack_ts`? Remove the column or implement real ack recording? |
| R-037 | Does the Go bridge connect over raw TCP or WebSocket? (Verify against `hft_stream.go`/`streams.go`.) |
| R-135/R-134 | Confirm intended retention values (calendar vs trading days) before changing DDL retention. |

---

## Completion Rules

A task is complete only when:

- implementation finished
- acceptance criteria satisfied
- related tests pass
- no new review issues introduced

Update the task's Status checkbox, the Summary counts, and the File Index as tasks move to completion.
