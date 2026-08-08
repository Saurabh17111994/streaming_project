# Performance findings (13)

## code/01_platform/02_sql/ddl/06_ranking_results.sql

### [medium] lines 32-32

The table is declared as a "per-evaluation ranking audit" and `rank`/`selected` only have meaning within one evaluation, yet it is bucketed by `candidate_id`. A single evaluation's rows (one per candidate) will therefore be scattered across all 8 buckets, so a consumer reading one evaluation's full ranking must scan and merge every bucket and can no longer rely on append order (rank order) within a bucket. If the primary consumer reads per evaluation (e.g., Signal job reconstructing the ranking to derive Trade_Decisions), `evaluation_id` would be a better bucket key to co-locate an evaluation's rows and preserve insertion order. Note the header comment also says "Bucket key: evaluation/candidate routing identity", which is ambiguous and doesn't match the actual `candidate_id`. Please confirm the actual read pattern and align the bucket key (or the header comment) accordingly.

## code/01_platform/04_scripts/soak-monitor.sh

### [medium] lines 59-59

Every sample re-greps the entire journal for each pattern — the journal rolls at 64 MB, so over a multi-hour soak each 5 s sample performs up to 4 full-file O(n) scans, adding continuous CPU/disk I/O to the ingestion host. Also `grep -c` counts matching *lines* (not occurrences), and the totals are cumulative since file start, so when the daily/64 MB rolling policy rotates the file the counts silently reset and trend analysis is misleading. Prefer incremental reads (e.g. `tail -c +N` from the last offset) and per-interval deltas; use `grep -Fc` for these literal patterns since they are fixed strings, not regexes.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go

### [medium] lines 47-47

The HTTP client is created with no timeout and the three request helpers use plain `HTTPClient.Do` (no context, deadline, or `DoTimeout`/`DoDeadline`). A stalled Arrow endpoint blocks the calling goroutine indefinitely: at startup this hangs `main` inside `AutoLogin` before the signal handler is installed, and during HFT auth refresh it stalls the read goroutine (tick decoding and the raw_table write path stop with no way to cancel) and leaks a goroutine per stuck refresh. Set `ReadTimeout`/`WriteTimeout` on the fasthttp client and/or bound each call with `DoTimeout`/`DoDeadline`.

## code/02_services/01_ingestion/go-bridge/token_provider.go

### [medium] lines 42-42

Refresh holds p.mu while invoking refreshFn, which for a token provider typically performs network I/O. During the refresh window, every Current() call and every queued Refresh() caller blocks on the exclusive Lock(); because sync.Mutex.Lock is not context-aware, waiters queued behind a slow/hung refresh cannot honor their context cancellation or deadline while blocked. This can stall the auth path that depends on Current once the provider is wired into the bridge. Consider running refreshFn outside the exclusive lock (snapshot `current`, release the lock, call refreshFn, then re-acquire and update only on success) and serialize in-flight refreshes with a dedicated mechanism (e.g., a singleflight/atomic flag or a shared result channel) so readers are not blocked for the whole refresh. At minimum, use RWMutex so Current takes the read lock.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

### [medium] lines 714-715

`refreshResourceMetrics()` is invoked once per processed frame and performs a directory listing of `/proc/self/fd`, a read of `/proc/sys/fs/file-max`, a full read of `/proc/self/status`, and a `ThreadMXBean.getThreadCount()` call. At HFT tick rates (thousands of frames/sec) these syscalls add measurable per-frame overhead on the hot ingestion path. Throttle the refresh to a fixed interval (e.g., 1s) instead of every frame.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java

### [medium] lines 187-194

The Fluss `Connection` opened in the constructor is a local variable — it is never stored, and `close()` only calls `writer.flush()`, so the connection (and the underlying Sender/MetadataUpdater threads) is never released. If `getTable()` or `createWriter()` throws after the connection is created, it also leaks on the exception path. In a long-running service this leaks client resources. Store the connection in a field and close it in `close()` (and on the constructor failure path).

## code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java

### [medium] lines 148-150

`generateTicks()` runs on the single scheduled executor thread and synchronously `write`/`flush`es to every connected client in a loop. A slow or stalled client can block the scheduler, stalling tick generation for all clients and making the mock unresponsive. There is no per-client send queue, write timeout, or backpressure. Consider decoupling per-client delivery (e.g. bounded per-client queues with a dedicated sender per client, or socket write timeouts).

## code/02_services/01_ingestion/go-bridge/faketool/main.go

### [low] lines 100-101

A new zstd encoder (including its internal state/table initialization) is constructed and closed for every outbound frame inside the per-connection loop. Since `Encoder.EncodeAll` is safe for concurrent use, create one encoder per connection (or in `main`) and reuse it across frames; this removes repeated per-message allocation/init overhead that can make the fake broker's throughput/latency unrepresentative of the real one during the E2E test.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java

### [low] lines 28-29

Every NDJSON line is fully JSON-parsed here (`mapper.readTree`) and again in `parse()`, and then a third time by `MAPPER.readValue(line, GoTick.class)` in the caller — so each high-frequency tick is parsed 3 times, even though this parser only needs the `record_type` field to decide it is not a lifecycle/quarantine record. On the HFT ingestion hot path (ticks with large base64 `raw_payload`) this is significant avoidable overhead. Consider doing a cheap `record_type` pre-check (e.g. substring/lookup) before the full tree parse, or having `parse()`/`parseQuarantine()` share one parsed node/dispatch result so the caller can avoid re-parsing.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java

### [low] lines 38-39

`String.matches("[0-9a-f]{64}")` compiles a fresh regex Pattern on every invocation. This validator runs for every tick on the ingestion hot path (HFT feed can be thousands of records/sec), so the per-call compile adds avoidable allocation/GC pressure. Precompile the pattern once as a static field and use `Pattern.matcher(...).matches()` instead.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java

### [low] lines 29-29

The defensive clone in rawPayload() is taken multiple times per tick on the ingestion hot path: RawTickWriter.write() → rowConverter.estimatedRowSize(packet) calls raw.rawPayload().length (cloning the entire payload just to read its length), and RealFlussRowConverter.append() calls raw.rawPayload() again to build the Fluss row. At market-data rates each tick therefore allocates at least two full payload copies, adding avoidable GC pressure. Consider exposing a length-only accessor (e.g., rawPayloadLength()) for the size estimate, or document the copy cost so callers can reuse a single cached array.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java

### [low] lines 96-97

`Files.lines(journalPath).count()` opens an I/O-backed stream that is never closed — each call leaks a file descriptor. It also re-scans the entire journal file just to report the entry count, making repeated writes O(n^2). Use try-with-resources, or track the count in memory (e.g. an AtomicLong) instead of re-reading the file.

## code/common/src/main/java/com/trading/common/observability/Json.java

### [low] lines 70-70

`String.format("\\u%04x", (int) c)` is invoked inside the per-character loop for every escaped control character, allocating a formatted String each time. For a low-level JSON builder likely used on a telemetry hot path, prefer writing the four hex digits directly (e.g. via a `"0123456789abcdef"` lookup) or appending to the `StringBuilder` with a small helper.

