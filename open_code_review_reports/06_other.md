# Other findings (12)

## code/01_platform/02_sql/ddl/03_feature_candles_15s.sql

### [medium] lines 27-29

The previous WITH clause carried the datalake/iceberg settings (`table.datalake.enabled`, `table.datalake.format`, `table.datalake.freshness`, `table.datalake.auto-compaction`) that backed the header's "Lake: EOD Iceberg offload" claim, but the new clause contains only bucket and retention settings. If these properties are not configured as cluster-wide defaults, the EOD Iceberg offload will not run for this table, and the 7-day retention will permanently delete candle data before it is offloaded. Please confirm cluster-level defaults exist or restore the per-table datalake settings.

## code/01_platform/04_scripts/soak-monitor.sh

### [medium] lines 53-53

The `bridge_threads` column approximates goroutine count with the OS `Threads:` count from `/proc/<pid>/status`, but the Go runtime multiplexes goroutines over a small pool of OS threads — a goroutine leak usually does not increase the thread count, so a stable `bridge_threads` can hide a real goroutine explosion. This directly weakens the script's stated goal (the header says thread stability "implies no goroutine explosion"). Prefer sampling an explicit `runtime.NumGoroutine()` metric exposed by the bridge (e.g. a periodic health/telemetry event on stdout) rather than this proxy.

## code/01_platform/04_scripts/soak-reconnect-loop.sh

### [medium] lines 74-76

The script's headline claim is to 'verify NO leak', and it dutifully records java_fds / bridge_fds / java_threads for every cycle, but it never analyzes them: there is no monotonic-increase detection, threshold, or trend check anywhere. recovered only checks 'Java alive + bridge present + ticks advanced'. A genuine FD/thread/socket leak would therefore pass the script unnoticed — the TSV would require post-hoc manual analysis. Add a per-cycle comparison (e.g., fail when the after-metrics exceed the first-cycle baseline by a margin, or a simple running regression) so the run itself detects the leak it is supposed to prove absent.

## code/02_services/01_ingestion/src/main/resources/log4j2.xml

### [medium] lines 15-15

`LOG_DIR` defaults to the relative path `logs`. No launcher passes `-Dlog.dir`: the Docker entrypoint runs `java -cp /app/ingestion.jar ...` with no volume mount for logs, so in the container the JSON file lands in `/app/logs` (ephemeral writable layer, lost on container recreate); local runs resolve it relative to the working directory (`code/logs` — a `code/logs/ingestion.json` has even been committed to the repo). Note also that `start-all.sh` exports a `LOG_DIR` env var, but this config reads `sys:log.dir`, so that env var is silently ignored. Read the environment variable (and/or mount/point to an absolute path) so the JSON log is written to a durable, well-known location.

## code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java

### [medium] lines 25-25

Unit inconsistency in the same broker module: `ArrowOrderResponse.requestTime` is documented as epoch **ms**, while `fillTime` here is epoch **s**, and the Fills DDL column `broker_event_time BIGINT` carries no unit comment. When Action Capture persists `fillTime` and later compares/joins it against market-data timestamps (which the pipeline stores in epoch ms), any code that forgets the *1000 conversion will silently produce time-window errors (wrong fill ordering, misaligned candles/ranking). Either align the field to the platform's epoch-ms convention or make the seconds unit explicit at every consumption point and in the DDL comment.

## code/01_platform/02_sql/ddl/10_positions.sql

### [low] lines 31-31

`table.retention.days = '7'` on a 'current state' projection means a position that remains open without any source event (fill/postback) for more than 7 days may be expired from the KV store and disappear from the current-state view. Other current-state KV tables in this project use longer retention (instruments: 90, execution_gate: 30). If positions can span more than a few days, or if the Fills-audit rebuild depends on this row still existing, confirm this retention is intentional; otherwise extend it to match the intended position lifecycle.

## code/01_platform/04_scripts/run-monday-gates.sh

### [low] lines 32-32

Neither the Go suite nor the Java suite is wrapped with a timeout. The E2E/FLUSS integration tests can hang indefinitely (e.g., Fluss down, stuck JVM, or offline-mode dependency resolution), which would block a CI/cron run forever without a decisive result. Wrap each suite with `timeout` to bound the run.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go

### [low] lines 59-61

When the API returns a 200 with `status != "success"` (an application-level error), the server's `errorMessage`/`errorCode` fields in the response body are discarded and only the status string is surfaced (this repeats across all six methods in this file). `c.request`/`rawRequestAuth` only return the body for HTTP >= 400, so these application-level failures are hard to diagnose (e.g. invalid token, unavailable Greeks, bad segment). Include the error body or an `errorMessage` field in the returned error for diagnosability.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java

### [low] lines 80-80

Files.newBufferedReader(Path.of(path)) on Java 17 (the project targets release 17 and the launcher pins Java 17) decodes using the platform default charset and does not strip a UTF-8 BOM. If the Arrow CSV (e.g., an Excel-exported NSE_CM_EQUITY.csv) is saved with a UTF-8 BOM, or the host locale is non-UTF-8, header/field parsing can be corrupted and a valid manifest rejected. Suggest explicitly using StandardCharsets.UTF_8 and stripping a leading BOM from the first line.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java

### [low] lines 35-35

`Integer.parseInt(args[0])` is unguarded: a non-numeric first argument (e.g. a typo) throws an uncaught `NumberFormatException` with no usage guidance, and `limit <= 0` only catches the negative/zero case, not invalid input. For a developer-facing CLI utility, wrapping the parse and printing a short usage message would make the failure mode much clearer.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java

### [low] lines 96-97

Unclassified exceptions default to RETRYABLE. The class javadoc states that fatal failures should "halt the append path, open the safety gate", and by the time RawTickWriter invokes this classifier the Fluss client's built-in retries have already been exhausted. For an unknown/unexpected exception, a fail-open default of RETRYABLE means the pipeline will retry and then drop the record as FAILED without ever opening the halt gate, silently masking systemic issues. Consider a fail-safe default of FATAL (or make the default configurable) so unknowns surface through the safety gate.

## code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java

### [low] lines 40-40

The fail-closed guard compares the raw env var without trimming. Values such as `' false '` or `'false\n'` (common when the variable is exported from config files with trailing whitespace) would trigger the `IllegalStateException` and crash startup even though the effective value is `false`. Trim the value before the comparison to keep the guard robust.

