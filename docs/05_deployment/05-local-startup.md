# Local Startup Sequence (Ingestion)

Applies to: `02_services/01_ingestion` (Go arrow-bridge → Java `IngestionService` → Fluss).

## Prerequisites

| Requirement | Check |
|-------------|-------|
| Java 17 | `java -version` reports `17` |
| Go toolchain | `go version` (for building the bridge) |
| Fluss running locally | `nc -z localhost 9123` succeeds (coordinator) |
| Arrow credentials | `~/.env.arrow` exists, `chmod 600` (see `04-secrets-rotation.md`) |
| Instrument manifest | `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv` (approved, 1,024) |

## Order of operations

1. **Start Fluss stack** (if not running):
   ```
   cd code/01_platform/01_docker && docker compose up -d
   ```
   Wait for `localhost:9123` to answer. Tables are **pre-created by DDL** —
   the service only verifies them (`verifyTables`, read-only) unless
   `ALLOW_RUNTIME_DDL=true` (local dev only).

2. **Build the Go bridge**:
   ```
   cd code/02_services/01_ingestion/go-bridge && go build -o arrow-bridge .
   ```

3. **Verify schema** (optional, read-only):
   ```
   cd code/02_services/01_ingestion
   # DdlBootstrap.verifyTables — 19 tables expected, 0 mismatches
   ```

4. **Run the full pipeline**:
   ```
   cd code && ./run-ingestion-full.sh
   ```
   The script sources `~/.env.arrow`, exports Arrow creds, points the bridge at
   the approved manifest, and launches Java, which spawns the bridge subprocess.

5. **Observe**:
   - Java logs: `ingestion: Fluss connected`, `manifest loaded (instruments=1024)`,
     `arrow-bridge started (pid=…)`.
   - Bridge NDJSON on the Java side is parsed into ticks/events.
   - Metrics flush every 10s to the OTLP collector (`:4318`).

## Startup dependency order (why)

| Dependency | Required because | Startup check |
|------------|------------------|---------------|
| Fluss coordinator | Schema verify + append writers | `DdlBootstrap.verifyTables` |
| Instrument manifest | Bridge subscription plan (tokens) | `loadTokensFromCSV` — refuses to start with 0 tokens |
| Arrow credentials | Bridge auth (`token_len` logged, never the token) | `:?` guard in the script |
| Bridge binary | Java `ProcessBuilder` spawn | `docker-entrypoint.sh` preflight |

## Shutdown sequence

1. Send `SIGTERM`/`SIGINT` to Java.
2. Java stops the bridge subprocess (exit 0), emits `bridge_shutdown` drain event.
3. Java flushes append writers, closes Fluss connections, exports final metrics.
