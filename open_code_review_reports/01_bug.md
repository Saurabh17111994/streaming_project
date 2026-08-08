# Bug findings (167)

## code/01_platform/04_scripts/soak-reconnect-loop.sh

### [critical] lines 59-60

The core premise of this soak test is broken by the service's own design. The Go bridge installs a SIGTERM handler (signal.NotifyContext with syscall.SIGTERM in go-bridge/main.go:155), so the default kill makes the bridge exit cleanly with code 0. In IngestionService.recordBridgeExit, requested = exitCode == 0 || !running is true, so bridgeRestartDecision returns NO_RESTART and the JVM calls shutdown() — the whole ingestion service terminates on the very first kill, not 'Java's runWithBridge restart loop restarts it'. Even with kill -9 (non-zero exit), MAX_BRIDGE_RESTARTS = 1 (IngestionService.java:77) means Java restarts the bridge exactly once; the second kill is TERMINAL and shuts Java down. With the default CYCLES=100, this script tears the pipeline down instead of soaking it. The kill/settle strategy must be aligned with the service's actual restart semantics (or the service's restart policy changed) before this script can fulfil its purpose.

### [high] lines 48-48

grep -c '"feed":"hft"' on $LOG_FILE (Java stdout/stderr tee'd into logs/ingestion.log) can never match: the Go bridge emits tick NDJSON to its own stdout, which Java consumes in processLine() and never re-logs (see 'Tick stdout is never logged' in IngestionService.drainStderr). So t0_ticks and ticks_now are always 0, the recovery check [ "$ticks_now" -le "$t0_ticks" ] is always true (vacuous), and the ticks_total column in the TSV is always 0. Additionally, even if the pattern did match, a fixed cumulative baseline means the criterion passes forever once the first tick arrives after startup, so it can never detect a per-cycle stall. Count a progress signal that is actually written to the log (e.g. Java's 'ingestion: bridge exited ...' / 'subscription complete' lines) or query Fluss/readiness, and compare per-cycle deltas instead of a cumulative total.

### [low] lines 65-65

java_pid_before is captured once at startup and never refreshed, so every cycle's 'before' FD/thread readings use the same (potentially stale) PID. If the Java process is ever restarted mid-run (crash + supervisor, or the shutdown path triggered by the kill issue above), count_fds/threads_of on the dead PID return 0 — or worse, the PID could be recycled by an unrelated process — producing meaningless TSV rows. Also find_pid uses pgrep -f "$1" | tail -1, which can select the wrong process when more than one command line matches. Recompute the baseline PID each cycle (from java_pid_after) and verify it is non-empty before trusting the numbers.

## code/02_services/01_ingestion/Dockerfile

### [critical] lines 18-18

This Dockerfile now assumes the build context is the Maven reactor root (`code/`) — it does `COPY pom.xml .`, `COPY common ./common`, and `COPY 02_services/01_ingestion/...`. However, the `ingestion` service in `code/01_platform/01_docker/docker-compose.yml` (updated in the same change set) still sets `build.context: ../../02_services/01_ingestion`, i.e. only the ingestion directory. Building via `docker compose build ingestion` will therefore fail with `COPY failed: file not found` because the parent POM and the `common` module are outside that context. Either update the compose build to `context: ../..` + `dockerfile: 02_services/01_ingestion/Dockerfile`, or make the Dockerfile self-contained so it can build from the ingestion directory.

### [high] lines 31-31

The runtime image launches Java via the copied docker-entrypoint.sh using `java -cp /app/ingestion.jar` without `--add-opens=java.base/java.nio=ALL-UNNAMED`. The project explicitly documents this flag as required for the Fluss client's shaded Arrow (MemoryUtil touches java.nio internals on JDK 17+) — it is used in the parent POM's surefire config and in both host launchers (`run-ingestion-full.sh`, `start-all.sh`). Without it, the containerized IngestionService will likely fail at startup with `InaccessibleObjectException` when the Fluss client initializes. Add the flag to the entrypoint's java command (or set `JAVA_TOOL_OPTIONS` in this stage) so the container behaves like the verified host run path.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

### [critical] lines 130-131

This drop-and-recreate path is destructive for DDL-provisioned clusters. For every existing table whose column count differs from the in-code schema — which is the case for all 15 MINIMAL_SCHEMA tables (7 cols vs DDL 11-24) and the stale Postback_Quarantine/suspected_discontinuities schemas — ensureTables() will unconditionally drop the table and recreate it with a reduced/incorrect schema, wiping all data. A bootstrap utility should never drop tables based on a column-count heuristic; restrict mutation to creating missing tables and leave schema reconciliation to the offline DDL gate.

### [high] lines 59-60

verifyTables() compares the DDL-created tables' column counts against the in-code schemas, but these schemas do not match the authoritative SQL DDL for most tables: 15 of the 19 entries use MINIMAL_SCHEMA (7 columns) while the DDL creates those tables with 11-24 columns (e.g. feature_candles_15s=15, Signal_Candidates=21, Trade_Decisions=24, Fills=22, instruments=14). Postback_Quarantine (in-code 18 vs DDL 13) and suspected_discontinuities (in-code 15 vs DDL 11) are also stale. Since IngestionService.main() treats a false return as FATAL (System.exit(1)), the default production start path will always fail with 'schema-mismatch' even when the DDL has been applied correctly. The in-code schemas must match the DDL column counts, or verification should only cover the tables ingestion actually owns.

### [high] lines 297-298

ALL_TABLES omits `ingestion_quarantine` (DDL 21_ingestion_quarantine.sql, 10 columns). This table is written by the ingestion service's own QuarantineWriter (TABLE_NAME = "ingestion_quarantine"), which connects to it during service construction. As a result: ensureTables() will never create it, so the ALLOW_RUNTIME_DDL=true local path fails when QuarantineWriter connects; and verifyTables() never checks it, so the read-only verification gives false confidence that the pipeline's tables exist. The class javadoc also claims 'all 19 platform tables' while the DDL directory contains 21 files. Add ingestion_quarantine with the 10-column schema matching the DDL and the writer's GenericRow.

### [high] lines 206-206

POSTBACK_QUARANTINE_SCHEMA is stale: it carries the old 18-column shape (broker_order_id, client_order_ref, broker_status, resolution_ts, operator_identity, ...) while the migrated DDL 16_postback_quarantine.sql now defines a 13-column schema (quarantine_id, postback_event_id, reason, original_payload, payload_hash, broker_order_id, instruction_id, correlation_attempt, disposition, disposition_reason, quarantined_ts, disposition_ts, schema_version). DISCONTINUITY_SCHEMA has the same problem — 15 columns vs the 11 columns in 19_suspected_discontinuities.sql and the 11 values DiscontinuityWriter actually appends. These stale schemas drive the verifyTables()/ensureTables() failures above. Keep the in-code schemas in sync with the DDL and the writer rows.

## code/01_platform/01_docker/docker-compose.yml

### [high] lines 125-126

The ingestion `environment:` block omits required Java config keys that `IngestionConfig.validate()` treats as mandatory: `RAW_TABLE_NAME`, `ARROW_MAX_EVENT_AGE_MS`, and `ARROW_MAX_FUTURE_EVENT_SKEW_MS` (the latter two are defined in `.env.example`, and none are in this map). Docker Compose only injects keys listed here into the container, so `IngestionConfig.validate()` will throw `IllegalStateException` ("RAW_TABLE_NAME is required but not set", etc.) and the Java service exits before the bridge ever launches — the Phase-1 pipeline cannot start as configured. Add pass-through entries for these keys (e.g. `RAW_TABLE_NAME: ${RAW_TABLE_NAME:-raw_table_1}`).

## code/01_platform/02_sql/ddl/02_raw_table_1.sql

### [high] lines 24-24

`ack_ts BIGINT NOT NULL` cannot be satisfied truthfully on an immutable append-only LOG table. The ack timestamp is not known at row-build time: the ingestion writer (`RealFlussRowConverter.append`) writes a placeholder `0L` (comment: 'set after append') and there is no post-append code path that updates the row — Fluss LOG rows are immutable and the append result (offset) is discarded. As a result every stored record has `ack_ts = 0`, permanently defeating the DDL's acknowledgment-timestamp contract and misleading any consumer using `ack_ts` for latency/ordering analysis. Remove the column, make it nullable with 0 meaning 'unknown', or introduce an explicit ack-time recording design before declaring it NOT NULL.

### [high] lines 42-42

The WITH clause drops all previously configured datalake/Iceberg options (`table.datalake.enabled/format/freshness/auto-compaction`) and caps retention at 7 days, while the header still claims 'Lake: EOD Iceberg offload' and 'extend while EOD offload unverified'. No lake-tiering config exists elsewhere in this repo (docker-compose only has a placeholder comment about S3 lake tiering). If the EOD Iceberg offload is not enabled by some other mechanism, every accepted tick will be hard-deleted after 7 days without ever being offloaded — permanent loss of the only market-data record. Restore the datalake options or an EOD offload job, or update the header claims to match reality. Also verify `table.retention.days` is a valid Fluss 0.9.1 table property (the previous DDL used `table.log.ttl`); if unsupported, Fluss may reject the DDL or silently ignore retention.

### [medium] lines 28-28

The schema declares `bid_price_paise`/`bid_qty`/`ask_price_paise`/`ask_qty` and marks `last_price_paise`/`last_qty` NOT NULL, but the ingestion path does not carry bid/ask data: `RealFlussRowConverter` hardcodes 0 for all four quote columns (TickPacket has no bid/ask fields), and QUOTE (`VALID_NON_TRADE`) ticks still write `last_price_paise`/`last_qty` from the quote frame's ltp/volume (frequently 0). Every QUOTE row therefore stores fabricated zero bid/ask and ambiguous trade fields — downstream consumers reading bid/ask or aggregating price/volume will be silently corrupted. Align the implementation with the schema (propagate Arrow bid/ask for QUOTE ticks and leave trade fields null/absent for QUOTE rows) or drop the unimplemented columns.

## code/01_platform/02_sql/ddl/03_feature_candles_15s.sql

### [high] lines 22-24

This DDL rewrite changes the table contract (adds `exchange`/`symbol`, replaces `candle_version` with `algorithm_version`+`configuration_version`, renames `ingest_ts` to `output_ts`, OHLC → paise BIGINT), but the shared producer model `com.trading.common.model.Candle15s` added in this same change set still declares `candleVersion`/`ingestTs` and has no `exchange`/`symbol`/`algorithmVersion`/`configurationVersion` fields. Since Fluss auto-infers the table column schema from the first append (see ddl-init.sh / DdlBootstrap.verifyTables), a writer built on `Candle15s` would create a 12-column table that does not match this 15-column DDL, causing append/column-resolution or column-count verification failures. Align the model and this DDL (and any consumer) before landing.

### [medium] lines 29-29

The header documents "Retention: ≤7 trading days (ceiling); extend while EOD offload unverified", but `table.retention.days='7'` is 7 calendar days (≈5 trading days after weekends/holidays), so rows would be evicted earlier than the stated business requirement. In addition, there is no mechanism here to extend retention while the EOD offload is unverified, so candle data could be purged before it is safely offloaded. Either express the value in trading days (≈10 calendar days) or add an explicit extension control.

## code/01_platform/02_sql/ddl/09_order_lifecycle.sql

### [high] lines 21-23

The header declares "Scope: account_scope_id", but the table has no `account_scope_id` column and the primary key is only `broker_order_id`. Broker-assigned order IDs are typically unique only within a single brokerage account; in a multi-account deployment two accounts can legitimately produce the same `broker_order_id`, and this KV projection would silently overwrite one account's order state with another's. This also diverges from the sibling schema convention (e.g. `Trade_Decisions` stores `account_scope_id STRING NOT NULL` to match its scope header). Suggest adding `account_scope_id` as a column and including it in the primary key (and bucket.key) so order state is scoped correctly.

## code/01_platform/04_scripts/ddl_apply.py

### [high] lines 252-254

Control-flow defect: this branch returns 0 whenever the committed schema_manifest.json already matches the computed manifest — the steady-state case for a re-run — so the `--apply-verified` / `--matrix-evidence` handling below is never reached. A caller that passes both flags in the synced state gets exit code 0 and a "Manifest is current" message, silently skipping the documented step 6 (gated apply). Today the apply branch is a stub, but the structure guarantees this becomes a silent success-with-no-apply the moment real Fluss DDL application is implemented. Restructure so the apply step runs (or is refused) regardless of drift state, e.g. only return early when not applying.

## code/01_platform/04_scripts/digest-pin.sh

### [high] lines 27-28

`docker manifest inspect` (without `--verbose`) prints the raw manifest JSON, which contains the `config` and `layers` blob digests but NOT the manifest's own digest. The first `sha256:` match is therefore typically the config blob digest, so the script emits `img@sha256:<config-digest>` — an invalid manifest reference. When this output is written to runtime.lock, image pulls will fail (or, for a manifest list, pin an arbitrary platform manifest instead of the index). Use `docker manifest inspect --verbose` and extract the top-level `Descriptor.digest`, or `docker buildx imagetools inspect "$img" --format '{{.Manifest.Digest}}'`. Note `skopeo inspect` and `crane digest` do return the real manifest digest, so this is the highest-priority branch to fix.

### [medium] lines 44-44

The script does not validate the input reference. If a caller passes a reference that is already pinned (`image:tag@sha256:abc`), the script blindly appends another digest and emits a malformed reference like `image:tag@sha256:abc@sha256:def`, silently corrupting runtime.lock. References without a tag are likewise accepted. Consider rejecting any input that already contains `@sha256:` (or is not in `name:tag` form) with a clear error before resolving.

## code/01_platform/04_scripts/run-monday-gates.sh

### [high] lines 41-42

The Java gate enables the E2E test (INGESTION_INT_TEST_E2E=true), but the preceding `go test -count=1 ./...` step does not build the binaries FullStackE2ETest execs: it defaults to `go-bridge/faketool/faketool` and `go-bridge/arrow-bridge` (relative to the surefire working dir). `faketool` is behind a `//go:build faketool` tag, so `go test ./...` doesn't even compile it, and `go test` never emits binaries into the module tree. On a clean checkout the E2E test will fail with IOException, so the gate cannot pass no matter how correct the code is. Build these binaries before running the Java suite.

## code/01_platform/04_scripts/soak-headroom.sh

### [high] lines 26-26

Default LOG_FILE points to `$PROJECT_ROOT/logs/ingestion.log`, but the pipeline never writes that file: log4j2.xml writes `logs/ingestion.json` (a JSON log, e.g. `code/logs/ingestion.json`), and the launcher (`run-ingestion-full.sh`) tees the console stream to `$HOME/.local/state/trading-platform/ingestion/ingestion-<timestamp>.log`. `logs/ingestion.log` does not exist in the repo, so running the script with no arguments immediately fails at the `[ -f "$LOG_FILE" ]` FATAL check. Point the default at the actual log path (e.g. `$PROJECT_ROOT/code/logs/ingestion.json`) or auto-discover the latest dated log.

### [high] lines 34-34

This regex expects the fields `state=ACTIVE assigned=1024 acknowledged=1024 rejected=0` to be adjacent tokens, but neither actual log source produces that. The Java service logs (IngestionService.handleBridgeEvent, line 807) the message as `bridge lifecycle event=subscription_ack slot=... state=ACTIVE epoch=1 assigned=1024 acknowledged=1024 rejected=0 ...` — `epoch=` sits between `state=` and `assigned=`, so the pattern never matches and the "Subscription acks" summary is always empty. The raw Go bridge NDJSON is also JSON (`"state":"ACTIVE","assigned_tokens":1024,...`), which this flat pattern cannot match either. The headroom evidence this script is meant to prove (plan §1379) is therefore silently absent. Align the pattern with the actual message format (tolerate `epoch=` between `state=` and `assigned=`) or parse the JSON fields.

### [low] lines 60-60

`int(n*0.99)+1` overestimates p99 whenever `n*0.99` is an integer: for n=100 it yields idx=100 (the maximum), so "p99" degenerates to the max and the reported distribution is biased toward the cap (understating headroom). Use the nearest-rank definition, e.g. `idx = int(n*0.99 + 0.5)` with a lower clamp of 1.

## code/01_platform/04_scripts/soak-monitor.sh

### [high] lines 27-27

The default LOG_FILE (`$PROJECT_ROOT/logs/ingestion.log`) is never created by the ingestion stack. The Log4j2 file appender writes JSON-structured records to `logs/ingestion.json` relative to `code/` (i.e. `code/logs/ingestion.json`), and the launcher (`run-ingestion-full.sh`) additionally tees a copy under `~/.local/state/trading-platform/ingestion/ingestion-*.log`. With this default, `count_events` always greps a non-existent file and every event column silently reads 0 — giving false confidence in a healthy soak. Point the default at the real journal (or derive it from the script location) and, since the journal is one-JSON-object-per-line, parse it accordingly.

### [high] lines 73-73

Even with a corrected log path, the `ticks`/`reconnects`/`hbfail`/`stalls` columns will always be 0: bridge tick NDJSON (`"feed":"hft"`) and lifecycle events (`"event":"reconnect"`, `heartbeat_failed`, `feed_stalled`) are emitted by the Go bridge on stdout via `BridgeEmitter` and consumed in-process by `IngestionService` — `IngestionService.java` explicitly notes "Tick stdout is never logged". None of these are written to the Log4j2 journal, so this monitor can never observe the pipeline's actual tick/health behavior. Sample the bridge stdout/NDJSON directly (or the OTel metrics endpoint) instead of grepping the journal.

### [high] lines 49-49

Under `set -euo pipefail`, `ls /proc/$pid/fd 2>/dev/null | wc -l` will abort the entire monitor if the monitored process exits (or is restarted) between `find_pid` and this read: `ls` returns non-zero, pipefail propagates it, and `set -e` kills the script. That is precisely the failure scenario this soak monitor is meant to observe, so the summary would be truncated exactly when the pipeline goes down. Guard the read so a missing process yields 0 without aborting.

### [medium] lines 45-45

`pgrep -f "$1" | tail -1` selects the numerically highest PID, not the newest or intended process. If multiple instances match (e.g. leftover dev runs or the stale-bridge cleanup window), the monitor samples an unrelated process and reports misleading FDs/threads. Additionally, the Java and bridge PIDs are resolved in two separate `find_pid` calls per sample, so after a restart one sample can mix a new generation's bridge PID with an old generation's Java PID. Prefer selecting the newest process (e.g. filter `ps -eo pid,etimes,cmd` and sort by start time) and resolve both PIDs consistently within the same sample.

## code/02_services/01_ingestion/docker-entrypoint.sh

### [high] lines 19-19

Environment variable mismatch between the entrypoint and the Java consumer: `InstrumentManifestLoader.loadDefault()` reads `INSTRUMENT_MANIFEST_PATH`, never `ARROW_INSTRUMENT_MANIFEST` (the latter is consumed only by the Go bridge's `main.go`). Here the resolved path is exported solely as `ARROW_INSTRUMENT_MANIFEST`, so when an operator supplies only `ARROW_INSTRUMENT_MANIFEST` (a supported input per the resolution above), Java sees no manifest path and fails startup with an empty instrument set. And if both variables are set to different values, Java and the Go bridge would load different manifests. Suggest normalizing and exporting the resolved path under both names, e.g.: ```bash export ARROW_INSTRUMENT_MANIFEST="$MANIFEST_PATH" export INSTRUMENT_MANIFEST_PATH="$MANIFEST_PATH" ```

## code/02_services/01_ingestion/go-bridge/main.go

### [high] lines 489-492

`classifyAuthRefresh` returns `authResumed` as soon as `refreshErr == nil`, before checking `hasRefresh`/budget. Callers pass `refreshErr == nil` in two terminal cases: (1) token-only deployments (`refreshAuth == nil`, so the `if refreshAuth != nil && authRefreshes < 3` guard never runs) and (2) after the refresh budget guard skips an attempt. In both cases an auth failure is misclassified as a successful refresh: the error callback emits `authentication_refreshed`, the epoch returns retryable, and the slot reconnects forever instead of emitting `auth_failure`/terminating — permanently invalid credentials are silently masked. Note also that `authRefreshes` is a per-epoch local in `runHFTEpoch` and each epoch ends after the first auth error, so the 3-attempt budget never accumulates across reconnects even when `refreshAuth` exists, making the terminal branch unreachable. Reorder the checks to evaluate `!hasRefresh || authRefreshes >= 3` before the `refreshErr == nil` short-circuit, and carry the auth-refresh budget across epochs (e.g., in the slot/loop state) so the documented terminal policy actually takes effect.

### [medium] lines 594-595

Standard mode writes ticks through the global `encoder` (json.NewEncoder(os.Stdout)) while all events — including `bridge_shutdown` — are written through `bridgeEmitter`, whose mutex only guards its own writes. These are two independent writers to the same stdout fd. `runStandard` returns as soon as the context is cancelled without joining the `ds.ReadTicks` goroutine, so a final tick can still be mid-`emit()` when `main()` calls `emitShutdownEvent()`. The two writes can interleave (corrupting an NDJSON line) or a tick can land after the drain marker, breaking the documented guarantee that `bridge_shutdown` is the last line. Route standard ticks through `bridgeEmitter` (shared mutex) and/or join/stop the reader goroutine before emitting the shutdown event.

### [medium] lines 383-385

The heartbeat and watchdog goroutines only exit on `ctx.Done()` or their own failure condition; they are not stopped when the epoch ends via `epochStop`. After a disconnect/decode-burst ends an epoch (and `defer stream.Close()` runs), the old goroutines keep running against the closed stream: the heartbeat can emit a stale `heartbeat_failed` for an obsolete epoch at the next 3s tick, and the watchdog can emit `feed_stalled` up to 15s after the frozen `lastFrameNanos`. Across repeated reconnect cycles goroutines accumulate until their timers fire, and can leak indefinitely if `WriteText` blocks on the dead connection. Select on a per-epoch context (e.g., the `readCtx` cancelled by the deferred `stopRead()`) so these goroutines terminate together with the epoch.

### [low] lines 601-606

`envOrFatal` exits with status 1 for a missing required environment variable, but the file's own exit-status contract (`exitRequested=0, exitSupervisor=1, exitFatalStart=2`) assigns status 2 to fatal config/startup failures. A missing `ARROW_APP_ID`/`ARROW_APP_SECRET` is exactly such a startup config failure, so supervisors that distinguish config failures from runtime stream failures will misclassify it. Use `os.Exit(exitFatalStart)`.

### [low] lines 628-633

Instrument tokens are parsed with `strconv.Atoi` and narrowed to `int32` without range or sign validation. A token outside the int32 range silently wraps (e.g., `99999999999` → a different instrument) and negative values are accepted verbatim, so the bridge can subscribe to the wrong instruments or get an opaque rejection mid-subscription. Validate `0 <= n <= math.MaxInt32` (or use `strconv.ParseInt(..., 10, 32)` with a range check) and skip/reject invalid entries at the ingestion boundary.

### [low] lines 202-202

The single-socket policy violation path emits `Event: "auth_failure"`, which the NDJSON contract reserves for credential failures. Alerting and telemetry keyed on `auth_failure` will fire (and the Java side may treat it as an auth terminal state) for a deployment/config error, masking the real cause. Use a distinct event type (e.g., `slot_state` with `SlotTerminal` and a `single_socket_policy_violation` reason) instead of `auth_failure`.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/client.go

### [high] lines 90-94

Use-after-release of the pooled fasthttp body buffer. `resp.Body()` returns a slice that aliases the response's internal buffer, and `defer fasthttp.ReleaseResponse(resp)` returns that buffer to the pool as soon as this function returns. The `[]byte` handed to the caller therefore points at recycled memory that the next request executed on this client can overwrite. In this same package, `GetCandleData` returns `json.RawMessage(bytes.TrimSpace(resp))` — a sub-slice of the released buffer retained beyond the call — and any concurrent use of the shared client will silently corrupt previously returned bodies. Copy the body before returning (e.g. `append([]byte(nil), resp.Body()...)` or `fasthttp.CopyBody`). This applies to all three helpers (`request`, `rawRequest`, `rawRequestAuth`).

## code/02_services/01_ingestion/pom.xml

### [high] lines 37-41

log4j-slf4j-impl:2.25.4 is the SLF4J 1.7.x binding, but this module (and the parent-managed slf4j-api 2.0.9 used by common and the Fluss client) is on SLF4J 2.0. SLF4J 2.0 discovers providers via ServiceLoader for org.slf4j.spi.SLF4JServiceProvider, which the 1.7 binding does not implement — so at runtime logging silently falls back to NOPLogger (or fails with NoSuchMethodError). Use the SLF4J 2.0 binding artifact log4j-slf4j2-impl instead.

### [high] lines 7-12

This parent + sibling `com.trading:common` dependency makes the ingestion module non-self-contained: any build must run from the Maven reactor root (`code/`). The updated Dockerfile already requires that (copies `pom.xml`, `common/`, `02_services/01_ingestion/...`), but `docker-compose.yml` still builds ingestion with `context: ../../02_services/01_ingestion`, which contains neither the parent POM nor the `common/` module — so `docker compose build ingestion` fails at the first `COPY` in the Dockerfile. Update the compose build context to the reactor root (e.g. `context: ../..` from `01_docker/`) so the parent and sibling module are available.

### [medium] lines 75-79

The shaded fat jar only merges the manifest. Dependencies such as the Fluss client / Arrow bring their own `META-INF/services` provider files and Log4j2 plugin metadata; without a ServicesResourceTransformer these files are overwritten (last one wins), so ServiceLoader-based providers can be silently missing at runtime. Add `ServicesResourceTransformer` to the transformer list.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java

### [high] lines 264-264

syntheticSet() generates tokens as 100_000 + i*100 + (i%10), i.e. {100000, 100101, 100202, ..., 104949}, but MockArrowServer.main() builds its default 50-instrument set as 100_000 + i*100, i.e. {100000, 100100, ..., 104900}. The two sets share only 5 tokens, so in the ALLOW_SYNTHETIC_MANIFEST dev path the ticks emitted by MockArrowServer will mostly be absent from the loaded manifest — IngestionService will quarantine them as MISSING_INSTRUMENT and the subscription-completeness check (seenTokens >= instrumentMap.size()) will never pass. Fix the formula to match MockArrowServer: 100_000L + i * 100L.

### [medium] lines 142-142

loadFromPath() returns approved=true unconditionally after parsing, even when zero data rows were loaded (header-only file, or every row malformed) or when some rows were skipped. This contradicts the class javadoc's SCH-22 semantics ('one approved manifest version defines the active subscription set… if validation fails, readiness remains false') — a truncated/corrupt production CSV with a valid header would still be reported as approved. The count/fingerprint validation in isManifestApproved() is never invoked from this loader or its caller. Suggest returning approved=false when rows were skipped or the parsed set is empty.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEventParser.java

### [high] lines 17-17

This throws for any record_type other than `tick`/`bridge_event` — including `broker_quarantine`. In `IngestionService.processLine`, `parse()` is called before `parseQuarantine()` on every line, and the caller never falls through after an exception (it's caught by the outer `catch (Exception e)` and logged as an INTERNAL_ERROR quarantine). As a result, a `broker_quarantine` line will never reach `parseQuarantine()`, and the broker-quarantine handling path is effectively dead. Since the caller relies on `parse()` returning `Optional.empty()` to fall through to the next parser, non-`bridge_event` records should be skipped, not rejected.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java

### [high] lines 161-164

The event vocabulary here does not match the actual BridgeEvent contract. BridgeEvent validates `event` against exactly {slot_state, subscription_ack, heartbeat_failed, feed_stalled, disconnect, reconnect, auth_failure, bridge_shutdown}. Consequently: (1) `case "bridge_exit"` is dead code — no such event can ever reach this switch; the real bridge-exit event is `bridge_shutdown`, which is unmapped, so a bridge shutdown produces no DROP evidence; (2) the Javadoc promises `SUBSCRIPTION_PARTIAL -> FEED_HEALTH`, and IngestionService explicitly calls writeBridgeEvent for `subscription_ack` with rejectedTokens>0, but "subscription_ack" is unmapped — mapEventToReason returns null and that feed-health evidence is silently dropped. Align the switch with the validated event names (handle `bridge_shutdown` and the partial-subscription case, e.g. in writeBridgeEvent where rejectedTokens is available).

### [high] lines 237-245

The CompletableFuture returned by writer.append(row) is discarded at both append sites (write and writeWithEpoch). Fluss AppendWriter.append is asynchronous — broker-side or serialization failures are surfaced by completing the future exceptionally, not by throwing from append(), so the surrounding try/catch never sees them and the discontinuity row is silently lost with no retry, metric, or alarm. This is a money-safety evidence path; contrast RawTickWriter, which awaits the future. Attach a whenComplete/exceptionally handler (or await with timeout) and log/record a metric on failure.

### [medium] lines 253-263

The Connection created in the constructor is a local variable and is never stored or exposed, yet close() states it "is closed by the creator" — no caller holds a reference, so it can never be closed. Every DiscontinuityWriter leaks its Fluss coordinator session/TCP connection until JVM exit. Since IngestionService creates this writer once per process the leak is currently bounded, but to make the class genuinely AutoCloseable, store the Connection as a field and close it in close().

### [medium] lines 216-221

For connection-wide events (before == null and no instrument context), this row writes last_tick_token = 0L instead of SQL NULL, and last_tick_exchange/last_tick_symbol become BinaryString.EMPTY_UTF8 empty strings because bs(null) maps to EMPTY_UTF8. The DDL defines these columns as nullable and the class Javadoc explicitly states they should be "null for connection-wide events". Downstream IS NULL checks (or treating token 0 as an unknown instrument) will misclassify connection-wide discontinuities as instrument-specific. Return null from bs() for null input (as QuarantineWriter/SafetyHaltWriter do) and pass null for instToken when no instrument context is available.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java

### [high] lines 115-121

Per-slot frame recency check will go stale during healthy steady-state operation, making isReady() permanently false ~15s after the last ACTIVE bridge event. `slot.lastFrameNanos` is only written by `updateSlot(...)`, and the sole caller (`IngestionService.processBridgeEvent`) passes a fresh timestamp only when a bridge lifecycle event arrives with state ACTIVE (line 758: `active ? System.nanoTime() : 0L`). The Go bridge emits no periodic lifecycle events while a slot is healthy — ticks flow as NDJSON lines that update only the *global* `setLastFrameReceived(...)` timestamp, never the per-slot one. So in steady state no new bridge event arrives, `System.nanoTime() - slot.lastFrameNanos` exceeds the 15s timeout, and `isDataReady()` (hence `isReady()`) flips to false even while frames keep flowing. Consider refreshing `lastFrameNanos` on all ACTIVE slots inside `setLastFrameReceived(...)`, or feeding actual per-tick arrival timestamps into `updateSlot(...)` so the per-slot recency reflects real frame flow.

### [low] lines 136-139

`isFrameRecent()` treats the default value 0 of `lastFrameReceivedNanos` as a valid timestamp. `System.nanoTime()` has an arbitrary (boot-time) origin; if it is still within 15s of its origin (e.g. a freshly booted host or container), `System.nanoTime() - 0` is `< FRAME_STALE_TIMEOUT`, so the probe reports a 'recent frame' before any frame has ever arrived. The check should treat 0 as an explicit 'never received' sentinel and return false.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java

### [high] lines 118-121

When all NTP servers are unreachable and `required=false` — which is the default, since `CLOCK_CHECK_REQUIRED` defaults to `false` in `IngestionConfig` — this fallback unconditionally marks the check as passed (`lastCheckPassed=true`, offset reported as 0) as long as the local wall clock is merely after 2024-01-01. This bypasses the documented `CLOCK_OFFSET_LIMIT_MS` (100 ms) readiness contract entirely: a clock skewed by hours or days still passes `HealthProbe.isClockOk()`, so ingestion starts writing ticks with wrong timestamps into raw_table while diagnostics report `clock_ok=true, clock_offset_ms=0`. Since NTP UDP/123 is often blocked in containers, this degraded path is the likely default in practice. Consider fail-closed readiness (report clock as 'unverified' instead of passing), a much tighter fallback sanity bound, and/or surfacing the degraded state so operators can distinguish a verified offset from a guessed one.

### [medium] lines 172-175

queryNtp() accepts any datagram without validating that it is a real NTP server response: it never checks `response.getLength() == NTP_PACKET_SIZE`, the response LI/VN/Mode byte, or that the origin timestamp (bytes 24-31) echoes the client's transmit timestamp. Because the request never sets bytes 40-47 (transmit timestamp stays zero), origin-timestamp matching is impossible, and a short/truncated datagram leaves bytes 40-47 at zero — producing `ntpMillis ≈ -2.2e12` and a bogus offset; a spoofed or corrupted packet can likewise force a false pass/fail of the ingestion readiness gate. Set a real (current time/random) transmit timestamp in the request and validate packet length + mode + origin timestamp before computing the offset.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java

### [high] lines 151-152

The CompletableFuture returned by writer.append(row) is assigned to an unused local and never observed (neither awaited nor given a failure handler). Fluss appends are asynchronous, so if the append completes exceptionally, the error is silently swallowed — the quarantine evidence is lost without the ERROR log promised in the class contract ("failures are logged at ERROR and must not block the ingestion pipeline"). Note the sibling RawTickWriter awaits the future with future.get(...), which is the correct pattern in this codebase. Since this writer's entire purpose is preserving rejection evidence, attach a whenComplete/exceptionally handler (or await with a timeout) so async failures are logged and not silently dropped.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java

### [high] lines 133-136

The `CompletableFuture<AppendResult>` returned by `writer.append(row)` is discarded (note the `@SuppressWarnings("unused")`). Fluss appends are asynchronous — this try-catch only catches synchronous exceptions, so any async write failure is silently lost, while `LOG.info("wrote ...")` is emitted before persistence is confirmed. This is the safety-halt path consumed by the Signal job: a missed/undelivered halt request means an unsafe state can go un-halted with no alert. Additionally, the caller dedups on the returned `halt_request_id`, so a failed append is never retried. Await the future (e.g. `future.get(timeout, TimeUnit.MILLISECONDS)` as RawTickWriter does) or attach an `exceptionally`/`whenComplete` handler that at minimum logs at ERROR, and only log success after the future completes.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

### [high] lines 141-143

close() sets `closed = true` before calling flush(), but flush() begins with `if (closed) return;`. The documented "final flush before shutdown" therefore never executes — every shutdown silently discards up to 10s of buffered metrics, defeating the class's lifecycle contract and the caller's expectation (IngestionService.shutdown() relies on metrics.close() to trigger a final flush). Reorder so the final flush runs before the closed flag is set.

### [high] lines 399-399

OTLP/HTTP JSON payload is not spec-compliant and will be rejected by strict collectors: (1) `asDouble` is emitted as a JSON string ("asDouble":"...") but protobuf JSON requires double fields to be JSON numbers — since process.fd_usage_percent and every slot capacity gauge use this, each flush contains an invalid value; (2) sum data points omit the required `aggregationTemporality` and `isMonotonic` fields; (3) the histogram always sends `bucketCounts:[0,0,0,0]` with `explicitBounds:[0,0,0,0]` (explicitBounds must be one element shorter than bucketCounts, and bucket totals must reconcile with `count`), so the histogram is internally inconsistent even when count/sum are non-zero. Emit `asDouble` as a number and populate temporality/monotonic and the histogram fields correctly.

### [medium] lines 158-162

Thread-safety race on the latency ring buffer: `latencyRingPos` is read-check-then-written and `latencyRing` is a plain long[] shared with the background scheduler thread, which concurrently reads and resets the same state in flush()/computeLatencyPercentiles(). Concurrent recorders can write the same slot (losing a sample), and a flush can copy a half-written snapshot or reset the position while a producer writes, corrupting the p50/p90/p99 approximations. recordAppendLatencyMs() runs on the ingestion reader thread while flush() runs on the scheduler thread, so this is a genuine data race. Synchronize access (lock both the record and percentile-computation paths) or use an atomic/reset-free approach.

### [medium] lines 440-440

esc() escapes only backslash and double-quote, not JSON control characters. Reason strings passed to incrementDecodeError (e.g., broker quarantine reasons or exception messages) can contain \n, \t, \r or other control characters, which will produce invalid JSON and make the entire 10-second POST fail. Escape all characters below 0x20 (\n, \r, \t, \u00XX) as well.

### [medium] lines 241-246

Two robustness defects in the flush path: (1) reportHealth(true) is invoked even when the collector returns HTTP >= 400, so rejected/malformed payloads are still reported as healthy and otel.collector.healthy stays 1, masking real failures (only the exception path reports false) — health should reflect the actual HTTP status; (2) conn.disconnect() is not in a finally block, so when getOutputStream()/getResponseCode() throw (collector down) the connection is never explicitly released and the 10s retry loop can accumulate unclosed sockets/fds over a long outage. Move disconnect into a finally block.

### [low] lines 75-76

The latency buffer never wraps: once 1024 samples are collected between flushes, every further recordAppendLatencyMs() call is silently dropped until the next flush resets latencyRingPos. Under high append rates the p50/p90/p99 window covers only the first 1024 samples of each 10s interval, biasing percentiles and hiding tail-latency spikes that occur later in the window. Size the buffer to cover the expected window or make it a true overwriting ring.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

### [high] lines 130-134

On timeout, the append future returned by rowConverter.append() may still be in-flight, but tracker.onAppendFailure(rowBytes) is called immediately, decrementing the pending counters before the append has actually completed. This directly violates the AppendTracker contract ('Pending counters decrease only after append completes') and abandons the future without cancellation. Under sustained Fluss latency, timed-out appends keep running in the Fluss client's background while the tracker under-counts in-flight work, so the backpressure accounting no longer reflects reality and client-side memory/connection usage can grow. Consider canceling the future (future.cancel(true)) and deferring the counter release until the future actually completes (e.g., future.whenComplete) so accounting stays accurate.

### [medium] lines 214-215

close() never invokes rowConverter.close(). FlussRowConverter extends AutoCloseable, and RealFlussRowConverter.close() is the only place that releases the underlying Fluss Connection (and its sockets). The Javadoc here even claims it 'force-closes the connection', but the underlying connection is leaked on every close, which can exhaust connections/sockets across reopen cycles. Close the converter after the drain completes.

### [medium] lines 102-107

write() performs a check-then-act on the volatile closed flag: the entry check can pass, then a concurrent close() (e.g., from the shutdown-hook thread while the main read loop is still calling write()) sets closed=true and drains pending counters, and only afterwards does this thread reach tracker.tryAccept/append. A late append can be issued after close() has returned, making shutdown non-hermetic and leaving that append unaccounted. Re-check closed after reserving capacity (and release the slot if closed), or synchronize write()/close() on the same lock.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RetryClassifier.java

### [high] lines 72-79

The retryable checks return RETRYABLE immediately, short-circuiting the cause-chain walk before deeper fatal causes can be inspected. If any wrapper exception in the chain (e.g., an ExecutionException/RuntimeException whose message contains "connection", "timeout", "leader", or "coordinator") matches a retryable pattern, the loop never reaches an underlying fatal cause such as AuthenticationException, AccessControlException, or TableNotExistException. In RawTickWriter.write() this results in a permanent failure being retried MAX_RETRY_ATTEMPTS times and ending as FAILED instead of FATAL, so the safety halt gate is never opened — masking permanent failures in the ingestion pipeline. Fatal patterns should take precedence across the *entire* cause chain before deciding RETRYABLE; e.g., collect a `retryable` flag during the walk and only return RETRYABLE after the full chain has been checked.

### [medium] lines 73-73

The substring check `name.contains("Retriable")` has two problems: (1) it uses the uncommon spelling "Retriable" while the javadoc and the rest of the codebase use "Retryable", so a `RetryableException` would not match; (2) `contains` also matches negative names — a class named `NonRetriableException` (or `NonRetryableException`) contains/relates to the same token and would be classified RETRYABLE, which is exactly backwards for a permanent failure. Consider matching the standard spelling and explicitly treating `NonRetryable`/`NonRetriable` prefixes as FATAL.

## code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java

### [high] lines 64-64

The class Javadoc advertises this as a "plain WebSocket server" (port 8888, ws:// scheme), but the implementation only opens a raw TCP `ServerSocket` and never performs the HTTP Upgrade/WebSocket handshake or frame encoding. Any real WebSocket client (e.g. gorilla/websocket used by the Go Arrow bridge, or a browser) will fail the handshake because the server never responds with `101 Switching Protocols` — it just starts writing raw bytes. Either implement an actual WebSocket endpoint (handshake + frame codec) or correct the documentation and log message to describe a plain TCP newline-delimited server.

### [high] lines 137-137

The documented message contract is "newline-delimited JSON" with one tick object per line, but `generateTicks()` serializes the whole `List<Map<String,Object>>` batch via `mapper.writeValueAsString(batch)`, i.e. one JSON **array** per line. Any downstream parser that reads line-by-line and expects a single tick object per line (as the Javadoc and typical NDJSON consumers do) will fail to deserialize the array. Serialize each tick individually per line, or update the documented contract to reflect array-per-line.

### [medium] lines 109-109

`tickRatePerSec` is misleading: with the fixed 10 ms scheduler interval (100 batches/sec) and `batchSize = ceil(instruments.size() * tickRatePerSec / 100.0)`, the server actually emits ~`instruments.size() * tickRatePerSec` ticks/sec (e.g. 50×20 = 1000/s), not `tickRatePerSec`. The startup log `({} instruments, {} ticks/s)` therefore understates the true total rate by a factor of `instruments.size()`. Additionally, `Math.max(1, ...)` forces at least 100 ticks/s even for very low configured rates. Clarify whether the parameter is per-instrument or total and adjust the formula/log accordingly.

### [low] lines 96-104

If `BufferedWriter`/`OutputStreamWriter` construction throws IOException, the accepted client `Socket` is never closed, leaking the open connection (no close in the catch block and the session was never added to `clients`). Also, the initial `ClientSession(client, null, ...)` with a null writer is a throwaway object only used to carry `connectedAt`. Close the socket on failure and simplify by constructing the writer directly.

## code/common/invariants/LiveMoneyGuard.java

### [high] lines 15-15

This file lives at `code/common/invariants/LiveMoneyGuard.java`, but the `common` Maven module compiles only the default source root `code/common/src/main/java` (confirmed: `code/common/pom.xml` has no `<sourceDirectory>` override or build-helper-maven-plugin, and all other module classes sit under `src/main/java/com/trading/common/...`). As a result `LiveMoneyGuard` and `LiveMoneyStopCondition` will not be compiled or packaged by `mvn package` (`make build`), so the guard silently disappears at runtime. Move both files under `code/common/src/main/java/com/trading/common/invariants/` and align the package with `com.trading.common.invariants`, or register the directory as an additional Maven source root.

### [medium] lines 161-162

All ten fact booleans default to `false` in the builder, so an omitted setter is indistinguishable from an explicit `false`. Because `evaluate()` approves live money whenever the triggered set is empty, a caller who forgets to supply one fact (e.g., `criticalRiskOpen`) will silently receive approval instead of a halt. For a safety-critical guard this should fail closed: validate in `build()` that every field was explicitly supplied (track a set flag per field), or require all ten values via a constructor.

### [low] lines 181-182

`evaluate` dereferences `facts` without a null check (`facts.triggered()` would throw an NPE at the call site). For a safety gate, failing fast with an explicit message via `Objects.requireNonNull(facts, "facts")` makes the cause of the failure clear.

## code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java

### [high] lines 20-20

Ambiguous time unit: the field comment says the standard feed provides epoch seconds while HFT provides epoch nanoseconds, but the model exposes no feed source or time-unit discriminator — Mode is insufficient since LTP/LTPC can appear on either feed. A downstream consumer (15s candle bucketing, discontinuity detection, dedup) cannot reliably tell seconds from nanoseconds and would silently misinterpret the value (a 10^9/10^3 error). Note the Go bridge already normalizes every timestamp to epoch ms (`ts_ms`). Recommend storing a single unambiguous unit (e.g., epoch ms) or adding an explicit unit/feed field so the value is self-describing.

### [medium] lines 15-15

Mode declares LTPC, QUOTE and FULL, but the fields only capture last-trade data (lastTradedPrice, lastTradedQty, volume, averagePrice, openInterest) — there is no previous-close field for LTPC, no best bid/ask for QUOTE, and no order-book depth for FULL. The Go bridge emits close_paise, bid_px[5], ask_px[5], bid_qty/ask_qty, and raw_table_1 has bid_price_paise/ask_price_paise columns, so decoding a QUOTE/FULL packet into this model would silently drop that data at the model boundary. Either restrict Mode to the representable values or add the missing fields (close, bid/ask price+qty, depth).

## code/common/src/main/java/com/trading/common/identity/IdentityModel.java

### [high] lines 74-80

Inconsistent value semantics: only InstructionId, ClientOrderRef, BrokerOrderId, InstrumentToken, and ExchangeId override equals()/hashCode(). The remaining 11 identity classes (CandidateId, ExecutionAttemptId, TradeContextId, PositionId, PostbackEventId, AccountScopeId, PortfolioId, ExecutionPartitionId, ReservationId, HaltRequestId, ActionId) fall back to reference equality. For a canonical identity model whose entire purpose is unambiguous value-based identity, two instances wrapping the same logical ID will not be equal — breaking HashSet/HashMap lookups, deduplication, state comparison, and cross-service matching, which could silently produce duplicate or missed operations. Since Java 17 is configured in the parent POM (maven.compiler.release=17), consider converting these classes to records to guarantee uniform equals/hashCode/toString, or at least add equals()/hashCode() to every identity type.

### [medium] lines 16-16

Constructors accept null/blank values without validation. For the String-based classes, equals()/hashCode() call value.equals(...)/value.hashCode(), so a null value throws NPE at the first comparison or collection use, while toString() silently renders "null". For ClientOrderRef, the javadoc documents a hard limit of "max 16 chars for Arrow remarks", yet no length check exists — an oversized value would only fail at the broker boundary during submission after significant pipeline work. Consider validating non-null/non-blank (and enforcing the 16-char limit for ClientOrderRef) in the constructors, e.g. throw IllegalArgumentException on invalid input, applied consistently to all identity types.

### [medium] lines 51-53

InstrumentToken performs no range/sign validation even though it is documented as the join key across market/postback/order books (equals Arrow Token, an int32). A zero or negative token is accepted without complaint and would silently produce wrong joins in raw_table, candle generation, or position tracking, making schema-level integrity issues hard to diagnose. Consider rejecting invalid tokens (e.g. <= 0) at construction time.

## code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java

### [high] lines 149-149

Logic error: when `to == PREPARED` (the only enum value falling into this `default` branch), `legalSources` becomes `Set.of(from)`, so `legalSources.contains(from)` is always true. Combined with the earlier same-phase idempotent check (`from != to`), this makes *every* transition back into `PREPARED` legal — from `ACCEPTED`, `REJECTED`, `CANCELLED`, or `UNKNOWN` — directly contradicting the comment "PREPARED has no incoming transitions defined". This can lead to re-processing, duplicate submissions, or a corrupted audit trail. The `default` branch should return an empty set.

### [medium] lines 67-68

The stale-epoch rejection records `GateState.HALTED` as the `from` state instead of the actual `currentState`. This misrepresents the audit trail (an operator would see the gate as already HALTED even if it was ENABLED) and, more importantly, breaks `requiresHalt()`: for a gate actually in `ENABLED` receiving a stale request, `from == GateState.ENABLED` would be false, so the process would not halt even though the epoch mismatch indicates a lost lease / delayed message. Use `currentState` as the source state.

## code/common/src/main/java/com/trading/common/observability/Json.java

### [high] lines 18-24

The shared mutable `first` flag is reset to `true` at the start of every `obj(...)`/`arr(...)` block and is never restored when the block returns, so the enclosing container's separator state is corrupted by any nested structure. For example, `arr(a -> { a.obj(o -> o.kv("a","b")); a.obj(o -> o.kv("c","d")); })` produces `[{"a":"b"}{"c":"d"}]` (missing comma), and `obj(w -> { w.kv("x",1); w.arr(a -> a.kv("y",2)); })` produces `{"x":1["y":2]}`. Any non-flat JSON built with this class is malformed. Fix by tracking per-container separator state (e.g. a stack of booleans) and having `obj`/`arr` emit a separator from the parent container before opening their bracket.

### [medium] lines 55-58

`escape(null)` returns `""`, so `kv(k, null)` silently serializes a null value as an empty JSON string (`"k":""`) instead of `null` or an omitted field. In telemetry/OTLP records an absent attribute and an empty string are semantically different; this silently loses null information. Either have `kv` write `null` for null values or skip the pair entirely.

## code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java

### [high] lines 30-31

`event.level` and `event.message` are concatenated into the JSON document without going through `escapeJson()`, and the same applies to `event.service` in the `service.name` attribute above. `message` is free-text log content that can easily contain double quotes, backslashes, or control characters (e.g. a message like `Order "AAPL" rejected`); in that case the emitted OTLP/JSON document is syntactically invalid and the OpenTelemetry Collector may reject the whole log record. Wrap all interpolated dynamic values with `escapeJson()`.

### [high] lines 53-54

Same JSON-escaping gap in `emitAlert()`: `service`, `host`, `vmId`, `environment`, `correlationId`, `category`, `message` and `alert.name()` are interpolated into the document raw, while only `alert.condition` goes through `escapeJson()`. Since `message` is free text and the identity/correlation fields can contain arbitrary characters, a quote or backslash will corrupt the `trading_alerts` record and cause alert loss in OpenObserve. Wrap every interpolated dynamic value with `escapeJson()`.

### [medium] lines 74-78

`escapeJson()` only escapes `"`, `\`, `\n` and `\r`. Per RFC 8259 all control characters in the range U+0000–U+001F (including tab, backspace, form feed) must be escaped in JSON strings. A log message or alert condition containing a tab or other control char will still produce invalid OTLP/JSON. Add handling for `\t`, `\b`, `\f` and a `c < 0x20` → `\uXXXX` fallback.

## code/common/src/main/java/com/trading/common/version/VersionGate.java

### [high] lines 23-26

Placeholder versions bypass the gate. The class contract states a placeholder is not accepted for a live-money path, and PlaceholderVersions declares placeholders as "intentionally NOT real versions". However, requirePinned only rejects null/blank/"latest", so a placeholder like FLINK_VERSION_TO_BE_PINNED passes through. Because requireAllPinned (the CI gate) delegates to requirePinned, CI can proceed with unresolved placeholder versions — inconsistent with version_matrix_verify.py which treats TO_BE_PINNED as a pin blocker. Add a PlaceholderVersions.isPlaceholder check inside requirePinned so both the runtime and CI gates reject placeholders.

### [medium] lines 27-27

requirePinned trims the value only for the 'latest' comparison but returns the original, untrimmed string. A version such as " 1.2.3 " passes the gate and is returned with leading/trailing whitespace, which can silently break downstream exact-match comparisons, version matrix lookups, or path construction. Return the trimmed value instead.

### [low] lines 48-48

requireAllPinned dereferences entries without a null check; a null list would throw a raw NullPointerException instead of a descriptive error in a safety-critical CI gate. Add a defensive null check before iterating.

## code/run-ingestion-full.sh

### [high] lines 146-150

In this background pipeline, `$!` is the PID of the *last* pipeline element (`tee -a`), not the JVM. Consequently `wait "$PID"` returns `tee`'s exit status — a JVM crash (non-zero exit) is reported as a successful run because `tee` exits 0 after EOF — and the TERM/INT trap plus the EXIT cleanup send SIGTERM to `tee` instead of the JVM, orphaning the ingestion JVM and its child arrow-bridge after `kill` or Ctrl+C. Capture the JVM's PID directly via process substitution so `$!`, `wait`, and signal forwarding all target the actual JVM, e.g.: `"$JAVA_BIN" ... > >(tee -a "$RUN_LOG") 2>&1 &`. (Note: `start-all.sh` runs the same pipeline in the foreground and is not affected.)

### [medium] lines 114-117

`cleanup_stale_bridges` only matches `arrow-bridge` binaries. If a previous run left the `IngestionService` JVM alive (e.g. after the wrong-PID/tee issue above, or a `kill` that only hit the pipeline), this launcher starts a second JVM against the same Fluss/Arrow resources, producing duplicate ingestion and double writes. `IngestionService` has no PID-file/lock protection against concurrent instances, so the launcher should also detect a running `com.trading.ingestion.IngestionService` process (e.g. include it in the `pgrep` pattern or maintain a PID file) before starting.

### [medium] lines 57-57

The token list is derived with `cut -f4 | tail -n +2 | paste` without validating the expected count (1,024), the header row, or CRLF/BOM/quoted fields. The Go bridge (`loadTokensFromCSV`) trusts `ARROW_INSTRUMENT_TOKENS` first and only falls back to its own robust CSV parsing when the env list is empty, so a partially-wrong list silently produces missing/incorrect subscriptions (Java only notices later via the 30s subscription-completeness check). Validate the extracted token count equals the expected 1,024 and strip CR/BOM before exporting.

## code/smoke-test.sh

### [high] lines 7-11

SmokeTest calls IngestionConfig.validate(), which treats ARROW_MAX_EVENT_AGE_MS and ARROW_MAX_FUTURE_EVENT_SKEW_MS as required keys with no default (IngestionConfig.validateFrom lines 170-171). These are not set here, so SmokeTest will throw IllegalStateException at the first config-validation step ("ARROW_MAX_EVENT_AGE_MS is required but not set") and the smoke test can never run. run-ingestion-full.sh already documents these as required with no code default. Add both exports with the approved values (5000/2000).

## Makefile

### [medium] lines 38-38

The `-o` (offline) flag makes `make build`/`make test` fail on any machine whose local Maven repository (~/.m2) is not already populated (fresh checkout, CI, or clean container), because Maven will not download dependencies. Note the ingestion Dockerfile explicitly runs `mvn dependency:go-offline` first to seed the repo; the Makefile path has no such step. Either drop `-o` or document the prerequisite.

## code/01_platform/01_docker/ddl-init.sh

### [medium] lines 31-35

This script claims to create the `default` database (header comment, this log line, and the 'done' message), but it never issues any Fluss/Database command — it only waits for a TCP connection to the coordinator port, sleeps 5s, and exits 0. As written it is a no-op init container that gives false confidence that the database exists. Note that the real creation is done by DdlBootstrap.ensureTables() in the ingestion service (or by ddl_apply.py), and this script is not wired into docker-compose at all. Either perform the actual creation here (call the Fluss Admin API / DdlBootstrap and verify `listDatabases` contains `default`), or remove the misleading 'creating database' output and document that database bootstrap happens inside the ingestion service. Additionally, a TCP connect is not a readiness check — the coordinator can accept connections before it can serve admin/DDL requests, so the fixed `sleep 5` is a fragile heuristic for gating downstream work.

### [low] lines 14-14

The `${COORDINATOR%%:*}` / `${COORDINATOR##*:}` split assumes exactly `host:port`. If the argument is passed without a port (e.g., `fluss-coordinator`), both expansions return the whole string and the probe path becomes `/dev/tcp/fluss-coordinator/fluss-coordinator`, so the wait can never succeed and the loop ends with a misleading timeout error; IPv6 addresses break the same way. Parse defensively (split on last `:`, then verify both host and a numeric port are non-empty) so a misconfigured argument fails fast with a clear message instead of hanging for MAX_WAIT.

## code/01_platform/02_sql/ddl/05_signal_candidates.sql

### [medium] lines 30-31

This table is declared as an immutable LOG (no primary key), and the schema manifest registers it as `table_kind: LOG`. However, `superseded_by_candidate_id` can only be populated by updating an already-appended row (candidate A is written first, and its `superseded_by_candidate_id` can only be known when a later candidate B supersedes it). Append-only LOG tables do not support in-place updates, so this column will always be NULL — the supersession chain is only discoverable in the reverse direction via `supersedes_candidate_id` on the newer row. Either drop `superseded_by_candidate_id` and reconstruct chains from `supersedes_candidate_id`, or if the field is truly required, change this to a KV table (e.g., `PRIMARY KEY (candidate_id)`) so the row can be updated when supersession occurs.

## code/01_platform/02_sql/ddl/08_fills.sql

### [medium] lines 16-17

The header declares "Scope: account_scope_id" and this table feeds the encrypted 7-year audit lake, but the schema has no `account_scope_id` column. The analogous LOG audit table `Execution_Audit` includes `account_scope_id STRING NOT NULL` (as do Trade_Decisions and Positions). Without this column, fills cannot be attributed to or filtered by account, breaking per-account data isolation and weakening the compliance/audit trail the table is meant to provide. Add an `account_scope_id STRING NOT NULL` column (or drop the scope claim from the design).

### [low] lines 34-36

The header requires "Retention: ≥3 complete trading days", but `table.retention.days = '3'` is a calendar-day setting in Fluss. Over a weekend or holiday, 3 calendar days can contain only 1–2 trading days, so audit records could be expired before the stated compliance floor is met. Use a calendar-day value that guarantees at least 3 complete trading days (e.g., 5), or document that retention is calendar-based and adjust the requirement accordingly.

## code/01_platform/02_sql/ddl/13_order_correlation.sql

### [medium] lines 11-11

The primary key is `instruction_id` while `execution_attempt_id` is NOT NULL. Since a single instruction can be retried as multiple execution attempts (each attempt can produce a distinct `broker_order_id`, per the `Execution_Attempts` table keyed by attempt), this single-row KV can only hold the correlation of the latest attempt — earlier attempts' broker mapping is silently overwritten. If per-attempt correlation must be preserved for reconciliation, key the table on `(instruction_id, execution_attempt_id)`; otherwise confirm and document that this intentionally stores only the current/latest mapping.

## code/01_platform/02_sql/ddl/14_execution_audit.sql

### [medium] lines 26-26

Retention configuration contradicts the stated requirement. The header declares "Retention: ≥3 complete trading days", but `table.retention.days = '3'` enforces 3 calendar days. Since trading days exclude weekends/holidays, this cannot guarantee 3 complete trading sessions are retained (e.g., on a Monday, only Friday's data remains and Thursday's is already purged; after a multi-day holiday, even more is lost). For an immutable audit log that must be offloaded to the 7-year lake, this risks silently dropping evidence before the EOD offload runs. Either raise the retention (e.g., ≥5 calendar days to cover a weekend) or correct the header to state the actual calendar-day retention.

## code/01_platform/02_sql/ddl/16_postback_quarantine.sql

### [medium] lines 27-27

`table.retention.days` is not a standard Fluss log-table option — the previous version of this DDL used `table.log.ttl = '7d'`, which is the actual Fluss option (default TTL is also 7 days). If the pinned Fluss 0.9.1-incubating server does not recognize `table.retention.days`, the CREATE TABLE may be rejected or, more likely, the option is silently ignored so the intended retention policy is not explicitly enforced. Since every DDL in this update shares this option, verify the option name against the pinned Fluss version before the DDL apply gate rather than assuming it takes effect.

## code/01_platform/02_sql/ddl/18_safety_halt_requests.sql

### [medium] lines 18-18

halt_request_id is a deterministic SHA-256 of the transition tuple, but this is an append-only LOG table with no primary key, so the storage layer does not enforce uniqueness. In the caller path (IngestionService.emitSafetyTransition), SafetyHaltWriter.write() is invoked unconditionally and the in-memory safetyEmitted dedup set only gates the log message — it is also lost on process restart. So if the same transition tuple is re-emitted (duplicate bridge lifecycle event, restart, or ack-loss retry), a second row with the same halt_request_id is appended. Since downstream consumers treat this table as a control signal, consider declaring halt_request_id as the PRIMARY KEY (KV table, e.g. like Execution_Gate) so re-emission becomes an idempotent upsert, or ensure dedup happens before the append.

## code/01_platform/02_sql/ddl/20_instruments.sql

### [medium] lines 19-23

The header says this table retains "current and prior instrument manifest versions", but the primary key is only `instrument_token`. This is a one-row-per-instrument KV table: a Fluss upsert on the same key overwrites the previous row, so prior manifest versions can never be retained — `manifest_version` would always reflect only the latest version, and the header's retention claim is unmet. If history is actually required, use a composite key `PRIMARY KEY (instrument_token, manifest_version) NOT ENFORCED`. If only the current version is needed (matching SCH-22's "one approved manifest version" model and `InstrumentManifestLoader` always writing version 1), update the header comment to "current state only" to avoid a misleading contract.

## code/01_platform/04_scripts/cep_guard.sh

### [medium] lines 11-11

Silent false-negative: if `$ROOT` is a typo or the script is invoked from an unexpected working directory, `grep -r` on a nonexistent path emits nothing to stdout (stderr is suppressed by `2>/dev/null`) and exits non-zero, which is swallowed by `|| true`. The script then prints "OK: no Flink CEP references found" and exits 0, so the guard would silently pass without scanning anything — defeating its purpose as a CI enforcement gate. Validate that the scan root actually exists (and is a directory) before running grep, failing the build otherwise.

## code/01_platform/04_scripts/version_matrix_verify.py

### [medium] lines 46-49

`yaml.safe_load(fh)` returns `None` for an empty or comment-only file, and raises `yaml.YAMLError` for malformed YAML. Neither case is handled here: an empty file crashes with `AttributeError: 'NoneType' object has no attribute 'get'`, and a malformed file propagates a raw traceback. For a CI structural gate, this produces an opaque error instead of the intended clear failure. Guard the document type and catch the parse error.

### [medium] lines 55-56

The loop assumes every row is a dict and that `proposed_version` / `evidence_owner` / `evidence_method` / `compatibility_class` are strings before calling `.strip()` / `.upper()`. YAML parses unquoted scalars like `2.2`, `true`, or `2024-01-01` into float/bool/date, so `.strip()` raises `AttributeError`, and a non-dict row crashes at `row.get`. Since this validator exists precisely to flag bad entries, a crash obscures the offending row. Coerce defensively or validate types explicitly.

## code/02_services/01_ingestion/go-bridge/faketool/main.go

### [medium] lines 44-45

Data race on the `connections` counter. Each WebSocket handler runs in its own goroutine under net/http, so `connections++` and `idx := connections` are unsynchronized concurrent accesses. Under concurrent or rapidly reconnecting clients (reconnect loops, multiple slots), two handlers can observe the same index or skip indices, making `-disconnect-after=N` close the wrong connection (or none) and turning the E2E disconnect scenario nondeterministic. Use `sync/atomic.Int32` (declare `var connections atomic.Int32` and read via `idx := connections.Add(1)`).

## code/02_services/01_ingestion/go-bridge/hft_slot.go

### [medium] lines 152-152

Stall detection has a blind window that defeats its purpose: `BeginConnect()` resets `lastFrame` to the zero time, and this guard requires `!s.lastFrame.IsZero()`, so a slot that reaches `SlotActive` but never receives a single frame (a completely silent/dead feed — exactly the failure stall detection exists for) will never be flagged as stalled. Slots stuck in CONNECTING/SUBSCRIBING are likewise never considered (`state == SlotActive` required). The production supervisor (runHFTEpoch) avoids this by seeding `lastFrameNanos` at subscription time, but this method as written would silently mask total feed loss if `HFTSlot` is ever wired into the supervisor. Consider seeding `lastFrame` when entering ACTIVE and treating a zero `lastFrame` after ACTIVE as stalled.

### [medium] lines 131-135

`BeginConnect()` does not honor the `closed` flag, unlike `SetState()`/`Close()`. After `Close()` sets `closed=true` and `SlotTerminal`, a late/racing `BeginConnect()` silently resurrects the slot to `SlotAuthenticating` and bumps the epoch, so a reconnect overlapping shutdown can attempt connections on a closed slot (and performs the illegal TERMINAL→AUTHENTICATING move). Additionally, `NewHFTSlot` initializes state to `SlotTerminal`, making a never-started slot indistinguishable from a closed one for readers of `State()`. Guard against `s.closed` (and consider an explicit idle state for a fresh slot).

## code/02_services/01_ingestion/go-bridge/ndjson.go

### [medium] lines 103-108

validateBridgeEvent is defined here but never invoked by production code: EmitEvent writes the event directly without validating it, and the only callers of validateBridgeEvent are the unit tests. As a result, invalid bridge events are not rejected at the source. A concrete reachable case exists: the single-socket policy path in main.go emits an `auth_failure` event with `ConnectionEpoch: 0`, which this validator (and the Java BridgeEvent constructor) would reject as `connection_epoch must be positive` — so the invalid line is written to stdout and only fails later on the Java side, where the root cause is obscured. Wire the validator into EmitEvent so invalid events fail fast with a clear Go-side error.

### [low] lines 61-64

The doc comment states the per-slot counter "resets when the process restarts (a new connection epoch begins)", but seqBySlot is keyed only by slotID and lives for the process lifetime. The reconnect loop (runReconnectLoop) advances the epoch on every reconnect without ever resetting this map, so after a reconnect `feed_sequence_local` continues from the previous epoch instead of restarting at 1. The emitted epoch on the line changes while the sequence does not — a consumer treating (connection_epoch, feed_sequence_local) as per-epoch counters would mis-detect gaps or mis-order ticks after a reconnect. Either key the counter by slotID+epoch (e.g. reset on first tick of a new epoch) or correct the comment to state that the sequence is process-lifetime and continuous across reconnects.

### [low] lines 94-99

sha256Hex documents that it returns the SHA-256 hex digest of b but special-cases empty input to return "". Combined with `json:"payload_hash,omitempty"` (and the same on raw_payload), a tick whose raw payload is empty is emitted with neither field present. The Java PayloadHashValidator then classifies it as MALFORMED_HASH (payload_hash missing) and quarantines every such tick — so the integrity contract is silently skipped for empty payloads instead of being satisfied with the (well-defined) digest of empty bytes. If an empty payload genuinely indicates an upstream problem, that's the right moment to fail loudly; either always compute the digest (remove the special case) or make an empty payload an explicit emit error rather than silently dropping both integrity fields.

### [low] lines 135-137

s = s[:512] truncates on a byte boundary and can split a multi-byte UTF-8 rune mid-sequence when a broker-supplied Reason exceeds 512 bytes. Go strings tolerate the invalid UTF-8, but json.Marshal will then replace the broken sequence with U+FFFD, corrupting the tail of the diagnostic message (e.g. CJK or emoji payloads). Impact is limited to cosmetic degradation of audit data, but the truncation should be rune-safe (e.g. loop back to a rune boundary or truncate on []rune) if multi-byte diagnostics are possible.

## code/02_services/01_ingestion/go-bridge/subscription_plan.go

### [medium] lines 71-78

The SHA-256 fingerprint is computed only from SlotID/ConnectionID/Tokens; the per-slot `Requests` partitioning and the `requestLimit` parameter are excluded. Because the request chunks are what actually get sent to the broker in `runHFTEpoch` (one `SubscribeHFTTokens` call per chunk), two plans built from the same sorted tokens/slots/connectionLimit but with a different `requestLimit` (e.g., 512 vs 256) produce identical fingerprints while issuing different subscription requests. This defeats the digest's purpose as a plan identifier for drift/change detection (the fingerprint is exposed as `SubscriptionPlan.Fingerprint` and logged at startup). Since the requests are already computed at this point, include them (and/or `requestLimit`) in the hash.

### [low] lines 44-45

`BuildSubscriptionPlan` validates that the token list is non-empty and duplicate-free but never checks the token value domain. A zero or negative int32 token passes plan construction and is distributed into real `SubscribeHFTTokens` requests, failing only at subscription time (the broker rejects it and the slot enters a terminal state). Tokens originate from `ARROW_INSTRUMENT_TOKENS` (parsed via `strconv.Atoi` and narrowed with `int32(n)`, which can also silently wrap out-of-range values) or the instrument CSV, so the failure should be surfaced here at plan-build time in this fail-fast pipeline.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go

### [medium] lines 135-137

Request bodies are assembled with `fmt.Sprintf` raw string interpolation (`userID`/`password` here, and `requestToken`/`appID` in `Authenticate`). Any credential containing `"`, `\`, or control characters produces invalid JSON, so authentication fails for otherwise-valid credentials (e.g., passwords containing quotes). Build these payloads with `encoding/json.Marshal` (a `struct` or `map`) instead of string interpolation.

### [medium] lines 83-87

`Authenticate` reports success whenever `Status == "success"` without checking that `Data.Token` is non-empty, and `AutoLogin` never verifies that `requestToken` was actually extracted from the redirect URL (or that `loginResp.Data.RequestID` is non-empty). A partial/malformed response or a redirect URL without `request-token` therefore returns a nil error with an empty token; main.go only logs `token_len` and proceeds, so all subsequent authenticated calls fail with opaque auth errors. Validate `requestToken != ""` and `authResponse.Data.Token != ""` and return an explicit error otherwise.

### [medium] lines 67-67

`Authenticate`/`AutoLogin` call `c.request`/`c.rawRequest`, which execute on `&fasthttp.Client{}` (no `ReadTimeout`/`WriteTimeout` configured in `NewClient`) and take no context or deadline. A stalled network connection blocks indefinitely: at startup `client.AutoLogin` hangs before the pipeline starts, and during reconnects `refreshAuth(ctx)` hangs the HFT read goroutine so the epoch and reconnect loop never complete (the caller's context is checked only before the call). Add a per-request timeout/context (e.g., a `fasthttp.Client` Timeout or `fasthttp.DoTimeout`) and honor the caller's context.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/hft_stream.go

### [medium] lines 81-85

Data race on the zstd decoder between Close() and the read loop. In main.go's runHFTEpoch the read loop runs in a fire-and-forget goroutine (`go stream.ReadHFTWithFrame(...)`) and `stream.Close()` is invoked from the epoch goroutine via defer without joining the read goroutine first. decodeHFTPayload does an unsynchronized check-then-use of `s.zdec` (`if s.zdec == nil { return payload }` then `s.zdec.DecodeAll(...)`), so it can observe a decoder that is being closed/concurrently niled. This is race-detector visible and calling DecodeAll on a just-closed decoder is undefined (can panic, crashing the whole bridge). Guard zdec with the existing mutex (and have decodeHFTPayload take the same lock around the check + DecodeAll), or ensure the read loop is stopped/joined before closing the decoder.

### [medium] lines 287-289

No read/write deadlines are set anywhere in this client, so a wedged TCP connection blocks indefinitely. writeJSON/WriteText hold `s.mu` across a blocking WriteMessage — the 3s heartbeat goroutine in runHFTEpoch and all subscription/unsubscription writes share this mutex, so one stalled write starves all of them. On the read side, ctx cancellation cannot interrupt `s.conn.ReadMessage()` (ctx is only checked at the top of the loop), so the documented "until ctx is done" contract is not honored and only Close() unblocks it. Worse, Close() itself writes a close frame with no deadline, so a full send buffer can hang the entire shutdown path. Recommend setting a write deadline (e.g. SetWriteDeadline(time.Now().Add(N*time.Second))) before each write and a read deadline/pong-based deadline per loop so a stalled socket surfaces an error and the reconnect loop can act.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/market.go

### [medium] lines 188-189

The historical data endpoint hardcodes the production host `https://historical-api.arrow.trade` and goes through `rawRequestAuth` with an absolute URL, bypassing the configurable `Config.BaseURL` that every other method in this file honors (via `c.request`). I verified `rawRequestAuth` does accept an absolute URL, so the request is not malformed — but the host cannot be redirected for a non-production environment (sandbox/soak/test), where these calls will still silently hit the live production historical API. Consider deriving the base from a config field or an override (e.g. `Config.HistoricalBaseURL` defaulting to the production host).

### [low] lines 196-196

`token` and `interval` are interpolated into the URL path with `fmt.Sprintf` without any path escaping (same applies to `GetInstrumentsCSV` with `segment`). The current pipeline feeds these from typed constants/manifests, but as a public SDK method the caller can pass values containing reserved characters (`/`, `?`, `#`, `%`), which would corrupt the endpoint or, if ever derived from untrusted input, enable path injection. Use `url.PathEscape` on each path segment.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/orders.go

### [medium] lines 178-181

The broker's `errorCode` and `message` are parsed and logged but dropped from the returned error — callers receive `fmt.Errorf("order placement failed")` with no rejection taxonomy. This makes it impossible to distinguish a broker order rejection (insufficient margin, invalid symbol, rate limit, etc.) from other failures, which is critical for retry/abort decisions in order workflows. The same pattern appears in `ModifyOrder`, `CancelOrder`, and `GetOrder`. Suggest including the parsed details in the returned error, e.g. `fmt.Errorf("order placement failed: code=%s message=%s", result.ErrorCode, result.Message)`.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/user.go

### [medium] lines 76-76

GetUserDetails performs an unbounded blocking HTTP request: it calls c.request(), which executes c.HTTPClient.Do(req, resp). The fasthttp.Client created in NewClient sets no ReadTimeout/WriteTimeout, and request() applies no deadline, so if the Arrow /user/details endpoint stalls after the connection is established (e.g. never sends a response), this call blocks the invoking goroutine indefinitely with no recovery. For an ingestion pipeline that must start or fail fast, this can hang startup/health paths. Suggest configuring ReadTimeout/WriteTimeout on the fasthttp client, or using a deadline-bounded call (DoTimeout/DoDeadline) so a hung endpoint returns an error instead of hanging the caller.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java

### [medium] lines 174-179

The GenericRow is fully materialized before `writer.append(row)` is invoked, so the comment 'set after append' is misleading — `ack_ts` will always be persisted as 0 in every stored row. Additionally, `thenApply` ignores the real Fluss `AppendResult` (the `result` parameter is unused) and fabricates `RawTickWriter.AppendResult(0, tablePath)`, so RawTickWriter success tracking/outcomes will always report `offset=0, partition=<tablePath>` regardless of the actual Fluss acknowledgement. Either preserve the real ack metadata (offset/partition) and remove the placeholder value, or drop the misleading ack_ts field/comment.

### [low] lines 180-184

In `exceptionally`, `ex.getCause()` can be null when the future completes with a plain (non-CompletionException) exception, in which case the original exception is dropped entirely (`new RuntimeException("Fluss append failed", null)`). Since `RetryClassifier` walks the cause chain to distinguish FATAL (e.g., schema mismatch) from RETRYABLE failures, losing the root cause would silently downgrade fatal errors to retryable attempts. Wrap the received exception directly to preserve the chain: `throw new RuntimeException("Fluss append failed", ex)`.

### [low] lines 57-63

If `connection.getTable(path)` or `table.newAppend().createWriter()` throws during startup, the already-created `Connection` is never closed, leaking client sockets and background threads in a long-running ingestion service. Wrap the setup sequence in try/catch and close the connection on any failure before rethrowing.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

### [medium] lines 314-319

The ING-1 broker-staleness detection is effectively a no-op. This check only executes inside the read loop after a new line has already arrived — during a genuine feed outage `readLine()` blocks and the check never runs. When the first post-stall frame finally arrives, this block marks the broker disconnected, but the very next lines (`lastFrameNanos = nowNanos; if (!health.isBrokerConnected()) ... setBrokerConnected(true)`) immediately flip it back to connected within the same iteration, so `brokerConnected`/`metrics.setBridgeConnected` never observably stay false. If ING-1 is meant to detect disconnects independently of the Go bridge's own 15s `feed_stalled` watchdog, it needs a background watchdog thread that sets `brokerConnected=false` based on `lastFrameNanos` without requiring a frame arrival; otherwise this block is misleading dead code.

### [medium] lines 372-374

The ING-3 Slow-Fluss pause percentage is computed against the static `AppendTracker.MAX_PENDING_RECORDS` (10,000), but the tracker is constructed with `config.maxPendingRecords`, which is configurable via `MAX_PENDING_APPEND_RECORDS` (valid range 100..1,000,000). If the configured limit is below 10,000, the 90% pause threshold (9,000) is never reached before the tracker halts at 100%, so the read-pause protection never engages; if configured higher, the pause fires far too early. Compute the percentage against the tracker's actual configured capacity.

### [medium] lines 418-420

On a bridge restart, the subscription-completeness state is not reset. `seenTokens` is declared outside the restart loop, so tokens seen by the previous process still count toward completeness, and `health.setSubscriptionComplete(false)` is only asserted once before the loop — `resetSlotsToAuthenticating()` does not touch the `subscriptionComplete` flag. If completeness had been reached in the first process, the ING-2 check is skipped entirely after restart, masking a partial subscription in the fresh process. Reset both before relaunching.

### [medium] lines 717-718

`lastTickSnapshot` is updated unconditionally even when `writer.write()` returned REJECTED, TIMEOUT, UNCERTAIN, FAILED, or FATAL — outcomes where the tick was not persisted (or, for UNCERTAIN, may not have been). This contradicts the comment "Track last accepted tick for discontinuity evidence": after a later bridge crash/reconnect, `DiscontinuityWriter` will use a snapshot referencing a tick Fluss may never have stored, corrupting gap-boundary evidence. Only update the snapshot when the append was accepted.

### [low] lines 1063-1066

`readFdLimit()` reads `/proc/sys/fs/file-max`, which is the *system-wide* FD limit, not the process's per-process `RLIMIT_NOFILE` (available under `/proc/self/limits`). The FD-usage-percent gauge is therefore computed against the wrong denominator and will appear near 0% even as the process approaches its real FD cap, hiding genuine FD exhaustion. Parse `RLIMIT_NOFILE` from `/proc/self/limits` instead.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/StubFlussRowConverter.java

### [medium] lines 48-49

This stub unconditionally acknowledges every append as successfully persisted (with a locally incremented offset) while discarding the packet entirely. Since this class lives in the production source tree (src/main/java) and the production entry point (IngestionService.main → FlussClientAdapter.connect) already returns the real converter, this is currently unreachable — but if it is ever wired into the ingestion path it will silently count every tick as durably stored without writing anywhere, causing unrecoverable data loss while misleading the uncertainty journal and backpressure accounting. Consider relocating the stub to test scope or adding a hard guard (e.g., fail-fast when a stub is requested under a production profile) so it can never be selected accidentally.

### [medium] lines 41-43

The volatile `closed` field is written by `close()` but never read, and `append()` does not check it — so `close()` has no observable effect and appends submitted afterwards still succeed. This breaks the AutoCloseable lifecycle contract: RealFlussRowConverter.append() returns `CompletableFuture.failedFuture(new IllegalStateException(...))` once closed, so the stub should mirror that to prevent writes-after-close from being acknowledged as persisted (which could mask shutdown-ordering bugs in the writer).

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java

### [medium] lines 170-171

requiredLong() accepts 0 for ARROW_MAX_EVENT_AGE_MS / ARROW_MAX_FUTURE_EVENT_SKEW_MS (only negatives are rejected). In IngestionService.classifyFreshness() the check is `receiveTsMs - tsMs > maxEventAgeMs`, so maxEventAgeMs=0 would classify any tick whose receive time is even 1ms after the broker timestamp as STALE and quarantine it — a missing digit or bad value would silently kill the entire market-data feed, which is exactly what this fail-fast startup validator exists to prevent. Enforce a positive minimum (e.g. >= 1) and/or an upper sanity bound, and fix the message to say 'long' instead of 'integer'.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/Instrument.java

### [medium] lines 18-18

The Builder does not validate `instrumentToken` (it defaults to 0) before `build()`, yet `equals()`/`hashCode()` are based solely on `instrumentToken`. Any instrument built without an explicit token — or parsed from a manifest row containing token `0` (which `Long.parseLong` in the loader accepts) — collapses to the same identity and silently overwrites/collides with other entries in hash-based collections. Since the class contract states every field must be validated, `build()`/the constructor should reject `instrumentToken <= 0` (and a default `manifestVersion` of 0) with an explicit exception rather than silently accepting them.

### [medium] lines 22-22

`builder.lotSize > 0 ? builder.lotSize : 1` silently coerces any non-positive lot size (0, negative, or omitted) to 1. This masks invalid manifest data and contradicts the class contract that every field must be validated — a bad lot size is silently turned into a plausible-looking value that will be used in downstream quantity calculations with no signal to monitoring. Consider rejecting invalid values (e.g., throw IllegalArgumentException) or explicitly documenting the normalization as intended behavior.

### [low] lines 19-19

The constructor rejects only `null` for `tradingSymbol`/`exchange`; empty or whitespace-only values pass validation even though these are documented as required routing fields. The manifest loader can produce an empty `tradingSymbol` when the CSV column is missing, so a blank routing symbol can reach lookups/routing logic. Consider rejecting blank strings (e.g., `isBlank()` check) or trimming plus validating non-empty in `build()`.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java

### [medium] lines 88-88

NPE risk in the shutdown path: `journalPath.getParent()` can return `null` when the configured path is a bare filename (e.g. `UNCERTAINTY_JOURNAL_PATH=journal.jsonl`). `Files.createDirectories(null)` throws `NullPointerException`, which is a `RuntimeException` and is NOT caught by the `catch (IOException e)` block, so it escapes `write()` and aborts the rest of `shutdown()` (metrics flush, drain). Note `ensureWritable()` already null-checks the parent gracefully — this method should do the same for consistency and robustness.

### [low] lines 150-150

`escape()` only escapes backslash and double-quote. Control characters such as newline (`\n`), carriage return (`\r`), and tab (`\t`) are emitted verbatim, which can break the JSONL invariant (an embedded newline creates an extra physical line and malformed JSON). `instanceId` and `shutdownReason` are public API fields; if they ever carry multi-line text (e.g. an exception message) the journal becomes unparseable for resume-after-restart. Consider escaping control chars or using a proper JSON writer.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java

### [medium] lines 116-117

`totalBytesAccepted` is declared with a getter and consumed at shutdown by `IngestionService.shutdown()` (uncertainty journal), but it is never incremented anywhere in this class. As a result `totalBytesAccepted()` always returns 0, so the journal and any byte-volume monitoring record zero accepted bytes. Increment it by `recordBytes` on the successful accept path (alongside `totalAccepted`).

### [low] lines 83-86

The `halted` check at the top of `tryAccept` is a non-atomic check-then-act: a thread can pass `if (halted)` and then, before it increments the counters, another thread hits the 100% limit and sets `halted = true`. The first thread will still accept and return `true` for a record after the tracker is halted, violating the documented '100% → stop accepting broker data' contract and allowing the backlog to exceed the safety bound. The class documents itself as 'Thread-safe'; if concurrent producers are ever used (the current main-loop mutation is single-threaded, but the class contract claims otherwise), the halt check should be performed atomically with the capacity reservation (e.g., a synchronized block or CAS loop on the halted flag).

### [low] lines 109-113

In the warning branch the pluggable `BackpressureListener` is invoked synchronously while the pending counters are already incremented but before `totalAccepted.incrementAndGet()`. If a listener implementation throws a runtime exception (e.g., a logging/telemetry failure in a future listener), the exception propagates to the caller: `RawTickWriter.write()` will not submit the record, yet `pendingRecords`/`pendingBytes` remain incremented — a phantom pending entry that inflates backpressure and can trigger a spurious halt. Guard the listener invocation (catch-and-log) or document that listeners must not throw.

## code/02_services/01_ingestion/src/main/resources/log4j2.xml

### [medium] lines 21-21

This CONSOLE_PATTERN renders a non-empty `correlation_id` three times with no separator. `%equals{%mdc{correlation_id}}{}{}` outputs the MDC value whenever it is non-empty, `%equals{%mdc{correlation_id}}{ - cid=}{}` always outputs the value again (a real correlation ID never equals the literal ` - cid=`), and the trailing `%mdc{correlation_id}` prints it a third time. The result for a correlation id `abc` is `abcabcabc`, making console logs unreadable and preventing the intended ` - cid=` prefix from ever appearing. Replace the three expressions with a single conditional rendering.

## code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java

### [medium] lines 58-58

The Flink job graph is empty — no source, transformation, or sink is added before `env.execute()`. Flink's `StreamGraphGenerator` rejects an empty topology at submission with `IllegalStateException: No operators defined in streaming topology. Cannot execute.`, so this job will fail on startup instead of running as a safe no-op (and `submit-jobs.sh` submits `babysitter` as one of the expected jobs). If the MVP intent is a deployable no-op job, add at least a placeholder source + sink (e.g., `env.fromElements(0).print()`) or defer `env.execute()` until the Positions changelog source is wired per the TODO.

## code/common/src/main/java/com/trading/common/arrow/ArrowOrderRequest.java

### [medium] lines 46-47

`clientOrderRef.value()` is dereferenced before any null check on `clientOrderRef` itself. If a caller passes a null `ClientOrderRef`, the constructor throws a `NullPointerException` instead of the intended `IllegalArgumentException`, which is harder to diagnose. Also note `ClientOrderRef`'s own constructor performs no validation, so `new ClientOrderRef(null)` is possible — add a null guard for the wrapper.

### [medium] lines 58-58

`price` is accepted as an arbitrary String with no format or cross-field validation. For LMT/SL_LMT orders the broker requires a valid numeric price, while MKT/SL_MKT should use "0". Without validating that price is numeric and consistent with `order`, an invalid payload can be constructed here and only be rejected later by the broker, or worse, sent with unintended order semantics. Consider validating the numeric format and requiring non-empty price for limit-type orders / "0" for market-type orders.

### [medium] lines 43-45

Mandatory Arrow fields — `exchange`, `instrumentToken`, `transactionType`, `order`, `product`, and `validity` — are not null-checked in the constructor. A null in any of these will not be caught at construction time and will surface later as an NPE while building/serializing the request payload, making the failure harder to attribute. Add explicit null checks (or use Objects.requireNonNull) alongside the existing symbol/quantity/ref validation to fail fast.

### [low] lines 60-60

`disclosedQty` is not validated and can be negative. Arrow expects a non-negative disclosed quantity, so a negative value would only be rejected by the broker after construction. Add a `disclosedQty >= 0` check for fail-fast behavior.

## code/common/src/main/java/com/trading/common/arrow/ArrowOrderResponse.java

### [medium] lines 32-32

When `requestTime` is absent from the response or is not a JSON number (e.g., the broker returns it as a quoted string), the method silently defaults to 0L. Since 0 is an invalid epoch-ms timestamp (1970-01-01), this can silently corrupt downstream ordering/audit logic, and it is inconsistent with the `orderNo` handling above, which throws on missing values. Consider throwing `IllegalArgumentException` when `requestTime` is missing or non-numeric, or at least validate `time > 0` before constructing the response.

### [low] lines 26-27

`data.get("orderNo")` will throw a bare NPE if the caller passes a null map (e.g., when a JSON body of `null` is decoded). Add a null guard on `data` for a clearer failure mode, consistent with the explicit validation used for `orderNo`.

## code/common/src/main/java/com/trading/common/arrow/ArrowOrderStatus.java

### [medium] lines 24-24

OrderStatus.from uses a strict valueOf() after trim/uppercase, which throws an unchecked IllegalArgumentException for any value not exactly matching one of the enum constants. Note the internal spelling inconsistency in this same class: OrderStatus.CANCELLED uses a double "L", while ReportType.CANCELED (wire "Canceled") uses a single "L". Since both claim to model Arrow's cancellation vocabulary, if the broker's orderStatus uses the single-L spelling (as its own reportType wire does), OrderStatus.from("CANCELED") will fail to match and throw. Any new/renamed broker status will also abort the current record decode. Consider aligning the spellings and mapping unrecognized statuses to an UNKNOWN sentinel (or returning Optional) so unexpected broker data is quarantined rather than crashing the pipeline.

### [medium] lines 51-51

ReportType.from throws IllegalArgumentException for any unrecognized value, and the wire vocabulary declared here ("NewAck", "PendingNew", "Canceled") may not cover what the broker actually emits. The vendored Arrow Go SDK in this repo documents reportType as "Type of order report (e.g., NEW, FILL, CANCEL)" (go-bridge/third_party/go-arrow/arrow/orders.go), i.e. a real reportType such as "NEW" would not match "NewAck"/"NEW_ACK" or "CANCEL"/"CANCELED" and would throw, aborting the event decode path. Since this consumes external broker data, consider returning an UNKNOWN sentinel (or Optional) and quarantining the raw event instead of propagating an unhandled exception.

## code/common/src/main/java/com/trading/common/config/PlatformConfig.java

### [medium] lines 62-62

This validation can never trigger: `DEDUP_TTL_MS` and `CANDLE_WINDOW_MS` are `static final` compile-time constants initialized to exactly the compared literals (300_000L / 15_000L), so both `!=` conditions are compile-time-constant expressions that are always false. The javadoc claims startup rejects wrong values, but this method is both unreachable dead code and never invoked anywhere — the actual ingestion entry point (`IngestionService.main`) validates via `IngestionConfig.validate()` instead, so the promised safety net provides only false assurance. If these values truly must be validated at startup, they should be loaded from configuration rather than hard-coded finals; otherwise the method should be removed. Also note the literal `300_000L`/`15_000L` duplicates violate the class's own 'no scattered numeric literals' rule and will go stale if the constants change.

### [low] lines 53-53

When `containerMemoryLimitBytes <= 0` (e.g., an unreadable cgroup limit surfaced as 0, or a negative value from a misconfigured/test harness), `Math.floor(limit * 0.10)` yields <= 0 and `Math.min(67_108_864L, derived)` returns 0 or a negative value. If the result is used as a buffer/queue capacity or byte-array size, this can silently disable buffering or fail at allocation. Consider clamping to a safe floor (e.g., `Math.max(1L, derived)` or the original lower bound) and/or rejecting non-positive input explicitly.

## code/common/src/main/java/com/trading/common/model/MarketTick.java

### [medium] lines 39-39

`isValid()` compares `validityState` against the exact literal `"VALID"`, but the Ingestion pipeline (`RealFlussRowConverter.append`) writes the `ValidityClassification` enum name into the `validity_state` column of raw_table_1 — i.e. `"VALID_TRADE"`, `"VALID_NON_TRADE"`, `"INVALID_VALUES"`, etc. Since none of the values actually persisted equals `"VALID"`, `isValid()` (and therefore `isValidTrade()`) will always return `false` for the rows this record claims to represent. Align the check with the stored values (e.g. `validityState != null && validityState.startsWith("VALID")`), or change the writer to persist a canonical `"VALID"`/`"INVALID"` string.

## code/common/src/main/java/com/trading/common/observability/SafetyHaltRequest.java

### [medium] lines 21-23

`isIdempotentDuplicate` returns true whenever `prior == incoming`, which treats repeated `FAILED` or `PENDING` results as idempotent no-ops as well. A `FAILED` result means the halt was never applied, and `PENDING` is non-terminal — treating a repeat of those as a duplicate would suppress a retry and could leave the system halted-intended-but-not-actually-halted. Idempotency should only hold for terminal success outcomes (`APPLIED`, `ALREADY_HALTED`), so `FAILED`/`PENDING` repeats should be retried rather than deduped.

## code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java

### [medium] lines 165-167

The Javadoc states 12 required fields (timestamp, level, service, component, subsystem, host, vm_id, environment, correlation_id, trace_id, span_id, message) and this class is the only supported log shape, but `build()` performs no validation. If any required field is passed as null, `toAttributes()` will emit null/empty values for mandated attributes, and `OtlpEmitter.emitLog` will append a literal `null` string for service/level/message (it appends these directly without null handling), producing a malformed record that violates the OpenObserve contract (docs/04_contracts/openobserve.md §F). Suggest fail-fast validation with `Objects.requireNonNull` on all required fields in the constructor/build().

## code/pom.xml

### [medium] lines 143-143

This JVM flag is only wired into the surefire (test) argLine, but the comment states it is required by Arrow MemoryUtil (Fluss client) at runtime on JDK 17+. The production launch paths are inconsistent: `run-ingestion-full.sh` does pass `--add-opens=java.base/java.nio=ALL-UNNAMED`, while the Docker deployment (`docker-entrypoint.sh`) launches `java -cp /app/ingestion.jar` with no JVM options. If the Fluss/Arrow native-memory path is exercised in the container (as the comment implies), the service can fail with `InaccessibleObjectException`/`IllegalAccessError`. Add the flag to the runtime JVM launch (e.g. via `JAVA_TOOL_OPTIONS` in the Dockerfile/entrypoint) so test and production environments match.

## start-all.sh

### [medium] lines 81-81

The script depends on `nc` (netcat) for the Fluss reachability check without verifying it is installed. On a host without `nc`, `! nc -z ...` evaluates to true (command-not-found 127 negated), so the script starts docker compose unnecessarily, and the wait loop below can never detect readiness — it always dies with "Fluss did not come up" after 60s even when Fluss is already healthy. Use bash's built-in `/dev/tcp` (as run-ingestion-full.sh does) or add a `command -v nc` preflight check first.

## code/02_services/01_ingestion/go-bridge/supervisor.go

### [low] lines 117-120

Each slot runs in a bare goroutine with no `recover`. A panic in one slot's main flow — e.g. a nil stream returned by `makeFactory(client, i)` with a nil error (the production factory's SDK dial can't return nil,nil today, but an injectable factory in the supervisor's contract can), an SDK panic during `SubscribeHFTTokens`, or an unexpected panic in `runHFTEpoch` — propagates out of this goroutine and crashes the whole bridge process, taking down healthy slots and the downstream Java consumer. Since the supervisor's stated purpose is to isolate slots and keep healthy peers alive during a peer's retry, adding a per-slot recover that converts a panic into a terminal outcome would preserve that isolation. Note this only covers the slot's main flow; the read/heartbeat/watchdog sub-goroutines spawned inside `runHFTEpoch` still cannot be recovered from here, but the slot's own panic path would no longer be fatal to peers.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go

### [low] lines 76-80

When the API returns `data: null` (or `[]`) with status `success`, `json.Unmarshal` into `[]map[string]any` succeeds and the function returns a nil/empty slice with a nil error. A batch quote call that returned no quotes is therefore indistinguishable from a genuine server anomaly, and a caller that indexes `quotes[0]` without checking length would panic. Consider returning an explicit error for null data (or documenting the empty-result contract), since nothing in the current code lets callers detect this condition.

### [low] lines 108-111

When the server replies `{"data": null, "status": "success"}`, `result.Data` is a nil map and is returned with a nil error. This diverges from the nil-handling convention used elsewhere in this same package (e.g., `GetBasketMargin` and `GetAllOptionChainSymbols` normalize nil `data` to empty containers); a caller that writes into the returned map (per that convention) would panic on a nil map. Normalize nil data to an empty map or return an error.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/streams.go

### [low] lines 174-176

parseQuote reuses parseLTPC, which computes NetChange from bytes 13:17 as if they were the close price. parseQuote then overwrites LTQ with those same bytes (13:17) and Close with bytes 45:49, but the NetChange recompute only runs when the new Close != 0. For quote/full ticks where the real close price (45:49) is 0 (e.g., newly listed or illiquid instruments), NetChange is left computed from LTQ, yielding a meaningless percentage. Compute NetChange only from the final Close and explicitly clear it when Close is 0.

### [low] lines 106-112

ReadTicks (and the analogous ReadUpdates) check ctx.Done() only at the top of the loop; the blocking conn.ReadMessage() has no read deadline, so canceling ctx while the socket is idle does not unblock the goroutine — it can only stop via socket close or incoming data. The standard-path caller currently closes the socket on shutdown (defer ds.Close()), so the bridge doesn't hang today, but any caller relying on ctx alone to stop the reader will leak the goroutine indefinitely. Consider a goroutine watching ctx.Done() that closes the connection, or set/refresh a read deadline so cancellation can interrupt the read.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java

### [low] lines 98-99

Unlike the string fields (which are guarded by `text()` via `isNullAt`), the numeric fields `event_time` (11), `instrument_token` (4), `last_price_paise` (15), and `last_qty` (16) are read with `row.getLong(...)` directly. Although these columns are declared NOT NULL in the DDL, Fluss LOG tables do not guarantee runtime enforcement of nullability, and this tool is precisely for inspecting data quality — including malformed rows. A NULL in any of these columns would throw an NPE and terminate the polling loop, making the viewer unavailable exactly when it is needed most. Consider guarding numeric reads with `isNullAt` (similar to `text()`) and printing a placeholder.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BridgeEvent.java

### [low] lines 30-30

The Go-side bridge contract (`validateBridgeEvent` in ndjson.go) requires `received_ts_ms > 0`, but this constructor validates every other field except `receivedTsMs`. Since `BridgeEventParser` defaults a missing `received_ts_ms` to 0 (and `asLong` can yield 0/negative values on malformed input), an event with a missing or non-positive receive timestamp would pass this validation gate and flow downstream as evidence. Mirror the bridge contract for defense in depth: add a `receivedTsMs <= 0` check alongside the `connectionEpoch` check.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/BrokerQuarantine.java

### [low] lines 31-34

The record is documented as "Immutable" and its constructor validates `payloadHash` against `rawPayload`, but the `byte[]` is stored without a defensive copy. Because arrays are mutable, a caller can mutate `rawPayload` after construction (the accessor returns the same reference), which silently breaks the record's core invariant — the stored `payloadHash` would no longer match the payload bytes, and the mutated bytes could be persisted downstream (e.g., into `ingestion_quarantine`) without ever being re-validated. Take a defensive copy in the compact constructor so the hash invariant and documented immutability actually hold.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/ReadinessFile.java

### [low] lines 20-20

`Files.move` with `ATOMIC_MOVE` throws `AtomicMoveNotSupportedException` on filesystems that do not support atomic moves (e.g., some NFS- or bind-mounted volumes in container deployments). Since this marker drives the container readiness probe, a single unsupported-move failure would leave the marker unwritten and the container permanently not-ready, while the caller in IngestionService only logs a warning. Consider falling back to a non-atomic move when the filesystem does not support it, keeping the tmp-then-rename pattern.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java

### [low] lines 103-103

The Builder defaults eventTime to Instant.EPOCH and the constructor never assigns null, so the `eventTime != null` guard here can never detect a missing/unset event time — a packet built without an explicit eventTime still evaluates as trade-eligible. Check for a real timestamp (e.g., eventTime.isAfter(Instant.EPOCH)) or document that EPOCH is a valid sentinel.

## code/02_services/04_executor/main.py

### [low] lines 20-20

The boolean env-var check is case-sensitive: values like `TRUE`, `True`, or `1` will silently evaluate to disabled. Since this flag gates real order execution, a typo like `EXECUTION_ENABLED=TRUE` would silently leave the executor disabled (or worse, a future implementation could silently disable safeguards). Normalize the value before comparing.

