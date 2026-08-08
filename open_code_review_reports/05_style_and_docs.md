# Style And Docs findings (6)

## code/02_services/01_ingestion/go-bridge/faketool/main.go

### [low] lines 7-7

The documented invocation does not match the program: `FAKE_HFT_PORT` / `FAKE_HFT_DISCONNECT_AFTER` environment variables are never read (the tool only defines `-port` and `-disconnect-after` flags), and the file is `main.go`, not `faketool.go`. A developer following this usage line will get `go: no such file 'faketool.go'` or, if they rename/point at the file, silently won't get the intended forced-disconnect behavior. Update the usage comment to e.g. `go run -tags faketool main.go -port 8899 -disconnect-after 1`.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

### [low] lines 17-18

The class javadoc claims 'Fluss infers the column schema from the first AppendWriter.append(GenericRow) call, so we only need table existence + bucket distribution', but the code actually creates explicit in-code schemas and verifyTables() strictly compares column counts. If Fluss truly inferred the schema, this column-count check would be meaningless; if it doesn't, the 7-column MINIMAL_SCHEMA is insufficient for the DDL-defined tables. The comment contradicts the implementation — clarify the actual Fluss behavior and make the schemas consistent across the DDL, writers, and this bootstrap.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java

### [low] lines 4-4

The `java.util.Arrays` import is never referenced anywhere in this class. Remove it to avoid dead code and potential lint/checkstyle failures.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java

### [low] lines 35-35

The javadoc's column mapping references "16_postback_quarantine.sql" and documents an 18-column schema (postback_event_id, broker_order_id, client_order_ref, broker_status, broker_timestamp, status, resolution_ts, ...). The table this class actually writes is ingestion_quarantine, created by 21_ingestion_quarantine.sql, which has only 10 columns (and the GenericRow construction correctly matches it). The stale javadoc will mislead maintainers verifying the row layout against the real DDL. Update it to reference 21_ingestion_quarantine.sql with the actual 10-column mapping, and remove the duplicated bs() javadoc line below.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

### [low] lines 15-15

`DoubleAdder` is imported and the class javadoc states gauges use DoubleAdder, but no DoubleAdder is ever used — all gauges are volatile primitives and counters are AtomicLong. Remove the unused import and align the javadoc so readers aren't misled about the metric semantics.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

### [low] lines 157-157

The class and write() Javadoc say 'Retry with linear backoff', but the implementation doubles the delay each attempt (100ms, 200ms, 400ms — BASE_RETRY_BACKOFF_MS * (1L << (attempt - 1))), i.e., exponential backoff. Align the documentation with the actual behavior (or the code with the documentation) so operators tuning retry parameters aren't misled.

