# Maintainability findings (82)

## run-ingestion.sh

### [high] lines 46-46

The `exec` target is a hardcoded absolute path tied to a single developer's machine (`/home/saurabh/...`). Any clone, move, or another user's checkout will fail with a generic `exec: No such file or directory` at the very end of the script. Since `code/run-ingestion-full.sh` ships inside this same repo, derive the path from the script's own location so it remains portable.

## Makefile

### [medium] lines 46-47

The new targets `cep-check`, `test`, and `test-ingestion` are not declared in `.PHONY`. Since these targets do not correspond to real files, Make will skip them if a file/directory with the same name exists (a `test` directory at the repo root is a common occurrence), silently doing nothing. Add them to the existing `.PHONY` line.

## code/01_platform/02_sql/ddl/09_order_lifecycle.sql

### [medium] lines 26-27

The header states this table is "rebuildable from Fills audit", but the source `Fills` table has `table.retention.days = '3'` while this table retains 7 days. Any rebuild beyond the 3-day Fills window would be incomplete, contradicting the 7-day retention claim. Either align retention with the rebuild source (keep Order_Lifecycle ≤ Fills retention) or document that full rebuild is limited to the Fills retention window.

## code/01_platform/02_sql/ddl/13_order_correlation.sql

### [medium] lines 6-6

The header comment declares the table scope as `account_scope_id`, but no `account_scope_id` column exists in the schema (unlike `Positions`, which materializes the scope column). If `instruction_id` is not guaranteed to be globally unique across accounts, KV upserts keyed only on `instruction_id` will collide and silently overwrite another account's correlation record. Either add an `account_scope_id` column (and include it in the PK/bucket key if scoping is required) or correct the header comment to reflect that the key is globally unique.

## code/01_platform/02_sql/ddl/16_postback_quarantine.sql

### [medium] lines 25-27

The header claims "Retention: until disposition + buffer" and "Lake: encrypted evidence per policy", but the WITH clause only sets a fixed 7-day retention and drops the datalake options that the previous DDL had (`table.datalake.enabled`/`format`/`freshness`/`auto-compaction`). A quarantined postback awaiting operator disposition past 7 days will expire, and evidence will never be offloaded to the encrypted lake — contradicting the stated evidence policy and the original 7-year encrypted retention intent. Either restore the datalake options and use a disposition-aware retention (e.g. extend while OPEN), or correct the header to match the actual (7-day, no-lake) behavior.

### [low] lines 19-19

`reason` and `disposition` are NOT NULL string columns but their allowed values are no longer documented or constrained. The previous DDL explicitly listed the vocabularies (e.g. MISSING_BROKER_ID/AMBIGUOUS_CORRELATION/NO_MATCHING_INSTRUCTION, OPEN/INVESTIGATING/RESOLVED/DISMISSED). Without this contract, the future Action Capture writer can emit inconsistent values that operators and downstream tooling cannot reliably interpret. Add a comment documenting the allowed `reason` and `disposition` enumerations (matching whatever the writer will emit).

## code/01_platform/04_scripts/ddl_apply.py

### [medium] lines 284-286

Exception-hardening gap: main() only catches RuntimeError, but several non-RuntimeError failures can occur upstream: a structurally-wrong yet valid-JSON manifest makes diff_manifests() raise KeyError/TypeError (e.g., a table entry missing 'ddl_path'), and compute_manifest_entries() opens DDL files with encoding='utf-8' while only catching OSError, so a non-UTF-8 file raises an unhandled UnicodeDecodeError. All of these escape as raw tracebacks instead of the intended clean 'DDL contract error' diagnostic, which is especially counterproductive for a safety-gate script. Normalize/validate the manifest structure on load and broaden the catch (e.g., include KeyError/TypeError/UnicodeDecodeError, or catch Exception) in main().

### [low] lines 234-234

Dead assignment: `_existing_raw` is destructured from load_existing_manifest() but never referenced afterwards; the raw JSON string is only computed (read into memory) to be thrown away. Drop the unused second return value from load_existing_manifest() (or actually use it, e.g., to print the raw manifest on drift), to avoid confusing future maintainers.

## code/01_platform/04_scripts/digest-pin.sh

### [medium] lines 33-33

All three resolver branches redirect stderr to /dev/null, so registry authentication errors, unreachable registries, and nonexistent tags are hidden. The user only ever sees the generic "could not resolve digest" message, which in a CI pipeline obscures the root cause and slows debugging. Consider capturing the underlying error and printing it on failure (e.g., keep stderr in a variable and echo it when the digest is empty).

### [low] lines 30-31

Parsing the resolver's JSON with `grep -o | head -1` is fragile: it depends on field ordering and exact quoting, and will silently pick the wrong digest if the tool changes its output layout or a non-sha256 digest appears first. For skopeo, prefer the structured `skopeo inspect --format '{{.Digest}}' docker://...`; for docker use `--verbose` and parse the `Descriptor.digest`. This makes extraction deterministic and self-documenting.

### [low] lines 26-26

Gating the docker branch on `docker info &>/dev/null` requires a running daemon and can block on a daemon connection timeout. However, `docker manifest inspect` queries the registry directly and does not need the daemon, so this gate may skip a perfectly usable docker-based resolution or add latency. Consider checking only that the docker CLI is present (and applying a timeout if a daemon probe is truly needed).

## code/01_platform/04_scripts/run-monday-gates.sh

### [medium] lines 18-18

PROJECT_ROOT defaults to a hard-coded personal absolute path. The script's stated purpose is "wire it into CI or a cron", where this path will not exist, so `mkdir -p $OUT_DIR` and the `cd $BRIDGE_DIR`/`cd $CODE_DIR` steps will fail with confusing errors. Derive the root from the script location instead (overridable via env).

### [medium] lines 23-23

No preflight validation: with `set -euo pipefail`, a missing `go`/`mvn`/`java` or a nonexistent project directory produces an opaque "command not found"/`cd` failure buried in a log file, making environment breakdowns hard to diagnose in CI/cron. Add explicit prerequisite checks up front so failures are actionable.

### [low] lines 33-33

On failure the script exits immediately after writing the per-suite FAIL line, so SUMMARY.txt lacks a terminal status marker equivalent to "ALL GATES PASSED". For automated parsers expecting a single decisive terminal line, append a `GATE RESULT: FAIL` marker before each `exit 1`.

## code/01_platform/04_scripts/soak-monitor.sh

### [medium] lines 26-26

The hard-coded `PROJECT_ROOT=/home/saurabh/...` default makes the script non-portable. On any other checkout/host, `LOG_FILE` and `OUT_DIR` resolve to non-existent paths and every event count silently becomes 0 — the monitor would report a healthy soak even though it is sampling nothing. Derive the root from the script's own location (e.g. `SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"` then `$SCRIPT_DIR/..`, or `git rev-parse --show-toplevel`) and keep the absolute path only as an env override.

## code/02_services/01_ingestion/.dockerignore

### [medium] lines 1-12

This `.dockerignore` is placed in a subdirectory of the build context and will never be honored by `docker build`. Per the Dockerfile header, the build context MUST be the reactor root `code/` (`docker build -f 02_services/01_ingestion/Dockerfile .` from `code/`), and Docker only reads `.dockerignore` at the context root. The same exclusions are already effectively applied by `code/.dockerignore` via `**/`-prefixed patterns (`**/.env`, `**/logs/`, `**/target/`, `**/*.log`, etc.). As written, this file is dead config that may mislead developers into thinking the context is sanitized when building from this directory (which would actually fail, since the Dockerfile's COPY paths are relative to `code/`). Recommend removing this file, or if per-module ignore rules are desired, documenting that they must live in `code/.dockerignore` to take effect.

## code/02_services/01_ingestion/go-bridge/hft_slot.go

### [medium] lines 163-163

`Run()` is a no-op session driver: it only blocks on `ctx.Done()` and returns `nil` — it never calls `BeginConnect()`, `SubscribeHFTTokens()`, or the frame-read loop, and never propagates errors. If this method were ever used as the per-slot main loop (its clear intent), slots would silently "succeed" without ingesting a single tick. Broader context: the whole `HFTSlot`/`SlotConfig`/`validateRequestUnion` surface is referenced only by `hft_slot_test.go`; the production supervisor (`runHFTSlotWithFactory`/`runHFTEpoch` in supervisor.go/main.go) implements a separate functional slot machine and never touches this type. This file is effectively a duplicate, test-only stub of the supervisor logic — either wire it into the real path or remove it to avoid drift between two implementations.

### [low] lines 174-179

The package-level helper `min(a, b int)` is never referenced anywhere in the go-bridge package (including the test file), so it is dead code. On this module's Go version (go 1.24.x per go.mod) it also shadows the built-in `min` and can produce type/behavior confusion. Remove it (or rename) rather than leaving an unused shadow of a standard builtin.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java

### [medium] lines 121-121

Detecting an existing table by scanning the exception message for the substring 'already exist' is fragile: the text is version/locale dependent, and any future change in Fluss's error wording will misclassify the error as a real failure (or, worse, mask one). Additionally, in the inner catch that logs 'already exists, skip schema check', ok++ still counts the table as verified even though the schema check was skipped. Prefer the ignoreIfExists flag semantics or a dedicated exception type, and surface schema-check failures explicitly.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TickTableViewer.java

### [medium] lines 94-97

The viewer reads columns via hardcoded positional indexes (4, 5, 6, 11, 14, 15, 16, 25) that are coupled to the exact column order of `raw_table_1`. While these currently match the DDL (`instrument_token`, `exchange`, `symbol`, `event_time`, `tick_type`, `last_price_paise`, `last_qty`, `validity_state`), both the DDL (02_raw_table_1.sql) and the row converters (RealFlussRowConverter/StubFlussRowConverter) are changed in this same update. Any future column reorder, insertion, or count change will silently misalign the displayed data or throw IndexOutOfBounds and crash the viewer. Since `table.getTableInfo().getRowType()` is already available, consider resolving indexes by field name from the schema (or at least defining named constants mirroring the DDL order) and validating the field count before reading.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java

### [medium] lines 0-0

MAX_PENDING_APPEND_BYTES is validated twice with conflicting behavior. The earlier longRange() call already enforces the 1 MiB floor and records range/format errors, but this block re-reads the same env key and unconditionally overwrites b.maxPendingBytes with Long.parseLong(), bypassing the min/max check (and on parse failure it appends a duplicate error for the same key). The 'container memory' comment doesn't match the source — it is the exact same variable already validated above, so the first validation result is effectively dead. Recommend removing this redundant block so the longRange() validation is authoritative; if a dynamic container-memory override is genuinely intended, read a distinct key and derive the byte value from it.

### [low] lines 320-325

Contract mismatch: the class javadoc promises 'missing required keys or out-of-range values throw IllegalStateException' and that production 'never falls back to ... guessed values', but exactInt() (and intRange/longRange/doubleRange) silently return plan defaults when a key is missing or blank, with no warning logged. A typo'd or renamed variable in the deployment (e.g. ARROW_HFT_MIN_ACTIVE_SLOTS, INGESTION_MAX_BATCH_RECORDS) would deploy with plan defaults and no observable signal, masking misconfiguration. Consider logging a warning on fallback (consistent with the required(env,key,fallback,errors) overload), and note DEPLOY_ENV also silently defaults to 'dev', which disables the production guards if a production deploy forgets to set it.

### [low] lines 306-313

The errors parameter of this required-with-fallback overload is never used — it always logs a warning and returns the fallback instead of recording an error. Combined with the misleading 'required' name, this can mislead maintainers into thinking GO_ARROW_SDK_VERSION is mandatory and that validation failures are recorded through errors here. Either drop the unused parameter and rename the method to reflect fallback semantics, or record the missing-key case as an error to honor the 'required' contract.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/DiscontinuityEvent.java

### [medium] lines 11-12

This model class is dead code: it is never referenced anywhere in the codebase. `DiscontinuityWriter` persists suspected-discontinuity evidence by building Fluss `GenericRow` rows directly, and `IngestionService`/`DiscontinuityReasonMappingTest` never construct `DiscontinuityEvent`. An unused model that duplicates the writer's own field mapping will silently drift from the real persistence path (it already does: the DDL has no `status`/`affected_scope` columns). Either wire this model into `DiscontinuityWriter` so the record shape stays consistent, or remove it.

### [medium] lines 20-20

The `status` field is an unvalidated free-form String with magic values (OPEN/ACKNOWLEDGED/CLOSED). A typo such as "OPEn" would be silently accepted and produce inconsistent evidence, and the `suspected_discontinuities` DDL contains no `status` column, so this field does not map to the persisted schema. Use an enum or shared constants (with validation) and keep the model aligned with the actual DDL columns if this class is intended for persistence.

### [medium] lines 25-26

The builder silently substitutes defaults for potentially required fields: `connectionEpoch` is a primitive `long`, so omitting it yields 0 (indistinguishable from a real epoch and easily mistaken for 1970-01-01), `reasonCode` becomes "UNKNOWN", and `status` becomes "OPEN". This masks construction bugs and can produce misleading evidence records. Consider requiring these fields (e.g., boxed `Long` with a null check) or making the fallback behavior explicit and documented.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java

### [medium] lines 58-58

appendAckTs is a final field hardcoded to Instant.EPOCH in the constructor, and the Builder exposes no field or setter for it. Despite the comment "set post-append", there is no code path that can ever update it, so appendAckTs() always returns the epoch sentinel (1970-01-01). Ack timing is actually tracked outside the packet via RawTickWriter.AppendOutcome.ackTime(); any future consumer of appendAckTs() would silently get a wrong timestamp. Either remove this field/accessor or make it settable (e.g., via the Builder).

### [low] lines 160-160

The class Javadoc states "All typed fields are verified and normalized before construction", but build() performs no validation at all. A caller can construct a packet with null validity/raw, inconsistent OHLC (high < low), or negative prices, which contradicts the class contract and can cause NPEs (e.g., validity().name() in the Fluss row conversion) or malformed data downstream. Enforce the invariants in build() (or clearly document that verification is the caller's responsibility).

## code/02_services/01_ingestion/src/main/resources/log4j2.xml

### [medium] lines 40-41

`HOST`, `ENV`, and `VM_ID` are declared as Log4j2 `<Property>` lookups but are never referenced by any appender/layout. `JsonLayout properties="true"` serializes only the ThreadContext (MDC) map — arbitrary configuration `<Property>` values are not emitted. Consequently the JSON file output does not contain `host`, `vm_id`, `environment` (nor `service`, `component`, `subsystem`, `correlation_id`, `trace_id`, `span_id`), even though the header comment states this file "Matches the OpenObserve contract", which requires those fields (docs/04_contracts/openobserve.md §F). Wire the values into the layout via `<KeyValuePair>` children and populate the correlation/trace/span keys into the MDC so emitted records satisfy the contract.

## code/common/src/main/java/com/trading/common/broker/ArrowMarketTick.java

### [medium] lines 27-27

This is an immutable value object intended for pipeline data exchange but it does not implement equals/hashCode (nor toString). If instances are ever placed in sets/maps or compared for equality (e.g., dedup by tick), default identity semantics will silently produce wrong results. Sibling value types in this module all provide value equality (ExchangeId, InstrumentToken implement equals/hashCode; MarketTick is a record). Consider implementing equals/hashCode based on exchange, instrumentToken, mode, exchangeTimestamp and lastTradedPrice.

## code/common/src/main/java/com/trading/common/model/MarketTick.java

### [medium] lines 30-30

`byte[] rawPayload` is a mutable array component in a record that is documented as the normalized immutable market tick. Records generate `equals()`/`hashCode()`/`toString()` using reference identity for array components, so two content-identical ticks will compare unequal, and the `rawPayload()` accessor returns the internal array directly, allowing callers to mutate the supposedly immutable payload. This diverges from the project convention in `RawTick`, which defensively clones on construction and access. Consider a compact constructor that clones the input and an accessor that returns a clone.

## code/logs/ingestion.json

### [medium] lines 1-1

This file is a generated runtime artifact, not source: log4j2.xml defines a `RollingFile` appender writing JSON lines to `${LOG_DIR}/ingestion.json` (LOG_DIR defaults to `logs/`), so this file is rewritten on every service run and grows unbounded. Committing it produces noisy diffs on each execution, repository bloat, and eventual merge conflicts. It should be excluded via `.gitignore` and removed from the changeset rather than tracked.

## code/run-ingestion-full.sh

### [medium] lines 6-7

`REPO_ROOT` and `ARROW_INSTRUMENT_MANIFEST` are hard-coded to a specific user's absolute paths. `ARROW_INSTRUMENT_MANIFEST` in particular is exported unconditionally, overwriting any pre-set env value, so it cannot be overridden from outside — contradicting the script's otherwise env-overridable design. Derive `REPO_ROOT` from the script location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/..`) and allow env overrides (`${ARROW_INSTRUMENT_MANIFEST:-...}`), matching the pattern already used in `start-all.sh`.

## code/smoke-test.sh

### [medium] lines 5-5

Hardcoded absolute path tied to this developer's machine makes the script non-portable (breaks for any other checkout location). Derive the directory from the script location instead, which also avoids the `cd "$DIR"` silently succeeding even when the script itself is run from elsewhere.

## start-all.sh

### [medium] lines 22-22

PROJECT_ROOT and MANIFEST default to a hardcoded developer path (`/home/saurabh/Jupyter_notebook/...`, and the manifest filename even contains a space). For any other checkout location the script fails immediately before the pipeline starts, undermining the "ONE COMMAND to start" goal. Derive PROJECT_ROOT from the script's own location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`) and validate MANIFEST explicitly with a clear error instead of relying on an absolute user-specific default.

### [low] lines 100-101

The header claims builds happen only "if they're out of date", but the script always rebuilds both the Go bridge and the Java jar on every run. Also the default `MVN_FLAGS=-o` (offline) will fail on a fresh machine with an empty Maven local repository, and there are no `command -v` checks for java/go/mvn/docker/nc/awk. Consider a source-vs-artifact staleness check and a tool preflight so a first run fails with actionable messages instead of obscure build errors.

## code/.dockerignore

### [low] lines 9-9

Since the build context is the reactor root `code/` (per the ingestion Dockerfile header: "build context MUST be the Maven reactor root code/"), the repository's `.git` directory sits above the context and this `.git/` pattern matches nothing — it is effectively dead config. The file's stated purpose is keeping credentials/history out of the build context, so consider `**/.git/` to also cover any nested `.git` directories (e.g., if a future `third_party` submodule is vendored with its own `.git`), or remove the line to avoid dead configuration.

## code/01_platform/01_docker/docker-compose.yml

### [low] lines 123-124

`FLUSS_BOOTSTRAP_SERVERS` is never consumed: the Go bridge does not read it, and the Java client reads `FLUSS_BOOTSTRAP` (via `IngestionConfig`), with `FlussClientAdapter` deriving the Fluss `bootstrap.servers` from that same value. This duplicate key is dead configuration that can silently drift from the real bootstrap endpoint. Remove it (or actually use it in the client).

## code/01_platform/02_sql/ddl/02_raw_table_1.sql

### [low] lines 18-18

Option/derivative metadata columns `instrument_type`, `strike_paise`, `expiry`, and `option_type` are declared nullable but are never populated by the ingestion pipeline (`RealFlussRowConverter` writes empty string/null for every row, and neither TickPacket nor the Go bridge supplies these fields). The raw_table_1 LOG thus implies option metadata is captured when it is not. If no downstream producer will ever fill these columns, consider removing them to keep the schema honest, or document them as reserved for a later phase.

## code/01_platform/02_sql/ddl/10_positions.sql

### [low] lines 19-19

`current_quantity` is a derived value (open_quantity - closed_quantity). Persisting it as a separate NOT NULL column in the KV projection creates a consistency risk: any write path that updates only one of the three quantity fields will silently corrupt position state, and nothing in the schema enforces the invariant. Consider computing it at read time, or at minimum documenting/enforcing the invariant (e.g., a projector-level test that current_quantity == open_quantity - closed_quantity after every update).

## code/01_platform/02_sql/ddl/12_execution_attempts.sql

### [low] lines 6-6

The header declares `Scope: execution_partition_id, account_scope_id`, but the schema has no `account_scope_id` column. Sibling KV tables that claim account scoping (Execution_Gate, Portfolio_Reservations) materialize `account_scope_id` as a NOT NULL column. Without it, account-scoped queries, access control, and audit of attempts require a join through Execution_Gate, and the table cannot be filtered/partitioned by account on its own. Either add the `account_scope_id` column (consistent with Execution_Gate) or correct the scope comment to match the schema.

### [low] lines 22-22

This is a KV state table whose row is upserted as the attempt advances through phases (PREPARED → SUBMITTING → ACCEPTED/REJECTED/CANCELLED/UNKNOWN), yet only `prepared_ts` and `submitted_ts` exist — later terminal transitions have no recorded timestamp here, and there is no monotonic ordering column (e.g. `updated_ts`/`transition_version` as used in Portfolio_Reservations) to guard against stale-write overwrites if updates ever race. Consider adding a state-transition timestamp (and/or a version column) so the row's lifecycle is fully ordered and auditable.

## code/01_platform/02_sql/ddl/17_postback_projection_ledger.sql

### [low] lines 17-17

Typo in the column name: `completeted_ts` should be `completed_ts`. Since this is a schema definition, the misspelled identifier will be baked into the Fluss table and can propagate into downstream projection/recovery code, making it awkward to fix later. Please rename it for consistency with the `*_ts` naming convention used in the other DDL files (e.g., `disposition_ts`, `quarantined_ts`).

## code/01_platform/04_scripts/soak-headroom.sh

### [low] lines 6-7

The header documents `capacity_used = acknowledged_tokens / assigned_tokens` and `headroom = 1 - capacity_used`, but the implementation never uses `assigned`: it computes `used = (acked*100)/cap` against the per-connection cap (1024) and only conditionally parses `assigned`. These two definitions only coincide when `assigned == cap`. Align the comment with the implementation (acked vs cap) or switch the calc to acked/assigned so the comment and the metric agree.

### [low] lines 25-25

The default PROJECT_ROOT is a developer-specific absolute path (`/home/saurabh/...`), so on any other machine the script fails unless PROJECT_ROOT is exported manually. Derive it from the script location (e.g. `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`) or leave it unset and require the env var.

### [low] lines 27-27

OUT_DIR is defined but never used — no directory is created and no output file is written. Either implement saving the summary to `$OUT_DIR` or remove this dead configuration so it doesn't suggest the script persists artifacts.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/auth.go

### [low] lines 99-100

`Login()` has no return value and swallows authentication errors (only logs them), so callers cannot determine whether interactive login actually succeeded. Return an `error` (or bool) so automated flows can branch on failure instead of assuming success.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/constants.go

### [low] lines 43-46

The order-type constants define two different wire encodings for the same order type: `OrderTypeSL`/`OrderTypeSLM` ("SL"/"SL-M", labeled legacy) versus `OrderTypeSLLMT`/`OrderTypeSLMKT` ("SL-LMT"/"SL-MKT", labeled REST docs). These raw strings are sent to the broker verbatim, and the SDK's own OrderRequest/OrderDetails docs (orders.go) describe yet another vocabulary ("MARKET, LIMIT, SL, SL-M"). A future caller has no canonical value to use for stop-loss orders, so picking the wrong alias results in a rejected order or an unintended order mode. Consider keeping exactly one constant per supported order type (documenting the canonical wire value the REST API actually accepts) and deprecating/removing the rest.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/limits.go

### [low] lines 36-38

The non-success error discards the actual API status, unlike the sibling SDK methods GetMargin/GetUserDetails which include `result.Status` in the error. When the broker returns e.g. `status: "error"` with a reason code, callers/operators only see a generic message with no diagnostic detail. Include the status value (and, where available, a message field) in the returned error.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/margin.go

### [low] lines 69-71

On a non-success response, the server-provided error details are lost: `MarginResponse` has no `Message`/`ErrorCode` field (unlike `OrderResponse` in orders.go), so the returned error only contains the bare status string. Consider adding a `Message`/`ErrorCode` field to `MarginResponse` and including it in the error to make production failures debuggable.

### [low] lines 69-71

On a non-success response, the server-provided error details are lost: `MarginResponse` has no `Message`/`ErrorCode` field (unlike `OrderResponse` in orders.go), so the returned error only contains the bare status string. Consider adding a `Message`/`ErrorCode` field to `MarginResponse` and including it in the error to make production failures debuggable.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/arrow/quote.go

### [low] lines 50-50

`mode` (type `InfoQuoteMode`) is interpolated into the URL path without being validated against the three allowed constants (`ltp`, `full`, `ohlcv`). Since Go does not enforce a string-const set at compile time, any caller passing an unexpected value (e.g. `"../foo"`) silently builds a different request path. Add a switch that rejects unknown modes before constructing the endpoint so the exported API contract is enforced.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java

### [low] lines 155-155

`packet.validity().name().contains("NON_TRADE")` is a fragile substring match on an enum name. Prefer an explicit enum comparison (`packet.validity() == ValidityClassification.VALID_NON_TRADE`) so renaming/adding validity states cannot silently change tick classification. Note also that this classifies every non-quote state (INVALID_VALUES, INVALID_TIMESTAMP, DECODE_FAILURE, etc.) as "TRADE", which may mislabel the stored data.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java

### [low] lines 137-138

`metrics.setManifestVersion(1)` hardcodes the schema/manifest version, while `main()` already loads `manifestResult.version()` and never passes it into the service. When the instrument manifest version evolves, telemetry will keep reporting the stale literal `1`. Pass the actual manifest version into the service (constructor or setter) instead of hardcoding it.

### [low] lines 895-898

`updateReadinessFile()` is only invoked from `processBridgeEvent()`. The tick-processing path and the backpressure listener never refresh the readiness marker, so during a long healthy feed — or during silent degradation (append failures/backpressure) with no bridge lifecycle event — the marker stays at its last event-driven value and can report ready while the service is degraded. Refresh the marker periodically or from the tick/backpressure path as well.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java

### [low] lines 121-124

manifestVersion is hardcoded to 1 in both the production CSV path and the synthetic fallback, so the version check inside isManifestApproved() is effectively vacuous — it can only ever match an expected version of 1, and any approved manifest version bump (e.g., from versions.pin / schema_manifest) would require this hardcoded value to change in lockstep. Consider deriving the version from the manifest source or configuration instead.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/bridge/PayloadHashValidator.java

### [low] lines 37-41

`Result[] out` is a fragile, undocumented out-parameter: the method dereferences `out[0]` in every branch without any precondition, so a future caller passing a null or zero-length array gets an NPE / ArrayIndexOutOfBoundsException instead of a classified result. The current callers always pass `new Result[1]`, but the contract should be enforced or, better, replaced with a small immutable result type (result + packet bytes) which also removes the per-tick array allocation on the ingestion hot path. At minimum, add a null/length guard and document the precondition.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/DiscontinuityWriter.java

### [low] lines 129-132

The `after` parameter is documented as "first tick after gap (null if none yet)" and `note` as an operator note, but neither is ever persisted — `after` is dropped entirely and `note` only appears in an INFO log (the DDL has no after-gap or note columns). This makes the API contract misleading: callers passing after-gap snapshots or notes cannot tell the data is discarded. Either remove the unused parameters or add matching columns and persist them.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java

### [low] lines 112-112

`sha256()` hardcodes the literal `"SHA-256"`, while the class-level `ALGORITHM` constant is used only for `Result.algorithm` metadata. If the constant is ever changed (e.g., to SHA-512), the actual digest produced here and the algorithm reported in the result would silently diverge, corrupting the fingerprint metadata that downstream dedup depends on. Use the constant here so the two can never drift apart.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java

### [low] lines 149-152

`diagnostics()` — intended as 'a human-readable readiness breakdown' — omits the telemetry readiness dimension (`otlpHealthy` / `isTelemetryReady()`), even though the field and setter exist and telemetry is a tracked readiness input for live-money release. Operators debugging why a readiness gate fails can't see the telemetry health in the same map. Add a `telemetry_ready` entry next to the other dimensions.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/RawTick.java

### [low] lines 60-60

The Builder never validates required inputs. If a caller passes null for rawPayload, the constructor's `builder.rawPayload.clone()` throws NPE at build() time; a null payloadHash makes toString() NPE via `payloadHash.substring(...)`. Additionally, because receiveTime defaults to Instant.EPOCH, a caller that forgets to set it silently produces a 1970-01-01 timestamp that downstream storage/ordering would treat as a real receive time. Since this is a public value class, consider failing fast in build() (e.g., requireNonNull for rawPayload/payloadHash and explicit receiveTime) so misconfiguration surfaces at the construction site rather than in unrelated paths such as logging.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/quarantine/QuarantineWriter.java

### [low] lines 167-171

The Connection and Table created in the constructor are local variables that are never retained or closed; close() only calls writer.flush() and never releases the underlying Fluss connection. Since the connection object is dropped, the client resources can only be freed by process exit. This is fine for the single per-process instance used today, but if QuarantineWriter is ever recreated (reconnect/retry lifecycle), each instance leaks a coordinator connection. Retain the Connection/Table (or the writer's own close) and close it in close() to make the resource lifecycle explicit.

### [low] lines 63-63

BUCKET_COUNT = 8 is declared but never referenced anywhere in this class. It appears to be leftover from a table-creation path that was never implemented (the DDL in 21_ingestion_quarantine.sql already sets 'bucket.num' = '8'). This is dead code that can mislead maintainers into thinking the writer creates the table. Remove it unless it is actually used.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/safety/SafetyHaltWriter.java

### [low] lines 105-107

Dead code: `Instant now = Instant.now();` is declared but never used anywhere in this method (the row uses `detectedTsMs`). Remove it.

### [low] lines 5-7

Unused import `java.util.UUID` — it is not referenced anywhere in this class. Remove it.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/shutdown/UncertaintyJournal.java

### [low] lines 3-3

`java.io.BufferedWriter` is imported but never used anywhere in this class — dead import that should be removed.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java

### [low] lines 172-172

decodeReasonCounters is write-only: it is populated in incrementDecodeError() but never read, serialized, or emitted anywhere, so the documented "decode.errors by reason" breakdown never reaches the collector — dead state. It is also an unbounded ConcurrentHashMap keyed by arbitrary reason strings, so a long-running process can accumulate entries without limit. Either emit the per-reason counts in buildMetricsJson() or remove the map.

## code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java

### [low] lines 257-257

AppendOutcome.timeout(...) and Status.TIMEOUT are defined but never produced by write() — a timeout is deliberately reported as UNCERTAIN (the TimeoutException branch returns AppendOutcome.uncertain). The timeout() factory has no callers, and the caller's TIMEOUT branch is therefore unreachable. Remove the unused factory (and either drop TIMEOUT or use it consistently) to avoid semantic confusion between timeout and uncertainty.

### [low] lines 219-219

close() releases remaining pending bytes using an arbitrary average of 512 bytes/record ((int) (remaining * 512)). This magic number can over- or under-release the tracker's pendingBytes counter, causing byte accounting to drift from the actual pending capacity. Track per-record sizes for accurate release (e.g., keep a queue of outstanding row sizes), or document the approximation.

## code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java

### [low] lines 30-30

The class documents itself as a "Deterministic" workload, and the tests assert that two instances with the same seed produce identical tick sequences. However, the priority queue is ordered only by `Due.timeMs`. With 33/34 ms (PEAK) or 40–60 ms (BASELINE) intervals and modulo-1000 staggered offsets across many instruments, equal due timestamps are common, and `PriorityQueue` breaks ties arbitrarily (its ordering is not stable). The extraction order — and therefore the assigned `sequence` numbers — is thus not guaranteed reproducible across JVM implementations, which undermines the determinism guarantee for benchmarks. Add the instrument index as a secondary sort key to make ordering fully deterministic.

## code/common/src/main/java/com/trading/common/broker/ArrowOrderUpdate.java

### [low] lines 29-29

The 11-parameter positional constructor has three adjacent `long` fields (`fillQuantity`, `fillPrice`, `fillTime`); swapping any two at a call site compiles silently and corrupts data (e.g., quantity written as price). The rest of the codebase uses the Builder pattern for multi-field DTOs (`RawTick.Builder`, `TickPacket.Builder`). Consider introducing a builder (or at least named static factories) so arguments are bound to field names, making accidental reordering impossible.

## code/common/src/main/java/com/trading/common/config/PlatformConfig.java

### [low] lines 25-25

`BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` duplicates `FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30` added in the same change. These are load-bearing capacity values (also referenced by `FixedScope.maxSustainedTicksPerSec()`); keeping two independent copies of the same bound in separate classes creates a silent drift risk where one module tunes a value and another keeps the old bound. Single-source the value (e.g., have one class reference the other) or consolidate the constants.

## code/common/src/main/java/com/trading/common/observability/OtlpEmitter.java

### [low] lines 37-38

Attribute keys from `event.toAttributes()` are written to the JSON document without `escapeJson()` while only the values are escaped. The current keys are compile-time constants from `StructuredLogEvent.toAttributes()`, so the risk is low today, but escaping both keys and values uniformly would make the serializer robust against future dynamic keys.

### [low] lines 18-18

The public constant `TRADING_ALERTS_STREAM` is never referenced in this class — `emitAlert()` hardcodes the literal `"trading_alerts"` for the `stream` attribute instead. Using the constant here would keep the stream name in sync and avoid silent drift if it is ever renamed.

## code/common/src/main/java/com/trading/common/observability/StructuredLogEvent.java

### [low] lines 108-110

`equals()`/`hashCode()` only consider timestampMs, level, service, message, and correlationId, while ignoring identity/correlation fields such as host, vm_id, trace_id, and span_id (plus all optional fields). Two distinct log records that differ only in host/trace/span would therefore compare as equal. If instances are ever stored in a Set or used as Map keys, log records could be silently deduplicated, breaking per-host/per-trace observability. If subset equality is intentional (e.g., correlation-based dedup), document it; otherwise include all fields in `equals`/`hashCode`.

## code/common/src/main/java/com/trading/common/schema/SchemaManifestEntry.java

### [low] lines 23-23

`schemaState` is declared as a raw `String` even though the same package already defines a dedicated `SchemaState` enum (PROPOSED/APPROVED/APPLYING/OBSERVED/REJECTED) whose values exactly match the inline comment. The same applies to `compatibilityClass`, which duplicates the `CompatibilityClass` enum in `com.trading.common.version`. Using String re-implements vocabulary that already exists as strongly-typed enums, allowing invalid values to slip through without compile-time checks and risking drift between the DTO and the enum state machine (e.g., `SchemaState.isExecutableAuthority()` semantics). Jackson can bind JSON string values to enum types by name directly, so the fields can be typed with the existing enums without changing the wire format. Consider changing these fields to the enum types.

## code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java

### [low] lines 27-32

`isPlaceholder` compares the input against the constant *values*, but the class-level javadoc requires each of these constants to be replaced by a pinned, evidence-verified value before the live-money path is enabled. The moment any constant is replaced with a real version (e.g. `FLINK_VERSION_TO_BE_PINNED = "1.17.2"`), this method will start returning `true` for that correctly pinned value, and `VersionGate.isPinnedAndVerified` will reject a valid version — silently defeating the safety check it is meant to provide. The javadoc on this method says "equals its own sentinel name", but the implementation does not compare against the sentinel name/string literal; it is coupled to the (soon-to-be-mutated) constant values. Suggest checking against fixed sentinel literals (e.g. the name strings or a dedicated immutable sentinel set) that are independent of the pinning substitution.

## code/common/workitem/WorkItem.java

### [low] lines 33-34

The spec states a blocked item "must record the owner, missing evidence, and unblock condition" (see WorkItemState javadoc). `transitionTo` doesn't enforce this: it permits entering BLOCKED with all three fields null, and also allows BLOCKED -> BLOCKED re-blocking while silently dropping the prior context. Consider validating that owner/missingEvidence/unblockCondition are non-blank when `next == BLOCKED`, and guarding against re-entering BLOCKED from an already-BLOCKED item if that isn't intended.

## code/pom.xml

### [low] lines 38-38

The comment says "Flink (provided scope — runtime provides these)", but these managed dependencies declare no `<scope>provided</scope>`. In `dependencyManagement`, omitting scope means children inherit compile scope, which would bundle Flink into fat jars and cause classloader conflicts at runtime. Today there is no functional break because neither reactor module (common/ingestion) depends on Flink and the out-of-reactor compute module redeclares `provided` explicitly — but the comment/declaration mismatch is misleading and a future child relying on the managed version will silently get compile scope. Either add `<scope>provided</scope>` to the three Flink entries (matching the comment) or fix the comment.

### [low] lines 27-27

`scala.binary.version` is declared but never referenced by any dependency or plugin in this POM or its children (all managed Flink artifacts are non-Scala Java artifacts). This dead property misleads maintainers into thinking Scala-specific artifacts are managed here; remove it or use it consistently.

### [low] lines 15-18

This aggregator only lists `common` and `02_services/01_ingestion`, but `02_services/02_compute`, `02_services/03_action_capture`, and `02_services/05_mock_arrow` also declare `com.trading:trading-platform` as their parent. They are silently excluded from a root `mvn package` reactor build (and building them standalone requires the parent to be installed first). If the exclusion is intentional (ingestion-only scope), document it in the module comment; otherwise add them to `<modules>` so the root build covers the whole platform.

## show-ticks.sh

### [low] lines 18-18

The pre-flight check hardcodes 127.0.0.1:9123, but the launched Java program honors the FLUSS_BOOTSTRAP env var (defaults to localhost:9123, see TickTableViewer). If a user overrides FLUSS_BOOTSTRAP to a different host/port, this check will either falsely reject a healthy remote cluster, or pass against a local server while the viewer actually connects elsewhere. Consider deriving the probe target from FLUSS_BOOTSTRAP (falling back to 127.0.0.1:9123), or documenting that this wrapper only supports the local cluster.

