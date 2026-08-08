# Security findings (7)

## code/logs/ingestion.json

### [high] lines 37-37

Sensitive operational data is committed to version control in this runtime log: absolute host paths exposing the developer account (`/home/saurabh/Jupyter_notebook/...`), the live broker account identifier (`user QP3796`), internal endpoints (`localhost:9123`, `otel-collector:4318`), and the arrow-bridge binary path. Anyone with repository access can infer infrastructure layout and user identity. The `.gitignore` change in this update does not add any log patterns, so this file and future runs will keep being committed. Recommend removing this file from the changeset and adding ignore rules (e.g. `code/logs/`, `logs/`, `*.log`, `ingestion*.json`) to `.gitignore`.

## start-all.sh

### [high] lines 52-54

The fallback credential branch pipes raw `.env` content through `eval` without quoting the value, so any ARROW_* value containing shell metacharacters is interpreted as shell code. E.g. `ARROW_PASSWORD=abc$def` silently expands `$def` (wrong secret), `ARROW_PASSWORD=a b` tries to execute `b` as a command, and `ARROW_PASSWORD=x;cmd` runs `cmd`. This contradicts the script's own SECURITY comment (values handled safely) and can break the export or enable arbitrary command execution. Parse key/value with `IFS='=' read` and use a quoted `export "$key=$value"` (a single assignment argument is never re-evaluated) instead of `eval`.

## code/02_services/04_executor/Dockerfile

### [medium] lines 1-1

Pinning to the exact patch `3.11.9` is quite stale (released April 2024), so the resulting image misses subsequent Python 3.11 security and bug-fix patches as well as updated OS packages. If the intent is to stay on Python 3.11, prefer a floating tag like `python:3.11-slim` to track the latest 3.11.x patch; if a fully reproducible build is required, pin to the current 3.11 patch and add a comment/automation so the pin is regularly refreshed.

## code/common/src/main/java/com/trading/common/observability/AuditLogger.java

### [medium] lines 21-21

The redaction check is case-sensitive and exact-match only. For a "mandatory redaction" invariant, this silently leaks sensitive data whenever the caller passes a field name that differs in case (e.g., `apiKey`, `authToken`, `API_KEY`) or contains a sensitive keyword (e.g., `client_secret`, `access_token`) — none of which match the literal entries in REDACTED_FIELDS. Consider normalizing the field (lowercase/trim) before the lookup, and optionally match on containment so variants of known sensitive keys are still caught.

## run-ingestion.sh

### [medium] lines 29-30

The secrets file is sourced after only an existence check; nothing verifies it is not group/world readable. If the file was created with a default umask (e.g. 644), the Arrow credentials are exposed to other local users. Enforce restrictive permissions before sourcing, matching the `chmod 600` guidance in the header.

## code/02_services/01_ingestion/go-bridge/third_party/go-arrow/.gitignore

### [low] lines 1-1

Ignoring `go.sum` in a Go module is generally discouraged: it holds cryptographic checksums that verify dependency integrity and enable reproducible builds (Go docs recommend committing it). `third_party/go-arrow` is a real Go module with its own dependencies (websocket, fasthttp, zerolog, etc.), and its `go.mod` is already committed. While the parent module `go-bridge/go.sum` captures the checksums for normal builds via the `replace` directive, this module's own checksums are not version-controlled — so if the SDK is ever built/tidied/tested on its own (e.g., running the SDK's own tests, or if the `replace` is later removed), `go mod` will regenerate and silently accept whatever the proxy returns, weakening supply-chain verification. Suggest removing the `go.sum` line (or documenting why it is intentionally ignored).

## code/run-ingestion-full.sh

### [low] lines 27-28

`$SECRETS_FILE` holds `ARROW_APP_SECRET` and the TOTP key, but the script never verifies its permissions (it only mentions `chmod 600` in a comment). If the file was created under a permissive umask, other local users can read the credentials. Add a check that fails or warns unless the file is owner-only (e.g. `[ "$(stat -c '%a' "$SECRETS_FILE")" = "600" ]`).

