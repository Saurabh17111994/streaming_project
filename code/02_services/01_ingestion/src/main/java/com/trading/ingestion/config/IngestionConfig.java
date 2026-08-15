package com.trading.ingestion.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates every ingestion configuration key at startup.
 *
 * <p>Contract: missing required keys or out-of-range values throw
 * {@link IllegalStateException}, preventing the service from starting.
 * Production never falls back to demo credentials or guessed values.
 *
 * <p>Validated keys (from {@code docs/08_implementation/03-ingestion.md} §Configuration contract):
 */
public final class IngestionConfig {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionConfig.class);

    // ---- Constants (matching dossier) ----
    public static final long MAX_PENDING_RECORDS = 50_000L;
    public static final long MAX_PENDING_BYTES = 67_108_864L; // 64 MiB
    public static final double WARNING_PERCENT = 0.80;
    public static final long CLOCK_OFFSET_LIMIT_MS = 100L;

    // ---- Validated values (populated by validate()) ----
    public final String arrowAppId;
    public final String arrowAppSecret;
    public final String arrowToken;
    public final String arrowUserId;
    public final String arrowPassword;
    public final String arrowTotpKey;
    public final int arrowHftLatencyMs;
    public final String arrowInstrumentTokens;
    public final String flussBootstrap;
    public final String rawTableName;
    public final int maxBatchRecords;
    public final int maxBatchWaitMs;
    public final int maxPendingRecords;
    public final long maxPendingBytes;
    public final double pendingWarningPercent;
    public final Duration appendTimeout;
    public final Duration drainDeadline;
    public final long clockOffsetLimitMs;
    public final long arrowMaxEventAgeMs;
    public final long arrowMaxFutureEventSkewMs;
    public final String goArrowSdkVersion;
    public final boolean allowRuntimeDdl;
    public final boolean clockCheckRequired;
    public final String uncertaintyJournalPath;

    // ---- HFT connection policy (plan §IngestionConfig — exact values) ----
    public final int arrowHftConnections;
    public final int arrowHftMaxTokensPerConnection;
    public final int arrowHftMaxTokensPerRequest;
    public final int arrowHftHeartbeatSeconds;
    public final int arrowHftStallTimeoutSeconds;
    public final int arrowHftSubscriptionResponseTimeoutSeconds;
    public final int arrowHftReconnectBaseSeconds;
    public final int arrowHftReconnectMaxSeconds;
    public final int arrowHftAuthRefreshAttempts;
    public final int arrowHftMinActiveSlots;
    public final boolean arrowHftMultiConnectionApproved;
    public final boolean ingestionAllowDegraded;

    private IngestionConfig(Builder b) {
        this.arrowAppId = b.arrowAppId;
        this.arrowAppSecret = b.arrowAppSecret;
        this.arrowToken = b.arrowToken;
        this.arrowUserId = b.arrowUserId;
        this.arrowPassword = b.arrowPassword;
        this.arrowTotpKey = b.arrowTotpKey;
        this.arrowHftLatencyMs = b.arrowHftLatencyMs;
        this.arrowInstrumentTokens = b.arrowInstrumentTokens;
        this.flussBootstrap = b.flussBootstrap;
        this.rawTableName = b.rawTableName;
        this.maxBatchRecords = b.maxBatchRecords;
        this.maxBatchWaitMs = b.maxBatchWaitMs;
        this.maxPendingRecords = b.maxPendingRecords;
        this.maxPendingBytes = b.maxPendingBytes;
        this.pendingWarningPercent = b.pendingWarningPercent;
        this.appendTimeout = b.appendTimeout;
        this.drainDeadline = b.drainDeadline;
        this.clockOffsetLimitMs = b.clockOffsetLimitMs;
        this.arrowMaxEventAgeMs = b.arrowMaxEventAgeMs;
        this.arrowMaxFutureEventSkewMs = b.arrowMaxFutureEventSkewMs;
        this.goArrowSdkVersion = b.goArrowSdkVersion;
        this.allowRuntimeDdl = b.allowRuntimeDdl;
        this.clockCheckRequired = b.clockCheckRequired;
        this.uncertaintyJournalPath = b.uncertaintyJournalPath;
        this.arrowHftConnections = b.arrowHftConnections;
        this.arrowHftMaxTokensPerConnection = b.arrowHftMaxTokensPerConnection;
        this.arrowHftMaxTokensPerRequest = b.arrowHftMaxTokensPerRequest;
        this.arrowHftHeartbeatSeconds = b.arrowHftHeartbeatSeconds;
        this.arrowHftStallTimeoutSeconds = b.arrowHftStallTimeoutSeconds;
        this.arrowHftSubscriptionResponseTimeoutSeconds = b.arrowHftSubscriptionResponseTimeoutSeconds;
        this.arrowHftReconnectBaseSeconds = b.arrowHftReconnectBaseSeconds;
        this.arrowHftReconnectMaxSeconds = b.arrowHftReconnectMaxSeconds;
        this.arrowHftAuthRefreshAttempts = b.arrowHftAuthRefreshAttempts;
        this.arrowHftMinActiveSlots = b.arrowHftMinActiveSlots;
        this.arrowHftMultiConnectionApproved = b.arrowHftMultiConnectionApproved;
        this.ingestionAllowDegraded = b.ingestionAllowDegraded;
    }

    // ---- Validation ----

    /**
     * Read all keys from environment, validate, and return a validated config.
     * Throws {@link IllegalStateException} on any violation.
     */
    public static IngestionConfig validate() {
        return validateFrom(System.getenv());
    }

    /**
     * Validate a config from an explicit environment map. Package-private so
     * tests can exercise exact-value validation without mutating the real env.
     */
    static IngestionConfig validateFrom(Map<String, String> env) {
        List<String> errors = new ArrayList<>();
        Builder b = new Builder();

        // ---- Arrow auth ----
        b.arrowAppId = required(env, "ARROW_APP_ID", errors);
        b.arrowAppSecret = required(env, "ARROW_APP_SECRET", errors);
        b.arrowToken = optional(env, "ARROW_TOKEN");
        b.arrowUserId = optional(env, "ARROW_USER_ID");
        b.arrowPassword = optional(env, "ARROW_PASSWORD");
        b.arrowTotpKey = optional(env, "ARROW_TOTP_KEY");

        // At least one auth mechanism must be available
        boolean hasToken = !b.arrowToken.isBlank();
        boolean hasAutoLogin = !b.arrowUserId.isBlank()
                && !b.arrowPassword.isBlank()
                && !b.arrowTotpKey.isBlank();
        if (!hasToken && !hasAutoLogin) {
            errors.add("Either ARROW_TOKEN or ARROW_USER_ID+PASSWORD+TOTP_KEY must be set");
        }

        // ---- Arrow feed (HFT only — the Standard feed was removed 2026-08-14) ----
        b.arrowHftLatencyMs = intRange(env, "ARROW_HFT_LATENCY_MS", 50, 50, 60_000, errors);
        b.arrowInstrumentTokens = optional(env, "ARROW_INSTRUMENT_TOKENS");

        // ---- Fluss ----
        b.flussBootstrap = required(env, "FLUSS_BOOTSTRAP", errors);
        b.rawTableName = required(env, "RAW_TABLE_NAME", errors);

        // ---- Batching (max bounds; app-level batching stays off at the
        // defaults — the Fluss client owns transport-level coalescing) ----
        b.maxBatchRecords = intRange(env, "INGESTION_MAX_BATCH_RECORDS", 1, 1, 1000, errors);
        b.maxBatchWaitMs = intRange(env, "INGESTION_MAX_BATCH_WAIT_MS", 0, 0, 100, errors);

        // ---- Backpressure ----
        b.maxPendingRecords = intRange(env, "MAX_PENDING_APPEND_RECORDS",
                50_000, 100, 1_000_000, errors);
        b.maxPendingBytes = longRange(env, "MAX_PENDING_APPEND_BYTES",
                67_108_864L, 1_048_576L, Long.MAX_VALUE, errors);
        b.pendingWarningPercent = doubleRange(env, "PENDING_APPEND_WARNING_PERCENT",
                0.80, 0.10, 0.99, errors);

        // ---- Timing ----
        int timeoutSec = intRange(env, "APPEND_TIMEOUT_SECONDS", 5, 1, 30, errors);
        b.appendTimeout = Duration.ofSeconds(timeoutSec);
        b.drainDeadline = Duration.ofSeconds(
                intRange(env, "DRAIN_DEADLINE_SECONDS", 30, 1, 300, errors));
        b.clockOffsetLimitMs = longRange(env, "CLOCK_OFFSET_LIMIT_MS",
                100L, 10L, 60_000L, errors);
        b.arrowMaxEventAgeMs = requiredLong(env, "ARROW_MAX_EVENT_AGE_MS", errors);
        b.arrowMaxFutureEventSkewMs = requiredLong(env, "ARROW_MAX_FUTURE_EVENT_SKEW_MS", errors);

        // ---- HFT connection policy (plan §IngestionConfig — exact values) ----
        b.arrowHftConnections = exactInt(env, "ARROW_HFT_CONNECTIONS", 1, errors);
        b.arrowHftMaxTokensPerConnection = exactInt(env, "ARROW_HFT_MAX_TOKENS_PER_CONNECTION", 1024, errors);
        b.arrowHftMaxTokensPerRequest = exactInt(env, "ARROW_HFT_MAX_TOKENS_PER_REQUEST", 512, errors);
        b.arrowHftHeartbeatSeconds = exactInt(env, "ARROW_HFT_HEARTBEAT_SECONDS", 3, errors);
        b.arrowHftStallTimeoutSeconds = intRange(env, "ARROW_HFT_STALL_TIMEOUT_SECONDS", 15, 5, 60, errors);
        b.arrowHftSubscriptionResponseTimeoutSeconds =
                intRange(env, "ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", 10, 1, 60, errors);
        b.arrowHftReconnectBaseSeconds = exactInt(env, "ARROW_HFT_RECONNECT_BASE_SECONDS", 1, errors);
        b.arrowHftReconnectMaxSeconds = exactInt(env, "ARROW_HFT_RECONNECT_MAX_SECONDS", 30, errors);
        b.arrowHftAuthRefreshAttempts = exactInt(env, "ARROW_HFT_AUTH_REFRESH_ATTEMPTS", 3, errors);
        b.arrowHftMinActiveSlots = exactInt(env, "ARROW_HFT_MIN_ACTIVE_SLOTS", 1, errors);
        b.arrowHftMultiConnectionApproved = "true".equalsIgnoreCase(
                env.getOrDefault("ARROW_HFT_MULTI_CONNECTION_APPROVED", "false"));
        b.ingestionAllowDegraded = "true".equalsIgnoreCase(
                env.getOrDefault("INGESTION_ALLOW_DEGRADED", "false"));

        // Production rejects degraded mode and unapproved multi-connection.
        String deployEnv = env.getOrDefault("DEPLOY_ENV", "dev");
        boolean production = "prod".equalsIgnoreCase(deployEnv)
                || "production".equalsIgnoreCase(deployEnv);
        if (production) {
            if (b.ingestionAllowDegraded) {
                errors.add("INGESTION_ALLOW_DEGRADED must be false in production");
            }
            if (b.arrowHftMultiConnectionApproved) {
                errors.add("ARROW_HFT_MULTI_CONNECTION_APPROVED must be false until broker evidence "
                        + "proves the account may hold the configured socket count");
            }
        }

        // ---- Fingerprint & SDK version ----
        // Pinned in versions.pin (go-arrow v0.0.0-20260622-7cce1630, tree
        // sha256:f622f8a9...); fallback kept for dev, logged as warning.
        b.goArrowSdkVersion = optionalWithFallback(env, "GO_ARROW_SDK_VERSION",
                "v0.0.0-20260622-7cce1630");

        // ---- DDL & clock strictness ----
        b.allowRuntimeDdl = "true".equalsIgnoreCase(
                env.getOrDefault("ALLOW_RUNTIME_DDL", "false"));
        b.clockCheckRequired = "true".equalsIgnoreCase(
                env.getOrDefault("CLOCK_CHECK_REQUIRED", "false"));
        b.uncertaintyJournalPath = optional(env, "UNCERTAINTY_JOURNAL_PATH");

        // ---- Standard derived values ----
        // (MAX_PENDING_APPEND_BYTES is validated exactly once by the longRange
        // call above — the duplicate block that re-parsed the env key and
        // bypassed the 1 MiB floor was removed, R-156.)

        // ---- Fail if any errors ----
        if (!errors.isEmpty()) {
            String msg = "Ingestion config validation failed (" + errors.size() + " errors):\n  "
                    + String.join("\n  ", errors);
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        IngestionConfig cfg = b.build();
        LOG.info("ingestion-config: validated {} keys", cfg.toMap().size());
        return cfg;
    }

    /** Diagnostic dump — never logs secrets. */
    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ARROW_APP_ID", "***" + arrowAppId.substring(Math.max(0, arrowAppId.length() - 4)));
        m.put("ARROW_APP_SECRET", "***");
        m.put("ARROW_TOKEN", arrowToken.isBlank() ? "(not set)" : "***");
        m.put("ARROW_USER_ID", arrowUserId.isBlank() ? "(not set)" : "***");
        m.put("ARROW_AUTH_METHOD", !arrowToken.isBlank() ? "token" : "autologin");
        m.put("ARROW_HFT_LATENCY_MS", arrowHftLatencyMs);
        m.put("ARROW_INSTRUMENT_TOKENS", arrowInstrumentTokens.isBlank() ? "(synthetic)" : "***");
        m.put("FLUSS_BOOTSTRAP", flussBootstrap);
        m.put("RAW_TABLE_NAME", rawTableName);
        m.put("INGESTION_MAX_BATCH_RECORDS", maxBatchRecords);
        m.put("INGESTION_MAX_BATCH_WAIT_MS", maxBatchWaitMs);
        m.put("MAX_PENDING_APPEND_RECORDS", maxPendingRecords);
        m.put("MAX_PENDING_APPEND_BYTES", maxPendingBytes);
        m.put("PENDING_APPEND_WARNING_PERCENT", pendingWarningPercent);
        m.put("APPEND_TIMEOUT", appendTimeout);
        m.put("DRAIN_DEADLINE_SECONDS", drainDeadline.getSeconds());
        m.put("CLOCK_OFFSET_LIMIT_MS", clockOffsetLimitMs);
        m.put("ARROW_MAX_EVENT_AGE_MS", arrowMaxEventAgeMs);
        m.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", arrowMaxFutureEventSkewMs);
        m.put("GO_ARROW_SDK_VERSION", goArrowSdkVersion);
        m.put("ALLOW_RUNTIME_DDL", allowRuntimeDdl);
        m.put("CLOCK_CHECK_REQUIRED", clockCheckRequired);
        m.put("UNCERTAINTY_JOURNAL_PATH", uncertaintyJournalPath.isBlank() ? "(default)" : uncertaintyJournalPath);
        m.put("ARROW_HFT_CONNECTIONS", arrowHftConnections);
        m.put("ARROW_HFT_MAX_TOKENS_PER_CONNECTION", arrowHftMaxTokensPerConnection);
        m.put("ARROW_HFT_MAX_TOKENS_PER_REQUEST", arrowHftMaxTokensPerRequest);
        m.put("ARROW_HFT_HEARTBEAT_SECONDS", arrowHftHeartbeatSeconds);
        m.put("ARROW_HFT_STALL_TIMEOUT_SECONDS", arrowHftStallTimeoutSeconds);
        m.put("ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", arrowHftSubscriptionResponseTimeoutSeconds);
        m.put("ARROW_HFT_RECONNECT_BASE_SECONDS", arrowHftReconnectBaseSeconds);
        m.put("ARROW_HFT_RECONNECT_MAX_SECONDS", arrowHftReconnectMaxSeconds);
        m.put("ARROW_HFT_AUTH_REFRESH_ATTEMPTS", arrowHftAuthRefreshAttempts);
        m.put("ARROW_HFT_MIN_ACTIVE_SLOTS", arrowHftMinActiveSlots);
        m.put("ARROW_HFT_MULTI_CONNECTION_APPROVED", arrowHftMultiConnectionApproved);
        m.put("INGESTION_ALLOW_DEGRADED", ingestionAllowDegraded);
        return m;
    }

    // ---- env helpers ----

    private static String required(Map<String, String> env, String key, List<String> errors) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            errors.add(key + " is required but not set");
            return "";
        }
        return v;
    }

    private static long requiredLong(Map<String, String> env, String key, List<String> errors) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            errors.add(key + " is required but not set");
            return 0L;
        }
        try {
            long parsed = Long.parseLong(value);
            // R-114: a 0 ms age/skew limit would quarantine every tick whose
            // receive time differs by even 1ms — these limits must be positive.
            if (parsed <= 0) errors.add(key + " must be positive (>0)");
            return parsed;
        } catch (NumberFormatException e) {
            errors.add(key + " must be an integer, got: " + value);
            return 0L;
        }
    }

    /**
     * Optional key with a plan default (R-226): the old "required-with-fallback"
     * overload's {@code errors} parameter was never used — it always warned and
     * returned the fallback, which misled maintainers into thinking
     * {@code GO_ARROW_SDK_VERSION} is mandatory and that violations reach the
     * error list. It is optional with a dev fallback (the pinned version in
     * versions.pin); missing values log a warning.
     */
    private static String optionalWithFallback(Map<String, String> env, String key, String fallback) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            LOG.warn("ingestion-config: {} not set; using fallback {}", key, fallback);
            return fallback;
        }
        return v;
    }

    private static String optional(Map<String, String> env, String key) {
        String v = env.get(key);
        return v != null ? v : "";
    }

    private static int exactInt(Map<String, String> env, String key, int expected, List<String> errors) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            LOG.warn("ingestion-config: {} not set; using plan default {}", key, expected);
            return expected; // default matches expected
        }
        int parsed;
        try {
            parsed = Integer.parseInt(v);
        } catch (NumberFormatException e) {
            errors.add(key + " must be an integer, got: " + v);
            return expected;
        }
        if (parsed != expected) {
            errors.add(key + " must be exactly " + expected + ", got: " + parsed);
        }
        return parsed;
    }

    private static int intRange(Map<String, String> env, String key, int defVal, int min, int max, List<String> errors) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            LOG.warn("ingestion-config: {} not set; using default {}", key, defVal);
            return defVal;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(v);
        } catch (NumberFormatException e) {
            errors.add(key + " must be an integer, got: " + v);
            return defVal;
        }
        if (parsed < min || parsed > max) {
            errors.add(key + " must be in range [" + min + ", " + max + "], got: " + parsed);
        }
        return parsed;
    }

    private static long longRange(Map<String, String> env, String key, long defVal, long min, long max, List<String> errors) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            LOG.warn("ingestion-config: {} not set; using default {}", key, defVal);
            return defVal;
        }
        long parsed;
        try {
            parsed = Long.parseLong(v);
        } catch (NumberFormatException e) {
            errors.add(key + " must be a long, got: " + v);
            return defVal;
        }
        if (parsed < min || parsed > max) {
            errors.add(key + " must be in range [" + min + ", " + max + "], got: " + parsed);
        }
        return parsed;
    }

    private static double doubleRange(Map<String, String> env, String key, double defVal, double min, double max,
                                       List<String> errors) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            LOG.warn("ingestion-config: {} not set; using default {}", key, defVal);
            return defVal;
        }
        double parsed;
        try {
            parsed = Double.parseDouble(v);
        } catch (NumberFormatException e) {
            errors.add(key + " must be a number, got: " + v);
            return defVal;
        }
        if (parsed < min || parsed > max) {
            errors.add(key + " must be in range [" + min + ", " + max + "], got: " + parsed);
        }
        return parsed;
    }

    // ---- Builder ----

    private static class Builder {
        String arrowAppId = "", arrowAppSecret = "", arrowToken = "";
        String arrowUserId = "", arrowPassword = "", arrowTotpKey = "";
        int arrowHftLatencyMs = 50;
        String arrowInstrumentTokens = "";
        String flussBootstrap = "fluss-coordinator:9123";
        String rawTableName = "raw_table_1";
        int maxBatchRecords = 1, maxBatchWaitMs;
        int maxPendingRecords = 50_000;
        long maxPendingBytes = 67_108_864L;
        double pendingWarningPercent = 0.80;
        Duration appendTimeout = Duration.ofSeconds(5);
        Duration drainDeadline = Duration.ofSeconds(30);
        long clockOffsetLimitMs = 100L;
        long arrowMaxEventAgeMs;
        long arrowMaxFutureEventSkewMs;
        String goArrowSdkVersion = "v0.0.0-20260622-7cce1630";
        boolean allowRuntimeDdl;
        boolean clockCheckRequired;
        String uncertaintyJournalPath = "";
        int arrowHftConnections = 1;
        int arrowHftMaxTokensPerConnection = 1024;
        int arrowHftMaxTokensPerRequest = 512;
        int arrowHftHeartbeatSeconds = 3;
        int arrowHftStallTimeoutSeconds = 15;
        int arrowHftSubscriptionResponseTimeoutSeconds = 10;
        int arrowHftReconnectBaseSeconds = 1;
        int arrowHftReconnectMaxSeconds = 30;
        int arrowHftAuthRefreshAttempts = 3;
        int arrowHftMinActiveSlots = 1;
        boolean arrowHftMultiConnectionApproved;
        boolean ingestionAllowDegraded;

        IngestionConfig build() {
            return new IngestionConfig(this);
        }
    }
}
