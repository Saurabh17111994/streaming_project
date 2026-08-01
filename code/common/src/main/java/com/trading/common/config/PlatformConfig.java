package com.trading.common.config;

import java.util.Map;
import java.util.Set;

/**
 * Centralized, versioned runtime configuration constants for the trading platform.
 *
 * <p>No numeric literals for these keys may be scattered through source files. Startup must
 * reject two values outright (see {@link #validateStartup()}):
 * <ul>
 *   <li>{@code DEDUP_TTL_MS} must equal 300000</li>
 *   <li>{@code CANDLE_WINDOW_MS} must equal 15000</li>
 * </ul>
 *
 * <p>Source: docs/08_implementation/01-foundation.md &rarr; "Required configuration constants"
 * (orig L37) and "Fixed scope" (orig L23).
 */
public final class PlatformConfig {

    private PlatformConfig() {}

    // ---- ingestion / workload profile ----
    public static final int BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC = 20;
    public static final int BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC = 30;
    public static final int INGESTION_MAX_BATCH_RECORDS = 1;
    public static final int INGESTION_MAX_BATCH_WAIT_MS = 0;
    public static final int MAX_PENDING_APPEND_RECORDS = 10000;
    public static final int PENDING_APPEND_WARNING_PERCENT = 80;

    // ---- dedup / candles (reject-startup values) ----
    public static final long DEDUP_TTL_MS = 300_000L;
    public static final long CANDLE_WINDOW_MS = 15_000L;

    // ---- checkpointing ----
    public static final long CHECKPOINT_INTERVAL_MS = 10_000L;
    public static final long CHECKPOINT_TIMEOUT_MS = 30_000L;
    public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

    // ---- JVM / container memory ----
    public static final int JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65;
    public static final int NON_HEAP_MEMORY_RESERVE_PERCENT = 35;
    public static final int CONTAINER_MEMORY_ALERT_PERCENT = 85;

    // ---- signal job ----
    public static final int MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT = 1;

    /**
     * {@code MAX_PENDING_APPEND_BYTES = min(67108864, floor(container_memory_limit_bytes * 0.10))}.
     * Capped at 64 MiB so very large container limits do not over-buffer.
     */
    public static long maxPendingAppendBytes(long containerMemoryLimitBytes) {
        long derived = (long) Math.floor(containerMemoryLimitBytes * 0.10);
        return Math.min(67_108_864L, derived);
    }

    /**
     * The two constants whose value is load-bearing for correctness; any other value must abort
     * startup rather than degrade silently.
     */
    public static void validateStartup() {
        if (DEDUP_TTL_MS != 300_000L) {
            throw new IllegalStateException(
                "DEDUP_TTL_MS must be 300000 (5 min); found " + DEDUP_TTL_MS
                    + ". Refusing to start (docs/08_implementation/01-foundation.md L37).");
        }
        if (CANDLE_WINDOW_MS != 15_000L) {
            throw new IllegalStateException(
                "CANDLE_WINDOW_MS must be 15000 (15 s); found " + CANDLE_WINDOW_MS
                    + ". Refusing to start (docs/08_implementation/01-foundation.md L37).");
        }
    }

    /**
     * Configuration invariant (orig L716): a required config value missing at load time means the
     * component is NOT ready. No unsafe default may be substituted.
     */
    public static void requirePresent(Map<String, String> loaded, Set<String> requiredKeys) {
        for (String key : requiredKeys) {
            String v = loaded.get(key);
            if (v == null || v.isBlank()) {
                throw new IllegalStateException(
                    "Required configuration '" + key + "' is missing or blank; component is not ready.");
            }
        }
    }
}
