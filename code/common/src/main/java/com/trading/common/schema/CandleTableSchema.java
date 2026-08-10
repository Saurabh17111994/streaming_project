package com.trading.common.schema;

import java.util.List;

/**
 * Shared, versioned contract for the feature-candle tables
 * (CANDLE-KV-REPLAY-001, docs/08_implementation/13-candle-log-kv-replay-safety.md).
 *
 * <p>Two tables carry the same 15-column v2 candle row
 * ({@code code/01_platform/02_sql/ddl/03_feature_candles_15s.sql},
 * R-012):
 * <ul>
 *   <li>{@link #LOG_TABLE} — immutable LOG, one final row per non-empty 15s
 *       window. The append-only evidence trail; duplicates from a full replay
 *       are permanent (no row-level delete on a LOG).</li>
 *   <li>{@link #CURRENT_TABLE} — KV companion keyed by
 *       {@code (instrument_token, window_start)}. Idempotent: a replayed or
 *       re-emitted candle upserts the same key instead of appending a
 *       duplicate, so consumers always read the canonical current row.</li>
 * </ul>
 *
 * <p>The KV primary key is a <em>superset</em> of the bucket key: the Fluss
 * connector requires only {@code bucket.key ⊆ primary key} (fluss-common
 * {@code TableDescriptor}: {@code pkColumns.containsAll(bucketKeys)}), so the
 * KV table keeps {@code bucket.key=instrument_token} and colocates with the
 * LOG table — every candle of a ticker lands in the same bucket in both
 * tables.
 *
 * <p>{@code output_ts} is emit metadata, not row identity: two rows for the
 * same key that agree on the business fields are the same canonical candle
 * re-emitted (replay / restart), regardless of {@code output_ts}
 * ({@code CanonicalCandlePolicy}, compute module).
 */
public final class CandleTableSchema {

    private CandleTableSchema() {}

    /** DDL schema version of the candle row (v2 since R-012). */
    public static final String ROW_SCHEMA_VERSION = "2";

    /** Immutable LOG table (evidence trail). */
    public static final String LOG_TABLE = "feature_candles_15s";

    /** KV current-state table (idempotent canonical rows). */
    public static final String CURRENT_TABLE = "feature_candles_15s_current";

    /** Both tables use 16 buckets. */
    public static final int BUCKET_COUNT = 16;

    /** Both tables route by instrument_token (colocation contract). */
    public static final String BUCKET_KEY = "instrument_token";

    /**
     * The 15 candle columns in DDL index order (v2, R-012). Physical writer
     * layouts ({@code CandleTableColumns}) MUST derive from this list so the
     * LOG and KV sinks, the preflight metadata validator, the migration tool,
     * and the DDLs cannot drift apart.
     */
    public static final List<String> COLUMNS = List.of(
            "instrument_token",
            "exchange",
            "symbol",
            "window_start",
            "window_end",
            "open_paise",
            "high_paise",
            "low_paise",
            "close_paise",
            "volume",
            "tick_count",
            "algorithm_version",
            "configuration_version",
            "output_ts",
            "schema_version");

    /** KV primary key of {@link #CURRENT_TABLE} (canonical row identity). */
    public static final List<String> KEY_COLUMNS = List.of("instrument_token", "window_start");

    /** Column count — must equal {@code COLUMNS.size()}; mirrors the DDL. */
    public static final int FIELD_COUNT = COLUMNS.size();
}
