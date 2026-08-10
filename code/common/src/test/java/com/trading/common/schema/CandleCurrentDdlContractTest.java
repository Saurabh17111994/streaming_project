package com.trading.common.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Candle DDL contract guards (CANDLE-KV-REPLAY-001 A4.1, evidence CANDLE-KV-001).
 *
 * <p>Both candle tables — the immutable LOG {@code 03_feature_candles_15s.sql}
 * and the KV current-state {@code 22_feature_candles_15s_current.sql} — must
 * agree with {@link CandleTableSchema} on the 15-column v2 order and stay
 * routing-compatible:
 * <ul>
 *   <li>The LOG keeps its append-only nature: it MUST NOT gain a primary key
 *       (a PK would flip it to KV and destroy the evidence trail).</li>
 *   <li>The KV table MUST declare exactly {@code PRIMARY KEY
 *       (instrument_token, window_start) NOT ENFORCED} and keep
 *       {@code bucket.key=instrument_token} — a subset of the PK, so every
 *       candle of a ticker lands in the same bucket as its LOG twin
 *       (colocation contract).</li>
 *   <li>Both tables share the identical 15-column layout, types, bucket count,
 *       and 7-day LOG retention.</li>
 * </ul>
 */
@DisplayName("Candle DDL contract: LOG stays LOG, KV carries the exact PK, columns match the shared schema")
class CandleCurrentDdlContractTest {

    /** CWD is the common module dir; DDLs live under code/01_platform/02_sql/ddl. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();

    private static final Pattern CREATE_TABLE = Pattern.compile(
            "(?i)\\bCREATE\\s+TABLE\\s+([A-Za-z_][A-Za-z0-9_]*)");
    private static final Pattern PRIMARY_KEY = Pattern.compile(
            "PRIMARY\\s+KEY\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern BUCKET_KEY = Pattern.compile(
            "'bucket\\.key'\\s*=\\s*'([^']*)'");
    private static final Pattern BUCKET_NUM = Pattern.compile(
            "'bucket\\.num'\\s*=\\s*'([^']*)'");
    private static final Pattern LOG_TTL = Pattern.compile(
            "'table\\.log\\.ttl'\\s*=\\s*'([^']*)'");
    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    @Test
    @DisplayName("LOG DDL stays LOG: no primary key, instrument_token bucket, 15 shared columns")
    void logDdlRemainsImmutableLog() throws IOException {
        String ddl = readDdl("03_feature_candles_15s.sql");
        assertEquals("feature_candles_15s", tableName(ddl));
        assertFalse(hasPrimaryKey(ddl),
                "feature_candles_15s is the immutable evidence trail — it must never gain a PRIMARY KEY "
                        + "(a PK flips the table to KV and breaks append-only evidence)");
        assertColumnContract(ddl, CandleTableSchema.LOG_TABLE);
    }

    @Test
    @DisplayName("KV DDL declares the exact (instrument_token, window_start) PK and keeps the shared column layout")
    void kvDdlCarriesExactPrimaryKey() throws IOException {
        String ddl = readDdl("22_feature_candles_15s_current.sql");
        assertEquals("feature_candles_15s_current", tableName(ddl));

        assertTrue(hasPrimaryKey(ddl), "feature_candles_15s_current must be a KV table (PRIMARY KEY present)");
        List<String> pk = primaryKeyColumns(ddl);
        assertEquals(CandleTableSchema.KEY_COLUMNS, pk,
                "KV primary key must be exactly " + CandleTableSchema.KEY_COLUMNS);

        assertColumnContract(ddl, CandleTableSchema.CURRENT_TABLE);
    }

    @Test
    @DisplayName("both candle DDLs agree with the shared 15-column schema, routing, and retention")
    void bothDdlsAgreeWithSharedSchema() throws IOException {
        String log = readDdl("03_feature_candles_15s.sql");
        String kv = readDdl("22_feature_candles_15s_current.sql");

        List<String> logCols = parseColumns(log);
        List<String> kvCols = parseColumns(kv);
        assertEquals(CandleTableSchema.COLUMNS, logCols,
                "LOG DDL column order must match the shared contract");
        assertEquals(CandleTableSchema.COLUMNS, kvCols,
                "KV DDL column order must match the shared contract");

        assertEquals(parseTypes(log), parseTypes(kv),
                "LOG and KV DDLs must declare identical column types");

        assertEquals("16", bucketNum(log), "LOG bucket.num must stay 16");
        assertEquals("16", bucketNum(kv), "KV bucket.num must stay 16");
        assertEquals("7d", logTtl(log), "LOG retention must stay 7d");
        assertEquals("7d", logTtl(kv), "KV retention must stay 7d");
    }

    // ---- shared column-layout assertions ----

    private static void assertColumnContract(String ddl, String table) throws IOException {
        List<String> cols = parseColumns(ddl);
        assertEquals(CandleTableSchema.FIELD_COUNT, cols.size(),
                table + " DDL must declare " + CandleTableSchema.FIELD_COUNT + " columns");
        assertEquals(CandleTableSchema.COLUMNS, cols,
                table + " DDL column order must match the shared contract");
        String bucket = bucketKey(ddl);
        assertNotNull(bucket, table + " must declare bucket.key");
        assertEquals(CandleTableSchema.BUCKET_KEY, bucket,
                table + " must route by " + CandleTableSchema.BUCKET_KEY + " (colocation contract)");
    }

    // ---- DDL parsing helpers ----

    private static String readDdl(String name) throws IOException {
        Path p = DDL_DIR.resolve(name);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private static String tableName(String ddl) {
        Matcher m = CREATE_TABLE.matcher(ddl);
        assertTrue(m.find(), "no CREATE TABLE in DDL");
        return m.group(1);
    }

    private static boolean hasPrimaryKey(String ddl) {
        return PRIMARY_KEY.matcher(ddl).find();
    }

    private static List<String> primaryKeyColumns(String ddl) {
        Matcher m = PRIMARY_KEY.matcher(ddl);
        assertTrue(m.find(), "no PRIMARY KEY in DDL");
        List<String> out = new ArrayList<>();
        for (String col : m.group(1).split(",")) {
            out.add(col.trim().toLowerCase(java.util.Locale.ROOT));
        }
        return out;
    }

    private static String bucketKey(String ddl) {
        Matcher m = BUCKET_KEY.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    private static String bucketNum(String ddl) {
        Matcher m = BUCKET_NUM.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    private static String logTtl(String ddl) {
        Matcher m = LOG_TTL.matcher(ddl);
        return m.find() ? m.group(1) : null;
    }

    /** Top-level column names in declared order. */
    private static List<String> parseColumns(String ddl) {
        List<String> names = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            names.add(m.group(1).toLowerCase(java.util.Locale.ROOT));
        }
        return names;
    }

    private static List<String> parseTypes(String ddl) {
        List<String> types = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            types.add(m.group(2).toUpperCase(java.util.Locale.ROOT));
        }
        return types;
    }
}
