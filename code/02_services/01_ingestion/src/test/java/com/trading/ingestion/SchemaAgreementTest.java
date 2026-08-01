package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * ING-SCHEMA-001: managed writers append exactly the columns declared in their
 * offline DDL files (plan §Database: raw_table_1, suspected_discontinuities,
 * ingestion_quarantine must match source DDL column order and count).
 *
 * <p>This parses the DDL SQL files under code/01_platform/02_sql/ddl/ and
 * verifies the column name sequences match what the runtime schema descriptors
 * (DdlBootstrap) expect — the writers append positionally against those.
 */
@DisplayName("ING-SCHEMA-001: writer columns agree with source DDL")
class SchemaAgreementTest {

    /** CWD is the ingestion module dir; DDLs live one level up under code/01_platform. */
    private static final Path DDL_DIR = Path.of(
            "../../01_platform/02_sql/ddl"
    ).toAbsolutePath();

    /** DDL file name → expected first column (bucket key) per the plan. */
    private static final String[][] MANAGED_TABLES = {
            {"19_suspected_discontinuities.sql", "discontinuity_id"},
            {"21_ingestion_quarantine.sql", "quarantine_id"},
    };

    @Test
    @DisplayName("discontinuity DDL declares the 11-column schema in plan order")
    void discontinuityDdlMatchesPlan() throws IOException {
        List<String> cols = parseColumns(readDdl("19_suspected_discontinuities.sql"));
        assertEquals(11, cols.size(), "suspected_discontinuities must have 11 columns");
        String[] want = {
                "discontinuity_id", "source", "reason", "connection_epoch",
                "last_tick_ts", "last_tick_fingerprint", "last_tick_token",
                "last_tick_exchange", "last_tick_symbol", "detected_ts",
                "schema_version"};
        for (int i = 0; i < want.length; i++) {
            assertEquals(want[i], cols.get(i), "column " + i);
        }
    }

    @Test
    @DisplayName("ingestion_quarantine DDL declares the 10-column schema in plan order")
    void quarantineDdlMatchesPlan() throws IOException {
        List<String> cols = parseColumns(readDdl("21_ingestion_quarantine.sql"));
        assertEquals(10, cols.size(), "ingestion_quarantine must have 10 columns");
        String[] want = {
                "quarantine_id", "reason", "instrument_token", "exchange",
                "symbol", "raw_payload", "payload_hash", "detected_ts",
                "detail", "schema_version"};
        for (int i = 0; i < want.length; i++) {
            assertEquals(want[i], cols.get(i), "column " + i);
        }
    }

    @Test
    @DisplayName("every managed DDL exists and declares its bucket key first")
    void managedDdlsExist() throws IOException {
        for (String[] table : MANAGED_TABLES) {
            Path p = DDL_DIR.resolve(table[0]);
            assertTrue(Files.exists(p), "missing DDL: " + p);
            List<String> cols = parseColumns(Files.readString(p, StandardCharsets.UTF_8));
            assertNotNull(cols);
            assertTrue(!cols.isEmpty(), "no columns parsed from " + table[0]);
            assertEquals(table[1], cols.get(0), "bucket key must be first column in " + table[0]);
        }
    }

    // ---- helpers ----

    private static String readDdl(String name) throws IOException {
        Path p = DDL_DIR.resolve(name);
        assertTrue(Files.exists(p), "missing DDL file: " + p);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    /** Extract the top-level CREATE TABLE column names in declared order. */
    private static List<String> parseColumns(String ddl) {
        List<String> out = new ArrayList<>();
        // Columns are the lines between "( " and ") WITH" that start with a name.
        Pattern col = Pattern.compile(
                "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)",
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher m = col.matcher(ddl);
        while (m.find()) {
            out.add(m.group(1).toLowerCase(java.util.Locale.ROOT));
        }
        return out;
    }
}
