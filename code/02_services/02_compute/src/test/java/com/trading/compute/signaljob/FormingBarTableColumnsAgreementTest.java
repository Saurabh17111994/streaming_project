package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.trading.common.model.FormingBar;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

/**
 * DEC-038 durable forming-bar projection: the {@link FormingBarTableColumns}
 * pin must mirror {@code code/01_platform/02_sql/ddl/04_forming_bar.sql} v1
 * (11 columns, DDL order, types, nullability), and the
 * {@link FormingBarRowMapper} round-trips the in-process record with the KV
 * layout — the storage half of Slice 2.2; the producer wiring lands with the
 * forming-bar computation.
 */
class FormingBarTableColumnsAgreementTest {

    private static final Path DDL_DIR = Path.of("../../01_platform/02_sql/ddl").toAbsolutePath();
    private static final String DDL_FILE = "04_forming_bar.sql";

    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)(.*)$",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private record Column(String name, String type, boolean nullableInDdl) {}

    private static List<Column> parseColumns() throws IOException {
        Path p = DDL_DIR.resolve(DDL_FILE);
        assert Files.exists(p) : "missing DDL file " + p;
        String ddl = Files.readString(p, StandardCharsets.UTF_8);
        List<Column> out = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            boolean notNull = m.group(3).toUpperCase(Locale.ROOT).contains("NOT NULL");
            String type = m.group(2).toUpperCase(Locale.ROOT);
            out.add(new Column(m.group(1).toLowerCase(Locale.ROOT),
                    type.equals("INT") ? "INTEGER" : type,
                    !notNull));
        }
        return out;
    }

    @Test
    void ddlDeclares11ColumnsInPinnedOrder() throws IOException {
        List<Column> cols = parseColumns();
        assertEquals(FormingBarTableColumns.FIELD_COUNT, cols.size());
        assertEquals(List.of(FormingBarTableColumns.NAMES),
                cols.stream().map(Column::name).toList());
    }

    @Test
    void ddlTypesMatchTypeRootsPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(FormingBarTableColumns.TYPE_ROOTS.get(i), cols.get(i).type(),
                    "column " + i + " (" + cols.get(i).name() + ") type root");
        }
    }

    @Test
    void ddlNullabilityMatchesPerColumn() throws IOException {
        List<Column> cols = parseColumns();
        for (int i = 0; i < cols.size(); i++) {
            assertEquals(FormingBarTableColumns.COLUMN_NULLABLE_IN_DDL.get(i),
                    cols.get(i).nullableInDdl(),
                    "column " + i + " (" + cols.get(i).name() + ") nullability");
        }
    }

    @Test
    void kvContractSingleFieldPkOnInstrumentToken() throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(DDL_FILE), StandardCharsets.UTF_8);
        assertEquals(true, ddl.contains("PRIMARY KEY (instrument_token) NOT ENFORCED"));
        assertEquals(true, ddl.contains("'bucket.key' = 'instrument_token'"));
    }

    @Test
    void mapperRoundTripsRecordToRowAndBack() {
        FormingBar bar = new FormingBar(
                12345L, 1_700_000_000_000L, 1_700_000_015_000L,
                100L, 120L, 90L, 110L, 1_000L, 42L, 1_700_000_014_999L, "fp-1");
        RowData row = FormingBarRowMapper.toRow(bar);

        assertEquals(FormingBarTableColumns.FIELD_COUNT, row.getArity());
        FormingBar back = FormingBarRowMapper.fromRow(row);
        assertEquals(bar.instrumentToken(), back.instrumentToken());
        assertEquals(bar.windowStart(), back.windowStart());
        assertEquals(bar.openPaise(), back.openPaise());
        assertEquals(bar.highPaise(), back.highPaise());
        assertEquals(bar.lowPaise(), back.lowPaise());
        assertEquals(bar.closePaise(), back.closePaise());
        assertEquals(bar.volume(), back.volume());
        assertEquals(bar.tickCount(), back.tickCount());
        assertEquals(bar.lastEventTime(), back.lastEventTime());
        assertEquals(bar.lastFingerprint(), back.lastFingerprint());
        // windowEnd is not persisted in v1 — the caller restores it.
        assertEquals(0L, back.windowEnd());
    }

    @Test
    void nullFingerprintRoundTripsAsNull() {
        FormingBar bar = new FormingBar(
                1L, 100L, 200L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, null);
        RowData row = FormingBarRowMapper.toRow(bar);
        assertEquals(true, row.isNullAt(FormingBarTableColumns.LAST_EVENT_FINGERPRINT));
        assertNull(FormingBarRowMapper.fromRow(row).lastFingerprint());
    }
}
