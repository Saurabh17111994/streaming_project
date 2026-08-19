package com.trading.common.schema.ownership;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * SCH-15 cross-boundary pin: each {@link ColumnOwnership} matrix must stay a
 * faithful mirror of its DDL — full column coverage (no unowned column), every
 * PK column identity, {@code schema_version} identity, and the schema-version
 * header — so a DDL change or a one-sided ownership edit fails the suite
 * before any Executor-era writer exists.
 */
class ColumnOwnershipAgreementTest {

    /** CWD is the common module dir (code/common); DDLs live one level up. */
    private static final Path DDL_DIR = Path.of("../01_platform/02_sql/ddl").toAbsolutePath();

    private static final Pattern COLUMN = Pattern.compile(
            "^\\s*([a-z_][a-z0-9_]*)\\s+(STRING|BIGINT|INT|BYTES|DOUBLE|FLOAT|BOOLEAN)(.*)$",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private record TableCase(String label, ColumnOwnership matrix, String ddlFile,
                             String schemaVersionHeader, String schemaVersion) {
        @Override
        public String toString() {
            return label;
        }
    }

    static Stream<TableCase> tables() {
        return Stream.of(
                new TableCase("Order_Lifecycle", OrderLifecycleColumnOwnership.MATRIX,
                        "09_order_lifecycle.sql", "Schema version: 2", "2"),
                new TableCase("Positions", PositionsColumnOwnership.MATRIX,
                        "10_positions.sql", "Schema version: 2", "2"),
                new TableCase("Execution_Gate", ExecutionGateColumnOwnership.MATRIX,
                        "11_execution_gate.sql", "Schema version: 3", "3"),
                new TableCase("Execution_Attempts", ExecutionAttemptsColumnOwnership.MATRIX,
                        "12_execution_attempts.sql", "Schema version: 3", "3"));
    }

    private static List<String> ddlColumnNames(String ddlFile) throws IOException {
        Path p = DDL_DIR.resolve(ddlFile);
        assertThat(p).as("missing DDL file %s", p).exists();
        String ddl = Files.readString(p, StandardCharsets.UTF_8);
        List<String> names = new ArrayList<>();
        Matcher m = COLUMN.matcher(ddl);
        while (m.find()) {
            names.add(m.group(1).toLowerCase(Locale.ROOT));
        }
        return names;
    }

    private static String primaryKeyClause(String ddlFile) throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(ddlFile), StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("PRIMARY KEY\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE)
                .matcher(ddl);
        assertThat(m.find()).as("PRIMARY KEY clause in %s", ddlFile).isTrue();
        return m.group(1).trim();
    }

    @ParameterizedTest
    @MethodSource("tables")
    void matrixCoversEveryDdlColumn(TableCase tc) throws IOException {
        List<String> ddlNames = ddlColumnNames(tc.ddlFile());
        assertThat(ddlNames).as("DDL column count for %s", tc.label)
                .hasSize(tc.matrix().columnNames().length);
        for (int i = 0; i < ddlNames.size(); i++) {
            boolean owned = tc.matrix().isIdentity(i) || tc.matrix().writerFor(i) != null;
            assertThat(owned)
                    .as("column %d (%s) of %s must be identity or writer-owned", i, ddlNames.get(i), tc.label)
                    .isTrue();
        }
    }

    @ParameterizedTest
    @MethodSource("tables")
    void pkColumnsAreIdentity(TableCase tc) throws IOException {
        String pk = primaryKeyClause(tc.ddlFile());
        List<String> ddlNames = ddlColumnNames(tc.ddlFile());
        for (String pkCol : pk.split(",")) {
            String name = pkCol.trim().toLowerCase(Locale.ROOT);
            int idx = ddlNames.indexOf(name);
            assertThat(idx).as("PK column %s of %s exists in DDL", name, tc.label)
                    .isGreaterThanOrEqualTo(0);
            assertThat(tc.matrix().isIdentity(idx))
                    .as("PK column %s of %s must be identity (creation-only)", name, tc.label)
                    .isTrue();
        }
    }

    @ParameterizedTest
    @MethodSource("tables")
    void schemaVersionColumnIsIdentity(TableCase tc) {
        String[] names = tc.matrix().columnNames();
        int schemaVersionIdx = names.length - 1;
        assertThat(names[schemaVersionIdx]).isEqualTo("schema_version");
        assertThat(tc.matrix().isIdentity(schemaVersionIdx))
                .as("schema_version of %s must be identity (stamped at creation, preserved by merge)", tc.label)
                .isTrue();
    }

    @ParameterizedTest
    @MethodSource("tables")
    void schemaVersionHeaderMatchesMatrix(TableCase tc) throws IOException {
        String ddl = Files.readString(DDL_DIR.resolve(tc.ddlFile()), StandardCharsets.UTF_8);
        assertThat(ddl).containsIgnoringCase(tc.schemaVersionHeader);
        assertThat(tc.matrix().schemaVersion()).isEqualTo(tc.schemaVersion);
    }

    @ParameterizedTest
    @MethodSource("tables")
    void everyWriterColumnResolvesToPinnedName(TableCase tc) {
        String[] names = tc.matrix().columnNames();
        for (ColumnOwnership.Writer w : tc.matrix().writers()) {
            for (int idx : w.columns()) {
                assertThat(names[idx]).as("writer %s column %d of %s", w.name(), idx, tc.label)
                        .isNotBlank();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("tables")
    void checkWriteAcceptsOnlyOwnedColumns(TableCase tc) {
        for (ColumnOwnership.Writer w : tc.matrix().writers()) {
            tc.matrix().checkWrite(w.name(), w.columns());
            assertThatThrownBy(() -> tc.matrix().checkWrite(w.name(), tc.matrix().identityColumns()))
                    .as("writer %s of %s must not partial-update identity columns", w.name(), tc.label)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        // cross-writer: a writer must never touch another writer's group
        if (tc.matrix().writers().length > 1) {
            ColumnOwnership.Writer w0 = tc.matrix().writers()[0];
            ColumnOwnership.Writer w1 = tc.matrix().writers()[1];
            assertThatThrownBy(() -> tc.matrix().checkWrite(w0.name(), w1.columns()))
                    .as("writer %s of %s must not write %s's group", w0.name(), tc.label, w1.name())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @ParameterizedTest
    @MethodSource("tables")
    void matrixColumnNamesMatchDdlInOrder(TableCase tc) throws IOException {
        // the matrix's columnNames must equal the DDL names exactly, in order
        assertThat(ddlColumnNames(tc.ddlFile())).as("%s pinned names", tc.label)
                .containsExactlyElementsOf(Arrays.asList(tc.matrix().columnNames()));
    }
}
