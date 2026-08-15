package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the authoritative instruction-hash index
 * {@code trade_instruction_state} (SCH-19, REQ-FLS-008/015).
 *
 * <p>Must mirror the frozen 4-column layout of
 * {@code code/01_platform/02_sql/ddl/25_trade_instruction_state.sql} v1 (KV,
 * PK {@code instruction_id}) — the LOG twin of {@code Trade_Decisions} that
 * the instruction-feed protocol checks before every append. Pinned by
 * {@code TradeInstructionStateColumnsAgreementTest} against the DDL file
 * itself (cross-boundary pin habit).
 */
public final class TradeInstructionStateColumns {

    private TradeInstructionStateColumns() {}

    public static final int INSTRUCTION_ID = 0;
    public static final int CANONICAL_HASH = 1;
    public static final int FIRST_WRITTEN_TS = 2;
    public static final int SCHEMA_VERSION = 3;

    public static final int FIELD_COUNT = 4;

    public static final String SCHEMA_VERSION_V1 = "1";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of("STRING", "STRING", "BIGINT", "STRING");

    /** DDL nullability per column (25 DDL v1): all NOT NULL. */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(false, false, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final String[] NAMES = {
        "instruction_id", "canonical_hash", "first_written_ts", "schema_version"
    };

    /** Stream type info for emitted index rows, derived from the v1 DDL order. */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH), // instruction_id
                new VarCharType(VarCharType.MAX_LENGTH), // canonical_hash
                new BigIntType(),                        // first_written_ts
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES);
}
