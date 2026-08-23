package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the execution lifecycle KV
 * {@code Position_State} ({@code code/01_platform/02_sql/ddl/29_position_state.sql}
 * v1, PK {@code instrument_token}, 16 buckets). Pinned against the DDL.
 *
 * <p>Writer: Execution Gateway / Nautilus (sole writer); Reader: Signal job
 * (feedback to clear ActiveSignalFilter). Lifecycle handshake for Option B:
 * Flink → signal → Nautilus → broker → Nautilus UPSERTS CLOSED. No TTL.
 */
public final class PositionStateTableColumns {

    private PositionStateTableColumns() {}

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int STATUS = 1;
    public static final int POSITION_ID = 2;
    public static final int UPDATED_TS = 3;
    public static final int CLOSED_TS = 4;
    public static final int CLOSED_REASON = 5;
    public static final int SCHEMA_VERSION = 6;

    public static final int FIELD_COUNT = 7;

    public static final String STATUS_OPEN = "OPEN";
    public static final String STATUS_CLOSED = "CLOSED";
    public static final String STATUS_ADMIN_CLEAR = "ADMIN_CLEAR";
    public static final String SCHEMA_VERSION_V1 = "1";

    /** Fluss DataTypeRoot per column, DDL order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "BIGINT", "STRING", "STRING", "BIGINT", "BIGINT", "STRING", "STRING");

    /** DDL nullability: position_id, closed_ts, closed_reason nullable. */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, true, false, true, true, false);

    public static final String[] NAMES = {
            "instrument_token", "status", "position_id",
            "updated_ts", "closed_ts", "closed_reason", "schema_version"
    };

    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(),                        // instrument_token
                new VarCharType(VarCharType.MAX_LENGTH), // status
                new VarCharType(VarCharType.MAX_LENGTH), // position_id
                new BigIntType(),                        // updated_ts
                new BigIntType(),                        // closed_ts
                new VarCharType(VarCharType.MAX_LENGTH), // closed_reason
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES);
}
