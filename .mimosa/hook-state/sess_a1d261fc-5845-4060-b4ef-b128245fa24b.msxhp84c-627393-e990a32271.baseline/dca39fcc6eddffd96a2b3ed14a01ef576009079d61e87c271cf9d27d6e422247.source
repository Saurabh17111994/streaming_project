package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the durable forming-bar KV projection
 * {@code forming_bar} ({@code code/01_platform/02_sql/ddl/04_forming_bar.sql}
 * v1, PK {@code instrument_token}, 16 buckets). Pinned by
 * {@code FormingBarTableColumnsAgreementTest} against the DDL file.
 *
 * <p>v1 deliberately drops the in-process {@code windowEnd} (the FormingBar
 * record carries it; the KV current-state projection does not — the window
 * end is {@code window_start + window} at the consumer, and persisting it
 * invites a second source of truth). The mapper and the record stay in step
 * here.
 */
public final class FormingBarTableColumns {

    private FormingBarTableColumns() {}

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int WINDOW_START = 1;
    public static final int OPEN_PAISE = 2;
    public static final int HIGH_PAISE = 3;
    public static final int LOW_PAISE = 4;
    public static final int CLOSE_PAISE = 5;
    public static final int VOLUME = 6;
    public static final int TICK_COUNT = 7;
    public static final int LAST_EVENT_TIME = 8;
    public static final int LAST_EVENT_FINGERPRINT = 9;
    public static final int SCHEMA_VERSION = 10;

    public static final int FIELD_COUNT = 11;

    public static final String SCHEMA_VERSION_V1 = "1";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "INTEGER", "BIGINT", "STRING", "STRING");

    /** DDL nullability per column (04 DDL v1): last_event_fingerprint nullable. */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false, false, false, false, true, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final String[] NAMES = {
            "instrument_token", "window_start", "open_paise", "high_paise",
            "low_paise", "close_paise", "volume", "tick_count",
            "last_event_time", "last_event_fingerprint", "schema_version"
    };

    /** Stream type info for emitted forming-bar rows (v1 DDL order). */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(),                        // instrument_token
                new BigIntType(),                        // window_start
                new BigIntType(),                        // open_paise
                new BigIntType(),                        // high_paise
                new BigIntType(),                        // low_paise
                new BigIntType(),                        // close_paise
                new BigIntType(),                        // volume
                new IntType(),                           // tick_count
                new BigIntType(),                        // last_event_time
                new VarCharType(VarCharType.MAX_LENGTH), // last_event_fingerprint (nullable)
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES);
}
