package com.trading.compute.signaljob;

import com.trading.common.schema.CandleTableSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the feature-candle rows written by the Signal job
 * to the LOG sink ({@code feature_candles_15s}) — the sole candle output
 * since the KV current-state twin was retired 2026-08-13.
 *
 * <p>Must mirror {@link CandleTableSchema} (the shared 15-column v2 contract,
 * CANDLE-KV-REPLAY-001) — {@link #FIELD_COUNT} and {@link #NAMES} derive from
 * it, and {@code code/01_platform/02_sql/ddl/03_feature_candles_15s.sql}
 * exactly. The Fluss sink's
 * {@code RowDataSerializationSchema} writes rows in table column order, so
 * emitted {@code GenericRowData} fields must be positioned accordingly
 * (R-012).
 */
public final class CandleTableColumns {

    private CandleTableColumns() {}

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int EXCHANGE = 1;
    public static final int SYMBOL = 2;
    public static final int WINDOW_START = 3;
    public static final int WINDOW_END = 4;
    public static final int OPEN_PAISE = 5;
    public static final int HIGH_PAISE = 6;
    public static final int LOW_PAISE = 7;
    public static final int CLOSE_PAISE = 8;
    public static final int VOLUME = 9;
    public static final int TICK_COUNT = 10;
    public static final int ALGORITHM_VERSION = 11;
    public static final int CONFIGURATION_VERSION = 12;
    public static final int OUTPUT_TS = 13;
    public static final int SCHEMA_VERSION = 14;

    /** Field count from the shared contract (must stay 15). */
    public static final int FIELD_COUNT = CandleTableSchema.FIELD_COUNT;

    /** DDL column names in index order — derived from the shared contract. */
    public static final String[] NAMES =
            CandleTableSchema.COLUMNS.toArray(new String[0]);

    /**
     * Stream type info for emitted candle rows, derived from the v2 DDL column
     * order. Declared explicitly because TypeExtractor cannot resolve a bare
     * {@code RowData} to a schema — it would fall back to GenericTypeInfo and
     * route RowData through Kryo at operator boundaries.
     */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(), new BigIntType(),
                new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType(),
                new BigIntType(), new IntType(),
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)
            },
            NAMES);
}
