package com.trading.compute.signaljob;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the compute-side quarantine rows written by the
 * Signal job to the {@code ingestion_quarantine} LOG table — the SAME table
 * the ingestion pipeline writes invalid raw ticks to (DDL 21, schema version
 * 1; bucket key {@code quarantine_id}). The Signal job reuses the table for
 * candle evidence: a candle that fails one of the five
 * {@link CandleInvariantCheck} invariants is appended here instead of
 * {@code feature_candles_15s} (streaming-3000 T6, decision 24).
 *
 * <p>Must mirror {@code code/01_platform/02_sql/ddl/21_ingestion_quarantine.sql}
 * exactly: 10 columns, LOG (no primary key), {@code quarantine_id} routing,
 * {@code raw_payload} BYTES NOT NULL + {@code payload_hash} STRING NOT NULL
 * (the candle evidence bytes + their SHA-256 hex — the same evidence
 * contract the ingestion QuarantineWriter uses for tick bytes).
 */
public final class CandleQuarantineColumns {

    private CandleQuarantineColumns() {}

    /** The Fluss LOG table compute writes invalid candles to (owner: ingestion, DDL 21). */
    public static final String TABLE_NAME = "ingestion_quarantine";

    /** The table's schema version, written on every row (DDL 21 header). */
    public static final String SCHEMA_VERSION = "v1";

    public static final int QUARANTINE_ID = 0;
    public static final int REASON = 1;
    public static final int INSTRUMENT_TOKEN = 2;
    public static final int EXCHANGE = 3;
    public static final int SYMBOL = 4;
    public static final int RAW_PAYLOAD = 5;
    public static final int PAYLOAD_HASH = 6;
    public static final int DETECTED_TS = 7;
    public static final int DETAIL = 8;
    public static final int SCHEMA_VERSION_INDEX = 9;

    /** DDL 21 column count. */
    public static final int FIELD_COUNT = 10;

    /** DDL column names in index order (21_ingestion_quarantine.sql). */
    public static final String[] NAMES = {
        "quarantine_id",
        "reason",
        "instrument_token",
        "exchange",
        "symbol",
        "raw_payload",
        "payload_hash",
        "detected_ts",
        "detail",
        "schema_version"
    };

    /**
     * Stream type info for quarantine rows, derived from the DDL 21 column
     * order (String, String, BigInt, String, String, Binary, String, BigInt,
     * String, String). Declared explicitly so the side-output stream is typed
     * ({@code GenericRowData} never crosses an operator boundary as Kryo).
     */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH),
                new VarBinaryType(VarBinaryType.MAX_LENGTH),
                new VarCharType(VarCharType.MAX_LENGTH), new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH)
            },
            NAMES);
}
