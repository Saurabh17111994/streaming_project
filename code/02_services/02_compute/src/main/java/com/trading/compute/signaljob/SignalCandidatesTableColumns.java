package com.trading.compute.signaljob;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of {@code Signal_Candidates} as written by the
 * Signal job.
 *
 * <p>Must mirror {@code code/01_platform/02_sql/ddl/05_signal_candidates.sql}
 * v2 (22 columns, KV primary key {@code candidate_id} — R-084) exactly. The
 * Fluss KV sink's {@code RowDataSerializationSchema} projects by column name
 * and upserts on the primary key, so emitted {@code GenericRowData} fields
 * must be positioned in DDL column order.
 */
public final class SignalCandidatesTableColumns {

    private SignalCandidatesTableColumns() {}

    public static final int CANDIDATE_ID = 0;
    public static final int INSTRUCTION_ID = 1;
    public static final int TRADE_CONTEXT_ID = 2;
    public static final int INSTRUMENT_TOKEN = 3;
    public static final int EXCHANGE = 4;
    public static final int SYMBOL = 5;
    public static final int STRATEGY_ID = 6;
    public static final int STRATEGY_VERSION = 7;
    public static final int RULE_ID = 8;
    public static final int DETECTION_TS = 9;
    public static final int EVALUATION_TS = 10;
    public static final int ACTION = 11;
    public static final int SIDE = 12;
    public static final int QUANTITY = 13;
    public static final int ORDER_TYPE = 14;
    public static final int LIMIT_PRICE_PAISE = 15;
    public static final int SCORE_INPUTS = 16;
    public static final int FORMATION_SNAPSHOT_REF = 17;
    public static final int VALIDITY_REASON = 18;
    public static final int SUPERSEDES_CANDIDATE_ID = 19;
    public static final int SUPERSEDED_BY_CANDIDATE_ID = 20;
    public static final int SCHEMA_VERSION = 21;

    public static final int FIELD_COUNT = 22;

    /** Fixed candidate values written by the MVP detection operator (DEC-034). */
    public static final String ACTION_ENTRY = "ENTRY";
    public static final String SIDE_BUY = "BUY";
    public static final String ORDER_TYPE_MARKET = "MARKET";
    public static final String VALIDITY_REASON_VALID = "VALID";
    public static final String SCHEMA_VERSION_V2 = "2";

    /** DDL column names in index order (diagnostics). */
    public static final String[] NAMES = {
        "candidate_id", "instruction_id", "trade_context_id", "instrument_token",
        "exchange", "symbol", "strategy_id", "strategy_version", "rule_id",
        "detection_ts", "evaluation_ts", "action", "side", "quantity",
        "order_type", "limit_price_paise", "score_inputs", "formation_snapshot_ref",
        "validity_reason", "supersedes_candidate_id", "superseded_by_candidate_id",
        "schema_version"
    };

    /**
     * Stream type info for emitted candidate rows, derived from the v2 DDL
     * column order. Declared explicitly because TypeExtractor cannot resolve a
     * bare {@code RowData} to a schema — it would fall back to GenericTypeInfo
     * and route RowData through Kryo at operator boundaries.
     */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH), // candidate_id
                new VarCharType(VarCharType.MAX_LENGTH), // instruction_id
                new VarCharType(VarCharType.MAX_LENGTH), // trade_context_id
                new BigIntType(),                        // instrument_token
                new VarCharType(VarCharType.MAX_LENGTH), // exchange
                new VarCharType(VarCharType.MAX_LENGTH), // symbol
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_id
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_version
                new VarCharType(VarCharType.MAX_LENGTH), // rule_id
                new BigIntType(),                        // detection_ts
                new BigIntType(),                        // evaluation_ts
                new VarCharType(VarCharType.MAX_LENGTH), // action
                new VarCharType(VarCharType.MAX_LENGTH), // side
                new BigIntType(),                        // quantity
                new VarCharType(VarCharType.MAX_LENGTH), // order_type
                new BigIntType(),                        // limit_price_paise
                new VarCharType(VarCharType.MAX_LENGTH), // score_inputs
                new VarCharType(VarCharType.MAX_LENGTH), // formation_snapshot_ref
                new VarCharType(VarCharType.MAX_LENGTH), // validity_reason
                new VarCharType(VarCharType.MAX_LENGTH), // supersedes_candidate_id
                new VarCharType(VarCharType.MAX_LENGTH), // superseded_by_candidate_id
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES);
}
