package com.trading.common.schema.position;

import java.util.List;

/**
 * Physical column layout of the immutable Fills LOG
 * ({@code code/01_platform/02_sql/ddl/08_fills.sql} v2). Pinned by
 * {@code FillsColumnsAgreementTest} against the DDL file (cross-boundary pin
 * habit). The projector consumes a caller-resolved subset; the full layout is
 * pinned here so the caller-side mapping and the DDL can never drift.
 */
public final class FillsColumns {

    private FillsColumns() {}

    public static final int POSTBACK_EVENT_ID = 0;
    public static final int POSTBACK_FINGERPRINT = 1;
    public static final int FINGERPRINT_VERSION = 2;
    public static final int ACCOUNT_SCOPE_ID = 3;
    public static final int BROKER_ORDER_ID = 4;
    public static final int INSTRUCTION_ID = 5;
    public static final int EXECUTION_ATTEMPT_ID = 6;
    public static final int TRADE_CONTEXT_ID = 7;
    public static final int ORDER_STATUS = 8;
    public static final int CUMULATIVE_QTY = 9;
    public static final int PENDING_QTY = 10;
    public static final int FILL_QTY = 11;
    public static final int FILL_PRICE_PAISE = 12;
    public static final int FILL_ID = 13;
    public static final int BROKER_EVENT_TIME = 14;
    public static final int RECEIVE_TIME = 15;
    public static final int INGEST_TS = 16;
    public static final int ORIGINAL_PAYLOAD = 17;
    public static final int PAYLOAD_HASH = 18;
    public static final int CORRELATION_STATE = 19;
    public static final int CORRELATION_REASON = 20;
    public static final int DECODER_VERSION = 21;
    public static final int SCHEMA_VERSION = 22;

    public static final int FIELD_COUNT = 23;

    public static final String SCHEMA_VERSION_V2 = "2";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING",
            "STRING", "STRING", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "STRING",
            "BIGINT", "BIGINT", "BIGINT", "BYTES", "STRING", "STRING", "STRING",
            "STRING", "STRING");

    /** DDL nullability per column (08 DDL v2). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, true, true, true, true, false, false,
            false, true, true, true, true, false, false, false, false, false,
            true, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final String[] NAMES = {
            "postback_event_id", "postback_fingerprint", "fingerprint_version",
            "account_scope_id", "broker_order_id", "instruction_id",
            "execution_attempt_id", "trade_context_id", "order_status",
            "cumulative_qty", "pending_qty", "fill_qty", "fill_price_paise",
            "fill_id", "broker_event_time", "receive_time", "ingest_ts",
            "original_payload", "payload_hash", "correlation_state",
            "correlation_reason", "decoder_version", "schema_version"
    };
}
