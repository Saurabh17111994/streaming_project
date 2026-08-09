package com.trading.common.safety;

import java.util.Map;

/**
 * Parses a {@code Safety_Halt_Requests} KV row (column-name &rarr; value map,
 * as bridged from the Flink source row) into a {@link SlotSafetyRequest}.
 *
 * <p>Column names and value domain mirror the DDL
 * (code/01_platform/02_sql/ddl/18_safety_halt_requests.sql, schema v3) and
 * the ingestion writer (SafetyHaltWriter, contract_version 2). Malformed
 * rows throw {@link ParseException} — the Flink job converts that into a
 * metric and skips the row; it never crashes the pipeline.
 */
public final class SafetyHaltRequestParser {

    /** DDL / writer column names. */
    public static final String COL_HALT_REQUEST_ID = "halt_request_id";
    public static final String COL_SOURCE_COMPONENT = "source_component";
    public static final String COL_SLOT_ID = "slot_id";
    public static final String COL_CONNECTION_EPOCH = "connection_epoch";
    public static final String COL_STATE = "state";
    public static final String COL_REASON_CODE = "reason_code";
    public static final String COL_MANIFEST_FINGERPRINT = "manifest_fingerprint";
    public static final String COL_ASSIGNED_TOKEN_SET_HASH = "assigned_token_set_hash";
    public static final String COL_DETECTION_TIME = "detection_time";
    public static final String COL_CONTRACT_VERSION = "contract_version";

    private SafetyHaltRequestParser() {}

    /**
     * @param row column-name &rarr; value; strings as {@code String}, numbers
     *            as {@code Integer}/{@code Long}/{@code BigDecimal}
     * @throws ParseException if a required field is missing, a value is
     *         malformed, the state is not UNSAFE/RECOVERED, contract_version
     *         is not 2, or an UNSAFE row carries no reason
     */
    public static SlotSafetyRequest parse(Map<String, Object> row) {
        if (row == null) {
            throw new ParseException("row must not be null");
        }
        int contractVersion = readInt(row, COL_CONTRACT_VERSION);
        if (contractVersion != SlotSafetyRequest.CONTRACT_VERSION) {
            throw new ParseException(COL_CONTRACT_VERSION + " must be "
                    + SlotSafetyRequest.CONTRACT_VERSION + ", got " + contractVersion);
        }
        String state = readString(row, COL_STATE);
        SlotSafetyStatus status;
        try {
            status = SlotSafetyStatus.valueOf(state);
        } catch (IllegalArgumentException e) {
            throw new ParseException(COL_STATE + " must be UNSAFE or RECOVERED, got '" + state + "'");
        }
        String reasonCode = readStringOrEmpty(row, COL_REASON_CODE);
        if (status == SlotSafetyStatus.UNSAFE && reasonCode.isBlank()) {
            throw new ParseException(COL_REASON_CODE + " is required for UNSAFE rows");
        }
        try {
            return new SlotSafetyRequest(
                    readString(row, COL_HALT_REQUEST_ID),
                    readString(row, COL_SOURCE_COMPONENT),
                    readString(row, COL_SLOT_ID),
                    readLong(row, COL_CONNECTION_EPOCH),
                    status,
                    reasonCode,
                    readString(row, COL_MANIFEST_FINGERPRINT),
                    readString(row, COL_ASSIGNED_TOKEN_SET_HASH),
                    readLong(row, COL_DETECTION_TIME),
                    contractVersion);
        } catch (IllegalArgumentException e) {
            throw new ParseException("invalid safety row: " + e.getMessage());
        }
    }

    /** A malformed safety row; carries a human-readable reason. */
    public static final class ParseException extends RuntimeException {
        public ParseException(String message) {
            super(message);
        }
    }

    private static String readString(Map<String, Object> row, String col) {
        Object value = row.get(col);
        if (!(value instanceof String s) || s.isBlank()) {
            throw new ParseException(col + " must be a non-blank string, got " + describe(value));
        }
        return s;
    }

    private static String readStringOrEmpty(Map<String, Object> row, String col) {
        Object value = row.get(col);
        return value instanceof String s ? s : "";
    }

    private static long readLong(Map<String, Object> row, String col) {
        Object value = row.get(col);
        if (value instanceof Number n) {
            return n.longValue();
        }
        throw new ParseException(col + " must be a number, got " + describe(value));
    }

    private static int readInt(Map<String, Object> row, String col) {
        Object value = row.get(col);
        if (value instanceof Number n) {
            return n.intValue();
        }
        throw new ParseException(col + " must be a number, got " + describe(value));
    }

    private static String describe(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + "('" + value + "')";
    }
}
