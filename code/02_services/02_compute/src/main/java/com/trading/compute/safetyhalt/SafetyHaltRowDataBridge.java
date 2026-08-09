package com.trading.compute.safetyhalt;

import com.trading.common.safety.SafetyHaltRequestParser;
import com.trading.common.safety.SlotSafetyRequest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bridges a {@link RowData} changelog row of the {@code Safety_Halt_Requests}
 * table into the tested {@link SlotSafetyRequest} model via the column map
 * accepted by {@link SafetyHaltRequestParser}.
 *
 * <p>Column positions are the DDL v3 column order
 * (code/01_platform/02_sql/ddl/18_safety_halt_requests.sql, 21 columns).
 * Only the columns the consumer needs are read; the row contract gate
 * (contract_version = 2, state vocabulary, UNSAFE-reason) lives in the
 * parser and tracker, both unit-tested in the common module.
 */
public final class SafetyHaltRowDataBridge {

    // DDL v3 column positions (0-indexed, 21 columns total).
    private static final int IDX_HALT_REQUEST_ID = 0;
    private static final int IDX_SOURCE_COMPONENT = 4;
    private static final int IDX_REASON_CODE = 6;
    private static final int IDX_DETECTION_TIME = 8;
    private static final int IDX_SLOT_ID = 14;
    private static final int IDX_CONNECTION_EPOCH = 15;
    private static final int IDX_MANIFEST_FINGERPRINT = 16;
    private static final int IDX_ASSIGNED_TOKEN_SET_HASH = 17;
    private static final int IDX_STATE = 18;
    private static final int IDX_CONTRACT_VERSION = 20;

    private SafetyHaltRowDataBridge() {}

    /**
     * @param row a current-value changelog row ({@link RowKind#INSERT} or
     *            {@link RowKind#UPDATE_AFTER}); BEFORE/DELETE rows are not
     *            valid inputs
     * @throws SafetyHaltRequestParser.ParseException on any contract violation
     */
    public static SlotSafetyRequest toRequest(RowData row) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(SafetyHaltRequestParser.COL_HALT_REQUEST_ID,
                row.getString(IDX_HALT_REQUEST_ID).toString());
        map.put(SafetyHaltRequestParser.COL_SOURCE_COMPONENT,
                row.getString(IDX_SOURCE_COMPONENT).toString());
        map.put(SafetyHaltRequestParser.COL_SLOT_ID,
                row.getString(IDX_SLOT_ID).toString());
        map.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH,
                row.getLong(IDX_CONNECTION_EPOCH));
        map.put(SafetyHaltRequestParser.COL_STATE,
                row.getString(IDX_STATE).toString());
        map.put(SafetyHaltRequestParser.COL_REASON_CODE,
                row.getString(IDX_REASON_CODE).toString());
        map.put(SafetyHaltRequestParser.COL_MANIFEST_FINGERPRINT,
                row.getString(IDX_MANIFEST_FINGERPRINT).toString());
        map.put(SafetyHaltRequestParser.COL_ASSIGNED_TOKEN_SET_HASH,
                row.getString(IDX_ASSIGNED_TOKEN_SET_HASH).toString());
        map.put(SafetyHaltRequestParser.COL_DETECTION_TIME,
                row.getLong(IDX_DETECTION_TIME));
        map.put(SafetyHaltRequestParser.COL_CONTRACT_VERSION,
                row.getInt(IDX_CONTRACT_VERSION));
        return SafetyHaltRequestParser.parse(map);
    }
}
