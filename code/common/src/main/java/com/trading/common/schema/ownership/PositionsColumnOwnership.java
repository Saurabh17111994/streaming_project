package com.trading.common.schema.ownership;

/**
 * Writer/column ownership matrix for Positions
 * ({@code code/01_platform/02_sql/ddl/10_positions.sql} v2, SCH-15).
 *
 * <p>The position projector runs in-process within Action Capture (contract
 * docs/04_contracts/06-action-capture.md) and is the sole owner
 * ({@link com.trading.common.ownership.OwnershipMatrix} row "position
 * aggregate"). Identity columns — {@code position_id} (PK), the account /
 * instrument / exchange / symbol / side scope, {@code created_ts}, and
 * {@code schema_version} — are written once at minting and never
 * partial-updated. The mutable group (state, the quantity pair, average
 * prices, per-event version evidence, {@code last_update_ts}) is owned by one
 * writer; v2 removed the derived {@code current_quantity} precisely so no
 * partial update can corrupt one side of the quantity pair.
 */
public final class PositionsColumnOwnership {

    private PositionsColumnOwnership() {}

    public static final String TABLE_NAME = "Positions";
    public static final String OWNER = "action-capture";
    public static final String WRITER_POSITION_PROJECTOR = "action-capture:position-projector";

    /** Identity: 0-6 (PK + scope) + 14 (created_ts) + 16 (schema_version). */
    private static final int[] IDENTITY = {
            com.trading.common.schema.position.PositionsColumns.POSITION_ID,
            com.trading.common.schema.position.PositionsColumns.TRADE_CONTEXT_ID,
            com.trading.common.schema.position.PositionsColumns.ACCOUNT_SCOPE_ID,
            com.trading.common.schema.position.PositionsColumns.INSTRUMENT_TOKEN,
            com.trading.common.schema.position.PositionsColumns.EXCHANGE,
            com.trading.common.schema.position.PositionsColumns.SYMBOL,
            com.trading.common.schema.position.PositionsColumns.SIDE,
            com.trading.common.schema.position.PositionsColumns.CREATED_TS,
            com.trading.common.schema.position.PositionsColumns.SCHEMA_VERSION
    };

    public static final ColumnOwnership MATRIX = new ColumnOwnership(
            TABLE_NAME,
            com.trading.common.schema.position.PositionsColumns.SCHEMA_VERSION_V2,
            OWNER,
            com.trading.common.schema.position.PositionsColumns.NAMES,
            IDENTITY,
            new ColumnOwnership.Writer(WRITER_POSITION_PROJECTOR,
                    com.trading.common.schema.position.PositionsColumns.STATE,
                    com.trading.common.schema.position.PositionsColumns.OPEN_QUANTITY,
                    com.trading.common.schema.position.PositionsColumns.CLOSED_QUANTITY,
                    com.trading.common.schema.position.PositionsColumns.AVERAGE_ENTRY_PAISE,
                    com.trading.common.schema.position.PositionsColumns.AVERAGE_EXIT_PAISE,
                    com.trading.common.schema.position.PositionsColumns.SOURCE_EVENT_ID,
                    com.trading.common.schema.position.PositionsColumns.SOURCE_VERSION,
                    com.trading.common.schema.position.PositionsColumns.LAST_UPDATE_TS)
    );
}
