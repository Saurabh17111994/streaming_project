package com.trading.common.schema.ownership;

/**
 * Writer/column ownership matrix for Order_Lifecycle
 * ({@code code/01_platform/02_sql/ddl/09_order_lifecycle.sql} v2, SCH-15).
 *
 * <p>Action Capture is the sole owner (its {@code lifecycle-projector} mints
 * and updates the row; {@link com.trading.common.ownership.OwnershipMatrix}
 * row "order lifecycle"). Identity columns — the composite PK
 * {@code (account_scope_id, broker_order_id)} plus the correlation results
 * {@code instruction_id}/{@code execution_attempt_id}/{@code trade_context_id}
 * and {@code schema_version} — are written once at projection creation and are
 * never part of a {@code partial_update}. The mutable group
 * (lifecycle state, quantities, per-event version evidence, correlation state)
 * is owned by exactly one writer, so a partial update can never clobber the
 * row's identity.
 */
public final class OrderLifecycleColumnOwnership {

    private OrderLifecycleColumnOwnership() {}

    public static final String TABLE_NAME = "Order_Lifecycle";
    public static final String OWNER = "action-capture";
    public static final String WRITER_LIFECYCLE_PROJECTOR = "action-capture:lifecycle-projector";

    /** Identity: 0-4 (composite PK + correlation identity) + 14 (schema_version). */
    private static final int[] IDENTITY = {
            OrderLifecycleColumns.ACCOUNT_SCOPE_ID, OrderLifecycleColumns.BROKER_ORDER_ID,
            OrderLifecycleColumns.INSTRUCTION_ID, OrderLifecycleColumns.EXECUTION_ATTEMPT_ID,
            OrderLifecycleColumns.TRADE_CONTEXT_ID, OrderLifecycleColumns.SCHEMA_VERSION
    };

    public static final ColumnOwnership MATRIX = new ColumnOwnership(
            TABLE_NAME,
            OrderLifecycleColumns.SCHEMA_VERSION_V2,
            OWNER,
            OrderLifecycleColumns.NAMES,
            IDENTITY,
            new ColumnOwnership.Writer(WRITER_LIFECYCLE_PROJECTOR,
                    OrderLifecycleColumns.NORMALIZED_STATE,
                    OrderLifecycleColumns.CUMULATIVE_QTY,
                    OrderLifecycleColumns.PENDING_QTY,
                    OrderLifecycleColumns.AVERAGE_FILL_PRICE_PAISE,
                    OrderLifecycleColumns.SOURCE_EVENT_ID,
                    OrderLifecycleColumns.SOURCE_VERSION,
                    OrderLifecycleColumns.SOURCE_EVENT_TIME,
                    OrderLifecycleColumns.LAST_RECEIVE_TIME,
                    OrderLifecycleColumns.CORRELATION_STATE)
    );
}
