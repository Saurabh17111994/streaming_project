package com.trading.compute.babysitter;

import com.trading.common.model.PositionState;
import com.trading.common.schema.position.PositionSnapshot;
import com.trading.common.schema.position.PositionsColumns;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes a current-value {@code Positions} changelog row
 * ({@code RowData} full image) into a validated {@link PositionSnapshot}.
 *
 * <p>Only the deserializer parses and validates rows; it never computes
 * position/PnL arithmetic (Nautilus is the only authority) and never emits a
 * {@code Position_Actions} record. A row that fails schema/version/quantity
 * validation is counted as malformed and skipped — under no circumstance is
 * it routed to an action or broker path (Task 7 failure posture:
 * malformed/missing source means not-ready/no-action, not silent skip into a
 * live path; but a single bad changelog row is a non-fatal, counted skip).
 */
public final class PositionsRowDeserializer
        extends RichFlatMapFunction<RowData, PositionSnapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(PositionsRowDeserializer.class);
    private static final long serialVersionUID = 1L;

    private transient Counter observed;
    private transient Counter malformed;

    @Override
    public void open(OpenContext ctx) {
        observed = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.rows.observed");
        malformed = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.rows.malformed");
    }

    @Override
    public void flatMap(RowData row, Collector<PositionSnapshot> out) {
        observed.inc();
        try {
            out.collect(toSnapshot(row));
        } catch (IllegalArgumentException | NullPointerException ex) {
            malformed.inc();
            LOG.warn("babysitter: malformed Positions row skipped (no action): {}", ex.getMessage());
        }
    }

    /**
     * Parses and validates a full {@code Positions} row image into a
     * {@link PositionSnapshot}. Throws on missing required column, unsupported
     * {@code schema_version}, unknown {@code state}, or a quantity invariant
     * violation (validated by the {@link PositionSnapshot} constructor).
     */
    static PositionSnapshot toSnapshot(RowData row) {
        String schemaVersion = row.getString(PositionsColumns.SCHEMA_VERSION).toString();
        if (!PositionsColumns.SCHEMA_VERSION_V2.equals(schemaVersion)) {
            throw new IllegalArgumentException(
                    "unsupported Positions schema_version '" + schemaVersion + "'");
        }
        PositionState state = PositionState.valueOf(
                row.getString(PositionsColumns.STATE).toString());
        long avgEntry = row.isNullAt(PositionsColumns.AVERAGE_ENTRY_PAISE)
                ? 0L : row.getLong(PositionsColumns.AVERAGE_ENTRY_PAISE);
        long avgExit = row.isNullAt(PositionsColumns.AVERAGE_EXIT_PAISE)
                ? 0L : row.getLong(PositionsColumns.AVERAGE_EXIT_PAISE);
        // Constructor validates position_id non-blank + open >= closed >= 0.
        return new PositionSnapshot(
                row.getString(PositionsColumns.POSITION_ID).toString(),
                row.getString(PositionsColumns.TRADE_CONTEXT_ID).toString(),
                row.getString(PositionsColumns.ACCOUNT_SCOPE_ID).toString(),
                row.getLong(PositionsColumns.INSTRUMENT_TOKEN),
                row.getString(PositionsColumns.EXCHANGE).toString(),
                row.getString(PositionsColumns.SYMBOL).toString(),
                row.getString(PositionsColumns.SIDE).toString(),
                state,
                row.getLong(PositionsColumns.OPEN_QUANTITY),
                row.getLong(PositionsColumns.CLOSED_QUANTITY),
                avgEntry,
                avgExit,
                row.getString(PositionsColumns.SOURCE_EVENT_ID).toString(),
                row.getLong(PositionsColumns.SOURCE_VERSION),
                row.getLong(PositionsColumns.CREATED_TS),
                row.getLong(PositionsColumns.LAST_UPDATE_TS),
                schemaVersion);
    }
}
