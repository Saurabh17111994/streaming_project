package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * Maps one immutable {@code Trade_Decisions} LOG row (25 columns, see
 * {@link TradeDecisionsTableColumns}) to its {@code trade_instruction_state}
 * KV index row (4 columns, see {@link TradeInstructionStateColumns}) — the
 * dual-sink twin the instruction-feed protocol checks before every append
 * (SCH-19, REQ-FLS-015).
 *
 * <p>The canonical content hash is recomputed from the row's executable
 * request via {@link TradeDecisionBuilder#canonicalHash} (the row round-trips
 * back to the typed decision), so the index always stores exactly the hash
 * the protocol compares — a mismatch means a bug, not a drift between the two
 * sinks. The routing identity ({@code instruction_id}) is copied through
 * unchanged; {@code first_written_ts} = the decision's {@code created_ts}.
 *
 * <p>Stateless and pure — unit-testable without a cluster.
 */
public final class TradeDecisionIndexMapper implements MapFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(RowData decision) {
        TradeDecision d = toDecision(decision);
        GenericRowData index = new GenericRowData(TradeInstructionStateColumns.FIELD_COUNT);
        index.setField(TradeInstructionStateColumns.INSTRUCTION_ID,
                StringData.fromString(TradeDecisionBuilder.instructionId(d)));
        index.setField(TradeInstructionStateColumns.CANONICAL_HASH,
                StringData.fromString(TradeDecisionBuilder.canonicalHash(d)));
        index.setField(TradeInstructionStateColumns.FIRST_WRITTEN_TS, d.createdTs());
        index.setField(TradeInstructionStateColumns.SCHEMA_VERSION,
                StringData.fromString(TradeInstructionStateColumns.SCHEMA_VERSION_V1));
        return index;
    }

    /** Rebuild the typed decision from the 25-column row (all inputs are in the row). */
    static TradeDecision toDecision(RowData row) {
        return new TradeDecision(
                row.getString(TradeDecisionsTableColumns.CANDIDATE_ID).toString(),
                row.getString(TradeDecisionsTableColumns.TRADE_CONTEXT_ID).toString(),
                row.getLong(TradeDecisionsTableColumns.INSTRUMENT_TOKEN),
                row.getString(TradeDecisionsTableColumns.EXCHANGE).toString(),
                row.getString(TradeDecisionsTableColumns.SYMBOL).toString(),
                row.getString(TradeDecisionsTableColumns.SIDE).toString(),
                row.getLong(TradeDecisionsTableColumns.QUANTITY),
                row.getString(TradeDecisionsTableColumns.ORDER_TYPE).toString(),
                row.getString(TradeDecisionsTableColumns.PRODUCT_TYPE).toString(),
                row.isNullAt(TradeDecisionsTableColumns.LIMIT_PRICE_PAISE) ? null
                        : row.getLong(TradeDecisionsTableColumns.LIMIT_PRICE_PAISE),
                row.getString(TradeDecisionsTableColumns.PORTFOLIO_ID).toString(),
                row.getString(TradeDecisionsTableColumns.ACCOUNT_SCOPE_ID).toString(),
                row.getString(TradeDecisionsTableColumns.STRATEGY_ID).toString(),
                row.getString(TradeDecisionsTableColumns.STRATEGY_VERSION).toString(),
                row.getString(TradeDecisionsTableColumns.CONFIGURATION_VERSION).toString(),
                row.getString(TradeDecisionsTableColumns.EVALUATION_ID).toString(),
                row.isNullAt(TradeDecisionsTableColumns.COMPOSITE_SCORE) ? null
                        : row.getDouble(TradeDecisionsTableColumns.COMPOSITE_SCORE),
                row.getString(TradeDecisionsTableColumns.RESERVATION_ID).toString(),
                row.getString(TradeDecisionsTableColumns.RESERVATION_VERSION).toString(),
                row.getLong(TradeDecisionsTableColumns.CREATED_TS),
                row.isNullAt(TradeDecisionsTableColumns.EXPIRY_TS) ? null
                        : row.getLong(TradeDecisionsTableColumns.EXPIRY_TS),
                row.isNullAt(TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID) ? null
                        : row.getString(TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID).toString(),
                row.isNullAt(TradeDecisionsTableColumns.SUPERSEDED_BY_INSTRUCTION_ID) ? null
                        : row.getString(TradeDecisionsTableColumns.SUPERSEDED_BY_INSTRUCTION_ID).toString());
    }
}
