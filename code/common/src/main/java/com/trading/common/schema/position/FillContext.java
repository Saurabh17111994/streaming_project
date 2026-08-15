package com.trading.common.schema.position;

/**
 * The caller-resolved context of a Fills LOG row that the row itself cannot
 * carry (SCH-20 operator wiring). The Fills LOG (08_fills.sql v2) has NO
 * {@code side} and NO instrument/exchange/symbol columns, so Action Capture
 * resolves them from the correlated instruction/attempt before projection:
 * <ul>
 *   <li>{@code side} — from the correlated instruction (never guessed);</li>
 *   <li>{@code instrumentToken}/{@code exchange}/{@code symbol} — the traded
 *       instrument identity, from the correlated order.</li>
 * </ul>
 *
 * <p>{@code position_id} is NOT part of this context — it is minted by
 * {@link PositionProjectorDriver} on the first uniquely correlated fill that
 * creates exposure (05-action-capture.md &rarr; "Position projection protocol")
 * and passed to {@link FillEventMapper#mapIfFill} separately.
 */
public record FillContext(
        String side,
        long instrumentToken,
        String exchange,
        String symbol) {

    public FillContext {
        if (!FillEvent.SIDE_BUY.equals(side) && !FillEvent.SIDE_SELL.equals(side)) {
            throw new IllegalArgumentException("side must be BUY or SELL, got " + side);
        }
        if (instrumentToken <= 0) {
            throw new IllegalArgumentException("instrument_token must be positive, got "
                    + instrumentToken);
        }
        if (exchange == null || exchange.isBlank()) {
            throw new IllegalArgumentException("exchange is required");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
    }
}
