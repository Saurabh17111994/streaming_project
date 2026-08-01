# Instrument Manifest

Place instrument CSV files here. The Ingestion service loads instruments from this directory at startup.

## Expected CSV format

Arrow Trade `GET /all` or `GET /nse` format:

```
Exchange,Segment,ExchSeg,Token,FullName,Symbol,TradingSymbol,Series,ISIN,LotSize,TickSize,PricePrecision,OptionType,Underlying,UnderlyingToken,StrikePrice,Expiry,FreezeQty,Lower Band,Upper Band,SurCodes,Events,ExchangeID
```

Or a simplified format with at minimum: `token`, `exchange`, `symbol`, `trading_symbol`, `lot_size`, `tick_size`.

## Files

| File | Purpose |
|------|---------|
| `instruments.csv` | Active instrument manifest loaded at Ingestion startup |

## Status

- **Current:** `instruments.csv` is not yet present. Ingestion uses a **TEST-ONLY** stub (50 random tokens) for local development.
- **Production:** Drop the real instrument CSV here before live trading. See `docs/09_data_gaps.md` DATA-GAP-001.

## Safety guard

When `EXECUTION_ENABLED=true`, the Ingestion service SHALL fail startup if the active instrument manifest is the TEST-ONLY stub or is otherwise absent. The TEST-ONLY stub SHALL log a `WARN`-level message on every startup identifying itself as a development fixture. Production readiness SHALL remain false until a validated production manifest is loaded.

## References

- Ingestion requirement: [REQ-ING-004](/docs/02_requirements/02-functional/01-ingestion.md)
- Data gap: [DATA-GAP-001](/docs/09_data_gaps.md)
- Import script: [import_instruments.sh](/code/01_platform/04_scripts/import_instruments.sh)
