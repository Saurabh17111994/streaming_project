package main

// fromStandardTick maps a standard-stream MarketTick (ds.arrow.trade) to the
// unified NDJSON Tick contract. These tests pin the standard-path emission
// shape: feed="standard", mode passthrough, TS = LTT*1000 (fallback Time*1000),
// OI zero-guard, depth truncated to 5 levels, and QUOTE-mode field carry-over.

import (
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

func fullMarketTick() arrow.MarketTick {
	bids := make([]arrow.DepthLevel, 5)
	asks := make([]arrow.DepthLevel, 5)
	for i := range 5 {
		bids[i] = arrow.DepthLevel{Quantity: int64(1000 + i), Price: int32(100000 + i*100), Orders: int16(i + 1)}
		asks[i] = arrow.DepthLevel{Quantity: int64(2000 + i), Price: int32(110000 + i*100), Orders: int16(i + 6)}
	}
	return arrow.MarketTick{
		Token: 757614, Mode: arrow.StreamModeFull,
		LTP: 120500, Close: 100000, Open: 99000, High: 125000, Low: 98000,
		LTQ: 500, Volume: 1000000, OI: 250000,
		TotalBuyQuantity: 12345678, TotalSellQuantity: 87654321,
		ChangeFlag: 1, AvgPrice: 115000,
		LowerLimit: 90000, UpperLimit: 130000,
		LTT: 1700000000, Time: 1700000000,
		Bids: bids, Asks: asks,
	}
}

func TestFromStandardTickFull(t *testing.T) {
	tick := fromStandardTick(fullMarketTick())
	if tick.Feed != "standard" || tick.Mode != "full" {
		t.Fatalf("feed/mode = %q/%q, want standard/full", tick.Feed, tick.Mode)
	}
	if tick.Token != 757614 || tick.LTP != 120500 || tick.Close != 100000 {
		t.Fatalf("token/ltp/close = %d/%d/%d", tick.Token, tick.LTP, tick.Close)
	}
	if tick.Open != 99000 || tick.High != 125000 || tick.Low != 98000 || tick.VWAP != 0 {
		t.Fatalf("ohlc/vwap = %d/%d/%d/%d", tick.Open, tick.High, tick.Low, tick.VWAP)
	}
	if tick.LTQ != 500 || tick.Volume != 1000000 || tick.OI != 250000 {
		t.Fatalf("ltq/volume/oi = %d/%d/%d", tick.LTQ, tick.Volume, tick.OI)
	}
	if tick.TBQ != 12345678 || tick.TSQ != 87654321 {
		t.Fatalf("tbq/tsq = %d/%d", tick.TBQ, tick.TSQ)
	}
	if tick.ChangeFlag != 1 || tick.AvgPrice != 115000 {
		t.Fatalf("changeFlag/avgPrice = %d/%d", tick.ChangeFlag, tick.AvgPrice)
	}
	if tick.LowerLimit != 90000 || tick.UpperLimit != 130000 {
		t.Fatalf("limits = %d/%d", tick.LowerLimit, tick.UpperLimit)
	}
	if tick.TS != 1700000000*1000 {
		t.Fatalf("ts = %d, want LTT*1000 = %d", tick.TS, int64(1700000000)*1000)
	}
	for i := range 5 {
		if tick.BidPx[i] != int32(100000+i*100) || tick.BidSize[i] != int32(1000+i) || tick.BidOrd[i] != uint16(i+1) {
			t.Fatalf("bid[%d] = px %d qty %d ord %d", i, tick.BidPx[i], tick.BidSize[i], tick.BidOrd[i])
		}
		if tick.AskPx[i] != int32(110000+i*100) || tick.AskSize[i] != int32(2000+i) || tick.AskOrd[i] != uint16(i+6) {
			t.Fatalf("ask[%d] = px %d qty %d ord %d", i, tick.AskPx[i], tick.AskSize[i], tick.AskOrd[i])
		}
	}
}

func TestFromStandardTickDepthTruncation(t *testing.T) {
	// Defensive guard: more than 5 levels must be truncated, never overflow.
	mt := fullMarketTick()
	mt.Bids = append(mt.Bids, arrow.DepthLevel{Quantity: 99, Price: 1, Orders: 1})
	mt.Asks = append(mt.Asks, arrow.DepthLevel{Quantity: 99, Price: 1, Orders: 1})
	tick := fromStandardTick(mt)
	if tick.BidPx[4] == 1 || tick.AskPx[4] == 1 {
		t.Fatal("depth beyond 5 levels leaked into the emitted tick")
	}
}

func TestFromStandardTickQuote(t *testing.T) {
	mt := fullMarketTick()
	mt.Mode = arrow.StreamModeQuote
	mt.Bids, mt.Asks = nil, nil
	mt.LTT = 0 // force TS fallback to Time
	tick := fromStandardTick(mt)
	if tick.Mode != "quote" {
		t.Fatalf("mode = %q, want quote", tick.Mode)
	}
	if tick.TS != int64(1700000000)*1000 {
		t.Fatalf("ts = %d, want Time*1000 fallback", tick.TS)
	}
	if tick.BidPx != [5]int32{} || tick.AskPx != [5]int32{} {
		t.Fatal("quote tick must carry empty depth arrays")
	}
	if tick.LTQ != 500 || tick.Volume != 1000000 || tick.OI != 250000 {
		t.Fatalf("quote fields ltq/volume/oi = %d/%d/%d", tick.LTQ, tick.Volume, tick.OI)
	}
}

func TestFromStandardTickOIZeroGuard(t *testing.T) {
	mt := fullMarketTick()
	mt.OI = 0
	if tick := fromStandardTick(mt); tick.OI != 0 {
		t.Fatalf("oi = %d, want 0 (zero-guard)", tick.OI)
	}
}
