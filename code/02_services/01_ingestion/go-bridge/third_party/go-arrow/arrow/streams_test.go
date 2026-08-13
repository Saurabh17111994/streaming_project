package arrow

// Decode coverage for the standard data stream (ds.arrow.trade) market-tick
// payloads. ParseMarketTick dispatches purely on payload size:
//
//	13  → LTP    (token + ltp)
//	17  → LTPC   (token + ltp + changeFlag + close)
//	93  → QUOTE  (LTPC prefix + LTQ, avg price, aggregates, OHLC, volume,
//	             timestamps, open interest)
//	241 → FULL   (QUOTE prefix + limits + 5-level bid/ask depth)
//
// All integers are big-endian; prices are in paise. LTPC and QUOTE were the
// only modes with zero test coverage (golden corpus + HFT tests cover LTP and
// FULL only) — these tests pin the byte layout and the R-203 NetChange rule
// (bytes 13:17 are LTQ in a quote frame, never the close price).

import (
	"encoding/binary"
	"math"
	"testing"
)

func beU32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func beU64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

// buildQuotePrefix returns bytes [0:17) shared by LTPC and QUOTE frames:
// token, ltp, changeFlag, 4 reserved bytes, then the ambiguous 13:17 field
// (close in LTPC, LTQ in QUOTE).
func buildQuotePrefix(token, ltp uint32, changeFlag byte, field13 uint32) []byte {
	b := make([]byte, 17)
	copy(b[0:4], beU32(token))
	copy(b[4:8], beU32(ltp))
	b[8] = changeFlag
	copy(b[13:17], beU32(field13))
	return b
}

func buildLTPCPayload(token, ltp uint32, changeFlag byte, closePx uint32) []byte {
	return buildQuotePrefix(token, ltp, changeFlag, closePx)
}

func buildQuotePayload(
	token, ltp uint32, changeFlag byte, ltq uint32,
	avgPrice uint32, tbq, tsq uint64,
	open, high, closePx, low uint32,
	volume uint64, ltt, tm uint32,
	oi, oiDayHigh, oiDayLow uint64,
) []byte {
	b := make([]byte, 93)
	copy(b[0:17], buildQuotePrefix(token, ltp, changeFlag, ltq))
	copy(b[17:21], beU32(avgPrice))
	copy(b[21:29], beU64(tbq))
	copy(b[29:37], beU64(tsq))
	copy(b[37:41], beU32(open))
	copy(b[41:45], beU32(high))
	copy(b[45:49], beU32(closePx))
	copy(b[49:53], beU32(low))
	copy(b[53:61], beU64(volume))
	copy(b[61:65], beU32(ltt))
	copy(b[65:69], beU32(tm))
	copy(b[69:77], beU64(oi))
	copy(b[77:85], beU64(oiDayHigh))
	copy(b[85:93], beU64(oiDayLow))
	return b
}

func buildFullPayload(quote []byte, lower, upper uint32) []byte {
	b := make([]byte, 241)
	copy(b[0:93], quote)
	copy(b[93:97], beU32(lower))
	copy(b[97:101], beU32(upper))
	for i := range 10 {
		off := 101 + i*14
		copy(b[off:off+8], beU64(uint64(1000+i)))
		copy(b[off+8:off+12], beU32(uint32(100000+i*100)))
		binary.BigEndian.PutUint16(b[off+12:off+14], uint16(i+1))
	}
	return b
}

func almostEqual(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestParseMarketTickLTP(t *testing.T) {
	p := make([]byte, 13)
	copy(p[0:4], beU32(757614))
	copy(p[4:8], beU32(120500))

	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(13B) error: %v", err)
	}
	if tick.Token != 757614 || tick.LTP != 120500 {
		t.Fatalf("token/ltp = %d/%d, want 757614/120500", tick.Token, tick.LTP)
	}
	if tick.Mode != StreamModeLTP {
		t.Fatalf("mode = %q, want %q", tick.Mode, StreamModeLTP)
	}
}

func TestParseMarketTickLTPC(t *testing.T) {
	p := buildLTPCPayload(757614, 120500, 1, 100000) // close = 1000.00 ₹

	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(17B) error: %v", err)
	}
	if tick.Token != 757614 || tick.LTP != 120500 || tick.Close != 100000 {
		t.Fatalf("token/ltp/close = %d/%d/%d, want 757614/120500/100000", tick.Token, tick.LTP, tick.Close)
	}
	if tick.ChangeFlag != 1 {
		t.Fatalf("changeFlag = %d, want 1", tick.ChangeFlag)
	}
	if tick.Mode != StreamModeLTPC {
		t.Fatalf("mode = %q, want %q", tick.Mode, StreamModeLTPC)
	}
	// NetChange = (ltp - close) * 100 / close = (1205.00 - 1000.00) / 1000.00 = 20.5%
	if !almostEqual(tick.NetChange, 20.5) {
		t.Fatalf("NetChange = %v, want 20.5", tick.NetChange)
	}
}

func TestParseMarketTickLTPCZeroCloseNetChangeZero(t *testing.T) {
	p := buildLTPCPayload(757614, 120500, 0, 0)
	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(17B) error: %v", err)
	}
	if tick.NetChange != 0 {
		t.Fatalf("NetChange = %v, want 0 when close == 0", tick.NetChange)
	}
}

func TestParseMarketTickQuote(t *testing.T) {
	p := buildQuotePayload(
		757614, 120500, 1, 500, // ltq = 5.00 ₹ — must NOT feed NetChange (R-203)
		115000, 12345678, 87654321,
		99000, 125000, 100000, 98000,
		1000000, 1700000000, 1700000000,
		250000, 280000, 240000,
	)

	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(93B) error: %v", err)
	}
	if tick.Mode != StreamModeQuote {
		t.Fatalf("mode = %q, want %q", tick.Mode, StreamModeQuote)
	}
	if tick.Token != 757614 || tick.LTP != 120500 || tick.ChangeFlag != 1 {
		t.Fatalf("token/ltp/changeFlag = %d/%d/%d", tick.Token, tick.LTP, tick.ChangeFlag)
	}
	if tick.LTQ != 500 || tick.AvgPrice != 115000 {
		t.Fatalf("ltq/avgPrice = %d/%d, want 500/115000", tick.LTQ, tick.AvgPrice)
	}
	if tick.TotalBuyQuantity != 12345678 || tick.TotalSellQuantity != 87654321 {
		t.Fatalf("tbq/tsq = %d/%d", tick.TotalBuyQuantity, tick.TotalSellQuantity)
	}
	if tick.Open != 99000 || tick.High != 125000 || tick.Close != 100000 || tick.Low != 98000 {
		t.Fatalf("ohlc = %d/%d/%d/%d", tick.Open, tick.High, tick.Close, tick.Low)
	}
	if tick.Volume != 1000000 || tick.LTT != 1700000000 || tick.Time != 1700000000 {
		t.Fatalf("volume/ltt/time = %d/%d/%d", tick.Volume, tick.LTT, tick.Time)
	}
	if tick.OI != 250000 || tick.OIDayHigh != 280000 || tick.OIDayLow != 240000 {
		t.Fatalf("oi = %d/%d/%d", tick.OI, tick.OIDayHigh, tick.OIDayLow)
	}
	// R-203: NetChange derives from the real close (45:49), never LTQ (13:17).
	if !almostEqual(tick.NetChange, 20.5) {
		t.Fatalf("NetChange = %v, want 20.5 (from close, not LTQ)", tick.NetChange)
	}
	if len(tick.Bids) != 0 || len(tick.Asks) != 0 {
		t.Fatalf("quote must have no depth ladder, got bids=%d asks=%d", len(tick.Bids), len(tick.Asks))
	}
}

func TestParseMarketTickQuoteR203LtqNotNetChange(t *testing.T) {
	// close = 0, ltq = 500: NetChange must be 0, not derived from LTQ
	// (pre-R-203 parseLTPC would have computed (ltp-ltq)*100/ltq).
	p := buildQuotePayload(
		757614, 120500, 0, 500,
		0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0,
		0, 0, 0,
	)
	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(93B) error: %v", err)
	}
	if tick.NetChange != 0 {
		t.Fatalf("NetChange = %v, want 0 (LTQ must not be treated as close)", tick.NetChange)
	}
}

func TestParseMarketTickFull(t *testing.T) {
	quote := buildQuotePayload(
		757614, 120500, 1, 500,
		115000, 12345678, 87654321,
		99000, 125000, 100000, 98000,
		1000000, 1700000000, 1700000000,
		250000, 280000, 240000,
	)
	p := buildFullPayload(quote, 90000, 130000)

	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(241B) error: %v", err)
	}
	if tick.Mode != StreamModeFull {
		t.Fatalf("mode = %q, want %q", tick.Mode, StreamModeFull)
	}
	if tick.LowerLimit != 90000 || tick.UpperLimit != 130000 {
		t.Fatalf("limits = %d/%d, want 90000/130000", tick.LowerLimit, tick.UpperLimit)
	}
	if tick.Close != 100000 || !almostEqual(tick.NetChange, 20.5) {
		t.Fatalf("close/NetChange = %d/%v, want 100000/20.5", tick.Close, tick.NetChange)
	}
	if len(tick.Bids) != 5 || len(tick.Asks) != 5 {
		t.Fatalf("depth = %d bids / %d asks, want 5/5", len(tick.Bids), len(tick.Asks))
	}
	for i := range 5 {
		b := tick.Bids[i]
		if b.Quantity != int64(1000+i) || b.Price != int32(100000+i*100) || b.Orders != int16(i+1) {
			t.Fatalf("bid[%d] = qty %d price %d orders %d", i, b.Quantity, b.Price, b.Orders)
		}
		a := tick.Asks[i]
		if a.Quantity != int64(1000+5+i) || a.Price != int32(100000+(5+i)*100) || a.Orders != int16(5+i+1) {
			t.Fatalf("ask[%d] = qty %d price %d orders %d", i, a.Quantity, a.Price, a.Orders)
		}
	}
}

func TestParseMarketTickUnsupportedSize(t *testing.T) {
	if _, err := ParseMarketTick(make([]byte, 14)); err == nil {
		t.Fatal("ParseMarketTick(14B) = nil error, want unsupported-size error")
	}
	if _, err := ParseMarketTick(nil); err == nil {
		t.Fatal("ParseMarketTick(0B) = nil error, want unsupported-size error")
	}
}
