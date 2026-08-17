package arrow

// Decode coverage for the standard data stream (ds.arrow.trade) market-tick
// payloads. ParseMarketTick dispatches purely on payload size:
//
//	13  → LTP    (token + ltp)
//	17  → LTPC   (token + ltp + changeFlag + close)
//	93  → QUOTE  (LTPC prefix + LTQ, avg price, aggregates, OHLC, volume,
//	             timestamps, open interest)
//	249 → FULL   (QUOTE prefix + limits + 8 reserved bytes + 5-level depth)
//	241 → FULL   (legacy layout: depth immediately after limits)
//	29/33/109/265 → base + 16-byte Closing Auction Session trailer
//
// All integers are big-endian; prices are in paise. LTPC and QUOTE were the
// only modes with zero test coverage (golden corpus + HFT tests cover LTP and
// FULL only) — these tests pin the byte layout and the R-203 NetChange rule
// (bytes 13:17 are LTQ in a quote frame, never the close price). The 249 and
// 93 live-frame tests use raw broker bytes captured 2026-08-13 (BROKER-MD-001),
// not bytes produced by this package's own builders — non-circular fixtures.

import (
	"encoding/binary"
	"encoding/hex"
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

// Live QUOTE frame, RELIANCE token 2885, captured 2026-08-13 from
// wss://ds.arrow.trade (BROKER-MD-001). Values cross-validated against the HFT
// feed (socket.arrow.trade) — identical ltp/vwap/volume/ltt on the same token.
const liveQuote2885Hex = "00000b45000202742000000000000000c8000201380000000000015bf90000000000000000000206ac000206ac000207240001fea000000000008f64356a7d9ca16a7d9ca2000000000000000000000000000000000000000000000000"

// Live FULL frame, RELIANCE token 2885, captured 2026-08-13 from
// wss://ds.arrow.trade (BROKER-MD-001). 249 bytes = current wire layout: the
// 93-byte quote prefix, lower/upper limits at 93:101, 8 reserved bytes at
// 101:109, then 10 depth levels of 14 bytes at 109+i*14.
const liveFull2492885Hex = "00000b45000202742000000000000000c8000201380000000000015bf90000000000000000000206ac000206ac000207240001fea000000000008f64356a7d9ca16a7d9ca20000000000000000000000000000000000000000000000000001cf02000235e600000000000000000000000000015bf9000202740015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

func TestParseMarketTickQuoteLive(t *testing.T) {
	p, err := hex.DecodeString(liveQuote2885Hex)
	if err != nil {
		t.Fatal(err)
	}
	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(93B live) error: %v", err)
	}
	if tick.Mode != StreamModeQuote || tick.Token != 2885 || tick.LTP != 131700 {
		t.Fatalf("mode/token/ltp = %q/%d/%d, want quote/2885/131700", tick.Mode, tick.Token, tick.LTP)
	}
	if tick.LTQ != 200 || tick.AvgPrice != 131384 {
		t.Fatalf("ltq/avg = %d/%d, want 200/131384", tick.LTQ, tick.AvgPrice)
	}
	if tick.TotalBuyQuantity != 89081 || tick.TotalSellQuantity != 0 {
		t.Fatalf("tbq/tsq = %d/%d, want 89081/0", tick.TotalBuyQuantity, tick.TotalSellQuantity)
	}
	if tick.Open != 132780 || tick.High != 132780 || tick.Close != 132900 || tick.Low != 130720 {
		t.Fatalf("ohlc = %d/%d/%d/%d, want 132780/132780/132900/130720", tick.Open, tick.High, tick.Close, tick.Low)
	}
	if tick.Volume != 9397301 || tick.LTT != 1786616993 || tick.Time != 1786616994 {
		t.Fatalf("volume/ltt/time = %d/%d/%d", tick.Volume, tick.LTT, tick.Time)
	}
}

func TestParseMarketTickFull249Live(t *testing.T) {
	p, err := hex.DecodeString(liveFull2492885Hex)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 249 {
		t.Fatalf("live full frame len = %d, want 249", len(p))
	}
	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(249B live) error: %v", err)
	}
	if tick.Mode != StreamModeFull || tick.Token != 2885 || tick.LTP != 131700 {
		t.Fatalf("mode/token/ltp = %q/%d/%d, want full/2885/131700", tick.Mode, tick.Token, tick.LTP)
	}
	if tick.Close != 132900 {
		t.Fatalf("close = %d, want 132900", tick.Close)
	}
	if tick.LowerLimit != 118530 || tick.UpperLimit != 144870 {
		t.Fatalf("limits = %d/%d, want 118530/144870", tick.LowerLimit, tick.UpperLimit)
	}
	if len(tick.Bids) != 5 || len(tick.Asks) != 5 {
		t.Fatalf("depth = %d bids / %d asks, want 5/5", len(tick.Bids), len(tick.Asks))
	}
	b0 := tick.Bids[0]
	if b0.Quantity != 89081 || b0.Price != 131700 || b0.Orders != 21 {
		t.Fatalf("bid[0] = qty %d price %d orders %d, want 89081/131700/21", b0.Quantity, b0.Price, b0.Orders)
	}
	for i := 1; i < 5; i++ {
		if l := tick.Bids[i]; l.Quantity != 0 || l.Price != 0 || l.Orders != 0 {
			t.Fatalf("bid[%d] = %+v, want zero", i, l)
		}
	}
	for i := range 5 {
		if l := tick.Asks[i]; l.Quantity != 0 || l.Price != 0 || l.Orders != 0 {
			t.Fatalf("ask[%d] = %+v, want zero", i, l)
		}
	}
}

func TestParseMarketTickFull249CasTrailer(t *testing.T) {
	// Closing Auction Session: 249-byte full frame + 16-byte trailer
	// (imbalance_qty i64 signed, indicative_close i32, ref_price i32) —
	// appended to every mode after ~15:15 IST (pyarrow_client sockets.py).
	raw := liveFull2492885Hex +
		"fffffffffffffb2e" + // imbalance_qty = -1234 (two's complement i64 BE)
		"00020724" + // indicative_close = 132900
		"00020274" // ref_price = 131700
	p, err := hex.DecodeString(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 265 {
		t.Fatalf("CAS full frame len = %d, want 265", len(p))
	}
	tick, err := ParseMarketTick(p)
	if err != nil {
		t.Fatalf("ParseMarketTick(265B CAS) error: %v", err)
	}
	if tick.Mode != StreamModeFull || tick.Token != 2885 {
		t.Fatalf("mode/token = %q/%d, want full/2885", tick.Mode, tick.Token)
	}
	if tick.ImbalanceQty != -1234 || tick.IndicativeClose != 132900 || tick.RefPrice != 131700 {
		t.Fatalf("cas = imb %d / iclose %d / ref %d, want -1234/132900/131700",
			tick.ImbalanceQty, tick.IndicativeClose, tick.RefPrice)
	}
	// Base fields must still parse through the trailer.
	if tick.LowerLimit != 118530 || tick.UpperLimit != 144870 {
		t.Fatalf("limits = %d/%d, want 118530/144870", tick.LowerLimit, tick.UpperLimit)
	}
}
