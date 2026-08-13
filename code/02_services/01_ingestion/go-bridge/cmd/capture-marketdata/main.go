// capture-marketdata connects to the real Arrow market-data feeds (standard
// wss://ds.arrow.trade and HFT wss://socket.arrow.trade) with credentials
// from the environment and records raw wire frames to a JSONL file.
//
// Purpose: BROKER-MD-001 protocol evidence. The vendored parse layouts are
// validated against real broker bytes, and the full-mode size discrepancy is
// resolved (docs claim 249 bytes for the standard Full tick, the SDK parses
// 241) — the captured lengths decide which layout is correct.
//
// The tool is read-only: it only subscribes to market data and records what
// the broker sends. It never places orders or sends anything beyond the
// subscribe/unsubscribe messages.
//
// Environment:
//
//	ARROW_APP_ID + ARROW_APP_SECRET       → client.Login token exchange
//	ARROW_REQUEST_TOKEN (with ARROW_APP_SECRET) → device flow: Authenticate(requestToken)
//	ARROW_USER_ID + ARROW_PASSWORD + ARROW_TOTP_KEY → client.AutoLogin
//	ARROW_TOKEN (optional, overrides)     → use an existing token verbatim
//	CAPTURE_STANDARD=1 (default)          → capture ds.arrow.trade
//	CAPTURE_HFT=1 (default)               → capture socket.arrow.trade
//	CAPTURE_TOKENS=2885,1333,3045         → instrument ids (NSE tokens)
//	CAPTURE_DURATION=20                   → seconds per feed
//	CAPTURE_OUT=marketdata-capture.jsonl  → output path
//
// JSONL record kinds:
//
//	{"kind":"connect","feed":"standard","ok":true}
//	{"kind":"subscribe","feed":"standard","mode":"ltp","ok":true}
//	{"kind":"raw","feed":"standard","len":13,"hex":"..."}        (binary payload)
//	{"kind":"frame","feed":"hft","mt":2,"len":123}               (HFT frame, msg type + compressed size)
//	{"kind":"response","feed":"hft","errorCode":"","successCount":5,...}
//	{"kind":"error","feed":"hft","err":"..."}
//	{"kind":"summary","standard":{"raw":{...}},"hft":{...}}
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

type recorder struct {
	mu   sync.Mutex
	w    *bufio.Writer
	file *os.File
}

func (r *recorder) emit(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.w.Write(b); err != nil {
		return err
	}
	return r.w.WriteByte('\n')
}

func (r *recorder) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.w.Flush()
}

type lenCounts map[int]int

func (l lenCounts) add(n int) { l[n]++ }

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s=%q not an int, using %d\n", name, v, def)
		return def
	}
	return n
}

func envBool(name string, def bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s=%q not a bool, using %v\n", name, v, def)
		return def
	}
	return b
}

func main() {
	if os.Getenv("ARROW_APP_ID") == "" {
		fmt.Fprintln(os.Stderr, "ARROW_APP_ID is required (see .env)")
		os.Exit(2)
	}
	if os.Getenv("ARROW_REQUEST_TOKEN") != "" && os.Getenv("ARROW_APP_SECRET") == "" {
		fmt.Fprintln(os.Stderr, "ARROW_REQUEST_TOKEN requires ARROW_APP_SECRET for the checksum exchange")
		os.Exit(2)
	}
	if os.Getenv("ARROW_APP_SECRET") == "" && os.Getenv("ARROW_TOKEN") == "" &&
		(os.Getenv("ARROW_USER_ID") == "" || os.Getenv("ARROW_PASSWORD") == "" || os.Getenv("ARROW_TOTP_KEY") == "") {
		fmt.Fprintln(os.Stderr, "need ARROW_APP_SECRET (Login), ARROW_TOKEN, or the AutoLogin trio")
		os.Exit(2)
	}

	outPath := os.Getenv("CAPTURE_OUT")
	if outPath == "" {
		outPath = "marketdata-capture.jsonl"
	}
	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", outPath, err)
		os.Exit(2)
	}
	rec := &recorder{w: bufio.NewWriter(f), file: f}
	defer func() {
		_ = rec.close()
		_ = f.Close()
	}()

	tokens := []int32{2885, 1333, 3045, 11536, 1594} // RELIANCE, HDFCBANK, SBIN, TCS, INFY
	if v := os.Getenv("CAPTURE_TOKENS"); v != "" {
		tokens = tokens[:0]
		for _, p := range strings.Split(v, ",") {
			n, err := strconv.ParseInt(strings.TrimSpace(p), 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bad token %q\n", p)
				os.Exit(2)
			}
			tokens = append(tokens, int32(n))
		}
	}
	duration := time.Duration(envInt("CAPTURE_DURATION", 20)) * time.Second

	client := arrow.NewClient(os.Getenv("ARROW_APP_ID"), os.Getenv("ARROW_APP_SECRET"))

	switch {
	case os.Getenv("ARROW_REQUEST_TOKEN") != "":
		if _, err := client.Authenticate(os.Getenv("ARROW_REQUEST_TOKEN")); err != nil {
			fmt.Fprintf(os.Stderr, "Authenticate(requestToken) failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Authenticate OK, token len %d\n", len(client.GetToken()))
	case os.Getenv("ARROW_TOKEN") != "":
		client.SetToken(os.Getenv("ARROW_TOKEN"))
		fmt.Fprintf(os.Stderr, "using ARROW_TOKEN (len %d)\n", len(os.Getenv("ARROW_TOKEN")))
	case os.Getenv("ARROW_USER_ID") != "":
		if err := client.AutoLogin(os.Getenv("ARROW_USER_ID"), os.Getenv("ARROW_PASSWORD"), os.Getenv("ARROW_TOTP_KEY")); err != nil {
			fmt.Fprintf(os.Stderr, "AutoLogin failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "AutoLogin OK, token len %d\n", len(client.GetToken()))
	case os.Getenv("ARROW_APP_SECRET") != "":
		if err := client.Login(); err != nil {
			fmt.Fprintf(os.Stderr, "Login failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Login OK, token len %d\n", len(client.GetToken()))
	}

	var wg sync.WaitGroup
	summary := struct {
		Standard       lenCounts `json:"standard"`
		HFT            lenCounts `json:"hft"`
		StandardFrames int       `json:"standardFrames"`
		HFTFrames      int       `json:"hftFrames"`
	}{Standard: lenCounts{}, HFT: lenCounts{}}
	var smu sync.Mutex

	if envBool("CAPTURE_STANDARD", true) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			captureStandard(rec, client, tokens, duration, &summary, &smu)
		}()
	}
	if envBool("CAPTURE_HFT", true) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			captureHFT(rec, client, tokens, duration, &summary, &smu)
		}()
	}
	wg.Wait()

	_ = rec.emit(map[string]any{"kind": "summary", "standard": summary.Standard, "hft": summary.HFT})
	fmt.Fprintf(os.Stderr, "capture complete: standard=%v hft=%v -> %s\n", summary.Standard, summary.HFT, outPath)
}

func captureStandard(rec *recorder, client *arrow.Client, tokens []int32, dur time.Duration, summary *struct {
	Standard       lenCounts `json:"standard"`
	HFT            lenCounts `json:"hft"`
	StandardFrames int       `json:"standardFrames"`
	HFTFrames      int       `json:"hftFrames"`
}, smu *sync.Mutex) {
	ds, err := client.ConnectDataStream()
	if err != nil {
		_ = rec.emit(map[string]any{"kind": "connect", "feed": "standard", "ok": false, "err": err.Error()})
		fmt.Fprintf(os.Stderr, "standard connect failed: %v\n", err)
		return
	}
	defer ds.Close()
	_ = rec.emit(map[string]any{"kind": "connect", "feed": "standard", "ok": true})

	for _, mode := range []arrow.StreamMode{arrow.StreamModeLTP, arrow.StreamModeLTPC, arrow.StreamModeQuote, arrow.StreamModeFull} {
		err := ds.Subscribe(mode, tokens)
		_ = rec.emit(map[string]any{"kind": "subscribe", "feed": "standard", "mode": string(mode), "ok": err == nil, "err": errStr(err)})
		fmt.Fprintf(os.Stderr, "standard subscribe mode=%s ok=%v err=%v\n", mode, err == nil, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	ds.ReadRawTicks(ctx, func(payload []byte) {
		smu.Lock()
		summary.Standard.add(len(payload))
		summary.StandardFrames++
		smu.Unlock()
		_ = rec.emit(map[string]any{"kind": "raw", "feed": "standard", "len": len(payload), "hex": hex.EncodeToString(payload)})
	}, func(err error) {
		if ctx.Err() != nil {
			return
		}
		_ = rec.emit(map[string]any{"kind": "error", "feed": "standard", "err": err.Error()})
		fmt.Fprintf(os.Stderr, "standard read error: %v\n", err)
	})
}

func captureHFT(rec *recorder, client *arrow.Client, tokens []int32, dur time.Duration, summary *struct {
	Standard       lenCounts `json:"standard"`
	HFT            lenCounts `json:"hft"`
	StandardFrames int       `json:"standardFrames"`
	HFTFrames      int       `json:"hftFrames"`
}, smu *sync.Mutex) {
	hds, err := client.ConnectHFTDataStream()
	if err != nil {
		_ = rec.emit(map[string]any{"kind": "connect", "feed": "hft", "ok": false, "err": err.Error()})
		fmt.Fprintf(os.Stderr, "hft connect failed: %v\n", err)
		return
	}
	defer hds.Close()
	_ = rec.emit(map[string]any{"kind": "connect", "feed": "hft", "ok": true})

	latency := envInt("ARROW_HFT_LATENCY_MS", 50)
	for _, mode := range []string{"ltpc", "full"} {
		err := hds.SubscribeHFTTokens(mode, 0, tokens, latency)
		_ = rec.emit(map[string]any{"kind": "subscribe", "feed": "hft", "mode": mode, "ok": err == nil, "err": errStr(err)})
		fmt.Fprintf(os.Stderr, "hft subscribe mode=%s ok=%v err=%v\n", mode, err == nil, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	hds.ReadHFTWithFrame(ctx,
		func(tick arrow.HFTLTPTick) {},
		func(tick arrow.HFTFullTick) {},
		func(pkt arrow.HFTResponsePacket) {
			_ = rec.emit(map[string]any{"kind": "response", "feed": "hft", "frameSize": pkt.FrameSize, "pktType": pkt.PktType, "errorCode": pkt.ErrorCode, "errorMsg": pkt.ErrorMsg, "requestType": pkt.RequestTypeStr, "mode": pkt.ModeStr, "successCount": pkt.SuccessCount, "errorCount": pkt.ErrorCount})
			fmt.Fprintf(os.Stderr, "hft response: type=%s mode=%s success=%d errors=%d code=%q msg=%q\n", pkt.RequestTypeStr, pkt.ModeStr, pkt.SuccessCount, pkt.ErrorCount, pkt.ErrorCode, pkt.ErrorMsg)
		},
		func(mt int, payload []byte) {
			smu.Lock()
			summary.HFTFrames++
			smu.Unlock()
			_ = rec.emit(map[string]any{"kind": "frame", "feed": "hft", "mt": mt, "len": len(payload)})
		},
		func(payload []byte) {
			smu.Lock()
			summary.HFT.add(len(payload))
			smu.Unlock()
			_ = rec.emit(map[string]any{"kind": "raw", "feed": "hft", "len": len(payload), "hex": hex.EncodeToString(payload)})
		},
		func(err error) {
			if ctx.Err() != nil {
				return
			}
			_ = rec.emit(map[string]any{"kind": "error", "feed": "hft", "err": err.Error()})
			fmt.Fprintf(os.Stderr, "hft read error: %v\n", err)
		})
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
