//go:build faketool

// faketool runs a standalone fake HFT broker for the full-stack E2E test.
// It speaks the real wire format: JSON `sub` inbound, zstd-compressed binary
// response + full-tick frames outbound, with an optional forced disconnect.
//
// Usage: go run -tags faketool main.go -port 8899 -disconnect-after 1
// (the -port / -disconnect-after flags replace the historical
// FAKE_HFT_PORT / FAKE_HFT_DISCONNECT_AFTER env vars, R-274).
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
)

const (
	hftSizeResponse = 540
	hftSizeFull     = 196
	hftPktResponse  = 99
	hftPktFull      = 2
)

func main() {
	port := flag.Int("port", 8899, "listen port")
	disconnect := flag.Int("disconnect-after", 0, "connection index to force-close (1-based); 0 = never")
	// Soak-marathon mode: close every Nth new connection after the
	// subscription response + first tick, so the real supervisor cycles
	// through reconnect/backoff with no external driver (run-full-suite.sh).
	disconnectEvery := flag.Int("disconnect-every", 0, "close every Nth new connection (1-based); 0 = never")
	// R-220: a connection due for a forced close must first serve the client's
	// complete subscription batch — the bridge splits a 1024-token plan into
	// 2x512 requests, so closing right after the first response would leave
	// every epoch half-subscribed (no ACTIVE ack ever emitted). Close only
	// after the connection has been idle for close-linger-ms.
	closeLinger := flag.Duration("close-linger", 500*time.Millisecond, "how long a forced-close connection keeps serving sub frames before closing")
	// Soak mode: keep sending a full-tick frame every N ms so a long soak has
	// continuous Fluss append traffic instead of one tick per connection.
	tickInterval := flag.Int("tick-interval-ms", 0, "if >0, send a full-tick frame every N ms until the client disconnects; 0 = single tick then idle")
	flag.Parse()

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	// R-094: each WebSocket handler runs in its own goroutine under net/http;
	// the shared connection counter must be atomic or concurrent/reconnecting
	// clients race on it.
	var connections atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		idx := int(connections.Add(1))
		// R-213: one zstd encoder per connection — constructing and closing a
		// fresh encoder (table initialization included) per frame was wasteful.
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "zstd encoder init failed:", err)
			return
		}
		defer enc.Close()
		logf := func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "faketool: conn=%d %s\n", idx, fmt.Sprintf(format, args...))
		}
		logf("accepted")
		// gorilla/websocket panics on concurrent writes ("concurrent write to
		// websocket connection"): the ticker goroutine's data frames raced the
		// library's automatic pong replies to the client's heartbeats. All
		// writes — data frames, control frames, and the pong handler — share
		// one per-connection mutex so nothing can interleave.
		var wmu sync.Mutex
		send := func(frame []byte) error {
			wmu.Lock()
			defer wmu.Unlock()
			// Compress inside the lock: EncodeAll returns a slice aliasing the
			// encoder's internal buffer, and the ticker goroutine can compress
			// while the handler serves a second sub batch — a concurrent
			// EncodeAll would overwrite this payload mid-write and corrupt the
			// stream ("hft unknown packet: N trailing bytes" on the bridge).
			return conn.WriteMessage(websocket.BinaryMessage, compressZstd(enc, frame))
		}
		sendControl := func(kind int, payload []byte) error {
			wmu.Lock()
			defer wmu.Unlock()
			return conn.WriteControl(kind, payload, time.Now().Add(time.Second))
		}
		conn.SetPingHandler(func(appData string) error {
			wmu.Lock()
			defer wmu.Unlock()
			return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})
		closePending := (*disconnect > 0 && idx == *disconnect) ||
			(*disconnectEvery > 0 && idx%*disconnectEvery == 0)
		tickerStarted := false
		for {
			if closePending {
				// R-220: linger for further subscription frames instead of
				// closing right after the first response — the client may split
				// its plan across several sub requests and must complete the
				// whole batch (and emit its ACTIVE ack) before the forced close.
				if err := conn.SetReadDeadline(time.Now().Add(*closeLinger)); err != nil {
					logf("set read deadline failed: %v", err)
					return
				}
			}
			_, payload, err := conn.ReadMessage()
			if err != nil {
				if closePending {
					var netErr net.Error
					if errors.As(err, &netErr) && netErr.Timeout() {
						logf("subscription batch idle — force closing")
					} else {
						logf("read end: %v", err)
					}
					_ = sendControl(websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseNormalClosure, "every"))
				} else {
					logf("read end: %v", err)
				}
				return
			}
			var sub map[string]any
			if json.Unmarshal(payload, &sub) != nil {
				logf("non-json frame (len=%d)", len(payload))
				continue
			}
			if code, _ := sub["code"].(string); code != "sub" {
				logf("non-sub frame code=%q (len=%d)", code, len(payload))
				continue
			}
			ids := []any{}
			if symIds, ok := sub["symIds"].([]any); ok && len(symIds) > 0 {
				if seg, ok := symIds[0].(map[string]any); ok {
					if segIDs, ok := seg["ids"].([]any); ok {
						ids = segIDs
					}
				}
			}
			logf("sub received (ids=%d)", len(ids))
			resp := make([]byte, hftSizeResponse)
			binary.LittleEndian.PutUint32(resp[0:4], hftSizeResponse)
			resp[4] = hftPktResponse
			copy(resp[6:22], "SUCCESS")
			resp[534] = 0
			resp[535] = 1
			binary.LittleEndian.PutUint16(resp[536:538], uint16(len(ids)))
			if err := send(resp); err != nil {
				logf("send response failed: %v", err)
				return
			}
			// Soak mode: keep emitting ticks until the client goes away. The
			// ticker starts right after the first sub response (before the
			// snapshot burst) so ticks flow continuously from subscription
			// time; it exits via write errors once the connection closes.
			if !closePending && *tickInterval > 0 && !tickerStarted {
				tickerStarted = true
				logf("ticker start (every %dms)", *tickInterval)
				tickerFrame := make([]byte, hftSizeFull)
				binary.LittleEndian.PutUint16(tickerFrame[0:2], hftSizeFull)
				tickerFrame[2] = hftPktFull
				binary.LittleEndian.PutUint32(tickerFrame[4:8], 757614)
				binary.LittleEndian.PutUint32(tickerFrame[8:12], 15050)
				go func() {
					t := time.NewTicker(time.Duration(*tickInterval) * time.Millisecond)
					defer t.Stop()
					for range t.C {
						binary.LittleEndian.PutUint64(tickerFrame[180:188], uint64(time.Now().UnixNano()))
						if err := send(tickerFrame); err != nil {
							logf("ticker send failed: %v", err)
							return
						}
					}
				}()
			}
			// Initial snapshot: a real broker sends one full-tick frame per
			// subscribed token right after the sub response — the ingestion
			// side counts seen tokens toward subscription completeness
			// (all 1024 must appear or readiness stays false). Emit the
			// approved fake token 757614 first, then one frame per requested
			// id so the whole assignment is seen.
			//
			// The burst runs in its own goroutine so the read loop keeps
			// draining sub requests: the bridge splits a 1024-token plan into
			// 2x512 requests and waits for BOTH responses inside a 10s
			// window. Serving sub1's 513-frame burst inline would delay sub2's
			// response by the whole burst (minutes under backpressure), so a
			// real broker's promptness is simulated: every request gets its
			// response immediately, bursts proceed concurrently.
			//
			// Frames are paced (~300ms) so the burst never outruns the Java
			// pipeline's consumption of the bridge's stdout pipe. The bridge
			// emits each tick synchronously to that pipe; when the Java side
			// processes ticks slowly (per-tick Fluss append + resource
			// metrics, ~10/s), an unpaced 1026-frame burst fills the pipe,
			// blocks the bridge's read loop mid-burst, and its second
			// subscription response (in-stream after the first 513 frames)
			// arrives past the 10s window — terminating the subscription.
			// Two bursts (one per 512-token sub) run concurrently, so the
			// combined rate is 2x this pacing + the 2/s ticker: 2/0.3 + 2
			// = ~8.7/s, which stays under the pipe's ~10/s drain rate.
			// (120ms pacing measured 16.7/s combined — enough to back up
			// the pipe and quarantine every aged frame as STALE.)
			go func(ids []any) {
				full := make([]byte, hftSizeFull)
				binary.LittleEndian.PutUint16(full[0:2], hftSizeFull)
				full[2] = hftPktFull
				binary.LittleEndian.PutUint32(full[4:8], 757614) // token present in approved CSV
				binary.LittleEndian.PutUint32(full[8:12], 15050)
				// A real broker stamps every frame with send-time nanoseconds
				// (bridge ts_ms = ts/1e6). Without it ts_ms=0 and the
				// ingestion freshness gate quarantines every tick as
				// INVALID_VALUES.
				binary.LittleEndian.PutUint64(full[180:188], uint64(time.Now().UnixNano()))
				if err := send(full); err != nil {
					logf("send snapshot tick failed: %v", err)
					return
				}
				snapshotSent := 1
				for _, id := range ids {
					tok, ok := id.(float64)
					if !ok {
						continue
					}
					time.Sleep(300 * time.Millisecond)
					binary.LittleEndian.PutUint32(full[4:8], uint32(int64(tok)))
					binary.LittleEndian.PutUint64(full[180:188], uint64(time.Now().UnixNano()))
					if err := send(full); err != nil {
						logf("send snapshot tick failed at %d: %v", snapshotSent, err)
						return
					}
					snapshotSent++
				}
				logf("snapshot sent (%d ticks)", snapshotSent)
			}(ids)
		}
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("fake HFT broker listening on %s (disconnect_after=%d disconnect_every=%d tick_interval_ms=%d)\n",
		addr, *disconnect, *disconnectEvery, *tickInterval)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func compressZstd(enc *zstd.Encoder, payload []byte) []byte {
	return enc.EncodeAll(payload, nil)
}
