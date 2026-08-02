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
	"flag"
	"fmt"
	"net/http"
	"os"
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
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var sub map[string]any
			if json.Unmarshal(payload, &sub) != nil {
				continue
			}
			if code, _ := sub["code"].(string); code != "sub" {
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
			resp := make([]byte, hftSizeResponse)
			binary.LittleEndian.PutUint32(resp[0:4], hftSizeResponse)
			resp[4] = hftPktResponse
			copy(resp[6:22], "SUCCESS")
			resp[534] = 0
			resp[535] = 1
			binary.LittleEndian.PutUint16(resp[536:538], uint16(len(ids)))
			if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(enc, resp)); err != nil {
				return
			}
			full := make([]byte, hftSizeFull)
			binary.LittleEndian.PutUint16(full[0:2], hftSizeFull)
			full[2] = hftPktFull
			binary.LittleEndian.PutUint32(full[4:8], 757614) // token present in approved CSV
			binary.LittleEndian.PutUint32(full[8:12], 15050)
			if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(enc, full)); err != nil {
				return
			}
			if *disconnect > 0 && idx == *disconnect {
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "forced"), time.Now())
				return
			}
		}
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("fake HFT broker listening on %s (disconnect_after=%d)\n", addr, *disconnect)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func compressZstd(enc *zstd.Encoder, payload []byte) []byte {
	return enc.EncodeAll(payload, nil)
}
