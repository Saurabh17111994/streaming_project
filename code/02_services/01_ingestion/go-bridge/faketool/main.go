//go:build faketool

// faketool runs a standalone fake HFT broker for the full-stack E2E test.
// It speaks the real wire format: JSON `sub` inbound, zstd-compressed binary
// response + full-tick frames outbound, with an optional forced disconnect.
//
// Usage: FAKE_HFT_PORT=8899 FAKE_HFT_DISCONNECT_AFTER=1 go run -tags faketool faketool.go
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
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
	connections := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connections++
		idx := connections
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
			if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(resp)); err != nil {
				return
			}
			full := make([]byte, hftSizeFull)
			binary.LittleEndian.PutUint16(full[0:2], hftSizeFull)
			full[2] = hftPktFull
			binary.LittleEndian.PutUint32(full[4:8], 757614) // token present in approved CSV
			binary.LittleEndian.PutUint32(full[8:12], 15050)
			if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(full)); err != nil {
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

func compressZstd(payload []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	defer enc.Close()
	return enc.EncodeAll(payload, nil)
}
