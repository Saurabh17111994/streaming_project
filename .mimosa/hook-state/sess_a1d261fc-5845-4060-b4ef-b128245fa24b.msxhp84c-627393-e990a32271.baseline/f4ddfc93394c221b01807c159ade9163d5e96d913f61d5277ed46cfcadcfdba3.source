package arrow

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
)

func TestHFTFakeSocketProtocol(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	requests := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		for i := 0; i < 2; i++ {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				t.Errorf("read subscription: %v", err)
				return
			}
			requests <- payload
			response := make([]byte, hftSizeResponse)
			binary.LittleEndian.PutUint32(response[0:4], hftSizeResponse)
			response[4] = hftPktResponse
			copy(response[6:22], []byte("SUCCESS"))
			response[534] = 0
			response[535] = 1
			binary.LittleEndian.PutUint16(response[536:538], 512)
			if err := conn.WriteMessage(websocket.BinaryMessage, compressHFTTest(response)); err != nil {
				t.Errorf("write response: %v", err)
				return
			}
		}
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("read heartbeat: %v", err)
			return
		}
		if string(payload) != "PONG" {
			t.Errorf("heartbeat=%q, want PONG", payload)
		}
	}))
	defer server.Close()

	client := NewClient("app", "secret")
	client.SetToken("token")
	stream, err := client.connectHFTDataStreamURL("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	responses := make(chan HFTResponsePacket, 2)
	go stream.ReadHFT(ctx, nil, nil, func(r HFTResponsePacket) { responses <- r }, nil)
	if err := stream.SubscribeHFTTokens("full", HFTExchNSECM, make([]int32, 512), 50); err != nil {
		t.Fatal(err)
	}
	if err := stream.SubscribeHFTTokens("full", HFTExchNSECM, make([]int32, 512), 50); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case r := <-responses:
			if r.ErrorCode != "SUCCESS" || r.SuccessCount != 512 || r.ErrorCount != 0 {
				t.Fatalf("response=%+v", r)
			}
		case <-time.After(time.Second):
			t.Fatal("response timeout")
		}
	}
	if got := <-requests; len(got) == 0 {
		t.Fatal("empty first request")
	}
	if got := <-requests; len(got) == 0 {
		t.Fatal("empty second request")
	}
	if err := stream.WriteText("PONG"); err != nil {
		t.Fatal(err)
	}
}

func compressHFTTest(payload []byte) []byte {
	encoder, _ := zstd.NewWriter(nil)
	defer encoder.Close()
	return encoder.EncodeAll(payload, nil)
}

func TestHFTFakeSocketPartialAndAllInvalidResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     string
		success  uint16
		errors   uint16
		wantCode string
	}{
		{name: "partial", code: "E_PARTIAL", success: 511, errors: 1, wantCode: "E_PARTIAL"},
		{name: "all-invalid", code: "E_ALL_INVALID", success: 0, errors: 512, wantCode: "E_ALL_INVALID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Errorf("upgrade: %v", err)
					return
				}
				defer conn.Close()
				if _, _, err := conn.ReadMessage(); err != nil {
					t.Errorf("read subscription: %v", err)
					return
				}
				response := make([]byte, hftSizeResponse)
				binary.LittleEndian.PutUint32(response[0:4], hftSizeResponse)
				response[4] = hftPktResponse
				copy(response[6:22], []byte(tc.code))
				response[535] = 1
				binary.LittleEndian.PutUint16(response[536:538], tc.success)
				binary.LittleEndian.PutUint16(response[538:540], tc.errors)
				_ = conn.WriteMessage(websocket.BinaryMessage, compressHFTTest(response))
			}))
			defer server.Close()

			client := NewClient("app", "secret")
			client.SetToken("token")
			stream, err := client.connectHFTDataStreamURL("ws" + strings.TrimPrefix(server.URL, "http"))
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer stream.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			responses := make(chan HFTResponsePacket, 1)
			go stream.ReadHFT(ctx, nil, nil, func(r HFTResponsePacket) { responses <- r }, nil)
			if err := stream.SubscribeHFTTokens("full", HFTExchNSECM, make([]int32, 512), 50); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-responses:
				if got.ErrorCode != tc.wantCode || got.SuccessCount != tc.success || got.ErrorCount != tc.errors {
					t.Fatalf("response=%+v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("response timeout")
			}
		})
	}
}

func TestHFTFakeSocketLTPAndFullFrames(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ltpFrame := make([]byte, hftSizeLTP)
	binary.LittleEndian.PutUint16(ltpFrame[0:2], hftSizeLTP)
	ltpFrame[2], ltpFrame[3] = hftPktLTP, HFTExchNSECM
	binary.LittleEndian.PutUint32(ltpFrame[4:8], 1234)
	binary.LittleEndian.PutUint32(ltpFrame[8:12], 10050)
	fullFrame := make([]byte, hftSizeFull)
	binary.LittleEndian.PutUint16(fullFrame[0:2], hftSizeFull)
	fullFrame[2], fullFrame[3] = hftPktFull, HFTExchNSECM
	binary.LittleEndian.PutUint32(fullFrame[4:8], 1234)
	binary.LittleEndian.PutUint32(fullFrame[8:12], 10050)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		payload := append(ltpFrame, fullFrame...)
		if err := conn.WriteMessage(websocket.BinaryMessage, compressHFTTest(payload)); err != nil {
			t.Errorf("write ticks: %v", err)
		}
	}))
	defer server.Close()

	client := NewClient("app", "secret")
	client.SetToken("token")
	stream, err := client.connectHFTDataStreamURL("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ltpTicks := make(chan HFTLTPTick, 1)
	fullTicks := make(chan HFTFullTick, 1)
	decodedFrames := make(chan []byte, 2)
	go stream.ReadHFTWithFrame(ctx, func(t HFTLTPTick) { ltpTicks <- t }, func(t HFTFullTick) { fullTicks <- t }, nil, nil, func(frame []byte) { decodedFrames <- frame }, nil)
	select {
	case got := <-ltpTicks:
		if got.Token != 1234 || got.LTP != 10050 {
			t.Fatalf("ltp=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("LTP timeout")
	}
	select {
	case got := <-fullTicks:
		if got.Token != 1234 || got.LTP != 10050 {
			t.Fatalf("full=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("full timeout")
	}
	// onDecoded must deliver the exact decompressed frame bytes for LTP then full.
	for i, want := range [][]byte{ltpFrame, fullFrame} {
		select {
		case got := <-decodedFrames:
			if len(got) != len(want) || !equalBytes(got, want) {
				t.Fatalf("decoded frame %d: got %d bytes, want %d", i, len(got), len(want))
			}
		case <-time.After(time.Second):
			t.Fatalf("decoded frame %d timeout", i)
		}
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHFTFakeSocketSilentServerDoesNotAcknowledge(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		time.Sleep(250 * time.Millisecond)
	}))
	defer server.Close()
	client := NewClient("app", "secret")
	client.SetToken("token")
	stream, err := client.connectHFTDataStreamURL("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	responses := make(chan HFTResponsePacket, 1)
	go stream.ReadHFT(ctx, nil, nil, func(r HFTResponsePacket) { responses <- r }, nil)
	if err := stream.SubscribeHFTTokens("full", HFTExchNSECM, make([]int32, 512), 50); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responses:
		t.Fatalf("unexpected acknowledgement: %+v", response)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHFTFakeSocketMalformedFrameReportsError(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.BinaryMessage, compressHFTTest([]byte{1, 2, 3, 4, 5}))
	}))
	defer server.Close()
	client := NewClient("app", "secret")
	client.SetToken("token")
	stream, err := client.connectHFTDataStreamURL("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errors := make(chan error, 1)
	go stream.ReadHFT(ctx, nil, nil, nil, func(err error) { errors <- err })
	if err := stream.SubscribeHFTTokens("full", HFTExchNSECM, make([]int32, 1), 50); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errors:
		if err == nil {
			t.Fatal("expected malformed-frame error")
		}
	case <-time.After(time.Second):
		t.Fatal("malformed-frame error timeout")
	}
}

// TestHFTTextFrameInvokesOnFrame — plan §hft_stream.go: onFrame must fire once
// after every successful WebSocket frame read, including text frames.
func TestHFTTextFrameInvokesOnFrame(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage() // wait for the subscription write
		if err := conn.WriteMessage(websocket.TextMessage, []byte("{\"type\":\"ping\"}")); err != nil {
			t.Errorf("write text frame: %v", err)
		}
	}))
	defer server.Close()

	client := NewClient("app", "secret")
	client.SetToken("token")
	stream, err := client.connectHFTDataStreamURL("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	frames := make(chan int, 2)
	go stream.ReadHFTWithFrame(ctx, nil, nil, nil,
		func(mt int, _ []byte) { frames <- mt },
		nil, nil)
	// Trigger the server to send its text frame.
	_ = stream.SubscribeHFTTokens("full", HFTExchNSECM, make([]int32, 1), 50)

	select {
	case mt := <-frames:
		if mt != websocket.TextMessage {
			t.Fatalf("onFrame message type = %d, want TextMessage(%d)", mt, websocket.TextMessage)
		}
	case <-time.After(time.Second):
		t.Fatal("onFrame not invoked for text frame")
	}
}

// TestHFTConcurrentWritesSerialized — plan §hft_stream.go: all socket writes
// reuse the HFTDataStream mutex, so concurrent WriteText calls must all succeed
// without interleaving corruption.
func TestHFTConcurrentWritesSerialized(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	received := make(chan string, 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for i := 0; i < 50; i++ {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			received <- string(payload)
		}
	}))
	defer server.Close()

	client := NewClient("app", "secret")
	client.SetToken("token")
	stream, err := client.connectHFTDataStreamURL("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			payload := []byte("PONG" + fmt.Sprintf("%02d", n))
			if err := stream.WriteText(string(payload)); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write failed: %v", err)
	}

	// All 50 writes must arrive intact (serialized under the mutex).
	seen := make(map[string]bool, 50)
	for i := 0; i < 50; i++ {
		select {
		case p := <-received:
			seen[p] = true
		case <-time.After(time.Second):
			t.Fatal("server did not receive all concurrent writes")
		}
	}
	if len(seen) != 50 {
		t.Fatalf("got %d distinct payloads, want 50 (interleaving corruption)", len(seen))
	}
}
