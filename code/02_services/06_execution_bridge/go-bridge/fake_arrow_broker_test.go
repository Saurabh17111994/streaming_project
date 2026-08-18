package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
	"github.com/gorilla/websocket"
)

// TestFakeArrowHTTPBroker exercises the pinned SDK adapter against the same
// HTTP verbs and paths used by Arrow.  It deliberately uses no credentials or
// external network and covers the complete synchronous command surface.
func TestFakeArrowHTTPBroker(t *testing.T) {
	var mu sync.Mutex
	seen := make([]string, 0, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("appId") != "app" || r.Header.Get("token") != "token" {
			t.Errorf("missing SDK auth headers: app=%q token=%q", r.Header.Get("appId"), r.Header.Get("token"))
		}
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/order/regular":
			_, _ = w.Write([]byte(`{"status":"success","data":{"orderNo":"BRK-HTTP-1","requestTime":"2026-08-19T10:00:00Z"}}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/order/regular/BRK-HTTP-1":
			_, _ = w.Write([]byte(`{"status":"success","data":{"orderNo":"BRK-HTTP-1","requestTime":"2026-08-19T10:00:01Z"}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/order/regular/BRK-HTTP-1":
			_, _ = w.Write([]byte(`{"status":"success","data":{"message":"cancelled"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/order/BRK-HTTP-1":
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/user/orders":
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/user/trades":
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/user/positions":
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	command := validPlaceCommand()

	placed := broker.Place(t.Context(), command)
	if placed.Outcome != OutcomeSuccess || placed.BrokerOrderID != "BRK-HTTP-1" {
		t.Fatalf("place=%+v", placed)
	}
	command.BrokerOrderID = placed.BrokerOrderID
	if modified := broker.Modify(t.Context(), command); modified.Outcome != OutcomeSuccess {
		t.Fatalf("modify=%+v", modified)
	}
	if cancelled := broker.Cancel(t.Context(), command); cancelled.Outcome != OutcomeSuccess {
		t.Fatalf("cancel=%+v", cancelled)
	}
	if queried := broker.QueryOrder(t.Context(), command); queried.Outcome != OutcomeSuccess {
		t.Fatalf("query=%+v", queried)
	}
	if orders := broker.ReconcileOrders(t.Context(), command); orders.Outcome != OutcomeSuccess {
		t.Fatalf("orders=%+v", orders)
	}
	if trades := broker.ReconcileTrades(t.Context(), command); trades.Outcome != OutcomeSuccess {
		t.Fatalf("trades=%+v", trades)
	}
	if positions := broker.ReconcilePositions(t.Context(), command); positions.Outcome != OutcomeSuccess {
		t.Fatalf("positions=%+v", positions)
	}

	mu.Lock()
	got := strings.Join(seen, ",")
	mu.Unlock()
	want := "POST /order/regular,PATCH /order/regular/BRK-HTTP-1,DELETE /order/regular/BRK-HTTP-1,GET /order/BRK-HTTP-1,GET /user/orders,GET /user/trades,GET /user/positions"
	if got != want {
		t.Fatalf("HTTP calls=%q, want %q", got, want)
	}
}

func TestFakeArrowHTTPBrokerCoversTimeoutAsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(`{"status":"success","data":{"orderNo":"too-late"}}`))
	}))
	defer server.Close()
	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	client.HTTPClient.ReadTimeout = 10 * time.Millisecond
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(context.Background(), validPlaceCommand())
	if result.Outcome != OutcomeUnknown {
		t.Fatalf("timeout result=%+v, want UNKNOWN", result)
	}
}

func TestFakeArrowHTTPBrokerCoversDocumentedRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":"error","message":"quantity exceeds limit"}`))
	}))
	defer server.Close()
	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(context.Background(), validPlaceCommand())
	if result.Outcome != OutcomeRejected {
		t.Fatalf("rejection result=%+v, want REJECTED", result)
	}
}

type fakeArrowOrderStream struct {
	url string
}

func (s *fakeArrowOrderStream) Read(ctx context.Context, onUpdate func(map[string]any), onError func(error)) {
	conn, _, err := websocket.DefaultDialer.Dial(s.url, nil)
	if err != nil {
		onError(err)
		return
	}
	defer conn.Close()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	for {
		messageType, payload, readErr := conn.ReadMessage()
		if readErr != nil {
			if ctx.Err() == nil {
				onError(readErr)
			}
			return
		}
		if messageType != websocket.TextMessage {
			continue
		}
		var update map[string]any
		// Match the SDK's behavior: keepalive/non-JSON text is ignored, while
		// valid but unknown order statuses are delivered as UNKNOWN by the
		// bridge normalizer.
		if json.Unmarshal(payload, &update) != nil {
			continue
		}
		onUpdate(update)
	}
}

func (s *fakeArrowOrderStream) Close() error { return nil }

func TestFakeArrowWebSocketLifecycleAndReconnect(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		mu.Lock()
		connections++
		connection := connections
		mu.Unlock()
		defer conn.Close()
		if connection == 1 {
			frames := []map[string]any{
				{"id": "BRK-WS-1", "remarks": "INS-WS-1", "orderStatus": "OPEN", "reportType": "NewAck"},
				{"id": "BRK-WS-1", "remarks": "INS-WS-1", "orderStatus": "OPEN", "reportType": "Fill", "fillShares": "2", "fillQuantity": "2", "fillPrice": "15050", "averagePrice": "15050"},
				{"id": "BRK-WS-1", "remarks": "INS-WS-1", "orderStatus": "COMPLETE", "reportType": "Fill", "fillShares": "2", "fillQuantity": "2", "fillPrice": "15050", "averagePrice": "15050"},
				{"id": "BRK-WS-1", "remarks": "INS-WS-1", "orderStatus": "OPEN", "reportType": "Fill", "fillShares": "1", "fillQuantity": "1", "fillPrice": "15040", "averagePrice": "15045"},
				{"id": "BRK-WS-1", "remarks": "INS-WS-1", "orderStatus": "CANCELLED", "reportType": "Canceled"},
				{"id": "BRK-WS-2", "remarks": "INS-WS-2", "orderStatus": "REJECTED", "reportType": "Rejected"},
				{"id": "BRK-WS-3", "remarks": "INS-WS-3", "orderStatus": "BROKER_ADDED_STATUS"},
			}
			for _, frame := range frames {
				if err := conn.WriteJSON(frame); err != nil {
					return
				}
			}
			// SDK-compatible keepalive/malformed text is intentionally ignored.
			_ = conn.WriteMessage(websocket.TextMessage, []byte("PONG"))
			_ = conn.WriteMessage(websocket.TextMessage, []byte("not-json"))
			_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "fixture complete"))
			return
		}
		// A reconnect is observation-only; it emits one postback and never a
		// place command. This is the recovery leg of the fixture.
		_ = conn.WriteJSON(map[string]any{"id": "BRK-WS-1", "remarks": "INS-WS-1", "orderStatus": "COMPLETE", "reportType": "Fill", "fillShares": "2", "fillQuantity": "2", "fillPrice": "15050", "averagePrice": "15050"})
		time.Sleep(10 * time.Millisecond)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	reports := make(chan ReportEnvelope, 8)
	var published atomic.Int32
	connects := 0
	done := make(chan struct{})
	go func() {
		runPostbackLoop(ctx, func() (OrderUpdateSource, error) {
			connects++
			return &fakeArrowOrderStream{url: "ws" + strings.TrimPrefix(server.URL, "http")}, nil
		}, func(report ReportEnvelope) error {
			reports <- report
			if published.Add(1) == 8 {
				cancel()
			}
			return nil
		}, nil, time.Millisecond, 2*time.Millisecond)
		close(done)
	}()

	var got []ReportEnvelope
	deadline := time.After(2 * time.Second)
	for len(got) < 8 {
		select {
		case report := <-reports:
			got = append(got, report)
		case <-deadline:
			t.Fatalf("received %d reports, want 8", len(got))
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("postback loop did not stop")
	}
	if connects < 2 {
		t.Fatalf("connects=%d, want reconnect", connects)
	}
	if got[1].FillShares != "2" || got[1].ReportType != "Fill" {
		t.Fatalf("partial fill=%+v", got[1])
	}
	if got[4].OrderStatus != "CANCELLED" || got[5].Outcome != OutcomeRejected {
		t.Fatalf("cancel/rejection reports=%+v %+v", got[4], got[5])
	}
	if got[6].Outcome != OutcomeUnknown {
		t.Fatalf("unknown status=%+v", got[6])
	}
	if got[1].PostbackEventID == got[2].PostbackEventID {
		t.Fatal("different lifecycle updates must not share an event identity")
	}
	if got[2].PostbackEventID != got[7].PostbackEventID {
		t.Fatal("replayed update must preserve deterministic postback identity")
	}
}
