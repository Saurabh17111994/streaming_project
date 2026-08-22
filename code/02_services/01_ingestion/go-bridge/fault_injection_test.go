package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// syncBuffer is a mutex-guarded capture buffer for the bridge NDJSON output —
// safe to read from the test goroutine while the bridge goroutine writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func newSyncBuffer() *syncBuffer { return &syncBuffer{} }

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestFaultInjectionDecodeBurstRecovers — plan §Error Handling / Phase 9:
// inject a decode-error burst (100 decode-class errors in <10s) into a live
// supervised slot. The burst must terminate that epoch, but the slot's
// reconnect loop must recover on a fresh epoch and reach ACTIVE again — the
// fault must not kill the bridge.
func TestFaultInjectionDecodeBurstRecovers(t *testing.T) {
	// Stream 1: healthy subscribe, then a decode-error burst (100) delivered
	// via onError, then a read error that ends the epoch.
	burst := newFakeHFTStream()
	burst.response = hftResponse("SUCCESS", 1, 0)
	burst.onFullTick = arrow.HFTFullTick{Token: 1000, LTP: 15050}
	burst.decodeErrCount = 100 // burst threshold → epoch terminates
	burst.readErr = errFakeRead

	// Stream 2 (reconnect): healthy — reaches ACTIVE and stays.
	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onFullTick = arrow.HFTFullTick{Token: 1000, LTP: 15051}

	// The slot factory yields the burst stream once, then healthy forever.
	attempts := 0
	factory := func() (hftStream, error) {
		attempts++
		if attempts == 1 {
			return burst, nil
		}
		return healthy, nil
	}

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	slot := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{1000}, Requests: [][]int32{{1000}}}

	oldEmitter := bridgeEmitter
	// Thread-safe capture buffer — the bridge goroutine writes concurrently
	// while the test polls (a plain bytes.Buffer would be a data race).
	capture := newSyncBuffer()
	bridgeEmitter = NewBridgeEmitter(capture)
	defer func() { bridgeEmitter = oldEmitter }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHFTSlotWithFactory(ctx, factory, slot, 50, 10*time.Second, nil, t.Logf)
	}()
	// Poll for the recovered ACTIVE ack instead of a fixed 300ms sleep —
	// under `go test -race` full-suite load the burst + reconnect can take
	// longer than 300ms, which made this assertion flaky.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(capture.String(), `"event":"subscription_ack","state":"ACTIVE"`) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	out := capture.String()

	// The burst epoch must have produced an error, then a reconnect, and the
	// second epoch must reach ACTIVE — the fault was survived.
	events := eventsFrom(t, out)
	if got := lastEventState(events, "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("slot must recover to ACTIVE after decode burst, got %q\n%s", got, out)
	}
	if got := lastEventState(events, "reconnect"); got != "BACKOFF" {
		t.Fatalf("expected reconnect BACKOFF after burst, got %q\n%s", got, out)
	}
	// At least one reconnect means the burst killed epoch 1 and the loop retried.
	reconnects := 0
	for _, e := range events {
		if e["event"] == "reconnect" {
			reconnects++
		}
	}
	if reconnects < 1 {
		t.Fatalf("expected >=1 reconnect after fault injection, got %d\n%s", reconnects, out)
	}
}

// TestFaultInjectionAuthRefreshRecovers — plan §hft_slot.go: an auth error
// triggers a refresh; if the refresh succeeds the slot resumes (reconnect
// BACKOFF with reason authentication_refreshed), not terminal.
func TestFaultInjectionAuthRefreshRecovers(t *testing.T) {
	// Stream delivers an auth error on read; refreshAuth succeeds once.
	authFail := newFakeHFTStream()
	authFail.response = hftResponse("SUCCESS", 1, 0)
	authFail.readErr = errFakeAuth // classification → auth failure

	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onFullTick = arrow.HFTFullTick{Token: 2000, LTP: 15200}

	attempts := 0
	factory := func() (hftStream, error) {
		attempts++
		if attempts == 1 {
			return authFail, nil
		}
		return healthy, nil
	}

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	slot := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{2000}, Requests: [][]int32{{2000}}}

	ctx, cancel := context.WithCancel(context.Background())
	out := captureBridge(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			// refreshAuth returns nil (success) — the slot resumes.
			runHFTSlotWithFactory(ctx, factory, slot, 50, 10*time.Second,
				func(context.Context) error { return nil }, t.Logf)
		}()
		time.Sleep(300 * time.Millisecond)
		cancel()
		<-done
	})

	events := eventsFrom(t, out)
	if got := lastEventState(events, "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("slot must recover to ACTIVE after auth refresh, got %q\n%s", got, out)
	}
	// No TERMINAL/auth_failure — the refresh prevented a terminal state.
	for _, e := range events {
		if e["event"] == "auth_failure" {
			t.Fatalf("auth refresh success must not produce auth_failure\n%s", out)
		}
	}
}
