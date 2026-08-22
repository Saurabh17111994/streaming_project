package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// fakeHFTStream is a scripted hftStream for fault tests. Each knob triggers a
// specific fault when non-nil; ticks can be scheduled after subscription.
type fakeHFTStream struct {
	mu sync.Mutex

	subscribeErr   error                        // error returned by SubscribeHFTTokens
	response       *arrow.HFTResponsePacket     // the single response delivered for the first request
	ackCh          chan arrow.HFTResponsePacket // when set: one SUCCESS ack per SubscribeHFTTokens batch (multi-request slots)
	noResponse     bool                         // never deliver a response (→ timeout)
	heartbeatErr   error                        // next WriteText("PONG") returns this
	readErr        error                        // onError delivered once the read loop starts
	decodeErrCount int                          // deliver this many decode-class onError calls
	closeErr       error                        // Close returns this
	onLTPTick      arrow.HFTLTPTick             // emitted after subscribe (via callbacks)
	onFullTick     arrow.HFTFullTick
	onDecodedFrame []byte // delivered via onDecoded before the tick

	writeCalls   []string
	subscribeIds [][]int32
	closed       bool
}

func newFakeHFTStream() *fakeHFTStream {
	return &fakeHFTStream{}
}

func (f *fakeHFTStream) SubscribeHFTTokens(mode string, exchSeg int, ids []int32, latencyMS int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribeIds = append(f.subscribeIds, append([]int32(nil), ids...))
	if f.ackCh != nil {
		select {
		case f.ackCh <- arrow.HFTResponsePacket{ErrorCode: "SUCCESS", SuccessCount: uint16(len(ids))}:
		default:
		}
	}
	if f.subscribeErr != nil {
		return f.subscribeErr
	}
	return nil
}

// IsClosed reports whether the stream was Close()d, lock-protected for
// cross-goroutine assertions (supervisor slot-isolation tests).
func (f *fakeHFTStream) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeHFTStream) WriteText(payload string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeCalls = append(f.writeCalls, payload)
	if f.heartbeatErr != nil {
		return f.heartbeatErr
	}
	return nil
}

func (f *fakeHFTStream) ReadHFTWithFrame(ctx context.Context,
	onLTP func(arrow.HFTLTPTick),
	onFull func(arrow.HFTFullTick),
	onResponse func(arrow.HFTResponsePacket),
	onFrame func(mt int, payload []byte),
	onDecoded func(frame []byte),
	onError func(err error)) {
	f.mu.Lock()
	readErr := f.readErr
	response := f.response
	ackCh := f.ackCh
	noResponse := f.noResponse
	ltp := f.onLTPTick
	full := f.onFullTick
	decoded := f.onDecodedFrame
	decodeErrCount := f.decodeErrCount
	f.mu.Unlock()

	// Deliver the subscription response (unless suppressed → timeout).
	if !noResponse && response != nil {
		onResponse(*response)
	}
	// Multi-batch mode: SubscribeHFTTokens enqueues one SUCCESS ack per batch;
	// relay them to onResponse so a 1024-token slot (2×512 requests) gets an
	// ack for every batch without a response timeout. The relay stops when the
	// read loop ends (readErr or ctx cancel).
	if ackCh != nil {
		relayDone := make(chan struct{})
		defer close(relayDone)
		go func() {
			for {
				select {
				case r := <-ackCh:
					onResponse(r)
				case <-relayDone:
					return
				}
			}
		}()
	}
	// Emit a decoded frame + tick so the watchdog sees a frame.
	if decoded != nil {
		onDecoded(append([]byte(nil), decoded...))
		onFrame(2, decoded)
	}
	if ltp.Token != 0 {
		onLTP(ltp)
	}
	if full.Token != 0 {
		onFull(full)
	}
	// Deliver decode-class errors repeatedly to exercise the burst classifier.
	for i := 0; i < decodeErrCount; i++ {
		onError(errors.New("hft unknown packet: 5 trailing bytes"))
	}
	if readErr != nil {
		onError(readErr)
		return
	}
	<-ctx.Done()
}

func (f *fakeHFTStream) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

// ----- helpers -----

func hftResponse(code string, success, errCount int) *arrow.HFTResponsePacket {
	return &arrow.HFTResponsePacket{
		ErrorCode:    code,
		SuccessCount: uint16(success),
		ErrorCount:   uint16(errCount),
	}
}

var errFakeHeartbeat = errors.New("websocket: write PONG failed")
var errFakeRead = errors.New("websocket: read failed")
var errFakeAuth = errors.New("websocket unauthorized")
var errFakeDial = errors.New("dial tcp 127.0.0.1:1: connection refused")

func timeoutDial() (time.Duration, error) { return 0, errFakeDial }
