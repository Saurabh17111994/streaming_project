package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// countEvents returns the number of NDJSON lines whose event field equals want.
func countEvents(t *testing.T, out string, event string) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var ev struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal line %q: %v", line, err)
		}
		if ev.Event == event {
			n++
		}
	}
	return n
}

// lastLineEvent returns the event field of the final NDJSON line.
func lastLineEvent(t *testing.T, out string) string {
	t.Helper()
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return ""
	}
	lines := strings.Split(trimmed, "\n")
	var ev struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ev); err != nil {
		t.Fatalf("unmarshal last line %q: %v", lines[len(lines)-1], err)
	}
	return ev.Event
}

// withShutdownOnce resets the package-level shutdown once and restores it after.
func withShutdownOnce(t *testing.T) {
	t.Helper()
	old := bridgeShutdownOnce
	bridgeShutdownOnce = &sync.Once{}
	t.Cleanup(func() { bridgeShutdownOnce = old })
}

// TestShutdownDrainEventEmittedOnce — the drain marker (bridge_shutdown) is
// emitted exactly once and is the last line after preceding tick writes.
func TestShutdownDrainEventEmittedOnce(t *testing.T) {
	old := bridgeEmitter
	var out bytes.Buffer
	bridgeEmitter = NewBridgeEmitter(&out)
	defer func() { bridgeEmitter = old }()
	withShutdownOnce(t)

	// Simulate a run that emitted ticks, then a single shutdown.
	_ = bridgeEmitter.EmitTick(Tick{Feed: "hft", Mode: "full", Token: 1000, LTP: 15050}, "hft-0", "hft-0", 1, time.Now(), nil)
	_ = bridgeEmitter.EmitTick(Tick{Feed: "hft", Mode: "full", Token: 1000, LTP: 15051}, "hft-0", "hft-0", 1, time.Now(), nil)
	emitShutdownEvent()

	if got := countEvents(t, out.String(), EventBridgeShutdown); got != 1 {
		t.Fatalf("bridge_shutdown emitted %d times, want 1\n%s", got, out.String())
	}
	// The shutdown event must be the terminal (last) line — the drain marker.
	if got := lastLineEvent(t, out.String()); got != EventBridgeShutdown {
		t.Fatalf("last event = %q, want %q (drain must be the final line)\n%s", got, EventBridgeShutdown, out.String())
	}
}

// TestShutdownDuplicateIsIdempotent — calling the shutdown path twice (e.g. a
// redundant signal handler + main fallthrough) must still emit exactly one
// bridge_shutdown event and not corrupt the stream.
func TestShutdownDuplicateIsIdempotent(t *testing.T) {
	old := bridgeEmitter
	var out bytes.Buffer
	bridgeEmitter = NewBridgeEmitter(&out)
	defer func() { bridgeEmitter = old }()
	withShutdownOnce(t)

	emitShutdownEvent()
	emitShutdownEvent() // duplicate shutdown path

	if got := countEvents(t, out.String(), EventBridgeShutdown); got != 1 {
		t.Fatalf("bridge_shutdown emitted %d times on duplicate shutdown, want 1\n%s", got, out.String())
	}
	// Stream must still be valid NDJSON (no partial/corrupt line).
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Fatalf("duplicate shutdown left corrupt NDJSON: %q: %v", line, err)
		}
	}
}
