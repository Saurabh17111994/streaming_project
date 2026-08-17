package main

import "testing"

func TestReconnectStreakSemantics(t *testing.T) {
	reconnectState.mu.Lock()
	reconnectState.perSlot = map[string]int{}
	reconnectState.mu.Unlock()
	defer func() {
		reconnectState.mu.Lock()
		reconnectState.perSlot = map[string]int{}
		reconnectState.mu.Unlock()
	}()

	noteReconnect("hft-0")
	noteReconnect("hft-0")
	noteReconnect("hft-1")
	if got := maxReconnectConsecutive(); got != 2 {
		t.Fatalf("maxReconnectConsecutive=%d, want 2", got)
	}
	// A successful ACTIVE subscription resets only that slot's streak.
	noteRecovered("hft-0")
	if got := maxReconnectConsecutive(); got != 1 {
		t.Fatalf("after recovery maxReconnectConsecutive=%d, want 1 (hft-1 streak)", got)
	}
	noteRecovered("hft-1")
	if got := maxReconnectConsecutive(); got != 0 {
		t.Fatalf("after all recoveries maxReconnectConsecutive=%d, want 0", got)
	}
}

func TestActiveSocketsCounter(t *testing.T) {
	before := activeSockets.Load()
	addActiveSocket(1)
	addActiveSocket(1)
	addActiveSocket(-1)
	if got := activeSockets.Load(); got != before+1 {
		t.Fatalf("activeSockets=%d, want %d", got, before+1)
	}
	addActiveSocket(-1)
	if got := activeSockets.Load(); got != before {
		t.Fatalf("activeSockets=%d, want %d after close", got, before)
	}
}
