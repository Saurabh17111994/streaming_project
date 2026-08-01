package main

import (
	"context"
	"testing"
	"time"
)

func TestHFTSlotValidationAndEpoch(t *testing.T) {
	assignment := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1}, Requests: [][]int32{{1}}}
	slot, err := NewHFTSlot(assignment, SlotConfig{Mode: "full", LatencyMs: 50, Heartbeat: 3 * time.Second, StallTimeout: 15 * time.Second, ResponseTimeout: 10 * time.Second, MaxAuthRefreshes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if slot.BeginConnect() != 1 || slot.BeginConnect() != 2 {
		t.Fatal("epoch did not increment")
	}
	slot.SetState(SlotActive)
	now := time.Now()
	slot.ObserveFrame(now)
	if slot.Stalled(now.Add(14 * time.Second)) {
		t.Fatal("premature stall")
	}
	if !slot.Stalled(now.Add(16 * time.Second)) {
		t.Fatal("stall not detected")
	}
	slot.Close()
	slot.Close()
	if slot.State() != SlotTerminal {
		t.Fatal("close not idempotent")
	}
}

func TestBackoffSequence(t *testing.T) {
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second} {
		if got := Backoff(i); got != want {
			t.Fatalf("%d: %v", i, got)
		}
	}
}

func TestSlotCancellation(t *testing.T) {
	slot, _ := NewHFTSlot(SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1}, Requests: [][]int32{{1}}}, SlotConfig{Mode: "full", LatencyMs: 50, Heartbeat: 3 * time.Second, StallTimeout: 15 * time.Second, ResponseTimeout: 10 * time.Second, MaxAuthRefreshes: 3})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := slot.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if slot.State() != SlotTerminal {
		t.Fatal("slot not closed")
	}
}

// TestSlotConfigExactTimeouts — plan §hft_slot.go requires stall 15s and
// response 10s exactly (not just positive).
func TestSlotConfigExactTimeouts(t *testing.T) {
	base := SlotConfig{Mode: "full", LatencyMs: 50, Heartbeat: 3 * time.Second,
		StallTimeout: 15 * time.Second, ResponseTimeout: 10 * time.Second, MaxAuthRefreshes: 3}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	wrongStall := base
	wrongStall.StallTimeout = 14 * time.Second
	if err := wrongStall.Validate(); err == nil {
		t.Fatal("stall timeout must be exactly 15s")
	}
	wrongResponse := base
	wrongResponse.ResponseTimeout = 9 * time.Second
	if err := wrongResponse.Validate(); err == nil {
		t.Fatal("response timeout must be exactly 10s")
	}
}

// TestRequestUnionValidation — plan §hft_slot.go: the union of request batches
// must equal the assignment's tokens exactly (no missing, duplicated, or foreign
// tokens).
func TestRequestUnionValidation(t *testing.T) {
	good := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{1, 2, 3, 4}, Requests: [][]int32{{1, 2}, {3, 4}}}
	if err := validateRequestUnion(good); err != nil {
		t.Fatalf("valid union rejected: %v", err)
	}

	// Missing token (1 not covered).
	missing := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{1, 2, 3, 4}, Requests: [][]int32{{2, 3}, {4}}}
	if err := validateRequestUnion(missing); err == nil {
		t.Fatal("missing token must be rejected")
	}

	// Duplicated token across requests.
	dup := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{1, 2, 3}, Requests: [][]int32{{1, 2}, {1, 3}}}
	if err := validateRequestUnion(dup); err == nil {
		t.Fatal("duplicated token must be rejected")
	}

	// Foreign token not in assignment.
	foreign := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{1, 2}, Requests: [][]int32{{1, 99}}}
	if err := validateRequestUnion(foreign); err == nil {
		t.Fatal("foreign token must be rejected")
	}

	// NewHFTSlot enforces it too.
	cfg := SlotConfig{Mode: "full", LatencyMs: 50, Heartbeat: 3 * time.Second,
		StallTimeout: 15 * time.Second, ResponseTimeout: 10 * time.Second, MaxAuthRefreshes: 3}
	if _, err := NewHFTSlot(missing, cfg); err == nil {
		t.Fatal("NewHFTSlot must reject a non-covering request union")
	}
}
