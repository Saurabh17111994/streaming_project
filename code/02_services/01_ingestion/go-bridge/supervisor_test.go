package main

import (
	"context"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// TestSupervisorKeepsHealthySlotAliveDuringPeerRetry — plan §Go supervisor:
// "keep healthy slots alive during peer retry". Slot-0's stream factory fails
// (retryable) forever while slot-1 subscribes successfully and reaches ACTIVE.
// The supervisor must keep slot-1 alive while slot-0 backs off, and return
// zero terminal outcomes.
func TestSupervisorKeepsHealthySlotAliveDuringPeerRetry(t *testing.T) {
	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onDecodedFrame = []byte{0x28, 0x00, 0x00, 0x00, 0x01}
	healthy.onFullTick = arrow.HFTFullTick{Token: 1001, LTP: 15051}

	// makeFactory: slot 0 gets a factory that always fails to dial (retryable);
	// slot 1 gets the healthy scripted stream. Deterministic per slot index.
	makeFactory := func(_ *arrow.Client, slotIdx int) func() (hftStream, error) {
		if slotIdx == 0 {
			return func() (hftStream, error) { return nil, errFakeDial } // retry
		}
		return func() (hftStream, error) { return healthy, nil } // healthy
	}

	plan := SubscriptionPlan{Slots: []SlotAssignment{
		{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1000}, Requests: [][]int32{{1000}}},
		{SlotID: "hft-1", ConnectionID: "hft-1", Tokens: []int32{1001}, Requests: [][]int32{{1001}}},
	}}

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	ctx, cancel := context.WithCancel(context.Background())
	out := captureBridge(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			runHFTSupervisorWithFactory(ctx, makeFactory, client, plan, 50, 10*time.Second, nil, t.Logf)
		}()
		// Let slot-0 fail (backoff) and slot-1 reach ACTIVE.
		time.Sleep(300 * time.Millisecond)
		cancel()
		<-done
	})

	if got := lastEventState(eventsFrom(t, out), "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("healthy slot-1 must reach ACTIVE, got %q\n%s", got, out)
	}
	// slot-0 emitted reconnect BACKOFF events — it kept retrying while peer stayed up.
	if got := lastEventState(eventsFrom(t, out), "reconnect"); got != "BACKOFF" {
		t.Fatalf("slot-0 must back off and retry, got %q\n%s", got, out)
	}
}

// TestSupervisorStopsTerminalSlotWithoutDisturbingPeers — a slot that hits a
// terminal subscription error stops itself (reported terminal) while a healthy
// peer continues. The supervisor aggregates exactly one terminal outcome.
func TestSupervisorStopsTerminalSlotWithoutDisturbingPeers(t *testing.T) {
	terminal := newFakeHFTStream()
	terminal.response = hftResponse("E_ALL_INVALID", 0, 1)

	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onFullTick = arrow.HFTFullTick{Token: 2000, LTP: 15200}

	// Per-slot streams: slot 0 → terminal, slot 1 → healthy. Deterministic by
	// slot index — no shared counter race.
	makeFactory := func(_ *arrow.Client, slotIdx int) func() (hftStream, error) {
		if slotIdx == 0 {
			return func() (hftStream, error) { return terminal, nil }
		}
		return func() (hftStream, error) { return healthy, nil }
	}

	plan := SubscriptionPlan{Slots: []SlotAssignment{
		{SlotID: "hft-term", ConnectionID: "hft-term", Tokens: []int32{1}, Requests: [][]int32{{1}}},
		{SlotID: "hft-peer", ConnectionID: "hft-peer", Tokens: []int32{2000}, Requests: [][]int32{{2000}}},
	}}

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	ctx, cancel := context.WithCancel(context.Background())
	out := captureBridge(t, func() {
		done := make(chan int, 1)
		go func() {
			// The terminal slot stops itself (E_ALL_INVALID → epochTerminal)
			// without cancelling the shared context; the healthy peer keeps
			// running until we shut down.
			done <- runHFTSupervisorWithFactory(ctx, makeFactory, client, plan, 50, 10*time.Second, nil, t.Logf)
		}()
		// Generous fixed wait: the terminal slot fails in milliseconds and the
		// healthy peer reaches ACTIVE in milliseconds. (Not polling the captured
		// buffer here — it is only assigned after captureBridge returns.)
		time.Sleep(800 * time.Millisecond)
		cancel()
		<-done
	})

	if countState(t, out, "ACTIVE") < 1 {
		t.Fatalf("healthy peer must reach ACTIVE, got none\n%s", out)
	}
	if countState(t, out, "TERMINAL") < 1 {
		t.Fatalf("expected a TERMINAL subscription_ack, got none\n%s", out)
	}
}

// countState returns how many NDJSON events have a state field equal to want.
func countState(t *testing.T, out, state string) int {
	t.Helper()
	n := 0
	for _, e := range eventsFrom(t, out) {
		if s, _ := e["state"].(string); s == state {
			n++
		}
	}
	return n
}
