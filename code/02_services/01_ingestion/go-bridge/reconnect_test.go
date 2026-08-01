package main

import (
	"context"
	"testing"
	"time"
)

func TestReconnectLoopEpochAndBackoffAfterForcedDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var epochs []uint64
	var delays []time.Duration
	runReconnectLoop(ctx,
		func(epoch uint64) bool {
			epochs = append(epochs, epoch)
			if epoch == 3 {
				cancel()
			}
			return false
		},
		func(_ uint64, delay time.Duration) { delays = append(delays, delay) },
		func(_ context.Context, _ time.Duration) {},
	)
	if got, want := len(epochs), 3; got != want {
		t.Fatalf("epochs=%v, want %d attempts", epochs, want)
	}
	for i, want := range []uint64{1, 2, 3} {
		if epochs[i] != want {
			t.Fatalf("epochs=%v, want [1 2 3]", epochs)
		}
	}
	if len(delays) != 2 || delays[0] != time.Second || delays[1] != 2*time.Second {
		t.Fatalf("delays=%v, want [1s 2s]", delays)
	}
}

func TestReconnectLoopStopsDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := 0
	runReconnectLoop(ctx,
		func(uint64) bool { called++; return false },
		func(_ uint64, _ time.Duration) {},
		func(ctx context.Context, _ time.Duration) { cancel(); <-ctx.Done() },
	)
	if called != 1 {
		t.Fatalf("attempts=%d, want one attempt before cancellation", called)
	}
}

// TestReconnectLoopRecoversAfterFailures — a slot that fails twice then
// succeeds (returns true) terminates the reconnect loop: independent recovery
// without cancelling the shared context.
func TestReconnectLoopRecoversAfterFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	epochs := []uint64{}
	runReconnectLoop(ctx,
		func(epoch uint64) bool {
			epochs = append(epochs, epoch)
			return epoch == 3 // recover on the third attempt
		},
		func(_ uint64, _ time.Duration) {},
		func(_ context.Context, _ time.Duration) {},
	)
	if len(epochs) != 3 {
		t.Fatalf("epochs=%v, want 3 attempts (2 failures + recovery)", epochs)
	}
	if ctx.Err() != nil {
		t.Fatal("recovered slot must not cancel the shared context")
	}
}
