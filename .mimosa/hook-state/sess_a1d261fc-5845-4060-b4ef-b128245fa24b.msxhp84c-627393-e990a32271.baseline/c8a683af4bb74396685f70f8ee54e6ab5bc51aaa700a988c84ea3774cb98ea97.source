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

// ING-RES-003 — Backoff(attempt) golden sequence: exactly 1,2,4,8,16,30,30…s
// with NO jitter — deterministic for soak accounting (ING-RES-001).
func TestBackoffGoldenSequence(t *testing.T) {
	want := []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second,
		30 * time.Second,
	}
	for i, w := range want {
		if got := Backoff(i); got != w {
			t.Fatalf("Backoff(%d)=%v, want %v — the sequence must be deterministic", i, got, w)
		}
	}
	// Negative attempts clamp to attempt 0 (1 s) — a caller passing a
	// decremented counter below zero must not produce a zero/negative delay.
	if got := Backoff(-3); got != time.Second {
		t.Fatalf("Backoff(-3)=%v, want 1s (clamped)", got)
	}
}
