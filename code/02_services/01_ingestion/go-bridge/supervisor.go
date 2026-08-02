package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// slotEpochResult is the outcome of one slot epoch (one connect attempt).
// It drives the reconnect loop's decision to retry or stop the slot.
type slotEpochResult int

const (
	// epochRetryable — the connection dropped, stalled, or heartbeats failed;
	// the slot should back off and retry with a fresh epoch.
	epochRetryable slotEpochResult = iota
	// epochTerminal — the subscription was rejected as invalid or auth refresh
	// was exhausted; the slot must stop and be reported as terminal.
	epochTerminal
	// epochRecovered — the slot reached ACTIVE and was later cancelled or
	// ended cleanly; it is done without error.
	epochRecovered
)

// slotOutcome is the terminal result of one slot's supervised run.
type slotOutcome struct {
	slotID   string
	terminal bool // true if the slot stopped due to a terminal error
	reason   string
}

// streamFactoryFor builds the production stream factory for one slot.
// ARROW_HFT_URL is a development/test-only override to point the bridge at a
// local fake broker; production uses the SDK constant via NewStreamsWithHFT.
func streamFactoryFor(client *arrow.Client, _ int) func() (hftStream, error) {
	return func() (hftStream, error) {
		if override := os.Getenv("ARROW_HFT_URL"); override != "" {
			return client.ConnectHFTDataStreamURL(override)
		}
		streams, err := client.NewStreamsWithHFT()
		if err != nil {
			return nil, err
		}
		return streams.HFTDataStream, nil
	}
}

// runHFTSlot runs one slot's reconnect loop until the slot terminates, the
// context is cancelled, or a terminal error stops the slot. It returns the
// slot's terminal outcome so the supervisor can aggregate and cancel peers.
func runHFTSlot(ctx context.Context, cancel context.CancelFunc, client *arrow.Client, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) slotOutcome {
	return runHFTSlotWithFactory(ctx, cancel, streamFactoryFor(client, 0), slot, latencyMs, responseTimeout, refreshAuth, logf)
}

// runHFTSlotWithFactory is runHFTSlot with an injectable stream factory.
func runHFTSlotWithFactory(ctx context.Context, cancel context.CancelFunc, factory func() (hftStream, error), slot SlotAssignment, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) slotOutcome {
	outcome := slotOutcome{slotID: slot.SlotID}
	runReconnectLoop(ctx, func(epoch uint64) bool {
		result := runHFTEpoch(ctx, cancel, factory, slot, latencyMs, responseTimeout, epoch, refreshAuth, logf)
		switch result {
		case epochTerminal:
			// Terminal: report and stop this slot (the epoch already emitted
			// the TERMINAL event). No retry.
			outcome.terminal = true
			outcome.reason = "terminal"
			return true
		case epochRecovered:
			return true
		default: // epochRetryable — reconnect with backoff.
			return false
		}
	}, func(epoch uint64, delay time.Duration) {
		_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "reconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: fmt.Sprintf("retry_in_%s", delay), ReceivedTsMs: time.Now().UnixMilli()})
	}, func(ctx context.Context, delay time.Duration) {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	})
	return outcome
}

// runHFTSupervisor starts one slot goroutine per slot, keeps healthy slots
// alive during peer retry, aggregates terminal outcomes, and cancels the
// shared context when the process shuts down or a terminal policy applies.
// It returns the number of slots that ended terminal.
//
// Policy: a terminal slot stops itself; healthy slots are not disturbed. When
// the process context is cancelled (shutdown), every slot stops.
func runHFTSupervisor(ctx context.Context, cancel context.CancelFunc, client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) int {
	return runHFTSupervisorWithFactory(ctx, cancel, streamFactoryFor, client, plan, latencyMs, responseTimeout, refreshAuth, logf)
}

// runHFTSupervisorWithFactory is runHFTSupervisor with an injectable stream
// factory builder, so tests can drive the real supervisor with fake streams.
// The builder is invoked once per slot with the slot index, allowing tests to
// give each slot a distinct scripted stream deterministically.
func runHFTSupervisorWithFactory(ctx context.Context, cancel context.CancelFunc, makeFactory func(*arrow.Client, int) func() (hftStream, error), client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) int {
	if len(plan.Slots) == 0 {
		return 0
	}
	var wg sync.WaitGroup
	outcomes := make([]slotOutcome, len(plan.Slots))
	for i, slot := range plan.Slots {
		wg.Add(1)
		go func(i int, slot SlotAssignment) {
			defer wg.Done()
			// R-200: a panic in one slot's main flow must not take down the
			// whole bridge — recover, record the slot as terminal, and let the
			// supervisor aggregate.
			defer func() {
				if r := recover(); r != nil {
					logf("HFT slot %s panicked: %v", slot.SlotID, r)
					outcomes[i] = slotOutcome{slotID: slot.SlotID, terminal: true, reason: fmt.Sprintf("panic: %v", r)}
				}
			}()
			outcomes[i] = runHFTSlotWithFactory(ctx, cancel, makeFactory(client, i), slot, latencyMs, responseTimeout, refreshAuth, logf)
		}(i, slot)
	}
	wg.Wait()
	terminal := 0
	for _, o := range outcomes {
		if o.terminal {
			terminal++
		}
	}
	return terminal
}
