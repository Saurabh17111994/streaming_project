package main

import (
	"errors"
	"testing"
)

func TestSDKErrorClassificationNeverTurnsAmbiguityIntoSuccess(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		outcome string
	}{
		{name: "documented rejection", err: errors.New(`request failed with status 400: {"status":"error","message":"bad quantity"}`), outcome: OutcomeRejected},
		{name: "auth", err: errors.New(`request failed with status 401: {"status":"error"}`), outcome: OutcomeUnknown},
		{name: "timeout", err: errors.New(`request failed with status 408: timeout`), outcome: OutcomeUnknown},
		{name: "server", err: errors.New(`request failed with status 500: failed`), outcome: OutcomeUnknown},
		{name: "malformed", err: errors.New("order placement failed"), outcome: OutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifySDKError(test.err)
			if got.Outcome != test.outcome {
				t.Fatalf("outcome=%s, want %s (%+v)", got.Outcome, test.outcome, got)
			}
			if got.Outcome == OutcomeSuccess {
				t.Fatal("ambiguous SDK error became success")
			}
		})
	}
}

func TestFakeBrokerRecordsOnlyOneAttempt(t *testing.T) {
	fake := NewFakeBroker()
	fake.SetResult(CommandPlace, BrokerResult{Outcome: OutcomeUnknown, Reason: "ambiguous_broker_response"})
	command := validPlaceCommand()
	first := fake.Place(t.Context(), command)
	if first.Outcome != OutcomeUnknown || fake.Calls(CommandPlace) != 1 {
		t.Fatalf("first=%+v calls=%d", first, fake.Calls(CommandPlace))
	}
	// The bridge deliberately has no retry loop. A second call is only made by
	// an explicit caller after reconciliation, never internally.
	if fake.Calls(CommandPlace) != 1 {
		t.Fatal("fake broker call count changed without an explicit command")
	}
}
