package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNormalizeOrderUpdateMapsIdentityAndFill(t *testing.T) {
	update := map[string]any{
		"id": "BRK-1", "remarks": "INS1234567890123", "token": "3045",
		"orderStatus": "COMPLETE", "reportType": "Fill", "fillShares": "2",
		"averagePrice": "15050", "exchangeOrderID": "EX-1",
		"exchangeUpdateTime": "2026-08-19T10:00:00Z",
	}
	report := NormalizeOrderUpdate(update)
	if report.Outcome != OutcomeSuccess || report.BrokerOrderID != "BRK-1" ||
		report.ClientOrderRef != "INS1234567890123" || report.FillShares != "2" {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.PostbackEventID == "" || len(report.PostbackEventID) != 64 {
		t.Fatalf("postback identity missing: %q", report.PostbackEventID)
	}
	if again := NormalizeOrderUpdate(update); again.PostbackEventID != report.PostbackEventID {
		t.Fatal("identical postbacks must have the same deterministic event identity")
	}
}

func TestNormalizeOrderUpdateDoesNotAcceptUnknownStatus(t *testing.T) {
	report := NormalizeOrderUpdate(map[string]any{"id": "BRK-1", "orderStatus": "NEW_STATUS"})
	if report.Outcome != OutcomeUnknown {
		t.Fatalf("unknown status outcome=%s, want UNKNOWN", report.Outcome)
	}
}

type scriptedOrderSource struct {
	updates []map[string]any
	err     error
	closed  bool
}

func (s *scriptedOrderSource) Read(ctx context.Context, onUpdate func(map[string]any), onError func(error)) {
	for _, update := range s.updates {
		onUpdate(update)
	}
	if s.err != nil {
		onError(s.err)
	}
	<-ctx.Done()
}
func (s *scriptedOrderSource) Close() error {
	s.closed = true
	return nil
}

func TestRunPostbackLoopPublishesAndClosesOnCancellation(t *testing.T) {
	source := &scriptedOrderSource{updates: []map[string]any{{"id": "BRK-1", "orderStatus": "OPEN"}}, err: errors.New("disconnect")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports := make(chan ReportEnvelope, 1)
	done := make(chan struct{})
	go func() {
		RunPostbackLoop(ctx, func() (OrderUpdateSource, error) { return source, nil }, func(report ReportEnvelope) error {
			reports <- report
			cancel()
			return nil
		}, nil)
		close(done)
	}()
	select {
	case report := <-reports:
		if report.BrokerOrderID != "BRK-1" {
			t.Fatalf("report=%+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("postback report timeout")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("postback loop did not stop")
	}
	if !source.closed {
		t.Fatal("postback source must close on cancellation")
	}
}
