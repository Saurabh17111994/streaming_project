package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestSandboxAutoReauth: fake clock expiry → exactly one re-auth → success;
// repeated failure → /healthz reports UP disabled, no order attempted, no infinite loop.
func TestSandboxAutoReauth(t *testing.T) {
	// Sequence: first Place returns 401 auth failure, re-auth succeeds, retry succeeds.
	t.Run("one_reauth_then_success", func(t *testing.T) {
		calls := 0
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			calls++
			if calls == 1 {
				return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
			}
			return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-REAUTH-1"}
		}}
		reauthCalls := 0
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			reauthCalls++
			return nil
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Outcome != OutcomeSuccess || result.BrokerOrderID != "BRK-REAUTH-1" {
			t.Fatalf("after re-auth: %+v", result)
		}
		if reauthCalls != 1 {
			t.Fatalf("reauthCalls=%d want 1", reauthCalls)
		}
		if calls != 2 {
			t.Fatalf("inner calls=%d want 2 (original + retry)", calls)
		}
		if rb.IsDisabled() {
			t.Fatal("broker should not be disabled after successful re-auth")
		}
		// healthz must still be UP
		server, _ := NewBridgeServer(rb, "test-token", "live")
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		server.Handler().ServeHTTP(rec, req)
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body["status"] != "UP" {
			t.Fatalf("healthz status=%v want UP", body["status"])
		}
	})

	t.Run("reauth_failure_then_disabled", func(t *testing.T) {
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
		}}
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			return errors.New("TOTP refresh failed")
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Outcome != OutcomeUnknown || result.Reason != "broker_disabled" {
			t.Fatalf("after failed re-auth: %+v", result)
		}
		if rb.ReauthCalls() != 1 {
			t.Fatalf("reauthCalls=%d want 1 (no loop)", rb.ReauthCalls())
		}
		if !rb.IsDisabled() {
			t.Fatal("broker should be disabled after re-auth failure")
		}
		// healthz must report UP disabled
		server, _ := NewBridgeServer(rb, "test-token", "live")
		for _, path := range []string{"/healthz", "/readyz"} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			server.Handler().ServeHTTP(rec, req)
			var body map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &body)
			if body["status"] != "UP disabled" {
				t.Fatalf("%s status=%v want UP disabled", path, body["status"])
			}
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s code=%d want 503", path, rec.Code)
			}
		}
	})

	t.Run("second_401_after_reauth_also_disabled", func(t *testing.T) {
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
		}}
		reauthCalls := 0
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			reauthCalls++
			return nil // re-auth succeeds but retry still 401
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Reason != "broker_disabled" {
			t.Fatalf("second 401 should become broker_disabled: %+v", result)
		}
		if reauthCalls != 1 {
			t.Fatalf("reauthCalls=%d want 1 (no second retry)", reauthCalls)
		}
		if !rb.IsDisabled() {
			t.Fatal("should be disabled after retry still returns 401")
		}
	})

	t.Run("non_auth_error_no_reauth", func(t *testing.T) {
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			return BrokerResult{Outcome: OutcomeRejected, Reason: "bad quantity"}
		}}
		reauthCalls := 0
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			reauthCalls++
			return nil
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Outcome != OutcomeRejected {
			t.Fatalf("should pass through rejected: %+v", result)
		}
		if reauthCalls != 0 {
			t.Fatalf("reauthCalls=%d want 0 for non-auth error", reauthCalls)
		}
	})
}

type countingBroker struct {
	fn func(context.Context, CommandEnvelope) BrokerResult
}

func (c *countingBroker) Place(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) Modify(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) Cancel(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) QueryOrder(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) ReconcileOrders(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) ReconcileTrades(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) ReconcilePositions(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
