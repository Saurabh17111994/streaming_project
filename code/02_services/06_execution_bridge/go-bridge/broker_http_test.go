package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

func TestArrowBrokerUsesPinnedSDKForPlaceAndPreservesReference(t *testing.T) {
	var got arrow.OrderRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/order/regular" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("appId") != "app" || r.Header.Get("token") != "token" {
			t.Fatalf("missing Arrow auth headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"orderNo":"BRK-1","requestTime":"now"}}`))
	}))
	defer server.Close()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(t.Context(), validPlaceCommand())
	if result.Outcome != OutcomeSuccess || result.BrokerOrderID != "BRK-1" {
		t.Fatalf("result=%+v", result)
	}
	if got.Remarks != validPlaceCommand().ClientOrderRef || got.TransactionType != "B" || got.Price != "15050" {
		t.Fatalf("Arrow request mapping=%+v", got)
	}
}

func TestArrowBrokerTreatsMalformedSuccessAsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{}}`))
	}))
	defer server.Close()
	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(t.Context(), validPlaceCommand())
	if result.Outcome != OutcomeUnknown || result.Reason != "malformed_success_response" {
		t.Fatalf("result=%+v", result)
	}
}

func TestArrowBrokerRejectsDocumentedHTTPRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":"error","message":"bad quantity"}`))
	}))
	defer server.Close()
	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(t.Context(), validPlaceCommand())
	if result.Outcome != OutcomeRejected || result.Reason != "broker_error" {
		t.Fatalf("result=%+v", result)
	}
}

func TestNoCredentialsAppearInSanitizedReasons(t *testing.T) {
	reason := sanitizeReason(errors.New("token=secret-app-token"))
	if strings.Contains(reason, "secret-app-token") {
		t.Fatalf("secret leaked in reason %q", reason)
	}
}
