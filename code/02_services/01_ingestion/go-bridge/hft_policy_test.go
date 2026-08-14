package main

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// noopLogf matches the logf signature used by hftPin/hftRange.
var noopLogf = func(string, ...any) {}

func TestHFTPolicyDefaults(t *testing.T) {
	// Unset keys must fall back to the pinned/default values, and the
	// package vars must match the documented defaults.
	for _, k := range []string{
		"ARROW_HFT_MAX_TOKENS_PER_CONNECTION",
		"ARROW_HFT_MAX_TOKENS_PER_REQUEST",
		"ARROW_HFT_HEARTBEAT_SECONDS",
		"ARROW_HFT_STALL_TIMEOUT_SECONDS",
		"ARROW_HFT_RECONNECT_BASE_SECONDS",
		"ARROW_HFT_RECONNECT_MAX_SECONDS",
		"ARROW_HFT_AUTH_REFRESH_ATTEMPTS",
		"ARROW_HFT_MIN_ACTIVE_SLOTS",
	} {
		t.Setenv(k, "")
	}
	if got := hftPin(noopLogf, "ARROW_HFT_MAX_TOKENS_PER_CONNECTION", 1024); got != 1024 {
		t.Fatalf("MAX_TOKENS_PER_CONNECTION default = %d, want 1024", got)
	}
	if got := hftPin(noopLogf, "ARROW_HFT_MAX_TOKENS_PER_REQUEST", 512); got != 512 {
		t.Fatalf("MAX_TOKENS_PER_REQUEST default = %d, want 512", got)
	}
	if got := hftPin(noopLogf, "ARROW_HFT_HEARTBEAT_SECONDS", 3); got != 3 {
		t.Fatalf("HEARTBEAT_SECONDS default = %d, want 3", got)
	}
	if got := hftRange(noopLogf, "ARROW_HFT_STALL_TIMEOUT_SECONDS", 15, 5, 60); got != 15 {
		t.Fatalf("STALL_TIMEOUT_SECONDS default = %d, want 15", got)
	}
	if got := hftRange(noopLogf, "ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", 10, 1, 60); got != 10 {
		t.Fatalf("SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS default = %d, want 10", got)
	}
	if got := hftPin(noopLogf, "ARROW_HFT_RECONNECT_BASE_SECONDS", 1); got != 1 {
		t.Fatalf("RECONNECT_BASE_SECONDS default = %d, want 1", got)
	}
	if got := hftPin(noopLogf, "ARROW_HFT_RECONNECT_MAX_SECONDS", 30); got != 30 {
		t.Fatalf("RECONNECT_MAX_SECONDS default = %d, want 30", got)
	}
	if got := hftPin(noopLogf, "ARROW_HFT_AUTH_REFRESH_ATTEMPTS", 3); got != 3 {
		t.Fatalf("AUTH_REFRESH_ATTEMPTS default = %d, want 3", got)
	}
	if got := hftPin(noopLogf, "ARROW_HFT_MIN_ACTIVE_SLOTS", 1); got != 1 {
		t.Fatalf("MIN_ACTIVE_SLOTS default = %d, want 1", got)
	}
	if heartbeatInterval != 3*time.Second || stallTimeout != 15*time.Second {
		t.Fatalf("package defaults changed: heartbeat=%s stall=%s", heartbeatInterval, stallTimeout)
	}
}

func TestHFTPolicyAcceptsPinAndRange(t *testing.T) {
	t.Setenv("ARROW_HFT_MAX_TOKENS_PER_CONNECTION", "1024")
	t.Setenv("ARROW_HFT_STALL_TIMEOUT_SECONDS", "30")
	t.Setenv("ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", "45")
	if got := hftPin(noopLogf, "ARROW_HFT_MAX_TOKENS_PER_CONNECTION", 1024); got != 1024 {
		t.Fatalf("pin value not accepted: got %d", got)
	}
	if got := hftRange(noopLogf, "ARROW_HFT_STALL_TIMEOUT_SECONDS", 15, 5, 60); got != 30 {
		t.Fatalf("range value not accepted: got %d", got)
	}
	if got := hftRange(noopLogf, "ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", 10, 1, 60); got != 45 {
		t.Fatalf("range value not accepted: got %d", got)
	}
}

// TestHFTPinFatalOnWrongValue proves a value other than the pin exits FATAL
// (status 2) via the exec-self subprocess pattern — os.Exit cannot be tested
// in-process.
func TestHFTPinFatalOnWrongValue(t *testing.T) {
	if os.Getenv("GO_WANT_HFT_HELPER") == "1" {
		hftPin(noopLogf, "ARROW_HFT_HEARTBEAT_SECONDS", 3)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestHFTPinFatalOnWrongValue")
	cmd.Env = append(os.Environ(), "GO_WANT_HFT_HELPER=1", "ARROW_HFT_HEARTBEAT_SECONDS=9")
	out, err := cmd.CombinedOutput()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != 2 {
		t.Fatalf("wrong pin value: exit=%v want status 2; output=%s", err, out)
	}
}

// TestHFTRangeFatalOutOfBounds proves an out-of-range tunable exits FATAL.
func TestHFTRangeFatalOutOfBounds(t *testing.T) {
	if os.Getenv("GO_WANT_HFT_HELPER") == "1" {
		hftRange(noopLogf, "ARROW_HFT_STALL_TIMEOUT_SECONDS", 15, 5, 60)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestHFTRangeFatalOutOfBounds")
	cmd.Env = append(os.Environ(), "GO_WANT_HFT_HELPER=1", "ARROW_HFT_STALL_TIMEOUT_SECONDS=120")
	out, err := cmd.CombinedOutput()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != 2 {
		t.Fatalf("out-of-range value: exit=%v want status 2; output=%s", err, out)
	}
}
