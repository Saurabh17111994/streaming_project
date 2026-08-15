package main

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

// noopLogf matches the logf signature used by hftPin/hftRange.
var noopLogf = func(string, ...any) {}

// hftPolicy describes one ARROW_HFT_* key's validation contract. It is the
// single source of truth for this test file and mirrors ConfigParityTest.java
// (ING-UNIT-018): pinned keys must equal `pin` exactly; tunable keys must be
// within [min, max] with default `def`. Every ARROW_HFT_* key read by Java's
// IngestionConfig exactInt/intRange must appear here with identical bounds.
type hftPolicy struct {
	key string
	pin int // pinned value; -1 marks a tunable (range) key
	def int
	min int
	max int
}

var hftPolicyTable = []hftPolicy{
	{"ARROW_HFT_CONNECTIONS", 1, 1, 1, 1},
	{"ARROW_HFT_LATENCY_MS", -1, 50, 50, 60_000},
	{"ARROW_HFT_MAX_TOKENS_PER_CONNECTION", 1024, 1024, 1024, 1024},
	{"ARROW_HFT_MAX_TOKENS_PER_REQUEST", 512, 512, 512, 512},
	{"ARROW_HFT_HEARTBEAT_SECONDS", 3, 3, 3, 3},
	{"ARROW_HFT_STALL_TIMEOUT_SECONDS", -1, 15, 5, 60},
	{"ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", -1, 10, 1, 60},
	{"ARROW_HFT_RECONNECT_BASE_SECONDS", 1, 1, 1, 1},
	{"ARROW_HFT_RECONNECT_MAX_SECONDS", 30, 30, 30, 30},
	{"ARROW_HFT_AUTH_REFRESH_ATTEMPTS", 3, 3, 3, 3},
	{"ARROW_HFT_MIN_ACTIVE_SLOTS", 1, 1, 1, 1},
}

// readPolicy dispatches one table entry through the same hftPin/hftRange
// helpers main() uses, so the test exercises the production validation path.
func readPolicy(key string) int {
	for _, p := range hftPolicyTable {
		if p.key != key {
			continue
		}
		if p.pin >= 0 {
			return hftPin(noopLogf, key, p.pin)
		}
		return hftRange(noopLogf, key, p.def, p.min, p.max)
	}
	panic("hft policy table missing key " + key)
}

func policyDefault(p hftPolicy) int {
	if p.pin >= 0 {
		return p.pin
	}
	return p.def
}

// acceptedValue is a value every validator of this shape must accept: the pin
// itself for pinned keys, a mid-range value for tunables.
func acceptedValue(p hftPolicy) int {
	if p.pin >= 0 {
		return p.pin
	}
	return (p.min + p.max) / 2
}

// rejectedValue is a value every validator of this shape must reject: pin+1
// for pinned keys, below-range for tunables.
func rejectedValue(p hftPolicy) int {
	if p.pin >= 0 {
		return p.pin + 1
	}
	return p.min - 1
}

func TestHFTPolicyParityDefaults(t *testing.T) {
	// Unset keys must fall back to the documented defaults, matching Java's
	// exactInt/intRange behavior on missing values.
	for _, k := range []string{
		"ARROW_HFT_CONNECTIONS",
		"ARROW_HFT_LATENCY_MS",
		"ARROW_HFT_MAX_TOKENS_PER_CONNECTION",
		"ARROW_HFT_MAX_TOKENS_PER_REQUEST",
		"ARROW_HFT_HEARTBEAT_SECONDS",
		"ARROW_HFT_STALL_TIMEOUT_SECONDS",
		"ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS",
		"ARROW_HFT_RECONNECT_BASE_SECONDS",
		"ARROW_HFT_RECONNECT_MAX_SECONDS",
		"ARROW_HFT_AUTH_REFRESH_ATTEMPTS",
		"ARROW_HFT_MIN_ACTIVE_SLOTS",
	} {
		t.Setenv(k, "")
	}
	for _, p := range hftPolicyTable {
		if got := readPolicy(p.key); got != policyDefault(p) {
			t.Errorf("%s default = %d, want %d", p.key, got, policyDefault(p))
		}
	}
	if heartbeatInterval != 3*time.Second || stallTimeout != 15*time.Second {
		t.Errorf("package defaults changed: heartbeat=%s stall=%s",
			heartbeatInterval, stallTimeout)
	}
}

func TestHFTPolicyParityAccepts(t *testing.T) {
	// Every accepted value (pin for pinned, in-range for tunables) must be
	// accepted and returned — the Java mirror table asserts the same values.
	for _, p := range hftPolicyTable {
		t.Setenv(p.key, "")
		accepted := acceptedValue(p)
		t.Setenv(p.key, strconv.Itoa(accepted))
		if got := readPolicy(p.key); got != accepted {
			t.Errorf("%s = %d: got %d, want %d", p.key, accepted, got, accepted)
		}
	}
}

// TestHFTPolicyParityRejects proves a rejected value exits FATAL (status 2)
// via the exec-self subprocess pattern — os.Exit cannot be tested in-process.
// One subprocess per rejected value: a non-pin / out-of-range value for every
// key, plus one non-integer probe per validator shape (the Atoi path is
// identical across keys).
func TestHFTPolicyParityRejects(t *testing.T) {
	if os.Getenv("GO_HFT_HELPER") == "1" {
		key := os.Getenv("GO_HFT_KEY")
		os.Setenv(key, os.Getenv("GO_HFT_VALUE"))
		readPolicy(key)
		return
	}
	cases := []struct{ key, value string }{
		// One rejected value per key (pin+1 / below-range).
		{"ARROW_HFT_CONNECTIONS", "2"},
		{"ARROW_HFT_LATENCY_MS", "49"},
		{"ARROW_HFT_MAX_TOKENS_PER_CONNECTION", "1025"},
		{"ARROW_HFT_MAX_TOKENS_PER_REQUEST", "513"},
		{"ARROW_HFT_HEARTBEAT_SECONDS", "4"},
		{"ARROW_HFT_STALL_TIMEOUT_SECONDS", "4"},
		{"ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", "0"},
		{"ARROW_HFT_RECONNECT_BASE_SECONDS", "2"},
		{"ARROW_HFT_RECONNECT_MAX_SECONDS", "31"},
		{"ARROW_HFT_AUTH_REFRESH_ATTEMPTS", "4"},
		{"ARROW_HFT_MIN_ACTIVE_SLOTS", "2"},
		// Non-integer: one pinned key and one tunable (same Atoi path).
		{"ARROW_HFT_CONNECTIONS", "abc"},
		{"ARROW_HFT_LATENCY_MS", "abc"},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.value, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=TestHFTPolicyParityRejects")
			cmd.Env = append(os.Environ(), "GO_HFT_HELPER=1",
				"GO_HFT_KEY="+c.key, "GO_HFT_VALUE="+c.value)
			out, err := cmd.CombinedOutput()
			ee, ok := err.(*exec.ExitError)
			if !ok || ee.ExitCode() != 2 {
				t.Fatalf("%s=%s: exit=%v want status 2; output=%s",
					c.key, c.value, err, out)
			}
		})
	}
}

