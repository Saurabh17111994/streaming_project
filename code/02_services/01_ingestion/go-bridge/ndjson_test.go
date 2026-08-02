package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestEmitterTickCarriesRawPayloadAndHash(t *testing.T) {
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	// Simulated decompressed broker packet bytes (LTP frame shape: 40 bytes).
	packet := make([]byte, 40)
	packet[0], packet[1] = 40, 0
	packet[2] = 1 // hftPktLTP
	packet[4] = 0x04
	packet[5] = 0xD2 // token 1234
	if err := emitter.EmitTick(Tick{Feed: "hft", Mode: "ltpc", Token: 1234, LTP: 10050, TS: 1_000}, "ingestion-local/hft-0", "hft-0", 1, time.Now(), packet); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatalf("expected one line: %q", out.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	rawB64, _ := decoded["raw_payload"].(string)
	hash, _ := decoded["payload_hash"].(string)
	if rawB64 == "" || hash == "" {
		t.Fatalf("raw_payload/payload_hash missing: %s", out.String())
	}
	rawBytes, err := base64.StdEncoding.DecodeString(rawB64)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(rawBytes)
	if hex.EncodeToString(sum[:]) != hash {
		t.Fatalf("payload_hash does not match raw_payload bytes")
	}
	if !bytes.Equal(rawBytes, packet) {
		t.Fatal("raw_payload bytes do not equal the original packet")
	}
}

func TestEmitterOneLineAndRedaction(t *testing.T) {
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	if err := emitter.EmitEvent(BridgeEvent{Event: "auth_failure", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "AUTH_FAILED", Reason: "ARROW_TOKEN=secret", ReceivedTsMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatalf("expected one line: %q", out.String())
	}
	if strings.Contains(out.String(), "secret") {
		t.Fatal("secret leaked")
	}
	if len(sanitizeDiagnostic(strings.Repeat("x", 600))) != 512 {
		t.Fatal("diagnostic not bounded")
	}
}

func TestEmitterValidatesEventsAtSource(t *testing.T) {
	// R-097: EmitEvent must reject invalid events instead of writing them.
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	// Unknown event name → rejected.
	if err := emitter.EmitEvent(BridgeEvent{Event: "not_a_real_event", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()}); err == nil {
		t.Fatal("unknown event must be rejected (R-097)")
	}
	// Missing received_ts_ms → rejected.
	if err := emitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE"}); err == nil {
		t.Fatal("zero received_ts_ms must be rejected (R-097)")
	}
	// Nothing was written.
	if out.Len() != 0 {
		t.Fatalf("invalid events must not be written, got: %q", out.String())
	}
	// A valid event still passes.
	if err := emitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()}); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
}

func TestEmitterEmptyPayloadStillCarriesHash(t *testing.T) {
	// R-186: an empty raw payload must still carry its real SHA-256 digest.
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	if err := emitter.EmitTick(Tick{Feed: "hft", Mode: "ltpc", Token: 1, TS: 1_000}, "c", "hft-0", 1, time.Now(), nil); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	hash, _ := decoded["payload_hash"].(string)
	// sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	if hash != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatalf("empty payload must carry sha256(empty) digest, got %q (R-186)", hash)
	}
}

func TestSanitizeDiagnosticPreservesUTF8(t *testing.T) {
	// R-187: truncation must not split a multi-byte rune.
	long := strings.Repeat("₹", 400) // 400 × 3 bytes = 1200 bytes
	s := sanitizeDiagnostic(long)
	if !strings.HasPrefix(s, "₹₹") {
		t.Fatal("prefix lost")
	}
	// The result must be valid UTF-8 (no replacement chars from a split rune).
	if !utf8.ValidString(s) {
		t.Fatal("truncation split a rune (R-187)")
	}
	if len(s) > 512*3 {
		t.Fatalf("not bounded: %d bytes", len(s))
	}
}

func TestEmitterFeedSequenceLocal(t *testing.T) {
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	packet := make([]byte, 40)
	// Emit two ticks for slot hft-0 and one for hft-1.
	for i := 0; i < 2; i++ {
		if err := emitter.EmitTick(Tick{Feed: "hft", Mode: "full", Token: int32(1000 + i), TS: 1_000}, "ingestion-local/hft-0", "hft-0", 1, time.Now(), packet); err != nil {
			t.Fatal(err)
		}
	}
	if err := emitter.EmitTick(Tick{Feed: "hft", Mode: "full", Token: 2000, TS: 1_000}, "ingestion-local/hft-1", "hft-1", 1, time.Now(), packet); err != nil {
		t.Fatal(err)
	}
	var seqs0, seqs1 []uint64
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatal(err)
		}
		slot, _ := decoded["slot_id"].(string)
		seq, _ := decoded["feed_sequence_local"].(float64)
		if slot == "hft-0" {
			seqs0 = append(seqs0, uint64(seq))
		} else {
			seqs1 = append(seqs1, uint64(seq))
		}
	}
	if len(seqs0) != 2 || seqs0[0] != 1 || seqs0[1] != 2 {
		t.Fatalf("hft-0 sequence = %v, want [1 2]", seqs0)
	}
	if len(seqs1) != 1 || seqs1[0] != 1 {
		t.Fatalf("hft-1 sequence = %v, want [1]", seqs1)
	}
}

func TestBridgeEventValidation(t *testing.T) {
	if err := validateBridgeEvent(BridgeEvent{RecordType: "bridge_event", ContractVersion: NDJSONContractVersion, Event: EventSlotState, SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if err := validateBridgeEvent(BridgeEvent{RecordType: "bridge_event", ContractVersion: 1, Event: EventSlotState, SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()}); err == nil {
		t.Fatal("expected version rejection")
	}
}

func TestBridgeEventRejectsUnknownEventAndNegativeCounts(t *testing.T) {
	base := BridgeEvent{RecordType: "bridge_event", ContractVersion: NDJSONContractVersion, SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()}
	base.Event = "unknown"
	if err := validateBridgeEvent(base); err == nil {
		t.Fatal("expected unknown event rejection")
	}
	base.Event = EventSubscriptionAck
	base.AcknowledgedTokens = -1
	if err := validateBridgeEvent(base); err == nil {
		t.Fatal("expected negative count rejection")
	}
}

func TestEmitterConcurrentOutputRemainsLineDelimited(t *testing.T) {
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	done := make(chan struct{}, 32)
	for i := 0; i < 32; i++ {
		go func() {
			_ = emitter.EmitEvent(BridgeEvent{Event: EventSlotState, SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()})
			done <- struct{}{}
		}()
	}
	for i := 0; i < 32; i++ {
		<-done
	}
	if strings.Count(out.String(), "\n") != 32 {
		t.Fatalf("lines=%d, want 32", strings.Count(out.String(), "\n"))
	}
}

// ING-SEC-RED-001 — inject every mandated secret class into SDK-style errors
// and assert none survive the Go diagnostic sanitizer.
func TestIngSecRed001SecretRedaction(t *testing.T) {
	secrets := []string{
		"ARROW_APP_SECRET=superSecretAppSecret123",
		"ARROW_PASSWORD=P@ssw0rd!secret",
		"ARROW_TOTP_KEY=JBSWY3DPEHPK3PXP",
		"ARROW_TOKEN=eyJhbGciOiJIUzI1NiJ9.secret",
		"access_token=ghp_secretToken456",
		"token=abcd1234secret",
		"appID=b3b40c832fcd",
		"Authorization=Bearer secretBearerToken",
		"https://socket.arrow.trade?appID=b3b40c832fcd&token=secretQueryToken",
		"KAEAAAAAAA==", // base64 raw payload
	}
	for _, s := range secrets {
		sanitized := sanitizeDiagnostic("connection failed: " + s)
		if strings.Contains(sanitized, "secret") && !strings.Contains(sanitized, "[REDACTED]") {
			t.Fatalf("ING-SEC-RED-001: secret leaked through sanitizer: %q -> %q", s, sanitized)
		}
	}
	// Values after secret-bearing names must be scrubbed, not the name itself.
	got := sanitizeDiagnostic("ARROW_TOKEN=realTokenValue extra")
	if strings.Contains(got, "realTokenValue") {
		t.Fatalf("ING-SEC-RED-001: token value leaked: %q", got)
	}
	// Output bounded to 512 chars.
	long := sanitizeDiagnostic("x: " + strings.Repeat("A", 1000))
	if len(long) > 512 {
		t.Fatalf("ING-SEC-RED-001: diagnostic not bounded (%d chars)", len(long))
	}
}
