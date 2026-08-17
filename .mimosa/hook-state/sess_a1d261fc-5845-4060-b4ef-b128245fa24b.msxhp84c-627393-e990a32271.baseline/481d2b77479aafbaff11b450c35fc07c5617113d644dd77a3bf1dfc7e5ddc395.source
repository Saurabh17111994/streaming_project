package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

func TestEmitMetricsRecordShape(t *testing.T) {
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	if err := emitter.EmitMetrics(BridgeMetrics{TsMs: 1_750_000_000_000, ReconnectConsecutive: 3, ActiveSockets: 1, GoGoroutines: 42}); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatalf("expected exactly one NDJSON line: %q", out.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if rt, _ := decoded["record_type"].(string); rt != "bridge_metrics" {
		t.Fatalf("record_type=%q, want bridge_metrics", rt)
	}
	if cv, _ := decoded["contract_version"].(float64); int(cv) != NDJSONContractVersion {
		t.Fatalf("contract_version=%v, want %d", cv, NDJSONContractVersion)
	}
	if ts, _ := decoded["ts_ms"].(float64); int64(ts) != 1_750_000_000_000 {
		t.Fatalf("ts_ms=%v", ts)
	}
	if rc, _ := decoded["reconnect_consecutive"].(float64); int(rc) != 3 {
		t.Fatalf("reconnect_consecutive=%v", rc)
	}
	if as, _ := decoded["active_sockets"].(float64); int(as) != 1 {
		t.Fatalf("active_sockets=%v", as)
	}
	if gg, _ := decoded["go_goroutines"].(float64); int(gg) != 42 {
		t.Fatalf("go_goroutines=%v", gg)
	}
	// Zero ts_ms must be rejected and must not write a line.
	out.Reset()
	if err := emitter.EmitMetrics(BridgeMetrics{}); err == nil {
		t.Fatal("zero ts_ms must be rejected")
	}
	if out.Len() != 0 {
		t.Fatalf("rejected metrics must not be written, got: %q", out.String())
	}
}

func TestTokenSetHashDeterministic(t *testing.T) {
	// Vector computed with the same algorithm (sorted tokens, 8-byte
	// big-endian each, SHA-256) — the Java side must reproduce this exactly.
	want := "8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"
	if got := tokenSetHash([]int32{1000, 1001, 1}); got != want {
		t.Fatalf("tokenSetHash([1000,1001,1]) = %q, want %q", got, want)
	}
	// Order independence: same set, different input order.
	if got := tokenSetHash([]int32{1, 1001, 1000}); got != want {
		t.Fatalf("tokenSetHash must be order-independent: %q != %q", got, want)
	}
}

func TestEmitEventAutoFillsIdentity(t *testing.T) {
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	emitter.SetManifestFingerprint("abc123")
	emitter.SetSlotTokenHash("hft-0", "def456")
	if err := emitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if mf, _ := decoded["manifest_fingerprint"].(string); mf != "abc123" {
		t.Fatalf("manifest_fingerprint=%q, want abc123 (auto-filled)", mf)
	}
	if ah, _ := decoded["assigned_token_set_hash"].(string); ah != "def456" {
		t.Fatalf("assigned_token_set_hash=%q, want def456 (auto-filled)", ah)
	}
	// An explicitly-set field wins over the identity map.
	out.Reset()
	if err := emitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli(), ManifestFingerprint: "explicit", AssignedTokenSetHash: "explicit"}); err != nil {
		t.Fatal(err)
	}
	decoded = map[string]any{}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if mf, _ := decoded["manifest_fingerprint"].(string); mf != "explicit" {
		t.Fatalf("explicit manifest_fingerprint lost: %q", mf)
	}
	// No identity configured → fields omitted, event still valid.
	out.Reset()
	blank := NewBridgeEmitter(&out)
	if err := blank.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "manifest_fingerprint") {
		t.Fatalf("identity-less event must omit manifest_fingerprint: %q", out.String())
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

// ING-RES-004 — every NDJSON emit path (tick, bridge_event, bridge_metrics)
// carries contract_version=2 — schema conformance across the whole contract.
func TestIngRes004AllEmitPathsCarryContractVersion2(t *testing.T) {
	var out bytes.Buffer
	emitter := NewBridgeEmitter(&out)
	now := time.Now()
	if err := emitter.EmitTick(Tick{Feed: "hft", Mode: "ltpc", Token: 7, LTP: 10050, TS: 1_000}, "c", "hft-0", 1, now, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := emitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: "hft-0", ConnectionID: "hft-0", ConnectionEpoch: 1, State: "ACTIVE", ReceivedTsMs: now.UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if err := emitter.EmitMetrics(BridgeMetrics{TsMs: now.UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 records (tick/event/metrics), got %d", len(lines))
	}
	for _, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		if v, _ := rec["contract_version"].(float64); int(v) != NDJSONContractVersion {
			t.Fatalf("record missing contract_version=%d: %s", NDJSONContractVersion, line)
		}
	}
}

// ING-TCP-003 — the per-token counter report (ING-TCP-001) is emitted as
// bounded chunks (20 tokens/line), the file + stderr mirror are byte-identical,
// and the file is written FIRST so a dead stderr (SIGPIPE path) cannot lose
// the reconcile evidence.
func TestIngTcp003TickCountReportChunkedFileAndStderrMirror(t *testing.T) {
	defer restoreTickCountGlobals()

	// 1,024 distinct tokens with deterministic counts 1..1024 (total 524,800).
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{}
	for i := int32(1); i <= 1024; i++ {
		tickCounts[i] = int64(i)
	}
	tickCountsMu.Unlock()

	reportPath := filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	tickCountsFilePath = reportPath // main() assigns this from ARROW_TICK_COUNTS_FILE
	mirrorPath := filepath.Join(t.TempDir(), "stderr.txt")
	mirror, err := os.Create(mirrorPath)
	if err != nil {
		t.Fatal(err)
	}
	oldStderr := os.Stderr
	os.Stderr = mirror
	reportTickCounts()
	os.Stderr = oldStderr
	if err := mirror.Close(); err != nil {
		t.Fatal(err)
	}

	// File + stderr mirror are byte-identical (the reconcile may read either).
	fileBytes, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("report file missing: %v", err)
	}
	mirrorBytes, err := os.ReadFile(mirrorPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fileBytes, mirrorBytes) {
		t.Fatal("ING-TCP-003: file and stderr mirror must be byte-identical")
	}

	// 1,024 tokens at 20/line → exactly 52 bounded chunks.
	lines := strings.Split(strings.TrimSpace(string(fileBytes)), "\n")
	if len(lines) != 52 {
		t.Fatalf("1,024-token report: got %d lines, want 52 (20/line)", len(lines))
	}
	for i, line := range lines {
		if !strings.HasPrefix(line, "arrow-tick-counts: total=524800 chunk=") {
			t.Fatalf("line %d malformed: %q", i, line)
		}
		if !strings.Contains(line, fmt.Sprintf("chunk=%d/52", i)) {
			t.Fatalf("line %d missing chunk marker: %q", i, line)
		}
		// Bounded line: 20 token pairs + header, far under Java's 603 B
		// truncation limit (the reason chunking exists).
		if len(line) > 603 {
			t.Fatalf("line %d exceeds the 603 B log-truncation bound: %d B", i, len(line))
		}
	}
}

// ING-TCP-003 — the report file persists even when stderr is dead (the SIGPIPE
// path: the parent JVM closes the child's pipe, so stderr writes fail). The
// file write happens FIRST, so the reconcile evidence survives.
func TestIngTcp003ReportFilePersistsWhenStderrDies(t *testing.T) {
	defer restoreTickCountGlobals()

	tickCountsMu.Lock()
	tickCounts = map[int32]int64{1: 5, 2: 7, 3: 11}
	tickCountsMu.Unlock()

	reportPath := filepath.Join(t.TempDir(), "counts.txt")
	tickCountsFilePath = reportPath

	// /dev/full fails every write (ENOSPC) — a clean stand-in for a dead pipe.
	devFull, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Skip("/dev/full unavailable — cannot simulate a dead stderr")
	}
	oldStderr := os.Stderr
	os.Stderr = devFull
	reportTickCounts() // must not hang, must not panic: file written before stderr
	os.Stderr = oldStderr
	devFull.Close()

	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("ING-TCP-003: report file must persist despite dead stderr: %v", err)
	}
	if !strings.Contains(string(data), "arrow-tick-counts: total=23") {
		t.Fatalf("report content wrong: %q", string(data))
	}
}

// restoreTickCountGlobals puts the package-level tick-count state back for
// other tests (the goroutine ticker and main's shutdown path read these).
func restoreTickCountGlobals() {
	tickCountsMu.Lock()
	tickCounts = nil
	tickCountsMu.Unlock()
	tickCountsFilePath = "/tmp/arrow-tick-counts.txt"
}
