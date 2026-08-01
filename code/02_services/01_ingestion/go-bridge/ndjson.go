package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
)

const NDJSONContractVersion = 2

const (
	EventSlotState       = "slot_state"
	EventSubscriptionAck = "subscription_ack"
	EventHeartbeatFailed = "heartbeat_failed"
	EventFeedStalled     = "feed_stalled"
	EventDisconnect      = "disconnect"
	EventReconnect       = "reconnect"
	EventAuthFailure     = "auth_failure"
	EventBridgeShutdown  = "bridge_shutdown"
)

var bridgeEvents = map[string]bool{
	EventSlotState: true, EventSubscriptionAck: true, EventHeartbeatFailed: true,
	EventFeedStalled: true, EventDisconnect: true, EventReconnect: true,
	EventAuthFailure: true, EventBridgeShutdown: true,
}

type BridgeEvent struct {
	RecordType         string `json:"record_type"`
	ContractVersion    int    `json:"contract_version"`
	Event              string `json:"event"`
	SlotID             string `json:"slot_id"`
	ConnectionID       string `json:"connection_id"`
	ConnectionEpoch    uint64 `json:"connection_epoch"`
	State              string `json:"state"`
	AssignedTokens     int    `json:"assigned_tokens,omitempty"`
	AcknowledgedTokens int    `json:"acknowledged_tokens,omitempty"`
	RejectedTokens     int    `json:"rejected_tokens,omitempty"`
	Reason             string `json:"reason,omitempty"`
	ReceivedTsMs       int64  `json:"received_ts_ms"`
}

type BridgeEmitter struct {
	mu        sync.Mutex
	w         io.Writer
	seqBySlot map[string]uint64
	seqMu     sync.Mutex
}

func NewBridgeEmitter(w io.Writer) *BridgeEmitter {
	return &BridgeEmitter{w: w, seqBySlot: map[string]uint64{}}
}

// nextSeq returns the next monotonic per-slot tick sequence (starts at 1).
// The counter is keyed by slot id and is independent per slot; it resets
// when the process restarts (a new connection epoch begins).
func (e *BridgeEmitter) nextSeq(slotID string) uint64 {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()
	e.seqBySlot[slotID]++
	return e.seqBySlot[slotID]
}

func (e *BridgeEmitter) EmitTick(t Tick, connectionID, slotID string, epoch uint64, received time.Time, rawPayload []byte) error {
	value := struct {
		Tick
		RecordType        string `json:"record_type"`
		ContractVersion   int    `json:"contract_version"`
		ConnectionID      string `json:"connection_id"`
		ConnectionEpoch   uint64 `json:"connection_epoch"`
		SlotID            string `json:"slot_id"`
		FeedSequenceLocal uint64 `json:"feed_sequence_local"`
		ReceivedTsMs      int64  `json:"received_ts_ms"`
		RawPayload        string `json:"raw_payload,omitempty"`
		PayloadHash       string `json:"payload_hash,omitempty"`
	}{
		Tick: t, RecordType: "tick", ContractVersion: NDJSONContractVersion,
		ConnectionID: connectionID, ConnectionEpoch: epoch, SlotID: slotID,
		FeedSequenceLocal: e.nextSeq(slotID),
		ReceivedTsMs:      received.UnixMilli(),
		RawPayload:        base64.StdEncoding.EncodeToString(rawPayload),
		PayloadHash:       sha256Hex(rawPayload),
	}
	return e.write(value)
}

// sha256Hex returns the lowercase SHA-256 hex digest of b.
func sha256Hex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (e *BridgeEmitter) EmitEvent(event BridgeEvent) error {
	event.RecordType = "bridge_event"
	event.ContractVersion = NDJSONContractVersion
	event.Reason = sanitizeDiagnostic(event.Reason)
	return e.write(event)
}

func (e *BridgeEmitter) write(value any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err = e.w.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// secretPattern scrubs values after secret-bearing names.
//
// Two-pass design (ING-SEC-RED-001): the Bearer pattern runs FIRST so that
// `Bearer <token>` (space-separated) is consumed before the name=value pattern
// can eat only the literal `Bearer` and leave the real token exposed (e.g.
// `Authorization=Bearer secretToken`).
var secretPattern = regexp.MustCompile(`(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|access_token|authorization|appID|token)([=:][^&\s,}]+)`)
var bearerPattern = regexp.MustCompile(`(?i)\bBearer[=:\s]+[^\s,}]+`)

func sanitizeDiagnostic(s string) string {
	s = bearerPattern.ReplaceAllString(s, "Bearer=[REDACTED]")
	s = secretPattern.ReplaceAllString(s, "$1=[REDACTED]")
	if len(s) > 512 {
		s = s[:512]
	}
	return strings.TrimSpace(s)
}

func validateBridgeEvent(event BridgeEvent) error {
	if event.RecordType != "bridge_event" {
		return fmt.Errorf("invalid record_type")
	}
	if event.ContractVersion != NDJSONContractVersion {
		return fmt.Errorf("unsupported contract_version %d", event.ContractVersion)
	}
	if event.SlotID == "" || event.ConnectionID == "" {
		return fmt.Errorf("slot_id and connection_id are required")
	}
	if event.State == "" {
		return fmt.Errorf("state is required")
	}
	if !bridgeEvents[event.Event] {
		return fmt.Errorf("unknown event %q", event.Event)
	}
	if event.ConnectionEpoch == 0 {
		return fmt.Errorf("connection_epoch must be positive")
	}
	if event.ReceivedTsMs <= 0 {
		return fmt.Errorf("received_ts_ms must be positive")
	}
	if event.AssignedTokens < 0 || event.AcknowledgedTokens < 0 || event.RejectedTokens < 0 {
		return fmt.Errorf("token counts cannot be negative")
	}
	return nil
}
