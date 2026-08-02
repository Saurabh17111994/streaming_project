package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// Tick is the unified NDJSON output format consumed by the Java pipeline.
// All prices in integer paise (₹1 = 100 paise). Timestamps in epoch ms.
type Tick struct {
	Feed   string `json:"feed"` // "standard" or "hft"
	Mode   string `json:"mode"` // "ltp", "ltpc", "quote", "full"
	Token  int32  `json:"token"`
	LTP    int32  `json:"ltp_paise"`
	Close  int32  `json:"close_paise,omitempty"`
	Open   int32  `json:"open_paise,omitempty"`
	High   int32  `json:"high_paise,omitempty"`
	Low    int32  `json:"low_paise,omitempty"`
	VWAP   int32  `json:"vwap_paise,omitempty"`
	LTQ    int32  `json:"ltq,omitempty"`
	Volume int64  `json:"volume,omitempty"`
	TBQ    int64  `json:"total_buy_qty,omitempty"`
	TSQ    int64  `json:"total_sell_qty,omitempty"`
	ATV    uint32 `json:"atv,omitempty"`
	BTV    uint32 `json:"btv,omitempty"`
	OI     int64  `json:"open_interest,omitempty"`
	// unix epoch milliseconds
	TS      int64     `json:"ts_ms"`
	BidPx   [5]int32  `json:"bid_px,omitempty"`
	AskPx   [5]int32  `json:"ask_px,omitempty"`
	BidSize [5]int32  `json:"bid_qty,omitempty"`
	AskSize [5]int32  `json:"ask_qty,omitempty"`
	BidOrd  [5]uint16 `json:"bid_orders,omitempty"`
	AskOrd  [5]uint16 `json:"ask_orders,omitempty"`
	// standard-stream extras
	ChangeFlag int8  `json:"change_flag,omitempty"`
	AvgPrice   int32 `json:"avg_price_paise,omitempty"`
	LowerLimit int32 `json:"lower_limit_paise,omitempty"`
	UpperLimit int32 `json:"upper_limit_paise,omitempty"`
}

var bridgeEmitter = NewBridgeEmitter(os.Stdout)

// maxDecodeErrorsPer10s is the decode-error burst threshold per slot (plan
// §Error Handling): 100 errors in 10 seconds closes the slot and reconnects.
const maxDecodeErrorsPer10s = 100

// Process exit statuses per plan §main.go:
//
//	0 — requested shutdown
//	1 — unexpected supervisor failure (standard-mode connect error, stdout broken pipe)
//	2 — fatal auth/plan/config failure at startup
const (
	exitRequested  = 0
	exitSupervisor = 1
	exitFatalStart = 2
)

func main() {
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "arrow-bridge: "+format+"\n", args...)
	}

	appID := envOrFatal("ARROW_APP_ID")
	appSecret := envOrFatal("ARROW_APP_SECRET")
	userID := envOrDefault("ARROW_USER_ID", "")
	password := envOrDefault("ARROW_PASSWORD", "")
	totpKey := envOrDefault("ARROW_TOTP_KEY", "")

	client := arrow.NewClient(appID, appSecret)
	var refreshAuth func(context.Context) error

	if userID != "" && password != "" && totpKey != "" {
		refreshAuth = func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return client.AutoLogin(userID, password, totpKey)
		}
		logf("auto-login user=%s", userID)
		if err := client.AutoLogin(userID, password, totpKey); err != nil {
			fmt.Fprintf(os.Stderr, "arrow-bridge: AutoLogin failed: %v\n", err)
			// Fatal auth failure → status 2 (plan §main.go).
			os.Exit(exitFatalStart)
		}
	} else if token := os.Getenv("ARROW_TOKEN"); token != "" {
		client.SetToken(token)
		logf("using ARROW_TOKEN from env (len=%d)", len(token))
	} else {
		logf("no ARROW_TOKEN or autologin creds; cannot authenticate")
		// Fatal auth failure → status 2 (plan §main.go).
		os.Exit(exitFatalStart)
	}
	logf("authenticated (token_len=%d)", len(client.GetToken()))

	// instrument tokens — auto-extracted from Arrow broker CSV
	// Priority: 1) ARROW_INSTRUMENT_TOKENS env var, 2) CSV at
	// ARROW_INSTRUMENT_MANIFEST (fallback to approved 1,024 manifest),
	// 3) fail fast — no synthetic fallback in production (plan: startup MUST
	// reject rather than silently ingest fake tokens).
	csvPath := os.Getenv("ARROW_INSTRUMENT_MANIFEST")
	if csvPath == "" {
		csvPath = "/instruments/NSE_CM_EQUITY (1024).csv"
	}
	tokens := loadTokensFromCSV("ARROW_INSTRUMENT_TOKENS", csvPath)
	if len(tokens) == 0 {
		logf("FATAL: no instrument tokens (set ARROW_INSTRUMENT_TOKENS or ARROW_INSTRUMENT_MANIFEST CSV); refusing to start")
		os.Exit(exitFatalStart)
	}
	logf("%d instruments", len(tokens))
	slotCount := 1
	if v := os.Getenv("ARROW_HFT_CONNECTIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			slotCount = n
		}
	}
	if slotCount != 1 {
		logf("FATAL: production policy permits exactly one Arrow HFT socket; ARROW_HFT_CONNECTIONS=%d", slotCount)
		os.Exit(exitFatalStart)
	}
	plan, err := BuildSubscriptionPlan(tokens, slotCount, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		logf("FATAL: invalid subscription plan: %v", err)
		os.Exit(exitFatalStart)
	}
	logf("subscription plan=%s slots=%d", plan.Fingerprint, len(plan.Slots))

	useHFT := !truthy(os.Getenv("ARROW_USE_STANDARD"))
	latencyMs := 50
	if v := os.Getenv("ARROW_HFT_LATENCY_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 50 {
			latencyMs = n
		}
	}
	responseTimeout := 10 * time.Second
	if v := os.Getenv("ARROW_HFT_RESPONSE_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			responseTimeout = time.Duration(n) * time.Millisecond
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if useHFT {
		runHFT(ctx, cancel, client, plan, latencyMs, responseTimeout, refreshAuth, logf)
	} else {
		runStandard(ctx, cancel, client, tokens, logf)
	}
	// Drain point: all EmitTick/EmitEvent calls are synchronous and ordered
	// under the emitter mutex, so the bridge_shutdown event below is
	// guaranteed to be the last NDJSON line. Duplicate shutdown paths are
	// collapsed by bridgeShutdownOnce — the event is emitted exactly once.
	emitShutdownEvent()
}

// bridgeShutdownOnce collapses duplicate shutdown paths so the
// bridge_shutdown drain event is emitted exactly once per process.
// A pointer so tests can reset it between cases.
var bridgeShutdownOnce = &sync.Once{}

// emitShutdownEvent writes the terminal bridge_shutdown event (the drain
// marker). Safe to call multiple times from any goroutine.
func emitShutdownEvent() {
	bridgeShutdownOnce.Do(func() {
		_ = bridgeEmitter.EmitEvent(BridgeEvent{
			Event:           EventBridgeShutdown,
			SlotID:          "hft-0",
			ConnectionID:    "hft-0",
			ConnectionEpoch: 1,
			State:           string(SlotTerminal),
			Reason:          "drain_complete",
			ReceivedTsMs:    time.Now().UnixMilli(),
		})
	})
}

func runHFT(ctx context.Context, cancel context.CancelFunc, client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) {
	if len(plan.Slots) == 0 {
		cancel()
		return
	}
	// Production policy: exactly one Arrow HFT socket is approved for this
	// phase. Multi-slot is exercised by tests (runHFTSupervisor) and the
	// deferred multi-connection phase; here we refuse to start.
	if len(plan.Slots) != 1 {
		slot := plan.Slots[0]
		logf("FATAL: this deployment is approved for exactly one Arrow HFT socket; planned=%d", len(plan.Slots))
		// R-177: a deployment/policy violation is NOT a credential failure —
		// emit disconnect (non-auth vocabulary) so alerting keyed on
		// auth_failure does not fire for a non-credential condition.
		_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "disconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: 1, State: string(SlotTerminal), Reason: "single_socket_policy_violation", ReceivedTsMs: time.Now().UnixMilli()})
		cancel()
		return
	}
	runHFTSupervisor(ctx, cancel, client, plan, latencyMs, responseTimeout, refreshAuth, logf)
}

func runReconnectLoop(ctx context.Context, run func(uint64) bool, onRetry func(uint64, time.Duration), wait func(context.Context, time.Duration)) {
	for attempt, epoch := 0, uint64(1); ; attempt, epoch = attempt+1, epoch+1 {
		if run(epoch) {
			return
		}
		if ctx.Err() != nil {
			return
		}
		delay := Backoff(attempt)
		onRetry(epoch, delay)
		wait(ctx, delay)
		if ctx.Err() != nil {
			return
		}
	}
}

func runHFTEpoch(ctx context.Context, cancel context.CancelFunc, streamFactory hftStreamFactory, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, epoch uint64, refreshAuth func(context.Context) error, logf func(string, ...any)) slotEpochResult {
	tokens := slot.Tokens
	stream, err := streamFactory()
	if err != nil {
		_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "disconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
		logf("HFT connect failed: %v", err)
		return epochRetryable
	}
	defer stream.Close()

	epochStop := make(chan struct{}, 1)
	signalEpochStop := func() {
		select {
		case epochStop <- struct{}{}:
		default:
		}
	}
	_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotConnecting), ReceivedTsMs: time.Now().UnixMilli()})
	responses := make(chan arrow.HFTResponsePacket, 1)
	lastFrameNanos := atomic.Int64{}
	decodeErrors := 0
	decodeWindow := time.Now()
	authRefreshes := 0
	terminalAuthFailure := false
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	// R-185: a new connection epoch begins — restart the per-slot tick
	// sequence so feed_sequence_local does not grow across reconnects.
	bridgeEmitter.resetSeq(slot.SlotID)

	// onDecoded fires immediately before each LTP/full tick dispatch in the
	// same read goroutine, so lastDecoded holds the exact decompressed packet
	// bytes for the next tick callback. This preserves the plan's raw_payload
	// invariant: decoded JSON must not replace the original broker packet bytes.
	var lastDecoded []byte
	go stream.ReadHFTWithFrame(readCtx,
		func(t arrow.HFTLTPTick) {
			lastFrameNanos.Store(time.Now().UnixNano())
			_ = bridgeEmitter.EmitTick(Tick{
				Feed: "hft", Mode: "ltpc", Token: t.Token,
				LTP: t.LTP, VWAP: t.VWAP, Volume: t.Volume,
				ATV: t.ATV, BTV: t.BTV,
				TS: int64(t.LTT / 1_000_000),
			}, slot.ConnectionID, slot.SlotID, epoch, time.Now(), lastDecoded)
		},
		func(t arrow.HFTFullTick) {
			lastFrameNanos.Store(time.Now().UnixNano())
			_ = bridgeEmitter.EmitTick(Tick{
				Feed: "hft", Mode: "full", Token: t.Token,
				LTP: t.LTP, LTQ: t.LTQ, VWAP: t.VWAP,
				Open: t.Open, High: t.High, Close: t.Close, Low: t.Low,
				TBQ: t.TBQ, TSQ: t.TSQ, Volume: t.Volume,
				OI: int64(t.OI), ATV: t.ATV, BTV: t.BTV,
				BidPx: t.BidPx, AskPx: t.AskPx,
				BidSize: t.BidSize, AskSize: t.AskSize,
				BidOrd: t.BidOrd, AskOrd: t.AskOrd,
				TS: int64(t.TS / 1_000_000),
			}, slot.ConnectionID, slot.SlotID, epoch, time.Now(), lastDecoded)
		},
		func(r arrow.HFTResponsePacket) {
			select {
			case responses <- r:
			default:
			}
		},
		func(mt int, payload []byte) {
			lastFrameNanos.Store(time.Now().UnixNano())
		},
		func(frame []byte) {
			lastDecoded = frame
		},
		func(err error) {
			message := err.Error()
			if isHFTAuthError(message) {
				refreshErr := error(nil)
				if refreshAuth != nil && authRefreshes < 3 {
					authRefreshes++
					refreshErr = refreshAuth(ctx)
				}
				// classifyAuthRefresh expects the prior count; authRefreshes was
				// already incremented above, so pass authRefreshes-1.
				switch classifyAuthRefresh(refreshAuth != nil, authRefreshes-1, refreshErr) {
				case authResumed:
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "reconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: "authentication_refreshed", ReceivedTsMs: time.Now().UnixMilli()})
					signalEpochStop()
					return
				case authRetry:
					logf("HFT authentication refresh failed (attempt %d): %v", authRefreshes, sanitizeDiagnostic(err.Error()))
					signalEpochStop()
					return
				default: // authTerminal or authTerminalExhausted
					reason := "authentication_refresh_failed"
					if refreshErr == nil {
						reason = "authentication_refresh_exhausted"
					}
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "auth_failure", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotTerminal), Reason: reason, ReceivedTsMs: time.Now().UnixMilli()})
					logf("HFT authentication failed; refresh exhausted")
					signalEpochStop()
					cancel()
					terminalAuthFailure = true
					return
				}
			}
			if isHFTDecodeError(message) {
				now := time.Now()
				burst := isDecodeErrorBurst(decodeErrors, decodeWindow, now)
				decodeErrors = burst.count
				decodeWindow = burst.windowStart
				logf("HFT decode error count=%d: %s", decodeErrors, sanitizeDiagnostic(message))
				// Plan §Error Handling: burst threshold is 100 errors in 10
				// seconds per slot; exceeding it closes that slot and reconnects.
				if burst.exceeded {
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "feed_stalled", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotStalled), Reason: "decode_error_burst", ReceivedTsMs: time.Now().UnixMilli()})
					signalEpochStop()
				}
				return
			}
			_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "disconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
			logf("HFT stream ended: %v", err)
			signalEpochStop()
		},
	)

	_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotSubscribing), ReceivedTsMs: time.Now().UnixMilli()})
	acknowledged := 0
	for _, request := range slot.Requests {
		if err := stream.SubscribeHFTTokens("full", arrow.HFTExchNSECM, request, latencyMs); err != nil {
			_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotPartial), AssignedTokens: len(tokens), AcknowledgedTokens: acknowledged, RejectedTokens: len(request), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
			logf("HFT subscribe write failed: %v", err)
			signalEpochStop()
			return epochRetryable
		}
		select {
		case r := <-responses:
			switch classifySubscriptionResponse(r.ErrorCode, int(r.SuccessCount), int(r.ErrorCount), len(request)) {
			case subAccepted:
				acknowledged += int(r.SuccessCount)
			case subTerminal:
				_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotTerminal), AssignedTokens: len(tokens), AcknowledgedTokens: acknowledged + int(r.SuccessCount), RejectedTokens: int(r.ErrorCount), Reason: sanitizeDiagnostic(r.ErrorMsg), ReceivedTsMs: time.Now().UnixMilli()})
				logf("HFT subscription invalid: code=%s success=%d errors=%d", r.ErrorCode, r.SuccessCount, r.ErrorCount)
				signalEpochStop()
				return epochTerminal
			default: // subPartial
				_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotPartial), AssignedTokens: len(tokens), AcknowledgedTokens: acknowledged + int(r.SuccessCount), RejectedTokens: int(r.ErrorCount), Reason: sanitizeDiagnostic(r.ErrorMsg), ReceivedTsMs: time.Now().UnixMilli()})
				logf("HFT subscription rejected: code=%s success=%d errors=%d", r.ErrorCode, r.SuccessCount, r.ErrorCount)
				signalEpochStop()
				return epochRetryable
			}
		case <-time.After(responseTimeout):
			_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotTerminal), AssignedTokens: len(tokens), RejectedTokens: len(tokens), Reason: "subscription_response_timeout", ReceivedTsMs: time.Now().UnixMilli()})
			signalEpochStop()
			return epochTerminal
		case <-ctx.Done():
			return epochRecovered
		}
	}
	logf("HFT subscribed %d tokens (latency=%dms)", len(tokens), latencyMs)
	lastFrameNanos.Store(time.Now().UnixNano())
	_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotActive), AssignedTokens: len(tokens), AcknowledgedTokens: len(tokens), ReceivedTsMs: time.Now().UnixMilli()})

	// R-059: heartbeat and watchdog goroutines must stop when the epoch ends
	// (epochStop), not just on the process context — otherwise a dead epoch's
	// goroutines keep writing to a closed stream and emit spurious events.
	heartbeat := time.NewTicker(3 * time.Second)
	defer heartbeat.Stop()
	go func() {
		for {
			select {
			case <-heartbeat.C:
				if err := stream.WriteText("PONG"); err != nil {
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "heartbeat_failed", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
					logf("HFT heartbeat failed: %v", err)
					signalEpochStop()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	watchdog := time.NewTicker(time.Second)
	defer watchdog.Stop()
	go func() {
		for {
			select {
			case <-watchdog.C:
				last := lastFrameNanos.Load()
				if last > 0 && time.Since(time.Unix(0, last)) > 15*time.Second {
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "feed_stalled", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotStalled), Reason: "no_tick_for_15s", ReceivedTsMs: time.Now().UnixMilli()})
					signalEpochStop()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-epochStop:
	}
	if terminalAuthFailure {
		return epochTerminal
	}
	if ctx.Err() != nil {
		return epochRecovered
	}
	return epochRetryable
}

func isHFTDecodeError(message string) bool {
	return strings.Contains(message, "hft unknown packet") ||
		strings.Contains(message, "hft incomplete frame") ||
		(strings.Contains(message, "hft ") && strings.Contains(message, "parse"))
}

// decodeBurstResult is the outcome of one decode-error observation against the
// 10-second window.
type decodeBurstResult struct {
	count       int // running error count within the current window
	windowStart time.Time
	exceeded    bool // count reached maxDecodeErrorsPer10s within the window
}

// isDecodeErrorBurst applies the plan's decode-error burst policy: errors are
// counted in a rolling 10-second window; reaching maxDecodeErrorsPer10s
// (100) within one window reports exceeded=true. A window older than 10s
// resets the count. Callers pass the prior count and window start.
func isDecodeErrorBurst(priorCount int, windowStart time.Time, now time.Time) decodeBurstResult {
	if !windowStart.IsZero() && now.Sub(windowStart) > 10*time.Second {
		priorCount = 0
		windowStart = now
	}
	if windowStart.IsZero() {
		windowStart = now
	}
	count := priorCount + 1
	return decodeBurstResult{
		count:       count,
		windowStart: windowStart,
		exceeded:    count >= maxDecodeErrorsPer10s,
	}
}

func isHFTAuthError(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "authentication") ||
		strings.Contains(lower, "invalid token") ||
		strings.Contains(lower, "token expired") ||
		strings.Contains(lower, "401")
}

// authRefreshOutcome classifies one authentication-refresh step per the plan:
// the slot may retry (still within the 3-attempt budget), become terminal
// because the budget is exhausted, or resume because the refresh succeeded.
type authRefreshOutcome int

const (
	authRetry             authRefreshOutcome = iota // refresh failed, attempts remain
	authTerminal                                    // refresh failed and attempts exhausted
	authTerminalExhausted                           // no refresh function or budget already exhausted
	authResumed                                     // refresh succeeded
)

// classifyAuthRefresh is a pure decision helper for the plan's bounded auth
// refresh (3 attempts per slot failure episode). hasRefresh=false means no
// refresh is possible → terminal. refreshErr != nil means this refresh failed.
//
// R-023: a nil refreshErr must NOT be treated as success when no refresh is
// possible — a token-only deployment (refreshAuth == nil, so refreshErr stays
// nil) previously looped "reconnect / authentication_refreshed" forever.
func classifyAuthRefresh(hasRefresh bool, authRefreshes int, refreshErr error) authRefreshOutcome {
	if !hasRefresh || authRefreshes >= 3 {
		return authTerminalExhausted
	}
	if refreshErr == nil {
		return authResumed
	}
	// This attempt counted: the next check sees authRefreshes+1.
	if authRefreshes+1 >= 3 {
		return authTerminal
	}
	return authRetry
}

// subscriptionResponseOutcome classifies a subscription response against the
// requested batch size (plan §Integrations): SUCCESS with success_count equal
// to the batch and zero errors is accepted; an all-invalid/parameter error is
// terminal; any other partial/nonzero-error outcome is a partial rejection.
type subscriptionResponseOutcome int

const (
	subAccepted subscriptionResponseOutcome = iota
	subPartial
	subTerminal
)

func classifySubscriptionResponse(errorCode string, successCount, errorCount, requested int) subscriptionResponseOutcome {
	if errorCode == "SUCCESS" && successCount == requested && errorCount == 0 {
		return subAccepted
	}
	switch errorCode {
	case "E_ALL_INVALID", "E_INVALID_JSON", "E_MISSING_FIELD", "E_INVALID_PARAM":
		return subTerminal
	default:
		return subPartial
	}
}

func runStandard(ctx context.Context, cancel context.CancelFunc, client *arrow.Client, tokens []int32, logf func(string, ...any)) {
	ds, err := client.ConnectDataStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: standard connect failed: %v\n", err)
		os.Exit(1)
	}
	defer ds.Close()

	if err := ds.Subscribe(arrow.StreamModeFull, tokens); err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: standard subscribe failed: %v\n", err)
		os.Exit(1)
	}
	logf("standard subscribed %d tokens (mode=full)", len(tokens))

	go ds.ReadTicks(ctx, func(t arrow.MarketTick) {
		emit(fromStandardTick(t))
	}, func(err error) {
		logf("standard stream ended: %v", err)
		cancel()
	})

	<-ctx.Done()
	logf("standard bridge stopped")
}

func fromStandardTick(t arrow.MarketTick) Tick {
	tsMs := int64(t.LTT) * 1000
	if tsMs == 0 {
		tsMs = int64(t.Time) * 1000
	}
	var bidPx, askPx [5]int32
	var bidQty, askQty [5]int32
	var bidOrd, askOrd [5]uint16
	for i, b := range t.Bids {
		if i >= 5 {
			break
		}
		bidPx[i] = b.Price
		bidQty[i] = int32(b.Quantity)
		bidOrd[i] = uint16(b.Orders)
	}
	for i, a := range t.Asks {
		if i >= 5 {
			break
		}
		askPx[i] = a.Price
		askQty[i] = int32(a.Quantity)
		askOrd[i] = uint16(a.Orders)
	}
	oi := int64(0)
	if t.OI > 0 {
		oi = int64(t.OI)
	}
	return Tick{
		Feed: "standard", Mode: string(t.Mode), Token: t.Token,
		LTP: t.LTP, Close: t.Close, Open: t.Open, High: t.High, Low: t.Low,
		LTQ: t.LTQ, Volume: t.Volume, OI: oi,
		TBQ: int64(t.TotalBuyQuantity), TSQ: int64(t.TotalSellQuantity),
		ChangeFlag: t.ChangeFlag, AvgPrice: t.AvgPrice,
		LowerLimit: t.LowerLimit, UpperLimit: t.UpperLimit,
		BidPx: bidPx, AskPx: askPx,
		BidSize: bidQty, AskSize: askQty,
		BidOrd: bidOrd, AskOrd: askOrd,
		TS: tsMs,
	}
}

func emit(t Tick) {
	// R-058: standard-mode ticks and lifecycle events MUST share one stdout
	// writer. The global encoder and bridgeEmitter were two independent
	// writers — interleaved NDJSON lines corrupted ordering for Java.
	if err := bridgeEmitter.write(t); err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: stdout write error: %v\n", err)
		os.Exit(1)
	}
}

func envOrFatal(key string) string {
	v := os.Getenv(key)
	if v == "" {
		// R-175: a missing required env is a fatal config/startup failure —
		// exit status 2 per the file's own contract, not 1 (unexpected
		// supervisor failure).
		fmt.Fprintf(os.Stderr, "arrow-bridge: missing required env: %s\n", key)
		os.Exit(exitFatalStart)
	}
	return v
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseTokensEnv(key string) []int32 {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	var out []int32
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "arrow-bridge: invalid token %q: %v\n", s, err)
			continue
		}
		// R-176: a token outside the int32 range silently wrapped and negative
		// values were accepted verbatim — both corrupt the subscription set.
		if n <= 0 || int64(n) > int64(^uint32(0)>>1) {
			fmt.Fprintf(os.Stderr, "arrow-bridge: token %d out of range (must be 1..%d); skipping\n", n, int64(^uint32(0)>>1))
			continue
		}
		out = append(out, int32(n))
	}
	return out
}

// loadTokensFromCSV reads Arrow broker instrument CSV and extracts all tokens.
// Priority: 1) ARROW_INSTRUMENT_TOKENS env var, 2) CSV file at csvPath, 3) synthetic fallback.
func loadTokensFromCSV(envKey, csvPath string) []int32 {
	// 1. ENV var override (explicit is always respected)
	if tokens := parseTokensEnv(envKey); len(tokens) > 0 {
		fmt.Fprintf(os.Stderr, "arrow-bridge: %d tokens from %s env var\n", len(tokens), envKey)
		return tokens
	}

	// 2. Parse CSV file — Arrow format: Exchange,Segment,ExchSeg,Token,...
	f, err := os.Open(csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: cannot open %s: %v (falling back)\n", csvPath, err)
		return nil
	}
	defer f.Close()

	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: cannot read CSV header from %s: %v\n", csvPath, err)
		return nil
	}

	// Find Token column index
	tokenIdx := -1
	for i, col := range header {
		if strings.TrimSpace(col) == "Token" {
			tokenIdx = i
			break
		}
	}
	if tokenIdx < 0 {
		fmt.Fprintf(os.Stderr, "arrow-bridge: CSV %s has no Token column\n", csvPath)
		return nil
	}

	var tokens []int32
	lineNum := 1
	for {
		record, err := reader.Read()
		if err != nil {
			break // EOF or error
		}
		lineNum++
		if len(record) <= tokenIdx {
			continue
		}
		tokenStr := strings.TrimSpace(record[tokenIdx])
		if tokenStr == "" {
			continue
		}
		n, err := strconv.Atoi(tokenStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "arrow-bridge: bad token at CSV line %d: %q\n", lineNum, tokenStr)
			continue
		}
		// R-176: reject out-of-range/negative tokens instead of silently
		// narrowing to int32.
		if n <= 0 || int64(n) > int64(^uint32(0)>>1) {
			fmt.Fprintf(os.Stderr, "arrow-bridge: token %d out of range at CSV line %d; skipping\n", n, lineNum)
			continue
		}
		tokens = append(tokens, int32(n))
	}

	fmt.Fprintf(os.Stderr, "arrow-bridge: %d tokens from %s (%d lines)\n", len(tokens), csvPath, lineNum-1)
	return tokens
}

func truthy(s string) bool {
	s = strings.TrimSpace(strings.ToLower(s))
	return s == "1" || s == "true" || s == "yes"
}
