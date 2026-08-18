package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

func main() {
	authToken := strings.TrimSpace(os.Getenv("EXECUTION_BRIDGE_AUTH_TOKEN"))
	if authToken == "" {
		fmt.Fprintln(os.Stderr, "execution-bridge: EXECUTION_BRIDGE_AUTH_TOKEN is required")
		os.Exit(2)
	}
	mode := strings.ToLower(strings.TrimSpace(envOrDefault("EXECUTION_BRIDGE_MODE", "disabled")))
	broker, client, err := brokerFromEnvironment(mode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execution-bridge: startup blocked: %v\n", err)
		os.Exit(2)
	}
	server, err := NewBridgeServer(broker, authToken, mode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execution-bridge: startup blocked: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if client != nil {
		go RunPostbackLoop(ctx, func() (OrderUpdateSource, error) {
			return NewArrowOrderUpdateSource(client)
		}, server.hub.Publish, func(err error) {
			fmt.Fprintf(os.Stderr, "execution-bridge: postback: %s\n", sanitizeReason(err))
		})
	}

	addr := envOrDefault("EXECUTION_BRIDGE_LISTEN_ADDR", "127.0.0.1:8787")
	httpServer := &http.Server{Addr: addr, Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	fmt.Fprintf(os.Stderr, "execution-bridge: listening addr=%s mode=%s\n", addr, mode)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "execution-bridge: server failed: %v\n", err)
		os.Exit(1)
	}
}

func brokerFromEnvironment(mode string) (Broker, *arrow.Client, error) {
	switch mode {
	case "disabled":
		return NewFakeBrokerWithDisabledResult(), nil, nil
	case "fake":
		return NewFakeBroker(), nil, nil
	case "live":
		appID := strings.TrimSpace(os.Getenv("ARROW_APP_ID"))
		appSecret := strings.TrimSpace(os.Getenv("ARROW_APP_SECRET"))
		if appID == "" || appSecret == "" {
			return nil, nil, fmt.Errorf("live mode requires Arrow credentials inside the bridge")
		}
		client := arrow.NewClient(appID, appSecret)
		if token := strings.TrimSpace(os.Getenv("ARROW_TOKEN")); token != "" {
			client.SetToken(token)
		} else {
			user, password, totp := os.Getenv("ARROW_USER_ID"), os.Getenv("ARROW_PASSWORD"), os.Getenv("ARROW_TOTP_KEY")
			if user == "" || password == "" || totp == "" {
				return nil, nil, fmt.Errorf("live mode requires ARROW_TOKEN or AutoLogin credentials")
			}
			if err := client.AutoLogin(user, password, totp); err != nil {
				return nil, nil, fmt.Errorf("Arrow authentication failed")
			}
		}
		broker, err := NewArrowBroker(client)
		return broker, client, err
	default:
		return nil, nil, fmt.Errorf("unsupported EXECUTION_BRIDGE_MODE %q", mode)
	}
}

func NewFakeBrokerWithDisabledResult() *FakeBroker {
	fake := NewFakeBroker()
	disabled := BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	for _, command := range []string{CommandPlace, CommandModify, CommandCancel, CommandQueryOrder,
		CommandReconcileOrders, CommandReconcileTrades, CommandReconcilePosition} {
		fake.SetResult(command, disabled)
	}
	return fake
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
