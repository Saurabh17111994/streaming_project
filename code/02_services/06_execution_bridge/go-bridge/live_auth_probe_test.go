package main

// Live Arrow auth probe (intentionally env-gated + secret-safe).
// Usage: load the 5 ARROW_* keys from .env, set LIVE_AUTH_PROBE=1, then
//   go test -run TestLiveAuthProbe -v
// It exercises the exact AutoLogin path the bridge uses in live mode and
// prints only AUTH_PASS / AUTH_FAIL / AUTH_SKIP (secrets are redacted by the
// caller before display; nothing here prints credential values). With
// LIVE_AUTH_PROBE unset the test skips, so normal `go test ./...` is
// unaffected and needs no broker/network.
//
// See plan docs/plans/arrow-rest-live-capability-unblock.md Phase 0.

import (
	"os"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

func TestLiveAuthProbe(t *testing.T) {
	if os.Getenv("LIVE_AUTH_PROBE") != "1" {
		t.Skip("LIVE_AUTH_PROBE not set")
	}
	got := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	appID, appSecret := got("ARROW_APP_ID"), got("ARROW_APP_SECRET")
	user, password, totp := got("ARROW_USER_ID"), got("ARROW_PASSWORD"), got("ARROW_TOTP_KEY")
	for _, k := range []string{"ARROW_APP_ID", "ARROW_APP_SECRET", "ARROW_USER_ID", "ARROW_PASSWORD", "ARROW_TOTP_KEY"} {
		if got(k) == "" {
			t.Logf("AUTH_SKIP: missing %s", k)
			return
		}
	}
	client := arrow.NewClient(appID, appSecret)
	if err := client.AutoLogin(user, password, totp); err != nil {
		// Do not print creds; error is relayed and sanitized/redacted upstream.
		t.Logf("AUTH_FAIL: %v", err)
		return
	}
	t.Log("AUTH_PASS")
}