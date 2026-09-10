package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"time"
)

// newStopAlertSender returns a sink that POSTs each StopEvent to url as JSON,
// the moment the gateway stops a run. The body carries the structured fields
// plus a human line under both "text" (Slack incoming webhooks) and "content"
// (Discord webhooks), so pasting either kind of URL works with no adapter.
//
// It is metadata only — run, owner, reason, counts, dollars; never a prompt —
// and fail-open: a slow or failing sink is logged and dropped. The controller
// already invokes the sink on its own goroutine, so this may block on the
// network without ever touching a request.
func newStopAlertSender(url string) func(StopEvent) {
	client := &http.Client{Timeout: 10 * time.Second}
	return func(ev StopEvent) {
		body, err := json.Marshal(stopAlertPayload(ev))
		if err != nil {
			stopAlertErr(err)
			return
		}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			stopAlertErr(err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			stopAlertErr(err)
			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			stopAlertErr(fmt.Errorf("alert endpoint returned %s", resp.Status))
		}
	}
}

func stopAlertErr(err error) {
	fmt.Fprintln(os.Stderr, "gateway: stop alert not delivered (fail-open):", err)
}

// stopAlertPayload shapes the event for a webhook. "text"/"content" are the
// human-readable line; the rest is machine-readable for anything custom.
func stopAlertPayload(ev StopEvent) map[string]any {
	who := "run " + ev.Run
	if ev.Agent != "" {
		who = ev.Agent + " (run " + ev.Run + ")"
	}
	line := fmt.Sprintf("AxiGate stopped %s — %s. %d calls, $%.2f spent so far; further calls are being refused.",
		who, ev.Reason, ev.Calls, ev.SpendUSD)
	return map[string]any{
		"text":      line,
		"content":   line,
		"event":     "run_stopped",
		"run":       ev.Run,
		"agent":     ev.Agent,
		"team":      ev.Team,
		"model":     ev.Model,
		"reason":    ev.Reason,
		"calls":     ev.Calls,
		"spend_usd": math.Round(ev.SpendUSD*1e6) / 1e6, // no float noise for JSON consumers
		"at":        ev.At.UTC().Format(time.RFC3339),
	}
}
